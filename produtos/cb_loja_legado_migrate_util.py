"""Promove EAN-13 válido (bip) como principal e guarda legado 230… em opcionais."""
from __future__ import annotations

import logging
from typing import Any

from django.db import transaction

from produtos.agro_codigo_barras_loja_util import (
    codigos_grupo_bip_canonico,
    ean13_checksum_ok,
    ean13_para_bip_codigo_barras_loja,
    eh_codigo_barras_loja,
)
from produtos.mongo_index_codigos import mesclar_codigos_barras_opcionais_adicionar

logger = logging.getLogger(__name__)


def _digits(cb: str) -> str:
    return "".join(ch for ch in str(cb or "") if ch.isdigit())


def preparar_codigo_barras_loja_legado(
    cb: str,
    *,
    produto_externo_id: str = "",
    db=None,
    col: str | None = None,
) -> tuple[str, str | None, str | None]:
    """
    Legado 230… (DV inválido) → principal = EAN bipável; legado fica para opcionais.

    Retorna (principal, legado_opcional, erro). Sem alteração: (cb, None, None).
    """
    from produtos.agro_codigo_barras_loja_util import validar_codigo_barras_loja_para_salvar

    atual = _digits(cb)
    if not eh_codigo_barras_loja(atual) or ean13_checksum_ok(atual):
        return atual, None, None

    novo = ean13_para_bip_codigo_barras_loja(atual)
    if not novo or novo == atual or not ean13_checksum_ok(novo):
        return atual, None, None

    pid = str(produto_externo_id or "").strip()[:64]
    erro = validar_codigo_barras_loja_para_salvar(
        novo,
        produto_externo_id=pid,
        db=db,
        col=col,
    )
    if erro:
        return (
            atual,
            None,
            f"{erro} (cadastro legado {atual} → bip {novo}). "
            "Reatribua o código 230 no produto que já usa o EAN bipável e tente de novo.",
        )
    return novo, atual, None


def mesclar_legado_cb_em_cadastro_extras(
    cadastro_extras: dict | None,
    *,
    legado: str,
    principal: str,
) -> dict:
    ce = dict(cadastro_extras) if isinstance(cadastro_extras, dict) else {}
    merged = mesclar_codigos_barras_opcionais_adicionar(
        ce,
        [legado],
        principal=principal,
    )
    if merged:
        ce["codigos_barras_opcionais"] = merged
    return ce


def _rotulo_produto_cb(produto) -> str:
    gm = str(getattr(produto, "codigo_nfe", "") or "").strip()
    nome = str(getattr(produto, "nome", "") or "").strip()[:40]
    if gm:
        return gm if not nome else f"{gm} · {nome}"
    return nome or str(getattr(produto, "produto_externo_id", "") or "")[:20]


def _intruso_com_cb_literal(bip: str, *, dono_pid: str):
    """Outro produto com o EAN bipável literal (bloqueia promoção do legado)."""
    from produtos.models import Produto, ProdutoGestaoOverlayAgro

    bip = _digits(bip)
    pid = str(dono_pid or "").strip()[:64]
    if not bip:
        return None
    p = (
        Produto.objects.filter(codigo_barras=bip)
        .exclude(produto_externo_id=pid)
        .only("id", "produto_externo_id", "codigo_barras", "codigo_nfe", "nome")
        .first()
    )
    if p is not None:
        return p
    ov = (
        ProdutoGestaoOverlayAgro.objects.filter(codigo_barras=bip)
        .exclude(produto_externo_id=pid)
        .only("produto_externo_id")
        .first()
    )
    if ov is None:
        return None
    return (
        Produto.objects.filter(produto_externo_id=ov.produto_externo_id)
        .only("id", "produto_externo_id", "codigo_barras", "codigo_nfe", "nome")
        .first()
    )


def _liberar_intruso_grupo_bip(
    bip: str,
    *,
    dono_pid: str,
    legado: str,
    dry_run: bool,
    db=None,
    col: str | None = None,
) -> dict[str, Any] | None:
    """Reatribui 230 no produto que ocupa o EAN bipável do mesmo grupo físico."""
    from produtos.cb_loja_reatribuir_util import reatribuir_cb_loja_exclusivo

    bip = _digits(bip)
    legado = _digits(legado)
    if not bip or not legado or bip not in set(codigos_grupo_bip_canonico(legado)):
        return None
    intruso = _intruso_com_cb_literal(bip, dono_pid=dono_pid)
    if intruso is None or not getattr(intruso, "pk", None):
        return None
    cb_intruso = _digits(getattr(intruso, "codigo_barras", ""))
    if cb_intruso != bip:
        from produtos.models import ProdutoGestaoOverlayAgro

        ov_i = ProdutoGestaoOverlayAgro.objects.filter(
            produto_externo_id=intruso.produto_externo_id
        ).only("codigo_barras").first()
        if ov_i is None or _digits(ov_i.codigo_barras) != bip:
            return None
        if cb_intruso != bip:
            intruso.codigo_barras = bip
    res = reatribuir_cb_loja_exclusivo(
        intruso,
        esperado_atual=bip,
        dry_run=dry_run,
        db=db,
        col=col,
    )
    return {
        "intruso_id": str(intruso.produto_externo_id or intruso.pk),
        "intruso_rotulo": _rotulo_produto_cb(intruso),
        "reatribuir": res,
    }


def migrar_cb_loja_legado_em_produto(
    produto,
    *,
    overlay=None,
    dry_run: bool = False,
    liberar_intruso: bool = False,
    db=None,
    col: str | None = None,
) -> dict[str, Any] | None:
    """
    Se ``codigo_barras`` for 230… legado sem DV: principal = EAN bipável; legado → opcional.
    Retorna resumo da alteração ou None se nada a fazer.
    """
    from produtos.models import ProdutoGestaoOverlayAgro

    pid = str(getattr(produto, "produto_externo_id", "") or "").strip()
    if not pid:
        return None
    if overlay is None:
        overlay = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()

    atual = _digits(getattr(produto, "codigo_barras", "") or "")
    if not atual and overlay:
        atual = _digits(getattr(overlay, "codigo_barras", "") or "")
    if not eh_codigo_barras_loja(atual) or ean13_checksum_ok(atual):
        return None

    bip_alvo = ean13_para_bip_codigo_barras_loja(atual) or atual

    novo, legado, erro = preparar_codigo_barras_loja_legado(
        atual,
        produto_externo_id=pid,
        db=db,
        col=col,
    )
    lib_meta: dict[str, Any] | None = None
    if erro and liberar_intruso and bip_alvo and bip_alvo != atual:
        lib_meta = _liberar_intruso_grupo_bip(
            bip_alvo,
            dono_pid=pid,
            legado=atual,
            dry_run=dry_run,
            db=db,
            col=col,
        )
        if lib_meta:
            novo, legado, erro = preparar_codigo_barras_loja_legado(
                atual,
                produto_externo_id=pid,
                db=db,
                col=col,
            )
        else:
            intruso = _intruso_com_cb_literal(bip_alvo, dono_pid=pid)
            extra = ""
            if intruso:
                extra = f" Ocupado por {_rotulo_produto_cb(intruso)} ({intruso.produto_externo_id})."
            erro = f"{erro}{extra}"

    if erro:
        return {
            "produto_externo_id": pid,
            "legado": atual,
            "principal_novo": bip_alvo,
            "dry_run": dry_run,
            "erro": erro,
        }
    if not legado or novo == atual:
        return None

    res = {
        "produto_externo_id": pid,
        "legado": legado,
        "principal_novo": novo,
        "dry_run": dry_run,
    }
    if lib_meta:
        res["liberar_intruso"] = lib_meta
    if dry_run:
        return res

    with transaction.atomic():
        if getattr(produto, "pk", None):
            produto.codigo_barras = novo
            produto.save(update_fields=["codigo_barras"])
        elif overlay is None:
            overlay, _ = ProdutoGestaoOverlayAgro.objects.get_or_create(
                produto_externo_id=pid[:64],
            )

        if overlay is None:
            overlay = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()
        if overlay is not None:
            overlay.codigo_barras = novo
            ce = mesclar_legado_cb_em_cadastro_extras(
                overlay.cadastro_extras,
                legado=legado,
                principal=novo,
            )
            overlay.cadastro_extras = ce
            overlay.save(update_fields=["codigo_barras", "cadastro_extras"])

    logger.info("cb loja migrado %s: %s -> %s (legado opcional)", pid, legado, novo)
    return res


def _cb_loja_legado_invalido(cb: str) -> bool:
    d = _digits(cb)
    return bool(eh_codigo_barras_loja(d) and not ean13_checksum_ok(d))


def iter_produtos_cb_loja_legado(*, limit: int = 5000):
    """Produtos com 230… legado (Postgres e/ou overlay da gestão). Yields (produto, overlay|None)."""
    from produtos.models import Produto, ProdutoGestaoOverlayAgro

    vistos: set[str] = set()
    n = 0

    def _emit(p, overlay=None):
        nonlocal n
        pid = str(getattr(p, "produto_externo_id", "") or "").strip()
        if not pid or pid in vistos:
            return False
        vistos.add(pid)
        n += 1
        return True

    qs = Produto.objects.exclude(codigo_barras="").only(
        "id", "produto_externo_id", "codigo_barras"
    )
    for p in qs.iterator(chunk_size=200):
        if not _cb_loja_legado_invalido(p.codigo_barras):
            continue
        if _emit(p):
            yield p, None
        if n >= limit:
            return

    for ov in (
        ProdutoGestaoOverlayAgro.objects.exclude(codigo_barras="")
        .only("produto_externo_id", "codigo_barras")
        .iterator(chunk_size=200)
    ):
        if not _cb_loja_legado_invalido(ov.codigo_barras):
            continue
        pid = str(ov.produto_externo_id or "").strip()
        if not pid or pid in vistos:
            continue
        p = (
            Produto.objects.filter(produto_externo_id=pid)
            .only("id", "produto_externo_id", "codigo_barras")
            .first()
        )
        if p is None:
            p = Produto(produto_externo_id=pid, codigo_barras="")
        if _emit(p, ov):
            yield p, ov
        if n >= limit:
            return


def migrar_cb_loja_legado_lote(
    *,
    limit: int = 5000,
    dry_run: bool = False,
    liberar_intruso: bool = False,
    db=None,
    col: str | None = None,
) -> dict[str, Any]:
    """Varredura única: corrige o que puder; devolve contagem e lista de colisões."""
    ok = 0
    colisoes: list[dict[str, Any]] = []
    ignorados = 0

    for item in iter_produtos_cb_loja_legado(limit=limit):
        if isinstance(item, tuple):
            p, overlay = item
        else:
            p, overlay = item, None
        r = migrar_cb_loja_legado_em_produto(
            p,
            overlay=overlay,
            dry_run=dry_run,
            liberar_intruso=liberar_intruso,
            db=db,
            col=col,
        )
        if not r:
            ignorados += 1
            continue
        if r.get("erro"):
            colisoes.append(r)
            continue
        ok += 1

    return {
        "dry_run": dry_run,
        "corrigidos": ok,
        "colisoes": len(colisoes),
        "colisoes_detalhe": colisoes,
        "ignorados": ignorados,
    }
