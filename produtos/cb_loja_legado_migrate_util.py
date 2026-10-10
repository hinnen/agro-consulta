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
    pos_liberacao_grupo: bool = False,
) -> tuple[str, str | None, str | None]:
    """
    Legado 230… (DV inválido) → principal = EAN bipável; legado fica para opcionais.

    Retorna (principal, legado_opcional, erro). Sem alteração: (cb, None, None).
    """
    from produtos.agro_codigo_barras_loja_util import (
        validar_codigo_barras_loja_para_salvar,
        validar_codigo_barras_loja_pos_grupo_migracao,
    )

    atual = _digits(cb)
    if not eh_codigo_barras_loja(atual) or ean13_checksum_ok(atual):
        return atual, None, None

    novo = ean13_para_bip_codigo_barras_loja(atual)
    if not novo or novo == atual or not ean13_checksum_ok(novo):
        return atual, None, None

    pid = str(produto_externo_id or "").strip()[:64]
    if pos_liberacao_grupo:
        erro = validar_codigo_barras_loja_pos_grupo_migracao(
            novo,
            produto_externo_id=pid,
            db=db,
            col=col,
        )
    else:
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
    pos_liberacao_grupo: bool = False,
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
        pos_liberacao_grupo=pos_liberacao_grupo,
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
                pos_liberacao_grupo=pos_liberacao_grupo,
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


def _cb_atual_de_produto(produto, overlay=None) -> str:
    atual = _digits(getattr(produto, "codigo_barras", "") or "")
    if not atual and overlay is not None:
        atual = _digits(getattr(overlay, "codigo_barras", "") or "")
    return atual


def _escolher_vencedor_grupo_migracao(members: list[tuple]) -> tuple:
    """Preferir cadastro GM (ex. GM0024-P) quando vários legados bipam o mesmo EAN."""
    from produtos.models import Produto

    best: tuple | None = None
    best_score = -1
    for p, ov, _atual, _bip in members:
        pid = str(getattr(p, "produto_externo_id", "") or "").strip()
        row = (
            Produto.objects.filter(produto_externo_id=pid)
            .only("codigo_nfe", "nome", "produto_externo_id")
            .first()
            if pid
            else None
        )
        gm = str(getattr(row, "codigo_nfe", "") or "").upper()
        nome = str(getattr(row, "nome", "") or "").upper()
        score = 0
        if gm.startswith("GM"):
            score += 100
        if "-P" in gm or "-P" in nome:
            score += 20
        if score > best_score:
            best_score = score
            best = (p, ov, _atual, _bip)
    return best or members[0]


def _reatribuir_demais_do_grupo_bip(
    legado_ref: str,
    *,
    vencedor_pid: str,
    dry_run: bool,
    db=None,
    col: str | None = None,
) -> list[dict[str, Any]]:
    """Tira do grupo físico todos os outros (230 novo) para o vencedor ficar com o EAN bipável."""
    from produtos.cb_loja_reatribuir_util import reatribuir_cb_loja_exclusivo
    from produtos.models import Produto, ProdutoGestaoOverlayAgro

    grupo = set(codigos_grupo_bip_canonico(legado_ref))
    vencedor_pid = str(vencedor_pid or "").strip()[:64]
    feitos: list[dict[str, Any]] = []
    vistos: set[str] = set()

    def _reatribuir_pid(pid: str, cb: str) -> None:
        if not pid or pid in vistos or pid == vencedor_pid:
            return
        cb = _digits(cb)
        if cb not in grupo:
            return
        p = Produto.objects.filter(produto_externo_id=pid).first()
        if p is None or not getattr(p, "pk", None):
            return
        if _digits(p.codigo_barras) != cb:
            ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()
            if ov is None or _digits(ov.codigo_barras) != cb:
                return
        vistos.add(pid)
        res = reatribuir_cb_loja_exclusivo(
            p,
            esperado_atual=cb,
            dry_run=dry_run,
            db=db,
            col=col,
        )
        feitos.append({"pid": pid, "reatribuir": res})

    for p in Produto.objects.filter(codigo_barras__in=grupo).only(
        "id", "produto_externo_id", "codigo_barras"
    ):
        _reatribuir_pid(str(p.produto_externo_id or ""), p.codigo_barras)

    for ov in ProdutoGestaoOverlayAgro.objects.filter(codigo_barras__in=grupo).only(
        "produto_externo_id", "codigo_barras"
    ):
        _reatribuir_pid(str(ov.produto_externo_id or ""), ov.codigo_barras)

    return feitos


def _limpar_opcionais_grupo_bip_outros(
    legado_ref: str,
    *,
    vencedor_pid: str,
    dry_run: bool,
) -> int:
    """Remove códigos do grupo físico dos opcionais de outros produtos (não bloqueia o bip)."""
    from produtos.models import ProdutoGestaoOverlayAgro
    from produtos.mongo_index_codigos import (
        CAD_EXTRAS_CB_OPCIONAIS_KEYS,
        codigos_barras_opcionais_de_cadastro_extras,
    )

    grupo = set(codigos_grupo_bip_canonico(legado_ref))
    vencedor_pid = str(vencedor_pid or "").strip()[:64]
    alterados = 0
    for ov in ProdutoGestaoOverlayAgro.objects.exclude(
        produto_externo_id=vencedor_pid
    ).only("produto_externo_id", "cadastro_extras"):
        ce = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
        opc = codigos_barras_opcionais_de_cadastro_extras(ce)
        if not opc:
            continue
        nova_lista = [c for c in opc if _digits(c) not in grupo]
        if len(nova_lista) == len(opc):
            continue
        alterados += 1
        if dry_run:
            continue
        ce = dict(ce)
        for key in CAD_EXTRAS_CB_OPCIONAIS_KEYS:
            ce.pop(key, None)
        if nova_lista:
            ce["codigos_barras_opcionais"] = nova_lista
        ov.cadastro_extras = ce
        ov.save(update_fields=["cadastro_extras"])
    return alterados


def _migrar_cb_loja_legado_lote_por_grupo(
    *,
    limit: int = 5000,
    dry_run: bool = False,
    db=None,
    col: str | None = None,
) -> dict[str, Any]:
    """Um EAN bipável por grupo: reatribui os demais, migra o vencedor (GM preferido)."""
    from collections import defaultdict

    por_bip: dict[str, list[tuple]] = defaultdict(list)
    for item in iter_produtos_cb_loja_legado(limit=limit):
        p, overlay = item if isinstance(item, tuple) else (item, None)
        atual = _cb_atual_de_produto(p, overlay)
        bip = ean13_para_bip_codigo_barras_loja(atual)
        if not bip or not atual:
            continue
        por_bip[bip].append((p, overlay, atual, bip))

    ok = 0
    colisoes: list[dict[str, Any]] = []
    reatribuidos = 0

    for _bip_key, members in sorted(por_bip.items()):
        p, overlay, atual, _bip = _escolher_vencedor_grupo_migracao(members)
        pid = str(getattr(p, "produto_externo_id", "") or "").strip()
        if not pid:
            continue

        def _processar_grupo() -> dict[str, Any] | None:
            nonlocal reatribuidos
            feitos: list[dict[str, Any]] = []
            if dry_run:
                reatribuidos += max(0, len(members) - 1)
            else:
                feitos = _reatribuir_demais_do_grupo_bip(
                    atual,
                    vencedor_pid=pid,
                    dry_run=False,
                    db=db,
                    col=col,
                )
                reatribuidos += len(feitos)
                _limpar_opcionais_grupo_bip_outros(
                    atual,
                    vencedor_pid=pid,
                    dry_run=False,
                )
            return migrar_cb_loja_legado_em_produto(
                p,
                overlay=overlay,
                dry_run=dry_run,
                liberar_intruso=False,
                pos_liberacao_grupo=True,
                db=db,
                col=col,
            )

        if dry_run:
            r = _processar_grupo()
        else:
            with transaction.atomic():
                r = _processar_grupo()

        if r and r.get("erro"):
            colisoes.append(r)
        elif r:
            ok += 1

    return {
        "dry_run": dry_run,
        "corrigidos": ok,
        "colisoes": len(colisoes),
        "colisoes_detalhe": colisoes,
        "reatribuidos_grupo": reatribuidos,
        "grupos": len(por_bip),
    }


def migrar_cb_loja_legado_lote(
    *,
    limit: int = 5000,
    dry_run: bool = False,
    liberar_intruso: bool = False,
    db=None,
    col: str | None = None,
) -> dict[str, Any]:
    """Varredura única: corrige o que puder; devolve contagem e lista de colisões."""
    if liberar_intruso:
        return _migrar_cb_loja_legado_lote_por_grupo(
            limit=limit,
            dry_run=dry_run,
            db=db,
            col=col,
        )

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
