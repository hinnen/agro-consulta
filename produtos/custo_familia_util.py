"""
Custo família (saco → pacote/granel).

No filho (ex. milho 5 kg): ``cadastro_extras.custo_familia`` aponta para o saco (pai).
Quando o custo do pai muda (cadastro, Entrada NF, planilha), os filhos com
``auto_sync`` recalculam: ``custo_filho = custo_pai * (kg_filho / kg_pai)``.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

logger = logging.getLogger(__name__)

_Q2 = Decimal("0.01")
_Q4 = Decimal("0.0001")


def _dec(v: Any) -> Decimal | None:
    if v is None or str(v).strip() == "":
        return None
    try:
        return Decimal(str(v).replace(",", ".").strip())
    except Exception:
        return None


def _pid(v: Any) -> str:
    return str(v or "").strip()[:64]


def calcular_custo_filho(
    custo_pai: Decimal | float | str | None,
    kg_pai: Decimal | float | str | None,
    kg_filho: Decimal | float | str | None,
) -> Decimal | None:
    """custo_filho = custo_pai × (kg_filho / kg_pai), 2 casas."""
    cp = _dec(custo_pai)
    kp = _dec(kg_pai)
    kf = _dec(kg_filho)
    if cp is None or kp is None or kf is None:
        return None
    if kp <= 0 or kf <= 0:
        return None
    if cp < 0:
        return None
    return (cp * (kf / kp)).quantize(_Q2, rounding=ROUND_HALF_UP)


def normalizar_custo_familia(
    raw: Any,
    *,
    filho_id: str | None = None,
) -> dict[str, Any] | None:
    """
    Retorna dict canônico ou None (sem vínculo / desligado).
    Campos: ativo, pai_produto_id, pai_nome, kg_pai, kg_filho, auto_sync.
    """
    if not isinstance(raw, dict):
        return None
    ativo = raw.get("ativo")
    if isinstance(ativo, bool):
        on = ativo
    else:
        on = str(ativo or "").strip().lower() in ("1", "true", "yes", "on", "sim", "s")
    pai = _pid(raw.get("pai_produto_id") or raw.get("pai_id"))
    if not on or not pai:
        return None
    filho = _pid(filho_id)
    if filho and pai == filho:
        return None
    kg_pai = _dec(raw.get("kg_pai"))
    kg_filho = _dec(raw.get("kg_filho"))
    if kg_pai is None or kg_filho is None or kg_pai <= 0 or kg_filho <= 0:
        return None
    auto = raw.get("auto_sync")
    if auto is None:
        auto_sync = True
    elif isinstance(auto, bool):
        auto_sync = auto
    else:
        auto_sync = str(auto).strip().lower() not in ("0", "false", "no", "off", "nao", "não", "n")
    out: dict[str, Any] = {
        "ativo": True,
        "pai_produto_id": pai,
        "pai_nome": str(raw.get("pai_nome") or "").strip()[:200],
        "kg_pai": float(kg_pai.quantize(_Q4, rounding=ROUND_HALF_UP)),
        "kg_filho": float(kg_filho.quantize(_Q4, rounding=ROUND_HALF_UP)),
        "auto_sync": auto_sync,
    }
    if raw.get("ultimo_custo_calculado") is not None:
        uc = _dec(raw.get("ultimo_custo_calculado"))
        if uc is not None:
            out["ultimo_custo_calculado"] = float(uc.quantize(_Q2, rounding=ROUND_HALF_UP))
    if raw.get("atualizado_em"):
        out["atualizado_em"] = str(raw.get("atualizado_em"))[:40]
    return out


def extrair_custo_familia(extras: Any) -> dict[str, Any] | None:
    if not isinstance(extras, dict):
        return None
    return normalizar_custo_familia(extras.get("custo_familia"))


def mesclar_custo_familia_no_extras(
    ex: dict,
    raw: Any,
    *,
    filho_id: str | None = None,
) -> dict:
    """Atualiza ``ex`` in-place; remove chave se vínculo inválido/desligado."""
    if raw is None:
        return ex
    if not isinstance(raw, dict):
        ex.pop("custo_familia", None)
        return ex
    ativo = raw.get("ativo")
    if ativo is False or str(ativo or "").strip().lower() in (
        "0",
        "false",
        "off",
        "nao",
        "não",
        "n",
    ):
        ex.pop("custo_familia", None)
        return ex
    norm = normalizar_custo_familia(raw, filho_id=filho_id)
    if not norm:
        ex.pop("custo_familia", None)
        return ex
    prev = ex.get("custo_familia") if isinstance(ex.get("custo_familia"), dict) else {}
    if prev.get("ultimo_custo_calculado") is not None and "ultimo_custo_calculado" not in norm:
        norm["ultimo_custo_calculado"] = prev.get("ultimo_custo_calculado")
    if prev.get("atualizado_em") and "atualizado_em" not in norm:
        norm["atualizado_em"] = prev.get("atualizado_em")
    ex["custo_familia"] = norm
    return ex


def ler_custo_produto(pid: str) -> Decimal | None:
    """Custo canônico SisVale: overlay → Produto.custo."""
    pid64 = _pid(pid)
    if not pid64:
        return None
    try:
        from produtos.models import ProdutoGestaoOverlayAgro

        ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid64).first()
        if ov and isinstance(ov.cadastro_extras, dict):
            raw = ov.cadastro_extras.get("preco_custo_overlay")
            d = _dec(raw)
            if d is not None:
                return d.quantize(_Q2, rounding=ROUND_HALF_UP)
        from produtos.catalogo_agro import obter_produto_model

        p = obter_produto_model(pid64)
        if p is not None and p.custo is not None:
            return Decimal(str(p.custo)).quantize(_Q2, rounding=ROUND_HALF_UP)
    except Exception:
        logger.warning("custo_familia: ler_custo_produto %s", pid64, exc_info=True)
    return None


def listar_overlays_filhos_do_pai(pai_id: str) -> list:
    """Overlays cujo custo_familia aponta para este pai."""
    pai = _pid(pai_id)
    if not pai:
        return []
    from produtos.models import ProdutoGestaoOverlayAgro

    out = []
    try:
        qs = ProdutoGestaoOverlayAgro.objects.filter(
            cadastro_extras__custo_familia__pai_produto_id=pai
        )
        for ov in qs.iterator(chunk_size=100):
            cf = extrair_custo_familia(ov.cadastro_extras)
            if cf and cf.get("pai_produto_id") == pai:
                out.append(ov)
    except Exception:
        # Fallback (SQLite / JSON path antigo): varredura limitada
        logger.info("custo_familia: lookup JSON path falhou; fallback scan", exc_info=True)
        for ov in ProdutoGestaoOverlayAgro.objects.exclude(cadastro_extras={}).iterator(
            chunk_size=200
        ):
            cf = extrair_custo_familia(ov.cadastro_extras)
            if cf and cf.get("pai_produto_id") == pai:
                out.append(ov)
    return out


def resumo_filhos_do_pai(pai_id: str) -> list[dict[str, Any]]:
    rows = []
    for ov in listar_overlays_filhos_do_pai(pai_id):
        cf = extrair_custo_familia(ov.cadastro_extras) or {}
        rows.append(
            {
                "produto_id": ov.produto_externo_id,
                "nome": (ov.nome or "")[:200],
                "kg_filho": cf.get("kg_filho"),
                "kg_pai": cf.get("kg_pai"),
                "auto_sync": bool(cf.get("auto_sync", True)),
                "ultimo_custo_calculado": cf.get("ultimo_custo_calculado"),
            }
        )
    return rows


def _gravar_custo_filho(
    ov,
    custo: Decimal,
    *,
    cf: dict,
    origem: str = "custo_familia",
    usuario=None,
) -> bool:
    """Grava custo no filho sem repropagar (evita loop)."""
    from produtos.agro_fonte_config import agro_catalogo_usa_postgres
    from produtos.cadastro_alteracao_historico_util import (
        registrar_diffs_cadastro,
        snapshot_overlay,
    )
    from produtos.models import ProdutoCadastroAlteracaoAgro

    pid = _pid(ov.produto_externo_id)
    if not pid:
        return False
    custo_q = custo.quantize(_Q2, rounding=ROUND_HALF_UP)
    antes = snapshot_overlay(ov)
    ex = dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
    ex["preco_custo_overlay"] = float(custo_q)
    cf2 = dict(cf)
    cf2["ultimo_custo_calculado"] = float(custo_q)
    cf2["atualizado_em"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ex["custo_familia"] = cf2
    ov.cadastro_extras = ex
    depois = snapshot_overlay(ov)
    try:
        origem_map = {
            "entrada_nf": ProdutoCadastroAlteracaoAgro.Origem.NF,
            "planilha": ProdutoCadastroAlteracaoAgro.Origem.PLANILHA,
            "overlay": ProdutoCadastroAlteracaoAgro.Origem.MODAL,
            "gestao": ProdutoCadastroAlteracaoAgro.Origem.GESTAO,
        }
        orig = origem_map.get(origem, ProdutoCadastroAlteracaoAgro.Origem.OUTRO)
        registrar_diffs_cadastro(
            produto_id=pid,
            antes=antes,
            depois=depois,
            usuario=usuario,
            origem=orig,
        )
    except Exception:
        logger.warning("custo_familia: histórico filho %s", pid, exc_info=True)
    ov.save(update_fields=["cadastro_extras", "atualizado_em"])

    if agro_catalogo_usa_postgres():
        try:
            from produtos import catalogo_agro

            catalogo_agro.sincronizar_modelo_produto_de_overlay(
                pid, ov, custo_payload=custo_q
            )
        except Exception:
            logger.warning("custo_familia: sync PG filho %s", pid, exc_info=True)

    try:
        from produtos.agro_mongo_guard import agro_mongo_escrita_bloqueada

        if not agro_mongo_escrita_bloqueada():
            from produtos.views import _mongo_filtro_id_produto_externo, obter_conexao_mongo

            client, db = obter_conexao_mongo()
            if db is not None and client is not None:
                vf = float(custo_q)
                db[client.col_p].update_one(
                    _mongo_filtro_id_produto_externo(pid),
                    {"$set": {"PrecoCusto": vf, "ValorCusto": vf}},
                )
    except Exception:
        logger.warning("custo_familia: mongo filho %s", pid, exc_info=True)
    return True


def propagar_custo_familia_de_pai(
    pai_id: str,
    custo_pai: Decimal | float | str | None = None,
    *,
    origem: str = "custo_familia",
    usuario=None,
    so_auto_sync: bool = True,
) -> dict[str, Any]:
    """
    Recalcula custo de todos os filhos ligados a ``pai_id``.
    Retorna ``{ok, atualizados, erros, detalhes}``.
    """
    pai = _pid(pai_id)
    out: dict[str, Any] = {"ok": True, "atualizados": 0, "erros": [], "detalhes": []}
    if not pai:
        out["ok"] = False
        out["erros"].append("pai_id vazio")
        return out
    cp = _dec(custo_pai) if custo_pai is not None else ler_custo_produto(pai)
    if cp is None:
        out["ok"] = False
        out["erros"].append("custo do saco (pai) indisponível")
        return out
    for ov in listar_overlays_filhos_do_pai(pai):
        cf = extrair_custo_familia(ov.cadastro_extras)
        if not cf:
            continue
        if so_auto_sync and not cf.get("auto_sync", True):
            out["detalhes"].append(
                {"produto_id": ov.produto_externo_id, "pulado": "auto_sync off"}
            )
            continue
        novo = calcular_custo_filho(cp, cf.get("kg_pai"), cf.get("kg_filho"))
        if novo is None:
            out["erros"].append(
                {"produto_id": ov.produto_externo_id, "erro": "kg inválido"}
            )
            continue
        try:
            if _gravar_custo_filho(
                ov, novo, cf=cf, origem=origem, usuario=usuario
            ):
                out["atualizados"] += 1
                out["detalhes"].append(
                    {
                        "produto_id": ov.produto_externo_id,
                        "custo": float(novo),
                    }
                )
        except Exception as exc:
            logger.warning(
                "custo_familia: falha filho %s", ov.produto_externo_id, exc_info=True
            )
            out["erros"].append(
                {"produto_id": ov.produto_externo_id, "erro": str(exc)[:200]}
            )
    if out["atualizados"]:
        try:
            from django.core.cache import cache

            cache.delete("pdv_catalogo_produtos_por_dia_v2")
            cache.delete("pdv_catalogo_produtos_prev_v2")
        except Exception:
            pass
    return out


def custo_filho_desde_familia(extras: Any, *, custo_pai: Decimal | None = None) -> Decimal | None:
    """Se o produto tem família ativa + auto_sync, devolve custo calculado."""
    cf = extrair_custo_familia(extras)
    if not cf or not cf.get("auto_sync", True):
        return None
    cp = custo_pai if custo_pai is not None else ler_custo_produto(cf["pai_produto_id"])
    if cp is None:
        return None
    return calcular_custo_filho(cp, cf.get("kg_pai"), cf.get("kg_filho"))
