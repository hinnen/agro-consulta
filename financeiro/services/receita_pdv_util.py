"""Receita operacional do DRE = faturamento do PDV (mesmo número do BI)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any


def _dec(x) -> Decimal:
    try:
        return Decimal(str(x or 0))
    except Exception:
        return Decimal("0")


def deposito_pdv_por_empresa_nome(nome: str | None) -> str | None:
    n = (nome or "").casefold()
    if "vila" in n:
        return "vila"
    if "centro" in n:
        return "centro"
    return None


def deposito_pdv_por_empresa_id(empresa_id: int | None) -> str | None:
    if not empresa_id:
        return None
    from base.models import Empresa

    e = Empresa.objects.filter(pk=empresa_id).only("nome_fantasia").first()
    return deposito_pdv_por_empresa_nome(e.nome_fantasia if e else None)


def faturamento_pdv_periodo(data_ini, data_fim, deposito: str | None = None) -> dict[str, Any]:
    try:
        from produtos.views import _dashboard_mongo_vendas_serie

        s = _dashboard_mongo_vendas_serie(data_ini, data_fim, deposito=deposito)
    except Exception:
        return {"ok": False, "total": Decimal("0"), "por_dia": {}, "fonte": "pdv"}
    if not s.get("ok"):
        return {
            "ok": False,
            "total": Decimal("0"),
            "por_dia": {},
            "fonte": s.get("fonte") or "pdv",
        }
    return {
        "ok": True,
        "total": _dec(s.get("total")),
        "por_dia": s.get("por_dia") or {},
        "fonte": s.get("fonte") or "pdv",
        "filtro_loja": s.get("filtro_loja") or deposito or "todas",
    }


def aplicar_receita_pdv_no_resumo(
    core: dict[str, Any],
    data_inicio,
    data_fim,
    *,
    empresa_nome: str | None = None,
    deposito: str | None = None,
) -> dict[str, Any]:
    """Troca receita operacional pelos totais do PDV e recalcula lucro/líquido."""
    if not isinstance(core, dict) or core.get("erro"):
        return core

    dep = deposito if deposito is not None else deposito_pdv_por_empresa_nome(empresa_nome)
    fat = faturamento_pdv_periodo(data_inicio, data_fim, deposito=dep)
    core["faturamento_pdv"] = fat
    core["receita_lancamentos"] = _dec(core.get("receita_operacional"))
    if not fat.get("ok"):
        core["receita_fonte"] = "lancamentos"
        return core

    rec = _dec(fat.get("total"))
    cmv = _dec(core.get("cmv"))
    df = _dec(core.get("despesas_fixas"))
    dv = _dec(core.get("despesas_variaveis"))
    dfin = _dec(core.get("despesas_financeiras"))
    core["receita_operacional"] = rec
    core["lucro_bruto"] = rec - cmv
    core["resultado_operacional"] = rec - cmv - df - dv
    core["resultado_liquido_gerencial"] = core["resultado_operacional"] - dfin
    core["receita_fonte"] = "pdv"
    return core


_KEYS_SOMA_DRE = (
    "receita_operacional",
    "receita_nao_operacional",
    "cmv",
    "despesas_fixas",
    "despesas_variaveis",
    "despesas_financeiras",
    "emprestimos_entrada",
    "amortizacao_emprestimos",
    "aportes_socios",
    "retiradas_socios",
    "geracao_caixa",
    "receita_lancamentos",
)


def somar_resumos_dre_empresas(subs: list[dict[str, Any]]) -> dict[str, Any]:
    """Consolida grupo somando cada empresa (já com PDV da própria loja)."""
    out = {k: Decimal("0") for k in _KEYS_SOMA_DRE}
    fontes: list[str] = []
    ok = 0
    for sub in subs:
        if not isinstance(sub, dict) or sub.get("erro"):
            continue
        ok += 1
        for k in _KEYS_SOMA_DRE:
            out[k] = out[k] + _dec(sub.get(k))
        fontes.append(str(sub.get("receita_fonte") or "lancamentos"))
    rec = out["receita_operacional"]
    cmv = out["cmv"]
    df = out["despesas_fixas"]
    dv = out["despesas_variaveis"]
    dfin = out["despesas_financeiras"]
    out["lucro_bruto"] = rec - cmv
    out["resultado_operacional"] = rec - cmv - df - dv
    out["resultado_liquido_gerencial"] = out["resultado_operacional"] - dfin
    out["receita_fonte"] = "pdv" if any(f == "pdv" for f in fontes) else "lancamentos"
    out["empresas_ok"] = ok
    return out
