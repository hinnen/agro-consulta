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

    qs = Empresa.objects.filter(pk=empresa_id).only("nome_fantasia")
    e = None
    first = getattr(qs, "first", None)
    if callable(first):
        try:
            e = first()
        except Exception:
            e = None
    if e is None:
        try:
            want = int(empresa_id)
            for item in qs:
                if int(getattr(item, "pk", 0) or 0) == want:
                    e = item
                    break
        except Exception:
            e = None
    return deposito_pdv_por_empresa_nome(e.nome_fantasia if e else None)


def normalizar_loja_filtro(raw: str | None) -> str:
    """todas | centro | vila. Padrão = as duas lojas."""
    n = (raw or "").strip().lower()
    if n in ("centro", "1", "central"):
        return "centro"
    if n in ("vila", "2", "vila elias", "vila_elias", "vilaelias"):
        return "vila"
    return "todas"


def deposito_de_loja(loja: str | None) -> str | None:
    """centro/vila ou None (Centro + Vila)."""
    n = normalizar_loja_filtro(loja)
    if n in ("centro", "vila"):
        return n
    return None


def label_loja_filtro(loja: str | None) -> str:
    n = normalizar_loja_filtro(loja)
    if n == "centro":
        return "Centro"
    if n == "vila":
        return "Vila Elias"
    return "Centro + Vila"


def empresas_ids_para_deposito(deposito: str | None) -> list[int]:
    """IDs de empresa do DRE. Vila sem cadastro próprio cai na empresa da loja (Centro)."""
    from base.models import Empresa

    dep = (deposito or "").strip().lower() or None
    if dep not in ("centro", "vila"):
        dep = None
    casadas: list[int] = []
    lojas: list[int] = []
    qualquer: list[int] = []
    for e in Empresa.objects.filter(ativo=True).only("id", "nome_fantasia"):
        pk = int(e.pk)
        qualquer.append(pk)
        mapped = deposito_pdv_por_empresa_nome(e.nome_fantasia)
        if mapped in ("centro", "vila"):
            lojas.append(pk)
            if dep is None or mapped == dep:
                casadas.append(pk)
    if dep is None:
        return lojas or qualquer
    if casadas:
        return casadas
    return lojas or qualquer


def resolver_deposito_pdv(
    deposito: str | None = None,
    empresa_nome: str | None = None,
) -> str | None:
    """centro/vila, None = as duas lojas. ``todas`` força as duas mesmo com nome Centro."""
    if deposito == "todas":
        return None
    if deposito in ("centro", "vila"):
        return deposito
    return deposito_pdv_por_empresa_nome(empresa_nome)


def deposito_pdv_efetivo(
    *,
    empresa_id: int | None = None,
    deposito_filtro: str | None = None,
    n_empresas: int = 1,
) -> str:
    """Valor p/ ``aplicar_receita_pdv``: centro | vila | todas."""
    if deposito_filtro in ("centro", "vila"):
        return deposito_filtro
    if n_empresas > 1 and empresa_id:
        mapped = deposito_pdv_por_empresa_id(empresa_id)
        if mapped in ("centro", "vila"):
            return mapped
    return "todas"


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

    dep = resolver_deposito_pdv(deposito, empresa_nome)
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


_RESUMO_CMV_JS_FIELDS = (
    "cmv",
    "lucro_bruto",
    "resultado_operacional",
    "resultado_liquido_gerencial",
    "markup_pct",
    "margem_bruta_pct",
    "margem_contribuicao_pct",
    "faturamento_equilibrio",
    "faturamento_diario_equilibrio",
)


def recalc_resumo_cmv(core: dict[str, Any], cmv_novo, *, dias_periodo: int = 30) -> dict[str, Any]:
    """Recalcula lucro/líquido/PE do resumo com outro CMV. Caixa não muda."""
    rec = _dec(core.get("receita_operacional"))
    df = _dec(core.get("despesas_fixas"))
    dv = _dec(core.get("despesas_variaveis"))
    dfin = _dec(core.get("despesas_financeiras"))
    cmv = _dec(cmv_novo)
    out = dict(core)
    out["cmv"] = cmv
    out["lucro_bruto"] = rec - cmv
    out["resultado_operacional"] = rec - cmv - df - dv
    out["resultado_liquido_gerencial"] = out["resultado_operacional"] - dfin
    out["margem_bruta_pct"] = (out["lucro_bruto"] / rec * Decimal("100")) if rec > 0 else Decimal("0")
    out["markup_pct"] = ((rec / cmv) - Decimal("1")) * Decimal("100") if cmv > 0 else Decimal("0")
    from financeiro.services.equilibrio import EquilibrioFinanceiroService

    dias_u = max(int(dias_periodo or 1), 1)
    eq = EquilibrioFinanceiroService().calcular(rec, cmv, df, dv, dias_periodo=dias_u)
    out["margem_contribuicao_pct"] = eq["margem_contribuicao_pct"]
    out["faturamento_equilibrio"] = eq["faturamento_equilibrio"]
    out["faturamento_diario_equilibrio"] = eq["faturamento_diario_equilibrio"]
    return out


def pack_resumo_cmv_js(core: dict[str, Any]) -> dict[str, float]:
    return {k: float(_dec(core.get(k))) for k in _RESUMO_CMV_JS_FIELDS}


def aplicar_cmv_modos_no_resumo(
    core: dict[str, Any],
    data_inicio,
    data_fim,
    *,
    empresa_nome: str | None = None,
    deposito: str | None = None,
    dias_periodo: int | None = None,
) -> dict[str, Any]:
    """Anexa CMV vendida × paga no resumo. Não troca o CMV raiz (paga) — o JS escolhe o modo."""
    if not isinstance(core, dict) or core.get("erro"):
        return core

    try:
        dias = int(dias_periodo) if dias_periodo else (data_fim - data_inicio).days + 1
    except Exception:
        dias = 30
    dias = max(dias, 1)

    paga_cmv = _dec(core.get("cmv"))
    snap_paga = recalc_resumo_cmv(core, paga_cmv, dias_periodo=dias)

    dep = resolver_deposito_pdv(deposito, empresa_nome)
    cmv_v = {"ok": False, "total": Decimal("0"), "skus_com_custo": 0, "skus_sem_custo": 0}
    try:
        from produtos.relatorios_vendas_util import custo_mercadoria_vendida

        cmv_v = custo_mercadoria_vendida(data_inicio, data_fim, deposito=dep)
    except Exception:
        cmv_v = {"ok": False, "total": Decimal("0"), "skus_com_custo": 0, "skus_sem_custo": 0}

    ok = bool(cmv_v.get("ok"))
    snap_vend = (
        recalc_resumo_cmv(core, _dec(cmv_v.get("total")), dias_periodo=dias) if ok else dict(snap_paga)
    )
    out = dict(core)
    out["cmv_paga"] = paga_cmv
    out["cmv_vendida"] = _dec(cmv_v.get("total")) if ok else paga_cmv
    out["cmv_modo"] = "vendida" if ok else "paga"
    out["cmv_skus_sem_custo"] = int(cmv_v.get("skus_sem_custo") or 0) if ok else 0
    out["cmv_skus_com_custo"] = int(cmv_v.get("skus_com_custo") or 0) if ok else 0
    out["cmv_modos"] = {
        "vendida": pack_resumo_cmv_js(snap_vend),
        "paga": pack_resumo_cmv_js(snap_paga),
        "skus_sem_custo": out["cmv_skus_sem_custo"],
        "skus_com_custo": out["cmv_skus_com_custo"],
        "ok_vendida": ok,
    }
    return out


def fundir_cmv_modos_grupo(
    consolidado: dict[str, Any],
    subs: list[dict[str, Any]],
    *,
    dias_periodo: int = 30,
) -> dict[str, Any]:
    """Soma CMV vendida/paga das lojas no consolidado do grupo."""
    if not isinstance(consolidado, dict) or consolidado.get("erro"):
        return consolidado
    paga = Decimal("0")
    vendida = Decimal("0")
    ok_any = False
    sem = 0
    com = 0
    for sub in subs:
        if not isinstance(sub, dict) or sub.get("erro"):
            continue
        paga += _dec(sub.get("cmv_paga", sub.get("cmv")))
        if (sub.get("cmv_modos") or {}).get("ok_vendida"):
            ok_any = True
            vendida += _dec(sub.get("cmv_vendida"))
        sem += int(sub.get("cmv_skus_sem_custo") or 0)
        com += int(sub.get("cmv_skus_com_custo") or 0)
    dias = max(int(dias_periodo or 1), 1)
    snap_paga = recalc_resumo_cmv(consolidado, paga, dias_periodo=dias)
    snap_vend = recalc_resumo_cmv(consolidado, vendida, dias_periodo=dias) if ok_any else dict(snap_paga)
    out = dict(consolidado)
    out["cmv_paga"] = paga
    out["cmv_vendida"] = vendida if ok_any else paga
    out["cmv_modo"] = "vendida" if ok_any else "paga"
    out["cmv_skus_sem_custo"] = sem if ok_any else 0
    out["cmv_skus_com_custo"] = com if ok_any else 0
    out["cmv_modos"] = {
        "vendida": pack_resumo_cmv_js(snap_vend),
        "paga": pack_resumo_cmv_js(snap_paga),
        "skus_sem_custo": out["cmv_skus_sem_custo"],
        "skus_com_custo": out["cmv_skus_com_custo"],
        "ok_vendida": ok_any,
    }
    return out


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
