"""Pacote leve de despesas por categoria para a prévia visual do DRE."""
from __future__ import annotations

from datetime import timedelta
from typing import Any


def _grupo_despesa_dre(plano: str) -> str | None:
    from financeiro.models import LancamentoFinanceiro as NF
    from financeiro.services.resumo_operacional_mongo import classificar_despesa_plano

    nat = classificar_despesa_plano(plano or "")
    if nat == NF.NATUREZA_DESPESA_FIXA:
        return "fixa"
    if nat == NF.NATUREZA_DESPESA_VARIAVEL:
        return "variavel"
    if nat == NF.NATUREZA_DESPESA_FINANCEIRA:
        return "financeira"
    return None


def despesas_categorias_dre_pg(
    *,
    empresa_nome: str,
    data_inicio,
    data_fim,
    por: str = "competencia",
    valor: str = "bruto",
) -> dict[str, Any]:
    """Despesas por plano no mesmo recorte do DRE (filtro + empresa + valor)."""
    from django.conf import settings

    from produtos.lancamentos_financeiro_pg_analytics_util import dre_resumo_simples_pg

    extra = getattr(settings, "DRE_RESULTADO_EXCLUIR_REGEX_EXTRA", "") or ""
    raw = dre_resumo_simples_pg(
        data_de=data_inicio,
        data_ate=data_fim,
        por=por or "competencia",
        valor=valor or "bruto",
        filtro_contas="resultado",
        regex_excluir_extra=extra or None,
        empresa=empresa_nome,
    )
    if not raw.get("ok"):
        return {
            "ok": False,
            "erro": raw.get("erro") or "falha",
            "top": [],
            "grupos": [],
            "total": 0.0,
        }
    from financeiro.services.plano_conta_dre_util import nome_oficial_plano

    grupos = {
        "fixa": {"key": "fixa", "label": "Despesas fixas", "total": 0.0},
        "variavel": {"key": "variavel", "label": "Despesas variáveis", "total": 0.0},
        "financeira": {"key": "financeira", "label": "Despesas financeiras", "total": 0.0},
    }
    agg: dict[str, dict[str, Any]] = {}
    for row in raw.get("linhas") or []:
        des = float(row.get("despesa") or 0)
        if des <= 0.005:
            continue
        bruto = str(row.get("plano") or "")
        gkey = _grupo_despesa_dre(bruto)
        if not gkey:
            continue
        nome = nome_oficial_plano(bruto) or bruto
        grupos[gkey]["total"] += des
        item = agg.get(nome)
        if item is None:
            agg[nome] = {
                "plano": nome,
                "valor": des,
                "ultimo": des,
                "grupo": gkey,
            }
        else:
            item["valor"] += des
            item["ultimo"] = item["valor"]
    linhas = [
        {
            "plano": v["plano"],
            "valor": round(float(v["valor"]), 2),
            "ultimo": round(float(v["ultimo"]), 2),
            "grupo": v["grupo"],
        }
        for v in agg.values()
    ]
    linhas.sort(key=lambda x: -float(x["valor"]))
    for g in grupos.values():
        g["total"] = round(float(g["total"]), 2)
        g["ultimo"] = g["total"]
    total = round(sum(g["total"] for g in grupos.values()), 2)
    return {
        "ok": True,
        "top": linhas[:12],
        "grupos": [grupos["fixa"], grupos["variavel"], grupos["financeira"]],
        "total": total,
    }


def janela_mes_passado(data_inicio):
    """Mês calendário anterior ao início do filtro."""
    primeiro = data_inicio.replace(day=1)
    prev_fim = primeiro - timedelta(days=1)
    return prev_fim.replace(day=1), prev_fim


def janela_90d_antes(data_inicio):
    """90 dias corridos terminando no dia anterior ao filtro."""
    fim = data_inicio - timedelta(days=1)
    ini = fim - timedelta(days=89)
    return ini, fim


def _dias_periodo(ini, fim) -> int:
    try:
        return max(int((fim - ini).days) + 1, 1)
    except Exception:
        return 1


def _fnum(x) -> float:
    try:
        return float(x or 0)
    except Exception:
        return 0.0


def _snapshot_kpis_dre(core: dict[str, Any], *, dias_ref: int, dias_atual: int) -> dict[str, Any]:
    df = _fnum(core.get("despesas_fixas"))
    dv = _fnum(core.get("despesas_variaveis"))
    dfin = _fnum(core.get("despesas_financeiras"))
    rec = _fnum(core.get("receita_operacional"))
    cmv = _fnum(core.get("cmv"))
    lucro = rec - cmv
    margem = _fnum(core.get("margem_bruta_pct"))
    if abs(margem) < 1e-9 and rec > 0.005:
        margem = (lucro / rec) * 100.0
    markup = _fnum(core.get("markup_pct"))
    if abs(markup) < 1e-9 and cmv > 0.005:
        markup = ((rec / cmv) - 1.0) * 100.0
    modos = core.get("cmv_modos") if isinstance(core.get("cmv_modos"), dict) else {}
    slim: dict[str, Any] = {}
    for key in ("vendida", "paga"):
        snap = modos.get(key)
        if isinstance(snap, dict):
            slim[key] = {
                "cmv": round(_fnum(snap.get("cmv")), 2),
                "margem_bruta_pct": round(_fnum(snap.get("margem_bruta_pct")), 2),
                "markup_pct": round(_fnum(snap.get("markup_pct")), 2),
            }
    k = float(dias_atual) / float(max(int(dias_ref or 1), 1))
    return {
        "despesas": round(df + dv + dfin, 2),
        "receita": round(rec, 2),
        "cmv": round(cmv, 2),
        "margem_bruta_pct": round(margem, 2),
        "markup_pct": round(markup, 2),
        "cmv_modos": slim,
        "ok_vendida": bool(modos.get("ok_vendida")),
        "dias": int(dias_ref),
        "dias_atual": int(dias_atual),
        "k": round(k, 6),
    }


def comparativo_kpis_dre_pg(
    *,
    empresa_id: int,
    data_inicio,
    data_fim,
    por: str = "competencia",
    valor: str = "bruto",
) -> dict[str, Any]:
    """KPIs do mês passado e dos 90 dias anteriores, para projetar no tamanho do filtro."""
    from financeiro.services.resumo_operacional_pg import consolidar_empresa_pg

    dias_atual = _dias_periodo(data_inicio, data_fim)
    mes_ini, mes_fim = janela_mes_passado(data_inicio)
    d90_ini, d90_fim = janela_90d_antes(data_inicio)

    def _load(ini, fim) -> dict[str, Any] | None:
        try:
            core = consolidar_empresa_pg(
                empresa_id=empresa_id,
                data_inicio=ini,
                data_fim=fim,
                por=por or "competencia",
                valor=valor or "bruto",
                anexar_cmv_modos=True,
            )
        except Exception:
            return None
        if not isinstance(core, dict) or core.get("erro"):
            return None
        dias_ref = _dias_periodo(ini, fim)
        snap = _snapshot_kpis_dre(core, dias_ref=dias_ref, dias_atual=dias_atual)
        snap["de"] = ini.isoformat()
        snap["ate"] = fim.isoformat()
        return snap

    mes = _load(mes_ini, mes_fim)
    d90 = _load(d90_ini, d90_fim)
    if not mes and not d90:
        return {"ok": False}
    return {
        "ok": True,
        "mes": mes,
        "d90": d90,
        "dias_atual": dias_atual,
    }


def montar_dre_visual(
    *,
    empresa_id: int,
    por: str = "competencia",
    data_inicio=None,
    data_fim=None,
    empresa_nome: str | None = None,
    valor: str = "bruto",
) -> dict[str, Any]:
    from financeiro.services.gastos_variacao_pg import gastos_variacao_pg

    var = gastos_variacao_pg(
        empresa_id=empresa_id,
        modo="mes",
        por=por or "competencia",
        top_chart=8,
    )
    if not var.get("ok"):
        return {
            "ok": False,
            "erro": var.get("erro") or "variação indisponível",
            "variacao": {"ok": False},
        }
    top: list[dict[str, Any]] = []
    for row in (var.get("linhas") or [])[:12]:
        vals = row.get("valores") or []
        ultimo = float(vals[-1] if vals else 0)
        top.append(
            {
                "plano": row.get("plano") or row.get("categoria") or "",
                "ultimo": round(ultimo, 2),
                "delta_abs": round(float(row.get("delta_abs") or 0), 2),
                "tendencia": row.get("tendencia") or "flat",
            }
        )
    emprestimos: dict[str, Any] = {"ok": False}
    receita_categorias: dict[str, Any] = {"ok": False}
    despesas_categorias: dict[str, Any] = {"ok": False}
    comparativo: dict[str, Any] = {"ok": False}
    if data_inicio and data_fim:
        from financeiro.services.receita_pdv_util import (
            deposito_pdv_por_empresa_id,
            deposito_pdv_por_empresa_nome,
        )
        from produtos.relatorios_vendas_util import receita_categorias_pdv

        dep = deposito_pdv_por_empresa_nome(empresa_nome) or deposito_pdv_por_empresa_id(
            empresa_id
        )
        try:
            receita_categorias = receita_categorias_pdv(
                data_inicio, data_fim, deposito=dep, top=6
            )
        except Exception:
            receita_categorias = {"ok": False}
        if (empresa_nome or "").strip():
            from financeiro.services.dre_emprestimos_util import resumo_emprestimos_pg

            try:
                emprestimos = resumo_emprestimos_pg(
                    empresa_nome=empresa_nome,
                    data_inicio=data_inicio,
                    data_fim=data_fim,
                    por=por or "competencia",
                    valor=valor or "bruto",
                )
            except Exception:
                emprestimos = {"ok": False}
            try:
                despesas_categorias = despesas_categorias_dre_pg(
                    empresa_nome=empresa_nome,
                    data_inicio=data_inicio,
                    data_fim=data_fim,
                    por=por or "competencia",
                    valor=valor or "bruto",
                )
            except Exception:
                despesas_categorias = {"ok": False}
        try:
            comparativo = comparativo_kpis_dre_pg(
                empresa_id=empresa_id,
                data_inicio=data_inicio,
                data_fim=data_fim,
                por=por or "competencia",
                valor=valor or "bruto",
            )
        except Exception:
            comparativo = {"ok": False}
    return {
        "ok": True,
        "variacao": {
            "ok": True,
            "buckets": var.get("buckets") or [],
            "resumo_grupos": var.get("resumo_grupos") or [],
            "total_ultimo_periodo": var.get("total_ultimo_periodo") or 0,
            "top": top,
        },
        "emprestimos": emprestimos,
        "receita_categorias": receita_categorias,
        "despesas_categorias": despesas_categorias,
        "comparativo": comparativo,
    }
