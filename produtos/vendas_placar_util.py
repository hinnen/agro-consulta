"""Placar de vendas Centro × Vila — período (dia / semana / mês / ano)."""
from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta

PERIODOS = ("dia", "semana", "mes", "ano")
_MESES = (
    "",
    "janeiro",
    "fevereiro",
    "março",
    "abril",
    "maio",
    "junho",
    "julho",
    "agosto",
    "setembro",
    "outubro",
    "novembro",
    "dezembro",
)
_ALIAS = {
    "dia": "dia",
    "hoje": "dia",
    "day": "dia",
    "d": "dia",
    "semana": "semana",
    "semana_atual": "semana",
    "week": "semana",
    "s": "semana",
    "mes": "mes",
    "mês": "mes",
    "mes_atual": "mes",
    "month": "mes",
    "m": "mes",
    "ano": "ano",
    "ano_atual": "ano",
    "year": "ano",
    "a": "ano",
    "y": "ano",
}


def normalizar_periodo(raw) -> str:
    key = str(raw or "").strip().lower()
    return _ALIAS.get(key, "dia")


def _add_months(d: date, n: int) -> date:
    m0 = d.month - 1 + n
    y = d.year + m0 // 12
    m = m0 % 12 + 1
    last = monthrange(y, m)[1]
    return date(y, m, min(d.day, last))


def _segunda(d: date) -> date:
    return d - timedelta(days=d.weekday())


def ancora_periodo(periodo: str, ref: date) -> date:
    """Data âncora estável para avançar/voltar o recorte."""
    p = normalizar_periodo(periodo)
    if p == "semana":
        return _segunda(ref)
    if p == "mes":
        return date(ref.year, ref.month, 1)
    if p == "ano":
        return date(ref.year, 1, 1)
    return ref


def deslocar_ancora(periodo: str, ref: date, passo: int) -> date:
    p = normalizar_periodo(periodo)
    a = ancora_periodo(p, ref)
    if p == "dia":
        return a + timedelta(days=passo)
    if p == "semana":
        return a + timedelta(days=7 * passo)
    if p == "mes":
        return _add_months(a, passo)
    return date(a.year + passo, 1, 1)


def bounds_periodo(periodo: str, ref: date) -> tuple[date, date]:
    p = normalizar_periodo(periodo)
    a = ancora_periodo(p, ref)
    if p == "dia":
        return a, a
    if p == "semana":
        return a, a + timedelta(days=6)
    if p == "mes":
        return a, date(a.year, a.month, monthrange(a.year, a.month)[1])
    return a, date(a.year, 12, 31)


def rotulo_periodo(periodo: str, data_ini: date, data_fim: date, hoje: date) -> str:
    p = normalizar_periodo(periodo)
    if p == "dia":
        if data_ini == hoje:
            return f"Hoje · {data_ini.strftime('%d/%m/%Y')}"
        if data_ini == hoje - timedelta(days=1):
            return f"Ontem · {data_ini.strftime('%d/%m/%Y')}"
        return data_ini.strftime("%d/%m/%Y")
    if p == "semana":
        return f"{data_ini.strftime('%d/%m')} — {data_fim.strftime('%d/%m/%Y')}"
    if p == "mes":
        nome = _MESES[data_ini.month]
        return f"{nome.capitalize()} {data_ini.year}"
    return str(data_ini.year)


def resolver_periodo(periodo_raw, ref: date | None, hoje: date) -> dict:
    periodo = normalizar_periodo(periodo_raw)
    if ref is None:
        ref = hoje
    ancora = ancora_periodo(periodo, ref)
    data_ini, data_fim = bounds_periodo(periodo, ancora)
    prev = deslocar_ancora(periodo, ancora, -1)
    nxt = deslocar_ancora(periodo, ancora, 1)
    nxt_ini, _nxt_fim = bounds_periodo(periodo, nxt)
    pode_avancar = nxt_ini <= hoje
    return {
        "periodo": periodo,
        "ancora": ancora,
        "data_ini": data_ini,
        "data_fim": data_fim,
        "label": rotulo_periodo(periodo, data_ini, data_fim, hoje),
        "prev": prev,
        "next": nxt,
        "pode_avancar": pode_avancar,
        "e_hoje": data_ini <= hoje <= data_fim,
    }
