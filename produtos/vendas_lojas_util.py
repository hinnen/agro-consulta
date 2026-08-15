"""Totais de vendas PDV das duas lojas (Centro + Vila) para o painel simples."""
from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from django.db.models import Q, Sum

from produtos.models import VendaAgro

MODOS = ("dia", "semana", "mes", "ano")
_MESES = (
    "",
    "Janeiro",
    "Fevereiro",
    "Março",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro",
)


def _dec(val) -> Decimal:
    try:
        return Decimal(str(val or 0)).quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0.00")


def format_moeda_br(val) -> str:
    q = _dec(val)
    neg = q < 0
    q = abs(q)
    inteiro, _, frac = f"{q:.2f}".partition(".")
    partes: list[str] = []
    while inteiro:
        partes.append(inteiro[-3:])
        inteiro = inteiro[:-3]
    corpo = ".".join(reversed(partes)) if partes else "0"
    s = f"{corpo},{frac}"
    return f"-{s}" if neg else s


def parse_data_iso(raw: str | None) -> date | None:
    if not raw:
        return None
    s = str(raw).strip()
    if len(s) < 8:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def resolver_periodo_vendas_lojas(
    *, modo: str | None, ref: date | None, hoje: date
) -> dict:
    """
    ``modo``: dia | semana | mes | ano. Padrão: dia (hoje).
    ``ref`` ancora o período (um dia qualquer). Sem ref = hoje.
    """
    m = (modo or "dia").strip().lower()
    if m not in MODOS:
        m = "dia"
    ancora = ref or hoje
    if ancora > hoje:
        ancora = hoje

    if m == "dia":
        ini = fim = ancora
        if ancora == hoje:
            rotulo = f"Hoje · {ancora.strftime('%d/%m/%Y')}"
        else:
            rotulo = ancora.strftime("%d/%m/%Y")
        prev_ref = ancora - timedelta(days=1)
        next_ref = ancora + timedelta(days=1)
    elif m == "semana":
        ini = ancora - timedelta(days=ancora.weekday())
        fim = ini + timedelta(days=6)
        rotulo = f"Semana {ini.strftime('%d/%m')} — {fim.strftime('%d/%m/%Y')}"
        prev_ref = ini - timedelta(days=7)
        next_ref = ini + timedelta(days=7)
    elif m == "mes":
        ini = ancora.replace(day=1)
        ultimo = monthrange(ini.year, ini.month)[1]
        fim = date(ini.year, ini.month, ultimo)
        rotulo = f"{_MESES[ini.month]} {ini.year}"
        prev_ref = (ini - timedelta(days=1)).replace(day=1)
        if ini.month == 12:
            next_ref = date(ini.year + 1, 1, 1)
        else:
            next_ref = date(ini.year, ini.month + 1, 1)
    else:
        ini = date(ancora.year, 1, 1)
        fim = date(ancora.year, 12, 31)
        rotulo = str(ini.year)
        prev_ref = date(ini.year - 1, 1, 1)
        next_ref = date(ini.year + 1, 1, 1)

    return {
        "modo": m,
        "ref": ancora,
        "data_ini": ini,
        "data_fim": fim,
        "rotulo": rotulo,
        "prev_ref": prev_ref,
        "next_ref": next_ref,
        "pode_avancar": next_ref <= hoje,
    }


def totais_vendas_lojas(data_ini: date, data_fim: date) -> dict:
    """Soma PDV (VendaAgro), sem devolvidas. Centro = não-Vila (legado sem depósito)."""
    qs = VendaAgro.objects.filter(
        criado_em__date__gte=data_ini,
        criado_em__date__lte=data_fim,
        devolvida_em__isnull=True,
    )
    centro = _dec(
        qs.filter(Q(deposito__iexact="centro") | Q(deposito="") | Q(deposito__isnull=True))
        .aggregate(s=Sum("total"))
        .get("s")
    )
    vila = _dec(qs.filter(deposito__iexact="vila").aggregate(s=Sum("total")).get("s"))
    soma = (centro + vila).quantize(Decimal("0.01"))
    return {
        "centro": centro,
        "vila": vila,
        "soma": soma,
        "centro_fmt": format_moeda_br(centro),
        "vila_fmt": format_moeda_br(vila),
        "soma_fmt": format_moeda_br(soma),
    }


def payload_vendas_lojas(*, modo: str | None, ref: date | None, hoje: date) -> dict:
    periodo = resolver_periodo_vendas_lojas(modo=modo, ref=ref, hoje=hoje)
    totais = totais_vendas_lojas(periodo["data_ini"], periodo["data_fim"])
    return {
        "ok": True,
        "periodo": periodo["modo"],
        "ref": periodo["ref"].isoformat(),
        "data_ini": periodo["data_ini"].isoformat(),
        "data_fim": periodo["data_fim"].isoformat(),
        "rotulo": periodo["rotulo"],
        "prev_ref": periodo["prev_ref"].isoformat(),
        "next_ref": periodo["next_ref"].isoformat(),
        "pode_avancar": periodo["pode_avancar"],
        "hoje": hoje.isoformat(),
        "centro": float(totais["centro"]),
        "vila": float(totais["vila"]),
        "soma": float(totais["soma"]),
        "centro_fmt": totais["centro_fmt"],
        "vila_fmt": totais["vila_fmt"],
        "soma_fmt": totais["soma_fmt"],
        "fonte": "pdv",
    }
