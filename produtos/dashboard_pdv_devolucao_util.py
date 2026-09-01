"""Abatimento de devolução no dia do evento (BI / vendas das lojas)."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.db.models import Exists, OuterRef

from produtos.models import DevolucaoVendaAgro, VendaAgro
from produtos.pdv_deposito_util import normalizar_deposito


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


def venda_deposito_bate_filtro(deposito_venda: str | None, filtro: str | None) -> bool:
    """Mesma regra da lista/BI: vila só vila; centro = resto (vazio incluso)."""
    if filtro is None or filtro in ("", "todas", "todos", "all"):
        return True
    dep = normalizar_deposito(deposito_venda or "centro")
    if filtro == "vila":
        return dep == "vila"
    return dep != "vila"


def _iter_abatimentos_com_venda(data_ini: date, data_fim: date):
    """(dia, deposito_norm, valor, venda|None)."""
    qs_ev = DevolucaoVendaAgro.objects.filter(
        criado_em__date__gte=data_ini,
        criado_em__date__lte=data_fim,
    ).select_related("venda")
    for ev in qs_ev:
        venda = getattr(ev, "venda", None)
        dep = normalizar_deposito(getattr(venda, "deposito", None) or "centro")
        d = timezone_local_date(getattr(ev, "criado_em", None))
        if d is None:
            continue
        yield d, dep, _dec(ev.total), venda

    has_ev = Exists(DevolucaoVendaAgro.objects.filter(venda_id=OuterRef("pk")))
    qs_leg = (
        VendaAgro.objects.filter(
            devolvida_em__isnull=False,
            devolvida_em__date__gte=data_ini,
            devolvida_em__date__lte=data_fim,
        )
        .annotate(_tem_ev=has_ev)
        .filter(_tem_ev=False)
    )
    for v in qs_leg:
        d = timezone_local_date(getattr(v, "devolvida_em", None))
        if d is None:
            continue
        dep = normalizar_deposito(getattr(v, "deposito", None) or "centro")
        yield d, dep, _dec(v.total), v


def _iter_abatimentos(data_ini: date, data_fim: date):
    """(dia, deposito_norm, valor) — evento de devolução; legado sem evento."""
    for d, dep, val, _venda in _iter_abatimentos_com_venda(data_ini, data_fim):
        yield d, dep, val


def timezone_local_date(dt) -> date | None:
    if dt is None:
        return None
    try:
        from django.utils import timezone as tz

        if timezone_is_aware(dt):
            return tz.localtime(dt).date()
        return dt.date()
    except Exception:
        try:
            return dt.date()
        except Exception:
            return None


def timezone_is_aware(dt) -> bool:
    try:
        from django.utils import timezone as tz

        return tz.is_aware(dt)
    except Exception:
        return getattr(dt, "tzinfo", None) is not None


def abatimento_devolucoes_por_dia(
    data_ini: date, data_fim: date, deposito: str | None = None
) -> dict[str, Decimal]:
    """Soma a descontar em cada dia civil (chave ISO)."""
    out: dict[str, Decimal] = {}
    for d, dep, val in _iter_abatimentos(data_ini, data_fim):
        if not venda_deposito_bate_filtro(dep, deposito):
            continue
        k = d.isoformat()
        out[k] = out.get(k, Decimal("0.00")) + val
    for k, v in list(out.items()):
        out[k] = v.quantize(Decimal("0.01"))
    return out


def abatimento_devolucoes_totais_loja(
    data_ini: date, data_fim: date
) -> tuple[Decimal, Decimal]:
    """(centro, vila) no intervalo, pelo depósito da venda original."""
    centro = Decimal("0.00")
    vila = Decimal("0.00")
    for _d, dep, val in _iter_abatimentos(data_ini, data_fim):
        if dep == "vila":
            vila += val
        else:
            centro += val
    return centro.quantize(Decimal("0.01")), vila.quantize(Decimal("0.01"))


def abatimento_devolucoes_por_operador(
    data_ini: date, data_fim: date, deposito: str | None = None
) -> dict[str, Decimal]:
    """Desconto no ranking do vendedor: evento no intervalo, PIN da venda original."""
    out: dict[str, Decimal] = {}
    for _d, dep, val, venda in _iter_abatimentos_com_venda(data_ini, data_fim):
        if not venda_deposito_bate_filtro(dep, deposito):
            continue
        op = (getattr(venda, "usuario_registro", None) or "").strip() if venda else ""
        if not op:
            continue
        out[op] = out.get(op, Decimal("0.00")) + val
    return {k: v.quantize(Decimal("0.01")) for k, v in out.items()}


def abatimento_devolucoes_por_cliente(
    data_ini: date, data_fim: date, deposito: str | None = None
) -> dict[str, Decimal]:
    """Desconto no top cliente do BI — mesma chave da lista (nome / ERP / avulso)."""
    out: dict[str, Decimal] = {}
    for _d, dep, val, venda in _iter_abatimentos_com_venda(data_ini, data_fim):
        if not venda_deposito_bate_filtro(dep, deposito):
            continue
        if venda is None:
            continue
        nm = (getattr(venda, "cliente_nome", None) or "").strip()
        if nm:
            chave = nm
        else:
            cid = (getattr(venda, "cliente_id_erp", None) or "").strip()
            chave = f"Cliente ERP {cid}" if cid else "Consumidor não identificado"
        out[chave] = out.get(chave, Decimal("0.00")) + val
    return {k: v.quantize(Decimal("0.01")) for k, v in out.items()}


def soma_devolucoes_periodo(
    data_ini: date, data_fim: date, deposito: str | None = None
) -> tuple[int, Decimal]:
    """Quantidade de eventos (+ legado) e valor — lista de vendas / badge."""
    n = 0
    soma = Decimal("0.00")
    qs_ev = DevolucaoVendaAgro.objects.filter(
        criado_em__date__gte=data_ini,
        criado_em__date__lte=data_fim,
    ).select_related("venda")
    for ev in qs_ev:
        venda = getattr(ev, "venda", None)
        dep = getattr(venda, "deposito", None) if venda is not None else None
        if not venda_deposito_bate_filtro(dep, deposito):
            continue
        n += 1
        soma += _dec(ev.total)
    has_ev = Exists(DevolucaoVendaAgro.objects.filter(venda_id=OuterRef("pk")))
    qs_leg = (
        VendaAgro.objects.filter(
            devolvida_em__isnull=False,
            devolvida_em__date__gte=data_ini,
            devolvida_em__date__lte=data_fim,
        )
        .annotate(_tem_ev=has_ev)
        .filter(_tem_ev=False)
    )
    for v in qs_leg:
        if not venda_deposito_bate_filtro(getattr(v, "deposito", None), deposito):
            continue
        n += 1
        soma += _dec(v.total)
    return n, soma.quantize(Decimal("0.01"))


def aplicar_abatimento_por_dia(
    por_dia: dict[str, float], abat: dict[str, Decimal]
) -> dict[str, float]:
    out = dict(por_dia)
    for k, val in abat.items():
        atual = Decimal(str(out.get(k) or 0))
        out[k] = float((atual - val).quantize(Decimal("0.01")))
    return out
