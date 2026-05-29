"""Cashback na venda: percentual por produto e movimento no saldo do cliente."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from django.conf import settings
from django.db.models import F

from produtos.caixa_util import normalizar_forma_pagamento_caixa
from produtos.fiado_credito_util import cliente_agro_pk_de_ref, resolver_cliente_fiado
from produtos.models import ClienteAgro, ProdutoGestaoOverlayAgro


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or "0").replace(",", ".").strip())
    except Exception:
        return Decimal("0")


def cashback_percentual_padrao() -> Decimal:
    raw = getattr(settings, "AGRO_CASHBACK_PERCENTUAL_PADRAO", "1")
    try:
        return max(Decimal("0"), min(Decimal("100"), _dec(raw)))
    except Exception:
        return Decimal("1")


def cashback_percentual_de_overlay(ov: ProdutoGestaoOverlayAgro | None) -> Decimal:
    if ov is not None and ov.cashback_percentual is not None:
        return max(Decimal("0"), min(Decimal("100"), _dec(ov.cashback_percentual)))
    return cashback_percentual_padrao()


def mapa_cashback_percentual_por_produto(ids: list[str]) -> dict[str, Decimal]:
    ids_u = [str(x).strip()[:64] for x in ids if str(x or "").strip()]
    if not ids_u:
        return {}
    padrao = cashback_percentual_padrao()
    out = {pid: padrao for pid in ids_u}
    for ov in ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__in=ids_u).only(
        "produto_externo_id", "cashback_percentual"
    ):
        out[str(ov.produto_externo_id)] = cashback_percentual_de_overlay(ov)
    return out


def valor_cashback_usado_no_payload(data: dict | None) -> Decimal:
    if not data or not isinstance(data, dict):
        return Decimal("0")
    total = Decimal("0")
    pag = data.get("pagamentos")
    if isinstance(pag, list):
        for row in pag:
            if not isinstance(row, dict):
                continue
            fn = normalizar_forma_pagamento_caixa(
                str(
                    row.get("formaPagamento")
                    or row.get("forma_pagamento")
                    or row.get("forma")
                    or ""
                )
            )
            if fn != "Cashback":
                continue
            total += _dec(row.get("valorPagamento", row.get("valor_pagamento", row.get("valor"))))
    if total > 0:
        return total.quantize(Decimal("0.01"))
    forma = str(data.get("forma_pagamento") or data.get("formaPagamento") or "")
    if "cashback" in forma.lower():
        return _dec(data.get("total") or data.get("valor_total")).quantize(Decimal("0.01"))
    return Decimal("0")


def calcular_cashback_gerado_itens(
    raw_itens: list,
    *,
    desconto_geral: Decimal | None = None,
) -> Decimal:
    if not raw_itens:
        return Decimal("0")
    linhas: list[tuple[str, Decimal]] = []
    subtotal = Decimal("0")
    for i in raw_itens:
        if not isinstance(i, dict):
            continue
        pid = str(i.get("id") or "").strip()[:64]
        if not pid:
            continue
        qtd = _dec(i.get("qtd"))
        vu = _dec(i.get("preco"))
        if qtd <= 0 or vu < 0:
            continue
        vt = (qtd * vu).quantize(Decimal("0.01"))
        subtotal += vt
        linhas.append((pid, vt))
    if not linhas:
        return Decimal("0")
    desc = max(Decimal("0"), _dec(desconto_geral))
    if desc > subtotal:
        desc = subtotal
    pct_map = mapa_cashback_percentual_por_produto([p for p, _ in linhas])
    gerado = Decimal("0")
    for pid, vt in linhas:
        pct = pct_map.get(pid, cashback_percentual_padrao())
        if pct <= 0:
            continue
        base = vt
        if subtotal > 0 and desc > 0:
            base = (vt - (vt / subtotal * desc)).quantize(Decimal("0.01"))
        if base <= 0:
            continue
        gerado += (base * pct / Decimal("100")).quantize(Decimal("0.01"))
    return gerado.quantize(Decimal("0.01"))


def validar_cashback_payload(
    data: dict,
    raw_itens: list,
    *,
    cliente_agro: ClienteAgro | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    usado = valor_cashback_usado_no_payload(data)
    desconto = _dec(data.get("desconto_geral") or data.get("descontoGeral"))
    gerado = calcular_cashback_gerado_itens(raw_itens, desconto_geral=desconto)
    info = {
        "cashback_usado": float(usado),
        "cashback_gerado": float(gerado),
    }
    if usado <= 0 and gerado <= 0:
        return True, "", info
    if cliente_agro is None:
        cid = str(data.get("cliente_id") or data.get("ClienteID") or "").strip()
        pk = cliente_agro_pk_de_ref(cid, data.get("cliente_agro_pk"))
        if pk:
            cliente_agro = ClienteAgro.objects.filter(pk=pk, ativo=True).first()
    if cliente_agro is None:
        nome = str(data.get("cliente") or "").strip().upper()
        if "CONSUMIDOR" in nome and "IDENTIFICADO" in nome:
            if usado > 0:
                return False, "Cashback exige cliente cadastrado (não use consumidor final).", info
            return True, "", info
        if usado > 0 or gerado > 0:
            return False, "Cashback exige cliente cadastrado.", info
        return True, "", info
    saldo = _dec(cliente_agro.saldo_cashback)
    if usado > saldo + Decimal("0.009"):
        return (
            False,
            f"Cashback acima do saldo. Disponível R$ {saldo:.2f}".replace(".", ","),
            info,
        )
    return True, "", info


def aplicar_movimento_cashback_venda(
    data: dict,
    raw_itens: list,
    *,
    cliente_agro: ClienteAgro | None = None,
) -> dict[str, Any]:
    usado = valor_cashback_usado_no_payload(data)
    desconto = _dec(data.get("desconto_geral") or data.get("descontoGeral"))
    gerado = calcular_cashback_gerado_itens(raw_itens, desconto_geral=desconto)
    out = {
        "aplicado": False,
        "cashback_usado": float(usado),
        "cashback_gerado": float(gerado),
        "saldo_apos": None,
    }
    if usado <= 0 and gerado <= 0:
        return out
    if cliente_agro is None:
        _erp_id, _pk, cliente_agro = resolver_cliente_fiado(
            str(data.get("cliente_id") or ""),
            cliente_agro_pk=data.get("cliente_agro_pk"),
        )
    if cliente_agro is None:
        return out
    ok, msg, _ = validar_cashback_payload(data, raw_itens, cliente_agro=cliente_agro)
    if not ok:
        raise ValueError(msg)
    delta = (gerado - usado).quantize(Decimal("0.01"))
    if delta == 0:
        out["aplicado"] = True
        out["saldo_apos"] = float(_dec(cliente_agro.saldo_cashback))
        return out
    ClienteAgro.objects.filter(pk=cliente_agro.pk).update(
        saldo_cashback=F("saldo_cashback") + delta,
        editado_local=True,
    )
    cliente_agro.refresh_from_db(fields=["saldo_cashback"])
    out["aplicado"] = True
    out["saldo_apos"] = float(_dec(cliente_agro.saldo_cashback))
    return out
