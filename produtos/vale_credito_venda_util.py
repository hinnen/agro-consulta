"""Vale crédito na venda: baixa o saldo do cliente ao pagar com a forma."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from django.db import transaction

from produtos.caixa_util import normalizar_forma_pagamento_caixa
from produtos.cliente_operacoes_util import _gravar_evento, _q2, payload_e_compra_vale_credito
from produtos.fiado_credito_util import cliente_agro_pk_de_ref, resolver_cliente_fiado
from produtos.models import ClienteAgro, ClienteAgroEventoAgro


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or "0").replace(",", ".").strip())
    except Exception:
        return Decimal("0")


def valor_vale_credito_usado_no_payload(data: dict | None) -> Decimal:
    if not data or not isinstance(data, dict):
        return Decimal("0")
    if payload_e_compra_vale_credito(data, data.get("itens")):
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
            if fn != "Vale crédito":
                continue
            total += _dec(row.get("valorPagamento", row.get("valor_pagamento", row.get("valor"))))
    if total > 0:
        return total.quantize(Decimal("0.01"))
    forma = str(data.get("forma_pagamento") or data.get("formaPagamento") or "")
    if "vale" in forma.lower() and "cr" in forma.lower():
        return _dec(data.get("total") or data.get("valor_total")).quantize(Decimal("0.01"))
    return Decimal("0")


def validar_vale_credito_payload(
    data: dict,
    *,
    cliente_agro: ClienteAgro | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    usado = valor_vale_credito_usado_no_payload(data)
    info = {"vale_credito_usado": float(usado)}
    if usado <= 0:
        return True, "", info
    if cliente_agro is None:
        cid = str(data.get("cliente_id") or data.get("ClienteID") or "").strip()
        pk = cliente_agro_pk_de_ref(cid, data.get("cliente_agro_pk"))
        if pk:
            cliente_agro = ClienteAgro.objects.filter(pk=pk, ativo=True).first()
    if cliente_agro is None:
        return False, "Vale crédito exige cliente cadastrado (não use consumidor final).", info
    saldo = _q2(cliente_agro.saldo_vale_credito)
    if usado > saldo + Decimal("0.009"):
        return (
            False,
            f"Vale crédito acima do saldo. Disponível R$ {saldo:.2f}".replace(".", ","),
            info,
        )
    return True, "", info


def aplicar_movimento_vale_credito_venda(
    data: dict,
    *,
    cliente_agro: ClienteAgro | None = None,
    venda_pk: int | None = None,
    usuario: str = "",
) -> dict[str, Any]:
    usado = valor_vale_credito_usado_no_payload(data)
    out = {
        "aplicado": False,
        "vale_credito_usado": float(usado),
        "saldo_apos": None,
    }
    if usado <= 0:
        return out
    if cliente_agro is None:
        _erp_id, _pk, cliente_agro = resolver_cliente_fiado(
            str(data.get("cliente_id") or ""),
            cliente_agro_pk=data.get("cliente_agro_pk"),
        )
    if cliente_agro is None:
        return out
    ok, msg, _ = validar_vale_credito_payload(data, cliente_agro=cliente_agro)
    if not ok:
        raise ValueError(msg)
    with transaction.atomic():
        cli = ClienteAgro.objects.select_for_update().filter(pk=cliente_agro.pk).first()
        if not cli:
            return out
        antes = _q2(cli.saldo_vale_credito)
        if usado > antes + Decimal("0.009"):
            raise ValueError(
                f"Vale crédito acima do saldo. Disponível R$ {antes:.2f}".replace(".", ",")
            )
        cli.saldo_vale_credito = antes - usado
        cli.editado_local = True
        cli.save(update_fields=["saldo_vale_credito", "editado_local", "atualizado_em"])
        _gravar_evento(
            tipo=ClienteAgroEventoAgro.Tipo.VALE_USADO,
            cliente=cli,
            payload={
                "valor": float(usado),
                "saldo_antes": float(antes),
                "saldo_depois": float(_q2(cli.saldo_vale_credito)),
                "venda_pk": venda_pk,
            },
            usuario=usuario,
            origem_tela="pdv",
        )
        cliente_agro.saldo_vale_credito = cli.saldo_vale_credito
    out["aplicado"] = True
    out["saldo_apos"] = float(_q2(cli.saldo_vale_credito))
    return out


def creditar_vale_devolucao(
    *,
    venda,
    valor,
    usuario: str = "",
) -> dict[str, Any]:
    v = _q2(valor)
    if v <= 0 or venda is None:
        return {"ok": False}
    _erp, _pk, cli = resolver_cliente_fiado(
        str(getattr(venda, "cliente_id_erp", "") or ""),
        cliente_agro_pk=None,
    )
    if cli is None:
        return {"ok": False, "erro": "Cliente não encontrado para devolver vale."}
    with transaction.atomic():
        locked = ClienteAgro.objects.select_for_update().filter(pk=cli.pk).first()
        if not locked:
            return {"ok": False}
        antes = _q2(locked.saldo_vale_credito)
        locked.saldo_vale_credito = antes + v
        locked.editado_local = True
        locked.save(update_fields=["saldo_vale_credito", "editado_local", "atualizado_em"])
        _gravar_evento(
            tipo=ClienteAgroEventoAgro.Tipo.VALE_DEVOLUCAO,
            cliente=locked,
            payload={
                "valor": float(v),
                "saldo_antes": float(antes),
                "saldo_depois": float(_q2(locked.saldo_vale_credito)),
                "venda_pk": getattr(venda, "pk", None),
            },
            usuario=usuario,
            origem_tela="devolucao",
        )
    return {"ok": True, "saldo_apos": float(_q2(locked.saldo_vale_credito))}
