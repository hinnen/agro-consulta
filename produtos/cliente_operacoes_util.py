"""Operações de cadastro de cliente: duplicata, exclusão, saldos, vale crédito."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from django.db import transaction
from django.db.models import Q

from produtos.caixa_util import operador_label_de_pin, usuario_django_de_pin
from produtos.cliente_whatsapp_util import extrair_whatsapp_digits
from produtos.models import (
    ClienteAgro,
    ClienteAgroEventoAgro,
    FiadoTituloAgro,
    PedidoEntrega,
)

PID_VALE_CREDITO = "vale-credito"
PID_FIADO_COBRANCA = "fiado-cobranca"


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or "0").replace(",", ".").strip())
    except Exception:
        return Decimal("0")


def _q2(v) -> Decimal:
    return _dec(v).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def item_id_e_servico_pdv(pid: str) -> bool:
    p = str(pid or "").strip().lower()
    return p in {PID_VALE_CREDITO, PID_FIADO_COBRANCA} or p.startswith("vale-credito")


def payload_e_compra_vale_credito(data: dict | None, raw_itens=None) -> bool:
    data = data if isinstance(data, dict) else {}
    if data.get("compra_vale_credito") or (data.get("compraValeCredito") or {}).get("ativo"):
        return True
    itens = raw_itens if isinstance(raw_itens, list) else data.get("itens") or []
    if not isinstance(itens, list):
        return False
    for i in itens:
        if not isinstance(i, dict):
            continue
        pid = str(i.get("id") or i.get("produto_id") or "").strip().lower()
        if pid == PID_VALE_CREDITO or pid.startswith("vale-credito"):
            return True
    return False


def valor_compra_vale_credito(raw_itens) -> Decimal:
    total = Decimal("0")
    if not isinstance(raw_itens, list):
        return total
    for i in raw_itens:
        if not isinstance(i, dict):
            continue
        pid = str(i.get("id") or i.get("produto_id") or "").strip().lower()
        if pid != PID_VALE_CREDITO and not pid.startswith("vale-credito"):
            continue
        qtd = _dec(i.get("qtd") if i.get("qtd") is not None else i.get("quantidade") or 1)
        vu = _dec(i.get("preco") if i.get("preco") is not None else i.get("valor_unitario") or 0)
        total += (qtd * vu)
    return _q2(total)


def validar_pin_operador(pin: str) -> tuple[bool, str, Any, str]:
    ok, label, err = operador_label_de_pin(pin)
    if not ok:
        return False, "", None, err or "PIN inválido."
    return True, label, usuario_django_de_pin(pin), ""


def _gravar_evento(
    *,
    tipo: str,
    cliente: ClienteAgro | None,
    destino: ClienteAgro | None = None,
    payload: dict | None = None,
    usuario: str = "",
    origem_tela: str = "",
) -> ClienteAgroEventoAgro:
    return ClienteAgroEventoAgro.objects.create(
        tipo=tipo,
        cliente_agro=cliente,
        cliente_pk_snap=getattr(cliente, "pk", None),
        cliente_nome_snap=(getattr(cliente, "nome", None) or "")[:200],
        destino_agro=destino,
        destino_pk_snap=getattr(destino, "pk", None),
        destino_nome_snap=(getattr(destino, "nome", None) or "")[:200],
        payload_json=payload or {},
        usuario=(usuario or "")[:150],
        pin_operador=(usuario or "")[:150],
        origem_tela=(origem_tela or "")[:32],
    )


def limpar_whatsapp_duplicado(
    *,
    alvo_pk: int,
    pin: str,
    origem_tela: str = "pdv",
) -> dict:
    ok, label, _user, err = validar_pin_operador(pin)
    if not ok:
        return {"ok": False, "erro": err}
    alvo = ClienteAgro.objects.filter(pk=alvo_pk).first()
    if not alvo:
        return {"ok": False, "erro": "Cadastro do outro cliente não encontrado."}
    antigo = extrair_whatsapp_digits(alvo.whatsapp)
    if not antigo:
        return {"ok": True, "cliente": _linha_min(alvo), "ja_limpo": True}
    alvo.whatsapp = ""
    alvo.editado_local = True
    alvo.save(update_fields=["whatsapp", "editado_local", "atualizado_em"])
    _gravar_evento(
        tipo=ClienteAgroEventoAgro.Tipo.LIMPAR_WHATSAPP,
        cliente=alvo,
        payload={"whatsapp_antes": antigo},
        usuario=label,
        origem_tela=origem_tela,
    )
    return {"ok": True, "cliente": _linha_min(alvo)}


def _linha_min(c: ClienteAgro) -> dict:
    return {
        "pk": c.pk,
        "cliente_agro_pk": c.pk,
        "nome": c.nome,
        "whatsapp": c.whatsapp or "",
        "saldo_cashback": float(_q2(c.saldo_cashback)),
        "saldo_vale_credito": float(_q2(c.saldo_vale_credito)),
        "ativo": bool(c.ativo),
    }


def fiado_em_aberto_cliente(cli: ClienteAgro) -> dict:
    qs = FiadoTituloAgro.objects.filter(
        cliente_agro=cli,
        situacao__in=(FiadoTituloAgro.Situacao.ABERTO, FiadoTituloAgro.Situacao.PARCIAL),
    )
    n = qs.count()
    saldo = Decimal("0")
    for t in qs.only("valor_bruto", "valor_pago"):
        saldo += t.saldo_aberto
    return {"n": n, "saldo": float(_q2(saldo)), "bloqueia": n > 0 and saldo > Decimal("0")}


def preview_exclusao(pk: int) -> dict:
    cli = ClienteAgro.objects.filter(pk=pk).first()
    if not cli:
        return {"ok": False, "erro": "Cliente não encontrado."}
    fiado = fiado_em_aberto_cliente(cli)
    cb = _q2(cli.saldo_cashback)
    vale = _q2(cli.saldo_vale_credito)
    n_entrega = PedidoEntrega.objects.filter(cliente_agro=cli).exclude(
        status__in=(PedidoEntrega.Status.ENTREGUE, PedidoEntrega.Status.CANCELADO)
    ).count()
    n_rh = 0
    try:
        from rh.models import Funcionario

        n_rh = Funcionario.objects.filter(cliente_agro=cli).count()
    except Exception:
        n_rh = 0
    bloqueio = ""
    if fiado.get("bloqueia"):
        saldo_txt = f"{fiado['saldo']:.2f}".replace(".", ",")
        bloqueio = (
            f"Este cliente tem fiado em aberto (R$ {saldo_txt}). "
            "Quite o fiado antes de excluir."
        )
    elif n_rh:
        bloqueio = "Este cadastro está ligado a um funcionário no RH. Não dá para excluir."
    return {
        "ok": True,
        "cliente": _linha_min(cli),
        "fiado_aberto": fiado,
        "saldo_cashback": float(cb),
        "saldo_vale_credito": float(vale),
        "precisa_transferir": (cb + vale) > Decimal("0"),
        "entregas_pendentes": n_entrega,
        "rh_vinculado": n_rh > 0,
        "pode_excluir": not bool(bloqueio),
        "bloqueio": bloqueio,
    }


def transferir_saldos(
    *,
    origem_pk: int,
    destino_pk: int,
    pin: str,
    origem_tela: str = "pdv",
    cashback: bool = True,
    vale: bool = True,
) -> dict:
    ok, label, _user, err = validar_pin_operador(pin)
    if not ok:
        return {"ok": False, "erro": err}
    if int(origem_pk) == int(destino_pk):
        return {"ok": False, "erro": "Escolha outro cadastro para receber os saldos."}
    with transaction.atomic():
        origem = ClienteAgro.objects.select_for_update().filter(pk=origem_pk).first()
        destino = ClienteAgro.objects.select_for_update().filter(pk=destino_pk).first()
        if not origem or not destino:
            return {"ok": False, "erro": "Cadastro origem ou destino não encontrado."}
        if not destino.ativo:
            return {"ok": False, "erro": "O cadastro destino está inativo."}
        mov_cb = _q2(origem.saldo_cashback) if cashback else Decimal("0")
        mov_vale = _q2(origem.saldo_vale_credito) if vale else Decimal("0")
        if mov_cb <= 0 and mov_vale <= 0:
            return {
                "ok": True,
                "origem": _linha_min(origem),
                "destino": _linha_min(destino),
                "movido": {"cashback": 0, "vale": 0},
            }
        if mov_cb > 0:
            destino.saldo_cashback = _q2(destino.saldo_cashback) + mov_cb
            origem.saldo_cashback = Decimal("0")
        if mov_vale > 0:
            destino.saldo_vale_credito = _q2(destino.saldo_vale_credito) + mov_vale
            origem.saldo_vale_credito = Decimal("0")
        origem.editado_local = True
        destino.editado_local = True
        origem.save()
        destino.save()
        _gravar_evento(
            tipo=ClienteAgroEventoAgro.Tipo.TRANSFERIR_SALDOS,
            cliente=origem,
            destino=destino,
            payload={
                "cashback": float(mov_cb),
                "vale_credito": float(mov_vale),
            },
            usuario=label,
            origem_tela=origem_tela,
        )
    return {
        "ok": True,
        "origem": _linha_min(origem),
        "destino": _linha_min(destino),
        "movido": {"cashback": float(mov_cb), "vale": float(mov_vale)},
    }


def _soltar_fks_antes_excluir(cli: ClienteAgro) -> dict:
    n_fiado = FiadoTituloAgro.objects.filter(cliente_agro=cli).update(cliente_agro=None)
    n_ent = PedidoEntrega.objects.filter(cliente_agro=cli).update(cliente_agro=None)
    from produtos.models import OrcamentoPdvAgro

    n_orc = OrcamentoPdvAgro.objects.filter(cliente_agro=cli).update(cliente_agro=None)
    return {"fiado_titulos": n_fiado, "entregas": n_ent, "orcamentos": n_orc}


def excluir_cliente(
    *,
    pk: int,
    pin: str,
    destino_pk: int | None = None,
    origem_tela: str = "pdv",
) -> dict:
    prev = preview_exclusao(pk)
    if not prev.get("ok"):
        return prev
    if not prev.get("pode_excluir"):
        return {"ok": False, "erro": prev.get("bloqueio") or "Não é possível excluir."}
    ok, label, _user, err = validar_pin_operador(pin)
    if not ok:
        return {"ok": False, "erro": err}
    precisa = prev.get("precisa_transferir")
    if precisa and not destino_pk:
        return {
            "ok": False,
            "erro": "Este cadastro tem cashback ou vale crédito. Transfira para o cadastro certo antes de excluir.",
            "precisa_transferir": True,
            "preview": prev,
        }
    with transaction.atomic():
        cli = ClienteAgro.objects.select_for_update().filter(pk=pk).first()
        if not cli:
            return {"ok": False, "erro": "Cliente não encontrado."}
        fiado = fiado_em_aberto_cliente(cli)
        if fiado.get("bloqueia"):
            return {"ok": False, "erro": prev.get("bloqueio")}
        destino = None
        mov = {"cashback": 0.0, "vale": 0.0}
        if precisa:
            if int(destino_pk) == int(pk):
                return {"ok": False, "erro": "Escolha outro cadastro para receber os saldos."}
            tr = transferir_saldos(
                origem_pk=pk,
                destino_pk=int(destino_pk),
                pin=pin,
                origem_tela=origem_tela,
            )
            if not tr.get("ok"):
                return tr
            mov = tr.get("movido") or mov
            destino = ClienteAgro.objects.filter(pk=destino_pk).first()
            cli.refresh_from_db()
        snap = {
            "pk": cli.pk,
            "nome": cli.nome,
            "whatsapp": cli.whatsapp or "",
            "cpf": cli.cpf or "",
            "endereco": cli.endereco or "",
            "externo_id": cli.externo_id or "",
            "saldo_cashback": float(_q2(cli.saldo_cashback)),
            "saldo_vale_credito": float(_q2(cli.saldo_vale_credito)),
            "transferido": mov,
            "fks": _soltar_fks_antes_excluir(cli),
        }
        _gravar_evento(
            tipo=ClienteAgroEventoAgro.Tipo.EXCLUIR,
            cliente=cli,
            destino=destino,
            payload=snap,
            usuario=label,
            origem_tela=origem_tela,
        )
        nome = cli.nome
        cli.delete()
    return {"ok": True, "excluido_pk": pk, "excluido_nome": nome, "snapshot": snap}


def creditar_vale_manual(
    *,
    pk: int,
    valor,
    motivo: str,
    pin: str,
    origem_tela: str = "pdv",
) -> dict:
    ok, label, _user, err = validar_pin_operador(pin)
    if not ok:
        return {"ok": False, "erro": err}
    v = _q2(valor)
    if v <= 0:
        return {"ok": False, "erro": "Informe um valor maior que zero."}
    mot = (motivo or "").strip()
    if len(mot) < 3:
        return {"ok": False, "erro": "Informe o motivo (mínimo 3 letras)."}
    with transaction.atomic():
        cli = ClienteAgro.objects.select_for_update().filter(pk=pk).first()
        if not cli:
            return {"ok": False, "erro": "Cliente não encontrado."}
        if not cli.ativo:
            return {"ok": False, "erro": "Cadastro inativo."}
        antes = _q2(cli.saldo_vale_credito)
        cli.saldo_vale_credito = antes + v
        cli.editado_local = True
        cli.save(update_fields=["saldo_vale_credito", "editado_local", "atualizado_em"])
        _gravar_evento(
            tipo=ClienteAgroEventoAgro.Tipo.VALE_MANUAL,
            cliente=cli,
            payload={
                "valor": float(v),
                "saldo_antes": float(antes),
                "saldo_depois": float(_q2(cli.saldo_vale_credito)),
                "motivo": mot[:300],
                "caixa": False,
            },
            usuario=label,
            origem_tela=origem_tela,
        )
    return {"ok": True, "cliente": _linha_min(cli), "valor": float(v)}


def aplicar_vale_pago_apos_venda(
    *,
    cliente: ClienteAgro,
    valor: Decimal,
    venda_pk: int | None,
    usuario: str = "",
    origem_tela: str = "pdv",
) -> dict:
    v = _q2(valor)
    if v <= 0 or cliente is None:
        return {"ok": False, "erro": "Valor ou cliente inválido."}
    with transaction.atomic():
        cli = ClienteAgro.objects.select_for_update().filter(pk=cliente.pk).first()
        if not cli:
            return {"ok": False, "erro": "Cliente não encontrado."}
        antes = _q2(cli.saldo_vale_credito)
        cli.saldo_vale_credito = antes + v
        cli.editado_local = True
        cli.save(update_fields=["saldo_vale_credito", "editado_local", "atualizado_em"])
        _gravar_evento(
            tipo=ClienteAgroEventoAgro.Tipo.VALE_PAGO,
            cliente=cli,
            payload={
                "valor": float(v),
                "saldo_antes": float(antes),
                "saldo_depois": float(_q2(cli.saldo_vale_credito)),
                "venda_pk": venda_pk,
                "caixa": True,
            },
            usuario=usuario,
            origem_tela=origem_tela,
        )
    return {"ok": True, "cliente": _linha_min(cli)}


def listar_eventos_cliente(pk: int, limite: int = 40) -> list[dict]:
    qs = ClienteAgroEventoAgro.objects.filter(
        Q(cliente_agro_id=pk) | Q(cliente_pk_snap=pk) | Q(destino_agro_id=pk) | Q(destino_pk_snap=pk)
    ).order_by("-criado_em")[: max(1, min(int(limite or 40), 80))]
    out = []
    for e in qs:
        out.append(
            {
                "id": e.pk,
                "tipo": e.tipo,
                "tipo_label": e.get_tipo_display(),
                "usuario": e.usuario,
                "criado_em": e.criado_em.isoformat() if e.criado_em else "",
                "payload": e.payload_json or {},
                "cliente_nome": e.cliente_nome_snap,
                "destino_nome": e.destino_nome_snap,
            }
        )
    return out
