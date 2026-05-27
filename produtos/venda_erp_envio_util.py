"""Envio manual de venda fiado ao ERP: validação, histórico, logs e reversão local."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from django.utils import timezone

from produtos.models import VendaAgro


def _usuario_label(request) -> str:
    u = getattr(request, "user", None)
    if u is not None and getattr(u, "is_authenticated", False):
        return (
            (u.get_full_name() or "").strip()
            or (u.get_username() if hasattr(u, "get_username") else str(u.pk))
        )[:150]
    return ""


def _resumo_erp(resposta_raw) -> str:
    if resposta_raw is None:
        return ""
    try:
        if isinstance(resposta_raw, (dict, list)):
            txt = json.dumps(resposta_raw, ensure_ascii=False)
        else:
            txt = str(resposta_raw)
    except Exception:
        txt = str(resposta_raw)
    return txt[:4000]


def append_erp_envio_log(
    venda: VendaAgro,
    *,
    acao: str,
    ok: bool,
    status: str = "",
    http_status: int | None = None,
    mensagem: str = "",
    usuario: str = "",
    detalhe: str = "",
    erp_resposta=None,
) -> dict:
    logs = venda.erp_envio_log_json if isinstance(venda.erp_envio_log_json, list) else []
    entry = {
        "ts": timezone.now().isoformat(),
        "acao": str(acao or "")[:40],
        "ok": bool(ok),
        "status": str(status or "")[:32],
        "http_status": int(http_status) if http_status is not None else None,
        "mensagem": str(mensagem or "")[:500],
        "usuario": str(usuario or "")[:150],
        "detalhe": str(detalhe or "")[:1000],
        "erp_resposta_resumo": _resumo_erp(erp_resposta),
    }
    logs.append(entry)
    venda.erp_envio_log_json = logs[-80:]
    return entry


def erp_envio_logs_ordenados(venda: VendaAgro) -> list[dict]:
    logs = venda.erp_envio_log_json if isinstance(venda.erp_envio_log_json, list) else []
    return list(reversed(logs))


def status_erp_ui(venda: VendaAgro) -> dict[str, str]:
    if venda.devolvida_em:
        return {"codigo": "devolvida", "label": "Devolvida", "cor": "rose"}
    if venda.fiado_aguarda_envio_erp:
        return {"codigo": "aguarda", "label": "Aguardando envio ERP", "cor": "orange"}
    st = venda.erp_sync_efetivo
    if st == VendaAgro.ErpSyncStatus.ACEITO or venda.enviado_erp:
        return {"codigo": "enviado", "label": "Enviado ao ERP", "cor": "emerald"}
    if st == VendaAgro.ErpSyncStatus.RECUSADO_ERP:
        return {"codigo": "recusado", "label": "ERP recusou", "cor": "rose"}
    if st == VendaAgro.ErpSyncStatus.FALHA_COMUNICACAO:
        return {"codigo": "falha", "label": "Falha no envio", "cor": "amber"}
    if st == VendaAgro.ErpSyncStatus.PENDENTE:
        return {"codigo": "pendente", "label": "Pendente ERP", "cor": "slate"}
    return {"codigo": "outro", "label": st or "—", "cor": "slate"}


def pode_enviar_venda_fiado_erp(venda: VendaAgro) -> tuple[bool, str]:
    if venda.devolvida_em:
        return False, "Venda devolvida — não pode enviar ao ERP."
    if not venda.tem_fiado():
        return False, "Só vendas com pagamento Fiado usam envio manual ao ERP."
    if venda.fiado_aguarda_envio_erp:
        return True, ""
    st = venda.erp_sync_efetivo
    if st in (VendaAgro.ErpSyncStatus.RECUSADO_ERP, VendaAgro.ErpSyncStatus.FALHA_COMUNICACAO):
        return True, ""
    if venda.enviado_erp and st == VendaAgro.ErpSyncStatus.ACEITO:
        return False, "Já consta como enviada ao ERP. Reverta o envio local antes de tentar de novo."
    return False, "Esta venda não está elegível para envio manual ao ERP."


def pode_reverter_envio_erp(venda: VendaAgro) -> tuple[bool, str]:
    if venda.devolvida_em:
        return False, "Venda devolvida."
    if not venda.tem_fiado():
        return False, "Somente vendas fiado."
    if venda.fiado_aguarda_envio_erp:
        return False, "Ainda não foi enviada ao ERP."
    if venda.enviado_erp or venda.erp_sync_efetivo == VendaAgro.ErpSyncStatus.ACEITO:
        return True, ""
    if venda.erp_sync_efetivo in (
        VendaAgro.ErpSyncStatus.RECUSADO_ERP,
        VendaAgro.ErpSyncStatus.FALHA_COMUNICACAO,
    ):
        return True, ""
    logs = venda.erp_envio_log_json if isinstance(venda.erp_envio_log_json, list) else []
    if any(
        isinstance(x, dict) and x.get("acao") in ("envio_sucesso", "envio_recusado", "envio_falha")
        for x in logs
    ):
        return True, ""
    return False, "Não há envio ERP para reverter."


def reverter_envio_erp_local(venda: VendaAgro, *, request, motivo: str = "") -> VendaAgro:
    ok_rev, msg = pode_reverter_envio_erp(venda)
    if not ok_rev:
        raise ValueError(msg)
    usuario = _usuario_label(request)
    append_erp_envio_log(
        venda,
        acao="revertido",
        ok=True,
        status="pendente",
        mensagem=motivo or "Envio ERP revertido no Agro (o ERP não é alterado automaticamente).",
        usuario=usuario,
        detalhe="Status local voltou para aguardando envio manual.",
    )
    venda.erp_sync_status = VendaAgro.ErpSyncStatus.PENDENTE
    venda.enviado_erp = False
    venda.erp_http_status = None
    venda.save(
        update_fields=[
            "erp_sync_status",
            "enviado_erp",
            "erp_http_status",
            "erp_envio_log_json",
        ]
    )
    return venda


def venda_payload_de_venda_agro(venda: VendaAgro) -> dict:
    from produtos.fiado_credito_util import cliente_agro_pk_de_ref

    pagamentos_erp = []
    pj = venda.pagamentos_json if isinstance(venda.pagamentos_json, list) else []
    for row in pj:
        if not isinstance(row, dict):
            continue
        fn = str(row.get("forma") or "")
        vp = float(row.get("valor") or 0)
        item = {"formaPagamento": fn, "valorPagamento": vp, "quitar": fn != "Fiado"}
        if fn == "Fiado":
            if row.get("fiado_parcelas"):
                item["fiadoParcelas"] = row.get("fiado_parcelas")
            if row.get("fiado_dias_primeiro"):
                item["fiadoDiasVencimento"] = row.get("fiado_dias_primeiro")
            cron = row.get("fiado_cronograma") or venda.fiado_cronograma_json
            if isinstance(cron, list) and cron:
                item["fiadoCronograma"] = cron
        pagamentos_erp.append(item)
    cid = (venda.cliente_id_erp or "").strip()
    agro_pk = cliente_agro_pk_de_ref(cid)
    data = {
        "cliente": venda.cliente_nome,
        "cliente_id": cid,
        "cliente_documento": venda.cliente_documento,
        "forma_pagamento": venda.forma_pagamento,
        "pagamentos": pagamentos_erp or None,
        "itens": [
            {
                "id": it.produto_id_externo,
                "nome": it.descricao,
                "qtd": float(it.quantidade),
                "preco": float(it.valor_unitario),
                "codigo": it.codigo,
            }
            for it in venda.itens.all()
        ],
    }
    if agro_pk:
        data["cliente_agro_pk"] = agro_pk
    return data


def serializar_venda_erp_painel(venda: VendaAgro) -> dict[str, Any]:
    st = status_erp_ui(venda)
    pode_env, msg_env = pode_enviar_venda_fiado_erp(venda)
    pode_rev, msg_rev = pode_reverter_envio_erp(venda)
    return {
        "venda_id": venda.pk,
        "cliente_nome": venda.cliente_nome,
        "total": float(venda.total),
        "forma_pagamento": venda.forma_pagamento,
        "tem_fiado": venda.tem_fiado(),
        "fiado_aguarda_envio_erp": venda.fiado_aguarda_envio_erp,
        "status_erp": st,
        "pode_enviar_erp": pode_env,
        "msg_enviar_erp": msg_env,
        "pode_reverter_erp": pode_rev,
        "msg_reverter_erp": msg_rev,
        "logs": erp_envio_logs_ordenados(venda),
    }
