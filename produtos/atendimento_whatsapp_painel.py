"""Painel CRM dos atendimentos WhatsApp (visão visual, só leitura operacional)."""
from __future__ import annotations

import os
from datetime import timedelta

from django.db.models import Count, Q
from django.utils import timezone

from produtos.atendimento_whatsapp_util import serializar_conversa, serializar_ponte
from produtos.models import WhatsAppConversaAgro, WhatsAppMensagemAgro


def _eh_teste() -> bool:
    return os.environ.get("AGRO_WA_TESTE") == "1"


def garantir_demo_painel() -> bool:
    """No Zap teste, se ainda não há conversa, cria exemplos só para o painel."""
    if not _eh_teste():
        return False
    if WhatsAppConversaAgro.objects.exists():
        return False
    agora = timezone.now()
    exemplos = [
        {
            "jid": "5513997001001@s.whatsapp.net",
            "telefone": "5513997001001",
            "nome": "Marina Souza",
            "loja": WhatsAppConversaAgro.LOJA_PENDENTE,
            "nao_lidas": 2,
            "aguardando_loja": True,
            "ultima_preview": "Bom dia! Vocês têm ração 15 kg?",
            "ultima_em": agora - timedelta(minutes=4),
            "extras": {"demo": True, "ultima_direcao": "in", "tags": ["novo"], "nota": "Primeira mensagem"},
            "msgs": [
                ("in", "Bom dia! Vocês têm ração 15 kg?", 8),
                ("bot", "Olá! Em qual loja prefere ser atendida?", 7),
            ],
        },
        {
            "jid": "5513997001002@s.whatsapp.net",
            "telefone": "5513997001002",
            "nome": "Carlos Mendes",
            "loja": WhatsAppConversaAgro.LOJA_CENTRO,
            "nao_lidas": 0,
            "aguardando_loja": False,
            "ultima_preview": "Pode separar 2 sacos, por favor",
            "ultima_em": agora - timedelta(minutes=18),
            "extras": {"demo": True, "ultima_direcao": "out", "ultima_ack": 2, "tags": ["retirada"], "nota": "Cliente conhecido"},
            "msgs": [
                ("in", "Tem premiatta adulto?", 40),
                ("out", "Temos sim. Quer retirar no Centro?", 30),
                ("in", "Pode separar 2 sacos, por favor", 18),
                ("out", "Separado. Te aviso no balcão.", 12),
            ],
        },
        {
            "jid": "5513997001003@s.whatsapp.net",
            "telefone": "5513997001003",
            "nome": "Ana Paula",
            "loja": WhatsAppConversaAgro.LOJA_VILA,
            "nao_lidas": 3,
            "aguardando_loja": True,
            "ultima_preview": "O frete chega ainda hoje?",
            "ultima_em": agora - timedelta(minutes=9),
            "extras": {"demo": True, "ultima_direcao": "in", "vip": True, "tags": ["entrega"], "nota": "VIP · entrega Vila"},
            "msgs": [
                ("in", "Quero entregar na Rua das Flores", 25),
                ("out", "Combinado. Qual horário?", 20),
                ("in", "O frete chega ainda hoje?", 9),
            ],
        },
        {
            "jid": "5513997001004@s.whatsapp.net",
            "telefone": "5513997001004",
            "nome": "José Ribeiro",
            "loja": WhatsAppConversaAgro.LOJA_CENTRO,
            "nao_lidas": 1,
            "aguardando_loja": True,
            "ultima_preview": "Quanto ficou o fiado?",
            "ultima_em": agora - timedelta(minutes=2),
            "extras": {"demo": True, "ultima_direcao": "in", "tags": ["fiado"]},
            "msgs": [
                ("in", "Quanto ficou o fiado?", 2),
            ],
        },
        {
            "jid": "5513997001005@s.whatsapp.net",
            "telefone": "5513997001005",
            "nome": "Luciana Alves",
            "loja": WhatsAppConversaAgro.LOJA_VILA,
            "nao_lidas": 0,
            "aguardando_loja": False,
            "ultima_preview": "Obrigada!",
            "ultima_em": agora - timedelta(hours=2),
            "arquivada": True,
            "arquivada_em": agora - timedelta(hours=1, minutes=50),
            "arquivada_por": "Renan",
            "extras": {"demo": True, "ultima_direcao": "in", "tags": ["resolvida"]},
            "msgs": [
                ("in", "Chegou a encomenda?", 130),
                ("out", "Sim, já está na Vila.", 120),
                ("in", "Obrigada!", 115),
            ],
        },
        {
            "jid": "5513997001006@s.whatsapp.net",
            "telefone": "5513997001006",
            "nome": "Pedro Lima",
            "loja": WhatsAppConversaAgro.LOJA_PENDENTE,
            "nao_lidas": 1,
            "aguardando_loja": True,
            "ultima_preview": "Vocês abrem domingo?",
            "ultima_em": agora - timedelta(minutes=35),
            "extras": {"demo": True, "ultima_direcao": "in", "tags": ["horário"]},
            "msgs": [
                ("in", "Vocês abrem domingo?", 35),
            ],
        },
        {
            "jid": "5513997001007@s.whatsapp.net",
            "telefone": "5513997001007",
            "nome": "Fernanda Costa",
            "loja": WhatsAppConversaAgro.LOJA_CENTRO,
            "nao_lidas": 0,
            "aguardando_loja": False,
            "ultima_preview": "Pode deixar no balcão",
            "ultima_em": agora - timedelta(minutes=50),
            "extras": {"demo": True, "ultima_direcao": "out", "ultima_ack": 3, "tags": ["balcão"]},
            "msgs": [
                ("in", "Vou buscar o pedido", 70),
                ("out", "Pode deixar no balcão", 50),
            ],
        },
        {
            "jid": "5513997001008@s.whatsapp.net",
            "telefone": "5513997001008",
            "nome": "Roberto Dias",
            "loja": WhatsAppConversaAgro.LOJA_VILA,
            "nao_lidas": 0,
            "aguardando_loja": False,
            "ultima_preview": "Combinado, até amanhã",
            "ultima_em": agora - timedelta(hours=5),
            "arquivada": True,
            "arquivada_em": agora - timedelta(hours=4, minutes=40),
            "arquivada_por": "Agromais",
            "extras": {"demo": True, "ultima_direcao": "out", "ultima_ack": 2, "tags": ["agendado"]},
            "msgs": [
                ("in", "Posso buscar amanhã?", 320),
                ("out", "Combinado, até amanhã", 300),
            ],
        },
    ]
    for ex in exemplos:
        msgs = ex.pop("msgs", [])
        c = WhatsAppConversaAgro.objects.create(**ex)
        for direcao, texto, mins in msgs:
            WhatsAppMensagemAgro.objects.create(
                conversa=c,
                direcao=direcao,
                texto=texto,
                criado_em=agora - timedelta(minutes=mins),
                enviado_em=agora - timedelta(minutes=mins) if direcao != "in" else None,
                ack=2 if direcao == "out" else 0,
            )
    return True


def _coluna_de(c: WhatsAppConversaAgro) -> str:
    if c.arquivada:
        return "resolvidas"
    if c.loja == WhatsAppConversaAgro.LOJA_PENDENTE:
        return "fila"
    if c.loja == WhatsAppConversaAgro.LOJA_CENTRO:
        return "centro"
    if c.loja == WhatsAppConversaAgro.LOJA_VILA:
        return "vila"
    return "fila"


def _enriquecer(c: WhatsAppConversaAgro) -> dict:
    row = serializar_conversa(c)
    row["coluna"] = _coluna_de(c)
    row["demo"] = bool((getattr(c, "extras", None) or {}).get("demo"))
    row["msgs_total"] = int(getattr(c, "_msgs_total", 0) or 0)
    row["espera_min"] = 0
    if c.ultima_em and (c.aguardando_loja or int(c.nao_lidas or 0) > 0):
        delta = timezone.now() - c.ultima_em
        row["espera_min"] = max(0, int(delta.total_seconds() // 60))
    return row


def montar_painel() -> dict:
    garantir_demo_painel()
    qs = WhatsAppConversaAgro.objects.all().annotate(
        _msgs_total=Count("mensagens", filter=~Q(mensagens__tipo_midia="reaction"))
    )
    rows = list(qs.order_by("-ultima_em", "-id")[:200])
    itens = [_enriquecer(c) for c in rows]
    colunas = {
        "fila": [],
        "centro": [],
        "vila": [],
        "resolvidas": [],
    }
    for item in itens:
        colunas.setdefault(item["coluna"], []).append(item)

    ativas = [c for c in rows if not c.arquivada]
    aguardando = [c for c in ativas if c.aguardando_loja or int(c.nao_lidas or 0) > 0]
    nao_lidas = sum(int(c.nao_lidas or 0) for c in ativas)
    resolvidas_hoje = WhatsAppConversaAgro.objects.filter(
        arquivada=True,
        arquivada_em__gte=timezone.localtime().replace(hour=0, minute=0, second=0, microsecond=0),
    ).count()

    por_loja = {
        WhatsAppConversaAgro.LOJA_PENDENTE: 0,
        WhatsAppConversaAgro.LOJA_CENTRO: 0,
        WhatsAppConversaAgro.LOJA_VILA: 0,
    }
    for c in ativas:
        if c.loja in por_loja:
            por_loja[c.loja] += 1

    return {
        "ok": True,
        "teste": _eh_teste(),
        "demo": any(bool((c.extras or {}).get("demo")) for c in rows),
        "ponte": serializar_ponte(),
        "resumo": {
            "ativas": len(ativas),
            "aguardando": len(aguardando),
            "nao_lidas": nao_lidas,
            "resolvidas_hoje": int(resolvidas_hoje),
            "fila": por_loja[WhatsAppConversaAgro.LOJA_PENDENTE],
            "centro": por_loja[WhatsAppConversaAgro.LOJA_CENTRO],
            "vila": por_loja[WhatsAppConversaAgro.LOJA_VILA],
            "total_msgs": int(
                WhatsAppMensagemAgro.objects.exclude(tipo_midia="reaction").aggregate(n=Count("id")).get("n")
                or 0
            ),
        },
        "colunas": [
            {
                "id": "fila",
                "titulo": "Fila",
                "sub": "Ainda sem loja",
                "itens": colunas["fila"],
            },
            {
                "id": "centro",
                "titulo": "Centro",
                "sub": "Em atendimento",
                "itens": colunas["centro"],
            },
            {
                "id": "vila",
                "titulo": "Vila",
                "sub": "Em atendimento",
                "itens": colunas["vila"],
            },
            {
                "id": "resolvidas",
                "titulo": "Resolvidas",
                "sub": "Arquivadas",
                "itens": colunas["resolvidas"],
            },
        ],
        "atualizado_em": timezone.localtime().isoformat(),
    }


def detalhe_painel(conversa_id: int) -> tuple[dict | None, str]:
    try:
        c = WhatsAppConversaAgro.objects.annotate(
            _msgs_total=Count("mensagens", filter=~Q(mensagens__tipo_midia="reaction"))
        ).get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return None, "Atendimento não encontrado."
    item = _enriquecer(c)
    msgs = list(
        WhatsAppMensagemAgro.objects.filter(conversa=c)
        .exclude(tipo_midia="reaction")
        .order_by("-id")[:12]
    )
    msgs.reverse()
    item["timeline"] = [
        {
            "id": int(m.pk),
            "direcao": m.direcao,
            "texto": (m.texto or "")[:240],
            "autor": (m.autor_nome or "")[:80],
            "quando": timezone.localtime(m.criado_em).strftime("%d/%m %H:%M") if m.criado_em else "",
        }
        for m in msgs
    ]
    return item, ""
