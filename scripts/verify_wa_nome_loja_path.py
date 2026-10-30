#!/usr/bin/env python3
"""Prova: pushName da loja não vira o nome do cliente.

Sqlite temporário, só as tabelas do Zap. O migrate completo deste repo
quebra no sqlite (migration 0039 / NfceNumeracaoAgro) — por isso não usa
manage.py test.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ["DATABASE_URL"] = "sqlite:///" + str(Path(tempfile.gettempdir()) / "wa_nome_loja_prova.sqlite3")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.db import connection

from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT
from produtos.atendimento_whatsapp_util import (
    corrigir_nomes_que_sao_da_loja,
    processar_entrada,
)
from produtos.models import (
    ClienteAgro,
    WhatsAppAgendaContatoAgro,
    WhatsAppBotConfigAgro,
    WhatsAppConversaAgro,
    WhatsAppMensagemAgro,
)

OK = 0
FAIL = 0
LOJA = "Agropecuária Agro Mais"


def check(label: str, cond: bool) -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {label}")
    else:
        FAIL += 1
        print(f" FAIL {label}")


def cfg_quieto(**extra):
    cfg = dict(BOT_DEFAULT)
    cfg["horario_ativo"] = False
    cfg["separar_lojas"] = False
    cfg["enviar_boas_vindas"] = False
    cfg["aviso_fora_ligado"] = False
    cfg.update(extra)
    return cfg


def limpar() -> None:
    WhatsAppMensagemAgro.objects.all().delete()
    WhatsAppConversaAgro.objects.all().delete()
    WhatsAppBotConfigAgro.objects.all().delete()
    WhatsAppAgendaContatoAgro.objects.all().delete()
    ClienteAgro.objects.all().delete()
    corrigir_nomes_que_sao_da_loja._mono = 0  # type: ignore[attr-defined]


def criar_tabelas() -> None:
    modelos = [
        ClienteAgro,
        WhatsAppBotConfigAgro,
        WhatsAppConversaAgro,
        WhatsAppMensagemAgro,
        WhatsAppAgendaContatoAgro,
    ]
    existing = set(connection.introspection.table_names())
    with connection.schema_editor() as editor:
        for model in modelos:
            if model._meta.db_table not in existing:
                editor.create_model(model)


def main() -> int:
    print("=== WA-NOME-LOJA ===\n")
    criar_tabelas()
    limpar()

    with patch("produtos.atendimento_whatsapp_bot_config.carregar_bot", side_effect=lambda *a, **k: cfg_quieto()):
        processar_entrada(
            jid="5513988002201@s.whatsapp.net",
            texto="Oi",
            nome="João da Silva",
            wa_id="cli-1",
        )
        processar_entrada(
            jid="5513988002201@s.whatsapp.net",
            texto="Pode retirar",
            nome=LOJA,
            nome_loja=LOJA,
            de_mim=True,
            wa_id="eco-1",
        )
    conv = WhatsAppConversaAgro.objects.get()
    check("eco celular mantem Joao", conv.nome == "João da Silva")
    dados = WhatsAppBotConfigAgro.objects.get().dados
    check("guardou nome_zap_loja", dados.get("nome_zap_loja") == LOJA)

    limpar()
    for i in range(4):
        WhatsAppConversaAgro.objects.create(
            jid=f"55139880030{i:02d}@s.whatsapp.net",
            telefone=f"55139880030{i:02d}",
            nome="Cliente Repetido Silva",
        )
    n4 = corrigir_nomes_que_sao_da_loja(cfg=cfg_quieto())
    check("4 iguais nao viram loja", n4 == 0)
    check(
        "4 nomes intactos",
        WhatsAppConversaAgro.objects.filter(nome="Cliente Repetido Silva").count() == 4,
    )
    check("4 nao gravou nome_zap", not WhatsAppBotConfigAgro.objects.exists())

    limpar()
    for i in range(8):
        WhatsAppConversaAgro.objects.create(
            jid=f"5513988004{i:03d}@s.whatsapp.net",
            telefone=f"5513988004{i:03d}",
            nome=LOJA,
        )
    n8 = corrigir_nomes_que_sao_da_loja(cfg=cfg_quieto())
    check("8 chats corrigidos", n8 == 8)
    check("nenhum ficou com nome da loja", not WhatsAppConversaAgro.objects.filter(nome=LOJA).exists())
    amostra = WhatsAppConversaAgro.objects.order_by("id").first()
    check("voltou o telefone", amostra is not None and amostra.telefone in (amostra.nome or ""))
    dados8 = WhatsAppBotConfigAgro.objects.get().dados
    check("8 gravou nome_zap_loja", dados8.get("nome_zap_loja") == LOJA)

    limpar()
    with patch(
        "produtos.atendimento_whatsapp_bot_config.carregar_bot",
        return_value=cfg_quieto(nome_zap_loja=LOJA),
    ):
        processar_entrada(
            jid="5513988001122@s.whatsapp.net",
            texto="oi",
            nome=LOJA,
            wa_id="loja-push",
        )
    conv2 = WhatsAppConversaAgro.objects.get()
    check("push da loja nao entra como cliente", conv2.nome != LOJA and "88001122" in (conv2.nome or ""))

    print(f"\nVERIFY_OK {OK}/{OK + FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
