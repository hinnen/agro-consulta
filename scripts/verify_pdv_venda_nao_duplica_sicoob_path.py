# -*- coding: utf-8 -*-
"""Prova: Pix Sicoob / Confirmar repetido não cria 4 vendas (client_request_id)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ.setdefault("AGRO_PIN_TESTE", os.environ.get("AGRO_PIN_TESTE") or "9973")

import django

django.setup()

fails = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global fails
    if cond:
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails += 1
        print(f"  FAIL  {name}" + (f" — {detail}" if detail else ""))


def main() -> int:
    print("== PDV-VENDA-NAO-DUPLICA-SICOOB ==")
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    models = (ROOT / "produtos/models.py").read_text(encoding="utf-8")

    check("JS: reusa clientRequestId", "ensureSaleClientRequestId" in wiz and "if (cur) return" in wiz)
    check("JS: payload manda client_request_id", "payload.client_request_id = idem" in wiz)
    check("API: não descarta client_request_id", "data.pop(\"client_request_id\"" not in views)
    check("API: busca venda existente", "client_request_id=client_req" in views or "filter(client_request_id=client_req)" in views)
    check("Model: campo client_request_id", "client_request_id = models.CharField" in models)
    check(
        "Migrate 0132",
        (ROOT / "produtos/migrations/0132_vendaagro_client_request_id.py").is_file(),
    )

    from produtos.models import VendaAgro

    check("ORM: atr client_request_id", hasattr(VendaAgro, "client_request_id"))

    from django.contrib.auth import get_user_model
    from django.test import RequestFactory
    from produtos.views import _persistir_venda_agro

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    from produtos.models import SessaoCaixa

    sess = SessaoCaixa.objects.filter(fechado_em__isnull=True).order_by("-id").first()
    if not user or not sess:
        print("  SKIP unit (sem user/caixa aberto local)")
    else:
        from django.contrib.sessions.backends.db import SessionStore

        rf = RequestFactory()
        req = rf.post("/api/pdv/enviar-pedido-erp/")
        req.user = user
        req.session = SessionStore()
        req.session.create()
        key = f"verify-sicoob-dup-{os.getpid()}"
        data = {
            "cliente": "VERIFY SICOOB DUP",
            "forma_pagamento": "PIX Sicoob — Chave Pix (WhatsApp)",
            "client_request_id": key,
            "operador_pdv": "Verify",
            "operador": "Verify",
            "pin_operador": os.environ.get("AGRO_PIN_TESTE") or "9973",
            "sessao_caixa_id": sess.pk,
            "pagamentos": [
                {
                    "formaPagamento": "PIX Sicoob — Chave Pix (WhatsApp)",
                    "valorPagamento": 1.23,
                    "maquinaId": "pix_sicoob_chave",
                }
            ],
            "itens": [
                {
                    "id": "verify-sicoob-1",
                    "nome": "VERIFY ITEM",
                    "qtd": 1,
                    "preco": 1.23,
                    "unidade": "UN",
                }
            ],
        }
        try:
            v1 = _persistir_venda_agro(
                req, data, data["itens"], None, None, False, erp_sync_status="aceito"
            )
            v2 = _persistir_venda_agro(
                req, data, data["itens"], None, None, False, erp_sync_status="aceito"
            )
            check("unit: 2x mesma chave = 1 pk", v1.pk == v2.pk, f"pk={v1.pk}")
            n = VendaAgro.objects.filter(client_request_id=key).count()
            check("unit: 1 linha no banco", n == 1, f"n={n}")
            VendaAgro.objects.filter(client_request_id=key).delete()
        except Exception as e:
            check("unit: persistir idempotente", False, str(e)[:200])

    print()
    if fails:
        print(f"VERIFY_FAIL {fails}")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
