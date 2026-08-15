"""
Pedido de transferência PDV (Centro ↔ Vila).
Roda: python scripts/verify_pdv_pedido_transferencia_path.py
"""
from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ["DATABASE_URL"] = f"sqlite:////tmp/agro-pt-verify.sqlite3"

import django

django.setup()

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import Client

from estoque.models import SolicitacaoTransferencia

PASS = 0
FAIL = 0


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print("OK  ", msg)


def fail(msg: str) -> None:
    global FAIL
    FAIL += 1
    print("FAIL", msg)


def check(cond: bool, msg: str, extra: str = "") -> None:
    if cond:
        ok(msg)
    else:
        fail(msg + ((" · " + extra) if extra else ""))


def main() -> int:
    call_command("migrate", "auth", verbosity=0, interactive=False)
    call_command("migrate", "sessions", verbosity=0, interactive=False)
    call_command("migrate", "estoque", verbosity=0, interactive=False)
    User = get_user_model()
    user = User.objects.filter(username="verify_pdv_pt").first()
    if user is None:
        user = User.objects.create_user("verify_pdv_pt", password="x")

    client = Client(HTTP_HOST="localhost")
    client.force_login(user)
    s = client.session
    s["pdv_deposito"] = "centro"
    s.save()

    SolicitacaoTransferencia.objects.filter(produto_externo_id__startswith="VPT").delete()

    r = client.post(
        "/api/pdv/pedido-transferencia/criar/",
        data=json.dumps(
            {
                "itens": [
                    {
                        "produto_id": "VPT1",
                        "nome": "Ração verify",
                        "codigo": "GMV1",
                        "quantidade": 2,
                    }
                ]
            }
        ),
        content_type="application/json",
    )
    body = r.json()
    check(r.status_code == 200 and body.get("ok"), "criar pedido", str(body)[:180])
    row = SolicitacaoTransferencia.objects.filter(produto_externo_id="VPT1").first()
    check(row is not None and row.loja_origem == "vila" and row.loja_destino == "centro", "origem Vila / destino Centro")
    check(row is not None and row.quantidade == Decimal("2.000"), "quantidade 2")

    r0 = client.get("/api/pdv/pedido-transferencia/resumo/").json()
    check(r0.get("badge") == 0, "badge 0 no Centro (quem pediu)")

    s = client.session
    s["pdv_deposito"] = "vila"
    s.save()
    r1 = client.get("/api/pdv/pedido-transferencia/resumo/").json()
    check(r1.get("badge") == 1, "badge 1 na Vila (quem recebe)", str(r1))
    lista = client.get("/api/pdv/pedido-transferencia/").json()
    check(len(lista.get("recebidos") or []) >= 1, "lista recebidos na Vila")

    pk = row.pk
    r_aceita = client.post(
        f"/api/pdv/pedido-transferencia/{pk}/status/",
        data=json.dumps({"acao": "aceitar"}),
        content_type="application/json",
    )
    check(r_aceita.json().get("ok"), "Vila aceita")
    row.refresh_from_db()
    check(row.status == SolicitacaoTransferencia.STATUS_ACEITO, "status ACEITO")

    fake = {"ok": True, "saldo_vila": 1.0, "saldo_centro": 3.0, "quantidade": 2.0, "direcao": "vila_centro"}
    with patch("estoque.views._transferir_entre_depositos_exec", return_value=fake), patch(
        "produtos.views._invalidar_caches_apos_ajuste_pin", return_value=None
    ):
        r_tr = client.post(
            "/api/pdv/pedido-transferencia/transferir/",
            data=json.dumps({"pin": "9999", "ids": [pk]}),
            content_type="application/json",
        )
    check(r_tr.json().get("ok"), "transferir com PIN mock", str(r_tr.json())[:180])
    row.refresh_from_db()
    check(row.status == SolicitacaoTransferencia.STATUS_TRANSFERIDO, "status TRANSFERIDO")

    s = client.session
    s["pdv_deposito"] = "centro"
    s.save()
    r_bad = client.post(
        "/api/pdv/pedido-transferencia/criar/",
        data=json.dumps({"itens": [{"produto_id": "VPT2", "nome": "X", "quantidade": 0}]}),
        content_type="application/json",
    )
    check(r_bad.status_code == 400 and not r_bad.json().get("ok"), "rejeita qtd 0")

    print(f"\n{PASS} ok · {FAIL} fail")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
