#!/usr/bin/env python3
"""A mensagem do celular não pode segurar a resposta da ponte."""
from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.test import RequestFactory, override_settings

from produtos import views_atendimento_whatsapp as vw

OK = 0
FAIL = 0


def check(label: str, cond: bool) -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {label}")
    else:
        FAIL += 1
        print(f" FAIL {label}")


def main() -> int:
    terminou = threading.Event()

    def lento(**kwargs):
        time.sleep(1.2)
        terminou.set()
        return None, ""

    vw.processar_entrada = lento
    rf = RequestFactory()
    req = rf.post(
        "/api/atendimento-whatsapp/bridge/entrada/",
        data=b'{"jid":"5513999999999@s.whatsapp.net","texto":"oi","wa_id":"lento-1"}',
        content_type="application/json",
        HTTP_HOST="agro-consulta-wa.onrender.com",
        HTTP_X_AGRO_WA_TOKEN="segredo-teste",
    )
    with override_settings(AGRO_WA_BRIDGE_TOKEN="segredo-teste"):
        t0 = time.monotonic()
        resp = vw.api_atendimento_whatsapp_bridge_entrada(req)
        dt = time.monotonic() - t0
    check("resposta na hora", resp.status_code == 200 and dt < 0.4)
    check("gravacao segue depois", terminou.wait(3))
    print(f"\n{OK} ok, {FAIL} falha")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
