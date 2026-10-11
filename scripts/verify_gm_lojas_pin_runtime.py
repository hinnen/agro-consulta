"""Runtime — sessão hub, allowlist Vendas/Tarefas, rotas GM Lojas (sem Mongo/PDV)."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ.setdefault("AGRO_VL_HUB_PINS_VENDAS_TAREFAS", "9973,1,2,3,4,8")

import django

django.setup()

from django.test import RequestFactory, override_settings
from django.urls import reverse

from produtos.vendas_lojas_hub_acesso_util import pin_liberado_vendas_tarefas
from produtos.vendas_lojas_hub_sessao_util import (
    SESSION_DESDE,
    SESSION_OPERADOR,
    SESSION_PIN,
    operador_da_sessao,
    pin_da_sessao,
)
from produtos.vendas_lojas_sem_acesso_util import resposta_sem_acesso_html
from produtos.views import vendas_lojas_hub
from tarefas.pin_util import exigir_operador_html as exigir_tarefas_html

OK = FAIL = 0


def check(label: str, cond: bool, detail: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {label}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"  FAIL {label}" + (f" — {detail}" if detail else ""))


def _req_session(pin: str, nome: str = "Renan Dev") -> object:
    rf = RequestFactory()
    req = rf.get("/vendas/lojas/")
    from django.contrib.sessions.middleware import SessionMiddleware

    mw = SessionMiddleware(lambda r: None)
    mw.process_request(req)
    req.session.save()
    req.session[SESSION_OPERADOR] = nome
    req.session[SESSION_PIN] = pin
    req.session[SESSION_DESDE] = time.time()
    req.session.save()
    return req


def main() -> int:
    print("VERIFY GM-LOJAS RUNTIME")
    print("== Rotas (reverse) ==")
    names = [
        "vendas_lojas_hub",
        "vendas_lojas_pin",
        "vendas_lojas_logout",
        "vendas_lojas_resumo",
        "fotos_produto_app",
        "etiquetas_solicitacao_app",
        "gestao_logs_painel",
        "api_etiquetas_solicitacao_mobile_criar",
    ]
    for name in names:
        try:
            path = reverse(name)
            check(f"reverse_{name}", path.startswith("/"), path)
        except Exception as e:
            check(f"reverse_{name}", False, str(e))
    try:
        imp = reverse("api_etiquetas_solicitacao_importar", kwargs={"pk": 1})
        check("reverse_api_etiquetas_importar", "/importar/" in imp, imp)
    except Exception as e:
        check("reverse_api_etiquetas_importar", False, str(e))

    print("== Allowlist (PIN 9973 no env de teste) ==")
    with override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="9973,1,2,3,4,8"):
        check("pin_9973_liberado", pin_liberado_vendas_tarefas("9973"))
        check("pin_5_bloqueado", not pin_liberado_vendas_tarefas("5"))

    print("== Sessão hub ==")
    r9973 = _req_session("9973")
    check("sessao_pin_9973", pin_da_sessao(r9973) == "9973")
    check("sessao_operador", operador_da_sessao(r9973) == "Renan Dev")

    print("== Hub (4 cartões, sem aviso) ==")
    hub_resp = vendas_lojas_hub(r9973)
    body = hub_resp.content
    check("hub_200", hub_resp.status_code == 200)
    check("hub_4_cards", all(x in body for x in (b"vl-hub-vendas", b"vl-hub-tarefas", b"vl-hub-fotos", b"vl-hub-etiquetas")))
    check("hub_sem_vl_acesso", b"vl-acesso" not in body)

    print("== Sem acesso (PIN 5) ==")
    r5 = _req_session("5", "Operador 5")

    @exigir_tarefas_html
    def _tarefas_stub(request):
        from django.http import HttpResponse

        return HttpResponse("OK-TAREFAS")

    t_resp = _tarefas_stub(r5)
    check("tarefas_pin5_sem_acesso", b"sem acesso" in t_resp.content.lower() and b"Tarefas" in t_resp.content)

    sa = resposta_sem_acesso_html(r5, "vendas")
    check("vendas_sem_acesso_page", b"Vendas" in sa.content and b"sem acesso" in sa.content.lower())

    print("== PIN 9973 passa guard tarefas ==")
    r9973b = _req_session("9973")
    t_ok = _tarefas_stub(r9973b)
    check("tarefas_pin9973_ok", t_ok.content == b"OK-TAREFAS")

    print(f"\nTOTAL OK={OK} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
