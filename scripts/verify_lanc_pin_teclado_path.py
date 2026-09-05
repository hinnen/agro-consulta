# -*- coding: utf-8 -*-
"""
LANC-PIN-TECLADO — prova detalhada do path.

Bug: alert nativo «Identifique-se com o PIN (modo descanso)» sem teclado
ao Finalizar / baixar / editar / excluir em Lançamentos.

Path:
  1) helpers em lancamentos_pin_entrada
  2) templates Novo / CP / CR (teclado, não alert)
  3) APIs exigem PIN fresco (_lancamentos_operador_label)
  4) runtime Client: 403 sem PIN · com PIN 9973 passa o gate · HTML tem sspin
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

PIN_TESTE = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def check_static() -> None:
    print("--- static ---")
    pin = read("produtos/templates/produtos/includes/lancamentos_pin_entrada.html")
    manual = read("produtos/templates/produtos/lancamentos_manual.html")
    cp = read("produtos/templates/produtos/lancamentos_contas_pagar_teste.html")
    cr = read("produtos/templates/produtos/lancamentos_financeiros.html")
    sspin = read("produtos/templates/produtos/_screensaver_pin.html")
    views = read("produtos/views.py")
    caixa = read("produtos/caixa_util.py")

    check("gmLancamentosTratarErroPin" in pin, "helper gmLancamentosTratarErroPin")
    check("gmLancamentosComOperador" in pin, "helper gmLancamentosComOperador")
    check("gmSspinAbrirSeErroPin" in pin, "helper chama AbrirSeErroPin")
    check("gmSspinGarantirOperador" in pin, "helper chama GarantirOperador")
    check("_screensaver_pin.html" in pin, "include sspin no pin entrada")

    check("gmSspinErroPedePin" in sspin, "sspin detecta msg PIN")
    check("Identifique-se com o PIN" in sspin, "sspin regex mensagem canônica")
    check("id=\"sspin-input\"" in sspin and "id=\"sspin-numpad\"" in sspin, "sspin tem campo+numpad")

    check("gmLancamentosComOperador" in manual, "manual: pede PIN antes de gravar")
    check("gmLancamentosTratarErroPin" in manual, "manual: 403 → teclado")
    check("gravarLoteManual" in manual, "manual: retry apos PIN")
    check("lancamentos_pin_entrada.html" in manual, "manual inclui pin entrada")
    # Finalizar nao pode so alertar a MSG canônica
    idx = manual.find("Falha ao gravar")
    chunk = manual[max(0, idx - 500) : idx + 250] if idx >= 0 else ""
    check("gmLancamentosTratarErroPin" in chunk, "Finalizar: tratar PIN antes do alert")
    check("alert((msgs[0] || j.erro" not in manual, "manual: removido alert puro da falha")

    for label, src in (("CP", cp), ("CR", cr)):
        check("alertOuPinLanc" in src, f"{label}: alertOuPinLanc")
        check("comOperadorLanc" in src, f"{label}: comOperadorLanc")
        check("gmLancamentosTratarErroPin" in src or "alertOuPinLanc" in src, f"{label}: caminho PIN")
        check("lancamentos_pin_entrada.html" in src, f"{label}: include pin entrada")

    check("enviarBaixa" in cp and "alertOuPinLanc" in cp, "CP baixa → PIN")
    check("enviarEditar" in cp and "alertOuPinLanc" in cp, "CP editar → PIN")
    check("enviarParcial" in cp and "alertOuPinLanc" in cp, "CP parcial → PIN")
    check("excluirLancamentoPorId" in cp and "comOperadorLanc" in cp, "CP excluir → PIN")

    check("enviarBaixaParcial" in cr and "alertOuPinLanc" in cr, "CR parcial → PIN")
    check("enviarEditar" in cr and "alertOuPinLanc" in cr, "CR editar → PIN")
    check("enviarBaixaLote" in cr and "comOperadorLanc" in cr, "CR baixa lote → PIN")

    msg = "Identifique-se com o PIN (modo descanso) antes de continuar."
    check(msg in caixa, "MSG canônica caixa_util")
    check("def _lancamentos_operador_label" in views, "gate _lancamentos_operador_label")
    check("api_lancamentos_criar_manual_lote" in views, "API criar lote existe")
    # gate usado nas escritas
    for nome in (
        "api_lancamentos_criar_manual_lote",
        "api_lancamentos_baixa",
        "api_lancamentos_baixa_parcial",
        "api_lancamentos_alterar",
        "api_lancamentos_excluir",
    ):
        check(nome in views, f"views tem {nome}")


def _limpar_pin_sessao(client) -> None:
    session = client.session
    for k in (
        "pdv_operador_nome",
        "pdv_operador_fresco_em",
        "pdv_caixa_gerido_operador",
        "pdv_operador_pin",
    ):
        session.pop(k, None)
    session.save()


def check_runtime() -> None:
    print("--- runtime Django ---")
    import django

    django.setup()

    from django.conf import settings
    from django.contrib.auth import get_user_model
    from django.test import Client, RequestFactory, override_settings
    from django.urls import reverse

    from produtos.caixa_util import (
        MSG_PIN_OPERADOR_OBRIGATORIO,
        exigir_operador_pin_request,
        rotulo_operador_pin,
    )
    from produtos.pdv_transf_loja_util import gravar_operador_sessao_pdv
    from produtos.views import _lancamentos_operador_label

    class Sess(dict):
        modified = False

    rf = RequestFactory()
    req = rf.get("/")
    req.session = Sess()
    req.user = SimpleNamespace(
        is_authenticated=True,
        pk=1,
        get_full_name=lambda: "Chrome Fantasma",
        get_username=lambda: "chrome",
        first_name="Chrome",
        email="chrome@loja.local",
    )

    op, err_resp = _lancamentos_operador_label(req)
    check(op is None and err_resp is not None, "label: sem PIN → JsonResponse erro")
    if err_resp is not None:
        body = json.loads(err_resp.content.decode("utf-8"))
        check(
            body.get("erro") == MSG_PIN_OPERADOR_OBRIGATORIO,
            "label: erro = MSG modo descanso",
        )
        check(err_resp.status_code == 403, "label: status 403")

    rot = (rotulo_operador_pin(PIN_TESTE) or "").strip()
    check(bool(rot), f"PIN {PIN_TESTE} existe no PG (rotulo={rot!r})")
    if not rot:
        fail("PIN 9973 nao encontrado — runtime HTTP skip parcial")
        return

    gravar_operador_sessao_pdv(req, PIN_TESTE)
    op2, err2 = _lancamentos_operador_label(req)
    check(op2 == rot and err2 is None, f"label: com PIN fresco ok ({op2})")

    lab, err = exigir_operador_pin_request(req)
    check(lab == rot and err == "", "exigir_operador fresco ok")

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    check(user is not None, "usuario Django para Client")
    if not user:
        return

    hosts = list(getattr(settings, "ALLOWED_HOSTS", []) or [])
    if "testserver" not in hosts:
        hosts = hosts + ["testserver", "localhost", "127.0.0.1"]

    with override_settings(ALLOWED_HOSTS=hosts):
        c = Client()
        c.force_login(user)

        url_op = reverse("api_pdv_registrar_operador")

        # --- sem PIN: APIs de escrita devem 403 com MSG ---
        _limpar_pin_sessao(c)
        c.post(url_op, data=json.dumps({"operador": ""}), content_type="application/json")
        _limpar_pin_sessao(c)

        apis_403 = [
            (
                "criar_manual_lote",
                "api_lancamentos_criar_manual_lote",
                {
                    "tipo": "pagar",
                    "data_competencia": "2026-09-05",
                    "data_vencimento": "2026-09-05",
                    "empresa_nome": "Agro Mais Centro",
                    "pessoa_nome": "Teste PIN Path",
                    "banco_nome": "Caixa",
                    # plano inexistente: passa o gate PIN e falha sem gravar título
                    "linhas": [
                        {
                            "plano_conta": "__plano_inexistente_lanc_pin_path__",
                            "valor": "1,00",
                        }
                    ],
                },
            ),
            (
                "baixa",
                "api_lancamentos_baixa",
                {
                    "ids": ["000000000000000000000000"],
                    "tipo": "pagar",
                    "data_movimento": "2026-09-05",
                    "forma_pagamento": "Dinheiro",
                    "banco": "Caixa",
                },
            ),
            (
                "baixa_parcial",
                "api_lancamentos_baixa_parcial",
                {
                    "lancamento_id": "000000000000000000000000",
                    "tipo": "pagar",
                    "data_movimento": "2026-09-05",
                    "parcelas": [
                        {
                            "valor": 1.0,
                            "forma_pagamento": "Dinheiro",
                            "banco": "Caixa",
                        }
                    ],
                },
            ),
            (
                "alterar",
                "api_lancamentos_alterar",
                {"id": "000000000000000000000000", "descricao": "x"},
            ),
            (
                "excluir",
                "api_lancamentos_excluir",
                {"id": "000000000000000000000000"},
            ),
        ]

        for label, url_name, body in apis_403:
            url = reverse(url_name)
            r = c.post(url, data=json.dumps(body), content_type="application/json")
            try:
                j = r.json()
            except Exception:
                j = {}
            erro = str(j.get("erro") or "")
            check(
                r.status_code == 403 and MSG_PIN_OPERADOR_OBRIGATORIO in erro,
                f"sem PIN {label} → 403 MSG (got {r.status_code} {erro[:60]!r})",
            )

        # --- com PIN 9973: gate passa (pode falhar depois por dados fake) ---
        r_pin = c.post(
            url_op,
            data=json.dumps({"pin": PIN_TESTE}),
            content_type="application/json",
        )
        j_pin = r_pin.json() if r_pin.status_code == 200 else {}
        check(
            r_pin.status_code == 200 and j_pin.get("ok") and (j_pin.get("operador") or "").strip(),
            f"POST operador PIN {PIN_TESTE} → {j_pin.get('operador')!r}",
        )

        r_get = c.get(url_op)
        j_get = r_get.json() if r_get.status_code == 200 else {}
        check(bool(j_get.get("fresco") and j_get.get("operador")), f"GET operador fresco ({j_get.get('operador')!r})")

        for label, url_name, body in apis_403:
            url = reverse(url_name)
            r = c.post(url, data=json.dumps(body), content_type="application/json")
            try:
                j = r.json()
            except Exception:
                j = {}
            erro = str(j.get("erro") or "")
            check(
                MSG_PIN_OPERADOR_OBRIGATORIO not in erro,
                f"com PIN {label} nao bloqueia por descanso (status={r.status_code} erro={erro[:70]!r})",
            )
            if label == "criar_manual_lote":
                ids = j.get("ids") or []
                check(not ids, f"criar com plano fake nao grava ids ({ids!r})")
                check(j.get("ok") is not True, "criar plano fake nao retorna ok=True")

        # --- HTML das telas: teclado + helpers ---
        pages = [
            ("manual", "lancamentos_manual"),
            ("cp", "lancamentos_contas_pagar"),
            ("cr", "lancamentos_contas_receber"),
        ]
        for label, url_name in pages:
            try:
                url = reverse(url_name)
            except Exception as e:
                fail(f"reverse {url_name}: {e}")
                continue
            r = c.get(url)
            if r.status_code in (301, 302):
                ok(f"GET {label} redirect {r.status_code}")
                continue
            check(r.status_code == 200, f"GET {label} ({r.status_code})")
            html = r.content.decode("utf-8", errors="replace")
            check("sspin-root" in html and "sspin-input" in html, f"{label} HTML tem teclado")
            check(
                "gmLancamentosTratarErroPin" in html or "gmLancamentosComOperador" in html,
                f"{label} HTML tem helpers PIN",
            )
            check("gmSspinAbrirSeErroPin" in html, f"{label} HTML tem AbrirSeErroPin")


def main() -> int:
    print("=== LANC-PIN-TECLADO path detalhado ===")
    check_static()
    try:
        check_runtime()
    except Exception as e:
        fail(f"runtime: {e}")
        import traceback

        traceback.print_exc()

    print("---")
    print(f"VERIFY_{'OK' if not FAILS else 'FAIL'} {OKS}/{OKS + len(FAILS)}")
    for f in FAILS:
        print(" -", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
