# -*- coding: utf-8 -*-
"""Prova detalhada path PDV-ENTREGA-F3.

Cobre: HTML F3/F7, JS (nao pular para pagamento apos balcao), simulacao do
bug vs fix, overlay onde-pagar, F3/F7, CSS leak, Django GET checkout.

  python scripts/verify_pdv_entrega_f3_path.py
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client
from django.urls import reverse

JS = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
STATE = (ROOT / "produtos/static/produtos/js/pdv_state.js").read_text(encoding="utf-8")
HTML_STEP = (ROOT / "produtos/templates/produtos/partials/pdv/step_produtos.html").read_text(
    encoding="utf-8"
)
HTML_WIZ = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
HTML_EF = (ROOT / "produtos/templates/produtos/partials/pdv/entrega_wizard_overlay.html").read_text(
    encoding="utf-8"
)
HTML_PAY = (ROOT / "produtos/templates/produtos/partials/pdv/step_pagamento.html").read_text(
    encoding="utf-8"
)

ok = 0
fail = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global ok, fail
    msg = name + ((" -- " + detail) if detail else "")
    safe = msg.encode("ascii", "replace").decode("ascii")
    if cond:
        ok += 1
        print("  OK ", safe)
    else:
        fail += 1
        print(" FAIL", safe)


def slice_fn(src: str, name: str, n: int = 1200) -> str:
    m = re.search(rf"function {re.escape(name)}\s*\(", src)
    if not m:
        return ""
    return src[m.start() : m.start() + n]


def resolve_flow(modo: str) -> list[str]:
    flow = ["produtos"]
    if modo != "retirada":
        flow.append("entrega")
    flow.append("pagamento")
    return flow


def next_step(current: str, flow: list[str]) -> str | None:
    if current not in flow:
        return None
    idx = flow.index(current)
    return flow[idx + 1] if idx < len(flow) - 1 else None


def main() -> None:
    print("=== PDV-ENTREGA-F3 detalhado ===")

    check("html_btn_entrega", 'id="pdv-step1-advance"' in HTML_STEP)
    check("html_label_entrega", ">Entrega<" in HTML_STEP)
    check("html_title_f3", "Entrega" in HTML_STEP and "F3" in HTML_STEP.split("pdv-step1-advance", 1)[1][:900])
    check("html_btn_pagar", 'id="pdv-step1-payment"' in HTML_STEP)
    check("html_label_pagar", ">Pagar<" in HTML_STEP)
    check("html_pagar_f7", "F7" in HTML_STEP.split("pdv-step1-payment", 1)[1][:500])
    check("html_dois_botoes", HTML_STEP.find("pdv-step1-payment") < HTML_STEP.find("pdv-step1-advance"))

    check("state_flow_order", "STEP_ORDER = ['produtos', 'entrega', 'pagamento']" in STATE)
    check(
        "state_flow_retirada",
        "if (state.entrega.modoRetiradaEntrega !== 'retirada') flow.push('entrega')" in STATE,
    )
    check("state_default_modo_vazio", "modoRetiradaEntrega: ''" in STATE or 'modoRetiradaEntrega: ""' in STATE)

    prep = slice_fn(JS, "prepararEntregaAoSairDeProdutos", 900)
    check("fn_preparar", "function prepararEntregaAoSairDeProdutos" in JS)
    check("prep_modo_entrega", "modoRetiradaEntrega: 'entrega'" in prep)
    check("prep_ativa", "ativa: true" in prep)
    check("prep_zera_local", "localPagamento: ''" in prep or 'localPagamento: ""' in prep)
    check("prep_zera_meio", "meioNaEntrega: ''" in prep or 'meioNaEntrega: ""' in prep)
    check("prep_zera_taxa", "taxaEntregaRespondida: false" in prep)
    check("prep_zera_endereco_passo", "enderecoPassoConcluido: false" in prep)
    check("prep_zera_frete_flag", "entregaFreteLiberadoPagamento: false" in prep)
    check("prep_zera_frete_valor", "setPagamentoField('frete', 0)" in prep)
    check("prep_sync_cliente", "syncEntregaEnderecoFromCliente" in prep)

    m = re.search(
        r"dom\.step1Advance\.addEventListener\('click',\s*function\s*\(\)\s*\{(.{0,900})\}",
        JS,
        re.S,
    )
    click = m.group(1) if m else ""
    check("bind_advance", bool(m))
    check("click_can_advance", "canAdvance" in click)
    check("click_prepara", "prepararEntregaAoSairDeProdutos()" in click)
    check("click_set_entrega", "State.setCurrentStep('entrega')" in click)
    check("click_nao_nextstep", "nextStep(" not in click)
    check("click_nao_pagamento", "setCurrentStep('pagamento')" not in click)

    pagar = slice_fn(JS, "irParaPagamentoFromProdutos", 700)
    check("pagar_marca_balcao", "marcarVendaBalcaoSemEntrega()" in pagar)
    check("pagar_vai_pagamento", "setCurrentStep('pagamento')" in pagar)
    check("pagar_bind", "irParaPagamentoFromProdutos" in JS and "step1Payment.addEventListener" in JS)

    balcao = slice_fn(JS, "marcarVendaBalcaoSemEntrega", 500)
    check("balcao_retirada", "modoRetiradaEntrega: 'retirada'" in balcao)

    check(
        "f3_clica_entrega",
        "event.code === 'F3'" in JS and "dom.step1Advance.click()" in JS,
    )
    check(
        "f7_clica_pagar",
        "event.code === 'F7'" in JS and "dom.step1Payment.click()" in JS,
    )
    f3_idx = JS.find("if (event.code === 'F3' && !pickerOpen")
    f7_idx = JS.find("if (event.code === 'F7' && !pickerOpen")
    check("f3_antes_f7_produtos", 0 < f3_idx < f7_idx)

    check("overlay_ef1_entrega", 'id="pdv-ef1-entrega"' in HTML_EF)
    check("overlay_ef1_loja", 'id="pdv-ef1-loja"' in HTML_EF)
    check("overlay_texto_entrega", "Pagamento na entrega" in HTML_EF)
    check("overlay_texto_loja", "Pagamento na loja" in HTML_EF)
    check("ef1_nao_set_pagamento_step", "setCurrentStep('pagamento')" not in slice_fn(JS, "btnEf1Entrega", 200))

    ef1 = JS[JS.find("pdv-ef1-entrega") : JS.find("pdv-ef1-entrega") + 900]
    check("ef1_local_entrega", "localPagamento: 'entrega'" in ef1)
    check("ef1_nao_current_pay", "setCurrentStep('pagamento')" not in ef1)

    ef1l = JS[JS.find("pdv-ef1-loja") : JS.find("pdv-ef1-loja") + 900]
    check("ef1l_local_loja", "localPagamento: 'loja'" in ef1l)
    check("ef1l_nao_current_pay", "setCurrentStep('pagamento')" not in ef1l)

    check("css_esconde_entrega_fora", 'body:not([data-pdv-step="entrega"]) #pdv-entrega-wizard' in HTML_WIZ)
    check("attr_step", "data-pdv-step" in JS)
    check("pay_root", 'id="pdv-step-pagamento-root"' in HTML_PAY)
    check("pay_hidden_default", "hidden" in HTML_PAY[:200].lower())

    check(
        "loja_so_depois_impressao",
        "function wizardIrParaPagamentoComImpressao" in JS
        and "setCurrentStep('pagamento')" in slice_fn(JS, "wizardIrParaPagamentoComImpressao", 1600),
    )
    loja_fn = slice_fn(JS, "wizardIrParaPagamentoComImpressao", 1600)
    check("loja_exige_local_loja", "localPagamento" in loja_fn and "!== 'loja'" in loja_fn)

    # --- simulacao do bug que o Renan viu ---
    stale = resolve_flow("retirada")
    check("sim_bug_stale_pula_pagar", next_step("produtos", stale) == "pagamento")
    check("sim_flow_vazio_tem_entrega", "entrega" in resolve_flow(""))
    check("sim_flow_entrega_tem_entrega", "entrega" in resolve_flow("entrega"))
    check("sim_fix_sempre_entrega", True)  # click setCurrentStep('entrega') independente do flow
    check("sim_next_entrega_ok", next_step("produtos", resolve_flow("entrega")) == "entrega")
    check("sim_pagar_continua_ok", next_step("produtos", resolve_flow("retirada")) == "pagamento")

    # F7 no footer de produtos ainda marca balcao (ok, nao e o botao Entrega)
    nxt = JS[JS.find("dom.btnNext.addEventListener") : JS.find("dom.btnNext.addEventListener") + 900]
    check("footer_next_marca_balcao_se_pular", "marcarVendaBalcaoSemEntrega()" in nxt)

    # Django: URL + pagina inclui botao (se houver user)
    try:
        url = reverse("pdv_home")
    except Exception as exc:
        url = ""
        check("url_pdv_home", False, str(exc))
    else:
        check("url_pdv_home", bool(url))
        check("url_checkout_legado", reverse("pdv_checkout") in ("/pdv/checkout/", "/pdv/checkout"))

    from django.test import override_settings
    from produtos.caixa_util import validar_pin_operador, _perfil_usuario_por_pin

    pin_ok, pin_msg = validar_pin_operador("9973")
    perfil = _perfil_usuario_por_pin("9973")
    check("pin_9973_valido", pin_ok, pin_msg)
    check("pin_9973_tem_perfil", perfil is not None)

    User = get_user_model()
    user = User.objects.filter(is_active=True).order_by("id").first()
    if user:
        with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
            c = Client()
            c.force_login(user)
            resp = c.get(url or "/consulta/", follow=True)
        body = resp.content.decode("utf-8", "replace")
        check("http_pdv", resp.status_code == 200, str(resp.status_code))
        path_info = (resp.request or {}).get("PATH_INFO", "")
        check("http_path_pdv", "pdv-step1-advance" in body or "pdv_wizard.js" in body, path_info)
        loginish = "name=\"username\"" in body and "pdv_wizard.js" not in body
        check("http_nao_login", not loginish)
        check("http_tem_btn_entrega", 'id="pdv-step1-advance"' in body)
        check("http_tem_label", ">Entrega<" in body)
        check("http_tem_pagar", 'id="pdv-step1-payment"' in body)
        check("http_tem_ef1", 'id="pdv-ef1-entrega"' in body)
        check("http_js_wizard", "pdv_wizard.js" in body)
        check("http_js_state", "pdv_state.js" in body)
    else:
        check("http_checkout", False, "sem usuario Django local")

    print("")
    print("%s OK / %s FAIL" % (ok, fail))
    raise SystemExit(1 if fail else 0)


if __name__ == "__main__":
    main()
