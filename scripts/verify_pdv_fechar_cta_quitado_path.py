#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""VERIFY PDV-FECHAR-CTA-QUITADO — prova detalhada.

Path:
  · Quitado abre popup grande (não bloco no meio)
  · Enter/F9 no popup · Voltar ao pagamento · Esc
  · tryConfirmSale oculta popup (hold) — não fica atrás do cupom
  · Cancelar escolha / NFC-e / PIN restaura popup
  · hold bloqueia sync + esconde barra reabrir
  · z-index escolha 360 · NFC-e CPF 370 > fechar 320
  · F12 / Voltar / stepper avisam venda paga
  · Idle 45s reabre + pulsa
  · Anti-reg Enter=sem · F9=com
  · HTTP /pdv/ 200 se local

Uso: python scripts/verify_pdv_fechar_cta_quitado_path.py
     AGRO_VERIFY_HTTP=1 python ...  (probe 127.0.0.1:8000)
"""
from __future__ import annotations

import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
oks = 0
fails = 0

JS = ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_wizard.js"
HTML_STEP = ROOT / "produtos" / "templates" / "produtos" / "partials" / "pdv" / "step_pagamento.html"
HTML_WIZ = ROOT / "produtos" / "templates" / "produtos" / "pdv_wizard.html"
VERSION = ROOT / "VERSION"


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    global fails
    fails += 1
    print(f" FAIL {msg}")


def read(p: Path) -> str:
    if not p.is_file():
        fail(f"ausente {p.relative_to(ROOT)}")
        return ""
    return p.read_text(encoding="utf-8")


def fn_body(src: str, name: str) -> str:
    m = re.search(rf"function\s+{re.escape(name)}\s*\([^)]*\)\s*\{{", src)
    if not m:
        return ""
    i = m.end() - 1
    depth = 0
    for j in range(i, len(src)):
        c = src[j]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return src[i : j + 1]
    return ""


def z_of(html: str, elem_id: str) -> int | None:
    m = re.search(
        rf'id="{re.escape(elem_id)}"[^>]*class="[^"]*z-\[(\d+)\]',
        html,
    )
    if not m:
        m = re.search(
            rf'id="{re.escape(elem_id)}"[\s\S]{{0,200}}?z-\[(\d+)\]',
            html,
        )
    return int(m.group(1)) if m else None


def section(title: str) -> None:
    print(f"\n[{title}]")


def main() -> int:
    js = read(JS)
    step = read(HTML_STEP)
    wiz = read(HTML_WIZ)
    ver = read(VERSION).strip()

    section("0 HTML popup")
    if 'id="pdv-payment-fechar-modal"' in step:
        ok("modal id")
    else:
        fail("modal id")
    if 'id="pdv-payment-fechar-modal-backdrop"' in step and "bg-slate-950/80" in step:
        ok("backdrop opaco")
    else:
        fail("backdrop")
    if "Voltar ao pagamento" in step and 'id="pdv-payment-fechar-voltar"' in step:
        ok("btn Voltar ao pagamento")
    else:
        fail("btn voltar")
    if 'id="pdv-fechar-hero-no-print"' in step and "Enter" in step:
        ok("btn SEM + Enter")
    else:
        fail("btn SEM")
    if 'id="pdv-fechar-hero-print"' in step and "F9" in step:
        ok("btn COM + F9")
    else:
        fail("btn COM")
    if "Pode fechar a venda" in step:
        ok("titulo Pode fechar")
    else:
        fail("titulo")
    if 'id="pdv-payment-fechar-hero"' not in step:
        ok("sem hero inline (só popup)")
    else:
        fail("ainda tem hero inline")
    if 'id="pdv-payment-reabrir-fechar"' in step and 'id="pdv-payment-reabrir-fechar-btn"' in step:
        ok("barra reabrir Fechar venda")
    else:
        fail("reabrir")
    if "min(92vw" in wiz or "92vw" in wiz:
        ok("painel largo (~marca vermelha)")
    else:
        fail("painel largo")
    if "72dvh" in wiz or "min-height: min(72dvh" in wiz:
        ok("painel alto 72dvh")
    else:
        fail("painel alto")

    section("1 z-index (cupom na frente)")
    z_fechar = z_of(step, "pdv-payment-fechar-modal")
    z_escolha = z_of(wiz, "modal-pdv-escolha-impressao")
    z_nfce = z_of(wiz, "modal-pdv-nfce-cpf")
    if z_fechar and z_fechar >= 320:
        ok(f"fechar z={z_fechar}")
    else:
        fail(f"fechar z={z_fechar}")
    if z_escolha and z_fechar and z_escolha > z_fechar:
        ok(f"escolha impressao z={z_escolha} > fechar")
    else:
        fail(f"escolha z={z_escolha} vs fechar {z_fechar}")
    if z_nfce and z_fechar and z_nfce > z_fechar:
        ok(f"nfce-cpf z={z_nfce} > fechar")
    else:
        fail(f"nfce z={z_nfce} vs fechar {z_fechar}")
    if z_escolha and z_nfce and z_nfce >= z_escolha:
        ok("nfce >= escolha")
    else:
        fail("ordem nfce/escolha")

    section("2 JS API modal")
    for name in (
        "vendaQuitadaSemFechar",
        "openFecharVendaModal",
        "closeFecharVendaModal",
        "syncFecharVendaModalUi",
        "focarFecharVendaQuitada",
        "confirmarSaidaComVendaQuitada",
        "scheduleQuitadoIdlePulse",
        "bumpQuitadoIdleActivity",
        "isFecharVendaModalOpen",
    ):
        if f"function {name}" in js:
            ok(f"fn {name}")
        else:
            fail(f"fn {name}")
    if "pdvFecharModalDismissed" in js:
        ok("flag dismissed")
    else:
        fail("flag dismissed")
    if "PDV_QUITADO_IDLE_MS = 45000" in js:
        ok("idle 45s")
    else:
        fail("idle ms")

    section("3 tryConfirmSale fecha popup (bug Com impressao)")
    body_try = fn_body(js, "tryConfirmSale")
    if "function ocultarFecharVendaParaConfirmar" in js and "ocultarFecharVendaParaConfirmar()" in body_try:
        ok("tryConfirmSale chama ocultarFechar")
    else:
        fail("tryConfirmSale nao oculta modal")
    if "pdvFecharModalHold" in js and "pdvFecharModalHold" in fn_body(js, "syncFecharVendaModalUi"):
        ok("hold impede reopen no render")
    else:
        fail("hold sync")
    if body_try.find("ocultarFecharVendaParaConfirmar") < body_try.find("confirmSale("):
        ok("oculta ANTES de confirmSale")
    else:
        fail("ordem oculta/confirmSale")

    section("4 cancelar cupom reabre popup")
    body_escolha_call = ""
    m = re.search(
        r"abrirModalEscolhaImpressao\(function\s*\(escolha\)\s*\{[\s\S]{0,500}?}\)",
        js,
    )
    if m:
        body_escolha_call = m.group(0)
    if "if (!escolha)" in body_escolha_call and "restaurarFecharVendaAposCancelarConfirm" in body_escolha_call:
        ok("cancela escolha -> restaura popup")
    else:
        fail("cancela escolha sem restaurar")
    body_nfce_call = ""
    m2 = re.search(
        r"abrirModalNfceCpf\(function\s*\(opts\)\s*\{[\s\S]{0,500}?}\)",
        js,
    )
    if m2:
        body_nfce_call = m2.group(0)
    if "if (!opts)" in body_nfce_call and "restaurarFecharVendaAposCancelarConfirm" in body_nfce_call:
        ok("cancela NFC-e CPF -> restaura popup")
    else:
        fail("cancela NFC-e sem restaurar")
    if re.search(
        r"onCancel:\s*function\s*\(\)\s*\{[\s\S]{0,120}?restaurarFecharVendaAposCancelarConfirm",
        js,
    ):
        ok("PIN cancel -> restaura popup")
    else:
        fail("PIN cancel sem restaurar")

    section("5 idle + saída")
    body_idle = fn_body(js, "scheduleQuitadoIdlePulse")
    if "openFecharVendaModal(true)" in body_idle and "applyQuitadoIdlePulseClass(true)" in body_idle:
        ok("idle 45s reabre + pulsa")
    else:
        fail("idle reopen/pulse")
    if "pdv-quitado-idle-pulse" in wiz and "pdv-quitado-confirm-pulse" in wiz:
        ok("CSS pulse")
    else:
        fail("CSS pulse")
    if "Tem venda paga sem fechar" in js and "Sim, fechar" in js:
        ok("aviso saída quitada")
    else:
        fail("aviso saída")
    body_nova = fn_body(js, "solicitarNovaVenda")
    if "confirmarSaidaComVendaQuitada" in body_nova:
        ok("F12/Nova venda guarda quitado")
    else:
        fail("F12 guarda")
    if "Não, voltar" in js and "Não, voltar etapa" in js:
        ok("Voltar + stepper guardam")
    else:
        fail("voltar/stepper")

    section("6 wire UI")
    if re.search(r"fecharHeroNoPrint[\s\S]{0,220}?tryConfirmSale\(false\)", js):
        ok("click SEM -> tryConfirmSale(false)")
    else:
        fail("click SEM")
    if re.search(r"fecharHeroPrint[\s\S]{0,220}?tryConfirmSale\(true\)", js):
        ok("click COM -> tryConfirmSale(true)")
    else:
        fail("click COM")
    if re.search(r"paymentFecharVoltar[\s\S]{0,200}?closeFecharVendaModal\(true\)", js):
        ok("Voltar fecha dismissed")
    else:
        fail("Voltar wire")
    if re.search(r"isFecharVendaModalOpen\(\)[\s\S]{0,120}?closeFecharVendaModal\(true\)", js):
        ok("Esc fecha popup")
    else:
        fail("Esc")
    if re.search(r"paymentReabrirFecharBtn[\s\S]{0,200}?openFecharVendaModal\(true\)", js):
        ok("reabrir btn")
    else:
        fail("reabrir btn")
    body_open_forma = fn_body(js, "openPaymentFormaModal")
    if "vendaQuitadaSemFechar" in body_open_forma and "focarFecharVendaQuitada" in body_open_forma:
        ok("F3 quitado -> foca fechar (nao abre forma)")
    else:
        fail("F3 quitado")
    body_after = fn_body(js, "afterCommitTrancheFlow")
    if "openFecharVendaModal(true)" in body_after:
        ok("apos lancar quitado -> abre popup")
    else:
        fail("afterCommit abre")

    section("7 anti-reg Enter/F9")
    if (
        "Enter = sempre sem impressão" in js
        and re.search(r"key === 'Enter'[\s\S]{0,500}?tryConfirmSale\(false\)", js)
    ):
        ok("kbd Enter -> sem impressao")
    else:
        fail("kbd Enter")
    if re.search(r"code === 'F9'[\s\S]{0,200}?tryConfirmSale\(true\)", js):
        ok("kbd F9 -> com impressao")
    else:
        fail("kbd F9")
    bad = re.findall(
        r"key === 'Enter'[\s\S]{0,300}?tryConfirmSale\(true\)",
        js,
    )
    if len(bad) == 0:
        ok("sem Enter->tryConfirmSale(true) direto")
    else:
        fail(f"Enter->true ocorrencias={len(bad)}")

    section("8 sync render quitado")
    body_pag = fn_body(js, "renderPagamento")
    if "syncFecharVendaModalUi(quitadoPay)" in body_pag:
        ok("render chama syncFechar")
    else:
        fail("render sync")
    if "paymentFormaAtualWrap" in body_pag and "pdvFecharModalDismissed" in body_pag:
        ok("esconde forma só com popup aberto")
    else:
        fail("forma wrap toggle")

    section("9 ajuda + VERSION + size")
    if "popup grande" in wiz:
        ok("ajuda wizard menciona popup")
    else:
        fail("ajuda")
    if ver:
        ok(f"VERSION={ver}")
    else:
        fail("VERSION")
    sz = JS.stat().st_size if JS.is_file() else 0
    if sz > 100_000:
        ok(f"js_size={sz}")
    else:
        fail(f"js_size={sz}")

    section("10 hold detalhado (popup some ao escolher)")
    body_ocultar = fn_body(js, "ocultarFecharVendaParaConfirmar")
    body_restaurar = fn_body(js, "restaurarFecharVendaAposCancelarConfirm")
    body_sync = fn_body(js, "syncFecharVendaModalUi")
    body_close = fn_body(js, "closeFecharVendaModal")
    if "pdvFecharModalHold = true" in body_ocultar and "closeFecharVendaModal(false)" in body_ocultar:
        ok("ocultar: hold=true + close(false)")
    else:
        fail("ocultar body")
    if "pdvFecharModalHold = false" in body_restaurar and "openFecharVendaModal(true)" in body_restaurar:
        ok("restaurar: hold=false + open(true)")
    else:
        fail("restaurar body")
    if "if (pdvFecharModalHold)" in body_sync and "return" in body_sync:
        ok("sync: hold early-return")
    else:
        fail("sync hold return")
    # reabrir bar must stay hidden while hold
    if re.search(
        r"pdvFecharModalHold[\s\S]{0,400}?paymentReabrirFechar",
        body_sync,
    ) or (
        "pdvFecharModalHold" in body_sync
        and "paymentReabrirFechar" in body_sync
    ):
        ok("sync trata reabrir com hold")
    else:
        # still ok if hold returns before reabrir — check order
        hold_i = body_sync.find("pdvFecharModalHold")
        reabrir_i = body_sync.find("paymentReabrirFechar")
        if hold_i >= 0 and (reabrir_i < 0 or hold_i < reabrir_i):
            ok("sync hold antes de reabrir (ou sem reabrir no hold)")
        else:
            fail("sync reabrir/hold")
    if "pdvFecharModalHold = false" in fn_body(js, "resetWizardParaNovaVenda"):
        ok("resetWizard zera hold")
    else:
        fail("reset hold")
    # Outro validation errors restore
    if body_try.count("restaurarFecharVendaAposCancelarConfirm") >= 2:
        ok("tryConfirmSale restaura em erros (>=2)")
    else:
        fail(f"tryConfirmSale restaura count={body_try.count('restaurarFecharVendaAposCancelarConfirm')}")
    # close(false) must NOT clear hold (dismissed only)
    if "pdvFecharModalHold" not in body_close or "pdvFecharModalHold = false" not in body_close:
        ok("closeFechar nao zera hold sozinho")
    else:
        fail("close zera hold (ruim)")

    section("11 HTTP local (opcional)")
    do_http = os.environ.get("AGRO_VERIFY_HTTP", "1").strip() not in ("0", "false", "no")
    if do_http:
        for path, label in (("/healthz", "healthz"), ("/pdv/", "pdv")):
            try:
                req = urllib.request.Request(
                    f"http://127.0.0.1:8000{path}",
                    headers={"User-Agent": "verify-pdv-fechar"},
                )
                with urllib.request.urlopen(req, timeout=8) as resp:
                    code = getattr(resp, "status", None) or resp.getcode()
                    if code == 200:
                        ok(f"HTTP {label} {code}")
                    else:
                        fail(f"HTTP {label} {code}")
            except Exception as e:
                fail(f"HTTP {label}: {e}")
    else:
        ok("HTTP skip (AGRO_VERIFY_HTTP=0)")

    print(f"\nRESULTADO: {oks} OK · {fails} FAIL")
    if fails:
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
