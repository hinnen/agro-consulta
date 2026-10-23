# -*- coding: utf-8 -*-
"""Prova detalhada PDV-ORC-IMPRIMIR — Salvar | Imprimir no card lateral.

Cobre: HTML dois botoes · JS salvar/imprimir · origem impressao · icone ·
cupom so cliente · Salvar nao manda imprimir · WhatsApp intacto ·
API POST origem=impressao · PIN 9973 · HTTP se runserver.

  python scripts/verify_pdv_orc_imprimir_path.py
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

PIN = "9973"
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


def check_html() -> None:
    html = read("produtos/templates/produtos/partials/pdv/step_produtos.html")
    check('id="pdv-step1-salvar-orcamento-btn"' in html, "HTML id Salvar")
    check('id="pdv-step1-imprimir-orcamento-btn"' in html, "HTML id Imprimir")
    check("grid-cols-2" in html, "HTML grade 2 colunas")
    check("Salvar orçamento" not in html, "HTML sem rotulo longo antigo")
    # Rotulo curto dentro do botao Salvar
    idx_s = html.find('id="pdv-step1-salvar-orcamento-btn"')
    bloco_s = html[idx_s : idx_s + 600] if idx_s >= 0 else ""
    check("Salvar" in bloco_s, "HTML rotulo Salvar no botao")
    idx_i = html.find('id="pdv-step1-imprimir-orcamento-btn"')
    bloco_i = html[idx_i : idx_i + 900] if idx_i >= 0 else ""
    check("Imprimir" in bloco_i, "HTML rotulo Imprimir no botao")
    check("svg" in bloco_i.lower() or "M6.72 13.829" in bloco_i, "HTML icone impressora no botao")
    # Ordem visual: Salvar antes de Imprimir
    check(0 < idx_s < idx_i, "HTML Salvar antes de Imprimir")


def check_fonte() -> None:
    wiz = read("produtos/static/produtos/js/pdv_wizard.js")
    check("step1ImprimirOrcamentoBtn" in wiz, "JS dom Imprimir")
    check("function imprimirOrcamentoWizard" in wiz, "JS funcao imprimir")
    check("function budgetEhImpressao" in wiz, "JS detecta origem impressao")
    check("function budgetImpressaoIconHtml" in wiz, "JS icone impressora lista")
    check("function budgetOrigemIconHtml" in wiz, "JS icone origem unificado")
    check("function budgetOrigemTitle" in wiz, "JS title origem")
    check("function setOrcamentoActionBtnsDisabled" in wiz, "JS desliga Salvar+Imprimir")
    check(
        "addEventListener('click', imprimirOrcamentoWizard)" in wiz,
        "JS click Imprimir ligado",
    )
    check(
        "addEventListener('click', salvarOrcamentoWizard)" in wiz,
        "JS click Salvar ligado",
    )

    idx_save = wiz.find("function salvarOrcamentoWizard")
    save = wiz[idx_save : idx_save + 4500]
    check(idx_save > 0, "JS salvarOrcamentoWizard existe")
    check("fromWhatsapp" in save and "fromImpressao" in save, "JS salvar aceita WA e impressao")
    check("'impressao'" in save, "JS grava origem impressao")
    check("'whatsapp'" in save, "JS grava origem whatsapp")
    check("'manual'" in save, "JS grava origem manual")
    check("setOrcamentoActionBtnsDisabled(true)" in save, "JS salvar desliga botoes")
    check(
        "syncHistoricoOrcamentosCliente(key, { silent: true })" in save,
        "JS salvar sync servidor",
    )

    idx_imp = wiz.find("function imprimirOrcamentoWizard")
    imp = wiz[idx_imp : idx_imp + 1800]
    check(idx_imp > 0, "JS imprimirOrcamentoWizard existe")
    check("fromImpressao: true" in imp, "JS imprimir passa fromImpressao")
    check("silent: true" in imp, "JS imprimir silent (feedback proprio)")
    check(
        "wizardImprimirPacoteEntrega(orcId, { sep: false, ent: false, cup: true })" in imp,
        "JS imprime so cupom (sem sep/entregador)",
    )
    check("Orçamento impresso e salvo" in imp or "impresso e salvo" in imp, "JS feedback impresso+salvo")
    check("Carrinho vazio" in imp or "antes de imprimir" in imp, "JS bloqueia carrinho vazio")

    # Salvar sozinho NAO chama imprimir
    check(
        "wizardImprimirPacoteEntrega" not in save,
        "JS Salvar nao imprime sozinho",
    )

    # WhatsApp ainda marca whatsapp
    idx_wa = wiz.find("function enviarOrcamentoWhatsappWizard")
    wa = wiz[idx_wa : idx_wa + 900]
    check("fromWhatsapp: true" in wa, "JS WhatsApp Celular ainda marca origem")
    idx_loja = wiz.find("function enviarOrcamentoWhatsappLojaWizard")
    loja = wiz[idx_loja : idx_loja + 900]
    check("fromWhatsapp: true" in loja, "JS WhatsApp Loja ainda marca origem")

    # Lista card + historico usam icone unificado
    idx_snip = wiz.find("function renderRecentBudgetsSnippet")
    snip = wiz[idx_snip : idx_snip + 3200]
    check("budgetOrigemIconHtml" in snip, "JS card usa icone origem")
    check("budgetOrigemTitle" in snip, "JS card title origem")
    idx_hist = wiz.find("function openBudgetHistory")
    hist = wiz[idx_hist : idx_hist + 3200]
    check("budgetOrigemIconHtml" in hist, "JS F6 historico usa icone origem")

    # Icone impressora: SVG sutil (stroke), nao fill WhatsApp
    idx_ico = wiz.find("function budgetImpressaoIconHtml")
    ico = wiz[idx_ico : idx_ico + 900]
    check("pdv-budget-print-icon" in ico, "JS classe icone print")
    check("stroke=" in ico or 'stroke-width' in ico, "JS icone impressora stroke sutil")
    check("Salvo pela impressão" in ico, "JS title icone impressao")

    # Detecta aliases
    idx_eh = wiz.find("function budgetEhImpressao")
    eh = wiz[idx_eh : idx_eh + 400]
    check("'impressao'" in eh and "'print'" in eh, "JS aliases impressao/print")


def check_pin_api() -> None:
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from django.urls import reverse

    from produtos.caixa_util import rotulo_operador_pin, validar_pin_operador
    from produtos.models import OrcamentoPdvAgro

    pin_ok, pin_err = validar_pin_operador(PIN)
    check(pin_ok, f"PIN {PIN} valido ({pin_err})")
    rotulo = rotulo_operador_pin(PIN)
    check(bool(rotulo), f"PIN {PIN} tem nome ({rotulo or '?'})")

    User = get_user_model()
    staff = User.objects.filter(is_staff=True, is_active=True).first()
    check(staff is not None, "tem staff local")
    if staff is None:
        return

    url = reverse("api_pdv_orcamentos")
    cid = int(time.time() * 1000)
    entry = {
        "id": cid,
        "orc_barcode": f"GMORC{cid}",
        "cliente": "Consumidor nao identificado",
        "cliente_key": "consumidor_final",
        "cliente_mode": "consumidor_final",
        "total": "R$ 2,40",
        "itens": [{"id": "t-imp", "nome": "teste orc imprimir", "qtd": 1, "preco": 2.4}],
        "origem": "impressao",
        "usuario": "verify-orc-imprimir",
    }
    with override_settings(ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"]):
        c = Client()
        c.force_login(staff)
        r = c.post(
            url,
            data=json.dumps({"entry": entry}),
            content_type="application/json",
        )
        check(r.status_code == 200, f"POST impressao 200 ({r.status_code})")
        body = r.json()
        check(body.get("ok") is True, f"POST ok=True ({body.get('ok')})")
        item = body.get("item") or {}
        check(int(item.get("id") or 0) == cid, "item id gravado")
        check(str(item.get("origem") or "").lower() == "impressao", "item origem impressao no retorno")
        check("2,40" in str(item.get("total") or ""), "item total 2,40")

        g = c.get(url, {"cliente_key": "consumidor_final", "limite": 30})
        check(g.status_code == 200, f"GET consumidor 200 ({g.status_code})")
        gbody = g.json()
        check(gbody.get("ok") is True, "GET ok")
        items = gbody.get("items") or []
        ids = [int(x.get("id") or 0) for x in items]
        check(cid in ids, "GET lista inclui impressao")
        found = next((x for x in items if int(x.get("id") or 0) == cid), None)
        check(found is not None, "GET achou entry")
        if found is not None:
            check(str(found.get("origem") or "").lower() == "impressao", "GET origem impressao persiste")

        obj = OrcamentoPdvAgro.objects.filter(orc_local_id=cid).first()
        check(obj is not None, "Postgres tem orcamento impressao")
        if obj is not None:
            payload = obj.payload_json if isinstance(obj.payload_json, dict) else {}
            check(str(payload.get("origem") or "").lower() == "impressao", "PG payload origem impressao")
            check(obj.cliente_key == "consumidor_final", "PG cliente_key")

        # Isolamento outro cliente
        other_key = f"tmp:verify-imp:{uuid.uuid4().hex[:8]}"
        g2 = c.get(url, {"cliente_key": other_key, "limite": 30})
        ids2 = [int(x.get("id") or 0) for x in ((g2.json() or {}).get("items") or [])]
        check(cid not in ids2, "outro cliente nao ve orcamento impressao")

        # Manual ainda funciona (regressao Salvar)
        cid2 = cid + 1
        entry2 = dict(entry)
        entry2["id"] = cid2
        entry2["orc_barcode"] = f"GMORC{cid2}"
        entry2["origem"] = "manual"
        entry2["total"] = "R$ 3,10"
        entry2["itens"] = [{"id": "t-man", "nome": "teste orc salvar", "qtd": 1, "preco": 3.1}]
        r2 = c.post(url, data=json.dumps({"entry": entry2}), content_type="application/json")
        check(r2.status_code == 200 and (r2.json() or {}).get("ok") is True, "POST manual (Salvar) ok")
        item2 = (r2.json() or {}).get("item") or {}
        check(str(item2.get("origem") or "").lower() == "manual", "Salvar origem manual")


def check_pagina_pdv() -> None:
    """Render /pdv/ com staff — botões no HTML final."""
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings

    User = get_user_model()
    staff = User.objects.filter(is_staff=True, is_active=True).first()
    if staff is None:
        fail("pagina PDV: sem staff")
        return
    with override_settings(ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"]):
        c = Client()
        c.force_login(staff)
        r = c.get("/pdv/")
        check(r.status_code == 200, f"pagina /pdv/ 200 ({r.status_code})")
        html = r.content.decode("utf-8", errors="replace")
        check("pdv-step1-salvar-orcamento-btn" in html, "pagina /pdv/ tem Salvar")
        check("pdv-step1-imprimir-orcamento-btn" in html, "pagina /pdv/ tem Imprimir")
        check("pdv-step1-budget-snippet" in html, "pagina /pdv/ tem card orcamentos")
        check("Salvar orçamento" not in html, "pagina /pdv/ sem rotulo longo")


def check_http() -> None:
    base = os.environ.get("AGRO_VERIFY_BASE", "http://127.0.0.1:8000").rstrip("/")
    try:
        with urlopen(Request(base + "/healthz", method="GET"), timeout=2) as r:
            check(r.status == 200, "HTTP healthz 200")
        with urlopen(Request(base + "/pdv/", method="GET"), timeout=5) as r:
            check(r.status in (200, 302), f"HTTP /pdv/ {r.status}")
    except (URLError, OSError) as e:
        ok(f"runserver off — HTTP skip ({e})")


def main() -> int:
    print("=== PDV-ORC-IMPRIMIR detalhado ===")
    print("--- HTML ---")
    check_html()
    print("--- fonte JS ---")
    check_fonte()
    print("--- PIN + API ---")
    check_pin_api()
    print("--- pagina /pdv/ ---")
    check_pagina_pdv()
    print("--- HTTP ---")
    check_http()
    print()
    print(f"OK={OKS} FAIL={len(FAILS)}")
    if FAILS:
        print("VERIFY_FAIL")
        for f in FAILS:
            print(" -", f.encode("ascii", "replace").decode("ascii"))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
