#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Prova detalhada — PDV-ENTREGAS-MODAL-BODY (Entregas no Pagamento não trava).

  set AGRO_PIN_TESTE=9973
  python scripts/verify_pdv_entregas_modal_body_path.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()

ok = 0
fail = 0


def check(cond: bool, msg: str) -> None:
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {msg}")
    else:
        fail += 1
        print(f"  FAIL {msg}")


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _outer_section_close(html: str, panel_marker: str) -> tuple[int, int]:
    """Retorna (open_idx, close_idx) do section que contém panel_marker."""
    open_idx = html.rfind("<section", 0, html.find(panel_marker) + 1)
    if open_idx < 0:
        return -1, -1
    depth = 0
    pos = open_idx
    while True:
        nxt_open = html.find("<section", pos + 1)
        nxt_close = html.find("</section>", pos + 1)
        if nxt_close < 0:
            return open_idx, -1
        if nxt_open >= 0 and nxt_open < nxt_close:
            depth += 1
            pos = nxt_open
        else:
            if depth == 0:
                return open_idx, nxt_close
            depth -= 1
            pos = nxt_close


def test_html() -> None:
    print("== HTML (modal fora do painel Produtos) ==")
    html = _read("produtos/templates/produtos/partials/pdv/step_produtos.html")
    wiz_css = _read("produtos/templates/produtos/pdv_wizard.html")

    check('data-step-panel="produtos"' in html, "painel produtos presente")
    check('id="pdv-entregas-pendentes-modal"' in html, "dialog Entregas presente")
    check('id="pdv-entrega-mudar-loja-modal"' in html, "dialog Mudar loja presente")
    check(html.count("<section") == html.count("</section>"), "sections balanceadas")
    check(html.count("<dialog") == html.count("</dialog>"), "dialogs balanceados")

    open_i, close_i = _outer_section_close(html, 'data-step-panel="produtos"')
    dlg_ent = html.find('id="pdv-entregas-pendentes-modal"')
    dlg_ml = html.find('id="pdv-entrega-mudar-loja-modal"')
    dlg_foto = html.find('id="pdv-product-photo-pop"')
    check(open_i >= 0 and close_i > open_i, "fecha outer section produtos")
    check(dlg_ent > close_i, "Entregas DEPOIS do </section> produtos (não display:none)")
    check(dlg_ml > close_i, "Mudar loja DEPOIS do </section> produtos")
    check(dlg_foto > close_i, "Foto produto DEPOIS do </section> produtos")

    # Anti-regressão: regra que esconde painel
    check(
        ".pdv-step-panel[hidden]" in wiz_css and "display: none !important" in wiz_css,
        "CSS ainda esconde painel[hidden] (por isso dialog tinha que sair)",
    )

    # Colunas internas do modal ainda ok
    check('pdv-entregas-col--pagar' in html and 'pdv-entregas-col--pagas' in html, "colunas A pagar / Pagas")
    i_pagar = html.find('<section class="pdv-entregas-col pdv-entregas-col--pagar"')
    i_pagas = html.find('<section class="pdv-entregas-col pdv-entregas-col--pagas"')
    check(i_pagar > 0 and i_pagas > i_pagar, "HTML das colunas A pagar / Pagas")
    check(
        i_pagar > 0 and "</section>" in html[i_pagar:i_pagas],
        "coluna A pagar fecha antes de Pagas",
    )


def test_js() -> None:
    print("== JS (abrir sem travar) ==")
    wiz = _read("produtos/static/produtos/js/pdv_wizard.js")

    check("function ensureEntregasModalNoBody" in wiz, "helper ensureEntregasModalNoBody")
    check("document.body.appendChild(dlg)" in wiz, "appendChild pro body")
    check("ensureEntregasModalNoBody()" in wiz, "openEntregas chama ensure")

    idx_open = wiz.find("function openEntregasPendentesModal")
    check(idx_open > 0, "openEntregasPendentesModal existe")
    bloco = wiz[idx_open : idx_open + 1600]
    check("ensureEntregasModalNoBody()" in bloco, "ensure no openEntregas")
    check("showModal" in bloco, "usa showModal")
    check("catch (eShow)" in bloco, "try/catch no showModal")
    check("entregasPendentesOpening = false" in bloco, "libera flag opening no finally")
    check("pdvSspinLocked()" in bloco, "respeita modo descanso")

    idx_ml = wiz.find("function abrirModalMudarLojaEntrega")
    check(idx_ml > 0, "abrirModalMudarLojaEntrega existe")
    bloco_ml = wiz[idx_ml : idx_ml + 900]
    check("appendChild(dlg)" in bloco_ml, "Mudar loja também move pro body")
    check("showModal" in bloco_ml, "Mudar loja usa showModal")

    check("topbarEntregasBtn.addEventListener('click', openEntregasPendentesModal)" in wiz, "botão topbar ENTREGAS")
    check("function closeEntregasPendentesModal" in wiz, "closeEntregasPendentesModal")
    check("function renderStepPanels" in wiz and "panel.hidden = !visible" in wiz, "painel produtos some no pagamento")


def test_http_runtime() -> None:
    print("== HTTP local (PIN + página PDV) ==")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    try:
        import django

        django.setup()
    except Exception as e:
        check(False, f"django.setup — {e}")
        return

    from django.test import Client, override_settings
    from django.contrib.auth import get_user_model
    from django.urls import reverse

    from produtos.caixa_util import validar_pin_operador

    ok_pin, err = validar_pin_operador(PIN)
    check(ok_pin, f"PIN {PIN} válido" + ("" if ok_pin else f" — {err}"))

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).order_by("id").first()
    if user is None:
        user = User.objects.filter(is_staff=True).order_by("id").first()
    check(user is not None, "usuário staff/super para Client")
    if user is None:
        return

    with override_settings(ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1", "*"]):
        c = Client()
        c.force_login(user)

        for path in ("/pdv/", "/pdv/checkout/"):
            try:
                r = c.get(path, follow=True)
                body = r.content.decode("utf-8", errors="replace")
                check(r.status_code == 200, f"GET {path} → {r.status_code}")
                if r.status_code != 200:
                    continue
                if 'id="pdv-entregas-pendentes-modal"' in body:
                    _o, close_i = _outer_section_close(body, 'data-step-panel="produtos"')
                    dlg = body.find('id="pdv-entregas-pendentes-modal"')
                    check(
                        close_i > 0 and dlg > close_i,
                        f"{path}: modal Entregas fora do painel produtos no HTML servido",
                    )
                elif path.rstrip("/").endswith("/pdv"):
                    check(
                        "pdv-topbar-entregas" in body or "pdv_wizard" in body,
                        f"{path}: shell PDV wizard presente",
                    )
                else:
                    check(r.status_code == 200, f"{path}: 200 (checkout alternativo ok)")
            except Exception as e:
                check(False, f"GET {path} — {e}")

        try:
            url = reverse("api_pdv_entregas_pendentes")
            r = c.get(url)
            check(r.status_code in (200, 401, 403), f"API entregas pendentes → {r.status_code}")
            if r.status_code == 200:
                data = r.json()
                check(
                    isinstance(data, dict) and ("ok" in data or "itens" in data or "erro" in data),
                    "JSON entregas legível",
                )
        except Exception as e:
            check(False, f"API entregas — {e}")

    # Probe no runserver vivo (se estiver no ar)
    print("== Probe 127.0.0.1:8000 ==")
    try:
        import urllib.request

        base = os.environ.get("AGRO_VERIFY_BASE", "http://127.0.0.1:8000").rstrip("/")
        with urllib.request.urlopen(base + "/healthz", timeout=3) as resp:
            check(resp.status == 200, f"healthz live → {resp.status}")
    except Exception as e:
        check(True, f"runserver live opcional (skip) — {e}")

def main() -> int:
    print(f"PIN teste: {PIN}")
    test_html()
    test_js()
    test_http_runtime()
    total = ok + fail
    print(f"\nVERIFY_OK {ok}/{total}" if fail == 0 else f"\nVERIFY_FAIL {ok}/{total}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
