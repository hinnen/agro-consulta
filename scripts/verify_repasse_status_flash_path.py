#!/usr/bin/env python
"""Prova detalhada — status central Repasse PDV (REPASSE-STATUS-FLASH).

Contratos: markup · CSS · JS · PIN 9973 · HTTP local se up · Django check.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

fails: list[str] = []
oks = 0
PIN = "9973"


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"OK {msg}")


def fail(msg: str) -> None:
    fails.append(msg)
    print(f"FAIL {msg}")


def must(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def main() -> int:
    overlay = (
        ROOT / "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html"
    ).read_text(encoding="utf-8", errors="replace")
    js = (ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js").read_text(
        encoding="utf-8", errors="replace"
    )

    print("== Markup / CSS ==")
    must('id="pdv-rp-status-flash"' in overlay, "flash #pdv-rp-status-flash")
    must('id="pdv-rp-status-flash-panel"' in overlay, "panel flash")
    must('id="pdv-rp-status-flash-title"' in overlay, "title flash")
    must('id="pdv-rp-status-flash-msg"' in overlay, "msg flash")
    must('id="pdv-rp-status-flash-ok"' in overlay, "botao Entendi flash")
    must("rp-status-pulse" in overlay, "animacao pulse")
    must("#pdv-repasse-overlay #pdv-rp-status-flash" in overlay, "z-index flash")
    must("z-index: 100" in overlay, "flash z-index 100")
    must("min(42rem" in overlay and ".rp-aviso-panel" in overlay, "aviso panel >= 42rem")
    must(
        "#pdv-repasse-overlay #pdv-rp-aviso-msg" in overlay
        and "clamp(1.05rem" in overlay,
        "aviso msg tipografia grande",
    )
    must(
        'id="pdv-rp-status"' in overlay and "border-4 border-rose-600" in overlay,
        "faixa status reforçada",
    )
    must(
        'id="pdv-rp-status"' in overlay and "hidden" in overlay.split('id="pdv-rp-status"')[1][:80],
        "faixa status inicia hidden",
    )

    print("== JS contratos ==")
    must("function setStatus(" in js, "setStatus()")
    must("function paintStatusLine(" in js, "paintStatusLine()")
    must("function hideStatusFlash(" in js, "hideStatusFlash()")
    must("function requestCloseOverlay(" in js, "requestCloseOverlay()")
    must("setStatus('Transferindo…', 'busy')" in js, "Transferindo → busy")
    must("setStatus('Salvando…', 'busy')" in js, "Salvando → busy")
    must("setStatus(okMsg, 'ok')" in js, "sucesso → ok flash")
    must("showNestedPopup(avisoModal)" in js, "aviso nested (não stack)")
    must("hideNestedPopup(avisoModal)" in js, "fecha aviso nested")
    must("showNestedPopup(statusFlash)" in js, "abre flash nested")
    must("transferência em andamento" in js, "bloqueio fechar busy")
    must(
        "dom.fechar.addEventListener('click', requestCloseOverlay)" in js,
        "× usa requestCloseOverlay",
    )
    must(
        "dom.cancelar.addEventListener('click', requestCloseOverlay)" in js,
        "Cancelar usa requestCloseOverlay",
    )
    must("statusFlashOk.addEventListener" in js, "Entendi do flash ligado")
    must("openAvisoModal(" in js and "Confira os 3 valores" in js, "validação → aviso modal")
    must("openAvisoModal('Informe ao menos um valor" in js, "zero total → aviso modal")
    # Não pode voltar a escrever status só na linha pequena no fluxo de confirmação
    bad_transfer = re.search(
        r"dom\.status\.textContent\s*=\s*['\"]Transferindo",
        js,
    )
    must(not bad_transfer, "não usa só linha pequena em Transferindo")
    # closeOverlay limpa flash
    must(
        "hideStatusFlash()" in js
        and "function closeOverlay" in js
        and js.index("function closeOverlay") < js.index("hideStatusFlash()", js.index("function closeOverlay")),
        "closeOverlay limpa flash",
    )

    print("== PIN 9973 ==")
    from produtos.caixa_util import operador_label_de_pin

    ok_lab, label, err_lab = operador_label_de_pin(PIN)
    must(ok_lab and bool(label), f"operador_label_de_pin → {label!r}")
    if err_lab:
        print(f"   detail: {err_lab}")
    bad_ok, _bad_label, _bad_err = operador_label_de_pin("0000")
    must(not bad_ok, "PIN 0000 rejeitado")

    print("== Django check ==")
    r = subprocess.run(
        [sys.executable, "manage.py", "check"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    must(r.returncode == 0, f"manage.py check (rc={r.returncode})")
    if r.returncode != 0:
        print((r.stdout or "")[-400])
        print((r.stderr or "")[-400])

    print("== node --check JS ==")
    nr = subprocess.run(
        ["node", "--check", str(ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js")],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    must(nr.returncode == 0, f"node --check pdv_repasse_vila.js (rc={nr.returncode})")
    if nr.returncode != 0:
        print((nr.stderr or "")[-300])

    print("== HTTP local (Client HTTP_HOST=127.0.0.1) ==")
    from django.contrib.auth import get_user_model
    from django.test import Client

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).order_by("id").first()
    if not user:
        user = User.objects.filter(is_staff=True).order_by("id").first()
    c = Client(HTTP_HOST="127.0.0.1")
    if user:
        c.force_login(user)
        ok(f"login force {user.username}")
    else:
        fail("sem user staff/superuser para Client")
        print(f"\n{oks} OK · {len(fails)} FAIL")
        for f in fails:
            print(f"  - {f}")
        print("VERIFY_REPASSE_STATUS_FLASH_PATH_FAIL")
        return 1

    # Página que embute o overlay (wizard PDV)
    page_ok = False
    for path in ("/pdv/", "/consulta/", "/pdv/checkout/"):
        try:
            resp = c.get(path)
            if resp.status_code != 200:
                continue
            body = resp.content.decode("utf-8", errors="replace")
            if "pdv-repasse-overlay" in body or "pdv-rp-status-flash" in body:
                must("pdv-rp-status-flash" in body, f"GET {path} contém flash")
                must("pdv-rp-aviso-modal" in body, f"GET {path} contém aviso modal")
                page_ok = True
                break
        except Exception as e:
            fail(f"GET {path} exceção: {e}")
    if not page_ok:
        # Fallback: lê template parcial direto (já coberto no markup) + tenta reverse
        from django.template.loader import render_to_string

        try:
            html = render_to_string(
                "produtos/partials/pdv/repasse_vila_overlay.html", {}
            )
            must("pdv-rp-status-flash" in html, "render_to_string overlay tem flash")
            must("pdv-rp-aviso-modal" in html, "render_to_string overlay tem aviso")
            page_ok = True
        except Exception as e:
            fail(f"render_to_string overlay: {e}")

    try:
        meta = c.get("/api/repasse-vila/meta/")
        must(meta.status_code == 200, f"GET meta status={meta.status_code}")
        if meta.status_code == 200:
            j = meta.json()
            must(bool(j.get("ok")), f"meta ok={j.get('ok')}")
    except Exception as e:
        fail(f"GET meta exceção: {e}")

    # Confirmar com valores zerados deve falhar com mensagem (não crash) — sem gravar
    try:
        conf = c.post(
            "/api/repasse-vila/confirmar/",
            data=(
                '{"pin":"%s","quem_levou":"VERIFY-STATUS-FLASH","percentual_lucro":0,'
                '"incluir_cmv":true,"incluir_lucro":true,"incluir_fiado":true,'
                '"modo_dia_cheio":false,"forma_pagamento":"Dinheiro",'
                '"incluir_acumulado":false,"separar_reserva":false,'
                '"valor_cofre_salario":"0","valor_cofre_vila_elias":"0",'
                '"valor_manual":"0","forcar_manual_zerado":true}'
            )
            % PIN,
            content_type="application/json",
        )
        must(conf.status_code in (200, 400), f"POST confirmar zero status={conf.status_code}")
        b4 = {}
        try:
            b4 = conf.json()
        except Exception:
            fail(f"POST confirmar zero body não-JSON status={conf.status_code}")
        else:
            must(b4.get("ok") is False, f"confirmar zero → ok=False (erro={b4.get('erro')!r})")
            must(bool(b4.get("erro")), "confirmar zero traz mensagem erro")
            # Mensagem de negócio (caixa/valor) — frontend abre aviso modal grande
            err = str(b4.get("erro") or "")
            must(len(err) >= 8, f"erro legível ({err[:80]!r})")
    except Exception as e:
        fail(f"POST confirmar zero exceção: {e}")

    print(f"\n{oks} OK · {len(fails)} FAIL")
    for f in fails:
        print(f"  - {f}")
    if fails:
        print("VERIFY_REPASSE_STATUS_FLASH_PATH_FAIL")
        return 1
    print("VERIFY_REPASSE_STATUS_FLASH_PATH_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
