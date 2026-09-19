# -*- coding: utf-8 -*-
"""Prova detalhada: via do entregador (dinheiro sem troco vs maquina).

Roda a funcao REAL extraida de pdv_wizard.js no Node + espelho do painel +
Django (PIN 9973) + HTTP local se o runserver estiver no ar.

  python scripts/verify_ent_via_dinheiro_sem_maquina.py
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

JS_PATH = ROOT / "produtos/static/produtos/js/pdv_wizard.js"
HTML_PATH = ROOT / "produtos/templates/produtos/entregas_painel.html"

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


def extract_fn(src: str, name: str) -> str:
    m = re.search(rf"function {re.escape(name)}\s*\(", src)
    if not m:
        return ""
    i = m.start()
    brace = src.find("{", i)
    if brace < 0:
        return ""
    depth = 0
    for j in range(brace, len(src)):
        ch = src[j]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return src[i : j + 1]
    return ""


def node_faixa(e: dict) -> str:
    js = JS_PATH.read_text(encoding="utf-8")
    parts = [
        "function escapeHtml(value) { return String(value == null ? '' : value); }",
        "function formatMoney(value) { return 'R$ ' + Number(value || 0).toFixed(2); }",
        extract_fn(js, "linhaObsMaquininhaEntrega"),
        extract_fn(js, "wizardPrintParseMoneyBr"),
        extract_fn(js, "wizardPrintTrocoLevarValor"),
        extract_fn(js, "wizardPrintHtmlFaixaPagamentoEntregador"),
    ]
    if any(not p for p in parts):
        return "__EXTRACT_FAIL__"
    harness = (
        "\n".join(parts)
        + "\nconst e = "
        + json.dumps(e, ensure_ascii=False)
        + ";\nprocess.stdout.write(wizardPrintHtmlFaixaPagamentoEntregador(e));\n"
    )
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as tmp:
        tmp.write(harness)
        path = tmp.name
    try:
        r = subprocess.run(
            ["node", path],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
            check=False,
        )
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
    if r.returncode != 0:
        return "__NODE_FAIL__:" + (r.stderr or r.stdout or "")[:200]
    return r.stdout or ""


def faixa_label(html: str) -> str:
    if "LEVAR MÁQUINA" in html or "LEVAR MAQUINA" in html:
        return "MAQUINA"
    if "LEVAR TROCO" in html:
        return "TROCO"
    if "COBRAR DINHEIRO" in html:
        return "DINHEIRO"
    if ">PAGO<" in html or ">PAGO</div>" in html:
        return "PAGO"
    if "PAGO" in html and "N" in html:
        return "PAGO"
    return "VAZIO"


def main() -> None:
    print("=== ENT-VIA-DIN-SEM-MAQ detalhado ===")
    js = JS_PATH.read_text(encoding="utf-8")
    html = HTML_PATH.read_text(encoding="utf-8")

    check("fn_linha_obs", "function linhaObsMaquininhaEntrega" in js)
    check("fn_faixa_pdv", "function wizardPrintHtmlFaixaPagamentoEntregador" in js)
    check("fn_faixa_painel", "function htmlFaixaPagamentoEntregador" in html)
    check("dinheiro_vence_cartao_js", "if (dinheiro) cartao = false;" in js)
    check("dinheiro_vence_cartao_html", "if (dinheiro) cartao = false;" in html)
    check("strip_maq_nao_js", "maquininha\\s*:\\s*n[aã]o" in js or r"maquininha\s*:\s*n[aã]o" in js)
    check("cobrar_dinheiro_js", "COBRAR DINHEIRO" in js)
    check("cobrar_dinheiro_html", "COBRAR DINHEIRO" in html)
    check("obs_nao_concat_antiga", "maquininha ? 'Maquininha:" not in js)
    check("flags_dinheiro_forma", "forma = 'Dinheiro'" in extract_fn(js, "entregaFlagsPagamentoPrint"))
    check(
        "flags_cartao_forma",
        "forma = 'Cartão de crédito'" in extract_fn(js, "entregaFlagsPagamentoPrint")
        or "forma = 'Cartao de credito'" in extract_fn(js, "entregaFlagsPagamentoPrint"),
    )
    check("set_maq_nao_no_troco", "setEntregaField('maquininha', 'nao')" in js)
    check("build_usa_linha_obs", "linhaObsMaquininhaEntrega(state.entrega.maquininha)" in js)
    check("print_usa_linha_obs", "linhaObsMaquininhaEntrega(e.maquininha)" in js)

    # linhaObs
    linha = extract_fn(js, "linhaObsMaquininhaEntrega")
    check("linha_vazio_nao", "s === 'nao'" in linha)
    check("linha_retorna_sim", "return 'Maquininha: ' + raw;" in linha)

    casos = [
        (
            "bug_maq_nao_dinheiro",
            {
                "forma_pagamento": "Dinheiro",
                "observacoes": "Maquininha: nao | troco: nao",
                "troco_precisa": False,
                "aguarda_pagamento_pdv": True,
                "pagamento_pdv": {"pago": False},
            },
            "DINHEIRO",
        ),
        (
            "bug_maq_nao_acento",
            {
                "forma_pagamento": "Dinheiro",
                "observacoes": "Maquininha: não",
                "troco_precisa": False,
                "aguarda_pagamento_pdv": True,
                "pagamento_pdv": {"pago": False},
            },
            "DINHEIRO",
        ),
        (
            "dinheiro_limpo_sem_troco",
            {
                "forma_pagamento": "Dinheiro",
                "observacoes": "",
                "troco_precisa": False,
                "aguarda_pagamento_pdv": True,
                "pagamento_pdv": {"pago": False},
            },
            "DINHEIRO",
        ),
        (
            "dinheiro_com_troco",
            {
                "forma_pagamento": "Dinheiro",
                "observacoes": "Troco: 50,00",
                "troco_precisa": True,
                "troco_paga_com": 50,
                "total_texto": "30,00",
                "aguarda_pagamento_pdv": True,
                "pagamento_pdv": {"pago": False},
            },
            "TROCO",
        ),
        (
            "cartao_entrega",
            {
                "forma_pagamento": "Cartão",
                "observacoes": "Maquininha: sim",
                "troco_precisa": False,
                "aguarda_pagamento_pdv": True,
                "pagamento_pdv": {"pago": False},
            },
            "MAQUINA",
        ),
        (
            "pago_loja",
            {
                "forma_pagamento": "Pago na loja",
                "observacoes": "",
                "troco_precisa": False,
                "aguarda_pagamento_pdv": False,
                "pagamento_pdv": {"pago": True, "label": "Pago na loja"},
            },
            "PAGO",
        ),
        (
            "pago_vence_obs_maquina",
            {
                "forma_pagamento": "Pago na loja",
                "observacoes": "Maquininha: sim",
                "troco_precisa": True,
                "aguarda_pagamento_pdv": False,
                "pagamento_pdv": {"pago": True, "label": "Pago na loja"},
            },
            "PAGO",
        ),
        (
            "dinheiro_ganha_de_obs_maquina",
            {
                "forma_pagamento": "Dinheiro",
                "observacoes": "Maquininha: sim",
                "troco_precisa": False,
                "aguarda_pagamento_pdv": True,
                "pagamento_pdv": {"pago": False},
            },
            "DINHEIRO",
        ),
    ]

    for name, payload, esperado in casos:
        html_out = node_faixa(payload)
        check("node_extract_" + name, not html_out.startswith("__"), html_out[:80])
        got = faixa_label(html_out)
        check("faixa_" + name, got == esperado, "got=%s html=%s" % (got, html_out[:90].replace("\n", " ")))
        if esperado == "DINHEIRO":
            check("nao_maquina_" + name, "LEVAR MÁQUINA" not in html_out and "LEVAR MAQUINA" not in html_out)
        if esperado == "MAQUINA":
            check("tem_maquina_" + name, "LEVAR MÁQUINA" in html_out)

    # Node: linhaObs
    linha_h = (
        extract_fn(js, "linhaObsMaquininhaEntrega")
        + "\nprocess.stdout.write(JSON.stringify(["
        + "linhaObsMaquininhaEntrega('nao'),"
        + "linhaObsMaquininhaEntrega('não'),"
        + "linhaObsMaquininhaEntrega(''),"
        + "linhaObsMaquininhaEntrega('sim')"
        + "]));\n"
    )
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as tmp:
        tmp.write(linha_h)
        p = tmp.name
    r = subprocess.run(
        ["node", p],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
        check=False,
    )
    try:
        os.unlink(p)
    except OSError:
        pass
    vals = []
    try:
        vals = json.loads(r.stdout or "[]")
    except Exception:
        vals = []
    check("obs_nao_vazio", vals[:3] == ["", "", ""] if len(vals) >= 3 else False, str(vals))
    check("obs_sim_texto", len(vals) == 4 and vals[3] == "Maquininha: sim", str(vals))

    # Django + PIN
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from django.urls import reverse
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
            pdv = c.get(reverse("pdv_home"), follow=True)
            ent = c.get("/entregas/", follow=True)
        bpdv = pdv.content.decode("utf-8", "replace")
        bent = ent.content.decode("utf-8", "replace")
        check("http_pdv_200", pdv.status_code == 200, str(pdv.status_code))
        check("http_entregas_200", ent.status_code == 200, str(ent.status_code))
        check("http_pdv_js", "pdv_wizard.js" in bpdv)
        check("http_painel_faixa", "htmlFaixaPagamentoEntregador" in bent or "COBRAR DINHEIRO" in bent)
    else:
        check("http_user", False, "sem usuario Django")

    # runserver vivo
    try:
        with urllib.request.urlopen("http://127.0.0.1:8000/healthz", timeout=4) as resp:
            hz = resp.status
    except Exception as exc:
        hz = 0
        check("live_healthz", False, str(exc)[:80])
    else:
        check("live_healthz", hz == 200, str(hz))
        try:
            with urllib.request.urlopen(
                "http://127.0.0.1:8000/static/produtos/js/pdv_wizard.js", timeout=8
            ) as resp:
                live_js = resp.read().decode("utf-8", "replace")
            check("live_js_cobrar", "COBRAR DINHEIRO" in live_js)
            check("live_js_vence", "if (dinheiro) cartao = false;" in live_js)
            check("live_js_linha", "function linhaObsMaquininhaEntrega" in live_js)
        except Exception as exc:
            check("live_js", False, str(exc)[:80])

    print("")
    print("%s OK / %s FAIL" % (ok, fail))
    raise SystemExit(1 if fail else 0)


if __name__ == "__main__":
    main()
