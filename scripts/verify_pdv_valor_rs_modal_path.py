# -*- coding: utf-8 -*-
"""
Prova detalhada — PDV-VALOR-RS-MODAL (v26.76).

Wizard `/pdv/checkout/`: após Enter/clique no produto → «Valor em R$?».
Enter vazio / Esc / Pular → qty normal. Valor → qty = valor÷preço.
Etiqueta balança / valor já na busca → não pergunta.

  .venv/bin/python scripts/verify_pdv_valor_rs_modal_path.py
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []
PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
UNIT = Decimal("9.40")


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_contratos() -> None:
    print("== Contratos ==")
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    help_html = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(
        encoding="utf-8"
    )
    prep = (ROOT / "docs/DEPLOY-PREP-CHECKLIST-0910f.md").read_text(encoding="utf-8")
    check("fn_ask", "function askValorReaisParaProduto" in wiz)
    check("fn_parse", "function parseValorReaisDigitado" in wiz)
    check("titulo_modal", "Valor em R$?" in wiz)
    check("enter_vazio_pula", "Enter vazio = lançar sem valor" in wiz)
    check("btn_pular", "data-pdv-valor-rs-skip" in wiz)
    check("btn_ok", "data-pdv-valor-rs-ok" in wiz)
    check("btn_close", "data-pdv-valor-rs-close" in wiz)
    check("input_valor", "data-pdv-valor-rs-input" in wiz)
    check("esc_fecha", "ev.key === 'Escape'" in wiz and "askValorReaisParaProduto" in wiz)
    check("enter_confirma", "ev.key === 'Enter'" in wiz and "confirmar" in wiz)
    check("click_backdrop", "ev.target === host" in wiz and "askValorReaisParaProduto" in wiz)
    check("hook_explicit", "precisaPerguntarValor" in wiz and "skipValorPrompt" in wiz)
    check("hook_valor_asked", "valorTotalAsked" in wiz)
    check("pula_etiqueta", "valor_etiqueta_balanca" in wiz and "precisaPerguntarValor" in wiz)
    check("pula_se_ja_valor", "valorTotalOpt == null" in wiz and "precisaPerguntarValor" in wiz)
    check("explicit_only", "explicitPick &&" in wiz and "precisaPerguntarValor" in wiz)
    check("qty_calc_ainda", "function calcularQtdPorValorTotal" in wiz)
    check("qty_add_ainda", "function qtyParaAdicionarProduto" in wiz)
    check("msg_ok_valor", "Valor R$ ·" in wiz)
    check("help_tela", "Valor em R$?" in help_html)
    check("prep_doc", "pronto para envio à produção" in prep and "26.76" in prep)
    check("prep_inclui_etq", "ETQ-EAN-LOJA-DV" in prep and "CHECKLIST 09/10f" in prep)
    check(
        "version_26_76",
        (ROOT / "VERSION").read_text(encoding="utf-8").strip() == "26.76",
    )
    check(
        "prep_ancestral",
        subprocess.run(
            [
                "git",
                "merge-base",
                "--is-ancestor",
                "origin/producao",
                "origin/deploy/prep-checklist-0910f",
            ],
            cwd=ROOT,
            capture_output=True,
        ).returncode
        == 0,
    )
    mig = subprocess.run(
        [
            "git",
            "diff",
            "--name-only",
            "origin/producao...origin/deploy/prep-checklist-0910f",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).stdout
    check("prep_sem_migrate", "migration" not in mig.lower(), mig.strip()[:80] or "ok")


def test_math() -> None:
    print("== Matemática loja (R$ 10 ÷ 9,40) ==")
    q10 = (Decimal("10") / UNIT).quantize(Decimal("0.001"))
    check("qty_r10", q10 == Decimal("1.064"), str(q10))
    check("linha_r10", (UNIT * q10).quantize(Decimal("0.01")) == Decimal("10.00"))
    q105 = (Decimal("10.50") / UNIT).quantize(Decimal("0.001"))
    check("qty_r10_50", q105 == Decimal("1.117"), str(q105))


def test_parse_node() -> None:
    print("== Parse + qty JS (node) ==")
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    m = re.search(
        r"function parseValorReaisDigitado\(raw\) \{[\s\S]*?\n    \}",
        wiz,
    )
    calc = re.search(
        r"function calcularQtdPorValorTotal\(precoUnit, valorTotal\) \{[\s\S]*?\n    \}",
        wiz,
    )
    check("extract_parse", bool(m))
    check("extract_calc", bool(calc))
    if not m or not calc:
        return
    js = (
        m.group(0)
        + "\n"
        + calc.group(0)
        + """
const assert = (c, msg) => { if (!c) { console.error('FAIL', msg); process.exit(2); } };
assert(parseValorReaisDigitado('') === null, 'vazio');
assert(parseValorReaisDigitado('  ') === null, 'espaco');
assert(parseValorReaisDigitado('10') === 10, '10');
assert(parseValorReaisDigitado('10,50') === 10.5, '10,50');
assert(parseValorReaisDigitado('R$10') === 10, 'R$10');
assert(parseValorReaisDigitado('r$ 10,50') === 10.5, 'r$10,50');
assert(parseValorReaisDigitado('0') === null, 'zero');
assert(parseValorReaisDigitado('-1') === null, 'neg');
assert(calcularQtdPorValorTotal(9.4, 10) === 1.064, 'qty10');
assert(calcularQtdPorValorTotal(9.4, 10.5) === 1.117, 'qty105');
assert(calcularQtdPorValorTotal(0, 10) === null, 'preco0');
assert(calcularQtdPorValorTotal(9.4, null) === null, 'valorNull');
console.log('OK js_valor_rs_modal');
"""
    )
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(js)
        path = f.name
    r = subprocess.run(["node", path], capture_output=True, text=True)
    check(
        "js_node",
        r.returncode == 0 and "OK js_valor_rs_modal" in r.stdout,
        (r.stderr or r.stdout)[:160],
    )


def test_pin_9973() -> None:
    print("== PIN 9973 ==")
    import django

    django.setup()
    from base.models import PerfilUsuario
    from django.contrib.auth import get_user_model
    from produtos.caixa_util import validar_pin_operador

    User = get_user_model()
    check("pin_env", PIN == "9973", PIN)
    user, _ = User.objects.get_or_create(
        username="pin9973_prova",
        defaults={
            "is_staff": True,
            "is_superuser": True,
            "is_active": True,
            "first_name": "Prova",
        },
    )
    user.set_password("prova-local-9973")
    user.is_staff = True
    user.is_superuser = True
    user.is_active = True
    user.save()
    perfil = PerfilUsuario.objects.filter(senha_rapida=PIN).first()
    created = False
    if not perfil:
        perfil = PerfilUsuario.objects.filter(user=user).first()
    if not perfil:
        cod = "9973"
        while PerfilUsuario.objects.filter(codigo_vendedor=cod).exists():
            cod = f"9{str(PerfilUsuario.objects.count()).zfill(3)}"[:4]
        perfil = PerfilUsuario.objects.create(
            user=user,
            codigo_vendedor=cod,
            senha_rapida=PIN,
            ativo=True,
            primeiro_acesso=False,
        )
        created = True
    else:
        perfil.senha_rapida = PIN
        perfil.ativo = True
        perfil.primeiro_acesso = False
        perfil.save()
    ok, msg = validar_pin_operador(PIN)
    check("pin_9973_vivo", ok, msg or ("created" if created else "ok"))


def test_client_wizard_js() -> None:
    print("== Client HTTP login + wizard static ==")
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings

    User = get_user_model()
    u = User.objects.filter(username="pin9973_prova").first()
    if not u:
        check("client_user", False, "sem user")
        return
    with override_settings(ALLOWED_HOSTS=["*", "127.0.0.1", "testserver", "localhost"]):
        c = Client()
        logged = c.login(username="pin9973_prova", password="prova-local-9973")
        check("client_login", logged)
        if not logged:
            return
        r = c.get("/pdv/checkout/")
        check("client_checkout", r.status_code in (200, 302), str(r.status_code))
        js = c.get("/static/produtos/js/pdv_wizard.js")
        if hasattr(js, "streaming_content"):
            body = b"".join(js.streaming_content)
        else:
            body = js.content
        txt = body.decode("utf-8", errors="replace")
        check("static_js_200", js.status_code == 200, str(js.status_code))
        check("static_has_ask", "function askValorReaisParaProduto" in txt)
        check("static_has_parse", "function parseValorReaisDigitado" in txt)
        check("static_has_hook", "precisaPerguntarValor" in txt)


def main() -> int:
    print("PDV-VALOR-RS-MODAL — prova detalhada\n")
    test_contratos()
    test_math()
    test_parse_node()
    test_pin_9973()
    test_client_wizard_js()
    print(f"\nOK={len(oks)} FAIL={len(fails)}")
    print("PREP_FAILS=" + str(len(fails)))
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
