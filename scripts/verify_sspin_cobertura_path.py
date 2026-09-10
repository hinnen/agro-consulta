# -*- coding: utf-8 -*-
"""
PIN-SSPIN-GLOBAL — cobertura: teclado em todas as telas de loja (não públicas).

Prova: base.html global · bridge sem alert nativo de PIN · amostra runtime com #sspin-root.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

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
    base = read("base/templates/base.html")
    ext = read("produtos/templates/produtos/_agro_open_external.html")
    sspin = read("produtos/templates/produtos/_screensaver_pin.html")

    check('include "produtos/_screensaver_pin.html"' in base, "base.html inclui sspin global")
    check("user.is_authenticated" in base, "sspin so autenticado")
    check("agro_sspin" in base, "block agro_sspin")

    # bridge: PIN nunca cai em nativeAlert no timeout
    i = ext.find("function agroPinAlertBridge")
    if i < 0:
        i = ext.find("agroPinAlertBridge")
    chunk = ext[i : i + 2500] if i >= 0 else ""
    check("agroPinAlertBridge" in ext, "bridge existe")
    check("n >= 150" in chunk, "bridge espera ~6s")
    check("if (n >= 50) nativeAlert" not in chunk, "sem fallback nativeAlert antigo")
    check("Nunca alert nativo" in chunk, "comentario anti-alert-nativo")
    # timeout do ramo PIN
    i_to = chunk.find("if (n >= 150)")
    to_chunk = chunk[i_to : i_to + 400] if i_to >= 0 else ""
    check("nativeAlert" not in to_chunk, "timeout PIN sem nativeAlert")

    # sspin patch
    i2 = sspin.find("__GM_SSPIN_ALERT_PATCH__")
    ch2 = sspin[i2 : i2 + 900] if i2 >= 0 else ""
    check("nunca alert nativo de PIN" in ch2, "sspin patch nao faz fallback nativo")

    # amostras standalone críticas
    for rel, label in (
        ("produtos/templates/produtos/caixa_abrir.html", "caixa abrir"),
        ("produtos/templates/produtos/caixa_saida.html", "caixa saida"),
        ("produtos/templates/produtos/promocoes_lista.html", "promocoes"),
        ("produtos/templates/produtos/pdv_checkout.html", "pdv checkout"),
        ("produtos/templates/produtos/resumo_financeiro_gerencial.html", "resumo gerencial"),
        ("rh/templates/rh/rh_hub.html", "RH hub"),
        ("rh/templates/rh/funcionario_ficha.html", "RH ficha"),
        ("produtos/templates/produtos/dashboard_gerencial.html", "dashboard"),
    ):
        t = read(rel)
        check(
            "_screensaver_pin.html" in t or "lancamentos_pin_entrada.html" in t or 'extends "layouts/base' in t or "extends 'layouts/base" in t or 'extends "base.html"' in t,
            f"{label} tem sspin ou herda base",
        )

    # publicas SEM sspin
    for rel, label in (
        ("produtos/templates/produtos/entrar.html", "login entrar"),
        ("produtos/templates/produtos/catalogo/catalogo_delivery.html", "catalogo delivery"),
    ):
        t = read(rel)
        check("_screensaver_pin.html" not in t, f"{label} sem sspin (publico)")


def check_runtime() -> None:
    print("--- runtime ---")
    import django

    django.setup()
    from django.conf import settings
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from django.urls import reverse

    from produtos.caixa_util import rotulo_operador_pin

    rot = (rotulo_operador_pin(PIN_TESTE) or "").strip()
    check(bool(rot), f"PIN {PIN_TESTE} = {rot!r}")

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    check(user is not None, "usuario Django")
    if not user:
        return

    hosts = list(getattr(settings, "ALLOWED_HOSTS", []) or []) + ["testserver"]
    with override_settings(ALLOWED_HOSTS=hosts):
        c = Client()
        c.force_login(user)

        pages = [
            ("home", "home"),
            ("cp", "lancamentos_contas_pagar"),
            ("emprestimos", "emprestimos_consulta"),
            ("repasse", "repasse_vila"),
            ("caixa_abrir", "caixa_abrir"),
            ("caixa_saida", "caixa_saida"),
            ("promocoes", "promocoes_lista"),
            ("resumo", "resumo_financeiro_gerencial"),
            ("rh", "rh_painel"),
            ("gestao", "produtos_gestao"),
            ("checkout", "pdv_checkout"),
            ("quem", "relatorios_quem_comprou"),
        ]
        for label, name in pages:
            try:
                url = reverse(name)
            except Exception as e:
                fail(f"reverse {name}: {e}")
                continue
            r = c.get(url)
            if r.status_code in (301, 302):
                ok(f"GET {label} redirect")
                continue
            check(r.status_code == 200, f"GET {label} ({r.status_code})")
            h = r.content.decode("utf-8", errors="replace")
            check("sspin-root" in h and "sspin-input" in h, f"{label} HTML teclado")
            check("gmSspinAbrirSeErroPin" in h, f"{label} AbrirSeErroPin")


def main() -> None:
    print("=== PIN-SSPIN-GLOBAL cobertura ===")
    check_static()
    check_runtime()
    print("---")
    if FAILS:
        print(f"VERIFY_FAIL {len(FAILS)} falhas / {OKS} oks")
        for f in FAILS:
            print(" -", f.encode("ascii", "replace").decode("ascii"))
        sys.exit(1)
    print(f"VERIFY_OK {OKS}/{OKS}")


if __name__ == "__main__":
    main()
