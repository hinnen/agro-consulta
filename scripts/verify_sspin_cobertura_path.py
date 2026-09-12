# -*- coding: utf-8 -*-
"""
PIN-SSPIN-GLOBAL — prova detalhada (cobertura + runtime + PIN 9973).

Path: base.html global → #sspin-root em telas autenticadas → bridge/patch
nunca alert nativo de PIN → API 403 MSG → login PIN → fresco OK.
"""
from __future__ import annotations

import json
import os
import re
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
    caixa = read("produtos/caixa_util.py")
    ns_js = read("produtos/static/produtos/js/lancamento_nova_saida.js")

    msg = "Identifique-se com o PIN (modo descanso) antes de continuar."
    check(msg in caixa, "MSG canônica servidor")

    def is_pin_msg(t: str) -> bool:
        if re.search(r"Identifique-se com o PIN", t, re.I):
            return True
        if re.search(r"modo descanso", t, re.I) and re.search(r"PIN", t, re.I):
            return True
        if re.search(r"Entre com o PIN no PDV", t, re.I):
            return True
        return False

    check(is_pin_msg(msg), "regex casa MSG canônica")
    check(not is_pin_msg("Falha ao gravar lote."), "regex ignora erro comum")

    check('include "produtos/_screensaver_pin.html"' in base, "base.html inclui sspin global")
    check("user.is_authenticated" in base, "sspin so autenticado")
    check("agro_sspin" in base, "block agro_sspin")

    i = ext.find("agroPinAlertBridge")
    chunk = ext[i : i + 2500] if i >= 0 else ""
    check("agroPinAlertBridge" in ext, "bridge existe")
    check("n >= 150" in chunk, "bridge espera ~6s")
    check("if (n >= 50) nativeAlert" not in chunk, "sem fallback nativeAlert antigo")
    check("Nunca alert nativo" in chunk, "comentario anti-alert-nativo")
    i_to = chunk.find("if (n >= 150)")
    to_chunk = chunk[i_to : i_to + 400] if i_to >= 0 else ""
    check("nativeAlert" not in to_chunk, "timeout PIN sem nativeAlert")

    check("__GM_SSPIN_ALERT_PATCH__" in sspin, "sspin patch alert")
    check("gmSspinAbrirSeErroPin" in sspin and "openLock(true)" in sspin, "AbrirSeErroPin + openLock")
    idx = sspin.find("window.gmSspinAbrirSeErroPin = function")
    fn = sspin[idx : idx + 1200] if idx >= 0 else ""
    check("openLock(true)" in fn, "AbrirSeErroPin força openLock")
    check("gmSspinGarantirOperador" not in fn, "AbrirSeErroPin nao confia no fresco GET")
    i2 = sspin.find("__GM_SSPIN_ALERT_PATCH__")
    ch2 = sspin[i2 : i2 + 900] if i2 >= 0 else ""
    check("nunca alert nativo de PIN" in ch2, "sspin patch sem fallback nativo")
    check("dups.length > 1" in sspin or "querySelectorAll('#sspin-root')" in sspin, "sspin remove duplicata")

    check("function tratarErroPin" in ns_js, "nova saida tratarErroPin")
    check("comOperador(doPost)" in ns_js, "nova saida Finalizar via comOperador")

    for rel, label in (
        ("produtos/templates/produtos/caixa_abrir.html", "caixa abrir"),
        ("produtos/templates/produtos/caixa_saida.html", "caixa saida"),
        ("produtos/templates/produtos/caixa_relatorio.html", "caixa relatorio"),
        ("produtos/templates/produtos/promocoes_lista.html", "promocoes"),
        ("produtos/templates/produtos/pdv_checkout.html", "pdv checkout"),
        ("produtos/templates/produtos/resumo_financeiro_gerencial.html", "resumo gerencial"),
        ("produtos/templates/produtos/estoque_sincronizacao.html", "estoque sync"),
        ("produtos/templates/produtos/planos_conta_config.html", "planos conta"),
        ("produtos/templates/produtos/vendas_lojas_hub.html", "vendas lojas hub"),
        ("rh/templates/rh/rh_hub.html", "RH hub"),
        ("rh/templates/rh/funcionario_ficha.html", "RH ficha"),
        ("rh/templates/rh/fechamento_detalhe.html", "RH fechamento"),
        ("produtos/templates/produtos/dashboard_gerencial.html", "dashboard"),
        ("produtos/templates/produtos/lancamentos_contas_pagar_teste.html", "CP"),
        ("produtos/templates/produtos/lancamentos_financeiros.html", "CR"),
    ):
        t = read(rel)
        has = (
            "_screensaver_pin.html" in t
            or "lancamentos_pin_entrada.html" in t
            or 'extends "layouts/base' in t
            or "extends 'layouts/base" in t
            or 'extends "base.html"' in t
        )
        check(has, f"{label} tem sspin ou herda base")

    for rel, label in (
        ("produtos/templates/produtos/entrar.html", "login entrar"),
        ("produtos/templates/produtos/catalogo/catalogo_delivery.html", "catalogo delivery"),
        ("produtos/templates/produtos/ajuste_mobile_login.html", "ajuste mobile login"),
    ):
        t = read(rel)
        check("_screensaver_pin.html" not in t, f"{label} sem sspin (publico/login)")


def _assert_teclado(h: str, label: str) -> None:
    check("sspin-root" in h and "sspin-input" in h, f"{label} HTML teclado")
    check("gmSspinAbrirSeErroPin" in h, f"{label} AbrirSeErroPin")
    check("__GM_SSPIN_ALERT_PATCH__" in h or "__GM_SSPIN_ALERT_BRIDGE__" in h, f"{label} patch/bridge")
    i_fn = h.find("window.gmSspinAbrirSeErroPin = function")
    if i_fn >= 0:
        ch = h[i_fn : i_fn + 1100]
        check("openLock(true)" in ch, f"{label} força openLock")
        check("gmSspinGarantirOperador" not in ch, f"{label} sem fresco mentiroso")
    else:
        fail(f"{label} AbrirSeErroPin nao encontrado no HTML")


def check_runtime() -> None:
    print("--- runtime ---")
    import django

    django.setup()
    from django.conf import settings
    from django.contrib.auth import get_user_model
    from django.core.management import call_command
    from django.test import Client, override_settings
    from django.urls import reverse

    from produtos.caixa_util import MSG_PIN_OPERADOR_OBRIGATORIO, rotulo_operador_pin

    rot = (rotulo_operador_pin(PIN_TESTE) or "").strip()
    check(bool(rot), f"PIN {PIN_TESTE} = {rot!r}")
    check(rot.lower() == "renan", f"PIN {PIN_TESTE} e Renan")

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    check(user is not None, "usuario Django")
    if not user:
        return

    # django check (rápido)
    try:
        call_command("check", verbosity=0)
        ok("django check")
    except Exception as e:
        fail(f"django check: {e}")

    hosts = list(getattr(settings, "ALLOWED_HOSTS", []) or []) + ["testserver"]
    with override_settings(ALLOWED_HOSTS=hosts):
        # anônimo: página de login sem teclado (template já coberto no static)
        c_anon = Client()
        for path_try in ("/entrar/", "/accounts/login/", "/login/"):
            r_ent = c_anon.get(path_try, follow=False)
            if r_ent.status_code == 200:
                h_ent = r_ent.content.decode("utf-8", errors="replace")
                check("sspin-root" not in h_ent, f"login {path_try} anonimo sem sspin-root")
                break
        else:
            ok("login URL nao mapeada — static ja cobre entrar.html")

        c = Client()
        c.force_login(user)

        pages = [
            ("home", "home"),
            ("cp", "lancamentos_contas_pagar"),
            ("cr", "lancamentos_contas_receber"),
            ("manual", "lancamentos_manual"),
            ("emprestimos", "emprestimos_consulta"),
            ("repasse", "repasse_vila"),
            ("caixa_abrir", "caixa_abrir"),
            ("caixa_painel", "caixa_painel"),
            ("promocoes", "promocoes_lista"),
            ("resumo", "resumo_financeiro_gerencial"),
            ("rh", "rh_painel"),
            ("rh_funcs", "rh_funcionarios_lista"),
            ("gestao", "produtos_gestao"),
            ("cadastro", "produtos_cadastro_erp"),
            ("consulta", "consulta_produtos"),
            ("fiado", "fiado_gestao"),
            ("vendas", "vendas_lista"),
            ("entrada", "entrada_nota"),
            ("compras", "compras_view"),
            ("estoque_sync", "estoque_sincronizacao"),
            ("quem", "relatorios_quem_comprou"),
            ("hub_lojas", "vendas_lojas_hub"),
            ("clientes", "clientes_lista"),
            ("historico", "historico_ajustes"),
        ]
        for label, name in pages:
            try:
                url = reverse(name)
            except Exception as e:
                fail(f"reverse {name}: {e}")
                continue
            r = c.get(url)
            if r.status_code in (301, 302):
                # seguir 1 redirect (ex. caixa_saida / checkout)
                r2 = c.get(url, follow=True)
                if r2.status_code == 200 and "sspin-root" in r2.content.decode("utf-8", errors="replace"):
                    ok(f"GET {label} redirect→teclado")
                    _assert_teclado(r2.content.decode("utf-8", errors="replace"), f"{label}+")
                else:
                    ok(f"GET {label} redirect ({r.status_code})")
                continue
            check(r.status_code == 200, f"GET {label} ({r.status_code})")
            if r.status_code != 200:
                continue
            h = r.content.decode("utf-8", errors="replace")
            _assert_teclado(h, label)

        # home: nova saida modal presente
        r_home = c.get(reverse("home"))
        h_home = r_home.content.decode("utf-8", errors="replace")
        check("agro-nova-saida-overlay" in h_home, "home tem modal Nova saida")
        check("lancamento_nova_saida.js" in h_home, "home carrega JS nova saida")

        # API sem PIN → 403 MSG
        session = c.session
        for k in ("pdv_operador_nome", "pdv_operador_fresco_em", "pdv_caixa_gerido_operador"):
            session.pop(k, None)
        session.save()

        url_lote = reverse("api_lancamentos_criar_manual_lote")
        body = {
            "tipo": "pagar",
            "data_competencia": "2026-09-10",
            "data_vencimento": "2026-09-10",
            "empresa_nome": "X",
            "pessoa_nome": "Y",
            "banco_nome": "Z",
            "linhas": [{"plano_conta": "__x__", "valor": "1,00"}],
        }
        r = c.post(url_lote, data=json.dumps(body), content_type="application/json")
        j = r.json() if r.content else {}
        check(
            r.status_code == 403 and MSG_PIN_OPERADOR_OBRIGATORIO in str(j.get("erro") or ""),
            "API sem PIN → 403 MSG (UI deve virar teclado)",
        )
        check(is_pin_msg := ("Identifique-se com o PIN" in str(j.get("erro") or "")), "MSG detectavel pelo JS")

        # login PIN 9973
        r_pin = c.post(
            reverse("api_pdv_registrar_operador"),
            data=json.dumps({"pin": PIN_TESTE}),
            content_type="application/json",
        )
        j_pin = r_pin.json() if r_pin.status_code == 200 else {}
        check(r_pin.status_code == 200 and j_pin.get("ok"), f"login PIN {PIN_TESTE}")
        check(
            (j_pin.get("operador") or "").strip() == rot,
            f"login devolve operador {rot!r}",
        )

        # fresco via GET api_pdv_registrar_operador
        r_f = c.get(reverse("api_pdv_registrar_operador"))
        j_f = r_f.json() if r_f.status_code == 200 else {}
        check(
            r_f.status_code == 200 and bool(j_f.get("fresco") and j_f.get("operador")),
            f"GET fresco apos PIN ({j_f.get('operador')!r})",
        )

        # com PIN: nao bloqueia por descanso (pode 400 por plano fake)
        r2 = c.post(url_lote, data=json.dumps(body), content_type="application/json")
        j2 = r2.json() if r2.content else {}
        err2 = str(j2.get("erro") or "")
        check(
            r2.status_code != 403 and MSG_PIN_OPERADOR_OBRIGATORIO not in err2,
            f"com PIN nao bloqueia descanso (status={r2.status_code})",
        )

        # outras APIs sem PIN
        session = c.session
        for k in ("pdv_operador_nome", "pdv_operador_fresco_em", "pdv_caixa_gerido_operador"):
            session.pop(k, None)
        session.save()
        for label, name, payload in (
            ("baixa", "api_lancamentos_baixa", {"ids": ["0" * 24], "tipo": "pagar", "data_movimento": "2026-09-10", "forma_pagamento": "Dinheiro", "banco": "Caixa"}),
            ("excluir", "api_lancamentos_excluir", {"id": "0" * 24}),
            ("alterar", "api_lancamentos_alterar", {"id": "0" * 24, "descricao": "x"}),
        ):
            try:
                u = reverse(name)
            except Exception as e:
                fail(f"reverse {name}: {e}")
                continue
            rr = c.post(u, data=json.dumps(payload), content_type="application/json")
            jj = rr.json() if rr.content else {}
            check(
                rr.status_code == 403 and MSG_PIN_OPERADOR_OBRIGATORIO in str(jj.get("erro") or ""),
                f"sem PIN {label} → 403 MSG",
            )


def main() -> None:
    print("=== PIN-SSPIN-GLOBAL path detalhado ===")
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
