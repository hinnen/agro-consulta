# -*- coding: utf-8 -*-
"""
PIN-ALERT-TECLADO — prova detalhada (global gestão/PDV/lançamentos).

Bug: alert nativo «Identifique-se com o PIN (modo descanso)» em várias telas.
Path: sspin força teclado (sem confiar em fresco) · patch alert · bridge UI · telas.
"""
from __future__ import annotations

import json
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
    sspin = read("produtos/templates/produtos/_screensaver_pin.html")
    ext = read("produtos/templates/produtos/_agro_open_external.html")
    manual = read("produtos/templates/produtos/lancamentos_manual.html")
    caixa = read("produtos/caixa_util.py")
    pin_inc = read("produtos/templates/produtos/includes/lancamentos_pin_entrada.html")

    msg = "Identifique-se com o PIN (modo descanso) antes de continuar."
    check(msg in caixa, "MSG canônica servidor")

    # Espelho das regex do bridge/sspin (o que o alert intercepta)
    import re

    def is_pin_msg(t: str) -> bool:
        if re.search(r"Identifique-se com o PIN", t, re.I):
            return True
        if re.search(r"modo descanso", t, re.I) and re.search(r"PIN", t, re.I):
            return True
        if re.search(r"Entre com o PIN no PDV", t, re.I):
            return True
        return False

    check(is_pin_msg(msg), "regex casa MSG canônica")
    check(is_pin_msg("Identifique-se com o PIN antes de continuar."), "regex casa variante curta")
    check(not is_pin_msg("Falha ao gravar lote."), "regex ignora erro comum")
    check(not is_pin_msg("PIN inválido."), "regex ignora PIN inválido puro")

    check("__GM_SSPIN_ALERT_PATCH__" in sspin, "sspin patch alert")
    check("window.alert = function" in sspin, "sspin substitui alert")
    check("gmSspinAbrirSeErroPin" in sspin and "openLock(true)" in sspin, "AbrirSeErroPin abre lock")
    idx = sspin.find("window.gmSspinAbrirSeErroPin = function")
    chunk = sspin[idx : idx + 1200] if idx >= 0 else ""
    check("openLock(true)" in chunk, "AbrirSeErroPin força openLock")
    check("gmSspinGarantirOperador" not in chunk, "AbrirSeErroPin nao confia no fresco GET")
    check("Digite seu PIN para gravar a ação" in chunk, "titulo/ação no teclado forçado")
    check("NÃO confiar" in sspin or "Nao confiar" in sspin or "não confiar" in sspin.lower(), "comentario anti-fresco")

    check("if (!root) return;" in sspin, "sspin exige #sspin-root")
    i_root = sspin.find("if (!root) return;")
    i_init = sspin.find("__GM_SSPIN_INIT__ = true")
    check(i_root >= 0 and i_init > i_root, "INIT so apos root existir")

    check("__GM_SSPIN_ALERT_BRIDGE__" in ext, "bridge alert no UI global")
    check("Identifique-se com o PIN" in ext, "bridge detecta msg")
    check("setInterval" in ext and "tryOpen" in ext, "bridge espera sspin carregar")

    check("gmLancamentosTratarErroPin" in manual, "manual trata PIN")
    check("gmLancamentosComOperador" in manual, "manual pede operador")
    check("gmLancamentosTratarErroPin" in pin_inc, "helper no include lancamentos")
    check("gmLancamentosComOperador" in pin_inc, "comOperador no include")

    for rel, label in (
        ("produtos/templates/produtos/fiado_gestao.html", "fiado"),
        ("produtos/templates/produtos/vendas_lista.html", "vendas"),
        ("produtos/templates/produtos/venda_agro_detalhe.html", "venda detalhe"),
        ("produtos/templates/produtos/historico_ajustes.html", "historico"),
        ("produtos/templates/produtos/clientes_lista.html", "clientes"),
        ("produtos/templates/produtos/caixa_painel.html", "caixa painel"),
        ("produtos/templates/produtos/produtos_gestao.html", "gestao"),
        ("produtos/templates/produtos/consulta_produtos.html", "consulta PDV"),
        ("produtos/templates/produtos/pdv_wizard.html", "wizard PDV"),
        ("produtos/templates/produtos/entrada_nota.html", "entrada NF"),
        ("produtos/templates/produtos/compras.html", "compras"),
        ("produtos/templates/produtos/lancamentos_contas_pagar_teste.html", "CP"),
        ("produtos/templates/produtos/lancamentos_financeiros.html", "CR"),
    ):
        html = read(rel)
        check("_screensaver_pin.html" in html or "lancamentos_pin_entrada.html" in html, f"{label} inclui sspin")


def check_runtime() -> None:
    print("--- runtime ---")
    import django

    django.setup()
    from django.conf import settings
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from django.urls import reverse

    from produtos.caixa_util import MSG_PIN_OPERADOR_OBRIGATORIO, rotulo_operador_pin

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
            ("manual", "lancamentos_manual"),
            ("cp", "lancamentos_contas_pagar"),
            ("cr", "lancamentos_contas_receber"),
            ("gestao", "produtos_gestao"),
            ("consulta", "consulta_produtos"),
            ("fiado", "fiado_gestao"),
            ("vendas", "vendas_lista"),
            ("entrada", "entrada_nota"),
            ("compras", "compras_view"),
            ("caixa", "caixa_painel"),
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
                ok(f"GET {label} redirect")
                continue
            check(r.status_code == 200, f"GET {label} ({r.status_code})")
            h = r.content.decode("utf-8", errors="replace")
            check("sspin-root" in h and "sspin-input" in h, f"{label} HTML teclado")
            check(
                "__GM_SSPIN_ALERT_PATCH__" in h or "__GM_SSPIN_ALERT_BRIDGE__" in h,
                f"{label} HTML patch/bridge alert",
            )
            check("gmSspinAbrirSeErroPin" in h, f"{label} HTML AbrirSeErroPin")
            # HTML vivo: AbrirSeErroPin nao deve chamar GarantirOperador no corpo da funcao
            i_fn = h.find("window.gmSspinAbrirSeErroPin = function")
            if i_fn >= 0:
                ch = h[i_fn : i_fn + 1100]
                check("openLock(true)" in ch, f"{label} HTML força openLock")
                check("gmSspinGarantirOperador" not in ch, f"{label} HTML sem fresco mentiroso")

        # API sem PIN ainda 403
        session = c.session
        for k in ("pdv_operador_nome", "pdv_operador_fresco_em", "pdv_caixa_gerido_operador"):
            session.pop(k, None)
        session.save()
        url = reverse("api_lancamentos_criar_manual_lote")
        body = {
            "tipo": "pagar",
            "data_competencia": "2026-09-08",
            "data_vencimento": "2026-09-08",
            "empresa_nome": "X",
            "pessoa_nome": "Y",
            "banco_nome": "Z",
            "linhas": [{"plano_conta": "__x__", "valor": "1,00"}],
        }
        r = c.post(url, data=json.dumps(body), content_type="application/json")
        j = r.json() if r.status_code else {}
        check(
            r.status_code == 403 and MSG_PIN_OPERADOR_OBRIGATORIO in str(j.get("erro") or ""),
            "API sem PIN → 403 MSG (UI deve virar teclado)",
        )

        # com PIN 9973
        r_pin = c.post(
            reverse("api_pdv_registrar_operador"),
            data=json.dumps({"pin": PIN_TESTE}),
            content_type="application/json",
        )
        j_pin = r_pin.json() if r_pin.status_code == 200 else {}
        check(r_pin.status_code == 200 and j_pin.get("ok"), f"login PIN {PIN_TESTE}")
        check(
            (j_pin.get("operador") or "").strip() == rot,
            f"login devolve operador {rot!r} (got {j_pin.get('operador')!r})",
        )
        r_get = c.get(reverse("api_pdv_registrar_operador"))
        j_get = r_get.json() if r_get.status_code == 200 else {}
        check(bool(j_get.get("fresco") and j_get.get("operador")), f"GET fresco apos PIN ({j_get.get('operador')!r})")

        r2 = c.post(url, data=json.dumps(body), content_type="application/json")
        j2 = r2.json() if r2.status_code else {}
        check(
            MSG_PIN_OPERADOR_OBRIGATORIO not in str(j2.get("erro") or ""),
            f"com PIN nao bloqueia descanso (status={r2.status_code})",
        )
        check(not (j2.get("ids") or []), "plano fake nao grava titulo")

        # outras APIs de escrita sem PIN de novo
        session = c.session
        for k in ("pdv_operador_nome", "pdv_operador_fresco_em", "pdv_caixa_gerido_operador"):
            session.pop(k, None)
        session.save()
        for label, uname, payload in (
            ("baixa", "api_lancamentos_baixa", {"ids": ["0" * 24], "tipo": "pagar", "data_movimento": "2026-09-08", "forma_pagamento": "Dinheiro", "banco": "Caixa"}),
            ("excluir", "api_lancamentos_excluir", {"id": "0" * 24}),
            ("alterar", "api_lancamentos_alterar", {"id": "0" * 24, "descricao": "x"}),
        ):
            rr = c.post(reverse(uname), data=json.dumps(payload), content_type="application/json")
            jj = rr.json() if rr.status_code else {}
            check(
                rr.status_code == 403 and MSG_PIN_OPERADOR_OBRIGATORIO in str(jj.get("erro") or ""),
                f"sem PIN {label} → 403 MSG",
            )


def main() -> int:
    print("=== PIN-ALERT-TECLADO path detalhado ===")
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
