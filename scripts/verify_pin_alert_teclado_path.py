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

    check("__GM_SSPIN_ALERT_PATCH__" in sspin, "sspin patch alert")
    check("window.alert = function" in sspin, "sspin substitui alert")
    check("gmSspinAbrirSeErroPin" in sspin and "openLock(true)" in sspin, "AbrirSeErroPin abre lock")
    # Nao deve mais chamar GarantirOperador dentro de AbrirSeErroPin (fresco mentiroso)
    idx = sspin.find("window.gmSspinAbrirSeErroPin = function")
    chunk = sspin[idx : idx + 1200] if idx >= 0 else ""
    check("openLock(true)" in chunk, "AbrirSeErroPin força openLock")
    check("gmSspinGarantirOperador" not in chunk, "AbrirSeErroPin nao confia no fresco GET")

    check("if (!root) return;" in sspin, "sspin exige #sspin-root")
    # INIT so depois do root
    i_root = sspin.find("if (!root) return;")
    i_init = sspin.find("__GM_SSPIN_INIT__ = true")
    check(i_root >= 0 and i_init > i_root, "INIT so apos root existir")

    check("__GM_SSPIN_ALERT_BRIDGE__" in ext, "bridge alert no UI global")
    check("Identifique-se com o PIN" in ext, "bridge detecta msg")

    check("gmLancamentosTratarErroPin" in manual, "manual trata PIN")
    check("gmLancamentosComOperador" in manual, "manual pede operador")

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
    ):
        html = read(rel)
        check("_screensaver_pin.html" in html, f"{label} inclui sspin")


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
            ("gestao", "produtos_gestao"),
            ("consulta", "consulta_produtos"),
            ("fiado", "fiado_gestao"),
            ("vendas", "vendas_lista"),
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

        # com PIN
        r_pin = c.post(
            reverse("api_pdv_registrar_operador"),
            data=json.dumps({"pin": PIN_TESTE}),
            content_type="application/json",
        )
        check(r_pin.status_code == 200 and (r_pin.json() or {}).get("ok"), f"login PIN {PIN_TESTE}")
        r2 = c.post(url, data=json.dumps(body), content_type="application/json")
        j2 = r2.json() if r2.status_code else {}
        check(
            MSG_PIN_OPERADOR_OBRIGATORIO not in str(j2.get("erro") or ""),
            f"com PIN nao bloqueia descanso (status={r2.status_code})",
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
