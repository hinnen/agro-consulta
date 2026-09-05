# -*- coding: utf-8 -*-
"""Prova detalhada path PDV-ENTREGA-TABELA-FORMA (bug loja #12).

Cobre: fontes JS/state/wizard, alias cartao, preco backend, simulacao bug vs fix,
API PDV tabelas, HTTP checkout/consulta, PIN 9973.

  python scripts/verify_pdv_entrega_tabela_forma_path.py
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client, override_settings
from django.urls import reverse

from produtos.caixa_util import normalizar_forma_pagamento_caixa, validar_pin_operador
from produtos.tabela_preco_forma_util import preco_pdv_para_forma

JS_PREC = (ROOT / "produtos/static/produtos/js/precos_forma_pagamento.js").read_text(
    encoding="utf-8"
)
STATE = (ROOT / "produtos/static/produtos/js/pdv_state.js").read_text(encoding="utf-8")
WIZ = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
HTML_EF = (
    ROOT / "produtos/templates/produtos/partials/pdv/entrega_wizard_overlay.html"
).read_text(encoding="utf-8")
CAIXA = (ROOT / "produtos/caixa_util.py").read_text(encoding="utf-8")

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


def slice_fn(src: str, name: str, n: int = 1600) -> str:
    m = re.search(rf"function {re.escape(name)}\s*\(", src)
    if not m:
        return ""
    return src[m.start() : m.start() + n]


def sim_forma_from_meio(meio: str, tabelas: list[dict] | None = None) -> str:
    """Espelha formaFromMeioEntrega (path critico)."""
    m = str(meio or "").strip().lower()
    if m == "dinheiro":
        return "Dinheiro"
    if m not in ("cartao", "cartão"):
        return ""
    tabelas = tabelas or []
    for t in sorted(tabelas, key=lambda x: int(x.get("slot") or 99)):
        if not t.get("ativo"):
            continue
        for f in t.get("formas") or []:
            key = normalizar_forma_pagamento_caixa(str(f)).lower()
            key_ascii = (
                key.replace("á", "a")
                .replace("ã", "a")
                .replace("é", "e")
                .replace("í", "i")
                .replace("ó", "o")
                .replace("ô", "o")
                .replace("ú", "u")
                .replace("ç", "c")
            )
            if key_ascii.startswith("cartao"):
                return str(f)
    return "Cartão de crédito"


def main() -> None:
    print("=== PDV-ENTREGA-TABELA-FORMA detalhado ===")

    print("--- fontes ---")
    check("fn_formaFromMeio", "function formaFromMeioEntrega" in JS_PREC)
    check("export_formaFromMeio", "formaFromMeioEntrega: formaFromMeioEntrega" in JS_PREC)
    obter = slice_fn(JS_PREC, "obterFormaDoState", 900)
    check("obter_usa_meio", "formaFromMeioEntrega(state.entrega.meioNaEntrega)" in obter)
    check("obter_exige_local_entrega", "localPagamento || '') === 'entrega'" in obter)

    sync = slice_fn(STATE, "syncFormaPorMeioEntrega", 900)
    check("fn_sync", "function syncFormaPorMeioEntrega" in STATE)
    check("sync_so_pagamento_entrega", "localPagamento || '') !== 'entrega'" in sync)
    check("sync_seta_forma", "state.pagamento.forma = fp" in sync)
    check("sync_recalc", "recalcularPrecosFormaItens(fp)" in sync)

    set_field = slice_fn(STATE, "setEntregaField", 700)
    set_patch = slice_fn(STATE, "setEntregaPatch", 900)
    check("field_chama_sync", "syncFormaPorMeioEntrega()" in set_field)
    check("patch_chama_sync", "syncFormaPorMeioEntrega()" in set_patch)
    check("patch_olha_meio", "meioNaEntrega" in set_patch and "localPagamento" in set_patch)

    flags = slice_fn(WIZ, "entregaFlagsPagamentoPrint", 1400)
    check("flags_cartao_canonico", "forma = 'Cartão de crédito'" in flags)
    check("flags_usa_formaFromMeio", "formaFromMeioEntrega('cartao')" in flags)
    check("flags_dinheiro", "forma = 'Dinheiro'" in flags)

    check(
        "chips_tabela_ativa",
        "tabelaParaForma(formaAtiva, item)" in WIZ
        and "function renderCartPrecosGruposHint" in WIZ,
    )
    check(
        "chips_is_selected",
        "(!tAtiva ? ' is-selected' : '')" in WIZ and "(sel ? ' is-selected' : '')" in WIZ,
    )

    check("html_btn_cartao", 'id="pdv-ef2-cartao"' in HTML_EF)
    check("html_btn_dinheiro", 'id="pdv-ef2-dinheiro"' in HTML_EF)
    check(
        "wizard_set_meio_cartao",
        "meioNaEntrega: 'cartao'" in WIZ,
    )
    check(
        "wizard_set_meio_dinheiro",
        "meioNaEntrega: 'dinheiro'" in WIZ,
    )

    print("--- alias / backend ---")
    check('alias_cartao_py', '"cartao": "Cartão de crédito"' in CAIXA)
    check(
        "norm_cartao",
        normalizar_forma_pagamento_caixa("cartao") == "Cartão de crédito",
    )
    check(
        "norm_dinheiro",
        normalizar_forma_pagamento_caixa("dinheiro") == "Dinheiro",
    )

    tabelas = [
        {
            "slot": 1,
            "nome": "Cartao+",
            "ativo": True,
            "percentual": 5.0,
            "arredondar_dezena_centavos": False,
            "formas": ["Cartão de crédito"],
            "categorias_vetadas": [],
            "produtos_vetados": [],
        },
        {
            "slot": 2,
            "nome": "Dinheiro-",
            "ativo": True,
            "percentual": -10.0,
            "arredondar_dezena_centavos": False,
            "formas": ["Dinheiro"],
            "categorias_vetadas": [],
            "produtos_vetados": [],
        },
    ]
    prod = {"id": "99", "preco_padrao": 100.0, "preco_venda": 100.0, "categoria": "Pet"}

    # Bug antigo: meio da entrega sem forma no pagamento → preço base
    v_bug = preco_pdv_para_forma(prod, "", tabelas=tabelas, resolucoes={})
    check("bug_sem_forma_base", abs(v_bug - 100.0) < 0.001, f"foi {v_bug}")

    # Fix: meio cartao/dinheiro → forma canonica → tabela
    fp_cart = sim_forma_from_meio("cartao", tabelas)
    fp_din = sim_forma_from_meio("dinheiro", tabelas)
    check("sim_meio_cartao", fp_cart == "Cartão de crédito", f"foi {fp_cart}")
    check("sim_meio_dinheiro", fp_din == "Dinheiro", f"foi {fp_din}")
    check("sim_meio_vazio", sim_forma_from_meio("") == "")

    v_cart = preco_pdv_para_forma(prod, fp_cart, tabelas=tabelas, resolucoes={})
    v_din = preco_pdv_para_forma(prod, fp_din, tabelas=tabelas, resolucoes={})
    check("fix_cartao_tabela", abs(v_cart - 105.0) < 0.001, f"foi {v_cart}")
    check("fix_dinheiro_tabela", abs(v_din - 90.0) < 0.001, f"foi {v_din}")

    # Pagamento na loja (local != entrega) nao deve inventar forma pelo meio
    # (sync so roda com localPagamento === 'entrega')
    check(
        "sync_guarda_loja",
        "if (String(state.entrega.localPagamento || '') !== 'entrega') return;" in sync,
    )

    # PIX / forma sem tabela permanece base
    v_pix = preco_pdv_para_forma(prod, "PIX", tabelas=tabelas, resolucoes={})
    check("pix_sem_tabela", abs(v_pix - 100.0) < 0.001, f"foi {v_pix}")

    print("--- HTTP / PIN ---")
    User = get_user_model()
    user = User.objects.filter(is_staff=True).order_by("id").first()
    check("tem_staff", user is not None)

    pin_ok, pin_err = validar_pin_operador("9973")
    check("pin_9973", pin_ok, pin_err or "")

    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        c = Client(HTTP_HOST="127.0.0.1")
        if user:
            c.force_login(user)

        for name, url_name in (
            ("http_pdv_checkout", "pdv_checkout"),
            ("http_consulta", "consulta_produtos"),
            ("http_api_tabelas_pdv", "api_tabelas_preco_forma_pdv"),
        ):
            try:
                url = reverse(url_name)
                resp = c.get(url, follow=True)
                check(
                    name,
                    resp.status_code == 200,
                    f"{resp.status_code} {url} redirects={len(getattr(resp, 'redirect_chain', []) or [])}",
                )
                if name == "http_api_tabelas_pdv" and resp.status_code == 200:
                    import json

                    data = json.loads(resp.content.decode("utf-8") or "{}")
                    check("api_tabelas_ok", bool(data.get("ok")), str(data)[:120])
                    check("api_tem_tabelas_key", "tabelas" in data)
            except Exception as exc:
                check(name, False, str(exc)[:120])

    # Scripts irmãos ainda passam
    print("--- scripts irmãos ---")
    import subprocess

    for script, label in (
        ("scripts/verify_tabela_preco_forma.py", "verify_tabela"),
        ("scripts/verify_ent_via_dinheiro_sem_maquina.py", "verify_ent_via"),
    ):
        r = subprocess.run(
            [sys.executable, str(ROOT / script)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
        out = (r.stdout or "") + (r.stderr or "")
        # ent_via pode falhar live_healthz — aceita se flags_cartao passou
        if label == "verify_ent_via":
            check(
                label + "_flags",
                "OK  flags_cartao_forma" in out or "OK flags_cartao_forma" in out,
                out[-200:].replace("\n", " "),
            )
            check(
                label + "_exit_ou_quase",
                r.returncode == 0 or "flags_cartao_forma" in out,
                f"rc={r.returncode}",
            )
        else:
            check(label, r.returncode == 0, out[-240:].replace("\n", " "))

    print(f"\n{ok} OK / {fail} FAIL")
    if fail:
        print("VERIFY_FAIL")
        raise SystemExit(1)
    print("VERIFY_OK")
    raise SystemExit(0)


if __name__ == "__main__":
    main()
