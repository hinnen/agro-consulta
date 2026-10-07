# -*- coding: utf-8 -*-
"""Prova detalhada path BUG31-CB-TABELA (bug loja #31).

Cashback/Vale nao mandam tabela se houver forma de mercadoria;
manda a de maior valor. So CB/Vale sozinho -> essa forma.

Cobre: backend, contratos JS/state/wizard, sim bug vs fix, grupos A/B,
tabela %, correcao no save, PIN 9973, HTTP PDV (se auth).

  python scripts/verify_bug31_cashback_tabela_forma.py
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client, override_settings

from produtos.caixa_util import validar_pin_operador
from produtos.precos_forma_pagamento_util import (
    corrigir_precos_itens_lista_sem_forma,
    forma_principal_para_preco,
    preco_venda_para_forma,
)
from produtos.tabela_preco_forma_util import preco_pdv_para_forma

JS_PREC = (ROOT / "produtos/static/produtos/js/precos_forma_pagamento.js").read_text(
    encoding="utf-8"
)
STATE = (ROOT / "produtos/static/produtos/js/pdv_state.js").read_text(encoding="utf-8")
WIZ = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
VIEWS = (ROOT / "produtos/views.py").read_text(encoding="utf-8")

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


def slice_fn(src: str, name: str, n: int = 1800) -> str:
    m = re.search(rf"function {re.escape(name)}\s*\(", src)
    if not m:
        return ""
    return src[m.start() : m.start() + n]


def sim_js_forma_principal(state: dict, forma_hint: str | None = None) -> str:
    """Espelha formaPrincipalParaPreco do JS (path critico)."""
    skip = {"Cashback", "Vale crédito"}
    totals: dict[str, float] = {}
    order: list[str] = []

    def add(forma: str, valor: float) -> None:
        f = str(forma or "").strip()
        if not f:
            return
        if f not in totals:
            totals[f] = 0.0
            order.append(f)
        totals[f] += float(valor or 0)

    pag = state.get("pagamento") or {}
    for L in pag.get("lancamentos") or []:
        if not isinstance(L, dict):
            continue
        add(str(L.get("forma") or ""), float(L.get("valor") or 0))
    cur = str(
        forma_hint
        if forma_hint is not None and str(forma_hint).strip()
        else pag.get("forma") or ""
    ).strip()
    if cur:
        cur_val = float(pag.get("valorDestaForma") or 0)
        if cur_val <= 0.0001:
            cur_val = float(pag.get("valorRecebido") or 0)
        add(cur, cur_val)
    if not order:
        return ""
    merc = [f for f in order if f not in skip]
    pool = merc or order
    best = pool[0]
    best_v = totals.get(best) or 0
    for f in pool[1:]:
        v = totals.get(f) or 0
        if v > best_v + 0.009:
            best, best_v = f, v
    return best


def main() -> int:
    print("=== BUG31-CB-TABELA path detalhado ===")

    print("--- fontes / contratos ---")
    check("fn_formaPrincipal", "function formaPrincipalParaPreco" in JS_PREC)
    check("export_formaPrincipal", "formaPrincipalParaPreco: formaPrincipalParaPreco" in JS_PREC)
    check("skip_cashback", "FORMAS_SKIP_PRECO" in JS_PREC and "Cashback" in JS_PREC)
    check("skip_vale", "Vale crédito" in JS_PREC)
    obter = slice_fn(JS_PREC, "obterFormaDoState", 700)
    check("obter_usa_principal", "formaPrincipalParaPreco(state)" in obter)
    check("fn_formaPrecoDoState", "function formaPrecoDoState" in STATE)
    recalc = slice_fn(STATE, "recalcularPrecosFormaItens", 500)
    check("recalc_usa_principal", "formaPrecoDoState(forma)" in recalc)
    sync = slice_fn(WIZ, "sincronizarPrecosFormaAntesGravar", 900)
    check("sync_usa_principal", "formaPrincipalParaPreco(state)" in sync)
    check("views_forma_principal", "forma_principal_para_preco" in VIEWS)
    check("views_corrigir", "corrigir_precos_itens_lista_sem_forma" in VIEWS)

    print("--- backend forma_principal ---")
    check(
        "mix_90_10",
        forma_principal_para_preco(
            "Cashback",
            [{"forma": "Dinheiro", "valor": 90}, {"forma": "Cashback", "valor": 10}],
        )
        == "Dinheiro",
    )
    check(
        "payload_erp_keys",
        forma_principal_para_preco(
            "Cashback + Dinheiro",
            [
                {"formaPagamento": "Cashback", "valorPagamento": 10},
                {"formaPagamento": "Dinheiro", "valorPagamento": 77},
            ],
        )
        == "Dinheiro",
    )
    check(
        "pix_vale",
        forma_principal_para_preco(
            "Vale crédito",
            [{"forma": "PIX", "valor": 200}, {"forma": "Vale crédito", "valor": 15}],
        )
        == "PIX",
    )
    check(
        "so_cashback",
        forma_principal_para_preco("Cashback", [{"forma": "Cashback", "valor": 50}])
        == "Cashback",
    )
    check(
        "so_cb_vale_maior",
        forma_principal_para_preco(
            "",
            [
                {"forma": "Cashback", "valor": 5},
                {"forma": "Vale crédito", "valor": 20},
            ],
        )
        == "Vale crédito",
    )
    check(
        "mercadoria_maior",
        forma_principal_para_preco(
            "",
            [
                {"forma": "Dinheiro", "valor": 40},
                {"forma": "Cartão de crédito", "valor": 60},
            ],
        )
        == "Cartão de crédito",
    )
    check(
        "rotulo_sem_lista",
        forma_principal_para_preco("Cashback + Dinheiro", None) == "Dinheiro",
    )
    check(
        "soma_mesma_forma",
        forma_principal_para_preco(
            "Cashback",
            [
                {"forma": "Dinheiro", "valor": 30},
                {"forma": "Dinheiro", "valor": 40},
                {"forma": "Cashback", "valor": 50},
            ],
        )
        == "Dinheiro",
        "30+40=70 > 50",
    )
    check(
        "cashback_maior_que_dinheiro_ainda_pula",
        forma_principal_para_preco(
            "Cashback",
            [
                {"forma": "Dinheiro", "valor": 10},
                {"forma": "Cashback", "valor": 90},
            ],
        )
        == "Dinheiro",
        "CB nunca manda se houver mercadoria",
    )
    check(
        "debito_mp_canonico",
        forma_principal_para_preco(
            "Cashback",
            [
                {"forma": "Cartão de débito Mercado Pago", "valor": 80},
                {"forma": "Cashback", "valor": 5},
            ],
        )
        in ("Cartão de débito", "Cartão de débito Mercado Pago"),
    )

    print("--- sim JS (espelho) ---")
    st_mix = {
        "pagamento": {
            "forma": "Cashback",
            "valorDestaForma": 10,
            "lancamentos": [{"forma": "Dinheiro", "valor": 90}],
        }
    }
    check("js_mix_hint_cashback", sim_js_forma_principal(st_mix, "Cashback") == "Dinheiro")
    check("js_mix_state", sim_js_forma_principal(st_mix) == "Dinheiro")
    st_so = {"pagamento": {"forma": "Cashback", "valorDestaForma": 50, "lancamentos": []}}
    check("js_so_cashback", sim_js_forma_principal(st_so, "Cashback") == "Cashback")
    st_hint = {
        "pagamento": {
            "forma": "Dinheiro",
            "valorDestaForma": 0,
            "lancamentos": [{"forma": "PIX", "valor": 100}],
        }
    }
    check(
        "js_hint_cashback_com_pix",
        sim_js_forma_principal(st_hint, "Cashback") == "PIX",
    )
    # bug antigo: usava so state.pagamento.forma
    bug_old = str(st_mix["pagamento"]["forma"])
    fix_new = sim_js_forma_principal(st_mix, "Cashback")
    check("bug_vs_fix", bug_old == "Cashback" and fix_new == "Dinheiro")

    print("--- preco grupos A/B (milho-like) ---")
    g = {
        "preco_a": 87.0,
        "preco_b": 92.0,
        "formas_a": ["Dinheiro", "PIX", "Cartão de débito"],
        "formas_b": [],
    }
    din = preco_venda_para_forma(92, None, "Dinheiro", precos_modo="grupos", precos_grupos=g)
    cb = preco_venda_para_forma(92, None, "Cashback", precos_modo="grupos", precos_grupos=g)
    cred = preco_venda_para_forma(
        92, None, "Cartão de crédito", precos_modo="grupos", precos_grupos=g
    )
    check("grupos_dinheiro_87", abs(din - 87.0) < 0.01, f"got {din}")
    check("grupos_cashback_cai_B", abs(cb - 92.0) < 0.01, f"got {cb}")
    check("grupos_credito_92", abs(cred - 92.0) < 0.01, f"got {cred}")
    forma_fix = forma_principal_para_preco(
        "Cashback",
        [{"forma": "Dinheiro", "valor": 174}, {"forma": "Cashback", "valor": 10}],
    )
    preco_fix = preco_venda_para_forma(
        92, None, forma_fix, precos_modo="grupos", precos_grupos=g
    )
    preco_bug = preco_venda_para_forma(
        92, None, "Cashback", precos_modo="grupos", precos_grupos=g
    )
    check("cenario_loja_fix_87", abs(preco_fix - 87.0) < 0.01 and forma_fix == "Dinheiro")
    check("cenario_loja_bug_92", abs(preco_bug - 92.0) < 0.01)

    print("--- tabela % global ---")
    produto = {
        "id": "p-bug31",
        "preco_padrao": 100.0,
        "preco_venda": 100.0,
        "precos_modo": "por_forma",
        "precos_por_forma": {},
    }
    tabelas = [
        {
            "slot": 1,
            "ativo": True,
            "nome": "A vista",
            "percentual": -5,
            "arredondar_dezena_centavos": False,
            "formas": ["Dinheiro", "PIX"],
            "categorias_vetadas": [],
            "produtos_vetados": [],
        },
        {
            "slot": 2,
            "ativo": True,
            "nome": "Credito",
            "percentual": 5,
            "arredondar_dezena_centavos": False,
            "formas": ["Cartão de crédito", "Cashback"],
            "categorias_vetadas": [],
            "produtos_vetados": [],
        },
    ]
    p_din = preco_pdv_para_forma(produto, "Dinheiro", tabelas=tabelas)
    p_cb = preco_pdv_para_forma(produto, "Cashback", tabelas=tabelas)
    forma_m = forma_principal_para_preco(
        "Cashback",
        [{"forma": "Dinheiro", "valor": 95}, {"forma": "Cashback", "valor": 5}],
    )
    p_mix = preco_pdv_para_forma(produto, forma_m, tabelas=tabelas)
    check("tab_dinheiro_95", abs(p_din - 95.0) < 0.01, f"got {p_din}")
    check("tab_cashback_105", abs(p_cb - 105.0) < 0.01, f"got {p_cb}")
    check("tab_mix_usa_avista", forma_m == "Dinheiro" and abs(p_mix - 95.0) < 0.01)

    print("--- correcao save (bug #24 path + #31) ---")
    ov = SimpleNamespace(
        produto_externo_id="pid-bug31",
        preco_venda=92.0,
        cadastro_extras={
            "precos_modo": "grupos",
            "precos_grupos": g,
            "precos_por_forma": {},
        },
    )
    itens = [{"id": "pid-bug31", "preco": 92.0}]
    forma_pre = forma_principal_para_preco(
        "Cashback",
        [{"forma": "Dinheiro", "valor": 164}, {"forma": "Cashback", "valor": 10}],
    )
    n = corrigir_precos_itens_lista_sem_forma(
        itens, forma_pre, overlays_by_pid={"pid-bug31": ov}
    )
    check("corrigiu_com_principal", n == 1 and abs(float(itens[0]["preco"]) - 87.0) < 0.01)

    print("--- PIN 9973 ---")
    pin_ok, pin_msg = validar_pin_operador("9973")
    check("pin_9973_valido", bool(pin_ok), str(pin_msg or ""))

    print("--- HTTP PDV (smoke) ---")
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.filter(
        is_staff=True
    ).first()
    if user is None:
        check("http_user", False, "sem usuario staff no PG local")
    else:
        c = Client()
        c.force_login(user)
        with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost"]):
            for path, label in (
                ("/pdv/", "pdv"),
                ("/pdv/checkout/", "checkout"),
                ("/consulta/", "consulta"),
            ):
                try:
                    r = c.get(path)
                    check(f"http_{label}", r.status_code in (200, 302), f"status={r.status_code}")
                except Exception as e:
                    check(f"http_{label}", False, type(e).__name__)

    print(f"\nResultado: {ok} ok, {fail} fail")
    if fail:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
