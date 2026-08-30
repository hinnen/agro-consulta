# -*- coding: utf-8 -*-
"""VERIFY PDV Enter / cupom — bug loja #9 (Enter = sem impressão) + overlay Vendas.

Cobre:
  · Enter (qualquer forma) → sem impressão (tryConfirmSale(false))
  · F9 → com impressão
  · Botão «sem impressão» continua false
  · Print ANTES de resetWizard (3 paths: finalize + 2× MP)
  · Foco pós-quitado no botão SEM impressão
  · Overlay Vendas: classe agro-vendas-in-overlay + CSS 100%
  · Cupom 80mm serializa venda Dinheiro (Postgres se disponível)

Uso: python scripts/verify_pdv_cupom_dinheiro_path.py
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails = 0
oks = 0

JS = "produtos/static/produtos/js/pdv_wizard.js"
HTML = "produtos/templates/produtos/vendas_lista.html"


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    global fails
    fails += 1
    print(f" FAIL {msg}")


def read(rel: str) -> str:
    p = ROOT / rel
    if not p.is_file():
        fail(f"ausente {rel}")
        return ""
    return p.read_text(encoding="utf-8")


def _fn_body(src: str, name: str) -> str:
    """Extrai corpo aproximado de function name(...) { ... } (nível 1)."""
    m = re.search(rf"function\s+{re.escape(name)}\s*\([^)]*\)\s*\{{", src)
    if not m:
        return ""
    i = m.end() - 1
    depth = 0
    for j in range(i, len(src)):
        c = src[j]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return src[i : j + 1]
    return ""


def check_contracts() -> None:
    print("\n[1] Contratos JS / HTML")
    js = read(JS)
    html = read(HTML)
    if not js or not html:
        return

    # Bug #9: Enter NÃO usa mais pagamentoSoDinheiro para imprimir
    if "tryConfirmSale(pagamentoSoDinheiro(st))" in js:
        fail("js:Enter ainda força cupom no dinheiro (pagamentoSoDinheiro)")
    else:
        ok("js:Enter_nao_forca_cupom_dinheiro")

    if "Enter = sempre sem impressão" in js or "tryConfirmSale(false)" in js:
        ok("js:Enter_sem_impressao")
    else:
        fail("js:Enter_sem_impressao")

    # Rótulo fixo: Enter no SEM, F9 no COM
    if (
        "Confirmar sem impressão" in js
        and ">Enter</kbd>" in js
        and "Confirmar com impressão" in js
        and ">F9</kbd>" in js
    ):
        ok("js:rotulo_Enter_no_sem_F9_no_com")
    else:
        fail("js:rotulo_atalhos")

    if "refreshConfirmSaleLabels" in js:
        fail("js:refreshConfirmSaleLabels_ainda_existe")
    else:
        ok("js:sem_rotulo_dinamico_dinheiro")

    body_after = _fn_body(js, "afterCommitTrancheFlow")
    if "confirmSalePrint.focus()" in body_after:
        fail("js:foco_ainda_no_COM_impressao")
    elif "pdv-confirm-sale-no-print" in body_after and ".focus()" in body_after:
        ok("js:foco_pos_quitado_no_SEM")
    else:
        fail("js:foco_pos_quitado")

    # Botão explícito sem impressão = false
    if "dom.confirmSaleNoPrint.addEventListener" in js and "tryConfirmSale(false)" in js:
        ok("js:botao_sem_impressao_continua_false")
    else:
        fail("js:botao_sem_impressao")

    # F9 ainda true
    if re.search(
        r"event\.code\s*===\s*['\"]F9['\"][\s\S]{0,200}tryConfirmSale\(true\)",
        js,
    ):
        ok("js:F9_ainda_com_impressao")
    else:
        fail("js:F9")

    if "Cupom ANTES do modal" in js:
        ok("js:Cupom ANTES do modal")
    else:
        fail("js:Cupom ANTES do modal")

    # Overlay vendas
    for needle in (
        "agro-vendas-in-overlay",
        "agro_pdv_overlay",
        "min-height: 100% !important",
        "max-height: 100% !important",
    ):
        if needle in html:
            ok(f"html:{needle[:40]}")
        else:
            fail(f"html falta:{needle}")


def check_print_before_reset() -> None:
    print("\n[2] Ordem print antes do reset (3 paths)")
    js = read(JS)
    if not js:
        return

    for name in (
        "finalizeConfirmedSale",
        "confirmSaleFinalizarMpPointOrders",
        "confirmSaleMercadoPagoPointProsseguir",
    ):
        body = _fn_body(js, name)
        if not body:
            # fallback: trecho após nome
            idx = js.find(f"function {name}")
            if idx < 0:
                fail(f"{name}:não achada")
                continue
            body = js[idx : idx + 12000]
        i_print = body.find("imprimirCupomAposVenda")
        i_reset = body.find("resetWizardParaNovaVenda")
        if i_print < 0 or i_reset < 0:
            fail(f"{name}:print/reset não achados ({i_print},{i_reset})")
            continue
        if i_print < i_reset:
            ok(f"{name}:print_antes_reset")
        else:
            fail(f"{name}:reset_antes_print (print@{i_print} reset@{i_reset})")

    bad = re.findall(
        r"resetWizardParaNovaVenda\(\);\s*\n\s*invalidateEntregasPendentesCache\(\);\s*\n\s*"
        r"refreshEntregasPendentesUi\([^)]+\);\s*\n\s*return imprimirCupomAposVenda",
        js,
    )
    if not bad:
        ok("sem_padrao_legado_reset_antes_print")
    else:
        fail(f"padrao_legado_ainda_presente x{len(bad)}")


def check_cupom_runtime() -> None:
    print("\n[4] Cupom 80mm venda Dinheiro (runtime)")
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    try:
        import django

        django.setup()
    except Exception as exc:
        fail(f"django.setup:{exc}")
        return

    from produtos.models import VendaAgro
    from produtos.venda_cupom_util import serializar_venda_cupom_80mm

    v = (
        VendaAgro.objects.filter(forma_pagamento__icontains="Dinheiro")
        .prefetch_related("itens")
        .order_by("-id")
        .first()
    )
    if not v:
        fail("nenhuma venda Dinheiro no PG (pule se DB vazio)")
        return
    if not v.itens.exists():
        v = None
        for cand in (
            VendaAgro.objects.filter(forma_pagamento__icontains="Dinheiro")
            .prefetch_related("itens")
            .order_by("-id")[:30]
        ):
            if cand.itens.exists():
                v = cand
                break
        if not v:
            fail("vendas Dinheiro sem itens")
            return
    cupom = serializar_venda_cupom_80mm(v, segunda_via=False)
    if cupom.get("itens"):
        ok(f"cupom_serializa venda#{v.pk} n={len(cupom['itens'])}")
    else:
        fail(f"cupom sem itens venda#{v.pk}")
    forma = str(cupom.get("forma_pagamento") or "")
    if re.search(r"dinheiro", forma, re.I) or "Dinheiro" in (v.forma_pagamento or ""):
        ok(f"cupom_forma:{forma[:40] or v.forma_pagamento}")
    else:
        fail(f"cupom forma inesperada:{forma!r}")


def check_overlay_script() -> None:
    print("\n[5] Script overlay liga classe no query")
    html = read(HTML)
    if not html:
        return
    if "classList.add('agro-vendas-in-overlay')" in html or 'classList.add("agro-vendas-in-overlay")' in html:
        ok("overlay_script_add_class")
    else:
        fail("overlay_script_add_class")
    if "agro_inapp_embed" in html and "agro_pdv_overlay" in html:
        ok("overlay_detecta_ambos_params")
    else:
        fail("overlay_params")


def main() -> int:
    print("VERIFY PDV ENTER SEM IMPRESSÃO PATH (bug loja #9)")
    check_contracts()
    check_print_before_reset()
    check_overlay_script()
    check_cupom_runtime()
    print(f"\nRESULTADO: {oks} OK · {fails} FAIL")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
