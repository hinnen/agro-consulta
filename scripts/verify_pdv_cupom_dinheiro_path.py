# -*- coding: utf-8 -*-
"""VERIFY PDV-CUPOM-DINHEIRO — bug loja #6 (venda dinheiro sem cupom + overlay Vendas branco).

Cobre:
  · Enter só-dinheiro → withPrint (igual F9)
  · PIX/cartão Enter → sem impressão
  · Botão «sem impressão» continua false
  · Print ANTES de resetWizard (3 paths: finalize + 2× MP)
  · Foco pós-dinheiro no botão COM impressão
  · Overlay Vendas: classe agro-vendas-in-overlay + CSS 100%
  · Lógica JS isolada (node) de pagamentoSoDinheiro
  · Cupom 80mm serializa venda Dinheiro (Postgres se disponível)

Uso: python scripts/verify_pdv_cupom_dinheiro_path.py
"""
from __future__ import annotations

import os
import re
import subprocess
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

    for needle in (
        "function pagamentoSoDinheiro",
        "function refreshConfirmSaleLabels",
        "/dinheiro/i.test",
        "tryConfirmSale(pagamentoSoDinheiro(st))",
        "Cupom ANTES do modal",
        "confirmSalePrint.focus()",
        "Dinheiro quitado: Enter = com cupom",
        "Enter fica no COM impressão",
    ):
        if needle in js:
            ok(f"js:{needle[:48]}")
        else:
            fail(f"js falta:{needle[:60]}")

    # Dinheiro: rótulo sem Enter no botão sem-impressão; Enter no COM
    if "Confirmar sem impressão</kbd>" not in js and 'n.innerHTML = \'Confirmar sem impressão\';' in js:
        ok("js:rotulo_dinheiro_sem_enter_no_botao_sem")
    elif "n.innerHTML = 'Confirmar sem impressão'" in js or 'n.innerHTML = "Confirmar sem impressão"' in js:
        ok("js:rotulo_dinheiro_sem_enter_no_botao_sem")
    else:
        fail("js:rotulo_dinheiro_sem_enter")

    if "refreshConfirmSaleLabels()" in js:
        ok("js:refreshConfirmSaleLabels_chamado")
    else:
        fail("js:refreshConfirmSaleLabels_nao_chamado")

    # Enter handler usa so-dinheiro; botão explícito sem impressão = false
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
    names = (
        "finalizeConfirmedSale",
        "confirmSaleFinalizarMpPointOrders",
        "confirmSaleMercadoPagoPointProsseguir",
    )
    # Em cada path, a 1ª ocorrência de imprimirCupomAposVenda deve vir ANTES
    # da 1ª resetWizardParaNovaVenda no mesmo bloco de finalização.
    for name in names:
        body = _fn_body(js, name)
        if not body:
            # MP paths são longas; buscar janela ao redor do nome
            idx = js.find(f"function {name}")
            if idx < 0:
                fail(f"fn ausente:{name}")
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

    # Contagem: não deve restar padrão antigo reset→print colado
    bad = re.findall(
        r"resetWizardParaNovaVenda\(\);\s*\n\s*invalidateEntregasPendentesCache\(\);\s*\n\s*"
        r"refreshEntregasPendentesUi\([^)]+\);\s*\n\s*return imprimirCupomAposVenda",
        js,
    )
    if not bad:
        ok("sem_padrao_legado_reset_antes_print")
    else:
        fail(f"padrao_legado_ainda_presente x{len(bad)}")


def check_node_logic() -> None:
    print("\n[3] Lógica pagamentoSoDinheiro (node)")
    script = r"""
function pagamentoSoDinheiro(state) {
  var arr = (state.pagamento && state.pagamento.lancamentos) || [];
  if (!arr.length) return false;
  return arr.every(function (L) {
    return /dinheiro/i.test(String((L && L.forma) || ''));
  });
}
function assert(name, cond) {
  if (!cond) { console.log('FAIL ' + name); process.exitCode = 1; }
  else console.log('OK ' + name);
}
assert('vazio', pagamentoSoDinheiro({pagamento:{lancamentos:[]}}) === false);
assert('so_dinheiro', pagamentoSoDinheiro({pagamento:{lancamentos:[{forma:'Dinheiro'}]}}) === true);
assert('dinheiro_x2', pagamentoSoDinheiro({pagamento:{lancamentos:[{forma:'Dinheiro'},{forma:'Dinheiro'}]}}) === true);
assert('pix', pagamentoSoDinheiro({pagamento:{lancamentos:[{forma:'PIX'}]}}) === false);
assert('misto', pagamentoSoDinheiro({pagamento:{lancamentos:[{forma:'Dinheiro'},{forma:'PIX'}]}}) === false);
assert('cartao', pagamentoSoDinheiro({pagamento:{lancamentos:[{forma:'Cartão de débito'}]}}) === false);
assert('case', pagamentoSoDinheiro({pagamento:{lancamentos:[{forma:'dinheiro'}]}}) === true);
"""
    tmp = ROOT / "scripts" / "_tmp_cupom_din_logic.js"
    try:
        tmp.write_text(script, encoding="utf-8")
        r = subprocess.run(
            ["node", str(tmp)],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
            timeout=15,
        )
        out = (r.stdout or "") + (r.stderr or "")
        for line in out.strip().splitlines():
            if line.startswith("OK "):
                ok(f"node:{line[3:]}")
            elif line.startswith("FAIL "):
                fail(f"node:{line[5:]}")
        if r.returncode not in (0, 1):
            fail(f"node exit {r.returncode}: {out[:200]}")
    except FileNotFoundError:
        fail("node não instalado")
    except Exception as exc:
        fail(f"node:{exc}")
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


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
        # tenta mais uma recente com itens
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


def check_overlay_class_script() -> None:
    print("\n[5] Script overlay liga classe no query")
    html = read(HTML)
    if "classList.add('agro-vendas-in-overlay')" in html or 'classList.add("agro-vendas-in-overlay")' in html:
        ok("overlay_script_add_class")
    else:
        fail("overlay_script_add_class")
    if "agro_inapp_embed" in html and "agro_pdv_overlay" in html:
        ok("overlay_detecta_ambos_params")
    else:
        fail("overlay_params")


def main() -> int:
    print("VERIFY PDV-CUPOM-DINHEIRO PATH (bug loja #6)")
    check_contracts()
    check_print_before_reset()
    check_node_logic()
    check_overlay_class_script()
    check_cupom_runtime()
    print(f"\nRESULTADO: {oks} OK · {fails} FAIL")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
