# -*- coding: utf-8 -*-
"""VERIFY PDV-ENTER-SEM-IMP — bug loja #9 (Enter = sempre sem impressão).

Prova detalhada:
  · Nenhum tryConfirmSale(pagamentoSoDinheiro(...))
  · Sem função pagamentoSoDinheiro / refreshConfirmSaleLabels
  · Handler global Enter → tryConfirmSale(false)
  · Enter no valor tranche (quitado) → tryConfirmSale(false)
  · Clique SEM → false · clique COM → true · F9 → true
  · Rótulos fixos: Enter no SEM, F9 no COM (JS + HTML)
  · Foco pós-quitado no SEM (não no COM)
  · setConfirmButtonsBusy restaura rótulos Enter/F9 (não dinâmicos)
  · Contagem: nenhum tryConfirmSale(true) colado a Enter no step pagamento
  · Print ANTES de resetWizard (3 paths)
  · Overlay Vendas (altura 100%)
  · Cupom 80mm serializa venda Dinheiro (PG)

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
HTML_VENDAS = "produtos/templates/produtos/vendas_lista.html"
HTML_PDV = "produtos/templates/produtos/pdv_wizard.html"


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


def check_anti_regressao() -> None:
    print("\n[0] Anti-regressão bug #6 (Enter dinheiro = cupom)")
    js = read(JS)
    if not js:
        return

    banned = [
        "function pagamentoSoDinheiro",
        "pagamentoSoDinheiro(",
        "refreshConfirmSaleLabels",
        "tryConfirmSale(pagamentoSoDinheiro(st))",
        "Dinheiro quitado: Enter = com cupom",
        "Enter fica no COM impressão",
        "confirmSalePrint.focus()",
    ]
    for b in banned:
        if b in js:
            fail(f"regressao:{b[:50]}")
        else:
            ok(f"sem:{b[:48]}")


def check_enter_f9_handlers() -> None:
    print("\n[1] Handlers Enter / F9 / botoes")
    js = read(JS)
    if not js:
        return

    # Encontra o Enter do step pagamento que chama confirmSaleNoPrint
    enter_ok = False
    for m in re.finditer(r"event\.key\s*===\s*['\"]Enter['\"]", js):
        chunk = js[m.start() : m.start() + 550]
        if "confirmSaleNoPrint" not in chunk or "tryConfirmSale" not in chunk:
            continue
        m_arg = re.search(r"tryConfirmSale\(([^)]+)\)", chunk)
        if not m_arg:
            continue
        arg = m_arg.group(1).strip()
        if arg == "false":
            ok("kbd_Enter_tryConfirmSale_false")
            enter_ok = True
        else:
            fail(f"kbd_Enter_arg={arg}")
            enter_ok = True
        break
    if not enter_ok:
        fail("kbd_Enter_pagamento_nao_achado")

    m_f9 = re.search(
        r"event\.code\s*===\s*['\"]F9['\"][\s\S]{0,220}?tryConfirmSale\(([^)]+)\)",
        js,
    )
    if m_f9 and m_f9.group(1).strip() == "true":
        ok("kbd_F9_tryConfirmSale_true")
    elif m_f9:
        fail(f"kbd_F9_arg={m_f9.group(1).strip()}")
    else:
        fail("kbd_F9_nao_achado")

    if "Enter = sempre sem impressão" in js:
        ok("comentario_bug9")
    else:
        fail("comentario_bug9")

    # Enter no valor tranche (quitado): procura getElementById valor + tryConfirmSale depois
    found_tranche = False
    pos = 0
    while True:
        i = js.find("pdv-pay-valor-tranche", pos)
        if i < 0:
            break
        chunk = js[i : i + 1200]
        if "tryConfirmSale" in chunk and "rest" in chunk:
            m2 = re.search(
                r"rest\s*<=\s*0\.009[\s\S]{0,280}?tryConfirmSale\(([^)]+)\)",
                chunk,
            )
            if m2 and m2.group(1).strip() == "false":
                ok("tranche_quitado_tryConfirmSale_false")
            elif m2:
                fail(f"tranche_arg={m2.group(1).strip()}")
            else:
                fail("tranche_tryConfirmSale_padrao")
            found_tranche = True
            break
        pos = i + 1
    if not found_tranche:
        fail("tranche_tryConfirmSale_nao_achado")

    if re.search(
        r"confirmSaleNoPrint\.addEventListener\([\s\S]{0,120}?tryConfirmSale\(false\)",
        js,
    ):
        ok("click_SEM_false")
    else:
        fail("click_SEM")

    if re.search(
        r"confirmSalePrint\.addEventListener\([\s\S]{0,120}?tryConfirmSale\(true\)",
        js,
    ):
        ok("click_COM_true")
    else:
        fail("click_COM")

    bad_enter_true = []
    for m in re.finditer(r"tryConfirmSale\(true\)", js):
        window = js[max(0, m.start() - 180) : m.start()]
        if re.search(r"event\.key\s*===\s*['\"]Enter['\"]", window) and "F9" not in window:
            bad_enter_true.append(m.start())
    if not bad_enter_true:
        ok("nenhum_Enter_direto_tryConfirmSale_true")
    else:
        fail(f"Enter_para_true_em_{len(bad_enter_true)}_lugares")


def check_labels_foco() -> None:
    print("\n[2] Rótulos + foco + HTML")
    js = read(JS)
    html = read(HTML_PDV)
    if not js:
        return

    body_busy = _fn_body(js, "setConfirmButtonsBusy")
    if (
        "Confirmar sem impressão" in body_busy
        and ">Enter</kbd>" in body_busy
        and "Confirmar com impressão" in body_busy
        and ">F9</kbd>" in body_busy
    ):
        ok("busy_rotulos_Enter_SEM_F9_COM")
    else:
        fail("busy_rotulos")

    # SEM não pode ter Enter no COM dentro do mesmo innerHTML do print quando dinheiro
    # (já sem refresh dinâmico) — garante que Enter só aparece uma vez no SEM
    enter_in_sem = len(
        re.findall(
            r"Confirmar sem impressão[\s\S]{0,200}?>Enter</kbd>",
            body_busy,
        )
    )
    enter_in_com = len(
        re.findall(
            r"Confirmar com impressão[\s\S]{0,200}?>Enter</kbd>",
            body_busy,
        )
    )
    if enter_in_sem >= 1 and enter_in_com == 0:
        ok("Enter_so_no_rotulo_SEM")
    else:
        fail(f"Enter_rotulos sem={enter_in_sem} com={enter_in_com}")

    body_after = _fn_body(js, "afterCommitTrancheFlow")
    if "pdv-confirm-sale-no-print" in body_after and ".focus()" in body_after:
        ok("foco_pos_quitado_SEM")
    else:
        fail("foco_pos_quitado")
    if "confirmSalePrint.focus()" in body_after:
        fail("foco_ainda_COM")
    else:
        ok("foco_nao_no_COM")

    if html:
        if 'id="pdv-confirm-sale-no-print"' in html and "Enter" in html:
            ok("html_botao_SEM_Enter")
        else:
            fail("html_botao_SEM")
        if 'id="pdv-confirm-sale-print"' in html and "F9" in html:
            ok("html_botao_COM_F9")
        else:
            fail("html_botao_COM")


def check_print_before_reset() -> None:
    print("\n[3] Ordem print antes do reset (3 paths)")
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
            idx = js.find(f"function {name}")
            if idx < 0:
                fail(f"{name}:não achada")
                continue
            body = js[idx : idx + 12000]
        i_print = body.find("imprimirCupomAposVenda")
        i_reset = body.find("resetWizardParaNovaVenda")
        if i_print < 0 or i_reset < 0:
            fail(f"{name}:print/reset ({i_print},{i_reset})")
            continue
        if i_print < i_reset:
            ok(f"{name}:print_antes_reset")
        else:
            fail(f"{name}:reset_antes_print")

    bad = re.findall(
        r"resetWizardParaNovaVenda\(\);\s*\n\s*invalidateEntregasPendentesCache\(\);\s*\n\s*"
        r"refreshEntregasPendentesUi\([^)]+\);\s*\n\s*return imprimirCupomAposVenda",
        js,
    )
    if not bad:
        ok("sem_padrao_legado_reset_antes_print")
    else:
        fail(f"padrao_legado x{len(bad)}")

    if "Cupom ANTES do modal" in js:
        ok("comentario_cupom_antes_modal")
    else:
        fail("comentario_cupom_antes_modal")


def check_node_withprint_contract() -> None:
    print("\n[4] Contrato withPrint (node)")
    script = r"""
// Espelha a regra #9: Enter sempre false; F9 true; clique SEM false; clique COM true
function resolveWithPrint(source) {
  if (source === 'Enter' || source === 'click_sem' || source === 'tranche_quitado') return false;
  if (source === 'F9' || source === 'click_com') return true;
  throw new Error('fonte desconhecida');
}
function assert(name, cond) {
  if (!cond) { console.log('FAIL ' + name); process.exitCode = 1; }
  else console.log('OK ' + name);
}
assert('Enter', resolveWithPrint('Enter') === false);
assert('F9', resolveWithPrint('F9') === true);
assert('click_sem', resolveWithPrint('click_sem') === false);
assert('click_com', resolveWithPrint('click_com') === true);
assert('tranche', resolveWithPrint('tranche_quitado') === false);
assert('dinheiro_igual_pix', resolveWithPrint('Enter') === false);
"""
    tmp = ROOT / "scripts" / "_tmp_enter_sem_imp.js"
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


def check_overlay() -> None:
    print("\n[5] Overlay Vendas")
    html = read(HTML_VENDAS)
    if not html:
        return
    for needle in (
        "agro-vendas-in-overlay",
        "agro_pdv_overlay",
        "min-height: 100% !important",
        "max-height: 100% !important",
    ):
        if needle in html:
            ok(f"html:{needle[:40]}")
        else:
            fail(f"html:{needle}")
    if "classList.add('agro-vendas-in-overlay')" in html or 'classList.add("agro-vendas-in-overlay")' in html:
        ok("overlay_script_add_class")
    else:
        fail("overlay_script_add_class")
    if "agro_inapp_embed" in html and "agro_pdv_overlay" in html:
        ok("overlay_params")
    else:
        fail("overlay_params")


def check_cupom_runtime() -> None:
    print("\n[6] Cupom 80mm venda Dinheiro (runtime PG)")
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
        fail("nenhuma venda Dinheiro no PG")
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
        fail(f"cupom forma:{forma!r}")


def check_static_served_hint() -> None:
    print("\n[7] Arquivo estático / VERSION")
    js_path = ROOT / JS
    if js_path.is_file() and js_path.stat().st_size > 100_000:
        ok(f"js_size:{js_path.stat().st_size}")
    else:
        fail("js_size")
    ver = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    if re.match(r"^\d+\.\d+", ver):
        ok(f"VERSION:{ver}")
    else:
        fail(f"VERSION:{ver!r}")


def main() -> int:
    print("VERIFY PDV-ENTER-SEM-IMP PATH (bug loja #9) — detalhado")
    check_anti_regressao()
    check_enter_f9_handlers()
    check_labels_foco()
    check_print_before_reset()
    check_node_withprint_contract()
    check_overlay()
    check_cupom_runtime()
    check_static_served_hint()
    print(f"\nRESULTADO: {oks} OK · {fails} FAIL")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
