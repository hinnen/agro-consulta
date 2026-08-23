# -*- coding: utf-8 -*-
"""VERIFY PDV-ORC-SALVO — path Salvar orçamento → Postgres → card Orçamentos.

Cobre: botão / toast / memória / localStorage / cliente_key consumidor /
API GET+POST / model PG / F6 / GMORC / Zap / bootstrap wizard.

Uso: python scripts/verify_pdv_orcamento_salvo_path.py
"""
from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails = 0
oks = 0

PY_FILES = (
    "produtos/pdv_orcamento_util.py",
    "produtos/views.py",
    "produtos/models.py",
    "produtos/tests_pdv_orcamentos.py",
    "produtos/urls.py",
    "pdv/views.py",
    "scripts/verify_pdv_orcamento_salvo_path.py",
)

JS_FILES = (
    "produtos/static/produtos/js/pdv_wizard.js",
    "produtos/static/produtos/js/pdv_state.js",
    "produtos/static/produtos/js/consulta_produtos.js",
    "static/js/home_modals_pdv.js",
)


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


def must_contain(rel: str, needles: list[str], label: str = "") -> None:
    txt = read(rel)
    if not txt:
        return
    tag = label or rel
    for n in needles:
        if n not in txt:
            fail(f"{tag}: falta `{n[:90]}`")
        else:
            ok(f"{tag}: `{n[:56]}`")


def must_not_contain(rel: str, needles: list[str], label: str = "") -> None:
    txt = read(rel)
    if not txt:
        return
    tag = label or rel
    for n in needles:
        if n in txt:
            fail(f"{tag}: não deveria ter `{n[:70]}`")
        else:
            ok(f"{tag}: sem `{n[:40]}`")


def check_ast() -> None:
    print("\n[1] AST Python")
    for rel in PY_FILES:
        src = read(rel)
        if not src:
            continue
        try:
            ast.parse(src)
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_postgres_fonte() -> None:
    print("\n[2] Postgres é fonte (roteiro §0.1) — não só localStorage")
    models = read("produtos/models.py")
    if "class OrcamentoPdvAgro" not in models:
        fail("model OrcamentoPdvAgro ausente")
        return
    ok("model OrcamentoPdvAgro")
    must_contain(
        "produtos/models.py",
        [
            "orc_local_id = models.BigIntegerField",
            "cliente_key = models.CharField",
            "payload_json = models.JSONField",
        ],
        "OrcamentoPdvAgro campos",
    )
    mig = read("produtos/migrations/0048_orcamento_pdv_agro.py")
    if "OrcamentoPdvAgro" not in mig:
        fail("migration 0048 sem OrcamentoPdvAgro")
    else:
        ok("migration 0048 OrcamentoPdvAgro")
    urls = read("produtos/urls.py")
    if "api/pdv/orcamentos/" not in urls:
        fail("rota api/pdv/orcamentos/ ausente")
    else:
        ok("rota GET/POST /api/pdv/orcamentos/")
    if "api/pdv/orcamentos/<int:orc_local_id>/" not in urls:
        fail("rota detalhe orçamento ausente")
    else:
        ok("rota GET /api/pdv/orcamentos/<id>/")
    must_contain(
        "produtos/views.py",
        [
            "def api_pdv_orcamentos(",
            "def api_pdv_orcamento_detalhe(",
            "OrcamentoPdvAgro.objects.update_or_create",
            'cliente_key__istartswith="tmp:consumidor"',
            "normalizar_orcamento_cliente_key",
            "carimbar_entry_orcamento_pdv",
        ],
        "API PG",
    )


def check_chave_consumidor() -> None:
    print("\n[3] Chave canônica consumidor_final")
    must_contain(
        "produtos/pdv_orcamento_util.py",
        [
            'ORCAMENTO_CLIENTE_KEY_CONSUMIDOR = "consumidor_final"',
            "def normalizar_orcamento_cliente_key(",
            "def carimbar_entry_orcamento_pdv(",
            "_ORC_CONSUMIDOR_RE",
            r'r"consumidor\s+n[aã]o\s+identificado"',
        ],
        "util chave",
    )
    wizard = read("produtos/static/produtos/js/pdv_wizard.js")
    for n in (
        "function normalizeOrcamentoClienteKey",
        "function canonicalizeOrcamentoEntry",
        "historicoOrcamentosMem",
        "function writeHistoricoOrcamentos",
        "function salvarOrcamentoWizard",
        "isNomeConsumidorFinal",
        "consumidor_final",
        "_orcamentoSyncSeq += 1",
    ):
        if n not in wizard:
            fail(f"wizard: falta `{n}`")
        else:
            ok(f"wizard: `{n[:52]}`")
    consulta = read("produtos/static/produtos/js/consulta_produtos.js")
    if "return 'consumidor_final'" in consulta:
        ok("legado consulta: consumidor_final no save")
    else:
        fail("legado consulta sem consumidor_final")


def check_ui_wizard() -> None:
    print("\n[4] UI wizard — botão, card, F6, Zap, toast")
    must_contain(
        "produtos/templates/produtos/partials/pdv/step_produtos.html",
        [
            'id="pdv-step1-salvar-orcamento-btn"',
            "Salvar orçamento",
            'id="pdv-step1-budget-snippet"',
            'id="pdv-step1-budget-ver-mais"',
            'id="pdv-step1-enviar-whatsapp"',
        ],
        "step_produtos",
    )
    wizard = read("produtos/static/produtos/js/pdv_wizard.js")
    salvar = wizard.split("function salvarOrcamentoWizard", 1)[-1]
    if salvar.find("writeHistoricoOrcamentos(historico)") < 0:
        fail("salvarOrcamentoWizard sem writeHistoricoOrcamentos")
    elif salvar.find("writeHistoricoOrcamentos(historico)") > salvar.find(
        "renderRecentBudgetsSnippet()"
    ):
        fail("salvarOrcamentoWizard renderiza antes de gravar")
    else:
        ok("salvar grava memória/storage antes de renderizar o card")
    if "title: 'Orçamento salvo'" not in wizard:
        fail("toast sem título Orçamento salvo")
    else:
        ok("toast Orçamento salvo")
    if "Orçamento salvo para " not in wizard:
        fail("toast sem 'Orçamento salvo para'")
    else:
        ok("toast cita o cliente")
    if "function renderRecentBudgetsSnippet" not in wizard:
        fail("sem renderRecentBudgetsSnippet")
    else:
        ok("card Orçamentos = renderRecentBudgetsSnippet")
    if "function filterHistoricoPorCliente" not in wizard:
        fail("sem filterHistoricoPorCliente")
    else:
        ok("card filtra por cliente")
    if "function reopenBudgetById" not in wizard:
        fail("sem reopenBudgetById")
    else:
        ok("clique na linha reabre")
    if "function reopenBudgetFromBarcode" not in wizard:
        fail("sem reopenBudgetFromBarcode")
    else:
        ok("bip GMORC reabre")
    if "GMORC" not in wizard:
        fail("wizard sem GMORC")
    else:
        ok("código GMORC")
    if "event.code === 'F6'" not in wizard or "openBudgetHistory()" not in wizard:
        fail("F6 não abre lista de orçamentos")
    else:
        ok("F6 abre openBudgetHistory")
    if "historico.slice(PDV_BUDGET_CARD_VISIBLE)" not in wizard:
        fail("Ver mais / F6 não lista o restante do mesmo cliente")
    else:
        ok("Ver mais lista o restante (além dos 3 do card)")
    if "function hydrateFromBudget" not in read("produtos/static/produtos/js/pdv_state.js"):
        fail("pdv_state sem hydrateFromBudget")
    else:
        ok("hydrateFromBudget no state")
    if "enviarOrcamentoWhatsappWizard" not in wizard:
        fail("sem Enviar WhatsApp")
    else:
        ok("Enviar WhatsApp chama save")
    if "fromWhatsapp: true" not in wizard:
        fail("Zap não marca origem whatsapp")
    else:
        ok("origem whatsapp no save do Zap")
    if "addEventListener('click', salvarOrcamentoWizard)" in wizard:
        fail("click ainda passa PointerEvent como opts")
    else:
        ok("click do botão não passa Event como opts")
    boot = read("pdv/views.py")
    if '"apiPdvOrcamentos": reverse("api_pdv_orcamentos")' not in boot:
        fail("pdv_home bootstrap sem apiPdvOrcamentos")
    else:
        ok("bootstrap wizard tem apiPdvOrcamentos")
    must_contain(
        "produtos/views.py",
        ['"apiPdvOrcamentos": reverse("api_pdv_orcamentos")'],
        "bootstrap consulta/home",
    )


def check_memoria_nao_engole() -> None:
    print("\n[5] Memória + retry localStorage (toast não mente)")
    wizard = read("produtos/static/produtos/js/pdv_wizard.js")
    write_fn = wizard.split("function writeHistoricoOrcamentos", 1)[-1][:1200]
    if "historicoOrcamentosMem =" not in write_fn:
        fail("write não atualiza memória")
    else:
        ok("write atualiza historicoOrcamentosMem")
    if "localStorage.setItem('historicoOrcamentos'" not in write_fn:
        fail("write sem localStorage")
    else:
        ok("write ainda espelha localStorage (cache PC)")
    if "catch (errW)" in write_fn and "return false" in write_fn:
        ok("quota localStorage não apaga a memória")
    else:
        fail("write ainda pode falhar sem fallback de memória")
    if "function compactOrcamentoItem" not in wizard:
        fail("sem compactOrcamentoItem (payload gordo)")
    else:
        ok("itens compactos (sem imagem) no save")


def check_node_js() -> None:
    print("\n[6] node --check JS")
    for rel in JS_FILES:
        p = ROOT / rel
        if not p.is_file():
            fail(f"ausente {rel}")
            continue
        r = subprocess.run(
            ["node", "--check", str(p)],
            capture_output=True,
            text=True,
            cwd=ROOT,
            timeout=20,
        )
        if r.returncode == 0:
            ok(f"node --check {rel}")
        else:
            fail(f"node --check {rel}: {(r.stderr or r.stdout)[:120]}")


def check_django_tests() -> None:
    print("\n[7] Django tests_pdv_orcamentos")
    py = ROOT / ".venv/bin/python"
    cmd = [str(py) if py.is_file() else sys.executable, "manage.py", "test", "produtos.tests_pdv_orcamentos", "-v1"]
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT, timeout=180)
    out = (r.stdout or "") + (r.stderr or "")
    if r.returncode == 0 and "OK" in out:
        m = re.search(r"Ran (\d+) test", out)
        n = m.group(1) if m else "?"
        ok(f"tests_pdv_orcamentos {n} OK")
    else:
        fail(f"tests_pdv_orcamentos falhou: {out[-200:]}")


def check_banana() -> None:
    print("\n[8] Checklist único + VERSION")
    banana = read("banana.md")
    roteiro = read("banana-roteiro.md")
    idx_cp = banana.find("## CHECKPOINT")
    if idx_cp < 0:
        idx_cp = banana.find("CHECKPOINT")
    section = banana[idx_cp : idx_cp + 12000] if idx_cp >= 0 else banana[:12000]
    if "PDV-ORC-SALVO" not in section:
        fail("CHECKPOINT topo sem PDV-ORC-SALVO")
    else:
        ok("CHECKPOINT topo PDV-ORC-SALVO")
    if "CHECKLIST ÚNICO" not in section and "CHECKLIST UNICO" not in section:
        fail("CHECKPOINT sem CHECKLIST ÚNICO")
    else:
        ok("CHECKPOINT tem CHECKLIST ÚNICO")
    idx = section.lower().find("pdv-orc-salvo")
    chunk = section[max(0, idx - 200) : idx + 2200] if idx >= 0 else section[:2500]
    clow = chunk.lower()
    if "pronto para envio" in clow:
        ok("CHECKPOINT PDV-ORC-SALVO marca pronto para envio")
    elif "enviado / live" in clow:
        ok("CHECKPOINT PDV-ORC-SALVO já Live")
    else:
        fail("CHECKPOINT sem pronto para envio (PDV-ORC-SALVO)")
    if "PDV-ORC-SALVO" not in roteiro:
        fail("banana-roteiro.md §7 sem PDV-ORC-SALVO")
    else:
        ok("banana-roteiro.md tem path PDV-ORC-SALVO")
    if "pronto para envio" not in roteiro.lower() and "pronto pra envio" not in roteiro.lower():
        fail("banana-roteiro.md sem pronto para envio")
    else:
        ok("banana-roteiro.md marca pronto para envio")
    v = read("VERSION").strip()
    try:
        major, minor = v.split(".", 1)
        ok_ver = int(major) > 17 or (int(major) == 17 and int(minor) >= 84)
    except ValueError:
        ok_ver = False
    if not ok_ver:
        fail(f"VERSION={v} (esperado >= 17.84)")
    else:
        ok(f"VERSION {v} (>=17.84)")
    must_contain(
        "banana-roteiro.md",
        ["Postgres = fonte da verdade"],
        "roteiro §0.1",
    )


def check_nao_quebra_vizinhos() -> None:
    print("\n[9] Fora do recorte (não mexer venda / NFC-e / balança / caixa)")
    wizard = read("produtos/static/produtos/js/pdv_wizard.js")
    if "function confirmarEntregaDetalhesModal" in wizard or "function confirmarEntregaTrocoModal" in wizard:
        ok("checkout entrega permanece no wizard")
    else:
        fail("checkout entrega ausente no wizard")
    if "Cupom fiscal (NFC-e)" in wizard or "NFC-e" in wizard:
        ok("NFC-e permanece no wizard")
    else:
        fail("NFC-e sumiu do wizard")
    tpl = read("produtos/templates/produtos/pdv_wizard.html")
    if "pdv_balanca.js" in tpl and "Pesar" in tpl:
        ok("overlay Pesar permanece no template")
    else:
        fail("overlay Pesar / pdv_balanca.js ausente no template")
    bal = ROOT / "produtos/static/produtos/js/pdv_balanca.js"
    if not bal.is_file():
        fail("ausente pdv_balanca.js")
    else:
        r = subprocess.run(
            ["node", "--check", str(bal)],
            capture_output=True,
            text=True,
            cwd=ROOT,
            timeout=20,
        )
        if r.returncode == 0:
            ok("node --check pdv_balanca.js")
        else:
            fail(f"node --check pdv_balanca.js: {(r.stderr or r.stdout)[:120]}")
    nfce = read("produtos/nfce_sp_emissao_util.py")
    if "def emitir_nfce_para_venda" in nfce:
        ok("NFC-e util intacto (emitir_nfce_para_venda)")
    else:
        fail("NFC-e util sem emitir_nfce_para_venda")


def main() -> None:
    print("=== VERIFY PDV-ORC-SALVO PATH ===")
    check_ast()
    check_postgres_fonte()
    check_chave_consumidor()
    check_ui_wizard()
    check_memoria_nao_engole()
    check_node_js()
    check_django_tests()
    check_banana()
    check_nao_quebra_vizinhos()
    print("---")
    print(f"checks OK={oks} FAIL={fails}")
    if fails:
        print(f"VERIFY_FAIL ({fails})")
        sys.exit(1)
    print("VERIFY_OK")
    sys.exit(0)


if __name__ == "__main__":
    main()
