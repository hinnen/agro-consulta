# -*- coding: utf-8 -*-
"""VERIFY FIADO-TITULO-FRETE — path PDV 2× Fiado (mercadoria + frete) → títulos.

Path:
  PDV wizard: lancamentos Fiado + Fiado (restante = taxa de entrega, ex. R$ 10)
    → pagamentosDetalheParaErp (todas as linhas, cada uma com fiadoCronograma)
    → _persistir_venda_agro:
         pagamentos_json (todas) + fiado_cronograma_json (extend, sem break)
    → criar_titulos_de_venda:
         _linhas_pagamento_fiado (todas) → _parcelas_fiado_para_titulos
         chaves pdv:{pk}:1, :2, … ; se já existe título curto, cria complemento
    → baixa: soma saldo de todos os títulos em aberto da seleção
    → backfill_fiado_titulos [--pk 3437] completa gap em venda antiga

Caso loja: Venda #3437 Joelma, total 409,50, título 399,50 quitado, frete 10
nunca virou título.

  python scripts/verify_fiado_titulo_frete_path.py
"""
from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails = 0
oks = 0


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
    for n in needles:
        if n not in txt:
            fail(f"{label or rel}: falta `{n[:90]}`")
        else:
            ok(f"{label or rel}: `{n[:60]}`")


def must_not_contain(rel: str, needles: list[str], label: str = "") -> None:
    txt = read(rel)
    if not txt:
        return
    for n in needles:
        if n in txt:
            fail(f"{label or rel}: não deveria ter `{n[:80]}`")
        else:
            ok(f"{label or rel}: sem `{n[:50]}`")


def _fn_source(rel: str, name: str) -> str:
    txt = read(rel)
    if not txt:
        return ""
    try:
        tree = ast.parse(txt)
    except SyntaxError as e:
        fail(f"AST {rel}: {e}")
        return ""
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(txt, node) or ""
    fail(f"{rel}: função {name} não encontrada")
    return ""


def check_ast_files() -> None:
    print("\n[1] AST Python")
    files = (
        "produtos/fiado_gestao_util.py",
        "produtos/fiado_credito_util.py",
        "produtos/views.py",
        "produtos/management/commands/backfill_fiado_titulos.py",
        "produtos/tests_fiado_titulos_frete.py",
        "scripts/verify_fiado_titulo_frete_path.py",
    )
    for rel in files:
        p = ROOT / rel
        if not p.is_file():
            fail(f"ausente {rel}")
            continue
        try:
            ast.parse(p.read_text(encoding="utf-8"))
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_pdv_envia_todas_linhas() -> None:
    print("\n[2] PDV envia todas as linhas Fiado")
    wiz = read("produtos/static/produtos/js/pdv_wizard.js")
    # pagamentosDetalheParaErp percorre lancamentos, não só o primeiro
    src = wiz.split("function pagamentosDetalheParaErp")[1].split("\nfunction ")[0] if "function pagamentosDetalheParaErp" in wiz else ""
    if "for (var i = 0; i < arr.length; i++)" in src and "fiadoCronograma" in src:
        ok("wizard: pagamentosDetalheParaErp itera todos os lançamentos + cronograma")
    else:
        fail("wizard: pagamentosDetalheParaErp não itera todas as linhas Fiado")
    if "arr[0]" in src and "arr.length" not in src:
        fail("wizard: pagamentosDetalheParaErp parece usar só arr[0]")
    must_contain(
        "produtos/static/produtos/js/pdv_state.js",
        ["function addPagamentoLancamento", "state.pagamento.lancamentos.push"],
        "pdv_state",
    )
    r = subprocess.run(
        ["node", "--check", str(ROOT / "produtos/static/produtos/js/pdv_wizard.js")],
        capture_output=True,
        text=True,
    )
    if r.returncode == 0:
        ok("node --check pdv_wizard.js")
    else:
        fail(f"node --check pdv_wizard.js: {(r.stderr or '')[:120]}")


def check_persist_sem_break() -> None:
    print("\n[3] Persistência da venda (sem break na 1ª linha Fiado)")
    src = _fn_source("produtos/views.py", "_persistir_venda_agro")
    if not src:
        return
    if "fiado_cron.extend" in src:
        ok("views._persistir_venda_agro: fiado_cron.extend (junta cronogramas)")
    else:
        fail("views._persistir_venda_agro: falta fiado_cron.extend")
    # o bug antigo: atribui e break no primeiro Fiado
    if re.search(r"fiado_cron\s*=\s*row\.get\([^\n]+\)\s*\n\s*break", src):
        fail("views._persistir_venda_agro: ainda faz break após a 1ª linha Fiado")
    else:
        ok("views._persistir_venda_agro: sem break após a 1ª linha Fiado")
    if "normalizar_forma_pagamento_caixa" in src and "fiado_cron" in src:
        ok("views._persistir_venda_agro: normaliza forma Fiado")
    else:
        fail("views._persistir_venda_agro: persist Fiado sem normalizar forma")
    if "criar_titulos_de_venda" in src:
        ok("views._persistir_venda_agro: chama criar_titulos_de_venda")
    else:
        fail("views._persistir_venda_agro: não chama criar_titulos_de_venda")
    cred = read("produtos/fiado_credito_util.py")
    if "def pagamentos_json_com_metadados_de_payload" in cred and "fiado_cronograma" in cred:
        ok("pagamentos_json_com_metadados preserva fiado_cronograma por linha")
    else:
        fail("pagamentos_json_com_metadados não preserva cronograma")


def check_criar_titulos() -> None:
    print("\n[4] criar_titulos_de_venda / parcelas")
    must_contain(
        "produtos/fiado_gestao_util.py",
        [
            "def _linhas_pagamento_fiado",
            "def _cronograma_de_linha_fiado",
            "def _parcelas_fiado_para_titulos",
            "def criar_titulos_de_venda",
            "complemento_fiado",
            "Pedido {venda.pk} · complemento",
            "pdv:{venda.pk}:complemento",
            "formaPagamento",
            "valorPagamento",
        ],
        "fiado_gestao_util",
    )
    src = _fn_source("produtos/fiado_gestao_util.py", "_parcelas_fiado_para_titulos")
    if "for row in _linhas_pagamento_fiado(venda):" in src and "parcelas.extend" in src:
        ok("_parcelas: extend de todas as linhas Fiado")
    else:
        fail("_parcelas: não percorre todas as linhas")
    if "falta > Decimal" in src or "falta >" in src:
        ok("_parcelas: completa gap vs valor_fiado")
    else:
        fail("_parcelas: não completa gap")
    criar = _fn_source("produtos/fiado_gestao_util.py", "criar_titulos_de_venda")
    if "return list(FiadoTituloAgro.objects.filter(venda_agro=venda))" in criar and "if not existentes" in criar:
        ok("criar_titulos: não retorna cedo só porque já existe título")
    else:
        # old bug: if existentes: return existentes  (no gap check)
        if re.search(r"if existentes:\s+return existentes", criar):
            fail("criar_titulos: ainda retorna cedo quando já existe título (ignora frete)")
        else:
            ok("criar_titulos: trata existentes + gap")
    if "falta = (valor_fiado - soma_exist)" in criar:
        ok("criar_titulos: complemento = valor_fiado - soma dos títulos")
    else:
        fail("criar_titulos: falta cálculo do complemento sobre títulos existentes")
    linhas = _fn_source("produtos/fiado_gestao_util.py", "_linhas_pagamento_fiado")
    if "out.append(row)" in linhas and "dict(row)" not in linhas:
        fail("_linhas_pagamento_fiado: devolve o dict original (pode mutar pagamentos_json)")
    elif "dict(row)" in linhas:
        ok("_linhas_pagamento_fiado: copia a linha (não muta pagamentos_json)")
    cancel = _fn_source("produtos/fiado_gestao_util.py", "cancelar_titulos_venda")
    if "FiadoTituloAgro.objects.filter(venda_agro=venda)" in cancel:
        ok("cancelar_titulos_venda: cancela todos os títulos da venda")
    else:
        fail("cancelar_titulos_venda: filtro da venda ausente")


def check_baixa_e_caixa() -> None:
    print("\n[5] Baixa e conferência de caixa")
    baixa = _fn_source("produtos/fiado_gestao_util.py", "baixar_titulos_selecionados")
    if "saldo_sel = sum(t.saldo_aberto for t in titulos)" in baixa:
        ok("baixa: soma saldo de todos os títulos selecionados")
    else:
        fail("baixa: não soma saldo dos títulos")
    caixa = read("produtos/caixa_util.py")
    if "criar_titulos_de_venda(venda)" in caixa:
        ok("caixa conferência: chama criar_titulos_de_venda (repara turno aberto)")
    else:
        fail("caixa conferência: não chama criar_titulos_de_venda")
    conf = _fn_source("produtos/caixa_util.py", "listar_fiado_vendas_conferencia_caixa")
    if "valor_fiado_venda_local(venda)" in conf:
        ok("caixa conferência: valor da linha = soma fiado da venda (não só 1º título)")
    else:
        fail("caixa conferência: valor não usa valor_fiado_venda_local")


def check_backfill() -> None:
    print("\n[6] Backfill")
    cmd = read("produtos/management/commands/backfill_fiado_titulos.py")
    if "--pk" in cmd and "venda_pk" in cmd:
        ok("comando: --pk para uma venda (ex. 3437)")
    else:
        fail("comando: falta --pk")
    if "8000" in cmd:
        ok("comando: limite padrão 8000 (não 500 vendas quaisquer)")
    else:
        fail("comando: limite padrão ainda apertado")
    bf = _fn_source("produtos/fiado_gestao_util.py", "backfill_titulos_vendas_fiado")
    if 'forma_pagamento__icontains="fiado"' in bf.replace("'", '"'):
        ok("backfill: filtra forma_pagamento icontains fiado")
    else:
        fail("backfill: não filtra só fiado (limite 500 de qualquer venda perderia a 3437)")
    if "venda_pk" in bf:
        ok("backfill: aceita venda_pk")
    else:
        fail("backfill: sem venda_pk")
    if "n_depois > n_antes" in bf:
        ok("backfill: conta títulos novos mesmo se a venda já tinha ledger")
    else:
        fail("backfill: ainda pula venda que já tem título")


def check_runtime_parcelas() -> None:
    print("\n[7] Runtime parcelas (Joelma #3437)")
    import django

    django.setup()
    from datetime import datetime
    from types import SimpleNamespace
    from zoneinfo import ZoneInfo

    from produtos.fiado_credito_util import montar_cronograma_fiado, valor_fiado_venda_local
    from produtos.fiado_gestao_util import _dec, _parcelas_fiado_para_titulos

    cron_p = montar_cronograma_fiado(Decimal("399.50"), 1, 30)
    cron_f = montar_cronograma_fiado(Decimal("10.00"), 1, 30)
    v = SimpleNamespace(
        pagamentos_json=[
            {
                "forma": "Fiado",
                "valor": 399.50,
                "fiado_cronograma": cron_p,
            },
            {
                "forma": "Fiado",
                "valor": 10.00,
                "fiado_cronograma": cron_f,
            },
        ],
        fiado_cronograma_json=cron_p,  # bug antigo: só a 1ª
        criado_em=datetime(2026, 7, 16, 17, 4, 32, tzinfo=ZoneInfo("America/Sao_Paulo")),
        forma_pagamento="Fiado + Fiado",
        total=Decimal("409.50"),
        frete=Decimal("10.00"),
        cliente_id_erp="",
    )
    vf = valor_fiado_venda_local(v)
    if vf == Decimal("409.50"):
        ok("valor_fiado_venda_local(Joelma) = 409.50")
    else:
        fail(f"valor_fiado_venda_local(Joelma) = {vf} (esperado 409.50)")
    parcelas = _parcelas_fiado_para_titulos(v)
    soma = sum((_dec(p["valor"]) for p in parcelas), Decimal("0"))
    if soma == Decimal("409.50") and len(parcelas) == 2:
        ok(f"parcelas Joelma: {len(parcelas)} fatias somam 409.50")
    else:
        fail(f"parcelas Joelma: n={len(parcelas)} soma={soma}")
    nums = [p.get("parcela") for p in parcelas]
    if nums == [1, 2]:
        ok("parcelas numeradas 1 e 2 (chaves pdv:pk:1 / :2 sem colidir)")
    else:
        fail(f"parcelas numeradas {nums} (esperado [1, 2])")

    # uma linha já com o total não inventa 2ª
    v1 = SimpleNamespace(
        pagamentos_json=[{"forma": "Fiado", "valor": 409.50, "fiado_cronograma": montar_cronograma_fiado(Decimal("409.50"), 1, 30)}],
        fiado_cronograma_json=montar_cronograma_fiado(Decimal("409.50"), 1, 30),
        criado_em=v.criado_em,
        forma_pagamento="Fiado",
        total=Decimal("409.50"),
        cliente_id_erp="",
    )
    p1 = _parcelas_fiado_para_titulos(v1)
    if len(p1) == 1 and _dec(p1[0]["valor"]) == Decimal("409.50"):
        ok("uma linha 409.50 não duplica complemento")
    else:
        fail(f"uma linha 409.50 gerou {len(p1)} parcelas")


def check_django_tests() -> None:
    print("\n[8] Django tests")
    r = subprocess.run(
        [sys.executable, "manage.py", "test", "produtos.tests_fiado_titulos_frete", "-v", "1"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    tail = (r.stdout or "")[-400:] + (r.stderr or "")[-200:]
    if r.returncode == 0 and "OK" in (r.stdout + r.stderr):
        ok("produtos.tests_fiado_titulos_frete OK")
    else:
        fail(f"tests_fiado_titulos_frete falhou: {tail[-180:]}")


def check_postgres_fonte() -> None:
    print("\n[9] Fonte da verdade = Postgres")
    models = read("produtos/models.py")
    if "class FiadoTituloAgro" in models and "chave_unica" in models:
        ok("FiadoTituloAgro no Postgres (chave_unica unique)")
    else:
        fail("FiadoTituloAgro / chave_unica ausente")
    must_not_contain(
        "produtos/fiado_gestao_util.py",
        ["localStorage", "DtoLancamento"],
        "títulos fiado não vão para localStorage/Mongo DtoLancamento",
    )


def check_banana_checklist() -> None:
    print("\n[10] Checklist único / VERSION")
    roteiro = read("banana-roteiro.md")
    banana = read("banana.md")
    if "FIADO-TITULO-FRETE" in roteiro and "pronto para envio" in roteiro:
        ok("banana-roteiro §7: checklist único FIADO-TITULO-FRETE pronto para envio")
    else:
        fail("banana-roteiro sem checklist único FIADO-TITULO-FRETE pronto para envio")
    if "FIADO-TITULO-FRETE" in banana and "pronto para envio" in banana[: banana.find("### 📦 PACOTE PRONTO — peso ao vivo") if "PACOTE PRONTO — peso ao vivo" in banana else 8000]:
        ok("banana.md CHECKPOINT: FIADO-TITULO-FRETE no topo")
    else:
        # still require the slug somewhere near the start of CHECKPOINT
        idx = banana.find("CHECKPOINT")
        head = banana[idx : idx + 3500] if idx >= 0 else banana[:4000]
        if "FIADO-TITULO-FRETE" in head and "pronto para envio" in head:
            ok("banana.md CHECKPOINT: FIADO-TITULO-FRETE pronto para envio")
        else:
            fail("banana.md CHECKPOINT sem FIADO-TITULO-FRETE pronto para envio")
    ver = read("VERSION").strip()
    try:
        major, minor = ver.split(".", 1)
        ok_ver = int(major) > 17 or (int(major) == 17 and int(minor) >= 83)
    except ValueError:
        ok_ver = False
    if ok_ver:
        ok(f"VERSION {ver} (>=17.83)")
    else:
        fail(f"VERSION={ver} (esperado >= 17.83)")


def main() -> None:
    print("=== VERIFY FIADO-TITULO-FRETE PATH ===")
    check_ast_files()
    check_pdv_envia_todas_linhas()
    check_persist_sem_break()
    check_criar_titulos()
    check_baixa_e_caixa()
    check_backfill()
    check_runtime_parcelas()
    check_django_tests()
    check_postgres_fonte()
    check_banana_checklist()
    print("---")
    print(f"checks OK={oks} FAIL={fails}")
    if fails:
        print(f"VERIFY_FAIL ({fails})")
        sys.exit(1)
    print("VERIFY_OK")
    sys.exit(0)


if __name__ == "__main__":
    main()
