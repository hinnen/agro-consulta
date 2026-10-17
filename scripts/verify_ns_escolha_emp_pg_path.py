#!/usr/bin/env python
"""
Prova detalhada do lote CP pós-v18.64:
  NS-ESCOLHA-EMP · CP-EMP-PG-FALLBACK

Sem gravar lixo permanente: se gravar PG, apaga os títulos de teste.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django  # noqa: E402

django.setup()

from django.urls import reverse  # noqa: E402

from produtos import mongo_financeiro_util as mfu  # noqa: E402

FAIL = 0
OK = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global FAIL, OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    print("=== PATH NS-ESCOLHA-EMP + CP-EMP-PG-FALLBACK ===\n")

    # --- A) UI escolha Nova saída ---
    print("## A) Nova saída — escolha Lançamento × Empréstimo\n")
    js = _read("produtos/static/produtos/js/lancamento_nova_saida.js")
    html_ns = _read("produtos/templates/produtos/includes/lancamento_nova_saida_modal.html")
    html_ne = _read("produtos/templates/produtos/includes/lancamento_novo_emprestimo_modal.html")
    dash = _read("produtos/templates/produtos/dashboard_gerencial.html")
    views = _read("produtos/views.py")
    mfu_src = _read("produtos/mongo_financeiro_util.py")

    check("passo escolha no HTML", "id=\"agro-ns-passo-escolha\"" in html_ns)
    check("botão Novo Lançamento", "id=\"agro-ns-btn-escolha-lancamento\"" in html_ns)
    check("botão Empréstimo", "id=\"agro-ns-btn-escolha-emprestimo\"" in html_ns)
    check("cards escolha em coluna", "flex-col" in html_ns and "min-h-[7.5rem]" in html_ns and "border-4" in html_ns)
    check("descrição horizontal (não nowrap no título longo)", "whitespace-normal" in html_ns and "Despesa ou receita normal" in html_ns)
    check(
        "sem nowrap espremendo descrição",
        'whitespace-nowrap text-3xl sm:text-4xl font-black uppercase tracking-wide text-red-900">Novo Lançamento' not in html_ns,
    )
    check("form começa oculto", 'id="agro-ns-form" class="hidden' in html_ns)
    check("JS showPassoEscolha", "function showPassoEscolha" in js)
    check("JS showPassoForm", "function showPassoForm" in js)
    check("JS abre empréstimo", "AgroNovoEmprestimo" in js and "abrirEmprestimoAposEscolha" in js)
    check("JS fallback URL ?novo_emprestimo=1", "novo_emprestimo=1" in js)
    check("JS Cancelar volta à escolha", "cancelarOuVoltar" in js)
    check("AgroNovoEmprestimo.open exposto", "window.AgroNovoEmprestimo" in html_ne)
    check("auto-open query novo_emprestimo", "novo_emprestimo" in html_ne and "openOverlay()" in html_ne)
    check("painel sucesso empréstimo", 'id="agro-ne-sucesso"' in html_ne and "Empréstimo registrado" in html_ne)
    check("sucesso não fecha sozinho", "showSucesso" in html_ne and "setTimeout(closeOverlay" not in html_ne)
    check("OK conclui sucesso", "agro-ne-sucesso-ok" in html_ne and "concluirSucesso" in html_ne)
    check("BI inclui modal empréstimo", "lancamento_novo_emprestimo_modal.html" in dash)
    check("BI passa emprestimos_defaults", "emprestimos_defaults" in views and "dashboard_gerencial_view" in views)
    check("helper defaults template", "def _emprestimos_defaults_para_template" in views)

    # --- B) Backend PG fallback ---
    print("\n## B) Empréstimo — fallback Postgres (Mongo off)\n")
    check(
        "criar usa dispatch PG",
        "inserir_lancamentos_manual_lote_dispatch" in mfu_src
        and mfu_src.count("inserir_lancamentos_manual_lote_dispatch") >= 2,
    )
    # api_emprestimos_criar: sem 503 Mongo no início
    idx = views.find("def api_emprestimos_criar")
    chunk = views[idx : idx + 2200] if idx >= 0 else ""
    check("api criar existe", idx >= 0)
    check(
        "api criar sem 503 Mongo early",
        "Mongo indisponível" not in chunk.split("criar_emprestimo_externo_agro")[0],
        "early block limpo",
    )
    check(
        "api cria com db None ok (comentário dispatch)",
        "dispatch" in chunk.lower() or "Postgres" in chunk or "não 503" in chunk.lower(),
    )
    check("url api criar", reverse("api_emprestimos_criar").endswith("/"))

    # Validação sem credor (ainda bloqueia antes do write)
    r_cred = mfu.criar_emprestimo_externo_agro(
        None,
        usuario_label="verify-path",
        empresa_nome="Agro Mais Centro",
        empresa_id=None,
        credor_nome="",
        credor_id=None,
        valor_recebido=Decimal("1"),
        valor_total_devido=Decimal("2"),
        data_entrada=date.today(),
        primeiro_vencimento=date.today() + timedelta(days=1),
        parcelas=1,
        intervalo_dias=30,
        banco_nome="",
        banco_id=None,
        forma_nome="",
        forma_id=None,
        plano_entrada_nome="",
        plano_entrada_id=None,
        plano_divida_nome="",
        plano_divida_id=None,
        variante="interno",
    )
    check("sem credor = erro", r_cred.get("ok") is False, str(r_cred.get("erro") or ""))

    # Caso do print Renan: interno 1→2, 1 parcela, Mongo off (valores únicos)
    import time

    uniq0 = int(time.time()) % 100000
    r = mfu.criar_emprestimo_externo_agro(
        None,
        usuario_label=f"verify-path-ns-emp-{uniq0}",
        empresa_nome="Agro Mais Centro",
        empresa_id=None,
        credor_nome=f"Geraldo Hinnen VERIFY {uniq0}",
        credor_id=None,
        valor_recebido=Decimal("1.00"),
        valor_total_devido=Decimal("2.00"),
        data_entrada=date.today(),
        primeiro_vencimento=date.today() + timedelta(days=1),
        parcelas=1,
        intervalo_dias=30,
        banco_nome="",
        banco_id=None,
        forma_nome="",
        forma_id=None,
        plano_entrada_nome="",
        plano_entrada_id=None,
        plano_divida_nome="",
        plano_divida_id=None,
        variante="interno",
        entrada_ja_quitada=True,
    )
    err = str(r.get("erro") or "")
    check(
        "print Renan Mongo-off NÃO é legado",
        "Mongo indispon" not in err and "serviço legado" not in err.lower(),
        err or f"ok={r.get('ok')} ref={r.get('ref')}",
    )
    check("print Renan ok=True", r.get("ok") is True, err or str(r.get("ref")))
    check("print Renan tem ref", bool(r.get("ref")), str(r.get("ref") or ""))
    check("print Renan juros 1,00", float(r.get("valor_juros") or 0) == 1.0, str(r.get("valor_juros")))
    ids_e = list(r.get("ids_entrada") or [])
    ids_d = list(r.get("ids_divida") or [])
    check("print Renan ids entrada", len(ids_e) >= 1, str(ids_e))
    check("print Renan ids dívida/juros", len(ids_d) >= 1, str(ids_d))

    # Limpa títulos de teste no PG (se gravou de verdade)
    apagados = 0
    try:
        from produtos.models import TituloFinanceiroAgro

        ids_all = [str(x) for x in (ids_e + ids_d) if x]
        reais = [i for i in ids_all if not str(i).startswith("staging-dry:")]
        for mid in reais:
            n, _ = TituloFinanceiroAgro.objects.filter(mongo_id=str(mid)).delete()
            apagados += n
        check(
            "cleanup PG (ou dry-run)",
            True,
            f"apagados={apagados} ids={len(reais)} dry={len(ids_all) - len(reais)}",
        )
    except Exception as exc:
        check("cleanup PG", False, str(exc)[:200])

    # Externo 100/120 2 parcelas Mongo off (valores únicos)
    import time

    uniq = int(time.time()) % 100000
    r2 = mfu.criar_emprestimo_externo_agro(
        None,
        usuario_label=f"verify-path-ns-emp-{uniq}",
        empresa_nome="Agro Mais Centro",
        empresa_id=None,
        credor_nome=f"Credor VERIFY EXT {uniq}",
        credor_id=None,
        valor_recebido=Decimal("100.00") + Decimal(uniq) / Decimal("1000"),
        valor_total_devido=Decimal("120.00") + Decimal(uniq) / Decimal("1000"),
        data_entrada=date.today(),
        primeiro_vencimento=date.today() + timedelta(days=30),
        parcelas=2,
        intervalo_dias=30,
        banco_nome="",
        banco_id=None,
        forma_nome="",
        forma_id=None,
        plano_entrada_nome="",
        plano_entrada_id=None,
        plano_divida_nome="",
        plano_divida_id=None,
        variante="externo",
        entrada_ja_quitada=True,
    )
    check("externo 2 parc Mongo-off ok", r2.get("ok") is True, str(r2.get("erro") or r2.get("ref")))
    check("externo juros 20", float(r2.get("valor_juros") or 0) == 20.0, str(r2.get("valor_juros")))
    try:
        from produtos.models import TituloFinanceiroAgro

        for tid in list(r2.get("ids_entrada") or []) + list(r2.get("ids_divida") or []):
            if str(tid).startswith("staging-dry:"):
                continue
            TituloFinanceiroAgro.objects.filter(mongo_id=str(tid)).delete()
    except Exception:
        pass

    print(f"\n=== Resultado: {OK} OK · {FAIL} FAIL ===")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
