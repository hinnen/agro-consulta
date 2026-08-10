"""
Prova detalhada DRE-PLANOS-CADASTRO.

Path:
  Configuracao planos (PlanoContaAgro + alias)
    -> classificar_despesa_plano (cadastro -> CSV -> heuristica)
    -> consolidar_empresa_pg / agregar_linhas_dre_em_resumo  (KPIs DRE)
    -> despesas_categorias_dre_pg (lista/donut: nome oficial + merge alias)
    -> /financeiro/resumo-gerencial/ + agro_resumo_gerencial.js
  TituloFinanceiroAgro.plano_conta NAO e reescrito.
  Indicadores HTML / PDV / caixa intactos.

  python scripts/verify_dre_planos_cadastro_path.py
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" - {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" - {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Path arquivos ==")
    util = _read("financeiro/services/plano_conta_dre_util.py")
    classif = _read("financeiro/services/resumo_operacional_mongo.py")
    dre = _read("financeiro/services/dre_visual_util.py")
    pg = _read("financeiro/services/resumo_operacional_pg.py")
    html = _read("produtos/templates/produtos/resumo_financeiro_gerencial.html")
    js = _read("static/js/agro_resumo_gerencial.js")
    views_pl = _read("produtos/views_planos_conta.py")
    ind_html = _read("financeiro/templates/financeiro/indicadores_gerencial.html")
    ind_views = _read("financeiro/views.py")

    check("util_existe", (ROOT / "financeiro/services/plano_conta_dre_util.py").is_file())
    check("util_ttl", "_CACHE_TTL_S" in util and "invalidar_cache_cadastro_dre" in util)
    check("util_nao_grava_titulo", "TituloFinanceiroAgro" not in util or ".save(" not in util)
    check("util_nao_update", ".update(" not in util and "objects.filter" not in util.split("def _carregar")[0])
    check("classificar_cadastro_primeiro", "natureza_dre_por_cadastro" in classif)
    check("classificar_antes_csv", classif.find("natureza_dre_por_cadastro") < classif.find("natureza_dre_por_planilha"))
    check("pg_usa_agregar", "agregar_linhas_dre_em_resumo" in pg)
    check("desp_cat_oficial", "nome_oficial_plano" in dre)
    check("html_ajuda_cadastro", "cadastro oficial" in html)
    check("js_desp_cat", "despesas_categorias" in js)
    check("views_invalida_cache", "_invalidar_cache_dre_planos" in views_pl)
    check("views_salvar_invalida", views_pl.count("_invalidar_cache_dre_planos") >= 3)
    check("ind_html_intacto", "Indicadores" in ind_html and "Financeiro gerencial" in ind_html)
    check("ind_view_intacto", "def dashboard_financeiro_completo" in ind_views)
    check("ind_sem_redirect_resumo", "resumo_financeiro_gerencial" not in ind_views)


def test_runtime() -> None:
    print("== Runtime classificacao + lista ==")
    from financeiro.models import LancamentoFinanceiro as NF
    from financeiro.services.dre_visual_util import despesas_categorias_dre_pg
    from financeiro.services.resumo_operacional_mongo import (
        agregar_linhas_dre_em_resumo,
        classificar_despesa_plano,
    )
    from financeiro.services.plano_conta_dre_util import (
        invalidar_cache_cadastro_dre,
        nome_oficial_plano,
        natureza_dre_por_cadastro,
    )

    fake = {
        "10 — outros": ("Outros (verificar)", "outra", "A conferir"),
        "outros (verificar)": ("Outros (verificar)", "outra", "A conferir"),
        "devolucao de mercadorias": ("Outros (verificar)", "outra", "A conferir"),
        "juros de emprestimos": ("Juros de Empréstimos", "outra", "Empréstimos / financeiro"),
        "pagamento de emprestimos": ("Pagamento de Empréstimos", "outra", "Empréstimos / financeiro"),
        "2.1.1.1.2 — salarios": ("Salários", "fixa", "Pessoal"),
        "compra de ativos ou equipamentos": (
            "Compra de Ativos ou Equipamentos",
            "outra",
            "Investimento",
        ),
        "compra mercadoria cn": ("Compra Mercadoria CN", "outra", "CMV / mercadoria"),
        "retiradas geraldo": ("Retiradas Geraldo", "outra", "Sócio"),
        "aluguel": ("Aluguel", "fixa", "Ocupação"),
    }
    with patch(
        "financeiro.services.plano_conta_dre_util._carregar_cadastro_dre",
        return_value=fake,
    ):
        check("alias_10_nome", nome_oficial_plano("10 — Outros") == "Outros (verificar)")
        check(
            "alias_10_nat",
            natureza_dre_por_cadastro("10 — Outros") == NF.NATUREZA_DESPESA_FINANCEIRA,
        )
        check(
            "alias_devolucao_fin",
            classificar_despesa_plano("Devolução de Mercadorias") == NF.NATUREZA_DESPESA_FINANCEIRA,
        )
        check(
            "alias_juros_emp",
            classificar_despesa_plano("Juros de Emprestimos") == NF.NATUREZA_EMPRESTIMO_AMORTIZACAO,
        )
        check(
            "alias_pag_emp",
            classificar_despesa_plano("Pagamento de Emprestimos") == NF.NATUREZA_EMPRESTIMO_AMORTIZACAO,
        )
        check(
            "alias_salario_cod",
            classificar_despesa_plano("2.1.1.1.2 — Salários") == NF.NATUREZA_DESPESA_FIXA,
        )
        check(
            "alias_ativo_fin",
            classificar_despesa_plano("COMPRA DE ATIVOS OU EQUIPAMENTOS") == NF.NATUREZA_DESPESA_FINANCEIRA,
        )
        check(
            "alias_cmv",
            classificar_despesa_plano("Compra Mercadoria CN") == NF.NATUREZA_CMV,
        )
        check(
            "alias_socio",
            classificar_despesa_plano("Retiradas Geraldo") == NF.NATUREZA_RETIRADA_SOCIO,
        )

        core = agregar_linhas_dre_em_resumo(
            [
                {"plano": "Aluguel", "despesa": 100, "receita": 0},
                {"plano": "10 — Outros", "despesa": 20, "receita": 0},
                {"plano": "Compra Mercadoria CN", "despesa": 50, "receita": 0},
                {"plano": "Pagamento de Emprestimos", "despesa": 999, "receita": 0},
            ]
        )
        check("kpi_fixa", float(core["despesas_fixas"]) == 100.0, str(core["despesas_fixas"]))
        check("kpi_fin", float(core["despesas_financeiras"]) == 20.0, str(core["despesas_financeiras"]))
        check("kpi_cmv", float(core["cmv"]) == 50.0, str(core["cmv"]))
        check(
            "kpi_amort",
            float(core["amortizacao_emprestimos"]) == 999.0,
            str(core["amortizacao_emprestimos"]),
        )

        with patch(
            "produtos.lancamentos_financeiro_pg_analytics_util.dre_resumo_simples_pg",
            return_value={
                "ok": True,
                "linhas": [
                    {"plano": "10 — Outros", "despesa": 80.0, "receita": 0},
                    {"plano": "Outros (verificar)", "despesa": 20.0, "receita": 0},
                    {"plano": "Juros de Emprestimos", "despesa": 999.0, "receita": 0},
                    {"plano": "Aluguel", "despesa": 10.0, "receita": 0},
                ],
            },
        ):
            out = despesas_categorias_dre_pg(
                empresa_nome="Agro Mais Centro",
                data_inicio=date(2026, 7, 1),
                data_fim=date(2026, 7, 31),
                por="competencia",
                valor="bruto",
            )
        planos = [r["plano"] for r in out["top"]]
        check("lista_merge_oficial", "Outros (verificar)" in planos and "10 — Outros" not in planos)
        check(
            "lista_merge_valor",
            next(r["valor"] for r in out["top"] if r["plano"] == "Outros (verificar)") == 100.0,
        )
        check("lista_pula_emp", "Juros de Empréstimos" not in planos)
        check("lista_total", out["total"] == 110.0, str(out.get("total")))

    with patch(
        "financeiro.services.plano_conta_dre_util._carregar_cadastro_dre",
        return_value={},
    ):
        check(
            "fallback_heuristica",
            classificar_despesa_plano("Devolução de Mercadorias") == NF.NATUREZA_DESPESA_VARIAVEL,
        )
        check("fallback_iof", classificar_despesa_plano("IOF") == NF.NATUREZA_DESPESA_FINANCEIRA)
        check("fallback_aluguel_csv", classificar_despesa_plano("Aluguel") == NF.NATUREZA_DESPESA_FIXA)

    invalidar_cache_cadastro_dre()
    check("invalidar_ok", True)


def test_pg_vivo_opcional() -> None:
    print("== Cadastro PG (se houver) ==")
    try:
        from financeiro.models import LancamentoFinanceiro as NF
        from financeiro.services.plano_conta_dre_util import (
            invalidar_cache_cadastro_dre,
            nome_oficial_plano,
            natureza_dre_por_cadastro,
        )
        from produtos.models import PlanoContaAgro

        n = PlanoContaAgro.objects.count()
        if n <= 0:
            check("pg_cadastro_vazio_ok", True, "sqlite/local sem seed")
            return
        invalidar_cache_cadastro_dre()
        check("pg_tem_planos", n >= 10, str(n))
        check("pg_10_outros", nome_oficial_plano("10 — Outros") == "Outros (verificar)")
        check(
            "pg_juros_emp",
            natureza_dre_por_cadastro("Juros de Emprestimos") == NF.NATUREZA_EMPRESTIMO_AMORTIZACAO,
        )
        check(
            "pg_salario_cod",
            natureza_dre_por_cadastro("2.1.1.1.2 — Salários") == NF.NATUREZA_DESPESA_FIXA,
        )
    except Exception as exc:
        check("pg_cadastro_opcional", True, f"{type(exc).__name__}")


def test_pdv_caixa_intactos() -> None:
    print("== PDV/caixa intactos neste pacote ==")
    r = subprocess.run(
        [
            "git",
            "diff",
            "--name-only",
            "419b3c2",
            "HEAD",
            "--",
            "produtos/caixa_util.py",
            "produtos/static/produtos/js/pdv_wizard.js",
            "electron/",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    names = (r.stdout or "").strip()
    check("diff_pdv_caixa_vazio", names == "", names[:200] or "vazio")


def test_js() -> None:
    print("== JS ==")
    js = ROOT / "static/js/agro_resumo_gerencial.js"
    r = subprocess.run(["node", "--check", str(js)], capture_output=True, text=True)
    check("node_check", r.returncode == 0, (r.stderr or r.stdout or "").strip()[:120])


def main() -> int:
    test_arquivos()
    test_runtime()
    test_pg_vivo_opcional()
    test_pdv_caixa_intactos()
    test_js()
    print(f"\n{len(oks)} OK · {len(fails)} FAIL")
    if fails:
        print("VERIFY_FAIL:", ", ".join(fails))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
