#!/usr/bin/env python
"""Prova profunda EXTRAVIO-APOS-DEPOSITO — classificação, DRE, anti-double-count, código."""
from __future__ import annotations

import ast
import os
import re
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from financeiro.models import LancamentoFinanceiro as NF
from financeiro.services.dre_visual_util import _grupo_despesa_dre
from financeiro.services.resumo_operacional_mongo import (
    agregar_linhas_dre_em_resumo,
    classificar_despesa_plano,
)
from produtos.extravio_deposito_util import (
    PLANO_EXTRAVIO_APOS_DEPOSITO,
    forma_eh_dinheiro,
    garantir_plano_extravio_apos_deposito,
    plano_eh_extravio_apos_deposito,
)
from produtos.models import PlanoContaAgro
from produtos.saida_caixa_planos import listar_planos_saida_caixa

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if not cond:
        FAIL += 1
        print(f"FAIL {name}: {detail or 'assertion'}")
        return
    PASS += 1
    print(f"OK {name}")


def main() -> int:
    # --- util ---
    check("nome", PLANO_EXTRAVIO_APOS_DEPOSITO == "Extravio após Depósito")
    check("detect", plano_eh_extravio_apos_deposito(PLANO_EXTRAVIO_APOS_DEPOSITO))
    check("detect_fold", plano_eh_extravio_apos_deposito("EXTRAVIO APOS DEPOSITO"))
    check("nao_geraldo", not plano_eh_extravio_apos_deposito("Retiradas Geraldo"))
    check("dinheiro", forma_eh_dinheiro("Dinheiro"))
    check("dinheiro_caixa", forma_eh_dinheiro("Dinheiro · Caixa 1"))
    check("nao_pix", not forma_eh_dinheiro("Pix"))
    check("nao_banco", not forma_eh_dinheiro("Transferência"))

    # --- cadastro / seed ---
    obj, _created = garantir_plano_extravio_apos_deposito()
    check("plano_existe", obj is not None and obj.pk)
    dbp = PlanoContaAgro.objects.filter(nome=PLANO_EXTRAVIO_APOS_DEPOSITO).first()
    check("db_tipo_outra", dbp and (dbp.tipo or "").casefold() == "outra", str(getattr(dbp, "tipo", None)))
    from produtos.extravio_deposito_util import _fold

    check("db_grupo_socio", dbp and "socio" in _fold(dbp.grupo or ""), str(getattr(dbp, "grupo", None)))
    check("db_ativo", dbp and dbp.ativo)
    check("db_exibir_pdv", dbp and dbp.exibir_pdv)

    # --- classificação ---
    nat = classificar_despesa_plano(PLANO_EXTRAVIO_APOS_DEPOSITO)
    check("nat_retirada", nat == NF.NATUREZA_RETIRADA_SOCIO, str(nat))
    check(
        "nat_nao_operacional",
        nat
        not in (
            NF.NATUREZA_DESPESA_FIXA,
            NF.NATUREZA_DESPESA_VARIAVEL,
            NF.NATUREZA_DESPESA_FINANCEIRA,
            NF.NATUREZA_CMV,
        ),
    )
    # donut despesas: extravio NÃO entra em fixa/var/financeira
    check("donut_fora", _grupo_despesa_dre(PLANO_EXTRAVIO_APOS_DEPOSITO) is None)

    # --- DRE agregação / lucro ---
    core = agregar_linhas_dre_em_resumo(
        [
            {"plano": "Salários", "despesa": 1000, "receita": 0},
            {"plano": "Combustível Strada", "despesa": 50, "receita": 0},
            {"plano": "Retiradas Geraldo", "despesa": 200, "receita": 0},
            {"plano": PLANO_EXTRAVIO_APOS_DEPOSITO, "despesa": 80, "receita": 0},
            {"plano": "Compra de Ativos ou Equipamentos", "despesa": 40, "receita": 0},
            {"plano": "Vendas", "despesa": 0, "receita": 5000},
        ]
    )
    check("extravio_80", Decimal(core["extravio_apos_deposito"]) == Decimal("80"))
    check("retiradas_200", Decimal(core["retiradas_socios"]) == Decimal("200"))
    check("fixas_1000", Decimal(core["despesas_fixas"]) == Decimal("1000"))
    check("variaveis_50", Decimal(core["despesas_variaveis"]) == Decimal("50"))
    check("financeiras_40", Decimal(core["despesas_financeiras"]) == Decimal("40"))  # ativo
    # líquido = op - financeiras; extravio NÃO entra
    liquido = Decimal(core["resultado_liquido_gerencial"])
    op = Decimal(core["resultado_operacional"])
    dfin = Decimal(core["despesas_financeiras"])
    check("liquido_sem_extravio", liquido == op - dfin)
    check(
        "liquido_maior_que_se_cortasse",
        liquido > op - dfin - Decimal("80"),
        "se cortasse extravio do líquido estaria menor",
    )
    esperado_caixa = (
        liquido
        + Decimal(core["emprestimos_entrada"])
        + Decimal(core["aportes_socios"])
        - Decimal(core["amortizacao_emprestimos"])
        - Decimal(core["retiradas_socios"])
        - Decimal(core["extravio_apos_deposito"])
    )
    check("caixa_desconta_ambos", Decimal(core["geracao_caixa"]) == esperado_caixa)

    # double-count: dois títulos extravio somam, não duplicam bucket
    core2 = agregar_linhas_dre_em_resumo(
        [
            {"plano": PLANO_EXTRAVIO_APOS_DEPOSITO, "despesa": 10, "receita": 0},
            {"plano": PLANO_EXTRAVIO_APOS_DEPOSITO, "despesa": 15, "receita": 0},
        ]
    )
    check("soma_extravio", Decimal(core2["extravio_apos_deposito"]) == Decimal("25"))
    check("retiradas_zero_so_extravio", Decimal(core2["retiradas_socios"]) == Decimal("0"))

    # --- lista PDV saída ---
    planos = listar_planos_saida_caixa()
    ext_entries = [p for p in planos if p.get("extravio") or plano_eh_extravio_apos_deposito(p.get("plano") or "")]
    check("lista_pdv_tem_extravio", len(ext_entries) >= 1, str(ext_entries))
    if ext_entries:
        check("flag_sem_retirada_padrao", bool(ext_entries[0].get("sem_retirada_caixa_padrao")))

    # --- código views / templates / JS ---
    views = (ROOT / "produtos" / "views.py").read_text(encoding="utf-8")
    check("views_baixa_flag", "retirar_caixa_pdv" in views)
    check("views_helper_baixa", "_anexar_retirada_caixa_apos_baixa_cp" in views)
    check("views_extravio_pular", "pular_retirada" in views and "plano_eh_extravio_apos_deposito" in views)
    # baixa valida caixa ANTES de gravar quando checkbox on
    idx_chk = views.find("querer_ret_caixa = bool(payload.get(\"retirar_caixa_pdv\"))")
    idx_baixa_pg = views.find("baixar_lancamentos_pg(", idx_chk if idx_chk >= 0 else 0)
    check("caixa_antes_baixa", idx_chk >= 0 and idx_baixa_pg > idx_chk)

    html = (ROOT / "produtos" / "templates" / "produtos" / "lancamentos_financeiros.html").read_text(
        encoding="utf-8"
    )
    check("html_chk", "bx-retirar-caixa" in html)
    # default unchecked: input sem checked=
    m = re.search(r'id="bx-retirar-caixa"[^>]*>', html)
    check("html_default_off", m is not None and "checked" not in m.group(0))

    embed = (ROOT / "produtos" / "templates" / "produtos" / "includes" / "caixa_saida_embed.html").read_text(
        encoding="utf-8"
    )
    check("embed_hint", "hint-extravio" in embed)
    check("embed_chk_off", "cx-retirar-caixa-extravio" in embed and "checked" not in embed.split("cx-retirar-caixa-extravio")[1][:80])

    js = (ROOT / "static" / "js" / "agro_resumo_gerencial.js").read_text(encoding="utf-8")
    check("js_linha", "Extravio após Depósito" in js)
    check("js_saldo_formula", "extravioDep" in js)

    mig = ROOT / "produtos" / "migrations" / "0127_plano_extravio_apos_deposito.py"
    check("migration_file", mig.is_file())
    ast.parse(mig.read_text(encoding="utf-8"))
    check("migration_ast", True)
    ast.parse((ROOT / "produtos" / "extravio_deposito_util.py").read_text(encoding="utf-8"))
    check("util_ast", True)

    csv = (ROOT / "docs" / "dados" / "plano_despesas_niveis_proposta.csv").read_text(encoding="utf-8")
    check("csv_linha", "Extravio após Depósito" in csv and "Sócio" in csv)

    print(f"\nRESULT {PASS} ok / {FAIL} fail")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
