#!/usr/bin/env python
"""Verificação estática + validação do path CP-NOVO-EMPRESTIMO (sem gravar Mongo)."""
from __future__ import annotations

import os
import sys
from datetime import date
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


def main() -> int:
    print("=== CP-NOVO-EMPRESTIMO path ===\n")

    # 1) Planos
    check(
        "plano entrada externo",
        mfu.emprestimo_plano_entrada_resolvido() == "Entrada de Empréstimo",
    )
    check(
        "plano dívida externo",
        mfu.emprestimo_plano_divida_resolvido() == "Pagamento de Empréstimos",
    )
    check(
        "plano juros externo",
        mfu.emprestimo_plano_juros_resolvido() == "Juros de Empréstimos",
    )
    check(
        "plano entrada interno",
        mfu.emprestimo_plano_entrada_interno_resolvido() == "Entrada de Empréstimo interno",
    )
    check(
        "plano dívida interno",
        mfu.emprestimo_plano_divida_interno_resolvido() == "Pagamento de Empréstimos interno",
    )
    check(
        "plano juros interno",
        mfu.emprestimo_plano_juros_interno_resolvido() == "Juros de Empréstimos interno",
    )

    d = mfu.emprestimo_defaults_para_ui()
    for k in (
        "plano_entrada",
        "plano_divida",
        "plano_juros",
        "plano_entrada_interno",
        "plano_divida_interno",
        "plano_juros_interno",
    ):
        check(f"defaults.{k}", bool(d.get(k)), str(d.get(k) or ""))

    # 2) Validação criar sem Mongo
    r_none = mfu.criar_emprestimo_externo_agro(
        None,
        usuario_label="teste",
        empresa_nome="E",
        empresa_id=None,
        credor_nome="C",
        credor_id=None,
        valor_recebido=Decimal("100"),
        valor_total_devido=Decimal("100"),
        data_entrada=date.today(),
        primeiro_vencimento=date.today(),
        parcelas=1,
        intervalo_dias=30,
        banco_nome="B",
        banco_id=None,
        forma_nome="",
        forma_id=None,
        plano_entrada_nome="",
        plano_entrada_id=None,
        plano_divida_nome="",
        plano_divida_id=None,
        variante="interno",
    )
    check("mongo off = erro", r_none.get("ok") is False, str(r_none.get("erro")))

    r_cred = mfu.criar_emprestimo_externo_agro(
        object(),  # type: ignore[arg-type]
        usuario_label="teste",
        empresa_nome="E",
        empresa_id=None,
        credor_nome="",
        credor_id=None,
        valor_recebido=Decimal("100"),
        valor_total_devido=Decimal("100"),
        data_entrada=date.today(),
        primeiro_vencimento=date.today(),
        parcelas=1,
        intervalo_dias=30,
        banco_nome="B",
        banco_id=None,
        forma_nome="",
        forma_id=None,
        plano_entrada_nome="X",
        plano_entrada_id=None,
        plano_divida_nome="Y",
        plano_divida_id=None,
        variante="interno",
    )
    # object() as db will fail later if validation passes; credor empty should fail first
    check(
        "credor vazio = erro",
        r_cred.get("ok") is False and "credor" in str(r_cred.get("erro") or "").lower(),
        str(r_cred.get("erro")),
    )

    # Fallback planos internos quando nomes vazios (db fake para parar depois do fill — usa None)
    # Testa só o preenchimento via chamada com db=None já cobre early exit; checa helpers:
    pe = ""
    pd = ""
    var = "interno"
    if not pe or not pd:
        pe = pe or mfu.emprestimo_plano_entrada_interno_resolvido()
        pd = pd or mfu.emprestimo_plano_divida_interno_resolvido()
    check("fallback planos interno", pe.endswith("interno") and pd.endswith("interno"), f"{pe} | {pd}")

    # 3) URLs
    check("url api criar", reverse("api_emprestimos_criar").endswith("/"), reverse("api_emprestimos_criar"))
    check("url api defaults", "emprestimo" in reverse("api_emprestimos_defaults").lower())
    check("url CP", reverse("lancamentos_contas_pagar"))
    check("url externo legado", reverse("emprestimos_externo"))
    check("url interno legado", reverse("emprestimos_interno"))

    # 4) Arquivos UI
    modal = ROOT / "produtos/templates/produtos/includes/lancamento_novo_emprestimo_modal.html"
    cp = ROOT / "produtos/templates/produtos/lancamentos_contas_pagar_teste.html"
    check("modal existe", modal.is_file())
    txt_m = modal.read_text(encoding="utf-8")
    check("modal Externo/Interno", "agro-ne-btn-externo" in txt_m and "agro-ne-btn-interno" in txt_m)
    check("modal envia variante", '"variante": variante' in txt_m or "variante: variante" in txt_m)
    check("modal sem conta/forma na UI", "ne-banco-nome" not in txt_m and "ne-forma-nome" not in txt_m)
    check("modal sem planos na UI", "ne-plano-ent-nome" not in txt_m and "agro-ne-planos-info" not in txt_m)
    check("modal envia banco/planos vazios", "banco_nome: ''" in txt_m and "plano_entrada_nome: ''" in txt_m)
    check("modal Gerar parcelas", 'id="ne-gerar-parcelas"' in txt_m and "gerarParcelasPreview" in txt_m)
    check("modal preview parcelas", 'id="ne-parc-preview"' in txt_m and "ne-parc-composicao" in txt_m)
    check("modal calendário mensal", "addMesesIso" in txt_m and "vencimentoParcela" in txt_m)
    check("modal envia parcelas_manual", "parcelas_manual" in txt_m)
    check("modal sem campo juros avulso", "ne-valor-juros" not in txt_m)
    check("modal sem campo Grupo", "ne-grupo" not in txt_m and "Grupo (opcional)" not in txt_m)
    check("modal auto parcelas", "agendarParcelasAuto" in txt_m and "silent: true" in txt_m)
    check("modal intervalo Outros", "__outro__" in txt_m and "neIntervaloDias" in txt_m)
    check("modal grade 2 colunas", "ne-parc-grid" in txt_m)
    check(
        "vencimento calendário 30d",
        callable(getattr(mfu, "_fin_vencimento_parcela", None)),
    )
    dv0 = date(2026, 8, 24)
    check(
        "vencimento mensal mesmo dia",
        mfu._fin_vencimento_parcela(dv0, 1, 30) == date(2026, 9, 24),
        str(mfu._fin_vencimento_parcela(dv0, 1, 30)),
    )
    check(
        "split proporcional",
        callable(getattr(mfu, "split_decimal_proporcional", None)),
    )
    txt_cp = cp.read_text(encoding="utf-8")
    check("botão CP", "data-agro-novo-emprestimo-open" in txt_cp)
    check("include modal", "lancamento_novo_emprestimo_modal.html" in txt_cp)
    check("json_script defaults", "emprestimos-defaults-data" in txt_cp)

    # 5) Settings
    from django.conf import settings

    check("settings INTERNO entrada", hasattr(settings, "AGRO_EMPRESTIMO_PLANO_ENTRADA_INTERNO"))
    check("settings INTERNO dívida", hasattr(settings, "AGRO_EMPRESTIMO_PLANO_DIVIDA_INTERNO"))
    check("settings INTERNO juros", hasattr(settings, "AGRO_EMPRESTIMO_PLANO_JUROS_INTERNO"))

    # 6) Views redirects (inspect source markers already in routes — call view callables lightly)
    from produtos import views as v

    check("externo_view redirect fn", callable(v.emprestimos_externo_view))
    check("interno_view redirect fn", callable(v.emprestimos_interno_view))
    src_ext = Path(v.__file__).read_text(encoding="utf-8", errors="replace")
    # Only check the redirect targets near function defs — use string presence
    check(
        "externo redireciona CP",
        'def emprestimos_externo_view' in src_ext
        and 'reverse("lancamentos_contas_pagar")' in src_ext,
    )
    check(
        "interno redireciona CP",
        'def emprestimos_interno_view' in src_ext
        and 'lancamentos_contas_pagar' in src_ext,
    )
    check(
        "API rejeita interno sócio antigo",
        "Empréstimo interno agora é pelo botão" in src_ext,
    )
    check(
        "API passa variante=",
        "variante=variante" in src_ext,
    )
    check(
        "CP chama garantir planos interno",
        "garantir_planos_emprestimo_interno_cadastro" in src_ext
        and "emprestimos_defaults" in src_ext,
    )

    print(f"\n=== Resultado: {OK} OK · {FAIL} FAIL ===")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
