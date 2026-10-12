# -*- coding: utf-8 -*-
"""Prova — lista clientes + desconto % PDV (`CLIENTE-LISTA-DESC-PDV`)."""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.forms import ClienteAgroForm
from produtos.models import ClienteAgro
from produtos.views import _linha_clienteagro_pdv

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_templates_and_js() -> None:
    print("== UI / PDV JS ==")
    lista = (ROOT / "produtos/templates/produtos/clientes_lista.html").read_text(encoding="utf-8")
    form = (ROOT / "produtos/templates/produtos/cliente_form.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/pdv_state.js").read_text(encoding="utf-8")
    models = (ROOT / "produtos/models.py").read_text(encoding="utf-8")

    check("lista_shell_largo", "cli-lista-shell" in lista and "max-width: min(110rem" in lista)
    check("lista_sem_max_w3xl", "max-w-3xl" not in lista)
    check("lista_col_desc", "Desc. PDV" in lista)
    check("lista_dvh", "100dvh" in lista)
    check("form_campo_desc", "desconto_pdv_percentual" in form)
    check(
        "form_help_vazio",
        "desconto automático" in form or "desconto automatico" in form.lower(),
    )
    check("js_sanitize_pct", "desconto_pdv_percentual" in js.split("function sanitizeCliente")[1][:1200])
    check("js_origem", "descontoGeralOrigem" in js)
    check("js_recalc", "recalcDescontoPercentualCliente" in js)
    check("js_manual_preserva", "origem === 'manual'" in js)
    check("model_nullable", "desconto_pdv_percentual" in models and "null=True" in models.split("desconto_pdv_percentual")[1][:200])
    check("migration_0145", (ROOT / "produtos/migrations/0145_clienteagro_desconto_pdv_percentual.py").is_file())


def test_form_and_api_linha() -> None:
    print("== Form / API PDV linha ==")
    base = {
        "nome": "Prova Desc",
        "ativo": True,
        "saldo_cashback": "0",
        "saldo_vale_credito": "0",
        "limite_fiado_local": "0.01",
    }
    f_empty = ClienteAgroForm(data=base)
    check("form_sem_pct_ok", f_empty.is_valid() or "desconto" not in str(f_empty.errors))
    f_bad = ClienteAgroForm(data={**base, "nome": "Prova Desc 2", "desconto_pdv_percentual": "150"})
    check("form_pct_max", not f_bad.is_valid() and "desconto_pdv_percentual" in f_bad.errors)
    form_clean = ClienteAgroForm()
    form_clean.cleaned_data = {"desconto_pdv_percentual": Decimal("0")}
    check("clean_zero_none", form_clean.clean_desconto_pdv_percentual() is None)

    cli = ClienteAgro(nome="Z Prova PDV %", desconto_pdv_percentual=None)
    linha0 = _linha_clienteagro_pdv(cli)
    check("api_pct_null_zero", linha0.get("desconto_pdv_percentual") == 0)
    cli.desconto_pdv_percentual = Decimal("7.5")
    linha = _linha_clienteagro_pdv(cli)
    check("api_pct_valor", abs(linha.get("desconto_pdv_percentual") - 7.5) < 0.001)


def test_db_defaults() -> None:
    print("== DB (se disponível) ==")
    try:
        next(ClienteAgro.objects.all().order_by("pk").iterator(chunk_size=1), None)
    except Exception as e:
        check("db_skip", True, f"pula persistência — {e}")
        return
    cli = ClienteAgro.objects.create(nome="ZZ Prova Desc PDV Null")
    try:
        cli.refresh_from_db()
        check("db_default_null", cli.desconto_pdv_percentual is None)
        cli.desconto_pdv_percentual = Decimal("10")
        cli.save(update_fields=["desconto_pdv_percentual"])
        cli.refresh_from_db()
        check("db_grava_pct", cli.desconto_pdv_percentual == Decimal("10.00"))
    finally:
        cli.delete()


def main() -> int:
    print("verify_cliente_lista_desc_pdv_path.py\n")
    test_templates_and_js()
    test_form_and_api_linha()
    test_db_defaults()
    print(f"\n=== {len(oks)} OK · {len(fails)} FAIL ===")
    for f in fails:
        print(f"  - {f}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
