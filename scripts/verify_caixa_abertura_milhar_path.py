# -*- coding: utf-8 -*-
"""Prova path CAIXA-ABERTURA-MILHAR (bug loja #25).

Cédulas grava 1.500,00 (pt-BR). O parse antigo fazia replace(',', '.') → 1.500.00
e o Decimal falhava → abertura R$ 0,00.

  python scripts/verify_caixa_abertura_milhar_path.py
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.views import _decimal_br_post

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print("  OK  " + name + ((" — " + detail) if detail else ""))
    else:
        fails.append(name)
        print("  FAIL " + name + ((" — " + detail) if detail else ""))


def _parse_antigo_quebrado(raw: str) -> Decimal:
    """Espelho do bug: milhar BR vira InvalidOperation → 0."""
    s = (raw or "0").replace(",", ".").strip()
    try:
        return Decimal(s).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


def test_parse() -> None:
    print("== Parse BR abertura ==")
    casos = [
        ("1.500,00", Decimal("1500.00")),
        ("1.234,56", Decimal("1234.56")),
        ("500,00", Decimal("500.00")),
        ("500", Decimal("500")),
        ("0,00", Decimal("0.00")),
        ("12.345,67", Decimal("12345.67")),
    ]
    for raw, esperado in casos:
        antigo = _parse_antigo_quebrado(raw)
        novo = _decimal_br_post(raw, "0").quantize(Decimal("0.01"))
        if "," in raw and "." in raw:
            check(
                f"antigo_zera_{raw}",
                antigo == Decimal("0"),
                f"antigo={antigo}",
            )
        check(f"novo_{raw}", novo == esperado, f"got={novo}")


def test_arquivos() -> None:
    print("== Arquivos ==")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    html = (ROOT / "produtos/templates/produtos/caixa_abrir.html").read_text(
        encoding="utf-8"
    )
    # Trecho da abertura do caixa (evitar falso positivo em outras views)
    idx = views.find("def caixa_abrir")
    check("view_caixa_abrir", idx > 0)
    trecho = views[idx : idx + 9000] if idx > 0 else ""
    check(
        "usa_decimal_br_post",
        "_decimal_br_post(raw_abertura" in trecho or "_decimal_br_post(raw_abertura," in trecho,
    )
    check("bloqueia_vazio_server", "campo vazio não abre o caixa" in trecho)
    check(
        "sem_replace_cego",
        'valor_abertura") or "0").replace(",", ".")' not in trecho,
    )
    check("bloqueia_vazio_js", "Informe o valor em gaveta" in html)


def main() -> int:
    test_parse()
    test_arquivos()
    print()
    print(f"Resultado: {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("FALHAS:", ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
