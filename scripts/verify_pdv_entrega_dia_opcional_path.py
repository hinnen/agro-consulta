#!/usr/bin/env python3
"""PDV-ENT-DIA-OPCIONAL — dia da entrega opcional no fechamento."""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ.setdefault("SECRET_KEY", "verify-pdv-ent-dia")

import django

django.setup()

from produtos.entrega_pdv_pendente_util import parse_data_prevista_entrega

PASS = 0
FAIL = 0


def check(ok: bool, msg: str) -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  OK  {msg}")
    else:
        FAIL += 1
        print(f" FAIL {msg}")


def main() -> int:
    html = (ROOT / "produtos/templates/produtos/partials/pdv/entrega_wizard_overlay.html").read_text(
        encoding="utf-8"
    )
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    util = (ROOT / "produtos/entrega_pdv_pendente_util.py").read_text(encoding="utf-8")
    model = (ROOT / "produtos/models.py").read_text(encoding="utf-8")
    mig = (ROOT / "produtos/migrations/0133_pedido_entrega_data_prevista.py").read_text(encoding="utf-8")

    print("=== entrega dia opcional ===")
    check('id="pdv-ed-dia-opcoes"' in html, "botões Hoje / Amanhã / Outro dia")
    check('value="hoje"' in html and 'value="amanha"' in html and 'value="outro"' in html, "3 opções")
    check('id="pdv-entrega-dia-outro"' in html and 'type="date"' in html, "data só em Outro dia")
    check("commitEntregaDiaOpcao" in js and "data_prevista:" in js, "JS grava data_prevista")
    check("dia && dia > hoje) return 0" in js, "alerta não dispara em dia futuro")
    check("data_prevista" in model and "data_prevista" in mig, "campo no model + migrate 0133")
    check("Q(data_prevista__isnull=True)" in util, "caixa de hoje não trava dia futuro")
    check("data_prevista__gte=hoje" in util, "pagas na loja ficam até o dia combinado")

    hoje = date(2026, 9, 29)
    vazio, err = parse_data_prevista_entrega("", hoje)
    check(vazio is None and err == "", "vazio = hoje (não grava)")
    mesmo, err2 = parse_data_prevista_entrega("2026-09-29", hoje)
    check(mesmo is None and err2 == "", "hoje explícito = não grava")
    amanha, err3 = parse_data_prevista_entrega("2026-09-30", hoje)
    check(amanha == hoje + timedelta(days=1) and err3 == "", "amanhã grava")
    br, err4 = parse_data_prevista_entrega("02/10/2026", hoje)
    check(br == date(2026, 10, 2) and err4 == "", "data BR grava")
    passado, err5 = parse_data_prevista_entrega("2026-09-28", hoje)
    check(passado is None and "passado" in err5, "passado recusa")
    ruim, err6 = parse_data_prevista_entrega("semana que vem", hoje)
    check(ruim is None and err6 != "", "texto inválido recusa")

    print(f"\n{PASS} ok · {FAIL} fail")
    if FAIL:
        print("FAILED")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
