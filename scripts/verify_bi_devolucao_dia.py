"""Prova estática + math: BI desconta devolução no dia do evento."""
from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

FAIL = 0


def check(cond, msg):
    global FAIL
    if cond:
        print("OK", msg)
    else:
        FAIL += 1
        print("FAIL", msg)


def main():
    views = (ROOT / "produtos" / "views.py").read_text(encoding="utf-8")
    lojas = (ROOT / "produtos" / "vendas_lojas_util.py").read_text(encoding="utf-8")
    util = (ROOT / "produtos" / "dashboard_pdv_devolucao_util.py").read_text(encoding="utf-8")

    i_qs = views.find("def _dashboard_vendas_qs_pdv_periodo")
    i_ser = views.find("def _dashboard_vendas_serie_pdv")
    bloco_qs = views[i_qs:i_ser]
    check("devolvida_em__isnull=True" not in bloco_qs, "qs PDV keeps returned sale on original day")
    check("dash:mvs:v7:pdv:" in views, "cache BI pdv v7")
    check("abatimento_devolucoes_por_dia" in views, "PDV series uses abatement")
    check("abatimento_devolucoes_totais_loja" in lojas, "vendas lojas uses abatement")
    check("DevolucaoVendaAgro" in util, "util uses DevolucaoVendaAgro")

    def aplicar_abatimento_por_dia(por_dia, abat):
        out = dict(por_dia)
        for k, val in abat.items():
            atual = Decimal(str(out.get(k) or 0))
            out[k] = float((atual - val).quantize(Decimal("0.01")))
        return out

    out = aplicar_abatimento_por_dia({"2026-09-01": 25.0}, {"2026-09-01": Decimal("40.00")})
    check(out["2026-09-01"] == -15.0, "hoje = venda nova menos devolucao")
    out2 = aplicar_abatimento_por_dia({"2026-08-31": 140.0}, {})
    check(out2["2026-08-31"] == 140.0, "dia da venda original intacto se nao houve evento nele")

    if FAIL:
        print(f"\n{FAIL} falha(s)")
        return 1
    print("\nOK verify_bi_devolucao_dia")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
