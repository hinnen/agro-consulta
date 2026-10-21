#!/usr/bin/env python3
"""Prova matemática: rateio manual desconto/acréscimo (mesma regra da etapa 2 Entrada NF)."""
from __future__ import annotations


def round_half_up(x: float, d: int = 6) -> float:
    from decimal import Decimal, ROUND_HALF_UP

    q = Decimal("1").scaleb(-d)
    return float(Decimal(str(x)).quantize(q, rounding=ROUND_HALF_UP))


def unit_com_rateios(
    base: float,
    q: float,
    vp: float,
    soma_prod: float,
    acresc_xml: float,
    incluir_xml: bool,
    manual_tipo: str,
    manual_valor: float,
) -> float:
    if not (q > 0 and vp > 0 and soma_prod > 0):
        return base
    line_val = vp
    if incluir_xml and acresc_xml > 0:
        line_val += acresc_xml * (vp / soma_prod)
    if manual_tipo in ("desconto", "acrescimo") and manual_valor > 0:
        sh = manual_valor * (vp / soma_prod)
        if manual_tipo == "desconto":
            line_val = max(0.0, line_val - sh)
        else:
            line_val += sh
    return round_half_up(line_val / q, 6)


def main() -> int:
    # 2 linhas: A 100 @ q=2 (vp=200), B 50 @ q=1 (vp=50) → soma 250
    soma = 250.0
    u_a = unit_com_rateios(10.0, 2, 200, soma, 0, False, "desconto", 25.0)
    u_b = unit_com_rateios(50.0, 1, 50, soma, 0, False, "desconto", 25.0)
    assert abs(u_a * 2 + u_b * 1 - (200 + 50 - 25)) < 0.02, (u_a, u_b)

    # desconto maior que a linha zera o total da linha (não negativo)
    u_c = unit_com_rateios(1.0, 1, 1, 100, 0, False, "desconto", 100.0)
    assert u_c == 0.0

    # XML + manual empilham no valor da linha
    u_d = unit_com_rateios(10.0, 1, 100, 100, 10.0, True, "acrescimo", 10.0)
    assert abs(u_d - 120.0) < 0.01

    # proporcional: metade do acrescimo na linha com 50% do soma
    u_e = unit_com_rateios(5.0, 10, 50, 100, 0, False, "acrescimo", 10.0)
    assert abs(u_e * 10 - 55.0) < 0.02

    print("VERIFY_OK nf_ajuste_manual_rateio")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
