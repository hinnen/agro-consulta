"""
ETQ-EAN-LOJA: código 230… novo com DV EAN-13; legado reconhecido sem migrar.
python scripts/verify_etq_ean_loja_path.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from produtos.agro_codigo_barras_loja_util import (  # noqa: E402
    eh_codigo_barras_loja,
    ean13_checksum_ok,
    ean13_digito_verificador,
    formatar_codigo_barras_loja,
    parsear_seq_codigo_barras_loja,
    _seqs_para_max_alocacao,
)

n = 0
fail = 0


def ok(cond: bool, msg: str) -> None:
    global n, fail
    n += 1
    if not cond:
        fail += 1
        print("FAIL", msg)


def main() -> int:
    legado = "2300000001571"
    ok(eh_codigo_barras_loja(legado), "legado eh loja")
    ok(not ean13_checksum_ok(legado), "legado DV invalido")
    ok(parsear_seq_codigo_barras_loja(legado) == 1571, "legado seq 1571")

    novo = formatar_codigo_barras_loja(1572)
    ok(len(novo) == 13 and novo.startswith("230"), "novo 13 digitos 230")
    ok(ean13_checksum_ok(novo), f"novo DV ok {novo}")
    ok(parsear_seq_codigo_barras_loja(novo) == 1572, f"novo seq 1572 got {parsear_seq_codigo_barras_loja(novo)}")

    d12 = "230000001572"
    ok(ean13_digito_verificador(d12) == int(novo[-1]), "formatar usa DV correto")

    # Acidentalmente válido (legado 1570): max deve considerar 1570 e 157
    lucky = "2300000001570"
    ok(ean13_checksum_ok(lucky), "1570 legado cai DV ok por acaso")
    seqs = _seqs_para_max_alocacao(lucky)
    ok(1570 in seqs and 157 in seqs, f"max candidatos lucky {seqs}")

    ok(not eh_codigo_barras_loja("7898752405197"), "789 nao e loja")
    ok(not eh_codigo_barras_loja("0120125412229"), "012 nao e loja")

    print("FAIL" if fail else "OK", f"{n - fail}/{n}" if fail else f"{n}/{n}")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
