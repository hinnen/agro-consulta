"""Prova NF-IMP-NO-BOLETO: imposto fora da NF entra no boleto do contas a pagar.

Caso da loja: nota 1.428,52 + 0,02 de imposto, 3 boletos de 476,18.
O POST manda o valor da parcela. Não nasce título separado de centavos.
VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import re
import sys
from decimal import Decimal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

CHECKS = 0


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"OK {msg}")


def _read(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def _fn(src: str, name: str) -> str:
    m = re.search(rf"function {name}\s*\([^)]*\)\s*\{{", src)
    if not m:
        fail(f"função {name} ausente")
    i = m.end() - 1
    depth = 0
    for j in range(i, len(src)):
        if src[j] == "{":
            depth += 1
        elif src[j] == "}":
            depth -= 1
            if depth == 0:
                return src[m.start() : j + 1]
    fail(f"função {name} sem fecho")
    return ""


def prova_fonte() -> None:
    html = _read("produtos/templates/produtos/entrada_nota.html")
    views = _read("produtos/views.py")
    coletar = _fn(html, "entradaNfeColetarParcelasManualFinanceiro")
    if "entradaNfeDecomporParcelasCombinadasCents" in coletar:
        fail("POST ainda tira o imposto da parcela")
    if "Math.round(combinedCents[i]) / 100" not in coletar:
        fail("POST não manda o valor que está na parcela")
    ok("POST manda o valor da parcela (com o imposto)")

    if "const valsDefault = splitCentavosEmNParcelas(totalPagarCents, nParc);" not in html:
        fail("prévia não divide nota + imposto")
    ok("prévia divide nota + imposto")

    if "Entra no valor de cada boleto" not in html:
        fail("ajuda ainda fala em título separado")
    ok("ajuda: imposto entra no boleto")

    ini = views.find("def api_entrada_nota_financeiro")
    fim = views.find("\ndef ", ini + 10)
    salvar = views[ini:fim]
    if "soma_parcelas_entrada_nf_aceita" not in salvar:
        fail("API não classifica a soma")
    if "if imposto_ja_nas_parcelas:" not in salvar or "valor_impostos_dec = None" not in salvar:
        fail("API ainda geraria título de imposto por cima do boleto")
    ok("API: imposto já na parcela não vira outro título")


def prova_caso_loja() -> None:
    import django

    django.setup()
    from produtos.nfe_entrada_util import soma_parcelas_entrada_nf_aceita

    nota = Decimal("1428.52")
    imposto = Decimal("0.02")
    boletos = [Decimal("476.18"), Decimal("476.18"), Decimal("476.18")]
    soma = sum(boletos, Decimal("0"))
    if soma != nota + imposto:
        fail(f"3×476,18 não fecha nota+imposto: {soma}")
    aceita, ja = soma_parcelas_entrada_nf_aceita(soma, nota, imposto)
    if not aceita or not ja:
        fail(f"caso loja recusado: {aceita} {ja}")
    ok("caso loja 1.428,52 + 0,02 = 3×476,18 entra no boleto")

    # Cliente antigo: parcelas só da nota + imposto à parte. Continua aceito, título separado.
    aceita_old, ja_old = soma_parcelas_entrada_nf_aceita(nota, nota, imposto)
    if not aceita_old or ja_old:
        fail(f"parcela só da nota mudou de regra: {aceita_old} {ja_old}")
    ok("parcela só da nota ainda aceita o imposto à parte")

    aceita_grande, ja_grande = soma_parcelas_entrada_nf_aceita(
        Decimal("105.00"), Decimal("100.00"), Decimal("5.00")
    )
    if not aceita_grande or not ja_grande:
        fail("imposto de 5 reais não entrou no boleto")
    ok("imposto maior (5,00) também entra no boleto")

    aceita_ruim, _ = soma_parcelas_entrada_nf_aceita(
        Decimal("90.00"), Decimal("100.00"), Decimal("0.02")
    )
    if aceita_ruim:
        fail("soma errada passou")
    ok("soma que não fecha é recusada")

    # 1 centavo em 2 boletos: um fica com o centavo, sem título de 0,01.
    aceita_1, ja_1 = soma_parcelas_entrada_nf_aceita(
        Decimal("10.01"), Decimal("10.00"), Decimal("0.01")
    )
    if not aceita_1 or not ja_1:
        fail("1 centavo em 2 boletos não entrou")
    ok("1 centavo fica no boleto")

    # Sem imposto, a soma da nota segue valendo.
    aceita_zero, ja_zero = soma_parcelas_entrada_nf_aceita(
        Decimal("100.00"), Decimal("100.00"), None
    )
    if not aceita_zero or ja_zero:
        fail("nota sem imposto quebrou")
    ok("nota sem imposto não muda")


def main() -> None:
    prova_fonte()
    prova_caso_loja()
    print(f"VERIFY_OK {CHECKS}")


if __name__ == "__main__":
    main()
