#!/usr/bin/env python3
"""VERIFY NF-FISCAL-XML — NCM/CFOP/CSOSN do XML → grade + propagar catálogo."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

failures: list[str] = []


def ok(msg: str) -> None:
    print(f"  OK {msg}")


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        failures.append(msg)
        print(f"FAIL {msg}")


def main() -> int:
    from produtos.agro_produto_fiscal_defaults import cfop_venda_de_cfop_nf_entrada
    from produtos.nfe_entrada_util import parse_nfe_xml_bytes

    check(cfop_venda_de_cfop_nf_entrada("1102") == "5102", "cfop 1102 → 5102")
    check(cfop_venda_de_cfop_nf_entrada("5405") == "5405", "cfop saída mantém 5405")

    xml = b"""<?xml version="1.0"?><nfeProc xmlns="http://www.portalfiscal.inf.br/nfe"><NFe><infNFe><det nItem="1">
    <prod><NCM>3004.90.99</NCM><CFOP>1102</CFOP></prod>
    <imposto><ICMS><ICMSSN102><orig>2</orig><CSOSN>400</CSOSN></ICMSSN102></ICMS></imposto>
    </det></infNFe></NFe></nfeProc>"""
    it = parse_nfe_xml_bytes(xml)["itens"][0]
    check(it.get("ncm") == "30049099", "parse NCM dígitos")
    check(it.get("csosn") == "400", "parse CSOSN")
    check(it.get("origem") == "2", "parse origem")

    html = (ROOT / "produtos/templates/produtos/entrada_nota.html").read_text(encoding="utf-8")
    check('class="in-ncm"' in html, "grade hidden in-ncm")
    check("ncm: (tr.querySelector('.in-ncm')" in html, "coletarLinhas envia ncm")
    check("xml.ncm != null" in html, "merge XML mantém ncm")

    util = (ROOT / "produtos/nfe_entrada_util.py").read_text(encoding="utf-8")
    check("cfop_venda_de_cfop_nf_entrada" in util, "propagar usa cfop venda")
    check("atualizados_mongo" in util, "propagar sync mongo")
    check("isinstance(ov.cadastro_extras, dict)" in util, "overlay isinstance dict")
    check('origem="nf"' in util, "historico origem Entrada NF")
    check("if not igual:" in util, "nao grava overlay se fiscal igual")

    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check(
        "propagar_fiscal_nf_catalogo_entrada_nota(linhas, db=db, client_m=client)" in views,
        "views passa db/client fiscal",
    )

    if failures:
        print(f"\nVERIFY_FAILS={len(failures)}")
        return 1
    print("\nVERIFY_OK 14")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
