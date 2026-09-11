#!/usr/bin/env python
"""Prova NF-LOTE-XML — validade/lote do XML (prod/rastro) entram na etapa 4.

Caso loja 11/09: NF 269263 · 32 itens PEND. na etapa Lote mesmo com data na nota.
VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FAIL: list[str] = []
OK = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL.append(name + (f" — {detail}" if detail else ""))
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8").replace("\r\n", "\n")


def main() -> int:
    src = _read("produtos/nfe_entrada_util.py")
    js = _read("produtos/templates/produtos/entrada_nota.html")
    tests = _read("produtos/tests_entrada_nf_lote_xml.py")

    check("helper prod/rastro", "_nfe_preencher_lote_item" in src)
    check("helper olha prod e det", 'for parent in (prod, det)' in src)
    check("infAdProd fallback", "_nfe_aplicar_infadprod_lote" in src)
    check("parse chama helper", "_nfe_preencher_lote_item(item, det, prod)" in src)
    check("nao olha rastro so em det", "if _localname(child.tag) != \"rastro\":" not in src)
    check("JS aplica lote XML", "lote_xml" in js and "entradaNfeLoteDataParaInput" in js)
    check("JS confere se XML tem lote", "if (xml && (xml.lote_numero || xml.lote_fabricacao || xml.lote_validade))" in js)
    check("teste rastro em prod", "test_rastro_dentro_de_prod" in tests)
    check("teste infAdProd", "test_infadprod_quando_nao_tem_rastro" in tests)

    from produtos.nfe_entrada_util import parse_nfe_xml_bytes

    xml_prod = """<?xml version="1.0" encoding="UTF-8"?>
<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe">
  <NFe>
    <infNFe Id="NFe35260911111111000191550010002692631000000010" versao="4.00">
      <ide><nNF>269263</nNF><serie>1</serie></ide>
      <emit><CNPJ>11111111000191</CNPJ><xNome>Lab</xNome></emit>
      <dest><CNPJ>48900774000103</CNPJ></dest>
      <det nItem="1">
        <prod>
          <cProd>A1</cProd><cEAN>7898185261131</cEAN><xProd>ATROPINA</xProd>
          <NCM>30049099</NCM><CFOP>5405</CFOP><uCom>UN</uCom>
          <qCom>1</qCom><vUnCom>10</vUnCom><vProd>10</vProd>
          <rastro><nLote>L24015</nLote><dFab>2025-01-10</dFab><dVal>2027-09-30</dVal></rastro>
        </prod>
      </det>
      <total><ICMSTot><vProd>10.00</vProd><vNF>10.00</vNF></ICMSTot></total>
    </infNFe>
  </NFe>
</nfeProc>
""".encode("utf-8")
    parsed = parse_nfe_xml_bytes(xml_prod)
    it = (parsed.get("itens") or [{}])[0]
    check("parse ok", bool(parsed.get("ok")), str(parsed.get("erro") or ""))
    check("lote do prod/rastro", it.get("lote_numero") == "L24015")
    check("validade do prod/rastro", it.get("lote_validade") == "2027-09-30")
    check("lote_xml marcado", bool(it.get("lote_xml")))

    xml_inf = xml_prod.replace(
        b"<rastro><nLote>L24015</nLote><dFab>2025-01-10</dFab><dVal>2027-09-30</dVal></rastro>",
        b"",
    ).replace(
        b"</prod>",
        b"</prod><infAdProd>Lote: XYZ1 Validade: 31/12/2028</infAdProd>",
    )
    parsed_inf = parse_nfe_xml_bytes(xml_inf)
    it2 = (parsed_inf.get("itens") or [{}])[0]
    check("infAdProd lote", it2.get("lote_numero") == "XYZ1")
    check("infAdProd validade", it2.get("lote_validade") == "2028-12-31")

    print(f"\n{OK} ok · {len(FAIL)} falha")
    if FAIL:
        for f in FAIL:
            print(f"VERIFY_FAIL: {f}")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
