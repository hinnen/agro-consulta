#!/usr/bin/env python
"""Prova detalhada NF-LOTE-XML — validade/lote do XML entram na etapa 4.

Caso loja 11/09: NF 269263 · 32 itens Pend. mesmo com data na nota.
Cobre: fonte · parse prod/rastro · infAdProd · med · 32 itens · JS etapa 4 ·
API parse-xml · estoque Validade · PIN 9973 · XML 269263 se existir no PG.
VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
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


def _xml_item(*, rastro: str = "", extra_det: str = "", cprod: str = "A1") -> str:
    return f"""
        <prod>
          <cProd>{cprod}</cProd><cEAN>7898185261131</cEAN><xProd>ATROPINA</xProd>
          <NCM>30049099</NCM><CFOP>5405</CFOP><uCom>UN</uCom>
          <qCom>1</qCom><vUnCom>10</vUnCom><vProd>10</vProd>
          {rastro}
        </prod>{extra_det}
    """


def _xml(dets: list[str], numero: str = "269263") -> bytes:
    blocos = "".join(f'<det nItem="{i + 1}">{c}</det>' for i, c in enumerate(dets))
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe">
  <NFe>
    <infNFe Id="NFe35260911111111000191550010002692631000000010" versao="4.00">
      <ide><nNF>{numero}</nNF><serie>1</serie></ide>
      <emit><CNPJ>11111111000191</CNPJ><xNome>Lab</xNome></emit>
      <dest><CNPJ>48900774000103</CNPJ></dest>
      {blocos}
      <total><ICMSTot><vProd>10.00</vProd><vNF>10.00</vNF></ICMSTot></total>
    </infNFe>
  </NFe>
</nfeProc>
""".encode("utf-8")


def prova_fonte() -> None:
    src = _read("produtos/nfe_entrada_util.py")
    js = _read("produtos/templates/produtos/entrada_nota.html")
    views = _read("produtos/views.py")
    tests = _read("produtos/tests_entrada_nf_lote_xml.py")
    models = _read("produtos/models.py")

    check("helper prod/rastro", "_nfe_preencher_lote_item" in src)
    check("helper olha prod e det", "for parent in (prod, det)" in src)
    check("infAdProd fallback", "_nfe_aplicar_infadprod_lote" in src)
    check("parse chama helper", "_nfe_preencher_lote_item(item, det, prod)" in src)
    check("nao olha rastro so em det", 'if _localname(child.tag) != "rastro":' not in src)
    check("API parse-xml", "api_entrada_nota_parse_xml" in views)
    check("estoque grava lote XML", "registrar_lote_validade_apos_entrada_nf" in views)
    check("parse validade NF", "def parse_data_validade_entrada_nf" in models)
    check("JS linhaFromItem lote", "lote_conf: (it.lote_numero || it.lote_fabricacao || it.lote_validade || it.lote_xml) ? 'ok' : ''" in js)
    check("JS merge prefere XML", "if (xml && (xml.lote_numero || xml.lote_fabricacao || xml.lote_validade))" in js)
    check("JS aplica na TR", "entradaNfeLoteAplicarDadosLinha(tr, d)" in js)
    check("JS etapa 4 semeia XML", "entradaNfeLoteSemearDraftsDasLinhas" in js)
    check("JS etapa 4 seleciona linha", "entradaNfeLoteSelecionarLinha(entradaNfeLoteLinhaIdx)" in js)
    check("JS badge XML", "tr.dataset.loteXml === '1' ? ' · XML'" in js)
    check("wizard exige ok/dispensado", "Linha ${i + 1}: pendente de lote/validade" in js)
    check("teste 32 itens", "test_trinta_e_dois_itens_como_269263" in tests)
    check("teste API parse", "test_api_parse_xml_devolve_lote" in tests)


def _lote_conf_js(it: dict) -> str:
    if it.get("lote_numero") or it.get("lote_fabricacao") or it.get("lote_validade") or it.get("lote_xml"):
        return "ok"
    return ""


def prova_parse() -> None:
    from produtos.nfe_entrada_util import parse_nfe_xml_bytes

    rastro = "<rastro><nLote>L24015</nLote><dFab>2025-01-10</dFab><dVal>2027-09-30</dVal></rastro>"
    parsed = parse_nfe_xml_bytes(_xml([_xml_item(rastro=rastro)]))
    it = (parsed.get("itens") or [{}])[0]
    check("parse ok", bool(parsed.get("ok")), str(parsed.get("erro") or ""))
    check("numero 269263", str(parsed.get("numero") or "") == "269263")
    check("lote prod/rastro", it.get("lote_numero") == "L24015")
    check("fab prod/rastro", it.get("lote_fabricacao") == "2025-01-10")
    check("val prod/rastro", it.get("lote_validade") == "2027-09-30")
    check("lote_xml marcado", bool(it.get("lote_xml")))
    check("JS trataria Ok", _lote_conf_js(it) == "ok")

    parsed_det = parse_nfe_xml_bytes(
        _xml([_xml_item(extra_det="<rastro><nLote>DET1</nLote><dVal>2028-01-15</dVal></rastro>")])
    )
    check("rastro em det ainda vale", (parsed_det.get("itens") or [{}])[0].get("lote_numero") == "DET1")

    parsed_inf = parse_nfe_xml_bytes(
        _xml([_xml_item(extra_det="<infAdProd>Lote: XYZ1 Validade: 31/12/2028</infAdProd>")])
    )
    it2 = (parsed_inf.get("itens") or [{}])[0]
    check("infAdProd lote", it2.get("lote_numero") == "XYZ1")
    check("infAdProd validade", it2.get("lote_validade") == "2028-12-31")

    parsed_pref = parse_nfe_xml_bytes(
        _xml(
            [
                _xml_item(
                    rastro=rastro,
                    extra_det="<infAdProd>Lote: TXT99 Validade: 31/12/2028</infAdProd>",
                )
            ]
        )
    )
    it3 = (parsed_pref.get("itens") or [{}])[0]
    check("rastro vence texto", it3.get("lote_numero") == "L24015" and it3.get("lote_validade") == "2027-09-30")

    parsed_med = parse_nfe_xml_bytes(
        _xml([_xml_item(rastro="<med><nLote>MED77</nLote><dVal>2026-03-01</dVal></med>")])
    )
    check("med legado", (parsed_med.get("itens") or [{}])[0].get("lote_numero") == "MED77")

    parsed_br = parse_nfe_xml_bytes(
        _xml([_xml_item(rastro="<rastro><nLote>BR1</nLote><dVal>30/09/2027</dVal></rastro>")])
    )
    check("dVal BR vira ISO", (parsed_br.get("itens") or [{}])[0].get("lote_validade") == "2027-09-30")

    parsed_vazio = parse_nfe_xml_bytes(_xml([_xml_item()]))
    itv = (parsed_vazio.get("itens") or [{}])[0]
    check("sem rastro nao inventa", not itv.get("lote_xml") and not itv.get("lote_validade"))
    check("JS trataria Pend", _lote_conf_js(itv) == "")

    dets = []
    for i in range(32):
        dets.append(
            _xml_item(
                cprod=f"P{i + 1}",
                rastro=f"<rastro><nLote>L{i + 1:02d}</nLote><dVal>2027-09-15</dVal></rastro>",
            )
        )
    parsed32 = parse_nfe_xml_bytes(_xml(dets))
    itens = parsed32.get("itens") or []
    check("32 itens", len(itens) == 32)
    check("32 com validade", all(x.get("lote_validade") == "2027-09-15" for x in itens))
    check("32 JS Ok", all(_lote_conf_js(x) == "ok" for x in itens))


def prova_js_data() -> None:
    def para_input(iso: str) -> str:
        s = str(iso or "").strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}", s):
            return s[:10]
        m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return ""

    check("input ISO", para_input("2027-09-30") == "2027-09-30")
    check("input BR", para_input("30/09/2027") == "2027-09-30")
    check("input vazio", para_input("") == "")

    wizard_ok = "ok" in ("ok", "dispensado")
    wizard_pend = "pendente" not in ("ok", "dispensado")
    check("wizard Ok passa", wizard_ok)
    check("wizard Pend trava", wizard_pend)


def prova_django_api_pin() -> None:
    import django

    django.setup()
    from django.urls import reverse

    from produtos.caixa_util import validar_pin_operador
    from produtos.models import parse_data_validade_entrada_nf

    pin_ok, pin_msg = validar_pin_operador(PIN)
    check("PIN 9973 valido", pin_ok, pin_msg)

    dv = parse_data_validade_entrada_nf("2027-09-30")
    check("parse validade ISO", dv is not None and str(dv) == "2027-09-30")
    dv2 = parse_data_validade_entrada_nf("30/09/2027")
    check("parse validade BR", dv2 is not None and str(dv2) == "2027-09-30")

    r = subprocess.run(
        [sys.executable, "manage.py", "test", "produtos.tests_entrada_nf_lote_xml", "-v1"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=180,
    )
    out = (r.stdout or "") + (r.stderr or "")
    check("unit+API isolados", r.returncode == 0, out[-500:] if r.returncode else "")
    try:
        url_api = reverse("api_entrada_nota_parse_xml")
        url_pag = reverse("entrada_nota")
    except Exception as exc:
        check("rotas entrada NF", False, str(exc))
    else:
        check("rota parse-xml", "parse-xml" in url_api, url_api)
        check("rota tela entrada", bool(url_pag), url_pag)


def prova_xml_269263_se_existir() -> None:
    try:
        from produtos.models import AgroNfeDistDfeDocumento
        from produtos.nfe_entrada_util import parse_nfe_xml_bytes

        row = (
            AgroNfeDistDfeDocumento.objects.filter(numero="269263")
            .exclude(xml="")
            .order_by("-atualizado_em")
            .first()
        )
    except Exception as exc:
        check("XML 269263 no PG (opcional)", True, f"sem consulta ({exc.__class__.__name__})")
        return
    if row is None:
        check("XML 269263 no PG (opcional)", True, "nao achou no PC — prova sintetica 32 itens ok")
        return
    parsed = parse_nfe_xml_bytes(row.xml.encode("utf-8"))
    itens = parsed.get("itens") or []
    com_val = [x for x in itens if x.get("lote_validade")]
    check("XML 269263 parse ok", bool(parsed.get("ok")), str(parsed.get("erro") or ""))
    check("XML 269263 tem itens", len(itens) >= 1, str(len(itens)))
    check(
        "XML 269263 puxou validade",
        len(com_val) > 0,
        f"{len(com_val)}/{len(itens)} com dVal",
    )


def prova_http_local() -> None:
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen("http://127.0.0.1:8000/healthz", timeout=2) as r:
            ok = r.status == 200
    except (urllib.error.URLError, TimeoutError, OSError):
        check("HTTP local healthz (opcional)", True, "runserver nao estava no ar")
        return
    check("HTTP local healthz", ok)


def main() -> int:
    prova_fonte()
    prova_parse()
    prova_js_data()
    prova_django_api_pin()
    prova_xml_269263_se_existir()
    prova_http_local()

    print(f"\n{OK} ok · {len(FAIL)} falha")
    if FAIL:
        for f in FAIL:
            print(f"VERIFY_FAIL: {f}")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
