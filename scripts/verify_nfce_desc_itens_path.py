# -*- coding: utf-8 -*-
"""VERIFY NFCE-DESC-ITENS — bug loja #7 (desconto → cupom fiscal não sai).

Cobre: rateio centavos · SEFAZ 531 (soma vDesc itens = ICMSTot) · frete+desconto ·
sem desconto (sem tag) · vPag = vNF · cupom 80mm · AST · testes Django.

Uso: python scripts/verify_nfce_desc_itens_path.py
"""
from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
fails = 0
oks = 0

PY_FILES = (
    "produtos/nfce_sp_emissao_util.py",
    "produtos/nfce_cupom_util.py",
    "produtos/tests_nfce_loja.py",
    "scripts/verify_nfce_desc_itens_path.py",
)


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    global fails
    fails += 1
    print(f" FAIL {msg}")


def read(rel: str) -> str:
    p = ROOT / rel
    if not p.is_file():
        fail(f"ausente {rel}")
        return ""
    return p.read_text(encoding="utf-8")


def check_ast() -> None:
    print("\n[1] AST Python")
    for rel in PY_FILES:
        p = ROOT / rel
        if not p.is_file():
            fail(f"ausente {rel}")
            continue
        try:
            ast.parse(p.read_text(encoding="utf-8"))
            ok(f"ast {rel}")
        except SyntaxError as exc:
            fail(f"ast {rel}: {exc}")


def check_source_contracts() -> None:
    print("\n[2] Contratos no código")
    em = read("produtos/nfce_sp_emissao_util.py")
    for needle in (
        "def _ratear_valor_proporcional",
        "SEFAZ 531",
        'if v_desc_item > 0:',
        '_sub(prod, "vDesc"',
        "descontos_itens",
        "resto_desc",
    ):
        if needle in em:
            ok(f"emissao: `{needle[:48]}`")
        else:
            fail(f"emissao: falta `{needle}`")
    cup = read("produtos/nfce_cupom_util.py")
    if "desconto = max(0.0, round(subtotal_itens + frete - total, 2))" in cup:
        ok("cupom: desconto derivado do total")
    else:
        fail("cupom: desconto ainda fixo 0 ou fórmula ausente")
    if '"desconto": desconto' in cup or '"desconto": desconto,' in cup:
        ok("cupom: campo desconto dinâmico")
    else:
        fail("cupom: ainda desconto: 0.0")


def _montar_xml_caso(*, itens_vt: list[str], total: str, frete: str = "0") -> ET.Element:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from datetime import datetime

    from produtos.nfce_sp_emissao_util import NS, _montar_xml_nfce

    class Item:
        def __init__(self, vt: str, idx: int):
            self.quantidade = Decimal("1")
            self.valor_unitario = Decimal(vt)
            self.valor_total = Decimal(vt)
            self.codigo = f"GM{idx}"
            self.produto_id_externo = str(idx)
            self.descricao = f"PROD {idx}"
            self.unidade = "UN"

    total_d = Decimal(total)
    frete_d = Decimal(frete)

    class Venda:
        pk = 7
        total = total_d
        frete = frete_d
        pagamentos_json = [{"forma": "Dinheiro", "valor": float(total_d)}]
        cliente_nome = ""

    fiscal = {
        "ncm": "01012100",
        "cfop": "5102",
        "origem": "0",
        "csosn": "102",
        "cest": "",
    }
    itens = [Item(vt, i + 1) for i, vt in enumerate(itens_vt)]
    cfg = {
        "tp_amb": 2,
        "cnpj": "48900774000103",
        "razao_social": "TESTE",
        "fantasia": "TESTE",
        "logradouro": "RUA",
        "numero": "1",
        "bairro": "CENTRO",
        "cmun": "3524600",
        "cidade": "JACUPIRANGA",
        "uf": "SP",
        "cep": "11940000",
        "fone": "",
        "ie": "123",
        "csc_id": "1",
        "csc_token": "abc",
    }
    with patch("produtos.nfce_sp_emissao_util.ibpt_valor_item", return_value=Decimal("0")), patch(
        "produtos.nfce_sp_emissao_util.calcular_ibpt_venda_itens",
        return_value={"ibpt_texto": "Trib approx R$ 0,00"},
    ), patch(
        "produtos.nfce_sp_emissao_util._qr_code_url",
        return_value="https://example.com/qr",
    ):
        xml_body, _ = _montar_xml_nfce(
            cfg,
            Venda(),
            itens,
            serie=21,
            numero=1,
            chave="35" + "0" * 42,
            dh_emi=datetime(2026, 8, 29, 12, 0, 0),
            cpf_dest="",
            fiscal_itens=[fiscal] * len(itens),
        )
    return ET.fromstring(xml_body)


def _dec_txt(el: ET.Element | None) -> Decimal:
    if el is None or el.text is None:
        return Decimal("0")
    return Decimal(el.text)


def assert_531(root: ET.Element, label: str) -> None:
    ns = {"n": "http://www.portalfiscal.inf.br/nfe"}
    v_tot = _dec_txt(root.find(".//n:ICMSTot/n:vDesc", ns))
    itens = [_dec_txt(el) for el in root.findall(".//n:det/n:prod/n:vDesc", ns)]
    soma = sum(itens, Decimal("0"))
    if v_tot == soma:
        ok(f"{label}: 531 ok (tot={v_tot} soma={soma} n={len(itens)})")
    else:
        fail(f"{label}: 531 FAIL tot={v_tot} soma={soma} itens={itens}")
    v_nf = _dec_txt(root.find(".//n:ICMSTot/n:vNF", ns))
    v_prod = _dec_txt(root.find(".//n:ICMSTot/n:vProd", ns))
    v_frete = _dec_txt(root.find(".//n:ICMSTot/n:vFrete", ns))
    esperado = (v_prod - v_tot + v_frete).quantize(Decimal("0.01"))
    if esperado == v_nf:
        ok(f"{label}: vNF={v_nf} (= vProd-vDesc+vFrete)")
    else:
        fail(f"{label}: vNF {v_nf} != {esperado} (prod={v_prod} desc={v_tot} frete={v_frete})")
    pags = [_dec_txt(el) for el in root.findall(".//n:detPag/n:vPag", ns)]
    if abs(sum(pags, Decimal("0")) - v_nf) < Decimal("0.01"):
        ok(f"{label}: vPag soma={sum(pags, Decimal('0'))} = vNF")
    else:
        fail(f"{label}: vPag {pags} != vNF {v_nf}")
    # Ordem XSD: vDesc depois de vFrete (se houver) e antes de indTot
    for prod in root.findall(".//n:det/n:prod", ns):
        tags = [c.tag.split("}")[-1] for c in list(prod)]
        if "vDesc" in tags:
            if tags.index("vDesc") > tags.index("vUnTrib") and tags.index("vDesc") < tags.index("indTot"):
                ok(f"{label}: ordem vDesc ok")
            else:
                fail(f"{label}: ordem tags prod={tags}")
            break


def check_runtime_xml() -> None:
    print("\n[4] XML runtime (531 / vNF / vPag)")
    casos = [
        ("2 itens −10", ["50.00", "50.00"], "90.00", "0"),
        ("1 item −5,55", ["40.00"], "34.45", "0"),
        ("3 itens centavos", ["10.00", "10.00", "10.00"], "29.99", "0"),
        ("sem desconto", ["15.00", "25.00"], "40.00", "0"),
        ("frete+desc", ["100.00"], "105.00", "10.00"),  # itens 100 + frete 10 − desc 5
        ("total zero c/ frete", ["80.00", "20.00"], "0.00", "10.00"),
        ("desconto 1 cent", ["10.00", "10.00"], "19.99", "0"),
    ]
    for label, vts, total, frete in casos:
        try:
            root = _montar_xml_caso(itens_vt=vts, total=total, frete=frete)
            assert_531(root, label)
            ns = {"n": "http://www.portalfiscal.inf.br/nfe"}
            v_tot = _dec_txt(root.find(".//n:ICMSTot/n:vDesc", ns))
            n_tags = len(root.findall(".//n:det/n:prod/n:vDesc", ns))
            if v_tot == 0 and n_tags == 0:
                ok(f"{label}: sem tag vDesc nos itens (desc=0)")
            elif v_tot > 0 and n_tags >= 1:
                ok(f"{label}: tags vDesc presentes")
            elif v_tot > 0 and n_tags == 0:
                fail(f"{label}: desc={v_tot} sem tags nos itens (bug #7)")
            for el in root.findall(".//n:det/n:prod", ns):
                vp = _dec_txt(el.find("n:vProd", ns))
                vd = _dec_txt(el.find("n:vDesc", ns))
                if vd > vp:
                    fail(f"{label}: vDesc {vd} > vProd {vp}")
        except Exception as exc:
            fail(f"{label}: exceção {exc}")


def check_rateio_unit() -> None:
    print("\n[3] Rateio unitário")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from produtos.nfce_sp_emissao_util import _ratear_valor_proporcional

    cases = [
        ([Decimal("10"), Decimal("20"), Decimal("30")], Decimal("6")),
        ([Decimal("10"), Decimal("10")], Decimal("0.01")),
        ([Decimal("99.99")], Decimal("99.99")),
        ([Decimal("1"), Decimal("1"), Decimal("1")], Decimal("0.02")),
        ([], Decimal("5")),
        ([Decimal("10")], Decimal("0")),
        ([Decimal("5"), Decimal("5")], Decimal("100")),  # cap
    ]
    for pesos, tot in cases:
        partes = _ratear_valor_proporcional(pesos, tot)
        if not pesos:
            if partes == []:
                ok("rateio lista vazia")
            else:
                fail(f"rateio vazia → {partes}")
            continue
        soma = sum(partes, Decimal("0"))
        esperado = min(tot, sum(pesos, Decimal("0")))
        if tot <= 0:
            esperado = Decimal("0")
        if soma == esperado.quantize(Decimal("0.01")) or (tot <= 0 and soma == 0):
            ok(f"rateio {pesos} / {tot} -> {partes}")
        else:
            fail(f"rateio {pesos} / {tot} soma={soma} esperado={esperado} -> {partes}")
        for p, w in zip(partes, pesos):
            if p > w or p < 0:
                fail(f"rateio parte inválida {p} peso {w}")


def check_django_tests() -> None:
    print("\n[5] Django tests NfceDescontoRateio")
    r = subprocess.run(
        [
            sys.executable,
            "-m",
            "django",
            "test",
            "produtos.tests_nfce_loja.NfceDescontoRateioTests",
            "produtos.tests_nfce_loja.NfceDestDocumentoTests",
            "produtos.tests_nfce_loja.NfceLojaConfigTests",
            "-v1",
            "--settings=config.settings",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    out = (r.stdout or "") + (r.stderr or "")
    if r.returncode == 0 and "OK" in out:
        m = re.search(r"Ran (\d+) test", out)
        ok(f"django tests OK ({m.group(1) if m else '?'} casos)")
    else:
        fail(f"django tests rc={r.returncode}")
        print(out[-800:])


def check_regressao_bug7() -> None:
    print("\n[6] Regressão bug #7 (antes vs agora)")
    # Simula o XML “antigo”: ICMSTot com vDesc sem tags nos itens → 531
    fake = ET.fromstring(
        """<NFe xmlns="http://www.portalfiscal.inf.br/nfe">
        <infNFe><total><ICMSTot><vDesc>10.00</vDesc><vNF>90.00</vNF><vProd>100.00</vProd><vFrete>0.00</vFrete></ICMSTot></total>
        <det><prod><vProd>50.00</vProd></prod></det>
        <det><prod><vProd>50.00</vProd></prod></det>
        <pag><detPag><vPag>90.00</vPag></detPag></pag>
        </infNFe></NFe>"""
    )
    ns = {"n": "http://www.portalfiscal.inf.br/nfe"}
    v_tot = _dec_txt(fake.find(".//n:ICMSTot/n:vDesc", ns))
    soma = sum((_dec_txt(el) for el in fake.findall(".//n:det/n:prod/n:vDesc", ns)), Decimal("0"))
    if v_tot > 0 and soma == 0:
        ok("cenário legado reproduzido (tot>0, itens sem vDesc)")
    else:
        fail("cenário legado não montou")
    root = _montar_xml_caso(itens_vt=["50.00", "50.00"], total="90.00")
    v_tot2 = _dec_txt(root.find(".//n:ICMSTot/n:vDesc", ns))
    soma2 = sum((_dec_txt(el) for el in root.findall(".//n:det/n:prod/n:vDesc", ns)), Decimal("0"))
    if v_tot2 == Decimal("10.00") and soma2 == Decimal("10.00"):
        ok("fix atual: tot=soma=10.00")
    else:
        fail(f"fix atual falhou tot={v_tot2} soma={soma2}")


def main() -> int:
    print("VERIFY NFCE-DESC-ITENS (bug loja #7)")
    check_ast()
    check_source_contracts()
    check_rateio_unit()
    check_runtime_xml()
    check_regressao_bug7()
    check_django_tests()
    print(f"\n=== RESULTADO: {oks} OK · {fails} FAIL ===")
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
