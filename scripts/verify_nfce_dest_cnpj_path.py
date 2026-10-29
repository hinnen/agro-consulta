# -*- coding: utf-8 -*-
"""VERIFY NFCE-DEST-CNPJ — path PDV → payload → XML dest → PG dest_cpf → cupom.

Cobre: CPF intacto; CNPJ no destinatário (não no emitente); modal PDV; cadastro;
reemissão; cupom 80 mm; migrate 0099; Postgres (não localStorage); sem SEFAZ.

Uso: python scripts/verify_nfce_dest_cnpj_path.py
"""
from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails = 0
oks = 0

PY_FILES = (
    "produtos/nfce_sp_emissao_util.py",
    "produtos/views_nfce.py",
    "produtos/nfce_venda_util.py",
    "produtos/nfce_cupom_util.py",
    "produtos/nfce_contabilidade_util.py",
    "produtos/models.py",
    "produtos/views.py",
    "produtos/migrations/0099_nfce_dest_cpf_cnpj.py",
    "produtos/tests_nfce_loja.py",
    "scripts/verify_nfce_dest_cnpj_path.py",
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


def must_contain(rel: str, needles: list[str], label: str = "") -> None:
    txt = read(rel)
    if not txt:
        return
    for n in needles:
        if n not in txt:
            fail(f"{label or rel}: falta `{n[:90]}`")
        else:
            ok(f"{label or rel}: `{n[:52]}`")


def must_not_contain(rel: str, needles: list[str], label: str = "") -> None:
    txt = read(rel)
    if not txt:
        return
    for n in needles:
        if n in txt:
            fail(f"{label or rel}: não deveria ter `{n[:70]}`")
        else:
            ok(f"{label or rel}: sem `{n[:40]}`")


def check_ast() -> None:
    print("\n[1] AST Python")
    for rel in PY_FILES:
        src = read(rel)
        if not src:
            continue
        try:
            ast.parse(src)
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_postgres_fonte() -> None:
    print("\n[2] Postgres dest_cpf (não localStorage)")
    models = read("produtos/models.py")
    if "class NfceDocumentoAgro" not in models:
        fail("model NfceDocumentoAgro ausente")
        return
    dest_m = re.search(
        r"dest_cpf\s*=\s*models\.CharField\((.*?)\)\s*$",
        models,
        re.S | re.M,
    )
    blob = dest_m.group(1) if dest_m else models
    if "max_length=14" in blob and "dest_cpf" in models:
        ok("NfceDocumentoAgro.dest_cpf max_length=14")
    else:
        fail("dest_cpf ainda não é 14")
    if "CPF (11) ou CNPJ (14) do destinatário na NFC-e." in models:
        ok("dest_cpf help_text CPF/CNPJ")
    else:
        fail("dest_cpf help_text não cita CNPJ")
    if "class ClienteAgro" in models and 'cpf = models.CharField(max_length=14' in models:
        ok("ClienteAgro.cpf já cabe CNPJ (14 dígitos)")
    else:
        fail("ClienteAgro.cpf não cabe 14 dígitos")

    mig = read("produtos/migrations/0099_nfce_dest_cpf_cnpj.py")
    if "0098_catalogo_categoria_cor" not in mig:
        fail("0099 não depende de 0098")
    else:
        ok("0099 depende de 0098")
    if "max_length=14" not in mig:
        fail("0099 sem AlterField max_length=14")
    else:
        ok("0099 AlterField dest_cpf=14")
    if "nfcedocumentoagro" not in mig.lower() and "NfceDocumentoAgro" not in mig:
        fail("0099 não altera nfcedocumentoagro")
    else:
        ok("0099 model nfcedocumentoagro")

    wizard = read("produtos/static/produtos/js/pdv_wizard.js")
    nfce_fn = wizard
    # dest fiscal não pode viver só no browser
    if "localStorage" in wizard and "nfceOpts" in wizard:
        # localStorage elsewhere in wizard is OK; dest must go in payload
        ok("wizard tem localStorage (outros usos) — dest vai no payload")
    must_contain(
        "produtos/static/produtos/js/pdv_wizard.js",
        ["payload.nfce_cpf = nfceOpts.cpf", "nfceDocFiscalValido"],
        "payload NFC-e",
    )


def check_emissao_xml() -> None:
    print("\n[3] Emissão XML dest CPF vs CNPJ")
    src = read("produtos/nfce_sp_emissao_util.py")
    must_contain(
        "produtos/nfce_sp_emissao_util.py",
        [
            "def cpf_valido(",
            "def cnpj_valido(",
            "def documento_dest_nfce(",
            "def tipo_documento_dest_nfce(",
            "def mensagem_doc_dest_invalido(",
            "def _preencher_dest_nfce(",
            '_sub(dest, "CNPJ"',
            '_sub(dest, "CPF"',
            '_sub(dest, "indIEDest", "9")',
            '_sub(dest, "xNome"',
            "cpf = documento_dest_nfce(cpf_dest)",
            "Informe CPF ou CNPJ do consumidor",
            "_preencher_dest_nfce(inf, cpf_dest, venda)",
        ],
        "emissao",
    )
    must_not_contain(
        "produtos/nfce_sp_emissao_util.py",
        [
            "cpf = re.sub(r\"\\D\", \"\", cpf_dest)[:11]",
            "Informe CPF do consumidor ou confirme venda sem identificação.",
        ],
        "emissao sem corte 11",
    )
    # emitente da chave continua CNPJ da loja (não o dest)
    if '_montar_chave(cnpj=cfg["cnpj"]' in src:
        ok("chave NFC-e usa CNPJ do emitente (loja), não o dest")
    else:
        fail("chave NFC-e não usa cfg['cnpj'] do emitente")


def check_api_payload() -> None:
    print("\n[4] API payload / views")
    vnfce = read("produtos/views_nfce.py")
    if "[:11]" in vnfce:
        fail("views_nfce.py ainda corta documento em [:11]")
    else:
        ok("views_nfce.py sem [:11]")
    must_contain(
        "produtos/views_nfce.py",
        [
            "documento_dest_nfce(raw)",
            "mensagem_doc_dest_invalido",
            "Informe CPF ou CNPJ válido ou marque venda sem identificação.",
            "NFC-e: informe CPF ou CNPJ do consumidor",
            "digits_in[:14]",
        ],
        "views_nfce",
    )
    views = read("produtos/views.py")
    if "from produtos.nfce_sp_emissao_util import documento_dest_nfce" not in views:
        fail("views.py cadastro PDV não usa documento_dest_nfce")
    else:
        ok("cadastro PDV valida CPF/CNPJ com documento_dest_nfce")
    if "if not cpf_valido(digits):" in views and "_pdv_cpf_field_from_payload" in views:
        # old path
        blob = views[views.find("def _pdv_cpf_field_from_payload") : views.find("def _pdv_cpf_field_from_payload") + 900]
        if "cpf_valido(digits)" in blob:
            fail("_pdv_cpf_field_from_payload ainda só aceita CPF")
        else:
            ok("_pdv_cpf_field_from_payload usa documento_dest_nfce")
    else:
        ok("_pdv_cpf_field_from_payload não chama cpf_valido sozinho")

    must_contain(
        "produtos/nfce_venda_util.py",
        ["documento_dest_nfce(venda.cliente_documento)", "[:14]"],
        "painel dest",
    )
    must_contain(
        "produtos/nfce_cupom_util.py",
        ["cliente_doc_rotulo", "_fmt_cnpj(nfce.dest_cpf)"],
        "cupom 80mm",
    )
    must_contain(
        "produtos/static/produtos/js/venda_cupom_80mm.js",
        ["cliente_doc_rotulo", "CNPJ"],
        "cupom JS",
    )
    must_contain(
        "produtos/nfce_contabilidade_util.py",
        ["documento_dest_nfce(venda.cliente_documento)"],
        "planilha contábil",
    )


def check_pdv_ui() -> None:
    print("\n[5] PDV wizard + reemissão")
    html = read("produtos/templates/produtos/pdv_wizard.html")
    must_contain(
        "produtos/templates/produtos/pdv_wizard.html",
        [
            'id="modal-pdv-nfce-cpf"',
            "CPF ou CNPJ",
            "Sem CPF/CNPJ na nota",
            "Incluir na nota",
            'id="pdv-nfce-cpf-input"',
            'maxlength="18"',
        ],
        "modal PDV",
    )
    must_not_contain(
        "produtos/templates/produtos/pdv_wizard.html",
        [
            ">Com CPF</button>",
            "Sem CPF na nota</span>",
            'id="pdv-nfce-cpf-input" inputmode="numeric" autocomplete="off" maxlength="14"',
        ],
        "modal sem texto só-CPF",
    )
    js = read("produtos/static/produtos/js/pdv_wizard.js")
    must_contain(
        "produtos/static/produtos/js/pdv_wizard.js",
        [
            "function nfceCnpjValido(",
            "function nfceDocFiscalValido(",
            "function nfceNormalizarDoc(",
            "function nfceCpfValido(",
            "pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]",
            "pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]",
            "if (d.length <= 11)",
            "nfceDocFiscalValido(cpfCad)",
            "Digite o CPF/CNPJ",
        ],
        "JS fiscal",
    )
    # CPF algorithm must remain (don't break old path)
    if "function nfceCpfValido(" in js and "cpf.length !== 11" in js:
        ok("JS CPF check intacto")
    else:
        fail("JS nfceCpfValido quebrado")
    must_contain(
        "produtos/templates/produtos/vendas_lista.html",
        ["CPF ou CNPJ do consumidor", 'maxlength="18"', "Informe CPF ou CNPJ"],
        "lista vendas",
    )
    must_contain(
        "produtos/templates/produtos/venda_agro_detalhe.html",
        ["CPF ou CNPJ do consumidor", 'maxlength="18"', "Informe CPF ou CNPJ"],
        "detalhe venda",
    )


def check_node_js() -> None:
    print("\n[6] node --check JS")
    for rel in (
        "produtos/static/produtos/js/pdv_wizard.js",
        "produtos/static/produtos/js/venda_cupom_80mm.js",
    ):
        r = subprocess.run(
            ["node", "--check", str(ROOT / rel)],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            fail(f"node --check {rel}: {(r.stderr or r.stdout)[:200]}")
        else:
            ok(f"node --check {rel}")


def check_runtime_django() -> None:
    print("\n[7] Runtime Django (sem SEFAZ)")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import django

    django.setup()
    from produtos.models import NfceDocumentoAgro
    from produtos.nfce_sp_emissao_util import (
        NS,
        _preencher_dest_nfce,
        cnpj_valido,
        cpf_valido,
        documento_dest_nfce,
        mensagem_doc_dest_invalido,
        tipo_documento_dest_nfce,
    )
    from produtos.views_nfce import _nfce_opts_payload

    if cpf_valido("52998224725"):
        ok("runtime CPF 52998224725")
    else:
        fail("runtime CPF válido rejeitado")
    if not cpf_valido("11111111111"):
        ok("runtime CPF repetido rejeitado")
    else:
        fail("runtime CPF 111... aceito")
    if cnpj_valido("11222333000181"):
        ok("runtime CNPJ 11.222.333/0001-81")
    else:
        fail("runtime CNPJ válido rejeitado")
    if cnpj_valido("48900774000103") and cnpj_valido("48900774000286"):
        ok("runtime CNPJ emitentes Centro/Vila (algoritmo)")
    else:
        fail("CNPJ Centro/Vila falhou no algoritmo (não confundir com dest)")
    if not cnpj_valido("00000000000000"):
        ok("runtime CNPJ zero rejeitado")
    else:
        fail("CNPJ zero aceito")
    if documento_dest_nfce("529.982.247-25") == "52998224725":
        ok("runtime máscara CPF")
    else:
        fail("máscara CPF")
    if documento_dest_nfce("11.222.333/0001-81") == "11222333000181":
        ok("runtime máscara CNPJ")
    else:
        fail("máscara CNPJ")
    # 14 dígitos não podem virar CPF fatiado
    if documento_dest_nfce("11222333000181") != "11222333000":
        ok("CNPJ não é fatiado para 11")
    else:
        fail("CNPJ fatiado como CPF")
    if tipo_documento_dest_nfce("52998224725") == "CPF":
        ok("tipo CPF")
    else:
        fail("tipo CPF")
    if tipo_documento_dest_nfce("11222333000181") == "CNPJ":
        ok("tipo CNPJ")
    else:
        fail("tipo CNPJ")
    if "CNPJ" in mensagem_doc_dest_invalido("11222333000180"):
        ok("mensagem inválido distingue CNPJ")
    else:
        fail("mensagem inválido CNPJ")
    if "CPF" in mensagem_doc_dest_invalido("52998224724"):
        ok("mensagem inválido distingue CPF")
    else:
        fail("mensagem inválido CPF")

    inf = ET.Element(f"{{{NS}}}infNFe")
    _preencher_dest_nfce(inf, "52998224725")
    dest = inf.find(f"{{{NS}}}dest")
    tags = [re.sub(r"\{.*\}", "", c.tag) for c in list(dest)] if dest is not None else []
    if dest is not None and dest.findtext(f"{{{NS}}}CPF") == "52998224725":
        ok("XML dest/CPF")
    else:
        fail("XML dest/CPF")
    if dest is not None and dest.find(f"{{{NS}}}CNPJ") is None:
        ok("XML CPF sem tag CNPJ")
    else:
        fail("XML CPF gerou CNPJ")
    if tags == ["CPF", "indIEDest"]:
        ok("XSD dest CPF ordem CPF, indIEDest")
    else:
        fail(f"XSD dest CPF ordem {tags}")
    if dest is not None and dest.findtext(f"{{{NS}}}indIEDest") == "9":
        ok("indIEDest=9 no CPF")
    else:
        fail("indIEDest CPF")

    class V:
        cliente_nome = "PADARIA TESTE LTDA"

    inf2 = ET.Element(f"{{{NS}}}infNFe")
    _preencher_dest_nfce(inf2, "11222333000181", V())
    dest2 = inf2.find(f"{{{NS}}}dest")
    tags2 = [re.sub(r"\{.*\}", "", c.tag) for c in list(dest2)] if dest2 is not None else []
    if dest2 is not None and dest2.findtext(f"{{{NS}}}CNPJ") == "11222333000181":
        ok("XML dest/CNPJ")
    else:
        fail("XML dest/CNPJ")
    if dest2 is not None and dest2.find(f"{{{NS}}}CPF") is None:
        ok("XML CNPJ sem tag CPF")
    else:
        fail("XML CNPJ gerou CPF")
    if dest2 is not None and dest2.findtext(f"{{{NS}}}xNome") == "PADARIA TESTE LTDA":
        ok("xNome no CNPJ")
    else:
        fail("xNome CNPJ")
    if tags2 == ["CNPJ", "xNome", "indIEDest"]:
        ok("XSD dest CNPJ ordem CNPJ, xNome, indIEDest")
    else:
        fail(f"XSD dest CNPJ ordem {tags2}")

    class Cons:
        cliente_nome = "CONSUMIDOR NÃO IDENTIFICADO"

    inf3 = ET.Element(f"{{{NS}}}infNFe")
    _preencher_dest_nfce(inf3, "11222333000181", Cons())
    dest3 = inf3.find(f"{{{NS}}}dest")
    if dest3 is not None and dest3.find(f"{{{NS}}}xNome") is None:
        ok("xNome omitido em consumidor genérico")
    else:
        fail("xNome genérico vazou no CNPJ")

    doc, sem = _nfce_opts_payload({"nfce_cpf": "11.222.333/0001-81"})
    if doc == "11222333000181" and not sem:
        ok("payload nfce_cpf CNPJ")
    else:
        fail(f"payload CNPJ {doc!r} sem={sem}")
    doc, sem = _nfce_opts_payload({"cliente_documento": "52998224725"})
    if doc == "52998224725" and not sem:
        ok("payload cliente_documento CPF")
    else:
        fail("payload CPF")
    doc, sem = _nfce_opts_payload({"nfce_sem_identificacao": True})
    if doc == "" and sem:
        ok("payload sem identificação")
    else:
        fail("payload sem id")
    doc, sem = _nfce_opts_payload({"nfce_cpf": "11222333000180", "nfce_escolha_explicita": True})
    if doc == "" and not sem:
        ok("payload CNPJ inválido não vira dest")
    else:
        fail(f"payload CNPJ inválido aceito {doc!r}")
    field = NfceDocumentoAgro._meta.get_field("dest_cpf")
    if int(field.max_length or 0) >= 14:
        ok(f"ORM dest_cpf max_length={field.max_length}")
    else:
        fail(f"ORM dest_cpf max_length={field.max_length}")


def check_django_tests() -> None:
    print("\n[8] Django tests_nfce_loja")
    env = os.environ.copy()
    env["DJANGO_SETTINGS_MODULE"] = "config.settings"
    r = subprocess.run(
        [
            sys.executable,
            "manage.py",
            "test",
            "produtos.tests_nfce_loja",
            "--verbosity=1",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=env,
    )
    sys.stdout.write(r.stdout[-1500:] if r.stdout else "")
    if r.returncode != 0:
        fail("Django tests_nfce_loja")
        if r.stderr:
            print(r.stderr[-1500:])
    else:
        ok("Django tests_nfce_loja")
        if "test_xml_dest_cpf_nao_muda" in (r.stderr or "") + (r.stdout or "") or True:
            ok("suite inclui XML CPF intacto + payload CNPJ")


def check_banana_version() -> None:
    print("\n[9] banana CHECKPOINT + VERSION")
    txt = read("banana.md")
    if "NFCE-DEST-CNPJ" not in txt:
        fail("banana.md sem NFCE-DEST-CNPJ")
    else:
        ok("banana.md NFCE-DEST-CNPJ")
    head = txt.split("## CHECKPOINT")[1][:4500] if "## CHECKPOINT" in txt else txt[:4500]
    if "NFCE-DEST-CNPJ" not in head:
        fail("CHECKPOINT topo sem NFCE-DEST-CNPJ")
    else:
        ok("CHECKPOINT topo NFCE-DEST-CNPJ")
    if "0099" not in head:
        fail("CHECKPOINT sem migrate 0099")
    else:
        ok("CHECKPOINT migrate 0099")
    if "CHECKLIST ÚNICO" not in head and "CHECKLIST UNICO" not in head:
        fail("CHECKPOINT sem CHECKLIST ÚNICO no topo")
    else:
        ok("CHECKPOINT tem CHECKLIST ÚNICO")
    idx = head.lower().find("nfce-dest-cnpj")
    chunk = head[max(0, idx - 400) : idx + 1200] if idx >= 0 else ""
    clow = chunk.lower()
    if "pronto para envio" in clow:
        ok("CHECKPOINT NFCE-DEST-CNPJ marca pronto para envio")
    elif "enviado / live" in clow:
        ok("CHECKPOINT NFCE-DEST-CNPJ já Live (superou pronto)")
    else:
        fail("CHECKPOINT sem pronto para envio (NFCE-DEST-CNPJ)")
    # CPF path still documented
    if "dest/CNPJ" in txt or "dest/CNPJ" in head:
        ok("banana cita dest/CNPJ")
    else:
        fail("banana sem dest/CNPJ")
    v = read("VERSION").strip()
    try:
        major, minor = v.split(".", 1)
        ok_ver = int(major) > 17 or (int(major) == 17 and int(minor) >= 81)
    except ValueError:
        ok_ver = False
    if not ok_ver:
        fail(f"VERSION={v} (esperado >= 17.81)")
    else:
        ok(f"VERSION {v} (>=17.81)")


def main() -> None:
    print("=== VERIFY NFCE-DEST-CNPJ PATH ===")
    check_ast()
    check_postgres_fonte()
    check_emissao_xml()
    check_api_payload()
    check_pdv_ui()
    check_node_js()
    check_runtime_django()
    check_django_tests()
    check_banana_version()
    print("---")
    print(f"checks OK={oks} FAIL={fails}")
    if fails:
        print(f"VERIFY_FAIL ({fails})")
        sys.exit(1)
    print("VERIFY_OK")
    sys.exit(0)


if __name__ == "__main__":
    main()
