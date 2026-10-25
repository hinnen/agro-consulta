"""Validação obrigatória do XML NF-e 4.00 contra XSD PL 010f (produtos/schemas/nfe/PL_010f/)."""
from __future__ import annotations

import os
from pathlib import Path

from django.conf import settings

from produtos.nfe_saida_tributacao_util import FiscalBloqueio

_SCHEMA_PACK = "PL_010f"
_SCHEMA_DIR = Path(settings.BASE_DIR) / "produtos/schemas/nfe" / _SCHEMA_PACK
_SCHEMA_ENTRY = "nfe_v4.00.xsd"
_NS_NFE = "http://www.portalfiscal.inf.br/nfe"
_NS_DSIG = "http://www.w3.org/2000/09/xmldsig#"


def xsd_nfe_v4_path() -> Path | None:
    env = (os.environ.get("NFE_SAIDA_XSD_PATH") or "").strip()
    if env:
        p = Path(env)
        return p if p.is_file() else None
    for cand in (
        _SCHEMA_DIR / _SCHEMA_ENTRY,
        Path(settings.BASE_DIR) / "produtos/schemas/nfe" / _SCHEMA_ENTRY,
    ):
        if cand.is_file():
            return cand
    return None


def xsd_nfe_disponivel() -> bool:
    return xsd_nfe_v4_path() is not None


def _xml_para_validacao_xsd(xml_body: str) -> bytes:
    """XML não assinado recebe Signature stub; XML já assinado valida como está."""
    from lxml import etree

    parser = etree.XMLParser(resolve_entities=False)
    root = etree.fromstring(xml_body.encode("utf-8"), parser)
    if root.find(f".//{{{_NS_DSIG}}}Signature") is not None:
        return xml_body.encode("utf-8")
    inf = root.find(f".//{{{_NS_NFE}}}infNFe")
    inf_id = (inf.get("Id") if inf is not None else "") or "NFe"
    sig = etree.SubElement(root, f"{{{_NS_DSIG}}}Signature")
    si = etree.SubElement(sig, f"{{{_NS_DSIG}}}SignedInfo")
    cm = etree.SubElement(si, f"{{{_NS_DSIG}}}CanonicalizationMethod")
    cm.set("Algorithm", "http://www.w3.org/TR/2001/REC-xml-c14n-20010315")
    sm = etree.SubElement(si, f"{{{_NS_DSIG}}}SignatureMethod")
    sm.set("Algorithm", "http://www.w3.org/2000/09/xmldsig#rsa-sha1")
    ref = etree.SubElement(si, f"{{{_NS_DSIG}}}Reference")
    ref.set("URI", f"#{inf_id}")
    trans = etree.SubElement(ref, f"{{{_NS_DSIG}}}Transforms")
    for algo in (
        "http://www.w3.org/2000/09/xmldsig#enveloped-signature",
        "http://www.w3.org/TR/2001/REC-xml-c14n-20010315",
    ):
        tr = etree.SubElement(trans, f"{{{_NS_DSIG}}}Transform")
        tr.set("Algorithm", algo)
    dm = etree.SubElement(ref, f"{{{_NS_DSIG}}}DigestMethod")
    dm.set("Algorithm", "http://www.w3.org/2000/09/xmldsig#sha1")
    dv = etree.SubElement(ref, f"{{{_NS_DSIG}}}DigestValue")
    dv.text = "JAVH7tMQy9jfb9klsKLxwvL8xkQ="
    sv = etree.SubElement(sig, f"{{{_NS_DSIG}}}SignatureValue")
    sv.text = "AQID"
    ki = etree.SubElement(sig, f"{{{_NS_DSIG}}}KeyInfo")
    x509d = etree.SubElement(ki, f"{{{_NS_DSIG}}}X509Data")
    cert = etree.SubElement(x509d, f"{{{_NS_DSIG}}}X509Certificate")
    cert.text = "AQIDBA=="
    return etree.tostring(root, encoding="utf-8")


def validar_xml_nfe_xsd(xml_body: str) -> tuple[bool, str]:
    """
    Valida XML da NF-e contra PL 010f. Retorna (True, 'ok') ou (False, motivo).
    Ausência de XSD ou lxml é falha — não há skip silencioso.
    """
    xsd = xsd_nfe_v4_path()
    if not xsd:
        return (
            False,
            f"missing_xsd: pacote {_SCHEMA_PACK} não encontrado (produtos/schemas/nfe/{_SCHEMA_PACK}/)",
        )
    try:
        from lxml import etree
    except ImportError:
        return False, "missing_lxml: instale lxml para validação XSD"
    try:
        parser = etree.XMLParser(resolve_entities=False)
        schema_doc = etree.parse(str(xsd), parser)
        schema = etree.XMLSchema(schema_doc)
        doc = etree.fromstring(_xml_para_validacao_xsd(xml_body), parser)
        schema.assertValid(doc)
        return True, "ok"
    except etree.DocumentInvalid as exc:
        log = exc.error_log
        return False, f"xsd_invalid: {log.last_error.message if log.last_error else exc}"[:800]
    except Exception as exc:
        return False, f"xsd_error: {exc}"[:800]


def validar_xml_nfe_xsd_obrigatorio(xml_body: str) -> None:
    """Impede assinatura/transmissão se o XML não validar no XSD PL 010f."""
    ok, det = validar_xml_nfe_xsd(xml_body)
    if not ok:
        raise FiscalBloqueio(
            f"XML NF-e rejeitado na validação XSD ({det}). "
            f"Corrija os dados fiscais ou atualize o pacote em produtos/schemas/nfe/{_SCHEMA_PACK}/."
        )
