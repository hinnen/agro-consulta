"""
Serialização XML fiscal SEFAZ — sem prefixos de namespace (rejeição 404/587).

ElementTree e signxml costumam emitir ns0:, ds: etc.; a SEFAZ exige xmlns padrão.
"""

from __future__ import annotations

import re

from lxml import etree

from produtos.sefaz_soap_util import normalizar_xml_envio

NS_NFE = "http://www.portalfiscal.inf.br/nfe"
NS_DSIG = "http://www.w3.org/2000/09/xmldsig#"


def _abertura_tag(xml: str, tag: str) -> str:
    m = re.search(rf"<{tag}\b[^>]*>", xml)
    return m.group(0) if m else ""


def _tag_tem_xmlns(abertura: str) -> bool:
    return 'xmlns="' in abertura or "xmlns='" in abertura or "xmlns:" in abertura


def _dedup_xmlns_abertura(abertura: str) -> str:
    """Remove xmlns duplicado na mesma tag de abertura."""
    if abertura.count('xmlns="') <= 1:
        return abertura
    out = abertura
    first = True
    for match in re.finditer(r'\sxmlns="[^"]*"', abertura):
        if first:
            first = False
            continue
        out = out.replace(match.group(0), "", 1)
    return out


def _garantir_xmlns_tag(xml: str, tag: str, ns: str) -> str:
    abertura = _abertura_tag(xml, tag)
    if not abertura:
        return xml
    if not _tag_tem_xmlns(abertura):
        nova = re.sub(rf"<{tag}\b", f'<{tag} xmlns="{ns}"', abertura, count=1)
    else:
        nova = _dedup_xmlns_abertura(abertura)
    if nova != abertura:
        xml = xml.replace(abertura, nova, 1)
    return xml


def tostring_sem_prefixos(xml_in: str | etree._Element) -> str:
    """Converte XML fiscal para string sem prefixos (NFe + Signature)."""
    if isinstance(xml_in, etree._Element):
        raw = etree.tostring(xml_in, encoding="unicode", xml_declaration=False)
    else:
        raw = str(xml_in or "")

    raw = normalizar_xml_envio(raw)
    raw = re.sub(r'\sxmlns:ns\d+="[^"]*"', "", raw)
    raw = re.sub(r'\sxmlns:ds="[^"]*"', "", raw)
    raw = re.sub(r"<(/?)ns\d+:", r"<\1", raw)
    raw = re.sub(r"<(/?)ds:", r"<\1", raw)

    raw = _garantir_xmlns_tag(raw, "NFe", NS_NFE)
    raw = _garantir_xmlns_tag(raw, "evento", NS_NFE)
    raw = _garantir_xmlns_tag(raw, "envEvento", NS_NFE)
    raw = _garantir_xmlns_tag(raw, "Signature", NS_DSIG)

    raw = normalizar_xml_envio(raw)
    etree.fromstring(raw.encode("utf-8"))
    return raw
