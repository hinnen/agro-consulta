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

    if re.search(r"<NFe\b", raw) and not re.search(r'<NFe\s[^>]*\sxmlns="', raw):
        raw = re.sub(r"<NFe\b", f'<NFe xmlns="{NS_NFE}"', raw, count=1)

    if re.search(r"<Signature\b", raw):
        head = raw.split("<Signature", 1)[1].split(">", 1)[0]
        if 'xmlns="' not in head:
            raw = re.sub(r"<Signature\b", f'<Signature xmlns="{NS_DSIG}"', raw, count=1)

    raw = normalizar_xml_envio(raw)
    etree.fromstring(raw.encode("utf-8"))
    return raw
