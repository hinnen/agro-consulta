"""
Envelope SOAP para webservices NF-e/NFC-e 4.00 (SEFAZ estadual).

Layout 4.00: sem nfeCabecMsg no Header (obsoleto desde 2017) e sem wrapper
nfeAutorizacaoLote — só nfeDadosMsg no Body (PyNFe / NFePHP).
"""

from __future__ import annotations

import re

NS_NFE = "http://www.portalfiscal.inf.br/nfe"
NS_SOAP = "http://www.w3.org/2003/05/soap-envelope"

_SEFAZ_ERRO_TRANSIENTE_NEEDLES = (
    "connection refused",
    "connectionerror",
    "connecttimeout",
    "max retries exceeded",
    "failed to establish",
    "timed out",
    "timeout",
    "temporarily unavailable",
    "errno 111",
    "errno 110",
    "name or service not known",
    "httpsconnectionpool",
    "remotedisconnected",
    "connection reset",
    "broken pipe",
)


def sefaz_erro_transiente(mensagem: str) -> bool:
    """True se a falha parece rede/SEFAZ temporária (vale retry)."""
    e = (mensagem or "").strip().lower()
    if not e:
        return False
    return any(n in e for n in _SEFAZ_ERRO_TRANSIENTE_NEEDLES)


def normalizar_xml_envio(xml: str) -> str:
    """Remove declaração XML e espaços entre tags (rejeição 588)."""
    raw = (xml or "").strip()
    if raw.startswith("<?xml"):
        end = raw.find("?>")
        if end >= 0:
            raw = raw[end + 2 :].strip()
    return re.sub(r">\s+<", "><", raw)


def montar_envelope_nfe_dados_msg(wsdl_ns: str, dados_xml: str, metodo: str) -> tuple[str, dict[str, str]]:
    """
    Monta envelope SOAP 1.2 e headers HTTP.
    ``dados_xml`` = conteúdo fiscal (ex.: enviNFe completo).
    """
    inner = normalizar_xml_envio(dados_xml)
    soap = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
        f'xmlns:soap="{NS_SOAP}">'
        "<soap:Body>"
        f'<nfeDadosMsg xmlns="{wsdl_ns}">{inner}</nfeDadosMsg>'
        "</soap:Body></soap:Envelope>"
    )
    action = f"{wsdl_ns}/{metodo}"
    headers = {
        "Content-Type": f'application/soap+xml;charset=utf-8;action="{action}"',
        "Accept": "application/soap+xml; charset=utf-8;",
    }
    return soap, headers
