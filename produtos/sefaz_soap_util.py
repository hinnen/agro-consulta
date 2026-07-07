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
    "403",
    "forbidden",
    "access is denied",
)

# Perfil sync (HTTP do PDV/reemitir): cabe no timeout ~30s do Render.
SEFAZ_HTTP_TIMEOUT_SYNC: tuple[int, int] = (6, 35)
SEFAZ_HTTP_RETRY_DELAYS_SYNC: tuple[float, ...] = (0.3, 0.7, 1.5)
# Perfil completo (thread background): mais tentativas.
SEFAZ_HTTP_TIMEOUT: tuple[int, int] = (8, 45)
SEFAZ_HTTP_RETRY_DELAYS_S: tuple[float, ...] = (0.5, 1.5, 3.0, 5.0)


def sefaz_http_status_retry(status_code: int) -> bool:
    """HTTP da SEFAZ que costuma ser instabilidade temporária."""
    return status_code in (403, 408, 429, 502, 503, 504) or status_code >= 500


def sanitizar_erro_http_sefaz(status_code: int, corpo: str) -> str:
    """Mensagem legível para operador — sem HTML bruto da SEFAZ/IIS."""
    if status_code == 403:
        return (
            "SEFAZ SP recusou a conexão (403 Forbidden). "
            "Instabilidade ou bloqueio temporário — aguarde alguns minutos e reemita."
        )
    if status_code == 429:
        return "SEFAZ SP limitou tentativas (429). Aguarde alguns minutos e reemita."
    if status_code >= 500:
        return f"SEFAZ SP indisponível (HTTP {status_code}). Aguarde alguns minutos e reemita."
    txt = re.sub(r"<[^>]+>", " ", corpo or "")
    txt = re.sub(r"\s+", " ", txt).strip()
    if not txt:
        return f"Erro HTTP {status_code} na comunicação com a SEFAZ."
    return f"HTTP {status_code}: {txt[:240]}"


def sanitizar_erro_sefaz_exibicao(mensagem: str) -> str:
    """Texto amigável para tela — cobre erros HTTP, rede e registros antigos com HTML."""
    raw = (mensagem or "").strip()
    if not raw:
        return ""
    low = raw.lower()
    m_http = re.match(r"^http\s+(\d{3})\b", low)
    if m_http:
        return sanitizar_erro_http_sefaz(int(m_http.group(1)), raw)
    if sefaz_erro_transiente(low):
        if "connection refused" in low or "errno 111" in low:
            return (
                "SEFAZ SP não aceitou conexão do servidor (recusada). "
                "Instabilidade ou bloqueio temporário — aguarde e reemita em alguns minutos."
            )
        if "timed out" in low or "timeout" in low:
            return "SEFAZ SP não respondeu a tempo. Aguarde alguns minutos e reemita."
        return "Falha de comunicação com a SEFAZ SP. Aguarde alguns minutos e reemita."
    if "<!doctype" in low or "<html" in low:
        return sanitizar_erro_http_sefaz(403, raw)
    return raw[:400]


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
