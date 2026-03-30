"""
Distribuição DF-e (notas destinadas ao CNPJ) — SEFAZ nacional.

Requer no .env (ou ambiente):
  NFE_DIST_DFE_CERT_PATH — caminho absoluto do .pfx (A1)
  NFE_DIST_DFE_CERT_PASSWORD — senha do certificado
  NFE_DIST_DFE_CNPJ — CNPJ da empresa (somente dígitos, 14)
  NFE_DIST_DFE_UF — sigla UF (ex.: SP)
  NFE_DIST_DFE_TP_AMB — 1 produção, 2 homologação (default 2)

Dependências opcionais: cryptography, lxml, signxml
  pip install cryptography lxml signxml
"""
from __future__ import annotations

import logging
import re
import uuid
import xml.etree.ElementTree as ET
from typing import Any

import requests
from decouple import config

from produtos.nfe_entrada_util import decodificar_doc_zip_base64

logger = logging.getLogger(__name__)

UF_PARA_COD = {
    "RO": 11,
    "AC": 12,
    "AM": 13,
    "RR": 14,
    "PA": 15,
    "AP": 16,
    "TO": 17,
    "MA": 21,
    "PI": 22,
    "CE": 23,
    "RN": 24,
    "PB": 25,
    "PE": 26,
    "AL": 27,
    "SE": 28,
    "BA": 29,
    "MG": 31,
    "ES": 32,
    "RJ": 33,
    "SP": 35,
    "PR": 41,
    "SC": 42,
    "RS": 43,
    "MS": 50,
    "MT": 51,
    "GO": 52,
    "DF": 53,
}

URL_DIST_DFE = {
    1: "https://www1.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx",
    2: "https://hom1.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx",
}


def _cfg_dist_dfe() -> dict[str, Any]:
    return {
        "cert_path": (config("NFE_DIST_DFE_CERT_PATH", default="") or "").strip(),
        "cert_password": (config("NFE_DIST_DFE_CERT_PASSWORD", default="") or "").strip(),
        "cnpj": re.sub(r"\D", "", config("NFE_DIST_DFE_CNPJ", default="") or "")[:14],
        "uf": (config("NFE_DIST_DFE_UF", default="SP") or "SP").strip().upper()[:2],
        "tp_amb": int(config("NFE_DIST_DFE_TP_AMB", default="2") or 2),
    }


def distribuicao_dfe_configurada() -> bool:
    c = _cfg_dist_dfe()
    return bool(c["cert_path"] and c["cert_password"] and len(c["cnpj"]) == 14 and c["uf"] in UF_PARA_COD)


def _assinar_dist_dfe_xml(xml_unsigned: str, cert_path: str, cert_password: str) -> tuple[str | None, str | None]:
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption, pkcs12
        from lxml import etree
        from signxml import XMLSigner, methods
    except ImportError:
        return None, "Instale: pip install cryptography lxml signxml"

    try:
        with open(cert_path, "rb") as f:
            pfx = f.read()
        password = cert_password.encode("utf-8") if cert_password else b""
        private_key, certificate, _extra = pkcs12.load_key_and_certificates(
            pfx, password, default_backend()
        )
        if private_key is None or certificate is None:
            return None, "PFX sem chave ou certificado."

        cert_pem = certificate.public_bytes(Encoding.PEM)
        key_pem = private_key.private_bytes(
            Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
        )

        parser = etree.XMLParser(remove_blank_text=True, recover=True)
        root = etree.fromstring(xml_unsigned.encode("utf-8"), parser)
        root.set("Id", f"distNFe{uuid.uuid4().hex[:12]}")

        signer = XMLSigner(
            method=methods.enveloped,
            signature_algorithm="rsa-sha1",
            digest_algorithm="sha1",
            c14n_algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315",
        )
        signed_root = signer.sign(root, key=key_pem, cert=cert_pem)
        return etree.tostring(signed_root, encoding="unicode", xml_declaration=True), None
    except Exception as exc:
        logger.exception("assinar_dist_dfe")
        return None, str(exc)[:400]


def nfe_distribuicao_dfe_interesse(ult_nsu: str) -> dict[str, Any]:
    """
    Consulta documentos destinados ao CNPJ (iteração por ultNSU).
    Retorno: ok, c_stat, x_motivo, ult_nsu, max_nsu, notas_xml (lista de XML string), erro
    """
    out: dict[str, Any] = {
        "ok": False,
        "c_stat": None,
        "x_motivo": "",
        "ult_nsu": ult_nsu,
        "max_nsu": None,
        "notas_xml": [],
        "erro": None,
    }
    cfg = _cfg_dist_dfe()
    if not distribuicao_dfe_configurada():
        out["erro"] = (
            "Configure NFE_DIST_DFE_CERT_PATH, NFE_DIST_DFE_CERT_PASSWORD, "
            "NFE_DIST_DFE_CNPJ (14 dígitos) e NFE_DIST_DFE_UF no .env."
        )
        return out

    c_uf = UF_PARA_COD.get(cfg["uf"])
    if not c_uf:
        out["erro"] = "UF inválida."
        return out

    tp_amb = 1 if cfg["tp_amb"] == 1 else 2
    url = URL_DIST_DFE.get(tp_amb, URL_DIST_DFE[2])
    ult_nsu = re.sub(r"\D", "", str(ult_nsu or "0")) or "0"
    ult_nsu = ult_nsu.zfill(15)[:15]

    xml_body = (
        f'<distDFeInt xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.01">'
        f"<tpAmb>{tp_amb}</tpAmb>"
        f"<cUFAutor>{c_uf}</cUFAutor>"
        f"<CNPJ>{cfg['cnpj']}</CNPJ>"
        f"<distNSU><ultNSU>{ult_nsu}</ultNSU></distNSU>"
        f"</distDFeInt>"
    )

    signed, err = _assinar_dist_dfe_xml(xml_body, cfg["cert_path"], cfg["cert_password"])
    if err or not signed:
        out["erro"] = err or "Falha ao assinar XML."
        return out

    inner = signed.replace('<?xml version="1.0" encoding="UTF-8"?>', "").strip()
    soap = f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:xsd="http://www.w3.org/2001/XMLSchema"
  xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <nfeDistDFeInteresse xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe">
      <nfeDadosMsg><![CDATA[{inner}]]></nfeDadosMsg>
    </nfeDistDFeInteresse>
  </soap12:Body>
</soap12:Envelope>"""

    headers = {
        "Content-Type": 'application/soap+xml; charset=utf-8; action="http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe/nfeDistDFeInteresse"',
        "SOAPAction": "http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe/nfeDistDFeInteresse",
    }
    try:
        r = requests.post(url, data=soap.encode("utf-8"), headers=headers, timeout=60)
        text = r.text or ""
        if r.status_code >= 400:
            out["erro"] = f"HTTP {r.status_code}: {text[:500]}"
            return out
    except requests.RequestException as exc:
        out["erro"] = str(exc)[:400]
        return out

    # Parse resposta SOAP (best effort)
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        out["erro"] = "Resposta não é XML válido."
        return out

    def local(t: str) -> str:
        return t.split("}", 1)[-1] if t and "}" in t else (t or "")

    ret = None
    for el in root.iter():
        if local(el.tag) == "retDistDFeInt":
            ret = el
            break

    if ret is None:
        out["erro"] = "Não encontramos retDistDFeInt na resposta."
        out["raw_snippet"] = text[:800]
        return out

    c_stat = None
    x_motivo = ""
    max_nsu = None
    ult_nsu_ret = None
    notas: list[str] = []

    for ch in ret.iter():
        tag = local(ch.tag)
        if tag == "cStat" and ch.text:
            try:
                c_stat = int(ch.text.strip())
            except ValueError:
                c_stat = ch.text.strip()
        elif tag == "xMotivo" and ch.text:
            x_motivo = ch.text.strip()
        elif tag == "maxNSU" and ch.text:
            max_nsu = ch.text.strip()
        elif tag == "ultNSU" and ch.text:
            ult_nsu_ret = ch.text.strip()
        elif tag == "docZip" and ch.text:
            xml_doc = decodificar_doc_zip_base64(ch.text.strip())
            if xml_doc:
                notas.append(xml_doc)

    out["c_stat"] = c_stat
    out["x_motivo"] = x_motivo
    out["max_nsu"] = max_nsu
    if ult_nsu_ret:
        out["ult_nsu"] = ult_nsu_ret
    out["notas_xml"] = notas
    # 137: sem documento; 138: com documento(s); 656: consumo indevido / intervalo
    if c_stat == 656:
        out["ok"] = False
        out["erro"] = x_motivo or "Rejeição 656 — aguarde entre consultas idênticas."
    elif c_stat in (137, 138):
        out["ok"] = True
    elif c_stat is None:
        out["ok"] = False
        out["erro"] = out.get("erro") or "Resposta sem cStat reconhecido."
    else:
        out["ok"] = False
        out["erro"] = f"cStat={c_stat} {x_motivo}".strip()

    return out
