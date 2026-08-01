"""
Distribuição DF-e (notas destinadas ao CNPJ) — SEFAZ nacional.

Requer no .env (ou ambiente) — **mesmo A1 da NFC-e**:
  NFE_DIST_DFE_* (opcional) **ou** só NFC_E_CERT_PATH / NFC_E_CERT_BASE64,
  NFC_E_CERT_PASSWORD, NFC_E_CNPJ, NFC_E_UF, NFC_E_TP_AMB
  (Dist DF-e reusa NFC_E_* quando NFE_DIST_DFE_* estiver vazio).

  NFE_DIST_DFE_TP_AMB / NFC_E_TP_AMB — 1 produção, 2 homologação

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
from produtos.sefaz_ssl_util import sefaz_requests_verify

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
    """
    Prefere ``NFE_DIST_DFE_*``; se vazio, reusa o mesmo A1/CNPJ/UF da NFC-e
    (``NFC_E_CERT_*``, ``NFC_E_CNPJ``, ``NFC_E_UF``, ``NFC_E_TP_AMB``).
    """
    import os

    from produtos.nfce_config_util import nfce_garantir_certificado

    path = (config("NFE_DIST_DFE_CERT_PATH", default="") or "").strip()
    if not path or not os.path.isfile(path):
        path = nfce_garantir_certificado() or path
    password = (config("NFE_DIST_DFE_CERT_PASSWORD", default="") or "").strip() or (
        config("NFC_E_CERT_PASSWORD", default="") or ""
    ).strip()
    cnpj = re.sub(
        r"\D",
        "",
        (config("NFE_DIST_DFE_CNPJ", default="") or "")
        or (config("NFC_E_CNPJ", default="") or ""),
    )[:14]
    uf = (
        (config("NFE_DIST_DFE_UF", default="") or "").strip()
        or (config("NFC_E_UF", default="SP") or "SP").strip()
        or "SP"
    ).upper()[:2]
    try:
        tp_raw = config("NFE_DIST_DFE_TP_AMB", default="") or ""
        if str(tp_raw).strip() == "":
            tp_raw = config("NFC_E_TP_AMB", default="2") or "2"
        tp_amb = int(tp_raw)
    except (TypeError, ValueError):
        tp_amb = 2
    if tp_amb not in (1, 2):
        tp_amb = 2
    return {
        "cert_path": path,
        "cert_password": password,
        "cnpj": cnpj,
        "uf": uf,
        "tp_amb": tp_amb,
    }


def distribuicao_dfe_configurada() -> bool:
    import os

    c = _cfg_dist_dfe()
    return bool(
        c["cert_path"]
        and os.path.isfile(str(c["cert_path"]))
        and c["cert_password"]
        and len(c["cnpj"]) == 14
        and c["uf"] in UF_PARA_COD
    )


def _assinar_dist_dfe_xml(xml_unsigned: str, cert_path: str, cert_password: str) -> tuple[str | None, str | None]:
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption, pkcs12
        from lxml import etree
        from produtos.sefaz_signxml_util import criar_sefaz_xml_signer
        from produtos.sefaz_xml_fiscal_util import tostring_sem_prefixos
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

        signer = criar_sefaz_xml_signer()
        signed_root = signer.sign(root, key=key_pem, cert=cert_pem)
        # lxml: encoding=unicode + xml_declaration=True → TypeError
        return tostring_sem_prefixos(signed_root), None
    except Exception as exc:
        logger.exception("assinar_dist_dfe")
        return None, str(exc)[:400]


def _dfe_cnpj_cfg() -> str:
    return str(_cfg_dist_dfe().get("cnpj") or "")


def _dfe_cooldown_key(cnpj: str) -> str:
    return f"agro_dfe_cooldown:{cnpj}"


def _dfe_last_call_key(cnpj: str) -> str:
    return f"agro_dfe_last_call:{cnpj}"


def dfe_intervalo_minimo_segundos() -> int:
    """Intervalo mínimo entre cliques (anti-spam). NT: após 137/vazio = 1h."""
    try:
        return max(30, int(config("NFE_DIST_DFE_MIN_INTERVALO_S", default="60") or 60))
    except (TypeError, ValueError):
        return 60


def dfe_checar_limite_consulta(cnpj: str) -> dict[str, Any] | None:
    """
    Bloqueia consulta se ainda no cooldown da NT (1h após 137/656) ou intervalo mínimo.
    Retorna dict de erro ou None se liberado.
    """
    import time

    from django.core.cache import cache

    cnpj = re.sub(r"\D", "", cnpj or "")[:14]
    if len(cnpj) != 14:
        return None
    agora = time.time()
    cd = cache.get(_dfe_cooldown_key(cnpj))
    if isinstance(cd, dict):
        ate = float(cd.get("ate") or 0)
        if ate > agora:
            falta = int(ate - agora)
            motivo = str(cd.get("motivo") or "Aguarde antes de nova consulta à SEFAZ.")
            return {
                "ok": False,
                "erro": f"{motivo} Faltam ~{max(1, falta // 60)} min ({falta}s).",
                "aguardar_segundos": falta,
                "c_stat": 656 if "656" in motivo or "indevido" in motivo.lower() else 137,
            }
    last = cache.get(_dfe_last_call_key(cnpj))
    try:
        last_f = float(last) if last is not None else 0.0
    except (TypeError, ValueError):
        last_f = 0.0
    min_s = dfe_intervalo_minimo_segundos()
    if last_f and (agora - last_f) < min_s:
        falta = int(min_s - (agora - last_f))
        return {
            "ok": False,
            "erro": (
                f"Aguarde {falta}s entre consultas (mínimo {min_s}s). "
                "A SEFAZ bloqueia consumo indevido (cStat 656) se consultar demais."
            ),
            "aguardar_segundos": falta,
        }
    return None


def dfe_status_limite(cnpj: str) -> dict[str, Any]:
    """Para a UI: se pode consultar agora (sem bater na SEFAZ)."""
    bloqueio = dfe_checar_limite_consulta(cnpj)
    if not bloqueio:
        return {"liberado": True, "aguardar_segundos": 0, "motivo": ""}
    return {
        "liberado": False,
        "aguardar_segundos": int(bloqueio.get("aguardar_segundos") or 0),
        "motivo": str(bloqueio.get("erro") or "Aguarde antes de nova consulta."),
    }


def dfe_registrar_consulta_enviada(cnpj: str) -> None:
    import time

    from django.core.cache import cache

    cnpj = re.sub(r"\D", "", cnpj or "")[:14]
    if len(cnpj) != 14:
        return
    cache.set(_dfe_last_call_key(cnpj), time.time(), timeout=60 * 60 * 6)


def dfe_aplicar_cooldown_apos_resposta(
    cnpj: str,
    *,
    c_stat: Any,
    ult_nsu: str | None,
    max_nsu: str | None,
    x_motivo: str = "",
) -> None:
    """NT 2014.002: após 137 ou ultNSU==maxNSU (com docs), ou 656 → esperar 1 hora."""
    import time

    from django.core.cache import cache

    cnpj = re.sub(r"\D", "", cnpj or "")[:14]
    if len(cnpj) != 14:
        return
    motivo = ""
    try:
        st = int(c_stat) if c_stat is not None else None
    except (TypeError, ValueError):
        st = None
    # Erros de schema/validação (ex. 215) não disparam cooldown de 1h.
    if st is not None and st not in (137, 138, 656) and st >= 200:
        return
    u = re.sub(r"\D", "", str(ult_nsu or ""))
    m = re.sub(r"\D", "", str(max_nsu or ""))
    if st == 656:
        motivo = x_motivo or "Consumo indevido (656) — aguarde 1 hora."
    elif st == 137:
        motivo = (
            x_motivo
            or "Nenhum documento novo (137). Pela regra da SEFAZ, aguarde 1 hora para consultar de novo."
        )
    elif st == 138:
        try:
            # Só “fim do NSU” se ambos > 0 e iguais (zeros em rejeição 215 não contam).
            if u and m and int(u) > 0 and int(u) == int(m):
                motivo = "Já no último NSU (ultNSU = maxNSU). Aguarde 1 hora antes de nova consulta."
        except ValueError:
            pass
    if not motivo:
        return
    cache.set(
        _dfe_cooldown_key(cnpj),
        {"ate": time.time() + 3600, "motivo": motivo[:280]},
        timeout=3600 + 120,
    )


def nfe_distribuicao_dfe_interesse(ult_nsu: str) -> dict[str, Any]:
    """
    Consulta documentos destinados ao CNPJ (iteração por ultNSU).
    Retorno: ok, c_stat, x_motivo, ult_nsu, max_nsu, notas_xml (lista de XML string), erro
    """
    import os

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
            "Certificado DF-e/NFC-e incompleto. Use o mesmo A1 da NFC-e "
            "(NFC_E_CERT_PATH ou NFC_E_CERT_BASE64 + NFC_E_CERT_PASSWORD + NFC_E_CNPJ + NFC_E_UF) "
            "ou as variáveis NFE_DIST_DFE_*."
        )
        return out

    bloqueio = dfe_checar_limite_consulta(cfg["cnpj"])
    if bloqueio:
        # Trava local (cache) — NÃO ecoar o ultNSU pedido como se viesse da SEFAZ
        # (senão o Buscar no Aguarde podia gravar NSU digitado à toa).
        out.update(bloqueio)
        out["bloqueio_local"] = True
        out["ult_nsu"] = None
        out["max_nsu"] = None
        return out

    c_uf = UF_PARA_COD.get(cfg["uf"])
    if not c_uf:
        out["erro"] = "UF inválida."
        return out

    tp_amb = 1 if cfg["tp_amb"] == 1 else 2
    url = URL_DIST_DFE.get(tp_amb, URL_DIST_DFE[2])
    ult_nsu = re.sub(r"\D", "", str(ult_nsu or "0")) or "0"
    ult_nsu = ult_nsu.zfill(15)[:15]

    # Schema distDFeInt_v1.01: SEM assinatura XML / Id / Signature.
    # Autenticação = certificado na conexão (mTLS). Assinar aqui → cStat 215.
    xml_body = (
        f'<distDFeInt xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.01">'
        f"<tpAmb>{tp_amb}</tpAmb>"
        f"<cUFAutor>{c_uf}</cUFAutor>"
        f"<CNPJ>{cfg['cnpj']}</CNPJ>"
        f"<distNSU><ultNSU>{ult_nsu}</ultNSU></distNSU>"
        f"</distDFeInt>"
    )

    from produtos.sefaz_soap_util import normalizar_xml_envio

    wsdl = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe"
    inner = normalizar_xml_envio(xml_body)
    soap = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
        'xmlns:soap="http://www.w3.org/2003/05/soap-envelope">'
        "<soap:Body>"
        f'<nfeDistDFeInteresse xmlns="{wsdl}">'
        f'<nfeDadosMsg xmlns="{wsdl}">{inner}</nfeDadosMsg>'
        "</nfeDistDFeInteresse>"
        "</soap:Body></soap:Envelope>"
    )
    headers = {
        "Content-Type": f'application/soap+xml;charset=utf-8;action="{wsdl}/nfeDistDFeInteresse"',
        "Accept": "application/soap+xml; charset=utf-8;",
    }

    cleanup: list[str] = []
    text = ""
    try:
        from produtos.nfce_sp_emissao_util import _cert_pem_temporario

        cert_file, key_file, cleanup = _cert_pem_temporario(cfg["cert_path"], cfg["cert_password"])
        dfe_registrar_consulta_enviada(cfg["cnpj"])
        r = requests.post(
            url,
            data=soap.encode("utf-8"),
            headers=headers,
            cert=(cert_file, key_file),
            verify=sefaz_requests_verify(),
            timeout=60,
        )
        text = r.text or ""
        if r.status_code >= 400:
            msg = f"HTTP {r.status_code}: {text[:400]}"
            if r.status_code == 403:
                msg += (
                    " — Costuma ser certificado na conexão (mTLS), firewall ou SEFAZ indisponível. "
                    "Confira validade do .pfx e tente de novo em alguns minutos."
                )
            out["erro"] = msg
            return out
    except requests.RequestException as exc:
        out["erro"] = str(exc)[:400]
        return out
    finally:
        for p in cleanup:
            try:
                if p and os.path.isfile(p):
                    os.unlink(p)
            except OSError:
                pass

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

    dfe_aplicar_cooldown_apos_resposta(
        cfg["cnpj"],
        c_stat=c_stat,
        ult_nsu=str(ult_nsu_ret or ult_nsu),
        max_nsu=str(max_nsu or ""),
        x_motivo=x_motivo,
    )

    # 137: sem documento; 138: com documento(s); 656: consumo indevido / intervalo
    if c_stat == 656:
        out["ok"] = False
        out["erro"] = x_motivo or "Rejeição 656 — aguarde 1 hora entre consultas."
        out["aguardar_segundos"] = 3600
    elif c_stat == 215:
        out["ok"] = False
        out["erro"] = (
            (x_motivo or "Falha no esquema XML (215).")
            + " O pedido Dist DF-e não usa assinatura XML — só certificado na conexão. "
            "Se persistir após atualizar o sistema, avise o suporte."
        )
    elif c_stat in (137, 138):
        out["ok"] = True
        if c_stat == 137:
            out["aguardar_segundos"] = 3600
            out["x_motivo"] = (
                (x_motivo or "Nenhum documento localizado.")
                + " Próxima consulta automática liberada em ~1 hora (regra SEFAZ)."
            )
    elif c_stat is None:
        out["ok"] = False
        out["erro"] = out.get("erro") or "Resposta sem cStat reconhecido."
    else:
        out["ok"] = False
        out["erro"] = f"cStat={c_stat} {x_motivo}".strip()

    return out


def nfe_distribuicao_dfe_por_chave(chave: str) -> dict[str, Any]:
    """
    Baixa documento pela chave de acesso (consChNFe).
    Não altera o cursor ultNSU da loja — uso para recuperar nota já “passada” na fila.
    """
    import os

    out: dict[str, Any] = {
        "ok": False,
        "c_stat": None,
        "x_motivo": "",
        "ult_nsu": None,
        "max_nsu": None,
        "notas_xml": [],
        "erro": None,
    }
    cfg = _cfg_dist_dfe()
    if not distribuicao_dfe_configurada():
        out["erro"] = "Certificado DF-e/NFC-e incompleto."
        return out

    bloqueio = dfe_checar_limite_consulta(cfg["cnpj"])
    if bloqueio:
        out.update(bloqueio)
        out["bloqueio_local"] = True
        return out

    ch = re.sub(r"\D", "", str(chave or ""))[:44]
    if len(ch) != 44:
        out["erro"] = "Chave NF-e inválida (44 dígitos)."
        return out

    c_uf = UF_PARA_COD.get(cfg["uf"])
    if not c_uf:
        out["erro"] = "UF inválida."
        return out

    tp_amb = 1 if cfg["tp_amb"] == 1 else 2
    url = URL_DIST_DFE.get(tp_amb, URL_DIST_DFE[2])
    xml_body = (
        f'<distDFeInt xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.01">'
        f"<tpAmb>{tp_amb}</tpAmb>"
        f"<cUFAutor>{c_uf}</cUFAutor>"
        f"<CNPJ>{cfg['cnpj']}</CNPJ>"
        f"<consChNFe><chNFe>{ch}</chNFe></consChNFe>"
        f"</distDFeInt>"
    )

    from produtos.sefaz_soap_util import normalizar_xml_envio

    wsdl = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe"
    inner = normalizar_xml_envio(xml_body)
    soap = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
        'xmlns:soap="http://www.w3.org/2003/05/soap-envelope">'
        "<soap:Body>"
        f'<nfeDistDFeInteresse xmlns="{wsdl}">'
        f'<nfeDadosMsg xmlns="{wsdl}">{inner}</nfeDadosMsg>'
        "</nfeDistDFeInteresse>"
        "</soap:Body></soap:Envelope>"
    )
    headers = {
        "Content-Type": f'application/soap+xml;charset=utf-8;action="{wsdl}/nfeDistDFeInteresse"',
        "Accept": "application/soap+xml; charset=utf-8;",
    }

    cleanup: list[str] = []
    text = ""
    try:
        from produtos.nfce_sp_emissao_util import _cert_pem_temporario

        cert_file, key_file, cleanup = _cert_pem_temporario(cfg["cert_path"], cfg["cert_password"])
        dfe_registrar_consulta_enviada(cfg["cnpj"])
        r = requests.post(
            url,
            data=soap.encode("utf-8"),
            headers=headers,
            cert=(cert_file, key_file),
            verify=sefaz_requests_verify(),
            timeout=60,
        )
        text = r.text or ""
        if r.status_code >= 400:
            out["erro"] = f"HTTP {r.status_code}: {text[:400]}"
            return out
    except Exception as exc:
        logger.exception("nfe_distribuicao_dfe_por_chave")
        out["erro"] = str(exc)[:400]
        return out
    finally:
        for p in cleanup:
            try:
                if p and os.path.isfile(p):
                    os.remove(p)
            except OSError:
                pass

    c_stat = None
    x_motivo = ""
    max_nsu = None
    ult_nsu_ret = None
    notas: list[str] = []
    try:
        root = ET.fromstring(text)
        for ch_el in root.iter():
            tag = ch_el.tag.split("}")[-1] if "}" in ch_el.tag else ch_el.tag
            if tag == "cStat" and ch_el.text and c_stat is None:
                try:
                    c_stat = int(ch_el.text.strip())
                except ValueError:
                    c_stat = ch_el.text.strip()
            elif tag == "xMotivo" and ch_el.text and not x_motivo:
                x_motivo = ch_el.text.strip()
            elif tag == "maxNSU" and ch_el.text:
                max_nsu = ch_el.text.strip()
            elif tag == "ultNSU" and ch_el.text:
                ult_nsu_ret = ch_el.text.strip()
            elif tag == "docZip" and ch_el.text:
                xml_doc = decodificar_doc_zip_base64(ch_el.text.strip())
                if xml_doc:
                    notas.append(xml_doc)
    except ET.ParseError:
        out["erro"] = "Resposta SEFAZ inválida."
        return out

    out["c_stat"] = c_stat
    out["x_motivo"] = x_motivo
    out["max_nsu"] = max_nsu
    out["ult_nsu"] = ult_nsu_ret
    out["notas_xml"] = notas

    dfe_aplicar_cooldown_apos_resposta(
        cfg["cnpj"],
        c_stat=c_stat,
        ult_nsu=str(ult_nsu_ret or ""),
        max_nsu=str(max_nsu or ""),
        x_motivo=x_motivo,
    )

    if c_stat == 656:
        out["ok"] = False
        out["erro"] = x_motivo or "Rejeição 656 — aguarde 1 hora."
        out["aguardar_segundos"] = 3600
    elif c_stat in (137, 138):
        out["ok"] = True
    else:
        out["ok"] = False
        out["erro"] = f"cStat={c_stat} {x_motivo}".strip()
    return out
