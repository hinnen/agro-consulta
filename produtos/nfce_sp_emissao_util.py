"""
Emissão NFC-e modelo 65 — SEFAZ São Paulo (Simples Nacional, PDV).

Dependências: cryptography, lxml, signxml (já no requirements.txt).
"""
from __future__ import annotations

import hashlib
import logging
import random
import re
import tempfile
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import requests
from django.db import transaction
from django.utils import timezone

from produtos.caixa_util import pagamentos_lista_de_venda
from produtos.models import ItemVendaAgro, NfceDocumentoAgro, NfceNumeracaoAgro, VendaAgro
from produtos.nfce_config_util import nfce_cfg, nfce_configurada
from produtos.sefaz_ssl_util import sefaz_requests_verify
from produtos.nfce_fiscal_produto_util import fiscal_por_produto_id

logger = logging.getLogger(__name__)

NS = "http://www.portalfiscal.inf.br/nfe"
NS_WSDL = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeAutorizacao4"

URL_AUTORIZACAO = {
    1: "https://nfce.fazenda.sp.gov.br/ws/NFeAutorizacao4.asmx",
    2: "https://homologacao.nfce.fazenda.sp.gov.br/ws/NFeAutorizacao4.asmx",
}

URL_QR_BASE = {
    1: "https://www.nfce.fazenda.sp.gov.br/NFCeConsultaPublica/Paginas/ConsultaQRCode.aspx",
    2: "https://homologacao.nfce.fazenda.sp.gov.br/NFCeConsultaPublica/Paginas/ConsultaQRCode.aspx",
}

CUF_SP = "35"


def _q2(v: Decimal | float) -> str:
    return str(Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _q4(v: Decimal | float) -> str:
    return str(Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def cpf_valido(cpf_raw: str) -> bool:
    cpf = re.sub(r"\D", "", str(cpf_raw or ""))[:11]
    if len(cpf) != 11 or cpf == cpf[0] * 11:
        return False
    for j in (9, 10):
        s = sum(int(cpf[i]) * ((j + 1) - i) for i in range(j))
        d = 0 if (s * 10 % 11) == 10 else (s * 10 % 11)
        if int(cpf[j]) != d:
            return False
    return True


def _gerar_c_nf() -> str:
    return str(random.randint(10000000, 99999999))


def _dv_chave(chave43: str) -> str:
    pesos = [2, 3, 4, 5, 6, 7, 8, 9]
    soma = 0
    for i, d in enumerate(reversed(chave43)):
        soma += int(d) * pesos[i % len(pesos)]
    resto = soma % 11
    dv = 0 if resto in (0, 1) else 11 - resto
    return str(dv)


def _montar_chave(*, cnpj: str, serie: int, numero: int, tp_emis: str, c_nf: str, dh_emi: datetime) -> str:
    aamm = dh_emi.strftime("%y%m")
    ch43 = (
        f"{CUF_SP}{aamm}{cnpj}65{serie:03d}{numero:09d}{tp_emis}{c_nf:0>8}"[:43]
    )
    return ch43 + _dv_chave(ch43)


def _sub(parent: ET.Element, tag: str, text: str | None = None) -> ET.Element:
    el = ET.SubElement(parent, f"{{{NS}}}{tag}")
    if text is not None and str(text) != "":
        el.text = str(text)
    return el


def _map_tpag(forma: str) -> str:
    f = str(forma or "").strip().lower()
    if "dinheiro" in f:
        return "01"
    if "crédito" in f or "credito" in f:
        return "03"
    if "débito" in f or "debito" in f:
        return "04"
    if "pix" in f:
        return "17"
    if "fiado" in f or "credito loja" in f or "crédito loja" in f:
        return "05"
    return "99"


def _pagamentos_da_venda(venda: VendaAgro) -> list[dict[str, Any]]:
    rows = pagamentos_lista_de_venda(venda)
    out: list[dict[str, Any]] = []
    for row in rows:
        forma = str(row.get("forma") or "Outros")
        try:
            val = Decimal(str(row.get("valor") or 0)).quantize(Decimal("0.01"))
        except Exception:
            val = Decimal("0")
        if val <= 0:
            continue
        out.append({"tPag": _map_tpag(forma), "vPag": val, "xPag": forma if _map_tpag(forma) == "99" else ""})
    if not out:
        out.append({"tPag": "01", "vPag": Decimal(str(venda.total or 0)).quantize(Decimal("0.01")), "xPag": ""})
    return out


def _proximo_numero_serie(cfg: dict[str, Any]) -> tuple[int, int]:
    with transaction.atomic():
        num, _ = NfceNumeracaoAgro.objects.select_for_update().get_or_create(
            pk=1,
            defaults={"serie": cfg["serie"], "proximo_numero": 1},
        )
        if num.serie != cfg["serie"]:
            num.serie = cfg["serie"]
        n = int(num.proximo_numero or 1)
        num.proximo_numero = n + 1
        num.save(update_fields=["serie", "proximo_numero", "atualizado_em"])
        return num.serie, n


def _cert_pem_temporario(cert_path: str, cert_password: str) -> tuple[str, str, list[str]]:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat, pkcs12

    cleanup: list[str] = []
    with open(cert_path, "rb") as f:
        pfx = f.read()
    password = cert_password.encode("utf-8") if cert_password else b""
    private_key, certificate, _ = pkcs12.load_key_and_certificates(pfx, password, default_backend())
    if private_key is None or certificate is None:
        raise ValueError("PFX sem chave ou certificado.")
    cert_pem = certificate.public_bytes(Encoding.PEM)
    key_pem = private_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    cf = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    kf = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    cf.write(cert_pem)
    kf.write(key_pem)
    cf.close()
    kf.close()
    cleanup.extend([cf.name, kf.name])
    return cf.name, kf.name, cleanup


def _assinar_nfe_xml(xml_nfe: str, cert_path: str, cert_password: str, chave: str) -> tuple[str | None, str | None]:
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat, pkcs12
        from lxml import etree
        from produtos.sefaz_signxml_util import criar_sefaz_xml_signer
    except ImportError:
        return None, "Instale: pip install cryptography lxml signxml"

    try:
        with open(cert_path, "rb") as f:
            pfx = f.read()
        password = cert_password.encode("utf-8") if cert_password else b""
        private_key, certificate, _ = pkcs12.load_key_and_certificates(pfx, password, default_backend())
        if private_key is None or certificate is None:
            return None, "PFX sem chave ou certificado."
        cert_pem = certificate.public_bytes(Encoding.PEM)
        key_pem = private_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
        parser = etree.XMLParser(remove_blank_text=True)
        root = etree.fromstring(xml_nfe.encode("utf-8"), parser)
        inf = root.find(f".//{{{NS}}}infNFe")
        if inf is None:
            return None, "infNFe não encontrado."
        inf_id = inf.get("Id") or f"NFe{chave}"
        inf.set("Id", inf_id)
        signer = criar_sefaz_xml_signer()
        signed = signer.sign(root, key=key_pem, cert=cert_pem, reference_uri="#" + inf_id)
        return etree.tostring(signed, encoding="unicode", xml_declaration=False), None
    except Exception as exc:
        logger.exception("assinar_nfce")
        return None, str(exc)[:400]


def _qr_code_url(chave: str, tp_amb: int, csc_id: int, csc_token: str) -> str:
    n_versao = "2"
    digest = hashlib.sha1(f"{chave}{n_versao}{tp_amb}{csc_id}{csc_token}".encode()).hexdigest().upper()
    p = f"{chave}|{n_versao}|{tp_amb}|{csc_id}|{digest}"
    base = URL_QR_BASE.get(tp_amb, URL_QR_BASE[2])
    return f"{base}?p={p}"


def _montar_xml_nfce(
    cfg: dict[str, Any],
    venda: VendaAgro,
    itens: list[ItemVendaAgro],
    *,
    serie: int,
    numero: int,
    chave: str,
    dh_emi: datetime,
    cpf_dest: str,
    fiscal_itens: list[dict[str, str]],
    db=None,
    col_p: str | None = None,
) -> str:
    tp_amb = int(cfg["tp_amb"])
    dh_txt = dh_emi.strftime("%Y-%m-%dT%H:%M:%S-03:00")
    nfe = ET.Element(f"{{{NS}}}NFe")
    inf = ET.SubElement(nfe, f"{{{NS}}}infNFe", {"versao": "4.00", "Id": f"NFe{chave}"})

    ide = _sub(inf, "ide")
    _sub(ide, "cUF", CUF_SP)
    _sub(ide, "cNF", chave[35:43])
    _sub(ide, "natOp", "VENDA")
    _sub(ide, "mod", "65")
    _sub(ide, "serie", str(serie))
    _sub(ide, "nNF", str(numero))
    _sub(ide, "dhEmi", dh_txt)
    _sub(ide, "tpNF", "1")
    _sub(ide, "idDest", "1")
    _sub(ide, "cMunFG", cfg["cmun"])
    _sub(ide, "tpImp", "4")
    _sub(ide, "tpEmis", "1")
    _sub(ide, "cDV", chave[-1])
    _sub(ide, "tpAmb", str(tp_amb))
    _sub(ide, "finNFe", "1")
    _sub(ide, "indFinal", "1")
    _sub(ide, "indPres", "1")
    _sub(ide, "procEmi", "0")
    _sub(ide, "verProc", "AgroConsulta1.0")

    emit = _sub(inf, "emit")
    _sub(emit, "CNPJ", cfg["cnpj"])
    _sub(emit, "xNome", cfg["razao_social"])
    _sub(emit, "xFant", cfg["fantasia"])
    ender = _sub(emit, "enderEmit")
    _sub(ender, "xLgr", cfg["logradouro"])
    _sub(ender, "nro", cfg["numero"])
    _sub(ender, "xBairro", cfg["bairro"])
    _sub(ender, "cMun", cfg["cmun"])
    _sub(ender, "xMun", cfg["cidade"])
    _sub(ender, "UF", cfg["uf"])
    _sub(ender, "CEP", cfg["cep"])
    if cfg.get("fone"):
        _sub(ender, "fone", cfg["fone"])
    _sub(emit, "IE", cfg["ie"])
    _sub(emit, "CRT", "1")

    if cpf_dest:
        dest = _sub(inf, "dest")
        _sub(dest, "CPF", cpf_dest)
        _sub(dest, "indIEDest", "9")

    total_prod = Decimal("0")
    for idx, item in enumerate(itens, start=1):
        fis = fiscal_itens[idx - 1] if idx - 1 < len(fiscal_itens) else fiscal_por_produto_id("", db=db, col_p=col_p)
        qtd = Decimal(str(item.quantidade or 0))
        vu = Decimal(str(item.valor_unitario or 0))
        vt = Decimal(str(item.valor_total or 0)).quantize(Decimal("0.01"))
        total_prod += vt
        det = ET.SubElement(inf, f"{{{NS}}}det")
        det.set("nItem", str(idx))
        prod = _sub(det, "prod")
        _sub(prod, "cProd", str(item.codigo or item.produto_id_externo or idx)[:60])
        _sub(prod, "cEAN", "SEM GTIN")
        _sub(prod, "xProd", (item.descricao or "PRODUTO")[:120])
        _sub(prod, "NCM", fis["ncm"])
        if fis.get("cest"):
            _sub(prod, "CEST", fis["cest"])
        _sub(prod, "CFOP", fis["cfop"])
        _sub(prod, "uCom", "UN")
        _sub(prod, "qCom", _q4(qtd))
        _sub(prod, "vUnCom", _q4(vu))
        _sub(prod, "vProd", _q2(vt))
        _sub(prod, "cEANTrib", "SEM GTIN")
        _sub(prod, "uTrib", "UN")
        _sub(prod, "qTrib", _q4(qtd))
        _sub(prod, "vUnTrib", _q4(vu))
        _sub(prod, "indTot", "1")
        imposto = _sub(det, "imposto")
        icms = _sub(imposto, "ICMS")
        icmssn = _sub(icms, "ICMSSN102")
        _sub(icmssn, "orig", fis["origem"])
        _sub(icmssn, "CSOSN", fis["csosn"])
        pis = _sub(imposto, "PIS")
        pisnt = _sub(pis, "PISNT")
        _sub(pisnt, "CST", "07")
        cof = _sub(imposto, "COFINS")
        cofnt = _sub(cof, "COFINSNT")
        _sub(cofnt, "CST", "07")

    total_nf = Decimal(str(venda.total or total_prod)).quantize(Decimal("0.01"))
    icms_tot = _sub(inf, "total")
    icms = _sub(icms_tot, "ICMSTot")
    for tag, val in (
        ("vBC", "0.00"),
        ("vICMS", "0.00"),
        ("vICMSDeson", "0.00"),
        ("vFCP", "0.00"),
        ("vBCST", "0.00"),
        ("vST", "0.00"),
        ("vFCPST", "0.00"),
        ("vFCPSTRet", "0.00"),
        ("vProd", _q2(total_prod)),
        ("vFrete", "0.00"),
        ("vSeg", "0.00"),
        ("vDesc", "0.00"),
        ("vII", "0.00"),
        ("vIPI", "0.00"),
        ("vIPIDevol", "0.00"),
        ("vPIS", "0.00"),
        ("vCOFINS", "0.00"),
        ("vOutro", "0.00"),
        ("vNF", _q2(total_nf)),
    ):
        _sub(icms, tag, val)

    transp = _sub(inf, "transp")
    _sub(transp, "modFrete", "9")

    pag = _sub(inf, "pag")
    for pg in _pagamentos_da_venda(venda):
        det_p = _sub(pag, "detPag")
        _sub(det_p, "tPag", pg["tPag"])
        if pg["tPag"] == "99" and pg.get("xPag"):
            _sub(det_p, "xPag", str(pg["xPag"])[:60])
        _sub(det_p, "vPag", _q2(pg["vPag"]))

    inf_ad = _sub(inf, "infAdic")
    obs = f"Venda PDV #{venda.pk}"
    if tp_amb == 2:
        obs = "NF-E EMITIDA EM AMBIENTE DE HOMOLOGACAO - SEM VALOR FISCAL | " + obs
    _sub(inf_ad, "infCpl", obs[:5000])

    qr_url = _qr_code_url(chave, tp_amb, int(cfg["csc_id"]), cfg["csc_token"])
    xml_body = ET.tostring(nfe, encoding="unicode")
    return xml_body, qr_url


def _anexar_suplementar_qrcode(xml_assinado: str, qr_url: str, tp_amb: int) -> str:
    from lxml import etree

    parser = etree.XMLParser(remove_blank_text=False)
    root = etree.fromstring(xml_assinado.encode("utf-8"), parser)
    supl = etree.SubElement(root, f"{{{NS}}}infNFeSupl")
    qr_el = etree.SubElement(supl, f"{{{NS}}}qrCode")
    qr_el.text = qr_url
    url_el = etree.SubElement(supl, f"{{{NS}}}urlChave")
    url_el.text = (
        "https://www.fazenda.sp.gov.br/nfce/consulta"
        if tp_amb == 1
        else "https://www.homologacao.nfce.fazenda.sp.gov.br/consulta"
    )
    return etree.tostring(root, encoding="unicode", xml_declaration=False)


def _enviar_autorizacao(xml_assinado: str, cfg: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    tp_amb = int(cfg["tp_amb"])
    url = URL_AUTORIZACAO.get(tp_amb, URL_AUTORIZACAO[2])
    id_lote = str(int(datetime.now().timestamp()))[-15:]
    inner = f'<enviNFe xmlns="{NS}" versao="4.00"><idLote>{id_lote}</idLote><indSinc>1</indSinc>{xml_assinado}</enviNFe>'
    soap = f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:xsd="http://www.w3.org/2001/XMLSchema"
  xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <nfeAutorizacaoLote xmlns="{NS_WSDL}">
      <nfeDadosMsg><![CDATA[{inner}]]></nfeDadosMsg>
    </nfeAutorizacaoLote>
  </soap12:Body>
</soap12:Envelope>"""
    cert_file, key_file, cleanup = _cert_pem_temporario(cfg["cert_path"], cfg["cert_password"])
    headers = {
        "Content-Type": f'application/soap+xml; charset=utf-8; action="{NS_WSDL}/nfeAutorizacaoLote"',
    }
    try:
        r = requests.post(
            url,
            data=soap.encode("utf-8"),
            headers=headers,
            cert=(cert_file, key_file),
            verify=sefaz_requests_verify(),
            timeout=90,
        )
        text = r.text or ""
        if r.status_code >= 400:
            return None, f"HTTP {r.status_code}: {text[:500]}"
        return _parse_retorno_autorizacao(text), None
    except requests.RequestException as exc:
        return None, str(exc)[:400]
    finally:
        import os

        for p in cleanup:
            try:
                os.unlink(p)
            except OSError:
                pass


def _local(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _parse_retorno_autorizacao(soap_text: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "c_stat": "",
        "x_motivo": "",
        "protocolo": "",
        "chave": "",
        "xml_nfeproc": "",
        "autorizada": False,
    }
    try:
        root = ET.fromstring(soap_text)
    except ET.ParseError:
        out["x_motivo"] = "Resposta SOAP inválida."
        return out
    nfe_result = None
    for el in root.iter():
        if _local(el.tag) == "nfeResultMsg" and el.text:
            nfe_result = el.text
            break
    if not nfe_result:
        out["x_motivo"] = "Resposta sem nfeResultMsg."
        return out
    try:
        ret_root = ET.fromstring(nfe_result)
    except ET.ParseError:
        out["x_motivo"] = "XML de retorno inválido."
        return out
    for el in ret_root.iter():
        ln = _local(el.tag)
        if ln == "cStat" and not out["c_stat"]:
            out["c_stat"] = (el.text or "").strip()
        elif ln == "xMotivo" and not out["x_motivo"]:
            out["x_motivo"] = (el.text or "").strip()
        elif ln == "nProt":
            out["protocolo"] = (el.text or "").strip()
        elif ln == "chNFe":
            out["chave"] = (el.text or "").strip()
    c_stat = out["c_stat"]
    out["autorizada"] = c_stat in ("100", "150")
    if out["autorizada"]:
        out["xml_nfeproc"] = nfe_result
    return out


def emitir_nfce_para_venda(
    venda: VendaAgro,
    *,
    cpf_dest: str = "",
    sem_identificacao: bool = False,
    db=None,
    col_p: str | None = None,
) -> dict[str, Any]:
    """
    Emite NFC-e para venda já gravada. Retorna dict com ok, chave, erro, documento_id.
    """
    doc_existente = (
        NfceDocumentoAgro.objects.filter(venda=venda, status=NfceDocumentoAgro.Status.AUTORIZADA)
        .order_by("-pk")
        .first()
    )
    if doc_existente:
        return {
            "ok": True,
            "chave": doc_existente.chave,
            "numero": doc_existente.numero,
            "serie": doc_existente.serie,
            "protocolo": doc_existente.protocolo,
            "documento_id": doc_existente.pk,
            "reutilizada": True,
        }
    NfceDocumentoAgro.objects.filter(venda=venda).exclude(status=NfceDocumentoAgro.Status.AUTORIZADA).delete()
    if not nfce_configurada():
        return {"ok": False, "erro": "NFC-e não configurada (NFC_E_ENABLED e demais variáveis no .env)."}
    cfg = nfce_cfg()
    cpf = re.sub(r"\D", "", cpf_dest)[:11]
    if cpf and not cpf_valido(cpf):
        return {"ok": False, "erro": "CPF informado é inválido."}
    if not cpf and not sem_identificacao:
        return {"ok": False, "erro": "Informe CPF do consumidor ou confirme venda sem identificação."}

    itens = list(venda.itens.all().order_by("pk"))
    if not itens:
        return {"ok": False, "erro": "Venda sem itens para NFC-e."}

    serie, numero = _proximo_numero_serie(cfg)
    dh_emi = timezone.localtime(timezone.now())
    c_nf = _gerar_c_nf()
    chave = _montar_chave(cnpj=cfg["cnpj"], serie=serie, numero=numero, tp_emis="1", c_nf=c_nf, dh_emi=dh_emi)

    fiscal_rows = [
        fiscal_por_produto_id(str(it.produto_id_externo or ""), db=db, col_p=col_p) for it in itens
    ]
    try:
        xml_body, qr_url = _montar_xml_nfce(
            cfg,
            venda,
            itens,
            serie=serie,
            numero=numero,
            chave=chave,
            dh_emi=dh_emi,
            cpf_dest=cpf,
            fiscal_itens=fiscal_rows,
            db=db,
            col_p=col_p,
        )
    except Exception as exc:
        logger.exception("montar_xml_nfce venda %s", venda.pk)
        doc = NfceDocumentoAgro.objects.create(
            venda=venda,
            status=NfceDocumentoAgro.Status.ERRO,
            numero=numero,
            serie=serie,
            mensagem_sefaz=str(exc)[:2000],
            tp_amb=int(cfg["tp_amb"]),
            dest_cpf=cpf,
            consumidor_sem_identificacao=sem_identificacao,
        )
        return {"ok": False, "erro": str(exc)[:400], "documento_id": doc.pk}

    signed, err_sign = _assinar_nfe_xml(xml_body, cfg["cert_path"], cfg["cert_password"], chave)
    if err_sign or not signed:
        doc = NfceDocumentoAgro.objects.create(
            venda=venda,
            status=NfceDocumentoAgro.Status.ERRO,
            chave=chave,
            numero=numero,
            serie=serie,
            mensagem_sefaz=err_sign or "Falha ao assinar.",
            tp_amb=int(cfg["tp_amb"]),
            dest_cpf=cpf,
            consumidor_sem_identificacao=sem_identificacao,
        )
        return {"ok": False, "erro": err_sign or "Falha ao assinar XML.", "documento_id": doc.pk}

    signed_com_qr = _anexar_suplementar_qrcode(signed, qr_url, int(cfg["tp_amb"]))

    ret, err_http = _enviar_autorizacao(signed_com_qr, cfg)
    if err_http or not ret:
        doc = NfceDocumentoAgro.objects.create(
            venda=venda,
            status=NfceDocumentoAgro.Status.ERRO,
            chave=chave,
            numero=numero,
            serie=serie,
            mensagem_sefaz=err_http or "Sem resposta SEFAZ.",
            tp_amb=int(cfg["tp_amb"]),
            dest_cpf=cpf,
            consumidor_sem_identificacao=sem_identificacao,
        )
        return {"ok": False, "erro": err_http or "Sem resposta SEFAZ.", "documento_id": doc.pk}

    st = NfceDocumentoAgro.Status.AUTORIZADA if ret.get("autorizada") else NfceDocumentoAgro.Status.REJEITADA
    xml_save = ret.get("xml_nfeproc") or signed_com_qr
    doc = NfceDocumentoAgro.objects.create(
        venda=venda,
        status=st,
        chave=ret.get("chave") or chave,
        numero=numero,
        serie=serie,
        protocolo=ret.get("protocolo") or "",
        xml_autorizado=xml_save,
        qr_code_url=qr_url,
        mensagem_sefaz=f"{ret.get('c_stat', '')} — {ret.get('x_motivo', '')}".strip(" —"),
        tp_amb=int(cfg["tp_amb"]),
        dest_cpf=cpf,
        consumidor_sem_identificacao=sem_identificacao,
    )
    if st == NfceDocumentoAgro.Status.AUTORIZADA:
        return {
            "ok": True,
            "chave": doc.chave,
            "numero": doc.numero,
            "serie": doc.serie,
            "protocolo": doc.protocolo,
            "documento_id": doc.pk,
            "qr_code_url": doc.qr_code_url,
        }
    return {
        "ok": False,
        "erro": doc.mensagem_sefaz or "NFC-e rejeitada pela SEFAZ.",
        "documento_id": doc.pk,
        "c_stat": ret.get("c_stat"),
    }
