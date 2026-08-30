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
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import requests
from django.db import connection, transaction
from django.db.models import Max
from django.db.utils import IntegrityError
from django.utils import timezone

from produtos.caixa_util import normalizar_forma_pagamento_caixa, pagamentos_lista_de_venda
from produtos.models import ItemVendaAgro, NfceDocumentoAgro, NfceNumeracaoAgro, VendaAgro
from produtos.nfce_config_util import (
    nfce_cfg,
    nfce_configurada,
    nfce_cnpj_da_chave,
    nfce_loja_de_cnpj,
    nfce_loja_de_venda,
)
from produtos.sefaz_soap_util import (
    montar_envelope_nfe_dados_msg,
    normalizar_xml_envio,
    SEFAZ_HTTP_RETRY_DELAYS_S,
    SEFAZ_HTTP_RETRY_DELAYS_SYNC,
    SEFAZ_HTTP_TIMEOUT,
    SEFAZ_HTTP_TIMEOUT_SYNC,
    sanitizar_erro_http_sefaz,
    sanitizar_erro_sefaz_exibicao,
    sefaz_erro_transiente,
    sefaz_http_status_retry,
)
from produtos.sefaz_ssl_util import sefaz_requests_verify
from produtos.sefaz_xml_fiscal_util import tostring_sem_prefixos
from produtos.nfce_fiscal_produto_util import fiscal_por_produto_id
from produtos.nfce_ibpt_util import calcular_ibpt_venda_itens, ibpt_valor_item

logger = logging.getLogger(__name__)

NS = "http://www.portalfiscal.inf.br/nfe"
NS_WSDL = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeAutorizacao4"

URL_AUTORIZACAO = {
    1: "https://nfce.fazenda.sp.gov.br/ws/NFeAutorizacao4.asmx",
    2: "https://homologacao.nfce.fazenda.sp.gov.br/ws/NFeAutorizacao4.asmx",
}

URL_RECEPCAO_EVENTO = {
    1: "https://nfce.fazenda.sp.gov.br/ws/NFeRecepcaoEvento4.asmx",
    2: "https://homologacao.nfce.fazenda.sp.gov.br/ws/NFeRecepcaoEvento4.asmx",
}

NS_WSDL_EVENTO = "http://www.portalfiscal.inf.br/nfe/wsdl/NFeRecepcaoEvento4"

NFCE_MOTIVO_CANCELAMENTO_PADRAO = "Devolucao de mercadoria registrada no sistema Agro."

# Evento cancelamento registrado / duplicidade / NF já cancelada.
_NFCE_CANCEL_CSTAT_OK = frozenset({"135", "155", "573", "220"})

URL_QR_BASE = {
    1: "https://www.nfce.fazenda.sp.gov.br/NFCeConsultaPublica/Paginas/ConsultaQRCode.aspx",
    2: "https://homologacao.nfce.fazenda.sp.gov.br/NFCeConsultaPublica/Paginas/ConsultaQRCode.aspx",
}

CUF_SP = "35"

# SEFAZ já registrou esse nNF com outra chave (testes repetidos) — tenta próximo número.
_NFCE_RETRY_CSTAT_DUPLICIDADE = frozenset({"539", "204"})

# NT 2023.004 / 2024.003 — só estes tPag aceitam grupo card (tpIntegra 2 = sem TEF).
# Incluir 05 (crédito loja/fiado) ou 19–22 → SEFAZ 963.
_TPAG_REQUER_CARD = frozenset({"03", "04", "10", "11", "12", "13", "15", "17", "18"})


def _q2(v: Decimal | float) -> str:
    return str(Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _q4(v: Decimal | float) -> str:
    return str(Decimal(str(v)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def _ratear_valor_proporcional(valores: list[Decimal], total_rateio: Decimal) -> list[Decimal]:
    """
    Rateia ``total_rateio`` pelos pesos em ``valores`` (centavos).
    Soma das partes == total_rateio; cada parte ≤ peso correspondente.
    Usado no desconto NFC-e (SEFAZ 531: ICMSTot/vDesc = soma det/prod/vDesc).
    """
    n = len(valores)
    if n == 0:
        return []
    total_rateio = max(
        Decimal("0"),
        Decimal(str(total_rateio or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
    )
    pesos = [
        max(Decimal("0"), Decimal(str(v or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        for v in valores
    ]
    soma = sum(pesos, Decimal("0"))
    if total_rateio <= 0 or soma <= 0:
        return [Decimal("0")] * n
    if total_rateio > soma:
        total_rateio = soma
    out: list[Decimal] = []
    acumulado = Decimal("0")
    for i, peso in enumerate(pesos):
        if i == n - 1:
            parte = (total_rateio - acumulado).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        else:
            parte = (peso * total_rateio / soma).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if parte > peso:
            parte = peso
        if parte < 0:
            parte = Decimal("0")
        out.append(parte)
        acumulado += parte
    # Ajuste fino se teto no último item deixou diferença de centavos.
    diff = (total_rateio - sum(out, Decimal("0"))).quantize(Decimal("0.01"))
    if diff != 0:
        passo = Decimal("0.01") if diff > 0 else Decimal("-0.01")
        restantes = int(abs(diff) * 100)
        ordem = sorted(range(n), key=lambda i: pesos[i], reverse=True)
        for _ in range(restantes):
            moved = False
            for i in ordem:
                cand = (out[i] + passo).quantize(Decimal("0.01"))
                if cand < 0 or cand > pesos[i]:
                    continue
                out[i] = cand
                moved = True
                break
            if not moved:
                break
    return out


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


def cnpj_valido(cnpj_raw: str) -> bool:
    cnpj = re.sub(r"\D", "", str(cnpj_raw or ""))[:14]
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False

    def _dv(digs: str, pesos: list[int]) -> int:
        s = sum(int(d) * p for d, p in zip(digs, pesos))
        r = s % 11
        return 0 if r < 2 else 11 - r

    if int(cnpj[12]) != _dv(cnpj[:12], [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]):
        return False
    return int(cnpj[13]) == _dv(cnpj[:13], [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])


def documento_dest_nfce(raw: str) -> str:
    """CPF (11) ou CNPJ (14) válido para destinatário da NFC-e; senão vazio."""
    d = re.sub(r"\D", "", str(raw or ""))
    if len(d) == 11 and cpf_valido(d):
        return d
    if len(d) == 14 and cnpj_valido(d):
        return d
    return ""


def tipo_documento_dest_nfce(raw: str) -> str:
    d = documento_dest_nfce(raw)
    if len(d) == 14:
        return "CNPJ"
    if len(d) == 11:
        return "CPF"
    return ""


def mensagem_doc_dest_invalido(raw: str) -> str:
    d = re.sub(r"\D", "", str(raw or ""))
    if len(d) >= 12:
        return "CNPJ informado é inválido."
    if d:
        return "CPF informado é inválido."
    return "CPF/CNPJ informado é inválido."


_RE_CONSUMIDOR_GENERICO = re.compile(r"consumidor\s+n[aão]+\s+identificado", re.I)


def _nome_destinatario_nfce(venda) -> str:
    nome = _sanitizar_texto_xml(str(getattr(venda, "cliente_nome", "") or "").strip())
    if not nome or _RE_CONSUMIDOR_GENERICO.search(nome):
        return ""
    return nome[:60]


def _preencher_dest_nfce(inf: ET.Element, doc_dest: str, venda=None) -> None:
    """dest/CPF ou dest/CNPJ + indIEDest=9 (consumidor final / não contribuinte)."""
    digits = re.sub(r"\D", "", str(doc_dest or ""))
    if not digits:
        return
    dest = _sub(inf, "dest")
    if len(digits) == 14:
        _sub(dest, "CNPJ", digits[:14])
        nome = _nome_destinatario_nfce(venda)
        if nome:
            _sub(dest, "xNome", nome)
    else:
        _sub(dest, "CPF", digits[:11])
    _sub(dest, "indIEDest", "9")


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


def _sanitizar_texto_xml(text: str) -> str:
    """Evita caracteres tipográficos que quebram validação XSD em alguns ambientes."""
    t = str(text or "")
    return t.replace("\u2014", "-").replace("\u2013", "-").replace("\u00b7", ",")


def _map_tpag(forma: str) -> str:
    f = str(forma or "").strip().lower()
    if "dinheiro" in f:
        return "01"
    if "pix" in f or ("mercado pago" in f and "qr" in f):
        return "17"
    if "vale" in f and ("crédito" in f or "credito" in f):
        return "99"
    if "cashback" in f:
        return "99"
    if "débito" in f or "debito" in f:
        return "04"
    if "crédito" in f or "credito" in f:
        return "03"
    if "fiado" in f or "credito loja" in f or "crédito loja" in f:
        return "05"
    if "outro" in f:
        return "99"
    return "99"


def _pagamentos_da_venda(venda: VendaAgro, *, total_nf: Decimal | None = None) -> list[dict[str, Any]]:
    rows = pagamentos_lista_de_venda(venda)
    raw_formas: list[str] = []
    pj = getattr(venda, "pagamentos_json", None)
    if isinstance(pj, list):
        for row in pj:
            if not isinstance(row, dict):
                raw_formas.append("")
                continue
            raw_formas.append(
                str(
                    row.get("formaPagamento")
                    or row.get("forma_pagamento")
                    or row.get("forma")
                    or ""
                ).strip()
            )
    out: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        forma = str(row.get("forma") or "Outros").strip() or "Outros"
        tpag = _map_tpag(forma)
        if tpag == "99" and idx < len(raw_formas) and raw_formas[idx]:
            alt = _map_tpag(raw_formas[idx])
            if alt != "99":
                tpag = alt
                forma = normalizar_forma_pagamento_caixa(raw_formas[idx])
        try:
            val = Decimal(str(row.get("valor") or 0)).quantize(Decimal("0.01"))
        except Exception:
            val = Decimal("0")
        if val <= 0:
            continue
        out.append(
            {
                "tPag": tpag,
                "vPag": val,
                "xPag": forma[:60] if tpag == "99" else "",
            }
        )
    if not out:
        val = (total_nf or Decimal(str(venda.total or 0))).quantize(Decimal("0.01"))
        out.append({"tPag": "01", "vPag": val, "xPag": ""})
    for pg in out:
        if pg["tPag"] == "99" and not str(pg.get("xPag") or "").strip():
            pg["xPag"] = "Outros"
    if total_nf is not None and out:
        soma = sum(p["vPag"] for p in out)
        diff = total_nf - soma
        if abs(diff) >= Decimal("0.01"):
            out[-1]["vPag"] = (out[-1]["vPag"] + diff).quantize(Decimal("0.01"))
    return out


def _montar_det_pag(pag: ET.Element, pg: dict[str, Any]) -> None:
    tpag = str(pg["tPag"])
    det_p = _sub(pag, "detPag")
    _sub(det_p, "indPag", "0")
    _sub(det_p, "tPag", tpag)
    if tpag == "99" and pg.get("xPag"):
        _sub(det_p, "xPag", str(pg["xPag"])[:60])
    _sub(det_p, "vPag", _q2(pg["vPag"]))
    if tpag in _TPAG_REQUER_CARD:
        card = _sub(det_p, "card")
        _sub(card, "tpIntegra", "2")


def _sync_nfcenumeracao_pk_sequence() -> None:
    """Postgres: sequência do PK fora de sincronia após 1ª linha (Centro) quebra get_or_create da Vila."""
    if connection.vendor != "postgresql":
        return
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT setval(
                pg_get_serial_sequence('produtos_nfcenumeracaoagro', 'id'),
                COALESCE((SELECT MAX(id) FROM produtos_nfcenumeracaoagro), 1)
            )
            """
        )


def _get_or_create_numeracao(
    *,
    cnpj: str,
    serie: int,
    proximo_default: int = 1,
) -> tuple[NfceNumeracaoAgro, bool]:
    """get_or_create com retry se a sequência do PK estiver atrasada (IntegrityError pkey)."""
    try:
        return NfceNumeracaoAgro.objects.select_for_update().get_or_create(
            emitente_cnpj=cnpj,
            serie=serie,
            defaults={"proximo_numero": max(1, int(proximo_default or 1))},
        )
    except IntegrityError:
        _sync_nfcenumeracao_pk_sequence()
        return NfceNumeracaoAgro.objects.select_for_update().get_or_create(
            emitente_cnpj=cnpj,
            serie=serie,
            defaults={"proximo_numero": max(1, int(proximo_default or 1))},
        )


def _garantir_numeracao_inicial(cfg: dict[str, Any]) -> None:
    """Série/número alinhados por CNPJ emitente (Centro e Vila separados)."""
    serie_cfg = int(cfg["serie"])
    cnpj = re.sub(r"\D", "", str(cfg.get("cnpj") or ""))[:14]
    piso = int(cfg.get("proximo_numero_inicial") or 1)
    with transaction.atomic():
        num, _ = _get_or_create_numeracao(cnpj=cnpj, serie=serie_cfg, proximo_default=piso)
        qs = NfceDocumentoAgro.objects.filter(serie=serie_cfg, emitente_cnpj=cnpj)
        max_doc = qs.aggregate(m=Max("numero")).get("m") or 0
        # Legado: docs sem emitente_cnpj (só Centro, antes da filial)
        if nfce_loja_de_cnpj(cnpj) == "centro":
            max_legado = (
                NfceDocumentoAgro.objects.filter(serie=serie_cfg, emitente_cnpj="")
                .aggregate(m=Max("numero"))
                .get("m")
                or 0
            )
            max_doc = max(int(max_doc), int(max_legado))
        alvo = max(piso, int(max_doc) + 1)
        if int(num.proximo_numero or 1) < alvo:
            num.proximo_numero = alvo
            num.save(update_fields=["proximo_numero", "atualizado_em"])


def _proximo_numero_serie(cfg: dict[str, Any]) -> tuple[int, int]:
    cnpj = re.sub(r"\D", "", str(cfg.get("cnpj") or ""))[:14]
    serie_cfg = int(cfg["serie"])
    piso = int(cfg.get("proximo_numero_inicial") or 1)
    with transaction.atomic():
        num, _ = _get_or_create_numeracao(cnpj=cnpj, serie=serie_cfg, proximo_default=piso)
        n = max(int(num.proximo_numero or 1), piso)
        num.proximo_numero = n + 1
        num.save(update_fields=["proximo_numero", "atualizado_em"])
        return serie_cfg, n


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
        return tostring_sem_prefixos(signed), None
    except Exception as exc:
        logger.exception("assinar_nfce")
        return None, str(exc)[:400]


def _qr_code_url(chave: str, tp_amb: int, csc_id: int, csc_token: str) -> str:
    """QR Code v2 online SP — hash: SHA1(chave|2|tpAmb|idCSC + tokenCSC)."""
    n_versao = "2"
    id_csc = str(int(csc_id))
    token = (csc_token or "").strip()
    payload = f"{chave}|{n_versao}|{tp_amb}|{id_csc}{token}"
    digest = hashlib.sha1(payload.encode()).hexdigest().upper()
    p = f"{chave}|{n_versao}|{tp_amb}|{id_csc}|{digest}"
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
        _preencher_dest_nfce(inf, cpf_dest, venda)

    uf_ibpt = cfg.get("uf") or "SP"
    v_frete = Decimal(str(getattr(venda, "frete", 0) or 0)).quantize(Decimal("0.01"))
    if v_frete < 0:
        v_frete = Decimal("0")
    # SEFAZ 535: ICMSTot/vFrete = soma det/prod/vFrete — coloca o frete no 1º item.
    # SEFAZ 531: ICMSTot/vDesc = soma det/prod/vDesc — rateia desconto geral nos itens.
    # Se o desconto “passa” dos produtos (ex. total R$ 0 com frete), o restante abate o frete.
    item_vprods: list[Decimal] = []
    for item in itens:
        item_vprods.append(Decimal(str(item.valor_total or 0)).quantize(Decimal("0.01")))
    total_prod = sum(item_vprods, Decimal("0"))
    total_nf = Decimal(str(venda.total if venda.total is not None else total_prod)).quantize(Decimal("0.01"))
    raw_desc = max(Decimal("0"), (total_prod + v_frete - total_nf).quantize(Decimal("0.01")))
    v_desc = min(raw_desc, total_prod)
    resto_desc = (raw_desc - v_desc).quantize(Decimal("0.01"))
    if resto_desc > 0 and v_frete > 0:
        v_frete = max(Decimal("0"), (v_frete - resto_desc).quantize(Decimal("0.01")))
    descontos_itens = _ratear_valor_proporcional(item_vprods, v_desc)
    v_desc = sum(descontos_itens, Decimal("0")).quantize(Decimal("0.01"))
    total_v_tot_trib = Decimal("0")
    for idx, item in enumerate(itens, start=1):
        fis = fiscal_itens[idx - 1] if idx - 1 < len(fiscal_itens) else fiscal_por_produto_id("", db=db, col_p=col_p)
        qtd = Decimal(str(item.quantidade or 0))
        vu = Decimal(str(item.valor_unitario or 0))
        vt = item_vprods[idx - 1]
        v_desc_item = descontos_itens[idx - 1] if idx - 1 < len(descontos_itens) else Decimal("0")
        v_tot_trib_item = ibpt_valor_item(item, db=db, col_p=col_p, uf=uf_ibpt, fiscal=fis)
        total_v_tot_trib += v_tot_trib_item
        v_frete_item = v_frete if idx == 1 and v_frete > 0 else Decimal("0")
        det = ET.SubElement(inf, f"{{{NS}}}det")
        det.set("nItem", str(idx))
        prod = _sub(det, "prod")
        _sub(prod, "cProd", str(item.codigo or item.produto_id_externo or idx)[:60])
        _sub(prod, "cEAN", "SEM GTIN")
        _sub(prod, "xProd", (item.descricao or "PRODUTO")[:120])
        _sub(prod, "NCM", fis["ncm"])
        cest = re.sub(r"\D", "", str(fis.get("cest") or ""))
        if len(cest) == 7:
            _sub(prod, "CEST", cest)
        _sub(prod, "CFOP", fis["cfop"])
        u_com = str(getattr(item, "unidade", None) or "UN").strip().upper() or "UN"
        if u_com in ("KG.", "QUILO", "KILO", "KGS"):
            u_com = "KG"
        # Só unidades fiscais curtas; cadastro sujo → UN (comportamento antigo).
        if u_com not in ("UN", "KG", "G", "L", "ML", "M", "CM", "PC", "CX", "FD", "PCT", "DZ"):
            u_com = "UN"
        if len(u_com) > 6:
            u_com = u_com[:6]
        _sub(prod, "uCom", u_com)
        _sub(prod, "qCom", _q4(qtd))
        _sub(prod, "vUnCom", _q4(vu))
        _sub(prod, "vProd", _q2(vt))
        _sub(prod, "cEANTrib", "SEM GTIN")
        _sub(prod, "uTrib", u_com)
        _sub(prod, "qTrib", _q4(qtd))
        _sub(prod, "vUnTrib", _q4(vu))
        if v_frete_item > 0:
            _sub(prod, "vFrete", _q2(v_frete_item))
        # SEFAZ 537: se há desconto no total, informar vDesc em todo item (0.00 ok).
        if v_desc > 0:
            _sub(prod, "vDesc", _q2(v_desc_item))
        _sub(prod, "indTot", "1")
        imposto = _sub(det, "imposto")
        if v_tot_trib_item > 0:
            _sub(imposto, "vTotTrib", _q2(v_tot_trib_item))
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

    ibpt = calcular_ibpt_venda_itens(itens, db=db, col_p=col_p, uf=uf_ibpt)
    icms_tot = _sub(inf, "total")
    icms = _sub(icms_tot, "ICMSTot")
    v_tot_trib_nf = total_v_tot_trib.quantize(Decimal("0.01"))
    icms_tags: list[tuple[str, str]] = [
        ("vBC", "0.00"),
        ("vICMS", "0.00"),
        ("vICMSDeson", "0.00"),
        ("vFCPUFDest", "0.00"),
        ("vICMSUFDest", "0.00"),
        ("vICMSUFRemet", "0.00"),
        ("vFCP", "0.00"),
        ("vBCST", "0.00"),
        ("vST", "0.00"),
        ("vFCPST", "0.00"),
        ("vFCPSTRet", "0.00"),
        ("vProd", _q2(total_prod)),
        ("vFrete", _q2(v_frete)),
        ("vSeg", "0.00"),
        ("vDesc", _q2(v_desc)),
        ("vII", "0.00"),
        ("vIPI", "0.00"),
        ("vIPIDevol", "0.00"),
        ("vPIS", "0.00"),
        ("vCOFINS", "0.00"),
        ("vOutro", "0.00"),
        ("vNF", _q2(total_nf)),
    ]
    if v_tot_trib_nf > 0:
        icms_tags.append(("vTotTrib", _q2(v_tot_trib_nf)))
    for tag, val in icms_tags:
        _sub(icms, tag, val)

    transp = _sub(inf, "transp")
    _sub(transp, "modFrete", "9")

    pag = _sub(inf, "pag")
    for pg in _pagamentos_da_venda(venda, total_nf=total_nf):
        _montar_det_pag(pag, pg)

    inf_ad = _sub(inf, "infAdic")
    obs = f"Venda PDV #{venda.pk} | {ibpt['ibpt_texto']}"
    if tp_amb == 2:
        obs = "NF-E EMITIDA EM AMBIENTE DE HOMOLOGACAO - SEM VALOR FISCAL | " + obs
    _sub(inf_ad, "infCpl", _sanitizar_texto_xml(obs)[:5000])

    qr_url = _qr_code_url(chave, tp_amb, int(cfg["csc_id"]), cfg["csc_token"])
    xml_body = tostring_sem_prefixos(ET.tostring(nfe, encoding="unicode"))
    return xml_body, qr_url


def _anexar_suplementar_qrcode(xml_nfe: str, qr_url: str, tp_amb: int) -> str:
    """Insere infNFeSupl após infNFe e antes da Signature (ordem exigida pelo XSD)."""
    url_chave = (
        "https://www.nfce.fazenda.sp.gov.br/consulta"
        if tp_amb == 1
        else "https://homologacao.nfce.fazenda.sp.gov.br/consulta"
    )
    supl = (
        f"<infNFeSupl>"
        f"<qrCode><![CDATA[{qr_url}]]></qrCode>"
        f"<urlChave>{url_chave}</urlChave>"
        f"</infNFeSupl>"
    )
    m_sig = re.search(r"<Signature\b", xml_nfe)
    if m_sig:
        return xml_nfe[: m_sig.start()] + supl + xml_nfe[m_sig.start() :]
    m = re.search(r"</infNFe>", xml_nfe)
    if not m:
        return xml_nfe
    return xml_nfe[: m.end()] + supl + xml_nfe[m.end() :]


def _enviar_autorizacao(
    xml_assinado: str,
    cfg: dict[str, Any],
    *,
    retry_delays: tuple[float, ...] | None = None,
    http_timeout: tuple[int, int] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    tp_amb = int(cfg["tp_amb"])
    url = URL_AUTORIZACAO.get(tp_amb, URL_AUTORIZACAO[2])
    id_lote = str(int(datetime.now().timestamp()))[-15:]
    nfe_xml = normalizar_xml_envio(xml_assinado)
    envi_nfe = (
        f'<enviNFe xmlns="{NS}" versao="4.00">'
        f"<idLote>{id_lote}</idLote><indSinc>1</indSinc>{nfe_xml}</enviNFe>"
    )
    soap, headers = montar_envelope_nfe_dados_msg(NS_WSDL, envi_nfe, "nfeAutorizacaoLote")
    cert_file, key_file, cleanup = _cert_pem_temporario(cfg["cert_path"], cfg["cert_password"])
    delays = retry_delays if retry_delays is not None else SEFAZ_HTTP_RETRY_DELAYS_SYNC
    timeout = http_timeout if http_timeout is not None else SEFAZ_HTTP_TIMEOUT_SYNC
    last_err = ""
    try:
        for attempt, delay_s in enumerate(delays):
            if attempt > 0:
                time.sleep(delay_s)
            try:
                r = requests.post(
                    url,
                    data=soap.encode("utf-8"),
                    headers=headers,
                    cert=(cert_file, key_file),
                    verify=sefaz_requests_verify(),
                    timeout=timeout,
                )
                text = r.text or ""
                if r.status_code >= 400:
                    last_err = sanitizar_erro_http_sefaz(r.status_code, text)
                    if sefaz_http_status_retry(r.status_code) and attempt + 1 < len(delays):
                        logger.warning(
                            "SEFAZ autorização HTTP %s — retry %s/%s",
                            r.status_code,
                            attempt + 1,
                            len(delays),
                        )
                        continue
                    return None, last_err
                return _parse_retorno_autorizacao(text, xml_nfe_assinado=xml_assinado), None
            except requests.RequestException as exc:
                last_err = sanitizar_erro_sefaz_exibicao(str(exc))
                if sefaz_erro_transiente(str(exc)) and attempt + 1 < len(delays):
                    logger.warning(
                        "SEFAZ autorização rede — retry %s/%s: %s",
                        attempt + 1,
                        len(delays),
                        last_err[:160],
                    )
                    continue
                return None, last_err
        return None, last_err or "Sem resposta SEFAZ."
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


def _elemento_para_xml_str(el: ET.Element | None) -> str:
    if el is None:
        return ""
    txt = (el.text or "").strip()
    if txt:
        return txt
    if len(el):
        return ET.tostring(el, encoding="unicode")
    return "".join(el.itertext()).strip()


def _parse_xml_fiscal(raw: str) -> ET.Element:
    raw = (raw or "").strip()
    if raw.startswith("<?xml"):
        end = raw.find("?>")
        if end >= 0:
            raw = raw[end + 2 :].strip()
    return ET.fromstring(raw)


def _parse_retorno_autorizacao(soap_text: str, *, xml_nfe_assinado: str = "") -> dict[str, Any]:
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
        snippet = re.sub(r"\s+", " ", (soap_text or ""))[:350]
        out["x_motivo"] = f"Resposta SOAP inválida. Trecho: {snippet}"
        logger.warning("NFC-e SOAP inválido: %s", (soap_text or "")[:2000])
        return out

    for el in root.iter():
        if _local(el.tag) != "Fault":
            continue
        fault_txt = ""
        for ch in el.iter():
            tl = _local(ch.tag)
            if tl in ("faultstring", "Text") and ch.text:
                fault_txt = ch.text.strip()
                break
        out["x_motivo"] = fault_txt or "Erro SOAP Fault na SEFAZ."
        return out

    payload = ""
    ret_envi_el = None
    for el in root.iter():
        ln = _local(el.tag)
        if ln == "retEnviNFe" and ret_envi_el is None:
            ret_envi_el = el
        elif ln in ("nfeResultMsg", "nfeAutorizacaoLoteResult") and not payload:
            payload = _elemento_para_xml_str(el)

    if not payload and ret_envi_el is not None:
        payload = ET.tostring(ret_envi_el, encoding="unicode")

    if not payload:
        snippet = re.sub(r"\s+", " ", soap_text)[:350]
        logger.warning("NFC-e SOAP sem retEnviNFe/nfeResultMsg: %s", soap_text[:2500])
        out["x_motivo"] = f"Resposta SOAP sem retorno reconhecido. Trecho: {snippet}"
        return out

    try:
        ret_root = _parse_xml_fiscal(payload)
    except ET.ParseError:
        snippet = re.sub(r"\s+", " ", payload)[:350]
        out["x_motivo"] = f"XML de retorno inválido. Trecho: {snippet}"
        return out

    lote_cstat = ""
    lote_motivo = ""
    prot_nfe_el = None
    for el in ret_root.iter():
        ln = _local(el.tag)
        if ln == "retEnviNFe":
            for ch in el:
                cl = _local(ch.tag)
                if cl == "cStat" and ch.text and not lote_cstat:
                    lote_cstat = ch.text.strip()
                elif cl == "xMotivo" and ch.text and not lote_motivo:
                    lote_motivo = ch.text.strip()
                elif cl == "protNFe" and prot_nfe_el is None:
                    prot_nfe_el = ch
            break
    if not lote_cstat:
        for el in ret_root.iter():
            if _local(el.tag) == "cStat" and el.text:
                lote_cstat = el.text.strip()
                break
    if not lote_motivo:
        for el in ret_root.iter():
            if _local(el.tag) == "xMotivo" and el.text:
                lote_motivo = el.text.strip()
                break
    if prot_nfe_el is None:
        for el in ret_root.iter():
            if _local(el.tag) == "protNFe":
                prot_nfe_el = el
                break

    prot_cstat = ""
    prot_motivo = ""
    if prot_nfe_el is not None:
        for el in prot_nfe_el.iter():
            ln = _local(el.tag)
            if ln == "cStat" and el.text:
                prot_cstat = el.text.strip()
            elif ln == "xMotivo" and el.text:
                prot_motivo = el.text.strip()
            elif ln == "nProt" and el.text:
                out["protocolo"] = el.text.strip()
            elif ln == "chNFe" and el.text:
                out["chave"] = el.text.strip()

    if lote_cstat == "104" and prot_cstat:
        out["c_stat"] = prot_cstat
        out["x_motivo"] = prot_motivo or lote_motivo
    else:
        out["c_stat"] = lote_cstat or prot_cstat
        out["x_motivo"] = lote_motivo or prot_motivo

    out["autorizada"] = prot_cstat in ("100", "150") or (
        lote_cstat in ("100", "150") and not prot_cstat
    )

    if out["autorizada"] and xml_nfe_assinado and prot_nfe_el is not None:
        nfe_xml = normalizar_xml_envio(xml_nfe_assinado)
        prot_xml = normalizar_xml_envio(ET.tostring(prot_nfe_el, encoding="unicode"))
        out["xml_nfeproc"] = (
            f'<nfeProc xmlns="{NS}" versao="4.00">{nfe_xml}{prot_xml}</nfeProc>'
        )
    elif out["autorizada"]:
        out["xml_nfeproc"] = payload

    if not out["autorizada"] and lote_cstat == "104" and not prot_cstat:
        out["x_motivo"] = lote_motivo or "Lote processado sem protocolo da NFC-e."
        logger.warning("NFC-e 104 sem protNFe: %s", payload[:2500])

    return out


def _validar_fiscal_itens_nfce(itens: list, fiscal_rows: list[dict[str, str]]) -> str | None:
    for idx, item in enumerate(itens, start=1):
        fis = fiscal_rows[idx - 1] if idx - 1 < len(fiscal_rows) else {}
        ncm = re.sub(r"\D", "", str(fis.get("ncm") or ""))
        if len(ncm) != 8:
            nome = (item.descricao or f"Item {idx}")[:50]
            return f"Produto «{nome}» com NCM inválido ({ncm or 'vazio'}). Ajuste no cadastro/gestão."
        cfop = re.sub(r"\D", "", str(fis.get("cfop") or ""))
        if len(cfop) != 4:
            nome = (item.descricao or f"Item {idx}")[:50]
            return f"Produto «{nome}» com CFOP inválido ({cfop or 'vazio'})."
    return None


def emitir_nfce_para_venda(
    venda: VendaAgro,
    *,
    cpf_dest: str = "",
    sem_identificacao: bool = False,
    db=None,
    col_p: str | None = None,
    sefaz_perfil: str = "sync",
) -> dict[str, Any]:
    """
    Emite NFC-e para venda já gravada. Retorna dict com ok, chave, erro, documento_id.
    sefaz_perfil: ``sync`` (reemitir/PDV, timeout curto) ou ``completo`` (background).
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
    loja = nfce_loja_de_venda(venda)
    if not nfce_configurada(warmup=True, tentativas=3, loja=loja):
        rotulo = "Vila Elias" if loja == "vila" else "Centro"
        return {
            "ok": False,
            "erro": f"NFC-e {rotulo} não configurada (NFC_E_ENABLED e dados do emitente no .env).",
        }
    cfg = nfce_cfg(loja)
    tp_amb = int(cfg["tp_amb"])
    digits_dest = re.sub(r"\D", "", str(cpf_dest or ""))
    cpf = documento_dest_nfce(cpf_dest)
    if digits_dest and not cpf:
        from produtos.nfce_venda_util import registrar_nfce_erro_venda

        err_doc = mensagem_doc_dest_invalido(digits_dest)
        doc = registrar_nfce_erro_venda(
            venda,
            err_doc,
            cpf_dest=digits_dest[:14],
            sem_identificacao=sem_identificacao,
            tp_amb=tp_amb,
        )
        return {"ok": False, "erro": err_doc, "documento_id": doc.pk}
    if not cpf and not sem_identificacao:
        from produtos.nfce_venda_util import registrar_nfce_erro_venda

        doc = registrar_nfce_erro_venda(
            venda,
            "Informe CPF ou CNPJ do consumidor ou confirme venda sem identificação.",
            tp_amb=tp_amb,
        )
        return {
            "ok": False,
            "erro": "Informe CPF ou CNPJ do consumidor ou confirme venda sem identificação.",
            "documento_id": doc.pk,
        }

    itens = list(venda.itens.all().order_by("pk"))
    if not itens:
        from produtos.nfce_venda_util import registrar_nfce_erro_venda

        doc = registrar_nfce_erro_venda(
            venda,
            "Venda sem itens para NFC-e.",
            cpf_dest=cpf,
            sem_identificacao=sem_identificacao,
            tp_amb=tp_amb,
        )
        return {"ok": False, "erro": "Venda sem itens para NFC-e.", "documento_id": doc.pk}

    _garantir_numeracao_inicial(cfg)

    fiscal_rows = [
        fiscal_por_produto_id(str(it.produto_id_externo or ""), db=db, col_p=col_p) for it in itens
    ]
    err_fis = _validar_fiscal_itens_nfce(itens, fiscal_rows)
    if err_fis:
        from produtos.nfce_venda_util import registrar_nfce_erro_venda

        doc = registrar_nfce_erro_venda(
            venda,
            err_fis,
            cpf_dest=cpf,
            sem_identificacao=sem_identificacao,
            tp_amb=tp_amb,
        )
        return {"ok": False, "erro": err_fis, "documento_id": doc.pk}

    if sefaz_perfil == "completo":
        sefaz_delays = SEFAZ_HTTP_RETRY_DELAYS_S
        sefaz_timeout = SEFAZ_HTTP_TIMEOUT
    else:
        sefaz_delays = SEFAZ_HTTP_RETRY_DELAYS_SYNC
        sefaz_timeout = SEFAZ_HTTP_TIMEOUT_SYNC

    ret: dict[str, Any] | None = None
    err_http: str | None = None
    serie = 0
    numero = 0
    chave = ""
    qr_url = ""
    signed = ""

    for tentativa in range(5):
        serie, numero = _proximo_numero_serie(cfg)
        dh_emi = timezone.localtime(timezone.now())
        c_nf = _gerar_c_nf()
        chave = _montar_chave(cnpj=cfg["cnpj"], serie=serie, numero=numero, tp_emis="1", c_nf=c_nf, dh_emi=dh_emi)
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
                emitente_cnpj=cfg["cnpj"],
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
                emitente_cnpj=cfg["cnpj"],
                mensagem_sefaz=err_sign or "Falha ao assinar.",
                tp_amb=int(cfg["tp_amb"]),
                dest_cpf=cpf,
                consumidor_sem_identificacao=sem_identificacao,
            )
            return {"ok": False, "erro": err_sign or "Falha ao assinar XML.", "documento_id": doc.pk}

        signed = _anexar_suplementar_qrcode(signed, qr_url, int(cfg["tp_amb"]))
        try:
            signed = tostring_sem_prefixos(signed)
        except Exception as exc:
            logger.exception("XML NFC-e inválido após QR venda %s", venda.pk)
            doc = NfceDocumentoAgro.objects.create(
                venda=venda,
                status=NfceDocumentoAgro.Status.ERRO,
                chave=chave,
                numero=numero,
                serie=serie,
                emitente_cnpj=cfg["cnpj"],
                mensagem_sefaz=str(exc)[:2000],
                tp_amb=int(cfg["tp_amb"]),
                dest_cpf=cpf,
                consumidor_sem_identificacao=sem_identificacao,
            )
            return {"ok": False, "erro": str(exc)[:400], "documento_id": doc.pk}
        ret, err_http = _enviar_autorizacao(
            signed,
            cfg,
            retry_delays=sefaz_delays,
            http_timeout=sefaz_timeout,
        )
        if err_http or not ret:
            break
        if ret.get("autorizada"):
            break
        cstat = str(ret.get("c_stat") or "").strip()
        if cstat not in _NFCE_RETRY_CSTAT_DUPLICIDADE:
            break
        logger.warning(
            "NFC-e %s nNF=%s — tentando próximo número (%s/5)",
            ret.get("c_stat"),
            numero,
            tentativa + 1,
        )

    if err_http or not ret:
        doc = NfceDocumentoAgro.objects.create(
            venda=venda,
            status=NfceDocumentoAgro.Status.ERRO,
            chave=chave,
            numero=numero,
            serie=serie,
            emitente_cnpj=cfg["cnpj"],
            mensagem_sefaz=err_http or "Sem resposta SEFAZ.",
            tp_amb=int(cfg["tp_amb"]),
            dest_cpf=cpf,
            consumidor_sem_identificacao=sem_identificacao,
        )
        return {"ok": False, "erro": err_http or "Sem resposta SEFAZ.", "documento_id": doc.pk}

    st = NfceDocumentoAgro.Status.AUTORIZADA if ret.get("autorizada") else NfceDocumentoAgro.Status.REJEITADA
    xml_save = ret.get("xml_nfeproc") or signed
    mensagem_sefaz = f"{ret.get('c_stat', '')} — {ret.get('x_motivo', '')}".strip(" —")
    if ret.get("c_stat") == "270":
        mensagem_sefaz += f" (NFC_E_CMUN={cfg['cmun']}; Jacupiranga/SP=3524600)"
    doc = NfceDocumentoAgro.objects.create(
        venda=venda,
        status=st,
        chave=ret.get("chave") or chave,
        numero=numero,
        serie=serie,
        emitente_cnpj=cfg["cnpj"],
        protocolo=ret.get("protocolo") or "",
        xml_autorizado=xml_save,
        qr_code_url=qr_url,
        mensagem_sefaz=mensagem_sefaz,
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


def _assinar_evento_xml(
    xml_evento: str | Any,
    cert_path: str,
    cert_password: str,
    id_evento: str,
) -> tuple[str | None, str | None]:
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
        if isinstance(xml_evento, str):
            root = etree.fromstring(xml_evento.encode("utf-8"), parser)
        else:
            root = xml_evento
        inf = root.find(f".//{{{NS}}}infEvento")
        if inf is None:
            for el in root.iter():
                tag = el.tag.split("}")[-1] if "}" in str(el.tag) else str(el.tag)
                if tag == "infEvento":
                    inf = el
                    break
        if inf is None:
            return None, "infEvento não encontrado."
        inf_id = inf.get("Id") or id_evento
        inf.set("Id", inf_id)
        if inf.get("versao"):
            del inf.attrib["versao"]
        signer = criar_sefaz_xml_signer()
        signed = signer.sign(root, key=key_pem, cert=cert_pem, reference_uri="#" + inf_id)
        return tostring_sem_prefixos(signed), None
    except Exception as exc:
        logger.exception("assinar_evento_nfce")
        return None, str(exc)[:400]


def _montar_evento_cancelamento(
    cfg: dict[str, Any],
    *,
    chave: str,
    protocolo: str,
    x_just: str,
    n_seq: int = 1,
) -> tuple[str, Any]:
    """Monta `<evento>` de cancelamento (110111) com lxml + namespace padrão SEFAZ."""
    from lxml import etree

    tp_amb = int(cfg["tp_amb"])
    dh_txt = timezone.localtime(timezone.now()).strftime("%Y-%m-%dT%H:%M:%S-03:00")
    chave = re.sub(r"\D", "", chave)[:44]
    protocolo = re.sub(r"\D", "", protocolo)[:15]
    x_just = _sanitizar_texto_xml((x_just or NFCE_MOTIVO_CANCELAMENTO_PADRAO).strip())[:255]
    if len(x_just) < 15:
        x_just = NFCE_MOTIVO_CANCELAMENTO_PADRAO
    id_evento = f"ID110111{chave}{int(n_seq):02d}"

    def _el(parent, tag: str, text: str | None = None, **attrs):
        el = etree.SubElement(parent, etree.QName(NS, tag), **attrs)
        if text is not None:
            el.text = str(text)
        return el

    evento = etree.Element(etree.QName(NS, "evento"), nsmap={None: NS}, versao="1.00")
    inf = etree.SubElement(evento, etree.QName(NS, "infEvento"), Id=id_evento)
    _el(inf, "cOrgao", CUF_SP)
    _el(inf, "tpAmb", str(tp_amb))
    _el(inf, "CNPJ", cfg["cnpj"])
    _el(inf, "chNFe", chave)
    _el(inf, "dhEvento", dh_txt)
    _el(inf, "tpEvento", "110111")
    _el(inf, "nSeqEvento", str(n_seq))
    _el(inf, "verEvento", "1.00")
    det = etree.SubElement(inf, etree.QName(NS, "detEvento"), versao="1.00")
    _el(det, "descEvento", "Cancelamento")
    _el(det, "nProt", protocolo)
    _el(det, "xJust", x_just)
    return id_evento, evento


def _parse_retorno_evento(soap_text: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "c_stat": "",
        "x_motivo": "",
        "protocolo": "",
        "registrado": False,
    }
    try:
        root = ET.fromstring(soap_text)
    except ET.ParseError:
        snippet = re.sub(r"\s+", " ", (soap_text or ""))[:350]
        out["x_motivo"] = f"Resposta SOAP inválida. Trecho: {snippet}"
        return out

    for el in root.iter():
        if _local(el.tag) != "Fault":
            continue
        fault_txt = ""
        for ch in el.iter():
            if _local(ch.tag) in ("faultstring", "Text") and ch.text:
                fault_txt = ch.text.strip()
                break
        out["x_motivo"] = fault_txt or "Erro SOAP Fault na SEFAZ."
        return out

    payload = ""
    for el in root.iter():
        ln = _local(el.tag)
        if ln in ("nfeResultMsg", "nfeRecepcaoEventoResult") and not payload:
            payload = _elemento_para_xml_str(el)
    if not payload:
        for el in root.iter():
            if _local(el.tag) == "retEnvEvento":
                payload = ET.tostring(el, encoding="unicode")
                break
    if not payload:
        out["x_motivo"] = "Resposta SOAP sem retEnvEvento reconhecido."
        return out

    try:
        ret_root = _parse_xml_fiscal(payload)
    except ET.ParseError:
        out["x_motivo"] = "XML de retorno de evento inválido."
        return out

    ev_cstat = ""
    ev_motivo = ""
    for el in ret_root.iter():
        ln = _local(el.tag)
        if ln == "cStat" and el.text and not ev_cstat:
            ev_cstat = el.text.strip()
        elif ln == "xMotivo" and el.text and not ev_motivo:
            ev_motivo = el.text.strip()
        elif ln == "nProt" and el.text and not out["protocolo"]:
            out["protocolo"] = el.text.strip()

    for el in ret_root.iter():
        if _local(el.tag) != "infEvento":
            continue
        for ch in el:
            cl = _local(ch.tag)
            if cl == "cStat" and ch.text:
                ev_cstat = ch.text.strip()
            elif cl == "xMotivo" and ch.text:
                ev_motivo = ch.text.strip()

    out["c_stat"] = ev_cstat
    out["x_motivo"] = ev_motivo
    out["registrado"] = ev_cstat in _NFCE_CANCEL_CSTAT_OK
    return out


def _enviar_recepcao_evento(
    xml_evento_assinado: str,
    cfg: dict[str, Any],
    *,
    retry_delays: tuple[float, ...] | None = None,
    http_timeout: tuple[int, int] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    tp_amb = int(cfg["tp_amb"])
    url = URL_RECEPCAO_EVENTO.get(tp_amb, URL_RECEPCAO_EVENTO[2])
    id_lote = str(int(datetime.now().timestamp()))[-15:]
    inner = normalizar_xml_envio(xml_evento_assinado)
    env_evento = (
        f'<envEvento xmlns="{NS}" versao="1.00">'
        f"<idLote>{id_lote}</idLote>{inner}</envEvento>"
    )
    soap, headers = montar_envelope_nfe_dados_msg(NS_WSDL_EVENTO, env_evento, "nfeRecepcaoEvento")
    cert_file, key_file, cleanup = _cert_pem_temporario(cfg["cert_path"], cfg["cert_password"])
    delays = retry_delays if retry_delays is not None else SEFAZ_HTTP_RETRY_DELAYS_SYNC
    timeout = http_timeout if http_timeout is not None else SEFAZ_HTTP_TIMEOUT_SYNC
    last_err = ""
    try:
        for attempt, delay_s in enumerate(delays):
            if attempt > 0:
                time.sleep(delay_s)
            try:
                r = requests.post(
                    url,
                    data=soap.encode("utf-8"),
                    headers=headers,
                    cert=(cert_file, key_file),
                    verify=sefaz_requests_verify(),
                    timeout=timeout,
                )
                text = r.text or ""
                if r.status_code >= 400:
                    last_err = sanitizar_erro_http_sefaz(r.status_code, text)
                    if sefaz_http_status_retry(r.status_code) and attempt + 1 < len(delays):
                        continue
                    return None, last_err
                return _parse_retorno_evento(text), None
            except requests.RequestException as exc:
                last_err = sanitizar_erro_sefaz_exibicao(str(exc))
                if sefaz_erro_transiente(str(exc)) and attempt + 1 < len(delays):
                    continue
                return None, last_err
        return None, last_err or "Sem resposta SEFAZ."
    finally:
        import os

        for p in cleanup:
            try:
                os.unlink(p)
            except OSError:
                pass


def _minutos_desde_emissao_nfce(doc: NfceDocumentoAgro) -> int | None:
    if not doc.criado_em:
        return None
    return max(0, int((timezone.now() - doc.criado_em).total_seconds() // 60))


def _extrair_nprot_xml_autorizado(doc: NfceDocumentoAgro) -> str:
    """Protocolo de autorização (nProt) a partir do XML gravado, se existir."""
    xml = (doc.xml_autorizado or "").strip()
    if not xml:
        return (doc.protocolo or "").strip()
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return (doc.protocolo or "").strip()
    for el in root.iter():
        if _local(el.tag) == "nProt" and el.text:
            return re.sub(r"\D", "", el.text.strip())[:15]
    return re.sub(r"\D", "", doc.protocolo or "")[:15]


def cancelar_nfce_autorizada(
    doc: NfceDocumentoAgro,
    *,
    x_just: str = "",
) -> dict[str, Any]:
    """Cancela NFC-e autorizada na SEFAZ SP (evento 110111)."""
    if doc.status == NfceDocumentoAgro.Status.CANCELADA:
        return {"ok": True, "reutilizada": True, "documento_id": doc.pk}
    if doc.status != NfceDocumentoAgro.Status.AUTORIZADA:
        return {"ok": False, "erro": "Só é possível cancelar NFC-e autorizada.", "documento_id": doc.pk}
    if not doc.chave or not doc.protocolo:
        return {
            "ok": False,
            "erro": "NFC-e sem chave ou protocolo de autorização — cancelamento manual na SEFAZ.",
            "documento_id": doc.pk,
        }
    if not nfce_configurada():
        return {"ok": False, "erro": "NFC-e não configurada no servidor.", "documento_id": doc.pk}

    cnpj_doc = (doc.emitente_cnpj or "").strip() or nfce_cnpj_da_chave(doc.chave)
    loja = nfce_loja_de_cnpj(cnpj_doc)
    if getattr(doc, "venda_id", None):
        try:
            loja = nfce_loja_de_venda(doc.venda)
        except Exception:
            pass
    if not nfce_configurada(loja=loja):
        rotulo = "Vila Elias" if loja == "vila" else "Centro"
        return {
            "ok": False,
            "erro": f"NFC-e {rotulo} não configurada no servidor.",
            "documento_id": doc.pk,
        }

    cfg = nfce_cfg(loja)
    cfg_evt = dict(cfg)
    cfg_evt["tp_amb"] = int(doc.tp_amb or cfg["tp_amb"])
    protocolo = _extrair_nprot_xml_autorizado(doc)
    if not protocolo:
        return {
            "ok": False,
            "erro": "NFC-e sem protocolo de autorização (nProt) — cancelamento manual na SEFAZ.",
            "documento_id": doc.pk,
        }
    id_evento, xml_evento = _montar_evento_cancelamento(
        cfg_evt,
        chave=doc.chave,
        protocolo=protocolo,
        x_just=x_just or NFCE_MOTIVO_CANCELAMENTO_PADRAO,
    )
    signed, err_sign = _assinar_evento_xml(xml_evento, cfg["cert_path"], cfg["cert_password"], id_evento)
    if err_sign or not signed:
        return {"ok": False, "erro": err_sign or "Falha ao assinar evento de cancelamento.", "documento_id": doc.pk}

    ret, err_http = _enviar_recepcao_evento(signed, cfg)
    if err_http or not ret:
        return {"ok": False, "erro": err_http or "Sem resposta SEFAZ no cancelamento.", "documento_id": doc.pk}

    if ret.get("registrado"):
        msg = f"Cancelamento {ret.get('c_stat', '')} — {ret.get('x_motivo', '')}".strip(" —")
        doc.status = NfceDocumentoAgro.Status.CANCELADA
        if ret.get("protocolo"):
            doc.protocolo = ret["protocolo"]
        doc.mensagem_sefaz = (msg or "NFC-e cancelada na SEFAZ.")[:2000]
        doc.save(update_fields=["status", "protocolo", "mensagem_sefaz"])
        return {
            "ok": True,
            "documento_id": doc.pk,
            "protocolo_cancelamento": ret.get("protocolo") or "",
            "c_stat": ret.get("c_stat"),
        }

    cstat = str(ret.get("c_stat") or "")
    motivo = (ret.get("x_motivo") or "Cancelamento rejeitado pela SEFAZ.").strip()
    if cstat == "501":
        mins = _minutos_desde_emissao_nfce(doc)
        tempo = f" Cupom autorizado há ~{mins} min." if mins is not None else ""
        motivo = (
            f"{motivo}{tempo} "
            "Para NFC-e, a SEFAZ aceita cancelamento por evento só até ~30 minutos "
            "depois da autorização do cupom (não da devolução). "
            "Fora disso: NF-e de devolução (mod. 55) ou contador."
        )
    elif cstat == "225":
        motivo = (
            f"{motivo} Se persistir após atualização do sistema, avise o suporte "
            "(schema XML do evento de cancelamento)."
        )
    return {"ok": False, "erro": f"{cstat} — {motivo}".strip(" —"), "documento_id": doc.pk, "c_stat": cstat}
