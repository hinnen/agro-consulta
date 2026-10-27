"""Eventos NF-e 55 (cancelamento 110111 e CC-e 110110) — SEFAZ-SP. Não usa host NFC-e."""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone as dt_timezone
from typing import Any

from django.utils import timezone
from django.utils.html import escape

from produtos.nfce_sp_emissao_util import (
    CUF_SP,
    NS,
    NS_WSDL_EVENTO,
    _assinar_evento_xml,
    _parse_retorno_evento,
    _sanitizar_texto_xml,
)
from produtos.nfe_saida_util import NFE_SAIDA_STATUS_AUTORIZADA, NFE_SAIDA_STATUS_CANCELADA
from produtos.sefaz_soap_util import (
    SEFAZ_HTTP_RETRY_DELAYS_SYNC,
    SEFAZ_HTTP_TIMEOUT_SYNC,
    montar_envelope_nfe_dados_msg,
    normalizar_xml_envio,
    sanitizar_erro_http_sefaz,
    sanitizar_erro_sefaz_exibicao,
    sefaz_erro_transiente,
    sefaz_http_status_retry,
)
from produtos.sefaz_ssl_util import sefaz_requests_verify

logger = logging.getLogger(__name__)

URL_RECEPCAO_EVENTO_NFE_SP = {
    1: "https://nfe.fazenda.sp.gov.br/ws/NFeRecepcaoEvento4.asmx",
    2: "https://homologacao.nfe.fazenda.sp.gov.br/ws/NFeRecepcaoEvento4.asmx",
}

TP_CANCEL = "110111"
TP_CCE = "110110"
XSD_CANCEL = "eventoCancNFe_v1.00.xsd"
XSD_CCE = "CCe_v1.00.xsd"
CSTAT_EVENTO_OK = frozenset({"135", "136", "155"})
CSTAT_JA_CANCELADA = frozenset({"573", "218", "220"})

FAIXA_24H = "ate_24h"
FAIXA_24_480 = "24_a_480h"
FAIXA_480 = "apos_480h"

X_COND_USO_CCE = (
    "A Carta de Correcao e disciplinada pelo paragrafo 1o-A do art. 7o do Convenio S/N, "
    "de 15 de dezembro de 1970 e pode ser utilizada para regularizacao de erro ocorrido na "
    "emissao de documento fiscal, desde que o erro nao esteja relacionado com: I - as variaveis "
    "que determinam o valor do imposto tais como: base de calculo, aliquota, diferenca de preco, "
    "quantidade, valor da operacao ou da prestacao; II - a correcao de dados cadastrais que "
    "implique mudanca do remetente ou do destinatario; III - a data de emissao ou de saida."
)

AVISO_CCE = (
    "A CC-e não cancela a nota, não altera valor, quantidade, preço nem imposto, "
    "não troca emitente/destinatário e não muda data de emissão ou saída. "
    "O XML autorizado original não é editado."
)

ORIENTACAO_SEM_CANCEL = (
    "Cancelamento comum não cabe neste caso. Em São Paulo, após 480 horas da autorização "
    "o caminho é autorização fiscal extemporânea no SIPET/Posto Fiscal — não transmitimos "
    "cancelamento comum. Se a operação já ocorreu, use NF-e complementar, devolução ou entrada. "
    "O sistema não gera nota inversa automática e não desfaz estoque, venda ou financeiro."
)

AVISO_FORA_PRAZO = (
    "Cancelamento entre 24 e 480 horas da autorização: fora do prazo ordinário de SP. "
    "Pode haver penalidade. Só transmita se a operação não ocorreu e as condições legais "
    "estiverem atendidas (confirmação específica obrigatória)."
)


def _digits(raw: Any, n: int) -> str:
    return re.sub(r"\D", "", str(raw or ""))[:n]


def _local(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in str(tag) else str(tag)


def eventos_doc(doc: dict | None) -> list[dict]:
    d = doc if isinstance(doc, dict) else {}
    ev = d.get("eventos")
    return [e for e in ev if isinstance(e, dict)] if isinstance(ev, list) else []


def proximo_nseq(doc: dict, tp_evento: str) -> int:
    n = 0
    for e in eventos_doc(doc):
        if str(e.get("tp_evento") or "") != tp_evento:
            continue
        if not e.get("ok"):
            continue
        try:
            n = max(n, int(e.get("n_seq") or 0))
        except (TypeError, ValueError):
            continue
    return n + 1


def nota_ja_cancelada(doc: dict) -> bool:
    if str(doc.get("status") or "") == NFE_SAIDA_STATUS_CANCELADA:
        return True
    return any(
        e.get("ok") and str(e.get("tp_evento") or "") == TP_CANCEL for e in eventos_doc(doc)
    )


def dh_autorizacao(doc: dict) -> datetime | None:
    xml = str(doc.get("xml_autorizado") or "")
    if xml:
        try:
            root = ET.fromstring(xml)
        except ET.ParseError:
            root = None
        if root is not None:
            for el in root.iter():
                if _local(el.tag) == "dhRecbto" and el.text:
                    raw = el.text.strip()
                    try:
                        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
                    except ValueError:
                        pass
                if _local(el.tag) == "dhEmi" and el.text:
                    raw = el.text.strip()
                    try:
                        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
                    except ValueError:
                        pass
    for key in ("autorizada_em", "updated_at", "created_at"):
        v = doc.get(key)
        if isinstance(v, datetime):
            return v
        if isinstance(v, str) and len(v) >= 10:
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                continue
    return None


def faixa_cancelamento_sp(doc: dict, *, agora: datetime | None = None) -> str:
    dh = dh_autorizacao(doc)
    now = agora or timezone.now()
    if dh is None:
        return FAIXA_24H
    if dh.tzinfo is None:
        dh = dh.replace(tzinfo=dt_timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt_timezone.utc)
    horas = (now - dh).total_seconds() / 3600.0
    if horas <= 24:
        return FAIXA_24H
    if horas <= 480:
        return FAIXA_24_480
    return FAIXA_480


def nprot_autorizacao(doc: dict) -> str:
    p = _digits(doc.get("protocolo"), 15)
    xml = str(doc.get("xml_autorizado") or "")
    if xml:
        try:
            root = ET.fromstring(xml)
        except ET.ParseError:
            root = None
        if root is not None:
            for el in root.iter():
                if _local(el.tag) == "nProt" and el.text:
                    dig = _digits(el.text, 15)
                    if len(dig) >= 15:
                        return dig
                    if dig:
                        p = dig
    return p


def cnpj_da_chave(chave: str) -> str:
    ch = _digits(chave, 44)
    return ch[6:20] if len(ch) >= 20 else ""


def _montar_evento(
    *,
    tp_evento: str,
    cfg: dict[str, Any],
    chave: str,
    n_seq: int,
    nprot: str = "",
    x_just: str = "",
    x_correcao: str = "",
) -> tuple[str, Any]:
    from lxml import etree

    tp_amb = int(cfg["tp_amb"])
    dh_txt = timezone.localtime(timezone.now()).strftime("%Y-%m-%dT%H:%M:%S-03:00")
    chave = _digits(chave, 44)
    n_seq = max(1, min(20, int(n_seq)))
    id_evento = f"ID{tp_evento}{chave}{n_seq:02d}"

    def _el(parent, tag: str, text: str | None = None):
        el = etree.SubElement(parent, etree.QName(NS, tag))
        if text is not None:
            el.text = str(text)
        return el

    evento = etree.Element(etree.QName(NS, "evento"), nsmap={None: NS}, versao="1.00")
    inf = etree.SubElement(evento, etree.QName(NS, "infEvento"), Id=id_evento)
    _el(inf, "cOrgao", CUF_SP)
    _el(inf, "tpAmb", str(tp_amb))
    _el(inf, "CNPJ", _digits(cfg.get("cnpj"), 14))
    _el(inf, "chNFe", chave)
    _el(inf, "dhEvento", dh_txt)
    _el(inf, "tpEvento", tp_evento)
    _el(inf, "nSeqEvento", str(n_seq))
    _el(inf, "verEvento", "1.00")
    det = etree.SubElement(inf, etree.QName(NS, "detEvento"), versao="1.00")
    if tp_evento == TP_CANCEL:
        _el(det, "descEvento", "Cancelamento")
        _el(det, "nProt", _digits(nprot, 15))
        _el(det, "xJust", _sanitizar_texto_xml(x_just)[:255])
    else:
        _el(det, "descEvento", "Carta de Correcao")
        _el(det, "xCorrecao", _sanitizar_texto_xml(x_correcao)[:1000])
        _el(det, "xCondUso", X_COND_USO_CCE)
    return id_evento, evento


def validar_evento_xsd(xml_evento: str, tp_evento: str) -> tuple[bool, str]:
    from produtos.nfe_saida_xml_validate_util import validar_xml_evento_xsd

    schema = XSD_CANCEL if tp_evento == TP_CANCEL else XSD_CCE
    return validar_xml_evento_xsd(xml_evento, schema_file=schema)


def enviar_recepcao_evento_nfe(
    xml_evento_assinado: str,
    cfg: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    """POST NFeRecepcaoEvento4 no host NF-e SP (não nfce.*)."""
    import time

    import requests

    from produtos.nfce_sp_emissao_util import _cert_pem_temporario

    tp_amb = int(cfg["tp_amb"])
    url = URL_RECEPCAO_EVENTO_NFE_SP.get(tp_amb, URL_RECEPCAO_EVENTO_NFE_SP[2])
    id_lote = str(int(datetime.now().timestamp()))[-15:]
    inner = normalizar_xml_envio(xml_evento_assinado)
    env_evento = (
        f'<envEvento xmlns="{NS}" versao="1.00">'
        f"<idLote>{id_lote}</idLote>{inner}</envEvento>"
    )
    soap, headers = montar_envelope_nfe_dados_msg(NS_WSDL_EVENTO, env_evento, "nfeRecepcaoEvento")
    cert_file, key_file, cleanup = _cert_pem_temporario(cfg["cert_path"], cfg["cert_password"])
    delays = SEFAZ_HTTP_RETRY_DELAYS_SYNC
    timeout = SEFAZ_HTTP_TIMEOUT_SYNC
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


def _cfg_evento(doc: dict, cnpj_emit: str) -> tuple[dict[str, Any] | None, str | None]:
    from produtos.nfe_saida_regras_validas_util import estabelecimento_do_doc, resolver_cnpj_serie_emitente
    from produtos.nfe_saida_sp_emissao_util import _cfg_emissao_nfe

    chave = _digits(doc.get("chave"), 44)
    cnpj_ch = cnpj_da_chave(chave)
    if cnpj_ch and cnpj_emit and cnpj_ch != _digits(cnpj_emit, 14):
        return None, "CNPJ do certificado/pedido não é o da chave da NF-e (Centro e Vila não se misturam)."
    cnpj_ok, _serie, err = resolver_cnpj_serie_emitente(doc, cnpj_pedido=cnpj_ch or cnpj_emit)
    if err:
        return None, err
    if cnpj_ch and cnpj_ok != cnpj_ch:
        return None, "Estabelecimento da nota não bate com o CNPJ da chave."
    loja = estabelecimento_do_doc(doc, cnpj_emit=cnpj_ok)
    tp_amb = int(doc.get("tp_amb") or 2)
    cfg = _cfg_emissao_nfe(cnpj_ok, tp_amb, loja=loja)
    if not cfg.get("cert_path"):
        return None, "Certificado A1 do emitente não configurado."
    return cfg, None


def _xml_evento_str(evento) -> str:
    from lxml import etree

    from produtos.sefaz_xml_fiscal_util import tostring_sem_prefixos

    if isinstance(evento, str):
        return evento
    return tostring_sem_prefixos(etree.tostring(evento, encoding="unicode"))


def preparar_cancelamento(
    doc: dict,
    *,
    motivo: str,
    confirmar_fora_prazo: bool = False,
    agora: datetime | None = None,
) -> dict[str, Any]:
    if nota_ja_cancelada(doc):
        return {"ok": False, "erro": "Esta NF-e já está cancelada."}
    if str(doc.get("status") or "") != NFE_SAIDA_STATUS_AUTORIZADA:
        return {"ok": False, "erro": "Só é possível cancelar NF-e autorizada."}
    if doc.get("evento_em_curso"):
        return {"ok": False, "erro": "Já há um evento em transmissão. Aguarde."}
    x_just = _sanitizar_texto_xml((motivo or "").strip())[:255]
    if len(x_just) < 15:
        return {"ok": False, "erro": "Informe o motivo do cancelamento (mínimo 15 caracteres)."}
    chave = _digits(doc.get("chave"), 44)
    if len(chave) != 44:
        return {"ok": False, "erro": "NF-e sem chave de acesso."}
    nprot = nprot_autorizacao(doc)
    if len(nprot) < 15:
        return {"ok": False, "erro": "NF-e sem protocolo de autorização (nProt)."}
    faixa = faixa_cancelamento_sp(doc, agora=agora)
    if faixa == FAIXA_480:
        return {
            "ok": False,
            "erro": ORIENTACAO_SEM_CANCEL,
            "faixa": faixa,
            "transmitir": False,
        }
    if faixa == FAIXA_24_480 and not confirmar_fora_prazo:
        return {
            "ok": False,
            "erro": AVISO_FORA_PRAZO,
            "faixa": faixa,
            "exige_confirmacao_fora_prazo": True,
            "transmitir": False,
        }
    return {
        "ok": True,
        "faixa": faixa,
        "x_just": x_just,
        "chave": chave,
        "nprot": nprot,
        "aviso": AVISO_FORA_PRAZO if faixa == FAIXA_24_480 else "",
    }


def preparar_cce(doc: dict, *, texto: str) -> dict[str, Any]:
    if nota_ja_cancelada(doc):
        return {"ok": False, "erro": "Não é possível emitir CC-e de NF-e cancelada."}
    if str(doc.get("status") or "") != NFE_SAIDA_STATUS_AUTORIZADA:
        return {"ok": False, "erro": "CC-e só em NF-e autorizada."}
    if doc.get("evento_em_curso"):
        return {"ok": False, "erro": "Já há um evento em transmissão. Aguarde."}
    x_correcao = _sanitizar_texto_xml((texto or "").strip())[:1000]
    if len(x_correcao) < 15:
        return {"ok": False, "erro": "Descreva a correção (mínimo 15 caracteres). Texto consolidado se já houver CC-e."}
    n_seq = proximo_nseq(doc, TP_CCE)
    if n_seq > 20:
        return {"ok": False, "erro": "Limite de 20 cartas de correção atingido."}
    chave = _digits(doc.get("chave"), 44)
    if len(chave) != 44:
        return {"ok": False, "erro": "NF-e sem chave de acesso."}
    hist = [e for e in eventos_doc(doc) if e.get("ok") and str(e.get("tp_evento")) == TP_CCE]
    return {
        "ok": True,
        "x_correcao": x_correcao,
        "n_seq": n_seq,
        "chave": chave,
        "aviso": AVISO_CCE,
        "historico_cce": len(hist),
        "exige_texto_consolidado": len(hist) > 0,
    }


def transmitir_evento(
    doc: dict,
    *,
    cnpj_emit: str,
    tp_evento: str,
    n_seq: int,
    x_just: str = "",
    x_correcao: str = "",
    usuario: str = "",
    enviar=None,
) -> dict[str, Any]:
    cfg, err_cfg = _cfg_evento(doc, cnpj_emit)
    if err_cfg or not cfg:
        return {"ok": False, "erro": err_cfg or "Configuração do emitente indisponível."}
    nprot = nprot_autorizacao(doc)
    id_evento, xml_ev = _montar_evento(
        tp_evento=tp_evento,
        cfg=cfg,
        chave=_digits(doc.get("chave"), 44),
        n_seq=n_seq,
        nprot=nprot,
        x_just=x_just,
        x_correcao=x_correcao,
    )
    xml_str = _xml_evento_str(xml_ev)
    ok_xsd, det_xsd = validar_evento_xsd(xml_str, tp_evento)
    if not ok_xsd:
        return {"ok": False, "erro": f"XML do evento rejeitado no XSD ({det_xsd})."}
    signed, err_sign = _assinar_evento_xml(xml_ev, cfg["cert_path"], cfg["cert_password"], id_evento)
    if err_sign or not signed:
        return {"ok": False, "erro": err_sign or "Falha ao assinar o evento."}
    ok_xsd2, det2 = validar_evento_xsd(signed, tp_evento)
    if not ok_xsd2:
        return {"ok": False, "erro": f"XML assinado rejeitado no XSD ({det2})."}
    fn = enviar if enviar is not None else enviar_recepcao_evento_nfe
    ret, err_http = fn(signed, cfg)
    if err_http or not ret:
        return {"ok": False, "erro": err_http or "Sem resposta SEFAZ no evento."}
    cstat = str(ret.get("c_stat") or "")
    registrado = bool(ret.get("registrado")) or cstat in CSTAT_EVENTO_OK
    ja_ok = cstat in CSTAT_JA_CANCELADA and tp_evento == TP_CANCEL
    ok = registrado or ja_ok
    rec = {
        "tipo": "cancelamento" if tp_evento == TP_CANCEL else "cce",
        "tp_evento": tp_evento,
        "n_seq": n_seq,
        "xml": signed,
        "protocolo": ret.get("protocolo") or "",
        "c_stat": cstat,
        "x_motivo": ret.get("x_motivo") or "",
        "justificativa": x_just,
        "x_correcao": x_correcao,
        "usuario": (usuario or "")[:200],
        "dh_evento": timezone.now().isoformat(),
        "ok": ok,
    }
    if not ok:
        return {
            "ok": False,
            "erro": (ret.get("x_motivo") or "Evento rejeitado pela SEFAZ.").strip(),
            "c_stat": cstat,
            "evento": rec,
        }
    return {"ok": True, "evento": rec, "c_stat": cstat, "registrado": registrado}


def aplicar_evento_no_doc(doc: dict, rec: dict) -> dict:
    out = dict(doc)
    evs = list(eventos_doc(out))
    evs.append(rec)
    out["eventos"] = evs
    out["evento_em_curso"] = False
    if rec.get("ok") and rec.get("tp_evento") == TP_CANCEL:
        out["status"] = NFE_SAIDA_STATUS_CANCELADA
        if rec.get("protocolo"):
            out["protocolo_cancelamento"] = rec["protocolo"]
        out["cancelada_em"] = rec.get("dh_evento")
        out["cancelada_por"] = rec.get("usuario") or ""
        out["cancelada_motivo"] = rec.get("justificativa") or ""
    return out


def montar_html_comprovante_evento(doc: dict, rec: dict) -> str:
    tipo = "Cancelamento" if rec.get("tp_evento") == TP_CANCEL else "Carta de Correção"
    chave = escape(_digits(doc.get("chave"), 44))
    corpo = escape(rec.get("justificativa") or rec.get("x_correcao") or "")
    return f"""<!DOCTYPE html><html lang="pt-br"><head><meta charset="utf-8"><title>{tipo} NF-e</title>
<style>body{{font-family:sans-serif;padding:24px;max-width:48rem}}h1{{font-size:1.2rem}}pre{{white-space:pre-wrap}}</style>
</head><body>
<h1>{escape(tipo)} — NF-e 55</h1>
<p>Chave: {chave}</p>
<p>Protocolo: {escape(str(rec.get("protocolo") or "—"))} · cStat {escape(str(rec.get("c_stat") or ""))}</p>
<p>Data: {escape(str(rec.get("dh_evento") or ""))} · Usuário: {escape(str(rec.get("usuario") or ""))}</p>
<p>nSeq: {escape(str(rec.get("n_seq") or ""))}</p>
<pre>{corpo}</pre>
<p style="font-size:.85rem;color:#444">{escape(AVISO_CCE if rec.get("tp_evento")==TP_CCE else "Cancelamento somente após retorno válido da SEFAZ. Estoque/venda/financeiro não são desfeitos aqui.")}</p>
</body></html>"""


def resumo_eventos(doc: dict) -> list[dict[str, Any]]:
    out = []
    for e in eventos_doc(doc):
        out.append(
            {
                "tipo": e.get("tipo"),
                "tp_evento": e.get("tp_evento"),
                "n_seq": e.get("n_seq"),
                "ok": bool(e.get("ok")),
                "protocolo": e.get("protocolo") or "",
                "c_stat": e.get("c_stat") or "",
                "dh_evento": e.get("dh_evento") or "",
                "usuario": e.get("usuario") or "",
            }
        )
    return out
