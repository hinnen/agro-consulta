"""
Emissão NF-e modelo 55 — SEFAZ São Paulo (Simples Nacional).

Reusa assinatura e parse SOAP da NFC-e; endpoint em ``URL_AUTORIZACAO_NFE_SP``.
"""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from decouple import config
from django.db import transaction
from django.utils import timezone

from produtos.nfce_sp_emissao_util import (
    CUF_SP,
    NS,
    URL_AUTORIZACAO_NFE_SP,
    _assinar_nfe_xml,
    _enviar_autorizacao,
    _gerar_c_nf,
    _montar_det_pag,
    _q2,
    _q4,
    _sanitizar_texto_xml,
    _sub,
)
from produtos.nfe_saida_util import nfe_saida_serie_default
from produtos.sefaz_xml_fiscal_util import tostring_sem_prefixos

logger = logging.getLogger(__name__)

_NFE_RETRY_CSTAT_DUPLICIDADE = frozenset({"539", "204"})
_HOMOLOG_NOME_DEST = "NF-E EMITIDA EM AMBIENTE DE HOMOLOGACAO - SEM VALOR FISCAL"


def _texto_endereco(val: Any, *, fallback: str = "NAO INFORMADO", max_len: int = 60) -> str:
    t = _sanitizar_texto_xml(str(val or "").strip())
    if not t:
        t = fallback
    return t[:max_len]


def _dv_chave(chave43: str) -> str:
    pesos = [2, 3, 4, 5, 6, 7, 8, 9]
    soma = sum(int(d) * pesos[i % len(pesos)] for i, d in enumerate(reversed(chave43)))
    resto = soma % 11
    dv = 0 if resto in (0, 1) else 11 - resto
    return str(dv)


def _montar_chave_nfe(
    *,
    cnpj: str,
    serie: int,
    numero: int,
    tp_emis: str,
    c_nf: str,
    dh_emi: datetime,
) -> str:
    aamm = dh_emi.strftime("%y%m")
    ch43 = f"{CUF_SP}{aamm}{cnpj}55{serie:03d}{numero:09d}{tp_emis}{c_nf:0>8}"[:43]
    return ch43 + _dv_chave(ch43)


def _cfg_emissao_nfe(
    cnpj_emit: str,
    tp_amb: int,
    *,
    loja: str | None = None,
    serie_nfe: int | None = None,
) -> dict[str, Any]:
    from produtos.nfce_config_util import nfce_cfg, nfce_loja_de_cnpj
    from produtos.sefaz_dfe_client import _cfg_dist_dfe

    dfe = _cfg_dist_dfe()
    cnpj = re.sub(r"\D", "", cnpj_emit or dfe.get("cnpj") or "")[:14]
    loja_n = loja or nfce_loja_de_cnpj(cnpj)
    nfc = nfce_cfg(loja_n)
    try:
        piso = int(config("NFE_EMISSAO_PROXIMO_NUMERO", default="1") or 1)
    except (TypeError, ValueError):
        piso = 1
    serie = int(serie_nfe or nfe_saida_serie_default())
    return {
        "cert_path": dfe.get("cert_path") or nfc.get("cert_path") or "",
        "cert_password": dfe.get("cert_password") or nfc.get("cert_password") or "",
        "cnpj": cnpj,
        "tp_amb": int(tp_amb),
        "serie": serie,
        "proximo_numero_inicial": max(1, piso),
        "razao_social": str(nfc.get("razao_social") or "").strip()[:150],
        "fantasia": str(nfc.get("fantasia") or nfc.get("razao_social") or "").strip()[:60],
        "logradouro": str(nfc.get("logradouro") or "").strip()[:60],
        "numero": str(nfc.get("numero") or "S/N").strip()[:20],
        "bairro": str(nfc.get("bairro") or "").strip()[:60],
        "cmun": re.sub(r"\D", "", str(nfc.get("cmun") or ""))[:7],
        "cidade": str(nfc.get("cidade") or "").strip()[:60],
        "uf": str(nfc.get("uf") or dfe.get("uf") or "SP").strip().upper()[:2],
        "cep": re.sub(r"\D", "", str(nfc.get("cep") or ""))[:8],
        "ie": re.sub(r"\D", "", str(nfc.get("ie") or ""))[:14],
        "fone": re.sub(r"\D", "", str(nfc.get("fone") or ""))[:14],
    }


def _proximo_numero_nfe(cfg: dict[str, Any]) -> tuple[int, int]:
    from produtos.models import NfeNumeracaoAgro, NfeSaidaRascunhoAgro

    serie_cfg = int(cfg["serie"])
    cnpj = re.sub(r"\D", "", str(cfg.get("cnpj") or ""))[:14]
    piso = int(cfg.get("proximo_numero_inicial") or 1)
    with transaction.atomic():
        num, _ = NfeNumeracaoAgro.objects.select_for_update().get_or_create(
            emitente_cnpj=cnpj,
            serie=serie_cfg,
            defaults={"proximo_numero": max(1, piso)},
        )
        max_pg = 0
        for row in NfeSaidaRascunhoAgro.objects.filter(status="autorizada").only("corpo"):
            corpo = row.corpo if isinstance(row.corpo, dict) else {}
            if int(corpo.get("serie") or 0) != serie_cfg:
                continue
            ch = re.sub(r"\D", "", str(corpo.get("chave") or ""))
            if ch and len(ch) >= 20 and ch[6:20] != cnpj:
                continue
            try:
                n = int(corpo.get("nNF") or 0)
            except (TypeError, ValueError):
                n = 0
            if n > max_pg:
                max_pg = n
        alvo = max(piso, int(max_pg) + 1, int(num.proximo_numero or 1))
        n = alvo
        num.proximo_numero = n + 1
        num.save(update_fields=["proximo_numero", "atualizado_em"])
        return serie_cfg, n


def _cmun_destino(dest: dict, cfg_emit: dict) -> str:
    from produtos.nfe_saida_regras_validas_util import cmun_destinatario_ou_erro

    cmun, err = cmun_destinatario_ou_erro(
        dest,
        cidade_emit=str(cfg_emit.get("cidade") or ""),
        cmun_emit=str(cfg_emit.get("cmun") or ""),
    )
    if err:
        return ""
    return cmun


def _id_dest(uf_emit: str, uf_dest: str) -> str:
    ue = (uf_emit or "SP").strip().upper()[:2]
    ud = (uf_dest or ue).strip().upper()[:2]
    return "2" if ud and ue and ud != ue else "1"


def _ind_intermed_obrigatorio(ind_pres: int) -> bool:
    """NT 2020.006 — indIntermed quando indPres ∈ {1,2,3,4,9} (saída NF normal)."""
    return ind_pres in (1, 2, 3, 4, 9)


def _preencher_dest_nfe(
    inf: ET.Element, dest: dict, cfg_emit: dict[str, Any], *, tp_amb: int
) -> None:
    doc = re.sub(r"\D", "", str(dest.get("documento") or ""))
    if len(doc) not in (11, 14):
        return
    dest_el = _sub(inf, "dest")
    if len(doc) == 14:
        _sub(dest_el, "CNPJ", doc[:14])
    else:
        _sub(dest_el, "CPF", doc[:11])
    nome = _sanitizar_texto_xml(str(dest.get("nome") or "").strip())[:60]
    if tp_amb == 2:
        nome = _HOMOLOG_NOME_DEST
    elif not nome:
        nome = "DESTINATARIO"
    _sub(dest_el, "xNome", nome)
    ender = _sub(dest_el, "enderDest")
    _sub(ender, "xLgr", _texto_endereco(dest.get("logradouro"), fallback="S/N"))
    _sub(ender, "nro", _texto_endereco(dest.get("numero"), fallback="S/N", max_len=60))
    _sub(ender, "xBairro", _texto_endereco(dest.get("bairro")))
    _sub(ender, "cMun", _cmun_destino(dest, cfg_emit))
    _sub(ender, "xMun", _texto_endereco(dest.get("cidade"), fallback="MUNICIPIO"))
    _sub(ender, "UF", str(dest.get("uf") or "SP").strip().upper()[:2])
    cep = re.sub(r"\D", "", str(dest.get("cep") or ""))[:8]
    _sub(ender, "CEP", (cep or "00000000").zfill(8))
    _sub(ender, "cPais", "1058")
    _sub(ender, "xPais", "BRASIL")
    ind_ie = str(dest.get("ind_ie_dest") or "").strip()
    ie_xml = str(dest.get("ie_xml") or "").strip()
    ie = str(dest.get("ie") or "").strip().upper()
    if ind_ie in ("1", "2", "9"):
        _sub(dest_el, "indIEDest", ind_ie)
        if ind_ie == "1" and ie_xml:
            _sub(dest_el, "IE", ie_xml[:14])
        elif ind_ie == "2" and (ie == "ISENTO" or ie_xml == "ISENTO"):
            _sub(dest_el, "IE", "ISENTO")
    elif len(doc) == 14:
        if ie in ("", "ISENTO"):
            _sub(dest_el, "indIEDest", "2")
            if ie == "ISENTO":
                _sub(dest_el, "IE", "ISENTO")
        else:
            _sub(dest_el, "indIEDest", "1")
            _sub(dest_el, "IE", re.sub(r"\D", "", ie)[:14])
    else:
        if ie and ie != "ISENTO" and re.sub(r"\D", "", ie):
            _sub(dest_el, "indIEDest", "1")
            _sub(dest_el, "IE", re.sub(r"\D", "", ie)[:14])
        else:
            _sub(dest_el, "indIEDest", "9")
    email = str(dest.get("email") or "").strip()
    if email and "@" in email:
        _sub(dest_el, "email", email[:60])


def _montar_grupo_tributo(pai: ET.Element, nome_grupo: str, campos: dict[str, str]) -> None:
    grupo = _sub(pai, nome_grupo)
    for tag, val in campos.items():
        if val is not None and str(val) != "":
            _sub(grupo, tag, str(val))


def _montar_xml_nfe_saida(
    doc: dict,
    cfg: dict[str, Any],
    *,
    serie: int,
    numero: int,
    chave: str,
    dh_emi: datetime,
) -> str:
    from produtos.nfe_saida_tributacao_util import (
        FiscalBloqueio,
        apuracao_serializada,
        apurar_nfe_saida,
    )

    tp_amb = int(cfg["tp_amb"])
    dest = doc.get("destinatario") if isinstance(doc.get("destinatario"), dict) else {}
    fiscal = doc.get("fiscal") if isinstance(doc.get("fiscal"), dict) else {}
    itens = doc.get("itens") if isinstance(doc.get("itens"), list) else []
    pagamentos = doc.get("pagamento") if isinstance(doc.get("pagamento"), list) else []

    uf_emit = str(cfg.get("uf") or "SP")[:2]
    apuracao = apurar_nfe_saida(
        doc,
        uf_emit=uf_emit,
        cnpj_emit=str(cfg.get("cnpj") or ""),
        data_emissao=dh_emi.date() if hasattr(dh_emi, "date") else None,
    )
    if apuracao.bloqueios:
        raise FiscalBloqueio(apuracao.bloqueios[0])
    doc["fiscal_apuracao"] = apuracao_serializada(apuracao)
    dest = {**dest, **apuracao.destinatario}

    uf_dest = str(dest.get("uf") or uf_emit)[:2]
    dh_txt = dh_emi.strftime("%Y-%m-%dT%H:%M:%S-03:00")
    nat_op = _sanitizar_texto_xml(str(fiscal.get("natureza") or "VENDA"))[:60]
    mod_frete = str(fiscal.get("mod_frete") or "9").strip()[:1] or "9"
    try:
        ind_final = int(dest.get("ind_final") if dest.get("ind_final") is not None else 1)
    except (TypeError, ValueError):
        ind_final = 1
    try:
        ind_pres = int(dest.get("ind_pres") if dest.get("ind_pres") is not None else 9)
    except (TypeError, ValueError):
        ind_pres = 9

    nfe = ET.Element(f"{{{NS}}}NFe")
    inf = ET.SubElement(nfe, f"{{{NS}}}infNFe", {"versao": "4.00", "Id": f"NFe{chave}"})

    ide = _sub(inf, "ide")
    _sub(ide, "cUF", CUF_SP)
    _sub(ide, "cNF", chave[35:43])
    _sub(ide, "natOp", nat_op)
    _sub(ide, "mod", "55")
    _sub(ide, "serie", str(serie))
    _sub(ide, "nNF", str(numero))
    _sub(ide, "dhEmi", dh_txt)
    _sub(ide, "tpNF", "1")
    _sub(ide, "idDest", _id_dest(uf_emit, uf_dest))
    _sub(ide, "cMunFG", cfg["cmun"] or "3524600")
    _sub(ide, "tpImp", "1")
    _sub(ide, "tpEmis", "1")
    _sub(ide, "cDV", chave[-1])
    _sub(ide, "tpAmb", str(tp_amb))
    _sub(ide, "finNFe", "1")
    _sub(ide, "indFinal", str(ind_final))
    _sub(ide, "indPres", str(ind_pres))
    if _ind_intermed_obrigatorio(ind_pres):
        _sub(ide, "indIntermed", "0")
    _sub(ide, "procEmi", "0")
    _sub(ide, "verProc", "AgroConsulta1.0")

    emit = _sub(inf, "emit")
    _sub(emit, "CNPJ", cfg["cnpj"])
    _sub(emit, "xNome", cfg["razao_social"])
    if cfg.get("fantasia"):
        _sub(emit, "xFant", cfg["fantasia"])
    ender = _sub(emit, "enderEmit")
    _sub(ender, "xLgr", cfg["logradouro"])
    _sub(ender, "nro", cfg["numero"])
    _sub(ender, "xBairro", cfg["bairro"])
    _sub(ender, "cMun", cfg["cmun"] or "3524600")
    _sub(ender, "xMun", cfg["cidade"])
    _sub(ender, "UF", cfg["uf"])
    _sub(ender, "CEP", (cfg.get("cep") or "00000000").zfill(8))
    _sub(ender, "cPais", "1058")
    _sub(ender, "xPais", "BRASIL")
    if cfg.get("fone"):
        _sub(ender, "fone", cfg["fone"])
    _sub(emit, "IE", cfg["ie"])
    _sub(emit, "CRT", "1")

    _preencher_dest_nfe(inf, dest, cfg, tp_amb=tp_amb)

    total_prod = Decimal("0")
    ap_itens = apuracao.itens
    for idx, ln in enumerate(itens, start=1):
        if not isinstance(ln, dict):
            continue
        item_fis = ap_itens[idx - 1] if idx <= len(ap_itens) else None
        qtd = Decimal(str(ln.get("qtd") or 0))
        vu = Decimal(str(ln.get("v_unit") or 0))
        vt = Decimal(str(ln.get("v_total") or 0)).quantize(Decimal("0.01"))
        if vt <= 0 and qtd > 0:
            vt = (qtd * vu).quantize(Decimal("0.01"))
        total_prod += vt
        u_com = str(ln.get("un") or "UN").strip().upper() or "UN"
        if u_com not in ("UN", "KG", "G", "L", "ML", "M", "CM", "PC", "CX", "FD", "PCT", "DZ"):
            u_com = "UN"
        det = ET.SubElement(inf, f"{{{NS}}}det")
        det.set("nItem", str(idx))
        prod = _sub(det, "prod")
        cod = str(ln.get("codigo") or ln.get("produto_id") or idx)[:60]
        _sub(prod, "cProd", cod)
        _sub(prod, "cEAN", "SEM GTIN")
        _sub(prod, "xProd", _sanitizar_texto_xml(str(ln.get("descricao") or "PRODUTO"))[:120])
        ncm = re.sub(r"\D", "", str(ln.get("ncm") or ""))[:8]
        _sub(prod, "NCM", ncm.zfill(8))
        cfop = re.sub(r"\D", "", str((item_fis.cfop if item_fis else ln.get("cfop")) or "5102"))[:4]
        _sub(prod, "CFOP", cfop.zfill(4))
        _sub(prod, "uCom", u_com[:6])
        _sub(prod, "qCom", _q4(qtd))
        _sub(prod, "vUnCom", _q4(vu))
        _sub(prod, "vProd", _q2(vt))
        _sub(prod, "cEANTrib", "SEM GTIN")
        _sub(prod, "uTrib", u_com[:6])
        _sub(prod, "qTrib", _q4(qtd))
        _sub(prod, "vUnTrib", _q4(vu))
        _sub(prod, "indTot", "1")
        imposto = _sub(det, "imposto")
        if item_fis and item_fis.v_tot_trib is not None and item_fis.v_tot_trib > 0:
            _sub(imposto, "vTotTrib", _q2(item_fis.v_tot_trib))
        icms = _sub(imposto, "ICMS")
        if item_fis:
            _montar_grupo_tributo(icms, item_fis.icms_grupo, item_fis.icms_campos)
            pis = _sub(imposto, "PIS")
            _montar_grupo_tributo(pis, item_fis.pis_grupo, item_fis.pis_campos)
            cof = _sub(imposto, "COFINS")
            _montar_grupo_tributo(cof, item_fis.cofins_grupo, item_fis.cofins_campos)
        else:
            icmssn = _sub(icms, "ICMSSN102")
            _sub(icmssn, "orig", str(ln.get("origem") or "0")[:1])
            _sub(icmssn, "CSOSN", "102")

    total_nf = total_prod.quantize(Decimal("0.01"))
    icms_tot = _sub(inf, "total")
    icms = _sub(icms_tot, "ICMSTot")
    tot_tags: list[tuple[str, str]] = [
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
    ]
    if apuracao.v_tot_trib_nf is not None and apuracao.v_tot_trib_nf > 0:
        tot_tags.append(("vTotTrib", _q2(apuracao.v_tot_trib_nf)))
    for tag, val in tot_tags:
        _sub(icms, tag, val)

    transp = _sub(inf, "transp")
    _sub(transp, "modFrete", mod_frete)

    pag = _sub(inf, "pag")
    for pg in pagamentos:
        if not isinstance(pg, dict):
            continue
        try:
            val = Decimal(str(pg.get("valor") or 0)).quantize(Decimal("0.01"))
        except Exception:
            val = Decimal("0")
        if val <= 0:
            continue
        tpag = str(pg.get("tPag") or "01").zfill(2)[:2]
        row = {"tPag": tpag, "vPag": val, "xPag": str(pg.get("forma") or "")[:60] if tpag == "99" else ""}
        _montar_det_pag(pag, row)

    if not pagamentos:
        _montar_det_pag(pag, {"tPag": "01", "vPag": total_nf, "xPag": ""})

    inf_ad = _sub(inf, "infAdic")
    obs_parts: list[str] = []
    if doc.get("venda_agro_id"):
        obs_parts.append(f"Venda Agro #{doc.get('venda_agro_id')}")
    extra = str(doc.get("inf_complementar") or fiscal.get("inf_compl") or "").strip()
    if extra:
        obs_parts.append(extra)
    if apuracao.ibpt_texto:
        obs_parts.append(apuracao.ibpt_texto)
    if tp_amb == 2:
        obs_parts.insert(0, "NF-E EMITIDA EM AMBIENTE DE HOMOLOGACAO - SEM VALOR FISCAL")
    if obs_parts:
        _sub(inf_ad, "infCpl", _sanitizar_texto_xml(" | ".join(obs_parts))[:5000])

    return tostring_sem_prefixos(ET.tostring(nfe, encoding="unicode"))


def emitir_nfe_saida_sefaz(doc: dict, *, cnpj_emit: str) -> dict[str, Any]:
    """Monta XML mod 55, assina e envia NFeAutorizacao4 (SP)."""
    from produtos.nfe_saida_util import NFE_SAIDA_STATUS_AUTORIZADA, NFE_SAIDA_STATUS_REJEITADA
    from produtos.nfe_saida_xml_validate_util import validar_xml_nfe_xsd_obrigatorio

    tp_amb = int(doc.get("tp_amb") or 2)
    from produtos.nfe_saida_regras_validas_util import estabelecimento_do_doc, resolver_cnpj_serie_emitente

    cnpj_ok, serie_ok, err_em = resolver_cnpj_serie_emitente(doc, cnpj_pedido=cnpj_emit)
    if err_em:
        return {
            "ok": False,
            "status": NFE_SAIDA_STATUS_REJEITADA,
            "erro": err_em,
            "c_stat": 999,
            "x_motivo": err_em,
        }
    loja_doc = estabelecimento_do_doc(doc, cnpj_emit=cnpj_ok)
    cfg = _cfg_emissao_nfe(cnpj_ok, tp_amb, loja=loja_doc, serie_nfe=serie_ok)
    if not cfg.get("cert_path"):
        return {
            "ok": False,
            "status": NFE_SAIDA_STATUS_REJEITADA,
            "erro": "Certificado A1 não encontrado.",
            "c_stat": 280,
            "x_motivo": "Certificado não configurado.",
        }
    if not cfg.get("cnpj") or len(cfg["cnpj"]) != 14:
        return {
            "ok": False,
            "status": NFE_SAIDA_STATUS_REJEITADA,
            "erro": "CNPJ emitente inválido (NFE_DIST_DFE_CNPJ / NFC_E_CNPJ).",
            "c_stat": 280,
        }
    if not cfg.get("cmun") or not cfg.get("ie"):
        return {
            "ok": False,
            "status": NFE_SAIDA_STATUS_REJEITADA,
            "erro": "Emitente incompleto no .env (NFC_E_CMUN, NFC_E_IE).",
            "c_stat": 280,
        }

    cfg["serie"] = int(serie_ok or cfg["serie"])

    nnf_doc = doc.get("nNF")
    max_tentativas = 4
    ultimo: dict[str, Any] = {}
    for tentativa in range(max_tentativas):
        if nnf_doc and tentativa == 0:
            try:
                numero = int(nnf_doc)
            except (TypeError, ValueError):
                numero = None
        else:
            numero = None
        if numero is None:
            _, numero = _proximo_numero_nfe(cfg)
        serie = int(cfg["serie"])
        c_nf = _gerar_c_nf()
        dh_emi = timezone.localtime(timezone.now())
        chave = _montar_chave_nfe(
            cnpj=cfg["cnpj"],
            serie=serie,
            numero=numero,
            tp_emis="1",
            c_nf=c_nf,
            dh_emi=dh_emi,
        )
        try:
            xml_body = _montar_xml_nfe_saida(doc, cfg, serie=serie, numero=numero, chave=chave, dh_emi=dh_emi)
            validar_xml_nfe_xsd_obrigatorio(xml_body)
        except Exception as exc:
            from produtos.nfe_saida_tributacao_util import FiscalBloqueio

            if isinstance(exc, FiscalBloqueio):
                return {
                    "ok": False,
                    "status": NFE_SAIDA_STATUS_REJEITADA,
                    "erro": str(exc),
                    "c_stat": 999,
                    "x_motivo": str(exc)[:500],
                }
            raise
        xml_assinado, err_sig = _assinar_nfe_xml(
            xml_body, cfg["cert_path"], cfg["cert_password"], chave
        )
        if err_sig or not xml_assinado:
            return {
                "ok": False,
                "status": NFE_SAIDA_STATUS_REJEITADA,
                "erro": err_sig or "Falha ao assinar XML.",
                "c_stat": 0,
            }
        try:
            validar_xml_nfe_xsd_obrigatorio(xml_assinado)
        except Exception as exc:
            from produtos.nfe_saida_tributacao_util import FiscalBloqueio

            if isinstance(exc, FiscalBloqueio):
                return {
                    "ok": False,
                    "status": NFE_SAIDA_STATUS_REJEITADA,
                    "erro": str(exc),
                    "c_stat": 999,
                    "x_motivo": str(exc)[:500],
                }
            raise
        url = URL_AUTORIZACAO_NFE_SP.get(tp_amb, URL_AUTORIZACAO_NFE_SP[2])
        ret, err_http = _enviar_autorizacao(
            xml_assinado, cfg, url_autorizacao=url
        )
        if err_http:
            return {
                "ok": False,
                "status": NFE_SAIDA_STATUS_REJEITADA,
                "erro": err_http,
                "c_stat": 0,
                "x_motivo": err_http[:500],
            }
        if not ret:
            return {
                "ok": False,
                "status": NFE_SAIDA_STATUS_REJEITADA,
                "erro": "Resposta vazia da SEFAZ.",
                "c_stat": 0,
            }
        c_stat = str(ret.get("c_stat") or "")
        ultimo = ret
        if ret.get("autorizada"):
            return {
                "ok": True,
                "status": NFE_SAIDA_STATUS_AUTORIZADA,
                "chave": ret.get("chave") or chave,
                "protocolo": ret.get("protocolo") or "",
                "c_stat": int(c_stat) if c_stat.isdigit() else 100,
                "x_motivo": ret.get("x_motivo") or "Autorizado o uso da NF-e",
                "nNF": numero,
                "serie": serie,
                "xml_autorizado": ret.get("xml_nfeproc") or "",
                "simulado": False,
                "autorizacao_sefaz_real": True,
            }
        if c_stat in _NFE_RETRY_CSTAT_DUPLICIDADE and tentativa + 1 < max_tentativas:
            nnf_doc = None
            logger.warning("NF-e 55 duplicidade cStat=%s — tentando próximo nNF", c_stat)
            continue
        break

    motivo = str(ultimo.get("x_motivo") or "Rejeitada pela SEFAZ.")
    return {
        "ok": False,
        "status": NFE_SAIDA_STATUS_REJEITADA,
        "erro": motivo[:400],
        "c_stat": int(str(ultimo.get("c_stat") or "0") or 0),
        "x_motivo": motivo[:500],
        "chave": ultimo.get("chave") or "",
    }
