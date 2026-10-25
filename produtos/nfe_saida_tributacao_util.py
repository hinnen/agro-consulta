"""
Motor fiscal NF-e saída 55 — apuração centralizada (Simples Nacional / CRT 1).

Separa decisão tributária da montagem XML. Não altera NCM do item quando já informado.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from decouple import config

from produtos.agro_produto_fiscal_defaults import merge_fiscal_padrao_cadastro_manual_sp_sn, normalizar_ncm_somente_digitos
from produtos.nfe_saida_util import cfop_padrao_destino, documento_dest_tipo, nfe_saida_emitente_uf_default

_MOTOR_VERSAO = "nfe-trib-2026-10-02"

# CSOSN com grupo XML distinto (demais 102/103/300/400 usam ICMSSN102).
_CSOSN_GRUPO_DIRETO = {
    "101": "ICMSSN101",
    "201": "ICMSSN201",
    "500": "ICMSSN500",
    "900": "ICMSSN900",
}

_CSOSN_EXIGE_PARAM_EXTRA = frozenset({"201", "202", "203", "500", "900"})


def _grupo_xml_icms_csosn(cs: str) -> str:
    """Mapeia CSOSN ao elemento XML (NT NF-e Simples Nacional)."""
    if cs in ("102", "103", "300", "400"):
        return "ICMSSN102"
    if cs in ("202", "203"):
        return "ICMSSN202"
    return _CSOSN_GRUPO_DIRETO.get(cs, "")


def cfop_coerente_operacao(cfop: str, *, inter: bool) -> bool:
    c = _digits(cfop, 4)
    if len(c) != 4 or c[0] not in ("5", "6", "7"):
        return False
    if inter:
        return c[0] in ("6", "7")
    return c[0] == "5"

# CST PIS/COFINS → tipo de grupo no XML
_CST_NT = frozenset({"04", "05", "06", "07", "08", "09"})
_CST_OUTR = frozenset({"49", "50", "51", "52", "53", "54", "55", "56", "60", "61", "62", "63", "64", "65", "66", "67", "70", "71", "72", "73", "74", "75", "98", "99"})


class FiscalBloqueio(Exception):
    """Impede transmissão — dados ou parâmetro fiscal insuficiente."""

    def __init__(self, mensagem: str, *, codigo: str = "fiscal_bloqueio"):
        self.codigo = codigo
        super().__init__(mensagem)


def _q2(v: Decimal) -> str:
    return str(v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _dec(v: Any) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0.00")


def _digits(s: Any, n: int = 20) -> str:
    return re.sub(r"\D", "", str(s or ""))[:n]


def _cfg_cred_sn_aliquota() -> Decimal | None:
    raw = (config("NFE_SAIDA_PCRED_SN_ALIQ", default="") or "").strip()
    if not raw:
        return None
    try:
        return Decimal(raw.replace(",", ".")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    except Exception:
        return None


def ibpt_habilitado() -> bool:
    token = (config("NFC_E_IBPT_TOKEN", default="") or "").strip()
    if token:
        return True
    return (config("NFE_SAIDA_IBPT_USE_FALLBACK", default="") or "").strip().lower() in ("1", "true", "sim", "yes")


def normalizar_destinatario_fiscal(dest: dict | None) -> dict[str, Any]:
    """Deriva indIEDest, IE e indFinal sem presumir revenda só por ter IE."""
    d = dict(dest) if isinstance(dest, dict) else {}
    doc = _digits(d.get("documento"), 14)
    tipo = documento_dest_tipo(doc)
    ie_raw = str(d.get("ie") or "").strip().upper()
    ie_digits = _digits(ie_raw, 14)

    situacao = str(d.get("situacao_icms") or "").strip().lower()
    finalidade = str(d.get("finalidade_mercadoria") or "").strip().lower()

    if finalidade in ("revenda", "revender", "comercializacao"):
        ind_final = 0
    elif finalidade in ("consumo", "uso", "uso_proprio", "ativo"):
        ind_final = 1
    else:
        try:
            ind_final = int(d.get("ind_final") if d.get("ind_final") is not None else 1)
        except (TypeError, ValueError):
            ind_final = 1
        ind_final = 1 if ind_final != 0 else 0

    if situacao == "contribuinte":
        ind_ie = "1"
    elif situacao in ("isento", "isenta"):
        ind_ie = "2"
    elif situacao in ("nao_contribuinte", "nao-contribuinte", "nao"):
        ind_ie = "9"
    elif tipo == "cpf":
        if ie_digits or ie_raw == "ISENTO":
            ind_ie = "1" if ie_digits else "2"
        else:
            ind_ie = "9"
    elif tipo == "cnpj":
        if ie_raw == "ISENTO":
            ind_ie = "2"
        elif ie_digits:
            ind_ie = "1"
        elif not ie_raw:
            ind_ie = "9"
        else:
            ind_ie = "9"
    else:
        ind_ie = "9"

    ie_xml = ""
    if ind_ie == "1" and ie_digits:
        ie_xml = ie_digits
    elif ind_ie == "2" and ie_raw == "ISENTO":
        ie_xml = "ISENTO"

    out = dict(d)
    out["tipo_doc"] = tipo
    out["documento"] = doc
    out["ind_final"] = ind_final
    out["ind_ie_dest"] = ind_ie
    out["ie_xml"] = ie_xml
    return out


def sugerir_cfop_item(
    *,
    uf_emit: str,
    uf_dest: str,
    ind_final: int,
    cfop_linha: str,
    cfop_catalogo: str,
    mercadoria_st: bool = False,
) -> str:
    """Sugere CFOP; só aceita CFOP explícito coerente com interna/interestadual."""
    ue = (uf_emit or "SP").upper()[:2]
    ud = (uf_dest or ue).upper()[:2]
    inter = bool(ud and ue and ud != ue)
    for cand in (cfop_linha, cfop_catalogo):
        c = _digits(cand, 4)
        if len(c) == 4 and cfop_coerente_operacao(c, inter=inter):
            return c
    if mercadoria_st:
        return "6403" if inter else "5405"
    base = cfop_padrao_destino(ud, ue)
    if ind_final == 0 and not inter:
        return "5102" if base == "5102" else base
    return base


def _fiscal_linha_enriquecida(ln: dict, *, db=None, col_p: str | None = None) -> dict[str, str]:
    """Mescla linha do documento com cadastro (sem sobrescrever NCM/CSOSN/CFOP já na linha)."""
    base = {
        "ncm": normalizar_ncm_somente_digitos(ln.get("ncm")),
        "cfop": _digits(ln.get("cfop"), 4),
        "csosn": _digits(ln.get("csosn"), 3),
        "origem": _digits(ln.get("origem"), 1) or "0",
        "cst_pis_cofins": _digits(ln.get("cst_pis_cofins"), 2),
        "cest": _digits(ln.get("cest"), 7),
    }
    pid = str(ln.get("produto_id") or "").strip()
    if pid:
        try:
            from produtos.nfce_fiscal_produto_util import fiscal_por_produto_id

            cat = fiscal_por_produto_id(pid, db=db, col_p=col_p, aplicar_defaults_sn=False)
            for k in ("ncm", "cfop", "csosn", "origem", "cest"):
                if not base.get(k) and cat.get(k):
                    base[k] = cat[k]
            if not base.get("cst_pis_cofins") and cat.get("cst_pis_cofins"):
                base["cst_pis_cofins"] = _digits(cat.get("cst_pis_cofins"), 2)
        except Exception:
            pass
    if not base.get("cst_pis_cofins"):
        merged = merge_fiscal_padrao_cadastro_manual_sp_sn({"cst_pis_cofins": ln.get("cst_pis_cofins")})
        base["cst_pis_cofins"] = _digits(merged.get("cst_pis_cofins"), 2) or "49"
    if not base.get("csosn"):
        base["csosn"] = "102"
    return base


@dataclass
class ItemFiscalApurado:
    n_item: int
    cfop: str
    csosn: str
    origem: str
    cst_pis: str
    cst_cofins: str
    icms_grupo: str
    icms_campos: dict[str, str]
    pis_grupo: str
    pis_campos: dict[str, str]
    cofins_grupo: str
    cofins_campos: dict[str, str]
    v_tot_trib: Decimal | None
    regra_id: str
    alertas: list[str] = field(default_factory=list)


@dataclass
class ApuracaoNfeSaida:
    itens: list[ItemFiscalApurado]
    v_tot_trib_nf: Decimal | None
    ibpt_texto: str
    destinatario: dict[str, Any]
    motor_versao: str
    alertas: list[str]
    bloqueios: list[str]

    def exigir_liberado(self) -> None:
        if self.bloqueios:
            raise FiscalBloqueio(self.bloqueios[0])


def _apurar_icms_sn(csosn: str, origem: str, v_prod: Decimal) -> tuple[str, dict[str, str], list[str]]:
    cs = _digits(csosn, 3).zfill(3)
    grupo = _grupo_xml_icms_csosn(cs)
    bloqueios: list[str] = []
    if not grupo:
        bloqueios.append(f"CSOSN {cs} não suportado pelo motor (versão {_MOTOR_VERSAO}).")
        return "ICMSSN102", {"orig": origem[:1], "CSOSN": "102"}, bloqueios

    campos: dict[str, str] = {"orig": origem[:1] or "0", "CSOSN": cs}

    if cs == "101":
        aliq = _cfg_cred_sn_aliquota()
        if aliq is None:
            bloqueios.append(
                "CSOSN 101 exige alíquota de crédito configurada (NFE_SAIDA_PCRED_SN_ALIQ no servidor). "
                "Confirme com o contador antes de emitir."
            )
        else:
            v_cred = (v_prod * aliq / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            campos["pCredSN"] = str(aliq)
            campos["vCredICMSSN"] = _q2(v_cred)

    elif cs in _CSOSN_EXIGE_PARAM_EXTRA:
        bloqueios.append(
            f"CSOSN {cs} exige parametrização fiscal adicional (ST/operação especial). "
            "Cadastre CSOSN 102/103/300/400 ou configure com o contador."
        )

    return grupo, campos, bloqueios


def _apurar_pis_cofins(cst: str) -> tuple[str, dict[str, str], str, dict[str, str], list[str]]:
    c = _digits(cst, 2).zfill(2)
    bloqueios: list[str] = []
    if c in _CST_NT:
        return "PISNT", {"CST": c}, "COFINSNT", {"CST": c}, bloqueios
    if c in _CST_OUTR or c == "49":
        zero = {"CST": c, "vBC": "0.00", "pPIS": "0.0000", "vPIS": "0.00"}
        zero_cof = {"CST": c, "vBC": "0.00", "pCOFINS": "0.0000", "vCOFINS": "0.00"}
        if c == "49":
            zero["pPIS"] = "0.00"
            zero_cof["pCOFINS"] = "0.00"
        return "PISOutr", zero, "COFINSOutr", zero_cof, bloqueios
    bloqueios.append(f"CST PIS/COFINS {c} não mapeado — ajuste cst_pis_cofins no cadastro fiscal do produto.")
    return "PISOutr", {"CST": "49", "vBC": "0.00", "pPIS": "0.00", "vPIS": "0.00"}, "COFINSOutr", {
        "CST": "49",
        "vBC": "0.00",
        "pCOFINS": "0.00",
        "vCOFINS": "0.00",
    }, bloqueios


def _ibpt_item(ncm: str, v_prod: Decimal, uf: str) -> tuple[Decimal | None, str]:
    if not ibpt_habilitado():
        return None, ""
    ncm_d = normalizar_ncm_somente_digitos(ncm)
    if len(ncm_d) != 8:
        return None, ""
    try:
        from produtos.nfce_ibpt_util import _ibpt_aliquotas_ncm

        nac_p, est_p, mun_p = _ibpt_aliquotas_ncm(ncm_d, uf)
        fed = (v_prod * nac_p / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        est = (v_prod * est_p / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        mun = (v_prod * mun_p / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return fed + est + mun, ""
    except Exception:
        return None, ""


def apurar_nfe_saida(
    doc: dict,
    *,
    uf_emit: str | None = None,
    db=None,
    col_p: str | None = None,
) -> ApuracaoNfeSaida:
    dest_raw = doc.get("destinatario") if isinstance(doc.get("destinatario"), dict) else {}
    dest = normalizar_destinatario_fiscal(dest_raw)
    ue = (uf_emit or nfe_saida_emitente_uf_default() or "SP").upper()[:2]
    ud = str(dest.get("uf") or ue).upper()[:2]
    ind_final = int(dest.get("ind_final") or 1)

    itens_doc = doc.get("itens") if isinstance(doc.get("itens"), list) else []
    apurados: list[ItemFiscalApurado] = []
    bloqueios: list[str] = []
    alertas: list[str] = []
    total_trib = Decimal("0")
    trib_count = 0
    fed = est = mun = Decimal("0")

    for idx, ln in enumerate(itens_doc, start=1):
        if not isinstance(ln, dict):
            continue
        fis = _fiscal_linha_enriquecida(ln, db=db, col_p=col_p)
        ncm_ln = normalizar_ncm_somente_digitos(fis.get("ncm") or ln.get("ncm"))
        if len(ncm_ln) != 8:
            bloqueios.append(
                f"Item {idx}: NCM com 8 dígitos obrigatório — cadastre a classificação fiscal do produto "
                f"(não use preenchimento genérico automático na NF-e)."
            )
        qtd = _dec(ln.get("qtd"))
        vu = _dec(ln.get("v_unit"))
        vt = _dec(ln.get("v_total"))
        if vt <= 0 and qtd > 0:
            vt = (qtd * vu).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        st_flag = bool(ln.get("icms_st") or ln.get("substituicao_tributaria"))
        inter_op = bool(ud and ue and ud != ue)
        cfop_linha_raw = _digits(ln.get("cfop"), 4)
        if cfop_linha_raw and not cfop_coerente_operacao(cfop_linha_raw, inter=inter_op):
            bloqueios.append(
                f"Item {idx}: CFOP {cfop_linha_raw} incompatível com operação "
                f"{'interestadual' if inter_op else 'interna'} (UF {ue} → {ud})."
            )
        cfop = sugerir_cfop_item(
            uf_emit=ue,
            uf_dest=ud,
            ind_final=ind_final,
            cfop_linha=fis.get("cfop") or "",
            cfop_catalogo=fis.get("cfop") or "",
            mercadoria_st=st_flag,
        )
        csosn = _digits(fis.get("csosn"), 3) or "102"
        origem = _digits(fis.get("origem"), 1) or "0"
        icms_grupo, icms_campos, bl_icms = _apurar_icms_sn(csosn, origem, vt)
        pis_g, pis_c, cof_g, cof_c, bl_pis = _apurar_pis_cofins(fis.get("cst_pis_cofins") or "49")
        item_bloqueios = bl_icms + bl_pis
        bloqueios.extend(item_bloqueios)

        v_trib_item, _ = _ibpt_item(fis.get("ncm") or "", vt, ud)
        if v_trib_item is not None and v_trib_item > 0:
            total_trib += v_trib_item
            trib_count += 1
            try:
                from produtos.nfce_ibpt_util import _ibpt_aliquotas_ncm

                ncm_ibpt = normalizar_ncm_somente_digitos(fis.get("ncm") or ln.get("ncm"))
                if len(ncm_ibpt) != 8:
                    continue
                nac_p, est_p, mun_p = _ibpt_aliquotas_ncm(ncm_ibpt, ud)
                fed += (vt * nac_p / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                est += (vt * est_p / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                mun += (vt * mun_p / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            except Exception:
                pass

        apurados.append(
            ItemFiscalApurado(
                n_item=idx,
                cfop=cfop,
                csosn=csosn,
                origem=origem,
                cst_pis=pis_c.get("CST", "49"),
                cst_cofins=cof_c.get("CST", "49"),
                icms_grupo=icms_grupo,
                icms_campos=icms_campos,
                pis_grupo=pis_g,
                pis_campos=pis_c,
                cofins_grupo=cof_g,
                cofins_campos=cof_c,
                v_tot_trib=v_trib_item,
                regra_id=f"sn-{csosn}-pis{pis_c.get('CST')}",
                alertas=item_bloqueios,
            )
        )

    v_tot_trib_nf: Decimal | None = None
    ibpt_texto = ""
    if ibpt_habilitado() and trib_count > 0:
        v_tot_trib_nf = total_trib.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        from produtos.caixa_util import format_moeda_br

        ibpt_texto = (
            "Tributos aproximados (Lei 12.741/2012) — Federal "
            f"{format_moeda_br(fed)} · Estadual {format_moeda_br(est)} · "
            f"Municipal {format_moeda_br(mun)}. Fonte: IBPT. "
            f"Motor {_MOTOR_VERSAO}."
        )
    elif not ibpt_habilitado():
        alertas.append(
            "Tributos aproximados (IBPT) não calculados — configure NFC_E_IBPT_TOKEN no servidor "
            "ou NFE_SAIDA_IBPT_USE_FALLBACK=1 após validação contábil."
        )

    # dedupe bloqueios
    bloqueios_u = []
    seen = set()
    for b in bloqueios:
        if b not in seen:
            seen.add(b)
            bloqueios_u.append(b)

    return ApuracaoNfeSaida(
        itens=apurados,
        v_tot_trib_nf=v_tot_trib_nf,
        ibpt_texto=ibpt_texto,
        destinatario=dest,
        motor_versao=_MOTOR_VERSAO,
        alertas=alertas,
        bloqueios=bloqueios_u,
    )


def erros_fiscal_emissao(doc: dict, *, uf_emit: str | None = None) -> list[str]:
    """Bloqueios fiscais antes de montar XML / transmitir."""
    ap = apurar_nfe_saida(doc, uf_emit=uf_emit)
    return list(ap.bloqueios)


def apuracao_serializada(ap: ApuracaoNfeSaida) -> dict[str, Any]:
    """Trilha fiscal persistível no documento (sem dados sensíveis)."""
    return {
        "motor_versao": ap.motor_versao,
        "v_tot_trib_nf": _q2(ap.v_tot_trib_nf) if ap.v_tot_trib_nf is not None else None,
        "ibpt_texto": ap.ibpt_texto,
        "alertas": ap.alertas,
        "destinatario": {
            "ind_final": ap.destinatario.get("ind_final"),
            "ind_ie_dest": ap.destinatario.get("ind_ie_dest"),
            "finalidade_mercadoria": ap.destinatario.get("finalidade_mercadoria"),
            "situacao_icms": ap.destinatario.get("situacao_icms"),
        },
        "itens": [
            {
                "n_item": it.n_item,
                "cfop": it.cfop,
                "csosn": it.csosn,
                "cst_pis": it.cst_pis,
                "icms_grupo": it.icms_grupo,
                "pis_grupo": it.pis_grupo,
                "regra_id": it.regra_id,
                "v_tot_trib": _q2(it.v_tot_trib) if it.v_tot_trib is not None else None,
            }
            for it in ap.itens
        ],
    }
