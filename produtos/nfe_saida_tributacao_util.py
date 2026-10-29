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

from produtos.agro_produto_fiscal_defaults import normalizar_ncm_somente_digitos
from produtos.nfe_saida_regras_validas_util import (
    aliquota_credito_sn_validada,
    classificar_destinatario,
    cmun_destinatario_ou_erro,
    cst_pis_cofins_validado,
    estabelecimento_do_doc,
    ibpt_autorizado,
    resolver_regra_item,
)
from produtos.nfe_saida_util import cfop_padrao_destino, documento_dest_tipo, nfe_saida_emitente_uf_default

_MOTOR_VERSAO = "nfe-trib-2026-10-27-e3"

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
    """Valor monetário (2 casas). Quantidade usa `_dec_qtd`."""
    from produtos.nfe_saida_util import decimal_moeda

    return decimal_moeda(v)


def _dec_qtd(v: Any) -> Decimal:
    from produtos.nfe_saida_util import decimal_qtd

    return decimal_qtd(v)


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
    """Mescla linha com cadastro explícito. Não aplica 102/49/NCM genérico."""
    base = {
        "ncm": normalizar_ncm_somente_digitos(ln.get("ncm")),
        "cfop": _digits(ln.get("cfop"), 4),
        "csosn": _digits(ln.get("csosn"), 3),
        "origem": _digits(ln.get("origem"), 1) or "0",
        "cst_pis": _digits(ln.get("cst_pis") or ln.get("cst_pis_cofins"), 2),
        "cst_cofins": _digits(ln.get("cst_cofins") or ln.get("cst_pis_cofins"), 2),
        "cest": _digits(ln.get("cest"), 7),
        "categoria": str(ln.get("categoria") or "").strip(),
        "produto_id": str(ln.get("produto_id") or "").strip(),
    }
    pid = base["produto_id"]
    if pid:
        try:
            from produtos.nfce_fiscal_produto_util import fiscal_por_produto_id

            cat = fiscal_por_produto_id(pid, db=db, col_p=col_p, aplicar_defaults_sn=False)
            for k in ("ncm", "cfop", "csosn", "origem", "cest"):
                if not base.get(k) and cat.get(k):
                    base[k] = cat[k]
            if not base.get("cst_pis") and cat.get("cst_pis_cofins"):
                base["cst_pis"] = _digits(cat.get("cst_pis_cofins"), 2)
            if not base.get("cst_cofins") and cat.get("cst_pis_cofins"):
                base["cst_cofins"] = _digits(cat.get("cst_pis_cofins"), 2)
            if not base.get("categoria") and cat.get("categoria"):
                base["categoria"] = str(cat.get("categoria") or "").strip()
        except Exception:
            pass
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


def _apurar_icms_sn(
    csosn: str,
    origem: str,
    v_prod: Decimal,
    *,
    p_cred: Decimal | None = None,
    erro_cred: str | None = None,
) -> tuple[str, dict[str, str], list[str]]:
    cs = _digits(csosn, 3)
    bloqueios: list[str] = []
    if len(cs) != 3:
        bloqueios.append("CSOSN com 3 dígitos é obrigatório (regra Validada).")
        return "", {"orig": origem[:1] or "0"}, bloqueios
    grupo = _grupo_xml_icms_csosn(cs)
    if not grupo:
        bloqueios.append(f"CSOSN {cs} não suportado pelo motor (versão {_MOTOR_VERSAO}).")
        return "", {"orig": origem[:1] or "0", "CSOSN": cs}, bloqueios

    campos: dict[str, str] = {"orig": origem[:1] or "0", "CSOSN": cs}

    if cs == "101":
        if p_cred is None:
            bloqueios.append(erro_cred or "CSOSN 101 exige percentual de crédito Validado na competência.")
        else:
            v_cred = (v_prod * p_cred / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            campos["pCredSN"] = str(p_cred)
            campos["vCredICMSSN"] = _q2(v_cred)

    elif cs in _CSOSN_EXIGE_PARAM_EXTRA:
        bloqueios.append(
            f"CSOSN {cs} exige parametrização fiscal adicional (ST/operação especial). "
            "O motor não calcula ST/DIFAL nesta etapa."
        )

    return grupo, campos, bloqueios


def _apurar_pis_cofins(cst_pis: str, cst_cofins: str) -> tuple[str, dict[str, str], str, dict[str, str], list[str]]:
    cp = _digits(cst_pis, 2)
    cc = _digits(cst_cofins, 2)
    bloqueios: list[str] = []

    def _um(c: str, *, pis: bool) -> tuple[str, dict[str, str]]:
        if c in _CST_NT:
            return ("PISNT" if pis else "COFINSNT"), {"CST": c}
        if c in _CST_OUTR or c == "49":
            if pis:
                return "PISOutr", {"CST": c, "vBC": "0.00", "pPIS": "0.00" if c == "49" else "0.0000", "vPIS": "0.00"}
            return "COFINSOutr", {
                "CST": c,
                "vBC": "0.00",
                "pCOFINS": "0.00" if c == "49" else "0.0000",
                "vCOFINS": "0.00",
            }
        return "", {"CST": c}

    if len(cp) != 2 or len(cc) != 2:
        bloqueios.append("CST PIS e CST COFINS com 2 dígitos são obrigatórios.")
        return "PISOutr", {"CST": cp}, "COFINSOutr", {"CST": cc}, bloqueios
    pg, pc = _um(cp, pis=True)
    cg, cc_c = _um(cc, pis=False)
    if not pg:
        bloqueios.append(f"CST PIS {cp} não mapeado — o motor não inventa alíquota tributada.")
    if not cg:
        bloqueios.append(f"CST COFINS {cc} não mapeado — o motor não inventa alíquota tributada.")
    return pg or "PISOutr", pc, cg or "COFINSOutr", cc_c, bloqueios


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
    data_emissao=None,
    cnpj_emit: str | None = None,
    db=None,
    col_p: str | None = None,
) -> ApuracaoNfeSaida:
    from produtos.nfe_saida_regras_validas_util import data_emissao_doc

    dest_raw = doc.get("destinatario") if isinstance(doc.get("destinatario"), dict) else {}
    dest = normalizar_destinatario_fiscal(dest_raw)
    dest["tipo_destinatario"] = classificar_destinatario(dest)
    ue = (uf_emit or nfe_saida_emitente_uf_default() or "SP").upper()[:2]
    ud = str(dest.get("uf") or "").upper()[:2] or ue
    dia = data_emissao or data_emissao_doc(doc)
    loja = estabelecimento_do_doc(doc, cnpj_emit=cnpj_emit or "")
    cidade_emit = ""
    cmun_emit = ""
    try:
        from produtos.nfce_config_util import nfce_cfg

        cfg_e = nfce_cfg(loja) if loja else {}
        cidade_emit = str(cfg_e.get("cidade") or "")
        cmun_emit = str(cfg_e.get("cmun") or "")
        if loja and cfg_e.get("uf"):
            ue = str(cfg_e.get("uf") or ue)[:2]
    except Exception:
        pass
    cmun_d, err_cmun = cmun_destinatario_ou_erro(dest, cidade_emit=cidade_emit, cmun_emit=cmun_emit)

    itens_doc = doc.get("itens") if isinstance(doc.get("itens"), list) else []
    apurados: list[ItemFiscalApurado] = []
    bloqueios: list[str] = []
    alertas: list[str] = []
    total_trib = Decimal("0")
    trib_count = 0
    fed = est = mun = Decimal("0")
    if err_cmun:
        bloqueios.append(err_cmun)
    if not loja:
        bloqueios.append("Informe o estabelecimento emitente (Centro ou Vila Elias).")

    usar_ibpt = ibpt_autorizado(loja or "")
    p_cred, err_cred = (None, None)
    if loja:
        p_cred, err_cred = aliquota_credito_sn_validada(loja, dia)

    for idx, ln in enumerate(itens_doc, start=1):
        if not isinstance(ln, dict):
            continue
        fis = _fiscal_linha_enriquecida(ln, db=db, col_p=col_p)
        ncm_ln = normalizar_ncm_somente_digitos(fis.get("ncm") or ln.get("ncm"))
        if len(ncm_ln) != 8:
            bloqueios.append(
                f"Item {idx}: NCM com 8 dígitos obrigatório — cadastre a classificação do produto. "
                "A NF-e não inventa NCM."
            )
        qtd = _dec_qtd(ln.get("qtd"))
        vu = _dec(ln.get("v_unit"))
        vt = _dec(ln.get("v_total"))
        if vt <= 0 and qtd > 0:
            vt = (qtd * vu).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if ln.get("icms_st") or ln.get("substituicao_tributaria"):
            bloqueios.append(
                f"Item {idx}: ICMS-ST indicado sem cálculo implementado. Não emitimos com ST nesta etapa."
            )
        inter_op = bool(ud and ue and ud != ue)
        cfop_expl = _digits(fis.get("cfop"), 4)
        if cfop_expl and not cfop_coerente_operacao(cfop_expl, inter=inter_op):
            bloqueios.append(
                f"Item {idx}: CFOP {cfop_expl} incompatível com operação "
                f"{'interestadual' if inter_op else 'interna'} (UF {ue} → {ud})."
            )

        regra, err_regra = resolver_regra_item(
            doc,
            {**ln, **fis, "ncm": ncm_ln},
            idx,
            uf_emit=ue,
            uf_dest=ud,
            data_emissao=dia,
            cnpj_emit=cnpj_emit or "",
        )
        bloqueios.extend(err_regra)
        cfop = regra.cfop if regra else cfop_expl
        csosn = regra.csosn if regra else _digits(fis.get("csosn"), 3)
        if regra and cfop_expl and cfop_expl != regra.cfop:
            bloqueios.append(
                f"Item {idx}: CFOP {cfop_expl} do cadastro/item incompatível com a regra Validada {regra.cfop}."
            )
        if regra and _digits(fis.get("csosn"), 3) and _digits(fis.get("csosn"), 3) != regra.csosn:
            bloqueios.append(
                f"Item {idx}: CSOSN {_digits(fis.get('csosn'), 3)} do cadastro incompatível com a regra Validada {regra.csosn}."
            )

        origem = _digits(fis.get("origem"), 1) or "0"
        icms_grupo, icms_campos, bl_icms = _apurar_icms_sn(
            csosn, origem, vt, p_cred=p_cred, erro_cred=err_cred
        )
        cst_p, cst_c = "", ""
        extra_pis: list[str] = []
        if regra and loja:
            cst_p, cst_c, extra_pis = cst_pis_cofins_validado(
                loja, {**ln, **fis, "ncm": ncm_ln}, regra, tipo_dest=dest["tipo_destinatario"]
            )
            for rot, expl, val in (
                ("CST PIS", _digits(fis.get("cst_pis"), 2), cst_p),
                ("CST COFINS", _digits(fis.get("cst_cofins"), 2), cst_c),
            ):
                if expl and val and expl != val:
                    bloqueios.append(
                        f"Item {idx}: {rot} {expl} do cadastro incompatível com o valor Validado {val}."
                    )
        pis_g, pis_c, cof_g, cof_c, bl_pis = _apurar_pis_cofins(cst_p, cst_c)
        item_bloqueios = bl_icms + bl_pis + extra_pis
        bloqueios.extend(item_bloqueios)

        v_trib_item = None
        if usar_ibpt:
            v_trib_item, _ = _ibpt_item(ncm_ln, vt, ud)
            if v_trib_item is not None and v_trib_item > 0:
                total_trib += v_trib_item
                trib_count += 1
                try:
                    from produtos.nfce_ibpt_util import _ibpt_aliquotas_ncm

                    if len(ncm_ln) == 8:
                        nac_p, est_p, mun_p = _ibpt_aliquotas_ncm(ncm_ln, ud)
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
                cst_pis=pis_c.get("CST", cst_p),
                cst_cofins=cof_c.get("CST", cst_c),
                icms_grupo=icms_grupo,
                icms_campos=icms_campos,
                pis_grupo=pis_g,
                pis_campos=pis_c,
                cofins_grupo=cof_g,
                cofins_campos=cof_c,
                v_tot_trib=v_trib_item,
                regra_id=f"regra-{regra.pk}" if regra else "",
                alertas=item_bloqueios,
            )
        )

    v_tot_trib_nf: Decimal | None = None
    ibpt_texto = ""
    if usar_ibpt and trib_count > 0:
        v_tot_trib_nf = total_trib.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        from produtos.caixa_util import format_moeda_br

        ibpt_texto = (
            "Tributos aproximados (Lei 12.741/2012) — Federal "
            f"{format_moeda_br(fed)} · Estadual {format_moeda_br(est)} · "
            f"Municipal {format_moeda_br(mun)}. Fonte: IBPT. "
            f"Motor {_MOTOR_VERSAO}."
        )
    elif not usar_ibpt:
        alertas.append(
            "Tributos aproximados (IBPT) omitidos — cadastro IBPT não Validado ou token ausente. "
            "Não inventamos percentual."
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


def erros_fiscal_emissao(
    doc: dict,
    *,
    uf_emit: str | None = None,
    cnpj_emit: str | None = None,
    data_emissao=None,
) -> list[str]:
    """Bloqueios fiscais antes de montar XML / transmitir."""
    ap = apurar_nfe_saida(doc, uf_emit=uf_emit, cnpj_emit=cnpj_emit, data_emissao=data_emissao)
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
