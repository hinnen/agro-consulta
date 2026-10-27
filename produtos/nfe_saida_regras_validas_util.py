"""Etapa 3 — leitura de regras fiscais Validadas (Postgres). Sem inventar CFOP/CSOSN/CST."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from produtos.nfe_fiscal_config_util import parse_estabelecimento
from produtos.nfe_saida_util import nfe_saida_serie_default

_DIG = re.compile(r"\D")


def _digits(raw: Any, n: int) -> str:
    return _DIG.sub("", str(raw or ""))[:n]


@dataclass(frozen=True)
class RegraFiscalResolvida:
    pk: int
    estabelecimento: str
    escopo: str
    cfop: str
    csosn: str
    cst_pis: str
    cst_cofins: str


def data_emissao_doc(doc: dict | None) -> date:
    d = doc if isinstance(doc, dict) else {}
    for key in ("dhEmi", "data_emissao", "dh_emi"):
        raw = d.get(key)
        if isinstance(raw, datetime):
            return raw.date()
        if isinstance(raw, date):
            return raw
        s = str(raw or "").strip()
        if len(s) >= 10:
            try:
                return date.fromisoformat(s[:10])
            except ValueError:
                continue
    return date.today()


def estabelecimento_do_doc(doc: dict | None, *, cnpj_emit: str = "") -> str | None:
    d = doc if isinstance(doc, dict) else {}
    loja = parse_estabelecimento(d.get("estabelecimento"))
    if loja:
        return loja
    fiscal = d.get("fiscal") if isinstance(d.get("fiscal"), dict) else {}
    loja = parse_estabelecimento(fiscal.get("estabelecimento"))
    if loja:
        return loja
    cnpj = _digits(d.get("emitente_cnpj") or cnpj_emit, 14)
    if len(cnpj) == 14:
        try:
            from produtos.nfce_config_util import nfce_loja_de_cnpj

            return parse_estabelecimento(nfce_loja_de_cnpj(cnpj))
        except Exception:
            return None
    return None


def classificar_destinatario(dest: dict | None) -> str:
    """Não classifica só por CPF/CNPJ/IE."""
    d = dest if isinstance(dest, dict) else {}
    tipo_exp = str(d.get("tipo_destinatario") or "").strip().lower()
    if tipo_exp in ("orgao_publico", "revenda", "produtor_rural", "consumidor", "outro"):
        return tipo_exp
    if d.get("orgao_publico") is True:
        return "orgao_publico"
    if d.get("produtor_rural_ie") is True:
        return "produtor_rural"
    fin = str(d.get("finalidade_mercadoria") or "").strip().lower()
    if fin in ("orgao", "orgao_publico", "publico"):
        return "orgao_publico"
    if fin in ("produtor", "produtor_rural"):
        return "produtor_rural"
    if fin in ("revenda", "revender", "comercializacao"):
        return "revenda"
    if fin in ("consumo", "uso", "uso_proprio", "ativo"):
        return "consumidor"
    try:
        ind_final = int(d.get("ind_final") if d.get("ind_final") is not None else 1)
    except (TypeError, ValueError):
        ind_final = 1
    return "revenda" if ind_final == 0 else "consumidor"


def _vigente(row, dia: date) -> bool:
    ini = getattr(row, "vigencia_inicio", None)
    fim = getattr(row, "vigencia_fim", None)
    if ini and dia < ini:
        return False
    if fim and dia > fim:
        return False
    return True


def _filtros_ok(row, *, ambito: str, tipo: str, situacao: str, uf_o: str, uf_d: str) -> bool:
    if row.ambito and row.ambito != ambito:
        return False
    if row.tipo_destinatario and row.tipo_destinatario != tipo:
        return False
    sit = str(row.situacao_icms or "").strip().lower()
    if sit and sit != (situacao or "").strip().lower():
        return False
    if row.uf_origem and row.uf_origem.upper()[:2] != uf_o:
        return False
    if row.uf_destino and row.uf_destino.upper()[:2] != uf_d:
        return False
    return True


def _para_resolvida(row) -> RegraFiscalResolvida:
    return RegraFiscalResolvida(
        pk=int(row.pk),
        estabelecimento=row.estabelecimento,
        escopo=row.escopo,
        cfop=_digits(row.cfop, 4),
        csosn=_digits(row.csosn, 3),
        cst_pis=_digits(row.cst_pis, 2),
        cst_cofins=_digits(row.cst_cofins, 2),
    )


def resolver_regra_item(
    doc: dict,
    ln: dict,
    idx: int,
    *,
    uf_emit: str,
    uf_dest: str,
    data_emissao: date | None = None,
    cnpj_emit: str = "",
) -> tuple[RegraFiscalResolvida | None, list[str]]:
    from produtos.models import NfeFiscalStatus, NfeRegraOperacao

    loja = estabelecimento_do_doc(doc, cnpj_emit=cnpj_emit)
    if not loja:
        return None, [
            f"Item {idx}: informe o estabelecimento emitente (Centro ou Vila Elias) — "
            "não assumimos Centro."
        ]
    dia = data_emissao or data_emissao_doc(doc)
    dest = doc.get("destinatario") if isinstance(doc.get("destinatario"), dict) else {}
    tipo = classificar_destinatario(dest)
    ue = (uf_emit or "").upper()[:2]
    ud = (uf_dest or ue).upper()[:2]
    ambito = "interestadual" if ud and ue and ud != ue else "interna"
    situacao = str(dest.get("situacao_icms") or "").strip().lower()
    prod_id = str(ln.get("produto_id") or ln.get("produto_id_externo") or "").strip()
    ncm = _digits(ln.get("ncm"), 8)
    categoria = str(ln.get("categoria") or "").strip().lower()

    try:
        qs = list(
            NfeRegraOperacao.objects.filter(
                estabelecimento=loja,
                status=NfeFiscalStatus.VALIDADO,
            )
        )
    except Exception:
        return None, [
            f"Item {idx}: cadastro de regras fiscais indisponível. "
            "Valide uma regra em Fiscal / NF-e."
        ]

    cand = [
        r
        for r in qs
        if _vigente(r, dia) and _filtros_ok(r, ambito=ambito, tipo=tipo, situacao=situacao, uf_o=ue, uf_d=ud)
    ]
    if qs and not cand:
        vencidas = [r for r in qs if not _vigente(r, dia)]
        if vencidas and not [
            r
            for r in qs
            if _vigente(r, dia) and _filtros_ok(r, ambito=ambito, tipo=tipo, situacao=situacao, uf_o=ue, uf_d=ud)
        ]:
            return None, [
                f"Item {idx}: regra Validada fora da vigência na data {dia.isoformat()} "
                f"({loja})."
            ]

    t_prod = []
    t_cat = []
    t_geral = []
    for r in cand:
        if r.escopo == NfeRegraOperacao.Escopo.PRODUTO:
            pid_ok = bool(r.produto_id_externo and r.produto_id_externo == prod_id)
            ncm_ok = bool(r.ncm and ncm and r.ncm == ncm)
            if pid_ok or ncm_ok:
                t_prod.append(r)
        elif r.escopo == NfeRegraOperacao.Escopo.CATEGORIA:
            if categoria and str(r.categoria or "").strip().lower() == categoria:
                t_cat.append(r)
        else:
            t_geral.append(r)

    escolhido = t_prod or t_cat or t_geral
    if not escolhido:
        return None, [
            f"Item {idx}: sem regra Validada compatível ({loja}, {ambito}, {tipo}, "
            f"UF {ue}→{ud}, {dia.isoformat()}). Cadastre e valide em Fiscal / NF-e."
        ]
    chaves = {( _digits(r.cfop, 4), _digits(r.csosn, 3), _digits(r.cst_pis, 2), _digits(r.cst_cofins, 2)) for r in escolhido}
    if len(chaves) > 1:
        return None, [
            f"Item {idx}: regras Validadas conflitantes no mesmo nível de precedência. "
            "Ajuste o cadastro Fiscal / NF-e."
        ]
    return _para_resolvida(escolhido[0]), []


def aliquota_credito_sn_validada(loja: str, dia: date) -> tuple[Decimal | None, str | None]:
    from produtos.models import NfeAliquotaCreditoSn, NfeFiscalStatus

    if not loja:
        return None, "Estabelecimento do crédito SN não informado."
    comp = f"{dia.year:04d}-{dia.month:02d}"
    try:
        qs = list(
            NfeAliquotaCreditoSn.objects.filter(
                estabelecimento=loja,
                status=NfeFiscalStatus.VALIDADO,
                competencia=comp,
            )
        )
    except Exception:
        return None, "Cadastro de crédito SN indisponível."
    vigentes = [r for r in qs if _vigente(r, dia) and r.percentual is not None]
    if not vigentes:
        return None, (
            f"CSOSN 101 exige percentual de crédito Validado na competência {comp} "
            f"para {loja}. Não usamos outro mês, outro CNPJ nem o valor do .env."
        )
    vals = {Decimal(str(r.percentual)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP) for r in vigentes}
    if len(vals) > 1:
        return None, f"Percentuais de crédito Validados conflitantes na competência {comp} ({loja})."
    return vals.pop(), None


def cst_pis_cofins_validado(
    loja: str,
    ln: dict,
    regra: RegraFiscalResolvida,
    *,
    tipo_dest: str,
) -> tuple[str, str, list[str]]:
    from produtos.models import NfeFiscalStatus, NfeParametroPisCofins

    erros: list[str] = []
    cst_p, cst_c = regra.cst_pis, regra.cst_cofins
    try:
        param = NfeParametroPisCofins.objects.filter(
            estabelecimento=loja,
            status=NfeFiscalStatus.VALIDADO,
        ).first()
    except Exception:
        param = None
    if param:
        if param.cst_pis:
            cst_p = _digits(param.cst_pis, 2) or cst_p
        if param.cst_cofins:
            cst_c = _digits(param.cst_cofins, 2) or cst_c
        ex = param.excecoes_json if isinstance(param.excecoes_json, list) else []
        ncm = _digits(ln.get("ncm"), 8)
        prod = str(ln.get("descricao") or ln.get("produto") or "").strip().lower()
        pid = str(ln.get("produto_id") or "").strip()
        for row in ex:
            if not isinstance(row, dict):
                continue
            op = str(row.get("operacao") or "").strip().lower()
            if op and op not in (tipo_dest, str(ln.get("operacao") or "").strip().lower()):
                continue
            ncm_ex = _digits(row.get("ncm"), 8)
            prod_ex = str(row.get("produto") or "").strip().lower()
            hit = False
            if ncm_ex and ncm_ex == ncm:
                hit = True
            if prod_ex and (prod_ex == prod or prod_ex == pid.lower()):
                hit = True
            if op and not ncm_ex and not prod_ex:
                hit = True
            if not hit:
                continue
            if row.get("cst_pis"):
                cst_p = _digits(row.get("cst_pis"), 2)
            if row.get("cst_cofins"):
                cst_c = _digits(row.get("cst_cofins"), 2)
            break
    if len(cst_p) != 2 or len(cst_c) != 2:
        erros.append("CST PIS e CST COFINS Validados (2 dígitos) são obrigatórios.")
    return cst_p, cst_c, erros


def ibpt_autorizado(loja: str) -> bool:
    from decouple import config

    from produtos.models import NfeFiscalStatus, NfeParametroIbpt

    token = (config("NFC_E_IBPT_TOKEN", default="") or "").strip()
    fallback = (config("NFE_SAIDA_IBPT_USE_FALLBACK", default="") or "").strip().lower() in (
        "1",
        "true",
        "sim",
        "yes",
    )
    if not loja or not (token or fallback):
        return False
    try:
        return NfeParametroIbpt.objects.filter(
            estabelecimento=loja,
            status=NfeFiscalStatus.VALIDADO,
        ).exists()
    except Exception:
        return False


def resolver_cnpj_serie_emitente(doc: dict, *, cnpj_pedido: str = "") -> tuple[str, int | None, str | None]:
    """CNPJ e série do estabelecimento do documento. Recusa misturar Centro e Vila."""
    from produtos.models import NfeEstabelecimentoFiscal
    from produtos.nfce_config_util import nfce_cfg, nfce_loja_de_cnpj

    loja = estabelecimento_do_doc(doc, cnpj_emit=cnpj_pedido)
    cnpj_req = _digits(cnpj_pedido, 14)
    if not loja and len(cnpj_req) == 14:
        loja = parse_estabelecimento(nfce_loja_de_cnpj(cnpj_req))
    if not loja:
        return "", None, "Informe o estabelecimento emitente (Centro ou Vila Elias)."
    cfg = nfce_cfg(loja)
    cnpj = _digits(cfg.get("cnpj"), 14)
    if len(cnpj) != 14:
        return "", None, f"CNPJ do emitente ({loja}) não configurado."
    if cnpj_req and cnpj_req != cnpj:
        return (
            "",
            None,
            f"CNPJ informado não pertence ao estabelecimento {loja}. "
            "Não misture Centro e Vila Elias.",
        )
    serie = None
    try:
        row = NfeEstabelecimentoFiscal.objects.filter(estabelecimento=loja).first()
        if row and row.serie_nfe_55:
            serie = int(row.serie_nfe_55)
    except Exception:
        row = None
    if serie is None:
        if loja == "vila":
            return (
                cnpj,
                None,
                "Série NF-e 55 da Vila Elias não cadastrada em Fiscal / NF-e. "
                "Não usamos a série do Centro.",
            )
        serie = nfe_saida_serie_default()
    fiscal = doc.get("fiscal") if isinstance(doc.get("fiscal"), dict) else {}
    try:
        serie_doc = int(fiscal.get("serie") or doc.get("serie") or 0)
    except (TypeError, ValueError):
        serie_doc = 0
    if serie_doc and serie_doc != int(serie):
        return (
            cnpj,
            None,
            f"Série {serie_doc} não pertence ao emitente {loja} (série {serie}).",
        )
    return cnpj, int(serie), None


def cmun_destinatario_ou_erro(dest: dict | None, *, cidade_emit: str = "", cmun_emit: str = "") -> tuple[str, str | None]:
    d = dest if isinstance(dest, dict) else {}
    raw = _digits(d.get("cmun") or d.get("cMun"), 7)
    cidade = str(d.get("cidade") or "").strip()
    if len(raw) == 7:
        return raw, None
    dc = cidade.lower()
    ec = str(cidade_emit or "").strip().lower()
    if dc and ec and dc == ec:
        em = _digits(cmun_emit, 7)
        if len(em) == 7:
            return em, None
        return "", "Município do destinatário sem código IBGE (cMun) de 7 dígitos."
    if cidade:
        return (
            "",
            f"Município «{cidade}» exige código IBGE (cMun) de 7 dígitos. "
            "Não usamos Jacupiranga como fallback.",
        )
    return "", "Informe a cidade e o código IBGE (cMun) do destinatário."
