"""Preenchimento padrão Fiscal/NF-e — só o que a contadora confirmou. Sem Validado."""
from __future__ import annotations

from typing import Any

from produtos.models import (
    NfeEstabelecimentoFiscal,
    NfeFiscalStatus,
    NfeNumeracaoAgro,
    NfeParametroPisCofins,
    NfeRegraOperacao,
)
from produtos.nfe_fiscal_config_util import (
    ESTAB_CENTRO,
    ESTAB_VILA,
    _exigir_loja,
    _snap_model,
    registrar_log,
    rotulo_estabelecimento,
    status_apos_editar,
)

CRT_SN = "1"
CST_PIS = "49"
CST_COFINS = "49"
SERIE_CENTRO_SUGESTAO = 1

OBS_102 = (
    "Padrão da contadora: venda interna de mercadoria SEM ST, consumo próprio. "
    "CFOP 5102 / CSOSN 102. Não usar em mercadoria com ST. "
    "Status Preenchido — o contador precisa Validar. Sem NCM individual."
)
OBS_NAO_ST = (
    "Uma regra geral 102 não autoriza produto ST. "
    "Mercadoria com ST permanece bloqueada até regra própria (5405/500 ainda não ativada: motor sem cálculo)."
)

SUGESTAO_101 = (
    "Revenda interna com direito a crédito: CFOP 5102 / CSOSN 101. "
    "Não criada: falta percentual de crédito mensal Validado."
)
SUGESTAO_500 = (
    "ST anteriormente recolhida: contadora indicou CFOP 5405 / CSOSN 500. "
    "Não ativada: o motor ainda bloqueia CSOSN 500."
)


def _existe_regra(loja: str, spec: dict) -> bool:
    return NfeRegraOperacao.objects.filter(
        estabelecimento=loja,
        ambito=spec["ambito"],
        tipo_destinatario=spec["tipo_destinatario"],
        escopo=NfeRegraOperacao.Escopo.GERAL,
        cfop=spec["cfop"],
        csosn=spec["csosn"],
        ncm="",
        categoria="",
        produto_id_externo="",
    ).exists()


def _regras_sugeridas() -> list[dict[str, Any]]:
    comuns = {
        "ambito": "interna",
        "uf_origem": "SP",
        "uf_destino": "SP",
        "escopo": "geral",
        "cfop": "5102",
        "cst_pis": CST_PIS,
        "cst_cofins": CST_COFINS,
        "ncm": "",
    }
    return [
        {
            **comuns,
            "tipo_destinatario": "consumidor",
            "consumidor_final": True,
            "orgao_publico": False,
            "csosn": "102",
            "aplicar": True,
            "titulo": "Interna · consumo próprio · sem ST",
            "detalhe": "CFOP 5102 / CSOSN 102 / CST 49. Só mercadoria sem ST.",
        },
        {
            **comuns,
            "tipo_destinatario": "orgao_publico",
            "consumidor_final": True,
            "orgao_publico": True,
            "csosn": "102",
            "aplicar": True,
            "titulo": "Interna · órgão público · consumo próprio · sem ST",
            "detalhe": "Mesmos códigos 5102/102 confirmados para consumo próprio (ex.: prefeitura).",
        },
        {
            **comuns,
            "tipo_destinatario": "revenda",
            "consumidor_final": False,
            "orgao_publico": False,
            "csosn": "101",
            "aplicar": False,
            "titulo": "Interna · revenda com crédito",
            "detalhe": SUGESTAO_101,
        },
        {
            **comuns,
            "tipo_destinatario": "consumidor",
            "consumidor_final": True,
            "orgao_publico": False,
            "cfop": "5405",
            "csosn": "500",
            "aplicar": False,
            "titulo": "Interna · ST já recolhida",
            "detalhe": SUGESTAO_500,
        },
    ]


def _serie_centro_sugerida(loja: str) -> tuple[int | None, str]:
    if loja != ESTAB_CENTRO:
        return None, "Vila: série NF-e 55 permanece Pendente até confirmação."
    row = NfeEstabelecimentoFiscal.objects.filter(estabelecimento=loja).first()
    if row and row.serie_nfe_55:
        return int(row.serie_nfe_55), f"Série já cadastrada ({row.serie_nfe_55}) — não será alterada."
    nums = list(NfeNumeracaoAgro.objects.order_by("serie").values_list("serie", flat=True)[:8])
    if len(nums) == 1:
        return int(nums[0]), f"Série sugerida a partir da numeração já usada ({nums[0]})."
    return SERIE_CENTRO_SUGESTAO, "Série sugerida 1 (Centro). Só preenche se o campo estiver vazio."


def previa_padrao_contador(estab: str) -> dict[str, Any]:
    loja, err = _exigir_loja(estab)
    if err:
        return err
    emp = NfeEstabelecimentoFiscal.objects.filter(estabelecimento=loja).first()
    pis = NfeParametroPisCofins.objects.filter(estabelecimento=loja).first()
    serie, serie_nota = _serie_centro_sugerida(loja)
    crt_atual = (emp.crt if emp else "") or ""
    pis_atual = (pis.cst_pis if pis else "") or ""
    cof_atual = (pis.cst_cofins if pis else "") or ""
    emp_validado = bool(emp and emp.status == NfeFiscalStatus.VALIDADO)
    pis_validado = bool(pis and pis.status == NfeFiscalStatus.VALIDADO)

    acoes: list[dict[str, str]] = []
    if emp_validado:
        acoes.append(
            {
                "campo": "empresa",
                "acao": "manter",
                "texto": "Empresa já Validada — CRT/série não serão alterados.",
            }
        )
    else:
        if crt_atual:
            acoes.append({"campo": "crt", "acao": "manter", "texto": f"CRT já preenchido ({crt_atual})."})
        else:
            acoes.append({"campo": "crt", "acao": "preencher", "texto": f"CRT {CRT_SN} (Simples Nacional)."})
        if loja == ESTAB_VILA:
            acoes.append({"campo": "serie", "acao": "manter", "texto": serie_nota})
        elif emp and emp.serie_nfe_55:
            acoes.append({"campo": "serie", "acao": "manter", "texto": serie_nota})
        else:
            acoes.append({"campo": "serie", "acao": "preencher", "texto": serie_nota})

    if pis_validado:
        acoes.append({"campo": "pis", "acao": "manter", "texto": "PIS/COFINS já Validado — CST não será alterado."})
    else:
        if pis_atual or cof_atual:
            acoes.append(
                {
                    "campo": "pis",
                    "acao": "manter",
                    "texto": f"CST PIS/COFINS já cadastrado ({pis_atual or '—'} / {cof_atual or '—'}).",
                }
            )
        else:
            acoes.append(
                {
                    "campo": "pis",
                    "acao": "preencher",
                    "texto": f"CST PIS {CST_PIS} e CST COFINS {CST_COFINS}.",
                }
            )

    regras_prev = []
    for spec in _regras_sugeridas():
        existe = _existe_regra(loja, spec)
        item = {
            "titulo": spec["titulo"],
            "detalhe": spec["detalhe"],
            "cfop": spec["cfop"],
            "csosn": spec["csosn"],
            "tipo_destinatario": spec["tipo_destinatario"],
            "aplicar": bool(spec["aplicar"] and not existe),
            "existe": existe,
            "status_previsto": "preenchido" if spec["aplicar"] else "nao_criar",
        }
        if existe:
            item["texto_acao"] = "Já existe — não duplica."
        elif spec["aplicar"]:
            item["texto_acao"] = "Será criada como Preenchido (não Validado)."
        else:
            item["texto_acao"] = "Não será criada."
        regras_prev.append(item)

    return {
        "ok": True,
        "estabelecimento": loja,
        "estabelecimento_rotulo": rotulo_estabelecimento(loja),
        "aviso": (
            "Não marca Validado. Não preenche nome/CRC/data do contador. "
            "Não grava NCM de produto. " + OBS_NAO_ST
        ),
        "acoes": acoes,
        "regras": regras_prev,
        "serie_sugerida": serie,
        "crt": CRT_SN,
        "cst_pis": CST_PIS,
        "cst_cofins": CST_COFINS,
    }


def aplicar_padrao_contador(estab: str, user) -> dict[str, Any]:
    previa = previa_padrao_contador(estab)
    if not previa.get("ok"):
        return previa
    loja = previa["estabelecimento"]
    criados: list[str] = []
    mantidos: list[str] = []

    emp, emp_created = NfeEstabelecimentoFiscal.objects.get_or_create(estabelecimento=loja)
    if emp.status == NfeFiscalStatus.VALIDADO:
        mantidos.append("empresa validada")
    else:
        antes = _snap_model(emp)
        mudou = False
        if not (emp.crt or "").strip():
            emp.crt = CRT_SN
            mudou = True
            criados.append("CRT 1")
        else:
            mantidos.append("CRT existente")
        if loja == ESTAB_CENTRO and not emp.serie_nfe_55:
            serie, _nota = _serie_centro_sugerida(loja)
            if serie:
                emp.serie_nfe_55 = serie
                mudou = True
                criados.append(f"série {serie}")
        elif loja == ESTAB_VILA:
            mantidos.append("série Vila pendente")
        else:
            mantidos.append("série existente")
        if mudou:
            emp.status = status_apos_editar(emp.status, True)
            if emp.status != NfeFiscalStatus.VALIDADO:
                emp.validado_em = None
                emp.validado_por = None
            if emp_created:
                emp.criado_por = user if getattr(user, "pk", None) else None
            emp.atualizado_por = user if getattr(user, "pk", None) else None
            emp.save()
            registrar_log(
                tabela="NfeEstabelecimentoFiscal",
                acao="criar" if emp_created else "editar",
                user=user,
                estabelecimento=loja,
                registro_id=emp.pk,
                resumo="Padrão contadora (empresa)",
                antes=antes,
                depois=_snap_model(emp),
            )

    pis, pis_created = NfeParametroPisCofins.objects.get_or_create(estabelecimento=loja)
    if pis.status == NfeFiscalStatus.VALIDADO:
        mantidos.append("PIS/COFINS validado")
    elif (pis.cst_pis or "").strip() or (pis.cst_cofins or "").strip():
        mantidos.append("CST PIS/COFINS existente")
    else:
        antes = _snap_model(pis)
        pis.cst_pis = CST_PIS
        pis.cst_cofins = CST_COFINS
        pis.observacoes = (pis.observacoes or "").strip()
        extra = "Padrão da contadora: CST 49/49 (Simples Nacional). Sem alíquota inventada."
        if extra not in (pis.observacoes or ""):
            pis.observacoes = (pis.observacoes + " " + extra).strip()[:4000]
        pis.status = status_apos_editar(pis.status, True)
        if pis.status != NfeFiscalStatus.VALIDADO:
            pis.validado_em = None
            pis.validado_por = None
        if pis_created:
            pis.criado_por = user if getattr(user, "pk", None) else None
        pis.atualizado_por = user if getattr(user, "pk", None) else None
        pis.save()
        registrar_log(
            tabela="NfeParametroPisCofins",
            acao="criar" if pis_created else "editar",
            user=user,
            estabelecimento=loja,
            registro_id=pis.pk,
            resumo="Padrão contadora (PIS/COFINS 49)",
            antes=antes,
            depois=_snap_model(pis),
        )
        criados.append("CST PIS/COFINS 49")

    for spec in _regras_sugeridas():
        if not spec["aplicar"]:
            mantidos.append(spec["titulo"] + " (não criada)")
            continue
        if _existe_regra(loja, spec):
            mantidos.append(spec["titulo"] + " (já existia)")
            continue
        obj = NfeRegraOperacao(
            estabelecimento=loja,
            ambito=spec["ambito"],
            uf_origem=spec.get("uf_origem") or "",
            uf_destino=spec.get("uf_destino") or "",
            tipo_destinatario=spec["tipo_destinatario"],
            consumidor_final=spec.get("consumidor_final"),
            orgao_publico=spec.get("orgao_publico"),
            escopo=NfeRegraOperacao.Escopo.GERAL,
            cfop=spec["cfop"],
            csosn=spec["csosn"],
            cst_pis=spec["cst_pis"],
            cst_cofins=spec["cst_cofins"],
            observacoes=OBS_102,
            status=NfeFiscalStatus.PREENCHIDO,
        )
        if getattr(user, "pk", None):
            obj.criado_por = user
            obj.atualizado_por = user
        obj.save()
        registrar_log(
            tabela="NfeRegraOperacao",
            acao="criar",
            user=user,
            estabelecimento=loja,
            registro_id=obj.pk,
            resumo=f"Padrão contadora ({spec['cfop']}/{spec['csosn']} {spec['tipo_destinatario']})",
            antes={},
            depois=_snap_model(obj),
        )
        criados.append(f"regra {spec['cfop']}/{spec['csosn']} {spec['tipo_destinatario']}")

    return {
        "ok": True,
        "estabelecimento": loja,
        "criados": criados,
        "mantidos": mantidos,
        "mensagem": "Padrão gravado como Preenchido. O contador ainda precisa Validar.",
        "previa": previa_padrao_contador(loja),
    }
