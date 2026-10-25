"""Configuração fiscal NF-e 55 (Etapa 2) — persistência e leitura. Sem efeito no motor."""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from decouple import config
from django.db import IntegrityError
from django.forms.models import model_to_dict
from django.utils import timezone as dj_tz

from produtos.models import (
    NfeAliquotaCreditoSn,
    NfeAlteracaoFiscalLog,
    NfeEstabelecimentoFiscal,
    NfeExcecaoIcmsSt,
    NfeFiscalStatus,
    NfeNumeracaoAgro,
    NfeParametroIbpt,
    NfeParametroPisCofins,
    NfeQuestionarioResposta,
    NfeRegraOperacao,
)

logger = logging.getLogger(__name__)

ESTAB_CENTRO = "centro"
ESTAB_VILA = "vila"
ESTABELECIMENTOS = (
    (ESTAB_CENTRO, "Centro"),
    (ESTAB_VILA, "Vila Elias"),
)

MSG_PENDENTE = "Configuração fiscal pendente."
MSG_AGUARDANDO = "Aguardando confirmação do contador."
MSG_VALIDADA = "Regra validada."
MSG_CREDITO_VAZIO = "Percentual de crédito não informado."
MSG_SEM_DESTAQUE = "Esta operação pode não destacar ICMS."
MSG_CAMPOS_VAZIOS = "Campos vazios não alteram a emissão atual."
MSG_SALVA = "Configuração salva."
MSG_VER_ALTERACOES = "Ver alterações."

PRECEDENCIA_DOC = (
    "Precedência futura (Etapa 3): exceção do produto → categoria → regra geral. "
    "Nenhuma regra entra no motor nesta etapa."
)


def normalizar_estabelecimento(valor: Any) -> str:
    s = str(valor or "").strip().lower().replace(" ", "_").replace("-", "_")
    if s in ("vila", "vila_elias", "2"):
        return ESTAB_VILA
    return ESTAB_CENTRO


def rotulo_estabelecimento(chave: str) -> str:
    return "Vila Elias" if normalizar_estabelecimento(chave) == ESTAB_VILA else "Centro"


def mascarar_cnpj(cnpj: str) -> str:
    d = re.sub(r"\D", "", cnpj or "")[:14]
    if len(d) != 14:
        return ""
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


def _digits(raw: Any, n: int = 14) -> str:
    return re.sub(r"\D", "", str(raw or ""))[:n]


def _usuario_nome(user) -> str:
    if not user or not getattr(user, "is_authenticated", False):
        return ""
    return (getattr(user, "email", None) or user.get_username() or str(user.pk))[:200]


def _dt_iso(v) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        if dj_tz.is_naive(v):
            v = dj_tz.make_aware(v, dj_tz.get_current_timezone())
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def _dec_or_none(raw: Any) -> Decimal | None:
    if raw is None or raw == "":
        return None
    try:
        return Decimal(str(raw).replace(",", ".").strip())
    except (InvalidOperation, ValueError, TypeError):
        return None


def _date_or_none(raw: Any) -> date | None:
    s = str(raw or "").strip()[:10]
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def competencia_valida(raw: Any) -> str:
    s = str(raw or "").strip()[:7]
    if re.fullmatch(r"\d{4}-\d{2}", s):
        mm = int(s[5:7])
        if 1 <= mm <= 12:
            return s
    return ""


def status_apos_editar(status_atual: str, tem_conteudo: bool) -> str:
    if not tem_conteudo:
        return NfeFiscalStatus.PENDENTE
    if status_atual == NfeFiscalStatus.VALIDADO:
        return NfeFiscalStatus.PREENCHIDO
    return NfeFiscalStatus.PREENCHIDO


def serializar_status(status: str) -> dict[str, Any]:
    st = (status or NfeFiscalStatus.PENDENTE).strip() or NfeFiscalStatus.PENDENTE
    rotulo = dict(NfeFiscalStatus.choices).get(st, st)
    if st == NfeFiscalStatus.VALIDADO:
        msg = MSG_VALIDADA
    elif st == NfeFiscalStatus.PREENCHIDO:
        msg = MSG_AGUARDANDO
    else:
        msg = MSG_PENDENTE
    return {"status": st, "status_rotulo": rotulo, "status_msg": msg}


def registrar_log(
    *,
    tabela: str,
    acao: str,
    user=None,
    estabelecimento: str = "",
    registro_id: Any = "",
    resumo: str = "",
    antes: dict | None = None,
    depois: dict | None = None,
) -> None:
    try:
        NfeAlteracaoFiscalLog.objects.create(
            estabelecimento=estabelecimento or "",
            tabela=tabela[:64],
            registro_id=str(registro_id or "")[:40],
            acao=acao[:24],
            resumo=(resumo or "")[:240],
            antes_json=antes or {},
            depois_json=depois or {},
            usuario=user if getattr(user, "is_authenticated", False) else None,
            usuario_nome=_usuario_nome(user),
        )
    except Exception:
        logger.exception("nfe fiscal log")


def _snap_model(obj) -> dict[str, Any]:
    if obj is None:
        return {}
    data = model_to_dict(obj)
    out: dict[str, Any] = {}
    for k, v in data.items():
        if k.endswith("_por") or k in ("criado_por", "atualizado_por", "validado_por", "usuario"):
            out[k] = getattr(v, "pk", v) if v is not None else None
            continue
        if isinstance(v, Decimal):
            out[k] = str(v)
        elif isinstance(v, (date, datetime)):
            out[k] = _dt_iso(v)
        else:
            out[k] = v
    return out


def _audit_user(obj) -> dict[str, Any]:
    def nome(fk):
        if not fk:
            return ""
        return (getattr(fk, "email", None) or fk.get_username() or str(fk.pk))[:200]

    return {
        "criado_em": _dt_iso(getattr(obj, "criado_em", None)),
        "atualizado_em": _dt_iso(getattr(obj, "atualizado_em", None)),
        "validado_em": _dt_iso(getattr(obj, "validado_em", None)),
        "criado_por_nome": nome(getattr(obj, "criado_por", None)),
        "atualizado_por_nome": nome(getattr(obj, "atualizado_por", None)),
        "validado_por_nome": nome(getattr(obj, "validado_por", None)),
    }


def _marcar_validado(obj, user) -> None:
    obj.status = NfeFiscalStatus.VALIDADO
    obj.validado_em = dj_tz.now()
    obj.validado_por = user if getattr(user, "pk", None) else None
    obj.atualizado_por = user if getattr(user, "pk", None) else None
    obj.save()


# --- Empresa (leitura das configs já usadas) ---------------------------------


def _cert_situacao() -> dict[str, Any]:
    from produtos.nfce_config_util import nfce_resolve_cert_path

    path = nfce_resolve_cert_path()
    presente = bool(path and os.path.isfile(path))
    out = {
        "situacao": "ausente" if not presente else "presente",
        "situacao_rotulo": "Pendente" if not presente else "Presente",
        "validade": "",
        "aviso": "Certificado digital não encontrado." if not presente else "",
    }
    if not presente:
        return out
    password = (config("NFC_E_CERT_PASSWORD", default="") or config("NFE_DIST_DFE_CERT_PASSWORD", default="") or "").strip()
    if not password:
        out["aviso"] = "Arquivo presente. Validade não lida."
        return out
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.serialization import pkcs12

        with open(path, "rb") as fh:
            pfx = fh.read()
        _key, certificate, _ = pkcs12.load_key_and_certificates(pfx, password.encode("utf-8"), default_backend())
        if certificate is None:
            out["aviso"] = "Arquivo presente. Validade não lida."
            return out
        na = certificate.not_valid_after
        if na.tzinfo is None:
            na = na.replace(tzinfo=timezone.utc)
        out["validade"] = na.date().isoformat()
        agora = datetime.now(timezone.utc)
        if na < agora:
            out["situacao"] = "vencido"
            out["situacao_rotulo"] = "Vencido"
            out["aviso"] = "Certificado vencido."
        elif (na - agora).days <= 30:
            out["situacao"] = "vencendo"
            out["situacao_rotulo"] = "Vencendo"
            out["aviso"] = "Certificado vence em breve."
        else:
            out["situacao"] = "valido"
            out["situacao_rotulo"] = "Válido"
            out["aviso"] = ""
    except Exception:
        logger.debug("certificado: validade não lida", exc_info=True)
        out["aviso"] = "Arquivo presente. Validade não lida."
    return out


def snapshot_empresa(estab: str) -> dict[str, Any]:
    from produtos.nfce_config_util import nfce_cfg
    from produtos.nfe_saida_util import nfe_saida_serie_default, nfe_saida_tp_amb_default

    loja = normalizar_estabelecimento(estab)
    cfg = nfce_cfg(loja)
    cnpj = _digits(cfg.get("cnpj"), 14)
    ie = _digits(cfg.get("ie"), 14)
    uf = str(cfg.get("uf") or "").upper()[:2]
    cmun = _digits(cfg.get("cmun"), 7)
    cidade = str(cfg.get("cidade") or "").strip()
    row = NfeEstabelecimentoFiscal.objects.filter(estabelecimento=loja).first()
    serie_propria = row.serie_nfe_55 if row else None
    crt = (row.crt if row else "") or ""
    serie_emissao_atual = nfe_saida_serie_default()
    tp_amb = nfe_saida_tp_amb_default()
    nums = []
    if cnpj:
        for n in NfeNumeracaoAgro.objects.filter(emitente_cnpj=cnpj).order_by("serie"):
            nums.append(
                {
                    "serie": n.serie,
                    "proximo_numero": n.proximo_numero,
                    "atualizado_em": _dt_iso(n.atualizado_em),
                }
            )

    def _ou_pendente(val) -> str:
        s = str(val or "").strip()
        return s if s else "Pendente"

    cert = _cert_situacao()
    pendencias = []
    if not cnpj:
        pendencias.append("CNPJ")
    if not ie:
        pendencias.append("IE")
    if not crt:
        pendencias.append("CRT")
    if loja == ESTAB_VILA and not serie_propria:
        pendencias.append("Série NF-e 55 da Vila")
    if not cmun:
        pendencias.append("Município IBGE")

    payload = {
        "estabelecimento": loja,
        "estabelecimento_rotulo": rotulo_estabelecimento(loja),
        "cnpj": mascarar_cnpj(cnpj) if cnpj else "Pendente",
        "cnpj_ok": bool(len(cnpj) == 14),
        "ie": ie or "Pendente",
        "ie_ok": bool(ie),
        "crt": crt or "Pendente",
        "crt_valor": crt,
        "uf": uf or "Pendente",
        "municipio_ibge": cmun or "Pendente",
        "cidade": cidade or "Pendente",
        "serie_nfe_55": serie_propria,
        "serie_nfe_55_exibicao": str(serie_propria) if serie_propria else "Pendente",
        "serie_emissao_atual": serie_emissao_atual,
        "serie_emissao_nota": (
            "A transmissão NF-e 55 ainda usa a série única do emitente atual. "
            "O campo da Vila não altera essa seleção nesta etapa."
        ),
        "numeracao": nums,
        "numeracao_exibicao": nums or "Pendente",
        "ambiente": "Produção" if tp_amb == 1 else "Homologação",
        "ambiente_codigo": tp_amb,
        "certificado": cert,
        "observacoes": (row.observacoes if row else "") or "",
        "pendencias": pendencias,
        "aviso": MSG_CAMPOS_VAZIOS,
        **serializar_status(row.status if row else NfeFiscalStatus.PENDENTE),
        **(_audit_user(row) if row else {}),
        "id": row.pk if row else None,
    }
    payload["resumo_pendente"] = _ou_pendente("ok" if not pendencias else "")
    return payload


def salvar_empresa(estab: str, body: dict, user) -> dict[str, Any]:
    loja = normalizar_estabelecimento(estab)
    row, created = NfeEstabelecimentoFiscal.objects.get_or_create(estabelecimento=loja)
    antes = _snap_model(row)
    serie = body.get("serie_nfe_55")
    if serie in ("", None):
        row.serie_nfe_55 = None
    else:
        try:
            row.serie_nfe_55 = max(1, min(999, int(serie)))
        except (TypeError, ValueError):
            row.serie_nfe_55 = None
    row.crt = str(body.get("crt") or "").strip()[:8]
    row.observacoes = str(body.get("observacoes") or "").strip()[:4000]
    tem = bool(row.serie_nfe_55 or row.crt or row.observacoes)
    row.status = status_apos_editar(row.status, tem)
    if row.status != NfeFiscalStatus.VALIDADO:
        row.validado_em = None
        row.validado_por = None
    if created:
        row.criado_por = user if getattr(user, "pk", None) else None
    row.atualizado_por = user if getattr(user, "pk", None) else None
    row.save()
    registrar_log(
        tabela="NfeEstabelecimentoFiscal",
        acao="criar" if created else "editar",
        user=user,
        estabelecimento=loja,
        registro_id=row.pk,
        resumo="Empresa",
        antes=antes,
        depois=_snap_model(row),
    )
    return snapshot_empresa(loja)


# --- Crédito SN --------------------------------------------------------------


def serializar_credito(obj: NfeAliquotaCreditoSn) -> dict[str, Any]:
    d = {
        "id": obj.pk,
        "estabelecimento": obj.estabelecimento,
        "competencia": obj.competencia,
        "percentual": str(obj.percentual) if obj.percentual is not None else "",
        "vigencia_inicio": _dt_iso(obj.vigencia_inicio),
        "vigencia_fim": _dt_iso(obj.vigencia_fim),
        "observacoes": obj.observacoes or "",
        **serializar_status(obj.status),
        **_audit_user(obj),
    }
    if obj.percentual is None:
        d["aviso"] = MSG_CREDITO_VAZIO
    return d


def listar_creditos(estab: str) -> list[dict[str, Any]]:
    loja = normalizar_estabelecimento(estab)
    qs = NfeAliquotaCreditoSn.objects.filter(estabelecimento=loja).order_by("-competencia")
    return [serializar_credito(x) for x in qs]


def salvar_credito(estab: str, body: dict, user) -> dict[str, Any]:
    loja = normalizar_estabelecimento(estab)
    pk = body.get("id")
    comp = competencia_valida(body.get("competencia"))
    if not comp:
        return {"ok": False, "erro": "Informe a competência (AAAA-MM)."}
    percentual = _dec_or_none(body.get("percentual"))
    observacoes = str(body.get("observacoes") or "").strip()[:4000]
    obj = None
    created = False
    if pk:
        obj = NfeAliquotaCreditoSn.objects.filter(pk=pk, estabelecimento=loja).first()
        if not obj:
            return {"ok": False, "erro": "Competência não encontrada."}
    else:
        obj = NfeAliquotaCreditoSn.objects.filter(estabelecimento=loja, competencia=comp).first()
        if not obj:
            obj = NfeAliquotaCreditoSn(estabelecimento=loja, competencia=comp)
            created = True
            obj.criado_por = user if getattr(user, "pk", None) else None
    antes = _snap_model(obj) if obj.pk else {}
    if obj.competencia != comp:
        if NfeAliquotaCreditoSn.objects.filter(estabelecimento=loja, competencia=comp).exclude(pk=obj.pk).exists():
            return {"ok": False, "erro": "Já existe percentual para esta competência."}
        obj.competencia = comp
    obj.percentual = percentual
    obj.vigencia_inicio = _date_or_none(body.get("vigencia_inicio"))
    obj.vigencia_fim = _date_or_none(body.get("vigencia_fim"))
    obj.observacoes = observacoes
    tem = percentual is not None or bool(observacoes) or obj.vigencia_inicio or obj.vigencia_fim
    obj.status = status_apos_editar(obj.status if obj.pk else NfeFiscalStatus.PENDENTE, tem)
    if obj.status != NfeFiscalStatus.VALIDADO:
        obj.validado_em = None
        obj.validado_por = None
    obj.atualizado_por = user if getattr(user, "pk", None) else None
    try:
        obj.save()
    except IntegrityError:
        return {"ok": False, "erro": "Já existe percentual para esta competência."}
    registrar_log(
        tabela="NfeAliquotaCreditoSn",
        acao="criar" if created else "editar",
        user=user,
        estabelecimento=loja,
        registro_id=obj.pk,
        resumo=f"Crédito SN {comp}",
        antes=antes,
        depois=_snap_model(obj),
    )
    return {"ok": True, "item": serializar_credito(obj), "itens": listar_creditos(loja)}


# --- PIS/COFINS --------------------------------------------------------------


def serializar_pis(obj: NfeParametroPisCofins | None, estab: str) -> dict[str, Any]:
    if not obj:
        return {
            "id": None,
            "estabelecimento": estab,
            "cst_pis": "",
            "cst_cofins": "",
            "vigencia_inicio": "",
            "vigencia_fim": "",
            "excecoes": [],
            "observacoes": "",
            **serializar_status(NfeFiscalStatus.PENDENTE),
        }
    ex = obj.excecoes_json if isinstance(obj.excecoes_json, list) else []
    return {
        "id": obj.pk,
        "estabelecimento": obj.estabelecimento,
        "cst_pis": obj.cst_pis or "",
        "cst_cofins": obj.cst_cofins or "",
        "vigencia_inicio": _dt_iso(obj.vigencia_inicio),
        "vigencia_fim": _dt_iso(obj.vigencia_fim),
        "excecoes": ex,
        "observacoes": obj.observacoes or "",
        **serializar_status(obj.status),
        **_audit_user(obj),
    }


def salvar_pis(estab: str, body: dict, user) -> dict[str, Any]:
    loja = normalizar_estabelecimento(estab)
    obj, created = NfeParametroPisCofins.objects.get_or_create(estabelecimento=loja)
    antes = _snap_model(obj)
    obj.cst_pis = str(body.get("cst_pis") or "").strip()[:4]
    obj.cst_cofins = str(body.get("cst_cofins") or "").strip()[:4]
    obj.vigencia_inicio = _date_or_none(body.get("vigencia_inicio"))
    obj.vigencia_fim = _date_or_none(body.get("vigencia_fim"))
    obj.observacoes = str(body.get("observacoes") or "").strip()[:4000]
    ex = body.get("excecoes")
    if isinstance(ex, list):
        limpas = []
        for row in ex[:80]:
            if not isinstance(row, dict):
                continue
            limpas.append(
                {
                    "operacao": str(row.get("operacao") or "").strip()[:120],
                    "produto": str(row.get("produto") or "").strip()[:120],
                    "ncm": _digits(row.get("ncm"), 8),
                    "cst_pis": str(row.get("cst_pis") or "").strip()[:4],
                    "cst_cofins": str(row.get("cst_cofins") or "").strip()[:4],
                    "observacoes": str(row.get("observacoes") or "").strip()[:400],
                }
            )
        obj.excecoes_json = limpas
    tem = bool(obj.cst_pis or obj.cst_cofins or obj.observacoes or obj.excecoes_json)
    obj.status = status_apos_editar(obj.status, tem)
    if obj.status != NfeFiscalStatus.VALIDADO:
        obj.validado_em = None
        obj.validado_por = None
    if created:
        obj.criado_por = user if getattr(user, "pk", None) else None
    obj.atualizado_por = user if getattr(user, "pk", None) else None
    obj.save()
    registrar_log(
        tabela="NfeParametroPisCofins",
        acao="criar" if created else "editar",
        user=user,
        estabelecimento=loja,
        registro_id=obj.pk,
        resumo="PIS/COFINS",
        antes=antes,
        depois=_snap_model(obj),
    )
    return serializar_pis(obj, loja)


# --- IBPT --------------------------------------------------------------------


def snapshot_ibpt_integracao() -> dict[str, Any]:
    token = (config("NFC_E_IBPT_TOKEN", default="") or "").strip()
    fallback = (config("NFE_SAIDA_IBPT_USE_FALLBACK", default="") or "").strip().lower() in (
        "1",
        "true",
        "sim",
        "yes",
    )
    if token:
        fonte = "API IBPT (token no servidor)"
        situacao = "Configurado no servidor"
    elif fallback:
        fonte = "Fallback após validação (servidor)"
        situacao = "Fallback ligado — conferir com o contador"
    else:
        fonte = ""
        situacao = "Pendente"
    return {
        "token_configurado": bool(token),
        "fallback_ligado": fallback,
        "fonte_servidor": fonte or "Pendente",
        "situacao_servidor": situacao,
        "aviso": "O token IBPT não é exibido. Campos vazios não alteram a emissão atual.",
    }


def serializar_ibpt(obj: NfeParametroIbpt | None, estab: str) -> dict[str, Any]:
    integ = snapshot_ibpt_integracao()
    base = {
        "id": None,
        "estabelecimento": estab,
        "fonte": "",
        "versao_tabela": "",
        "validade": "",
        "orientacao_contador": "",
        "observacoes": "",
        **serializar_status(NfeFiscalStatus.PENDENTE),
        **integ,
    }
    if not obj:
        return base
    base.update(
        {
            "id": obj.pk,
            "fonte": obj.fonte or "",
            "versao_tabela": obj.versao_tabela or "",
            "validade": _dt_iso(obj.validade),
            "orientacao_contador": obj.orientacao_contador or "",
            "observacoes": obj.observacoes or "",
            **serializar_status(obj.status),
            **_audit_user(obj),
            **integ,
        }
    )
    return base


def salvar_ibpt(estab: str, body: dict, user) -> dict[str, Any]:
    loja = normalizar_estabelecimento(estab)
    obj, created = NfeParametroIbpt.objects.get_or_create(estabelecimento=loja)
    antes = _snap_model(obj)
    obj.fonte = str(body.get("fonte") or "").strip()[:80]
    obj.versao_tabela = str(body.get("versao_tabela") or "").strip()[:40]
    obj.validade = _date_or_none(body.get("validade"))
    obj.orientacao_contador = str(body.get("orientacao_contador") or "").strip()[:4000]
    obj.observacoes = str(body.get("observacoes") or "").strip()[:4000]
    tem = bool(obj.fonte or obj.versao_tabela or obj.validade or obj.orientacao_contador or obj.observacoes)
    obj.status = status_apos_editar(obj.status, tem)
    if obj.status != NfeFiscalStatus.VALIDADO:
        obj.validado_em = None
        obj.validado_por = None
    if created:
        obj.criado_por = user if getattr(user, "pk", None) else None
    obj.atualizado_por = user if getattr(user, "pk", None) else None
    obj.save()
    registrar_log(
        tabela="NfeParametroIbpt",
        acao="criar" if created else "editar",
        user=user,
        estabelecimento=loja,
        registro_id=obj.pk,
        resumo="IBPT",
        antes=antes,
        depois=_snap_model(obj),
    )
    return serializar_ibpt(obj, loja)


# --- Regras ------------------------------------------------------------------


def serializar_regra(obj: NfeRegraOperacao) -> dict[str, Any]:
    return {
        "id": obj.pk,
        "estabelecimento": obj.estabelecimento,
        "ambito": obj.ambito or "",
        "uf_origem": obj.uf_origem or "",
        "uf_destino": obj.uf_destino or "",
        "tipo_destinatario": obj.tipo_destinatario or "",
        "situacao_icms": obj.situacao_icms or "",
        "consumidor_final": obj.consumidor_final,
        "produtor_rural_ie": obj.produtor_rural_ie,
        "orgao_publico": obj.orgao_publico,
        "escopo": obj.escopo,
        "categoria": obj.categoria or "",
        "ncm": obj.ncm or "",
        "produto_id_externo": obj.produto_id_externo or "",
        "cfop": obj.cfop or "",
        "csosn": obj.csosn or "",
        "cst_pis": obj.cst_pis or "",
        "cst_cofins": obj.cst_cofins or "",
        "vigencia_inicio": _dt_iso(obj.vigencia_inicio),
        "vigencia_fim": _dt_iso(obj.vigencia_fim),
        "observacoes": obj.observacoes or "",
        "aviso": MSG_SEM_DESTAQUE if (obj.csosn or "") in ("102", "103", "300", "400") else "",
        "precedencia": PRECEDENCIA_DOC,
        **serializar_status(obj.status),
        **_audit_user(obj),
    }


def listar_regras(estab: str) -> list[dict[str, Any]]:
    loja = normalizar_estabelecimento(estab)
    qs = NfeRegraOperacao.objects.filter(estabelecimento=loja)
    return [serializar_regra(x) for x in qs]


def _bool_opt(raw: Any):
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        return raw
    s = str(raw).strip().lower()
    if s in ("1", "true", "sim", "yes"):
        return True
    if s in ("0", "false", "nao", "não", "no"):
        return False
    return None


def salvar_regra(estab: str, body: dict, user) -> dict[str, Any]:
    loja = normalizar_estabelecimento(estab)
    pk = body.get("id")
    created = False
    if pk:
        obj = NfeRegraOperacao.objects.filter(pk=pk, estabelecimento=loja).first()
        if not obj:
            return {"ok": False, "erro": "Regra não encontrada."}
    else:
        obj = NfeRegraOperacao(estabelecimento=loja)
        created = True
        obj.criado_por = user if getattr(user, "pk", None) else None
    antes = _snap_model(obj) if obj.pk else {}
    obj.ambito = str(body.get("ambito") or "").strip()[:16]
    obj.uf_origem = str(body.get("uf_origem") or "").strip().upper()[:2]
    obj.uf_destino = str(body.get("uf_destino") or "").strip().upper()[:2]
    obj.tipo_destinatario = str(body.get("tipo_destinatario") or "").strip()[:20]
    obj.situacao_icms = str(body.get("situacao_icms") or "").strip()[:32]
    obj.consumidor_final = _bool_opt(body.get("consumidor_final"))
    obj.produtor_rural_ie = _bool_opt(body.get("produtor_rural_ie"))
    obj.orgao_publico = _bool_opt(body.get("orgao_publico"))
    escopo = str(body.get("escopo") or NfeRegraOperacao.Escopo.GERAL).strip()[:16]
    if escopo not in dict(NfeRegraOperacao.Escopo.choices):
        escopo = NfeRegraOperacao.Escopo.GERAL
    obj.escopo = escopo
    obj.categoria = str(body.get("categoria") or "").strip()[:120]
    obj.ncm = _digits(body.get("ncm"), 8)
    obj.produto_id_externo = str(body.get("produto_id_externo") or "").strip()[:40]
    obj.cfop = str(body.get("cfop") or "").strip()[:4]
    obj.csosn = str(body.get("csosn") or "").strip()[:4]
    obj.cst_pis = str(body.get("cst_pis") or "").strip()[:4]
    obj.cst_cofins = str(body.get("cst_cofins") or "").strip()[:4]
    obj.vigencia_inicio = _date_or_none(body.get("vigencia_inicio"))
    obj.vigencia_fim = _date_or_none(body.get("vigencia_fim"))
    obj.observacoes = str(body.get("observacoes") or "").strip()[:4000]
    tem = bool(
        obj.ambito
        or obj.tipo_destinatario
        or obj.cfop
        or obj.csosn
        or obj.cst_pis
        or obj.cst_cofins
        or obj.observacoes
        or obj.ncm
        or obj.categoria
    )
    obj.status = status_apos_editar(obj.status if obj.pk else NfeFiscalStatus.PENDENTE, tem)
    if obj.status != NfeFiscalStatus.VALIDADO:
        obj.validado_em = None
        obj.validado_por = None
    obj.atualizado_por = user if getattr(user, "pk", None) else None
    obj.save()
    registrar_log(
        tabela="NfeRegraOperacao",
        acao="criar" if created else "editar",
        user=user,
        estabelecimento=loja,
        registro_id=obj.pk,
        resumo="Regra de operação",
        antes=antes,
        depois=_snap_model(obj),
    )
    return {"ok": True, "item": serializar_regra(obj), "itens": listar_regras(loja)}


def serializar_st(obj: NfeExcecaoIcmsSt) -> dict[str, Any]:
    return {
        "id": obj.pk,
        "estabelecimento": obj.estabelecimento,
        "tipo": obj.tipo or "st",
        "uf_destino": obj.uf_destino or "",
        "ncm": obj.ncm or "",
        "condicao": obj.condicao or "",
        "fundamentacao": obj.fundamentacao or "",
        "observacoes": obj.observacoes or "",
        **serializar_status(obj.status),
        **_audit_user(obj),
    }


def listar_st(estab: str) -> list[dict[str, Any]]:
    loja = normalizar_estabelecimento(estab)
    return [serializar_st(x) for x in NfeExcecaoIcmsSt.objects.filter(estabelecimento=loja)]


def salvar_st(estab: str, body: dict, user) -> dict[str, Any]:
    loja = normalizar_estabelecimento(estab)
    pk = body.get("id")
    created = False
    if pk:
        obj = NfeExcecaoIcmsSt.objects.filter(pk=pk, estabelecimento=loja).first()
        if not obj:
            return {"ok": False, "erro": "Exceção não encontrada."}
    else:
        obj = NfeExcecaoIcmsSt(estabelecimento=loja)
        created = True
        obj.criado_por = user if getattr(user, "pk", None) else None
    antes = _snap_model(obj) if obj.pk else {}
    obj.tipo = str(body.get("tipo") or "st").strip()[:16] or "st"
    obj.uf_destino = str(body.get("uf_destino") or "").strip().upper()[:2]
    obj.ncm = _digits(body.get("ncm"), 8)
    obj.condicao = str(body.get("condicao") or "").strip()[:200]
    obj.fundamentacao = str(body.get("fundamentacao") or "").strip()[:4000]
    obj.observacoes = str(body.get("observacoes") or "").strip()[:4000]
    tem = bool(obj.uf_destino or obj.ncm or obj.condicao or obj.fundamentacao or obj.observacoes)
    obj.status = status_apos_editar(obj.status if obj.pk else NfeFiscalStatus.PENDENTE, tem)
    if obj.status != NfeFiscalStatus.VALIDADO:
        obj.validado_em = None
        obj.validado_por = None
    obj.atualizado_por = user if getattr(user, "pk", None) else None
    obj.save()
    registrar_log(
        tabela="NfeExcecaoIcmsSt",
        acao="criar" if created else "editar",
        user=user,
        estabelecimento=loja,
        registro_id=obj.pk,
        resumo="Exceção ST/DIFAL",
        antes=antes,
        depois=_snap_model(obj),
    )
    return {"ok": True, "item": serializar_st(obj), "itens": listar_st(loja)}


# --- Questionário ------------------------------------------------------------

QUESTIONARIO_GRUPOS: list[dict[str, Any]] = [
    {
        "grupo": 1,
        "titulo": "Estabelecimentos",
        "perguntas": [
            {"chave": "centro_cnpj_ie_crt", "por_estab": False, "texto": "Confirme CNPJ, IE, CRT e série NF-e 55 do Centro."},
            {"chave": "vila_cnpj_ie_crt", "por_estab": False, "texto": "Confirme CNPJ, IE, CRT e série NF-e 55 da Vila Elias."},
            {"chave": "diferencas_cnpjs", "por_estab": False, "texto": "Quais diferenças fiscais existem entre os dois CNPJs?"},
        ],
    },
    {
        "grupo": 2,
        "titulo": "ICMS e crédito SN",
        "perguntas": [
            {"chave": "csosn", "por_estab": True, "texto": "Qual CSOSN usar na venda típica deste estabelecimento?"},
            {"chave": "direito_credito", "por_estab": True, "texto": "Há direito a crédito de ICMS (CSOSN 101) neste CNPJ?"},
            {"chave": "percentual_competencia", "por_estab": True, "texto": "Como informar o percentual de crédito por competência mensal?"},
            {"chave": "sem_destaque", "por_estab": True, "texto": "Quais operações não destacam ICMS?"},
            {"chave": "excecoes", "por_estab": True, "texto": "Quais exceções de ICMS se aplicam?"},
        ],
    },
    {
        "grupo": 3,
        "titulo": "PIS / COFINS",
        "perguntas": [
            {"chave": "cst_operacao", "por_estab": True, "texto": "Qual CST de PIS e de COFINS por tipo de operação?"},
            {"chave": "destaque", "por_estab": True, "texto": "Haverá destaque de PIS/COFINS na NF-e?"},
            {"chave": "regimes", "por_estab": True, "texto": "Há regimes ou situações especiais de PIS/COFINS?"},
            {"chave": "excecoes", "por_estab": True, "texto": "Quais exceções de PIS/COFINS (operação ou produto)?"},
        ],
    },
    {
        "grupo": 4,
        "titulo": "Regras por operação",
        "perguntas": [
            {"chave": "interna_consumidor", "por_estab": True, "texto": "Interna × consumidor final: CFOP, CSOSN, CST PIS, CST COFINS."},
            {"chave": "interna_revenda", "por_estab": True, "texto": "Interna × revenda: CFOP, CSOSN, CST PIS, CST COFINS."},
            {"chave": "interna_produtor", "por_estab": True, "texto": "Interna × produtor rural CPF+IE: CFOP, CSOSN, CST PIS, CST COFINS."},
            {"chave": "interna_orgao", "por_estab": True, "texto": "Interna × órgão público: CFOP, CSOSN, CST PIS, CST COFINS."},
            {"chave": "inter_consumidor", "por_estab": True, "texto": "Interestadual × consumidor final: CFOP, CSOSN, CST PIS, CST COFINS."},
            {"chave": "inter_revenda", "por_estab": True, "texto": "Interestadual × revenda: CFOP, CSOSN, CST PIS, CST COFINS."},
            {"chave": "inter_produtor", "por_estab": True, "texto": "Interestadual × produtor rural CPF+IE: CFOP, CSOSN, CST PIS, CST COFINS."},
            {"chave": "inter_orgao", "por_estab": True, "texto": "Interestadual × órgão público: CFOP, CSOSN, CST PIS, CST COFINS."},
        ],
    },
    {
        "grupo": 5,
        "titulo": "Produtos",
        "perguntas": [
            {"chave": "racoes", "por_estab": True, "texto": "Rações: exceção fiscal ou «Usar regra geral»?"},
            {"chave": "sacaria", "por_estab": True, "texto": "Sacaria: exceção fiscal ou «Usar regra geral»?"},
            {"chave": "medicamentos_vet", "por_estab": True, "texto": "Medicamentos veterinários: exceção ou «Usar regra geral»?"},
            {"chave": "defensivos", "por_estab": True, "texto": "Defensivos: exceção ou «Usar regra geral»?"},
            {"chave": "ferragens", "por_estab": True, "texto": "Ferragens: exceção ou «Usar regra geral»?"},
            {"chave": "hidraulica", "por_estab": True, "texto": "Hidráulica: exceção ou «Usar regra geral»?"},
        ],
    },
    {
        "grupo": 6,
        "titulo": "ST e DIFAL",
        "perguntas": [
            {"chave": "existe", "por_estab": True, "texto": "Há incidência de ICMS-ST ou DIFAL neste CNPJ? Não assumir incidência."},
            {"chave": "condicoes", "por_estab": True, "texto": "Em quais condições (NCM, operação, destinatário)?"},
            {"chave": "ufs", "por_estab": True, "texto": "Quais UFs de destino, se houver?"},
            {"chave": "responsaveis", "por_estab": True, "texto": "Quem é o responsável pelo recolhimento?"},
            {"chave": "fundamentacao", "por_estab": True, "texto": "Qual a fundamentação legal?"},
        ],
    },
    {
        "grupo": 7,
        "titulo": "Órgãos públicos",
        "perguntas": [
            {"chave": "empenho_pedido_contrato", "por_estab": False, "texto": "Como preencher empenho, pedido e contrato?"},
            {"chave": "xml_tags", "por_estab": False, "texto": "Como usar xNEmp, xPed, xCont, nItemPed e infCpl?"},
            {"chave": "retencoes", "por_estab": False, "texto": "Há retenções em notas para órgão público?"},
        ],
    },
    {
        "grupo": 8,
        "titulo": "IBPT",
        "perguntas": [
            {"chave": "uso", "por_estab": False, "texto": "A NF-e deve informar tributos aproximados (Lei 12.741)?"},
            {"chave": "fonte", "por_estab": False, "texto": "Qual a fonte da tabela IBPT?"},
            {"chave": "validade", "por_estab": False, "texto": "Qual a validade da tabela em uso?"},
            {"chave": "atualizacao", "por_estab": False, "texto": "Qual o procedimento de atualização da tabela?"},
        ],
    },
    {
        "grupo": 9,
        "titulo": "IBS / CBS",
        "perguntas": [
            {"chave": "cronograma", "por_estab": False, "texto": "Há cronograma da reforma (IBS/CBS) para estes CNPJs? Sem calcular agora."},
            {"chave": "contador", "por_estab": False, "texto": "Identifique o contador que validou as respostas deste questionário."},
        ],
    },
]


def listar_questionario() -> list[dict[str, Any]]:
    rows = {
        (r.grupo, r.chave, r.estabelecimento): r
        for r in NfeQuestionarioResposta.objects.all()
    }
    grupos = []
    for g in QUESTIONARIO_GRUPOS:
        perguntas = []
        for p in g["perguntas"]:
            alvos = [ESTAB_CENTRO, ESTAB_VILA] if p["por_estab"] else [""]
            for est in alvos:
                rec = rows.get((g["grupo"], p["chave"], est))
                item = {
                    "grupo": g["grupo"],
                    "chave": p["chave"],
                    "estabelecimento": est,
                    "estabelecimento_rotulo": rotulo_estabelecimento(est) if est else "Ambos",
                    "pergunta": p["texto"],
                    "por_estab": p["por_estab"],
                    "resposta": rec.resposta if rec else "",
                    "observacoes": rec.observacoes if rec else "",
                    "id": rec.pk if rec else None,
                    **serializar_status(rec.status if rec else NfeFiscalStatus.PENDENTE),
                }
                if rec:
                    item.update(_audit_user(rec))
                perguntas.append(item)
        grupos.append({"grupo": g["grupo"], "titulo": g["titulo"], "perguntas": perguntas})
    return grupos


def salvar_questionario_item(body: dict, user) -> dict[str, Any]:
    try:
        grupo = int(body.get("grupo") or 0)
    except (TypeError, ValueError):
        return {"ok": False, "erro": "Grupo inválido."}
    chave = str(body.get("chave") or "").strip()[:64]
    if not grupo or not chave:
        return {"ok": False, "erro": "Informe grupo e chave."}
    meta = None
    for g in QUESTIONARIO_GRUPOS:
        if g["grupo"] != grupo:
            continue
        for p in g["perguntas"]:
            if p["chave"] == chave:
                meta = p
                break
    if not meta:
        return {"ok": False, "erro": "Pergunta desconhecida."}
    estab = ""
    if meta["por_estab"]:
        estab = normalizar_estabelecimento(body.get("estabelecimento"))
    obj, created = NfeQuestionarioResposta.objects.get_or_create(
        grupo=grupo,
        chave=chave,
        estabelecimento=estab,
        defaults={"pergunta": meta["texto"]},
    )
    antes = _snap_model(obj)
    obj.pergunta = meta["texto"]
    obj.resposta = str(body.get("resposta") or "").strip()[:8000]
    obj.observacoes = str(body.get("observacoes") or "").strip()[:4000]
    tem = bool(obj.resposta or obj.observacoes)
    obj.status = status_apos_editar(obj.status, tem)
    if obj.status != NfeFiscalStatus.VALIDADO:
        obj.validado_em = None
        obj.validado_por = None
    if created:
        obj.criado_por = user if getattr(user, "pk", None) else None
    obj.atualizado_por = user if getattr(user, "pk", None) else None
    obj.save()
    registrar_log(
        tabela="NfeQuestionarioResposta",
        acao="criar" if created else "editar",
        user=user,
        estabelecimento=estab,
        registro_id=obj.pk,
        resumo=f"Questionário G{grupo}.{chave}",
        antes=antes,
        depois=_snap_model(obj),
    )
    return {"ok": True, "grupos": listar_questionario()}


def texto_questionario() -> str:
    linhas = [
        "QUESTIONÁRIO FISCAL NF-e 55 — GM Agro Mais / SisVale",
        "Centro e Vila Elias. Campos em branco = Pendente.",
        "Preenchido não significa Validado.",
        "Campos vazios não alteram a emissão atual.",
        "",
    ]
    for g in listar_questionario():
        linhas.append(f"{g['grupo']}. {g['titulo'].upper()}")
        linhas.append("-" * 60)
        for p in g["perguntas"]:
            alvo = p["estabelecimento_rotulo"]
            linhas.append(f"[{alvo}] {p['pergunta']}")
            linhas.append(f"Status: {p['status_rotulo']}")
            linhas.append(f"Resposta: {p['resposta'] or '(em branco)'}")
            if p.get("observacoes"):
                linhas.append(f"Obs.: {p['observacoes']}")
            linhas.append("")
        linhas.append("")
    return "\n".join(linhas).strip() + "\n"


# --- Validar / painel / histórico -------------------------------------------

_MODELOS_VALIDAR = {
    "empresa": NfeEstabelecimentoFiscal,
    "credito": NfeAliquotaCreditoSn,
    "pis": NfeParametroPisCofins,
    "ibpt": NfeParametroIbpt,
    "regra": NfeRegraOperacao,
    "st": NfeExcecaoIcmsSt,
    "questionario": NfeQuestionarioResposta,
}


def validar_registro(tipo: str, pk: int, user, estab: str = "") -> dict[str, Any]:
    model = _MODELOS_VALIDAR.get(str(tipo or "").strip())
    if not model:
        return {"ok": False, "erro": "Tipo inválido."}
    qs = model.objects.filter(pk=pk)
    loja = normalizar_estabelecimento(estab) if estab else ""
    if loja and hasattr(model, "estabelecimento") and model is not NfeQuestionarioResposta:
        qs = qs.filter(estabelecimento=loja)
    obj = qs.first()
    if not obj:
        return {"ok": False, "erro": "Registro não encontrado."}
    antes = _snap_model(obj)
    _marcar_validado(obj, user)
    registrar_log(
        tabela=model.__name__,
        acao="validar",
        user=user,
        estabelecimento=getattr(obj, "estabelecimento", "") or loja,
        registro_id=obj.pk,
        resumo="Validado pelo contador (manual)",
        antes=antes,
        depois=_snap_model(obj),
    )
    return {"ok": True, "status": obj.status}


def listar_historico(estab: str, limite: int = 80) -> list[dict[str, Any]]:
    loja = normalizar_estabelecimento(estab)
    qs = NfeAlteracaoFiscalLog.objects.filter(estabelecimento__in=[loja, ""])[: max(1, min(int(limite or 80), 200))]
    out = []
    for x in qs:
        out.append(
            {
                "id": x.pk,
                "quando": _dt_iso(x.criado_em),
                "tabela": x.tabela,
                "acao": x.acao,
                "resumo": x.resumo,
                "usuario": x.usuario_nome,
                "registro_id": x.registro_id,
            }
        )
    return out


def painel_estabelecimento(estab: str) -> dict[str, Any]:
    loja = normalizar_estabelecimento(estab)
    empresa = snapshot_empresa(loja)
    credito = listar_creditos(loja)
    pis_obj = NfeParametroPisCofins.objects.filter(estabelecimento=loja).first()
    ibpt_obj = NfeParametroIbpt.objects.filter(estabelecimento=loja).first()
    return {
        "ok": True,
        "estabelecimento": loja,
        "estabelecimento_rotulo": rotulo_estabelecimento(loja),
        "mensagens": {
            "pendente": MSG_PENDENTE,
            "aguardando": MSG_AGUARDANDO,
            "validada": MSG_VALIDADA,
            "credito_vazio": MSG_CREDITO_VAZIO,
            "sem_destaque": MSG_SEM_DESTAQUE,
            "campos_vazios": MSG_CAMPOS_VAZIOS,
            "salva": MSG_SALVA,
            "ver_alteracoes": MSG_VER_ALTERACOES,
        },
        "precedencia": PRECEDENCIA_DOC,
        "empresa": empresa,
        "creditos": credito,
        "pis": serializar_pis(pis_obj, loja),
        "ibpt": serializar_ibpt(ibpt_obj, loja),
        "regras": listar_regras(loja),
        "excecoes_st": listar_st(loja),
        "questionario": listar_questionario(),
        "historico": listar_historico(loja),
        "nao_integra_motor": True,
    }


def parse_json_body(request) -> dict:
    try:
        return json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return {}
