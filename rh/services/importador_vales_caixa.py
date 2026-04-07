"""
Importação de vales a partir de lançamentos Mongo (saída de caixa — adiantamento salário).
Desacoplado das views: chamado após gravar saída e via endpoint de reprocessamento.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from bson import ObjectId
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from rh.constants import (
    ADIANTAMENTO_PLANO_ID,
    PLANO_ADIANTAMENTO_CANONICO,
    REF_TIPO_MONGO_DTO_LANCAMENTO,
    REF_TIPO_RH_SALARIO_PARCIAL,
)
from rh.models import Funcionario, InconsistenciaIntegracaoRh, ValeFuncionario
from rh.services.fechamento import garantir_fechamento_aberto, recalcular_todos_abertos_funcionario
from rh.utils import resolver_empresa_por_nome_fantasia, resolver_perfil_rh_para_vale

logger = logging.getLogger(__name__)


def _normalizar_texto_plano_vale(texto: str) -> str:
    """Colapsa travessões/en-dash e espaços para comparar plano gravado no Mongo com o canônico."""
    s = (texto or "").strip()
    for ch in ("\u2014", "\u2013", "\u2012", "\u2010"):
        s = s.replace(ch, "-")
    s = s.replace("—", "-")
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip().lower()


def plano_e_adiantamento_salario_vale(texto: str) -> bool:
    raw = (texto or "").strip()
    if raw == PLANO_ADIANTAMENTO_CANONICO:
        return True
    n = _normalizar_texto_plano_vale(raw)
    if _normalizar_texto_plano_vale(PLANO_ADIANTAMENTO_CANONICO) == n:
        return True
    return "adiantamento" in n and "vale" in n


def _mongo_valor_saida(doc: dict[str, Any]) -> Decimal:
    try:
        v = float(doc.get("Saida") or 0)
    except (TypeError, ValueError):
        v = 0.0
    return Decimal(str(round(v, 2)))


def _mongo_data_competencia(doc: dict[str, Any]) -> date | None:
    dc = doc.get("DataCompetencia")
    if isinstance(dc, datetime):
        return dc.date()
    if isinstance(dc, date):
        return dc
    return None


def _extrair_nome_cliente(doc: dict[str, Any]) -> str:
    return str(doc.get("Cliente") or "").strip()


def _mongo_cliente_id(doc: dict[str, Any]) -> str:
    for k in ("ClienteID", "ClienteId", "clienteID", "clienteId"):
        v = doc.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


@transaction.atomic
def criar_ou_atualizar_vale_de_lancamento(
    doc: dict[str, Any],
    *,
    usuario=None,
) -> ValeFuncionario | None:
    """Idempotente por (referencia_externa_tipo, referencia_externa_id). Sincroniza com o Mongo."""
    if not doc.get("Despesa"):
        return None
    plano = str(doc.get("PlanoDeConta") or "")
    if not plano_e_adiantamento_salario_vale(plano):
        return None

    oid = doc.get("_id")
    if oid is None:
        return None
    mid = str(oid)

    empresa_nome = str(doc.get("Empresa") or "").strip()
    empresa = resolver_empresa_por_nome_fantasia(empresa_nome)
    if not empresa:
        InconsistenciaIntegracaoRh.objects.get_or_create(
            tipo=InconsistenciaIntegracaoRh.Tipo.VALE_SEM_FUNCIONARIO,
            referencia_externa_tipo=REF_TIPO_MONGO_DTO_LANCAMENTO,
            referencia_externa_id=mid,
            defaults={
                "detalhe": f"Empresa '{empresa_nome}' não encontrada no cadastro base.",
            },
        )
        return None

    pessoa = _extrair_nome_cliente(doc)
    cid = _mongo_cliente_id(doc)
    funcionario, modo = resolver_perfil_rh_para_vale(
        empresa,
        mongo_cliente_id=cid or None,
        texto_quem=pessoa or None,
    )
    if not funcionario:
        InconsistenciaIntegracaoRh.objects.get_or_create(
            tipo=InconsistenciaIntegracaoRh.Tipo.VALE_SEM_FUNCIONARIO,
            referencia_externa_tipo=REF_TIPO_MONGO_DTO_LANCAMENTO,
            referencia_externa_id=mid,
            defaults={
                "empresa": empresa,
                "detalhe": (
                    f"Plano de vale; sem perfil RH (ClienteID={cid!r}, modo={modo}, texto={pessoa!r})."
                ),
            },
        )
        return None

    if not funcionario.ativo:
        InconsistenciaIntegracaoRh.objects.get_or_create(
            tipo=InconsistenciaIntegracaoRh.Tipo.FUNCIONARIO_INATIVO,
            referencia_externa_tipo=REF_TIPO_MONGO_DTO_LANCAMENTO,
            referencia_externa_id=mid,
            defaults={
                "empresa": empresa,
                "detalhe": f"Perfil RH inativo: {funcionario.nome_cache} (id={funcionario.pk}).",
            },
        )

    if funcionario.empresa_id != empresa.id:
        InconsistenciaIntegracaoRh.objects.get_or_create(
            tipo=InconsistenciaIntegracaoRh.Tipo.DIVERGENCIA,
            referencia_externa_tipo=REF_TIPO_MONGO_DTO_LANCAMENTO,
            referencia_externa_id=mid,
            defaults={
                "empresa": empresa,
                "detalhe": (
                    f"Perfil RH #{funcionario.pk} é da empresa {funcionario.empresa_id}, "
                    f"mas o lançamento é da empresa {empresa.id} ({empresa_nome!r}). Não vinculado."
                ),
            },
        )
        return None

    dcomp = _mongo_data_competencia(doc) or timezone.localdate()
    valor = _mongo_valor_saida(doc)

    existente = ValeFuncionario.objects.filter(
        referencia_externa_tipo=REF_TIPO_MONGO_DTO_LANCAMENTO,
        referencia_externa_id=mid,
    ).first()

    if existente:
        if existente.tipo_origem != ValeFuncionario.TipoOrigem.CAIXAS:
            return existente
        antigo_fid = existente.funcionario_id
        existente.funcionario = funcionario
        existente.empresa = empresa
        existente.loja = funcionario.loja
        existente.data = dcomp
        existente.valor = valor
        existente.observacao = str(doc.get("Descricao") or "")[:500]
        if existente.cancelado:
            existente.cancelado = False
            existente.cancelado_em = None
            existente.motivo_cancelamento = ""
        existente.save()
        garantir_fechamento_aberto(funcionario, dcomp)
        recalcular_todos_abertos_funcionario(funcionario)
        if antigo_fid and antigo_fid != funcionario.id:
            antigo = Funcionario.objects.filter(pk=antigo_fid).first()
            if antigo:
                recalcular_todos_abertos_funcionario(antigo)
        return existente

    v = ValeFuncionario.objects.create(
        funcionario=funcionario,
        empresa=empresa,
        loja=funcionario.loja,
        data=dcomp,
        valor=valor,
        tipo_origem=ValeFuncionario.TipoOrigem.CAIXAS,
        observacao=str(doc.get("Descricao") or "")[:500],
        referencia_externa_tipo=REF_TIPO_MONGO_DTO_LANCAMENTO,
        referencia_externa_id=mid,
        criado_por=usuario if usuario and getattr(usuario, "is_authenticated", False) else None,
    )
    garantir_fechamento_aberto(funcionario, dcomp)
    return v


def processar_saida_caixa_apos_gravar(
    *,
    plano_id: str,
    mongo_ids: list[str],
    pessoa_nome: str,
    data_competencia: date,
    empresa_nome: str,
    valor: float,
    usuario,
) -> None:
    """Chamado logo após inserir_lancamentos_manual_lote na saída de caixa."""
    if plano_id != ADIANTAMENTO_PLANO_ID or not mongo_ids:
        return
    try:
        from produtos.mongo_financeiro_util import COL_DTO_LANCAMENTO
        from produtos.views import obter_conexao_mongo

        _, db = obter_conexao_mongo()
        if db is None:
            return
        col = db[COL_DTO_LANCAMENTO]
        for mid in mongo_ids:
            try:
                oid = ObjectId(str(mid).strip())
            except Exception:
                continue
            doc = col.find_one({"_id": oid})
            if doc:
                criar_ou_atualizar_vale_de_lancamento(doc, usuario=usuario)
    except Exception:
        logger.exception("RH: falha ao processar vale pós-saída caixa")


def importar_vales_periodo(
    data_de: date,
    data_ate: date,
    *,
    usuario=None,
) -> dict[str, int]:
    """Varre Mongo e cria/atualiza vales (idempotente)."""
    from datetime import time as dtime

    from produtos.mongo_financeiro_util import COL_DTO_LANCAMENTO
    from produtos.views import obter_conexao_mongo

    _, db = obter_conexao_mongo()
    if db is None:
        return {"processados": 0, "mongo_indisponivel": True}
    col = db[COL_DTO_LANCAMENTO]
    dt_ini = datetime.combine(data_de, dtime.min)
    dt_fim = datetime.combine(data_ate, dtime.max)
    q = {
        "Despesa": True,
        "DataCompetencia": {"$gte": dt_ini, "$lte": dt_fim},
    }
    n = 0
    for doc in col.find(q):
        if not plano_e_adiantamento_salario_vale(str(doc.get("PlanoDeConta") or "")):
            continue
        criar_ou_atualizar_vale_de_lancamento(doc, usuario=usuario)
        n += 1
    return {"processados": n}


def marcar_vales_cancelados_por_lancamento_removido(lancamento_id: str) -> int:
    """Quando o título Mongo é excluído no Agro, cancela vales vinculados."""
    lid = str(lancamento_id).strip()
    qs = ValeFuncionario.objects.filter(cancelado=False).filter(
        Q(referencia_externa_tipo=REF_TIPO_MONGO_DTO_LANCAMENTO, referencia_externa_id=lid)
        | Q(
            referencia_externa_tipo=REF_TIPO_RH_SALARIO_PARCIAL,
            referencia_externa_id__startswith=f"{lid}:",
        )
    )
    agora = timezone.now()
    count = 0
    for v in qs:
        v.cancelado = True
        v.cancelado_em = agora
        v.motivo_cancelamento = "Lançamento financeiro excluído no Agro."
        v.save()
        recalcular_todos_abertos_funcionario(v.funcionario)
        count += 1
    return count
