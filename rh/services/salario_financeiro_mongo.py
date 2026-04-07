"""
Título único de salário no Mongo (DtoLancamento) por fechamento/competência.
Vales entram como baixa parcial (ValorPago) nesse título — não criam despesa separada de adiantamento.
"""

from __future__ import annotations

import logging
import secrets
from datetime import date, datetime, time as dtime
from decimal import Decimal
from typing import Any

from bson import ObjectId
from django.conf import settings
from django.utils import timezone

from produtos.mongo_financeiro_util import (
    COL_DTO_LANCAMENTO,
    baixar_lancamento_parcial_mongo,
    inserir_lancamentos_manual_lote,
    _dt_naive_meia_noite_erp,
)
from produtos.views import obter_conexao_mongo

from rh.constants import REF_TIPO_RH_SALARIO_PARCIAL
from rh.models import FechamentoFolhaSimplificado, Funcionario, ValeFuncionario
from rh.services.fechamento import primeiro_dia_mes, recalcular_fechamento, total_vales_mes
from rh.services.vale_manual_financeiro import _split_id_nome
from rh.utils import resolver_empresa_por_nome_fantasia, resolver_perfil_rh_para_vale

logger = logging.getLogger(__name__)


def _plano_salario_folha() -> str:
    return (getattr(settings, "AGRO_RH_PLANO_SALARIO_FOLHA", None) or "").strip() or "2.1.1.1.2 — Salários"


def bruto_titulo_salario(f: FechamentoFolhaSimplificado) -> Decimal:
    x = f.salario_base_na_competencia + f.outros_proventos - f.outros_descontos
    if x < 0:
        return Decimal("0")
    return x


def _valor_pago_vales_vs_bruto(f: FechamentoFolhaSimplificado, bruto: Decimal) -> Decimal:
    comp = primeiro_dia_mes(f.competencia)
    tv = total_vales_mes(f.funcionario, comp.year, comp.month)
    return tv if tv < bruto else bruto


def _usuario_label(usuario) -> str:
    if usuario is not None and getattr(usuario, "is_authenticated", False):
        return (
            getattr(usuario, "email", None) or usuario.get_username() or str(usuario.pk)
        )[:120]
    return "RH"


def _data_movimento_naive(d: date) -> datetime:
    naive = datetime.combine(d, dtime(12, 0, 0))
    if timezone.is_naive(naive):
        return timezone.make_aware(naive, timezone.get_current_timezone())
    return naive


def _aplicar_totais_no_documento_mongo(
    db,
    mongo_id: str,
    *,
    saida: float,
    valor_pago: float,
    data_vencimento: date | None,
) -> dict[str, Any]:
    col = db[COL_DTO_LANCAMENTO]
    try:
        oid = ObjectId(str(mongo_id).strip())
    except Exception:
        return {"ok": False, "erro": "ID Mongo inválido."}
    doc = col.find_one({"_id": oid})
    if not doc:
        return {"ok": False, "erro": "Título não encontrado no Mongo."}
    now = timezone.now()
    mod = "Agro — sync folha RH"
    quitado = valor_pago >= saida - 0.02
    dv = _dt_naive_meia_noite_erp(data_vencimento) if data_vencimento else doc.get("DataVencimento")
    patch: dict[str, Any] = {
        "Saida": float(saida),
        "ValorPago": float(valor_pago),
        "Pago": bool(quitado),
        "LastUpdate": now,
        "ModificadoPor": mod[:200],
    }
    if data_vencimento:
        patch["DataVencimento"] = dv
        patch["DataVencimentoOriginal"] = dv
    if quitado:
        patch["DataPagamento"] = now
    else:
        patch["DataPagamento"] = doc.get("DataPagamento")
    col.update_one({"_id": oid}, {"$set": patch})
    return {"ok": True}


def sincronizar_valores_titulo_salario_mongo(fechamento: FechamentoFolhaSimplificado) -> dict[str, Any]:
    """Alinha Saida / ValorPago / vencimento do título com o fechamento e vales no RH."""
    mid = (fechamento.mongo_lancamento_salario_id or "").strip()
    if not mid:
        return {"ok": False, "erro": "Nenhum título de salário vinculado a este fechamento."}
    _, db = obter_conexao_mongo()
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível."}
    bruto = bruto_titulo_salario(fechamento)
    vp = _valor_pago_vales_vs_bruto(fechamento, bruto)
    return _aplicar_totais_no_documento_mongo(
        db,
        mid,
        saida=float(bruto),
        valor_pago=float(vp),
        data_vencimento=fechamento.data_vencimento_pagamento,
    )


def criar_ou_atualizar_titulo_salario_mongo(
    fechamento: FechamentoFolhaSimplificado,
    *,
    usuario,
    data_vencimento: date,
    forma_value: str,
    banco_value: str,
) -> dict[str, Any]:
    """
    Cria o DtoLancamento de salário (despesa) ou atualiza vencimento/cabeçalho básico.
    Valor bruto e ValorPago (adiantamentos) vêm do fechamento atual.
    """
    recalcular_fechamento(fechamento)
    fechamento.refresh_from_db()
    bruto = bruto_titulo_salario(fechamento)
    if bruto <= 0:
        return {"ok": False, "erro": "Bruto a pagar é zero — ajuste salário/proventos/descontos antes."}

    fid, fn = _split_id_nome(forma_value)
    bid, bn = _split_id_nome(banco_value)
    if not bn:
        return {"ok": False, "erro": "Conta / banco é obrigatório para gerar o título no financeiro."}
    fn = (fn or "").strip()
    if not fn:
        fn = ""
        fid = None

    f = fechamento.funcionario
    ca = f.cliente_agro
    pessoa_nome = (ca.nome or f.nome_cache or "").strip()[:300]
    if not pessoa_nome:
        return {"ok": False, "erro": "Pessoa base sem nome."}
    ext = (ca.externo_id or "").strip()
    pessoa_id = (ext or f"local:{ca.pk}")[:120]
    empresa_nome = (f.empresa.nome_fantasia or "").strip()
    if not empresa_nome:
        return {"ok": False, "erro": "Empresa inválida."}

    comp = primeiro_dia_mes(fechamento.competencia)
    desc = (
        f"Folha — {pessoa_nome} — competência {comp:%m/%Y}"
    )[:500]

    _, db = obter_conexao_mongo()
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível."}

    plano = _plano_salario_folha()
    usuario_label = _usuario_label(usuario)
    mid = (fechamento.mongo_lancamento_salario_id or "").strip()

    if mid:
        r = _aplicar_totais_no_documento_mongo(
            db,
            mid,
            saida=float(bruto),
            valor_pago=float(_valor_pago_vales_vs_bruto(fechamento, bruto)),
            data_vencimento=data_vencimento,
        )
        if not r.get("ok"):
            return r
        fechamento.data_vencimento_pagamento = data_vencimento
        fechamento.save(update_fields=["data_vencimento_pagamento", "atualizado_em"])
        return {"ok": True, "id": mid, "criado": False}

    linhas = [
        {
            "plano_conta": plano[:200],
            "plano_conta_id": None,
            "valor": float(bruto),
            "descricao": desc,
            "observacao": "Título único folha RH (vales = baixa parcial)",
        }
    ]
    resultado = inserir_lancamentos_manual_lote(
        db,
        despesa=True,
        empresa_nome=empresa_nome[:200],
        empresa_id=None,
        pessoa_nome=pessoa_nome,
        pessoa_id=pessoa_id,
        data_competencia=comp,
        data_vencimento=data_vencimento,
        banco_nome=bn[:200],
        banco_id=bid,
        forma_nome=(fn[:200] if fn else ""),
        forma_id=fid,
        grupo_nome=None,
        grupo_id=None,
        usuario_label=usuario_label,
        linhas=linhas,
    )
    ids = resultado.get("ids") or []
    if not ids:
        msg = "Falha ao gravar no financeiro."
        erros = resultado.get("erros") or []
        if erros:
            msg = str(erros[0].get("erro") or msg)[:400]
        return {"ok": False, "erro": msg}

    new_id = str(ids[0]).strip()
    vp = _valor_pago_vales_vs_bruto(fechamento, bruto)
    _aplicar_totais_no_documento_mongo(
        db,
        new_id,
        saida=float(bruto),
        valor_pago=float(vp),
        data_vencimento=data_vencimento,
    )
    fechamento.mongo_lancamento_salario_id = new_id[:32]
    fechamento.data_vencimento_pagamento = data_vencimento
    fechamento.save(
        update_fields=[
            "mongo_lancamento_salario_id",
            "data_vencimento_pagamento",
            "atualizado_em",
        ]
    )
    return {"ok": True, "id": new_id, "criado": True}


def _fechamento_para_data_funcionario(funcionario: Funcionario, d: date) -> FechamentoFolhaSimplificado | None:
    comp = primeiro_dia_mes(d)
    return (
        FechamentoFolhaSimplificado.objects.filter(funcionario=funcionario, competencia=comp)
        .select_related("funcionario", "funcionario__cliente_agro", "funcionario__empresa")
        .first()
    )


def registrar_vale_como_baixa_parcial_salario(
    *,
    funcionario: Funcionario,
    usuario,
    data: date,
    valor: Decimal,
    observacao: str,
    forma_value: str,
    banco_value: str,
    tipo_origem: str,
) -> dict[str, Any]:
    """
    Registra vale no RH e aplica baixa parcial no título de salário do mês (Mongo).
    tipo_origem: ValeFuncionario.TipoOrigem.MANUAL ou CAIXAS.
    """
    from rh.services.fechamento import garantir_fechamento_aberto, recalcular_todos_abertos_funcionario

    garantir_fechamento_aberto(funcionario, data)
    fech = _fechamento_para_data_funcionario(funcionario, data)
    if not fech:
        return {"ok": False, "erro": "Fechamento do mês não encontrado."}
    mid = (fech.mongo_lancamento_salario_id or "").strip()
    if not mid:
        return {
            "ok": False,
            "erro": (
                "Gere antes o título de salário no fechamento da competência "
                f"({fech.competencia:%m/%Y}) com data de vencimento — os vales entram como pagamento parcial desse título."
            ),
        }

    recalcular_fechamento(fech)
    fech.refresh_from_db()

    fid, fn = _split_id_nome(forma_value)
    bid, bn = _split_id_nome(banco_value)
    if not fn or not bn:
        return {"ok": False, "erro": "Forma e conta/banco são obrigatórios."}

    _, db = obter_conexao_mongo()
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível."}

    data_mov = _data_movimento_naive(data)
    usuario_label = _usuario_label(usuario)
    parc = [
        {
            "valor": float(valor),
            "forma_pagamento": fn[:200],
            "forma_pagamento_id": fid,
            "banco": bn[:200],
            "banco_id": bid,
        }
    ]
    r = baixar_lancamento_parcial_mongo(
        db,
        mid,
        despesa=True,
        data_movimento=data_mov,
        parcelas=parc,
        usuario_label=usuario_label,
    )
    if not r.get("ok"):
        return {"ok": False, "erro": (r.get("erro") or "Falha na baixa parcial.")[:500]}

    ref_id = f"{mid}:{secrets.token_hex(8)}"
    try:
        ValeFuncionario.objects.create(
            funcionario=funcionario,
            empresa=funcionario.empresa,
            loja=funcionario.loja,
            data=data,
            valor=valor,
            tipo_origem=tipo_origem,
            observacao=(observacao or "")[:500],
            referencia_externa_tipo=REF_TIPO_RH_SALARIO_PARCIAL,
            referencia_externa_id=ref_id[:64],
            criado_por=usuario if usuario and getattr(usuario, "is_authenticated", False) else None,
        )
    except Exception:
        logger.exception("RH: vale após baixa parcial — inconsistência possível (Mongo já movimentado)")
        return {
            "ok": False,
            "erro": "Baixa parcial aplicada no financeiro, mas falhou ao gravar o vale no RH. Conferir lançamentos.",
        }

    recalcular_todos_abertos_funcionario(funcionario)
    fech.refresh_from_db()
    sincronizar_valores_titulo_salario_mongo(fech)
    return {"ok": True, "mongo_titulo_id": mid, "ids": [mid], "quitado": bool(r.get("quitado"))}


def tentar_caixa_adiant_vale_como_baixa_parcial(
    *,
    db,
    data_competencia: date,
    empresa_nome: str,
    pessoa_nome: str,
    pessoa_id: str | None,
    valor: float,
    forma_nome: str,
    forma_id: str | None,
    banco_nome: str,
    banco_id: str | None,
    usuario,
    observacao_desc: str,
) -> dict[str, Any] | None:
    """
    Se houver perfil RH + título de salário na competência, aplica baixa parcial e cria Vale CAIXAS.
    Retorna dict de resultado; None = não aplicável (delegar fluxo antigo).
    """
    empresa = resolver_empresa_por_nome_fantasia(empresa_nome.strip())
    if not empresa:
        return {
            "ok": False,
            "erro": f"Empresa '{empresa_nome}' não encontrada — não foi possível vincular vale ao salário.",
            "ids": [],
        }

    funcionario, _modo = resolver_perfil_rh_para_vale(
        empresa,
        mongo_cliente_id=(pessoa_id or "").strip() or None,
        texto_quem=(pessoa_nome or "").strip() or None,
    )
    if not funcionario:
        return None

    fech = _fechamento_para_data_funcionario(funcionario, data_competencia)
    if not fech or not (fech.mongo_lancamento_salario_id or "").strip():
        return {
            "ok": False,
            "erro": (
                "Para vale no caixa: gere o título de salário (com vencimento) no fechamento RH "
                f"do mês {data_competencia:%m/%Y} para {funcionario.nome_exibicao}. "
                "Os vales passam a ser baixa parcial desse título, sem nova despesa de adiantamento."
            ),
            "ids": [],
        }

    forma_v = f"{(forma_id or '').strip()}|||{(forma_nome or '').strip()}"
    banco_v = f"{(banco_id or '').strip()}|||{(banco_nome or '').strip()}"
    obs = (observacao_desc or "")[:500]
    r = registrar_vale_como_baixa_parcial_salario(
        funcionario=funcionario,
        usuario=usuario,
        data=data_competencia,
        valor=Decimal(str(round(float(valor), 2))),
        observacao=obs,
        forma_value=forma_v,
        banco_value=banco_v,
        tipo_origem=ValeFuncionario.TipoOrigem.CAIXAS,
    )
    mid = (fech.mongo_lancamento_salario_id or "").strip()
    if r.get("ok"):
        return {
            "ok": True,
            "lote": None,
            "ids": [mid],
            "erros": [],
            "parcial_salario": True,
            "quitado": r.get("quitado"),
        }
    return {"ok": False, "erro": r.get("erro", "Falha."), "ids": [], "erros": []}
