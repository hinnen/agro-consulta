"""
Pagamentos de salário (não-vale) — CP, caixa ou sync folha.
Vales continuam em ValeFuncionario; estes registros somam no ValorPago do título único.
"""

from __future__ import annotations

import logging
import secrets
from datetime import date
from decimal import Decimal
from typing import Any

from django.contrib.auth.models import AnonymousUser
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from rh.constants import (
    REF_TIPO_PG_BAIXA_CP,
    REF_TIPO_RH_PAGAMENTO_SALARIO_PARCIAL,
)
from rh.models import FechamentoFolhaSimplificado, PagamentoSalarioFuncionario
from rh.services.fechamento import money_two_decimals, recalcular_fechamento
from rh.utils import resolver_empresa_por_nome_fantasia, resolver_perfil_rh_para_vale

logger = logging.getLogger(__name__)


def total_pagamentos_salario_mes(funcionario, ano: int, mes: int) -> Decimal:
    ini = date(ano, mes, 1)
    from calendar import monthrange

    fim = date(ano, mes, monthrange(ano, mes)[1])
    q = PagamentoSalarioFuncionario.objects.filter(
        funcionario=funcionario,
        data__gte=ini,
        data__lte=fim,
        cancelado=False,
    ).aggregate(t=Sum("valor"))
    return money_two_decimals(q["t"])


def total_pagamentos_salario_fechamento(fechamento: FechamentoFolhaSimplificado) -> Decimal:
    """Soma pagamentos de salário da competência (FK fechamento).

    Fallback só para órfãos do mês (sem FK) — não puxa baixas de outra folha
    só porque a data caiu neste calendário (ex.: salário jun/2026 pago em jul).
    """
    q = PagamentoSalarioFuncionario.objects.filter(
        fechamento=fechamento,
        cancelado=False,
    ).aggregate(t=Sum("valor"))
    total = money_two_decimals(q["t"])
    if total > Decimal("0"):
        return total
    comp = fechamento.competencia
    ini = date(comp.year, comp.month, 1)
    from calendar import monthrange

    fim = date(comp.year, comp.month, monthrange(comp.year, comp.month)[1])
    q2 = PagamentoSalarioFuncionario.objects.filter(
        funcionario=fechamento.funcionario,
        fechamento__isnull=True,
        data__gte=ini,
        data__lte=fim,
        cancelado=False,
    ).aggregate(t=Sum("valor"))
    return money_two_decimals(q2["t"])


def fechamento_por_titulo_mongo_id(mongo_id: str) -> FechamentoFolhaSimplificado | None:
    mid = (mongo_id or "").strip()
    if not mid:
        return None
    return (
        FechamentoFolhaSimplificado.objects.filter(mongo_lancamento_salario_id=mid)
        .select_related("funcionario", "funcionario__cliente_agro", "funcionario__empresa")
        .first()
    )


def _usuario_user(usuario_label: str, usuario=None):
    if usuario is not None and not isinstance(usuario, AnonymousUser):
        if getattr(usuario, "is_authenticated", False):
            return usuario
    return None


@transaction.atomic
def registrar_pagamento_salario_rh(
    *,
    fechamento: FechamentoFolhaSimplificado,
    valor: Decimal,
    data: date,
    tipo_origem: str,
    observacao: str = "",
    referencia_externa_tipo: str = "",
    referencia_externa_id: str = "",
    usuario=None,
) -> PagamentoSalarioFuncionario:
    valor = money_two_decimals(valor)
    ref_t = (referencia_externa_tipo or "").strip()
    ref_i = (referencia_externa_id or "").strip()
    if ref_t and ref_i:
        existente = PagamentoSalarioFuncionario.objects.filter(
            referencia_externa_tipo=ref_t,
            referencia_externa_id=ref_i,
        ).first()
        if existente:
            if existente.cancelado:
                existente.cancelado = False
                existente.cancelado_em = None
                existente.motivo_cancelamento = ""
                existente.valor = valor
                existente.data = data
                existente.save()
            return existente

    fn = fechamento.funcionario
    p = PagamentoSalarioFuncionario.objects.create(
        funcionario=fn,
        empresa=fn.empresa,
        loja=fn.loja,
        fechamento=fechamento,
        data=data,
        valor=valor,
        tipo_origem=tipo_origem,
        observacao=(observacao or "")[:500],
        referencia_externa_tipo=ref_t[:80],
        referencia_externa_id=ref_i[:64],
        criado_por=_usuario_user("", usuario),
    )
    _atualizar_controle_fechamento_apos_pagamentos(fechamento)
    recalcular_fechamento(fechamento)
    return p


def _atualizar_controle_fechamento_apos_pagamentos(
    f: FechamentoFolhaSimplificado,
    *,
    atualizar_status: bool = True,
) -> None:
    comp = f.competencia
    pagos = total_pagamentos_salario_fechamento(f)
    f.valor_pago = pagos
    update = ["valor_pago", "atualizado_em"]
    if atualizar_status:
        if pagos <= Decimal("0"):
            pass
        elif pagos + Decimal("0.02") >= f.valor_liquido_previsto:
            if f.status == FechamentoFolhaSimplificado.Status.ABERTO:
                f.status = FechamentoFolhaSimplificado.Status.PAGO_PARCIAL
                update.append("status")
        elif pagos > Decimal("0") and f.status == FechamentoFolhaSimplificado.Status.ABERTO:
            f.status = FechamentoFolhaSimplificado.Status.PAGO_PARCIAL
            update.append("status")
    f.save(update_fields=update)


def restaurar_valor_pago_controle_fechamento(f: FechamentoFolhaSimplificado) -> None:
    """Recalcula valor pago (controle) a partir dos pagamentos de salário registrados no RH."""
    _atualizar_controle_fechamento_apos_pagamentos(f, atualizar_status=False)


@transaction.atomic
def cancelar_pagamento_salario(
    pagamento: PagamentoSalarioFuncionario | int,
    *,
    motivo: str = "",
    sincronizar_cp: bool = True,
) -> dict[str, Any]:
    """Cancela pagamento de salário (não-vale) e realinha folha + título CP."""
    if isinstance(pagamento, int):
        p = PagamentoSalarioFuncionario.objects.select_related(
            "fechamento", "fechamento__funcionario", "funcionario"
        ).get(pk=pagamento)
    else:
        p = pagamento
    if p.cancelado:
        return {"ok": True, "skipped": True, "motivo": "ja_cancelado"}

    motivo_limpo = (motivo or "Cancelamento manual").strip()
    if len(motivo_limpo) < 3:
        return {"ok": False, "erro": "Informe motivo com pelo menos 3 caracteres."}

    p.cancelado = True
    p.cancelado_em = timezone.now()
    p.motivo_cancelamento = motivo_limpo[:400]
    p.save(update_fields=["cancelado", "cancelado_em", "motivo_cancelamento", "atualizado_em"])

    fech = p.fechamento
    if fech is None:
        fech = (
            FechamentoFolhaSimplificado.objects.filter(
                funcionario=p.funcionario,
                competencia__year=p.data.year,
                competencia__month=p.data.month,
            )
            .order_by("-competencia")
            .first()
        )

    sync = None
    if fech:
        _atualizar_controle_fechamento_apos_pagamentos(fech)
        recalcular_fechamento(fech)
        if sincronizar_cp and (fech.mongo_lancamento_salario_id or "").strip():
            from rh.services.salario_financeiro_mongo import sincronizar_valores_titulo_salario_mongo

            sync = sincronizar_valores_titulo_salario_mongo(fech)

    return {"ok": True, "pagamento_id": p.pk, "fechamento_id": fech.pk if fech else None, "sync": sync}


def processar_baixa_cp_titulo_salario(
    *,
    mongo_id: str,
    valor_baixa: Decimal | float,
    data: date,
    tipo_origem: str,
    observacao: str = "",
    referencia_externa_id: str = "",
    usuario=None,
) -> dict[str, Any]:
    """Após baixa no CP de título vinculado à folha RH."""
    fech = fechamento_por_titulo_mongo_id(mongo_id)
    if not fech:
        return {"ok": True, "skipped": True, "motivo": "nao_e_titulo_folha_rh"}

    val = money_two_decimals(valor_baixa)
    if val <= Decimal("0"):
        return {"ok": True, "skipped": True, "motivo": "valor_zero"}

    ref_id = (referencia_externa_id or "").strip() or f"{mongo_id}:{secrets.token_hex(8)}"
    registrar_pagamento_salario_rh(
        fechamento=fech,
        valor=val,
        data=data,
        tipo_origem=tipo_origem,
        observacao=observacao,
        referencia_externa_tipo=REF_TIPO_PG_BAIXA_CP,
        referencia_externa_id=ref_id[:64],
        usuario=usuario,
    )

    from rh.services.salario_financeiro_mongo import sincronizar_valores_titulo_salario_mongo

    fech.refresh_from_db()
    sr = sincronizar_valores_titulo_salario_mongo(fech)
    return {"ok": True, "fechamento_id": fech.pk, "sync": sr}


def valor_pago_titulo_salario(
    fechamento: FechamentoFolhaSimplificado,
    bruto: Decimal,
    *,
    db=None,
    mongo_id: str | None = None,
    respeitar_mongo_maior: bool = False,
) -> Decimal:
    """Vales + pagamentos salário ativos no fechamento."""
    comp = fechamento.competencia
    fn = fechamento.funcionario
    from rh.services.fechamento import total_vales_mes

    tv = total_vales_mes(fn, comp.year, comp.month)
    ps = total_pagamentos_salario_fechamento(fechamento)
    vp = money_two_decimals(tv + ps)
    if vp > bruto:
        vp = bruto

    if not respeitar_mongo_maior:
        return vp

    mid = (mongo_id or fechamento.mongo_lancamento_salario_id or "").strip()
    if db is not None and mid:
        try:
            from bson import ObjectId

            from produtos.mongo_financeiro_util import COL_DTO_LANCAMENTO

            doc = db[COL_DTO_LANCAMENTO].find_one({"_id": ObjectId(mid)})
            if doc:
                atual = money_two_decimals(doc.get("ValorPago"))
                if atual > vp:
                    vp = min(atual, bruto)
        except Exception:
            logger.debug("valor_pago_titulo_salario: leitura Mongo legado falhou", exc_info=True)
    return vp


def tentar_caixa_pagamento_salario(
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
    Saída caixa plano Salários: baixa parcial/total no título + registro RH (sem vale).
    None = não aplicável (delegar fluxo genérico).
    """
    from rh.services.salario_financeiro_mongo import registrar_pagamento_salario_com_baixa_titulo

    empresa = resolver_empresa_por_nome_fantasia(empresa_nome.strip())
    if not empresa:
        return {
            "ok": False,
            "erro": f"Empresa '{empresa_nome}' não encontrada.",
            "ids": [],
        }

    funcionario, _modo = resolver_perfil_rh_para_vale(
        empresa,
        mongo_cliente_id=(pessoa_id or "").strip() or None,
        texto_quem=(pessoa_nome or "").strip() or None,
    )
    if not funcionario:
        return None

    forma_v = f"{(forma_id or '').strip()}|||{(forma_nome or '').strip()}"
    banco_v = f"{(banco_id or '').strip()}|||{(banco_nome or '').strip()}"
    return registrar_pagamento_salario_com_baixa_titulo(
        funcionario=funcionario,
        usuario=usuario,
        data=data_competencia,
        valor=Decimal(str(round(float(valor), 2))),
        observacao=(observacao_desc or "")[:500],
        forma_value=forma_v,
        banco_value=banco_v,
    )
