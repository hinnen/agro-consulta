"""
Lançamento automático do título de salário no CP.

Regra (por funcionário):
- ``dia_envio_cp_auto`` (1–28): nesse dia o sistema gera/atualiza o título. 0 = off.
- ``dia_vencimento_salario`` (1–28): dia do vencimento no ciclo.
- Conta = placeholder «ADICIONAR BANCO/CONTA» até o pagamento.

Ciclo (exemplos):
- Envio **28**, vencimento **1/7/14** → competência = **mês do envio**;
  vencimento = dia V no **mês seguinte** (ex.: 28/07 → folha 07/2026 venc. 01/08 ou 07/08).
- Envio **1**, vencimento **5** → competência = **mês anterior**;
  vencimento = dia V no **mês do envio** (ex.: 01/08 → folha 07/2026 venc. 05/08).
"""

from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date
from typing import Any

from django.db import transaction
from django.utils import timezone

from rh.models import Funcionario
from rh.services.fechamento import garantir_fechamento_aberto, primeiro_dia_mes, recalcular_fechamento
from rh.services.salario_financeiro_mongo import garantir_titulo_salario_fechamento

logger = logging.getLogger(__name__)


def _clamp_dia(dia: int, ano: int, mes: int) -> int:
    d = int(dia or 0)
    if d < 1:
        d = 1
    ultimo = monthrange(ano, mes)[1]
    return min(max(d, 1), ultimo)


def _add_month(d: date, months: int = 1) -> date:
    y, m = d.year, d.month + months
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    return date(y, m, 1)


def data_vencimento_salario_competencia(competencia: date, dia_vencimento: int) -> date:
    """Salário da competência C vence no dia V do **mês seguinte** a C."""
    nxt = _add_month(primeiro_dia_mes(competencia), 1)
    dv = _clamp_dia(dia_vencimento or 5, nxt.year, nxt.month)
    return date(nxt.year, nxt.month, dv)


def competencia_e_vencimento_para_envio(
    hoje: date,
    *,
    dia_envio: int,
    dia_vencimento: int,
) -> tuple[date, date]:
    """Define competência + vencimento no dia de envio automático."""
    envio = int(dia_envio or 0)
    venc_dia = int(dia_vencimento or 5)
    if venc_dia > envio:
        # Envio cedo (ex. dia 1), vencimento depois no mesmo mês civil → folha do mês anterior.
        if hoje.month == 1:
            comp = date(hoje.year - 1, 12, 1)
        else:
            comp = date(hoje.year, hoje.month - 1, 1)
        dv = _clamp_dia(venc_dia, hoje.year, hoje.month)
        venc = date(hoje.year, hoje.month, dv)
    else:
        # Envio tarde (ex. 28) e vencimento cedo (1/7/14) → folha do mês do envio, vence no mês seguinte.
        comp = primeiro_dia_mes(hoje)
        venc = data_vencimento_salario_competencia(comp, venc_dia)
    return primeiro_dia_mes(comp), venc


def processar_envio_cp_automatico_funcionario(
    funcionario: Funcionario,
    *,
    hoje: date | None = None,
    usuario=None,
    forcar: bool = False,
) -> dict[str, Any]:
    """
    Gera ou atualiza título CP no dia de envio (ou forcar=True).
    Idempotente se o título já existir (ainda assim realinha vencimento).
    """
    hoje = hoje or timezone.localdate()
    dia_envio = int(funcionario.dia_envio_cp_auto or 0)
    if not forcar:
        if dia_envio < 1 or dia_envio > 28:
            return {"ok": True, "skipped": True, "motivo": "auto_desligado"}
        if hoje.day != dia_envio:
            return {"ok": True, "skipped": True, "motivo": "nao_e_dia_envio", "dia": hoje.day}

    if not funcionario.ativo:
        return {"ok": True, "skipped": True, "motivo": "inativo"}

    comp, venc = competencia_e_vencimento_para_envio(
        hoje,
        dia_envio=dia_envio or 28,
        dia_vencimento=int(funcionario.dia_vencimento_salario or 5),
    )

    with transaction.atomic():
        fech = garantir_fechamento_aberto(funcionario, comp)
        fech.data_vencimento_pagamento = venc
        fech.save(update_fields=["data_vencimento_pagamento", "atualizado_em"])
        recalcular_fechamento(fech)
        fech.refresh_from_db()
        r = garantir_titulo_salario_fechamento(fech, usuario=usuario)
        if not r.get("ok"):
            return {
                "ok": False,
                "funcionario_id": funcionario.pk,
                "competencia": comp.isoformat(),
                "erro": r.get("erro") or "Falha ao gerar título",
            }
        from rh.services.salario_financeiro_mongo import sincronizar_valores_titulo_salario_mongo

        if (fech.mongo_lancamento_salario_id or "").strip():
            sincronizar_valores_titulo_salario_mongo(fech)
        return {
            "ok": True,
            "funcionario_id": funcionario.pk,
            "nome": funcionario.nome_exibicao,
            "competencia": comp.isoformat(),
            "vencimento": venc.isoformat(),
            "titulo_id": r.get("id"),
            "criado": bool(r.get("criado")),
        }


def rodar_envio_cp_automatico_diario(*, hoje: date | None = None, dry_run: bool = False) -> dict[str, Any]:
    """Processa todos os ativos com dia_envio_cp_auto == dia de hoje."""
    hoje = hoje or timezone.localdate()
    qs = (
        Funcionario.objects.filter(ativo=True, dia_envio_cp_auto=hoje.day)
        .select_related("cliente_agro", "empresa")
        .order_by("empresa_id", "nome_cache", "pk")
    )
    out: dict[str, Any] = {
        "ok": True,
        "hoje": hoje.isoformat(),
        "candidatos": qs.count(),
        "criados": 0,
        "ja_existiam": 0,
        "erros": [],
        "itens": [],
    }
    if dry_run:
        out["dry_run"] = True
        out["funcionarios"] = list(qs.values_list("pk", "nome_cache", "dia_vencimento_salario"))
        return out

    for fn in qs:
        try:
            r = processar_envio_cp_automatico_funcionario(fn, hoje=hoje, usuario=None, forcar=False)
        except Exception as exc:
            logger.exception("RH envio CP auto funcionario=%s", fn.pk)
            out["erros"].append({"funcionario_id": fn.pk, "erro": str(exc)[:300]})
            continue
        out["itens"].append(r)
        if not r.get("ok"):
            out["erros"].append(r)
        elif r.get("skipped"):
            continue
        elif r.get("criado"):
            out["criados"] += 1
        else:
            out["ja_existiam"] += 1
    out["ok"] = not out["erros"]
    return out
