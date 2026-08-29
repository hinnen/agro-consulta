"""Repasse Vila Elias → Centro: CMV + % lucro bruto + fiado pago na Vila."""
from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from produtos.models import (
    FiadoBaixaAgro,
    ItemVendaAgro,
    MovimentoCaixa,
    RepasseVilaAcumuladoAjusteAgro,
    RepasseVilaCentroAgro,
    RepasseVilaConfigAgro,
    RepasseVilaDeltaDiaAgro,
    RepasseVilaReservaLogAgro,
    RepasseVilaReservaMovimentoAgro,
    VendaAgro,
)

ZERO = Decimal("0.00")
# Campo reserva_vila criado em 18/08/2026 — diário a partir desta data.
RESERVA_VILA_DESDE_DEFAULT = date(2026, 8, 18)
# Cofre Vila Elias (fatia do lucro que fica) — vigência a partir do pacote dois-cofrinhos.
COFRE_VILA_ELIAS_DESDE = date(2026, 8, 29)
# Limite para transferência de dia atrasado (esqueci ontem / semana).
REPASSE_MAX_DIAS_ATRASO = 180

COFRE_SALARIO = "salario"
COFRE_VILA_ELIAS = "vila_elias"
FORMAS_ELETRONICAS_REPASSE = frozenset(
    {
        "PIX",
        "Cartão de débito",
        "Cartão de crédito",
        "Cartão de crédito parcelado",
    }
)


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.01"))
    except Exception:
        return ZERO


def validar_data_ref_repasse(dia: date | None) -> tuple[date | None, str]:
    """Aceita hoje ou dia passado (até REPASSE_MAX_DIAS_ATRASO). Bloqueia futuro."""
    hoje = timezone.localdate()
    d = dia or hoje
    if d > hoje:
        return None, "Não dá para transferir dia futuro."
    atraso = (hoje - d).days
    if atraso > REPASSE_MAX_DIAS_ATRASO:
        return None, f"Data muito antiga (máximo {REPASSE_MAX_DIAS_ATRASO} dias)."
    return d, ""


def _aware_bounds(d0: date, d1: date) -> tuple[datetime, datetime]:
    desde = datetime.combine(d0, time.min)
    ate = datetime.combine(d1, time(23, 59, 59))
    if timezone.is_naive(desde):
        desde = timezone.make_aware(desde)
    if timezone.is_naive(ate):
        ate = timezone.make_aware(ate)
    return desde, ate


def obter_config() -> RepasseVilaConfigAgro:
    cfg = RepasseVilaConfigAgro.objects.order_by("pk").first()
    if cfg:
        return cfg
    return RepasseVilaConfigAgro.objects.create(percentual_lucro_padrao=Decimal("50.00"))


def salvar_percentual_padrao(pct, *, operador: str = "") -> RepasseVilaConfigAgro:
    p = _dec(pct)
    if p < 0:
        p = ZERO
    if p > 100:
        p = Decimal("100.00")
    cfg = obter_config()
    cfg.percentual_lucro_padrao = p
    cfg.atualizado_por = (operador or "")[:120]
    cfg.save(update_fields=["percentual_lucro_padrao", "atualizado_em", "atualizado_por"])
    return cfg


def salvar_reserva_vila(valor, *, operador: str = "") -> RepasseVilaConfigAgro:
    """Valor manual diário: desconta do lucro bruto antes do % ao Centro."""
    v = _dec(valor)
    if v < 0:
        v = ZERO
    if v > Decimal("99999.99"):
        v = Decimal("99999.99")
    cfg = obter_config()
    antes = _dec(cfg.reserva_vila)
    fields = ["reserva_vila", "atualizado_em", "atualizado_por"]
    cfg.reserva_vila = v
    cfg.atualizado_por = (operador or "")[:120]
    if getattr(cfg, "reserva_vila_desde", None) is None:
        cfg.reserva_vila_desde = RESERVA_VILA_DESDE_DEFAULT
        fields.append("reserva_vila_desde")
        _registrar_log_reserva(
            tipo=RepasseVilaReservaLogAgro.Tipo.DESDE,
            operador=operador,
            data_ref=cfg.reserva_vila_desde,
            valor_antes=antes,
            valor_depois=v,
            mensagem=(
                f"Data início diário definida: {cfg.reserva_vila_desde.strftime('%d/%m/%Y')}."
            ),
            detalhe={"reserva_vila_desde": cfg.reserva_vila_desde.isoformat()},
        )
    cfg.save(update_fields=fields)
    if antes != v:
        _registrar_log_reserva(
            tipo=RepasseVilaReservaLogAgro.Tipo.CONFIG,
            operador=operador,
            valor_antes=antes,
            valor_depois=v,
            mensagem=(
                f"Valor manual alterado de R$ {antes} para R$ {v} "
                f"(desconta do lucro antes do % · diário desde "
                f"{(cfg.reserva_vila_desde or RESERVA_VILA_DESDE_DEFAULT).strftime('%d/%m/%Y')})."
            ),
            detalhe={
                "reserva_vila_antes": float(antes),
                "reserva_vila_depois": float(v),
                "reserva_vila_desde": (
                    (cfg.reserva_vila_desde or RESERVA_VILA_DESDE_DEFAULT).isoformat()
                ),
                "regra": "lucro_bruto - reserva → depois aplica % ao Centro",
            },
        )
    return cfg


def reserva_vila_config(cfg: RepasseVilaConfigAgro | None = None) -> Decimal:
    cfg = cfg or obter_config()
    return max(ZERO, _dec(getattr(cfg, "reserva_vila", ZERO)))


def reserva_vila_desde_config(cfg: RepasseVilaConfigAgro | None = None) -> date:
    cfg = cfg or obter_config()
    d = getattr(cfg, "reserva_vila_desde", None)
    return d or RESERVA_VILA_DESDE_DEFAULT


def reserva_aplicada_no_dia(
    dia: date,
    cfg: RepasseVilaConfigAgro | None = None,
    *,
    lucro_bruto: Decimal | None = None,
) -> Decimal:
    """Valor manual do dia: 0 se antes da data início; limitado ao lucro bruto."""
    cfg = cfg or obter_config()
    if dia < reserva_vila_desde_config(cfg):
        return ZERO
    v = reserva_vila_config(cfg)
    if lucro_bruto is not None:
        v = min(v, max(ZERO, _dec(lucro_bruto)))
    return v.quantize(Decimal("0.01"))


def _registrar_log_reserva(
    *,
    tipo: str,
    operador: str = "",
    data_ref: date | None = None,
    valor_antes=ZERO,
    valor_depois=ZERO,
    mensagem: str = "",
    detalhe: dict | None = None,
    repasse: RepasseVilaCentroAgro | None = None,
) -> RepasseVilaReservaLogAgro:
    return RepasseVilaReservaLogAgro.objects.create(
        tipo=tipo,
        operador=(operador or "")[:120],
        data_ref=data_ref,
        valor_antes=_dec(valor_antes),
        valor_depois=_dec(valor_depois),
        mensagem=(mensagem or "")[:500],
        detalhe=detalhe if isinstance(detalhe, dict) else {},
        repasse=repasse,
    )


def listar_log_reserva(*, limit: int = 80) -> list[dict[str, Any]]:
    lim = max(1, min(int(limit or 80), 200))
    out: list[dict[str, Any]] = []
    for row in RepasseVilaReservaLogAgro.objects.order_by("-criado_em", "-pk")[:lim]:
        out.append(
            {
                "id": row.pk,
                "tipo": row.tipo,
                "tipo_label": row.get_tipo_display(),
                "criado_em": (
                    timezone.localtime(row.criado_em).strftime("%d/%m/%Y %H:%M:%S")
                    if row.criado_em
                    else ""
                ),
                "operador": row.operador or "",
                "data_ref": row.data_ref.isoformat() if row.data_ref else "",
                "valor_antes": float(_dec(row.valor_antes)),
                "valor_depois": float(_dec(row.valor_depois)),
                "mensagem": row.mensagem or "",
                "detalhe": row.detalhe if isinstance(row.detalhe, dict) else {},
                "repasse_id": row.repasse_id,
            }
        )
    return out


def _norm_cofre(cofre: str | None) -> str:
    c = str(cofre or COFRE_SALARIO).strip().lower()
    if c in (COFRE_VILA_ELIAS, "vila", "elias"):
        return COFRE_VILA_ELIAS
    return COFRE_SALARIO


def saldo_cofrinho_vila(cfg: RepasseVilaConfigAgro | None = None, *, cofre: str = COFRE_SALARIO) -> Decimal:
    cfg = cfg or obter_config()
    if _norm_cofre(cofre) == COFRE_VILA_ELIAS:
        return max(ZERO, _dec(getattr(cfg, "saldo_cofre_vila_elias", ZERO)))
    return max(ZERO, _dec(getattr(cfg, "saldo_reserva_vila", ZERO)))


def _inicio_janela_cofre(dia: date, cofre: str, cfg: RepasseVilaConfigAgro | None = None) -> date:
    cfg = cfg or obter_config()
    if _norm_cofre(cofre) == COFRE_VILA_ELIAS:
        desde = COFRE_VILA_ELIAS_DESDE
    else:
        desde = reserva_vila_desde_config(cfg)
    piso = dia - timedelta(days=REPASSE_MAX_DIAS_ATRASO)
    return max(desde, piso)


def _inicio_janela_reserva(dia: date, cfg: RepasseVilaConfigAgro | None = None) -> date:
    return _inicio_janela_cofre(dia, COFRE_SALARIO, cfg)


def _obrigacao_dia_cofre(calc: dict[str, Any], cofre: str) -> Decimal:
    if _norm_cofre(cofre) == COFRE_VILA_ELIAS:
        return _dec(calc.get("parte_vila_elias"))
    return _dec(calc.get("parte_salario", calc.get("reserva_aplicada")))


def obrigacao_reserva_cofrinho_ate(
    dia: date,
    cfg: RepasseVilaConfigAgro | None = None,
    *,
    cofre: str = COFRE_SALARIO,
) -> Decimal:
    """Soma das obrigações diárias do cofre desde a vigência até `dia`."""
    cfg = cfg or obter_config()
    cofre_n = _norm_cofre(cofre)
    inicio = _inicio_janela_cofre(dia, cofre_n, cfg)
    if dia < inicio:
        return ZERO
    total = ZERO
    cursor = inicio
    while cursor <= dia:
        calc = calcular_disponivel(cursor, _skip_acumulado=True)
        total += _obrigacao_dia_cofre(calc, cofre_n)
        cursor += timedelta(days=1)
    return total.quantize(Decimal("0.01"))


def credito_reserva_cofrinho_ate(
    dia: date,
    cfg: RepasseVilaConfigAgro | None = None,
    *,
    cofre: str = COFRE_SALARIO,
) -> Decimal:
    """
    Crédito que cobre a obrigação: separações líquidas + saldo inicial.
    Separar a mais hoje abate os próximos dias; saldo inicial também conta.
    """
    cfg = cfg or obter_config()
    cofre_n = _norm_cofre(cofre)
    inicio = _inicio_janela_cofre(dia, cofre_n, cfg)
    total = ZERO
    qs = (
        RepasseVilaReservaMovimentoAgro.objects.filter(
            data_ref__gte=inicio, data_ref__lte=dia, cofre=cofre_n
        )
        .select_related("estornado_de")
        .order_by("criado_em", "pk")
    )
    for mov in qs:
        if mov.tipo == RepasseVilaReservaMovimentoAgro.Tipo.SEPARACAO:
            total += _dec(mov.valor)
        elif (
            mov.tipo == RepasseVilaReservaMovimentoAgro.Tipo.ESTORNO
            and mov.estornado_de_id
            and mov.estornado_de.tipo == RepasseVilaReservaMovimentoAgro.Tipo.SEPARACAO
        ):
            total += _dec(mov.valor)
        elif (
            mov.tipo == RepasseVilaReservaMovimentoAgro.Tipo.AJUSTE
            and mov.origem == RepasseVilaReservaMovimentoAgro.Origem.SALDO_INICIAL
            and _dec(mov.valor) > 0
        ):
            total += _dec(mov.valor)
        elif (
            mov.tipo == RepasseVilaReservaMovimentoAgro.Tipo.ESTORNO
            and mov.estornado_de_id
            and mov.estornado_de.tipo == RepasseVilaReservaMovimentoAgro.Tipo.AJUSTE
            and mov.estornado_de.origem
            == RepasseVilaReservaMovimentoAgro.Origem.SALDO_INICIAL
        ):
            total += _dec(mov.valor)
    return total.quantize(Decimal("0.01"))


def pendente_reserva_cofrinho_ate(
    dia: date,
    cfg: RepasseVilaConfigAgro | None = None,
    *,
    cofre: str = COFRE_SALARIO,
) -> dict[str, Decimal]:
    """Obrigação acumulada − crédito (separações + saldo inicial)."""
    cfg = cfg or obter_config()
    cofre_n = _norm_cofre(cofre)
    obrigacao = obrigacao_reserva_cofrinho_ate(dia, cfg, cofre=cofre_n)
    credito = credito_reserva_cofrinho_ate(dia, cfg, cofre=cofre_n)
    pendente = max(ZERO, (obrigacao - credito).quantize(Decimal("0.01")))
    adiantado = max(ZERO, (credito - obrigacao).quantize(Decimal("0.01")))
    return {
        "obrigacao": obrigacao,
        "credito": credito,
        "pendente": pendente,
        "adiantado": adiantado,
    }


def separacao_realizada_no_dia(dia: date, *, cofre: str = COFRE_SALARIO) -> Decimal:
    """Total líquido separado no dia (inclui eventual estorno da separação)."""
    cofre_n = _norm_cofre(cofre)
    total = ZERO
    qs = RepasseVilaReservaMovimentoAgro.objects.filter(
        data_ref=dia, cofre=cofre_n
    ).select_related("estornado_de")
    for mov in qs:
        if mov.tipo == RepasseVilaReservaMovimentoAgro.Tipo.SEPARACAO:
            total += _dec(mov.valor)
        elif (
            mov.tipo == RepasseVilaReservaMovimentoAgro.Tipo.ESTORNO
            and mov.estornado_de_id
            and mov.estornado_de.tipo == RepasseVilaReservaMovimentoAgro.Tipo.SEPARACAO
        ):
            total += _dec(mov.valor)
    return max(ZERO, total.quantize(Decimal("0.01")))


def resumo_cofrinho_vila(
    dia: date | None = None,
    *,
    limit: int = 60,
    cofre: str = COFRE_SALARIO,
) -> dict[str, Any]:
    dia = dia or timezone.localdate()
    cfg = obter_config()
    cofre_n = _norm_cofre(cofre)
    calc = calcular_disponivel(dia, _skip_acumulado=True)
    prevista = _obrigacao_dia_cofre(calc, cofre_n)
    realizada = separacao_realizada_no_dia(dia, cofre=cofre_n)
    acum = pendente_reserva_cofrinho_ate(dia, cfg, cofre=cofre_n)
    movimentos = []
    qs = (
        RepasseVilaReservaMovimentoAgro.objects.filter(cofre=cofre_n)
        .select_related("sessao_caixa", "repasse", "estornado_de")
        .order_by("-criado_em", "-pk")[: max(1, min(int(limit or 60), 200))]
    )
    for mov in qs:
        movimentos.append(
            {
                "id": mov.pk,
                "cofre": mov.cofre,
                "tipo": mov.tipo,
                "tipo_label": mov.get_tipo_display(),
                "origem": mov.origem,
                "origem_label": mov.get_origem_display(),
                "criado_em": timezone.localtime(mov.criado_em).strftime("%d/%m/%Y %H:%M:%S"),
                "data_ref": mov.data_ref.isoformat(),
                "valor": float(_dec(mov.valor)),
                "saldo_anterior": float(_dec(mov.saldo_anterior)),
                "saldo_posterior": float(_dec(mov.saldo_posterior)),
                "operador": mov.operador,
                "observacao": mov.observacao,
                "sessao_caixa_id": mov.sessao_caixa_id,
                "repasse_id": mov.repasse_id,
                "movimento_caixa_id": mov.movimento_caixa_id,
                "estornado_de_id": mov.estornado_de_id,
                "estornado": RepasseVilaReservaMovimentoAgro.objects.filter(
                    estornado_de_id=mov.pk
                ).exists(),
            }
        )
    desde = (
        COFRE_VILA_ELIAS_DESDE
        if cofre_n == COFRE_VILA_ELIAS
        else reserva_vila_desde_config(cfg)
    )
    return {
        "ok": True,
        "cofre": cofre_n,
        "nome": (
            "Cofre Vila Elias"
            if cofre_n == COFRE_VILA_ELIAS
            else "Cofrinho Salário funcionário"
        ),
        "data_ref": dia.isoformat(),
        "saldo": float(saldo_cofrinho_vila(cfg, cofre=cofre_n)),
        "prevista_dia": float(prevista),
        "realizada_dia": float(realizada),
        "pendente_dia": float(acum["pendente"]),
        "obrigacao_acumulada": float(acum["obrigacao"]),
        "credito_acumulado": float(acum["credito"]),
        "adiantado": float(acum["adiantado"]),
        "reserva_vila_desde": desde.isoformat(),
        "movimentos": movimentos,
    }


def resumo_cofre_vila_elias(dia: date | None = None, *, limit: int = 60) -> dict[str, Any]:
    return resumo_cofrinho_vila(dia, limit=limit, cofre=COFRE_VILA_ELIAS)


@transaction.atomic
def _registrar_movimento_cofrinho(
    *,
    tipo: str,
    origem: str,
    valor,
    data_ref: date,
    operador: str,
    observacao: str = "",
    idempotencia_chave: str,
    usuario=None,
    sessao_caixa=None,
    movimento_caixa=None,
    repasse=None,
    estornado_de=None,
    detalhe: dict | None = None,
    cofre: str = COFRE_SALARIO,
) -> tuple[RepasseVilaReservaMovimentoAgro | None, bool, str]:
    op = str(operador or "").strip()
    if not op:
        return None, False, "Informe o operador."
    obs = str(observacao or "").strip()
    if tipo != RepasseVilaReservaMovimentoAgro.Tipo.SEPARACAO and len(obs) < 3:
        return None, False, "Informe o motivo/observação (mínimo 3 caracteres)."
    chave = str(idempotencia_chave or "").strip()[:160]
    cofre_n = _norm_cofre(cofre)
    cfg_base = obter_config()
    cfg = RepasseVilaConfigAgro.objects.select_for_update().get(pk=cfg_base.pk)
    existente = RepasseVilaReservaMovimentoAgro.objects.filter(
        idempotencia_chave=chave
    ).first()
    if existente:
        return existente, False, ""
    delta = _dec(valor)
    antes = saldo_cofrinho_vila(cfg, cofre=cofre_n)
    depois = (antes + delta).quantize(Decimal("0.01"))
    if delta == 0:
        return None, False, "O valor precisa ser diferente de zero."
    if depois < 0:
        nome = "Cofre Vila Elias" if cofre_n == COFRE_VILA_ELIAS else "Cofrinho Salário"
        return None, False, f"Saldo insuficiente no {nome}."
    mov = RepasseVilaReservaMovimentoAgro.objects.create(
        tipo=tipo,
        origem=origem,
        cofre=cofre_n,
        data_ref=data_ref,
        valor=delta,
        saldo_anterior=antes,
        saldo_posterior=depois,
        operador=op[:120],
        usuario=usuario,
        observacao=obs[:500],
        idempotencia_chave=chave,
        sessao_caixa=sessao_caixa,
        movimento_caixa=movimento_caixa,
        repasse=repasse,
        estornado_de=estornado_de,
        detalhe=detalhe if isinstance(detalhe, dict) else {},
    )
    if cofre_n == COFRE_VILA_ELIAS:
        cfg.saldo_cofre_vila_elias = depois
        saldo_field = "saldo_cofre_vila_elias"
    else:
        cfg.saldo_reserva_vila = depois
        saldo_field = "saldo_reserva_vila"
    cfg.atualizado_por = op[:120]
    cfg.save(update_fields=[saldo_field, "atualizado_em", "atualizado_por"])
    return mov, True, ""


@transaction.atomic
def separar_reserva_diaria(
    dia: date,
    *,
    origem: str,
    operador: str,
    usuario=None,
    sessao_caixa=None,
    repasse=None,
    observacao: str = "",
    cofre: str = COFRE_SALARIO,
) -> tuple[RepasseVilaReservaMovimentoAgro | None, bool, str]:
    """Separa o pendente acumulado (dias anteriores + hoje) da gaveta para o cofre."""
    cofre_n = _norm_cofre(cofre)
    cfg_base = obter_config()
    RepasseVilaConfigAgro.objects.select_for_update().get(pk=cfg_base.pk)
    desde = (
        COFRE_VILA_ELIAS_DESDE
        if cofre_n == COFRE_VILA_ELIAS
        else reserva_vila_desde_config()
    )
    nome = "Cofre Vila Elias" if cofre_n == COFRE_VILA_ELIAS else "Cofrinho Salário"
    if dia < desde:
        # Vila Elias: dias anteriores ao deploy = sem obrigação (skip, não erro no repasse).
        if cofre_n == COFRE_VILA_ELIAS:
            return None, False, ""
        return None, False, f"{nome} ainda não estava vigente nessa data."
    if sessao_caixa is None or getattr(sessao_caixa, "fechado_em", None):
        return None, False, "Abra o caixa da Vila para separar o dinheiro."
    if getattr(sessao_caixa, "ponto_caixa", "") != "vila":
        return None, False, "A separação precisa usar o caixa principal da Vila Elias."
    calc = calcular_disponivel(dia, _skip_acumulado=True)
    prevista = _obrigacao_dia_cofre(calc, cofre_n)
    realizada = separacao_realizada_no_dia(dia, cofre=cofre_n)
    acum = pendente_reserva_cofrinho_ate(dia, cofre=cofre_n)
    falta = acum["pendente"]
    if falta <= 0:
        existente = (
            RepasseVilaReservaMovimentoAgro.objects.filter(
                data_ref=dia,
                tipo=RepasseVilaReservaMovimentoAgro.Tipo.SEPARACAO,
                cofre=cofre_n,
            )
            .order_by("-criado_em", "-pk")
            .first()
        )
        return existente, False, ""
    from produtos.caixa_util import resumo_esperado_por_forma

    dinheiro = max(
        ZERO,
        _dec(resumo_esperado_por_forma(sessao_caixa).get("Dinheiro")),
    )
    separar = min(falta, dinheiro)
    if separar <= 0:
        return None, False, f"Não há dinheiro suficiente na gaveta para separar o {nome}."
    rotulo = "cofre Vila Elias" if cofre_n == COFRE_VILA_ELIAS else "cofrinho Salário"
    mov_caixa = MovimentoCaixa.objects.create(
        sessao_caixa=sessao_caixa,
        tipo=MovimentoCaixa.Tipo.RETIRADA,
        forma_pagamento="Dinheiro",
        valor=separar,
        observacao=(
            f"Reserva {rotulo} · ref {dia.strftime('%d/%m/%Y')} · permanece na loja"
        )[:500],
        usuario=usuario,
    )
    alvo_apos = (realizada + separar).quantize(Decimal("0.01"))
    chave = f"reserva-vila:{cofre_n}:separacao:{dia.isoformat()}:{int(alvo_apos * 100)}"
    mov, criado, err = _registrar_movimento_cofrinho(
        tipo=RepasseVilaReservaMovimentoAgro.Tipo.SEPARACAO,
        origem=origem,
        valor=separar,
        data_ref=dia,
        operador=operador,
        observacao=observacao or f"Separação diária · {nome}",
        idempotencia_chave=chave,
        usuario=usuario,
        sessao_caixa=sessao_caixa,
        movimento_caixa=mov_caixa,
        repasse=repasse,
        cofre=cofre_n,
        detalhe={
            "cofre": cofre_n,
            "prevista_dia": float(prevista),
            "realizada_antes": float(realizada),
            "realizada_depois": float(alvo_apos),
            "obrigacao_acumulada": float(acum["obrigacao"]),
            "credito_antes": float(acum["credito"]),
            "pendente_antes": float(falta),
            "lucro_bruto_dia": calc.get("lucro_bruto_dia"),
            "reserva_vila_desde": calc.get("reserva_vila_desde"),
        },
    )
    if err or not criado:
        mov_caixa.delete()
    return mov, criado, err


def registrar_uso_ou_ajuste_cofrinho(
    *,
    tipo: str,
    valor,
    observacao: str,
    operador: str,
    usuario=None,
    data_ref: date | None = None,
    idempotencia_chave: str = "",
    origem: str | None = None,
    cofre: str = COFRE_SALARIO,
) -> tuple[RepasseVilaReservaMovimentoAgro | None, bool, str]:
    cofre_n = _norm_cofre(cofre)
    v = _dec(valor)
    if tipo == RepasseVilaReservaMovimentoAgro.Tipo.RETIRADA:
        v = -abs(v)
    elif tipo != RepasseVilaReservaMovimentoAgro.Tipo.AJUSTE:
        return None, False, "Tipo de movimento inválido."
    origem_mov = origem or RepasseVilaReservaMovimentoAgro.Origem.AJUSTE
    if origem_mov not in {
        RepasseVilaReservaMovimentoAgro.Origem.AJUSTE,
        RepasseVilaReservaMovimentoAgro.Origem.SALDO_INICIAL,
    }:
        return None, False, "Origem de movimento inválida."
    if origem_mov == RepasseVilaReservaMovimentoAgro.Origem.SALDO_INICIAL and v <= 0:
        return None, False, "Saldo inicial precisa ser um valor positivo."
    chave = idempotencia_chave or (
        f"reserva-vila:{cofre_n}:{tipo}:{timezone.now().strftime('%Y%m%d%H%M%S%f')}"
    )
    mov, criado, err = _registrar_movimento_cofrinho(
        tipo=tipo,
        origem=origem_mov,
        valor=v,
        data_ref=data_ref or timezone.localdate(),
        operador=operador,
        observacao=observacao,
        idempotencia_chave=chave,
        usuario=usuario,
        cofre=cofre_n,
        detalhe={
            "saldo_inicial": origem_mov == RepasseVilaReservaMovimentoAgro.Origem.SALDO_INICIAL,
            "cofre": cofre_n,
        },
    )
    return mov, criado, err


def registrar_saldo_inicial_cofrinho(
    *,
    valor,
    observacao: str,
    operador: str,
    usuario=None,
    idempotencia_chave: str = "",
    cofre: str = COFRE_SALARIO,
) -> tuple[RepasseVilaReservaMovimentoAgro | None, bool, str]:
    """Uma vez: sobe o saldo físico e já conta como crédito da obrigação acumulada."""
    cofre_n = _norm_cofre(cofre)
    nome = "Cofre Vila Elias" if cofre_n == COFRE_VILA_ELIAS else "cofrinho Salário"
    obs = str(observacao or "").strip() or f"Saldo inicial do {nome} (já separado fisicamente)"
    return registrar_uso_ou_ajuste_cofrinho(
        tipo=RepasseVilaReservaMovimentoAgro.Tipo.AJUSTE,
        valor=valor,
        observacao=obs,
        operador=operador,
        usuario=usuario,
        data_ref=timezone.localdate(),
        idempotencia_chave=idempotencia_chave
        or f"reserva-vila:{cofre_n}:saldo-inicial:{timezone.now().strftime('%Y%m%d%H%M%S%f')}",
        origem=RepasseVilaReservaMovimentoAgro.Origem.SALDO_INICIAL,
        cofre=cofre_n,
    )


@transaction.atomic
def estornar_movimento_cofrinho(
    movimento_id: int,
    *,
    observacao: str,
    operador: str,
    usuario=None,
) -> tuple[RepasseVilaReservaMovimentoAgro | None, bool, str]:
    original = RepasseVilaReservaMovimentoAgro.objects.select_for_update().filter(
        pk=movimento_id
    ).first()
    if not original:
        return None, False, "Movimento não encontrado."
    if original.tipo == RepasseVilaReservaMovimentoAgro.Tipo.ESTORNO:
        return None, False, "Não é permitido estornar um estorno."
    cofre_n = _norm_cofre(getattr(original, "cofre", COFRE_SALARIO))
    mov_caixa_estorno = None
    if (
        original.tipo == RepasseVilaReservaMovimentoAgro.Tipo.SEPARACAO
        and original.sessao_caixa_id
        and original.sessao_caixa
        and original.sessao_caixa.fechado_em is None
    ):
        mov_caixa_estorno = MovimentoCaixa.objects.create(
            sessao_caixa=original.sessao_caixa,
            tipo=MovimentoCaixa.Tipo.REFORCO,
            forma_pagamento="Dinheiro",
            valor=abs(_dec(original.valor)),
            observacao=(f"Estorno reserva cofrinho #{original.pk} · {observacao}")[:500],
            usuario=usuario,
        )
    mov, criado, err = _registrar_movimento_cofrinho(
        tipo=RepasseVilaReservaMovimentoAgro.Tipo.ESTORNO,
        origem=RepasseVilaReservaMovimentoAgro.Origem.ESTORNO,
        valor=-_dec(original.valor),
        data_ref=original.data_ref,
        operador=operador,
        observacao=observacao,
        idempotencia_chave=f"reserva-vila:{cofre_n}:estorno:{original.pk}",
        usuario=usuario,
        sessao_caixa=original.sessao_caixa,
        movimento_caixa=mov_caixa_estorno,
        repasse=original.repasse,
        estornado_de=original,
        cofre=cofre_n,
        detalhe={"movimento_original_id": original.pk, "cofre": cofre_n},
    )
    if (err or not criado) and mov_caixa_estorno:
        mov_caixa_estorno.delete()
    return mov, criado, err


def resumo_reserva_fechamento_vila(sessoes) -> dict[str, Any]:
    """Reservas ainda não separadas (salário + Vila Elias) no turno aberto da Vila."""
    sessoes = [s for s in (sessoes or []) if getattr(s, "ponto_caixa", "") == "vila"]
    vazio = {
        "tem": False,
        "valor": "0.00",
        "saldo": str(saldo_cofrinho_vila(cofre=COFRE_SALARIO)),
        "saldo_vila_elias": str(saldo_cofrinho_vila(cofre=COFRE_VILA_ELIAS)),
        "dias": [],
        "texto": "",
        "pendente_salario": "0.00",
        "pendente_vila_elias": "0.00",
    }
    if not sessoes:
        return vazio
    hoje = timezone.localdate()
    cfg = obter_config()
    pend_sal = pendente_reserva_cofrinho_ate(hoje, cfg, cofre=COFRE_SALARIO)["pendente"]
    pend_ve = pendente_reserva_cofrinho_ate(hoje, cfg, cofre=COFRE_VILA_ELIAS)["pendente"]
    total = (pend_sal + pend_ve).quantize(Decimal("0.01"))
    if total <= 0:
        return vazio
    partes = []
    if pend_sal > 0:
        partes.append(f"Salário R$ {pend_sal}")
    if pend_ve > 0:
        partes.append(f"Vila Elias R$ {pend_ve}")
    texto = (
        f"Separe R$ {total} da gaveta e coloque nos cofrinhos ({' · '.join(partes)}). "
        "Esse valor permanece na loja fora da gaveta normal."
    )
    return {
        "tem": True,
        "valor": str(total),
        "saldo": str(saldo_cofrinho_vila(cfg, cofre=COFRE_SALARIO)),
        "saldo_vila_elias": str(saldo_cofrinho_vila(cfg, cofre=COFRE_VILA_ELIAS)),
        "dias": [],
        "texto": texto,
        "pendente_salario": str(pend_sal),
        "pendente_vila_elias": str(pend_ve),
    }


def aplicar_reserva_virtual_estado_caixa(estado: dict, reserva: dict) -> dict:
    """Antecipa no GET o desconto que será persistido ao confirmar o fechamento."""
    if not reserva.get("tem"):
        estado["aviso_reserva_vila"] = reserva
        return estado
    valor = min(
        _dec(reserva.get("valor")),
        max(ZERO, _dec(estado.get("tot_esperado_dinheiro"))),
    )
    reserva = dict(reserva)
    reserva["valor"] = str(valor)
    br = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    reserva["texto"] = (
        f"Separe R$ {br} da gaveta e coloque nos cofrinhos (Salário + Vila Elias). "
        "Esse dinheiro continua na loja, mas fica fora da contagem normal do caixa."
    )
    reserva["tem"] = valor > 0
    estado["tot_esperado_dinheiro"] = str(
        (_dec(estado.get("tot_esperado_dinheiro")) - valor).quantize(Decimal("0.01"))
    )
    for row in estado.get("linhas") or []:
        if row.get("forma") == "Dinheiro":
            row["esperado"] = str((_dec(row.get("esperado")) - valor).quantize(Decimal("0.01")))
            row["retiradas"] = str((_dec(row.get("retiradas")) + valor).quantize(Decimal("0.01")))
            row["com_movimento"] = True
            break
    for card in estado.get("cards") or []:
        # O lote Vila possui um único caixa principal; notebook não cria sessão própria.
        card["esperado_dinheiro"] = str(
            (_dec(card.get("esperado_dinheiro")) - valor).quantize(Decimal("0.01"))
        )
        for row in card.get("linhas") or []:
            if row.get("forma") == "Dinheiro":
                row["esperado"] = str((_dec(row.get("esperado")) - valor).quantize(Decimal("0.01")))
                row["retiradas"] = str((_dec(row.get("retiradas")) + valor).quantize(Decimal("0.01")))
                break
        break
    estado["aviso_reserva_vila"] = reserva
    return estado


def separar_reservas_ao_fechar_vila(
    sessoes,
    *,
    operador: str,
    usuario=None,
) -> tuple[list[RepasseVilaReservaMovimentoAgro], str]:
    principais = [s for s in (sessoes or []) if getattr(s, "ponto_caixa", "") == "vila"]
    if not principais:
        return [], ""
    resumo = resumo_reserva_fechamento_vila(principais)
    if not resumo.get("tem"):
        return [], ""
    out: list[RepasseVilaReservaMovimentoAgro] = []
    for cofre_n in (COFRE_SALARIO, COFRE_VILA_ELIAS):
        mov, criado, err = separar_reserva_diaria(
            timezone.localdate(),
            origem=RepasseVilaReservaMovimentoAgro.Origem.FECHAMENTO,
            operador=operador,
            usuario=usuario,
            sessao_caixa=principais[0],
            observacao="Separação automática no fechamento do caixa Vila",
            cofre=cofre_n,
        )
        if err:
            return out, err
        if criado and mov:
            out.append(mov)
    return out, ""


def _norm_plano_nome(nome: str) -> str:
    return " ".join(str(nome or "").strip().split())


def nomes_planos_desconto_centro(cfg: RepasseVilaConfigAgro | None = None) -> list[str]:
    cfg = cfg or obter_config()
    raw = cfg.planos_desconto_centro if isinstance(cfg.planos_desconto_centro, list) else []
    out: list[str] = []
    seen: set[str] = set()
    for x in raw:
        n = _norm_plano_nome(x)
        if not n:
            continue
        k = n.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(n)
    return out


def salvar_planos_desconto_centro(nomes, *, operador: str = "") -> RepasseVilaConfigAgro:
    limpos: list[str] = []
    seen: set[str] = set()
    for x in nomes or []:
        n = _norm_plano_nome(x)
        if not n:
            continue
        k = n.casefold()
        if k in seen:
            continue
        seen.add(k)
        limpos.append(n[:200])
        if len(limpos) >= 300:
            break
    cfg = obter_config()
    cfg.planos_desconto_centro = limpos
    cfg.atualizado_por = (operador or "")[:120]
    cfg.save(update_fields=["planos_desconto_centro", "atualizado_em", "atualizado_por"])
    return cfg


def _mapa_grafia_plano_oficial() -> dict[str, str]:
    from produtos.models import PlanoContaAgro, PlanoContaAliasAgro

    mapa: dict[str, str] = {}
    for p in PlanoContaAgro.objects.filter(ativo=True).only("nome"):
        n = _norm_plano_nome(p.nome)
        if n:
            mapa[n.casefold()] = n
    for a in PlanoContaAliasAgro.objects.select_related("plano").only("grafia", "plano__nome"):
        g = _norm_plano_nome(a.grafia)
        of = _norm_plano_nome(getattr(a.plano, "nome", "") or "")
        if g and of:
            mapa[g.casefold()] = of
    return mapa


def _oficializar_plano(nome: str, mapa: dict[str, str] | None = None) -> str:
    n = _norm_plano_nome(nome)
    if not n:
        return ""
    mp = mapa if mapa is not None else _mapa_grafia_plano_oficial()
    return mp.get(n.casefold(), n)


def _plano_eh_deposito(nome: str) -> bool:
    k = _norm_plano_nome(nome).casefold()
    return k.startswith("depósito") or k.startswith("deposito")


def despesas_caixa_vila_por_plano(d0: date, d1: date) -> dict[str, Decimal]:
    """Saídas de caixa da Vila no período, agrupadas pelo plano oficial."""
    from produtos.caixa_retiradas_util import listar_retiradas_historico

    hist = listar_retiradas_historico(
        data_de=d0,
        data_ate=d1,
        deposito="vila",
        exportar=True,
    )
    mapa = _mapa_grafia_plano_oficial()
    por: dict[str, Decimal] = {}
    for row in hist.get("linhas") or []:
        oficial = _oficializar_plano(str(row.get("plano") or ""), mapa)
        if not oficial or oficial in ("-", "—"):
            continue
        if _plano_eh_deposito(oficial):
            continue
        por[oficial] = (por.get(oficial, ZERO) + _dec(row.get("valor"))).quantize(Decimal("0.01"))
    return por


def partir_despesas_centro_vila(
    por_plano: dict[str, Decimal],
    selecionados: list[str] | set[str] | None,
) -> tuple[Decimal, Decimal]:
    """Marcado → desconta do envio ao Centro; o resto → do que ficou na Vila."""
    mapa = _mapa_grafia_plano_oficial()
    sel = {
        _oficializar_plano(x, mapa).casefold()
        for x in (selecionados or [])
        if _norm_plano_nome(x)
    }
    centro = ZERO
    vila = ZERO
    for nome, val in (por_plano or {}).items():
        v = _dec(val)
        if v <= 0:
            continue
        if _oficializar_plano(nome, mapa).casefold() in sel:
            centro += v
        else:
            vila += v
    return centro.quantize(Decimal("0.01")), vila.quantize(Decimal("0.01"))


def listar_planos_repasse_config(cfg: RepasseVilaConfigAgro | None = None) -> list[dict[str, Any]]:
    from produtos.models import PlanoContaAgro

    cfg = cfg or obter_config()
    sel = {_norm_plano_nome(x).casefold() for x in nomes_planos_desconto_centro(cfg)}
    out: list[dict[str, Any]] = []
    vistos: set[str] = set()
    for p in PlanoContaAgro.objects.filter(ativo=True).order_by("nome").only("nome")[:300]:
        n = _norm_plano_nome(p.nome)
        if not n:
            continue
        k = n.casefold()
        vistos.add(k)
        out.append({"nome": n, "marcado": k in sel})
    for extra in nomes_planos_desconto_centro(cfg):
        k = extra.casefold()
        if k in vistos:
            continue
        vistos.add(k)
        out.append({"nome": extra, "marcado": True})
    return out


def _vendas_vila_sem_fiado(desde: datetime, ate: datetime) -> list[int]:
    from produtos.fiado_credito_util import venda_local_tem_fiado

    qs = VendaAgro.objects.filter(
        criado_em__gte=desde,
        criado_em__lte=ate,
        devolvida_em__isnull=True,
        deposito__iexact="vila",
    ).only("pk", "pagamentos_json", "forma_pagamento", "total")
    out: list[int] = []
    for v in qs:
        if venda_local_tem_fiado(v):
            continue
        out.append(int(v.pk))
    return out


def _receita_e_cmv_vila(dia: date) -> dict[str, Any]:
    from produtos.relatorios_vendas_util import cmv_vendida_de_rows, mapa_produtos_meta

    desde, ate = _aware_bounds(dia, dia)
    ids = _vendas_vila_sem_fiado(desde, ate)
    if not ids:
        return {
            "receita": ZERO,
            "cmv": ZERO,
            "lucro_bruto": ZERO,
            "skus_com_custo": 0,
            "skus_sem_custo": 0,
            "n_vendas": 0,
        }
    receita = _dec(
        VendaAgro.objects.filter(pk__in=ids).aggregate(t=Sum("total")).get("t")
    )
    rows = list(
        ItemVendaAgro.objects.filter(venda_id__in=ids)
        .exclude(produto_id_externo="")
        .values("produto_id_externo")
        .annotate(qtd=Sum("quantidade"))
    )
    pids = [str(r.get("produto_id_externo") or "").strip() for r in rows]
    meta = mapa_produtos_meta(pids)
    cmv, skus_ok, skus_sem = cmv_vendida_de_rows(rows, meta)
    cmv = _dec(cmv)
    lucro = (receita - cmv).quantize(Decimal("0.01"))
    return {
        "receita": receita,
        "cmv": cmv,
        "lucro_bruto": lucro,
        "skus_com_custo": skus_ok,
        "skus_sem_custo": skus_sem,
        "n_vendas": len(ids),
    }


def _fiado_pago_vila(dia: date) -> Decimal:
    """Pagamentos de fiado na Vila, só títulos originados na Vila."""
    desde, ate = _aware_bounds(dia, dia)
    qs = FiadoBaixaAgro.objects.filter(
        criado_em__gte=desde,
        criado_em__lte=ate,
        sessao_caixa__ponto_caixa="vila",
        titulo__venda_agro__deposito__iexact="vila",
    )
    return _dec(qs.aggregate(t=Sum("valor")).get("t"))


def _forma_eh_eletronica(forma: str) -> bool:
    from produtos.caixa_util import normalizar_forma_pagamento_caixa

    return normalizar_forma_pagamento_caixa(forma) in FORMAS_ELETRONICAS_REPASSE


def _forma_eh_dinheiro(forma: str) -> bool:
    from produtos.caixa_util import normalizar_forma_pagamento_caixa

    return normalizar_forma_pagamento_caixa(forma or "Dinheiro") == "Dinheiro"


def _ja_eletronico_vila(dia: date) -> Decimal:
    """Cartão/PIX da Vila no dia — já cai na conta do Centro (não precisa levar)."""
    from produtos.caixa_util import pagamentos_por_forma_venda

    desde, ate = _aware_bounds(dia, dia)
    ids = _vendas_vila_sem_fiado(desde, ate)
    total = ZERO
    if ids:
        for v in VendaAgro.objects.filter(pk__in=ids).only(
            "pk", "pagamentos_json", "forma_pagamento", "total"
        ):
            por = pagamentos_por_forma_venda(v)
            for fn, val in por.items():
                if fn in FORMAS_ELETRONICAS_REPASSE:
                    total += _dec(val)
    # Fiado pago na Vila com cartão/PIX também já está no Centro
    baixas = FiadoBaixaAgro.objects.filter(
        criado_em__gte=desde,
        criado_em__lte=ate,
        sessao_caixa__ponto_caixa="vila",
        titulo__venda_agro__deposito__iexact="vila",
    ).only("valor", "forma_pagamento")
    for b in baixas:
        if _forma_eh_eletronica(b.forma_pagamento):
            total += _dec(b.valor)
    return total.quantize(Decimal("0.01"))


def _ja_enviado_dia(dia: date) -> dict[str, Decimal]:
    """Só repasses físicos em dinheiro (completo do bolo em espécie)."""
    qs = RepasseVilaCentroAgro.objects.filter(data_ref=dia)
    cmv = lucro = fiado = total = ZERO
    for e in qs.only(
        "valor_cmv", "valor_lucro", "valor_fiado", "valor_total", "forma_pagamento"
    ):
        if not _forma_eh_dinheiro(e.forma_pagamento):
            # legado / outras formas contam como físico também se não for eletrônico
            if _forma_eh_eletronica(e.forma_pagamento):
                continue
        cmv += _dec(e.valor_cmv)
        lucro += _dec(e.valor_lucro)
        fiado += _dec(e.valor_fiado)
        total += _dec(e.valor_total)
    return {
        "cmv": cmv.quantize(Decimal("0.01")),
        "lucro": lucro.quantize(Decimal("0.01")),
        "fiado": fiado.quantize(Decimal("0.01")),
        "total": total.quantize(Decimal("0.01")),
    }


def _aplicar_credito_eletronico(
    disp_cmv: Decimal,
    disp_lucro: Decimal,
    disp_fiado: Decimal,
    ja_elet: Decimal,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    """Desconta cartão/PIX do restante a levar, proporcional aos componentes."""
    total = (disp_cmv + disp_lucro + disp_fiado).quantize(Decimal("0.01"))
    if total <= 0 or ja_elet <= 0:
        return disp_cmv, disp_lucro, disp_fiado, ZERO
    elet = min(ja_elet, total)
    # proporção
    f_cmv = (disp_cmv / total) if total else ZERO
    f_lucro = (disp_lucro / total) if total else ZERO
    f_fiado = (disp_fiado / total) if total else ZERO
    c = (disp_cmv - (elet * f_cmv)).quantize(Decimal("0.01"))
    l = (disp_lucro - (elet * f_lucro)).quantize(Decimal("0.01"))
    fi = (disp_fiado - (elet * f_fiado)).quantize(Decimal("0.01"))
    if c < 0:
        c = ZERO
    if l < 0:
        l = ZERO
    if fi < 0:
        fi = ZERO
    # ajusta centavos no maior
    soma = (c + l + fi).quantize(Decimal("0.01"))
    esperado = (total - elet).quantize(Decimal("0.01"))
    dif = (esperado - soma).quantize(Decimal("0.01"))
    if dif != 0:
        if c >= l and c >= fi:
            c = (c + dif).quantize(Decimal("0.01"))
        elif l >= fi:
            l = (l + dif).quantize(Decimal("0.01"))
        else:
            fi = (fi + dif).quantize(Decimal("0.01"))
    return c, l, fi, elet.quantize(Decimal("0.01"))


def _alvo_fisico_de_calc(calc: dict[str, Any]) -> Decimal:
    """Quanto deveria ir em dinheiro (alvo − cartão/PIX já no Centro)."""
    alvos = calc.get("alvos") or {}
    alvo = _dec(alvos.get("cmv")) + _dec(alvos.get("lucro")) + _dec(alvos.get("fiado"))
    elet = _dec(calc.get("ja_eletronico_aplicado") or calc.get("ja_eletronico"))
    return max(ZERO, (alvo - elet).quantize(Decimal("0.01")))


def _dia_tem_atividade_repasse(dia: date) -> bool:
    if RepasseVilaCentroAgro.objects.filter(data_ref=dia).exists():
        return True
    base = _receita_e_cmv_vila(dia)
    if int(base.get("n_vendas") or 0) > 0:
        return True
    if _fiado_pago_vila(dia) > 0:
        return True
    if despesas_caixa_vila_por_plano(dia, dia):
        return True
    return False


def _ajustes_manuais_total() -> Decimal:
    return _dec(
        RepasseVilaAcumuladoAjusteAgro.objects.aggregate(t=Sum("valor")).get("t")
    )


def _datas_com_atividade(d0: date, d1: date) -> list[date]:
    """Dias no intervalo com repasse, venda Vila ou fiado pago — consulta em lote."""
    if d0 > d1:
        return []
    datas: set[date] = set(
        RepasseVilaCentroAgro.objects.filter(data_ref__gte=d0, data_ref__lte=d1).values_list(
            "data_ref", flat=True
        )
    )
    desde, ate = _aware_bounds(d0, d1)
    for ts in VendaAgro.objects.filter(
        criado_em__gte=desde,
        criado_em__lte=ate,
        deposito__iexact="vila",
        devolvida_em__isnull=True,
    ).values_list("criado_em", flat=True):
        if ts:
            datas.add(timezone.localtime(ts).date())
    for ts in FiadoBaixaAgro.objects.filter(
        criado_em__gte=desde,
        criado_em__lte=ate,
        sessao_caixa__ponto_caixa="vila",
    ).values_list("criado_em", flat=True):
        if ts:
            datas.add(timezone.localtime(ts).date())
    return sorted(d for d in datas if d0 <= d <= d1)


def _atualizar_delta_cache(dia: date, *, percentual_lucro=None) -> RepasseVilaDeltaDiaAgro:
    dd = delta_dia_repasse(dia, percentual_lucro=percentual_lucro)
    obj, _ = RepasseVilaDeltaDiaAgro.objects.update_or_create(
        data_ref=dia,
        defaults={
            "alvo_fisico": _dec(dd["alvo_fisico"]),
            "enviado": _dec(dd["enviado"]),
            "delta": _dec(dd["delta"]),
        },
    )
    return obj


def _preencher_cache_faltante(d0: date, d1: date) -> None:
    """Só calcula dias com atividade que ainda não estão no cache."""
    if d0 > d1:
        return
    cfg = obter_config()
    pct = _dec(cfg.percentual_lucro_padrao)
    cached = set(
        RepasseVilaDeltaDiaAgro.objects.filter(data_ref__gte=d0, data_ref__lte=d1).values_list(
            "data_ref", flat=True
        )
    )
    for d in _datas_com_atividade(d0, d1):
        if d not in cached:
            _atualizar_delta_cache(d, percentual_lucro=pct)


def _sum_delta_cache(d0: date, d1: date) -> Decimal:
    return _dec(
        RepasseVilaDeltaDiaAgro.objects.filter(data_ref__gte=d0, data_ref__lte=d1).aggregate(
            t=Sum("delta")
        ).get("t")
    )


def delta_dia_repasse(
    dia: date,
    *,
    percentual_lucro: Decimal | float | str | None = None,
) -> dict[str, Any]:
    """Delta do dia: alvo físico − enviado. Positivo = faltou levar; negativo = levou a mais."""
    cfg = obter_config()
    pct = _dec(percentual_lucro) if percentual_lucro is not None else _dec(cfg.percentual_lucro_padrao)
    calc = calcular_disponivel(dia, percentual_lucro=pct, modo_dia_cheio=False, _skip_acumulado=True)
    alvo = _alvo_fisico_de_calc(calc)
    enviado = _dec((calc.get("ja_enviado") or {}).get("total"))
    delta = (alvo - enviado).quantize(Decimal("0.01"))
    return {
        "data": dia.isoformat(),
        "alvo_fisico": float(alvo),
        "enviado": float(enviado),
        "delta": float(delta),
        "n_vendas": int(calc.get("n_vendas") or 0),
    }


def acumulado_anterior(
    dia: date,
    *,
    lookback_days: int = REPASSE_MAX_DIAS_ATRASO,
) -> Decimal:
    """Saldo bruto dos dias anteriores a `dia` + ajustes manuais. Positivo = faltou levar."""
    d0 = dia - timedelta(days=lookback_days)
    d_fim = dia - timedelta(days=1)
    if d_fim >= d0:
        _preencher_cache_faltante(d0, d_fim)
    saldo = _ajustes_manuais_total()
    if d_fim >= d0:
        saldo += _sum_delta_cache(d0, d_fim)
    return saldo.quantize(Decimal("0.01"))


def _extra_do_calc(calc: dict[str, Any]) -> Decimal:
    """Quanto o dinheiro do dia passou do alvo (abate acumulado)."""
    alvo = _alvo_fisico_de_calc(calc)
    enviado = _dec((calc.get("ja_enviado") or {}).get("total"))
    return max(ZERO, (enviado - alvo).quantize(Decimal("0.01")))


def _extra_enviado_apos(dia: date) -> Decimal:
    """Soma do que foi levado a mais depois de `dia` (delta negativo no cache)."""
    hoje = timezone.localdate()
    d0 = dia + timedelta(days=1)
    if d0 > hoje:
        return ZERO
    _preencher_cache_faltante(d0, hoje)
    extra = ZERO
    for row in RepasseVilaDeltaDiaAgro.objects.filter(
        data_ref__gte=d0, data_ref__lte=hoje
    ).only("delta"):
        dlt = _dec(row.delta)
        if dlt < 0:
            extra += -dlt
    return extra.quantize(Decimal("0.01"))


def abater_extras_do_acumulado(
    dia: date,
    acum_bruto: Decimal,
    calc_dia: dict[str, Any],
) -> Decimal:
    """Não pede de novo o que já saiu a mais neste dia ou nos dias seguintes."""
    extra = _extra_do_calc(calc_dia) + _extra_enviado_apos(dia)
    return (acum_bruto - extra).quantize(Decimal("0.01"))


def listar_acumulado_detalhe(
    dia: date,
    *,
    lookback_days: int = REPASSE_MAX_DIAS_ATRASO,
) -> dict[str, Any]:
    """Extrato do acumulado até o dia anterior a `dia` (não inclui falta do próprio dia)."""
    d0 = dia - timedelta(days=lookback_days)
    d_fim = dia - timedelta(days=1)
    if d_fim >= d0:
        _preencher_cache_faltante(d0, d_fim)
    linhas: list[dict[str, Any]] = []
    ajustes: list[dict[str, Any]] = []
    saldo = ZERO
    for adj in RepasseVilaAcumuladoAjusteAgro.objects.order_by("criado_em", "pk"):
        v = _dec(adj.valor)
        saldo = (saldo + v).quantize(Decimal("0.01"))
        ajustes.append(
            {
                "id": adj.pk,
                "tipo": "ajuste",
                "data": adj.data_ref.isoformat() if adj.data_ref else "",
                "delta": float(v),
                "observacao": adj.observacao,
                "operador": adj.operador or "",
                "criado_em": timezone.localtime(adj.criado_em).isoformat() if adj.criado_em else "",
                "saldo_apos": float(saldo),
            }
        )
    for row in RepasseVilaDeltaDiaAgro.objects.filter(
        data_ref__gte=d0, data_ref__lte=d_fim
    ).order_by("data_ref"):
        delta = _dec(row.delta)
        saldo = (saldo + delta).quantize(Decimal("0.01"))
        linhas.append(
            {
                "tipo": "dia",
                "data": row.data_ref.isoformat(),
                "alvo_fisico": float(_dec(row.alvo_fisico)),
                "enviado": float(_dec(row.enviado)),
                "delta": float(delta),
                "saldo_apos": float(saldo),
                "n_vendas": 0,
            }
        )
    acum_bruto = acumulado_anterior(dia, lookback_days=lookback_days)
    cfg = obter_config()
    pct = _dec(cfg.percentual_lucro_padrao)
    calc_hoje = calcular_disponivel(dia, percentual_lucro=pct, modo_dia_cheio=False, _skip_acumulado=True)
    acum = abater_extras_do_acumulado(dia, acum_bruto, calc_hoje)
    falta_dia = _dec(calc_hoje.get("falta_dinheiro"))
    reserva = _dec(calc_hoje.get("reserva_aplicada") or calc_hoje.get("reserva_vila"))
    total_sug = (falta_dia + acum).quantize(Decimal("0.01"))
    return {
        "ok": True,
        "data_ref": dia.isoformat(),
        "acumulado_anterior": float(acum),
        "acumulado_bruto": float(acum_bruto),
        "falta_dia": float(falta_dia),
        "reserva_vila": float(reserva),
        "reserva_aplicada": float(_dec(calc_hoje.get("reserva_aplicada"))),
        "lucro_penultimo_dia": float(_dec(calc_hoje.get("lucro_penultimo_dia"))),
        "total_sugerido_bruto": float(total_sug),
        "total_sugerido": float(max(ZERO, total_sug)),
        "credito": float(max(ZERO, -total_sug)) if total_sug < 0 else 0.0,
        "linhas_dias": linhas,
        "ajustes": ajustes,
    }


def registrar_ajuste_acumulado(
    valor,
    *,
    observacao: str,
    operador: str = "",
    data_ref: date | None = None,
    repasse: RepasseVilaCentroAgro | None = None,
) -> tuple[RepasseVilaAcumuladoAjusteAgro | None, str]:
    v = _dec(valor)
    if v == 0:
        return None, "Informe um valor diferente de zero."
    obs = " ".join(str(observacao or "").strip().split())
    if len(obs) < 3:
        return None, "Descreva o motivo (mín. 3 caracteres)."
    if v > Decimal("99999.99") or v < Decimal("-99999.99"):
        return None, "Valor fora do limite."
    adj = RepasseVilaAcumuladoAjusteAgro.objects.create(
        valor=v,
        observacao=obs[:500],
        operador=(operador or "")[:120],
        data_ref=data_ref,
        repasse=repasse,
    )
    return adj, ""


def quitar_acumulado_zerar(
    dia: date | None = None,
    *,
    observacao: str = "",
    operador: str = "",
) -> tuple[RepasseVilaAcumuladoAjusteAgro | None, str]:
    """Zera o acumulado (ex.: dinheiro já foi transferido antes da ferramenta)."""
    dia = dia or timezone.localdate()
    calc = calcular_disponivel(dia, _skip_acumulado=True)
    acum = abater_extras_do_acumulado(dia, acumulado_anterior(dia), calc)
    if acum <= 0:
        return None, "Acumulado já está zerado, é crédito, ou já foi coberto pelo dinheiro enviado."
    obs = (observacao or "").strip() or "Transferido antes da ferramenta / zerado manualmente"
    return registrar_ajuste_acumulado(
        -acum,
        observacao=obs,
        operador=operador,
        data_ref=dia,
    )


def _redistribuir_tres(
    v_cmv: Decimal,
    v_lucro: Decimal,
    v_fiado: Decimal,
    novo_total: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    """Escala CMV/lucro/fiado para bater `novo_total` (centavos no maior)."""
    base = (v_cmv + v_lucro + v_fiado).quantize(Decimal("0.01"))
    if base <= 0 or novo_total <= 0:
        return ZERO, ZERO, ZERO
    if base == novo_total:
        return v_cmv, v_lucro, v_fiado
    fator = novo_total / base
    c = (v_cmv * fator).quantize(Decimal("0.01"))
    l = (v_lucro * fator).quantize(Decimal("0.01"))
    fi = (v_fiado * fator).quantize(Decimal("0.01"))
    dif = (novo_total - (c + l + fi)).quantize(Decimal("0.01"))
    if dif != 0:
        if c >= l and c >= fi:
            c = (c + dif).quantize(Decimal("0.01"))
        elif l >= fi:
            l = (l + dif).quantize(Decimal("0.01"))
        else:
            fi = (fi + dif).quantize(Decimal("0.01"))
    return c, l, fi


def calcular_disponivel(
    dia: date | None = None,
    *,
    percentual_lucro: Decimal | float | str | None = None,
    modo_dia_cheio: bool = False,
    _skip_acumulado: bool = False,
) -> dict[str, Any]:
    """Monta o bolo do dia e o que ainda falta enviar (ou o dia cheio)."""
    dia = dia or timezone.localdate()
    cfg = obter_config()
    if percentual_lucro is None:
        pct = _dec(cfg.percentual_lucro_padrao)
    else:
        pct = _dec(percentual_lucro)
    if pct < 0:
        pct = ZERO
    if pct > 100:
        pct = Decimal("100.00")

    base = _receita_e_cmv_vila(dia)
    fiado_dia = _fiado_pago_vila(dia)
    lucro = base["lucro_bruto"]
    if lucro < 0:
        lucro = ZERO

    # Dois cofrinhos: Vila Elias = fatia que fica; Salário = config — ambos saem do lucro
    # antes do que vai ao Centro. CMV/fiado não entram nos cofres.
    reserva_cfg = reserva_vila_config(cfg)
    desde = reserva_vila_desde_config(cfg)
    parte_salario = reserva_aplicada_no_dia(dia, cfg, lucro_bruto=lucro)
    if dia >= COFRE_VILA_ELIAS_DESDE:
        parte_vila_elias = (lucro * (Decimal("100") - pct) / Decimal("100")).quantize(
            Decimal("0.01")
        )
        parte_vila_elias = max(ZERO, parte_vila_elias)
    else:
        parte_vila_elias = ZERO
    # Cap: não pode consumir mais que o lucro
    if (parte_vila_elias + parte_salario) > lucro:
        # Prioriza Vila Elias (fatia automática); salário usa o resto do lucro
        parte_vila_elias = min(parte_vila_elias, lucro)
        parte_salario = min(parte_salario, max(ZERO, (lucro - parte_vila_elias).quantize(Decimal("0.01"))))
    reserva_apl = parte_salario  # legado / snapshots
    lucro_penultimo = max(
        ZERO, (lucro - parte_vila_elias - parte_salario).quantize(Decimal("0.01"))
    )

    cmv_alvo = base["cmv"]
    lucro_alvo = lucro_penultimo  # já é o que sobra para o Centro após os cofres
    fiado_alvo = fiado_dia

    por_desp = despesas_caixa_vila_por_plano(dia, dia)
    sel_planos = nomes_planos_desconto_centro(cfg)
    desp_centro, desp_vila = partir_despesas_centro_vila(por_desp, sel_planos)
    lucro_alvo = max(ZERO, (lucro_alvo - desp_centro).quantize(Decimal("0.01")))

    ja = _ja_enviado_dia(dia)
    ja_elet = _ja_eletronico_vila(dia)
    if modo_dia_cheio:
        # Dia cheio = ignora dinheiro já enviado; cartão/PIX do dia ainda credita
        disp_cmv = cmv_alvo
        disp_lucro = lucro_alvo
        disp_fiado = fiado_alvo
    else:
        disp_cmv = max(ZERO, cmv_alvo - ja["cmv"])
        disp_lucro = max(ZERO, lucro_alvo - ja["lucro"])
        disp_fiado = max(ZERO, fiado_alvo - ja["fiado"])

    disp_cmv, disp_lucro, disp_fiado, elet_aplicado = _aplicar_credito_eletronico(
        disp_cmv, disp_lucro, disp_fiado, ja_elet
    )

    total_disp = (disp_cmv + disp_lucro + disp_fiado).quantize(Decimal("0.01"))
    alvo_total = (cmv_alvo + lucro_alvo + fiado_alvo).quantize(Decimal("0.01"))
    acum = ZERO
    acum_bruto = ZERO
    total_sugerido = total_disp
    if not _skip_acumulado:
        acum_bruto = acumulado_anterior(dia)
        mini = {
            "alvos": {"cmv": cmv_alvo, "lucro": lucro_alvo, "fiado": fiado_alvo},
            "ja_eletronico_aplicado": elet_aplicado,
            "ja_enviado": ja,
        }
        acum = abater_extras_do_acumulado(dia, acum_bruto, mini)
        total_sugerido = (total_disp + acum).quantize(Decimal("0.01"))
    # Cofres já saíram do lucro_penultimo — não corta de novo o total.
    total_sugerido_bruto = total_sugerido
    return {
        "ok": True,
        "data_ref": dia.isoformat(),
        "percentual_lucro": float(pct),
        "percentual_padrao": float(_dec(cfg.percentual_lucro_padrao)),
        "reserva_vila": float(reserva_cfg),
        "reserva_vila_desde": desde.isoformat(),
        "reserva_aplicada": float(reserva_apl),
        "parte_salario": float(parte_salario),
        "parte_vila_elias": float(parte_vila_elias),
        "cofre_vila_elias_desde": COFRE_VILA_ELIAS_DESDE.isoformat(),
        "lucro_penultimo_dia": float(lucro_penultimo),
        "modo_dia_cheio": bool(modo_dia_cheio),
        "receita_dia": float(base["receita"]),
        "cmv_dia": float(base["cmv"]),
        "lucro_bruto_dia": float(base["lucro_bruto"]),
        "fiado_pago_dia": float(fiado_dia),
        "n_vendas": base["n_vendas"],
        "skus_com_custo": base["skus_com_custo"],
        "skus_sem_custo": base["skus_sem_custo"],
        "ja_enviado": {k: float(v) for k, v in ja.items()},
        "ja_eletronico": float(ja_elet),
        "ja_eletronico_aplicado": float(elet_aplicado),
        "alvo_total": float(alvo_total),
        "falta_dinheiro": float(total_disp),
        "acumulado_anterior": float(acum),
        "acumulado_bruto": float(acum_bruto),
        "total_sugerido_bruto": float(total_sugerido_bruto),
        "total_sugerido": float(max(ZERO, total_sugerido)),
        "credito_acumulado": float(max(ZERO, -total_sugerido)) if total_sugerido < 0 else 0.0,
        "disponivel": {
            "cmv": float(disp_cmv),
            "lucro": float(disp_lucro),
            "fiado": float(disp_fiado),
            "total": float(total_disp),
        },
        "alvos": {
            "cmv": float(cmv_alvo),
            "lucro": float(lucro_alvo),
            "fiado": float(fiado_alvo),
        },
        "despesas_centro_dia": float(desp_centro),
        "despesas_vila_dia": float(desp_vila),
        "planos_desconto_centro": sel_planos,
    }


def _receita_e_cmv_vila_periodo(d0: date, d1: date) -> dict[str, Any]:
    """Receita/CMV/lucro bruto Vila no intervalo (um scan — card do mês)."""
    from produtos.relatorios_vendas_util import cmv_vendida_de_rows, mapa_produtos_meta

    desde, ate = _aware_bounds(d0, d1)
    ids = _vendas_vila_sem_fiado(desde, ate)
    if not ids:
        return {
            "receita": ZERO,
            "cmv": ZERO,
            "lucro_bruto": ZERO,
            "n_vendas": 0,
        }
    receita = _dec(
        VendaAgro.objects.filter(pk__in=ids).aggregate(t=Sum("total")).get("t")
    )
    rows = list(
        ItemVendaAgro.objects.filter(venda_id__in=ids)
        .exclude(produto_id_externo="")
        .values("produto_id_externo")
        .annotate(qtd=Sum("quantidade"))
    )
    pids = [str(r.get("produto_id_externo") or "").strip() for r in rows]
    meta = mapa_produtos_meta(pids)
    cmv, _skus_ok, _skus_sem = cmv_vendida_de_rows(rows, meta)
    cmv = _dec(cmv)
    lucro = (receita - cmv).quantize(Decimal("0.01"))
    return {
        "receita": receita,
        "cmv": cmv,
        "lucro_bruto": lucro,
        "n_vendas": len(ids),
    }


def historico_mes(ano: int | None = None, mes: int | None = None) -> dict[str, Any]:
    hoje = timezone.localdate()
    ano = int(ano or hoje.year)
    mes = int(mes or hoje.month)
    ultimo = monthrange(ano, mes)[1]
    d0 = date(ano, mes, 1)
    d1 = date(ano, mes, ultimo)

    envios = list(
        RepasseVilaCentroAgro.objects.filter(data_ref__gte=d0, data_ref__lte=d1).order_by(
            "data_ref", "criado_em"
        )
    )
    por_dia: dict[str, dict[str, Any]] = {}
    total_mes = ZERO
    lucro_enviado_mes = ZERO
    for e in envios:
        key = e.data_ref.isoformat()
        bucket = por_dia.setdefault(
            key,
            {
                "data": key,
                "dia": e.data_ref.day,
                "valor": ZERO,
                "valor_lucro": ZERO,
                "n": 0,
                "envios": [],
                "lucro_bruto_snap": ZERO,
            },
        )
        bucket["valor"] += _dec(e.valor_total)
        bucket["valor_lucro"] += _dec(e.valor_lucro)
        bucket["n"] += 1
        bucket["lucro_bruto_snap"] = _dec(e.lucro_bruto_dia)  # último do dia (ordem)
        bucket["envios"].append(
            {
                "id": e.pk,
                "hora": timezone.localtime(e.criado_em).strftime("%H:%M") if e.criado_em else "",
                "total": float(_dec(e.valor_total)),
                "cmv": float(_dec(e.valor_cmv)),
                "lucro": float(_dec(e.valor_lucro)),
                "fiado": float(_dec(e.valor_fiado)),
                "pct": float(_dec(e.percentual_lucro)),
                "quem": e.quem_levou,
                "status_centro": e.status_centro,
                "modo_dia_cheio": bool(e.modo_dia_cheio),
            }
        )
        total_mes += _dec(e.valor_total)
        lucro_enviado_mes += _dec(e.valor_lucro)

    base_mes = _receita_e_cmv_vila_periodo(d0, d1)
    lucro_bruto_mes = max(ZERO, _dec(base_mes["lucro_bruto"]))
    cfg = obter_config()
    por_desp = despesas_caixa_vila_por_plano(d0, d1)
    desp_centro, desp_vila = partir_despesas_centro_vila(
        por_desp, nomes_planos_desconto_centro(cfg)
    )
    lucro_ficou_vila = max(
        ZERO, (lucro_bruto_mes - lucro_enviado_mes - desp_vila).quantize(Decimal("0.01"))
    )

    dias_out = []
    for day in range(1, ultimo + 1):
        d = date(ano, mes, day)
        key = d.isoformat()
        b = por_dia.get(key)
        if not b:
            dias_out.append(
                {
                    "data": key,
                    "dia": day,
                    "valor": 0.0,
                    "pct_lucro_real": None,
                    "n": 0,
                    "envios": [],
                }
            )
            continue
        lucro_dia = _dec(b.get("lucro_bruto_snap") or 0)
        if lucro_dia <= 0:
            lucro_dia = max(ZERO, _dec(_receita_e_cmv_vila(d)["lucro_bruto"]))
        pct_real = None
        if lucro_dia > 0:
            pct_real = float(
                (b["valor_lucro"] / lucro_dia * Decimal("100")).quantize(Decimal("0.01"))
            )
        dias_out.append(
            {
                "data": key,
                "dia": day,
                "valor": float(_dec(b["valor"])),
                "pct_lucro_real": pct_real,
                "n": b["n"],
                "envios": b["envios"],
            }
        )

    total_geral = _dec(
        RepasseVilaCentroAgro.objects.aggregate(t=Sum("valor_total")).get("t")
    )

    return {
        "ok": True,
        "ano": ano,
        "mes": mes,
        "total_mes": float(_dec(total_mes)),
        "total_geral": float(total_geral),
        "lucro_bruto_mes": float(lucro_bruto_mes),
        "lucro_enviado_mes": float(_dec(lucro_enviado_mes)),
        "lucro_ficou_vila": float(lucro_ficou_vila),
        "despesas_centro_mes": float(desp_centro),
        "despesas_vila_mes": float(desp_vila),
        "dias": dias_out,
    }


def _obs_repasse(rep: RepasseVilaCentroAgro, lado: str) -> str:
    return (
        f"Repasse Vila→Centro #{rep.pk} · {lado} · quem={rep.quem_levou} · "
        f"{rep.data_ref.strftime('%d/%m/%Y')}"
    )[:500]


@transaction.atomic
def confirmar_repasse(
    *,
    request,
    quem_levou: str,
    percentual_lucro: Decimal | float | str | None = None,
    incluir_cmv: bool = True,
    incluir_lucro: bool = True,
    incluir_fiado: bool = True,
    modo_dia_cheio: bool = False,
    valor_manual: Decimal | float | str | None = None,
    forma_pagamento: str = "Dinheiro",
    operador: str = "",
    data_ref: date | None = None,
    incluir_acumulado: bool = False,
    separar_reserva: bool = False,
    forcar_manual_zerado: bool = False,
) -> tuple[RepasseVilaCentroAgro | None, str]:
    """
    Saída no caixa Vila (obrigatório aberto) + entrada no Centro (agora ou pendente).
    valor_manual: se informado, usa esse total (proporcional nas linhas marcadas).
    forcar_manual_zerado: permite valor manual quando CMV/%/fiado disponíveis = 0
    (dia já coberto) — exige confirmação forte no cliente (PIN de novo).
    """
    from produtos.caixa_util import (
        normalizar_forma_pagamento_caixa,
        obter_caixa_gaveta_aberto,
        obter_caixa_vila_aberto,
        obter_sessao_caixa_aberta_request,
    )

    quem = (quem_levou or "").strip()
    if len(quem) < 2:
        return None, "Informe quem levou o dinheiro."

    sessao_req = obter_sessao_caixa_aberta_request(request)
    sessao_vila = obter_caixa_vila_aberto()
    if not sessao_vila:
        return None, "Abra o caixa da Vila Elias para transferir."
    if not sessao_req:
        return None, "Abra o caixa da Vila neste computador para transferir."
    # Operação só na Vila (gaveta/notebook da Vila)
    if getattr(sessao_req, "ponto_caixa", "") not in ("vila", "notebook"):
        return None, "Troque a loja para Vila Elias (caixa da Vila) para transferir."
    if getattr(sessao_req, "ponto_caixa", "") == "notebook":
        pai = getattr(sessao_req, "sessao_principal", None)
        if not pai or getattr(pai, "ponto_caixa", "") != "vila":
            return None, "Notebook precisa estar vinculado ao caixa da Vila."

    dia = data_ref or timezone.localdate()
    dia, err_dia = validar_data_ref_repasse(dia)
    if err_dia or dia is None:
        return None, err_dia or "Data inválida."
    calc = calcular_disponivel(dia, percentual_lucro=percentual_lucro, modo_dia_cheio=modo_dia_cheio)
    disp = calc["disponivel"]
    v_cmv = _dec(disp["cmv"]) if incluir_cmv else ZERO
    v_lucro = _dec(disp["lucro"]) if incluir_lucro else ZERO
    v_fiado = _dec(disp["fiado"]) if incluir_fiado else ZERO
    total = (v_cmv + v_lucro + v_fiado).quantize(Decimal("0.01"))

    if valor_manual is not None and str(valor_manual).strip() != "":
        vm = _dec(valor_manual)
        if vm <= 0:
            return None, "Valor manual inválido."
        if vm > Decimal("99999.99"):
            return None, "Valor manual alto demais — confira o número."
        # Valor digitado manda: pode ser maior que o automático (ex. levar R$ 600
        # quando o cálculo deu R$ 512). Distribui nas linhas marcadas (proporção).
        base_soma = total
        if base_soma <= 0:
            br = f"{vm:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            msg_forcar = (
                f"O cálculo automático deste dia já está zerado (já enviado ou cartão/PIX cobriu). "
                f"Você está forçando R$ {br}. Confirme com o PIN de novo."
            )
            if not forcar_manual_zerado:
                return None, f"PRECISA_FORCAR_MANUAL::{msg_forcar}"
            if not (incluir_cmv or incluir_lucro or incluir_fiado):
                return None, "Marque CMV, % lucro ou fiado para forçar o valor manual."
            # Dia zerado: joga o valor forçado na 1ª linha marcada (sem proporção).
            v_cmv = v_lucro = v_fiado = ZERO
            if incluir_cmv:
                v_cmv = vm
            elif incluir_lucro:
                v_lucro = vm
            else:
                v_fiado = vm
            total = vm
        else:
            fator = vm / base_soma
            v_cmv = (v_cmv * fator).quantize(Decimal("0.01"))
            v_lucro = (v_lucro * fator).quantize(Decimal("0.01"))
            v_fiado = (v_fiado * fator).quantize(Decimal("0.01"))
            # ajusta centavos no maior componente
            total = (v_cmv + v_lucro + v_fiado).quantize(Decimal("0.01"))
            dif = (vm - total).quantize(Decimal("0.01"))
            if dif != 0:
                if v_cmv >= v_lucro and v_cmv >= v_fiado:
                    v_cmv = (v_cmv + dif).quantize(Decimal("0.01"))
                elif v_lucro >= v_fiado:
                    v_lucro = (v_lucro + dif).quantize(Decimal("0.01"))
                else:
                    v_fiado = (v_fiado + dif).quantize(Decimal("0.01"))
                total = vm
    elif incluir_acumulado:
        acum = _dec(calc.get("acumulado_anterior"))
        if acum != 0:
            novo = max(ZERO, (total + acum).quantize(Decimal("0.01")))
            if novo <= 0:
                return None, (
                    "Crédito acumulado cobre o dia — nada a levar agora. "
                    "Desmarque «Incluir acumulado» ou ajuste o valor manual."
                )
            v_cmv, v_lucro, v_fiado = _redistribuir_tres(v_cmv, v_lucro, v_fiado, novo)
            total = novo

    # Cofres já saíram do lucro_ao_Centro — não corta o total de novo.

    if total <= 0:
        return None, "Nada a levar em dinheiro (cartão/PIX já cobriu ou já enviado)."

    fn = normalizar_forma_pagamento_caixa(forma_pagamento or "Dinheiro")
    # Completar o dia = físico; se mandar eletrônico de novo, ainda registra no caixa
    user = (
        request.user
        if getattr(request, "user", None) and request.user.is_authenticated
        else None
    )

    movimentos_cofre: list = []
    if fn == "Dinheiro" and separar_reserva:
        from produtos.caixa_util import resumo_esperado_por_forma

        dinheiro_gaveta = max(
            ZERO,
            _dec(resumo_esperado_por_forma(sessao_vila).get("Dinheiro")),
        )
        pend_sal = _dec(resumo_cofrinho_vila(dia, cofre=COFRE_SALARIO).get("pendente_dia"))
        pend_ve = _dec(resumo_cofrinho_vila(dia, cofre=COFRE_VILA_ELIAS).get("pendente_dia"))
        pendente_reserva = (pend_sal + pend_ve).quantize(Decimal("0.01"))
        limite_transferencia = max(ZERO, dinheiro_gaveta - pendente_reserva)
        if total > limite_transferencia:
            return None, (
                f"A transferência de R$ {total} consumiria dinheiro que precisa permanecer "
                f"na Vila. Disponível na gaveta após separar os cofrinhos: R$ {limite_transferencia}. "
                f"Pendente Salário R$ {pend_sal} · Vila Elias R$ {pend_ve}."
            )

    if separar_reserva:
        for cofre_n, obs in (
            (COFRE_SALARIO, "Separação Salário junto com o repasse Vila → Centro"),
            (COFRE_VILA_ELIAS, "Separação Cofre Vila Elias junto com o repasse"),
        ):
            mov_c, _criado_c, err_c = separar_reserva_diaria(
                dia,
                origem=RepasseVilaReservaMovimentoAgro.Origem.REPASSE,
                operador=operador or quem,
                usuario=user,
                sessao_caixa=sessao_vila,
                observacao=obs,
                cofre=cofre_n,
            )
            if err_c:
                return None, err_c
            if mov_c:
                movimentos_cofre.append(mov_c)

    mov_saida = MovimentoCaixa.objects.create(
        sessao_caixa=sessao_vila,
        tipo=MovimentoCaixa.Tipo.RETIRADA,
        forma_pagamento=fn,
        valor=total,
        observacao=(
            f"Repasse Vila→Centro · ref {dia.strftime('%d/%m/%Y')} · {quem} · "
            f"falta dinheiro (máquinas já no Centro)"
        )[:500],
        usuario=user,
    )

    gaveta = obter_caixa_gaveta_aberto()
    mov_entrada = None
    status = RepasseVilaCentroAgro.StatusCentro.PENDENTE
    sessao_centro = None
    if gaveta:
        mov_entrada = MovimentoCaixa.objects.create(
            sessao_caixa=gaveta,
            tipo=MovimentoCaixa.Tipo.REFORCO,
            forma_pagamento=fn,
            valor=total,
            observacao=f"Repasse da Vila · {quem}"[:500],
            usuario=user,
        )
        status = RepasseVilaCentroAgro.StatusCentro.APLICADO
        sessao_centro = gaveta

    reserva_apl = _dec(calc.get("reserva_aplicada"))
    lucro_pen = _dec(calc.get("lucro_penultimo_dia"))
    rep = RepasseVilaCentroAgro.objects.create(
        data_ref=dia,
        percentual_lucro=_dec(calc["percentual_lucro"]),
        modo_dia_cheio=bool(modo_dia_cheio),
        incluir_cmv=bool(incluir_cmv),
        incluir_lucro=bool(incluir_lucro),
        incluir_fiado=bool(incluir_fiado),
        valor_cmv=v_cmv,
        valor_lucro=v_lucro,
        valor_fiado=v_fiado,
        valor_total=total,
        receita_dia=_dec(calc["receita_dia"]),
        cmv_dia=_dec(calc["cmv_dia"]),
        lucro_bruto_dia=_dec(calc["lucro_bruto_dia"]),
        reserva_aplicada=reserva_apl,
        lucro_penultimo_dia=lucro_pen,
        fiado_pago_dia=_dec(calc["fiado_pago_dia"]),
        quem_levou=quem[:120],
        forma_pagamento=fn,
        operador=(operador or "")[:120],
        usuario=user,
        sessao_vila=sessao_vila,
        sessao_centro=sessao_centro,
        movimento_saida=mov_saida,
        movimento_entrada=mov_entrada,
        status_centro=status,
    )
    for movimento_reserva in movimentos_cofre:
        if movimento_reserva.repasse_id is None:
            movimento_reserva.repasse = rep
            movimento_reserva.save(update_fields=["repasse"])
    mov_saida.observacao = _obs_repasse(rep, "saída Vila")
    mov_saida.save(update_fields=["observacao"])
    if mov_entrada:
        mov_entrada.observacao = _obs_repasse(rep, "entrada Centro")
        mov_entrada.save(update_fields=["observacao"])

    _registrar_log_reserva(
        tipo=RepasseVilaReservaLogAgro.Tipo.APLICADO,
        operador=operador or quem,
        data_ref=dia,
        valor_antes=ZERO,
        valor_depois=reserva_apl,
        mensagem=(
            f"Envio #{rep.pk} · dia {dia.strftime('%d/%m/%Y')} · "
            f"lucro bruto R$ {_dec(calc['lucro_bruto_dia'])} − reserva R$ {reserva_apl} "
            f"= penúltimo R$ {lucro_pen} · {_dec(calc['percentual_lucro'])}% → lucro enviado "
            f"R$ {v_lucro} · total R$ {total}."
        ),
        detalhe={
            "repasse_id": rep.pk,
            "data_ref": dia.isoformat(),
            "reserva_vila_config": float(_dec(calc.get("reserva_vila"))),
            "reserva_vila_desde": calc.get("reserva_vila_desde") or "",
            "reserva_aplicada": float(reserva_apl),
            "lucro_bruto_dia": float(_dec(calc.get("lucro_bruto_dia"))),
            "lucro_penultimo_dia": float(lucro_pen),
            "percentual_lucro": float(_dec(calc.get("percentual_lucro"))),
            "valor_cmv": float(v_cmv),
            "valor_lucro": float(v_lucro),
            "valor_fiado": float(v_fiado),
            "valor_total": float(total),
            "valor_manual": float(_dec(valor_manual)) if valor_manual is not None and str(valor_manual).strip() != "" else None,
            "modo_dia_cheio": bool(modo_dia_cheio),
            "quem_levou": quem,
            "forma_pagamento": fn,
            "status_centro": status,
            "alvos": calc.get("alvos") or {},
            "disponivel": calc.get("disponivel") or {},
            "acumulado_anterior": float(_dec(calc.get("acumulado_anterior"))),
            "regra": "lucro_bruto - reserva_diaria → % ao Centro; CMV e fiado intactos",
        },
        repasse=rep,
    )

    _atualizar_delta_cache(dia, percentual_lucro=calc.get("percentual_lucro"))
    # Extra enviado neste dia já abate o acumulado na conta (não cria ajuste:
    # senão o dia seguinte conta o extra duas vezes).

    return rep, ""


def aplicar_repasses_pendentes_centro(*, sessao_centro, usuario=None) -> list[RepasseVilaCentroAgro]:
    """Ao abrir Gaveta Centro: cria REFORCO dos pendentes e devolve lista p/ aviso."""
    pendentes = list(
        RepasseVilaCentroAgro.objects.filter(
            status_centro=RepasseVilaCentroAgro.StatusCentro.PENDENTE
        ).order_by("criado_em", "pk")
    )
    aplicados: list[RepasseVilaCentroAgro] = []
    for rep in pendentes:
        if rep.movimento_entrada_id:
            rep.status_centro = RepasseVilaCentroAgro.StatusCentro.APLICADO
            rep.sessao_centro = sessao_centro
            rep.save(update_fields=["status_centro", "sessao_centro"])
            aplicados.append(rep)
            continue
        mov = MovimentoCaixa.objects.create(
            sessao_caixa=sessao_centro,
            tipo=MovimentoCaixa.Tipo.REFORCO,
            forma_pagamento=rep.forma_pagamento or "Dinheiro",
            valor=_dec(rep.valor_total),
            observacao=_obs_repasse(rep, "entrada Centro (abertura)"),
            usuario=usuario,
        )
        rep.movimento_entrada = mov
        rep.sessao_centro = sessao_centro
        rep.status_centro = RepasseVilaCentroAgro.StatusCentro.APLICADO
        rep.save(
            update_fields=[
                "movimento_entrada",
                "sessao_centro",
                "status_centro",
            ]
        )
        aplicados.append(rep)
    return aplicados


def texto_aviso_abertura(repasses: list[RepasseVilaCentroAgro]) -> str:
    if not repasses:
        return ""
    if len(repasses) == 1:
        r = repasses[0]
        x = f"{_dec(r.valor_total):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return (
            f"Tem R$ {x} de repasse da Vila. Quem levou: {r.quem_levou}. "
            "Confira se o dinheiro veio ou já foi usado; se foi usado, verificar com "
            f"{r.quem_levou} o motivo da retirada e fazer a retirada."
        )
    total = sum((_dec(r.valor_total) for r in repasses), ZERO)
    x = f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    nomes = []
    for r in repasses:
        if r.quem_levou and r.quem_levou not in nomes:
            nomes.append(r.quem_levou)
    quem = ", ".join(nomes) if nomes else "—"
    return (
        f"Tem R$ {x} de repasse da Vila ({len(repasses)} envios). Quem levou: {quem}. "
        "Confira se o dinheiro veio ou já foi usado; se foi usado, verificar com quem levou "
        "o motivo da retirada e fazer a retirada."
    )


def serializar_repasse(rep: RepasseVilaCentroAgro) -> dict[str, Any]:
    return {
        "id": rep.pk,
        "data_ref": rep.data_ref.isoformat(),
        "valor_total": float(_dec(rep.valor_total)),
        "valor_cmv": float(_dec(rep.valor_cmv)),
        "valor_lucro": float(_dec(rep.valor_lucro)),
        "valor_fiado": float(_dec(rep.valor_fiado)),
        "percentual_lucro": float(_dec(rep.percentual_lucro)),
        "lucro_bruto_dia": float(_dec(rep.lucro_bruto_dia)),
        "reserva_aplicada": float(_dec(getattr(rep, "reserva_aplicada", ZERO))),
        "lucro_penultimo_dia": float(_dec(getattr(rep, "lucro_penultimo_dia", ZERO))),
        "quem_levou": rep.quem_levou,
        "status_centro": rep.status_centro,
        "modo_dia_cheio": bool(rep.modo_dia_cheio),
        "criado_em": timezone.localtime(rep.criado_em).isoformat() if rep.criado_em else "",
    }
