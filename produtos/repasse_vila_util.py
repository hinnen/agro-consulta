"""Repasse Vila Elias → Centro: CMV + % lucro bruto + fiado pago na Vila."""
from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from django.db.models import Sum
from django.utils import timezone

from produtos.models import (
    FiadoBaixaAgro,
    ItemVendaAgro,
    MovimentoCaixa,
    RepasseVilaCentroAgro,
    RepasseVilaConfigAgro,
    VendaAgro,
)

ZERO = Decimal("0.00")
# Limite para transferência de dia atrasado (esqueci ontem / semana).
REPASSE_MAX_DIAS_ATRASO = 180
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


def calcular_disponivel(
    dia: date | None = None,
    *,
    percentual_lucro: Decimal | float | str | None = None,
    modo_dia_cheio: bool = False,
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

    cmv_alvo = base["cmv"]
    lucro_alvo = (lucro * pct / Decimal("100")).quantize(Decimal("0.01"))
    fiado_alvo = fiado_dia

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
    return {
        "ok": True,
        "data_ref": dia.isoformat(),
        "percentual_lucro": float(pct),
        "percentual_padrao": float(_dec(cfg.percentual_lucro_padrao)),
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
    lucro_ficou_vila = max(ZERO, (lucro_bruto_mes - lucro_enviado_mes).quantize(Decimal("0.01")))

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

    return {
        "ok": True,
        "ano": ano,
        "mes": mes,
        "total_mes": float(_dec(total_mes)),
        "lucro_bruto_mes": float(lucro_bruto_mes),
        "lucro_enviado_mes": float(_dec(lucro_enviado_mes)),
        "lucro_ficou_vila": float(lucro_ficou_vila),
        "dias": dias_out,
    }


def _obs_repasse(rep: RepasseVilaCentroAgro, lado: str) -> str:
    return (
        f"Repasse Vila→Centro #{rep.pk} · {lado} · quem={rep.quem_levou} · "
        f"{rep.data_ref.strftime('%d/%m/%Y')}"
    )[:500]


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
) -> tuple[RepasseVilaCentroAgro | None, str]:
    """
    Saída no caixa Vila (obrigatório aberto) + entrada no Centro (agora ou pendente).
    valor_manual: se informado, usa esse total (proporcional nas linhas marcadas).
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
            return None, "Não há valor disponível nas linhas marcadas para o valor manual."
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

    if total <= 0:
        return None, "Nada a levar em dinheiro (cartão/PIX já cobriu ou já enviado)."

    fn = normalizar_forma_pagamento_caixa(forma_pagamento or "Dinheiro")
    # Completar o dia = físico; se mandar eletrônico de novo, ainda registra no caixa
    user = (
        request.user
        if getattr(request, "user", None) and request.user.is_authenticated
        else None
    )

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
    mov_saida.observacao = _obs_repasse(rep, "saída Vila")
    mov_saida.save(update_fields=["observacao"])
    if mov_entrada:
        mov_entrada.observacao = _obs_repasse(rep, "entrada Centro")
        mov_entrada.save(update_fields=["observacao"])

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
        "quem_levou": rep.quem_levou,
        "status_centro": rep.status_centro,
        "modo_dia_cheio": bool(rep.modo_dia_cheio),
        "criado_em": timezone.localtime(rep.criado_em).isoformat() if rep.criado_em else "",
    }
