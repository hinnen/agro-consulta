"""Mostruário META — metas manuais + média esperada (Meta C do BI)."""
from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from django.db import transaction

from produtos.vendas_lojas_util import (
    _q2,
    vendas_lojas_cmp_meta,
    vendas_lojas_cmp_meta_agora,
    vendas_lojas_meta_c_modos,
    vendas_lojas_total_deposito,
)

# Padrão do mês (Renan 05/10/2026) — seed quando competência ainda não tem faixa.
META_VENDA_PADRAO_FAIXAS: list[tuple[Decimal, Decimal, str]] = [
    (Decimal("105000.00"), Decimal("100.00"), ""),
    (Decimal("110000.00"), Decimal("150.00"), ""),
    (Decimal("115000.00"), Decimal("100.00"), "moleton"),
    (Decimal("120000.00"), Decimal("250.00"), ""),
    (Decimal("130000.00"), Decimal("500.00"), ""),
]


def meta_competencia_iso(d: date | None = None) -> str:
    alvo = d or date.today()
    return f"{alvo.year:04d}-{alvo.month:02d}"


def meta_parse_competencia(raw: str | None, fallback: date | None = None) -> str:
    s = (raw or "").strip()[:7]
    if len(s) == 7 and s[4] == "-":
        try:
            y, m = int(s[:4]), int(s[5:7])
            if 1 <= m <= 12 and 2000 <= y <= 2100:
                return f"{y:04d}-{m:02d}"
        except ValueError:
            pass
    return meta_competencia_iso(fallback)


def meta_competencia_bounds(competencia: str) -> tuple[date, date]:
    y, m = int(competencia[:4]), int(competencia[5:7])
    ini = date(y, m, 1)
    fim = date(y, m, monthrange(y, m)[1])
    return ini, fim


def meta_fmt_moeda(val) -> str:
    q = _q2(val)
    neg = q < 0
    q = abs(q)
    inteiro, _, frac = f"{q:.2f}".partition(".")
    partes = []
    while inteiro:
        partes.append(inteiro[-3:])
        inteiro = inteiro[:-3]
    corpo = ".".join(reversed(partes)) if partes else "0"
    s = f"R$ {corpo},{frac}"
    return f"-{s}" if neg else s


def meta_fmt_pct(val) -> str:
    if val is None:
        return "—"
    q = Decimal(str(val)).quantize(Decimal("0.1"))
    s = f"{q}".replace(".", ",")
    return f"{s}%"


def meta_bonus_label(bonus_valor, bonus_extra: str = "") -> str:
    extra = (bonus_extra or "").strip()
    bv = _q2(bonus_valor)
    if bv > 0 and extra:
        return f"{meta_fmt_moeda(bv)} + {extra}"
    if bv > 0:
        return meta_fmt_moeda(bv)
    if extra:
        return extra
    return "—"


def meta_parse_decimal(raw) -> Decimal | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float, Decimal)):
        try:
            return _q2(raw)
        except (InvalidOperation, ValueError):
            return None
    s = str(raw).strip()
    if not s:
        return None
    s = (
        s.replace("R$", "")
        .replace("r$", "")
        .replace(" ", "")
        .replace(".", "")
        .replace(",", ".")
    )
    try:
        return _q2(Decimal(s))
    except (InvalidOperation, ValueError):
        return None


def meta_ensure_faixas_padrao(competencia: str) -> int:
    """Se a competência não tem faixa, cria o padrão. Retorna quantas criou."""
    from produtos.models import MetaVendaFaixaAgro

    comp = meta_parse_competencia(competencia)
    if MetaVendaFaixaAgro.objects.filter(competencia=comp).exists():
        return 0
    objs = [
        MetaVendaFaixaAgro(
            competencia=comp,
            valor_meta=vm,
            bonus_valor=bv,
            bonus_extra=be,
            ordem=i,
        )
        for i, (vm, bv, be) in enumerate(META_VENDA_PADRAO_FAIXAS)
    ]
    MetaVendaFaixaAgro.objects.bulk_create(objs)
    return len(objs)


def meta_listar_faixas(competencia: str) -> list[dict]:
    from produtos.models import MetaVendaFaixaAgro

    comp = meta_parse_competencia(competencia)
    meta_ensure_faixas_padrao(comp)
    rows = []
    for f in MetaVendaFaixaAgro.objects.filter(competencia=comp).order_by(
        "ordem", "valor_meta", "id"
    ):
        rows.append(
            {
                "id": f.id,
                "competencia": f.competencia,
                "valor_meta": float(_q2(f.valor_meta)),
                "valor_meta_fmt": meta_fmt_moeda(f.valor_meta),
                "bonus_valor": float(_q2(f.bonus_valor)),
                "bonus_valor_fmt": meta_fmt_moeda(f.bonus_valor),
                "bonus_extra": (f.bonus_extra or "").strip(),
                "bonus_label": meta_bonus_label(f.bonus_valor, f.bonus_extra),
                "ordem": int(f.ordem or 0),
            }
        )
    return rows


def _cmp_bloco(cmp: dict) -> dict:
    sentido = cmp.get("sentido") or "sem"
    diff = cmp.get("diff")
    pct = cmp.get("pct")
    pct_signed = cmp.get("pct_signed")
    esperado = cmp.get("esperado")
    return {
        "esperado": _q2(esperado) if esperado is not None else Decimal("0"),
        "esperado_fmt": meta_fmt_moeda(esperado) if esperado is not None else "—",
        "diff": _q2(diff) if diff is not None else None,
        "diff_fmt": meta_fmt_moeda(diff) if diff is not None else "—",
        "pct": pct,
        "pct_fmt": meta_fmt_pct(pct) if pct is not None else "—",
        "pct_signed": pct_signed,
        "pct_signed_fmt": meta_fmt_pct(pct_signed) if pct_signed is not None else "—",
        "sentido": sentido,
        "sentido_label": {
            "acima": "acima",
            "abaixo": "abaixo",
            "igual": "no ponto",
            "sem": "sem base",
        }.get(sentido, "sem base"),
    }


def _progresso_meta(vendido, valor_meta) -> dict:
    vm = _q2(valor_meta)
    vd = _q2(vendido)
    if vm <= 0:
        return {
            "valor_meta": vm,
            "valor_meta_fmt": meta_fmt_moeda(vm),
            "falta": None,
            "falta_fmt": "—",
            "pct": None,
            "pct_fmt": "—",
            "atingida": False,
            "excedente": None,
            "excedente_fmt": "—",
        }
    falta = (vm - vd).quantize(Decimal("0.01"))
    pct = (vd / vm * Decimal("100")).quantize(Decimal("0.1"))
    atingida = vd >= vm
    excedente = (vd - vm).quantize(Decimal("0.01")) if atingida else None
    return {
        "valor_meta": vm,
        "valor_meta_fmt": meta_fmt_moeda(vm),
        "falta": falta if not atingida else Decimal("0.00"),
        "falta_fmt": meta_fmt_moeda(0 if atingida else falta),
        "pct": pct,
        "pct_fmt": meta_fmt_pct(pct),
        "atingida": atingida,
        "excedente": excedente,
        "excedente_fmt": meta_fmt_moeda(excedente) if excedente is not None else "—",
    }


def _json_deep(obj):
    if isinstance(obj, dict):
        return {k: _json_deep(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_deep(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def meta_montar_mostruario(
    *,
    competencia: str,
    hoje: date,
    agora: datetime,
    deposito: str | None = None,
) -> dict:
    """
    Painel pra postar no grupo:
    - hoje (vendido × média esperada do dia)
    - mês até hoje (ou mês fechado se competência passada)
    - faixas de meta manuais × vendido do mês
    """
    from produtos.views import _dashboard_serie_meta_c_vendas

    comp = meta_parse_competencia(competencia, hoje)
    mes_ini, mes_fim = meta_competencia_bounds(comp)
    dep = deposito if deposito in ("centro", "vila") else None

    mes_atual = meta_competencia_iso(hoje) == comp
    mes_fechado = (not mes_atual) or (hoje >= mes_fim)
    fim_vendido = min(hoje, mes_fim) if mes_atual else mes_fim
    if fim_vendido < mes_ini:
        fim_vendido = mes_ini

    vendido_mes = vendas_lojas_total_deposito(mes_ini, fim_vendido, dep)
    vendido_hoje = (
        vendas_lojas_total_deposito(hoje, hoje, dep) if mes_atual else Decimal("0.00")
    )

    # Média esperada — Meta C (mesma do BI / vendas-lojas)
    serie_mes = _dashboard_serie_meta_c_vendas(mes_ini, mes_fim, deposito=dep)
    meta_mes_cheia = _q2(sum(float(x or 0) for x in serie_mes))

    if mes_atual:
        n_ate = (hoje - mes_ini).days + 1
        serie_ate = serie_mes[: max(0, n_ate)]
        media_dia_todo, media_ate_agora, _ = vendas_lojas_meta_c_modos(
            mes_ini, hoje, dep, hoje=hoje, agora=agora
        )
        # Só o pedaço «até agora» da série do mês (já coberta por modos no intervalo mes_ini..hoje)
        _ = serie_ate  # mantém clareza; modos recalcula série — ok
        cmp_media_mes = _cmp_bloco(
            vendas_lojas_cmp_meta_agora(vendido_mes, media_ate_agora, media_dia_todo)
        )
        media_mes_ref = media_ate_agora
        media_mes_dia = media_dia_todo

        dia_todo, dia_agora, _ = vendas_lojas_meta_c_modos(
            hoje, hoje, dep, hoje=hoje, agora=agora
        )
        cmp_media_hoje = _cmp_bloco(
            vendas_lojas_cmp_meta_agora(vendido_hoje, dia_agora, dia_todo)
        )
        bloco_hoje = {
            "ativo": True,
            "data": hoje.isoformat(),
            "data_fmt": hoje.strftime("%d/%m/%Y"),
            "vendido": vendido_hoje,
            "vendido_fmt": meta_fmt_moeda(vendido_hoje),
            "media_ate_agora": _q2(dia_agora),
            "media_ate_agora_fmt": meta_fmt_moeda(dia_agora),
            "media_dia": _q2(dia_todo),
            "media_dia_fmt": meta_fmt_moeda(dia_todo),
            "vs_media": cmp_media_hoje,
        }
    else:
        cmp_media_mes = _cmp_bloco(vendas_lojas_cmp_meta(vendido_mes, meta_mes_cheia))
        media_mes_ref = meta_mes_cheia
        media_mes_dia = meta_mes_cheia
        bloco_hoje = {"ativo": False}

    faixas_raw = meta_listar_faixas(comp)
    faixas = []
    proxima = None
    ultima_batida = None
    for f in faixas_raw:
        prog = _progresso_meta(vendido_mes, f["valor_meta"])
        item = {**f, **prog}
        faixas.append(item)
        if prog["atingida"]:
            ultima_batida = item
        elif proxima is None:
            proxima = item

    out = {
        "competencia": comp,
        "competencia_fmt": f"{mes_ini.strftime('%m/%Y')}",
        "mes_ini": mes_ini.isoformat(),
        "mes_fim": mes_fim.isoformat(),
        "mes_atual": mes_atual,
        "mes_fechado": mes_fechado,
        "fim_vendido": fim_vendido.isoformat(),
        "fim_vendido_fmt": fim_vendido.strftime("%d/%m/%Y"),
        "deposito": dep or "todas",
        "vendido_mes": vendido_mes,
        "vendido_mes_fmt": meta_fmt_moeda(vendido_mes),
        "media_mes_cheia": meta_mes_cheia,
        "media_mes_cheia_fmt": meta_fmt_moeda(meta_mes_cheia),
        "media_mes_ref": _q2(media_mes_ref),
        "media_mes_ref_fmt": meta_fmt_moeda(media_mes_ref),
        "media_mes_dia": _q2(media_mes_dia),
        "media_mes_dia_fmt": meta_fmt_moeda(media_mes_dia),
        "vs_media_mes": cmp_media_mes,
        "hoje": bloco_hoje,
        "faixas": faixas,
        "proxima_meta": proxima,
        "ultima_batida": ultima_batida,
    }
    return _json_deep(out)


def meta_texto_zap(mostruario: dict) -> str:
    """Texto pronto pra colar no grupo."""
    linhas = [
        f"🎯 *META — {mostruario.get('competencia_fmt', '')}*",
        "",
    ]
    hoje = mostruario.get("hoje") or {}
    if hoje.get("ativo"):
        vs = hoje.get("vs_media") or {}
        linhas.extend(
            [
                f"📅 *HOJE ({hoje.get('data_fmt', '')})*",
                f"Vendido: *{hoje.get('vendido_fmt', '—')}*",
                f"Média esperada (até agora): {hoje.get('media_ate_agora_fmt', '—')}",
                f"vs média: *{vs.get('diff_fmt', '—')}* ({vs.get('pct_signed_fmt', '—')} {vs.get('sentido_label', '')})",
                "",
            ]
        )

    rotulo_mes = "MÊS FECHADO" if mostruario.get("mes_fechado") and not mostruario.get("mes_atual") else "MÊS ATÉ HOJE"
    vs_m = mostruario.get("vs_media_mes") or {}
    linhas.extend(
        [
            f"🗓 *{rotulo_mes}* (até {mostruario.get('fim_vendido_fmt', '')})",
            f"Vendido: *{mostruario.get('vendido_mes_fmt', '—')}*",
            f"Média esperada: {mostruario.get('media_mes_ref_fmt', '—')}",
            f"vs média: *{vs_m.get('diff_fmt', '—')}* ({vs_m.get('pct_signed_fmt', '—')} {vs_m.get('sentido_label', '')})",
            "",
            "🏁 *Metas do mês*",
        ]
    )
    for f in mostruario.get("faixas") or []:
        if f.get("atingida"):
            status = f"✅ batida · +{f.get('excedente_fmt', '—')}"
        else:
            status = f"faltam {f.get('falta_fmt', '—')} ({f.get('pct_fmt', '—')})"
        linhas.append(
            f"• {f.get('valor_meta_fmt', '—')} · bônus {f.get('bonus_label', '—')} — {status}"
        )
    prox = mostruario.get("proxima_meta")
    if prox:
        linhas.extend(
            [
                "",
                f"👉 Próxima: *{prox.get('valor_meta_fmt')}* — faltam *{prox.get('falta_fmt')}*",
            ]
        )
    elif mostruario.get("ultima_batida"):
        linhas.extend(["", "🏆 Todas as metas batidas!"])
    return "\n".join(linhas).strip() + "\n"


@transaction.atomic
def meta_salvar_faixa(
    *,
    competencia: str,
    valor_meta,
    bonus_valor=0,
    bonus_extra: str = "",
    faixa_id: int | None = None,
    ordem: int | None = None,
) -> dict:
    from produtos.models import MetaVendaFaixaAgro

    comp = meta_parse_competencia(competencia)
    vm = meta_parse_decimal(valor_meta)
    if vm is None or vm <= 0:
        raise ValueError("Informe o valor da meta de venda.")
    bv = meta_parse_decimal(bonus_valor)
    if bv is None:
        bv = Decimal("0.00")
    if bv < 0:
        raise ValueError("Bônus em R$ não pode ser negativo.")
    extra = (bonus_extra or "").strip()[:120]

    if faixa_id:
        f = MetaVendaFaixaAgro.objects.select_for_update().get(pk=int(faixa_id), competencia=comp)
        f.valor_meta = vm
        f.bonus_valor = bv
        f.bonus_extra = extra
        if ordem is not None:
            f.ordem = max(0, int(ordem))
        f.save()
    else:
        if ordem is None:
            ult = (
                MetaVendaFaixaAgro.objects.filter(competencia=comp)
                .order_by("-ordem")
                .values_list("ordem", flat=True)
                .first()
            )
            ordem = int(ult or 0) + 1
        f = MetaVendaFaixaAgro.objects.create(
            competencia=comp,
            valor_meta=vm,
            bonus_valor=bv,
            bonus_extra=extra,
            ordem=max(0, int(ordem)),
        )
    return {
        "id": f.id,
        "competencia": f.competencia,
        "valor_meta": float(_q2(f.valor_meta)),
        "bonus_valor": float(_q2(f.bonus_valor)),
        "bonus_extra": f.bonus_extra,
        "ordem": f.ordem,
    }


@transaction.atomic
def meta_excluir_faixa(*, competencia: str, faixa_id: int) -> None:
    from produtos.models import MetaVendaFaixaAgro

    comp = meta_parse_competencia(competencia)
    MetaVendaFaixaAgro.objects.filter(pk=int(faixa_id), competencia=comp).delete()
