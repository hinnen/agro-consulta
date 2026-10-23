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


def meta_deposito_label(deposito: str | None) -> str:
    if deposito == "centro":
        return "Centro"
    if deposito == "vila":
        return "Vila"
    return "Centro + Vila"


def meta_normalizar_deposito(raw: str | None) -> str:
    d = (raw or "").strip().lower()
    if d in ("centro", "vila"):
        return d
    return "centro"


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


def meta_ensure_faixas_padrao(competencia: str, deposito: str = "centro") -> int:
    """Se a competência+loja não tem faixa, cria o padrão. Retorna quantas criou."""
    from produtos.models import MetaVendaFaixaAgro

    comp = meta_parse_competencia(competencia)
    dep = meta_normalizar_deposito(deposito)
    if MetaVendaFaixaAgro.objects.filter(competencia=comp, deposito=dep).exists():
        return 0
    objs = [
        MetaVendaFaixaAgro(
            competencia=comp,
            deposito=dep,
            valor_meta=vm,
            bonus_valor=bv,
            bonus_extra=be,
            ordem=i,
        )
        for i, (vm, bv, be) in enumerate(META_VENDA_PADRAO_FAIXAS)
    ]
    MetaVendaFaixaAgro.objects.bulk_create(objs)
    return len(objs)


def _meta_faixa_row_dict(f) -> dict:
    dep = meta_normalizar_deposito(getattr(f, "deposito", None))
    return {
        "id": f.id,
        "competencia": f.competencia,
        "deposito": dep,
        "valor_meta": float(_q2(f.valor_meta)),
        "valor_meta_fmt": meta_fmt_moeda(f.valor_meta),
        "bonus_valor": float(_q2(f.bonus_valor)),
        "bonus_valor_fmt": meta_fmt_moeda(f.bonus_valor),
        "bonus_extra": (f.bonus_extra or "").strip(),
        "bonus_label": meta_bonus_label(f.bonus_valor, f.bonus_extra),
        "ordem": int(f.ordem or 0),
    }


def _meta_bonus_label_mesclado(fc, fv) -> str:
    partes = []
    if fc is not None:
        lbl = meta_bonus_label(fc.bonus_valor, fc.bonus_extra)
        if lbl != "—":
            partes.append(f"Centro {lbl}")
    if fv is not None:
        lbl = meta_bonus_label(fv.bonus_valor, fv.bonus_extra)
        if lbl != "—":
            partes.append(f"Vila {lbl}")
    if partes:
        return " · ".join(partes)
    return "—"


def _meta_mesclar_faixas_lojas(c_rows: list, v_rows: list) -> list[dict]:
    """Centro + Vila: soma metas por ordem; bônus exibidos por loja."""
    by_c = {int(f.ordem or 0): f for f in c_rows}
    by_v = {int(f.ordem or 0): f for f in v_rows}
    ordens = sorted(set(by_c) | set(by_v))
    out: list[dict] = []
    for ordem in ordens:
        fc = by_c.get(ordem)
        fv = by_v.get(ordem)
        vm = _q2(fc.valor_meta if fc else 0) + _q2(fv.valor_meta if fv else 0)
        if vm <= 0 and fc is None and fv is None:
            continue
        bv = _q2(fc.bonus_valor if fc else 0) + _q2(fv.bonus_valor if fv else 0)
        out.append(
            {
                "id": None,
                "competencia": (fc or fv).competencia,
                "deposito": "todas",
                "valor_meta": float(vm),
                "valor_meta_fmt": meta_fmt_moeda(vm),
                "bonus_valor": float(bv),
                "bonus_valor_fmt": meta_fmt_moeda(bv),
                "bonus_extra": "",
                "bonus_label": _meta_bonus_label_mesclado(fc, fv),
                "ordem": ordem,
            }
        )
    return out


def meta_listar_faixas(competencia: str, deposito: str | None = None) -> list[dict]:
    from produtos.models import MetaVendaFaixaAgro

    comp = meta_parse_competencia(competencia)
    dep = deposito if deposito in ("centro", "vila") else None

    if dep:
        meta_ensure_faixas_padrao(comp, dep)
        qs = MetaVendaFaixaAgro.objects.filter(competencia=comp, deposito=dep).order_by(
            "ordem", "valor_meta", "id"
        )
        return [_meta_faixa_row_dict(f) for f in qs]

    meta_ensure_faixas_padrao(comp, "centro")
    meta_ensure_faixas_padrao(comp, "vila")
    c_rows = list(
        MetaVendaFaixaAgro.objects.filter(competencia=comp, deposito="centro").order_by(
            "ordem", "valor_meta", "id"
        )
    )
    v_rows = list(
        MetaVendaFaixaAgro.objects.filter(competencia=comp, deposito="vila").order_by(
            "ordem", "valor_meta", "id"
        )
    )
    return _meta_mesclar_faixas_lojas(c_rows, v_rows)


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


def _fracao_ate_agora(media_ref, media_cheia) -> Decimal:
    """Quanto da média do mês já 'deveria' ter passado (0–1)."""
    cheia = _q2(media_cheia)
    ref = _q2(media_ref)
    if cheia <= 0:
        return Decimal("1")
    frac = (ref / cheia).quantize(Decimal("0.0001"))
    if frac < 0:
        return Decimal("0")
    if frac > 1:
        return Decimal("1")
    return frac


def _progresso_meta_modos(vendido, valor_meta, fracao_ate_agora: Decimal) -> dict:
    """
    Duas leituras da mesma faixa:
    - mes: vendido × meta cheia do mês
    - agora: vendido × parcela da meta até agora (ritmo da média esperada)
    """
    prog_mes = _progresso_meta(vendido, valor_meta)
    # Fração 0–1: 4 casas (não _q2 — arredondava 0,17 e distorcia a parcela).
    frac = Decimal(str(fracao_ate_agora)).quantize(Decimal("0.0001"))
    meta_agora = (_q2(valor_meta) * frac).quantize(Decimal("0.01"))
    prog_agora = _progresso_meta(vendido, meta_agora)
    return {
        **prog_mes,
        "meta_agora": meta_agora,
        "meta_agora_fmt": meta_fmt_moeda(meta_agora),
        "agora": prog_agora,
        "fracao_ate_agora": float(frac),
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

    faixas_raw = meta_listar_faixas(comp, dep)
    fracao = _fracao_ate_agora(media_mes_ref, meta_mes_cheia)
    faixas = []
    proxima = None
    ultima_batida = None
    proxima_agora = None
    ultima_batida_agora = None
    for f in faixas_raw:
        prog = _progresso_meta_modos(vendido_mes, f["valor_meta"], fracao)
        item = {**f, **prog}
        faixas.append(item)
        if prog["atingida"]:
            ultima_batida = item
        elif proxima is None:
            proxima = item
        agora = prog.get("agora") or {}
        if agora.get("atingida"):
            ultima_batida_agora = item
        elif proxima_agora is None:
            proxima_agora = item

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
        "deposito_label": meta_deposito_label(dep),
        "vendido_mes": vendido_mes,
        "vendido_mes_fmt": meta_fmt_moeda(vendido_mes),
        "media_mes_cheia": meta_mes_cheia,
        "media_mes_cheia_fmt": meta_fmt_moeda(meta_mes_cheia),
        "media_mes_ref": _q2(media_mes_ref),
        "media_mes_ref_fmt": meta_fmt_moeda(media_mes_ref),
        "media_mes_dia": _q2(media_mes_dia),
        "media_mes_dia_fmt": meta_fmt_moeda(media_mes_dia),
        "fracao_ate_agora": float(fracao),
        "fracao_ate_agora_pct_fmt": meta_fmt_pct((fracao * Decimal("100")).quantize(Decimal("0.1"))),
        "vs_media_mes": cmp_media_mes,
        "hoje": bloco_hoje,
        "faixas": faixas,
        "proxima_meta": proxima,
        "ultima_batida": ultima_batida,
        "proxima_meta_agora": proxima_agora,
        "ultima_batida_agora": ultima_batida_agora,
    }
    return _json_deep(out)


def _meta_valor_curto_de_fmt(vm_fmt: str) -> str:
    """``R$ 110.000,00`` → ``110.000`` (frase leiga no subtítulo)."""
    s = (vm_fmt or "").replace("R$", "").strip()
    if s.endswith(",00"):
        s = s[:-3]
    return s.strip() or "—"


def _meta_subtitulo_zap(mostruario: dict, modo_agora: bool) -> str:
    if not modo_agora:
        return "🏁 Meta do mês"
    prox = mostruario.get("proxima_meta_agora")
    if prox and prox.get("valor_meta_fmt"):
        curto = _meta_valor_curto_de_fmt(prox["valor_meta_fmt"])
        return (
            f"🏁 Ritmo até agora (ou seja, se mantemos esse ritmo a meta {curto} será batida)"
        )
    if mostruario.get("ultima_batida_agora"):
        return "🏁 Ritmo até agora — metas no caminho ✅"
    return "🏁 Ritmo até agora"


def _meta_linha_zap_enxuta(f: dict, *, modo_agora: bool) -> str:
    """Uma faixa no padrão enxuto (ex.: ``• R$ 105.000,00 · bônus R$ 100,00 ✅``)."""
    vm = f.get("valor_meta_fmt") or "—"
    bl = (f.get("bonus_label") or "").strip()
    meio = f" · bônus {bl}" if bl and bl != "—" else ""
    prog = (f.get("agora") or {}) if modo_agora else f
    suf = ""
    if prog.get("atingida"):
        ex_fmt = (prog.get("excedente_fmt") or "").strip()
        ex = prog.get("excedente")
        try:
            tem_exc = ex is not None and float(ex) > 0.005
        except (TypeError, ValueError):
            tem_exc = bool(ex_fmt and ex_fmt != "—")
        if tem_exc and ex_fmt:
            suf = f" ✅ +{ex_fmt}"
        else:
            suf = " ✅"
    return f"• {vm}{meio}{suf}"


def meta_texto_zap(mostruario: dict, modo: str = "mes") -> str:
    """Texto enxuto pro grupo Zap — cabeçalho + faixas (sem HOJE/MÊS/parcela/%)."""
    modo_agora = (modo or "mes").strip().lower() in ("agora", "ate_agora", "ritmo")
    faixas = mostruario.get("faixas") or []
    if not faixas:
        return ""
    comp = (mostruario.get("competencia_fmt") or "").strip()
    loja = (mostruario.get("deposito_label") or "").strip()
    titulo = f"🎯 META — {comp} · {loja}".strip(" ·")
    partes = [titulo, "", _meta_subtitulo_zap(mostruario, modo_agora), ""]
    partes.extend(_meta_linha_zap_enxuta(f, modo_agora=modo_agora) for f in faixas)
    return "\n".join(partes).strip() + "\n"


def _meta_dividir_meio_lojas(total: Decimal) -> tuple[Decimal, Decimal]:
    """Metade para Centro e resto para Vila (centavos)."""
    t = _q2(total)
    if t <= 0:
        return Decimal("0.00"), Decimal("0.00")
    metade = (t / Decimal("2")).quantize(Decimal("0.01"))
    return metade, t - metade


def _meta_proxima_ordem(comp: str) -> int:
    from produtos.models import MetaVendaFaixaAgro

    ult_c = (
        MetaVendaFaixaAgro.objects.filter(competencia=comp, deposito="centro")
        .order_by("-ordem")
        .values_list("ordem", flat=True)
        .first()
    )
    ult_v = (
        MetaVendaFaixaAgro.objects.filter(competencia=comp, deposito="vila")
        .order_by("-ordem")
        .values_list("ordem", flat=True)
        .first()
    )
    return max(int(ult_c or 0), int(ult_v or 0)) + 1


def _meta_upsert_faixa_loja(
    comp: str,
    deposito: str,
    ordem: int,
    vm: Decimal,
    bv: Decimal,
    extra: str,
) -> None:
    from produtos.models import MetaVendaFaixaAgro

    dep = meta_normalizar_deposito(deposito)
    f = (
        MetaVendaFaixaAgro.objects.select_for_update()
        .filter(competencia=comp, deposito=dep, ordem=ordem)
        .first()
    )
    if f:
        f.valor_meta = vm
        f.bonus_valor = bv
        f.bonus_extra = extra
        f.save()
    else:
        MetaVendaFaixaAgro.objects.create(
            competencia=comp,
            deposito=dep,
            valor_meta=vm,
            bonus_valor=bv,
            bonus_extra=extra,
            ordem=ordem,
        )


@transaction.atomic
def meta_salvar_faixa_todas(
    *,
    competencia: str,
    valor_meta,
    bonus_valor=0,
    bonus_extra: str = "",
    ordem: int | None = None,
) -> dict:
    """Salva meta somada: divide valor e bônus em 50/50 entre Centro e Vila."""
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
    ord = int(ordem) if ordem is not None else _meta_proxima_ordem(comp)
    ord = max(0, ord)
    c_vm, v_vm = _meta_dividir_meio_lojas(vm)
    c_bv, v_bv = _meta_dividir_meio_lojas(bv)
    _meta_upsert_faixa_loja(comp, "centro", ord, c_vm, c_bv, extra)
    _meta_upsert_faixa_loja(comp, "vila", ord, v_vm, v_bv, extra)
    merged = [x for x in meta_listar_faixas(comp, None) if int(x.get("ordem") or 0) == ord]
    return merged[0] if merged else {"competencia": comp, "deposito": "todas", "ordem": ord}


@transaction.atomic
def meta_salvar_faixa(
    *,
    competencia: str,
    valor_meta,
    bonus_valor=0,
    bonus_extra: str = "",
    faixa_id: int | None = None,
    ordem: int | None = None,
    deposito: str = "centro",
) -> dict:
    from produtos.models import MetaVendaFaixaAgro

    comp = meta_parse_competencia(competencia)
    dep_raw = (deposito or "centro").strip().lower()
    if dep_raw == "todas":
        return meta_salvar_faixa_todas(
            competencia=competencia,
            valor_meta=valor_meta,
            bonus_valor=bonus_valor,
            bonus_extra=bonus_extra,
            ordem=ordem,
        )
    dep = meta_normalizar_deposito(dep_raw)
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
        f.deposito = dep
        if ordem is not None:
            f.ordem = max(0, int(ordem))
        f.save()
    else:
        if ordem is None:
            ult = (
                MetaVendaFaixaAgro.objects.filter(competencia=comp, deposito=dep)
                .order_by("-ordem")
                .values_list("ordem", flat=True)
                .first()
            )
            ordem = int(ult or 0) + 1
        f = MetaVendaFaixaAgro.objects.create(
            competencia=comp,
            deposito=dep,
            valor_meta=vm,
            bonus_valor=bv,
            bonus_extra=extra,
            ordem=max(0, int(ordem)),
        )
    row = _meta_faixa_row_dict(f)
    return {
        "id": row["id"],
        "competencia": row["competencia"],
        "deposito": row["deposito"],
        "valor_meta": row["valor_meta"],
        "bonus_valor": row["bonus_valor"],
        "bonus_extra": row["bonus_extra"],
        "ordem": row["ordem"],
    }


@transaction.atomic
def meta_excluir_faixa(
    *,
    competencia: str,
    faixa_id: int | None = None,
    deposito: str | None = None,
    ordem: int | None = None,
) -> None:
    from produtos.models import MetaVendaFaixaAgro

    comp = meta_parse_competencia(competencia)
    dep_raw = (deposito or "").strip().lower()
    if dep_raw == "todas":
        if ordem is None:
            raise ValueError("Informe a ordem da faixa.")
        MetaVendaFaixaAgro.objects.filter(
            competencia=comp, ordem=max(0, int(ordem))
        ).delete()
        return
    if faixa_id is None:
        raise ValueError("Faixa inválida.")
    MetaVendaFaixaAgro.objects.filter(pk=int(faixa_id), competencia=comp).delete()
