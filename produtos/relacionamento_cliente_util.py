"""Painel de relacionamento PDV (rascunho) — agregações por ClienteAgro."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from django.db.models import Avg, Count, Q, Sum
from django.utils import timezone

from produtos.fiado_credito_util import resumo_credito_fiado_cliente
from produtos.models import ClienteAgro, FiadoTituloAgro, ItemVendaAgro, VendaAgro

_RACAO_RE = re.compile(r"ra[çc][ãa]o|racao|racão|sache|sach[eê]|pet\s*food", re.I)
_KG_RE = re.compile(r"(\d+)\s*kg", re.I)

_CROSS_RULES: list[tuple[tuple[str, ...], tuple[str, ...]]] = [
    (("ração", "racao", "racão", "racao"), ("petisco", "sachê", "sache", "osso", "bisc")),
    (("ração", "racao", "racão"), ("antipulga", "carrapato", "vermífugo", "vermifugo", "simparic", "bravecto")),
    (("ração", "racao", "racão"), ("tapete", "higiênico", "higienico", "areia", "sílica", "silica")),
    (("gato", "felino"), ("areia", "sílica", "silica", "arranhador")),
    (("medicamento", "vermífugo", "vermifugo"), ("petisco", "sachê", "sache")),
]


def _dec(val) -> Decimal:
    try:
        return Decimal(str(val or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


def _vendas_cliente_qs(cli: ClienteAgro):
    nome = (cli.nome or "").strip()
    q = Q()
    if nome:
        q |= Q(cliente_nome__iexact=nome)
    ext = (cli.externo_id or "").strip()
    if ext:
        q |= Q(cliente_id_erp=ext)
    q |= Q(cliente_id_erp=f"local:{cli.pk}")
    if not q:
        return VendaAgro.objects.none()
    return (
        VendaAgro.objects.filter(q)
        .filter(devolvida_em__isnull=True)
        .order_by("-criado_em")
    )


def _dias_desde(dt) -> int | None:
    if not dt:
        return None
    d = dt.date() if hasattr(dt, "date") else dt
    return (timezone.localdate() - d).days


def _estimativa_dias_pacote(descricao: str, qtd: Decimal) -> int:
    m = _KG_RE.search(descricao or "")
    kg = int(m.group(1)) if m else 0
    if kg >= 20:
        base = 35
    elif kg >= 15:
        base = 28
    elif kg >= 10:
        base = 21
    elif kg >= 1:
        base = 14
    else:
        base = 30
    try:
        mult = max(float(qtd or 1), 1.0)
    except (TypeError, ValueError):
        mult = 1.0
    return int(base * mult)


def _top_produtos(venda_ids: list[int], limit: int = 12) -> list[dict[str, Any]]:
    if not venda_ids:
        return []
    agg = (
        ItemVendaAgro.objects.filter(venda_id__in=venda_ids)
        .values("codigo", "descricao")
        .annotate(
            vezes=Count("id"),
            qtd_total=Sum("quantidade"),
            ultimo_valor=Avg("valor_unitario"),
        )
        .order_by("-vezes", "-qtd_total")[:limit]
    )
    out = []
    for row in agg:
        codigo = (row.get("codigo") or "").strip()
        desc = (row.get("descricao") or "").strip()
        out.append(
            {
                "codigo": codigo,
                "descricao": desc,
                "vezes": int(row.get("vezes") or 0),
                "qtd_total": float(row.get("qtd_total") or 0),
                "preco_medio": float(row.get("ultimo_valor") or 0),
            }
        )
    return out


def _ciclo_racao(venda_ids: list[int]) -> list[dict[str, Any]]:
    if not venda_ids:
        return []
    itens = (
        ItemVendaAgro.objects.filter(venda_id__in=venda_ids)
        .select_related("venda")
        .order_by("-venda__criado_em")
    )
    por_chave: dict[str, list[tuple]] = defaultdict(list)
    for it in itens:
        desc = (it.descricao or "").strip()
        if not _RACAO_RE.search(desc):
            continue
        chave = (it.codigo or desc[:80]).strip().lower()
        por_chave[chave].append((it.venda.criado_em, it.quantidade, desc, it.codigo))

    out = []
    hoje = timezone.localdate()
    for _ch, hist in por_chave.items():
        hist.sort(key=lambda x: x[0], reverse=True)
        ult = hist[0]
        ult_dt = ult[0].date() if ult[0] else None
        dias = (hoje - ult_dt).days if ult_dt else None
        intervalos = []
        for i in range(len(hist) - 1):
            d1 = hist[i][0].date()
            d2 = hist[i + 1][0].date()
            intervalos.append((d1 - d2).days)
        media_intervalo = int(sum(intervalos) / len(intervalos)) if intervalos else None
        estimativa = _estimativa_dias_pacote(ult[2], ult[1])
        ref = media_intervalo or estimativa
        status = "ok"
        if dias is not None and ref:
            if dias > ref + 7:
                status = "atrasado"
            elif dias >= ref - 5:
                status = "recompra"
        out.append(
            {
                "codigo": (ult[3] or "").strip(),
                "descricao": ult[2],
                "ultima_qtd": float(ult[1] or 0),
                "ultima_compra": ult_dt.isoformat() if ult_dt else None,
                "dias_desde": dias,
                "media_intervalo_dias": media_intervalo,
                "estimativa_dias_pacote": estimativa,
                "status": status,
            }
        )
    out.sort(key=lambda x: (0 if x["status"] == "atrasado" else 1, -(x["dias_desde"] or 0)))
    return out[:8]


def _cross_sell(top_produtos: list[dict], venda_ids: list[int]) -> list[dict[str, Any]]:
    if not top_produtos or not venda_ids:
        return []
    textos_cliente = " ".join((p.get("descricao") or "").lower() for p in top_produtos)
    gatilhos: set[str] = set()
    for keys, _ in _CROSS_RULES:
        if any(k in textos_cliente for k in keys):
            gatilhos.update(keys)
    if not gatilhos:
        return []

    alvo_terms: set[str] = set()
    for keys, sugestoes in _CROSS_RULES:
        if any(k in gatilhos for k in keys):
            alvo_terms.update(s.lower() for s in sugestoes)

    codigos_cliente = {(p.get("codigo") or "").lower() for p in top_produtos}
    qs = (
        ItemVendaAgro.objects.exclude(venda_id__in=venda_ids[:200])
        .order_by("-venda__criado_em")
        .values("codigo", "descricao")[:8000]
    )
    vistos: set[str] = set()
    out = []
    for row in qs:
        desc = (row.get("descricao") or "").strip()
        cod = (row.get("codigo") or "").strip()
        if not desc or cod.lower() in codigos_cliente:
            continue
        dl = desc.lower()
        hit = next((t for t in alvo_terms if t in dl), None)
        if not hit:
            continue
        key = cod.lower() or dl[:60]
        if key in vistos:
            continue
        vistos.add(key)
        out.append(
            {
                "codigo": cod,
                "descricao": desc,
                "motivo": f"Complementar — loja vende «{hit}»",
            }
        )
        if len(out) >= 8:
            break
    return out


def _fiado_resumo(cli: ClienteAgro) -> dict[str, Any]:
    cid = (cli.externo_id or "").strip() or f"agro:{cli.pk}"
    try:
        cred = resumo_credito_fiado_cliente(cid, cliente_agro_pk=cli.pk)
    except Exception:
        cred = {}
    titulos = (
        FiadoTituloAgro.objects.filter(cliente_agro=cli)
        .exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        )
        .order_by("vencimento")[:12]
    )
    titulos_out = []
    hoje = timezone.localdate()
    for t in titulos:
        saldo = float(t.saldo_aberto)
        if saldo <= 0:
            continue
        venc = t.vencimento
        titulos_out.append(
            {
                "id": t.pk,
                "documento": t.numero_documento or str(t.pk),
                "vencimento": venc.isoformat() if venc else None,
                "vencido": bool(venc and venc < hoje),
                "saldo": saldo,
                "valor_bruto": float(t.valor_bruto or 0),
            }
        )
    return {
        "resumo_erp": cred,
        "limite_local": float(cli.limite_fiado_local or 0),
        "titulos_abertos": titulos_out,
        "total_aberto": round(sum(x["saldo"] for x in titulos_out), 2),
    }


def montar_painel_relacionamento_cliente(cliente_agro_pk: int) -> dict[str, Any]:
    cli = ClienteAgro.objects.filter(pk=cliente_agro_pk, ativo=True).first()
    if not cli:
        return {"ok": False, "erro": "Cliente não encontrado."}

    vendas_qs = _vendas_cliente_qs(cli)
    venda_ids = list(vendas_qs.values_list("pk", flat=True)[:120])
    vendas_recentes = list(vendas_qs.prefetch_related("itens")[:12])

    totais = vendas_qs.aggregate(n=Count("id"), soma=Sum("total"))
    n_vendas = int(totais.get("n") or 0)
    soma = _dec(totais.get("soma"))
    ticket = float((soma / n_vendas).quantize(Decimal("0.01"))) if n_vendas else 0.0

    ultima = vendas_qs.first()
    dias_ultima = _dias_desde(ultima.criado_em) if ultima else None

    datas = list(vendas_qs.values_list("criado_em", flat=True)[:24])
    freq_dias = None
    if len(datas) >= 2:
        gaps = []
        for i in range(len(datas) - 1):
            d1 = datas[i].date()
            d2 = datas[i + 1].date()
            gaps.append(abs((d1 - d2).days))
        if gaps:
            freq_dias = int(sum(gaps) / len(gaps))

    top = _top_produtos(venda_ids)
    historico_vendas = []
    for v in vendas_recentes:
        linhas = [
            {
                "codigo": (it.codigo or "").strip(),
                "descricao": (it.descricao or "").strip(),
                "qtd": float(it.quantidade or 0),
                "total": float(it.valor_total or 0),
            }
            for it in v.itens.all()[:20]
        ]
        historico_vendas.append(
            {
                "id": v.pk,
                "data": timezone.localtime(v.criado_em).strftime("%d/%m/%Y %H:%M"),
                "total": float(v.total or 0),
                "forma": (v.forma_pagamento or "").strip(),
                "itens": linhas,
            }
        )

    wa = (cli.whatsapp or "").strip()
    wa_digits = re.sub(r"\D", "", wa)
    wa_url = f"https://wa.me/55{wa_digits}" if len(wa_digits) >= 10 else ""

    return {
        "ok": True,
        "rascunho": True,
        "cliente": {
            "pk": cli.pk,
            "nome": cli.nome,
            "whatsapp": wa,
            "whatsapp_url": wa_url,
            "endereco": (cli.endereco or "").strip(),
            "saldo_cashback": float(cli.saldo_cashback or 0),
            "saldo_vale_credito": float(cli.saldo_vale_credito or 0),
        },
        "metricas": {
            "total_vendas": n_vendas,
            "ticket_medio": ticket,
            "frequencia_media_dias": freq_dias,
            "ultima_visita_dias": dias_ultima,
            "total_comprado": float(soma),
        },
        "historico_rapido": {
            "top_produtos": top,
            "vendas": historico_vendas,
        },
        "ciclo_racao": _ciclo_racao(venda_ids),
        "cross_sell": _cross_sell(top, venda_ids),
        "financeiro_fiado": _fiado_resumo(cli),
        "fidelidade": {
            "cashback": float(cli.saldo_cashback or 0),
            "vale_credito": float(cli.saldo_vale_credito or 0),
        },
    }
