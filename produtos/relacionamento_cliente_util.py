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
from produtos.models import (
    ClienteAgro,
    FiadoTituloAgro,
    ItemVendaAgro,
    RelacionamentoItemHistoricoErpAgro,
    RelacionamentoVendaHistoricoErpAgro,
    VendaAgro,
)
from produtos.relacionamento_historico_erp_util import (
    codigos_gm_ativos_no_catalogo,
    normalizar_codigo_gm_rel,
    rel_historico_erp_habilitado,
    rel_pdv_sisvale_desde,
    resumo_historico_erp_cliente,
)

_REL_EXTRAS_VAZIO: dict[str, Any] = {"pets": [], "lembretes": [], "anotacoes": ""}
_MAX_PETS = 20
_MAX_LEMBRETES = 50
_MAX_ANOTACOES = 8000
HISTORICO_PAGE_SIZE = 12
_TOP_VENDAS_IDS_LIMIT = 150


def _clip_str(val: Any, max_len: int) -> str:
    return str(val or "").strip()[:max_len]


def _parse_data_iso(val: Any) -> date | None:
    s = _clip_str(val, 10)
    if not s or len(s) < 10:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def normalizar_relacionamento_extras(raw: Any) -> dict[str, Any]:
    """Sanitiza pets/lembretes/anotações para gravar ou devolver ao PDV."""
    base = dict(_REL_EXTRAS_VAZIO)
    if not isinstance(raw, dict):
        return base
    hoje = timezone.localdate()

    pets_out: list[dict[str, str]] = []
    for p in raw.get("pets") or []:
        if not isinstance(p, dict):
            continue
        nome = _clip_str(p.get("nome"), 80)
        if not nome:
            continue
        pets_out.append(
            {
                "nome": nome,
                "raca": _clip_str(p.get("raca"), 80),
                "porte": _clip_str(p.get("porte"), 20),
                "idade": _clip_str(p.get("idade"), 40),
            }
        )
        if len(pets_out) >= _MAX_PETS:
            break

    lemb_out: list[dict[str, Any]] = []
    for r in raw.get("lembretes") or []:
        if not isinstance(r, dict):
            continue
        dt = _parse_data_iso(r.get("data"))
        if not dt:
            continue
        lemb_out.append(
            {
                "tipo": _clip_str(r.get("tipo"), 40) or "Outro",
                "produto": _clip_str(r.get("produto"), 120),
                "data": dt.isoformat(),
                "vencido": dt < hoje,
            }
        )
        if len(lemb_out) >= _MAX_LEMBRETES:
            break

    base["pets"] = pets_out
    base["lembretes"] = lemb_out
    base["anotacoes"] = _clip_str(raw.get("anotacoes"), _MAX_ANOTACOES)
    return base


def ler_relacionamento_extras_cliente(cli: ClienteAgro) -> dict[str, Any]:
    raw = getattr(cli, "relacionamento_extras_json", None)
    return normalizar_relacionamento_extras(raw if isinstance(raw, dict) else {})


def salvar_relacionamento_extras_cliente(cli: ClienteAgro, payload: Any) -> dict[str, Any]:
    data = normalizar_relacionamento_extras(payload)
    cli.relacionamento_extras_json = data
    cli.save(update_fields=["relacionamento_extras_json", "atualizado_em"])
    return data

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
    qs = (
        VendaAgro.objects.filter(q)
        .filter(devolvida_em__isnull=True)
        .order_by("-criado_em")
    )
    pdv_desde = rel_pdv_sisvale_desde()
    if pdv_desde:
        qs = qs.filter(criado_em__date__gte=pdv_desde)
    return qs


def _vendas_historico_erp_qs(cli: ClienteAgro):
    if not rel_historico_erp_habilitado():
        return RelacionamentoVendaHistoricoErpAgro.objects.none()
    return RelacionamentoVendaHistoricoErpAgro.objects.filter(cliente_agro=cli).order_by("-data_venda")


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


def _top_produtos(venda_ids: list[int], hist_venda_pks: list[int] | None = None, limit: int = 12) -> list[dict[str, Any]]:
    acc: dict[str, dict[str, Any]] = {}
    hist_venda_pks = hist_venda_pks or []

    def ingest(codigo: str, descricao: str, qtd: float, preco: float, vezes: int = 1) -> None:
        cod = (codigo or "").strip()
        desc = (descricao or "").strip()
        chave = (cod or desc[:80]).lower()
        if not chave:
            return
        row = acc.get(chave)
        if not row:
            row = {
                "codigo": cod,
                "descricao": desc,
                "vezes": 0,
                "qtd_total": 0.0,
                "preco_sum": 0.0,
                "preco_n": 0,
            }
            acc[chave] = row
        row["vezes"] += int(vezes or 1)
        row["qtd_total"] += float(qtd or 0)
        if preco:
            row["preco_sum"] += float(preco)
            row["preco_n"] += 1
        if cod and not row.get("codigo"):
            row["codigo"] = cod
        if desc and (not row.get("descricao") or len(desc) > len(row.get("descricao") or "")):
            row["descricao"] = desc

    if venda_ids:
        agg = (
            ItemVendaAgro.objects.filter(venda_id__in=venda_ids)
            .values("codigo", "descricao")
            .annotate(
                vezes=Count("id"),
                qtd_total=Sum("quantidade"),
                ultimo_valor=Avg("valor_unitario"),
            )
        )
        for row in agg:
            ingest(
                (row.get("codigo") or "").strip(),
                (row.get("descricao") or "").strip(),
                float(row.get("qtd_total") or 0),
                float(row.get("ultimo_valor") or 0),
                int(row.get("vezes") or 0),
            )

    if hist_venda_pks:
        for row in (
            RelacionamentoItemHistoricoErpAgro.objects.filter(venda_id__in=hist_venda_pks)
            .values("codigo_gm", "descricao")
            .annotate(
                vezes=Count("id"),
                qtd_total=Sum("quantidade"),
                ultimo_valor=Avg("valor_unitario"),
            )
        ):
            ingest(
                (row.get("codigo_gm") or "").strip(),
                (row.get("descricao") or "").strip(),
                float(row.get("qtd_total") or 0),
                float(row.get("ultimo_valor") or 0),
                int(row.get("vezes") or 0),
            )

    if not acc:
        return []

    ranked = sorted(acc.values(), key=lambda x: (-x["vezes"], -x["qtd_total"]))[:limit]
    codigos = [(r.get("codigo") or "").strip() for r in ranked]
    ativos = codigos_gm_ativos_no_catalogo([c for c in codigos if c])
    out = []
    for row in ranked:
        codigo = (row.get("codigo") or "").strip()
        preco = float(row["preco_sum"] / row["preco_n"]) if row.get("preco_n") else 0.0
        cod_norm = normalizar_codigo_gm_rel(codigo)
        out.append(
            {
                "codigo": codigo,
                "descricao": row.get("descricao") or "",
                "vezes": int(row.get("vezes") or 0),
                "qtd_total": float(row.get("qtd_total") or 0),
                "preco_medio": preco,
                "catalogo_disponivel": bool(cod_norm and cod_norm in ativos),
            }
        )
    return out


def _ciclo_racao(venda_ids: list[int], hist_venda_pks: list[int] | None = None) -> list[dict[str, Any]]:
    hist_venda_pks = hist_venda_pks or []
    if not venda_ids and not hist_venda_pks:
        return []

    por_chave: dict[str, list[tuple]] = defaultdict(list)

    if venda_ids:
        itens = (
            ItemVendaAgro.objects.filter(venda_id__in=venda_ids)
            .select_related("venda")
            .order_by("-venda__criado_em")
        )
        for it in itens:
            desc = (it.descricao or "").strip()
            if not _RACAO_RE.search(desc):
                continue
            chave = (it.codigo or desc[:80]).strip().lower()
            por_chave[chave].append((it.venda.criado_em, it.quantidade, desc, it.codigo))

    if hist_venda_pks:
        for it in (
            RelacionamentoItemHistoricoErpAgro.objects.filter(venda_id__in=hist_venda_pks)
            .select_related("venda")
            .order_by("-venda__data_venda")
        ):
            desc = (it.descricao or "").strip()
            if not _RACAO_RE.search(desc):
                continue
            chave = (it.codigo_gm or desc[:80]).strip().lower()
            por_chave[chave].append((it.venda.data_venda, it.quantidade, desc, it.codigo_gm))

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


def _serialize_venda_historico_pdv(
    v: VendaAgro,
    ativos_it: set[str] | None = None,
) -> dict[str, Any]:
    if ativos_it is None:
        codigos_it = [(it.codigo or "").strip() for it in v.itens.all()[:20]]
        ativos_it = codigos_gm_ativos_no_catalogo([c for c in codigos_it if c])
    linhas = [
        {
            "codigo": (it.codigo or "").strip(),
            "descricao": (it.descricao or "").strip(),
            "qtd": float(it.quantidade or 0),
            "total": float(it.valor_total or 0),
            "catalogo_disponivel": bool(normalizar_codigo_gm_rel(it.codigo or "") in ativos_it),
        }
        for it in v.itens.all()[:20]
    ]
    return {
        "id": v.pk,
        "origem": "pdv",
        "data": timezone.localtime(v.criado_em).strftime("%d/%m/%Y %H:%M"),
        "total": float(v.total or 0),
        "forma": (v.forma_pagamento or "").strip(),
        "itens": linhas,
    }


def _serialize_venda_historico_erp(
    v: RelacionamentoVendaHistoricoErpAgro,
    ativos_it: set[str] | None = None,
) -> dict[str, Any]:
    if ativos_it is None:
        codigos_it = [(it.codigo_gm or "").strip() for it in v.itens.all()[:20]]
        ativos_it = codigos_gm_ativos_no_catalogo([c for c in codigos_it if c])
    linhas = []
    for it in v.itens.all()[:20]:
        cod = (it.codigo_gm or "").strip()
        linhas.append(
            {
                "codigo": cod,
                "descricao": (it.descricao or "").strip(),
                "qtd": float(it.quantidade or 0),
                "total": float(it.valor_total or 0),
                "catalogo_disponivel": bool(cod and normalizar_codigo_gm_rel(cod) in ativos_it),
            }
        )
    return {
        "id": f"erp-{v.pk}",
        "origem": "erp",
        "data": timezone.localtime(v.data_venda).strftime("%d/%m/%Y %H:%M"),
        "total": float(v.total or 0),
        "forma": (v.forma_pagamento or "").strip(),
        "itens": linhas,
    }


def _merged_historico_refs(
    vendas_qs,
    hist_qs,
    offset: int = 0,
    limit: int = HISTORICO_PAGE_SIZE,
) -> list[tuple[str, int]]:
    """Merge ordenado PDV + ERP histórico (só pk) — paginação sem carregar itens."""
    pdv_iter = vendas_qs.values_list("pk", "criado_em").iterator(chunk_size=200)
    erp_iter = hist_qs.values_list("pk", "data_venda").iterator(chunk_size=200)
    pdv_peek = next(pdv_iter, None)
    erp_peek = next(erp_iter, None)
    skipped = 0
    refs: list[tuple[str, int]] = []

    while (pdv_peek or erp_peek) and len(refs) < limit:
        take_pdv = False
        if pdv_peek and erp_peek:
            take_pdv = pdv_peek[1] >= erp_peek[1]
        elif pdv_peek:
            take_pdv = True

        if take_pdv:
            ref = ("pdv", int(pdv_peek[0]))
            pdv_peek = next(pdv_iter, None)
        else:
            ref = ("erp", int(erp_peek[0]))
            erp_peek = next(erp_iter, None)

        if skipped < offset:
            skipped += 1
            continue
        refs.append(ref)

    return refs


def _historico_vendas_paginado(
    cli: ClienteAgro,
    offset: int = 0,
    limit: int = HISTORICO_PAGE_SIZE,
) -> dict[str, Any]:
    vendas_qs = _vendas_cliente_qs(cli)
    hist_qs = _vendas_historico_erp_qs(cli)
    total = int(vendas_qs.count()) + int(hist_qs.count())
    refs = _merged_historico_refs(vendas_qs, hist_qs, offset=offset, limit=limit)

    pdv_pks = [pk for origem, pk in refs if origem == "pdv"]
    erp_pks = [pk for origem, pk in refs if origem == "erp"]
    pdv_map = {
        v.pk: v
        for v in VendaAgro.objects.filter(pk__in=pdv_pks).prefetch_related("itens")
    }
    erp_map = {
        v.pk: v
        for v in RelacionamentoVendaHistoricoErpAgro.objects.filter(pk__in=erp_pks).prefetch_related("itens")
    }

    codigos_batch: list[str] = []
    for v in pdv_map.values():
        codigos_batch.extend((it.codigo or "").strip() for it in v.itens.all()[:20])
    for v in erp_map.values():
        codigos_batch.extend((it.codigo_gm or "").strip() for it in v.itens.all()[:20])
    ativos_batch = codigos_gm_ativos_no_catalogo([c for c in codigos_batch if c])

    vendas_out: list[dict[str, Any]] = []
    for origem, pk in refs:
        if origem == "pdv":
            v = pdv_map.get(pk)
            if v:
                vendas_out.append(_serialize_venda_historico_pdv(v, ativos_it=ativos_batch))
        else:
            v = erp_map.get(pk)
            if v:
                vendas_out.append(_serialize_venda_historico_erp(v, ativos_it=ativos_batch))

    return {
        "vendas": vendas_out,
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": (offset + len(vendas_out)) < total,
    }


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


def _cliente_relacionamento_base(cli: ClienteAgro) -> dict[str, Any]:
    wa = (cli.whatsapp or "").strip()
    wa_digits = re.sub(r"\D", "", wa)
    wa_url = f"https://wa.me/55{wa_digits}" if len(wa_digits) >= 10 else ""
    return {
        "pk": cli.pk,
        "nome": cli.nome,
        "whatsapp": wa,
        "whatsapp_url": wa_url,
        "endereco": (cli.endereco or "").strip(),
        "saldo_cashback": float(cli.saldo_cashback or 0),
        "saldo_vale_credito": float(cli.saldo_vale_credito or 0),
    }


def _metricas_relacionamento_cliente(cli: ClienteAgro) -> dict[str, Any]:
    vendas_qs = _vendas_cliente_qs(cli)
    hist_qs = _vendas_historico_erp_qs(cli)

    totais_pdv = vendas_qs.aggregate(n=Count("id"), soma=Sum("total"))
    totais_hist = hist_qs.aggregate(n=Count("id"), soma=Sum("total"))
    n_vendas = int(totais_pdv.get("n") or 0) + int(totais_hist.get("n") or 0)
    soma = _dec(totais_pdv.get("soma")) + _dec(totais_hist.get("soma"))
    ticket = float((soma / n_vendas).quantize(Decimal("0.01"))) if n_vendas else 0.0

    ultima_pdv = vendas_qs.first()
    ultima_hist = hist_qs.first()
    ultima_dt = None
    if ultima_pdv and ultima_hist:
        ultima_dt = max(ultima_pdv.criado_em, ultima_hist.data_venda)
    elif ultima_pdv:
        ultima_dt = ultima_pdv.criado_em
    elif ultima_hist:
        ultima_dt = ultima_hist.data_venda
    dias_ultima = _dias_desde(ultima_dt) if ultima_dt else None

    datas: list = []
    for dt in vendas_qs.values_list("criado_em", flat=True)[:24]:
        datas.append(dt)
    for dt in hist_qs.values_list("data_venda", flat=True)[:24]:
        datas.append(dt)
    datas.sort(reverse=True)
    freq_dias = None
    if len(datas) >= 2:
        gaps = []
        for i in range(min(len(datas) - 1, 23)):
            d1 = datas[i].date() if hasattr(datas[i], "date") else datas[i]
            d2 = datas[i + 1].date() if hasattr(datas[i + 1], "date") else datas[i + 1]
            gaps.append(abs((d1 - d2).days))
        if gaps:
            freq_dias = int(sum(gaps) / len(gaps))

    return {
        "total_vendas": n_vendas,
        "ticket_medio": ticket,
        "frequencia_media_dias": freq_dias,
        "ultima_visita_dias": dias_ultima,
        "total_comprado": float(soma),
    }


def _venda_ids_amostra(cli: ClienteAgro) -> tuple[list[int], list[int]]:
    vendas_qs = _vendas_cliente_qs(cli)
    hist_qs = _vendas_historico_erp_qs(cli)
    lim = _TOP_VENDAS_IDS_LIMIT
    return (
        list(vendas_qs.values_list("pk", flat=True)[:lim]),
        list(hist_qs.values_list("pk", flat=True)[:lim]),
    )


def montar_secao_relacionamento_cliente(
    cliente_agro_pk: int,
    secao: str,
    *,
    historico_offset: int = 0,
    historico_limit: int = HISTORICO_PAGE_SIZE,
) -> dict[str, Any]:
    cli = ClienteAgro.objects.filter(pk=cliente_agro_pk, ativo=True).first()
    if not cli:
        return {"ok": False, "erro": "Cliente não encontrado."}

    secao = (secao or "").strip().lower()
    if secao == "historico":
        hist = _historico_vendas_paginado(cli, offset=historico_offset, limit=historico_limit)
        return {"ok": True, "historico_rapido": hist}

    venda_ids, hist_venda_pks = _venda_ids_amostra(cli)
    if secao == "ciclo_racao":
        return {"ok": True, "ciclo_racao": _ciclo_racao(venda_ids, hist_venda_pks)}
    if secao == "cross_sell":
        top = _top_produtos(venda_ids, hist_venda_pks)
        return {"ok": True, "cross_sell": _cross_sell(top, venda_ids)}

    return {"ok": False, "erro": "Seção inválida."}


def montar_painel_relacionamento_cliente(
    cliente_agro_pk: int,
    *,
    incluir_ciclo: bool = False,
    incluir_cross: bool = False,
    historico_offset: int = 0,
    historico_limit: int = HISTORICO_PAGE_SIZE,
) -> dict[str, Any]:
    cli = ClienteAgro.objects.filter(pk=cliente_agro_pk, ativo=True).first()
    if not cli:
        return {"ok": False, "erro": "Cliente não encontrado."}

    venda_ids, hist_venda_pks = _venda_ids_amostra(cli)
    top = _top_produtos(venda_ids, hist_venda_pks)
    historico = _historico_vendas_paginado(cli, offset=historico_offset, limit=historico_limit)

    return {
        "ok": True,
        "rascunho": True,
        "cliente": _cliente_relacionamento_base(cli),
        "metricas": _metricas_relacionamento_cliente(cli),
        "historico_rapido": {
            "top_produtos": top,
            "vendas": historico["vendas"],
            "total": historico["total"],
            "offset": historico["offset"],
            "limit": historico["limit"],
            "has_more": historico["has_more"],
        },
        "historico_erp": resumo_historico_erp_cliente(cli),
        "ciclo_racao": _ciclo_racao(venda_ids, hist_venda_pks) if incluir_ciclo else [],
        "cross_sell": (
            _cross_sell(top, venda_ids) if incluir_cross else []
        ),
        "financeiro_fiado": _fiado_resumo(cli),
        "fidelidade": {
            "cashback": float(cli.saldo_cashback or 0),
            "vale_credito": float(cli.saldo_vale_credito or 0),
        },
        "extras": ler_relacionamento_extras_cliente(cli),
    }
