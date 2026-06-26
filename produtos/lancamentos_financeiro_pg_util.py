"""Lista CP (contas a pagar) no Postgres ``TituloFinanceiroAgro`` — espelho Mongo com dedup."""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from django.db.models import Q, QuerySet
from django.utils import timezone

from produtos.models import TituloFinanceiroAgro

logger = logging.getLogger(__name__)

_TOL = Decimal("0.02")
_CAP_LINHAS = 25_000
_SEM_PLANO_MARKER = "__SEM_PLANO__"


def _dec2(v: object) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


def _titulo_manual_agro_lote(t: TituloFinanceiroAgro) -> bool:
    obs = (t.observacoes or "").lower()
    mod = (t.modificado_por or "").lower()
    return "lote manual agro" in obs or "manual em lote agro" in mod


def _tem_id_erp_valido(t: TituloFinanceiroAgro) -> bool:
    snap = t.dados_snapshot_json or {}
    x = str(snap.get("id_erp") or "").strip()
    if not x:
        return False
    if len(x) == 24 and re.match(r"^[a-fA-F0-9]{24}$", x):
        return False
    return True


def dedup_key_titulo(t: TituloFinanceiroAgro) -> str:
    if _titulo_manual_agro_lote(t):
        return f"O|{t.mongo_id}"
    venc = t.data_vencimento.isoformat() if t.data_vencimento else "nod"
    pag = t.data_pagamento.isoformat() if t.data_pagamento else "np"
    bruto = round(float(t.valor_bruto), 2)
    return "|".join(
        [
            "SIG",
            (t.empresa or "").strip(),
            str(bool(t.despesa)),
            (t.cliente or "").strip(),
            str(bruto),
            venc,
            (t.plano_conta or "").strip(),
            (t.forma_pagamento or "").strip(),
            str(int(t.parcela or 0)),
            pag,
        ]
    )


def _dedup_ord(t: TituloFinanceiroAgro) -> tuple[Any, ...]:
    lu = t.mongo_ultima_atualizacao
    if lu is None:
        lu = datetime.min.replace(tzinfo=timezone.get_current_timezone())
    elif timezone.is_naive(lu):
        lu = timezone.make_aware(lu, timezone.get_current_timezone())
    return (_tem_id_erp_valido(t), lu, t.mongo_id or "")


def dedup_titulos(titulos: list[TituloFinanceiroAgro]) -> list[TituloFinanceiroAgro]:
    buckets: dict[str, TituloFinanceiroAgro] = {}
    for t in titulos:
        k = dedup_key_titulo(t)
        prev = buckets.get(k)
        if prev is None or _dedup_ord(t) > _dedup_ord(prev):
            buckets[k] = t
    return list(buckets.values())


def _titulo_aberto(t: TituloFinanceiroAgro) -> bool:
    if t.quitado:
        return False
    return _dec2(t.valor_restante) > _TOL


def _titulo_quitado_negocio(t: TituloFinanceiroAgro) -> bool:
    if t.quitado:
        return True
    return _dec2(t.valor_restante) <= _TOL


def _aplicar_status_qs(qs: QuerySet, status: str) -> QuerySet:
    st = (status or "abertos").strip().lower()
    if st == "abertos":
        return qs.filter(quitado=False, valor_restante__gt=_TOL)
    if st == "quitados":
        return qs.filter(Q(quitado=True) | Q(valor_restante__lte=_TOL))
    return qs


def _aplicar_exclusao_planos(qs: QuerySet, excluir_planos: list[str] | None) -> QuerySet:
    raw = [str(x).strip() for x in (excluir_planos or []) if x and str(x).strip()]
    if not raw:
        return qs
    exclui_sem = _SEM_PLANO_MARKER in raw or any(x.lower() == "(sem plano)" for x in raw)
    nomes = [x for x in raw if x != _SEM_PLANO_MARKER and x.lower() != "(sem plano)"]
    if nomes:
        qs = qs.exclude(plano_conta__in=nomes[:200])
    if exclui_sem:
        qs = qs.exclude(plano_conta="")
    return qs


def _aplicar_texto_qs(qs: QuerySet, texto: str | None) -> QuerySet:
    t = (texto or "").strip()
    if not t:
        return qs
    tokens = [x for x in t.split() if x.strip()]
    for tok in tokens[:12]:
        tok = tok.strip()
        if not tok:
            continue
        q_tok = (
            Q(cliente__icontains=tok)
            | Q(descricao__icontains=tok)
            | Q(numero_documento__icontains=tok)
            | Q(plano_conta__icontains=tok)
            | Q(grupo__icontains=tok)
            | Q(forma_pagamento__icontains=tok)
            | Q(banco__icontains=tok)
            | Q(empresa__icontains=tok)
            | Q(observacoes__icontains=tok)
            | Q(mongo_id__icontains=tok)
        )
        qs = qs.filter(q_tok)
    return qs


def contas_pagar_montar_qs(
    *,
    status: str = "abertos",
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    competencia_de: date | None = None,
    competencia_ate: date | None = None,
    pagamento_de: date | None = None,
    pagamento_ate: date | None = None,
    texto: str | None = None,
    excluir_planos_nomes: list[str] | None = None,
    mongo_id: str | None = None,
) -> QuerySet:
    qs = TituloFinanceiroAgro.objects.filter(despesa=True)
    mid = (mongo_id or "").strip()
    if mid:
        return qs.filter(mongo_id=mid)
    qs = _aplicar_status_qs(qs, status)
    if vencimento_de is not None:
        qs = qs.filter(data_vencimento__gte=vencimento_de)
    if vencimento_ate is not None:
        qs = qs.filter(data_vencimento__lte=vencimento_ate)
    if competencia_de is not None:
        qs = qs.filter(data_competencia__gte=competencia_de)
    if competencia_ate is not None:
        qs = qs.filter(data_competencia__lte=competencia_ate)
    if pagamento_de is not None:
        qs = qs.filter(data_pagamento__gte=pagamento_de)
    if pagamento_ate is not None:
        qs = qs.filter(data_pagamento__lte=pagamento_ate)
    qs = _aplicar_exclusao_planos(qs, excluir_planos_nomes)
    qs = _aplicar_texto_qs(qs, texto)
    return qs


def _sort_key_titulo(t: TituloFinanceiroAgro, ordenacao: str) -> tuple:
    ord_ = (ordenacao or "vencimento_asc").strip().lower()
    venc = t.data_vencimento or date.min
    fluxo = t.data_fluxo or date.min
    if ord_ == "vencimento_desc":
        return (-venc.toordinal(), t.pk or 0)
    if ord_ == "fluxo_desc":
        return (-fluxo.toordinal(), t.pk or 0)
    if ord_ == "cliente_asc":
        return ((t.cliente or "").lower(), venc.toordinal())
    if ord_ == "cliente_desc":
        return (-1, (t.cliente or "").lower(), -venc.toordinal())
    if ord_ == "forma_asc":
        return ((t.forma_pagamento or "").lower(), venc.toordinal())
    if ord_ == "forma_desc":
        return (-1, (t.forma_pagamento or "").lower(), -venc.toordinal())
    if ord_ == "plano_asc":
        return ((t.plano_conta or "").lower(), venc.toordinal())
    if ord_ == "plano_desc":
        return (-1, (t.plano_conta or "").lower(), -venc.toordinal())
    if ord_ == "bruto_asc":
        return (float(t.valor_bruto), venc.toordinal())
    if ord_ == "bruto_desc":
        return (-float(t.valor_bruto), -venc.toordinal())
    if ord_ == "saldo_asc":
        return (float(t.valor_restante), venc.toordinal())
    if ord_ == "saldo_desc":
        return (-float(t.valor_restante), -venc.toordinal())
    return (venc.toordinal(), t.pk or 0)


def _totais_de_titulos(titulos: list[TituloFinanceiroAgro]) -> dict[str, float]:
    n = len(titulos)
    bruto = Decimal("0")
    mov = Decimal("0")
    saldo = Decimal("0")
    for t in titulos:
        bruto += _dec2(t.valor_bruto)
        mov += _dec2(t.valor_pago)
        saldo += _dec2(t.valor_restante)
    return {
        "quantidade": n,
        "bruto": float(bruto),
        "movimentado": float(mov),
        "saldo_aberto": float(saldo),
    }


def titulo_financeiro_agro_para_api(t: TituloFinanceiroAgro) -> dict[str, Any]:
    """Formato compatível com ``lancamento_para_api`` (lista CP)."""
    quitado = _titulo_quitado_negocio(t)
    mov_r = round(float(t.valor_pago), 2)
    rest = round(float(t.valor_restante), 2)
    bruto = round(float(t.valor_bruto), 2)

    def _iso_d(d: date | None) -> str | None:
        if d is None:
            return None
        return datetime.combine(d, datetime.min.time()).replace(
            tzinfo=timezone.get_current_timezone()
        ).isoformat()

    pode_excluir = False
    if not _tem_id_erp_valido(t):
        if _titulo_manual_agro_lote(t):
            pode_excluir = True
        elif not quitado and mov_r <= 0.02:
            pode_excluir = True

    snap = t.dados_snapshot_json or {}
    return {
        "id": t.mongo_id,
        "despesa": True,
        "descricao": t.descricao or "",
        "cliente": t.cliente or "",
        "cliente_id": t.cliente_id or "",
        "numero_documento": t.numero_documento or "",
        "parcela": int(t.parcela or 0),
        "plano_conta": t.plano_conta or "",
        "plano_conta_id": t.plano_conta_id or "",
        "grupo": t.grupo or "",
        "forma_pagamento": t.forma_pagamento or "",
        "forma_pagamento_id": t.forma_pagamento_id or "",
        "banco": t.banco or "",
        "banco_id": t.banco_id or "",
        "centro_custo": t.centro_custo or "",
        "empresa": t.empresa or "",
        "observacoes": (t.observacoes or "")[:500],
        "valor_bruto": bruto,
        "valor_movimentado": mov_r,
        "restante": rest,
        "pago": quitado,
        "data_vencimento": _iso_d(t.data_vencimento),
        "data_competencia": _iso_d(t.data_competencia),
        "data_fluxo": _iso_d(t.data_fluxo),
        "data_pagamento": _iso_d(t.data_pagamento),
        "valor_previsto": bruto,
        "valor_pago": mov_r,
        "pode_editar": not quitado,
        "pode_editar_valor": (not quitado) and mov_r <= 0.02,
        "pode_excluir": pode_excluir,
        "agro_recorrente": bool(t.agro_recorrente),
        "recorrencia_intervalo_meses": max(1, min(int(t.recorrencia_intervalo_meses or 1), 36)),
        "agro_recorrente_sempre": bool(t.agro_recorrente_sempre),
        "boleto_codigo_barras": (t.boleto_codigo_barras or "")[:54],
        "usuario_lancou": t.usuario_lancou or "",
        "usuario_quitou": t.usuario_quitou or "",
        "modificado_por": t.modificado_por or "",
        "criado_por": t.criado_por or "",
        "last_update": snap.get("last_update"),
        "data_modificacao": snap.get("data_modificacao"),
        "fonte_postgres": True,
    }


def contas_pagar_buscar_pagina_pg(
    *,
    status: str = "abertos",
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    competencia_de: date | None = None,
    competencia_ate: date | None = None,
    pagamento_de: date | None = None,
    pagamento_ate: date | None = None,
    texto: str | None = None,
    excluir_planos_nomes: list[str] | None = None,
    mongo_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
    ordenacao: str = "vencimento_asc",
    skip_totais: bool = False,
    limite_max: int = 200,
) -> tuple[list[dict], int, dict[str, float] | None]:
    page = max(1, page)
    cap = max(1, int(limite_max) if limite_max else 200)
    page_size = min(cap, max(1, page_size))
    skip = (page - 1) * page_size

    qs = contas_pagar_montar_qs(
        status=status,
        vencimento_de=vencimento_de,
        vencimento_ate=vencimento_ate,
        competencia_de=competencia_de,
        competencia_ate=competencia_ate,
        pagamento_de=pagamento_de,
        pagamento_ate=pagamento_ate,
        texto=texto,
        excluir_planos_nomes=excluir_planos_nomes,
        mongo_id=mongo_id,
    )
    rows = list(qs[: _CAP_LINHAS + 1])
    if len(rows) > _CAP_LINHAS:
        logger.warning("contas_pagar_buscar_pagina_pg: truncado em %s linhas", _CAP_LINHAS)
        rows = rows[:_CAP_LINHAS]

    if mongo_id and rows:
        deduped = rows
    else:
        deduped = dedup_titulos(rows)

    deduped.sort(key=lambda t: _sort_key_titulo(t, ordenacao))

    total = len(deduped)
    totais = None if skip_totais else _totais_de_titulos(deduped)
    page_rows = deduped[skip : skip + page_size]
    linhas = [titulo_financeiro_agro_para_api(t) for t in page_rows]
    return linhas, total, totais


def planos_distintos_cp_pg(
    *,
    status: str = "abertos",
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    competencia_de: date | None = None,
    competencia_ate: date | None = None,
    pagamento_de: date | None = None,
    pagamento_ate: date | None = None,
    texto: str | None = None,
    limit: int = 400,
) -> list[dict[str, str]]:
    qs = contas_pagar_montar_qs(
        status=status,
        vencimento_de=vencimento_de,
        vencimento_ate=vencimento_ate,
        competencia_de=competencia_de,
        competencia_ate=competencia_ate,
        pagamento_de=pagamento_de,
        pagamento_ate=pagamento_ate,
        texto=texto,
    )
    rows = dedup_titulos(list(qs[:_CAP_LINHAS]))
    nomes: set[str] = set()
    for t in rows:
        n = (t.plano_conta or "").strip()
        nomes.add(n if n else "(sem plano)")
    lim = min(max(int(limit or 400), 1), 500)
    return [{"nome": x} for x in sorted(nomes, key=lambda s: s.lower())][:lim]


def financeiro_pg_conferencia_abertos() -> dict[str, Any]:
    """Totais CP em aberto Mongo (dedup) vs Postgres (dedup) — diagnóstico pré-flag."""
    from produtos.mongo_financeiro_util import (
        contas_pagar_totais_filtrados,
        lancamentos_montar_query_mongo,
    )
    from produtos.views import obter_conexao_mongo

    out: dict[str, Any] = {
        "ok": True,
        "pg_registros_brutos": TituloFinanceiroAgro.objects.filter(despesa=True).count(),
    }
    _, total_pg, tot_pg = contas_pagar_buscar_pagina_pg(
        status="abertos",
        page=1,
        page_size=1,
        skip_totais=False,
        limite_max=200,
    )
    out["pg_abertos_dedup"] = {"quantidade": total_pg, **(tot_pg or {})}

    _, db = obter_conexao_mongo()
    if db is None:
        out["mongo_ok"] = False
        out["mongo_erro"] = "Mongo indisponível"
        return out
    q = lancamentos_montar_query_mongo(despesa=True, status="abertos")
    tot_m = contas_pagar_totais_filtrados(db, q)
    out["mongo_ok"] = True
    out["mongo_abertos_dedup"] = tot_m
    return out
