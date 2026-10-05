"""Views — laboratório de análise de crédito (shadow). Isolado do fiado operacional."""

from __future__ import annotations

from django.contrib import messages
from django.http import Http404, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_http_methods, require_POST

from produtos.credito_score_acesso_util import credito_score_shadow_required
from produtos.credito_score_shadow import analisar_clientes, analisar_cliente, persistir_analise
from produtos.models import ClienteAgro, ClienteAnaliseCreditoAgro


def _fmt_money(val) -> str:
    try:
        from decimal import Decimal

        d = Decimal(str(val or 0))
        return f"R$ {d:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"


def _snapshot_row(s: ClienteAnaliseCreditoAgro) -> dict:
    ind = s.indicadores_json if isinstance(s.indicadores_json, dict) else {}
    return {
        "id": s.pk,
        "cliente_pk": s.cliente_id,
        "cliente_nome": (s.cliente.nome if s.cliente_id else "") or "—",
        "calculado_em": s.calculado_em,
        "score": s.score,
        "classificacao": s.classificacao,
        "classificacao_label": s.get_classificacao_display(),
        "confianca": s.confianca,
        "confianca_label": s.get_confianca_display(),
        "limite_cadastrado": s.limite_cadastrado_snapshot,
        "limite_efetivo": s.limite_efetivo_snapshot,
        "saldo_aberto": s.saldo_aberto_snapshot,
        "saldo_vencido": s.saldo_vencido_snapshot,
        "media_fiado_3m": s.media_fiado_3m,
        "limite_sugerido": s.limite_sugerido,
        "tem_vencido": s.tem_vencido_snapshot,
        "maior_atraso_dias": s.maior_atraso_dias,
        "indicadores": ind,
        "alertas": s.alertas_json if isinstance(s.alertas_json, list) else [],
        "candidato_revisao": bool(ind.get("candidato_revisao_limite")),
        "situacao_label": "Vencido" if s.tem_vencido_snapshot else ("Sem histórico" if s.score is None else "Em dia"),
    }


@credito_score_shadow_required
@require_http_methods(["GET", "POST"])
def credito_score_laboratorio(request):
    """Lista últimos snapshots. POST recalcula todos (ou filtrados)."""
    if request.method == "POST":
        acao = (request.POST.get("acao") or "").strip()
        if acao == "recalcular_todos":
            analisar_clientes(persist=True)
            messages.success(request, "Análises recalculadas (snapshots novos gravados).")
            return redirect("credito_score_laboratorio")
        return HttpResponseForbidden("Ação inválida.")

    q = (request.GET.get("q") or "").strip()
    classif = (request.GET.get("classificacao") or "").strip()
    conf = (request.GET.get("confianca") or "").strip()
    venc = (request.GET.get("vencidos") or "").strip()  # 1 / 0 / sem_hist
    score_min = (request.GET.get("score_min") or "").strip()
    score_max = (request.GET.get("score_max") or "").strip()

    # Último snapshot por cliente (Subquery simples via dict)
    vistos: set[int] = set()
    rows: list[dict] = []
    qs = (
        ClienteAnaliseCreditoAgro.objects.select_related("cliente")
        .order_by("-calculado_em", "-pk")
    )
    for s in qs.iterator(chunk_size=200):
        if s.cliente_id in vistos:
            continue
        vistos.add(s.cliente_id)
        row = _snapshot_row(s)
        if q and q.lower() not in (row["cliente_nome"] or "").lower():
            continue
        if classif and row["classificacao"] != classif:
            continue
        if conf and row["confianca"] != conf:
            continue
        if venc == "1" and not row["tem_vencido"]:
            continue
        if venc == "0" and row["tem_vencido"]:
            continue
        if venc == "sem_hist" and row["score"] is not None:
            continue
        if score_min.isdigit() and (row["score"] is None or row["score"] < int(score_min)):
            continue
        if score_max.isdigit() and (row["score"] is None or row["score"] > int(score_max)):
            continue
        rows.append(row)

    rows.sort(key=lambda r: (-(r["score"] if r["score"] is not None else -1), r["cliente_nome"]))

    return render(
        request,
        "produtos/credito_score_laboratorio.html",
        {
            "rows": rows,
            "q": q,
            "classificacao": classif,
            "confianca": conf,
            "vencidos": venc,
            "score_min": score_min,
            "score_max": score_max,
            "classificacao_choices": ClienteAnaliseCreditoAgro.Classificacao.choices,
            "confianca_choices": ClienteAnaliseCreditoAgro.Confianca.choices,
            "fmt_money": _fmt_money,
            "total_clientes": len(rows),
        },
    )


@credito_score_shadow_required
@require_http_methods(["GET", "POST"])
def credito_score_cliente_detalhe(request, pk: int):
    cli = get_object_or_404(ClienteAgro, pk=pk)
    if request.method == "POST":
        acao = (request.POST.get("acao") or "").strip()
        if acao == "recalcular_cliente":
            r = analisar_cliente(cli)
            persistir_analise(r)
            messages.success(request, f"Snapshot novo gravado para {cli.nome}.")
            return redirect("credito_score_cliente_detalhe", pk=cli.pk)
        return HttpResponseForbidden("Ação inválida.")

    snaps = list(
        ClienteAnaliseCreditoAgro.objects.filter(cliente=cli)
        .order_by("-calculado_em", "-pk")[:30]
    )
    atual = snaps[0] if snaps else None
    return render(
        request,
        "produtos/credito_score_cliente_detalhe.html",
        {
            "cliente": cli,
            "atual": _snapshot_row(atual) if atual else None,
            "historico": [_snapshot_row(s) for s in snaps],
            "fmt_money": _fmt_money,
        },
    )
