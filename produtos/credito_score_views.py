"""Views — laboratório de análise de crédito (shadow). Isolado do fiado operacional."""

from __future__ import annotations

from decimal import Decimal
from io import BytesIO

from django.contrib import messages
from django.http import HttpResponse, HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_http_methods

from produtos.credito_score_acesso_util import credito_score_shadow_required
from produtos.credito_score_shadow import analisar_cliente, analisar_clientes, persistir_analise
from produtos.models import ClienteAgro, ClienteAnaliseCreditoAgro


def _fmt_money(val) -> str:
    try:
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
        "situacao_label": "Vencido"
        if s.tem_vencido_snapshot
        else ("Sem histórico" if s.score is None else "Em dia"),
    }


def _filtros_request(request) -> dict[str, str]:
    return {
        "q": (request.GET.get("q") or "").strip(),
        "classificacao": (request.GET.get("classificacao") or "").strip(),
        "confianca": (request.GET.get("confianca") or "").strip(),
        "vencidos": (request.GET.get("vencidos") or "").strip(),
        "score_min": (request.GET.get("score_min") or "").strip(),
        "score_max": (request.GET.get("score_max") or "").strip(),
    }


def _listar_snapshots_filtrados(filtros: dict[str, str]) -> list[dict]:
    """Último snapshot por cliente + filtros da tela (somente leitura)."""
    q = filtros.get("q") or ""
    classif = filtros.get("classificacao") or ""
    conf = filtros.get("confianca") or ""
    venc = filtros.get("vencidos") or ""
    score_min = filtros.get("score_min") or ""
    score_max = filtros.get("score_max") or ""

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
    return rows


def _money_cell(val) -> float:
    try:
        return float(Decimal(str(val or 0)))
    except Exception:
        return 0.0


def _montar_xlsx_laboratorio(rows: list[dict]) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Analise credito"
    headers = [
        ("ID cliente", "cliente_pk"),
        ("Cliente", "cliente_nome"),
        ("Score", "score"),
        ("Classificação", "classificacao_label"),
        ("Confiança", "confianca_label"),
        ("Limite cadastrado", "limite_cadastrado"),
        ("Limite atual (efetivo)", "limite_efetivo"),
        ("Em aberto", "saldo_aberto"),
        ("Saldo vencido", "saldo_vencido"),
        ("Média fiado 3m", "media_fiado_3m"),
        ("Limite sugerido", "limite_sugerido"),
        ("Dif. sugerido − atual", "diff_sugerido"),
        ("Situação", "situacao_label"),
        ("Candidato revisão", "candidato_revisao"),
        ("Maior atraso (dias)", "maior_atraso_dias"),
        ("Calculado em", "calculado_em"),
        ("Alertas", "alertas_txt"),
    ]
    fill_hdr = PatternFill("solid", fgColor="064E3B")
    font_hdr = Font(bold=True, color="FFFFFF")
    money_keys = {
        "limite_cadastrado",
        "limite_efetivo",
        "saldo_aberto",
        "saldo_vencido",
        "media_fiado_3m",
        "limite_sugerido",
        "diff_sugerido",
    }

    for col, (label, _key) in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col, value=label)
        cell.font = font_hdr
        cell.fill = fill_hdr

    for r_idx, row in enumerate(rows, start=2):
        lim_e = _money_cell(row.get("limite_efetivo"))
        sug = _money_cell(row.get("limite_sugerido"))
        alertas = row.get("alertas") or []
        payload = {
            **row,
            "diff_sugerido": round(sug - lim_e, 2),
            "candidato_revisao": "Sim" if row.get("candidato_revisao") else "Não",
            "alertas_txt": " | ".join(str(a) for a in alertas) if alertas else "",
        }
        for col, (_label, key) in enumerate(headers, start=1):
            val = payload.get(key)
            if key == "score" and val is None:
                val = ""
            if key == "calculado_em" and val is not None:
                try:
                    if timezone.is_aware(val):
                        val = timezone.localtime(val).replace(tzinfo=None)
                except Exception:
                    pass
            if key in money_keys:
                val = _money_cell(val)
            cell = ws.cell(row=r_idx, column=col, value=val)
            if key in money_keys:
                cell.number_format = "#,##0.00"

    widths = {
        "A": 12,
        "B": 36,
        "C": 10,
        "D": 14,
        "E": 12,
        "F": 16,
        "G": 18,
        "H": 12,
        "I": 14,
        "J": 14,
        "K": 14,
        "L": 16,
        "M": 12,
        "N": 16,
        "O": 14,
        "P": 18,
        "Q": 40,
    }
    for letter, w in widths.items():
        ws.column_dimensions[letter].width = w

    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{max(1, len(rows) + 1)}"
    ws.freeze_panes = "A2"

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


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

    filtros = _filtros_request(request)
    rows = _listar_snapshots_filtrados(filtros)

    return render(
        request,
        "produtos/credito_score_laboratorio.html",
        {
            "rows": rows,
            **filtros,
            "classificacao_choices": ClienteAnaliseCreditoAgro.Classificacao.choices,
            "confianca_choices": ClienteAnaliseCreditoAgro.Confianca.choices,
            "fmt_money": _fmt_money,
            "total_clientes": len(rows),
        },
    )


@credito_score_shadow_required
@require_GET
def credito_score_laboratorio_export_xlsx(request):
    """Excel ↓ — mesmas linhas/filtros da tela (somente leitura)."""
    filtros = _filtros_request(request)
    rows = _listar_snapshots_filtrados(filtros)
    payload = _montar_xlsx_laboratorio(rows)
    agora = timezone.localtime()
    fname = f"analise_credito_shadow_{agora.strftime('%Y%m%d_%H%M')}.xlsx"
    resp = HttpResponse(
        payload,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = f'attachment; filename="{fname}"'
    return resp


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
