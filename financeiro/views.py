from datetime import date, timedelta
import json

from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import JsonResponse
from django.urls import reverse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_http_methods

from base.models import Empresa

from financeiro.models import GraficoGastosAtalhoAgro
from financeiro.services.indicadores_gerencial_pg import get_indicadores_gerencial_pg
from produtos.views import _dashboard_periodo_from_request


@login_required(login_url="/admin/login/")
def dashboard_financeiro_completo(request):
    """Indicadores financeiros gerenciais — dados do SisVale (TituloFinanceiroAgro)."""
    empresas = Empresa.objects.filter(ativo=True).order_by("nome_fantasia")
    default_eid = empresas.values_list("pk", flat=True).first()
    empresa_id = int(request.GET.get("empresa") or default_eid or 0)
    if empresa_id:
        get_object_or_404(Empresa, pk=empresa_id, ativo=True)

    data_ini, data_fim, periodo_label, periodo_key = _dashboard_periodo_from_request(
        request
    )

    por = (request.GET.get("por") or "competencia").strip().lower()
    valor = (request.GET.get("valor") or "bruto").strip().lower()
    # Sempre operação da loja — exclui contas patrimoniais; sem opção ERP/Mongo na tela.
    filtro_contas = "resultado"
    var_modo = (request.GET.get("var_modo") or "mes").strip().lower()
    if var_modo not in ("mes", "semana"):
        var_modo = "mes"
    var_por = (request.GET.get("var_por") or "competencia").strip().lower()
    if var_por not in ("competencia", "vencimento", "pagamento"):
        var_por = "competencia"
    var_grupo = (request.GET.get("var_grupo") or "todas").strip().lower()
    if var_grupo not in ("todas", "fixa", "variavel", "outra"):
        var_grupo = "todas"

    dados = (
        get_indicadores_gerencial_pg(
            empresa_id,
            data_ini,
            data_fim,
            por=por,
            valor=valor,
            filtro_contas=filtro_contas,
            var_modo=var_modo,
            var_por=var_por,
            var_grupo=var_grupo,
        )
        if empresa_id
        else None
    )
    filtro_dashboard = {
        "por": por,
        "valor": valor,
        "var_modo": var_modo,
        "var_por": var_por,
        "var_grupo": var_grupo,
    }
    return render(
        request,
        "financeiro/indicadores_gerencial.html",
        {
            "empresas": empresas,
            "empresa_id": empresa_id,
            "dados": dados,
            "filtro_dashboard": filtro_dashboard,
            "periodo_key": periodo_key,
            "periodo_label": periodo_label,
            "periodo_cal_ini": data_ini.isoformat(),
            "periodo_cal_fim": data_fim.isoformat(),
            "cp_url": reverse("lancamentos_contas_pagar"),
        },
    )


def _grafico_gastos_parse_date(raw: str | None) -> date | None:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


@login_required(login_url="/admin/login/")
def grafico_gastos_view(request):
    from produtos.lancamentos_financeiro_pg_util import planos_distintos_pg
    from produtos.mongo_financeiro_util import _grafico_gastos_status_para_lista_planos

    hoje = date.today()
    padrao_ini = hoje - timedelta(days=90)
    por = "vencimento"
    valor = "saldo"
    st_planos = _grafico_gastos_status_para_lista_planos(por, valor)
    raw = planos_distintos_pg(
        despesa=True,
        status=st_planos,
        vencimento_de=padrao_ini,
        vencimento_ate=hoje,
        limit=500,
    )
    planos_conta = [
        {"id": str(p.get("nome") or ""), "nome": str(p.get("nome") or "")}
        for p in raw
        if str(p.get("nome") or "").strip() and str(p.get("nome") or "").strip() != "(sem plano)"
    ]
    return render(
        request,
        "financeiro/grafico_gastos.html",
        {
            "planos_conta": planos_conta,
            "total_planos": len(planos_conta),
            "data_inicial_padrao": padrao_ini.isoformat(),
            "data_final_padrao": hoje.isoformat(),
            "api_planos_cp_url": reverse("api_lancamentos_planos_distintos"),
        },
    )


@never_cache
@login_required(login_url="/admin/login/")
@require_http_methods(["GET", "POST"])
def api_dados_grafico_gastos(request):
    if request.method == "POST":
        try:
            body = json.loads(request.body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return JsonResponse(
                {"erro": "JSON inválido", "labels": [], "datasets": []},
                status=400,
            )
        src = body
    else:
        src = request.GET

    agrupamento = (src.get("agrupamento") or "mes").strip().lower()
    if agrupamento not in ("dia", "semana", "mes", "ano"):
        agrupamento = "mes"

    planos_raw = src.get("planos")
    if isinstance(planos_raw, list):
        plano_ids = [str(p).strip() for p in planos_raw if str(p).strip()]
    else:
        planos_raw = (planos_raw or "").strip()
        plano_ids = [p.strip() for p in planos_raw.split(",") if p.strip()]

    hoje = date.today()
    data_ini = _grafico_gastos_parse_date(src.get("inicio"))
    data_fim = _grafico_gastos_parse_date(src.get("fim"))
    if data_ini is None:
        data_ini = (hoje.replace(day=1) - timedelta(days=1)).replace(day=1)
    if data_fim is None:
        data_fim = hoje
    if data_ini > data_fim:
        data_ini, data_fim = data_fim, data_ini

    individual = str(src.get("individual") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    por = (src.get("por") or "vencimento").strip().lower()
    valor = (src.get("valor") or "bruto").strip().lower()

    todos_planos = str(src.get("todos_planos") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    planos_excluir_raw = src.get("planos_excluir")
    if isinstance(planos_excluir_raw, list):
        planos_excluir = [str(p).strip() for p in planos_excluir_raw if str(p).strip()]
    else:
        planos_excluir = [
            p.strip()
            for p in (planos_excluir_raw or "").split("|")
            if p.strip()
        ]

    modo_tempo = (src.get("modo_tempo") or "real").strip().lower()
    data_ref = None
    if modo_tempo in ("historico", "comparar"):
        data_ref = _grafico_gastos_parse_date(src.get("data_referencia"))
        if data_ref is None:
            data_ref = hoje
        if data_ref > hoje:
            data_ref = hoje

    common_kw = dict(
        data_de=data_ini,
        data_ate=data_fim,
        agrupamento=agrupamento,
        plano_ids=plano_ids,
        planos_excluir_nomes=planos_excluir,
        todos_planos=todos_planos,
        por=por,
        valor=valor,
    )

    from produtos.lancamentos_financeiro_pg_analytics_util import grafico_gastos_serie_pg

    if modo_tempo == "comparar":
        common = dict(
            **common_kw,
            individual=False,
        )
        real = grafico_gastos_serie_pg(**common, data_referencia=None)
        if not real.get("ok"):
            payload = real
        else:
            hist = grafico_gastos_serie_pg(**common, data_referencia=data_ref)
            if not hist.get("ok"):
                payload = hist
            else:
                payload = _grafico_gastos_comparar_payload_from_series(real, hist, data_ref)
    else:
        payload = grafico_gastos_serie_pg(
            individual=individual,
            data_referencia=data_ref if modo_tempo == "historico" else None,
            **common_kw,
        )
    if not payload.get("ok"):
        return JsonResponse(
            {
                "erro": payload.get("erro") or "Falha na agregação",
                "labels": [],
                "datasets": [],
            },
            status=500,
        )
    return JsonResponse(_grafico_gastos_api_json(payload))


def _grafico_gastos_api_json(payload: dict) -> dict:
    out = {
        "labels": payload.get("labels") or [],
        "bucket_keys": payload.get("bucket_keys") or [],
        "datasets": payload.get("datasets") or [],
    }
    if payload.get("modo_tempo"):
        out["modo_tempo"] = payload["modo_tempo"]
    if payload.get("data_referencia"):
        out["data_referencia"] = payload["data_referencia"]
    if payload.get("deltas") is not None:
        out["deltas"] = payload["deltas"]
    if payload.get("comparacao"):
        out["comparacao"] = payload["comparacao"]
    return out


def _grafico_gastos_comparar_payload_from_series(real: dict, hist: dict, data_ref) -> dict:
    """Monta payload comparar a partir de duas séries já calculadas."""
    labels = real.get("labels") or []
    bucket_keys = real.get("bucket_keys") or []
    n = len(labels)
    real_data = list((real.get("datasets") or [{}])[0].get("data") or [])
    hist_data = list((hist.get("datasets") or [{}])[0].get("data") or [])
    if len(real_data) < n:
        real_data.extend([0.0] * (n - len(real_data)))
    if len(hist_data) < n:
        hist_data.extend([0.0] * (n - len(hist_data)))
    real_data = real_data[:n]
    hist_data = hist_data[:n]
    deltas = [round(float(r) - float(h), 2) for r, h in zip(real_data, hist_data)]
    total_real = round(sum(real_data), 2)
    total_hist = round(sum(hist_data), 2)
    ref_label = data_ref.strftime("%d/%m/%Y")
    return {
        "ok": True,
        "erro": None,
        "labels": labels,
        "bucket_keys": bucket_keys,
        "modo_tempo": "comparar",
        "data_referencia": data_ref.isoformat(),
        "deltas": deltas,
        "comparacao": {
            "total_real": total_real,
            "total_historico": total_hist,
            "delta_total": round(total_real - total_hist, 2),
        },
        "datasets": [
            {
                "label": "Tempo real (hoje)",
                "data": real_data,
                "borderColor": "#059669",
                "backgroundColor": "rgba(5, 150, 105, 0.08)",
                "ggSerie": "real",
            },
            {
                "label": f"Como era ({ref_label})",
                "data": hist_data,
                "borderColor": "#d97706",
                "backgroundColor": "rgba(217, 119, 6, 0.08)",
                "ggSerie": "historico",
            },
        ],
    }


def _grafico_gastos_atalhos_lista() -> list[dict]:
    by_slot = {a.slot: a for a in GraficoGastosAtalhoAgro.objects.all()}
    out = []
    for slot in range(1, 5):
        row = by_slot.get(slot)
        if row and row.nome:
            out.append(
                {
                    "slot": slot,
                    "nome": row.nome,
                    "payload": row.payload or {},
                    "eh_padrao": bool(row.eh_padrao),
                    "atualizado_em": row.atualizado_em.isoformat()
                    if row.atualizado_em
                    else None,
                }
            )
        else:
            out.append(
                {
                    "slot": slot,
                    "nome": "",
                    "payload": None,
                    "eh_padrao": False,
                    "atualizado_em": None,
                }
            )
    return out


@never_cache
@login_required(login_url="/admin/login/")
@require_http_methods(["GET"])
def api_grafico_gastos_atalhos(request):
    atalhos = _grafico_gastos_atalhos_lista()
    slot_padrao = next((a["slot"] for a in atalhos if a.get("eh_padrao")), None)
    return JsonResponse({"atalhos": atalhos, "slot_padrao": slot_padrao})


@never_cache
@login_required(login_url="/admin/login/")
@require_http_methods(["POST"])
def api_grafico_gastos_atalho_salvar(request, slot: int):
    if slot not in (1, 2, 3, 4):
        return JsonResponse({"erro": "Slot inválido"}, status=400)
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"erro": "JSON inválido"}, status=400)
    nome = (body.get("nome") or "").strip()
    payload = body.get("payload")
    if not isinstance(payload, dict):
        return JsonResponse({"erro": "Payload inválido"}, status=400)
    existente = GraficoGastosAtalhoAgro.objects.filter(slot=slot).first()
    if not nome and not (existente and existente.nome):
        return JsonResponse({"erro": "Informe um nome para o atalho"}, status=400)
    if not nome and existente:
        nome = existente.nome
    obj, _ = GraficoGastosAtalhoAgro.objects.update_or_create(
        slot=slot,
        defaults={
            "nome": nome[:80],
            "payload": payload,
            "atualizado_por": request.user,
        },
    )
    return JsonResponse(
        {
            "ok": True,
            "slot": obj.slot,
            "nome": obj.nome,
            "payload": obj.payload,
            "atualizado_em": obj.atualizado_em.isoformat(),
        }
    )


@never_cache
@login_required(login_url="/admin/login/")
@require_http_methods(["POST"])
def api_grafico_gastos_atalho_padrao(request, slot: int):
    if slot not in (1, 2, 3, 4):
        return JsonResponse({"erro": "Slot inválido"}, status=400)
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"erro": "JSON inválido"}, status=400)
    acao = (body.get("acao") or "definir").strip().lower()
    if acao == "remover":
        GraficoGastosAtalhoAgro.objects.filter(slot=slot, eh_padrao=True).update(eh_padrao=False)
        return JsonResponse({"ok": True, "slot_padrao": None})
    row = GraficoGastosAtalhoAgro.objects.filter(slot=slot).first()
    if not row or not row.nome:
        return JsonResponse({"erro": "Salve um atalho neste slot antes de fixar."}, status=400)
    with transaction.atomic():
        GraficoGastosAtalhoAgro.objects.filter(eh_padrao=True).update(eh_padrao=False)
        GraficoGastosAtalhoAgro.objects.filter(pk=row.pk).update(eh_padrao=True)
    return JsonResponse({"ok": True, "slot_padrao": slot})


@login_required(login_url="/admin/login/")
def classificacao_despesas_lista(request):
    """Lista planos CP classificados (staff) — para Renan conferir fixa/variável/outra."""
    if not getattr(request.user, "is_staff", False):
        from django.http import HttpResponseNotFound

        return HttpResponseNotFound()
    from django.http import HttpResponse

    from financeiro.services.gastos_variacao_pg import _GRUPO_LABEL, _grupo_despesa_ui
    from produtos.models import TituloFinanceiroAgro

    planos = sorted(
        {
            str(p or "").strip()
            for p in TituloFinanceiroAgro.objects.filter(despesa=True).values_list(
                "plano_conta", flat=True
            )
            if str(p or "").strip()
        },
        key=lambda x: x.casefold(),
    )
    grupos: dict[str, list[str]] = {"fixa": [], "variavel": [], "outra": []}
    for nome in planos:
        grupos[_grupo_despesa_ui(nome)].append(nome)

    lines = [
        "CLASSIFICAÇÃO ATUAL — planos de despesa (CP)",
        f"Total: {len(planos)} planos",
        "",
    ]
    for key in ("fixa", "variavel", "outra"):
        rows = grupos[key]
        lines.append(f"=== {_GRUPO_LABEL[key].upper()} ({len(rows)}) ===")
        for nome in rows:
            lines.append(f"- {nome}")
        lines.append("")
    body = "\n".join(lines)
    return HttpResponse(body, content_type="text/plain; charset=utf-8")


@login_required(login_url="/admin/login/")
def simulacao_unificar_planos_despesa(request):
    """Staff: simulação só leitura do mapa de unificação de planos."""
    if not getattr(request.user, "is_staff", False):
        from django.http import HttpResponseNotFound

        return HttpResponseNotFound()
    from django.http import HttpResponse
    from pathlib import Path

    from django.conf import settings

    from produtos.management.commands.unificar_planos_despesa import (
        _carregar_mapa,
        formatar_relatorio,
        simular_unificacao,
    )

    path = Path(settings.BASE_DIR) / "docs" / "dados" / "plano_despesas_mapa_unificacao.csv"
    try:
        pares = _carregar_mapa(path)
        sim = simular_unificacao(pares)
        body = formatar_relatorio(sim)
    except Exception as e:
        body = f"Erro na simulação: {e}"
    return HttpResponse(body, content_type="text/plain; charset=utf-8")
