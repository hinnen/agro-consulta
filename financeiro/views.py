from datetime import date, timedelta
import json

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_http_methods

from base.models import Empresa

from financeiro.services.dashboard_financeiro import get_dashboard_data
from produtos.mongo_financeiro_util import (
    grafico_gastos_planos_despesa_mongo,
    grafico_gastos_serie_mongo,
)
from produtos.views import _dashboard_periodo_from_request, obter_conexao_mongo


@login_required(login_url="/admin/login/")
def dashboard_financeiro_completo(request):
    empresas = Empresa.objects.filter(ativo=True).order_by("nome_fantasia")
    default_eid = empresas.values_list("pk", flat=True).first()
    empresa_id = int(request.GET.get("empresa") or default_eid or 0)
    if empresa_id:
        get_object_or_404(Empresa, pk=empresa_id, ativo=True)

    data_ini, data_fim, periodo_label, periodo_key = _dashboard_periodo_from_request(
        request
    )

    fonte = (request.GET.get("fonte") or "mongo").strip().lower()
    por = (request.GET.get("por") or "competencia").strip().lower()
    valor = (request.GET.get("valor") or "bruto").strip().lower()
    filtro_contas = (request.GET.get("contas") or "").strip()

    _, mongo_db = obter_conexao_mongo()

    dados = (
        get_dashboard_data(
            empresa_id,
            data_ini,
            data_fim,
            fonte=fonte,
            por=por,
            valor=valor,
            filtro_contas=filtro_contas,
            mongo_db=mongo_db,
        )
        if empresa_id
        else None
    )
    chart_bootstrap = None
    if dados:
        chart_bootstrap = {
            "labels": dados["extras"]["grafico_labels"],
            "data": dados["extras"]["grafico_data"],
        }
    filtro_dashboard = {
        "fonte": fonte,
        "por": por,
        "valor": valor,
        "contas": filtro_contas,
    }
    return render(
        request,
        "financeiro/dashboard_completo.html",
        {
            "empresas": empresas,
            "empresa_id": empresa_id,
            "dados": dados,
            "chart_bootstrap": chart_bootstrap,
            "filtro_dashboard": filtro_dashboard,
            "periodo_key": periodo_key,
            "periodo_label": periodo_label,
            "periodo_cal_ini": data_ini.isoformat(),
            "periodo_cal_fim": data_fim.isoformat(),
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
    _, mongo_db = obter_conexao_mongo()
    planos_conta = grafico_gastos_planos_despesa_mongo(mongo_db)
    hoje = date.today()
    padrao_ini = (hoje.replace(day=1) - timedelta(days=1)).replace(day=1)
    return render(
        request,
        "financeiro/grafico_gastos.html",
        {
            "planos_conta": planos_conta,
            "total_planos": len(planos_conta),
            "data_inicial_padrao": padrao_ini.isoformat(),
            "data_final_padrao": hoje.isoformat(),
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

    _, mongo_db = obter_conexao_mongo()
    if mongo_db is None:
        return JsonResponse(
            {"erro": "Mongo indisponível", "labels": [], "datasets": []},
            status=503,
        )

    payload = grafico_gastos_serie_mongo(
        mongo_db,
        data_de=data_ini,
        data_ate=data_fim,
        agrupamento=agrupamento,
        plano_ids=plano_ids,
        planos_excluir_nomes=planos_excluir,
        todos_planos=todos_planos,
        individual=individual,
        por=por,
        valor=valor,
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
    return JsonResponse(
        {"labels": payload["labels"], "datasets": payload["datasets"]}
    )
