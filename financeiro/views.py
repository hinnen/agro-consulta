from django.contrib.auth.decorators import login_required
from django.shortcuts import get_object_or_404, render

from base.models import Empresa

from financeiro.services.dashboard_financeiro import get_dashboard_data
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
