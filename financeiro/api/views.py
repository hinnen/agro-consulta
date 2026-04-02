from rest_framework.authentication import SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from rest_framework.views import APIView
from django.shortcuts import get_object_or_404

from financeiro.api.jsonutil import json_safe
from financeiro.models import GrupoEmpresarial
from financeiro.api.serializers import ResumoOperacionalQuerySerializer
from financeiro.services.consolidacao import ConsolidacaoFinanceiraService
from financeiro.services.equilibrio import EquilibrioFinanceiroService
from financeiro.services.resumo_operacional_mongo import (
    consolidar_empresa_mongo,
    consolidar_grupo_mongo,
)


class _AuthAPIView(APIView):
    authentication_classes = [SessionAuthentication]
    permission_classes = [IsAuthenticated]


class ResumoOperacionalAPIView(_AuthAPIView):
    def get(self, request):
        serializer = ResumoOperacionalQuerySerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)

        params = serializer.validated_data

        if params.get("fonte") == "mongo":
            from produtos.views import obter_conexao_mongo

            _, db = obter_conexao_mongo()
            if db is None:
                return Response(
                    {"detail": "Mongo indisponível — necessário para ler DtoLancamento (ERP)."},
                    status=status.HTTP_503_SERVICE_UNAVAILABLE,
                )
            por = params.get("por") or "competencia"
            valor = params.get("valor") or "bruto"
            fc = (params.get("contas") or "").strip()
            if params["modo"] == "empresa":
                data = consolidar_empresa_mongo(
                    db,
                    empresa_id=params["empresa_id"],
                    data_inicio=params["data_inicio"],
                    data_fim=params["data_fim"],
                    por=por,
                    valor=valor,
                    filtro_contas=fc,
                )
            else:
                get_object_or_404(
                    GrupoEmpresarial, pk=params["grupo_id"], ativo=True
                )
                data = consolidar_grupo_mongo(
                    db,
                    grupo_id=params["grupo_id"],
                    data_inicio=params["data_inicio"],
                    data_fim=params["data_fim"],
                    por=por,
                    valor=valor,
                    filtro_contas=fc,
                )
            if data.get("erro"):
                return Response(
                    json_safe({"detail": data["erro"], **data}),
                    status=status.HTTP_400_BAD_REQUEST,
                )
            if not params.get("incluir_linhas"):
                if isinstance(data, dict) and "linhas_dre" in data:
                    data = {k: v for k, v in data.items() if k != "linhas_dre"}
        else:
            service = ConsolidacaoFinanceiraService()
            if params["modo"] == "empresa":
                data = service.consolidar_empresa(
                    empresa_id=params["empresa_id"],
                    data_inicio=params["data_inicio"],
                    data_fim=params["data_fim"],
                )
            else:
                get_object_or_404(
                    GrupoEmpresarial, pk=params["grupo_id"], ativo=True
                )
                data = service.consolidar_grupo(
                    grupo_id=params["grupo_id"],
                    data_inicio=params["data_inicio"],
                    data_fim=params["data_fim"],
                )

        return Response(json_safe(data), status=status.HTTP_200_OK)


class GapEquilibrioAPIView(_AuthAPIView):
    def get(self, request):
        serializer = ResumoOperacionalQuerySerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        params = serializer.validated_data

        equilibrio_service = EquilibrioFinanceiroService()

        if params.get("fonte") == "mongo":
            from produtos.views import obter_conexao_mongo

            _, db = obter_conexao_mongo()
            if db is None:
                return Response(
                    {"detail": "Mongo indisponível."},
                    status=status.HTTP_503_SERVICE_UNAVAILABLE,
                )
            por = params.get("por") or "competencia"
            valor = params.get("valor") or "bruto"
            fc = (params.get("contas") or "").strip()
            if params["modo"] == "empresa":
                pack = consolidar_empresa_mongo(
                    db,
                    empresa_id=params["empresa_id"],
                    data_inicio=params["data_inicio"],
                    data_fim=params["data_fim"],
                    por=por,
                    valor=valor,
                    filtro_contas=fc,
                )
                if pack.get("erro"):
                    return Response(
                        json_safe({"detail": pack["erro"]}),
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                resumo = pack
            else:
                get_object_or_404(
                    GrupoEmpresarial, pk=params["grupo_id"], ativo=True
                )
                grupo = consolidar_grupo_mongo(
                    db,
                    grupo_id=params["grupo_id"],
                    data_inicio=params["data_inicio"],
                    data_fim=params["data_fim"],
                    por=por,
                    valor=valor,
                    filtro_contas=fc,
                )
                if grupo.get("erro"):
                    return Response(
                        json_safe({"detail": grupo["erro"]}),
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                resumo = grupo["consolidado"]
        else:
            consolidacao_service = ConsolidacaoFinanceiraService()
            if params["modo"] == "empresa":
                resumo = consolidacao_service.consolidar_empresa(
                    empresa_id=params["empresa_id"],
                    data_inicio=params["data_inicio"],
                    data_fim=params["data_fim"],
                )
            else:
                get_object_or_404(
                    GrupoEmpresarial, pk=params["grupo_id"], ativo=True
                )
                grupo = consolidacao_service.consolidar_grupo(
                    grupo_id=params["grupo_id"],
                    data_inicio=params["data_inicio"],
                    data_fim=params["data_fim"],
                )
                resumo = grupo["consolidado"]

        dias = params.get("dias_periodo") or 30
        data = equilibrio_service.calcular(
            receita_operacional=resumo["receita_operacional"],
            cmv=resumo["cmv"],
            despesas_fixas=resumo["despesas_fixas"],
            despesas_variaveis=resumo["despesas_variaveis"],
            dias_periodo=dias,
        )
        return Response(json_safe(data), status=status.HTTP_200_OK)
