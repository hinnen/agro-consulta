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


class _AuthAPIView(APIView):
    authentication_classes = [SessionAuthentication]
    permission_classes = [IsAuthenticated]


class ResumoOperacionalAPIView(_AuthAPIView):
    def get(self, request):
        serializer = ResumoOperacionalQuerySerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)

        params = serializer.validated_data
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

        consolidacao_service = ConsolidacaoFinanceiraService()
        equilibrio_service = EquilibrioFinanceiroService()

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
