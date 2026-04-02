import logging

from django.conf import settings
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.authentication import SessionAuthentication
from rest_framework.permissions import BasePermission, IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from base.models import Empresa
from financeiro.api.jsonutil import json_safe
from financeiro.models import GrupoEmpresarial
from financeiro.api.serializers import (
    DebugMongoResumoQuerySerializer,
    ResumoOperacionalQuerySerializer,
)
from financeiro.services.consolidacao import ConsolidacaoFinanceiraService
from financeiro.services.equilibrio import EquilibrioFinanceiroService
from financeiro.services.resumo_operacional_mongo import (
    consolidar_empresa_mongo,
    consolidar_grupo_mongo,
)

_log_resumo = logging.getLogger("financeiro.resumo_diagnostico")


def _resumo_diagnostico_ativo(request) -> bool:
    return request.query_params.get("debug_resumo") == "1" or getattr(
        settings, "FINANCEIRO_DEBUG_RESUMO", False
    )


class _AuthAPIView(APIView):
    authentication_classes = [SessionAuthentication]
    permission_classes = [IsAuthenticated]


class IsStaffUser(BasePermission):
    def has_permission(self, request, view):
        u = getattr(request, "user", None)
        return bool(u and u.is_authenticated and getattr(u, "is_staff", False))


class _StaffAuthAPIView(APIView):
    authentication_classes = [SessionAuthentication]
    permission_classes = [IsAuthenticated, IsStaffUser]


class DebugMongoResumoAPIView(_StaffAuthAPIView):
    """
    Contagens DtoLancamento alinhadas ao resumo gerencial (para achar onde some o dado).
    Remova ou restrinja após o diagnóstico.
    """

    def get(self, request):
        serializer = DebugMongoResumoQuerySerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        p = serializer.validated_data

        empresa = get_object_or_404(Empresa, pk=p["empresa_id"])
        nome = (empresa.nome_fantasia or "").strip()
        if not nome:
            return Response(
                {"detail": "Cadastre nome fantasia da empresa (filtro Mongo)."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        from produtos.mongo_financeiro_util import debug_resumo_mongo_lens
        from produtos.views import obter_conexao_mongo

        _, db = obter_conexao_mongo()
        if db is None:
            return Response({"detail": "Mongo indisponível."}, status=status.HTTP_503_SERVICE_UNAVAILABLE)

        fc = (p.get("contas") or "").strip().lower() or (
            getattr(settings, "DRE_RESULTADO_FILTRO", "resultado") or "resultado"
        )
        if fc not in ("resultado", "resultado_erp", "todas"):
            fc = "resultado"
        extra = getattr(settings, "DRE_RESULTADO_EXCLUIR_REGEX_EXTRA", "") or ""

        raw = debug_resumo_mongo_lens(
            db,
            data_de=p["data_inicio"],
            data_ate=p["data_fim"],
            por=p.get("por") or "competencia",
            filtro_contas=fc,
            regex_excluir_extra=extra or None,
            empresa=nome,
            empresa_id=str(empresa.pk),
        )
        if not raw.get("ok"):
            return Response(
                json_safe(
                    {
                        "detail": raw.get("erro") or "Falha ao consultar Mongo",
                        "total_documentos_periodo": raw.get("total_documentos_periodo", 0),
                        "total_documentos_empresa": raw.get("total_documentos_empresa", 0),
                        "total_documentos_resultado": raw.get("total_documentos_resultado", 0),
                        "exemplos_empresa_distinta": raw.get("exemplos_empresa_distinta", []),
                        "exemplos_planos_conta": raw.get("exemplos_planos_conta", []),
                    }
                ),
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        body = {
            "total_documentos_periodo": raw["total_documentos_periodo"],
            "total_documentos_empresa": raw["total_documentos_empresa"],
            "total_documentos_resultado": raw["total_documentos_resultado"],
            "exemplos_empresa_distinta": raw["exemplos_empresa_distinta"],
            "exemplos_planos_conta": raw["exemplos_planos_conta"],
            "campo_data_mongo": raw.get("campo_data"),
            "filtro_contas_mongo": raw.get("filtro_contas"),
            "empresa_id": empresa.pk,
            "empresa_nome_filtro": nome,
            "periodo": {
                "de": p["data_inicio"].isoformat(),
                "ate": p["data_fim"].isoformat(),
            },
        }
        return Response(json_safe(body), status=status.HTTP_200_OK)


class ResumoOperacionalAPIView(_AuthAPIView):
    def get(self, request):
        serializer = ResumoOperacionalQuerySerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)

        params = serializer.validated_data
        diagnostico = _resumo_diagnostico_ativo(request)

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
                    diagnostico=diagnostico,
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
                    diagnostico=diagnostico,
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

        if diagnostico and params.get("fonte") == "mongo":
            _log_resumo.info(
                "[FINANCEIRO_RESUMO_DIAG] resumo_operacional_api_payload=%s",
                json_safe(data) if isinstance(data, dict) else data,
            )
        return Response(json_safe(data), status=status.HTTP_200_OK)


class GapEquilibrioAPIView(_AuthAPIView):
    def get(self, request):
        serializer = ResumoOperacionalQuerySerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        params = serializer.validated_data
        diagnostico = _resumo_diagnostico_ativo(request)

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
                    diagnostico=diagnostico,
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
                    diagnostico=diagnostico,
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
        if diagnostico and params.get("fonte") == "mongo":
            _log_resumo.info(
                "[FINANCEIRO_RESUMO_DIAG] gap_equilibrio_payload=%s (insumo receita_op=%s)",
                json_safe(data),
                resumo.get("receita_operacional"),
            )
        return Response(json_safe(data), status=status.HTTP_200_OK)
