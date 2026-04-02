from decimal import Decimal

from django.db.models import Max
from rest_framework.authentication import SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from financeiro.api.jsonutil import json_safe
from estoque.models import IndicadorProdutoLoja


class _SessionAuthAPIView(APIView):
    authentication_classes = [SessionAuthentication]
    permission_classes = [IsAuthenticated]


def _dec(d):
    return str(d) if isinstance(d, Decimal) else str(Decimal(str(d)))


class SugestoesInteligentesAPIView(_SessionAuthAPIView):
    """
    GET /api/transferencias/sugestoes-inteligentes/
    Linhas de snapshot com transferência sugerida (origem + quantidade).
    """

    def get(self, request):
        empresa_id = request.query_params.get("empresa_id")
        loja_id = request.query_params.get("loja_id")
        if not empresa_id or not loja_id:
            return Response(
                {"detail": "empresa_id e loja_id são obrigatórios."}, status=400
            )
        try:
            empresa_id = int(empresa_id)
            loja_id = int(loja_id)
        except ValueError:
            return Response({"detail": "IDs inválidos."}, status=400)

        base_qs = IndicadorProdutoLoja.objects.filter(
            empresa_id=empresa_id, loja_id=loja_id
        )
        data_base = request.query_params.get("data_base")
        if data_base:
            ref_date = data_base
            qs = base_qs.filter(data_base=data_base)
        else:
            latest = base_qs.aggregate(m=Max("data_base"))["m"]
            if not latest:
                return Response(json_safe({"data_base": None, "sugestoes": []}))
            ref_date = latest
            qs = base_qs.filter(data_base=latest)

        qs = qs.select_related("produto", "loja", "loja_origem_sugerida").filter(
            sugestao_acao__in=["TRANSFERIR", "TRANSFERIR_E_COMPRAR"],
            qtd_transferir__gt=0,
        ).order_by("-score_prioridade", "-necessidade")

        out = []
        for row in qs[:300]:
            p = row.produto
            excedente_txt = ""
            orig = row.loja_origem_sugerida
            out.append(
                {
                    "loja_destino_id": row.loja_id,
                    "loja_destino_nome": row.loja.nome,
                    "loja_origem_id": row.loja_origem_sugerida_id,
                    "loja_origem_nome": orig.nome if orig else "",
                    "produto_id": p.id,
                    "produto_nome": p.nome,
                    "codigo_interno": p.codigo_interno,
                    "necessidade": _dec(row.necessidade),
                    "qtd_transferir": _dec(row.qtd_transferir),
                    "qtd_comprar": _dec(row.qtd_comprar),
                    "saldo_destino": _dec(row.saldo_atual),
                    "excedente_origem_resumo": excedente_txt,
                    "data_base": row.data_base.isoformat(),
                    "sugestao_acao": row.sugestao_acao,
                }
            )

        if hasattr(ref_date, "isoformat"):
            ref_out = ref_date.isoformat()
        else:
            ref_out = str(ref_date) if ref_date else None
        return Response(json_safe({"data_base": ref_out, "sugestoes": out}))
