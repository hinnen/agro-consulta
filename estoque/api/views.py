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


def _decimal_str(d):
    if d is None:
        return "0"
    if isinstance(d, Decimal):
        return str(d)
    return str(Decimal(str(d)))


class IndicadoresReposicaoAPIView(_SessionAuthAPIView):
    """
    GET /api/indicadores/reposicao/
    Snapshot persistido (sem recálculo pesado).
    """

    def get(self, request):
        empresa_id = request.query_params.get("empresa_id")
        loja_id = request.query_params.get("loja_id")
        if not empresa_id or not loja_id:
            return Response(
                {"detail": "empresa_id e loja_id são obrigatórios."},
                status=400,
            )

        try:
            empresa_id = int(empresa_id)
            loja_id = int(loja_id)
        except ValueError:
            return Response({"detail": "IDs inválidos."}, status=400)

        qs = IndicadorProdutoLoja.objects.filter(
            empresa_id=empresa_id, loja_id=loja_id
        ).select_related("produto", "loja", "loja_origem_sugerida")

        data_base = request.query_params.get("data_base")
        if data_base:
            qs = qs.filter(data_base=data_base)
            ref_date = data_base
        else:
            latest = qs.aggregate(m=Max("data_base"))["m"]
            if not latest:
                return Response(json_safe({"data_base": None, "itens": []}))
            qs = qs.filter(data_base=latest)
            ref_date = latest

        classe = (request.query_params.get("classe_abc") or "").strip().upper()
        if classe in ("A", "B", "C"):
            qs = qs.filter(classe_abc=classe)

        marca = (request.query_params.get("marca") or "").strip()
        if marca:
            qs = qs.filter(produto__marca__icontains=marca)
        marca_id = (request.query_params.get("marca_id") or "").strip()
        if marca_id and marca_id.isdigit():
            qs = qs.filter(produto_id=int(marca_id))

        categoria = (request.query_params.get("categoria") or "").strip()
        if categoria:
            qs = qs.filter(produto__categoria__icontains=categoria)
        categoria_id = (request.query_params.get("categoria_id") or "").strip()
        if categoria_id:
            qs = qs.filter(produto__categoria__icontains=categoria_id)

        sn = (request.query_params.get("somente_com_necessidade") or "").lower()
        if sn in ("1", "true", "yes", "sim"):
            qs = qs.filter(necessidade__gt=0)

        ordem = (request.query_params.get("ordenacao") or "score").strip().lower()
        if ordem == "necessidade":
            qs = qs.order_by("-necessidade", "-score_prioridade", "produto__nome")
        elif ordem == "cobertura":
            qs = qs.order_by("dias_cobertura_atual", "-score_prioridade")
        else:
            qs = qs.order_by("-score_prioridade", "-necessidade", "produto__nome")

        itens = []
        for row in qs[:500]:
            p = row.produto
            itens.append(
                {
                    "produto_id": p.id,
                    "codigo_interno": p.codigo_interno,
                    "nome": p.nome,
                    "saldo_atual": _decimal_str(row.saldo_atual),
                    "venda_media_dia": _decimal_str(row.venda_media_dia),
                    "dias_cobertura_atual": _decimal_str(row.dias_cobertura_atual),
                    "estoque_minimo": _decimal_str(row.estoque_minimo),
                    "estoque_ideal": _decimal_str(row.estoque_ideal),
                    "necessidade": _decimal_str(row.necessidade),
                    "sugestao_acao": row.sugestao_acao,
                    "qtd_transferir": _decimal_str(row.qtd_transferir),
                    "qtd_comprar": _decimal_str(row.qtd_comprar),
                    "score_prioridade": _decimal_str(row.score_prioridade),
                    "classe_abc": row.classe_abc,
                    "classe_criticidade": row.classe_criticidade,
                    "loja_origem_sugerida_id": row.loja_origem_sugerida_id,
                    "loja_origem_sugerida_nome": (
                        row.loja_origem_sugerida.nome
                        if row.loja_origem_sugerida_id
                        else ""
                    ),
                    "data_base": row.data_base.isoformat(),
                }
            )

        if hasattr(ref_date, "isoformat"):
            ref_out = ref_date.isoformat()
        else:
            ref_out = str(ref_date) if ref_date else None
        return Response(
            json_safe(
                {
                    "data_base": ref_out,
                    "total": len(itens),
                    "itens": itens,
                }
            )
        )


class IndicadoresReposicaoPingAPIView(_SessionAuthAPIView):
    """Evita 404 em health check humano."""

    def get(self, request):
        return Response({"ok": True, "recurso": "indicadores/reposicao"})
