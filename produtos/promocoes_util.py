"""Regras de promoção Agro (configuração + aplicação no PDV)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from django.db.models import Q
from django.utils import timezone

from .models import PromocaoAgro, PromocaoProdutoAgro

TELAS_PROMOCAO = (
    {"id": "pdv", "label": "PDV"},
    {"id": "venda_direta", "label": "Venda Direta"},
    {"id": "catalogo", "label": "Catálogo"},
)

EMPRESAS_PROMOCAO = (
    {"id": "centro", "label": "Agro Mais Centro"},
    {"id": "vila", "label": "Agro Mais Vila Elias"},
)

TIPO_LABEL = {
    PromocaoAgro.Tipo.LEVE_PAGUE: "Leve X, pague Y",
    PromocaoAgro.Tipo.ACIMA_UNIDADES: "Acima de X unidades, pague Y",
    PromocaoAgro.Tipo.VALOR_DIRETO: "Valor direto",
}


def _float_val(v, default=0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def promocao_tipo_label(tipo: str) -> str:
    return TIPO_LABEL.get(tipo, tipo or "—")


def promocao_eh_permanente(promo: PromocaoAgro) -> bool:
    if getattr(promo, "permanente", False):
        return True
    fim = getattr(promo, "data_fim", None)
    return bool(fim and fim.year >= 2090)


def promocao_ativa_hoje(promo: PromocaoAgro, hoje: date | None = None) -> bool:
    if not promo.ativo:
        return False
    hoje = hoje or timezone.localdate()
    if hoje < promo.data_inicio:
        return False
    if promocao_eh_permanente(promo):
        return True
    fim = promo.data_fim
    return bool(fim and hoje <= fim)


def promocao_aplica_tela(promo: PromocaoAgro, tela: str = "pdv") -> bool:
    """Lista vazia = válida em todo o sistema."""
    telas = promo.telas if isinstance(promo.telas, list) else []
    if not telas:
        return True
    return str(tela or "pdv").strip().lower() in {str(t).strip().lower() for t in telas}


def promocao_aplica_empresa(promo: PromocaoAgro, empresa: str = "centro") -> bool:
    empresas = promo.empresas if isinstance(promo.empresas, list) else []
    if not empresas:
        return True
    emp = str(empresa or "centro").strip().lower()
    return emp in {str(e).strip().lower() for e in empresas}


def criterio_promocao_atendido(
    tipo: str,
    qtd_x: float,
    quantidade: float,
) -> bool:
    qtd = _float_val(quantidade)
    lim = _float_val(qtd_x)
    if lim <= 0:
        return False
    if tipo == PromocaoAgro.Tipo.LEVE_PAGUE:
        return qtd >= lim
    if tipo == PromocaoAgro.Tipo.ACIMA_UNIDADES:
        return qtd > lim
    if tipo == PromocaoAgro.Tipo.VALOR_DIRETO:
        return True
    return False


def calcular_preco_promocional(
    *,
    tipo: str,
    qtd_x: float | Decimal | None,
    preco_y: float | Decimal | None,
    quantidade: float,
    preco_padrao: float,
    preco_produto_promo: float | Decimal | None = None,
) -> float:
    """Retorna o preço unitário a aplicar no carrinho."""
    padrao = _float_val(preco_padrao)
    if tipo == PromocaoAgro.Tipo.VALOR_DIRETO:
        pp = _float_val(preco_produto_promo, 0)
        if pp > 0:
            return round(pp, 4)
        py = _float_val(preco_y, 0)
        return round(py, 4) if py > 0 else padrao

    lim = _float_val(qtd_x)
    py = _float_val(preco_y)
    if lim <= 0 or py <= 0:
        return padrao
    if criterio_promocao_atendido(tipo, lim, quantidade):
        return round(py, 4)
    return padrao


def _promo_para_dict(
    promo: PromocaoAgro,
    *,
    preco_produto_promo: float | None = None,
) -> dict[str, Any]:
    return {
        "id": promo.pk,
        "nome": promo.nome,
        "tipo": promo.tipo,
        "tipo_label": promocao_tipo_label(promo.tipo),
        "qtd_x": _float_val(promo.qtd_x),
        "preco_y": _float_val(promo.preco_y),
        "preco_produto_promo": _float_val(preco_produto_promo),
    }


def buscar_promocoes_pdv_ativas(
    *,
    empresa: str = "centro",
    tela: str = "pdv",
    hoje: date | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Mapa produto_externo_id → regra de promoção vigente.
    Se um produto estiver em mais de uma promoção ativa, prevalece a de menor pk (mais antiga).
    """
    hoje = hoje or timezone.localdate()
    promos = (
        PromocaoAgro.objects.filter(ativo=True, data_inicio__lte=hoje)
        .filter(Q(permanente=True) | Q(data_fim__isnull=True) | Q(data_fim__gte=hoje))
        .prefetch_related("produtos")
        .order_by("pk")
    )
    out: dict[str, dict[str, Any]] = {}
    for promo in promos:
        if not promocao_aplica_tela(promo, tela):
            continue
        if not promocao_aplica_empresa(promo, empresa):
            continue
        for linha in promo.produtos.all():
            pid = str(linha.produto_externo_id or "").strip()
            if not pid:
                continue
            pp = None
            if promo.tipo == PromocaoAgro.Tipo.VALOR_DIRETO:
                pp = _float_val(linha.preco_promocional, 0) or None
            out[pid] = _promo_para_dict(promo, preco_produto_promo=pp)
    return out


def aplicar_promocao_em_produto_dict(
    row: dict[str, Any],
    promo_map: dict[str, dict[str, Any]],
    *,
    quantidade: float = 1,
) -> dict[str, Any]:
    """Ajusta preco_venda e anexa metadados de promoção no dict do produto."""
    pid = str(row.get("id") or row.get("Id") or "").strip()
    if not pid or pid not in promo_map:
        return row
    promo = promo_map[pid]
    preco_padrao = _float_val(row.get("preco_venda") or row.get("preco") or 0)
    preco = calcular_preco_promocional(
        tipo=promo["tipo"],
        qtd_x=promo.get("qtd_x"),
        preco_y=promo.get("preco_y"),
        quantidade=quantidade,
        preco_padrao=preco_padrao,
        preco_produto_promo=promo.get("preco_produto_promo"),
    )
    row = dict(row)
    row["preco_padrao"] = preco_padrao
    row["preco_venda"] = preco
    row["promocao"] = promo
    return row


def empresas_promocao_labels(ids: list) -> str:
    labels = {e["id"]: e["label"] for e in EMPRESAS_PROMOCAO}
    parts = [labels.get(str(i).strip().lower(), str(i)) for i in (ids or [])]
    return ", ".join(parts) if parts else "Todas"


def telas_promocao_labels(ids: list) -> str:
    if not ids:
        return "Todo o sistema"
    labels = {t["id"]: t["label"] for t in TELAS_PROMOCAO}
    parts = [labels.get(str(i).strip().lower(), str(i)) for i in ids]
    return ", ".join(parts) if parts else "Todo o sistema"


def buscar_produtos_para_promocao(q: str, *, limit: int = 24) -> list[dict[str, Any]]:
    """Busca produtos no Mongo (mesmo motor da Consulta) para a tela de promoções."""
    from produtos.busca_produtos_mongo import buscar_produtos_motor_pdv
    from produtos.views import (
        _float_api_json,
        _merge_produtos_overlay_codigo_consulta,
        _valor_texto_campo,
        obter_conexao_mongo,
    )

    q = str(q or "").strip()
    if len(q) < 2:
        return []
    client, db = obter_conexao_mongo()
    if db is None or client is None:
        return []
    prods = buscar_produtos_motor_pdv(q, limit=max(limit, 40))
    prods = _merge_produtos_overlay_codigo_consulta(q, prods, db, client)
    out: list[dict[str, Any]] = []
    for p in prods:
        pid = str(p.get("Id") or p.get("_id") or "").strip()
        if not pid or pid.lower() == "none":
            continue
        codigo = _valor_texto_campo(p.get("Codigo"))
        cod_nf = p.get("CodigoNFe")
        codigo_nfe = (_valor_texto_campo(cod_nf) if cod_nf not in (None, "") else "") or codigo
        pv = _float_api_json(p.get("ValorVenda") or p.get("PrecoVenda") or 0)
        out.append(
            {
                "produto_externo_id": pid,
                "codigo": codigo_nfe or codigo,
                "nome_produto": str(p.get("Nome") or "").strip(),
                "preco_padrao": round(float(pv), 2),
            }
        )
        if len(out) >= limit:
            break
    return out
