"""Histórico de alteração do cadastro do produto (não é movimentação de estoque)."""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from produtos.models import ProdutoCadastroAlteracaoAgro

# Campos do overlay + extras de cadastro (rótulos para a loja).
_CAMPOS_OVERLAY: list[tuple[str, str]] = [
    ("nome", "Nome"),
    ("descricao", "Descrição"),
    ("marca", "Marca"),
    ("categoria", "Categoria"),
    ("subcategoria", "Subcategoria"),
    ("subcategoria_2", "Subcategoria 2"),
    ("subcategoria_3", "Subcategoria 3"),
    ("subcategoria_4", "Subcategoria 4"),
    ("fornecedor_texto", "Fornecedor"),
    ("unidade", "Unidade"),
    ("peso_etiqueta", "Peso (etiqueta)"),
    ("codigo_barras", "Código de barras"),
    ("codigo_nfe", "Código GM / NFe"),
    ("preco_venda", "Preço venda"),
    ("cashback_percentual", "Cashback %"),
    ("ativo_exibicao", "Ativo na listagem"),
    ("estoque_min_centro", "Estoque mín. Centro"),
    ("estoque_max_centro", "Estoque máx. Centro"),
    ("estoque_min_vila", "Estoque mín. Vila"),
    ("estoque_max_vila", "Estoque máx. Vila"),
]

_CAMPOS_EXTRAS: list[tuple[str, str]] = [
    ("preco_custo", "Preço custo"),
    ("modelo", "Modelo"),
    ("permite_venda_estoque_negativo", "Venda com estoque negativo"),
    ("validade", "Validade"),
    ("lote", "Lote"),
    ("fiscal_ncm", "NCM"),
    ("fiscal_cest", "CEST"),
    ("fiscal_cfop", "CFOP"),
    ("fiscal_csosn", "CSOSN"),
    ("fiscal_origem", "Origem mercadoria"),
    ("fiscal_cst_pis_cofins", "CST PIS/COFINS"),
    ("kit_baixa_componentes", "Kit: baixar componentes"),
    ("kit_deposito", "Kit: depósito"),
    ("precos_modo", "Modo preços"),
    ("precos_por_forma", "Preços por forma"),
    ("precos_grupos", "Preços grupos A/B"),
    ("ean_embalagem_nf", "EAN embalagem NF"),
    ("codigos_barras_opcionais", "Barras opcionais"),
    ("custo_familia", "Custo do saco (família)"),
    ("variacoes", "Marcas / códigos"),
]

_LABELS = {k: lab for k, lab in _CAMPOS_OVERLAY + _CAMPOS_EXTRAS}


def _fmt(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, bool):
        return "Sim" if val else "Não"
    if isinstance(val, Decimal):
        s = format(val, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s
    if isinstance(val, (dict, list)):
        try:
            return json.dumps(val, ensure_ascii=False, sort_keys=True)[:500]
        except Exception:
            return str(val)[:500]
    return str(val).strip()[:500]


def _norm_cmp(val: Any) -> str:
    """Normaliza para comparação (ignora ruído de formatação)."""
    if val is None:
        return ""
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, Decimal):
        try:
            return str(val.normalize())
        except Exception:
            return str(val)
    if isinstance(val, (int, float)):
        try:
            return str(Decimal(str(val)).normalize())
        except Exception:
            return str(val)
    if isinstance(val, (dict, list)):
        try:
            return json.dumps(val, ensure_ascii=False, sort_keys=True)
        except Exception:
            return str(val)
    return str(val).strip()


def snapshot_overlay(ov) -> dict[str, Any]:
    """Estado atual do overlay (cadastro) — sem saldo/estoque operacional."""
    out: dict[str, Any] = {}
    for key, _lab in _CAMPOS_OVERLAY:
        out[key] = getattr(ov, key, None)
    ex = ov.cadastro_extras if isinstance(getattr(ov, "cadastro_extras", None), dict) else {}
    fiscal = ex.get("fiscal") if isinstance(ex.get("fiscal"), dict) else {}
    kit = ex.get("kit") if isinstance(ex.get("kit"), dict) else {}
    out["preco_custo"] = ex.get("preco_custo_overlay")
    out["modelo"] = ex.get("modelo") or ""
    out["permite_venda_estoque_negativo"] = ex.get("permite_venda_estoque_negativo")
    out["validade"] = ex.get("validade") or ""
    out["lote"] = ex.get("lote") or ""
    out["fiscal_ncm"] = fiscal.get("ncm") or ""
    out["fiscal_cest"] = fiscal.get("cest") or ""
    out["fiscal_cfop"] = fiscal.get("cfop") or ""
    out["fiscal_csosn"] = fiscal.get("csosn") or ""
    out["fiscal_origem"] = fiscal.get("origem") or ""
    out["fiscal_cst_pis_cofins"] = fiscal.get("cst_pis_cofins") or ""
    out["kit_baixa_componentes"] = kit.get("baixa_componentes")
    out["kit_deposito"] = kit.get("deposito") or ""
    out["precos_modo"] = ex.get("precos_modo") or ""
    out["precos_por_forma"] = ex.get("precos_por_forma")
    out["precos_grupos"] = ex.get("precos_grupos")
    out["ean_embalagem_nf"] = ex.get("entrada_nfe_ean_embalagem") or ""
    out["codigos_barras_opcionais"] = ex.get("codigos_barras_opcionais") or []
    out["custo_familia"] = ex.get("custo_familia") if isinstance(ex.get("custo_familia"), dict) else None
    return out


def snapshot_efetivo_catalogo(produto_id: str) -> dict[str, Any]:
    """
    Valores que a loja já via (Produto PG), mesmo com overlay vazio.
    Usado no «antes» do histórico para não gravar — → valor em todo campo no 1º save PDV.
    """
    pid = str(produto_id or "").strip()[:64]
    if not pid:
        return {}
    out: dict[str, Any] = {}
    try:
        from produtos.catalogo_agro import obter_produto_model, produto_agro_para_row

        p = obter_produto_model(pid)
        if p is None:
            return out
        row = produto_agro_para_row(p) or {}
    except Exception:
        return out

    def _pick(*keys):
        for k in keys:
            v = row.get(k)
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            return v
        return None

    out["nome"] = _pick("nome")
    out["unidade"] = _pick("unidade")
    out["codigo_barras"] = _pick("codigo_barras")
    out["codigo_nfe"] = _pick("codigo_nfe", "codigo_gm")
    out["marca"] = _pick("marca")
    out["categoria"] = _pick("categoria")
    pv = _pick("preco_venda")
    if pv is not None:
        try:
            out["preco_venda"] = Decimal(str(pv))
        except Exception:
            out["preco_venda"] = pv
    pc = _pick("preco_custo", "preco_custo_final", "custo")
    if pc is not None:
        try:
            out["preco_custo"] = Decimal(str(pc))
        except Exception:
            out["preco_custo"] = pc
    return out


def enriquecer_snapshot_antes_com_catalogo(produto_id: str, snap: dict[str, Any]) -> dict[str, Any]:
    """
    Onde o overlay ainda está vazio, usa o valor efetivo do catálogo como «antes».
    Assim o lápis PDV só registra o que de fato mudou (ex. 28 → 28,01).
    """
    if not isinstance(snap, dict):
        return {}
    efetivo = snapshot_efetivo_catalogo(produto_id)
    if not efetivo:
        return dict(snap)
    out = dict(snap)
    for k, v in list(out.items()):
        if _norm_cmp(v) != "":
            continue
        ev = efetivo.get(k)
        if _norm_cmp(ev) != "":
            out[k] = ev
    return out


def snapshot_variacoes_resumo(rows) -> str:
    """Resumo texto das variações marca/código (cadastro, não saldo)."""
    if not rows:
        return ""
    partes = []
    for r in rows:
        if hasattr(r, "marca"):
            marca = getattr(r, "marca", "") or ""
            cb = getattr(r, "codigo_barras", "") or ""
            cf = getattr(r, "codigo_fornecedor", "") or ""
            ci = getattr(r, "codigo_interno", "") or ""
        elif isinstance(r, dict):
            marca = str(r.get("marca") or "")
            cb = str(r.get("codigo_barras") or "")
            cf = str(r.get("codigo_fornecedor") or "")
            ci = str(r.get("codigo_interno") or "")
        else:
            continue
        partes.append(f"{marca}|{cb}|{cf}|{ci}")
    return " · ".join(partes)[:500]


def inferir_origem_payload(payload: dict | None) -> str:
    p = payload or {}
    raw = str(p.get("origem_historico") or p.get("historico_origem") or "").strip().lower()
    if raw in {c.value for c in ProdutoCadastroAlteracaoAgro.Origem}:
        return raw
    if p.get("pdv_edicao_rapida") or p.get("origem_pdv"):
        return ProdutoCadastroAlteracaoAgro.Origem.PDV
    if p.get("validar_cadastro_minimo"):
        return ProdutoCadastroAlteracaoAgro.Origem.MODAL
    if p.get("entrada_nfe") or p.get("origem_entrada_nf"):
        return ProdutoCadastroAlteracaoAgro.Origem.NF
    return ProdutoCadastroAlteracaoAgro.Origem.GESTAO


def registrar_diffs_cadastro(
    *,
    produto_id: str,
    antes: dict[str, Any],
    depois: dict[str, Any],
    usuario=None,
    origem: str = "outro",
) -> int:
    """Grava uma linha por campo alterado. Retorna quantidade criada."""
    pid = str(produto_id or "").strip()[:64]
    if not pid or not isinstance(antes, dict) or not isinstance(depois, dict):
        return 0
    origem_ok = origem if origem in {c.value for c in ProdutoCadastroAlteracaoAgro.Origem} else "outro"
    rows = []
    keys = set(antes.keys()) | set(depois.keys())
    for campo in keys:
        if campo not in _LABELS:
            continue
        a = antes.get(campo)
        b = depois.get(campo)
        if _norm_cmp(a) == _norm_cmp(b):
            continue
        rows.append(
            ProdutoCadastroAlteracaoAgro(
                produto_externo_id=pid,
                campo=campo[:64],
                campo_label=(_LABELS.get(campo) or campo)[:80],
                valor_antes=_fmt(a),
                valor_depois=_fmt(b),
                usuario=usuario if getattr(usuario, "is_authenticated", False) else None,
                origem=origem_ok,
            )
        )
    if not rows:
        return 0
    ProdutoCadastroAlteracaoAgro.objects.bulk_create(rows)
    return len(rows)


def listar_alteracoes_cadastro(
    *,
    produto_id: str,
    limit: int = 40,
    offset: int = 0,
    completo: bool = False,
) -> dict[str, Any]:
    pid = str(produto_id or "").strip()[:64]
    if not pid:
        return {"ok": True, "linhas": [], "tem_mais": False, "total_estimado": 0}

    if completo:
        limit = min(max(int(limit or 500), 1), 500)
        offset = 0
        teto = 500
    else:
        limit = min(max(int(limit or 40), 1), 40)
        offset = max(0, min(int(offset or 0), 80))
        teto = 120
        if offset + limit > teto:
            limit = max(0, teto - offset)
        if limit <= 0:
            return {"ok": True, "linhas": [], "tem_mais": False, "total_estimado": teto}

    qs = (
        ProdutoCadastroAlteracaoAgro.objects.filter(produto_externo_id=pid)
        .select_related("usuario")
        .order_by("-criado_em", "-id")
    )
    total = qs.count()
    page = list(qs[offset : offset + limit])
    linhas = []
    for row in page:
        user = row.usuario
        if user is not None:
            quem = (user.get_full_name() or "").strip() or (user.username or "")
        else:
            quem = "—"
        quando = row.criado_em
        from django.utils import timezone

        if quando and timezone.is_aware(quando):
            quando = timezone.localtime(quando)
        linhas.append(
            {
                "id": row.pk,
                "quando": quando.strftime("%d/%m/%Y %H:%M") if quando else "",
                "campo": row.campo,
                "campo_label": row.campo_label or row.campo,
                "valor_antes": row.valor_antes or "—",
                "valor_depois": row.valor_depois or "—",
                "operador": quem[:120] or "—",
                "origem": row.origem,
                "origem_label": row.get_origem_display(),
            }
        )
    tem_mais = (offset + limit) < min(total, teto) if not completo else False
    return {
        "ok": True,
        "linhas": linhas,
        "tem_mais": tem_mais,
        "total_estimado": min(total, 500 if completo else teto),
        "offset": offset,
        "limit": limit,
        "completo": bool(completo),
    }
