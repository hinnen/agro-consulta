"""Exportação / importação Excel do cadastro de produtos (fase 1 — overlay Agro)."""

from __future__ import annotations

import re
import unicodedata
from decimal import Decimal, InvalidOperation
from io import BytesIO
from pathlib import Path
from typing import Any

from django.core.cache import cache
from django.db import transaction
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

from produtos.models import CadastroPlanilhaImportHistoricoAgro, ProdutoGestaoOverlayAgro

EXPORT_MAX_ROWS = 15000
IMPORT_MAX_ROWS = 2500

COL_ID = "id"
COL_CODIGO_GM = "codigo_gm"
COL_NOME = "nome"
COL_MARCA = "marca"
COL_MODELO = "modelo"
COL_CATEGORIA = "categoria"
COL_SUBCATEGORIA = "subcategoria"
COL_SUBCATEGORIA_2 = "subcategoria_2"
COL_SUBCATEGORIA_3 = "subcategoria_3"
COL_SUBCATEGORIA_4 = "subcategoria_4"
COL_FORNECEDOR = "fornecedor"
COL_UNIDADE = "unidade"
COL_PESO = "peso_etiqueta"
COL_DESCRICAO = "descricao"
COL_CODIGO_BARRAS = "codigo_barras"
COL_PRECO_CUSTO = "preco_custo"
COL_PRECO_VENDA = "preco_venda"
COL_CASHBACK = "cashback_percentual"
COL_NCM = "ncm"
COL_CEST = "cest"
COL_CFOP = "cfop"
COL_CSOSN = "csosn"
COL_ORIGEM = "origem"
COL_EST_MIN_C = "estoque_min_centro"
COL_EST_MAX_C = "estoque_max_centro"
COL_EST_MIN_V = "estoque_min_vila"
COL_EST_MAX_V = "estoque_max_vila"
COL_ATIVO = "ativo"

EXPORT_HEADERS: list[tuple[str, str]] = [
    ("ID", COL_ID),
    ("Código GM", COL_CODIGO_GM),
    ("Nome", COL_NOME),
    ("Marca", COL_MARCA),
    ("Modelo", COL_MODELO),
    ("Categoria", COL_CATEGORIA),
    ("Subcategoria", COL_SUBCATEGORIA),
    ("Subcategoria 2", COL_SUBCATEGORIA_2),
    ("Subcategoria 3", COL_SUBCATEGORIA_3),
    ("Subcategoria 4", COL_SUBCATEGORIA_4),
    ("Fornecedor", COL_FORNECEDOR),
    ("Unidade", COL_UNIDADE),
    ("Peso", COL_PESO),
    ("Descrição", COL_DESCRICAO),
    ("Código barras", COL_CODIGO_BARRAS),
    ("Preço custo", COL_PRECO_CUSTO),
    ("Preço venda", COL_PRECO_VENDA),
    ("Cashback %", COL_CASHBACK),
    ("NCM", COL_NCM),
    ("CEST", COL_CEST),
    ("CFOP", COL_CFOP),
    ("CSOSN", COL_CSOSN),
    ("Origem", COL_ORIGEM),
    ("Estoque mín. Centro", COL_EST_MIN_C),
    ("Estoque máx. Centro", COL_EST_MAX_C),
    ("Estoque mín. Vila", COL_EST_MIN_V),
    ("Estoque máx. Vila", COL_EST_MAX_V),
    ("Ativo", COL_ATIVO),
]

# Decimais na planilha (comparação / Excel).
_COLS_DECIMAL = frozenset(
    {
        COL_PRECO_CUSTO,
        COL_PRECO_VENDA,
        COL_CASHBACK,
        COL_EST_MIN_C,
        COL_EST_MAX_C,
        COL_EST_MIN_V,
        COL_EST_MAX_V,
    }
)
_COLS_ESTOQUE = frozenset({COL_EST_MIN_C, COL_EST_MAX_C, COL_EST_MIN_V, COL_EST_MAX_V})
_COLS_FISCAL = frozenset({COL_NCM, COL_CEST, COL_CFOP, COL_CSOSN, COL_ORIGEM})
_COLS_EXTRAS = frozenset({COL_MODELO, COL_PRECO_CUSTO}) | _COLS_FISCAL

# Campos de lista (marca/cat/sub…) — aviso + typo + dropdown no Excel.
_COLS_FACETA = (
    COL_MARCA,
    COL_CATEGORIA,
    COL_SUBCATEGORIA,
    COL_SUBCATEGORIA_2,
    COL_SUBCATEGORIA_3,
    COL_SUBCATEGORIA_4,
    COL_FORNECEDOR,
    COL_UNIDADE,
)
_FACETA_ROTULO = {
    COL_MARCA: "Marca",
    COL_CATEGORIA: "Categoria",
    COL_SUBCATEGORIA: "Subcategoria",
    COL_SUBCATEGORIA_2: "Subcategoria 2",
    COL_SUBCATEGORIA_3: "Subcategoria 3",
    COL_SUBCATEGORIA_4: "Subcategoria 4",
    COL_FORNECEDOR: "Fornecedor",
    COL_UNIDADE: "Unidade",
}
# Similaridade mínima (0–1) para sugerir/corrigir typo automaticamente.
_FACETA_FUZZY_MIN = 0.86
_FACETA_FUZZY_MAX_LEN_DIFF = 3

def _norm_faceta_chave(s: str) -> str:
    t = unicodedata.normalize("NFD", str(s or ""))
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", t).strip().lower()


def carregar_facetas_planilha() -> dict[str, list[str]]:
    """Listas conhecidas p/ dropdown Excel + validação de import."""
    from produtos.agro_fonte_config import agro_catalogo_usa_postgres

    marcas: list[str] = []
    categorias: list[str] = []
    subcategorias: list[str] = []
    fornecedores: list[str] = []
    unidades: list[str] = []

    try:
        if agro_catalogo_usa_postgres():
            from produtos import catalogo_agro

            fac = catalogo_agro.facetas_gestao(limite=2000)
            marcas = list(fac.get("marcas") or [])
            categorias = list(fac.get("categorias") or [])
            subcategorias = list(fac.get("subcategorias") or [])
            fornecedores = list(fac.get("fornecedores") or [])
            unidades = list(fac.get("unidades") or [])
            qs = catalogo_agro.queryset_catalogo_ativos(inativos=True)
            for fld in ("subcategoria_2", "subcategoria_3", "subcategoria_4"):
                subcategorias.extend(
                    str(x).strip()
                    for x in qs.exclude(**{fld: ""}).values_list(fld, flat=True).distinct()[:800]
                    if str(x or "").strip()
                )
    except Exception:
        pass

    ov_qs = ProdutoGestaoOverlayAgro.objects.all()
    marcas.extend(
        str(x).strip()
        for x in ov_qs.exclude(marca="").values_list("marca", flat=True).distinct()[:2000]
        if str(x or "").strip()
    )
    categorias.extend(
        str(x).strip()
        for x in ov_qs.exclude(categoria="").values_list("categoria", flat=True).distinct()[:1200]
        if str(x or "").strip()
    )
    fornecedores.extend(
        str(x).strip()
        for x in ov_qs.exclude(fornecedor_texto="")
        .values_list("fornecedor_texto", flat=True)
        .distinct()[:1200]
        if str(x or "").strip()
    )
    unidades.extend(
        str(x).strip()
        for x in ov_qs.exclude(unidade="").values_list("unidade", flat=True).distinct()[:400]
        if str(x or "").strip()
    )
    for fld in ("subcategoria", "subcategoria_2", "subcategoria_3", "subcategoria_4"):
        subcategorias.extend(
            str(x).strip()
            for x in ov_qs.exclude(**{fld: ""}).values_list(fld, flat=True).distinct()[:800]
            if str(x or "").strip()
        )

    def _uniq(vals: list[str], lim: int = 1500) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for v in vals:
            s = str(v or "").strip()
            if not s:
                continue
            k = _norm_faceta_chave(s)
            if k in seen:
                continue
            seen.add(k)
            out.append(s)
            if len(out) >= lim:
                break
        out.sort(key=lambda x: x.lower())
        return out

    return {
        COL_MARCA: _uniq(marcas, 2500),
        COL_CATEGORIA: _uniq(categorias, 1200),
        COL_SUBCATEGORIA: _uniq(subcategorias, 1500),
        COL_SUBCATEGORIA_2: _uniq(subcategorias, 1500),
        COL_SUBCATEGORIA_3: _uniq(subcategorias, 1500),
        COL_SUBCATEGORIA_4: _uniq(subcategorias, 1500),
        COL_FORNECEDOR: _uniq(fornecedores, 1200),
        COL_UNIDADE: _uniq(unidades, 400),
    }


def _mapa_canonico_faceta(lista: list[str]) -> dict[str, str]:
    return {_norm_faceta_chave(v): v for v in lista if str(v or "").strip()}


def _sugerir_faceta(valor: str, conhecidos: list[str]) -> tuple[str | None, float]:
    """Retorna (sugestão, score) ou (None, 0)."""
    from difflib import SequenceMatcher

    alvo = _norm_faceta_chave(valor)
    if not alvo or not conhecidos:
        return None, 0.0
    melhor = None
    melhor_score = 0.0
    alvo_len = len(alvo)
    # Amostra grande o bastante sem varrer 2k em todo typo raro.
    for cand in conhecidos:
        ck = _norm_faceta_chave(cand)
        if not ck:
            continue
        if abs(len(ck) - alvo_len) > _FACETA_FUZZY_MAX_LEN_DIFF:
            continue
        sc = SequenceMatcher(None, alvo, ck).ratio()
        if sc > melhor_score:
            melhor_score = sc
            melhor = cand
            if sc >= 0.99:
                break
    if melhor and melhor_score >= _FACETA_FUZZY_MIN:
        return melhor, float(melhor_score)
    return None, float(melhor_score)


def _classificar_valor_faceta(
    campo: str,
    valor: str,
    facetas: dict[str, list[str]],
    canonicos: dict[str, dict[str, str]],
) -> dict[str, Any]:
    raw = str(valor or "").strip()
    rotulo = _FACETA_ROTULO.get(campo, campo)
    if not raw:
        return {"campo": campo, "rotulo": rotulo, "valor": "", "status": "vazio"}
    canon_map = canonicos.get(campo) or {}
    chave = _norm_faceta_chave(raw)
    if chave in canon_map:
        return {
            "campo": campo,
            "rotulo": rotulo,
            "valor": raw,
            "valor_final": canon_map[chave],
            "status": "ok",
            "score": 1.0,
        }
    sug, score = _sugerir_faceta(raw, facetas.get(campo) or [])
    if sug:
        return {
            "campo": campo,
            "rotulo": rotulo,
            "valor": raw,
            "valor_final": sug,
            "sugestao": sug,
            "status": "corrigir",
            "score": round(score, 3),
        }
    return {
        "campo": campo,
        "rotulo": rotulo,
        "valor": raw,
        "valor_final": raw,
        "status": "novo",
        "score": round(score, 3) if score else 0.0,
    }


def _resolver_facetas_no_patch(
    patch: dict[str, Any],
    facetas: dict[str, list[str]],
    canonicos: dict[str, dict[str, str]],
    *,
    permitir_novos: bool,
    linha: int | None = None,
) -> tuple[dict[str, Any], list[dict], list[str]]:
    """Ajusta patch (canon/typo). Retorna (patch, eventos, erros)."""
    out = dict(patch)
    eventos: list[dict] = []
    erros: list[str] = []
    for campo in _COLS_FACETA:
        if campo not in out:
            continue
        info = _classificar_valor_faceta(campo, str(out[campo] or ""), facetas, canonicos)
        info["linha"] = linha
        st = info.get("status")
        if st == "ok":
            out[campo] = info["valor_final"]
            if info["valor_final"] != str(patch[campo] or "").strip():
                eventos.append({**info, "acao": "canonico"})
        elif st == "corrigir":
            out[campo] = info["valor_final"]
            eventos.append({**info, "acao": "corrigir"})
        elif st == "novo":
            eventos.append({**info, "acao": "novo"})
            if not permitir_novos:
                erros.append(
                    f"{info['rotulo']} «{info['valor']}» não cadastrado"
                    + (f" (parecido: {info.get('sugestao')})" if info.get("sugestao") else "")
                    + ". Marque «permitir criar novos» ou use um nome da lista."
                )
    return out, eventos, erros


def _resumir_eventos_faceta(eventos: list[dict]) -> tuple[list[dict], list[dict]]:
    """Agrupa novos e correções por campo+valor."""
    novos_map: dict[tuple[str, str], dict] = {}
    corr_map: dict[tuple[str, str], dict] = {}
    for ev in eventos:
        campo = str(ev.get("campo") or "")
        valor = str(ev.get("valor") or "")
        key = (campo, _norm_faceta_chave(valor))
        linha = ev.get("linha")
        if ev.get("status") == "novo" or ev.get("acao") == "novo":
            slot = novos_map.setdefault(
                key,
                {
                    "campo": campo,
                    "rotulo": ev.get("rotulo") or campo,
                    "valor": valor,
                    "linhas": [],
                    "score": ev.get("score"),
                },
            )
            if linha and linha not in slot["linhas"]:
                slot["linhas"].append(linha)
        elif ev.get("status") == "corrigir" or ev.get("acao") == "corrigir":
            slot = corr_map.setdefault(
                key,
                {
                    "campo": campo,
                    "rotulo": ev.get("rotulo") or campo,
                    "valor": valor,
                    "sugestao": ev.get("sugestao") or ev.get("valor_final"),
                    "score": ev.get("score"),
                    "linhas": [],
                },
            )
            if linha and linha not in slot["linhas"]:
                slot["linhas"].append(linha)
    return list(novos_map.values()), list(corr_map.values())


# Colunas travadas no Excel (só ID — coluna oculta na planilha).
EXPORT_COLS_BLOQUEADAS = frozenset({COL_ID})
EXPORT_COLS_OCULTAS = frozenset({COL_ID})

EXPORT_COL_KEYS = [key for _, key in EXPORT_HEADERS]

def normalizar_colunas_export(raw: str | None) -> list[str]:
    """Colunas pedidas na exportação — ID sempre incluído."""
    if not raw or not str(raw).strip():
        return list(EXPORT_COL_KEYS)
    pedidas = [x.strip() for x in str(raw).split(",") if x.strip()]
    out: list[str] = []
    for k in pedidas:
        if k in EXPORT_COL_KEYS and k not in out:
            out.append(k)
    if COL_ID not in out:
        out.insert(0, COL_ID)
    return out or list(EXPORT_COL_KEYS)


def normalizar_categorias_export(raw: str | None) -> list[str]:
    if not raw or not str(raw).strip():
        return []
    return list(dict.fromkeys(x.strip() for x in str(raw).split("|") if x.strip()))[:80]


def _filtrar_rows_categorias(rows: list[dict], categorias: list[str]) -> list[dict]:
    if not categorias:
        return rows
    alvo = {c.strip().lower() for c in categorias if c.strip()}
    if not alvo:
        return rows
    return [r for r in rows if str(r.get("categoria") or "").strip().lower() in alvo]


def headers_export(colunas: list[str] | None = None) -> list[tuple[str, str]]:
    cols = colunas or list(EXPORT_COL_KEYS)
    return [(label, key) for label, key in EXPORT_HEADERS if key in cols]


IMPORT_KEYS = {
    COL_CODIGO_GM,
    COL_NOME,
    COL_MARCA,
    COL_MODELO,
    COL_CATEGORIA,
    COL_SUBCATEGORIA,
    COL_SUBCATEGORIA_2,
    COL_SUBCATEGORIA_3,
    COL_SUBCATEGORIA_4,
    COL_FORNECEDOR,
    COL_UNIDADE,
    COL_PESO,
    COL_DESCRICAO,
    COL_CODIGO_BARRAS,
    COL_PRECO_CUSTO,
    COL_PRECO_VENDA,
    COL_CASHBACK,
    COL_NCM,
    COL_CEST,
    COL_CFOP,
    COL_CSOSN,
    COL_ORIGEM,
    COL_EST_MIN_C,
    COL_EST_MAX_C,
    COL_EST_MIN_V,
    COL_EST_MAX_V,
    COL_ATIVO,
}

# Campos gravados direto no modelo overlay (não extras JSON).
OVERLAY_IMPORT_KEYS = (
    COL_CODIGO_GM,
    COL_NOME,
    COL_MARCA,
    COL_CATEGORIA,
    COL_SUBCATEGORIA,
    COL_SUBCATEGORIA_2,
    COL_SUBCATEGORIA_3,
    COL_SUBCATEGORIA_4,
    COL_FORNECEDOR,
    COL_UNIDADE,
    COL_PESO,
    COL_DESCRICAO,
    COL_CODIGO_BARRAS,
    COL_PRECO_VENDA,
    COL_CASHBACK,
    COL_EST_MIN_C,
    COL_EST_MAX_C,
    COL_EST_MIN_V,
    COL_EST_MAX_V,
    COL_ATIVO,
)

HISTORICO_IMPORT_LISTA_LIMITE = 30


def _overlay_model_field(key: str) -> str:
    if key == COL_CODIGO_GM:
        return "codigo_nfe"
    if key == COL_FORNECEDOR:
        return "fornecedor_texto"
    if key == COL_ATIVO:
        return "ativo_exibicao"
    return key


def _fmt_ativo_planilha(val) -> str:
    if val is True or str(val).strip().lower() in ("1", "true", "sim", "s", "ativo"):
        return "Sim"
    if val is False or str(val).strip().lower() in ("0", "false", "nao", "não", "n", "inativo"):
        return "Não"
    return ""


def _parse_ativo_planilha(val) -> bool | None:
    if val is None:
        return None
    s = str(val).strip().lower()
    if not s:
        return None
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    if s in ("1", "true", "sim", "s", "ativo", "yes", "on"):
        return True
    if s in ("0", "false", "nao", "n", "inativo", "no", "off"):
        return False
    return None


def _valor_atual_campo_import(atual: dict, key: str):
    if key in _COLS_DECIMAL:
        return atual.get(key)
    if key == COL_CODIGO_GM:
        return str(atual.get("codigo_gm") or atual.get("codigo_nfe") or "").strip()
    if key == COL_ATIVO:
        raw = atual.get(COL_ATIVO)
        if raw is None and "ativo_exibicao" in atual:
            raw = atual.get("ativo_exibicao")
        if raw is None and "inativo" in atual:
            raw = not bool(atual.get("inativo"))
        return _fmt_ativo_planilha(raw) if raw is not None and raw != "" else ""
    return str(atual.get(key) or "").strip()


def _norm_header(h: str) -> str:
    s = unicodedata.normalize("NFD", str(h or ""))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _map_headers(headers: list[str]) -> dict[str, str | None]:
    norm = {_norm_header(h): h for h in headers if str(h or "").strip()}
    aliases: dict[str, tuple[str, ...]] = {
        COL_ID: ("id", "produto id", "produto_id", "codigo produto"),
        COL_CODIGO_GM: ("codigo gm", "codigo_gm", "codigo nfe", "codigo erp", "gm"),
        COL_NOME: ("nome", "produto", "descricao produto"),
        COL_MARCA: ("marca",),
        COL_MODELO: ("modelo",),
        COL_CATEGORIA: ("categoria", "grupo"),
        COL_SUBCATEGORIA: ("subcategoria", "sub categoria", "subgrupo"),
        COL_SUBCATEGORIA_2: (
            "subcategoria 2",
            "subcategoria2",
            "sub categoria 2",
            "subgrupo 2",
        ),
        COL_SUBCATEGORIA_3: (
            "subcategoria 3",
            "subcategoria3",
            "sub categoria 3",
            "subgrupo 3",
        ),
        COL_SUBCATEGORIA_4: (
            "subcategoria 4",
            "subcategoria4",
            "sub categoria 4",
            "subgrupo 4",
        ),
        COL_FORNECEDOR: ("fornecedor", "fornecedor texto", "fabricante"),
        COL_UNIDADE: ("unidade", "un", "sigla unidade"),
        COL_PESO: ("peso", "peso etiqueta", "peso gondola"),
        COL_DESCRICAO: ("descricao", "descrição", "obs", "observacao", "observação"),
        COL_CODIGO_BARRAS: ("codigo barras", "codigo de barras", "ean", "barras", "cb"),
        COL_PRECO_CUSTO: ("preco custo", "preço custo", "custo", "custo unitario", "custo unitário"),
        COL_PRECO_VENDA: ("preco venda", "preço venda", "venda", "preco de venda", "preço de venda"),
        COL_CASHBACK: ("cashback", "cashback %", "cashback percentual", "% cashback"),
        COL_NCM: ("ncm",),
        COL_CEST: ("cest",),
        COL_CFOP: ("cfop",),
        COL_CSOSN: ("csosn",),
        COL_ORIGEM: ("origem", "origem mercadoria"),
        COL_EST_MIN_C: ("estoque min centro", "estoque minimo centro", "est min centro", "min centro"),
        COL_EST_MAX_C: ("estoque max centro", "estoque maximo centro", "est max centro", "max centro"),
        COL_EST_MIN_V: ("estoque min vila", "estoque minimo vila", "est min vila", "min vila"),
        COL_EST_MAX_V: ("estoque max vila", "estoque maximo vila", "est max vila", "max vila"),
        COL_ATIVO: ("ativo", "ativo na listagem", "ativo listagem", "status ativo"),
    }
    out: dict[str, str | None] = {}
    for key, keys in aliases.items():
        out[key] = None
        for k in keys:
            if k in norm:
                out[key] = norm[k]
                break
    return out


def _enriquecer_row_planilha(row: dict, ov: ProdutoGestaoOverlayAgro | None) -> None:
    """Completa campos usados só na planilha (estoque, fiscal, modelo, ativo)."""
    if ov:
        for fld in (
            COL_EST_MIN_C,
            COL_EST_MAX_C,
            COL_EST_MIN_V,
            COL_EST_MAX_V,
        ):
            val = getattr(ov, fld, None)
            if val is not None:
                try:
                    row[fld] = float(val)
                except (TypeError, ValueError):
                    pass
        if ov.cashback_percentual is not None:
            try:
                row[COL_CASHBACK] = float(ov.cashback_percentual)
            except (TypeError, ValueError):
                pass
        ex = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
        if ex.get("modelo") is not None and str(ex.get("modelo") or "").strip():
            row[COL_MODELO] = str(ex.get("modelo") or "").strip()[:200]
        fiscal = ex.get("fiscal") if isinstance(ex.get("fiscal"), dict) else {}
        for fk in (COL_NCM, COL_CEST, COL_CFOP, COL_CSOSN, COL_ORIGEM):
            if fiscal.get(fk) is not None and str(fiscal.get(fk) or "").strip():
                row[fk] = str(fiscal.get(fk) or "").strip()
        if ov.ativo_exibicao is not None:
            row[COL_ATIVO] = _fmt_ativo_planilha(ov.ativo_exibicao)
            row["ativo_exibicao"] = ov.ativo_exibicao
    if COL_ATIVO not in row or row.get(COL_ATIVO) in (None, ""):
        if row.get("inativo") is True:
            row[COL_ATIVO] = "Não"
        elif row.get("inativo") is False or row.get("ativo") is True:
            row[COL_ATIVO] = "Sim"
        elif "ativo_exibicao" in row and row.get("ativo_exibicao") is not None:
            row[COL_ATIVO] = _fmt_ativo_planilha(row.get("ativo_exibicao"))
    if not row.get(COL_MODELO) and row.get("modelo"):
        row[COL_MODELO] = str(row.get("modelo") or "").strip()[:200]
    if not row.get(COL_NCM) and row.get("ncm"):
        row[COL_NCM] = str(row.get("ncm") or "").strip()
    if not row.get(COL_FORNECEDOR) and row.get("fornecedor"):
        row[COL_FORNECEDOR] = str(row.get("fornecedor") or "").strip()
    if ov and str(getattr(ov, "peso_etiqueta", "") or "").strip():
        row[COL_PESO] = str(ov.peso_etiqueta).strip()[:40]
    if not row.get(COL_PESO) and row.get("peso_etiqueta"):
        row[COL_PESO] = str(row.get("peso_etiqueta") or "").strip()[:40]


def _cel_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        if val == int(val) or abs(val - round(val)) < 1e-9:
            return str(int(round(val)))
        s = str(val).strip()
        if "e" in s.lower():
            try:
                return str(int(Decimal(s)))
            except Exception:
                pass
        return s
    s = str(val).strip()
    if s.endswith(".0") and s[:-2].replace("-", "").isdigit():
        return s[:-2]
    return s


def _cel_opt_str(val) -> str | None:
    if val is None:
        return None
    s = _cel_str(val)
    return s if s else None


def _id_produto_planilha_valido(pid: str) -> bool:
    """ID exportado: ObjectId Mongo, Id numérico ERP ou id Postgres Agro (AGRO…)."""
    s = str(pid or "").strip()
    if not s or len(s) > 64:
        return False
    low = s.lower()
    if re.fullmatch(r"[0-9a-f]{24}", low):
        return True
    if s.isdigit() and 1 <= len(s) <= 12:
        return True
    if re.fullmatch(r"AGRO[0-9A-Fa-f]{8,48}", s):
        return True
    if low.startswith("local:") and low[6:].isdigit():
        return True
    return False


def _id_planilha_resumo(pid: str, max_len: int = 28) -> str:
    s = str(pid or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _parse_decimal_br(val) -> Decimal | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return Decimal(str(val))
    s = str(val).strip()
    if not s:
        return None
    s = s.replace("R$", "").replace(" ", "").strip()
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _ler_planilha(path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    suf = path.suffix.lower()
    if suf == ".csv":
        import csv

        for enc in ("utf-8-sig", "latin-1", "cp1252"):
            try:
                with path.open("r", encoding=enc, newline="") as f:
                    reader = csv.DictReader(f, delimiter=";")
                    if reader.fieldnames and len(reader.fieldnames) == 1:
                        f.seek(0)
                        reader = csv.DictReader(f, delimiter=",")
                    headers = list(reader.fieldnames or [])
                    rows = [dict(r) for r in reader]
                    return headers, rows
            except UnicodeDecodeError:
                continue
        raise ValueError("Não foi possível ler o CSV (encoding).")
    if suf in (".xlsx", ".xls"):
        from openpyxl import load_workbook

        wb = load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        it = ws.iter_rows(values_only=True)
        headers = [str(c or "").strip() for c in next(it, [])]
        rows = []
        for row in it:
            if not any(row):
                continue
            d: dict[str, Any] = {}
            for i, h in enumerate(headers):
                if h:
                    d[h] = row[i] if i < len(row) else None
            rows.append(d)
        wb.close()
        return headers, rows
    raise ValueError("Use arquivo .csv ou .xlsx.")


def coletar_linhas_export_cadastro(
    *,
    inativos: bool = False,
    categorias: list[str] | None = None,
) -> tuple[list[dict], bool]:
    """Retorna linhas mescladas (Mongo/Agro + overlay) para exportação."""
    from produtos.agro_fonte_config import agro_catalogo_usa_postgres
    from produtos.views import (
        _CADASTRO_LISTA_MONGO_PROJ,
        _aplicar_produto_gestao_overlay_em_dict,
        _overlay_mapa_por_ids_chunked,
        _produto_mongo_para_cadastro_row,
        obter_conexao_mongo,
    )

    rows: list[dict] = []
    truncado = False

    if agro_catalogo_usa_postgres():
        from produtos import catalogo_agro

        pagina = 1
        por_pagina = 500
        while len(rows) < EXPORT_MAX_ROWS:
            chunk, has_more = catalogo_agro.listar_paginado(
                pagina=pagina,
                por_pagina=por_pagina,
                sort_key="nome",
                sort_direction=1,
                inativos=inativos,
            )
            if not chunk:
                break
            pids = [str(r.get("id") or "") for r in chunk]
            ovs = _overlay_mapa_por_ids_chunked(pids)
            for r in chunk:
                pid = str(r.get("id") or "")
                ov = ovs.get(pid)
                _aplicar_produto_gestao_overlay_em_dict(r, ov)
                _enriquecer_row_planilha(r, ov)
                rows.append(r)
            if not has_more:
                break
            pagina += 1
        truncado = len(rows) >= EXPORT_MAX_ROWS
        rows = _filtrar_rows_categorias(rows[:EXPORT_MAX_ROWS], categorias or [])
        return rows, truncado

    client, db = obter_conexao_mongo()
    if db is None:
        raise ValueError("Mongo indisponível — não foi possível exportar o catálogo.")

    filtro = {} if inativos else {"CadastroInativo": {"$ne": True}}
    cur = (
        db[client.col_p]
        .find(filtro, _CADASTRO_LISTA_MONGO_PROJ)
        .sort("Nome", 1)
        .limit(EXPORT_MAX_ROWS + 1)
    )
    chunk = list(cur)
    truncado = len(chunk) > EXPORT_MAX_ROWS
    chunk = chunk[:EXPORT_MAX_ROWS]
    rows = [_produto_mongo_para_cadastro_row(p) for p in chunk]
    ovs = _overlay_mapa_por_ids_chunked([str(r.get("id") or "") for r in rows])
    for r in rows:
        pid = str(r.get("id") or "")
        ov = ovs.get(pid)
        _aplicar_produto_gestao_overlay_em_dict(r, ov)
        _enriquecer_row_planilha(r, ov)
    rows = _filtrar_rows_categorias(rows, categorias or [])
    return rows, truncado


def linha_export_planilha(row: dict) -> dict[str, Any]:
    def _dec_or_empty(key: str):
        v = row.get(key)
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    ativo_out = ""
    if row.get(COL_ATIVO) not in (None, ""):
        ativo_out = _fmt_ativo_planilha(row.get(COL_ATIVO))
    elif row.get("inativo") is True:
        ativo_out = "Não"
    elif row.get("inativo") is False:
        ativo_out = "Sim"

    return {
        COL_ID: str(row.get("id") or ""),
        COL_CODIGO_GM: str(row.get("codigo_nfe") or row.get("codigo") or ""),
        COL_NOME: str(row.get("nome") or ""),
        COL_MARCA: str(row.get("marca") or ""),
        COL_MODELO: str(row.get("modelo") or ""),
        COL_CATEGORIA: str(row.get("categoria") or ""),
        COL_SUBCATEGORIA: str(row.get("subcategoria") or ""),
        COL_SUBCATEGORIA_2: str(row.get("subcategoria_2") or ""),
        COL_SUBCATEGORIA_3: str(row.get("subcategoria_3") or ""),
        COL_SUBCATEGORIA_4: str(row.get("subcategoria_4") or ""),
        COL_FORNECEDOR: str(row.get("fornecedor") or ""),
        COL_UNIDADE: str(row.get("unidade") or ""),
        COL_PESO: str(row.get("peso_etiqueta") or ""),
        COL_DESCRICAO: str(row.get("descricao") or ""),
        COL_CODIGO_BARRAS: str(row.get("codigo_barras") or ""),
        COL_PRECO_CUSTO: float(row.get("preco_custo") or 0),
        COL_PRECO_VENDA: float(row.get("preco_venda") or 0),
        COL_CASHBACK: _dec_or_empty(COL_CASHBACK),
        COL_NCM: str(row.get("ncm") or ""),
        COL_CEST: str(row.get("cest") or ""),
        COL_CFOP: str(row.get("cfop") or ""),
        COL_CSOSN: str(row.get("csosn") or ""),
        COL_ORIGEM: str(row.get("origem") or ""),
        COL_EST_MIN_C: _dec_or_empty(COL_EST_MIN_C),
        COL_EST_MAX_C: _dec_or_empty(COL_EST_MAX_C),
        COL_EST_MIN_V: _dec_or_empty(COL_EST_MIN_V),
        COL_EST_MAX_V: _dec_or_empty(COL_EST_MAX_V),
        COL_ATIVO: ativo_out,
    }


def montar_xlsx_cadastro(rows: list[dict], colunas: list[str] | None = None) -> bytes:
    from openpyxl.styles import Protection
    from openpyxl.worksheet.datavalidation import DataValidation

    hdrs = headers_export(colunas)
    wb = Workbook()
    ws = wb.active
    ws.title = "Cadastro"
    hdr_fill = PatternFill("solid", fgColor="DCFCE7")
    hdr_font = Font(bold=True, color="14532D")
    lock_fill = PatternFill("solid", fgColor="F1F5F9")
    for col, (label, key) in enumerate(hdrs, start=1):
        c = ws.cell(row=1, column=col, value=label)
        c.font = hdr_font
        c.fill = hdr_fill
        c.protection = Protection(locked=key in EXPORT_COLS_BLOQUEADAS)
    for ri, src in enumerate(rows, start=2):
        line = linha_export_planilha(src)
        for col, (_, key) in enumerate(hdrs, start=1):
            val = line.get(key)
            cell = ws.cell(row=ri, column=col)
            if key in (COL_ID, COL_CODIGO_GM, COL_CODIGO_BARRAS, COL_PESO, COL_NCM, COL_CEST, COL_CFOP, COL_CSOSN, COL_ORIGEM):
                cell.value = str(val) if val is not None else ""
                cell.number_format = "@"
            else:
                cell.value = val
                if key in (COL_PRECO_CUSTO, COL_PRECO_VENDA, COL_CASHBACK):
                    cell.number_format = "#,##0.00"
                elif key in _COLS_ESTOQUE:
                    cell.number_format = "#,##0.000"
            bloqueada = key in EXPORT_COLS_BLOQUEADAS
            cell.protection = Protection(locked=bloqueada)
            if bloqueada:
                cell.fill = lock_fill
    for col in range(1, len(hdrs) + 1):
        letter = get_column_letter(col)
        ws.column_dimensions[letter].width = 16
        key = hdrs[col - 1][1]
        if key in EXPORT_COLS_OCULTAS:
            ws.column_dimensions[letter].hidden = True
            ws.column_dimensions[letter].width = 2
    nome_col = next(
        (get_column_letter(i + 1) for i, (_, key) in enumerate(hdrs) if key == COL_NOME),
        None,
    )
    if nome_col:
        ws.column_dimensions[nome_col].width = 42

    # Aba Listas + dropdown nas colunas de faceta (marca/cat/sub…).
    try:
        facetas = carregar_facetas_planilha()
    except Exception:
        facetas = {}
    if facetas:
        ws_list = wb.create_sheet("Listas")
        lista_cols = [
            (COL_MARCA, "Marcas"),
            (COL_CATEGORIA, "Categorias"),
            (COL_SUBCATEGORIA, "Subcategorias"),
            (COL_FORNECEDOR, "Fornecedores"),
            (COL_UNIDADE, "Unidades"),
        ]
        # Sub 2/3/4 compartilham a lista de Subcategorias (col C).
        faceta_para_lista_col = {
            COL_MARCA: 1,
            COL_CATEGORIA: 2,
            COL_SUBCATEGORIA: 3,
            COL_SUBCATEGORIA_2: 3,
            COL_SUBCATEGORIA_3: 3,
            COL_SUBCATEGORIA_4: 3,
            COL_FORNECEDOR: 4,
            COL_UNIDADE: 5,
        }
        for li, (fkey, titulo) in enumerate(lista_cols, start=1):
            ws_list.cell(row=1, column=li, value=titulo).font = Font(bold=True)
            vals = facetas.get(fkey) or []
            for ri, v in enumerate(vals, start=2):
                ws_list.cell(row=ri, column=li, value=v)
            ws_list.column_dimensions[get_column_letter(li)].width = 22
        ws_list.sheet_state = "hidden"
        n_rows = max(len(rows) + 50, 200)
        for col_idx, key in enumerate((k for _, k in hdrs), start=1):
            if key not in faceta_para_lista_col:
                continue
            lc = faceta_para_lista_col[key]
            letter_list = get_column_letter(lc)
            # Conta itens da lista (sem contar título).
            n_itens = max(1, len(facetas.get(key) or facetas.get(COL_SUBCATEGORIA) or []) )
            formula = f"Listas!${letter_list}$2:${letter_list}${n_itens + 1}"
            dv = DataValidation(
                type="list",
                formula1=formula,
                allow_blank=True,
                showDropDown=False,
                showErrorMessage=False,
                showInputMessage=True,
                promptTitle=_FACETA_ROTULO.get(key, key),
                prompt="Escolha da lista (ainda dá para digitar outro nome).",
            )
            letter = get_column_letter(col_idx)
            dv.add(f"{letter}2:{letter}{n_rows}")
            ws.add_data_validation(dv)

    ws.protection.sheet = True
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _estado_atual_produto(pid: str) -> dict | None:
    return _mapa_estado_atual_produtos([pid]).get(str(pid or "").strip()[:64])


def _mapa_estado_atual_produtos(
    pids: list[str],
    on_progress: None | Any = None,
) -> dict[str, dict]:
    """Carrega estado atual (Mongo + overlay) em lote — evita N consultas na prévia."""
    from bson import ObjectId

    from produtos.views import (
        _CADASTRO_LISTA_MONGO_PROJ,
        _aplicar_produto_gestao_overlay_em_dict,
        _mongo_filtro_id_produto_externo,
        _overlay_mapa_por_ids_chunked,
        _produto_mongo_para_cadastro_row,
        _produto_mongo_por_id_externo,
        obter_conexao_mongo,
    )

    uniq = list(dict.fromkeys(str(p or "").strip()[:64] for p in pids if str(p or "").strip()))
    if not uniq:
        return {}

    from produtos.agro_fonte_config import agro_catalogo_usa_postgres

    if agro_catalogo_usa_postgres():
        from produtos import catalogo_agro
        from produtos.models import Produto

        if on_progress:
            on_progress(10, "Catálogo Postgres…")
        ovs = _overlay_mapa_por_ids_chunked(uniq)
        prod_map = {
            str(p.produto_externo_id or "")[:64]: p
            for p in Produto.objects.filter(produto_externo_id__in=uniq)
        }
        out: dict[str, dict] = {}
        for pid in uniq:
            p = prod_map.get(pid) or catalogo_agro.obter_produto_model(pid)
            if p is None:
                continue
            pid_key = str(p.produto_externo_id or pid)[:64]
            ov = ovs.get(pid_key) or ovs.get(pid)
            row = catalogo_agro.produto_agro_para_row(p, ov)
            _enriquecer_row_planilha(row, ov)
            out[pid] = row
        return out

    client, db = obter_conexao_mongo()
    if db is None:
        return {}

    doc_map: dict[str, dict] = {}
    chunk_sz = 250
    n_chunks = max(1, (len(uniq) + chunk_sz - 1) // chunk_sz)

    def _registrar_doc(doc: dict, chunk_set: set[str]) -> None:
        id_val = doc.get("Id")
        if id_val is not None:
            sid = str(id_val).strip()[:64]
            if sid in chunk_set:
                doc_map[sid] = doc
                return
        sid = str(doc.get("_id") or "").strip()[:64]
        if sid in chunk_set:
            doc_map[sid] = doc

    for ci, start in enumerate(range(0, len(uniq), chunk_sz)):
        if on_progress:
            on_progress(8 + int(2 * ci / n_chunks), f"Catálogo Mongo… lote {ci + 1}/{n_chunks}")

        chunk_pids = uniq[start : start + chunk_sz]
        chunk_set = set(chunk_pids)
        ors: list[dict] = [{"Id": {"$in": chunk_pids}}]
        int_ids: list[int] = []
        for p in chunk_pids:
            try:
                int_ids.append(int(p))
            except (TypeError, ValueError):
                pass
        if int_ids:
            ors.append({"Id": {"$in": int_ids}})
        oids: list[ObjectId] = []
        for p in chunk_pids:
            try:
                oids.append(ObjectId(p))
            except Exception:
                pass
        if oids:
            ors.append({"_id": {"$in": oids}})

        try:
            for doc in db[client.col_p].find({"$or": ors}, _CADASTRO_LISTA_MONGO_PROJ):
                _registrar_doc(doc, chunk_set)
        except Exception:
            pass

    missing = [p for p in uniq if p not in doc_map]
    if missing:
        for mi in range(0, len(missing), 40):
            sub = missing[mi : mi + 40]
            sub_set = set(sub)
            ors_fb: list[dict] = []
            for pid in sub:
                filt = _mongo_filtro_id_produto_externo(pid)
                ors_fb.extend(filt.get("$or") or [filt])
            try:
                for doc in db[client.col_p].find({"$or": ors_fb}, _CADASTRO_LISTA_MONGO_PROJ):
                    _registrar_doc(doc, sub_set)
            except Exception:
                for pid in sub:
                    if pid in doc_map:
                        continue
                    doc = _produto_mongo_por_id_externo(db, client, pid)
                    if doc:
                        doc_map[pid] = doc

    if on_progress:
        on_progress(10, "Mesclando overlay Agro…")

    ovs = _overlay_mapa_por_ids_chunked(uniq)
    out: dict[str, dict] = {}
    for pid in uniq:
        doc = doc_map.get(pid)
        if not doc:
            continue
        row = _produto_mongo_para_cadastro_row(doc)
        ov = ovs.get(pid)
        _aplicar_produto_gestao_overlay_em_dict(row, ov)
        _enriquecer_row_planilha(row, ov)
        out[pid] = row
    return out


def _patch_da_linha(raw: dict, colmap: dict[str, str | None]) -> dict[str, Any]:
    patch: dict[str, Any] = {}

    def txt(key: str, mx: int) -> None:
        hdr = colmap.get(key)
        if not hdr or hdr not in raw:
            return
        v = _cel_opt_str(raw.get(hdr))
        if v is not None:
            patch[key] = v[:mx]

    def dec(key: str) -> None:
        hdr = colmap.get(key)
        if not hdr or hdr not in raw:
            return
        v = raw.get(hdr)
        if v is None or (isinstance(v, str) and not str(v).strip()):
            return
        d = _parse_decimal_br(v)
        if d is None:
            patch[f"__erro_{key}"] = f"Valor inválido em «{hdr}»."
        else:
            patch[key] = d

    txt(COL_CODIGO_GM, 64)
    txt(COL_NOME, 300)
    txt(COL_MARCA, 120)
    txt(COL_MODELO, 200)
    txt(COL_CATEGORIA, 200)
    txt(COL_SUBCATEGORIA, 200)
    txt(COL_SUBCATEGORIA_2, 200)
    txt(COL_SUBCATEGORIA_3, 200)
    txt(COL_SUBCATEGORIA_4, 200)
    txt(COL_FORNECEDOR, 300)
    txt(COL_UNIDADE, 20)
    txt(COL_PESO, 40)
    txt(COL_DESCRICAO, 16000)
    txt(COL_CODIGO_BARRAS, 80)
    txt(COL_NCM, 16)
    txt(COL_CEST, 16)
    txt(COL_CFOP, 8)
    txt(COL_CSOSN, 8)
    txt(COL_ORIGEM, 8)
    dec(COL_PRECO_CUSTO)
    dec(COL_PRECO_VENDA)
    dec(COL_CASHBACK)
    dec(COL_EST_MIN_C)
    dec(COL_EST_MAX_C)
    dec(COL_EST_MIN_V)
    dec(COL_EST_MAX_V)

    hdr_ativo = colmap.get(COL_ATIVO)
    if hdr_ativo and hdr_ativo in raw:
        a = _parse_ativo_planilha(raw.get(hdr_ativo))
        if a is not None:
            patch[COL_ATIVO] = a
        elif _cel_opt_str(raw.get(hdr_ativo)) is not None:
            patch[f"__erro_{COL_ATIVO}"] = f"Valor inválido em «{hdr_ativo}» (use Sim ou Não)."

    if COL_CASHBACK in patch:
        pct = patch[COL_CASHBACK]
        if pct < 0 or pct > 100:
            patch[f"__erro_{COL_CASHBACK}"] = "Cashback % deve estar entre 0 e 100."
            patch.pop(COL_CASHBACK, None)

    return patch


def _merged_row(atual: dict, patch: dict) -> dict:
    out = dict(atual)
    for k, v in patch.items():
        if k.startswith("__"):
            continue
        if k in _COLS_DECIMAL:
            out[k] = float(v)
        elif k == COL_CODIGO_GM:
            out["codigo_gm"] = v
            out["codigo_nfe"] = v
        elif k == COL_ATIVO:
            out[COL_ATIVO] = _fmt_ativo_planilha(v)
            out["ativo_exibicao"] = bool(v)
            out["inativo"] = not bool(v)
        elif k == COL_FORNECEDOR:
            out["fornecedor"] = v
        else:
            out[k] = v
    return out


def _validar_merged(row: dict) -> str | None:
    if not str(row.get("nome") or "").strip():
        return "Nome obrigatório."
    if not str(row.get("marca") or "").strip():
        return "Marca obrigatória."
    if not str(row.get("categoria") or "").strip():
        return "Categoria obrigatória."
    if not str(row.get("codigo_barras") or "").strip():
        return "Código de barras obrigatório."
    try:
        Decimal(str(row.get("preco_venda") or ""))
    except Exception:
        return "Preço de venda inválido."
    try:
        Decimal(str(row.get("preco_custo") or ""))
    except Exception:
        return "Preço de custo inválido."
    return None


def _tem_alteracao(atual: dict, patch: dict) -> bool:
    for k in IMPORT_KEYS:
        if k not in patch:
            continue
        if k in _COLS_DECIMAL:
            atual_v = _valor_atual_campo_import(atual, k)
            try:
                a = round(float(atual_v or 0), 3 if k in _COLS_ESTOQUE else 2)
                b = round(float(patch[k]), 3 if k in _COLS_ESTOQUE else 2)
            except (TypeError, ValueError):
                return True
            if a != b:
                return True
        elif k == COL_ATIVO:
            a = _fmt_ativo_planilha(_valor_atual_campo_import(atual, k))
            b = _fmt_ativo_planilha(patch[k])
            if a != b:
                return True
        elif _valor_atual_campo_import(atual, k) != str(patch[k] or "").strip():
            return True
    return False


def preview_importacao_cadastro(
    path: Path,
    on_progress: None | Any = None,
) -> dict[str, Any]:
    headers, rows_raw = _ler_planilha(path)
    if on_progress:
        on_progress(0, f"total:{len(rows_raw)}")
        on_progress(2, f"Planilha com {len(rows_raw)} linha(s) — lendo…")
    colmap = _map_headers(headers)
    if not colmap.get(COL_ID):
        raise ValueError("Coluna «ID» não encontrada na planilha.")

    hdr_id = colmap[COL_ID]
    rows_slice = rows_raw[:IMPORT_MAX_ROWS]
    vistos: set[str] = set()
    alteracoes: list[dict] = []
    ignoradas: list[dict] = []
    erros: list[dict] = []
    pendentes: list[dict] = []

    for i, raw in enumerate(rows_slice, start=2):
        pid = _cel_str(raw.get(hdr_id or ""))[:64]
        if not pid:
            continue
        if not _id_produto_planilha_valido(pid):
            erros.append(
                {
                    "linha": i,
                    "id": _id_planilha_resumo(pid),
                    "erro": "ID inválido (texto ou coluna errada). Apague a linha ou restaure o ID da exportação.",
                }
            )
            continue
        if pid in vistos:
            erros.append({"linha": i, "id": pid, "erro": "ID duplicado na planilha."})
            continue
        vistos.add(pid)

        patch = _patch_da_linha(raw, colmap)
        err_fields = [v for k, v in patch.items() if k.startswith("__erro_")]
        if err_fields:
            erros.append({"linha": i, "id": pid, "erro": err_fields[0]})
            continue
        if not any(k in patch for k in IMPORT_KEYS):
            ignoradas.append({"linha": i, "id": pid, "motivo": "Nenhum campo alterado (células vazias)."})
            continue
        pendentes.append({"linha": i, "id": pid, "patch": patch})

    if on_progress:
        on_progress(8, f"Conferindo {len(pendentes)} linha(s) preenchida(s)…")

    try:
        facetas = carregar_facetas_planilha()
    except Exception:
        facetas = {k: [] for k in _COLS_FACETA}
    canonicos = {k: _mapa_canonico_faceta(facetas.get(k) or []) for k in _COLS_FACETA}

    mapa = _mapa_estado_atual_produtos([p["id"] for p in pendentes], on_progress=on_progress)
    total_pend = len(pendentes)
    step = max(1, total_pend // 40)
    eventos_faceta: list[dict] = []

    for idx, item in enumerate(pendentes):
        pid = item["id"]
        patch = item["patch"]
        i = item["linha"]

        if on_progress and (idx == 0 or idx % step == 0 or idx == total_pend - 1):
            pct = 10 + int(85 * idx / max(1, total_pend))
            on_progress(pct, f"Analisando linha {i}… ({idx + 1}/{total_pend})")

        atual = mapa.get(pid)
        if not atual:
            erros.append({"linha": i, "id": pid, "erro": "Produto não encontrado no catálogo."})
            continue

        # Prévia: permite novos só para classificar (não bloqueia ainda).
        patch_res, evs, _errs_fac = _resolver_facetas_no_patch(
            patch, facetas, canonicos, permitir_novos=True, linha=i
        )
        eventos_faceta.extend(evs)

        if not _tem_alteracao(atual, patch_res):
            if any(e.get("acao") == "novo" for e in evs):
                # Valor novo igual ao atual? Improvável — trata como ignorada.
                ignoradas.append({"linha": i, "id": pid, "motivo": "Valores iguais ao cadastro atual."})
                continue
            if not _tem_alteracao(atual, patch):
                ignoradas.append({"linha": i, "id": pid, "motivo": "Valores iguais ao cadastro atual."})
                continue
            ignoradas.append(
                {
                    "linha": i,
                    "id": pid,
                    "motivo": "Após corrigir typo, valores iguais ao cadastro.",
                }
            )
            continue

        merged = _merged_row(atual, patch_res)
        vmsg = _validar_merged(merged)
        if vmsg:
            erros.append({"linha": i, "id": pid, "erro": vmsg})
            continue

        # Sem «permitir novos»: novos entram como erro na prévia (ainda mostra aviso).
        novos_na_linha = [e for e in evs if e.get("acao") == "novo"]
        if novos_na_linha:
            for nv in novos_na_linha:
                erros.append(
                    {
                        "linha": i,
                        "id": pid,
                        "erro": (
                            f"{nv.get('rotulo')} «{nv.get('valor')}» ainda não cadastrado. "
                            "Marque «Permitir criar novos» para gravar, ou use a lista / sugestão."
                        ),
                        "tipo": "valor_novo",
                        "campo": nv.get("campo"),
                        "valor": nv.get("valor"),
                    }
                )

        campos = []
        for k in IMPORT_KEYS:
            if k not in patch_res:
                continue
            de_v = atual.get(k) if k in _COLS_DECIMAL else _valor_atual_campo_import(atual, k)
            para_v = patch_res[k]
            if k == COL_ATIVO:
                de_v = _fmt_ativo_planilha(de_v if de_v not in (None, "") else atual.get("ativo_exibicao"))
                para_v = _fmt_ativo_planilha(para_v)
            elif k in _COLS_DECIMAL:
                try:
                    para_v = float(para_v)
                except (TypeError, ValueError):
                    pass
            item_c = {"campo": k, "de": de_v, "para": para_v}
            for ev in evs:
                if ev.get("campo") == k and ev.get("acao") == "corrigir":
                    item_c["sugestao"] = ev.get("sugestao")
                    item_c["de_planilha"] = ev.get("valor")
                    item_c["nota"] = f"typo → {ev.get('sugestao')}"
                elif ev.get("campo") == k and ev.get("acao") == "novo":
                    item_c["nota"] = "valor novo"
            campos.append(item_c)
        if not campos:
            continue
        alteracoes.append(
            {
                "linha": i,
                "id": pid,
                "nome": merged.get("nome") or atual.get("nome") or "",
                "campos": campos,
                "tem_valor_novo": bool(novos_na_linha),
            }
        )

    if on_progress:
        on_progress(100, "Concluído")

    if len(rows_raw) > IMPORT_MAX_ROWS:
        erros.append(
            {
                "linha": None,
                "id": "",
                "erro": f"Planilha truncada: só as primeiras {IMPORT_MAX_ROWS} linhas foram analisadas.",
            }
        )

    valores_novos, correcoes = _resumir_eventos_faceta(eventos_faceta)
    # Alterações sem valor novo (podem gravar sem checkbox).
    n_ok = sum(1 for a in alteracoes if not a.get("tem_valor_novo"))
    n_bloqueadas_novo = sum(1 for a in alteracoes if a.get("tem_valor_novo"))

    return {
        "total_linhas": len(rows_raw),
        "alteracoes": alteracoes[:500],
        "n_alteracoes": len(alteracoes),
        "n_alteracoes_ok": n_ok,
        "n_bloqueadas_valor_novo": n_bloqueadas_novo,
        "ignoradas": ignoradas[:80],
        "n_ignoradas": len(ignoradas),
        "erros": erros[:120],
        "n_erros": len(erros),
        "valores_novos": valores_novos[:80],
        "n_valores_novos": len(valores_novos),
        "correcoes_sugeridas": correcoes[:80],
        "n_correcoes_sugeridas": len(correcoes),
        "permitir_novos_padrao": False,
    }


def _snapshot_antes_import(db, client, pid: str, patch: dict, nome: str = "") -> dict:
    """Estado overlay + Mongo antes de gravar — usado para desfazer."""
    from produtos.views import _produto_mongo_por_id_externo

    pid = str(pid or "").strip()[:64]
    ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()
    overlay: dict[str, Any] = {}
    for k in OVERLAY_IMPORT_KEYS:
        if k == COL_PRECO_VENDA:
            val = ov.preco_venda if ov else None
            overlay[k] = str(val) if val is not None else None
        elif k == COL_CASHBACK:
            val = ov.cashback_percentual if ov else None
            overlay[k] = str(val) if val is not None else None
        elif k in _COLS_ESTOQUE:
            val = getattr(ov, k, None) if ov else None
            overlay[k] = str(val) if val is not None else None
        elif k == COL_ATIVO:
            if ov is None or ov.ativo_exibicao is None:
                overlay[k] = None
            else:
                overlay[k] = bool(ov.ativo_exibicao)
        else:
            fld = _overlay_model_field(k)
            overlay[k] = (getattr(ov, fld, "") or "") if ov else ""

    extras_antes: dict[str, Any] = {}
    if ov and isinstance(ov.cadastro_extras, dict):
        ex = dict(ov.cadastro_extras)
        extras_antes["preco_custo_overlay"] = ex.get("preco_custo_overlay")
        extras_antes["modelo"] = ex.get("modelo")
        fiscal = ex.get("fiscal") if isinstance(ex.get("fiscal"), dict) else {}
        extras_antes["fiscal"] = {
            COL_NCM: fiscal.get("ncm") or "",
            COL_CEST: fiscal.get("cest") or "",
            COL_CFOP: fiscal.get("cfop") or "",
            COL_CSOSN: fiscal.get("csosn") or "",
            COL_ORIGEM: fiscal.get("origem") or "",
        }

    mongo: dict[str, Any] = {}
    if db is not None:
        doc = _produto_mongo_por_id_externo(db, client, pid)
        if doc:
            for mk in ("PrecoCusto", "ValorCusto", "ValorVenda", "PrecoVenda"):
                if mk in doc and doc[mk] is not None:
                    mongo[mk] = float(doc[mk])

    produto_pg: dict[str, Any] = {}
    from produtos.agro_fonte_config import agro_catalogo_usa_postgres

    if agro_catalogo_usa_postgres():
        from produtos import catalogo_agro

        p_pg = catalogo_agro.obter_produto_model(pid)
        if p_pg is not None:
            produto_pg = {
                "custo": float(p_pg.custo or 0),
                "preco_venda": float(p_pg.preco_venda or 0),
            }

    campos = [k for k in IMPORT_KEYS if k in patch]
    para: dict[str, Any] = {}
    for k in campos:
        v = patch[k]
        if k in _COLS_DECIMAL:
            para[k] = float(v)
        elif k == COL_ATIVO:
            para[k] = bool(v)
        else:
            para[k] = v

    return {
        "id": pid,
        "nome": str(nome or "")[:300],
        "campos_alterados": campos,
        "overlay_existia": ov is not None,
        "overlay": overlay,
        "extras": extras_antes,
        "mongo": mongo,
        "produto_pg": produto_pg,
        "para": para,
    }


def _mx_overlay_texto(key: str) -> int:
    if key == COL_NOME:
        return 300
    if key == COL_CODIGO_GM:
        return 64
    if key == COL_CODIGO_BARRAS:
        return 80
    if key == COL_FORNECEDOR:
        return 300
    if key == COL_UNIDADE:
        return 20
    if key == COL_PESO:
        return 40
    if key == COL_DESCRICAO:
        return 16000
    if key == COL_MARCA:
        return 120
    return 200


def _overlay_import_esta_vazio(ov: ProdutoGestaoOverlayAgro) -> bool:
    for k in OVERLAY_IMPORT_KEYS:
        if k == COL_PRECO_VENDA:
            if ov.preco_venda is not None:
                return False
        elif k == COL_CASHBACK:
            if ov.cashback_percentual is not None:
                return False
        elif k in _COLS_ESTOQUE:
            if getattr(ov, k, None) is not None:
                return False
        elif k == COL_ATIVO:
            if ov.ativo_exibicao is not None:
                return False
        elif str(getattr(ov, _overlay_model_field(k), "") or "").strip():
            return False
    ex = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
    if ex.get("preco_custo_overlay") is not None:
        return False
    if str(ex.get("modelo") or "").strip():
        return False
    fiscal = ex.get("fiscal") if isinstance(ex.get("fiscal"), dict) else {}
    if any(str(fiscal.get(x) or "").strip() for x in ("ncm", "cest", "cfop", "csosn", "origem")):
        return False
    return True


def _aplicar_valor_overlay_revert(ov: ProdutoGestaoOverlayAgro, key: str, v) -> None:
    if key == COL_PRECO_VENDA:
        ov.preco_venda = Decimal(str(v)) if v is not None and str(v).strip() != "" else None
        return
    if key == COL_CASHBACK:
        ov.cashback_percentual = Decimal(str(v)) if v is not None and str(v).strip() != "" else None
        return
    if key in _COLS_ESTOQUE:
        setattr(
            ov,
            key,
            Decimal(str(v)) if v is not None and str(v).strip() != "" else None,
        )
        return
    if key == COL_ATIVO:
        if v is None or v == "":
            ov.ativo_exibicao = None
        else:
            ov.ativo_exibicao = bool(v) if isinstance(v, bool) else _parse_ativo_planilha(v)
        return
    fld = _overlay_model_field(key)
    setattr(ov, fld, str(v or "")[: _mx_overlay_texto(key)])


def _reverter_extras_import(ov: ProdutoGestaoOverlayAgro, item: dict, patch_keys: list[str]) -> None:
    extras_antes = item.get("extras") if isinstance(item.get("extras"), dict) else {}
    ex = dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
    if COL_PRECO_CUSTO in patch_keys:
        if "preco_custo_overlay" in extras_antes:
            if extras_antes.get("preco_custo_overlay") is None:
                ex.pop("preco_custo_overlay", None)
            else:
                ex["preco_custo_overlay"] = extras_antes.get("preco_custo_overlay")
        else:
            ex.pop("preco_custo_overlay", None)
    if COL_MODELO in patch_keys:
        if extras_antes.get("modelo"):
            ex["modelo"] = extras_antes.get("modelo")
        else:
            ex.pop("modelo", None)
    fiscal_keys = [k for k in patch_keys if k in _COLS_FISCAL]
    if fiscal_keys:
        fiscal = dict(ex.get("fiscal")) if isinstance(ex.get("fiscal"), dict) else {}
        fiscal_antes = extras_antes.get("fiscal") if isinstance(extras_antes.get("fiscal"), dict) else {}
        for fk in fiscal_keys:
            old = str(fiscal_antes.get(fk) or "").strip()
            if old:
                fiscal[fk] = old
            else:
                fiscal.pop(fk, None)
        if any(str(fiscal.get(x) or "").strip() for x in ("ncm", "cest", "cfop", "csosn", "origem")):
            ex["fiscal"] = fiscal
        else:
            ex.pop("fiscal", None)
    ov.cadastro_extras = ex


def _reverter_item_import(item: dict, db, client, user) -> None:
    from produtos.views import _mongo_filtro_id_produto_externo

    pid = str(item.get("id") or "").strip()[:64]
    if not pid:
        return
    patch_keys = list(item.get("campos_alterados") or item.get("para", {}).keys())
    overlay_antes = item.get("overlay") or {}
    overlay_existia = bool(item.get("overlay_existia"))

    ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()

    if overlay_existia:
        if not ov:
            ov = ProdutoGestaoOverlayAgro.objects.create(
                produto_externo_id=pid,
                usuario=user if user and user.is_authenticated else None,
            )
        for k in patch_keys:
            if k in OVERLAY_IMPORT_KEYS:
                _aplicar_valor_overlay_revert(ov, k, overlay_antes.get(k))
        _reverter_extras_import(ov, item, patch_keys)
        ov.save()
    elif ov:
        for k in patch_keys:
            if k not in OVERLAY_IMPORT_KEYS:
                continue
            if k == COL_PRECO_VENDA:
                ov.preco_venda = None
            elif k == COL_CASHBACK:
                ov.cashback_percentual = None
            elif k in _COLS_ESTOQUE:
                setattr(ov, k, None)
            elif k == COL_ATIVO:
                ov.ativo_exibicao = None
            else:
                setattr(ov, _overlay_model_field(k), "")
        if any(k in _COLS_EXTRAS for k in patch_keys):
            _reverter_extras_import(ov, item, patch_keys)
        if _overlay_import_esta_vazio(ov):
            ov.delete()
        else:
            ov.save()

    mongo_antes = item.get("mongo") or {}
    if db is not None and mongo_antes:
        mongo_set: dict[str, float] = {}
        if COL_PRECO_CUSTO in patch_keys:
            for mk in ("PrecoCusto", "ValorCusto"):
                if mk in mongo_antes:
                    mongo_set[mk] = float(mongo_antes[mk])
        if COL_PRECO_VENDA in patch_keys:
            for mk in ("ValorVenda", "PrecoVenda"):
                if mk in mongo_antes:
                    mongo_set[mk] = float(mongo_antes[mk])
        if mongo_set:
            from produtos.agro_mongo_guard import agro_mongo_escrita_bloqueada

            if not agro_mongo_escrita_bloqueada():
                db[client.col_p].update_one(_mongo_filtro_id_produto_externo(pid), {"$set": mongo_set})

    from produtos.agro_fonte_config import agro_catalogo_usa_postgres

    if agro_catalogo_usa_postgres():
        from decimal import Decimal

        from produtos import catalogo_agro

        pg_antes = item.get("produto_pg") or {}
        ov_atual = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()
        if ov_atual is not None:
            custo_rev = None
            if COL_PRECO_CUSTO in patch_keys and "custo" in pg_antes:
                custo_rev = Decimal(str(pg_antes["custo"])).quantize(Decimal("0.01"))
            catalogo_agro.sincronizar_modelo_produto_de_overlay(
                pid, ov_atual, custo_payload=custo_rev
            )
        elif pg_antes:
            p = catalogo_agro.obter_produto_model(pid)
            if p is not None:
                changed = False
                if COL_PRECO_CUSTO in patch_keys and "custo" in pg_antes:
                    p.custo = Decimal(str(pg_antes["custo"])).quantize(Decimal("0.01"))
                    changed = True
                if COL_PRECO_VENDA in patch_keys and "preco_venda" in pg_antes:
                    p.preco_venda = Decimal(str(pg_antes["preco_venda"])).quantize(Decimal("0.01"))
                    changed = True
                if changed:
                    p.save()


def _historico_import_resumo_item(item: dict) -> dict:
    campos = item.get("campos_alterados") or []
    detalhes = []
    para = item.get("para") or {}
    de_map = item.get("de_merged") or item.get("overlay") or {}
    for k in campos[:6]:
        detalhes.append({"campo": k, "de": de_map.get(k), "para": para.get(k)})
    return {
        "id": item.get("id"),
        "nome": item.get("nome") or "",
        "campos": campos,
        "detalhes": detalhes,
    }


def listar_historico_import_cadastro(*, limite: int = HISTORICO_IMPORT_LISTA_LIMITE) -> list[dict]:
    out: list[dict] = []
    qs = CadastroPlanilhaImportHistoricoAgro.objects.select_related("usuario", "revertido_por")
    if hasattr(CadastroPlanilhaImportHistoricoAgro, "tipo"):
        qs = qs.filter(tipo=CadastroPlanilhaImportHistoricoAgro.Tipo.CADASTRO)
    qs = qs.order_by("-criado_em")[:limite]
    for h in qs:
        items = (h.backup or {}).get("items") or []
        out.append(
            {
                "id": h.pk,
                "criado_em": h.criado_em.isoformat() if h.criado_em else "",
                "usuario": (h.usuario.get_username() if h.usuario else "") or "",
                "nome_arquivo": h.nome_arquivo or "",
                "n_produtos": h.n_produtos,
                "n_campos": h.n_campos,
                "status": h.status,
                "pode_reverter": h.status == CadastroPlanilhaImportHistoricoAgro.Status.APLICADO,
                "revertido_em": h.revertido_em.isoformat() if h.revertido_em else "",
                "revertido_por": (h.revertido_por.get_username() if h.revertido_por else "") or "",
                "resumo": [_historico_import_resumo_item(it) for it in items[:12]],
            }
        )
    return out


def reverter_importacao_cadastro(historico_id: int, user) -> dict[str, Any]:
    from django.utils import timezone

    hist = CadastroPlanilhaImportHistoricoAgro.objects.filter(pk=historico_id).first()
    if not hist:
        raise ValueError("Histórico não encontrado.")
    if getattr(hist, "tipo", "cadastro") == "estoque" or (hist.backup or {}).get("tipo") == "estoque":
        raise ValueError("Use o histórico de Excel estoque para desfazer esta importação.")
    if hist.status != CadastroPlanilhaImportHistoricoAgro.Status.APLICADO:
        raise ValueError("Esta importação já foi desfeita.")

    items = (hist.backup or {}).get("items") or []
    if not items:
        raise ValueError("Backup vazio — não é possível desfazer.")

    from produtos.agro_fonte_config import agro_catalogo_usa_postgres
    from produtos.views import obter_conexao_mongo

    use_pg = agro_catalogo_usa_postgres()
    client, db = (None, None)
    if not use_pg:
        client, db = obter_conexao_mongo()
        if db is None:
            raise ValueError("Mongo indisponível.")

    with transaction.atomic():
        for item in items:
            _reverter_item_import(item, db, client, user)
        hist.status = CadastroPlanilhaImportHistoricoAgro.Status.REVERTIDO
        hist.revertido_em = timezone.now()
        hist.revertido_por = user if user and user.is_authenticated else None
        hist.save(update_fields=["status", "revertido_em", "revertido_por"])

    _invalidar_cache_catalogo_pdv()
    return {
        "historico_id": hist.pk,
        "revertidos": len(items),
        "status": hist.status,
    }


def _invalidar_cache_catalogo_pdv() -> None:
    from produtos.views import CATALOGO_PDV_CACHE_ENTRY_KEY, CATALOGO_PDV_CACHE_PREV_ENTRY_KEY

    try:
        cur_cat = cache.get(CATALOGO_PDV_CACHE_ENTRY_KEY)
        if isinstance(cur_cat, dict) and cur_cat.get("version"):
            cache.set(CATALOGO_PDV_CACHE_PREV_ENTRY_KEY, cur_cat, timeout=86400 * 3)
        cache.delete(CATALOGO_PDV_CACHE_ENTRY_KEY)
    except Exception:
        pass


def _gravar_patch_produto(db, client, pid: str, patch: dict, user) -> None:
    from produtos.cadastro_alteracao_historico_util import (
        registrar_diffs_cadastro,
        snapshot_overlay,
    )
    from produtos.models import ProdutoCadastroAlteracaoAgro
    from produtos.views import _mongo_filtro_id_produto_externo

    ov, _ = ProdutoGestaoOverlayAgro.objects.get_or_create(
        produto_externo_id=pid[:64],
        defaults={"usuario": user if user and user.is_authenticated else None},
    )
    antes = snapshot_overlay(ov)
    ex = dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
    if COL_CODIGO_GM in patch:
        ov.codigo_nfe = str(patch[COL_CODIGO_GM] or "")[:64]
    if COL_NOME in patch:
        ov.nome = str(patch[COL_NOME] or "")[:300]
    if COL_MARCA in patch:
        ov.marca = str(patch[COL_MARCA] or "")[:120]
    if COL_CATEGORIA in patch:
        ov.categoria = str(patch[COL_CATEGORIA] or "")[:200]
    if COL_SUBCATEGORIA in patch:
        ov.subcategoria = str(patch[COL_SUBCATEGORIA] or "")[:200]
    if COL_SUBCATEGORIA_2 in patch:
        ov.subcategoria_2 = str(patch[COL_SUBCATEGORIA_2] or "")[:200]
    if COL_SUBCATEGORIA_3 in patch:
        ov.subcategoria_3 = str(patch[COL_SUBCATEGORIA_3] or "")[:200]
    if COL_SUBCATEGORIA_4 in patch:
        ov.subcategoria_4 = str(patch[COL_SUBCATEGORIA_4] or "")[:200]
    if COL_FORNECEDOR in patch:
        ov.fornecedor_texto = str(patch[COL_FORNECEDOR] or "")[:300]
    if COL_UNIDADE in patch:
        ov.unidade = str(patch[COL_UNIDADE] or "")[:20]
    if COL_PESO in patch:
        ov.peso_etiqueta = str(patch[COL_PESO] or "")[:40]
    if COL_DESCRICAO in patch:
        ov.descricao = str(patch[COL_DESCRICAO] or "")[:16000]
    if COL_CODIGO_BARRAS in patch:
        ov.codigo_barras = str(patch[COL_CODIGO_BARRAS] or "")[:80]
    if COL_PRECO_VENDA in patch:
        ov.preco_venda = patch[COL_PRECO_VENDA]
    if COL_CASHBACK in patch:
        ov.cashback_percentual = patch[COL_CASHBACK]
    if COL_ATIVO in patch:
        ov.ativo_exibicao = bool(patch[COL_ATIVO])
    for fld in _COLS_ESTOQUE:
        if fld in patch:
            setattr(ov, fld, patch[fld])
    if COL_PRECO_CUSTO in patch:
        ex["preco_custo_overlay"] = float(patch[COL_PRECO_CUSTO])
    if COL_MODELO in patch:
        modelo = str(patch[COL_MODELO] or "").strip()[:200]
        if modelo:
            ex["modelo"] = modelo
        else:
            ex.pop("modelo", None)
    fiscal_keys = [k for k in (COL_NCM, COL_CEST, COL_CFOP, COL_CSOSN, COL_ORIGEM) if k in patch]
    if fiscal_keys:
        fiscal = dict(ex.get("fiscal")) if isinstance(ex.get("fiscal"), dict) else {}
        for fk in fiscal_keys:
            val = str(patch[fk] or "").strip()
            if val:
                fiscal[fk] = val
            else:
                fiscal.pop(fk, None)
        if any(str(fiscal.get(x) or "").strip() for x in ("ncm", "cest", "cfop", "csosn", "origem")):
            ex["fiscal"] = fiscal
        else:
            ex.pop("fiscal", None)
    ov.cadastro_extras = ex
    depois = snapshot_overlay(ov)
    try:
        registrar_diffs_cadastro(
            produto_id=pid,
            antes=antes,
            depois=depois,
            usuario=user if user and getattr(user, "is_authenticated", False) else None,
            origem=ProdutoCadastroAlteracaoAgro.Origem.PLANILHA,
        )
    except Exception:
        pass
    ov.save()

    custo_payload = patch.get(COL_PRECO_CUSTO) if COL_PRECO_CUSTO in patch else None

    from produtos.agro_fonte_config import agro_catalogo_usa_postgres

    if agro_catalogo_usa_postgres():
        from produtos import catalogo_agro

        catalogo_agro.sincronizar_modelo_produto_de_overlay(
            pid, ov, custo_payload=custo_payload
        )

    mongo_set: dict[str, float] = {}
    if COL_PRECO_CUSTO in patch:
        cfloat = float(patch[COL_PRECO_CUSTO])
        mongo_set["PrecoCusto"] = cfloat
        mongo_set["ValorCusto"] = cfloat
    if COL_PRECO_VENDA in patch:
        pvfloat = float(patch[COL_PRECO_VENDA])
        mongo_set["ValorVenda"] = pvfloat
        mongo_set["PrecoVenda"] = pvfloat
    if mongo_set and db is not None:
        from produtos.agro_mongo_guard import agro_mongo_escrita_bloqueada

        if not agro_mongo_escrita_bloqueada():
            db[client.col_p].update_one(_mongo_filtro_id_produto_externo(pid), {"$set": mongo_set})

    if COL_PRECO_CUSTO in patch and custo_payload is not None:
        try:
            from produtos.custo_familia_util import propagar_custo_familia_de_pai

            propagar_custo_familia_de_pai(
                pid, custo_payload, origem="planilha", usuario=user
            )
        except Exception:
            pass


def aplicar_importacao_cadastro(
    path: Path,
    user,
    *,
    nome_arquivo: str = "",
    permitir_novos: bool = False,
    on_progress: None | Any = None,
) -> dict[str, Any]:
    from produtos.agro_fonte_config import agro_catalogo_usa_postgres
    from produtos.views import obter_conexao_mongo

    use_pg = agro_catalogo_usa_postgres()
    client, db = (None, None)
    if not use_pg:
        client, db = obter_conexao_mongo()
        if db is None:
            raise ValueError("Mongo indisponível.")

    headers, rows_raw = _ler_planilha(path)
    colmap = _map_headers(headers)
    hdr_id = colmap.get(COL_ID)
    if not hdr_id:
        raise ValueError("Coluna «ID» não encontrada.")

    if on_progress:
        on_progress(2, f"total:{len(rows_raw)}")
        on_progress(4, f"Lendo {len(rows_raw)} linha(s)…")

    try:
        facetas = carregar_facetas_planilha()
    except Exception:
        facetas = {k: [] for k in _COLS_FACETA}
    canonicos = {k: _mapa_canonico_faceta(facetas.get(k) or []) for k in _COLS_FACETA}

    candidatos: list[dict] = []
    vistos: set[str] = set()
    for i, raw in enumerate(rows_raw[:IMPORT_MAX_ROWS], start=2):
        pid = _cel_str(raw.get(hdr_id or ""))[:64]
        if not pid or not _id_produto_planilha_valido(pid) or pid in vistos:
            continue
        vistos.add(pid)
        patch = _patch_da_linha(raw, colmap)
        if any(k.startswith("__erro_") for k in patch):
            continue
        if not any(k in patch for k in IMPORT_KEYS):
            continue
        candidatos.append({"linha": i, "id": pid, "patch": patch})

    if on_progress:
        on_progress(8, f"Conferindo {len(candidatos)} linha(s) com dados…")

    mapa = _mapa_estado_atual_produtos([c["id"] for c in candidatos], on_progress=on_progress)

    fila: list[dict] = []
    bloqueados_novo = 0
    for item in candidatos:
        pid = item["id"]
        patch = item["patch"]
        i = item["linha"]
        atual = mapa.get(pid)
        if not atual:
            continue
        patch_res, _evs, errs_fac = _resolver_facetas_no_patch(
            patch, facetas, canonicos, permitir_novos=permitir_novos, linha=i
        )
        if errs_fac:
            bloqueados_novo += 1
            continue
        if not _tem_alteracao(atual, patch_res):
            continue
        merged = _merged_row(atual, patch_res)
        vmsg = _validar_merged(merged)
        if vmsg:
            continue
        fila.append({"linha": i, "id": pid, "patch": patch_res, "atual": atual})

    if not fila:
        if bloqueados_novo:
            raise ValueError(
                f"{bloqueados_novo} linha(s) com marca/categoria/sub nova. "
                "Marque «Permitir criar novos» ou use nomes da lista / correção de typo."
            )
        raise ValueError("Nenhuma alteração válida para gravar — confira a prévia.")

    if on_progress:
        on_progress(92, f"Gravando {len(fila)} produto(s)…")

    ok = 0
    falhas: list[dict] = []
    backups: list[dict] = []
    n_campos_total = 0
    total_fila = len(fila)

    for idx, item in enumerate(fila):
        pid = item["id"]
        patch = item["patch"]
        i = item["linha"]
        atual = item["atual"]
        if on_progress and (idx == 0 or idx == total_fila - 1 or idx % max(1, total_fila // 20) == 0):
            pct = 92 + int(7 * idx / max(1, total_fila))
            on_progress(pct, f"Gravando produto {idx + 1}/{total_fila}…")

        snap = _snapshot_antes_import(
            db,
            client,
            pid,
            patch,
            nome=str(atual.get("nome") or ""),
        )
        snap["de_merged"] = {
            k: atual.get(k) for k in snap.get("campos_alterados") or [] if k in patch
        }
        try:
            with transaction.atomic():
                _gravar_patch_produto(db, client, pid, patch, user)
            backups.append(snap)
            n_campos_total += len(snap.get("campos_alterados") or [])
            ok += 1
        except Exception as exc:
            falhas.append({"linha": i, "id": pid, "erro": str(exc) or "Falha ao gravar."})

    hist = None
    if backups:
        hist = CadastroPlanilhaImportHistoricoAgro.objects.create(
            usuario=user if user and getattr(user, "is_authenticated", False) else None,
            nome_arquivo=str(nome_arquivo or "")[:255],
            n_produtos=ok,
            n_campos=n_campos_total,
            backup={"items": backups, "permitir_novos": bool(permitir_novos)},
            **(
                {"tipo": CadastroPlanilhaImportHistoricoAgro.Tipo.CADASTRO}
                if hasattr(CadastroPlanilhaImportHistoricoAgro, "tipo")
                else {}
            ),
        )

    if ok:
        _invalidar_cache_catalogo_pdv()

    if on_progress:
        on_progress(100, "Concluído")

    return {
        "n_alteracoes": ok,
        "gravados": ok,
        "n_falhas": len(falhas),
        "falhas": falhas[:40],
        "n_bloqueados_valor_novo": bloqueados_novo,
        "historico_id": hist.pk if hist else None,
        "permitir_novos": bool(permitir_novos),
    }
