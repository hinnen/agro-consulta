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
COL_CATEGORIA = "categoria"
COL_SUBCATEGORIA = "subcategoria"
COL_CODIGO_BARRAS = "codigo_barras"
COL_PRECO_CUSTO = "preco_custo"
COL_PRECO_VENDA = "preco_venda"

EXPORT_HEADERS: list[tuple[str, str]] = [
    ("ID", COL_ID),
    ("Código GM", COL_CODIGO_GM),
    ("Nome", COL_NOME),
    ("Marca", COL_MARCA),
    ("Categoria", COL_CATEGORIA),
    ("Subcategoria", COL_SUBCATEGORIA),
    ("Código barras", COL_CODIGO_BARRAS),
    ("Preço custo", COL_PRECO_CUSTO),
    ("Preço venda", COL_PRECO_VENDA),
]

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
    COL_CATEGORIA,
    COL_SUBCATEGORIA,
    COL_CODIGO_BARRAS,
    COL_PRECO_CUSTO,
    COL_PRECO_VENDA,
}

OVERLAY_IMPORT_KEYS = (
    COL_CODIGO_GM,
    COL_NOME,
    COL_MARCA,
    COL_CATEGORIA,
    COL_SUBCATEGORIA,
    COL_CODIGO_BARRAS,
    COL_PRECO_VENDA,
)

HISTORICO_IMPORT_LISTA_LIMITE = 30


def _overlay_model_field(key: str) -> str:
    return "codigo_nfe" if key == COL_CODIGO_GM else key


def _valor_atual_campo_import(atual: dict, key: str):
    if key in (COL_PRECO_CUSTO, COL_PRECO_VENDA):
        return atual.get(key)
    if key == COL_CODIGO_GM:
        return str(atual.get("codigo_gm") or atual.get("codigo_nfe") or "").strip()
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
        COL_CATEGORIA: ("categoria", "grupo"),
        COL_SUBCATEGORIA: ("subcategoria", "sub categoria", "subgrupo"),
        COL_CODIGO_BARRAS: ("codigo barras", "codigo de barras", "ean", "barras", "cb"),
        COL_PRECO_CUSTO: ("preco custo", "preço custo", "custo", "custo unitario", "custo unitário"),
        COL_PRECO_VENDA: ("preco venda", "preço venda", "venda", "preco de venda", "preço de venda"),
    }
    out: dict[str, str | None] = {}
    for key, keys in aliases.items():
        out[key] = None
        for k in keys:
            if k in norm:
                out[key] = norm[k]
                break
    return out


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
    """ID exportado: ObjectId Mongo (24 hex) ou Id numérico ERP."""
    s = str(pid or "").strip()
    if not s or len(s) > 64:
        return False
    low = s.lower()
    if re.fullmatch(r"[0-9a-f]{24}", low):
        return True
    return bool(s.isdigit() and 1 <= len(s) <= 12)


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
            chunk, total = catalogo_agro.listar_paginado(
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
                _aplicar_produto_gestao_overlay_em_dict(r, ovs.get(str(r.get("id") or "")))
                rows.append(r)
            if pagina * por_pagina >= total:
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
        _aplicar_produto_gestao_overlay_em_dict(r, ovs.get(str(r.get("id") or "")))
    rows = _filtrar_rows_categorias(rows, categorias or [])
    return rows, truncado


def linha_export_planilha(row: dict) -> dict[str, Any]:
    return {
        COL_ID: str(row.get("id") or ""),
        COL_CODIGO_GM: str(row.get("codigo_nfe") or row.get("codigo") or ""),
        COL_NOME: str(row.get("nome") or ""),
        COL_MARCA: str(row.get("marca") or ""),
        COL_CATEGORIA: str(row.get("categoria") or ""),
        COL_SUBCATEGORIA: str(row.get("subcategoria") or ""),
        COL_CODIGO_BARRAS: str(row.get("codigo_barras") or ""),
        COL_PRECO_CUSTO: float(row.get("preco_custo") or 0),
        COL_PRECO_VENDA: float(row.get("preco_venda") or 0),
    }


def montar_xlsx_cadastro(rows: list[dict], colunas: list[str] | None = None) -> bytes:
    from openpyxl.styles import Protection

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
            if key in (COL_ID, COL_CODIGO_GM, COL_CODIGO_BARRAS):
                cell.value = str(val) if val is not None else ""
                cell.number_format = "@"
            else:
                cell.value = val
                if key in (COL_PRECO_CUSTO, COL_PRECO_VENDA):
                    cell.number_format = "#,##0.00"
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
        _aplicar_produto_gestao_overlay_em_dict(row, ovs.get(pid))
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
    txt(COL_CATEGORIA, 200)
    txt(COL_SUBCATEGORIA, 200)
    txt(COL_CODIGO_BARRAS, 80)
    dec(COL_PRECO_CUSTO)
    dec(COL_PRECO_VENDA)
    return patch


def _merged_row(atual: dict, patch: dict) -> dict:
    out = dict(atual)
    for k, v in patch.items():
        if k.startswith("__"):
            continue
        if k in (COL_PRECO_CUSTO, COL_PRECO_VENDA):
            out[k] = float(v)
        elif k == COL_CODIGO_GM:
            out["codigo_gm"] = v
            out["codigo_nfe"] = v
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
        if k in (COL_PRECO_CUSTO, COL_PRECO_VENDA):
            if round(float(atual.get(k) or 0), 2) != round(float(patch[k]), 2):
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

    mapa = _mapa_estado_atual_produtos([p["id"] for p in pendentes], on_progress=on_progress)
    total_pend = len(pendentes)
    step = max(1, total_pend // 40)

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
        if not _tem_alteracao(atual, patch):
            ignoradas.append({"linha": i, "id": pid, "motivo": "Valores iguais ao cadastro atual."})
            continue

        merged = _merged_row(atual, patch)
        vmsg = _validar_merged(merged)
        if vmsg:
            erros.append({"linha": i, "id": pid, "erro": vmsg})
            continue

        campos = []
        for k in IMPORT_KEYS:
            if k not in patch:
                continue
            campos.append(
                {
                    "campo": k,
                    "de": atual.get(k),
                    "para": patch[k],
                }
            )
        alteracoes.append(
            {
                "linha": i,
                "id": pid,
                "nome": merged.get("nome") or atual.get("nome") or "",
                "campos": campos,
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

    return {
        "total_linhas": len(rows_raw),
        "alteracoes": alteracoes[:500],
        "n_alteracoes": len(alteracoes),
        "ignoradas": ignoradas[:80],
        "n_ignoradas": len(ignoradas),
        "erros": erros[:120],
        "n_erros": len(erros),
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
        else:
            fld = _overlay_model_field(k)
            overlay[k] = (getattr(ov, fld, "") or "") if ov else ""

    mongo: dict[str, Any] = {}
    if db is not None:
        doc = _produto_mongo_por_id_externo(db, client, pid)
        if doc:
            for mk in ("PrecoCusto", "ValorCusto", "ValorVenda", "PrecoVenda"):
                if mk in doc and doc[mk] is not None:
                    mongo[mk] = float(doc[mk])

    campos = [k for k in IMPORT_KEYS if k in patch]
    para: dict[str, Any] = {}
    for k in campos:
        v = patch[k]
        if k in (COL_PRECO_CUSTO, COL_PRECO_VENDA):
            para[k] = float(v)
        else:
            para[k] = v

    return {
        "id": pid,
        "nome": str(nome or "")[:300],
        "campos_alterados": campos,
        "overlay_existia": ov is not None,
        "overlay": overlay,
        "mongo": mongo,
        "para": para,
    }


def _overlay_import_esta_vazio(ov: ProdutoGestaoOverlayAgro) -> bool:
    for k in OVERLAY_IMPORT_KEYS:
        if k == COL_PRECO_VENDA:
            if ov.preco_venda is not None:
                return False
        elif str(getattr(ov, _overlay_model_field(k), "") or "").strip():
            return False
    return True


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
            if k not in OVERLAY_IMPORT_KEYS:
                continue
            v = overlay_antes.get(k)
            if k == COL_PRECO_VENDA:
                ov.preco_venda = Decimal(str(v)) if v is not None else None
            else:
                fld = _overlay_model_field(k)
                mx = (
                    300
                    if k == COL_NOME
                    else 64
                    if k == COL_CODIGO_GM
                    else 80
                    if k == COL_CODIGO_BARRAS
                    else 200
                    if k in (COL_CATEGORIA, COL_SUBCATEGORIA)
                    else 120
                )
                setattr(ov, fld, str(v or "")[:mx])
        ov.save()
    elif ov:
        for k in patch_keys:
            if k not in OVERLAY_IMPORT_KEYS:
                continue
            if k == COL_PRECO_VENDA:
                ov.preco_venda = None
            else:
                fld = _overlay_model_field(k)
                mx = (
                    300
                    if k == COL_NOME
                    else 64
                    if k == COL_CODIGO_GM
                    else 80
                    if k == COL_CODIGO_BARRAS
                    else 200
                    if k in (COL_CATEGORIA, COL_SUBCATEGORIA)
                    else 120
                )
                setattr(ov, fld, "")
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
            db[client.col_p].update_one(_mongo_filtro_id_produto_externo(pid), {"$set": mongo_set})


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
    qs = CadastroPlanilhaImportHistoricoAgro.objects.select_related("usuario", "revertido_por").order_by(
        "-criado_em"
    )[:limite]
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
    if hist.status != CadastroPlanilhaImportHistoricoAgro.Status.APLICADO:
        raise ValueError("Esta importação já foi desfeita.")

    items = (hist.backup or {}).get("items") or []
    if not items:
        raise ValueError("Backup vazio — não é possível desfazer.")

    from produtos.views import obter_conexao_mongo

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
    from produtos.views import _mongo_filtro_id_produto_externo

    ov, _ = ProdutoGestaoOverlayAgro.objects.get_or_create(
        produto_externo_id=pid[:64],
        defaults={"usuario": user if user and user.is_authenticated else None},
    )
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
    if COL_CODIGO_BARRAS in patch:
        ov.codigo_barras = str(patch[COL_CODIGO_BARRAS] or "")[:80]
    if COL_PRECO_VENDA in patch:
        ov.preco_venda = patch[COL_PRECO_VENDA]
    ov.save()

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
        db[client.col_p].update_one(_mongo_filtro_id_produto_externo(pid), {"$set": mongo_set})


def aplicar_importacao_cadastro(
    path: Path,
    user,
    *,
    nome_arquivo: str = "",
    on_progress: None | Any = None,
) -> dict[str, Any]:
    from produtos.views import obter_conexao_mongo

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

    candidatos: list[dict] = []
    vistos: set[str] = set()
    for i, raw in enumerate(rows_raw[:IMPORT_MAX_ROWS], start=2):
        pid = _cel_str(raw.get(hdr_id or ""))[:64]
        if not pid or not _id_produto_planilha_valido(pid) or pid in vistos:
            continue
        vistos.add(pid)
        patch = _patch_da_linha(raw, colmap)
        if patch.get("__erro_preco_custo") or patch.get("__erro_preco_venda"):
            continue
        if not any(k in patch for k in IMPORT_KEYS):
            continue
        candidatos.append({"linha": i, "id": pid, "patch": patch})

    if on_progress:
        on_progress(8, f"Conferindo {len(candidatos)} linha(s) com dados…")

    mapa = _mapa_estado_atual_produtos([c["id"] for c in candidatos], on_progress=on_progress)

    fila: list[dict] = []
    for item in candidatos:
        pid = item["id"]
        patch = item["patch"]
        i = item["linha"]
        atual = mapa.get(pid)
        if not atual or not _tem_alteracao(atual, patch):
            continue
        merged = _merged_row(atual, patch)
        vmsg = _validar_merged(merged)
        if vmsg:
            continue
        fila.append({"linha": i, "id": pid, "patch": patch, "atual": atual})

    if not fila:
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

    historico_id = None
    if backups:
        hist = CadastroPlanilhaImportHistoricoAgro.objects.create(
            usuario=user if user and user.is_authenticated else None,
            nome_arquivo=str(nome_arquivo or "")[:255],
            n_produtos=len(backups),
            n_campos=n_campos_total,
            backup={"items": backups},
        )
        historico_id = hist.pk

    if ok:
        _invalidar_cache_catalogo_pdv()

    if on_progress:
        on_progress(100, "Concluído")

    return {
        "gravados": ok,
        "falhas": falhas[:80],
        "n_falhas": len(falhas),
        "historico_id": historico_id,
        "n_alteracoes": len(fila),
    }
