"""
Taxonomia fixa de classes terapêuticas (medicamentos veterinários) + palavras-chave de busca no Agro.

Fonte da lista: ``produtos/data/taxonomia_medicamento_veterinario.json`` (editável pela loja).
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from integracoes.texto import normalizar

_TAXONOMIA_PATH = Path(__file__).resolve().parent / "data" / "taxonomia_medicamento_veterinario.json"
_CLASSIFICACAO_PATH = (
    Path(__file__).resolve().parent / "data" / "classificacao_medicamento_vet.json"
)

# Campos denormalizados no espelho Mongo (só Agro; não enviar ao ERP legado).
AGRO_PALAVRAS_CHAVE_CAMPO = "AgroPalavrasChave"
AGRO_CLASSES_VET_CAMPO = "AgroClassesVet"
AGRO_ESPECIES_VET_CAMPO = "AgroEspeciesVet"
AGRO_BUSCA_TEXTO_EXTRA_CAMPO = "AgroBuscaTextoExtra"

_CHAVES_EXTRAS_PALAVRAS = ("palavras_chave", "palavrasChave")
_CHAVES_EXTRAS_CLASSES = ("classes_vet", "classesVet", "classes_medicamento")
_CHAVES_EXTRAS_ESPECIES = ("especies_vet", "especiesVet", "especies_atendidas")


@lru_cache(maxsize=1)
def carregar_taxonomia() -> dict[str, Any]:
    with _TAXONOMIA_PATH.open(encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=1)
def carregar_classificacao() -> dict[str, Any]:
    with _CLASSIFICACAO_PATH.open(encoding="utf-8") as f:
        return json.load(f)


def mapa_slug_para_label() -> dict[str, str]:
    out: dict[str, str] = {}
    data = carregar_taxonomia()
    for g in data.get("grupos") or []:
        if not isinstance(g, dict):
            continue
        for it in g.get("itens") or []:
            if not isinstance(it, dict):
                continue
            sid = str(it.get("id") or "").strip()
            lab = str(it.get("label") or "").strip()
            if sid and lab:
                out[sid] = lab
    return out


def slugs_validos() -> set[str]:
    return set(mapa_slug_para_label().keys())


def mapa_especies_slug_para_label() -> dict[str, str]:
    out: dict[str, str] = {}
    data = carregar_classificacao()
    for it in data.get("especies") or []:
        if not isinstance(it, dict):
            continue
        sid = str(it.get("id") or "").strip()
        lab = str(it.get("label") or "").strip()
        if sid and lab:
            out[sid] = lab
    return out


def especies_slugs_validos() -> set[str]:
    return set(mapa_especies_slug_para_label().keys())


def grupos_prateleira_para_api() -> list[dict[str, str]]:
    data = carregar_classificacao()
    out: list[dict[str, str]] = []
    for it in data.get("grupos_prateleira") or []:
        if not isinstance(it, dict):
            continue
        sid = str(it.get("id") or "").strip()
        lab = str(it.get("label") or "").strip()
        if sid and lab:
            out.append({"id": sid, "label": lab})
    return out


def mapa_grupo_prateleira_label_por_id() -> dict[str, str]:
    return {g["id"]: g["label"] for g in grupos_prateleira_para_api() if g.get("id")}


def mapa_grupo_prateleira_id_por_label() -> dict[str, str]:
    out: dict[str, str] = {}
    for g in grupos_prateleira_para_api():
        lab = str(g.get("label") or "").strip()
        sid = str(g.get("id") or "").strip()
        if lab and sid:
            out[normalizar(lab)] = sid
            out[lab.casefold()] = sid
    return out


def label_grupo_prateleira_por_subcategoria_texto(subcategoria: str | None) -> str | None:
    """Resolve rótulo canônico da prateleira a partir do texto salvo (overlay/subcategoria)."""
    s = str(subcategoria or "").strip()
    if not s:
        return None
    labels = {g["label"] for g in grupos_prateleira_para_api()}
    for lab in labels:
        if lab == s or lab.casefold() == s.casefold():
            return lab
    sid_map = mapa_grupo_prateleira_id_por_label()
    sid = sid_map.get(normalizar(s)) or sid_map.get(s.casefold())
    if sid:
        return mapa_grupo_prateleira_label_por_id().get(sid)
    return None


def categoria_eh_medicamento(categoria: str | None) -> bool:
    c = normalizar(str(categoria or ""))
    if not c:
        return False
    if "medicamento" in c:
        return True
    if "veneno" in c and "medic" in c:
        return True
    return "medicamentos e venenos" in c or c.startswith("medicamentos ")


def normalizar_palavras_chave(raw: str | None) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    # Vírgula, ponto-e-vírgula e quebra de linha viram espaço; colapsa espaços.
    s = re.sub(r"[,;\n\r]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:8000]


def normalizar_especies_vet(raw: Any) -> list[str]:
    valid = especies_slugs_validos()
    seen: set[str] = set()
    out: list[str] = []
    if raw is None:
        return out
    partes: list[str]
    if isinstance(raw, list):
        partes = [str(x).strip() for x in raw]
    else:
        partes = re.split(r"[,;\s]+", str(raw or ""))
    for p in partes:
        sid = p.strip().lower().replace(" ", "_")
        if sid in valid and sid not in seen:
            seen.add(sid)
            out.append(sid)
    return out[:16]


def normalizar_classes_vet(raw: Any) -> list[str]:
    valid = slugs_validos()
    seen: set[str] = set()
    out: list[str] = []
    if raw is None:
        return out
    partes: list[str]
    if isinstance(raw, list):
        partes = [str(x).strip() for x in raw]
    else:
        partes = re.split(r"[,;\s]+", str(raw or ""))
    for p in partes:
        sid = p.strip().lower().replace(" ", "_")
        if sid in valid and sid not in seen:
            seen.add(sid)
            out.append(sid)
    return out[:32]


def ler_busca_de_cadastro_extras(ex: dict | None) -> tuple[str, list[str], list[str]]:
    if not isinstance(ex, dict):
        return "", [], []
    pk = ""
    for k in _CHAVES_EXTRAS_PALAVRAS:
        if k in ex:
            pk = normalizar_palavras_chave(ex.get(k))
            break
    cv: list[str] = []
    for k in _CHAVES_EXTRAS_CLASSES:
        if k in ex:
            cv = normalizar_classes_vet(ex.get(k))
            break
    ev: list[str] = []
    for k in _CHAVES_EXTRAS_ESPECIES:
        if k in ex:
            ev = normalizar_especies_vet(ex.get(k))
            break
    return pk, cv, ev


def montar_texto_busca_denormalizado(palavras_chave: str, classes_vet: list[str]) -> str:
    """Texto plano (rótulos + palavras) para índice de busca — normalizado."""
    partes: list[str] = []
    pk = normalizar_palavras_chave(palavras_chave)
    if pk:
        partes.append(pk)
    labels = mapa_slug_para_label()
    for sid in classes_vet or []:
        lab = labels.get(sid)
        if lab:
            partes.append(lab)
            partes.append(normalizar(lab))
    joined = " ".join(partes)
    return normalizar(joined) if joined else ""


def texto_busca_extra_de_extras(ex: dict | None) -> str:
    pk, cv, _ev = ler_busca_de_cadastro_extras(ex)
    return montar_texto_busca_denormalizado(pk, cv)


def aplicar_palavras_e_classes_em_extras(
    ex: dict,
    *,
    palavras_chave: str | None = None,
    classes_vet: Any | None = None,
    especies_vet: Any | None = None,
    payload_tem_palavras: bool = False,
    payload_tem_classes: bool = False,
    payload_tem_especies: bool = False,
) -> None:
    if payload_tem_palavras:
        pk = normalizar_palavras_chave(palavras_chave)
        if pk:
            ex["palavras_chave"] = pk
        else:
            ex.pop("palavras_chave", None)
    if payload_tem_classes:
        cv = normalizar_classes_vet(classes_vet)
        if cv:
            ex["classes_vet"] = cv
        else:
            ex.pop("classes_vet", None)
    if payload_tem_especies:
        ev = normalizar_especies_vet(especies_vet)
        if ev:
            ex["especies_vet"] = ev
        else:
            ex.pop("especies_vet", None)


def documento_mongo_set_busca_agro(
    palavras_chave: str,
    classes_vet: list[str],
    especies_vet: list[str] | None = None,
) -> dict[str, Any]:
    pk = normalizar_palavras_chave(palavras_chave)
    extra = montar_texto_busca_denormalizado(pk, classes_vet)
    ev = list(especies_vet or [])
    doc: dict[str, Any] = {
        AGRO_PALAVRAS_CHAVE_CAMPO: pk,
        AGRO_CLASSES_VET_CAMPO: list(classes_vet),
        AGRO_ESPECIES_VET_CAMPO: ev,
        AGRO_BUSCA_TEXTO_EXTRA_CAMPO: extra,
    }
    return doc


def _mongo_filtro_id_produto_externo(pid_str: str) -> dict:
    from bson import ObjectId

    pid_str = str(pid_str or "").strip()
    ors: list[dict] = [{"Id": pid_str}]
    try:
        ors.append({"_id": ObjectId(pid_str)})
    except Exception:
        pass
    return {"$or": ors}


def sincronizar_busca_agro_no_mongo(db, col_nome: str, pid: str, ex: dict | None) -> None:
    if db is None or not pid:
        return
    pk, cv, ev = ler_busca_de_cadastro_extras(ex)
    set_doc = documento_mongo_set_busca_agro(pk, cv, ev)
    db[col_nome].update_one(_mongo_filtro_id_produto_externo(pid), {"$set": set_doc})


def anexar_texto_busca_extras(busca_texto: str, ex: dict | None) -> str:
    extra = texto_busca_extra_de_extras(ex)
    if not extra:
        return (busca_texto or "").strip()
    base = (busca_texto or "").strip()
    # Mantém também tokens não normalizados das palavras-chave (nomes comerciais).
    pk, _, _ = ler_busca_de_cadastro_extras(ex)
    sufixo = " ".join(x for x in (pk, extra) if x).strip()
    if not sufixo:
        return base
    if not base:
        return sufixo
    if sufixo.lower() in base.lower():
        return base
    return f"{base} {sufixo}".strip()


def anexar_campos_busca_em_row_cadastro(
    row: dict,
    ex: dict | None,
    *,
    descricao_overlay: str | None = None,
) -> None:
    """Expõe busca do overlay no JSON do modal (agro_pg + Mongo)."""
    pk, cv, ev = ler_busca_de_cadastro_extras(ex)
    if not pk and str(descricao_overlay or "").strip():
        pk = str(descricao_overlay).strip()
    row["palavras_chave"] = pk
    row["classes_vet"] = cv
    row["especies_vet"] = ev


def taxonomia_para_api() -> dict[str, Any]:
    data = carregar_taxonomia()
    clf = carregar_classificacao()
    return {
        "versao": data.get("versao", 1),
        "grupos": data.get("grupos") or [],
        "grupos_prateleira": grupos_prateleira_para_api(),
        "especies": [
            {"id": str(it.get("id") or "").strip(), "label": str(it.get("label") or "").strip()}
            for it in (clf.get("especies") or [])
            if isinstance(it, dict)
            and str(it.get("id") or "").strip()
            and str(it.get("label") or "").strip()
        ],
        "classificacao_versao": clf.get("versao", 1),
    }
