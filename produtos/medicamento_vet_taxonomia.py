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

# Campos denormalizados no espelho Mongo (só Agro; não enviar ao ERP legado).
AGRO_PALAVRAS_CHAVE_CAMPO = "AgroPalavrasChave"
AGRO_CLASSES_VET_CAMPO = "AgroClassesVet"
AGRO_BUSCA_TEXTO_EXTRA_CAMPO = "AgroBuscaTextoExtra"

_CHAVES_EXTRAS_PALAVRAS = ("palavras_chave", "palavrasChave")
_CHAVES_EXTRAS_CLASSES = ("classes_vet", "classesVet", "classes_medicamento")


@lru_cache(maxsize=1)
def carregar_taxonomia() -> dict[str, Any]:
    with _TAXONOMIA_PATH.open(encoding="utf-8") as f:
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


def ler_busca_de_cadastro_extras(ex: dict | None) -> tuple[str, list[str]]:
    if not isinstance(ex, dict):
        return "", []
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
    return pk, cv


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
    pk, cv = ler_busca_de_cadastro_extras(ex)
    return montar_texto_busca_denormalizado(pk, cv)


def aplicar_palavras_e_classes_em_extras(
    ex: dict,
    *,
    palavras_chave: str | None = None,
    classes_vet: Any | None = None,
    payload_tem_palavras: bool = False,
    payload_tem_classes: bool = False,
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


def documento_mongo_set_busca_agro(palavras_chave: str, classes_vet: list[str]) -> dict[str, Any]:
    pk = normalizar_palavras_chave(palavras_chave)
    extra = montar_texto_busca_denormalizado(pk, classes_vet)
    doc: dict[str, Any] = {
        AGRO_PALAVRAS_CHAVE_CAMPO: pk,
        AGRO_CLASSES_VET_CAMPO: list(classes_vet),
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
    pk, cv = ler_busca_de_cadastro_extras(ex)
    set_doc = documento_mongo_set_busca_agro(pk, cv)
    db[col_nome].update_one(_mongo_filtro_id_produto_externo(pid), {"$set": set_doc})


def anexar_texto_busca_extras(busca_texto: str, ex: dict | None) -> str:
    extra = texto_busca_extra_de_extras(ex)
    if not extra:
        return (busca_texto or "").strip()
    base = (busca_texto or "").strip()
    # Mantém também tokens não normalizados das palavras-chave (nomes comerciais).
    pk, _ = ler_busca_de_cadastro_extras(ex)
    sufixo = " ".join(x for x in (pk, extra) if x).strip()
    if not sufixo:
        return base
    if not base:
        return sufixo
    if sufixo.lower() in base.lower():
        return base
    return f"{base} {sufixo}".strip()


def taxonomia_para_api() -> dict[str, Any]:
    data = carregar_taxonomia()
    return {
        "versao": data.get("versao", 1),
        "grupos": data.get("grupos") or [],
    }
