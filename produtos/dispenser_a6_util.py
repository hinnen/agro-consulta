"""Dispenser A6 — biblioteca compartilhada (Postgres)."""
from __future__ import annotations

from typing import Any

from produtos.models import DispenserDocumentoAgro, DispenserMidiaAgro

MAX_B64 = 1_700_000
MAX_THUMB_B64 = 400_000
MAX_POR_TIPO = {
    DispenserMidiaAgro.TIPO_LOGO: 24,
    DispenserMidiaAgro.TIPO_PET: 24,
    DispenserMidiaAgro.TIPO_ING: 24,
    DispenserMidiaAgro.TIPO_FLAVOR_ICO: 80,
}
MAX_DOCS = {
    DispenserDocumentoAgro.TIPO_FOLHA: 40,
    DispenserDocumentoAgro.TIPO_LAYOUT: 40,
    DispenserDocumentoAgro.TIPO_SABOR: 80,
}

TIPOS_MIDIA = {c[0] for c in DispenserMidiaAgro.TIPO_CHOICES}
TIPOS_DOC = {c[0] for c in DispenserDocumentoAgro.TIPO_CHOICES}


def strip_data_url(raw: str) -> tuple[str, str]:
    s = (raw or "").strip()
    mime = "image/png"
    if s.startswith("data:") and ";base64," in s:
        head, _, b64 = s.partition(";base64,")
        mime = head.replace("data:", "").strip() or mime
        return b64.strip(), mime
    return s, mime


def midia_to_dict(row: DispenserMidiaAgro) -> dict[str, Any]:
    return {
        "id": row.item_id,
        "label": row.label or row.item_id,
        "dataUrl": row.data_url(),
        "mime": row.mime or "image/png",
        "atualizado_em": row.atualizado_em.isoformat() if row.atualizado_em else "",
    }


def listar_biblioteca() -> dict[str, Any]:
    midias: dict[str, list] = {t: [] for t in TIPOS_MIDIA}
    for row in DispenserMidiaAgro.objects.all().order_by("tipo", "label", "item_id"):
        midias.setdefault(row.tipo, []).append(midia_to_dict(row))

    docs: dict[str, dict] = {t: {} for t in TIPOS_DOC}
    for row in DispenserDocumentoAgro.objects.all().order_by("tipo", "nome"):
        payload = row.payload if isinstance(row.payload, dict) else {}
        entry = dict(payload)
        if row.tipo == DispenserDocumentoAgro.TIPO_FOLHA:
            entry.setdefault("v", 2)
            entry.setdefault("kind", "folha")
            if row.thumb and not entry.get("thumb"):
                entry["thumb"] = row.thumb
            entry["savedAt"] = int(row.atualizado_em.timestamp() * 1000) if row.atualizado_em else 0
        else:
            entry.setdefault("v", 1)
            entry["savedAt"] = int(row.atualizado_em.timestamp() * 1000) if row.atualizado_em else 0
        docs.setdefault(row.tipo, {})[row.nome] = entry

    vazia = (
        not any(midias[t] for t in TIPOS_MIDIA)
        and not any(docs[t] for t in TIPOS_DOC)
    )
    return {
        "midias": midias,
        "documentos": docs,
        "vazia": vazia,
        "limites": {
            "midia": dict(MAX_POR_TIPO),
            "docs": dict(MAX_DOCS),
            "b64": MAX_B64,
        },
    }


def upsert_midia(
    *,
    tipo: str,
    item_id: str,
    label: str = "",
    data_url: str = "",
) -> tuple[DispenserMidiaAgro | None, str]:
    tipo = (tipo or "").strip()
    item_id = (item_id or "").strip()[:80]
    if tipo not in TIPOS_MIDIA:
        return None, "Tipo de mídia inválido."
    if not item_id:
        return None, "ID da mídia obrigatório."

    b64, mime = strip_data_url(data_url)
    if not b64:
        return None, "Imagem vazia."
    if len(b64) > MAX_B64:
        return None, "Foto grande demais (máx. ~1,2 MB)."

    existing = DispenserMidiaAgro.objects.filter(tipo=tipo, item_id=item_id).first()
    if not existing:
        qtd = DispenserMidiaAgro.objects.filter(tipo=tipo).count()
        teto = MAX_POR_TIPO.get(tipo, 24)
        if qtd >= teto:
            return None, f"Limite de {teto} itens deste tipo."

    row, _ = DispenserMidiaAgro.objects.update_or_create(
        tipo=tipo,
        item_id=item_id,
        defaults={
            "label": (label or item_id)[:120],
            "data_base64": b64,
            "mime": (mime or "image/png")[:40],
        },
    )
    return row, ""


def delete_midia(*, tipo: str, item_id: str) -> tuple[bool, str]:
    tipo = (tipo or "").strip()
    item_id = (item_id or "").strip()[:80]
    if tipo not in TIPOS_MIDIA or not item_id:
        return False, "Parâmetros inválidos."
    n, _ = DispenserMidiaAgro.objects.filter(tipo=tipo, item_id=item_id).delete()
    if not n:
        return False, "Item não encontrado."
    return True, ""


def upsert_documento(
    *,
    tipo: str,
    nome: str,
    payload: dict | None = None,
    thumb: str = "",
) -> tuple[DispenserDocumentoAgro | None, str]:
    tipo = (tipo or "").strip()
    nome = (nome or "").strip()[:80]
    if tipo not in TIPOS_DOC:
        return None, "Tipo de documento inválido."
    if not nome:
        return None, "Nome obrigatório."

    payload = payload if isinstance(payload, dict) else {}
    thumb_b64 = ""
    thumb_store = ""
    if thumb:
        thumb_b64, mime = strip_data_url(thumb)
        if len(thumb_b64) > MAX_THUMB_B64:
            thumb_b64 = ""
        elif thumb_b64:
            thumb_store = f"data:{(mime or 'image/jpeg')};base64,{thumb_b64}"

    existing = DispenserDocumentoAgro.objects.filter(tipo=tipo, nome=nome).first()
    if not existing:
        qtd = DispenserDocumentoAgro.objects.filter(tipo=tipo).count()
        teto = MAX_DOCS.get(tipo, 40)
        if qtd >= teto:
            return None, f"Limite de {teto} itens deste tipo."

    # Folha: thumb pode vir no payload; preferir campo dedicado
    if tipo == DispenserDocumentoAgro.TIPO_FOLHA:
        payload = dict(payload)
        if thumb_store:
            payload["thumb"] = thumb_store
        elif not thumb_store and payload.get("thumb"):
            t_b64, t_mime = strip_data_url(str(payload.get("thumb") or ""))
            if t_b64 and len(t_b64) <= MAX_THUMB_B64:
                thumb_store = f"data:{(t_mime or 'image/jpeg')};base64,{t_b64}"
            else:
                payload.pop("thumb", None)

    row, _ = DispenserDocumentoAgro.objects.update_or_create(
        tipo=tipo,
        nome=nome,
        defaults={
            "payload": payload,
            "thumb": thumb_store,
        },
    )
    return row, ""


def delete_documento(*, tipo: str, nome: str) -> tuple[bool, str]:
    tipo = (tipo or "").strip()
    nome = (nome or "").strip()[:80]
    if tipo not in TIPOS_DOC or not nome:
        return False, "Parâmetros inválidos."
    n, _ = DispenserDocumentoAgro.objects.filter(tipo=tipo, nome=nome).delete()
    if not n:
        return False, "Documento não encontrado."
    return True, ""


def migrar_lote(payload: dict) -> dict[str, Any]:
    """Importa mídias + documentos do PC (confirmação do usuário)."""
    midias_in = payload.get("midias") or {}
    docs_in = payload.get("documentos") or {}
    ok_m = 0
    err_m = 0
    ok_d = 0
    err_d = 0
    erros: list[str] = []

    for tipo, lista in (midias_in or {}).items():
        if not isinstance(lista, list):
            continue
        for it in lista:
            if not isinstance(it, dict):
                continue
            row, err = upsert_midia(
                tipo=tipo,
                item_id=str(it.get("id") or ""),
                label=str(it.get("label") or ""),
                data_url=str(it.get("dataUrl") or it.get("data_url") or ""),
            )
            if row:
                ok_m += 1
            else:
                err_m += 1
                if err and len(erros) < 8:
                    erros.append(f"{tipo}/{it.get('id')}: {err}")

    for tipo, mapa in (docs_in or {}).items():
        if not isinstance(mapa, dict):
            continue
        for nome, entry in mapa.items():
            if not isinstance(entry, dict):
                continue
            thumb = str(entry.get("thumb") or "")
            body = dict(entry)
            body.pop("thumb", None)
            row, err = upsert_documento(
                tipo=tipo,
                nome=str(nome),
                payload=body,
                thumb=thumb,
            )
            if row:
                ok_d += 1
            else:
                err_d += 1
                if err and len(erros) < 8:
                    erros.append(f"{tipo}/{nome}: {err}")

    return {
        "midias_ok": ok_m,
        "midias_erro": err_m,
        "docs_ok": ok_d,
        "docs_erro": err_d,
        "erros": erros,
        "biblioteca": listar_biblioteca(),
    }
