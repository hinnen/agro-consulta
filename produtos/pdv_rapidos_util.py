"""Slots rápidos do PDV — 9 produtos, apelido e foto manuais (Postgres · todos os PCs)."""
from __future__ import annotations

import io
from typing import Any

from PIL import Image, UnidentifiedImageError
from django.http import HttpResponse

from produtos.models import PdvRapidoSlotAgro

SLOTS = 9
NOME_MAX = 32
FOTO_MAX_BYTES = 6_000_000
FOTO_MAX_LADO = (720, 1100)

_TIPOS_OK = {"image/jpeg", "image/png", "image/webp", "image/jpg"}


def _vazio(posicao: int) -> dict[str, Any]:
    return {
        "posicao": posicao,
        "produto_id": "",
        "codigo": "",
        "nome": "",
        "tem_foto": False,
        "foto_v": 0,
    }


def _row_payload(row: PdvRapidoSlotAgro) -> dict[str, Any]:
    tem = bool(row.foto)
    foto_v = 0
    if tem and row.atualizado_em:
        foto_v = int(row.atualizado_em.timestamp())
    return {
        "posicao": int(row.posicao),
        "produto_id": (row.produto_id or "").strip(),
        "codigo": (row.codigo or "").strip(),
        "nome": (row.nome or "").strip(),
        "tem_foto": tem,
        "foto_v": foto_v,
    }


def listar_slots() -> list[dict[str, Any]]:
    rows = {int(r.posicao): r for r in PdvRapidoSlotAgro.objects.all()}
    out: list[dict[str, Any]] = []
    for n in range(1, SLOTS + 1):
        row = rows.get(n)
        out.append(_row_payload(row) if row else _vazio(n))
    return out


def _limpar_texto(valor: Any, limite: int) -> str:
    return str(valor or "").strip()[:limite]


def salvar_slots(itens: Any, usuario=None) -> tuple[bool, str]:
    if not isinstance(itens, list):
        return False, "Lista de slots inválida."
    vistos: set[int] = set()
    limpos: list[dict[str, Any]] = []
    for item in itens:
        if not isinstance(item, dict):
            return False, "Slot inválido."
        try:
            pos = int(item.get("posicao"))
        except (TypeError, ValueError):
            return False, "Posição inválida."
        if pos < 1 or pos > SLOTS or pos in vistos:
            return False, "Posição inválida."
        vistos.add(pos)
        limpar = bool(item.get("limpar"))
        produto_id = "" if limpar else _limpar_texto(item.get("produto_id"), 64)
        codigo = "" if limpar else _limpar_texto(item.get("codigo"), 40)
        nome = "" if limpar else _limpar_texto(item.get("nome"), NOME_MAX)
        if nome and not produto_id:
            return False, f"Slot {pos}: escolha o produto do cadastro."
        if produto_id and not nome:
            return False, f"Slot {pos}: escreva o apelido."
        limpos.append(
            {
                "posicao": pos,
                "produto_id": produto_id,
                "codigo": codigo,
                "nome": nome,
                "limpar": limpar or not produto_id,
            }
        )
    for item in limpos:
        row, _ = PdvRapidoSlotAgro.objects.get_or_create(posicao=item["posicao"])
        row.produto_id = item["produto_id"]
        row.codigo = item["codigo"]
        row.nome = item["nome"]
        update = ["produto_id", "codigo", "nome", "atualizado_em"]
        if item["limpar"]:
            row.foto = None
            row.foto_tipo = ""
            update.extend(["foto", "foto_tipo"])
        if usuario is not None and getattr(usuario, "is_authenticated", False):
            row.atualizado_por = usuario
            update.append("atualizado_por")
        row.save(update_fields=update)
    return True, ""


def preparar_foto(conteudo: bytes, content_type: str = "") -> tuple[bytes | None, str]:
    if not conteudo:
        return None, "Envie uma foto."
    if len(conteudo) > FOTO_MAX_BYTES:
        return None, "Foto grande demais (máximo 6 MB)."
    tipo = (content_type or "").split(";")[0].strip().lower()
    if tipo and tipo not in _TIPOS_OK:
        return None, "Use foto JPG, PNG ou WebP."
    try:
        im = Image.open(io.BytesIO(conteudo))
        im.load()
    except (UnidentifiedImageError, OSError, ValueError):
        return None, "Não foi possível ler essa foto."
    if im.mode not in ("RGB", "L"):
        im = im.convert("RGB")
    elif im.mode == "L":
        im = im.convert("RGB")
    im.thumbnail(FOTO_MAX_LADO, Image.Resampling.LANCZOS)
    out = io.BytesIO()
    im.save(out, format="JPEG", quality=82, optimize=True)
    return out.getvalue(), ""


def gravar_foto(posicao: int, conteudo: bytes, content_type: str = "", usuario=None) -> tuple[bool, str]:
    if posicao < 1 or posicao > SLOTS:
        return False, "Posição inválida."
    jpeg, err = preparar_foto(conteudo, content_type)
    if not jpeg:
        return False, err or "Foto inválida."
    row, _ = PdvRapidoSlotAgro.objects.get_or_create(posicao=posicao)
    row.foto = jpeg
    row.foto_tipo = "image/jpeg"
    update = ["foto", "foto_tipo", "atualizado_em"]
    if usuario is not None and getattr(usuario, "is_authenticated", False):
        row.atualizado_por = usuario
        update.append("atualizado_por")
    row.save(update_fields=update)
    return True, ""


def limpar_foto(posicao: int, usuario=None) -> tuple[bool, str]:
    if posicao < 1 or posicao > SLOTS:
        return False, "Posição inválida."
    row = PdvRapidoSlotAgro.objects.filter(posicao=posicao).first()
    if not row:
        return True, ""
    row.foto = None
    row.foto_tipo = ""
    update = ["foto", "foto_tipo", "atualizado_em"]
    if usuario is not None and getattr(usuario, "is_authenticated", False):
        row.atualizado_por = usuario
        update.append("atualizado_por")
    row.save(update_fields=update)
    return True, ""


def resposta_foto(posicao: int) -> HttpResponse:
    row = PdvRapidoSlotAgro.objects.filter(posicao=posicao).first()
    blob = bytes(row.foto) if row and row.foto else b""
    if not blob:
        return HttpResponse(status=404)
    resp = HttpResponse(blob, content_type=row.foto_tipo or "image/jpeg")
    resp["Cache-Control"] = "private, max-age=3600"
    return resp
