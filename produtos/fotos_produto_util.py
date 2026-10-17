"""Galeria de fotos do produto (1 principal + até 3 extras) no overlay Delivery."""

from __future__ import annotations

import base64
import hashlib
from typing import Any

from django.urls import reverse

from produtos.catalogo_delivery_util import (
    _comprimir_imagem_base64_delivery,
    _strip_data_url,
    delivery_de_extras,
    normalizar_imagens_extras_delivery,
)
from produtos.models import ProdutoGestaoOverlayAgro

MAX_EXTRAS = 3
MAX_SLOTS = 4  # 0=principal, 1..3=extras


def _pid(raw: Any) -> str:
    return str(raw or "").strip()[:64]


def normalizar_imagens_extras(raw: Any, *, processar: bool = False) -> list[dict]:
    return normalizar_imagens_extras_delivery(raw, processar=processar)


def delivery_com_extras(d: dict, *, processar_extras: bool = False) -> dict:
    """Garante ``imagens_extras`` no dict delivery já normalizado."""
    if not isinstance(d, dict):
        d = {}
    extras_raw = d.get("imagens_extras")
    d = dict(d)
    d["imagens_extras"] = normalizar_imagens_extras(
        extras_raw, processar=processar_extras
    )
    return d


def slot_tem_foto(d: dict, indice: int) -> bool:
    i = int(indice or 0)
    if i < 0 or i >= MAX_SLOTS:
        return False
    if i == 0:
        return bool(str(d.get("imagem_base64") or "").strip())
    extras = d.get("imagens_extras") if isinstance(d.get("imagens_extras"), list) else []
    if i - 1 >= len(extras):
        return False
    item = extras[i - 1]
    if not isinstance(item, dict):
        return False
    return bool(str(item.get("imagem_base64") or "").strip())


def bytes_e_mime_do_slot(d: dict, indice: int) -> tuple[bytes, str] | tuple[None, None]:
    i = int(indice or 0)
    if i < 0 or i >= MAX_SLOTS:
        return None, None
    if i == 0:
        b64 = str(d.get("imagem_base64") or "").strip()
        mime = str(d.get("imagem_mime") or "image/jpeg").strip() or "image/jpeg"
    else:
        extras = normalizar_imagens_extras(d.get("imagens_extras"), processar=False)
        item = extras[i - 1]
        b64 = str(item.get("imagem_base64") or "").strip()
        mime = str(item.get("imagem_mime") or "image/jpeg").strip() or "image/jpeg"
    if not b64:
        return None, None
    try:
        raw = base64.b64decode(b64, validate=False)
    except Exception:
        return None, None
    if not raw:
        return None, None
    if mime == "image/jpg":
        mime = "image/jpeg"
    return raw, mime


def versao_galeria(d: dict, atualizado_em: Any = None) -> str:
    """Token curto p/ cache-bust (não vaza o base64)."""
    parts: list[str] = []
    if atualizado_em is not None:
        try:
            parts.append(str(int(atualizado_em.timestamp())))
        except Exception:
            parts.append(str(atualizado_em)[:20])
    for i in range(MAX_SLOTS):
        if i == 0:
            b64 = str(d.get("imagem_base64") or "")[:48]
        else:
            extras = d.get("imagens_extras") if isinstance(d.get("imagens_extras"), list) else []
            item = extras[i - 1] if i - 1 < len(extras) else {}
            b64 = str((item or {}).get("imagem_base64") or "")[:48] if isinstance(item, dict) else ""
        parts.append("1" if b64 else "0")
        if b64:
            parts.append(hashlib.md5(b64.encode("ascii", errors="ignore")).hexdigest()[:6])
    return hashlib.md5("|".join(parts).encode()).hexdigest()[:10]


def url_foto_produto(produto_id: str, indice: int = 0, *, v: str = "") -> str:
    pid = _pid(produto_id)
    if not pid:
        return ""
    base = reverse("produto_foto_bytes", kwargs={"produto_id": pid})
    q = f"?i={max(0, min(MAX_SLOTS - 1, int(indice or 0)))}"
    if v:
        q += f"&v={str(v)[:16]}"
    return base + q


def urls_slots_presentes(produto_id: str, d: dict, *, v: str = "") -> list[dict]:
    """Metadados leves dos slots (sem base64)."""
    out: list[dict] = []
    for i in range(MAX_SLOTS):
        tem = slot_tem_foto(d, i)
        out.append(
            {
                "indice": i,
                "principal": i == 0,
                "tem_foto": tem,
                "url": url_foto_produto(produto_id, i, v=v) if tem else "",
                "rotulo": "Principal" if i == 0 else f"Extra {i}",
            }
        )
    return out


def gravar_slot_foto(
    ov: ProdutoGestaoOverlayAgro,
    *,
    indice: int,
    imagem_base64: str,
    imagem_mime: str = "image/jpeg",
) -> tuple[bool, str]:
    """Grava um slot (0–3). Comprime. Retorno (ok, erro)."""
    i = int(indice)
    if i < 0 or i >= MAX_SLOTS:
        return False, "Slot inválido (0 a 3)."
    b64, mime_guess = _strip_data_url(str(imagem_base64 or "").strip())
    mime = str(imagem_mime or mime_guess or "image/jpeg").strip()[:40] or "image/jpeg"
    if not b64:
        return False, "Foto vazia."
    b64, mime = _comprimir_imagem_base64_delivery(b64, mime)
    if not b64:
        return False, "Foto muito grande ou inválida."

    ex = dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
    d = delivery_de_extras(ex)
    d = delivery_com_extras(d, processar_extras=False)

    if i == 0:
        d["imagem_base64"] = b64
        d["imagem_mime"] = mime
    else:
        extras = list(d.get("imagens_extras") or [])
        while len(extras) < MAX_EXTRAS:
            extras.append({"imagem_base64": "", "imagem_mime": "image/jpeg"})
        extras[i - 1] = {"imagem_base64": b64, "imagem_mime": mime}
        d["imagens_extras"] = extras

    ex["delivery"] = d
    ov.cadastro_extras = ex
    ov.save(update_fields=["cadastro_extras", "atualizado_em"])
    return True, ""


def apagar_slot_foto(ov: ProdutoGestaoOverlayAgro, *, indice: int) -> tuple[bool, str]:
    i = int(indice)
    if i < 0 or i >= MAX_SLOTS:
        return False, "Slot inválido (0 a 3)."
    ex = dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
    d = delivery_de_extras(ex)
    d = delivery_com_extras(d, processar_extras=False)
    if i == 0:
        d["imagem_base64"] = ""
        d["imagem_mime"] = "image/jpeg"
    else:
        extras = list(d.get("imagens_extras") or [])
        while len(extras) < MAX_EXTRAS:
            extras.append({"imagem_base64": "", "imagem_mime": "image/jpeg"})
        extras[i - 1] = {"imagem_base64": "", "imagem_mime": "image/jpeg"}
        d["imagens_extras"] = extras
    # Se delivery ficou “vazio” de negócio, ainda mantém o dict (galeria pode ter extras).
    ex["delivery"] = d
    ov.cadastro_extras = ex
    ov.save(update_fields=["cadastro_extras", "atualizado_em"])
    return True, ""


def overlay_por_pid(produto_id: str) -> ProdutoGestaoOverlayAgro | None:
    pid = _pid(produto_id)
    if not pid:
        return None
    return ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()


def overlay_get_or_create(produto_id: str) -> ProdutoGestaoOverlayAgro | None:
    pid = _pid(produto_id)
    if not pid:
        return None
    ov, _ = ProdutoGestaoOverlayAgro.objects.get_or_create(
        produto_externo_id=pid,
        defaults={},
    )
    return ov


def contagem_fotos(d: dict) -> int:
    return sum(1 for i in range(MAX_SLOTS) if slot_tem_foto(d, i))
