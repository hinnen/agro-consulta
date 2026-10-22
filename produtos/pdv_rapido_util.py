"""Slots de produtos rápidos do PDV (9 atalhos · Postgres · todos os PCs)."""
from __future__ import annotations

import base64
from typing import Any


CHAVE_DEFAULT = "default"
SLOTS = (1, 2, 3, 4, 5, 6, 7, 8, 9)
APELIDO_MAX = 32
PRODUTO_ID_MAX = 64


def slot_int(raw: Any) -> int | None:
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    if n not in SLOTS:
        return None
    return n


def _strip_data_url(raw: str) -> tuple[str, str]:
    t = str(raw or "").strip().replace("\n", "").replace(" ", "")
    mime = "image/jpeg"
    if t.startswith("data:") and "," in t:
        head, t = t.split(",", 1)
        if "image/png" in head:
            mime = "image/png"
        elif "image/webp" in head:
            mime = "image/webp"
        elif "image/jpg" in head or "image/jpeg" in head:
            mime = "image/jpeg"
    return t, mime


def comprimir_foto_slot(raw_b64: str) -> tuple[bytes, str]:
    from produtos.catalogo_delivery_util import comprimir_imagem_upload

    b64, mime = _strip_data_url(raw_b64)
    if not b64:
        return b"", "image/jpeg"
    try:
        raw = base64.b64decode(b64, validate=False)
    except Exception:
        return b"", "image/jpeg"
    if not raw:
        return b"", "image/jpeg"
    out, mime_out = comprimir_imagem_upload(raw, max_lado=720, qualidade=74)
    if not out:
        return b"", "image/jpeg"
    if len(out) > 450_000:
        out2, mime2 = comprimir_imagem_upload(raw, max_lado=480, qualidade=68)
        if out2:
            return out2, str(mime2 or "image/jpeg")
    return out, str(mime_out or mime or "image/jpeg")


def normalizar_apelido(raw: Any) -> str:
    return " ".join(str(raw or "").split())[:APELIDO_MAX]


def normalizar_produto_id(raw: Any) -> str:
    return str(raw or "").strip()[:PRODUTO_ID_MAX]


def _preco_e_cadastro(pid: str) -> tuple[float, str, str]:
    from produtos.catalogo_agro import obter_produto_model, produto_agro_para_row
    from produtos.models import ProdutoGestaoOverlayAgro

    if not pid:
        return 0.0, "", ""
    p = obter_produto_model(pid)
    if p is None:
        return 0.0, "", ""
    row = produto_agro_para_row(p)
    cid = str(row.get("id") or pid).strip()[:PRODUTO_ID_MAX]
    nome = str(row.get("nome") or "").strip()
    preco = float(row.get("preco_venda") or 0)
    ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=cid).first()
    if ov is None and cid != pid:
        ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()
    if ov is not None and ov.preco_venda is not None:
        try:
            preco = float(ov.preco_venda)
        except (TypeError, ValueError):
            pass
        if str(ov.nome or "").strip():
            nome = str(ov.nome).strip()
    return round(preco, 2), nome, cid


def _row_vazio(n: int) -> dict[str, Any]:
    return {
        "slot": n,
        "produto_id": "",
        "apelido": "",
        "preco_venda": 0,
        "produto_nome": "",
        "tem_foto": False,
        "foto_url": "",
        "preenchido": False,
    }


def _row_de_model(row: PdvRapidoSlotAgro) -> dict[str, Any]:
    pid = normalizar_produto_id(row.produto_id)
    preco, nome_cad, cid = _preco_e_cadastro(pid)
    if cid:
        pid = cid
    from django.urls import reverse

    tem_foto = bool(row.foto)
    foto_url = ""
    if tem_foto:
        foto_url = reverse("api_pdv_rapido_foto", args=[row.slot]) + f"?v={int(row.foto_versao or 0)}"
    apelido = normalizar_apelido(row.apelido)
    preenchido = bool(pid)
    return {
        "slot": int(row.slot),
        "produto_id": pid if preenchido else "",
        "apelido": apelido,
        "preco_venda": preco if preenchido else 0,
        "produto_nome": nome_cad if preenchido else "",
        "tem_foto": tem_foto,
        "foto_url": foto_url,
        "preenchido": preenchido,
    }


def listar_slots(*, chave: str = CHAVE_DEFAULT) -> list[dict[str, Any]]:
    from produtos.models import PdvRapidoSlotAgro

    chave = str(chave or CHAVE_DEFAULT).strip()[:32] or CHAVE_DEFAULT
    by_n = {
        int(r.slot): r
        for r in PdvRapidoSlotAgro.objects.filter(chave=chave, slot__in=SLOTS)
    }
    return [_row_de_model(by_n[n]) if n in by_n else _row_vazio(n) for n in SLOTS]


def limpar_slot(*, slot: int, chave: str = CHAVE_DEFAULT, usuario=None) -> dict[str, Any]:
    from produtos.models import PdvRapidoSlotAgro

    n = slot_int(slot)
    if n is None:
        raise ValueError("Slot inválido.")
    chave = str(chave or CHAVE_DEFAULT).strip()[:32] or CHAVE_DEFAULT
    row, _ = PdvRapidoSlotAgro.objects.get_or_create(chave=chave, slot=n)
    row.produto_id = ""
    row.apelido = ""
    row.foto = None
    row.foto_mime = ""
    row.foto_versao = int(row.foto_versao or 0) + 1
    if usuario is not None and getattr(usuario, "is_authenticated", False):
        row.atualizado_por = usuario
    row.save()
    return _row_de_model(row)


def salvar_slot(
    *,
    slot: int,
    produto_id: str,
    apelido: str,
    foto_b64: str | None = None,
    limpar_foto: bool = False,
    chave: str = CHAVE_DEFAULT,
    usuario=None,
) -> dict[str, Any]:
    from produtos.models import PdvRapidoSlotAgro

    n = slot_int(slot)
    if n is None:
        raise ValueError("Slot inválido.")
    pid = normalizar_produto_id(produto_id)
    if not pid:
        raise ValueError("Escolha o produto do cadastro para este slot.")
    preco, nome_cad, cid = _preco_e_cadastro(pid)
    if not cid:
        raise ValueError("Produto não encontrado no cadastro.")
    apelido_n = normalizar_apelido(apelido) or normalizar_apelido(nome_cad)[:APELIDO_MAX]
    if not apelido_n:
        raise ValueError("Escreva um nome curto para o slot.")
    chave = str(chave or CHAVE_DEFAULT).strip()[:32] or CHAVE_DEFAULT
    row, _ = PdvRapidoSlotAgro.objects.get_or_create(chave=chave, slot=n)
    row.produto_id = cid
    row.apelido = apelido_n
    if limpar_foto:
        row.foto = None
        row.foto_mime = ""
        row.foto_versao = int(row.foto_versao or 0) + 1
    elif foto_b64:
        blob, mime = comprimir_foto_slot(foto_b64)
        if not blob:
            raise ValueError("Não foi possível ler a foto. Tente JPEG ou PNG.")
        row.foto = blob
        row.foto_mime = mime or "image/jpeg"
        row.foto_versao = int(row.foto_versao or 0) + 1
    if usuario is not None and getattr(usuario, "is_authenticated", False):
        row.atualizado_por = usuario
    row.save()
    out = _row_de_model(row)
    if not out["preco_venda"] and preco:
        out["preco_venda"] = preco
    return out


def foto_do_slot(*, slot: int, chave: str = CHAVE_DEFAULT) -> tuple[bytes, str] | tuple[None, None]:
    from produtos.models import PdvRapidoSlotAgro

    n = slot_int(slot)
    if n is None:
        return None, None
    chave = str(chave or CHAVE_DEFAULT).strip()[:32] or CHAVE_DEFAULT
    row = PdvRapidoSlotAgro.objects.filter(chave=chave, slot=n).first()
    if row is None or not row.foto:
        return None, None
    mime = str(row.foto_mime or "image/jpeg").strip() or "image/jpeg"
    if mime == "image/jpg":
        mime = "image/jpeg"
    return bytes(row.foto), mime
