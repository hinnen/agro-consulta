#!/usr/bin/env python
"""Smoke: FOTO-PDV — Gerais/Delivery compartilham foto → PDV. VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import base64
import os
import sys
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)

CHECKS: list[str] = []


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    for c in CHECKS:
        print(f"  ok até: {c}")
    sys.exit(1)


def ok(msg: str) -> None:
    CHECKS.append(msg)
    print(f"  OK {msg}")


def read(rel: str) -> str:
    path = os.path.join(ROOT, rel.replace("/", os.sep))
    if not os.path.isfile(path):
        fail(f"arquivo ausente: {rel}")
    return open(path, encoding="utf-8").read()


def check_modal_html() -> None:
    modal = read("produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html")

    if 'id="edit-img"' in modal:
        fail("campo URL edit-img ainda existe (deveria ter sido removido)")
    if "URL da imagem" in modal:
        fail("rótulo URL da imagem ainda no modal")
    ok("modal: sem campo URL legado")

    for needle in (
        'id="edit-produto-foto"',
        'id="edit-produto-foto-preview"',
        'id="edit-produto-foto-limpar"',
        'id="edit-delivery-foto"',
        'id="edit-delivery-imagem-b64"',
        'id="edit-delivery-imagem-mime"',
        'id="edit-delivery-foto-preview"',
        'id="edit-delivery-foto-limpar"',
    ):
        if needle not in modal:
            fail(f"modal sem {needle}")
    ok("modal: inputs Gerais + Delivery + hidden b64")

    for needle in (
        "function syncProdutoFotoUI",
        "function limparProdutoFotoCompartilhada",
        "bindProdutoFotoCompartilhada",
        "['edit-delivery-foto', 'edit-produto-foto']",
        "['edit-delivery-foto-preview', 'edit-produto-foto-preview']",
        "['edit-delivery-foto-limpar', 'edit-produto-foto-limpar']",
        "syncProdutoFotoUI()",
    ):
        if needle not in modal:
            fail(f"JS compartilhado ausente: {needle}")
    if modal.count("syncProdutoFotoUI()") < 3:
        fail("syncProdutoFotoUI deve ser chamado no load + bind + limpar (≥3)")
    ok("modal: sync compartilhado Gerais/Delivery")

    if "imagem_base64: gv('edit-delivery-imagem-b64')" not in modal:
        fail("salvar não manda delivery.imagem_base64")
    if "imagem_mime: gv('edit-delivery-imagem-mime')" not in modal:
        fail("salvar não manda delivery.imagem_mime")
    ok("modal: save grava foto no delivery")

    if "Mesma foto" not in modal and "mesma foto" not in modal.lower():
        fail("falta texto explicando foto compartilhada")
    ok("modal: copy Gerais/Delivery alinhada")


def check_backend_static() -> None:
    util = read("produtos/catalogo_delivery_util.py")
    views = read("produtos/views.py")
    agro = read("produtos/catalogo_agro.py")

    for fn in (
        "def aplicar_imagem_delivery_no_row",
        "def normalizar_delivery",
        "def data_url_imagem_delivery_de_overlay",
        "def _comprimir_imagem_base64_delivery",
    ):
        if fn not in util:
            fail(f"util sem {fn}")
    ok("util: funções de foto Delivery")

    if views.count("aplicar_imagem_delivery_no_row(") < 2:
        fail("views deve aplicar imagem Delivery em ≥2 builders de row")
    if 'd_del.get("imagem_base64")' not in views:
        fail("save overlay não persiste delivery só com foto")
    if "normalizar_delivery(payload.get(\"delivery\"), processar_imagem=True)" not in views:
        fail("save não comprime imagem no normalizar_delivery")
    ok("views: save + apply no row")

    if "_aplicar_produto_gestao_overlay_em_dict" not in agro:
        fail("catalogo_agro não aplica overlay (PDV PG)")
    if "row[\"imagem\"]" not in agro and '"imagem": ""' not in agro:
        fail("catalogo_agro sem campo imagem no row")
    ok("catalogo_agro: PDV passa pelo overlay (foto)")


def check_runtime() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()

    from produtos.catalogo_delivery_util import (
        aplicar_imagem_delivery_no_row,
        data_url_imagem_delivery_de_overlay,
        normalizar_delivery,
    )

    try:
        from PIL import Image
        import io
    except ImportError:
        fail("Pillow ausente — necessário para prova de foto")

    buf = io.BytesIO()
    Image.new("RGB", (32, 32), (34, 197, 94)).save(buf, format="JPEG", quality=85)
    raw = buf.getvalue()
    b64 = base64.b64encode(raw).decode("ascii")
    d = normalizar_delivery(
        {"ativo": False, "imagem_base64": b64, "imagem_mime": "image/jpeg"},
        processar_imagem=True,
    )
    if not d.get("imagem_base64"):
        fail("normalizar_delivery apagou foto no save")
    if d.get("ativo"):
        fail("foto sozinha não deve forçar delivery ativo")
    ok("runtime: normalizar guarda foto sem ativar catálogo")

    # Persistência: condição do save (espelho views)
    keep = bool(
        d.get("ativo")
        or any(
            (
                d.get("titulo"),
                d.get("descricao"),
                d.get("imagem_base64"),
                d.get("peso_texto"),
                d.get("permitir_estoque_negativo"),
                d.get("destaque"),
                int(d.get("ordem") or 0) > 0,
                int(d.get("categoria_id") or 0) > 0,
                int(d.get("subcategoria_id") or 0) > 0,
                int(d.get("subcategoria2_id") or 0) > 0,
                bool(d.get("embalagens")),
            )
        )
    )
    if not keep:
        fail("foto sozinha não manteria bloco delivery no overlay")
    ok("runtime: save manteria delivery só com foto")

    ov = SimpleNamespace(
        cadastro_extras={"delivery": {"imagem_base64": d["imagem_base64"], "imagem_mime": d["imagem_mime"]}}
    )
    url = data_url_imagem_delivery_de_overlay(ov)
    if not url.startswith("data:image/"):
        fail(f"data_url inválida: {url[:40]!r}")
    row: dict = {"id": "x", "imagem": ""}
    aplicar_imagem_delivery_no_row(row, ov)
    if row.get("imagem") != url:
        fail("aplicar_imagem_delivery_no_row não setou row['imagem']")
    ok("runtime: overlay → row.imagem (PDV)")

    # Leitura não descarta foto (processar_imagem=False)
    d2 = normalizar_delivery(
        {"imagem_base64": d["imagem_base64"], "imagem_mime": "image/jpeg"},
        processar_imagem=False,
    )
    if not d2.get("imagem_base64"):
        fail("leitura normalizar apagou foto")
    ok("runtime: leitura preserva foto")

    # Limpar: sem b64 → não sobrescreve imagem existente? aplicar só seta se url
    row2 = {"imagem": "https://exemplo.com/a.jpg"}
    ov_vazio = SimpleNamespace(cadastro_extras={})
    aplicar_imagem_delivery_no_row(row2, ov_vazio)
    if row2["imagem"] != "https://exemplo.com/a.jpg":
        fail("sem foto Delivery não deve apagar imagem existente")
    ok("runtime: sem foto Delivery mantém imagem prévia")


def main() -> None:
    print("verify_foto_pdv…")
    check_modal_html()
    check_backend_static()
    check_runtime()
    print(f"VERIFY_OK: {len(CHECKS)} checks")


if __name__ == "__main__":
    main()
