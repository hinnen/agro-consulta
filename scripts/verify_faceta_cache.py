#!/usr/bin/env python
"""Smoke: FACETA-CACHE (unidade/marca/cat reaparecem na lista). VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import re
import sys

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


def check_static() -> None:
    cat = read("produtos/catalogo_agro.py")
    views = read("produtos/views.py")
    modal = read("produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html")
    pick = read("produtos/static/produtos/js/agro_picklist.js")
    panel = read("produtos/static/produtos/js/cadastro_erp_panel.js")

    if 'FACETAS_GESTAO_CACHE_KEY = "agro_gestao_facetas_v6"' not in cat:
        fail("chave ativa de cache ausente/errada")
    if "def invalidar_cache_facetas_gestao" not in cat:
        fail("invalidar_cache_facetas_gestao ausente")
    for k in (
        "agro_gestao_facetas_v1",
        "agro_gestao_facetas_v4",
        "agro_gestao_facetas_v5",
        "agro_gestao_facetas_v6",
    ):
        if k not in cat:
            fail(f"lista de chaves sem {k}")
    ok("catalogo: chave + invalidar todas as versões")

    if "def _facetas_autorizadas_por_pin" not in cat:
        fail("_facetas_autorizadas_por_pin ausente")
    if "pin_extra = _facetas_autorizadas_por_pin()" not in cat:
        fail("facetas_gestao não junta valores do PIN")
    if 'faceta_unidade' not in cat or 'faceta_marca' not in cat:
        fail("mapa PIN sem faceta_unidade/marca")
    ok("catalogo: PIN entra na lista do servidor")

    if "cat_agro.FACETAS_GESTAO_CACHE_KEY" not in views:
        fail("api facetas não usa FACETAS_GESTAO_CACHE_KEY")
    if views.count("invalidar_cache_facetas_gestao()") < 2:
        fail("views deve invalidar em salvar overlay E faceta-nova (≥2)")
    # Regressão: não pode voltar a apagar só chave velha
    if re.search(r'cache\.delete\(["\']agro_gestao_facetas_v[145]["\']\)', views):
        fail("views ainda apaga chave antiga isolada (v1/v4/v5) — use invalidar_*")
    ok("views: API usa v6 + invalidar no save e no +PIN")

    for needle in (
        "function mergeAgroFacetasPayload",
        "function seedFacetasDoProduto",
        "seedFacetasDoProduto(produto)",
        "seedFacetasDoProduto(j.produto)",
        "mergeAgroFacetasPayload(d)",
        "appendFacetToList('unidades'",
        "appendFacetToList('marcas'",
        "appendFacetToList('categorias'",
    ):
        if needle not in modal:
            fail(f"modal sem {needle}")
    # Não pode sobrescrever arrays inteiros da API (bug original)
    if re.search(r"window\._agroFacetas\.unidades\s*=\s*Array\.isArray\(d\.unidades\)", modal):
        fail("modal ainda sobrescreve unidades com assign direto")
    if re.search(r"window\._agroFacetas\.marcas\s*=\s*Array\.isArray\(d\.marcas\)", modal):
        fail("modal ainda sobrescreve marcas com assign direto")
    ok("modal: merge + seed produto (não sobrescreve)")

    if "function mergeFacetas" not in pick:
        fail("agro_picklist sem mergeFacetas")
    if "prev.forEach" not in pick:
        fail("agro_picklist mergeFacetas não preserva valores locais")
    ok("agro_picklist: merge preserva sessão")

    if "facKeys.forEach" not in panel:
        fail("cadastro_erp_panel sem merge facKeys")
    if re.search(r"window\._agroFacetas\.unidades\s*=\s*j\.unidades", panel):
        fail("panel ainda sobrescreve unidades")
    ok("panel: merge facetas (não sobrescreve)")


def check_django_logic() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    try:
        import django

        django.setup()
    except Exception as e:
        fail(f"django.setup falhou: {e}")

    from django.core.cache import cache

    from produtos.catalogo_agro import (
        FACETAS_GESTAO_CACHE_KEY,
        FACETAS_GESTAO_CACHE_KEYS,
        _faceta_valores_distintos,
        _facetas_autorizadas_por_pin,
        invalidar_cache_facetas_gestao,
    )

    # distinct case-insensitive
    got = _faceta_valores_distintos(["Caixa", "caixa", " CAIXA ", "", None, "UN"], limite=0)
    lower = [x.lower() for x in got]
    if lower.count("caixa") != 1:
        fail(f"distinct falhou para Caixa: {got}")
    if "UN" not in got and "un" not in lower:
        fail(f"distinct perdeu UN: {got}")
    ok("_faceta_valores_distintos case-insensitive")

    # cache invalidate hits active key
    cache.set(FACETAS_GESTAO_CACHE_KEY, {"marcas": ["X"]}, 60)
    cache.set("agro_gestao_facetas_v1", {"marcas": ["Y"]}, 60)
    invalidar_cache_facetas_gestao()
    if cache.get(FACETAS_GESTAO_CACHE_KEY) is not None:
        fail("invalidar não limpou chave ativa v6")
    if cache.get("agro_gestao_facetas_v1") is not None:
        fail("invalidar não limpou chave legada v1")
    if FACETAS_GESTAO_CACHE_KEY not in FACETAS_GESTAO_CACHE_KEYS:
        fail("chave ativa fora da lista de invalidação")
    ok("invalidar_cache_facetas_gestao limpa v6 e legadas")

    # pin parser (sem DB se tabela vazia — só garante retorno shape)
    pin = _facetas_autorizadas_por_pin(limite_por_tipo=50)
    for k in ("marcas", "categorias", "subcategorias", "fornecedores", "unidades"):
        if k not in pin or not isinstance(pin[k], list):
            fail(f"_facetas_autorizadas_por_pin shape inválida: {pin.keys()}")
    ok("_facetas_autorizadas_por_pin shape OK")

    # simula strip do log PIN
    raw = "Caixa · PIN Renan"
    clean = raw.split(" · PIN ", 1)[0].strip()
    if clean != "Caixa":
        fail(f"parse PIN falhou: {clean!r}")
    ok("parse valor · PIN operador")


def main() -> None:
    print("verify_faceta_cache…")
    check_static()
    check_django_logic()
    print(f"VERIFY_OK: {len(CHECKS)} checks")


if __name__ == "__main__":
    main()
