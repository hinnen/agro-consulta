# -*- coding: utf-8 -*-
"""
ETQ-COLAR-MV — prova detalhada:
arquivos · extrair tokens · resolver GM · ranking mais vendidos · HTTP APIs · página.
"""
from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import Client, override_settings
from django.urls import reverse
from django.utils import timezone

from produtos.etiquetas_fila_util import (
    MAX_CODIGOS,
    extrair_tokens_codigos,
    produtos_mais_vendidos_para_etiquetas,
    resolver_produtos_por_codigos,
)
from produtos.models import ItemVendaAgro, Produto, ProdutoGestaoOverlayAgro, VendaAgro

FAILS: list[str] = []
OKS = 0
TAG = f"etqmv_{uuid.uuid4().hex[:10]}"
PIN_TESTE = "9973"


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def check_arquivos() -> None:
    print("--- arquivos ---")
    util = read("produtos/etiquetas_fila_util.py")
    check("def extrair_tokens_codigos" in util, "util extrair_tokens")
    check("def resolver_produtos_por_codigos" in util, "util resolver")
    check("def produtos_mais_vendidos_para_etiquetas" in util, "util mais_vendidos")
    check("MAX_CODIGOS = 400" in util, "teto 400 códigos")
    check("MAX_MAIS_VENDIDOS = 200" in util, "teto 200 ranking")

    urls = read("produtos/urls.py")
    check("api_etiquetas_resolver_codigos" in urls, "url resolver")
    check("api_etiquetas_mais_vendidos" in urls, "url mais-vendidos")
    check("resolver-codigos/" in urls, "path resolver-codigos")
    check("mais-vendidos/" in urls, "path mais-vendidos")

    views = read("produtos/views.py")
    check("def api_etiquetas_resolver_codigos" in views, "view resolver")
    check("def api_etiquetas_mais_vendidos" in views, "view mais vendidos")
    check("api_etq_resolver_url" in views, "ctx resolver URL")
    check("api_etq_mais_vendidos_url" in views, "ctx mv URL")
    check("@login_required" in views.split("def api_etiquetas_resolver_codigos")[0][-200:], "resolver login_required")

    html = read("produtos/templates/produtos/produtos_etiquetas.html")
    for needle, label in (
        ("etq-btn-colar-codigos", "btn colar"),
        ("etq-colar-texto", "textarea"),
        ("etq-colar-adicionar", "btn add colar"),
        ("etq-mv-periodo", "select periodo"),
        ("etq-mv-de", "data de"),
        ("etq-mv-ate", "data ate"),
        ("etq-mv-limite", "top N"),
        ("etq-mv-ordenar", "ordenar"),
        ("etq-mv-sentido", "sentido"),
        ("etq-mv-carregar", "btn carregar"),
        ("resolverUrl", "CFG resolver"),
        ("maisVendidosUrl", "CFG mv"),
        ("?v=17", "cache-bust v17"),
    ):
        check(needle in html, f"HTML {label}")

    js = read("produtos/static/produtos/js/produtos_etiquetas.js")
    for needle, label in (
        ("URL_RESOLVER", "URL_RESOLVER"),
        ("URL_MAIS_VENDIDOS", "URL_MAIS_VENDIDOS"),
        ("confirmarColarCodigos", "confirmarColar"),
        ("carregarMaisVendidos", "carregarMV"),
        ("adicionarProdutosNaFila", "add fila batch"),
        ("limit: 200", "limite ranking 200"),
        ("appendMultiLocal('categoria'", "MV manda categoria"),
        ("csrfToken", "CSRF no POST"),
    ):
        check(needle in js, f"JS {label}")


def check_extrair() -> None:
    print("--- extrair tokens ---")
    amostra = (
        "Codigo GM\tnome\n"
        "GM1140\tseringa 3ml\n"
        "GM1195 veneno rato\n"
        "GM1220-150;enronew\n"
        "4680\n"
        "#\n"
        "1\n"
        "GM1140\n"
        "gm0991\tagulha\n"
    )
    toks = extrair_tokens_codigos(amostra)
    check(toks[0] == "GM1140", f"1o token GM1140 ({toks[:3]})")
    check("GM1195" in toks, "tem GM1195")
    check("GM1220-150" in toks, "tem sufixo -150")
    check("4680" in toks, "codigo numerico 4+")
    check(toks.count("GM1140") == 1, "dedupe GM1140")
    check("gm0991" in toks or "GM0991" in toks, "case preserve gm0991")
    check("#" not in toks and "1" not in toks, "ignora # e posicao 1")
    check("Codigo" not in toks and "Código" not in toks, "ignora cabecalho")

    # lista longa truncada
    big = "\n".join(f"GM{i:04d}" for i in range(1, 500))
    toks_big = extrair_tokens_codigos(big)
    check(len(toks_big) == MAX_CODIGOS, f"teto MAX_CODIGOS={MAX_CODIGOS} got {len(toks_big)}")

    check(extrair_tokens_codigos("") == [], "vazio → []")
    check(extrair_tokens_codigos("   \n\n  ") == [], "só whitespace → []")


def _mk_produto(*, pid: str, gm: str, nome: str, cat: str = "Remedios", preco: str = "12.50") -> Produto:
    return Produto.objects.create(
        produto_externo_id=pid[:64],
        codigo_interno=gm[:50],
        codigo_nfe=gm[:64],
        nome=nome[:300],
        categoria=cat,
        preco_venda=Decimal(preco),
        custo=Decimal("5.00"),
        ativo=True,
        cadastro_inativo=False,
    )


def check_resolver_runtime() -> None:
    print("--- resolver runtime ---")
    pid_a = f"{TAG}_a"
    pid_b = f"{TAG}_b"
    pid_c = f"{TAG}_c"
    gm_a = f"GM{TAG[-4:].upper()}A"
    gm_b = f"GM{TAG[-4:].upper()}B-1"
    gm_ov = f"GM{TAG[-4:].upper()}OV"
    p_a = _mk_produto(pid=pid_a, gm=gm_a, nome=f"Etq MV A {TAG}")
    p_b = _mk_produto(pid=pid_b, gm=gm_b, nome=f"Etq MV B {TAG}")
    p_c = _mk_produto(pid=pid_c, gm=f"INT{TAG[-4:]}", nome=f"Etq MV C overlay {TAG}")
    ProdutoGestaoOverlayAgro.objects.update_or_create(
        produto_externo_id=pid_c[:64],
        defaults={"codigo_nfe": gm_ov[:64], "preco_venda": Decimal("9.90")},
    )

    texto = f"{gm_a}\n{gm_b}\tfake nome\n{gm_ov}\nNAOEXISTE999\n{gm_a}\n"
    data = resolver_produtos_por_codigos(texto, inativos=False)
    check(data["pedidos"] == 4, f"pedidos=4 (dedupe+inexistente) got {data['pedidos']}")
    check(data["achados"] == 3, f"achados=3 got {data['achados']}")
    ids = {str(p.get("id")) for p in data["produtos"]}
    check(pid_a in ids and pid_b in ids and pid_c in ids, f"ids resolvidos {ids}")
    check("NAOEXISTE999" in data["nao_encontrados"], "nao_encontrado listado")
    nomes = {str(p.get("nome") or "") for p in data["produtos"]}
    check(any(TAG in n for n in nomes), "nome hidratado no row")
    # ordem relativa A antes de B
    order_ids = [str(p.get("id")) for p in data["produtos"]]
    check(order_ids.index(pid_a) < order_ids.index(pid_b), "ordem colagem preservada A<B")

    # lista explícita
    data2 = resolver_produtos_por_codigos([gm_a, "ZZZ_X"], inativos=True)
    check(data2["achados"] == 1, "lista explícita acha 1")
    check(data2["nao_encontrados"] == ["ZZZ_X"], "lista explícita miss")

    # cleanup soft: leave for ranking test — return products
    return {"a": p_a, "b": p_b, "c": p_c, "gm_a": gm_a, "gm_b": gm_b, "pid_a": pid_a, "pid_b": pid_b}


def check_ranking_runtime(ctx: dict) -> None:
    print("--- ranking runtime ---")
    agora = timezone.now()
    v = VendaAgro.objects.create(
        total=Decimal("100.00"),
        forma_pagamento="Dinheiro",
        deposito="centro",
    )
    # força criado_em se auto_now_add
    VendaAgro.objects.filter(pk=v.pk).update(criado_em=agora - timedelta(hours=2))
    v.refresh_from_db()

    ItemVendaAgro.objects.create(
        venda=v,
        produto_id_externo=ctx["pid_a"],
        codigo=ctx["gm_a"],
        descricao=ctx["a"].nome,
        quantidade=Decimal("5"),
        valor_unitario=Decimal("10"),
        valor_total=Decimal("50"),
    )
    ItemVendaAgro.objects.create(
        venda=v,
        produto_id_externo=ctx["pid_b"],
        codigo=ctx.get("gm_b") or "GM-B",
        descricao=ctx["b"].nome,
        quantidade=Decimal("1"),
        valor_unitario=Decimal("10"),
        valor_total=Decimal("10"),
    )

    desde = agora - timedelta(days=1)
    ate = agora + timedelta(hours=1)
    out = produtos_mais_vendidos_para_etiquetas(
        desde,
        ate,
        limite=50,
        ordenar="qtd",
        sentido="mais",
    )
    ids = [str(p.get("id")) for p in out["produtos"]]
    check(ctx["pid_a"] in ids, "ranking inclui produto A (qtd 5)")
    check(ctx["pid_b"] in ids, "ranking inclui produto B (qtd 1)")
    if ctx["pid_a"] in ids and ctx["pid_b"] in ids:
        check(ids.index(ctx["pid_a"]) < ids.index(ctx["pid_b"]), "ordenar qtd: A antes de B")

    out_cat = produtos_mais_vendidos_para_etiquetas(
        desde,
        ate,
        limite=50,
        ordenar="valor",
        sentido="mais",
        categoria=["Remedios"],
    )
    ids_cat = {str(p.get("id")) for p in out_cat["produtos"]}
    check(ctx["pid_a"] in ids_cat, "filtro categoria Remedios mantém A")

    out_lim = produtos_mais_vendidos_para_etiquetas(desde, ate, limite=1, ordenar="qtd")
    check(len(out_lim["produtos"]) == 1, "limite=1 respeitado")
    check(str(out_lim["produtos"][0].get("id")) == ctx["pid_a"], "limite=1 = top qtd")


def check_http(ctx: dict) -> None:
    print("--- HTTP ---")
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    check(user is not None, "usuario Django existe")
    if not user:
        return

    # PIN só informativo (etiquetas não exige PIN nestas APIs)
    try:
        from produtos.caixa_util import rotulo_operador_pin

        rot = (rotulo_operador_pin(PIN_TESTE) or "").strip()
        check(bool(rot), f"PIN {PIN_TESTE} mapeado ({rot!r})")
    except Exception as exc:
        fail(f"PIN check: {exc}")

    hosts = list(getattr(settings, "ALLOWED_HOSTS", []) or []) + ["testserver", "127.0.0.1"]
    with override_settings(ALLOWED_HOSTS=hosts):
        anon = Client()
        r_anon = anon.post(
            reverse("api_etiquetas_resolver_codigos"),
            data=json.dumps({"texto": ctx["gm_a"]}),
            content_type="application/json",
        )
        check(r_anon.status_code in (302, 401, 403), f"anon resolver bloqueado ({r_anon.status_code})")

        c = Client()
        c.force_login(user)

        page = c.get(reverse("produtos_etiquetas"))
        check(page.status_code == 200, f"GET etiquetas ({page.status_code})")
        body = page.content.decode("utf-8", errors="replace")
        check("etq-btn-colar-codigos" in body, "página tem Colar códigos")
        check("etq-mv-carregar" in body, "página tem Carregar ranking")
        # escapejs vira hífen em \u002D — checar chave + trecho do path
        check("resolverUrl:" in body and "resolver" in body and "codigos" in body, "página embute resolverUrl")
        check("maisVendidosUrl:" in body and "mais" in body and "vendidos" in body, "página embute maisVendidosUrl")
        check("produtos_etiquetas.js?v=17" in body, "página puxa JS v17")

        # CSRF: Client emite cookie; force_login + json POST com CSRF
        c.get(reverse("produtos_etiquetas"))
        r_res = c.post(
            reverse("api_etiquetas_resolver_codigos"),
            data=json.dumps({"texto": f"{ctx['gm_a']}\nFALSOXYZ\n{ctx['gm_a']}"}),
            content_type="application/json",
        )
        check(r_res.status_code == 200, f"POST resolver ({r_res.status_code})")
        try:
            j = r_res.json()
        except Exception:
            j = {}
            fail("resolver JSON inválido")
        check(j.get("ok") is True, "resolver ok=true")
        check(int(j.get("achados") or 0) >= 1, f"resolver achados>={j.get('achados')}")
        check("FALSOXYZ" in (j.get("nao_encontrados") or []), "resolver lista miss")
        prods = j.get("produtos") or []
        check(any(str(p.get("id")) == ctx["pid_a"] for p in prods), "resolver retorna pid_a")
        check(any(p.get("preco_venda") is not None for p in prods), "produto tem preco_venda")

        r_mv = c.get(
            reverse("api_etiquetas_mais_vendidos"),
            {
                "periodo": "7d",
                "limite": "50",
                "ordenar": "qtd",
                "sentido": "mais",
                "categoria": "Remedios",
            },
        )
        check(r_mv.status_code == 200, f"GET mais-vendidos ({r_mv.status_code})")
        try:
            jm = r_mv.json()
        except Exception:
            jm = {}
            fail("mais-vendidos JSON inválido")
        check(jm.get("ok") is True, "mais-vendidos ok")
        mv_ids = {str(p.get("id")) for p in (jm.get("produtos") or [])}
        check(ctx["pid_a"] in mv_ids, "HTTP ranking traz produto A vendido")
        check(jm.get("de") and jm.get("ate"), "HTTP devolve de/ate")
        check(int(jm.get("limite") or 0) == 50, "HTTP limite ecoado")

        r_lim = c.get(
            reverse("api_etiquetas_mais_vendidos"),
            {"periodo": "30d", "limite": "999", "ordenar": "valor"},
        )
        jl = r_lim.json() if r_lim.status_code == 200 else {}
        check(int(jl.get("limite") or 0) == 200, f"teto HTTP limite 200 (got {jl.get('limite')})")


def cleanup(ctx: dict) -> None:
    print("--- cleanup ---")
    try:
        ItemVendaAgro.objects.filter(produto_id_externo__startswith=TAG).delete()
        VendaAgro.objects.filter(itens__produto_id_externo__startswith=TAG).distinct().delete()
    except Exception:
        pass
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__startswith=TAG).delete()
    Produto.objects.filter(produto_externo_id__startswith=TAG).delete()
    ok("cleanup TAG")


def main() -> int:
    check_arquivos()
    check_extrair()
    ctx = check_resolver_runtime()
    try:
        check_ranking_runtime(ctx)
        check_http(ctx)
    finally:
        cleanup(ctx)

    total = OKS + len(FAILS)
    print(f"\nVERIFY {'OK' if not FAILS else 'FAIL'} {OKS}/{total}")
    if FAILS:
        print("--- falhas ---")
        for f in FAILS:
            print("-", f.encode("ascii", "replace").decode("ascii"))
    return 0 if not FAILS else 1


if __name__ == "__main__":
    raise SystemExit(main())
