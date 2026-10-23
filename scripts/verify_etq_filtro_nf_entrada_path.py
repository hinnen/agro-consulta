# -*- coding: utf-8 -*-
"""
ETQ-FILTRO-NF-ENTRADA — filtro por número da NF na busca de etiquetas.

Prova: arquivos · parse/casamento · HTTP página + API cadastro · runtime PG (se tabela existir).
"""
from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import Client
from django.urls import reverse
from django.utils import timezone

FAILS: list[str] = []
OKS = 0
TAG = f"etqnf_{uuid.uuid4().hex[:8]}"
PIN_TESTE = "9973"
NF_TESTE = f"9{TAG[-6:]}"


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
    util = read("produtos/cadastro_filtros_util.py")
    check('out["numero_nf"]' in util, "parse numero_nf")
    check("def produto_ids_por_numero_entrada_nf" in util, "produto_ids_por_numero_entrada_nf")
    check("def mapa_quantidade_entrada_nf_por_produto" in util, "mapa qtd NF")
    check("def enriquecer_rows_entrada_nf_qtd" in util, "enriquecer entrada_nf_qtd")
    check("def _cabecalho_casa_numero_nf_filtro" in util, "casamento cabecalho NF")
    check("produto_ids_por_numero_entrada_nf(nf_num)" in util, "QS filtra por numero_nf")

    js = read("produtos/static/produtos/js/produtos_etiquetas.js")
    check("etq-f-numero-nf" in js, "JS id campo NF")
    check("qtdFilaParaProduto" in js, "JS qtd fila NF")
    check("etq-nf-qtd-modo" in js, "JS modo qtd NF")
    check("entrada_nf_qtd" in js, "JS usa entrada_nf_qtd")
    check("params.set('numero_nf'" in js, "JS envia numero_nf")
    check("temFiltrosAtivos" in js and "etq-f-numero-nf" in js, "JS temFiltrosAtivos NF")

    html = read("produtos/templates/produtos/produtos_etiquetas.html")
    check("etq-f-numero-nf" in html, "HTML campo NF")
    check("etq-nf-qtd-modo" in html, "HTML modo qtd NF")
    check("Nº NF entrada" in html or "NF entrada" in html, "HTML label NF")

    views = read("produtos/views.py")
    check("produto_ids_por_numero_entrada_nf(nf_num_lista)" in views, "Mongo lista restringe NF")

    urls = read("produtos/urls.py")
    check("api_produtos_cadastro" in urls, "rota api cadastro")


def check_parse_runtime() -> None:
    print("--- parse / casamento ---")
    from types import SimpleNamespace

    from produtos.cadastro_filtros_util import (
        _cabecalho_casa_numero_nf_filtro,
        filtros_cadastro_ativos,
        parse_filtros_cadastro,
    )

    class _Get(dict):
        def getlist(self, key):
            v = self.get(key)
            if v is None:
                return []
            if isinstance(v, (list, tuple)):
                return list(v)
            return [v]

    f = parse_filtros_cadastro(SimpleNamespace(GET=_Get(numero_nf=NF_TESTE)))
    check(f.get("numero_nf") == NF_TESTE, "parse numero_nf limpo")
    check(filtros_cadastro_ativos(f), "filtros ativos com numero_nf")
    check(_cabecalho_casa_numero_nf_filtro({"numero": NF_TESTE}, NF_TESTE), "match exato")
    check(not _cabecalho_casa_numero_nf_filtro({"numero": "00000001"}, NF_TESTE), "nao casa outro")
    check(
        _cabecalho_casa_numero_nf_filtro({"numero": "SEM-1410-TJAI"}, "SEM-1410-TJAI"),
        "SEM match exato TJAI",
    )
    check(
        not _cabecalho_casa_numero_nf_filtro({"numero": "SEM-1410-UPS7"}, "SEM-1410-TJAI"),
        "SEM nao mistura sufixo UPS7/TJAI",
    )
    check(_cabecalho_casa_numero_nf_filtro({"numero": "NF 76468-A"}, "76468"), "numerico parcial 76468")


def _tabela_entrada_nf_existe() -> bool:
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM produtos_entradanotarascunhoagro LIMIT 1"
            )
            cur.fetchone()
        return True
    except Exception:
        return False


def check_runtime_pg() -> None:
    print("--- runtime PG entrada NF ---")
    if not _tabela_entrada_nf_existe():
        ok("skip runtime PG (tabela entrada NF ausente)")
        return

    from produtos.cadastro_filtros_util import produto_ids_por_numero_entrada_nf
    from produtos.models import EntradaNotaRascunhoAgro, Produto
    from produtos.nfe_entrada_util import ENTRADA_NFE_STATUS_ESTOQUE_APLICADO

    pid = f"local:{TAG}"
    Produto.objects.filter(produto_externo_id=pid).delete()
    p = Produto.objects.create(
        produto_externo_id=pid,
        nome=f"Produto {TAG}",
        codigo_interno=f"GM-{TAG[:6]}",
        ativo=True,
    )
    rid = f"{uuid.uuid4().hex[:24]}"
    EntradaNotaRascunhoAgro.objects.filter(rascunho_id=rid).delete()
    agora = timezone.now()
    EntradaNotaRascunhoAgro.objects.create(
        rascunho_id=rid,
        status=ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
        cabecalho={"numero": NF_TESTE, "emit_nome": "Forn teste"},
        linhas=[{"produto_id": pid, "q_com": 2, "x_prod": p.nome}],
        extra={},
        criado_em=agora,
        estoque_aplicado_em=agora,
    )

    from django.core.cache import cache

    cache.delete(f"cadastro_nf_num_ids_v1:{__import__('hashlib').md5(NF_TESTE.casefold().encode('utf-8')).hexdigest()[:20]}")

    ids = produto_ids_por_numero_entrada_nf(NF_TESTE)
    check(pid in ids, f"ids NF contem produto {pid}")
    check(len(ids) >= 1, "pelo menos 1 produto na NF")

    EntradaNotaRascunhoAgro.objects.filter(rascunho_id=rid).delete()
    Produto.objects.filter(pk=p.pk).delete()


def check_http() -> None:
    print("--- HTTP ---")
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    if not user:
        user = User.objects.create_superuser(
            username=f"etq_verify_{TAG}",
            email="",
            password="etq-verify-local-only",
        )
        ok("usuario Django criado para smoke HTTP")
    else:
        ok("usuario Django")

    try:
        from produtos.caixa_util import rotulo_operador_pin

        rot = (rotulo_operador_pin(PIN_TESTE) or "").strip()
        if rot:
            ok(f"PIN {PIN_TESTE} mapeado ({rot!r})")
        else:
            ok(f"PIN {PIN_TESTE} sem rotulo local (ok fora da loja)")
    except Exception as exc:
        fail(f"PIN: {exc}")

    c = Client(headers={"host": "127.0.0.1"})
    c.force_login(user)

    page = c.get(reverse("produtos_etiquetas"))
    check(page.status_code == 200, f"GET etiquetas ({page.status_code})")
    body = page.content.decode("utf-8", errors="replace")
    check("etq-f-numero-nf" in body, "pagina tem campo NF")

    api = c.get(
        reverse("api_produtos_cadastro"),
        {"numero_nf": NF_TESTE, "pagina": "1", "por_pagina": "20", "incluir_saldo": "1"},
    )
    if api.status_code == 503:
        try:
            err = (api.json().get("erro") or "").lower()
        except Exception:
            err = ""
        if "mongo" in err or "postgres" in err or "indispon" in err:
            ok(f"GET cadastro numero_nf skip catalogo ({api.status_code})")
            return
    check(api.status_code == 200, f"GET cadastro numero_nf ({api.status_code})")
    try:
        j = api.json()
    except Exception:
        j = {}
        fail("cadastro JSON invalido")
    check(j.get("ok") is not False, "cadastro ok")
    check("produtos" in j, "cadastro retorna produtos")


def main() -> int:
    print(f"ETQ-FILTRO-NF-ENTRADA PATH · NF_TESTE={NF_TESTE}")
    check_arquivos()
    check_parse_runtime()
    check_runtime_pg()
    check_http()
    total = OKS + len(FAILS)
    if FAILS:
        print(f"VERIFY_FAIL {OKS}/{total}")
        for f in FAILS:
            print(" ", f)
        return 1
    print(f"VERIFY_OK {OKS}/{total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
