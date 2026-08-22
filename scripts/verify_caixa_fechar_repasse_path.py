# -*- coding: utf-8 -*-
"""Prova detalhada — FECHAR-CAIXA-REPASSE.

Refresh do Fechar caixa só da loja do aparelho + aviso imediato após repasse.
Reproduz o bug: −X na Vila + +X no Centro se anulavam no agregado operacional.
"""
from __future__ import annotations

import json
import os
import re
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.test import RequestFactory

from produtos.caixa_util import (
    filtrar_sessoes_por_deposito,
    filtrar_sessoes_teste,
    filtrar_sessoes_operacional,
    resumo_esperado_por_forma,
    serializar_estado_conferencia_fechar,
    linhas_conferencia_agregada,
)
from produtos.views import api_caixa_conferencia_estado

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


class _Rel:
    def __init__(self, items=None):
        self._items = list(items or [])

    def all(self):
        return self._items


class _Mov:
    def __init__(self, tipo: str, forma: str, valor: str, pk: int, obs: str = ""):
        self.tipo = tipo
        self.forma_pagamento = forma
        self.valor = Decimal(valor)
        self.pk = pk
        self.observacao = obs


def _sess(pk: int, ponto: str, abertura: str, movs=None):
    return SimpleNamespace(
        pk=pk,
        ponto_caixa=ponto,
        valor_abertura=Decimal(abertura),
        usuario=None,
        usuario_id=None,
        vendas=_Rel([]),
        movimentos=_Rel(movs or []),
        fechado_em=None,
    )


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _extrair_fn(src: str, nome: str, proxima: str) -> str:
    a = src.find(f"def {nome}")
    b = src.find(f"def {proxima}", a + 1)
    check(a >= 0, f"fn {nome} existe")
    if a < 0:
        return ""
    return src[a:b] if b > a else src[a:]


def _chama_api(sessoes, dep: str, escopo: str):
    qs = MagicMock()
    qs.select_related.return_value = qs
    qs.prefetch_related.return_value = qs
    qs.order_by.return_value = list(sessoes)
    rf = RequestFactory()
    req = rf.get("/api/caixa/conferencia-estado/", {"escopo": escopo})
    req.user = SimpleNamespace(is_authenticated=True, pk=1)
    req.session = {}
    with (
        patch("produtos.views.SessaoCaixa.objects.filter", return_value=qs),
        patch("produtos.views.deposito_caixa_browser", return_value=dep),
    ):
        resp = api_caixa_conferencia_estado(req)
    return json.loads(resp.content.decode())


def prova_arquivos() -> None:
    print("\n[1] Arquivos e invariantes")
    html = _read("produtos/templates/produtos/caixa_fechar.html")
    views = _read("produtos/views.py")
    js = _read("produtos/static/produtos/js/pdv_repasse_vila.js")
    util = _read("produtos/repasse_vila_util.py")
    saida = _read("produtos/templates/produtos/includes/caixa_saida_embed.html")
    painel = _read("produtos/templates/produtos/caixa_painel.html")
    urls = _read("produtos/urls.py")
    agents = _read("AGENTS.md")

    check("escopo=loja" in html, "fechar pede escopo=loja")
    check("escopo=operacional" not in html, "fechar nao pede operacional")
    check("agro-caixa-fechar-atualizar" in html, "fechar escuta postMessage")
    check("e.origin !== window.location.origin" in html, "fechar checa origin")
    check("agendarAtualizarConferencia" in html, "fechar agenda refresh")
    check("data-cf-refresh" in html, "popup retirada/reforco marca refresh")
    check("api_caixa_conferencia_estado" in urls, "url api estado")

    api_fn = _extrair_fn(views, "api_caixa_conferencia_estado", "caixa_painel")
    check("filtrar_sessoes_por_deposito" in api_fn, "api usa filtro deposito")
    check("filtrar_sessoes_operacional" not in api_fn, "api nao agrega operacional")
    check('escopo == "teste"' in api_fn, "api escopo teste")
    check('escopo == "todos"' in api_fn, "api escopo todos")
    check("dep_browser" in api_fn, "api le deposito do aparelho")

    fechar_fn = _extrair_fn(views, "caixa_fechar", "venda_agro_detalhe")
    check("sessoes_lote = filtrar_sessoes_por_deposito" in fechar_fn, "POST lote filtra loja")
    check("linhas_conferencia_agregada(sessoes_lote" in fechar_fn, "POST agregado e do lote")
    check("serializar_estado_conferencia_fechar" in fechar_fn, "GET inicial serializa lote")

    check("notifyParentFecharAtualizar" in js, "js tem notify")
    check("agro-caixa-fechar-atualizar" in js, "js postMessage type")
    check("window.parent !== window" in js, "js so avisa se iframe")
    check(js.index("notifyParentFecharAtualizar()") < js.index("setTimeout(closeOverlay"), "notify antes de fechar overlay")
    # Falha do fetch nao avisa a tela (nao zera esperado a toa)
    catch = js[js.find(".catch(function"): js.find(".catch(function") + 180]
    check("notifyParentFecharAtualizar" not in catch, "erro de rede nao notifica")
    erro_ok = js[js.find("if (!j.ok)"): js.find("if (!j.ok)") + 160]
    check("notifyParentFecharAtualizar" not in erro_ok, "j.ok falso nao notifica")

    check("agro-caixa-fechar-atualizar" in saida, "saida embed mesmo type")
    check("agro-caixa-fechar-atualizar" in painel, "painel mesmo type")

    conf = _extrair_fn(util, "confirmar_repasse", "aplicar_repasses_pendentes_centro")
    check("sessao_caixa=sessao_vila" in conf, "repasse saida na sessao Vila")
    check("Tipo.RETIRADA" in conf, "repasse cria RETIRADA")
    check("Tipo.REFORCO" in conf, "repasse cria REFORCO no Centro")
    check("obter_caixa_gaveta_aberto" in conf, "reforco so se gaveta aberta")

    check("refresh após retirada / repasse" in agents.lower() or "FECHAR-CAIXA" in agents or "escopo=loja" in agents, "AGENTS §7 registra")

    # Sem migrate novo neste pacote
    migs = list((ROOT / "produtos/migrations").glob("*.py"))
    novas = [p.name for p in migs if "fechar_repasse" in p.name or "caixa_repasse_refresh" in p.name]
    check(not novas, "sem migrate novo")


def prova_filtros() -> None:
    print("\n[2] Filtro deposito / teste / notebook")
    s_v = _sess(1, "vila", "10")
    s_c = _sess(2, "gaveta", "20")
    s_nb = _sess(3, "notebook", "5")
    s_t = _sess(4, "teste", "1")
    todos = [s_v, s_c, s_nb, s_t]

    check(filtrar_sessoes_por_deposito(todos, "vila") == [s_v], "vila so pai vila")
    check(filtrar_sessoes_por_deposito(todos, "centro") == [s_c], "centro so gaveta")
    check(filtrar_sessoes_por_deposito(todos, "xyz") == [s_c], "deposito invalido cai no centro")
    check(s_nb not in filtrar_sessoes_por_deposito(todos, "vila"), "notebook fora do lote vila")
    check(s_nb not in filtrar_sessoes_por_deposito(todos, "centro"), "notebook fora do lote centro")
    check(filtrar_sessoes_teste(todos) == [s_t], "teste so ponto teste")
    check(s_t not in filtrar_sessoes_por_deposito(todos, "vila"), "teste fora do lote")
    op = filtrar_sessoes_operacional(todos)
    check(s_v in op and s_c in op and s_t not in op, "operacional mistura lojas (legado)")


def prova_esperado_apos_repasse() -> None:
    print("\n[3] Esperado apos repasse (bug −X/+X)")
    # Vila: fundo 1000 − saida 400 = 600
    # Centro: fundo 200 + reforco 400 = 600
    # Agregado velho (operacional): 1200 = mesmo valor de ANTES do repasse (1000+200)
    s_v = _sess(
        11,
        "vila",
        "1000.00",
        [_Mov("retirada", "Dinheiro", "400.00", 91, "Repasse Vila→Centro · ref 22/08/2026")],
    )
    s_c = _sess(
        22,
        "gaveta",
        "200.00",
        [_Mov("reforco", "Dinheiro", "400.00", 92, "Repasse da Vila · quem")],
    )

    esp_v = resumo_esperado_por_forma(s_v)["Dinheiro"]
    esp_c = resumo_esperado_por_forma(s_c)["Dinheiro"]
    check(esp_v == Decimal("600.00"), f"vila esperado 600 apos saida (foi {esp_v})")
    check(esp_c == Decimal("600.00"), f"centro esperado 600 apos reforco (foi {esp_c})")

    misturado = linhas_conferencia_agregada([s_v, s_c], todas_formas=True)
    tot_mix = next(L["esperado"] for L in misturado if L["forma"] == "Dinheiro")
    check(tot_mix == Decimal("1200.00"), f"agregado velho 1200 (foi {tot_mix})")
    check(tot_mix == Decimal("1000.00") + Decimal("200.00"), "mix = fundo antes do repasse")

    st_v = serializar_estado_conferencia_fechar([s_v], deposito="vila")
    st_c = serializar_estado_conferencia_fechar([s_c], deposito="centro")
    check(st_v["tot_esperado_dinheiro"] == "600.00", "serializar vila 600")
    check(st_c["tot_esperado_dinheiro"] == "600.00", "serializar centro 600")
    check(st_v["qtd_caixas"] == 1, "serializar vila 1 caixa")


def prova_api_escopos() -> None:
    print("\n[4] API escopos loja / operacional / todos / teste")
    s_v = _sess(
        11,
        "vila",
        "1000.00",
        [_Mov("retirada", "Dinheiro", "400.00", 91, "Repasse")],
    )
    s_c = _sess(
        22,
        "gaveta",
        "200.00",
        [_Mov("reforco", "Dinheiro", "400.00", 92, "Repasse")],
    )
    s_t = _sess(33, "teste", "50.00")
    sessoes = [s_v, s_c, s_t]

    d_loja = _chama_api(sessoes, "vila", "loja")
    ids = {int(c.get("sessao_id") or 0) for c in (d_loja.get("cards") or [])}
    check(d_loja.get("ok") is True, "escopo=loja ok")
    check(d_loja.get("tot_esperado_dinheiro") == "600.00", "loja vila 600 nao 1200")
    check(11 in ids and 22 not in ids and 33 not in ids, "loja vila so card vila")
    check(d_loja.get("qtd_caixas") == 1, "loja vila qtd=1")

    d_op = _chama_api(sessoes, "vila", "operacional")
    check(d_op.get("tot_esperado_dinheiro") == "600.00", "operacional agora alias de loja")
    ids_op = {int(c.get("sessao_id") or 0) for c in (d_op.get("cards") or [])}
    check(22 not in ids_op, "operacional vila nao puxa centro")

    d_c = _chama_api(sessoes, "centro", "loja")
    check(d_c.get("tot_esperado_dinheiro") == "600.00", "loja centro 600 (200+400)")
    ids_c = {int(c.get("sessao_id") or 0) for c in (d_c.get("cards") or [])}
    check(22 in ids_c and 11 not in ids_c, "loja centro so gaveta")

    d_all = _chama_api(sessoes, "vila", "todos")
    check(d_all.get("tot_esperado_dinheiro") == "1250.00", "todos mistura 600+600+50")
    ids_all = {int(c.get("sessao_id") or 0) for c in (d_all.get("cards") or [])}
    check({11, 22, 33} <= ids_all, "todos inclui as tres sessoes")

    d_t = _chama_api(sessoes, "vila", "teste")
    check(d_t.get("tot_esperado_dinheiro") == "50.00", "escopo teste so 50")
    ids_t = {int(c.get("sessao_id") or 0) for c in (d_t.get("cards") or [])}
    check(ids_t == {33}, "escopo teste so card teste")

    d_so_v = _chama_api([s_v], "vila", "loja")
    check(d_so_v.get("tot_esperado_dinheiro") == "600.00", "so vila aberta: 600")

    d_vazio = _chama_api([s_c], "vila", "loja")
    check(d_vazio.get("qtd_caixas") == 0, "vila sem sessao → 0 caixas")
    check(d_vazio.get("tot_esperado_dinheiro") == "0.00", "vila sem sessao → 0.00")

    d_vazias = _chama_api([], "vila", "loja")
    check(d_vazias.get("qtd_caixas") == 0, "nenhuma sessao → 0")


def prova_js_ordem() -> None:
    print("\n[5] JS: notify so no sucesso + listener do pai")
    js = _read("produtos/static/produtos/js/pdv_repasse_vila.js")
    html = _read("produtos/templates/produtos/caixa_fechar.html")
    # Uma unica definicao + uma unica chamada no sucesso
    defs = len(re.findall(r"function notifyParentFecharAtualizar", js))
    calls = js.count("notifyParentFecharAtualizar();")
    check(defs == 1, f"uma fn notify (foi {defs})")
    check(calls == 1, f"uma chamada notify no sucesso (foi {calls})")
    check("window.location.origin" in js, "postMessage com origin")
    check("type === 'agro-caixa-fechar-atualizar'" in html, "listener type exato")
    check("popupRefreshOnClose" in html, "fecha iframe tambem refresh")


def main() -> int:
    print("=== FECHAR-CAIXA-REPASSE ===")
    prova_arquivos()
    prova_filtros()
    prova_esperado_apos_repasse()
    prova_api_escopos()
    prova_js_ordem()
    print(f"\n---\noks={OKS} fails={len(FAILS)}")
    if FAILS:
        for f in FAILS:
            print(" ", f)
        return 1
    print("VERIFY_FECHAR_REPASSE_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
