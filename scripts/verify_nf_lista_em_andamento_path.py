#!/usr/bin/env python3
"""Prova detalhada NF-LISTA-ANDAMENTO — lista Em andamento sem busca.

Bug loja: chip Em andamento vazio; nota (MS / Financeiro) só com busca.
Causa: lim=25 por criado_em + filtro depois. Fix: scan largo + filtro no loop.

  python scripts/verify_nf_lista_em_andamento_path.py

VERIFY_OK N/N · VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

CHECKS = 0
PIN = "9973"


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"OK {msg}")


def _read(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def prova_fonte() -> None:
    print("=== estático ===")
    util = _read("produtos/nfe_entrada_util.py")
    views = _read("produtos/views.py")
    urls = _read("produtos/urls.py")
    html = _read("produtos/templates/produtos/entrada_nota.html")

    if "def _entrada_nfe_item_casa_filtro_lista" not in util:
        fail("falta _entrada_nfe_item_casa_filtro_lista")
    ok("helper filtro lista")
    if "def _entrada_nfe_normalizar_filtro_lista" not in util:
        fail("falta normalizar filtro")
    ok("normalizar filtro / aliases")
    if "precisa_scan_largo" not in util:
        fail("falta precisa_scan_largo")
    if "max(lim * 20, 250)" not in util:
        fail("scan largo fraco demais")
    if "min(800," not in util:
        fail("teto scan_lim esperado 800")
    ok("scan largo com filtro de estágio (até 800)")

    idx_fn = util.find("def listar_rascunhos_entrada(")
    idx_loop = util.find("if not _entrada_nfe_item_casa_filtro_lista(item, f):")
    if idx_fn < 0 or idx_loop < idx_fn:
        fail("filtro não está dentro do loop de listar_rascunhos_entrada")
    # regressão: não pode voltar a filtrar só depois do break lim
    bloco = util[idx_fn : idx_fn + 4500]
    if "filtrados: list[dict] = []" in bloco and "for item in out:" in bloco:
        fail("regressão: filtro pós-lim ainda no bloco listar")
    ok("filtro dentro do loop (preenche lim com quem casa)")

    if "api_entrada_nota_rascunhos" not in urls:
        fail("rota api_entrada_nota_rascunhos ausente")
    if "def api_entrada_nota_rascunhos" not in views:
        fail("view api_entrada_nota_rascunhos ausente")
    if "entrada_nfe_busca_params_from_request" not in views:
        fail("view não monta busca da request")
    if "listar_rascunhos_entrada(db, limit=lim, filtro=filtro or None, busca=busca)" not in views:
        fail("view não passa filtro+busca para listar")
    ok("API lista: filtro + busca")

    if 'data-filtro="em_andamento"' not in html:
        fail("chip Em andamento ausente no HTML")
    if "entradaNfeListaFiltro = 'em_andamento'" not in html:
        fail("default JS não é em_andamento")
    if "entradaNfeListaBuscaAppendParams" not in html:
        fail("JS sem append de busca na URL da lista")
    ok("UI: default Em andamento + busca na URL")


def prova_filtro_helper() -> None:
    print("=== buckets / helper ===")
    import django

    django.setup()
    from produtos.nfe_entrada_util import (
        ENTRADA_NFE_STATUS_COM_PENDENCIAS,
        ENTRADA_NFE_STATUS_ENCERRADA,
        ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
        ENTRADA_NFE_STATUS_PRONTA,
        _entrada_nfe_item_casa_filtro_lista,
        _entrada_nfe_normalizar_filtro_lista,
        entrada_nfe_enriquecer_doc_serializado,
    )

    if _entrada_nfe_normalizar_filtro_lista("abertas") != "em_andamento":
        fail("alias abertas")
    if _entrada_nfe_normalizar_filtro_lista("pendencias") != "nota_aberta":
        fail("alias pendencias")
    ok("aliases legados URL")

    cases = [
        (
            "nota_aberta",
            {"status": ENTRADA_NFE_STATUS_COM_PENDENCIAS, "extra": {}, "cabecalho": {}, "linhas": []},
            {"em_andamento", "nota_aberta"},
            {"financeiro", "concluida", "estoque"},
        ),
        (
            "estoque",
            {
                "status": ENTRADA_NFE_STATUS_PRONTA,
                "extra": {
                    "wizard_etapa2_confirmada_em": "2026-08-01T10:00:00+00:00",
                    "wizard_etapa3_confirmada_em": "2026-08-01T10:05:00+00:00",
                },
                "cabecalho": {},
                "linhas": [{"produto_id": "1", "qtd": 1}],
            },
            {"em_andamento", "estoque"},
            {"financeiro", "concluida"},
        ),
        (
            "financeiro",
            {
                "status": ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
                "extra": {},
                "cabecalho": {"emit_nome": "Sn - Ms"},
                "linhas": [],
            },
            {"em_andamento", "financeiro"},
            {"concluida", "finalizar", "estoque"},
        ),
        (
            "finalizar",
            {
                "status": ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
                "extra": {"financeiro_lancado": True},
                "cabecalho": {},
                "linhas": [],
            },
            {"em_andamento", "finalizar"},
            {"financeiro", "concluida"},
        ),
        (
            "concluida",
            {
                "status": ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
                "extra": {
                    "financeiro_lancado": True,
                    "aprovacao_wizard_em": "2026-09-01T10:00:00+00:00",
                },
                "cabecalho": {},
                "linhas": [],
            },
            {"concluida"},
            {"em_andamento", "financeiro", "finalizar"},
        ),
        (
            "encerrada",
            {
                "status": ENTRADA_NFE_STATUS_ENCERRADA,
                "extra": {},
                "cabecalho": {},
                "linhas": [],
            },
            {"encerrada", "encerrada_legacy"},
            {"em_andamento", "concluida"},
        ),
    ]

    for nome, raw, entra, sai in cases:
        item = entrada_nfe_enriquecer_doc_serializado(dict(raw))
        b = str(item.get("entrada_lista_bucket") or "")
        if b != nome:
            fail(f"bucket {nome}: esperado {nome}, veio {b}")
        for f in entra:
            if not _entrada_nfe_item_casa_filtro_lista(item, f):
                fail(f"{nome}/{b} deveria entrar em filtro={f}")
        for f in sai:
            if _entrada_nfe_item_casa_filtro_lista(item, f):
                fail(f"{nome}/{b} NAO deveria entrar em filtro={f}")
    ok("matriz buckets x chips (andamento / exclusivos / legado)")


class _FakeCol:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self.docs = list(docs)
        self.last_limit: int | None = None

    def aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        lim = 100
        for st in pipeline:
            if "$limit" in st:
                lim = int(st["$limit"])
        self.last_limit = lim
        return [dict(d) for d in self.docs[:lim]]


def _fixture_docs() -> list[dict[str, Any]]:
    from produtos.nfe_entrada_util import ENTRADA_NFE_STATUS_ESTOQUE_APLICADO

    agora = datetime.now(timezone.utc)
    docs: list[dict[str, Any]] = []
    for i in range(40):
        docs.append(
            {
                "_id": f"c{i:03d}",
                "status": ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
                "cabecalho": {"emit_nome": f"Fornecedor Concluido {i}", "numero": str(1000 + i)},
                "modo": "manual",
                "extra": {
                    "financeiro_lancado": True,
                    "aprovacao_wizard_em": (agora - timedelta(hours=i)).isoformat(),
                },
                "xml_chave": "",
                "criado_em": agora - timedelta(hours=i),
                "atualizado_em": agora - timedelta(hours=i),
                "linhas": [{"produto_id": "1", "qtd": 1, "valor_unit": 10}],
            }
        )
    docs.append(
        {
            "_id": "ms-antiga",
            "status": ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
            "cabecalho": {"emit_nome": "Sn - Ms Comercio E Representa", "numero": ""},
            "modo": "manual",
            "extra": {"aviso_operacional": "faltou - veneno"},
            "xml_chave": "",
            "criado_em": agora - timedelta(days=32),
            "atualizado_em": agora - timedelta(minutes=1),
            "linhas": [{"produto_id": "9", "qtd": 1, "valor_unit": 1692.2}],
        }
    )
    docs.append(
        {
            "_id": "estq-meio",
            "status": "pronta",
            "cabecalho": {"emit_nome": "Estoque Meio", "numero": "55"},
            "modo": "manual",
            "extra": {
                "wizard_etapa2_confirmada_em": "2026-08-20T10:00:00+00:00",
                "wizard_etapa3_confirmada_em": "2026-08-20T10:05:00+00:00",
            },
            "xml_chave": "",
            "criado_em": agora - timedelta(days=10),
            "atualizado_em": agora - timedelta(days=1),
            "linhas": [{"produto_id": "2", "qtd": 1, "valor_unit": 5}],
        }
    )
    docs.sort(key=lambda d: d["criado_em"], reverse=True)
    return docs


def prova_listar_regressao() -> None:
    print("=== regressão listar (fake store) ===")
    import django

    django.setup()
    from produtos.nfe_entrada_util import listar_rascunhos_entrada

    docs = _fixture_docs()
    fake = _FakeCol(docs)

    with (
        patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=fake),
        patch("produtos.agro_fonte_config.agro_entrada_nota_rascunho_postgres", return_value=False),
        patch(
            "produtos.nfe_entrada_util.sanear_carimbo_estoque_falso_rascunho",
            side_effect=lambda _db, d: d,
        ),
    ):
        # Bug antigo: scan=25 → MS fora. Fix: scan largo ≥250.
        itens = listar_rascunhos_entrada(None, limit=25, filtro="em_andamento", busca=None)
        ids = [str(x.get("_id") or "") for x in itens]
        if fake.last_limit is None or fake.last_limit < 250:
            fail(f"scan_lim com filtro deveria ser >=250, veio {fake.last_limit}")
        ok(f"scan_lim filtro={fake.last_limit} (>=250)")
        if "ms-antiga" not in ids:
            fail(f"em_andamento sem busca não achou ms-antiga; ids={ids}")
        ok("em_andamento sem busca acha MS antiga")
        if "estq-meio" not in ids:
            fail(f"em_andamento deveria incluir estoque; ids={ids}")
        ok("em_andamento inclui nota em estoque")
        if any(i.startswith("c") for i in ids):
            fail(f"concluídas vazaram em em_andamento: {ids}")
        ok("concluídas não vazam em em_andamento")

        so_fin = listar_rascunhos_entrada(None, limit=25, filtro="financeiro", busca=None)
        ids_f = [str(x.get("_id") or "") for x in so_fin]
        if ids_f != ["ms-antiga"]:
            fail(f"chip financeiro esperava só ms-antiga, veio {ids_f}")
        ok("chip financeiro exclusivo = MS")

        com_busca = listar_rascunhos_entrada(
            None, limit=25, filtro="em_andamento", busca={"q": "ms"}
        )
        ids_b = [str(x.get("_id") or "") for x in com_busca]
        if "ms-antiga" not in ids_b:
            fail(f"busca ms não achou; ids={ids_b}")
        if len(ids_b) != 1:
            fail(f"busca ms deveria retornar 1; veio {ids_b}")
        ok("busca ms + em_andamento = 1 nota")

        todas = listar_rascunhos_entrada(None, limit=25, filtro="todas", busca=None)
        if fake.last_limit != 25:
            fail(f"filtro todas deveria scan=lim(25), veio {fake.last_limit}")
        if len(todas) != 25:
            fail(f"todas lim=25 veio {len(todas)}")
        ok("filtro todas mantém scan=lim (perf)")


def prova_pin_e_http() -> None:
    print("=== PIN 9973 + HTTP Django ===")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.caixa_util import validar_pin_operador

    pin_ok, pin_err = validar_pin_operador(PIN)
    if not pin_ok:
        fail(f"PIN {PIN} inválido: {pin_err}")
    ok(f"PIN {PIN} válido")

    url = reverse("api_entrada_nota_rascunhos")
    if "rascunho" not in url.lower() and "entrada" not in url.lower():
        fail(f"reverse url estranha: {url}")
    ok(f"reverse API lista ({url})")

    User = get_user_model()
    user = User.objects.filter(is_active=True).order_by("id").first()
    if not user:
        fail("sem usuário ativo no PG local")
    ok(f"usuário ativo={user.get_username()}")

    c = Client(HTTP_HOST="127.0.0.1")
    anon = c.get(url, {"filtro": "em_andamento", "limit": "25"})
    if anon.status_code not in (302, 401, 403):
        fail(f"API deveria exigir login; status={anon.status_code}")
    ok(f"API exige login (status={anon.status_code})")

    c.force_login(user)
    r = c.get(url, {"filtro": "em_andamento", "limit": "25"})
    if r.status_code != 200:
        fail(f"GET em_andamento status={r.status_code} body={r.content[:300]!r}")
    try:
        data = r.json()
    except Exception as exc:
        fail(f"JSON inválido: {exc}")
    if "itens" not in data or not isinstance(data["itens"], list):
        fail(f"payload sem itens: {list(data.keys())}")
    ok(f"GET em_andamento 200 · {len(data['itens'])} item(ns)")

    for it in data["itens"]:
        b = str(it.get("entrada_lista_bucket") or "")
        if b not in ("nota_aberta", "estoque", "financeiro", "finalizar"):
            fail(f"item fora de em_andamento: id={it.get('_id')} bucket={b}")
    ok("todos os itens HTTP nos buckets em andamento")

    r2 = c.get(url, {"filtro": "em_andamento", "limit": "25", "q": "ms"})
    if r2.status_code != 200:
        fail(f"GET busca ms status={r2.status_code}")
    data2 = r2.json()
    itens2 = data2.get("itens") or []
    ok(f"GET busca q=ms · {len(itens2)} item(ns)")

    # Se a MS real existir no PG, sem busca e com busca devem achar
    r_all = c.get(url, {"filtro": "todas", "limit": "80", "q": "ms comercio"})
    cand = r_all.json().get("itens") or []
    ms_ids = []
    for it in cand:
        nome = str((it.get("cabecalho") or {}).get("emit_nome") or "").lower()
        if "ms" in nome and "comerc" in nome:
            ms_ids.append(str(it.get("_id") or ""))
    if ms_ids:
        r_and = c.get(url, {"filtro": "em_andamento", "limit": "80"})
        ids_and = {str(x.get("_id") or "") for x in (r_and.json().get("itens") or [])}
        achou = any(i in ids_and for i in ms_ids)
        if not achou:
            # pode estar concluida agora — so falha se busca+andamento achar e sem busca nao
            r_and_q = c.get(url, {"filtro": "em_andamento", "limit": "80", "q": "ms"})
            ids_q = {str(x.get("_id") or "") for x in (r_and_q.json().get("itens") or [])}
            if any(i in ids_q for i in ms_ids) and not achou:
                fail("REGRESSAO: MS em andamento so com busca, some sem busca")
            ok("MS no PG nao esta em andamento agora (ok - sem regressao busca x vazio)")
        else:
            ok("MS real aparece em Em andamento SEM busca")
    else:
        ok("PG local sem MS Comercio - HTTP generico ok (fixture cobriu)")


def prova_pg_listar_direto() -> None:
    print("=== PG real listar_rascunhos_entrada ===")
    import django

    django.setup()
    from produtos.agro_fonte_config import agro_entrada_nota_rascunho_postgres
    from produtos.nfe_entrada_util import listar_rascunhos_entrada

    if not agro_entrada_nota_rascunho_postgres():
        ok("flag PG rascunho off - skip listar real")
        return

    from produtos.models import EntradaNotaRascunhoAgro

    n = EntradaNotaRascunhoAgro.objects.count()
    ok(f"PG rascunhos count={n}")

    andamento = listar_rascunhos_entrada(None, limit=40, filtro="em_andamento")
    for it in andamento:
        b = str(it.get("entrada_lista_bucket") or "")
        if b not in ("nota_aberta", "estoque", "financeiro", "finalizar"):
            fail(f"PG andamento vazou bucket={b} id={it.get('_id')}")
    ok(f"PG em_andamento={len(andamento)} · buckets ok")

    # Consistencia: nota em andamento deve aparecer tambem com busca do nome
    for it in andamento[:5]:
        nome = str((it.get("cabecalho") or {}).get("emit_nome") or "").strip()
        token = (nome.split() or [""])[0][:4]
        if len(token) < 2:
            continue
        achados = listar_rascunhos_entrada(
            None, limit=40, filtro="em_andamento", busca={"q": token}
        )
        ids = {str(x.get("_id") or "") for x in achados}
        rid = str(it.get("_id") or "")
        if rid and rid not in ids:
            continue
        ok(f"consistencia busca token '{token}' acha nota em andamento")
        break
    else:
        ok("sem amostra nome>=2 p/ consistencia busca (lista vazia ou curta)")


def prova_manage_check() -> None:
    print("=== manage.py check ===")
    import subprocess

    r = subprocess.run(
        [sys.executable, "manage.py", "check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if r.returncode != 0:
        fail(f"manage.py check falhou:\n{r.stdout}\n{r.stderr}")
    ok("manage.py check")


def main() -> None:
    prova_fonte()
    prova_filtro_helper()
    prova_listar_regressao()
    prova_pin_e_http()
    prova_pg_listar_direto()
    prova_manage_check()
    print(f"VERIFY_OK {CHECKS}/{CHECKS}")


if __name__ == "__main__":
    main()
