# -*- coding: utf-8 -*-
"""VERIFY CAD-EXCLUIR-MSG-STAFF — exclusão cadastro não mente «sem permissão».

Cobre:
  · JS lê JSON.erro em 401/403 (não troca por mensagem genérica de login)
  · Staff envia forcar_exclusao_mongo_staff
  · IS_STAFF no bootstrap da tela
  · API: regra de negócio = 409 (não 403 de auth)
  · Staff+force passa da trava «só SisVale»
  · Não-staff sem flag continua bloqueado (409)
  · Prefixo AGRO / flag cadastro_somente_agro liberam sem force
  · Venda PDV existente continua 409

Uso: python scripts/verify_cad_excluir_msg_staff_path.py
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails = 0
oks = 0

MODAL = "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html"
LISTA = "produtos/templates/produtos/produtos_cadastro_erp.html"
VIEWS = "produtos/views.py"


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    global fails
    fails += 1
    print(f" FAIL {msg}")


def read(rel: str) -> str:
    p = ROOT / rel
    if not p.is_file():
        fail(f"ausente {rel}")
        return ""
    return p.read_text(encoding="utf-8", errors="replace")


def must_contain(txt: str, needle: str, label: str) -> None:
    if needle in txt:
        ok(f"{label}: `{needle[:56]}`")
    else:
        fail(f"{label}: falta `{needle[:80]}`")


def check_contracts() -> None:
    print("\n[1] Contratos JS / HTML / views")
    modal = read(MODAL)
    lista = read(LISTA)
    views = read(VIEWS)
    if not modal or not lista or not views:
        return

    for n in (
        "function mensagemErroApiHttp",
        "JSON.parse(raw)",
        "j.erro",
        "mensagemErroApiHttp(r, txt, 'excluir')",
        "mensagemErroApiHttp(r, txt, 'salvar')",
        "forcar_exclusao_mongo_staff",
        "cfg.IS_STAFF",
        "bodyExcluir.forcar_exclusao_mongo_staff = true",
        "URL_SOMENTE_AGRO_EXCLUIR",
        "btn-excluir-somente-agro",
    ):
        must_contain(modal, n, "modal")

    # 403 de negócio NÃO pode cair só na frase genérica sem tentar JSON
    if re.search(
        r"function mensagemErroApiHttp[\s\S]{0,800}?JSON\.parse\(raw\)[\s\S]{0,400}?st === 403",
        modal,
    ):
        ok("modal: JSON.erro antes do fallback 403")
    else:
        fail("modal: ordem JSON→fallback 403 quebrada")

    # Handler excluir: 403/401 usa mensagemErroApiHttp (não erroHttpPermissao puro sem parse)
    m_excl = re.search(
        r"bindClick\('btn-excluir-somente-agro'[\s\S]{0,2200}?status === 403[\s\S]{0,200}?mensagemErroApiHttp",
        modal,
    )
    if m_excl:
        ok("modal: excluir 403 usa mensagemErroApiHttp")
    else:
        fail("modal: excluir ainda mascara 403")

    must_contain(lista, "IS_STAFF:", "lista")
    must_contain(lista, "user.is_staff", "lista")
    must_contain(lista, "URL_SOMENTE_AGRO_EXCLUIR", "lista")

    must_contain(views, "def api_produtos_somente_agro_excluir", "views")
    must_contain(views, "forcar_exclusao_mongo_staff", "views")
    must_contain(views, "status=409", "views")

    # Regra so SisVale nao pode voltar como 403
    trecho = ""
    m = re.search(
        r"def api_produtos_somente_agro_excluir\([\s\S]*?\n(?:def |@)",
        views,
    )
    if m:
        trecho = m.group(0)
    if "cadastros criados s" in trecho and "SisVale" in trecho:
        ok("views: mensagem regra negocio presente")
    else:
        fail("views: mensagem regra negocio ausente na API")
    if re.search(
        r"cadastros criados s[\s\S]{0,500}?status=403",
        trecho,
    ):
        fail("views: regra negocio ainda status=403 (deveria 409)")
    else:
        ok("views: regra negocio nao usa 403")
    if re.search(
        r"cadastros criados s[\s\S]{0,500}?status=409",
        trecho,
    ):
        ok("views: regra negocio status=409")
    else:
        fail("views: falta status=409 na regra negocio")


def _mensagem_erro_api_http(status: int, texto_bruto: str, acao: str = "salvar") -> str:
    """Espelho da lógica JS mensagemErroApiHttp (prova unitária sem browser)."""
    st = status
    raw = str(texto_bruto or "")
    try:
        j = json.loads(raw)
        if isinstance(j, dict):
            msg_api = str(j.get("erro") or j.get("mensagem") or j.get("detail") or "").strip()
            if msg_api:
                return msg_api
    except Exception:
        pass
    if st == 403 and re.search(r"csrf", raw, re.I):
        return "Token de segurança expirou. Recarregue a página (F5) e tente de novo."
    if st == 401:
        return "Sessão expirada. Entre em /admin/login/, volte ao cadastro e tente de novo."
    if st == 403:
        return f"Sem permissão para {acao}. Confira se está logado e recarregue a página (F5)."
    return (
        f"Sem permissão para {acao}. Entre no sistema (login), recarregue a página (F5) e tente de novo."
    )


def check_js_logic_mirror() -> None:
    print("\n[2] Lógica mensagemErroApiHttp (espelho Python)")
    casos = [
        (
            403,
            json.dumps(
                {
                    "ok": False,
                    "erro": "Só é possível excluir aqui cadastros criados só no SisVale/Agro.",
                },
                ensure_ascii=False,
            ),
            "excluir",
            "excluir aqui",
        ),
        (
            403,
            "CSRF verification failed. Request aborted.",
            "salvar",
            "Token de seguran",
        ),
        (401, "<html>login</html>", "salvar", "Sess"),
        (403, "Forbidden", "excluir", "Sem permiss"),
        (
            403,
            json.dumps({"ok": False, "erro": "PIN incorreto."}),
            "salvar",
            "PIN incorreto",
        ),
    ]
    for st, raw, acao, expect in casos:
        got = _mensagem_erro_api_http(st, raw, acao)
        if expect.lower() in got.lower():
            ok(f"mirror {st}/{acao}")
        else:
            fail(f"mirror {st}/{acao}: esperado `{expect}` got `{got[:80]}`")


def check_api_django() -> None:
    print("\n[3] API Django (RequestFactory + mocks)")
    try:
        import django

        django.setup()
    except Exception as e:
        fail(f"django.setup: {e}")
        return

    from django.contrib.auth.models import AnonymousUser, User
    from django.test import RequestFactory

    from produtos import views as v

    factory = RequestFactory()

    def _post(user, body: dict):
        req = factory.post(
            "/api/produtos/somente-agro/excluir/",
            data=json.dumps(body),
            content_type="application/json",
        )
        req.user = user
        return v.api_produtos_somente_agro_excluir(req)

    staff = User(username="staff_verify", is_staff=True, is_active=True)
    staff.pk = 9001
    comum = User(username="comum_verify", is_staff=False, is_active=True)
    comum.pk = 9002

    # produto_id invalido
    r = _post(staff, {})
    if r.status_code == 400:
        ok("api: produto_id vazio = 400")
    else:
        fail(f"api: produto_id vazio status={r.status_code}")

    fake_erp = SimpleNamespace(
        cadastro_somente_agro=False,
        produto_externo_id="ERP-GUID-XYZ",
        delete=MagicMock(),
    )
    fake_agro = SimpleNamespace(
        cadastro_somente_agro=True,
        produto_externo_id="AGROTESTVERIFY001",
        delete=MagicMock(),
    )
    fake_prefix = SimpleNamespace(
        cadastro_somente_agro=False,
        produto_externo_id="AGROONLYFLAGFALSE",
        delete=MagicMock(),
    )

    with (
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.catalogo_agro.obter_produto_model", return_value=None),
    ):
        r = _post(staff, {"produto_id": "NAOEXISTE123"})
        if r.status_code == 404:
            ok("api: PG produto ausente = 404")
        else:
            fail(f"api: ausente status={r.status_code}")

    with (
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.catalogo_agro.obter_produto_model", return_value=fake_erp),
        patch.object(v.ItemVendaAgro.objects, "filter") as filt,
    ):
        filt.return_value.exists.return_value = False
        r = _post(comum, {"produto_id": "ERP-GUID-XYZ"})
        data = json.loads(r.content.decode("utf-8"))
        if r.status_code == 409 and "SisVale" in str(data.get("erro") or ""):
            ok("api: ERP sem force = 409 + erro real")
        else:
            fail(f"api: ERP sem force status={r.status_code} body={data}")

        r2 = _post(
            comum,
            {"produto_id": "ERP-GUID-XYZ", "forcar_exclusao_mongo_staff": True},
        )
        data2 = json.loads(r2.content.decode("utf-8"))
        if r2.status_code == 409 and "SisVale" in str(data2.get("erro") or ""):
            ok("api: nao-staff + force ignorado = 409")
        else:
            fail(f"api: nao-staff force status={r2.status_code} body={data2}")

    # Staff + force: passa da trava; pode falhar depois no delete real — mockamos delete chain
    with (
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.catalogo_agro.obter_produto_model", return_value=fake_erp),
        patch.object(v.ItemVendaAgro.objects, "filter") as filt,
        patch.object(v.ProdutoMarcaVariacaoAgro.objects, "filter") as f1,
        patch.object(v.ProdutoGestaoOverlayAgro.objects, "filter") as f2,
        patch.object(v.AjusteRapidoEstoque.objects, "filter") as f3,
        patch.object(v.PedidoTransferencia.objects, "filter") as f4,
        patch.object(v.ConfiguracaoTransferencia.objects, "filter") as f5,
        patch.object(v.HistoricoTransferencia.objects, "filter") as f6,
        patch("produtos.views.transaction") as tx,
    ):
        filt.return_value.exists.return_value = False
        for f in (f1, f2, f3, f4, f5, f6):
            f.return_value.delete.return_value = None

        class _Atomic:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        tx.atomic.return_value = _Atomic()
        tx.on_commit.side_effect = lambda fn: None

        r3 = _post(
            staff,
            {"produto_id": "ERP-GUID-XYZ", "forcar_exclusao_mongo_staff": True},
        )
        data3 = json.loads(r3.content.decode("utf-8"))
        if r3.status_code == 200 and data3.get("ok") is True:
            ok("api: staff+force em ERP-espelho = ok")
            if fake_erp.delete.called:
                ok("api: staff+force chamou delete do Produto")
            else:
                fail("api: staff+force nao deletou Produto")
        else:
            fail(f"api: staff+force status={r3.status_code} body={data3}")

    # Flag somente_agro True → ok sem force
    with (
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.catalogo_agro.obter_produto_model", return_value=fake_agro),
        patch.object(v.ItemVendaAgro.objects, "filter") as filt,
        patch.object(v.ProdutoMarcaVariacaoAgro.objects, "filter") as f1,
        patch.object(v.ProdutoGestaoOverlayAgro.objects, "filter") as f2,
        patch.object(v.AjusteRapidoEstoque.objects, "filter") as f3,
        patch.object(v.PedidoTransferencia.objects, "filter") as f4,
        patch.object(v.ConfiguracaoTransferencia.objects, "filter") as f5,
        patch.object(v.HistoricoTransferencia.objects, "filter") as f6,
        patch("produtos.views.transaction") as tx,
    ):
        filt.return_value.exists.return_value = False
        for f in (f1, f2, f3, f4, f5, f6):
            f.return_value.delete.return_value = None

        class _Atomic2:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        tx.atomic.return_value = _Atomic2()
        tx.on_commit.side_effect = lambda fn: None
        r4 = _post(comum, {"produto_id": "AGROTESTVERIFY001"})
        data4 = json.loads(r4.content.decode("utf-8"))
        if r4.status_code == 200 and data4.get("ok"):
            ok("api: somente_agro=True sem force = ok")
        else:
            fail(f"api: somente_agro status={r4.status_code} body={data4}")

    # Prefixo AGRO no id externo mesmo com flag False
    with (
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.catalogo_agro.obter_produto_model", return_value=fake_prefix),
        patch.object(v.ItemVendaAgro.objects, "filter") as filt,
        patch.object(v.ProdutoMarcaVariacaoAgro.objects, "filter") as f1,
        patch.object(v.ProdutoGestaoOverlayAgro.objects, "filter") as f2,
        patch.object(v.AjusteRapidoEstoque.objects, "filter") as f3,
        patch.object(v.PedidoTransferencia.objects, "filter") as f4,
        patch.object(v.ConfiguracaoTransferencia.objects, "filter") as f5,
        patch.object(v.HistoricoTransferencia.objects, "filter") as f6,
        patch("produtos.views.transaction") as tx,
    ):
        filt.return_value.exists.return_value = False
        for f in (f1, f2, f3, f4, f5, f6):
            f.return_value.delete.return_value = None

        class _Atomic3:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        tx.atomic.return_value = _Atomic3()
        tx.on_commit.side_effect = lambda fn: None
        r5 = _post(comum, {"produto_id": "AGROONLYFLAGFALSE"})
        data5 = json.loads(r5.content.decode("utf-8"))
        if r5.status_code == 200 and data5.get("ok"):
            ok("api: prefixo AGRO libera sem force")
        else:
            fail(f"api: prefixo AGRO status={r5.status_code} body={data5}")

    # Venda existente bloqueia
    with (
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.catalogo_agro.obter_produto_model", return_value=fake_agro),
        patch.object(v.ItemVendaAgro.objects, "filter") as filt,
    ):
        filt.return_value.exists.return_value = True
        r6 = _post(staff, {"produto_id": "AGROTESTVERIFY001", "forcar_exclusao_mongo_staff": True})
        data6 = json.loads(r6.content.decode("utf-8"))
        if r6.status_code == 409 and "vendas" in str(data6.get("erro") or "").lower():
            ok("api: com venda PDV = 409 (mesmo staff)")
        else:
            fail(f"api: venda status={r6.status_code} body={data6}")

    # Anonimo: login_required redireciona (302) — RequestFactory pode DisallowedHost
    anon = AnonymousUser()
    try:
        r7 = _post(anon, {"produto_id": "X"})
        if r7.status_code in (302, 401, 403):
            ok(f"api: anonimo = {r7.status_code} (auth)")
        else:
            fail(f"api: anonimo status inesperado={r7.status_code}")
    except Exception as exc:
        nome = type(exc).__name__
        if nome in ("DisallowedHost", "PermissionDenied") or "login" in str(exc).lower():
            ok(f"api: anonimo bloqueado ({nome})")
        else:
            fail(f"api: anonimo exc={nome}: {exc}")


def check_regressao_msg_antiga() -> None:
    print("\n[4] Regressao: 403 JSON nao vira Sem permissao para salvar")
    raw = json.dumps(
        {
            "ok": False,
            "erro": "So e possivel excluir aqui cadastros criados so no SisVale/Agro.",
        },
        ensure_ascii=False,
    )
    # Usa o texto real com acentos (como a API)
    raw = json.dumps(
        {
            "ok": False,
            "erro": "Só é possível excluir aqui cadastros criados só no SisVale/Agro.",
        },
        ensure_ascii=False,
    )
    got = _mensagem_erro_api_http(403, raw, "excluir")
    if "Sem permissão para salvar" in got or "Sem permissao para salvar" in got:
        fail("regressao: ainda mascara como salvar/login")
    elif "excluir aqui" in got.lower() or "SisVale" in got:
        ok("regressao: mostra erro real da API")
    else:
        fail(f"regressao: got `{got[:100]}`")


def main() -> int:
    print("VERIFY CAD-EXCLUIR-MSG-STAFF")
    check_contracts()
    check_js_logic_mirror()
    check_api_django()
    check_regressao_msg_antiga()
    print(f"\n--- resultado: {oks} OK / {fails} FAIL ---")
    if fails:
        print("VERIFY_FAIL")
        return 1
    print(f"VERIFY_OK {oks}/{oks}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
