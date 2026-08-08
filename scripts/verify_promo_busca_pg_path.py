"""
Verificacao detalhada: busca de produto na promocao (PROMO-BUSCA-PG).
Path: tela etapa 2 → GET /api/promocoes/buscar-produto/ → buscar_produtos_para_promocao
      → buscar_produtos_motor_pdv → catalogo_agro (agro_pg) / Mongo legado.
Roda: python scripts/verify_promo_busca_pg_path.py
"""
from __future__ import annotations

import ast
import os
import re
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client
from django.urls import reverse

from produtos.agro_fonte_config import agro_catalogo_usa_postgres, agro_pdv_catalogo_somente_postgres
from produtos.busca_produtos_mongo import buscar_produtos_motor_pdv
from produtos.promocoes_util import buscar_produtos_para_promocao

PASS = 0
FAIL = 0


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print(f"  OK  {msg}")


def bad(msg: str) -> None:
    global FAIL
    FAIL += 1
    print(f"  FAIL {msg}")


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        bad(msg)


def test_arquivos_path() -> None:
    print("\n== Path arquivos ==")
    util = (ROOT / "produtos/promocoes_util.py").read_text(encoding="utf-8")
    motor = (ROOT / "produtos/busca_produtos_mongo.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/promocoes_views.py").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    form = (ROOT / "produtos/templates/produtos/promocoes_form.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/templates/produtos/includes/promocoes_form_script.html").read_text(
        encoding="utf-8"
    )

    check("agro_catalogo_usa_postgres" in util, "util importa flag agro_pg")
    check("if usa_pg:" in util, "util tem ramo Postgres")
    tree = ast.parse(util)
    fn = next(
        (n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "buscar_produtos_para_promocao"),
        None,
    )
    check(fn is not None, "buscar_produtos_para_promocao existe")
    if fn is not None:
        src = ast.get_source_segment(util, fn) or ""
        check("if usa_pg:" in src, "ramo if usa_pg na funcao")
        check(
            "if db is None or client is None:" in src and "else:" in src,
            "early-return Mongo so no legado",
        )
        idx_pg = src.find("if usa_pg:")
        idx_mongo = src.find("obter_conexao_mongo")
        check(idx_pg >= 0 and (idx_mongo < 0 or idx_pg < idx_mongo), "PG antes de Mongo na funcao")

    check("prods_mongo_style_busca_pdv" in motor, "motor usa catalogo_agro no agro_pg")
    check("agro_catalogo_usa_postgres" in motor, "motor checa agro_pg")
    check("motor_busca_consulta_documentos" in motor, "legado Mongo preservado")

    check("def api_promocoes_buscar_produto" in views, "view API existe")
    check("buscar_produtos_para_promocao(q, limit=24)" in views, "view chama util limit=24")
    check("api/promocoes/buscar-produto/" in urls, "rota API no urls")
    check("name='api_promocoes_buscar_produto'" in urls or 'name="api_promocoes_buscar_produto"' in urls, "nome da rota")

    check("api_buscar_produto_url" in form, "template passa URL da API promo")
    check("promocoes_form_script.html" in form, "tela usa script inline (nao static js)")
    check("cfg.apiBuscar" in js, "JS inline usa cfg.apiBuscar")
    check("/api/promocoes/buscar-produto/" in js, "fallback JS aponta API promo")
    check("d.produtos" in js, "JS le data.produtos")
    check("produto_externo_id" in js, "JS usa produto_externo_id")
    check("Nenhum produto para" in js, "mensagem vazio na tela")
    check("Nada para" in js, "painel vazio na tela")


def test_js_gm_hifen() -> None:
    print("\n== JS codigo GM com hifen ==")
    js = (ROOT / "produtos/templates/produtos/includes/promocoes_form_script.html").read_text(
        encoding="utf-8"
    )
    check(r"^GM\d+(-\d+)?$" in js or r"^GM\d+(-\d+)?$/i" in js, "ehCodigoGm aceita GM1507-30")
    check(r"^GM\d{3,}(-\d+)?$" in js, "codigoGmCompleto aceita GM1507-30")
    check("GM1507" in js or r"-\d+" in js, "regex de variante com hifen presente")
    check(not re.search(r"ehCodigoGm\([\s\S]*?\^GM\\d\+\$", js), "nao ficou so GM+digitos sem hifen")


def test_flags() -> None:
    print("\n== Flags catalogo ==")
    check(agro_catalogo_usa_postgres(), "AGRO_FONTE_CATALOGO=agro_pg neste PC")
    check(not agro_pdv_catalogo_somente_postgres() or agro_catalogo_usa_postgres(), "flag PDV sozinho ou agro_pg")


def test_util_local() -> None:
    print("\n== Util catalogo local ==")
    check(buscar_produtos_para_promocao("") == [], "q vazio = []")
    check(buscar_produtos_para_promocao("a") == [], "q 1 char = []")

    r = buscar_produtos_para_promocao("GM1507-30")
    check(len(r) >= 1, f"GM1507-30 achou {len(r)}")
    if r:
        check(str(r[0].get("codigo") or "").upper() == "GM1507-30", f"codigo={r[0].get('codigo')}")
        nome = str(r[0].get("nome_produto") or "").lower()
        check("farelo" in nome and "trigo" in nome, f"nome={r[0].get('nome_produto')}")
        check(bool(str(r[0].get("produto_externo_id") or "").strip()), "tem produto_externo_id")
        check(float(r[0].get("preco_padrao") or 0) > 0, f"preco={r[0].get('preco_padrao')}")

    r_low = buscar_produtos_para_promocao("gm1507-30")
    check(len(r_low) >= 1, f"gm minusculo achou {len(r_low)}")

    r_nome = buscar_produtos_para_promocao("farelo")
    check(len(r_nome) >= 1, f"nome farelo achou {len(r_nome)}")
    if r_nome:
        check(any("farelo" in str(x.get("nome_produto") or "").lower() for x in r_nome), "resultado tem farelo no nome")

    r_trigo = buscar_produtos_para_promocao("farelo de trigo")
    check(len(r_trigo) >= 1, f"farelo de trigo achou {len(r_trigo)}")

    motor = buscar_produtos_motor_pdv("GM1507-30", limit=40)
    check(len(motor) >= 1, f"motor_pdv GM1507-30 = {len(motor)}")


def test_mock_sem_mongo() -> None:
    print("\n== Mock: agro_pg nao abre Mongo ==")
    fake = [
        {
            "Id": "id-mock",
            "Codigo": "GM1507-30",
            "CodigoNFe": "GM1507-30",
            "Nome": "Racao farelo de trigo 30kg",
            "ValorVenda": 60,
        }
    ]
    with (
        patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
        patch("produtos.agro_fonte_config.agro_pdv_catalogo_somente_postgres", return_value=False),
        patch("produtos.busca_produtos_mongo.buscar_produtos_motor_pdv", return_value=fake),
        patch("produtos.views.obter_conexao_mongo") as mongo,
    ):
        out = buscar_produtos_para_promocao("GM1507-30")
    check(mongo.call_count == 0, "obter_conexao_mongo nao chamado no agro_pg")
    check(len(out) == 1 and out[0]["codigo"] == "GM1507-30", "mock devolve payload da tela")


def test_api_http() -> None:
    print("\n== API HTTP ==")
    User = get_user_model()
    user = User.objects.filter(is_active=True).order_by("pk").first()
    if user is None:
        bad("nenhum usuario ativo para logar no Client")
        return
    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(user)
    url = reverse("api_promocoes_buscar_produto")
    check(url.rstrip("/").endswith("buscar-produto"), f"reverse={url}")

    r0 = c.get(url, {"q": "a"})
    check(r0.status_code == 200, f"q=a status {r0.status_code}")
    check(r0.json().get("produtos") == [], "q=a = produtos []")

    r1 = c.get(url, {"q": "GM1507-30"})
    check(r1.status_code == 200, f"GM1507-30 status {r1.status_code}")
    prods = (r1.json() or {}).get("produtos") or []
    check(len(prods) >= 1, f"API GM1507-30 = {len(prods)}")
    if prods:
        check(str(prods[0].get("codigo") or "").upper() == "GM1507-30", f"API codigo={prods[0].get('codigo')}")
        check(bool(prods[0].get("produto_externo_id")), "API tem produto_externo_id")
        check("nome_produto" in prods[0], "API tem nome_produto")
        check("preco_padrao" in prods[0], "API tem preco_padrao")

    r2 = c.get(url, {"q": "farelo"})
    check(r2.status_code == 200, f"farelo status {r2.status_code}")
    check(len((r2.json() or {}).get("produtos") or []) >= 1, "API farelo achou")

    r3 = c.get(url)
    check(r3.status_code == 200 and (r3.json() or {}).get("produtos") == [], "sem q = []")


def main() -> int:
    print("PROMO-BUSCA-PG — verificacao detalhada")
    test_arquivos_path()
    test_js_gm_hifen()
    test_flags()
    test_util_local()
    test_mock_sem_mongo()
    test_api_http()
    print(f"\n{PASS}/{PASS + FAIL} checks")
    if FAIL:
        print("FALHOU")
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
