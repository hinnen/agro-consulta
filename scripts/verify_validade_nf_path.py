"""
Verificação do path Validade / Entrada NF → EstoqueLote / filtro loja / BCA.
Roda: python scripts/verify_validade_nf_path.py
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.contrib.messages.storage.fallback import FallbackStorage
from django.contrib.sessions.backends.db import SessionStore
from django.test import RequestFactory

from produtos.models import (
    EstoqueLote,
    ProdutoGestaoOverlayAgro,
    parse_data_validade_entrada_nf,
    registrar_lote_validade_apos_entrada_nf,
    reduzir_lote_validade_estorno_entrada_nf,
)
from produtos.views import relatorios_validade

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


def test_parse_datas() -> None:
    print("\n== parse_data_validade_entrada_nf ==")
    check(parse_data_validade_entrada_nf("2027-07-30") == date(2027, 7, 30), "ISO AAAA-MM-DD")
    check(parse_data_validade_entrada_nf("30/07/2027") == date(2027, 7, 30), "BR DD/MM/AAAA")
    check(parse_data_validade_entrada_nf("") is None, "vazio = None")
    check(parse_data_validade_entrada_nf("lixo") is None, "invalido = None")


def test_registrar_e_estorno() -> None:
    print("\n== registrar_lote + deposito + estorno ==")
    pid = "VERIFYVALPATH000000000001"
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).delete()
    ln = {"lote_numero": "VT-01", "lote_validade": "2028-06-15", "x_prod": "Verify Val Path"}
    info = registrar_lote_validade_apos_entrada_nf(
        pid, ln, Decimal("3"), nome_produto="Verify Val Path", deposito="centro"
    )
    check(info is not None and info.get("deposito") == "centro", "cria lote centro")
    ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id=pid)
    el = EstoqueLote.objects.get(overlay=ov, lote_codigo="VT-01")
    check(el.deposito == "centro", "campo deposito=centro")
    check(float(el.quantidade_atual) == 3.0, "qtd=3")
    check(str(ov.cadastro_extras.get("validade") or "")[:10] == "2028-06-15", "extras.validade sync")

    info2 = registrar_lote_validade_apos_entrada_nf(
        pid, ln, Decimal("2"), nome_produto="Verify Val Path", deposito="centro"
    )
    el.refresh_from_db()
    check(float(el.quantidade_atual) == 5.0, "soma qtd no mesmo lote = 5")

    reduzir_lote_validade_estorno_entrada_nf(
        pid, lote_codigo="VT-01", data_validade=date(2028, 6, 15), qtd=Decimal("2")
    )
    el.refresh_from_db()
    check(float(el.quantidade_atual) == 3.0, "estorno -2 = 3")

    reduzir_lote_validade_estorno_entrada_nf(
        pid, lote_codigo="VT-01", data_validade=date(2028, 6, 15), qtd=Decimal("99")
    )
    check(not EstoqueLote.objects.filter(overlay=ov, lote_codigo="VT-01").exists(), "estorno zera e apaga")
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).delete()


def _render_loja(loja: str) -> tuple[int, str]:
    User = get_user_model()
    u = User.objects.filter(is_superuser=True).first() or User.objects.first()
    rf = RequestFactory()
    req = rf.get(
        "/relatorios/validade/",
        {"loja": loja, "somente_com_estoque": "1"},
        HTTP_HOST="127.0.0.1",
    )
    req.user = u
    req.session = SessionStore()
    setattr(req, "_messages", FallbackStorage(req))
    resp = relatorios_validade(req)
    html = resp.content.decode("utf-8", "replace")
    return html.count("data-pid="), html


def test_relatorio_filtro_loja() -> None:
    print("\n== relatório filtro loja + colunas ==")
    pid_c = "VERIFYVALCENTRO0000000001"
    pid_v = "VERIFYVALVILA000000000001"
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__in=[pid_c, pid_v]).delete()

    registrar_lote_validade_apos_entrada_nf(
        pid_c,
        {"lote_numero": "C1", "lote_validade": (date.today() + timedelta(days=60)).isoformat()},
        Decimal("4"),
        nome_produto="Verify Centro Only",
        deposito="centro",
    )
    registrar_lote_validade_apos_entrada_nf(
        pid_v,
        {"lote_numero": "V1", "lote_validade": (date.today() + timedelta(days=90)).isoformat()},
        Decimal("7"),
        nome_produto="Verify Vila Only",
        deposito="vila",
    )

    n_todas, html_t = _render_loja("todas")
    n_centro, html_c = _render_loja("centro")
    n_vila, html_v = _render_loja("vila")

    check(n_todas >= 2, f"Todas tem linhas (n={n_todas})")
    check("Verify Centro Only" in html_t and "Verify Vila Only" in html_t, "Todas mostra centro+vila")
    check("Verify Centro Only" in html_c, "Centro mostra lote centro")
    check("Verify Vila Only" not in html_c, "Centro NÃO mostra lote vila")
    check("Verify Vila Only" in html_v, "Vila mostra lote vila")
    check("Verify Centro Only" not in html_v, "Vila NÃO mostra lote centro")
    check(">Centro<" in html_t and ">Vila<" in html_t, "colunas Centro/Vila no HTML")
    check("Buscar produto (BCA)" in html_t or "BUSCAR PRODUTO (BCA)" in html_t.upper(), "busca BCA na tela")
    check("data-rv-draft" in open(
        ROOT / "produtos/templates/produtos/relatorios_validade.html", encoding="utf-8"
    ).read(), "template tem linha draft BCA")

    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__in=[pid_c, pid_v]).delete()


def test_migration_and_files() -> None:
    print("\n== arquivos / migrate ==")
    from django.db import connection

    cols = {c.name for c in connection.introspection.get_table_description(connection.cursor(), "produtos_estoquelote")}
    check("deposito" in cols, "coluna deposito no PG/SQLite")
    mig = ROOT / "produtos/migrations/0086_estoque_lote_deposito.py"
    check(mig.is_file(), "migration 0086 existe")
    from produtos.views import aplicar_entrada_nota_estoque_agro
    import inspect

    src = inspect.getsource(aplicar_entrada_nota_estoque_agro)
    check("registrar_lote_validade_apos_entrada_nf" in src and "deposito=dep" in src, "entrada NF passa deposito")


def main() -> int:
    print("VERIFY validade NF path")
    test_parse_datas()
    test_registrar_e_estorno()
    test_relatorio_filtro_loja()
    test_migration_and_files()
    print(f"\n== RESULTADO {PASS}/{PASS + FAIL} ==")
    if FAIL:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
