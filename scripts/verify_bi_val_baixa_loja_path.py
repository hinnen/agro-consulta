"""
Verificacao BI-VAL-BAIXA-LOJA — baixa no Centro nao zera a Vila.
  .venv\\Scripts\\python.exe scripts/verify_bi_val_baixa_loja_path.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import RequestFactory
from django.urls import reverse
from django.utils import timezone

from produtos.models import EstoqueLote, ProdutoGestaoOverlayAgro
from produtos.views import (
    VALIDADE_DASHBOARD_CACHE_KEY,
    _aplicar_baixa_operacional_vencimento_loja,
    _contagem_validade_dashboard_lotes_agro,
    _invalidar_cache_dashboard_perdas_validade,
    api_relatorio_validade_baixa,
    relatorios_validade,
)

PREFIX = "VERIFYBIVALBX"
PASS = FAIL = 0


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print(f"  OK  {msg}")


def bad(msg: str) -> None:
    global FAIL
    FAIL += 1
    print(f"  FAIL {msg}")


def check(cond: bool, msg: str, detail: str = "") -> None:
    if cond:
        ok(msg + (f" — {detail}" if detail else ""))
    else:
        bad(msg + (f" — {detail}" if detail else ""))


def wipe() -> None:
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__startswith=PREFIX).delete()
    cache.clear()


def _user() -> User:
    user, _ = User.objects.get_or_create(username="verify_bi_val_bx")
    return user


def _post_baixa(lote_id: int, deposito: str, *, saldo: Decimal = Decimal("0")):
    rf = RequestFactory()
    req = rf.post(
        "/api/relatorio-validade-baixa/",
        data=json.dumps({"lote_id": lote_id, "deposito": deposito}),
        content_type="application/json",
        HTTP_HOST="127.0.0.1",
    )
    req.user = _user()
    mock_adj = MagicMock(return_value=(True, None))
    with patch("produtos.views.obter_conexao_mongo", return_value=(object(), object())):
        with patch("produtos.views._saldo_erp_produto_deposito_mongo", return_value=Decimal("0")):
            with patch("produtos.views._saldo_final_agro_com_pin", return_value=saldo):
                with patch("produtos.views._produto_mongo_por_id_externo", return_value=None):
                    with patch(
                        "produtos.views._aplicar_baixa_operacional_vencimento_loja",
                        mock_adj,
                    ):
                        resp = api_relatorio_validade_baixa(req)
    data = json.loads(resp.content.decode("utf-8"))
    return resp, data, mock_adj


def _html_relatorio(loja: str, status: str = "vencido") -> str:
    rf = RequestFactory()
    url = reverse("relatorios_validade") + f"?loja={loja}&status={status}"
    req = rf.get(url, HTTP_HOST="127.0.0.1")
    resp = relatorios_validade(req)
    return resp.content.decode("utf-8", errors="replace")


def test_fonte() -> None:
    print("\n== Codigo / migrate / modelo ==")
    src = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check("v7" in VALIDADE_DASHBOARD_CACHE_KEY, "cache v7")
    check("somente_deposito" in src, "baixa estoque so da loja")
    check("baixado_centro_em" in src and "baixado_vila_em" in src, "flags por loja")
    check("A outra loja continua vendo" in src, "mensagem outra loja")
    check("Este lote já foi conferido nesta loja" in src, "409 duplicata")
    check("if filtro_loja == \"centro\" and bc:" in src, "relatorio esconde baixado no Centro")
    check("if filtro_loja == \"vila\" and bv:" in src, "relatorio esconde baixado na Vila")
    tpl = (ROOT / "produtos/templates/produtos/relatorios_validade.html").read_text(
        encoding="utf-8"
    )
    check('deposito: "{{ deposito_baixa|escapejs }}"' in tpl, "JS envia deposito")
    check("Dar baixa só nesta loja" in tpl, "confirm so nesta loja")
    check("Conferido aqui" in tpl, "badge conferido nesta loja")
    mig = ROOT / "produtos/migrations/0096_estoque_lote_baixa_por_loja.py"
    check(mig.is_file(), "migrate 0096")
    fields = {f.name for f in EstoqueLote._meta.get_fields()}
    check("baixado_centro_em" in fields and "baixado_vila_em" in fields, "campos no modelo")


def test_contagem_apos_baixa_centro() -> None:
    print("\n== Contagem: Centro baixou, Vila permanece ==")
    wipe()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}A", nome="Venc A"
    )
    el = EstoqueLote.objects.create(
        overlay=ov,
        lote_codigo="BXA",
        data_validade=date(2026, 7, 1),
        quantidade_atual=Decimal("2"),
    )
    cache.clear()
    a_all = _contagem_validade_dashboard_lotes_agro(None)
    a_c = _contagem_validade_dashboard_lotes_agro("centro")
    a_v = _contagem_validade_dashboard_lotes_agro("vila")
    check(a_all == a_c == a_v, "antes da baixa tres iguais", str(a_all))
    check(a_v["vencidos"] >= 1, "vila via vencido antes")

    el.baixado_centro_em = timezone.now()
    el.save(update_fields=["baixado_centro_em"])
    cache.clear()
    d_all = _contagem_validade_dashboard_lotes_agro(None)
    d_c = _contagem_validade_dashboard_lotes_agro("centro")
    d_v = _contagem_validade_dashboard_lotes_agro("vila")
    check(d_c["vencidos"] == a_c["vencidos"] - 1, "centro caiu 1", str(d_c))
    check(d_v["vencidos"] == a_v["vencidos"], "vila inalterada", str(d_v))
    check(d_all["vencidos"] == a_all["vencidos"], "C+V inalterado", str(d_all))
    check(EstoqueLote.objects.filter(pk=el.pk).exists(), "lote nao apagado")

    el.baixado_vila_em = timezone.now()
    el.save(update_fields=["baixado_vila_em"])
    cache.clear()
    e_c = _contagem_validade_dashboard_lotes_agro("centro")
    e_v = _contagem_validade_dashboard_lotes_agro("vila")
    check(e_c["vencidos"] == a_c["vencidos"] - 1, "centro segue sem este overlay")
    check(e_v["vencidos"] == a_v["vencidos"] - 1, "vila caiu apos a 2a baixa")
    wipe()


def test_api_baixa_centro_preserva_lote() -> None:
    print("\n== API: baixa Centro nao apaga lote ==")
    wipe()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}API", nome="API"
    )
    el = EstoqueLote.objects.create(
        overlay=ov,
        lote_codigo="API1",
        data_validade=date(2026, 6, 1),
        quantidade_atual=Decimal("3"),
    )
    resp, data, mock_adj = _post_baixa(el.pk, "centro", saldo=Decimal("0"))
    check(resp.status_code == 200, f"HTTP {resp.status_code}")
    check(data.get("ok") is True, "ok True", str(data)[:180])
    check(data.get("loja") == "centro", "loja=centro")
    check(data.get("apagou_lote") is False, "nao apagou lote")
    check(mock_adj.call_count == 0, "sem estoque nesta loja: nao mexe C+V")
    el.refresh_from_db()
    check(el.baixado_centro_em is not None, "flag centro marcada")
    check(el.baixado_vila_em is None, "flag vila vazia")
    check(el.quantidade_atual == Decimal("3.00"), "qtd preservada")

    resp_dup, data_dup, _ = _post_baixa(el.pk, "centro", saldo=Decimal("0"))
    check(resp_dup.status_code == 409, f"duplicata HTTP {resp_dup.status_code}")
    check("já foi conferido" in str(data_dup.get("erro") or ""), "erro duplicata")

    _, data2, mock2 = _post_baixa(el.pk, "vila", saldo=Decimal("0"))
    check(data2.get("ok") is True, "2a baixa Vila ok")
    check(data2.get("apagou_lote") is True, "apagou so quando as duas conferiram")
    check(mock2.call_count == 0, "Vila sem estoque: nao mexe Centro")
    check(not EstoqueLote.objects.filter(pk=el.pk).exists(), "lote removido apos as duas")
    wipe()


def test_api_estoque_so_da_loja() -> None:
    print("\n== API: estoque so da loja que baixou ==")
    wipe()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}STK", nome="STK"
    )
    el = EstoqueLote.objects.create(
        overlay=ov,
        lote_codigo="STK1",
        data_validade=date(2026, 5, 1),
        quantidade_atual=Decimal("2"),
    )
    _, data, mock_adj = _post_baixa(el.pk, "centro", saldo=Decimal("5"))
    check(data.get("ok") is True, "baixa com saldo Centro ok")
    check(mock_adj.call_count == 1, "chamou ajuste 1x")
    kwargs = mock_adj.call_args.kwargs if mock_adj.call_args else {}
    check(kwargs.get("somente_deposito") == "centro", "somente_deposito=centro")
    check(kwargs.get("quantidade") == Decimal("2.000") or kwargs.get("quantidade") == Decimal("2"), "qtd=min(lote,saldo)")
    wipe()


def test_aplicar_nao_toca_outra_loja() -> None:
    print("\n== Aplicar baixa: so o deposito pedido ==")
    deps_vistos: list[str] = []

    def fake_saldo_erp(_db, _client, _pid, dep):
        return Decimal("0")

    def fake_saldo_pin(_pid, dep, _erp):
        deps_vistos.append(dep)
        return Decimal("10")

    with patch("produtos.views._saldo_erp_produto_deposito_mongo", side_effect=fake_saldo_erp):
        with patch("produtos.views._saldo_final_agro_com_pin", side_effect=fake_saldo_pin):
            with patch("produtos.views._empresa_loja_padrao_agro_estoque", return_value=(None, None)):
                with patch("produtos.views.AjusteRapidoEstoque.objects.create"):
                    ok_adj, err = _aplicar_baixa_operacional_vencimento_loja(
                        db=object(),
                        client_m=object(),
                        produto_externo_id=f"{PREFIX}PIN",
                        quantidade=Decimal("1"),
                        usuario_django=None,
                        nome_produto="X",
                        codigo_interno="",
                        somente_deposito="centro",
                    )
    check(ok_adj is True and err is None, "aplicar centro ok")
    check(deps_vistos == ["centro"], "nao consultou saldo da Vila", str(deps_vistos))


def test_relatorio_esconde_so_na_loja() -> None:
    print("\n== Relatorio: some no Centro, fica na Vila/Todas ==")
    wipe()
    codigo = f"{PREFIX}REL1"
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}REL", nome="Rel lote"
    )
    el = EstoqueLote.objects.create(
        overlay=ov,
        lote_codigo=codigo,
        data_validade=date(2026, 4, 1),
        quantidade_atual=Decimal("1"),
        deposito="centro",
    )
    html_todas = _html_relatorio("todas")
    check(codigo in html_todas, "Todas mostra lote antes")
    el.baixado_centro_em = timezone.now()
    el.save(update_fields=["baixado_centro_em"])
    html_c = _html_relatorio("centro")
    html_t = _html_relatorio("todas")
    check(codigo not in html_c, "Centro nao lista lote ja conferido")
    check(codigo in html_t, "Todas ainda lista (Vila nao conferiu)")
    wipe()


def test_cache_invalida() -> None:
    print("\n== Cache invalida nas 3 chaves ==")
    hoje = timezone.localdate().isoformat()
    for suf in ("all", "centro", "vila"):
        cache.set(f"{VALIDADE_DASHBOARD_CACHE_KEY}:{hoje}:{suf}", {"vencidos": 99}, 60)
    _invalidar_cache_dashboard_perdas_validade()
    vivos = [
        suf
        for suf in ("all", "centro", "vila")
        if cache.get(f"{VALIDADE_DASHBOARD_CACHE_KEY}:{hoje}:{suf}")
    ]
    check(not vivos, "apagou cache all/centro/vila", str(vivos))


def main() -> int:
    print("VERIFY BI-VAL-BAIXA-LOJA path")
    test_fonte()
    test_contagem_apos_baixa_centro()
    test_api_baixa_centro_preserva_lote()
    test_api_estoque_so_da_loja()
    test_aplicar_nao_toca_outra_loja()
    test_relatorio_esconde_so_na_loja()
    test_cache_invalida()
    print(f"\n== RESULTADO {PASS}/{PASS + FAIL} ==")
    if FAIL:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK bi_val_baixa_loja")
    return 0


if __name__ == "__main__":
    sys.exit(main())
