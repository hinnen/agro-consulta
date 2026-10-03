# -*- coding: utf-8 -*-
"""Prova path FECHAR-CAIXA-LOJA — Vila/Centro auto oculto + Point só pinpad + autosave intacto."""
from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import RequestFactory

from produtos.caixa_util import (
    FORMAS_CONFERENCIA_CAIXA,
    FORMAS_MP_POINT_AUTO_CONFERENCIA,
    filtrar_sessoes_por_deposito,
    forma_fechamento_auto_ocultavel,
    linha_conferencia_caixa_de_pagamento,
    pagamento_linha_eh_mp_point_auto,
    pagamentos_por_linha_conferencia_venda,
    serializar_estado_conferencia_fechar,
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


def _prova_api_estado_nao_mistura_lojas() -> None:
    """Com Centro e Vila abertos, o refresh da Vila não soma o dinheiro do Centro."""

    class _Rel:
        def all(self):
            return []

    def _sess(pk: int, ponto: str, abertura: str):
        return SimpleNamespace(
            pk=pk,
            ponto_caixa=ponto,
            valor_abertura=Decimal(abertura),
            usuario=None,
            usuario_id=None,
            vendas=_Rel(),
            movimentos=_Rel(),
            fechado_em=None,
        )

    s_v = _sess(101, "vila", "333.33")
    s_c = _sess(202, "gaveta", "777.77")
    only_v = filtrar_sessoes_por_deposito([s_v, s_c], "vila")
    only_c = filtrar_sessoes_por_deposito([s_v, s_c], "centro")
    check(only_v == [s_v], "filtro deposito vila so vila")
    check(only_c == [s_c], "filtro deposito centro so gaveta")

    qs = MagicMock()
    qs.select_related.return_value = qs
    qs.prefetch_related.return_value = qs
    qs.order_by.return_value = [s_v, s_c]
    rf = RequestFactory()
    req = rf.get("/api/caixa/conferencia-estado/", {"escopo": "loja"})
    req.user = SimpleNamespace(is_authenticated=True, pk=1)
    req.session = {}

    def _chama(dep: str, escopo: str):
        r = rf.get("/api/caixa/conferencia-estado/", {"escopo": escopo})
        r.user = req.user
        r.session = {}
        with (
            patch("produtos.views.SessaoCaixa.objects.filter", return_value=qs),
            patch("produtos.views.deposito_caixa_browser", return_value=dep),
        ):
            return api_caixa_conferencia_estado(r)

    try:
        resp = _chama("vila", "loja")
        data = json.loads(resp.content.decode())
        ids = {int(c.get("sessao_id") or 0) for c in (data.get("cards") or [])}
        tot = Decimal(str(data.get("tot_esperado_dinheiro") or "0"))
        check(data.get("ok") is True, "api loja vila ok")
        check(s_v.pk in ids, "api loja vila inclui sessao vila")
        check(s_c.pk not in ids, "api loja vila nao inclui gaveta centro")
        check(tot == Decimal("333.33"), "api vila esperado so 333.33")

        resp2 = _chama("centro", "operacional")
        data2 = json.loads(resp2.content.decode())
        ids2 = {int(c.get("sessao_id") or 0) for c in (data2.get("cards") or [])}
        tot2 = Decimal(str(data2.get("tot_esperado_dinheiro") or "0"))
        check(s_c.pk in ids2, "api operacional centro inclui gaveta")
        check(s_v.pk not in ids2, "api operacional centro nao inclui vila")
        check(tot2 == Decimal("777.77"), "api centro esperado so 777.77")
    except Exception as exc:
        fail(f"api estado loja: {exc}")


def main() -> int:
    html = (ROOT / "produtos/templates/produtos/caixa_fechar.html").read_text(encoding="utf-8")
    inc = (ROOT / "produtos/templates/produtos/includes/caixa_fechar_linha_conf.html").read_text(
        encoding="utf-8"
    )
    util = (ROOT / "produtos/caixa_util.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")

    check("agendarSalvarContagem" in html, "autosave agendarSalvarContagem")
    check("api_rascunho_salvar_url" in html, "autosave api rascunho")
    check("persistirContagemLocalAgora" in html, "autosave local")
    check("_caixa_contagem_pg_salvar" in views, "autosave PG helper")
    check("linhas_ocultas" in views and "linhas_visiveis" in views, "view split ocultas")
    check("cf-auto-bloco" in html, "UI bloco expandir")
    check("data-auto-contado" in inc, "include auto attr")
    check("aplicarAutoContadoEsperado" in html, "JS auto fill")
    check("pagamento_linha_eh_mp_point_auto" in util, "point auto helper")
    check("Fiado" in FORMAS_CONFERENCIA_CAIXA, "Fiado na lista conferencia")
    check("escopo=loja" in html, "refresh conferencia filtra loja")
    check("filtrar_sessoes_por_deposito" in views, "api estado filtra deposito")
    js_repasse = (ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js").read_text(
        encoding="utf-8"
    )
    check(
        "agro-caixa-fechar-atualizar" in js_repasse,
        "repasse avisa tela fechar",
    )
    check("notifyParentFecharAtualizar" in js_repasse, "repasse notify parent fn")

    check(pagamento_linha_eh_mp_point_auto({"maquinaId": "mp_balcao"}), "mp_balcao = point")
    check(pagamento_linha_eh_mp_point_auto({"maquinaId": "pix_mp_qr"}), "pix_mp_qr = point")
    check(not pagamento_linha_eh_mp_point_auto({"maquinaId": "mp_vila"}), "mp_vila manual != point")
    check(
        pagamento_linha_eh_mp_point_auto({"maquinaId": "mp_vila", "cobrarNoPointMp": True}),
        "mp_vila com marcador = point",
    )
    check(not pagamento_linha_eh_mp_point_auto({"maquinaId": "pix_mp_vila"}), "pix_mp_vila manual != point")
    check(not pagamento_linha_eh_mp_point_auto({"maquinaId": "mp_renan"}), "mp_renan != point")
    check(
        linha_conferencia_caixa_de_pagamento("PIX", mercado_pago=False) == "PIX",
        "PIX unico campo",
    )
    check(
        linha_conferencia_caixa_de_pagamento("PIX", mercado_pago=True) == "Pix — Mercado Pago",
        "PIX point label",
    )

    class _V:
        pk = 1
        pagamentos_json = [
            {"forma": "PIX", "valor": 10, "maquinaId": "pix_mp_qr"},
            {"forma": "PIX", "valor": 5, "maquinaId": "pix_cielo"},
            {"forma": "PIX", "valor": 3, "maquinaId": "pix_mp_renan"},
        ]
        forma_pagamento = "PIX"
        total = 18
        devolvida_em = None

    por = pagamentos_por_linha_conferencia_venda(_V())
    check(por.get("Pix — Mercado Pago") == __import__("decimal").Decimal("10.00"), "split so point")
    check(por.get("PIX") == __import__("decimal").Decimal("8.00"), "cielo+renan no PIX unico")

    check(forma_fechamento_auto_ocultavel("Fiado", deposito="vila"), "vila fiado auto")
    check(forma_fechamento_auto_ocultavel("Cashback", deposito="centro"), "centro cashback auto")
    check(
        forma_fechamento_auto_ocultavel("Pix — Mercado Pago", deposito="centro"),
        "centro point auto",
    )
    check(
        forma_fechamento_auto_ocultavel("Pix — Mercado Pago", deposito="vila"),
        "vila point auto",
    )

    st_v = serializar_estado_conferencia_fechar([], deposito="vila")
    formas_v = [L["forma"] for L in st_v["linhas"]]
    check("Pix — Mercado Pago" in formas_v, "vila tem linhas point")
    check(any(L["forma"] == "Fiado" and L.get("grupo_oculto") for L in st_v["linhas"]), "vila fiado oculto")

    st_c = serializar_estado_conferencia_fechar([], deposito="centro")
    check(
        any(
            L["forma"] in FORMAS_MP_POINT_AUTO_CONFERENCIA and L.get("auto_contado")
            for L in st_c["linhas"]
        ),
        "centro point no bloco auto",
    )
    check(any(L["forma"] == "PIX" and not L.get("grupo_oculto") for L in st_c["linhas"]), "centro PIX visivel")

    _prova_api_estado_nao_mistura_lojas()

    print(f"---\noks={OKS} fails={len(FAILS)}")
    if FAILS:
        for f in FAILS:
            print(" ", f)
        return 1
    print("VERIFY_FECHAR_LOJA_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
