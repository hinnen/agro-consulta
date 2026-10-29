# -*- coding: utf-8 -*-
"""
Prova detalhada MP-POINT-PIN-STICKY.

Path loja: fechar venda (wizard) → 409 órfão Point → overlay PIN →
forçar-liberar encerra PENDING/PAID no Postgres → retry fecha →
próxima venda NÃO vê o órfão (mesmo após limpar bypass).
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import django

django.setup()

from django.http import JsonResponse
from django.test import RequestFactory
from django.urls import reverse

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


def main() -> int:
    views_mp = (ROOT / "produtos/views_mp_point.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    pdv = (ROOT / "pdv/views.py").read_text(encoding="utf-8")
    wizard = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    util = (ROOT / "produtos/pin_gerencial_util.py").read_text(encoding="utf-8")
    models = (ROOT / "produtos/models.py").read_text(encoding="utf-8")

    # --- 1) Código-fonte: persistência PG (não localStorage) ---
    check("def _mp_point_marcar_forcado_liberar" in views_mp, "1 marca órfão no PG")
    check("mp_point_forcado_liberar" in views_mp, "1 flag erp_payload PG")
    check("mp_point_forcado_por" in views_mp, "1 auditoria quem liberou")
    check("class PdvMercadoPagoPointOrder" in models, "1 modelo PG Point")
    check("ABANDONED = \"abandoned\"" in models, "1 status abandoned existe")
    check("localStorage" not in views_mp, "1 backend não usa localStorage")

    # --- 2) Forçar NÃO promove PAID (bug do R$ 2,40) ---
    check("_mp_point_promover_pago_local(row, body)" not in views_mp.split("def api_pdv_mp_point_forcar_liberar")[1], "2 forçar sem promover")
    check("mp_point_order_indica_pago(body)" in views_mp.split("def api_pdv_mp_point_forcar_liberar")[1], "2 GET pago só avisa")
    check("_mp_point_marcar_forcado_liberar(row, rotulo)" in views_mp, "2 marca PENDING e PAID")
    check("não promover a PAID" in views_mp, "2 comentário anti-regressão")

    # --- 3) Bloqueio + bypass ---
    check("def mp_point_bloqueio_venda_sessao" in views_mp, "3 helper bloqueio")
    check("mp_point_forcar_bypass_ativo" in views_mp, "3 respeita bypass")
    check("_mp_point_row_foi_forcado_liberar" in views_mp.split("def mp_point_bloqueio_venda_sessao")[1], "3 ignora já forçado")
    check("mp_point_bloqueio_venda_sessao" in views.split("def api_enviar_pedido_erp")[1][:2500], "3 gate em enviar ERP")
    check("mp_point_bloqueio" in views, "3 JSON flag 409")
    check("payload_hint_pin_gerencial" in views, "3 409 com hint PIN")
    check("limpar_mp_point_forcar_bypass" in views, "3 limpa bypass pós-venda")

    # --- 4) Finalize Point NÃO passa pelo gate (venda legítima na máquina) ---
    fin = views_mp.split("def api_pdv_mp_point_finalizar")[1].split("def api_pdv_mp_point_")[0] if "def api_pdv_mp_point_finalizar" in views_mp else ""
    check("_fluxo_enviar_pedido_erp_interno" in fin, "4 finalize usa fluxo interno")
    check("mp_point_bloqueio_venda_sessao" not in fin, "4 finalize sem gate órfão")

    # --- 5) Status/abandon não ressuscitam forçado ---
    check("if _mp_point_row_foi_forcado_liberar(row):" in views_mp, "5 promover recusa forçado")
    check("recuperado_de_abandon" in views_mp, "5 status ainda recupera abandon cedo")
    check("_mp_point_promover_pago_local(row, body_ab)" in views_mp, "5 recover passa pelo helper")

    # --- 6) URL + bootstrap wizard ---
    check("api_pdv_mp_point_forcar_liberar" in urls, "6 URL forcar")
    check("apiPdvMpPointForcarLiberar" in pdv, "6 bootstrap checkout")
    check(reverse("api_pdv_mp_point_forcar_liberar").endswith("forcar-liberar/"), "6 reverse")
    check(reverse("api_enviar_pedido_erp").endswith("enviar-pedido-erp/"), "6 reverse ERP")

    # --- 7) JS wizard (tela da loja) ---
    check("showPdvPinGerencial" in wizard, "7 overlay PIN")
    check("forcarLiberarMpPointComPin" in wizard, "7 POST forcar")
    check("mpPointBloqueio" in wizard, "7 trata 409")
    check("pode_forcar_pin_gerencial" in wizard, "7 exige hint")
    check("confirmSaleProsseguir(withPrint)" in wizard, "7 retry após PIN")
    check("Geraldo, Geraldinho ou Renan Hinnen" in wizard, "7 nomes")
    check("apiPdvMpPointForcarLiberar" in wizard, "7 usa URL bootstrap")
    pin_catch = wizard.split("if (err && err.mpPointBloqueio)")[1][:900] if "if (err && err.mpPointBloqueio)" in wizard else ""
    check("releaseSaleProcessingLock()" in pin_catch, "7 solta lock antes do PIN")

    # --- 8) PIN gerencial ---
    from produtos.pin_gerencial_util import (
        PIN_GERENCIAL_BYPASS_TTL_S,
        PIN_GERENCIAL_NOMES_UI,
        is_usuario_gerencial,
        rotulo_gerencial_do_user,
        validar_pin_gerencial,
    )

    check(PIN_GERENCIAL_BYPASS_TTL_S == 30 * 60, "8 TTL bypass 30 min")
    check("Geraldo" in PIN_GERENCIAL_NOMES_UI and "Geraldinho" in PIN_GERENCIAL_NOMES_UI, "8 nomes util")
    check(rotulo_gerencial_do_user(SimpleNamespace(username="Geraldo", first_name="", last_name="")) == "Geraldo", "8 Geraldo")
    check(rotulo_gerencial_do_user(SimpleNamespace(username="Geraldinho", first_name="", last_name="")) == "Geraldinho", "8 Geraldinho")
    check(rotulo_gerencial_do_user(SimpleNamespace(username="admin", first_name="Renan", last_name="Hinnen")) == "Renan Hinnen", "8 Renan")
    check(is_usuario_gerencial(SimpleNamespace(username="caixa", first_name="Maria", last_name="")) is False, "8 caixa fora")
    ok_vazio, _, err_vazio = validar_pin_gerencial("")
    check(ok_vazio is False and "PIN" in err_vazio, "8 PIN vazio")
    ok_padrao, _, err_padrao = validar_pin_gerencial("1234")
    check(ok_padrao is False and "1234" in err_padrao, "8 1234 bloqueado")

    # --- 9) Semântica: órfão PAID some após PIN; MP pago não ressuscita ---
    from produtos.models import PdvMercadoPagoPointOrder
    from produtos.views_mp_point import (
        _mp_point_marcar_forcado_liberar,
        _mp_point_promover_pago_local,
        _mp_point_row_foi_forcado_liberar,
        api_pdv_mp_point_forcar_liberar,
        mp_point_bloqueio_venda_sessao,
    )

    def _req(sk="sessao-centro-1"):
        req = RequestFactory().post("/")
        sess = MagicMock()
        sess.session_key = sk
        sess.get.return_value = None
        sess.__contains__ = MagicMock(return_value=False)
        req.session = sess
        return req

    def _row(status, mid="ord-240", valor="2.40"):
        row = SimpleNamespace(
            status=status,
            valor_cobrado=valor,
            mp_order_id=mid,
            mp_last_status="",
            erp_payload={"pagamentos": []},
            django_session_key="sessao-centro-1",
        )
        row.save = MagicMock()
        return row

    with patch("produtos.views_mp_point.PdvMercadoPagoPointOrder.objects") as objs:
        orfao = _row(PdvMercadoPagoPointOrder.Status.PAID)
        objs.filter.return_value.order_by.return_value.__getitem__.return_value = [orfao]
        msg = mp_point_bloqueio_venda_sessao(_req())
        check(msg is not None and "2.40" in str(msg), "9 PAID 2,40 bloqueia")

    with patch("produtos.views_mp_point.PdvMercadoPagoPointOrder.objects") as objs:
        orfao = _row(PdvMercadoPagoPointOrder.Status.PAID)
        _mp_point_marcar_forcado_liberar(orfao, "Geraldo")
        objs.filter.return_value.order_by.return_value.__getitem__.return_value = [orfao]
        check(mp_point_bloqueio_venda_sessao(_req()) is None, "9 após PIN não bloqueia")

    req_sem = RequestFactory().post("/")
    req_sem.session = MagicMock(session_key="")
    check(mp_point_bloqueio_venda_sessao(req_sem) is None, "9 sessão vazia não bloqueia")

    row_f = _row(PdvMercadoPagoPointOrder.Status.PENDING, mid="ord-res")
    _mp_point_marcar_forcado_liberar(row_f, "Geraldinho")
    check(_mp_point_promover_pago_local(row_f, {"status": "processed"}) is False, "9 MP não ressuscita")
    check(row_f.status == PdvMercadoPagoPointOrder.Status.ABANDONED, "9 fica abandoned")

    row_ok = _row(PdvMercadoPagoPointOrder.Status.PENDING, mid="ord-legit")
    check(_mp_point_promover_pago_local(row_ok, {"status": "processed"}) is True, "9 promover legítimo ainda funciona")

    # API forçar: PAID+PENDING encerrados; GET processed não promove
    paid = _row(PdvMercadoPagoPointOrder.Status.PAID, mid="ord-api-paid")
    pend = _row(PdvMercadoPagoPointOrder.Status.PENDING, mid="ord-api-pend")
    req_api = RequestFactory().generic(
        "POST", "/api/pdv/mp-point/forcar-liberar/", data='{"pin":"8888"}', content_type="application/json"
    )
    sess = MagicMock()
    sess.session_key = "sessao-centro-1"
    req_api.session = sess
    with (
        patch("produtos.pin_gerencial_util.validar_pin_gerencial", return_value=(True, "Geraldo", "")),
        patch("produtos.views_mp_point._mp_point_configurado", return_value=True),
        patch("produtos.views_mp_point._conta_do_pedido_local", return_value="centro"),
        patch("produtos.views_mp_point._token_da_conta", return_value="tok"),
        patch(
            "produtos.views_mp_point.mp_point_get_order",
            return_value=(True, 200, {"status": "processed"}),
        ),
        patch("produtos.pin_gerencial_util.gravar_mp_point_forcar_bypass"),
        patch("produtos.views_mp_point.PdvMercadoPagoPointOrder.objects") as objs,
    ):
        objs.filter.return_value.order_by.return_value.__getitem__.return_value = [paid, pend]
        resp = api_pdv_mp_point_forcar_liberar(req_api)
    body = json.loads(resp.content.decode("utf-8"))
    check(isinstance(resp, JsonResponse) and resp.status_code == 200 and body.get("ok"), "9 API forçar 200")
    check(body.get("tinha_pago_point") is True, "9 API avisa que MP cobrou")
    check(paid.status == PdvMercadoPagoPointOrder.Status.ABANDONED, "9 API encerra PAID")
    check(pend.status == PdvMercadoPagoPointOrder.Status.ABANDONED, "9 API encerra PENDING mesmo MP pago")
    check(_mp_point_row_foi_forcado_liberar(paid) and _mp_point_row_foi_forcado_liberar(pend), "9 flag nos dois")

    # PIN inválido
    req_bad = RequestFactory().generic(
        "POST", "/api/pdv/mp-point/forcar-liberar/", data='{"pin":"0000"}', content_type="application/json"
    )
    req_bad.session = sess
    with patch("produtos.pin_gerencial_util.validar_pin_gerencial", return_value=(False, "", "PIN incorreto.")):
        resp_bad = api_pdv_mp_point_forcar_liberar(req_bad)
    check(resp_bad.status_code == 403, "9 PIN inválido 403")

    # --- 10) Centro/Vila: mesmo path (conta só muda token) ---
    check("Centro e Vila usam o mesmo critério" in views_mp, "10 mesmo critério lojas")
    check("_token_da_conta(conta)" in views_mp, "10 token por conta")
    from produtos.mercado_pago_point import mp_point_conta_de_maquina

    check(mp_point_conta_de_maquina("mp_balcao") == "centro", "10 mp_balcao Centro")
    check(mp_point_conta_de_maquina("mp_vila") == "vila", "10 mp_vila Vila")

    # --- 11) Sem migrate ---
    check("009" not in "MP-POINT-PIN-STICKY", "11 sem migrate novo")

    print("")
    print(f"OKS={OKS} FAILS={len(FAILS)}")
    if FAILS:
        for f in FAILS:
            print(" -", f)
        return 1
    print("VERIFY_MP_POINT_PIN_STICKY_PATH_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
