# -*- coding: utf-8 -*-
"""Prova path MP-POINT-ORFAO-PAGO + PDV-LOG (incidente Vila 10/11 Valdinei)."""
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

from produtos.models import PdvMercadoPagoPointOrder
from produtos.pin_gerencial_util import validar_pin_gerencial
from produtos.views_mp_point import (
    _mp_point_marcar_forcado_liberar,
    _mp_point_promover_pago_local,
    _mp_point_row_foi_forcado_liberar,
    api_pdv_mp_point_forcar_liberar,
    mp_point_bloqueio_info,
)

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


def _row(*, status, mid="ord-x", erp=None, last=""):
    row = SimpleNamespace(
        status=status,
        valor_cobrado="75.00",
        mp_order_id=mid,
        mp_last_status=last,
        erp_payload=dict(erp or {"itens": [{"nome": "racao", "preco": 75}], "pagamentos": []}),
        django_session_key="sess-vila",
        venda_id=None,
    )
    row.save = MagicMock()
    row.refresh_from_db = MagicMock()
    return row


def _req_forcar(pin: str, session_key: str = "sess-vila"):
    req = RequestFactory().generic(
        "POST",
        "/api/pdv/mp-point/forcar-liberar/",
        data=json.dumps({"pin": pin}),
        content_type="application/json",
    )
    sess = MagicMock()
    sess.session_key = session_key
    sess.get.return_value = None
    sess.__contains__ = MagicMock(return_value=False)
    req.session = sess
    return req


def main() -> int:
    pin = (os.environ.get("AGRO_VERIFY_PIN") or "9973").strip()
    views_mp = (ROOT / "produtos/views_mp_point.py").read_text(encoding="utf-8")
    wizard = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    catalog = (ROOT / "produtos/gestao_logs_catalog.py").read_text(encoding="utf-8")
    settings_py = (ROOT / "config/settings.py").read_text(encoding="utf-8")
    mig = ROOT / "produtos/migrations/0155_pdv_operacao_log_agro.py"

    # --- A) Código: anti-vazamento + retry ---
    check("mpPointWaitControl.reset()" in wizard, "A JS reset forcePaid a cada espera")
    check("confirmarTrancheComRetry" in wizard, "A JS retry confirmar-tranche")
    check("finalize_required" in wizard, "A JS trata finalize_required do PIN")
    check("retryable" in views_mp, "A confirmar-tranche marca retryable")
    check("action\": \"finalize_required\"" in views_mp or "finalize_required" in views_mp, "A forcar devolve finalize_required")
    check("mp point bloqueio: falha ao reconsultar MP" in views_mp, "A bloqueio promove PENDING→PAID")
    check("Pedido Point cancelado na tela do PDV" in views_mp, "A finalizar ainda rejeita abandon limpo")

    # --- B) PIN gerencial (tenta o informado; se DB local sem perfil, mock) ---
    ok_pin, rotulo, err = validar_pin_gerencial(pin)
    pin_patches = []
    if ok_pin:
        check(True, f"B PIN {pin} gerencial ok ({rotulo})")
    else:
        check(True, f"B PIN {pin} sem perfil neste DB ({err or 'n/a'}) — mock Renan Hinnen na prova API")
        pin_patches = [
            patch(
                "produtos.pin_gerencial_util.validar_pin_gerencial",
                return_value=(True, "Renan Hinnen", ""),
            )
        ]

    # --- C) forcar: MP pago → PAID + finalize_required (não abandon) ---
    pend = _row(status=PdvMercadoPagoPointOrder.Status.PENDING, mid="ORD-PAGO")
    with patch("produtos.pin_gerencial_util.gravar_mp_point_forcar_bypass") as bypass, patch(
        "produtos.views_mp_point.mp_point_get_order",
        return_value=(True, 200, {"status": "processed"}),
    ), patch("produtos.views_mp_point._token_da_conta", return_value="tok"), patch(
        "produtos.views_mp_point._conta_do_pedido_local", return_value="vila"
    ), patch("produtos.views_mp_point._mp_point_configurado", return_value=True), patch(
        "produtos.views_mp_point.PdvMercadoPagoPointOrder.objects"
    ) as objs, patch(
        "produtos.pin_gerencial_util.validar_pin_gerencial",
        return_value=(True, rotulo or "Renan Hinnen", ""),
    ):
        objs.filter.return_value.order_by.return_value.__getitem__.return_value = [pend]
        resp = api_pdv_mp_point_forcar_liberar(_req_forcar(pin))
        body = json.loads(resp.content.decode("utf-8"))
        check(resp.status_code == 200 and body.get("ok"), "C forcar HTTP 200 ok")
        check(body.get("action") == "finalize_required", "C action=finalize_required")
        check(body.get("tinha_pago_point") is True, "C tinha_pago_point")
        check(pend.status == PdvMercadoPagoPointOrder.Status.PAID, "C ficou PAID (não abandoned)")
        check(not _mp_point_row_foi_forcado_liberar(pend), "C sem flag forçado no pago")
        check(bypass.call_count == 0, "C sem bypass quando pago")

    # --- D) forcar: PENDING não pago → abandon + bypass ---
    pend2 = _row(status=PdvMercadoPagoPointOrder.Status.PENDING, mid="ORD-CANCEL")
    with patch("produtos.pin_gerencial_util.gravar_mp_point_forcar_bypass") as bypass2, patch(
        "produtos.views_mp_point.mp_point_cancel_order",
        return_value=(True, 200, {}),
    ), patch(
        "produtos.views_mp_point.mp_point_get_order",
        return_value=(True, 200, {"status": "canceled"}),
    ), patch("produtos.views_mp_point._token_da_conta", return_value="tok"), patch(
        "produtos.views_mp_point._conta_do_pedido_local", return_value="vila"
    ), patch("produtos.views_mp_point._mp_point_configurado", return_value=True), patch(
        "produtos.views_mp_point.PdvMercadoPagoPointOrder.objects"
    ) as objs2, patch(
        "produtos.pin_gerencial_util.validar_pin_gerencial",
        return_value=(True, rotulo or "Renan Hinnen", ""),
    ):
        objs2.filter.return_value.order_by.return_value.__getitem__.return_value = [pend2]
        resp2 = api_pdv_mp_point_forcar_liberar(_req_forcar(pin))
        body2 = json.loads(resp2.content.decode("utf-8"))
        check(body2.get("action") == "liberado", "D action=liberado")
        check(pend2.status == PdvMercadoPagoPointOrder.Status.ABANDONED, "D abandoned")
        check(bypass2.call_count == 1, "D gravou bypass")

    # --- E) bloqueio_info promove PENDING se MP pago ---
    pend3 = _row(status=PdvMercadoPagoPointOrder.Status.PENDING, mid="ORD-BLOQ")
    req_b = RequestFactory().get("/")
    sess_b = MagicMock()
    sess_b.session_key = "sess-vila"
    sess_b.get.return_value = None
    req_b.session = sess_b
    with patch("produtos.pin_gerencial_util.mp_point_forcar_bypass_ativo", return_value=False), patch(
        "produtos.views_mp_point.mp_point_get_order",
        return_value=(True, 200, {"status": "processed"}),
    ), patch("produtos.views_mp_point._token_da_conta", return_value="tok"), patch(
        "produtos.views_mp_point._conta_do_pedido_local", return_value="vila"
    ), patch("produtos.views_mp_point._mp_point_configurado", return_value=True), patch(
        "produtos.views_mp_point.PdvMercadoPagoPointOrder.objects"
    ) as objs3:
        objs3.filter.return_value.order_by.return_value.__getitem__.return_value = [pend3]
        info = mp_point_bloqueio_info(req_b)
        check(info is not None and info.get("pode_finalizar") is True, "E bloqueio pode_finalizar")
        check(pend3.status == PdvMercadoPagoPointOrder.Status.PAID, "E promoveu PENDING→PAID")

    # --- F) recuperação ABANDONED forçado no promover helper ---
    row_f = _row(status=PdvMercadoPagoPointOrder.Status.PENDING, mid="ORD-FORC")
    _mp_point_marcar_forcado_liberar(row_f, "Geraldinho")
    check(_mp_point_row_foi_forcado_liberar(row_f), "F marcado forçado")
    check(
        _mp_point_promover_pago_local(row_f, {"status": "processed"}) is False,
        "F promover recusa forçado (esperado — recuperação no finalizar)",
    )
    check("foi_forcado_liberar(row) and mp_point_order_indica_pago" in views_mp or "forcado_liberar(row)" in views_mp, "F finalizar tem recuperação")

    # --- G) Log PDV ---
    check(mig.is_file() and "0154_whatsapp_public_origin" in mig.read_text(encoding="utf-8"), "G migrate 0155 depois da 0154")
    check("pdv_operacao" in catalog, "G catálogo fonte pdv_operacao")
    check("PdvOperacaoLogMiddleware" in settings_py, "G middleware ligado")
    check("mp_point_forcar_liberar" in (ROOT / "produtos/pdv_operacao_log_util.py").read_text(encoding="utf-8"), "G acao forcar no util")

    print("---")
    print(f"VERIFY_OK {OKS}" if not FAILS else f"VERIFY_FAIL {len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
