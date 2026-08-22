"""PIN gerencial precisa encerrar o órfão Point — senão o aviso volta em toda venda."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.http import JsonResponse
from django.test import RequestFactory, SimpleTestCase

from produtos.models import PdvMercadoPagoPointOrder
from produtos.views_mp_point import (
    _mp_point_marcar_forcado_liberar,
    _mp_point_promover_pago_local,
    _mp_point_row_foi_forcado_liberar,
    api_pdv_mp_point_forcar_liberar,
    mp_point_bloqueio_venda_sessao,
)


def _req(session_key: str = "sessao-centro-1"):
    req = RequestFactory().post("/api/pdv/mp-point/forcar-liberar/")
    sess = MagicMock()
    sess.session_key = session_key
    sess.get.return_value = None
    sess.__contains__ = MagicMock(return_value=False)
    req.session = sess
    return req


def _row(*, status, valor="2.40", mid="ord-240", last="", erp=None):
    row = SimpleNamespace(
        status=status,
        valor_cobrado=valor,
        mp_order_id=mid,
        mp_last_status=last,
        erp_payload=dict(erp or {"pagamentos": []}),
        django_session_key="sessao-centro-1",
    )
    row.save = MagicMock()
    row.refresh_from_db = MagicMock()
    return row


class MpPointPinForcarNaoVoltaTests(SimpleTestCase):
    def test_flag_helper_forced_by_e_payload(self):
        self.assertTrue(
            _mp_point_row_foi_forcado_liberar(
                SimpleNamespace(mp_last_status="forced_by_Geraldo", erp_payload={})
            )
        )
        self.assertTrue(
            _mp_point_row_foi_forcado_liberar(
                SimpleNamespace(
                    mp_last_status="processed",
                    erp_payload={"mp_point_forcado_liberar": True},
                )
            )
        )
        self.assertFalse(
            _mp_point_row_foi_forcado_liberar(
                SimpleNamespace(mp_last_status="processed", erp_payload={})
            )
        )

    def test_marcar_paid_vira_abandon_com_flag(self):
        row = _row(status=PdvMercadoPagoPointOrder.Status.PAID)
        _mp_point_marcar_forcado_liberar(row, "Geraldo")
        self.assertEqual(row.status, PdvMercadoPagoPointOrder.Status.ABANDONED)
        self.assertTrue(row.erp_payload.get("mp_point_forcado_liberar"))
        self.assertEqual(row.erp_payload.get("mp_point_forcado_por"), "Geraldo")
        self.assertTrue(str(row.mp_last_status).startswith("forced_by_"))
        self.assertTrue(_mp_point_row_foi_forcado_liberar(row))
        row.save.assert_called_once()

    @patch("produtos.views_mp_point.PdvMercadoPagoPointOrder.objects")
    def test_paid_orfao_bloqueia_ate_o_pin(self, objs):
        row = _row(status=PdvMercadoPagoPointOrder.Status.PAID)
        objs.filter.return_value.order_by.return_value.__getitem__.return_value = [row]
        msg = mp_point_bloqueio_venda_sessao(_req())
        self.assertIsNotNone(msg)
        self.assertIn("2.40", str(msg))

    @patch("produtos.views_mp_point.PdvMercadoPagoPointOrder.objects")
    def test_depois_do_pin_proxima_venda_passa_sem_bypass(self, objs):
        row = _row(status=PdvMercadoPagoPointOrder.Status.PAID)
        _mp_point_marcar_forcado_liberar(row, "Geraldo")
        objs.filter.return_value.order_by.return_value.__getitem__.return_value = [row]
        self.assertIsNone(mp_point_bloqueio_venda_sessao(_req()))

    def test_mp_nao_ressuscita_pedido_forcado(self):
        row = _row(status=PdvMercadoPagoPointOrder.Status.PENDING)
        _mp_point_marcar_forcado_liberar(row, "Geraldinho")
        ok = _mp_point_promover_pago_local(row, {"status": "processed"})
        self.assertFalse(ok)
        self.assertEqual(row.status, PdvMercadoPagoPointOrder.Status.ABANDONED)

    @patch("produtos.views_mp_point.PdvMercadoPagoPointOrder.objects")
    def test_outra_sessao_nao_bloqueia(self, objs):
        row = _row(status=PdvMercadoPagoPointOrder.Status.PAID)
        objs.filter.return_value.order_by.return_value.__getitem__.return_value = [row]
        req = _req("outra-sessao")
        # filtro usa session_key da request; mock devolve o órfão só se o path consultar —
        # com session diferente o queryset real seria vazio. Simula lista vazia.
        objs.filter.return_value.order_by.return_value.__getitem__.return_value = []
        self.assertIsNone(mp_point_bloqueio_venda_sessao(req))
        objs.filter.assert_called()
        kwargs = objs.filter.call_args.kwargs
        self.assertEqual(kwargs.get("django_session_key"), "outra-sessao")

    def test_promover_ainda_funciona_sem_forcar(self):
        row = _row(status=PdvMercadoPagoPointOrder.Status.PENDING, mid="ord-ok")
        ok = _mp_point_promover_pago_local(row, {"status": "processed"})
        self.assertTrue(ok)
        self.assertEqual(row.status, PdvMercadoPagoPointOrder.Status.PAID)

    @patch("produtos.pin_gerencial_util.gravar_mp_point_forcar_bypass")
    @patch("produtos.views_mp_point._mp_point_configurado", return_value=False)
    @patch(
        "produtos.pin_gerencial_util.validar_pin_gerencial",
        return_value=(True, "Geraldo", ""),
    )
    @patch("produtos.views_mp_point.PdvMercadoPagoPointOrder.objects")
    def test_forcar_api_encerra_paid_e_pending(self, objs, _pin, _cfg, _bypass):
        paid = _row(status=PdvMercadoPagoPointOrder.Status.PAID, mid="ord-api-paid")
        pend = _row(status=PdvMercadoPagoPointOrder.Status.PENDING, mid="ord-api-pend")
        objs.filter.return_value.order_by.return_value.__getitem__.return_value = [
            paid,
            pend,
        ]
        req = RequestFactory().generic(
            "POST",
            "/api/pdv/mp-point/forcar-liberar/",
            data='{"pin":"8888"}',
            content_type="application/json",
        )
        sess = MagicMock()
        sess.session_key = "sessao-centro-1"
        req.session = sess

        resp = api_pdv_mp_point_forcar_liberar(req)
        self.assertIsInstance(resp, JsonResponse)
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.content.decode("utf-8"))
        self.assertTrue(body.get("ok"))
        self.assertTrue(body.get("tinha_pago_point"))
        self.assertEqual(paid.status, PdvMercadoPagoPointOrder.Status.ABANDONED)
        self.assertEqual(pend.status, PdvMercadoPagoPointOrder.Status.ABANDONED)
        self.assertTrue(_mp_point_row_foi_forcado_liberar(paid))
        self.assertTrue(_mp_point_row_foi_forcado_liberar(pend))

    @patch("produtos.pin_gerencial_util.gravar_mp_point_forcar_bypass")
    @patch(
        "produtos.views_mp_point.mp_point_get_order",
        return_value=(True, 200, {"status": "processed"}),
    )
    @patch("produtos.views_mp_point._token_da_conta", return_value="tok")
    @patch("produtos.views_mp_point._conta_do_pedido_local", return_value="centro")
    @patch("produtos.views_mp_point._mp_point_configurado", return_value=True)
    @patch(
        "produtos.pin_gerencial_util.validar_pin_gerencial",
        return_value=(True, "Geraldinho", ""),
    )
    @patch("produtos.views_mp_point.PdvMercadoPagoPointOrder.objects")
    def test_forcar_nao_promove_quando_mp_diz_pago(
        self, objs, _pin, _cfg, _conta, _tok, get_order, _bypass
    ):
        pend = _row(status=PdvMercadoPagoPointOrder.Status.PENDING, mid="ord-mp-pago")
        objs.filter.return_value.order_by.return_value.__getitem__.return_value = [pend]
        req = RequestFactory().generic(
            "POST",
            "/api/pdv/mp-point/forcar-liberar/",
            data='{"pin":"8888"}',
            content_type="application/json",
        )
        sess = MagicMock()
        sess.session_key = "sessao-centro-1"
        req.session = sess

        resp = api_pdv_mp_point_forcar_liberar(req)
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(json.loads(resp.content.decode("utf-8")).get("tinha_pago_point"))
        self.assertEqual(pend.status, PdvMercadoPagoPointOrder.Status.ABANDONED)
        self.assertTrue(_mp_point_row_foi_forcado_liberar(pend))
        get_order.assert_called_once()
