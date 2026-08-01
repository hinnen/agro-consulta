"""Provas do card Validade (BI) vs lógica do relatório."""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase, override_settings

from produtos.models import EstoqueLote, ProdutoGestaoOverlayAgro


def _hoje() -> date:
    return date(2026, 8, 1)


@override_settings(
    CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}
)
class ContagemValidadePorLojaTests(TestCase):
    """Caminho `_contagem_validade_dashboard_por_loja` — cenários da loja."""

    def setUp(self):
        from django.core.cache import cache

        cache.clear()

    def _ov(self, pid: str, nome: str = "P") -> ProdutoGestaoOverlayAgro:
        return ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id=pid, nome=nome
        )

    def _lote(self, ov, *, dv: date, qtd: float, codigo: str = "L1") -> EstoqueLote:
        return EstoqueLote.objects.create(
            overlay=ov,
            lote_codigo=codigo,
            data_validade=dv,
            quantidade_atual=qtd,
        )

    @patch("produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro")
    def test_lote_qtd_com_c_v_zerado_conta_nas_duas_lojas(self, mock_saldos):
        """Bug Renan: relatório lista lote; card Centro e Vila ficavam 0."""
        from produtos.views import _contagem_validade_dashboard_por_loja

        ov = self._ov("PID-VENC")
        self._lote(ov, dv=date(2026, 7, 30), qtd=1)
        mock_saldos.return_value = {
            "PID-VENC": {"saldo_centro": 0.0, "saldo_vila": 0.0},
        }
        hoje = _hoje()
        c = _contagem_validade_dashboard_por_loja(hoje, "centro")
        v = _contagem_validade_dashboard_por_loja(hoje, "vila")
        self.assertEqual(c["vencidos"], 1)
        self.assertEqual(v["vencidos"], 1)
        self.assertEqual(c["vencendo_mes"], 0)

    @patch("produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro")
    def test_no_mes_lote_sem_saldo_operacional(self, mock_saldos):
        from produtos.views import _contagem_validade_dashboard_por_loja

        ov = self._ov("PID-MES")
        self._lote(ov, dv=date(2026, 8, 30), qtd=3)
        mock_saldos.return_value = {
            "PID-MES": {"saldo_centro": 0.0, "saldo_vila": 0.0},
        }
        c = _contagem_validade_dashboard_por_loja(_hoje(), "centro")
        self.assertEqual(c["vencidos"], 0)
        self.assertEqual(c["vencendo_mes"], 1)

    @patch("produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro")
    def test_saldo_so_centro_nao_aparece_na_vila(self, mock_saldos):
        from produtos.views import _contagem_validade_dashboard_por_loja

        ov = self._ov("PID-CTR")
        self._lote(ov, dv=date(2026, 7, 30), qtd=1)
        mock_saldos.return_value = {
            "PID-CTR": {"saldo_centro": 2.0, "saldo_vila": 0.0},
        }
        c = _contagem_validade_dashboard_por_loja(_hoje(), "centro")
        v = _contagem_validade_dashboard_por_loja(_hoje(), "vila")
        self.assertEqual(c["vencidos"], 1)
        self.assertEqual(v["vencidos"], 0)

    @patch("produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro")
    def test_duas_datas_mesmo_produto_conta_vencido_e_mes(self, mock_saldos):
        """Antes o card pegava só 1 data por overlay — perdia vencido/mês."""
        from produtos.views import _contagem_validade_dashboard_por_loja

        ov = self._ov("PID-2L")
        self._lote(ov, dv=date(2027, 1, 1), qtd=1, codigo="FUT")
        self._lote(ov, dv=date(2026, 7, 30), qtd=1, codigo="VENC")
        self._lote(ov, dv=date(2026, 8, 15), qtd=1, codigo="MES")
        mock_saldos.return_value = {
            "PID-2L": {"saldo_centro": 0.0, "saldo_vila": 0.0},
        }
        c = _contagem_validade_dashboard_por_loja(_hoje(), "centro")
        self.assertEqual(c["vencidos"], 1)
        self.assertEqual(c["vencendo_mes"], 1)

    @patch("produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro")
    def test_lote_zerado_sem_saldo_nao_conta_vencidos(self, mock_saldos):
        from produtos.views import _contagem_validade_dashboard_por_loja

        ov = self._ov("PID-Z")
        self._lote(ov, dv=date(2026, 7, 30), qtd=0)
        mock_saldos.return_value = {
            "PID-Z": {"saldo_centro": 0.0, "saldo_vila": 0.0},
        }
        c = _contagem_validade_dashboard_por_loja(_hoje(), "centro")
        self.assertEqual(c["vencidos"], 0)
        self.assertEqual(c["vencendo_mes"], 0)

    @patch("produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro")
    def test_mapa_vazio_ainda_conta_lote_com_qtd(self, mock_saldos):
        from produtos.views import _contagem_validade_dashboard_por_loja

        ov = self._ov("PID-MAP")
        self._lote(ov, dv=date(2026, 7, 30), qtd=1)
        mock_saldos.return_value = {}
        c = _contagem_validade_dashboard_por_loja(_hoje(), "centro")
        self.assertEqual(c["vencidos"], 1)

    @patch("produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro")
    def test_estoque_so_vila_com_lote_nao_infla_centro(self, mock_saldos):
        """Lote+qtd com saldo só na Vila: Centro não deve contar (C+V>0)."""
        from produtos.views import _contagem_validade_dashboard_por_loja

        ov = self._ov("PID-VILA")
        self._lote(ov, dv=date(2026, 7, 30), qtd=5)
        mock_saldos.return_value = {
            "PID-VILA": {"saldo_centro": 0.0, "saldo_vila": 5.0},
        }
        c = _contagem_validade_dashboard_por_loja(_hoje(), "centro")
        v = _contagem_validade_dashboard_por_loja(_hoje(), "vila")
        self.assertEqual(c["vencidos"], 0)
        self.assertEqual(v["vencidos"], 1)


class ContagemValidadeEmpresaSmoke(SimpleTestCase):
    """Sanidade das datas (sem DB): vencido vs mês."""

    def test_limites_mes_agosto_2026(self):
        from produtos.views import _bounds_mes_atual

        a, b = _bounds_mes_atual(date(2026, 8, 1))
        self.assertEqual(a, date(2026, 8, 1))
        self.assertEqual(b, date(2026, 8, 31))
        # Print Renan: 30/07 = vencido; 30/08 = no mês
        self.assertTrue(date(2026, 7, 30) < date(2026, 8, 1))
        self.assertTrue(date(2026, 8, 1) <= date(2026, 8, 30) <= b)
