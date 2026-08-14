"""NFC-e emitente por loja (Centro × Vila) — helpers sem SEFAZ."""
from __future__ import annotations

from django.test import SimpleTestCase


class NfceLojaConfigTests(SimpleTestCase):
    def test_loja_de_venda_deposito(self):
        from produtos.nfce_config_util import nfce_loja_de_venda

        class V:
            deposito = "vila"
            sessao_caixa = None

        self.assertEqual(nfce_loja_de_venda(V()), "vila")
        V.deposito = "centro"
        self.assertEqual(nfce_loja_de_venda(V()), "centro")
        V.deposito = ""
        self.assertEqual(nfce_loja_de_venda(V()), "centro")

    def test_cnpj_chave_e_loja(self):
        from produtos.nfce_config_util import nfce_cnpj_da_chave, nfce_loja_de_cnpj

        # cUF(35) + AAMM + CNPJ Vila + resto
        chave = "352608" + "48900774000286" + "650210000000011234567890"
        self.assertEqual(nfce_cnpj_da_chave(chave), "48900774000286")
        self.assertEqual(nfce_loja_de_cnpj("48900774000286"), "vila")
        self.assertEqual(nfce_loja_de_cnpj("48900774000103"), "centro")
