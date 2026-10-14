"""Planilha cadastro — ID corrompido com mensagem ERP."""
from unittest.mock import patch

from django.test import SimpleTestCase

from produtos.cadastro_planilha_util import (
    _codigo_gm_extraido_de_texto,
    _id_produto_planilha_valido,
    _recuperar_id_planilha_texto_erp,
)


class CadastroPlanilhaIdErpTests(SimpleTestCase):
    def test_extrai_gm_de_mensagem_erp(self):
        s = '"PRODUTO ALTERADO COM SUCESSO. Produto GM6085 - Enrotril Antibió'
        self.assertEqual(_codigo_gm_extraido_de_texto(s), "GM6085")
        self.assertFalse(_id_produto_planilha_valido(s))

    @patch("produtos.cadastro_overlay_orfao_util.produto_externo_id_catalogo_por_codigo_ou_nome")
    def test_recupera_id_pelo_gm_na_celula(self, mock_resolve):
        pid_mongo = "69937a58a42b90be60615c02"
        mock_resolve.return_value = pid_mongo
        lixo = '"PRODUTO ALTERADO COM SUCESSO. Produto GM6085 - Enrotril Antibió'
        novo, aviso = _recuperar_id_planilha_texto_erp(lixo)
        self.assertEqual(novo, pid_mongo)
        self.assertIn("recuperado", (aviso or "").lower())
        mock_resolve.assert_called_once()
        self.assertEqual(mock_resolve.call_args.kwargs.get("codigo"), "GM6085")
