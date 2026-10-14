from django.test import SimpleTestCase

from produtos.mongo_produto_id_util import id_externo_de_doc_mongo, mongo_campo_id_erp_parece_valido


class MongoProdutoIdUtilTests(SimpleTestCase):
    def test_rejeita_mensagem_erp_no_campo_id(self):
        lixo = '"PRODUTO ALTERADO COM SUCESSO. Produto GM6068 - Veneno formiga c'
        self.assertFalse(mongo_campo_id_erp_parece_valido(lixo))

    def test_prefere_objectid_quando_id_corrompido(self):
        oid = "699382cb4f504f3b9b794981"
        doc = {
            "_id": oid,
            "Id": '"PRODUTO ALTERADO COM SUCESSO. Produto GM6068 - Veneno formiga c',
            "Nome": "Veneno formiga",
        }
        self.assertEqual(id_externo_de_doc_mongo(doc), oid)

    def test_usa_id_erp_numerico_valido(self):
        doc = {"_id": "699382cb4f504f3b9b794981", "Id": 12345}
        self.assertEqual(id_externo_de_doc_mongo(doc), "12345")
