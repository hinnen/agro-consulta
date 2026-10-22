"""Slots rápidos do PDV — apelido manual, foto no Postgres, preço fora daqui."""
import io
import json

from django.contrib.auth import get_user_model
from django.test import RequestFactory, TestCase
from PIL import Image

from produtos.pdv_rapidos_util import gravar_foto, listar_slots, preparar_foto, resposta_foto, salvar_slots
from produtos.pdv_topbar_layout_util import QUENTE_DEFAULT, normalizar_layout
from produtos.views_pdv_rapidos import api_pdv_rapidos, api_pdv_rapidos_foto


def _jpeg(cor=(240, 180, 40)) -> bytes:
    im = Image.new("RGB", (80, 140), cor)
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=80)
    return buf.getvalue()


class PdvRapidosTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(username="pdv_rapidos", password="x")
        self.rf = RequestFactory()

    def test_lista_nove_vazios(self):
        slots = listar_slots()
        self.assertEqual(len(slots), 9)
        self.assertTrue(all(not s["produto_id"] and not s["tem_foto"] for s in slots))

    def test_exige_produto_e_apelido(self):
        ok, err = salvar_slots([{"posicao": 1, "nome": "Milho", "produto_id": ""}])
        self.assertFalse(ok)
        self.assertIn("produto", err)
        ok, err = salvar_slots([{"posicao": 2, "nome": "", "produto_id": "abc"}])
        self.assertFalse(ok)
        self.assertIn("apelido", err)

    def test_salvar_foto_e_preco_nao_grava(self):
        ok, err = salvar_slots(
            [
                {
                    "posicao": 1,
                    "produto_id": "prod-1",
                    "codigo": "GM1",
                    "nome": "Milho grande",
                }
            ],
            usuario=self.user,
        )
        self.assertTrue(ok, err)
        jpeg, erro_foto = preparar_foto(_jpeg())
        self.assertTrue(jpeg, erro_foto)
        ok, err = gravar_foto(1, _jpeg(), content_type="image/jpeg", usuario=self.user)
        self.assertTrue(ok, err)
        slot = listar_slots()[0]
        self.assertEqual(slot["nome"], "Milho grande")
        self.assertEqual(slot["produto_id"], "prod-1")
        self.assertNotIn("preco", slot)
        self.assertTrue(slot["tem_foto"])
        resp = resposta_foto(1)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp["Content-Type"], "image/jpeg")
        self.assertTrue(resp.content[:2] == b"\xff\xd8")

    def test_limpar_apaga_foto(self):
        salvar_slots([{"posicao": 3, "produto_id": "p", "codigo": "C", "nome": "Saco"}])
        gravar_foto(3, _jpeg(), content_type="image/jpeg")
        ok, err = salvar_slots([{"posicao": 3, "limpar": True}])
        self.assertTrue(ok, err)
        slot = next(s for s in listar_slots() if s["posicao"] == 3)
        self.assertEqual(slot["nome"], "")
        self.assertFalse(slot["tem_foto"])
        self.assertEqual(resposta_foto(3).status_code, 404)

    def test_foto_invalida(self):
        jpeg, err = preparar_foto(b"nao-e-imagem", "image/jpeg")
        self.assertIsNone(jpeg)
        self.assertTrue(err)

    def test_api_login(self):
        req = self.rf.get("/api/pdv/rapidos/")
        req.user = self.user
        resp = api_pdv_rapidos(req)
        body = json.loads(resp.content.decode("utf-8"))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(body["slots"]), 9)
        req = self.rf.post(
            "/api/pdv/rapidos/",
            data=json.dumps(
                {"slots": [{"posicao": 4, "produto_id": "z", "codigo": "GM", "nome": "Fruto"}]}
            ),
            content_type="application/json",
        )
        req.user = self.user
        resp = api_pdv_rapidos(req)
        self.assertEqual(resp.status_code, 200)
        up = self.rf.post("/api/pdv/rapidos/4/foto/", {"limpar": "1"})
        up.user = self.user
        resp = api_pdv_rapidos_foto(up, 4)
        self.assertEqual(resp.status_code, 200)

    def test_layout_rapidos_entra_no_quente(self):
        self.assertIn("rapidos", QUENTE_DEFAULT)
        self.assertLess(QUENTE_DEFAULT.index("fiado"), QUENTE_DEFAULT.index("rapidos"))
        n = normalizar_layout(
            quente=["pedir_loja", "fiado", "nova_venda"],
            frio=["pin"],
        )
        self.assertIn("rapidos", n["quente"])
        self.assertLess(n["quente"].index("rapidos"), n["quente"].index("nova_venda"))
        self.assertEqual(n["quente"].count("rapidos"), 1)
