"""PDV-RACOES — match tipo / marca / peso."""
from django.test import SimpleTestCase

from produtos.pdv_racoes_util import (
    TIPOS_RACOES,
    eh_categoria_racoes,
    filtrar_racoes,
    parse_peso_racoes,
    tipo_racoes_por_id,
)


def _row(**kwargs):
    base = {
        "id": "p1",
        "nome": "Racao teste",
        "categoria": "Rações",
        "subcategoria": "Cão",
        "subcategoria_2": "Adulto",
        "marca": "ESTIMACAO",
        "peso_etiqueta": "15",
        "inativo": False,
    }
    base.update(kwargs)
    return base


class ParsePesoTests(SimpleTestCase):
    def test_numeros_e_kg(self):
        self.assertEqual(parse_peso_racoes("1"), "kg:1")
        self.assertEqual(parse_peso_racoes("1kg"), "kg:1")
        self.assertEqual(parse_peso_racoes("1 kg"), "kg:1")
        self.assertEqual(parse_peso_racoes("5 KG"), "kg:5")
        self.assertEqual(parse_peso_racoes("10kg"), "kg:10")
        self.assertEqual(parse_peso_racoes("15"), "kg:15")
        self.assertEqual(parse_peso_racoes("20 kg"), "kg:20")
        self.assertEqual(parse_peso_racoes("25"), "kg:25")

    def test_pacote(self):
        self.assertEqual(parse_peso_racoes("pacote"), "pacote")
        self.assertEqual(parse_peso_racoes("Pacote"), "pacote")
        self.assertEqual(parse_peso_racoes("pacote 10"), "pacote")
        self.assertEqual(parse_peso_racoes("10,00"), "kg:10")

    def test_invalido(self):
        self.assertIsNone(parse_peso_racoes(""))
        self.assertIsNone(parse_peso_racoes("7"))
        self.assertIsNone(parse_peso_racoes("500 g"))


class CategoriaTipoTests(SimpleTestCase):
    def test_categoria(self):
        self.assertTrue(eh_categoria_racoes("Rações"))
        self.assertTrue(eh_categoria_racoes("racoes"))
        self.assertFalse(eh_categoria_racoes("Farelo"))

    def test_oito_tipos(self):
        self.assertEqual(len(TIPOS_RACOES), 8)
        self.assertIsNotNone(tipo_racoes_por_id("cao_adulto"))
        self.assertEqual(tipo_racoes_por_id("cao_adulto")["sub2"], "Adulto")


class FiltrarTests(SimpleTestCase):
    def test_tipo_marca_peso(self):
        tipo = tipo_racoes_por_id("cao_adulto")
        rows = [
            _row(),
            _row(id="p2", marca="GOLDEN", peso_etiqueta="10kg"),
            _row(id="p3", subcategoria_2="Filhote", peso_etiqueta="15"),
            _row(id="p4", categoria="Farelo"),
            _row(id="p5", peso_etiqueta="pacote", marca="ESTIMACAO"),
            _row(id="p6", inativo=True),
        ]
        so_est_15 = filtrar_racoes(rows, tipo, marca="ESTIMACAO", peso_key="kg:15")
        self.assertEqual([r["id"] for r in so_est_15], ["p1"])
        todas_15 = filtrar_racoes(rows, tipo, marca=None, peso_key="kg:15")
        self.assertEqual([r["id"] for r in todas_15], ["p1"])
        todas = filtrar_racoes(rows, tipo, marca=None, peso_key=None)
        self.assertEqual([r["id"] for r in todas], ["p1", "p2", "p5"])
        pacote = filtrar_racoes(rows, tipo, marca="estimacao", peso_key="pacote")
        self.assertEqual([r["id"] for r in pacote], ["p5"])

    def test_adulto_nao_pega_rp(self):
        tipo = tipo_racoes_por_id("cao_adulto")
        rows = [
            _row(id="a", subcategoria_2="Adulto"),
            _row(id="rp", subcategoria_2="Adulto RP", peso_etiqueta="10"),
        ]
        self.assertEqual([r["id"] for r in filtrar_racoes(rows, tipo)], ["a"])
        tipo_rp = tipo_racoes_por_id("cao_adulto_rp")
        self.assertEqual([r["id"] for r in filtrar_racoes(rows, tipo_rp)], ["rp"])

    def test_sub1_cao_vs_cachorro(self):
        tipo = tipo_racoes_por_id("cao_adulto")
        rows = [_row(id="old", subcategoria="cachorro")]
        self.assertEqual(filtrar_racoes(rows, tipo), [])

    def test_filhote_nao_pega_rp(self):
        tipo = tipo_racoes_por_id("cao_filhote")
        rows = [
            _row(id="f", subcategoria_2="Filhote", peso_etiqueta="10"),
            _row(id="frp", subcategoria_2="Filhote RP", peso_etiqueta="10"),
        ]
        self.assertEqual([r["id"] for r in filtrar_racoes(rows, tipo)], ["f"])
        tipo_rp = tipo_racoes_por_id("cao_filhote_rp")
        self.assertEqual([r["id"] for r in filtrar_racoes(rows, tipo_rp)], ["frp"])

    def test_gato_nao_mistura_cao(self):
        gato = tipo_racoes_por_id("gato_adulto")
        rows = [
            _row(id="g", subcategoria="Gato", subcategoria_2="Adulto", peso_etiqueta="5"),
            _row(id="c", subcategoria="Cão", subcategoria_2="Adulto", peso_etiqueta="5"),
        ]
        self.assertEqual([r["id"] for r in filtrar_racoes(rows, gato)], ["g"])

    def test_acentos_e_caixa(self):
        tipo = tipo_racoes_por_id("cao_senior")
        rows = [_row(id="s", subcategoria="CAO", subcategoria_2="SENIOR", peso_etiqueta="20")]
        self.assertEqual([r["id"] for r in filtrar_racoes(rows, tipo)], ["s"])

    def test_sem_marca_e_todas(self):
        tipo = tipo_racoes_por_id("cao_adulto")
        rows = [
            _row(id="a", marca="GOLDEN", peso_etiqueta="10"),
            _row(id="b", marca="", peso_etiqueta="10"),
        ]
        self.assertEqual([r["id"] for r in filtrar_racoes(rows, tipo, marca="")], ["b"])
        self.assertEqual(
            [r["id"] for r in filtrar_racoes(rows, tipo, marca=None, peso_key="kg:10")],
            ["a", "b"],
        )

    def test_oito_tipos_catalogo_minimo(self):
        rows = []
        for i, t in enumerate(TIPOS_RACOES):
            rows.append(
                _row(
                    id=f"t{i}",
                    subcategoria=t["sub1"],
                    subcategoria_2=t["sub2"],
                    peso_etiqueta="10",
                )
            )
        for t in TIPOS_RACOES:
            hit = filtrar_racoes(rows, t, peso_key="kg:10")
            self.assertEqual(len(hit), 1, msg=t["id"])
