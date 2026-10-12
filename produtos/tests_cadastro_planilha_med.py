from django.test import SimpleTestCase

from produtos.cadastro_planilha_util import (
    COL_MED_CLASSES,
    COL_MED_ESPECIES,
    COL_MED_PALAVRAS,
    EXPORT_COL_KEYS,
    IMPORT_KEYS,
    MED_IMPORT_KEYS,
    _aplicar_patch_med,
    _enriquecer_row_med_planilha,
    _format_lista_slugs_planilha,
    _map_headers,
    _parse_slugs_planilha,
    _patch_da_linha,
    _tem_alteracao,
    headers_export,
    linha_export_planilha,
)
from produtos.models import ProdutoGestaoOverlayAgro


class CadastroPlanilhaMedTests(SimpleTestCase):
    def test_headers_incluem_med(self):
        for k in MED_IMPORT_KEYS:
            self.assertIn(k, EXPORT_COL_KEYS)
            self.assertIn(k, IMPORT_KEYS)
        labels = [lab for lab, _ in headers_export()]
        self.assertIn("Med palavras-chave", labels)
        self.assertIn("Med classes (slugs)", labels)
        self.assertIn("Med espécies (slugs)", labels)

    def test_aliases_med(self):
        m = _map_headers(
            [
                "ID",
                "Med palavras-chave",
                "Med classes (slugs)",
                "Med espécies (slugs)",
            ]
        )
        self.assertEqual(m[COL_MED_PALAVRAS], "Med palavras-chave")
        self.assertEqual(m[COL_MED_CLASSES], "Med classes (slugs)")
        self.assertEqual(m[COL_MED_ESPECIES], "Med espécies (slugs)")

    def test_celula_vazia_med_nao_entra_patch(self):
        colmap = _map_headers(["ID", "Med palavras-chave", "Med classes (slugs)"])
        patch = _patch_da_linha(
            {"ID": "x", "Med palavras-chave": "", "Med classes (slugs)": "   "},
            colmap,
        )
        self.assertNotIn(COL_MED_PALAVRAS, patch)
        self.assertNotIn(COL_MED_CLASSES, patch)

    def test_slug_invalido_erro(self):
        colmap = _map_headers(["ID", "Med classes (slugs)"])
        patch = _patch_da_linha(
            {"ID": "x", "Med classes (slugs)": "antibiotico, slug-inexistente"},
            colmap,
        )
        self.assertIn("__erro_classes_vet", patch)

    def test_slug_valido_lista(self):
        colmap = _map_headers(["ID", "Med classes (slugs)", "Med espécies (slugs)"])
        patch = _patch_da_linha(
            {
                "ID": "x",
                "Med classes (slugs)": "antibiotico; anti-inflamatorio",
                "Med espécies (slugs)": "caes, gatos",
            },
            colmap,
        )
        self.assertEqual(patch[COL_MED_CLASSES], ["antibiotico", "anti-inflamatorio"])
        self.assertEqual(patch[COL_MED_ESPECIES], ["caes", "gatos"])

    def test_limpar_lista_via_traco(self):
        parsed, err = _parse_slugs_planilha("-", {"caes"})
        self.assertIsNone(err)
        self.assertEqual(parsed, [])

    def test_aplicar_patch_med_extras(self):
        ov = ProdutoGestaoOverlayAgro(produto_externo_id="tmp-med")
        ov.cadastro_extras = {
            "palavras_chave": "Antigo",
            "classes_vet": ["vermifugo"],
            "especies_vet": ["bovinos"],
        }
        err = _aplicar_patch_med(
            ov,
            {
                COL_MED_PALAVRAS: "Dexon Biodex",
                COL_MED_CLASSES: ["antibiotico"],
            },
        )
        self.assertIsNone(err)
        ex = ov.cadastro_extras
        self.assertEqual(ex.get("palavras_chave"), "Dexon Biodex")
        self.assertEqual(ex.get("classes_vet"), ["antibiotico"])
        self.assertEqual(ex.get("especies_vet"), ["bovinos"])

    def test_tem_alteracao_lista_slugs(self):
        atual = {
            "classes_vet": ["antibiotico"],
            "especies_vet": ["caes"],
            "palavras_chave": "Foo",
        }
        self.assertFalse(
            _tem_alteracao(atual, {COL_MED_CLASSES: ["antibiotico"]}),
        )
        self.assertTrue(
            _tem_alteracao(atual, {COL_MED_CLASSES: ["vermifugo"]}),
        )

    def test_export_linha_med(self):
        row = {
            "id": "1",
            "nome": "Prod",
            COL_MED_PALAVRAS: "A B",
            COL_MED_CLASSES: ["antibiotico", "vermifugo"],
            COL_MED_ESPECIES: ["caes"],
        }
        line = linha_export_planilha(row)
        self.assertEqual(line[COL_MED_PALAVRAS], "A B")
        self.assertEqual(line[COL_MED_CLASSES], "antibiotico, vermifugo")
        self.assertEqual(line[COL_MED_ESPECIES], "caes")

    def test_enriquecer_row_de_extras(self):
        ov = ProdutoGestaoOverlayAgro(produto_externo_id="x")
        ov.cadastro_extras = {
            "palavras_chave": "Palavra",
            "classes_vet": ["antibiotico"],
            "especies_vet": ["equinos"],
        }
        row: dict = {"id": "x"}
        _enriquecer_row_med_planilha(row, ov)
        self.assertEqual(row[COL_MED_PALAVRAS], "Palavra")
        self.assertEqual(_format_lista_slugs_planilha(row["classes_vet"]), "antibiotico")
