from produtos.medicamento_vet_taxonomia import (
    anexar_campos_busca_em_row_cadastro,
    aplicar_palavras_e_classes_em_extras,
    categoria_eh_medicamento,
    documento_mongo_set_busca_agro,
    grupos_prateleira_para_api,
    montar_texto_busca_denormalizado,
    normalizar_classes_vet,
    normalizar_especies_vet,
    normalizar_palavras_chave,
    taxonomia_para_api,
)


def test_categoria_medicamento():
    assert categoria_eh_medicamento("Medicamentos E Venenos")
    assert categoria_eh_medicamento("medicamentos")
    assert not categoria_eh_medicamento("Ração")


def test_palavras_chave_normaliza():
    assert normalizar_palavras_chave("Dexon, Biodex") == "Dexon, Biodex"
    assert normalizar_palavras_chave("oleo\n2 tempo") == "oleo, 2 tempo"


def test_classes_vet_valida_slugs():
    cv = normalizar_classes_vet(["antibiotico", "foo", "anti-inflamatorio"])
    assert "antibiotico" in cv
    assert "anti-inflamatorio" in cv
    assert "foo" not in cv


def test_montar_texto_busca_inclui_label():
    txt = montar_texto_busca_denormalizado("Dexon", ["antibiotico"])
    assert "dexon" in txt
    assert "antibiotico" in txt


def test_grupos_prateleira_nove():
    grupos = grupos_prateleira_para_api()
    assert len(grupos) == 9
    assert grupos[0]["id"] == "med_prat_01"
    assert grupos[0]["label"].startswith("01 -")


def test_especies_vet_slugs():
    ev = normalizar_especies_vet(["caes", "gatos", "invalido"])
    assert ev == ["caes", "gatos"]


def test_extras_especies_persistem_e_mongo():
    ex: dict = {}
    aplicar_palavras_e_classes_em_extras(
        ex,
        especies_vet=["bovinos", "equinos"],
        payload_tem_especies=True,
    )
    assert ex["especies_vet"] == ["bovinos", "equinos"]
    doc = documento_mongo_set_busca_agro("", [], ex["especies_vet"])
    assert doc["AgroEspeciesVet"] == ["bovinos", "equinos"]


def test_api_taxonomia_inclui_prateleira_e_especies():
    api = taxonomia_para_api()
    assert len(api.get("grupos_prateleira") or []) == 9
    assert len(api.get("especies") or []) == 10


def test_anexar_campos_busca_em_row():
    row = {}
    anexar_campos_busca_em_row_cadastro(
        row,
        {"palavras_chave": "Dexon, Biodex", "classes_vet": ["antibiotico"]},
    )
    assert row["palavras_chave"] == "Dexon, Biodex"
    assert "antibiotico" in row["classes_vet"]
