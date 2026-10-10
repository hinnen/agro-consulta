from produtos.medicamento_vet_taxonomia import (
    categoria_eh_medicamento,
    montar_texto_busca_denormalizado,
    normalizar_classes_vet,
    normalizar_palavras_chave,
)


def test_categoria_medicamento():
    assert categoria_eh_medicamento("Medicamentos E Venenos")
    assert categoria_eh_medicamento("medicamentos")
    assert not categoria_eh_medicamento("Ração")


def test_palavras_chave_normaliza():
    assert normalizar_palavras_chave("Dexon, Biodex") == "Dexon Biodex"


def test_classes_vet_valida_slugs():
    cv = normalizar_classes_vet(["antibiotico", "foo", "anti-inflamatorio"])
    assert "antibiotico" in cv
    assert "anti-inflamatorio" in cv
    assert "foo" not in cv


def test_montar_texto_busca_inclui_label():
    txt = montar_texto_busca_denormalizado("Dexon", ["antibiotico"])
    assert "dexon" in txt
    assert "antibiotico" in txt
