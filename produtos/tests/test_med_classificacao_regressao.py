"""Regressão MED-CLASSIFICACAO-SISVALE — persistência segura no overlay."""

from pathlib import Path

from produtos.medicamento_vet_taxonomia import (
    aplicar_palavras_e_classes_em_extras,
    ler_busca_de_cadastro_extras,
    montar_texto_busca_denormalizado,
    taxonomia_para_api,
)


def test_modal_cadastro_define_gv_antes_da_ui_medicamento():
    """abrirModalProduto chama atualizarUISubcategoriaMedicamento, que usa gv()."""
    modal = Path("produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html").read_text(
        encoding="utf-8"
    )
    idx_gv = modal.find("function gv(id)")
    idx_med = modal.find("function preencherSelectPrateleiraMed")
    assert idx_gv >= 0 and idx_med >= 0 and idx_gv < idx_med


def test_modal_subcategorias_med_dicas_ui_somente_visual():
    modal = Path("produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html").read_text(
        encoding="utf-8"
    )
    assert "atualizarDicasSubcategoriaMedicamento" in modal
    assert modal.count("data-agro-med-sub-nivel=") >= 8
    assert "Grupo da prateleira" in modal
    assert "Princípio ativo / detalhamento" in modal
    assert 'id="edit-subcategoria-2"' in modal
    assert "subcategoria_2: gv('edit-subcategoria-2')" in modal
    assert "function abrirAgroMedSubTip" in modal


def test_ler_busca_retorna_tres_valores():
    pk, cv, ev = ler_busca_de_cadastro_extras(
        {"palavras_chave": "A", "classes_vet": ["antibiotico"], "especies_vet": ["caes"]}
    )
    assert pk == "A"
    assert cv == ["antibiotico"]
    assert ev == ["caes"]


def test_payload_sem_chave_nao_apaga_classes_nem_especies():
    ex = {
        "palavras_chave": "legado",
        "classes_vet": ["antibiotico", "anti-inflamatorio"],
        "especies_vet": ["bovinos"],
    }
    aplicar_palavras_e_classes_em_extras(ex, palavras_chave="novo", payload_tem_palavras=True)
    assert ex["classes_vet"] == ["antibiotico", "anti-inflamatorio"]
    assert ex["especies_vet"] == ["bovinos"]
    assert ex["palavras_chave"] == "novo"


def test_limpar_especies_so_com_payload_explicito():
    ex = {"especies_vet": ["caes", "gatos"]}
    aplicar_palavras_e_classes_em_extras(ex, especies_vet=[], payload_tem_especies=True)
    assert "especies_vet" not in ex


def test_produto_antigo_sem_campos_busca_med():
    pk, cv, ev = ler_busca_de_cadastro_extras({})
    assert pk == "" and cv == [] and ev == []


def test_especies_nao_entram_no_texto_busca_pdv():
    txt = montar_texto_busca_denormalizado("foo", ["antibiotico"])
    assert "caes" not in txt
    assert "bovinos" not in txt


def test_taxonomia_api_completa_para_ui():
    api = taxonomia_para_api()
    assert len(api.get("grupos") or []) >= 3
    assert len(api.get("grupos_prateleira") or []) == 9
    assert len(api.get("especies") or []) == 10


def test_simulacao_salvamento_preserva_campos_overlay_nao_enviados():
    """Campos de overlay só mudam se a chave vier no payload (espelho da view)."""
    estado = {
        "nome": "Produto X",
        "categoria": "Ração",
        "subcategoria": "Premium",
        "preco_venda": "12,50",
        "cadastro_extras": {
            "classes_vet": ["vermifugo"],
            "especies_vet": ["caes"],
            "fiscal": {"ncm": "23099020"},
        },
    }
    payload = {"nome": "Produto X", "preco_venda": "13,00"}
    ex = dict(estado["cadastro_extras"])
    aplicar_palavras_e_classes_em_extras(ex, payload_tem_palavras=False)
    if "nome" in payload:
        estado["nome"] = payload["nome"]
    if "preco_venda" in payload:
        estado["preco_venda"] = payload["preco_venda"]
    assert estado["categoria"] == "Ração"
    assert estado["subcategoria"] == "Premium"
    assert ex["classes_vet"] == ["vermifugo"]
    assert ex["especies_vet"] == ["caes"]
    assert ex["fiscal"]["ncm"] == "23099020"
