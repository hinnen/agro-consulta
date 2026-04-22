"""
Compatibilidade: a busca por código foi movida para ``mongo_index_codigos`` (campo ``index_codigos``).
"""
from produtos.mongo_index_codigos import (  # noqa: F401
    AGRO_INDEX_AT_CAMPO,
    CAMPOS_CODIGO_RAIZ_MONGO,
    INDEX_CODIGOS_CAMPO,
    aplicar_index_codigos_no_mongo,
    coletar_extras_agro_para_busca,
    encontrar_produto_casar_entrada_nfe,
    extrair_index_codigos_de_documento_mongo,
    mapa_extras_agro_por_produto_externo_id,
    merge_busca_codigo_prioridade_principal,
    montar_index_codigos_final,
    mongo_query_so_index_codigo,
    produto_termo_bate_campos_principais,
)
