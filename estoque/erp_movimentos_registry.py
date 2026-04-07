"""
Registro de referência: movimentos que **devem** existir no Mongo espelho do ERP
para o saldo do Agro acompanhar a realidade.

O Agro **não** grava de volta no ERP; o processo externo (SisVale / integração)
deve alimentar o Mongo. Coleções usadas no código (``integracoes/venda_erp_mongo.py``):

- ``DtoProduto`` — cadastro
- ``DtoEstoqueDepositoProduto`` — saldo por produto e depósito (base do PDV)
- ``DtoPessoa`` — clientes/fornecedores

Outras leituras (ex.: movimentos de compra/venda, NF) aparecem em ``produtos/views.py``
e utilitários Mongo conforme integração ativa.

Para **camada Agro** (delta sobre o Mongo): ver ``AjusteRapidoEstoque`` e
``OrigemAjusteEstoque`` em ``estoque/models.py``.
"""

# Tipos lógicos (documentação; o ERP pode usar outros nomes internos)
TIPOS_MOVIMENTO_ERP_ESPERADOS = (
    "compra_entrada_mercadoria",
    "venda_baixa_estoque",
    "devolucao_venda",
    "devolucao_compra",
    "transferencia_entre_depositos",
    "ajuste_inventario_erp",
    "producao_consumo",
    "bonificacao",
    "perda_quebra",
    "outro",
)
