# Rollback — PDV-EDIT-CB-ETQ

**Pacote:** lápis PDV — Adicionar código + 6 cadastrados + Etiqueta com preset.  
**Versão teste:** v25.93  
**Migrate:** não.

## Voltar (só com frase + senha do Renan)

1. Tag sugerida antes do cherry: `rollback/pre-pdv-edit-cb-etq-v25.91` (tip loja anterior ao pacote).
2. Reverter no `producao` os commits deste pacote (wizard HTML/JS + API edicao_rapida opcionais se ainda não estiverem na loja por outro caminho).
3. Ctrl+F5 no PDV — some botão Etiqueta e os 6 campos do lápis.

**Não mexe em:** caixa, venda fechada, financeiro, estoque Mongo.
