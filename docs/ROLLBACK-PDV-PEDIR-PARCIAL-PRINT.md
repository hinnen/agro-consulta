# Rollback — PDV-PEDIR-PARCIAL-PRINT

Revert só o pacote Pedir loja: imprimir todos, lista limpa, □ parcial, Aceitar todos, badges RESTANTE/ENVIO PARCIAL.

**Não** rodar sem frase explícita + senha de produção do Renan.

## O que reverter

- `produtos/static/produtos/js/pdv_pedir_loja.js`
- `produtos/templates/produtos/partials/pdv/pedir_loja_overlay.html`
- `produtos/pdv_transf_loja_util.py` (`adiar_item_ids` / resto / `eh_resto`)
- `produtos/views_pdv_transf_loja.py` (ordenação + adiar)

## O que NÃO mexer

- Migrate `estoque.0018` / `0020` (já na loja)
- Transferência forçada, chat, topbar

## Migrate

**Nenhuma.**
