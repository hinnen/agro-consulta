# PREP — checklist 28/10b · quantidade no bip · v27.26.41

Base: `producao` @ `3c7dbc89` (VERSION **27.26.40**).

Entra só **AJUSTE-QTD-BIP**:

- No bip, a quantidade daquele item usa −/+, −10/+10 e Ok. Sem teclado.
- Na lista, **Somar** ou **Alterar** o total daquela vez.
- Item já gravado em bloco fechado não é alterado por esse caminho.

Migrate: **não**. PDV, caixa e checkout: **não**.

Revert: `git push origin rollback/pre-v272641-2810b-qtd-bip:producao` → `3c7dbc89`.
