# PREP — checklist 28/10d · horário do bip e correção do bloco · v27.26.42

Base: `producao` @ `dcaa89bb` (VERSION **27.26.41**).

Entra só **AJUSTE-BIP-HORA**:

- Bip de produto já contado avisa e soma. O horário fica o do **primeiro** bip.
- **Alterar** um item de bloco já fechado regrava o saldo. Não soma de novo o que o bloco já gravou.
- Venda depois do primeiro bip continua na conta.

Migrate: **não**. PDV, caixa e checkout: **não**.

Revert: `git push origin rollback/pre-v272642-2810d-bip-hora:producao` → `dcaa89bb`.
