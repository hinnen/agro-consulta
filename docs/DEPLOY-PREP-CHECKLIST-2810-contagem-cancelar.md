# PREP — checklist 28/10 · cancelar contagem · v27.26.40

Base: `producao` @ `0fdbe24c` (VERSION **27.26.39**).

Entra só **CONTAGEM-CANCELAR**:

- Botão **Cancelar contagem** em cada sessão aberta do painel da gerência.
- Marca a sessão como cancelada. O estoque já gravado em bloco fechado permanece.

Migrate: **não**. PDV, caixa e checkout: **não**.

Revert: `git push origin rollback/pre-v272640-2810-contagem-cancelar:producao` → `0fdbe24c`.
