# PREP — 23/10c + 24/10 · v27.26.26

Base: `producao` @ `d37449e6` (v27.26.23). Sem migrate.

1. **CP-EXPORT-LISTA** — botão Exportar em `/lancamentos/contas-pagar/` (CSV, Excel, PDF, mesmos filtros, até 5.000).
2. **CADASTRO-BUSCA-PALAVRAS** — busca do cadastro alinhada ao PDV (palavras e acento: SHAMPO CAO = CÃO). Cache de busca v12.
3. **Gaveta da busca no PDV** — mais larga só na coluna do nome; GM e preço fixos.

Caixa e checkout não entram.

Revert: `git push origin rollback/pre-v272626-2410-cp-busca:producao` → `d37449e6`.
