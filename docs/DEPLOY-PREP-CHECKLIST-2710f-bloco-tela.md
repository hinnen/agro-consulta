# PREP — checklist 27/10f · bloco e amostra · v27.26.37

Base: `producao` @ `d8b5ce7b` (v27.26.36).

Entra só **AJUSTE-BLOCO-TELA**:

- Amostra para imprimir só no painel do computador.
- No celular, o item do bloco já gravado fica marcado como fechado.
- Produto achado de novo soma em cima do saldo atual (5+2=7).

Migrate: **não**. PDV, caixa e checkout: **não**.

Revert: `git push origin rollback/pre-v272637-2710f-bloco-tela:producao` → `d8b5ce7b`.
