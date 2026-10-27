# PREP — checklist 27/10g · quantidade no bloco fechado · v27.26.38

Base: `producao` @ `6d7da78c` (VERSION **27.26.37**).

Entra só o ajuste do **AJUSTE-BLOCO-TELA** que ainda não está na loja:

- Item de bloco já fechado mostra a **quantidade** (não a marca FALTA).
- O payload manda `contado` e `qtd_contada` nesse item.

Migrate: **não**. PDV, caixa e checkout: **não**.

Revert: `git push origin rollback/pre-v272638-2710g-bloco-qtd:producao` → `6d7da78c`.
