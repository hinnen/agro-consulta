# PREP — checklist 27/10d · Contagens tela cheia + operador RH · v27.26.35

Base: `producao` @ `723f96ce` (v27.26.28).

Fora deste corte: **NFE-55-FISCAL-ENDERECO-PDV** (item 2). Já está na loja em `91c8ca6a`.

Entra só o que ainda não está na loja dos itens 1, 3 e 4:

1. **CICLICA-ADMIN-PAINEL** — Contagens abre em tela cheia no SistApp. No PC, F10 continua dentro da gestão.
2. **INVENTARIO-PIN-BLOCO** — operador marcado na lista do RH (não digita o PIN).

Migrate: **não**. PDV, caixa e checkout: **não**.

Revert: `git push origin rollback/pre-v272635-2710d-contagens-rh:producao` → `723f96ce`.
