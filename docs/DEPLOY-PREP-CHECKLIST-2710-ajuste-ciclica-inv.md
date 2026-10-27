# PREP — checklist 27/10 itens 1, 3 e 4 · v27.26.28

Base: `producao` @ `91c8ca6a` (v27.26.27).

Fora deste corte: **NFE-55-FISCAL-ENDERECO-PDV** (item 2). A loja já tem o commit `91c8ca6a` com esse assunto.

1. **AJUSTE-CAMERA-BIP** — câmera no Ajuste do SistApp, bip contínuo com Bip +1.
2. **CICLICA-ADMIN-PAINEL** — painel da gerência e atalho **F10 → Estoque → Contagens**.
3. **INVENTARIO-PIN-BLOCO** — PIN direto, bloco, amostra e zerar saldo (digitar ZERAR). Migrate **estoque 0021**.

Também tira marcadores de conflito que já estavam em `produtos/views_nfe_saida.py` na loja (o arquivo não importava). O endpoint fiscal continua só para staff.

Revert: `git push origin rollback/pre-v272628-2710-ajuste-ciclica-inv:producao` → `91c8ca6a`.
