# PREP — checklist 27/10e · bolinha Fechar + câmera sem duplicar · v27.26.36

Base: `producao` @ `7499c746` (v27.26.35).

Entra só **AJUSTE-BOLINHA-CAMERA** (`mobile_ajuste.html`):

- Bolinha da contagem cíclica ganha **Fechar** (e Esc).
- Câmera não soma o mesmo código duas vezes (intervalo 2,5 s e mudo de 1,6 s depois do bip).

Migrate: **não**. PDV, caixa e checkout: **não**.

Revert: `git push origin rollback/pre-v272636-2710e-bolinha-camera:producao` → `7499c746`.
