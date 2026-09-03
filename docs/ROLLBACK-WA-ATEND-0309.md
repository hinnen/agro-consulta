# Rollback WhatsApp Atendimento (03/09)

Se precisar voltar a loja ao estado **antes** do WhatsApp lojas (Live v21.08):

1. Tag de segurança: `rollback/pre-wa-atend-0309-v21.08` (commit base Live v21.08 / `3a89b86`).
2. Branch espelho: `producao-backup-pre-wa-atend-0309-v21.08`.

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-wa-atend-0309-v21.08
git push origin producao
```

No Render, aguarde o deploy automático da branch `producao`.

**Atenção:** migrations WhatsApp (0108–0122) já aplicadas no Postgres da loja exigem plano de migrate reverso ou restore de backup de banco — só reset de código não desfaz tabelas. Prefira rollback de código + manter tabelas ociosas se o problema for só UI/ponte.

PDV: botão WhatsApp permanece **Em breve** (não abre chat).
