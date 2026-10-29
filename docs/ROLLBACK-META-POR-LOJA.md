# Rollback — META-POR-LOJA (v27.01)

**Antes do deploy:** Live **v27.00** @ `14f5cad7`  
**Tag:** `rollback/pre-meta-por-loja-v27.00` @ `14f5cad7`  
**Backup branch (após cutover):** `producao-backup-pre-v2701-meta-por-loja`

## Reverter código

```bash
git fetch origin producao-backup-pre-v2701-meta-por-loja rollback/pre-meta-por-loja-v27.00
git push origin rollback/pre-meta-por-loja-v27.00:producao
```

Render redeploy automático → badge **v27.00**.

## Migration 0144

A migration **adiciona** `deposito` e clona faixas para Vila. Reverter código **não** remove a coluna; o app v27.00 ignora o campo se o model antigo não tiver `deposito` — **testar** `/meta/` após rollback. Se necessário, restaurar backup Postgres (Render) do instante pré-deploy.
