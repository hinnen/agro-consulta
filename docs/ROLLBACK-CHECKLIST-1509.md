# Rollback — Checklist 15/09 (v25.33)

Pacotes: `PDV-ENT-LOJA-LANC` · `MP-POINT-PIX-NAO-FECHA` · `PDV-VENDA-NAO-DUP`

## Antes (Live)

- Branch/tag: `rollback/pre-checklist-1509-v25.23`
- Backup branch: `producao-backup-pre-v2523-checklist-1509`
- Commit: `218db50` (Live v25.23)

## Reverter (só com frase + senha 99738595)

```bash
git checkout producao
git reset --hard rollback/pre-checklist-1509-v25.23
git push origin producao --force-with-lease
```

Ou merge da branch backup. Depois redeploy Render.

## Migrate

- Subida inclui `produtos.0132` (`client_request_id`).
- Rollback de código **não** remove a coluna sozinha (inofensiva se ficar).
