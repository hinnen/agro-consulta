# Rollback — CHECKLIST 10/10 (v26.79 → v26.78.1)

Somente com autorização explícita e senha.

```bash
git fetch origin tag rollback/pre-checklist-1010-v26.78.1
git push origin rollback/pre-checklist-1010-v26.78.1:producao --force-with-lease
```

Checkpoint: `5f460cb6` · migrate: nenhum.
