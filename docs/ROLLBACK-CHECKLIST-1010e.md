# Rollback — CHECKLIST 10/10e (v26.83 → Live v26.82)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

**Checkpoint Live (antes do cutover):** `b7bdde7b` · **v26.82**  
**Tag:** `rollback/pre-checklist-1010e-v26.82`  
**Backup branch:** `producao-backup-pre-v2683-checklist-1010e`

```bash
git fetch origin
git push origin rollback/pre-checklist-1010e-v26.82:producao --force-with-lease
```

Migrate: **nenhum**. Lojas: Ctrl+F5 · badge **v26.82**.
