# Rollback — CHECKLIST 10/10d (v26.82 → Live v26.80)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

**Checkpoint Live (antes do cutover):** `2ac4789c` · **v26.80**  
**Tag:** `rollback/pre-checklist-1010d-v26.80`  
**Backup branch:** `producao-backup-pre-v2682-checklist-1010d`

```bash
git fetch origin
git push origin rollback/pre-checklist-1010d-v26.80:producao --force-with-lease
```

Migrate: **nenhum**. Lojas: Ctrl+F5 · badge **v26.80**.
