# Rollback — CHECKLIST 10/10b (v26.80 → Live v26.79)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

**Checkpoint Live (antes do cutover):** `735698c785ad385d0c803c8c293e6a8960a32018` (`735698c7`) · **v26.79**  
**Tag:** `rollback/pre-checklist-1010b-v26.79`  
**Backup branch:** `producao-backup-pre-v2680-checklist-1010b`

```bash
git fetch origin
git push origin rollback/pre-checklist-1010b-v26.79:producao --force-with-lease
```

Migrate: **nenhum**. Lojas: Ctrl+F5 · badge **v26.79**.
