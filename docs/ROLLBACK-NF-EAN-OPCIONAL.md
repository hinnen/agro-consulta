# Rollback — NF-EAN-OPCIONAL (v26.80 → Live v26.79)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

**Checkpoint:** `735698c7` · tag `rollback/pre-checklist-1010b-v26.79`  
**Backup:** `producao-backup-pre-v2680-checklist-1010b`

```bash
git fetch origin
git push origin rollback/pre-checklist-1010b-v26.79:producao --force-with-lease
```

Migrate: nenhum. Ver também `docs/ROLLBACK-CHECKLIST-1010b.md`.
