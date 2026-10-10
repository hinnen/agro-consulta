# Rollback — CHECKLIST 09/10h (v26.78 → v26.76)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin tag rollback/pre-checklist-0910h-v26.76
git checkout producao
git reset --hard rollback/pre-checklist-0910h-v26.76
git push origin producao --force-with-lease
```

Tag = Live **v26.76** (`52da69c2`). Backup: `producao-backup-pre-v2678-checklist-0910h`. Migrate Django: nenhum.
