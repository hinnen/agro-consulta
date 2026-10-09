# Rollback — PDV-VALOR-RS-MODAL (v26.76 → v26.74)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin tag rollback/pre-checklist-0910f-v26.74
git checkout producao
git reset --hard rollback/pre-checklist-0910f-v26.74
git push origin producao --force-with-lease
```

Tag = Live **v26.74** (lote **09/10f** inteiro). Backup: `producao-backup-pre-v2676-checklist-0910f`. Migrate: nenhum.
