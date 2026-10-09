# Rollback — PDV-VALOR-RS-MODAL (v26.76 → v26.74)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin tag rollback/pre-pdv-valor-rs-modal-v26.74
git checkout producao
git reset --hard rollback/pre-pdv-valor-rs-modal-v26.74
git push origin producao --force-with-lease
```

Tag = Live **v26.74**. Backup: `producao-backup-pre-v2676-valor-rs`. Migrate: nenhum.
