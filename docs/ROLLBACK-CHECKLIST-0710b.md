# Rollback — Checklist 07/10b (v26.56 → v26.53)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin tag rollback/pre-checklist-0710b-v26.53
git checkout producao
git reset --hard rollback/pre-checklist-0710b-v26.53
git push origin producao --force-with-lease
```

Tag aponta para Live anterior **v26.53** @ `32198975`.  
Backup branch: `producao-backup-pre-v2656-checklist-20261007b`.

**Migrate:** nenhuma neste lote (não precisa reverter migration).
