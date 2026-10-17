# Rollback — Checklist 08/10b (v26.66 → v26.60)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin tag rollback/pre-checklist-0810b-v26.60
git checkout producao
git reset --hard rollback/pre-checklist-0810b-v26.60
git push origin producao --force-with-lease
```

Tag aponta para o Live atual **v26.60** @ `e4206bbf`.  
Backup branch: `producao-backup-pre-v2666-checklist-20261008b`.

**Migrate:** nenhuma neste lote.
