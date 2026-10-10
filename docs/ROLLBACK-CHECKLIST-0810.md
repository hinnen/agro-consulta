# Rollback — Checklist 08/10 (v26.62 → v26.60)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin tag rollback/pre-checklist-0810-v26.60
git checkout producao
git reset --hard rollback/pre-checklist-0810-v26.60
git push origin producao --force-with-lease
```

Tag aponta para o Live atual **v26.60** @ `e4206bbf` (inclui o bip da balança que já está na loja).  
Backup branch: `producao-backup-pre-v2662-checklist-20261008`.

**Migrate:** nenhuma neste lote.
