# Rollback — Checklist 08/10c (v26.70 → v26.66)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin tag rollback/pre-checklist-0810c-v26.66
git checkout producao
git reset --hard rollback/pre-checklist-0810c-v26.66
git push origin producao --force-with-lease
```

Tag aponta para o Live atual **v26.66** @ `f29d3f8e`.  
Backup branch: `producao-backup-pre-v2670-checklist-20261008c`.

**Migrate `0141`:** cria `CreditoLimiteRevisaoDecisaoAgro`. O rollback do código não apaga essa tabela. Decisões já gravadas no lab ficam no Postgres.
