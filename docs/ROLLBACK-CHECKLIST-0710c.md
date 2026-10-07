# Rollback — Checklist 07/10c (v26.60 → v26.56)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin tag rollback/pre-checklist-0710c-v26.56
git checkout producao
git reset --hard rollback/pre-checklist-0710c-v26.56
git push origin producao --force-with-lease
```

Tag aponta para Live anterior **v26.56** @ `d08617b0`.  
Backup branch: `producao-backup-pre-v2660-checklist-20261007c`.

**Migrate `0140`:** tabela `ContaBancariaLojaAgro` fica no Postgres (não apaga sozinha). Contas novas da loja continuam no banco; código antigo volta a usar fonte anterior. Se precisar limpar a tabela, fazer à mão depois — **não** é automático neste rollback.
