# Rollback — PDV-BALANCA-QTY-VALOR (v26.74 → v26.71)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin tag rollback/pre-pdv-balanca-qty-valor-v26.71
git checkout producao
git reset --hard rollback/pre-pdv-balanca-qty-valor-v26.71
git push origin producao --force-with-lease
```

Tag = Live **v26.71** @ `2eab76c1`.  
Backup: `producao-backup-pre-v2674-qty-valor` (mesmo tip).  
Alias já existente: `rollback/pre-pdv-balanca-preco-v26.71` (mesmo SHA).

**Migrate:** nenhum neste pacote — rollback só código/static.
