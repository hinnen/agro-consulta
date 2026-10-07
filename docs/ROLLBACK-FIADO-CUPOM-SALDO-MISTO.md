# Rollback — FIADO-CUPOM-SALDO-MISTO (v26.56)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-fiado-cupom-saldo-misto-v26.53
git push origin producao
```

Volta o cupom 80mm ao layout anterior (TOTAL grande também no misto). Sem migrate.
