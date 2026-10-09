# Rollback — PDV-VALOR-RS-KG (v26.77 → v26.76)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin
git checkout producao
git reset --hard 52da69c2
git push origin producao --force-with-lease
```

Volta ao Live **v26.76** (modal em todos os produtos). Migrate: nenhum.
