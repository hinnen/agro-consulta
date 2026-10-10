# Rollback — NF-EAN-OPCIONAL (v26.80 → Live v26.79)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

```bash
git fetch origin
git checkout producao
git reset --hard origin/producao   # tip Live v26.79 antes do cutover; ou SHA do CHECKPOINT Live
git push origin producao --force-with-lease
```

Migrate: nenhum. Reverter tip PREP / commits do pacote na `producao`.
