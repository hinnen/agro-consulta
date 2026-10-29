# Rollback — CHECKLIST 10/10f (v26.84 → Live v26.83)

**Só** com frase explícita + senha `99738595` na mesma mensagem.

**Checkpoint Live (antes do cutover):** `0c39a6e0` · **v26.83**  
**Tag:** `rollback/pre-checklist-1010f-v26.83`  
**Backup branch:** `producao-backup-pre-v2684-checklist-1010f`

```bash
git fetch origin
git push origin rollback/pre-checklist-1010f-v26.83:producao --force-with-lease
```

Migrate: **nenhum**. Códigos já promovidos pelo `migrar_cb_loja_legado` **permanecem** (rollback de app não desfaz dados — anotar se precisar reverter manualmente).

Lojas: Ctrl+F5 · badge **v26.83**.
