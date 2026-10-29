# Rollback — CHECKLIST 10/10g (v26.85 → Live v26.84)

**Checkpoint:** `35308cbf` · **v26.84**  
**Tag:** `rollback/pre-checklist-1010g-v26.84`

```bash
git fetch origin
git push origin rollback/pre-checklist-1010g-v26.84:producao --force-with-lease
```

Migrate: **nenhum**. Dados alterados pelo `migrar_cb_loja_legado` **permanecem** após rollback de código.
