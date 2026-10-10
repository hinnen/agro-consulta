# Rollback — CHECKLIST 10/10h (v26.86 → Live v26.85)

```bash
git fetch origin rollback/pre-checklist-1010h-v26.85
git push origin rollback/pre-checklist-1010h-v26.85:producao --force-with-lease
```

Aguardar Render · Ctrl+F5 · badge **v26.85**.

Se a migração `--liberar-intruso` já rodou em v26.86, os códigos 230 alterados **não** voltam sozinhos — anotar PIDs/códigos antes do Shell se quiser conferência manual.
