# Rollback — lote checklist 05/10c (`deploy/prep-checklist-0510c`)

Volta a loja ao estado **antes** deste lote (Live **v26.08**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `8b636b67` · VERSION **26.08** |
| **Tag segurança** | `rollback/pre-checklist-0510c-v26.08` |
| **Branch backup** | `producao-backup-pre-v2635-checklist-20261005` |
| **Branch PREP** | `deploy/prep-checklist-0510c` · alvo **v26.35** |
| **Migrate** | **NÃO** |
| **Merge `teste`?** | **NÃO** |

## Pacotes (7)

PDV-PEDIR-PARCIAL-RESTO · PDV-PEDIR-PRONTO-TRANSF · META-MODO-AGORA · CREDITO-SCORE-XLSX-COLS · ETQ-PRESET-ESPELHO · ETQ-PRINT-DIRETO · PDV-PEDIR-PRINT-3

```bash
git fetch origin tag rollback/pre-checklist-0510c-v26.08
git checkout producao
git reset --hard rollback/pre-checklist-0510c-v26.08
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan.
