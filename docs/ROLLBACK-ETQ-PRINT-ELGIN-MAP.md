# Rollback — ETQ-PRINT-ELGIN-MAP (`deploy/prep-etq-print-elgin-map`)

Volta a loja ao estado **antes** deste pacote (Live **v26.35**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `c517af0a` · VERSION **26.35** |
| **Tag segurança** | `rollback/pre-etq-print-elgin-map-v26.35` |
| **Branch backup** | `producao-backup-pre-v2640-etq-elgin-map` |
| **Branch PREP** | `deploy/prep-etq-print-elgin-map` · alvo **v26.40** |
| **Migrate** | **NÃO** |
| **Merge `teste`?** | **NÃO** |

## Pacote

**ETQ-PRINT-ELGIN-MAP** — mapa tamanho→impressora (Elgin 40×40 / 50×30)

```bash
git fetch origin tag rollback/pre-etq-print-elgin-map-v26.35
git checkout producao
git reset --hard rollback/pre-etq-print-elgin-map-v26.35
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan.
