# Rollback — etiquetas térmicas barras laser (`ETQ-BARCODE-LASER`)

Volta a loja **Live v25.85** (estado **antes** deste pacote).

| Item | Valor |
| ---- | ----- |
| **Commit loja (antes)** | `7bb7aad5` |
| **Tag** | `rollback/pre-etq-barcode-laser-v25.85` |
| **Branch backup** | `producao-backup-pre-v2586-etq-barcode-laser-20261001` |
| **O quê sobe** | Barras 1D nas etiquetas térmicas: traço mais largo, quiet zone GS1, SVG sem encolher, cache core **v=25**. |
| **Migrate** | **NÃO** |

```bash
git fetch origin tag rollback/pre-etq-barcode-laser-v25.85
git checkout producao
git reset --hard rollback/pre-etq-barcode-laser-v25.85
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan.

## Prova

`node scripts/verify_etiquetas_termica_path.js` → **44/44**.
