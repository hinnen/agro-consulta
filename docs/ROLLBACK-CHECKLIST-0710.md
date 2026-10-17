# Rollback — lote checklist 07/10 (`deploy/prep-checklist-0710`)

Volta a loja ao estado **antes** deste lote (Live **v26.46**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `68ef04ce` · VERSION **26.46** |
| **Tag segurança** | `rollback/pre-checklist-0710-v26.46` |
| **Branch backup** | `producao-backup-pre-v2653-checklist-20261007` |
| **Branch PREP** | `deploy/prep-checklist-0710` · alvo **v26.53** |
| **Migrate** | **SIM** `0139` (AddField + backfill deposito; não apaga títulos) |
| **Merge `teste`?** | **NÃO** |

## Pacotes (2)

ETQ-PONTE-TOPBAR-BIP · FIADO-LOJA-COMPRA

```bash
git fetch origin tag rollback/pre-checklist-0710-v26.46
git checkout producao
git reset --hard rollback/pre-checklist-0710-v26.46
git push origin producao --force-with-lease
```

Após voltar o código: coluna `deposito` pode permanecer no banco (inofensiva). **Só** com frase + senha do Renan.
