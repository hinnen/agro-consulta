# Rollback — CHECKLIST 11/10

| Cenário | Tag / branch | Commit live |
| ------- | ------------ | ----------- |
| **Antes do hotfix gerência (v26.97)** | `rollback/pre-checklist-1110-v26.94` | `da745c08` · pacote 11/10 já na loja |
| **Antes do pacote 11/10 inteiro** | `rollback/pre-checklist-1110-v26.90` | `b2a7b02d` |

**Backup no cutover `--exec`:** `producao-backup-pre-v2697-checklist-1110` (aponta para produção imediatamente antes do push).

## Reverter hotfix VL-PIN (volta v26.94)

```bash
git push origin rollback/pre-checklist-1110-v26.94:producao
```

## Reverter pacote inteiro (volta v26.90)

```bash
git push origin rollback/pre-checklist-1110-v26.90:producao
```

Migrate **não** desfaz tabelas `0142`/`0143` — aceitável para rollback só de código.
