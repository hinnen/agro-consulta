# Rollback — PDV balança USE-P2 (v17.80)

Hotfix do overlay **Pesar granel** (F10). Sem migrate.

## Produção — USE-P2 ESC N 1 (**v17.80**) — ponto **antes** deste pacote (Live v17.79)

| Item | Valor |
| ---- | ----- |
| **Commit anterior** | `589aa20` (Live v17.79 · hash banana) |
| **Tag** | `rollback/pre-pdv-balanca-use-p2-v17.79` |
| **Tag do pacote verificado** | `checkpoint-use-p2-pronto-99738595` |
| **Branch backup** | `producao-backup-pre-v1780-balanca-use-p2-20260823` |
| **O quê reverte** | parser USE-P2 (ESC N 1 / dump COM4) · serial 8N2 · SEM PORTA · auto-add |
| **Migrate** | **nenhuma** |

Se a loja falhar depois do v17.80, volte para v17.79:

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-pdv-balanca-use-p2-v17.79
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan (`99738595`). Volta para Live **v17.79**.

Para reaplicar o pacote verificado (não o v17.79):

```bash
git reset --hard checkpoint-use-p2-pronto-99738595
git push origin producao --force-with-lease
```
