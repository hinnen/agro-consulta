# Rollback — NFC-e reemit sync (loja alvo **v19.95**)

Ponto **antes** deste deploy = loja **Live v19.94** (`f84ac71`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes)** | `f84ac71` (Live v19.94) |
| **Tag** | `rollback/pre-nfce-reemit-sync-v19.94` |
| **Branch backup** | `producao-backup-pre-v1995-nfce-reemit-sync-20260830` |
| **Branch PREP** | `deploy/prep-nfce-reemit-sync-v1995` |
| **O quê sobe** | Só `NFCE-REEMIT-SYNC` (reemitir síncrono + Abort 28s) |
| **O quê NÃO sobe** | Outros WIP do `teste` · Meta C Vila |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git push origin f84ac71:producao --force-with-lease
# ou:
git reset --hard rollback/pre-nfce-reemit-sync-v19.94
git push origin HEAD:producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v19.94**.

## Arquivos

| Arquivo | Nota |
| ------- | ---- |
| `produtos/views_nfce.py` | Reemitir sync (sem thread) |
| `produtos/templates/produtos/vendas_lista.html` | Abort 28s · loading reset |
| `produtos/templates/produtos/venda_agro_detalhe.html` | Abort 28s · loading reset |
| `scripts/verify_nfce_reemit_timeout_path.py` | prova |
| `VERSION` | **19.95** |

## Pós-deploy

1. **Ctrl+F5** `/vendas/`
2. Badge **v19.95**
3. Reemitir #6478 **1×** · aguarda até ~20s · autoriza **ou** erro na tela
