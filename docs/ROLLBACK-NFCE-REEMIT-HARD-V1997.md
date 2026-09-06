# Rollback — NFC-e reemit hard timeout (loja alvo **v19.97**)

Ponto **antes** deste deploy = loja **Live v19.95** (`29c3613`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes)** | `29c3613` (Live v19.95) |
| **Tag** | `rollback/pre-nfce-reemit-hard-v19.95` |
| **Branch backup** | `producao-backup-pre-v1997-nfce-reemit-hard-20260830` |
| **Branch PREP** | `deploy/prep-nfce-reemit-hard-v1997` |
| **O quê sobe** | Só `NFCE-REEMIT-HARD-TIMEOUT` |
| **O quê NÃO sobe** | Outros WIP do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git push origin 29c3613:producao --force-with-lease
# ou:
git reset --hard rollback/pre-nfce-reemit-hard-v19.95
git push origin HEAD:producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v19.95**.

## Arquivos

| Arquivo | Nota |
| ------- | ---- |
| `produtos/views_nfce.py` | Teto 20s + carimbo + shutdown wait=False |
| `produtos/sefaz_soap_util.py` | TIMEOUT_SYNC (3,12) |
| `produtos/templates/produtos/vendas_lista.html` | hardTimer 20s |
| `produtos/templates/produtos/venda_agro_detalhe.html` | hardTimer 20s |
| `produtos/tests_nfce_loja.py` | prova |
| `scripts/verify_nfce_reemit_timeout_path.py` | prova |
| `VERSION` | **19.97** |

## Pós-deploy

1. **Ctrl+F5** `/vendas/`
2. Badge **v19.97**
3. Reemitir #6507/#6478 **1×** · ≤20s autoriza **ou** mensagem vermelha
