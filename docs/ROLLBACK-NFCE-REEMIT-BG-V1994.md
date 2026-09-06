# Rollback — NFC-e reemit background (loja alvo **v19.94**)

Ponto **antes** deste deploy = loja **Live v19.92** (`116aa74`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes)** | `116aa74` (Live v19.92) |
| **Tag** | `rollback/pre-nfce-reemit-bg-v19.92` |
| **Branch backup** | `producao-backup-pre-v1994-nfce-reemit-bg-20260830` |
| **Branch PREP** | `deploy/prep-nfce-reemit-bg-v1994` |
| **O quê sobe** | Só `NFCE-REEMIT-BG` (reemitir em thread + poll) |
| **O quê NÃO sobe** | Outros WIP do `teste` · Meta C Vila |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git push origin 116aa74:producao --force-with-lease
# ou:
git reset --hard rollback/pre-nfce-reemit-bg-v19.92
git push origin HEAD:producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v19.92**.

## Arquivos

| Arquivo | Nota |
| ------- | ---- |
| `produtos/views_nfce.py` | Thread + 202 |
| `produtos/nfce_venda_util.py` | processando = lock |
| `produtos/templates/produtos/vendas_lista.html` | poll |
| `produtos/templates/produtos/venda_agro_detalhe.html` | poll |
| `produtos/tests_nfce_loja.py` | prova |
| `scripts/verify_nfce_reemit_timeout_path.py` | prova |
| `VERSION` | **19.94** |

## Pós-deploy

1. **Ctrl+F5** `/vendas/`
2. Badge **v19.94**
3. Reemitir #6478 **1×** · aparece «Em emissão…» · espera até ~1 min
