# Rollback — NFC-e stamp-first (loja alvo **v20.01**)

Ponto **antes** deste deploy = loja **Live v19.97** (`5430647`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes)** | `5430647` (Live v19.97) |
| **Tag** | `rollback/pre-nfce-stamp-first-v19.97` |
| **Branch backup** | `producao-backup-pre-v2001-nfce-stamp-first-20260830` |
| **Branch PREP** | `deploy/prep-nfce-stamp-first-v2001` |
| **O quê sobe** | Só `NFCE-REEMIT-STAMP-FIRST` |
| **O quê NÃO sobe** | Outros WIP do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git push origin 5430647:producao --force-with-lease
# ou:
git reset --hard rollback/pre-nfce-stamp-first-v19.97
git push origin HEAD:producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v19.97**.

## Arquivos

| Arquivo | Nota |
| ------- | ---- |
| `produtos/views_nfce.py` | Carimbo antes do Redis · teto 18s |
| `config/settings.py` | Redis SOCKET timeout 2s |
| `produtos/nfce_config_util.py` | warmup=False no resumo |
| `produtos/templates/produtos/vendas_lista.html` | msg timeout + Tentativa |
| `produtos/templates/produtos/venda_agro_detalhe.html` | idem |
| `produtos/tests_nfce_loja.py` | prova |
| `scripts/verify_nfce_reemit_timeout_path.py` | prova |
| `VERSION` | **20.01** |

## Pós-deploy

1. **Ctrl+F5** `/vendas/`
2. Badge **v20.01**
3. Reemitir #6507 **1×** → F5 → erro deve virar «Tentativa HH:MM…» ou autorizar/timeout gravado
