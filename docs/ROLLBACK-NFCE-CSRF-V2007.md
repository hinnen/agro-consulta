# Rollback — NFC-e CSRF lista (loja alvo **v20.07**)

Ponto **antes** deste deploy = loja **Live v20.01** (`e7f8154`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes)** | `e7f8154` (Live v20.01) |
| **Tag** | `rollback/pre-nfce-csrf-v20.01` |
| **Branch backup** | `producao-backup-pre-v2007-nfce-csrf-20260830` |
| **Branch PREP** | `deploy/prep-nfce-csrf-v2007` |
| **O quê sobe** | Só `NFCE-REEMIT-CSRF` |
| **O quê NÃO sobe** | Outros WIP do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git push origin e7f8154:producao --force-with-lease
# ou:
git push origin rollback/pre-nfce-csrf-v20.01:producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v20.01**.

## Arquivos

| Arquivo | Nota |
| ------- | ---- |
| `produtos/templates/produtos/vendas_lista.html` | `csrf()` + `{{ csrf_token }}` + token antes do POST |
| `scripts/verify_nfce_reemit_timeout_path.py` | prova path CSRF |
| `VERSION` | **20.07** |
| `banana.md` | CHECKPOINT Live |

## Pós-deploy

1. **Ctrl+F5** `/vendas/`
2. Badge **v20.07**
3. Reemitir #6507 **1×** — o pedido deve ir à SEFAZ (não só timeout 20s com 537 antigo)
