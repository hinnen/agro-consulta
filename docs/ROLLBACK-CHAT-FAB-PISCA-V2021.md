# Rollback — Chat aba + pisca 2 cores (`PDV-CHAT-FAB-PISCA` · loja alvo **v20.21**)

Ponto **antes** deste deploy = loja **Live v20.16** (`f7f326e`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes)** | `f7f326e` (Live v20.16 · Chat+Promo) |
| **Tag** | `rollback/pre-v2021-chat-pisca-v2016` |
| **Branch backup** | `producao-backup-pre-v2021-chat-pisca-20260831` |
| **Branch PREP** | `deploy/prep-chat-pisca-v2020` |
| **O quê sobe** | Só `PDV-CHAT-FAB-PISCA` (CSS aba/janela + pisca laranja↔vermelho) |
| **O quê NÃO sobe** | Demais WIP do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git push origin f7f326e:producao --force-with-lease
# ou:
git push origin rollback/pre-v2021-chat-pisca-v2016:producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v20.16**.

## Arquivos (código)

| Arquivo | Pacote |
| ------- | ------ |
| `produtos/templates/produtos/partials/pdv/chat_loja_overlay.html` | Aba/janela + pisca 2 cores |
| `scripts/verify_pdv_chat_fab_path.py` | Prova path **8/8** |
| `VERSION` | **20.21** |

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| `PDV-CHAT-FAB-PISCA` | **Não** (finalizar) | Só CSS da aba/janela Chat |

**Rotina:** Ctrl+F5 nos PDVs · smoke: venda rápida · msg Chat de outro PC (aba piscando).
