# Rollback — NFC-e reemit reforço (loja alvo **v19.92**)

Ponto **antes** deste deploy = loja **Live v19.83** (`09d5968` · lote checklist 3008).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `09d5968` (Live v19.83) |
| **Tag (criada no PREP)** | `rollback/pre-nfce-reemit-v19.83` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v1992-nfce-reemit-20260830` |
| **Branch PREP** | `deploy/prep-nfce-reemit-v1992` · tip **v19.92** |
| **O quê sobe** | Só `NFCE-REEMIT-TIMEOUT` (reforço: timeout 1 tentativa · Abort 22s · tip 537 · grava doc após SEFAZ) |
| **O quê NÃO sobe** | Demais WIP do `teste` · `BI-META-C-VILA-RAMP` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-nfce-reemit-v19.83
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v19.83**.

## Arquivos neste PREP

| Arquivo | Nota |
| ------- | ---- |
| `produtos/sefaz_soap_util.py` | Sync (4,15)s · 1 tentativa |
| `produtos/views_nfce.py` | Lock 45s · perfil sync |
| `produtos/nfce_sp_emissao_util.py` | `_gravar_doc_nfce_venda` · não apaga rejeitada antes da SEFAZ |
| `produtos/templates/produtos/vendas_lista.html` | Abort 22s · tip 537 · Abort no abrir modal |
| `produtos/templates/produtos/venda_agro_detalhe.html` | Abort 22s |
| `scripts/verify_nfce_*_path.py` | Prova path |
| `VERSION` | **19.92** |

## Prova

| Script | Resultado |
| ------ | --------- |
| `verify_nfce_reemit_timeout_path.py` | **44/44** |
| `verify_nfce_desc_itens_path.py` | **67/67** (#6478/#6507) |

## Rotina pós-deploy

1. **Ctrl+F5** em `/vendas/` (Centro + Vila).
2. Badge home **v19.92**.
3. Reemitir #6478 **1×** · ≤~22s (autoriza ou aviso claro).
4. Não spammar o botão.
