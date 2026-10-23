# Rollback — Chat PDV + Promo regra (`PDV-CHAT-FAB-UX` + `PROMO-REGRA-TABELA-SAVE` · loja alvo **v20.16**)

Ponto **antes** deste deploy = loja **Live v20.07** (`91a610a`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes)** | `91a610a` (Live v20.07 · NFCE-REEMIT-CSRF) |
| **Tag** | `rollback/pre-v2016-chat-promo-v2007` |
| **Branch backup** | `producao-backup-pre-v2016-chat-promo-20260831` |
| **Branch PREP** | `deploy/prep-chat-promo-v2016` |
| **O quê sobe** | Só checklist: `PDV-CHAT-FAB-UX` + `PROMO-REGRA-TABELA-SAVE` |
| **O quê NÃO sobe** | `BI-META-C-VILA-RAMP` e demais WIP do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git push origin 91a610a:producao --force-with-lease
# ou:
git push origin rollback/pre-v2016-chat-promo-v2007:producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v20.07**.

## Arquivos (código)

| Arquivo | Pacote |
| ------- | ------ |
| `produtos/templates/produtos/partials/pdv/chat_loja_overlay.html` | Chat maior + alerta |
| `produtos/static/produtos/js/pdv_chat_loja.js` | Badge/title mensagem nova |
| `produtos/templates/produtos/includes/promocoes_form_script.html` | Salva `regra_vs_tabela` |
| `VERSION` | **20.16** |

## Prova (teste local / path)

| Script | Resultado |
| ------ | --------- |
| `scripts/verify_pdv_chat_fab_path.py` | **5/5** |
| `scripts/verify_promo_regra_tabela_path.py` | **23/23** |

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| `PDV-CHAT-FAB-UX` | **Não** (finalizar) | Só CSS/JS da aba Chat |
| `PROMO-REGRA-TABELA-SAVE` | **Não** (finalizar) | Só tela editar promoção |

**Rotina deploy:** pausar vendas ~2 min · Ctrl+F5 nos PDVs · smoke: venda rápida dinheiro · abrir Chat · editar promo «Sempre promoção» e salvar.

## Deploy (próximo chat + senha)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-chat-promo-v2016
git push origin producao --force-with-lease
```

Render sobe · **sem migrate** · conferir healthz / badge **v20.16**.
