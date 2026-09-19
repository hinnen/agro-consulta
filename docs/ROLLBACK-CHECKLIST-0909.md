# Rollback — Checklist 09/09 (loja alvo **v23.70**)

Ponto **antes** deste lote = loja **Live v23.58** (`0e0c419` / checklist 08/09).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `0e0c419` (Live v23.58) |
| **Tag (criar no deploy)** | `rollback/pre-checklist-0909-v23.58` |
| **Branch backup (criar no deploy)** | `producao-backup-pre-v2370-checklist-20260909` |
| **Branch PREP** | `deploy/prep-checklist-0909` · tip **v23.70** |
| **O quê sobe** | `PDV-ENTREGA-PAGAS-24H` · `PDV-ENTREGA-LOJA-SAIDA` · `CAIXA-ENTREGA-ADIAR` · `TAREFAS-DETALHE-LOTE` · `REPASSE-COFRE-PLANO` · `REPASSE-GESTAO-SIMPLES` |
| **O quê NÃO sobe** | merge do `teste` · WhatsApp · Excel cadastro · DRE WIP · outros |
| **Migrate** | **SIM** `produtos.0128` + `produtos.0129` (Render no deploy) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-0909-v23.58
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.58**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `CAIXA-ENTREGA-ADIAR` | **Médio** (caixa/PDV Entregas) | Retomar + Adiar 1 dia; não muda venda balcão |
| `PDV-ENTREGA-LOJA-SAIDA` | **Médio** (só fluxo Entrega) | Escolha Centro/Vila; outra loja → painel sem Assumir |
| `PDV-ENTREGA-PAGAS-24H` | **Baixo–médio** (overlay Entregas) | Aba pagas 24 h; **não** trava fechar caixa |
| `TAREFAS-DETALHE-LOTE` | **Não** | Só hub `/vendas/lojas/tarefas/` |
| `REPASSE-GESTAO-SIMPLES` | **Não** (PDV) | Só `/repasse-vila/` layout |
| `REPASSE-COFRE-PLANO` | **Não** (PDV) | Retirada cofre com plano (empresa Vila) |

## Provas (pré-envio · refeitas 09/09)

| Pacote | Prova |
| ------ | ----- |
| `PDV-ENTREGA-PAGAS-24H` | `verify_pdv_entrega_pagas_loja_path.py` **64/64** · PIN 9973=Renan |
| `PDV-ENTREGA-LOJA-SAIDA` | `verify_pdv_entrega_loja_saida_path.py` **12/12** |
| `CAIXA-ENTREGA-ADIAR` | `verify_caixa_entrega_adiar_path.py` **26/26** |
| `TAREFAS-DETALHE-LOTE` | `verify_vl_hub_tarefas_path.py` **90/90** · PIN 9973 |
| `REPASSE-COFRE-PLANO` | `verify_repasse_cofre_plano_path.py` **62/62** |
| `REPASSE-GESTAO-SIMPLES` | `verify_repasse_gestao_simples_path.py` **64/64** |
| Django | `manage.py check` OK |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.70**
2. Confirmar migrate no Render (0128 + 0129)
3. PDV Entrega → loja sai · paga na loja → aba **Pagas** · Fechar caixa com pendência (Retomar / Adiar)
4. Tarefas → título + Salvar alterações
5. `/repasse-vila/` → Retirada cofre com plano
