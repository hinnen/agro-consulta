# PREP 15/11 — orçamento do botão Loja vai para o Zap · v27.26.72

Ainda **não** foi para a loja.

| | |
| - | - |
| O quê | O botão **Loja** do PDV encaminha o orçamento para `https://zap.zapsistvale.online` |
| Não muda | Venda, caixa, botão **Celular**, estoque |
| Migrate | Não |
| Base | `producao` @ `fba12fa1` (v27.26.71) |
| Volta | `git push origin fba12fa1a3ea1e2829fe951bb0c793885d8c2d63:producao` |

Prova nesta máquina: o corte aplica em cima da loja, o arquivo compila, o destino é o Zap e a loja não envia o WhatsApp sozinha.

Na senha, com as vendas pausadas:

```bash
./scripts/prep_checkpoint_checklist_1511_pdv_zap_orcamento.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='…' \
  ./scripts/cutover_loja_checklist_1511_pdv_zap_orcamento.sh --exec
```

Depois do build: `./scripts/post_deploy_checklist_1511_pdv_zap_orcamento.sh`
