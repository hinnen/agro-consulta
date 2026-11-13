# PREP 13/11b — botão Zap no SistApp · v27.26.60

Ainda **não** foi para a loja.

| | |
| - | - |
| Entra | No SistApp, o botão **Zap** abre `https://zap.zapsistvale.online/atendimento-whatsapp/celular/?sistapp=1` |
| Fica | Vendas, Tarefas, Fotos, Etiquetas, Ajuste e Contagens |
| Não entra | PDV, caixa, venda, PIN do Zap, tela do Zap, banco |
| Migrate | Não |
| Base | `producao` @ `418fb789` (v27.26.59) |
| Volta | `git push origin 418fb789bb88ad8098cada93fc254a6fcc4be272:producao` |

A página do Zap respondeu **200** e não cai em `/entrar/`. A loja ainda não tem o botão Dashboard nesse menu; este corte não cria esse botão.

Na senha, com as vendas pausadas:

```bash
./scripts/prep_checkpoint_checklist_1311b.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='…' \
  ./scripts/cutover_loja_checklist_1311b.sh --exec
```

Prova: `verify_vendas_lojas_pwa_install.py` **0 falhas**, inclusive `hub_zap_loja`.
