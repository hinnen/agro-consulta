# PREP 13/11c — Zap dentro do SistApp · v27.26.61

Ainda **não** foi para a loja.

| | |
| - | - |
| Entra | O botão **Zap** abre a tela no mesmo aplicativo. Faixa fininha. **Início** volta ao menu |
| Continua | O ícone do Zap na tela do celular, no endereço separado |
| Não entra | PDV, caixa, venda, banco |
| Migrate | Não |
| Base | `producao` @ `8cc412a9` (v27.26.60) |
| Volta | `git push origin 8cc412a936a6e49099ed27bbc2a86f0b53b93442:producao` |

O PIN do SistApp vale nessa tela. Sem ele a lista não abriria.

Na senha, com as vendas pausadas:

```bash
./scripts/prep_checkpoint_checklist_1311c.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='…' \
  ./scripts/cutover_loja_checklist_1311c.sh --exec
```

Prova: menu **0 falhas**. O botão não aponta mais para `zapsistvale.online`.
