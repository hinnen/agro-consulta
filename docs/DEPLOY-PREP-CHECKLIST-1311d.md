# PREP 13/11d — ligação do Zap no SistApp · v27.26.62

Ainda **não** foi para a loja.

A tela bonita fica. A lista e o “online” passam a vir do Zap que já está ligado. O celular não abre o outro site.

| | |
| - | - |
| Não entra | PDV, caixa, venda, banco |
| Migrate | Não |
| Base | `producao` @ `e3745fae` (v27.26.61) |
| Volta | `git push origin e3745faef6d1767026b6de7526354f23c2c08d20:producao` |

Na senha, com as vendas pausadas:

```bash
./scripts/prep_checkpoint_checklist_1311d.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='…' \
  ./scripts/cutover_loja_checklist_1311d.sh --exec
```
