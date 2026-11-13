# PREP 13/11h — hora da entrega e câmera do SistApp · v27.26.66

Ainda **não** foi para a loja.

| | |
| - | - |
| 1 | Papel da entrega, painel e reimpressão no horário da loja. 19:49 UTC vira 16:49 |
| 2 | Câmera do Ajuste, Fotos e Etiquetas sai do site de fora e fica no próprio sistema. A letra também |
| Não muda | Valor da venda, caixa, PDV de cobrar |
| Migrate | Não |
| Base | `producao` @ `3b9fc063` (v27.26.65) |
| Volta | `git push origin 3b9fc063612d72754a0ac3a657b68a78fdb56bba:producao` |

Prova da hora: **VERIFY_OK 18**. Câmera: os arquivos locais batem. A prova com página aberta falta a chave desta máquina.

Na senha, com as vendas pausadas:

```bash
./scripts/prep_checkpoint_checklist_1311h.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='…' \
  ./scripts/cutover_loja_checklist_1311h.sh --exec
```
