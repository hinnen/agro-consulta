# Corte 24/11 — preço do pacote + etiquetas do PDV

Ainda **não** foi para a loja. A loja segue **v27.26.86** @ `a7fa0150`.

| # | Pacote | O que muda | Tabela nova |
| - | ------ | ---------- | ----------- |
| 1 | **NF-PRECO-PACOTE** | Pacote com várias unidades não multiplica o preço de novo ao reabrir a nota ou marcar acréscimo | Não |
| 2 | **PDV-ETQ-PEDIDO** | Botão **Etiquetas** no PDV, mesmo pedido do celular | Não |

Não é merge do `teste`. Venda, caixa, fiado e estoque ficam como estão. A troca de preço que já está na loja continua.

**Na pausa, com frase + senha na mesma mensagem:**

`AGRO_LOJA_FRASE` + `AGRO_LOJA_SENHA` → `./scripts/cutover_loja_checklist_2411_preco_etq.sh --exec`

Sem `--exec` o script só monta e confere. Não envia.

**Depois do build:** Ctrl+F5. Badge **v27.26.95**. Entrada de nota com pacote. PDV → **Etiquetas**.

**Volta:** `git push origin a7fa0150:producao`
