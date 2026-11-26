# Corte 26/11 — aviso de etiqueta com o nome

**Ainda não enviado.** Loja no ar: **v27.26.102** @ `21fd9bb3`.

| # | Pacote | O que muda | Tabela nova |
| - | ------ | ---------- | ----------- |
| 1 | **PDV-ETQ-NOME** | O aviso de etiqueta mostra o nome do cadastro no lugar do id. Produto novo, quando o preço sai de zero, não entra na lista. Preço igual continua sem aviso | Não |

Não mexe em venda nem caixa. Não é merge do `teste`.

**Na senha, com as vendas pausadas:** `./scripts/cutover_loja_checklist_2611.sh --exec`

**Logo depois:** se o painel não começar o build, disparar.

**Depois do build:** Ctrl+F5. Badge **v27.26.105**.

**Volta:** `git push origin 21fd9bb3:producao`
