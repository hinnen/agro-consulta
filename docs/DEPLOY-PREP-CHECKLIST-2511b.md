# Corte 25/11b — planilha não regrava célula igual

**Ainda não enviado.** Loja no Git: **v27.26.98** @ `d6b22afb`.

| # | Pacote | O que muda | Tabela nova |
| - | ------ | ---------- | ----------- |
| 1 | **PLANILHA-IGUAL** | Na planilha de produtos, célula igual ao cadastro não vira alteração. Maiúscula diferente também não. Vale para qualquer arquivo. Só grava o que a célula mudou de verdade | Não |

Não mexe em PDV, caixa, venda nem estoque. Não é merge do `teste`.

**Na senha, com as vendas pausadas:** `./scripts/cutover_loja_checklist_2511b.sh --exec`

**Depois do build:** Ctrl+F5. Badge **v27.26.99**.

**Volta:** `git push origin d6b22afb:producao`
