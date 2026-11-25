# Corte 25/11b — planilha não regrava célula igual

**Enviado** `3143bbb8` (**v27.26.99**). Até o build terminar, a loja segue o que já estava no ar. Volta para a **v27.26.98** @ `d6b22afb`.

| # | Pacote | O que muda | Tabela nova |
| - | ------ | ---------- | ----------- |
| 1 | **PLANILHA-IGUAL** | Na planilha de produtos, célula igual ao cadastro não vira alteração. Maiúscula diferente também não. Vale para qualquer arquivo. Só grava o que a célula mudou de verdade | Não |

Não mexe em PDV, caixa, venda nem estoque. Não é merge do `teste`.

**Depois do build:** Ctrl+F5. Badge **v27.26.99**.

**Volta:** `git push origin d6b22afb:producao`
