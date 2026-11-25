# Corte 25/11 — etiquetas do Centro + código do fornecedor manual

**Enviado** `d6b22afb` (**v27.26.98**). Até o build terminar, no ar segue **v27.26.95** @ `5a6d514f`.

| # | Pacote | O que muda | Tabela nova |
| - | ------ | ---------- | ----------- |
| 1 | **PDV-ETQ-CENTRO** | O botão **Solicitações** separa **Loja do Centro** e **Demais**. Pedido da Vila não cai no Centro | Não |
| 2 | **CAD-COD-FORN-MANUAL** | Aba Fiscal: campo digitado à mão. Planilha: coluna **Cód. fornecedor manual**. Célula vazia não apaga. Hífen apaga. A busca e o próximo Ler XML também acham esse código. O campo da nota não muda | Não |

Não é merge do `teste`. Venda, caixa e fiado ficam como estão.

**Depois do build:** Ctrl+F5. Badge **v27.26.98**.

**Volta:** `git push origin 5a6d514f:producao`
