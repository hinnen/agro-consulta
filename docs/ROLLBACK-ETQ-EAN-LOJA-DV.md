# Rollback — ETQ-EAN-LOJA-DV

**O quê:** etiqueta **230… legado** imprime EAN-13 com **DV GS1** (ex. cadastro `2300000001480` → barras `2300000001488`); busca/index variantes cadastro↔bip.

**Migrate:** não.

**Antes:** Live **v26.74** (tip `teste` com **PDV-BALANCA-QTY-VALOR** + este fix).

**Rollback:** preferir rollback do **lote/checklist** do dia · **só** frase + senha.

**Você após deploy:** Ctrl+F5 · reimprimir etiquetas 230… legado · bip `2300000001488` → GM4046.
