# PREP — checklist 29/10 · fiscal do XML na entrada de nota · v27.26.44

Base: `producao` @ `ddd7df5c` (VERSION **27.26.43**).

Entra só **NF-FISCAL-XML**:

- A entrada de nota grava no cadastro o NCM, o CSOSN, a origem e o CFOP de venda vindos do XML.
- Se o fiscal já estiver igual, não grava de novo e não cria linha no histórico.
- Se mudar, a aba Alterações mostra origem **Entrada NF**.
- CFOP de compra vira venda (`1102` → `5102`). CFOP de saída (`5405`) permanece.

Migrate: **não**. PDV, caixa e checkout: **não neste delta**.

Revert: `git push origin rollback/pre-v272644-2910-nf-fiscal-xml:producao` → `ddd7df5c`.
