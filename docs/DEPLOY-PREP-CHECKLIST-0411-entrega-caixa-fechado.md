# PREP — entrega a pagar depois que o caixa fecha · v27.26.50

Base: `producao` @ `2d002112` (VERSION **27.26.49**).

Entra:

- A PAGAR do PDV continua mostrando a entrega com pagamento na entrega depois que o caixa de origem fechou.
- O caixa novo da mesma loja continua travado até pagar, cancelar ou adiar.
- Dia futuro não trava hoje. Vila e Centro não se misturam.
- Caixa Teste fechado não trava a gaveta.

Não entra: soltar o vínculo da sessão no fechamento. Sem migrate.

Revert: `git push origin rollback/pre-v272650-0411-entrega-caixa-fechado:producao` → `2d002112`.
