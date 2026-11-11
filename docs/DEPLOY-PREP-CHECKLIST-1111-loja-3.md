# PREP — 11/11 · v27.26.55 · três pacotes

| Pacote | O que muda na loja | Migrate |
| ------ | ------------------ | ------- |
| Unificar fornecedor na nota | Mesmo fornecedor com nomes diferentes junta na entrada da nota. Contas a pagar antigas não mudam. | Não |
| Point pago + log do PDV | Maquininha que já cobrou grava a venda. Registro em Gestão → LOG. | **0155** (tabela nova) |
| PIN da saída | Retirada grava quem digitou o PIN. PIN repetido é recusado. | Não |

**Base loja:** `producao` @ `af7af8cc` · v27.26.53.

**Cutover:** `./scripts/cutover_loja_checklist_1111_loja_3.sh`
**Checkpoint:** `./scripts/prep_checkpoint_checklist_1111_loja_3.sh`
**Pós-deploy (no Render, depois do build):** `./scripts/post_deploy_checklist_1111_loja_3.sh`
**Revert:** `git push origin rollback/pre-v272655-1111-loja-3:producao` → v27.26.53 @ `af7af8cc`

A loja não sobe com este PREP. O cutover com `--exec` exige frase e senha na hora.
