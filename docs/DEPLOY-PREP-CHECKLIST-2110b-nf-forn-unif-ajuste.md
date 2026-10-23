# PREP — **21/10b** · **v27.26.19** · NF-FORN-UNIF-ALIAS + NF-AJUSTE-CUSTO-MANUAL

| Pacote | Migrate | Prova |
| ------ | ------- | ----- |
| Entrada NF — unificar fornecedores (alias PG) + ajuste manual R$ rateado | **SIM** `0149` | 3× verify (path + rateio + alias) |

**Base loja:** `producao` @ **`f4808353`** · **v27.26.18**.

**Cutover:** `./scripts/cutover_loja_checklist_2110b_nf_forn_unif_ajuste.sh`  
**Checkpoint:** `./scripts/prep_checkpoint_checklist_2110b_nf_forn_unif_ajuste.sh`  
**Pós-deploy:** `./scripts/post_deploy_checklist_2110b_nf_forn_unif_ajuste.sh` (**migrate 0149**)  
**Revert:** `git push origin rollback/pre-v272619-2110b-nf-forn-unif-ajuste:producao` → **v27.26.18** @ **`f4808353`**

**Smoke:** `/entrada-nota/` → unificar 2 fornecedores → nota 2 itens → desconto/acréscimo manual → Reverter.

**PDV / caixa:** **não alterados** (delta só Entrada NF).
