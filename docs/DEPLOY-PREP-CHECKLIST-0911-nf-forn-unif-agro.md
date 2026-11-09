# PREP — **09/11** · **v27.26.54** · NF-FORN-UNIF-AGRO

| Pacote | Migrate | Prova |
| ------ | ------- | ----- |
| Entrada NF — unificar fornecedor lista Agro + título sem Id ERP | **NÃO** (reusa `0149`) | path + alias + caso RBS |

**Base loja:** `producao` @ **`af7af8cc`** · **v27.26.53**.

**Cutover:** `./scripts/cutover_loja_checklist_0911_nf_forn_unif_agro.sh`  
**Checkpoint:** `./scripts/prep_checkpoint_checklist_0911_nf_forn_unif_agro.sh`  
**Pós-deploy:** `./scripts/post_deploy_checklist_0911_nf_forn_unif_agro.sh`  
**Revert:** `git push origin rollback/pre-v272654-0911-nf-forn-unif-agro:producao` → **v27.26.53** @ **`af7af8cc`**

**Smoke (opcional):** `/entrada-nota/` → Unificar → buscar **Rbs** / **Sn - Rbs** → ver ERP + Agro + título → Unificar. CP antigos intactos.

**PDV / caixa:** **não alterados** (delta só Entrada NF unificar).
