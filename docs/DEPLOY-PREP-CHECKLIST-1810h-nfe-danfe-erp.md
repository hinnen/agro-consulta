# PREP — **18/10h** · **v27.26.11**

| Pacote | Migrate | Prova |
| ------ | ------- | ----- |
| **NFE-DANFE-ERP-LAYOUT** (canhoto, grade fiscal, logo Agro Mais) | **NÃO** | `verify_nfe_saida_path.py` |

**Base loja:** `producao` @ **`05a871df`** · **v27.26.10**.

**Cutover:** `./scripts/cutover_loja_checklist_1810h_nfe_danfe_erp.sh`  
**Checkpoint:** `./scripts/prep_checkpoint_checklist_1810h_nfe_danfe_erp.sh`  
**Pós-deploy:** `./scripts/post_deploy_checklist_1810h_nfe_danfe_erp.sh`  
**Revert:** `git push origin rollback/pre-v272611-1810h-danfe-erp:producao` → **v27.26.10** @ **`05a871df`**
