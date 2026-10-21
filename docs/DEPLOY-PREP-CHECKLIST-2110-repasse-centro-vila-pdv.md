# PREP — **21/10** · **v27.26.18** · REPASSE-CENTRO-VILA-PDV

| Pacote | Migrate | Prova |
| ------ | ------- | ----- |
| PDV Repasse **Centro → Vila** (raro; V→C inalterado) | **SIM** `0148_repasse_centro_vila` | `verify_repasse_centro_vila_path` · regressão `verify_repasse_vila_path` · `verify_repasse_pdv_overlay_path` |

**Base loja:** `producao` @ **`bb7a01a3`** · **v27.26.17**.

**Cutover:** `./scripts/cutover_loja_checklist_2110_repasse_centro_vila_pdv.sh`  
**Checkpoint:** `./scripts/prep_checkpoint_checklist_2110_repasse_centro_vila_pdv.sh`  
**Pós-deploy:** `./scripts/post_deploy_checklist_2110_repasse_centro_vila_pdv.sh` (**migrate 0148** no Render Shell)  
**Revert:** `git push origin rollback/pre-v272618-2110-repasse-centro-vila-pdv:producao` → **v27.26.17** @ **`bb7a01a3`**

**Smoke:** Ctrl+F5 → PDV → Repasse → **Centro → Vila** · valor · PIN · conferir saída Gaveta Centro + entrada/pendente Vila.
