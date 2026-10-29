# PREP — **19/10a** · **v27.26.12**

| Pacote | Migrate | Prova |
| ------ | ------- | ----- |
| **NFE-DANFE-ERP-LAYOUT** | **NÃO** | `verify_nfe_saida_path.py` |
| **NFE-EMAIL-DEST** (salvar e-mail + enviar XML/DANFE) | **NÃO** | verify NF-e **68/68** + `NfeSaidaEmailTests` |
| **REL-CAT-SUB-EXCEL** (Central cat/sub + Excel) | **NÃO** | `verify_rel_cat_sub_excel_path.py` + `tests_relatorios_central_filtros` |

**Base loja:** `producao` @ **`05a871df`** · **v27.26.10**.

**Cutover:** `./scripts/cutover_loja_checklist_1910a_nfe_danfe_email_rel.sh`  
**Checkpoint:** `./scripts/prep_checkpoint_checklist_1910a_nfe_danfe_email_rel.sh`  
**Pós-deploy:** `./scripts/post_deploy_checklist_1910a_nfe_danfe_email_rel.sh`  
**Revert:** `git push origin rollback/pre-v272612-1910a-checklist:producao` → **v27.26.10** @ **`05a871df`**

**Ops (não bloqueia cutover):** **FL-059** — Gmail no Render (`EMAIL_*`) para envio real de e-mail NF-e.
