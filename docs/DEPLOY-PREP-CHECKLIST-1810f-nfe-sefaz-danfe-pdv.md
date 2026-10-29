# PREP — **18/10f** · **v27.26.9**

| Pacote | Migrate | Prova |
| ------ | ------- | ----- |
| **NFE-55-SEFAZ+DANFE-AGRO** (emissão produção SEFAZ-SP + DANFE logo/layout) | **SIM `0147`** | `verify_nfe_saida_path.py` **59/59** |
| **CADASTRO-PALAVRAS-PDV-3** (busca mista / 2tempo) | **NÃO** | `verify_cadastro_busca_med_path.py` **56/56** |

**Base loja:** `producao` @ **`5e16d701`** · **v27.26.5**.

**Cutover dry-run:** `./scripts/cutover_loja_checklist_1810f_nfe_sefaz_danfe_pdv.sh`  
**Checkpoint:** `./scripts/prep_checkpoint_checklist_1810f_nfe_sefaz_danfe_pdv.sh`  
**Pós-deploy Shell:** `./scripts/post_deploy_checklist_1810f_nfe_sefaz_danfe_pdv.sh`  
**Revert:** `git push origin rollback/pre-v27265-1810f-nfe-sefaz:producao` → **v27.26.5** @ **`5e16d701`**

**Render:** vars NF-e produção (série **2**, `TP_AMB=1`) já no painel — ver `docs/NFE-EM-PRODUCAO-PASSO-A-PASSO.md`.

**Smoke:** DANFE com logo · 1ª NF série 2 · PDV `oleo 2tempo`.
