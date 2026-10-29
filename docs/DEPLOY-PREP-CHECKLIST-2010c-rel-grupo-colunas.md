# PREP 20/10c — REL-GRUPO-COLUNAS · v27.26.16

**Pacote:** Vendas por grupo — escolher colunas (venda / margem R$ / %) + ordenar pelo cabeçalho.

**Migrate:** não.

**Prova:** `AGRO_PIN_TESTE=9973 python scripts/verify_rel_grupo_colunas_path.py` · `verify_rel_grupo_margem_path.py` (17) · `verify_rel_cat_sub_excel_path.py` (30).

**Prep:** `./scripts/prep_checkpoint_checklist_2010c_rel_grupo_colunas.sh`  
**Cutover:** `./scripts/cutover_loja_checklist_2010c_rel_grupo_colunas.sh` · `--exec` + frase + senha loja.  
**Pós:** `./scripts/post_deploy_checklist_2010c_rel_grupo_colunas.sh`
