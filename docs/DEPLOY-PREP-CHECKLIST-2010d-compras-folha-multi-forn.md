# PREP — **20/10d** · **v27.26.17** · COMPRAS-FOLHA-MULTI-FORN

| Pacote | Migrate | Prova |
| ------ | ------- | ----- |
| Folha Compras por fornecedor — vários cadastros na mesma impressão | **NÃO** | `verify_compras_folha_multi_forn_path.py` · `tests_compras_relatorio_fornecedor_multi` |

**Base loja:** `producao` @ **`09c4d28f`** · **v27.26.16**.

**Cutover:** `./scripts/cutover_loja_checklist_2010d_compras_folha_multi_forn.sh`  
**Checkpoint:** `./scripts/prep_checkpoint_checklist_2010d_compras_folha_multi_forn.sh`  
**Pós-deploy:** `./scripts/post_deploy_checklist_2010d_compras_folha_multi_forn.sh`  
**Revert:** `git push origin rollback/pre-v272617-2010d-compras-folha-multi-forn:producao` → **v27.26.16** @ **`09c4d28f`**

**Smoke:** Compras → Folha Compras → Por fornecedor → 2+ cadastros → imprimir · rótulo `A + B`.
