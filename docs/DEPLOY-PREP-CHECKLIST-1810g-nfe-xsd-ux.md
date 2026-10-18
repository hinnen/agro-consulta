# PREP — **18/10g** · **v27.26.10**

| Pacote | Migrate | Prova |
| ------ | ------- | ----- |
| **NFE-55-XSD-DEST** (ordem dest, CEP, país, `indIntermed`) | **NÃO** | `verify_nfe_saida_path.py` |
| **NFE-MANUAL-UX** (typeahead cliente/produto/venda) | **NÃO** | verify + HTTP smoke |

**Base loja:** `producao` @ **`18046efd`** · **v27.26.9**.

**Cutover dry-run:** `./scripts/cutover_loja_checklist_1810g_nfe_xsd_ux.sh`  
**Checkpoint:** `./scripts/prep_checkpoint_checklist_1810g_nfe_xsd_ux.sh`  
**Pós-deploy Shell:** `./scripts/post_deploy_checklist_1810g_nfe_xsd_ux.sh`  
**Revert:** `git push origin rollback/pre-v272610-1810g-nfe-xsd:producao` → **v27.26.9** @ **`18046efd`**

**Smoke:** `/nfe/saida/` busca com lista · emitir NF série **2** em produção (schema SEFAZ).
