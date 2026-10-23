# PREP — **21/10c** · **v27.26.20** · NF-AJUSTE-CUSTO-MANUAL (hotfix)

| Pacote | Migrate | Prova |
| ------ | ------- | ----- |
| Entrada NF etapa 2 — rateio manual por **Qtd est.**; **Reverter**; rascunho com `v_prod` / `v_un_com_base_nf` | **NÃO** | `verify_nf_ajuste_manual_rateio_path` + `verify_nf_forn_unif_ajuste_path` |

**Base loja:** `producao` @ **`3c2b5117`** · **v27.26.19** (21/10b já Live).

**Cutover:** `./scripts/cutover_loja_checklist_2110c_nf_ajuste_manual_hotfix.sh`  
**Checkpoint:** `./scripts/prep_checkpoint_checklist_2110c_nf_ajuste_manual_hotfix.sh`  
**Pós-deploy:** `./scripts/post_deploy_checklist_2110c_nf_ajuste_manual_hotfix.sh` (**sem migrate**)  
**Revert:** `git push origin rollback/pre-v272620-2110c-nf-ajuste-manual-hotfix:producao` → **v27.26.19** @ **`3c2b5117`**

**Smoke:** Ctrl+F5 → `/entrada-nota/` → nota com item **Un/emb > 1** ou qtd est. ≠ qtd NF → desconto R$ (ex. 42,50) → V. unit **não** = total → **Reverter** → custo volta.

**PDV / caixa:** **não alterados**.
