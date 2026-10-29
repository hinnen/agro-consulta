# PREP — **CHECKLIST 18/10** · **v27.26.3**

Pacotes no mesmo cutover:

1. **NFE-SAIDA-55-PG** — rascunhos NF-e no Postgres (`0146`) sem Mongo ERP.
2. **CADASTRO-PALAVRAS-PDV-2** — busca PDV/BCA por apelido no overlay (índice + fallback SQL).

**Migrate:** **SIM** — `produtos.0146_nfe_saida_rascunho_agro` (só NF-e PG).

**Base loja:** v27.26 @ `f82f5be1`.

**PREP:** `deploy/prep-checklist-1810-nfe-pg-palavras-pdv2`.

**Prova:** `verify_nfe_saida_path.py` **41/41** · `verify_cadastro_busca_med_path.py` **50/50** · tests **6+6**.

**Checkpoint:** `./scripts/prep_checkpoint_checklist_1810_nfe_pg_palavras_pdv2.sh`.

**Revert:** `git push origin rollback/pre-v2726-1810-nfe-pg-palavras:producao` → **v27.26** @ `f82f5be1`.

**Cutover:** `./scripts/cutover_loja_checklist_1810_nfe_pg_palavras_pdv2.sh` · `--exec` + frase + senha.

**Pós-deploy (Render Shell):** `./scripts/post_deploy_checklist_1810_nfe_pg_palavras_pdv2.sh`

**Smoke:** `/nfe/saida/` **Nova** sem «legado indisponível» · PDV busca **2 tempo** (Ctrl+F5 uma vez).
