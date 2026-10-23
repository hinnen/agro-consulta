# PREP — **CADASTRO-PALAVRAS-PDV** · **16/10f** · **v27.25**

**Migrate:** não.

**Base loja:** v27.24 @ `d0df4700`.

**PREP:** `deploy/prep-checklist-1610f-cadastro-palavras-pdv`.

**Prova:** `verify_cadastro_busca_med_path.py` → **PREP_FAILS=0** (48/48) · `tests_cadastro_busca_pdv_alinhada` **5/5**.

**Checkpoint:** `./scripts/prep_checkpoint_checklist_1610f_cadastro_palavras_pdv.sh` → rollback `rollback/pre-v2724-cadastro-palavras-pdv` · backup `producao-backup-pre-v2725-cadastro-palavras-pdv`.

**Revert:** `git push origin rollback/pre-v2724-cadastro-palavras-pdv:producao` → **v27.24**.

**Cutover:** `./scripts/cutover_loja_checklist_1610f_cadastro_palavras_pdv.sh` · `--exec` + frase + senha.

**Smoke:** Cadastro → **2. Busca** `oleo, 2 tempo, 2tempo` → salvar → PDV buscar **2 tempo** (F5 PDV se cache antigo).
