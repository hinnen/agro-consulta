# PREP — **NFE-SAIDA-55** · **17/10** · **v27.26**

**Migrate:** não.

**Base loja:** v27.25 @ `b60a3f5b`.

**PREP:** `deploy/prep-checklist-1710-nfe-saida-55`.

**Prova:** `verify_nfe_saida_path.py` → **27/27** · `tests_nfe_saida_util` **4/4**.

**Checkpoint:** `./scripts/prep_checkpoint_checklist_1710_nfe_saida_55.sh`.

**Revert:** `git push origin rollback/pre-v2725-nfe-saida-55:producao` → **v27.25**.

**Cutover:** `./scripts/cutover_loja_checklist_1710_nfe_saida_55.sh` · `--exec` + frase + senha.

**Smoke:** Menu **NF-e saída** · **Nova** · emitir (homolog simula) · `?venda=<pk>`.

**Nota:** emissão SEFAZ produção (`tp_amb=1`) ainda pendente fase 2 SOAP — mensagem clara na UI.
