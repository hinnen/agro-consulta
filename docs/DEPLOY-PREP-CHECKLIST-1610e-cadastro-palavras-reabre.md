# PREP — **CADASTRO-PALAVRAS-REABRE** · **16/10e** · **v27.24**

**Migrate:** não.

**Base loja:** v27.23 @ `37ad1354`.

**PREP:** `deploy/prep-checklist-1610e-cadastro-palavras-reabre`.

**Prova:** `./.venv/bin/python scripts/verify_cadastro_busca_med_path.py` → **PREP_FAILS=0** (42/42).

**Checkpoint (antes do cutover):** `./scripts/prep_checkpoint_checklist_1610e_cadastro_palavras_reabre.sh` → `PREP_CHECKPOINT_OK` · rollback `rollback/pre-v2723-cadastro-palavras-reabre` @ produção atual · backup `producao-backup-pre-v2724-cadastro-palavras-reabre`.

**Revert rápido:** `git push origin rollback/pre-v2723-cadastro-palavras-reabre:producao` (volta **v27.23**).

**Cutover:** `./scripts/cutover_loja_checklist_1610e_cadastro_palavras_reabre.sh` · `--exec` + frase + senha.

**Smoke:** Ctrl+F5 · Cadastro ERP · produto **não medicamento** → aba **2. Busca** → palavras-chave → **Salvar** → fechar modal → **reabrir** (texto permanece).
