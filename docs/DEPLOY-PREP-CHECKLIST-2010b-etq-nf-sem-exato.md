# PREP — **ETQ-NF-SEM-EXATO** · **20/10b** · **v27.26.15**

**Migrate:** não.

**Base loja:** v27.26.14 @ produção atual.

**PREP:** `deploy/prep-checklist-2010b-etq-nf-sem-exato`.

**Prova:** `./.venv/bin/python scripts/verify_etq_filtro_nf_entrada_path.py` → **VERIFY_OK** (incl. `SEM-1410-TJAI` ≠ `SEM-1410-UPS7`).

**Cutover:** `./scripts/cutover_loja_checklist_2010b_etq_nf_sem_exato.sh` · `--exec` + frase + senha.

**Smoke loja:** Ctrl+F5 · **Etiquetas** → **Nº NF entrada** `SEM-1410-TJAI` (código completo) → só produtos dessa nota · **Qtd da nota (NF)** na fila.
