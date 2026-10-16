# PREP — **ETQ-NF-QTD-FILA** · **16/10d** · **v27.23**

**Migrate:** não.

**Base loja:** v27.22 @ `553e8a4c` (ou `aecf3a9a` código NF).

**PREP:** `deploy/prep-checklist-1610d-etq-nf-qtd`.

**Prova:** `./.venv/bin/python scripts/verify_etq_filtro_nf_entrada_path.py` → **VERIFY_OK 27/27**.

**Cutover:** `./scripts/cutover_loja_checklist_1610d_etq_nf_qtd.sh` · `--exec` + frase + senha.

**Smoke:** Ctrl+F5 · Etiquetas → NF → **1 etiqueta por produto** ou **Qtd da nota** → **Adicionar todos**.
