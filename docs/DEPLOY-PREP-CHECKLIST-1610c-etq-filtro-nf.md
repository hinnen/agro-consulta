# PREP — **ETQ-FILTRO-NF-ENTRADA** · **16/10c** · **v27.22**

**Migrate:** não.

**Base loja:** v27.21 @ produção atual.

**PREP:** `deploy/prep-checklist-1610c-etq-filtro-nf`.

**Prova:** `./.venv/bin/python scripts/verify_etq_filtro_nf_entrada_path.py` → **VERIFY_OK**.

**Cutover:** `./scripts/cutover_loja_checklist_1610c_etq_filtro_nf.sh` · `--exec` + frase + senha.

**Smoke loja:** Ctrl+F5 · **Etiquetas** → **Mais opções** → **Nº NF entrada** (nota já com estoque aplicado) → lista produtos → fila → imprimir.
