# PREP — **CAIXA-FECHAR-PERF-2** · **16/10** · **v27.21**

**Migrate:** não.

**Base loja:** v27.20 @ `2721cec2`.

**PREP:** `deploy/prep-checklist-1610-caixa-perf2`.

**Cutover:** `./scripts/cutover_loja_checklist_1610_caixa_perf2.sh` · `--exec` + frase + senha.

**Rollback:** tag `rollback/pre-v2720-caixa-perf2` → `2721cec2`.

**Smoke:** Ctrl+F5 · Caixa **Vila** → **Fechar caixa** (dois turnos) — poucos segundos.
