# PREP — CHECKLIST 10/10i · alvo **v26.87**

**Status:** 🟢 **armado** — Live **v26.86** @ `7ca04de5`.

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CB-230-LEGADO-POS-GRUPO** | legado **14/14** · bip **10/10** · Django **39/39** | **NÃO** |

**O quê:** após reatribuir o grupo, validação só no EAN bipável literal + limpa opcionais legados que ainda bloqueavam (12 colisões).

**Pós-deploy (Shell, uma vez):**

```bash
python manage.py migrar_cb_loja_legado --liberar-intruso
```

| Campo | Valor |
| ----- | ----- |
| **Tip PREP** | `deploy/prep-checklist-1010i` |
| **Rollback tag** | `rollback/pre-checklist-1010i-v26.86` |
| **Cutover** | `scripts/cutover_loja_checklist_1010i.sh` |
