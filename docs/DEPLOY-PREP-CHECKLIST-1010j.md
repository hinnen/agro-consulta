# PREP — CHECKLIST 10/10j · alvo **v26.88**

**Status:** 🟢 **armado / gates OK** — Live **v26.86** @ `7ca04de5` · PREP `deploy/prep-checklist-1010j`.

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CB-230-LEGADO-LITERAL-BIP** | legado **15/15** · bip **10/10** · Django **40/40** | **NÃO** |

**O quê:** na migração por grupo, **reatribui quem já tem o EAN da etiqueta** (6 colisões finais), limpa interno/opcionais; Mongo só `CodigoBarras` principal.

**Pós-deploy (Shell, uma vez):**

```bash
python manage.py migrar_cb_loja_legado --liberar-intruso
```

| Campo | Valor |
| ----- | ----- |
| **Tip PREP** | `deploy/prep-checklist-1010j` |
| **Rollback tag** | `rollback/pre-checklist-1010j-v26.86` → `7ca04de5` |
| **Backup branch** | `producao-backup-pre-v2688-checklist-1010j` |
| **Cutover** | `scripts/cutover_loja_checklist_1010j.sh` |

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1010j.sh --exec
```
