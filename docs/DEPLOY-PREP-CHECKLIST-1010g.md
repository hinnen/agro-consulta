# PREP — CHECKLIST 10/10g · alvo **v26.85**

**Status:** 🟡 **armado** — Live **v26.84** @ `35308cbf`.

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CB-230-LEGADO-LIBERAR** | legado **12/12** · bip **10/10** · Django **35/35** | **NÃO** |

**O quê:** `migrar_cb_loja_legado --liberar-intruso` — nas 93 colisões, gera 230 novo no intruso e promove legado → EAN bipável.

**Pós-deploy (Shell, uma vez):**

```bash
python manage.py migrar_cb_loja_legado --liberar-intruso --dry-run
python manage.py migrar_cb_loja_legado --liberar-intruso
```

| Campo | Valor |
| ----- | ----- |
| **Tip PREP** | `deploy/prep-checklist-1010g` |
| **Rollback tag** | `rollback/pre-checklist-1010g-v26.84` → `35308cbf` |
| **Backup branch** | `producao-backup-pre-v2685-checklist-1010g` |
| **Cutover** | `scripts/cutover_loja_checklist_1010g.sh` |

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1010g.sh --exec
```

## Voltar

`docs/ROLLBACK-CHECKLIST-1010g.md`
