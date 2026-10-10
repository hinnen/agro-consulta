# PREP — CHECKLIST 10/10h · alvo **v26.86**

**Status:** 🟡 **armado** — Live **v26.85** (migração legado por intruso único).

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CB-230-LEGADO-GRUPO** | legado **13/13** · bip **10/10** · Django **35/35** | **NÃO** |

**O quê:** `--liberar-intruso` passa a tratar **grupos** (vários cadastros legados no mesmo EAN bipável, ex. 1550–1559 → 1556): reatribui 230 novo nos demais, migra o vencedor (preferência código **GM**).

**Pós-deploy (Shell, uma vez):**

```bash
python manage.py migrar_cb_loja_legado --liberar-intruso --dry-run
python manage.py migrar_cb_loja_legado --liberar-intruso
```

Esperado: `Corrigidos` ≈ número de **grupos EAN** (não zero) e `Colisões` bem menor que 93.

| Campo | Valor |
| ----- | ----- |
| **Tip PREP** | `deploy/prep-checklist-1010h` |
| **Rollback tag** | `rollback/pre-checklist-1010h-v26.85` |
| **Backup branch** | `producao-backup-pre-v2686-checklist-1010h` |
| **Cutover** | `scripts/cutover_loja_checklist_1010h.sh` |

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1010h.sh --exec
```

## Voltar

`docs/ROLLBACK-CHECKLIST-1010h.md`
