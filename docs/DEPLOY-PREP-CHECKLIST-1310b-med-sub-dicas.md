# PREP — **MED-SUBCATEGORIA-DICAS** · CHECKLIST **13/10b** · **v27.15**

**Live antes:** **v27.14.4** @ `dc357d31`

| Campo | Valor |
| ----- | ----- |
| **O quê** | Modal cadastro ERP: dicas ⓘ subcategorias 1–4 só **Medicamentos E Venenos** (UI only) |
| **Migrate** | **NÃO** |
| **PREP** | `deploy/prep-checklist-1310b-med-sub-dicas` |
| **Rollback** | `rollback/pre-med-sub-v27144` @ `dc357d31` |

```bash
./scripts/cutover_loja_checklist_1310b_med_sub_dicas.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1310b_med_sub_dicas.sh --exec
```
