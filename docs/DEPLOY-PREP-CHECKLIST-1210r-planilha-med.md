# PREP — **PLANILHA-MED-EXCEL** · CHECKLIST **12/10r** · **v27.13**

**Live antes:** **v27.12** @ `ac38739ed` (GESTAO-MED-MODAL-GV)

| Campo | Valor |
| ----- | ----- |
| **O quê** | Cadastro Excel ↓/↑: colunas Med palavras-chave, classes (slugs), espécies (slugs) |
| **Migrate** | **NÃO** |
| **PREP** | `deploy/prep-checklist-1210r-planilha-med` |
| **Rollback** | `rollback/pre-planilha-med-v2712` @ `ac38739ed` |

```bash
./scripts/cutover_loja_checklist_1210r_planilha_med.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1210r_planilha_med.sh --exec
./scripts/post_deploy_checklist_1210r_planilha_med.sh
```

**Smoke loja:** Ctrl+F5 cadastro ERP → Exportar Excel → colunas MED · importar linha teste com slug válido.
