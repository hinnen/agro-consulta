# PREP — hotfix **GESTAO-MED-MODAL-GV** · CHECKLIST **12/10q** · **v27.12**

**Live antes:** **v27.11** @ `4f122337` (MED já Live)

| Campo | Valor |
| ----- | ----- |
| **O quê** | `gv()` no escopo do modal cadastro ERP · gestão: clique na linha + lápis com `abrirDrawerSeguro` |
| **Migrate** | **NÃO** |
| **Delta** | 5 arquivos (templates + teste + verify) |
| **PREP** | `deploy/prep-checklist-1210q-gv` |
| **Rollback** | `rollback/pre-hf-gestao-med-modal-gv-v2711` @ `4f122337` |

```bash
./scripts/cutover_loja_checklist_1210q_gv.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1210q_gv.sh --exec
./scripts/post_deploy_checklist_1210q_gv.sh
```

**Smoke loja:** Ctrl+F5 `/produtos/gestao/` → lápis ou linha abre drawer · cadastro ERP → modal medicamento sem erro `gv is not defined`.
