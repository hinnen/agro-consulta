# PREP — **CADASTRO-BUSCA-GM-PDV** hotfix · CHECKLIST **13/10** · **v27.14.2**

**Live hoje:** **v27.14** @ `697ac790` (busca cadastro ainda incorreta) · base rollback **v27.13** @ `904a6031`

| Campo | Valor |
| ----- | ----- |
| **O quê** | Complemento **Mongo** no cadastro · PG filtrado · GM hífen estrito · frase com espaço · cache BCA **v3** |
| **Migrate** | **NÃO** |
| **PREP** | `deploy/prep-checklist-1310-busca-gm-pdv` (atualizar de `teste` antes do `--exec`) |
| **Rollback** | `rollback/pre-busca-gm-pdv-v2713` @ `904a6031` |

**Pós-deploy:** Ctrl+F5 Cadastro ERP · `GM0037-1` · `ibiuna 5kg`

```bash
./scripts/cutover_loja_checklist_1310_busca_gm_pdv.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1310_busca_gm_pdv.sh --exec
./scripts/post_deploy_checklist_1310_busca_gm_pdv.sh
```
