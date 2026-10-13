# PREP — **CADASTRO-BUSCA-GM-PDV** · CHECKLIST **13/10** · **v27.14**

**Live antes:** **v27.13** @ `904a6031`

| Campo | Valor |
| ----- | ----- |
| **O quê** | GM com hífen exato · filtro/relevância cadastro = PDV · API cadastro não reordena A–Z sem `sort` |
| **Migrate** | **NÃO** |
| **PREP** | `deploy/prep-checklist-1310-busca-gm-pdv` |
| **Rollback** | `rollback/pre-busca-gm-pdv-v2713` @ `904a6031` |

**Toca busca PDV** (`busca_filtro_pdv_util`, `motor_busca_unificado`) — alinhamento intencional; cutover exige `verify_pdv_busca_coerente_path` **30/30** + CB-230.

```bash
./scripts/cutover_loja_checklist_1310_busca_gm_pdv.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1310_busca_gm_pdv.sh --exec
./scripts/post_deploy_checklist_1310_busca_gm_pdv.sh
```
