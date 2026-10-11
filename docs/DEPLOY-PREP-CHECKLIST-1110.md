# PREP — CHECKLIST 11/10 · **v26.97** (hotfix gerência + pacote)

| # | Pacote | Prova |
| - | ------ | ----- |
| 1 | **ETQ-SOLIC-MOBILE** | `verify_etq_solicitacao_vl_pin_path.py` **28/28** |
| 2 | **VL-HUB-PIN** | runtime **19/19** + `verify_vl_hub_gerencia_allowlist.py` |
| 3 | **GM-LOJAS-LOG** | `/gestao/logs/` |

**Live antes do hotfix:** **v26.94** @ `da745c08`  
**PREP:** `deploy/prep-checklist-1110` (= `producao` + hotfixes `v26.95`–`v26.97`)  
**Rollback hotfix:** `rollback/pre-checklist-1110-v26.94`  
**Cutover:** `scripts/cutover_loja_checklist_1110.sh`

Gerência Vendas/Tarefas (qualquer PIN pessoal): **Geraldinho, Geraldo Hinnen, Renan Hinnen, Isabela Cugler, Zuleide Hinnen** + códigos **#1,2,3,4,8**.

## Pós-deploy (Shell Render — rápido)

```bash
./scripts/post_deploy_checklist_1110.sh
```

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1110.sh --exec
```
