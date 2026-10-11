# PREP — CHECKLIST 11/10 · alvo **v26.94**

**ETQ-SOLIC-MOBILE** + **VL-HUB-PIN** + **GM-LOJAS-LOG** + PIN único hub (clique Vendas/Tarefas)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **ETQ-SOLIC-MOBILE** | `verify_etq_solicitacao_vl_pin_path.py` **28/28** | **SIM** `0142` |
| 2 | **VL-HUB-PIN** | path + `verify_gm_lojas_pin_runtime.py` | **NÃO** |
| 3 | **GM-LOJAS-LOG** | path + `/gestao/logs/` | **SIM** `0143` |

**Live antes:** **v26.90** @ `b2a7b02d`  
**PREP:** `deploy/prep-checklist-1110` (atualizar cherry-picks até `teste` @ v26.94 antes do cutover)  
**Rollback:** `rollback/pre-checklist-1110-v26.90`  
**Backup:** `producao-backup-pre-v2691-checklist-1110`  
**Cutover:** `scripts/cutover_loja_checklist_1110.sh`

## Pós-deploy (obrigatório, Shell Render)

```bash
python manage.py migrate produtos 0143
```

**PIN:** uma vez em `/vendas/lojas/pin/` · **7 dias** · hub com 4 cartões; Vendas/Tarefas só allowlist **1,2,3,4,8** ao clicar · log `GmLojasAppLogAgro` · **Gestão → LOG**.

## Smoke rápido (loja)

1. **Hub:** PIN válido → 4 botões; PIN **5** → Vendas/Tarefas = «sem acesso»; Fotos/Etiquetas OK.
2. **Celular:** Etiquetas → sacola → Enviar.
3. **PC:** `/produtos/etiquetas/` → Solicitações → Na fila.

## Na senha (próximo chat)

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1110.sh --exec
```

**Reverter:** `git push origin rollback/pre-checklist-1110-v26.90:producao`
