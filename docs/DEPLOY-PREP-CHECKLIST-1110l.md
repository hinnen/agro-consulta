# PREP — CHECKLIST 11/10l · **v27.04**

| # | Pacote | Migrate |
| - | ------ | ------- |
| 1 | **META-CENTRO-VILA-UNIFICADO** — soma + config `deposito=todas` (50/50 ao salvar) | **NÃO** |
| 2 | **VL-RITMO-HORA** — média Meta C «até agora» por histórico do dia | **NÃO** |

**Live antes:** **v27.03** @ `db8cdf6f`  
**Branch:** `teste` @ tip com VERSION **27.04**  
**Cutover:** `./scripts/cutover_loja_checklist_1110l.sh`  
**Rollback:** tag `rollback/pre-checklist-1110l-v27.03` @ `db8cdf6f`

**Prova:** `verify_meta_mostruario_path` core · `manage.py test produtos.tests_vendas_lojas_ritmo`
