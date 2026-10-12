# PREP — CHECKLIST 12/10n · **v27.08**

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **ETQ-MOBILE-POLISH** | ETQ path **35/35** · mobile flow **13/13** | **NÃO** |
| 2 | **CREDITO-REVISAR-LIMITE** | `verify_credito_limite_revisao_path` **27/27** | **NÃO** |
| 3 | **MED-CLASSIFICACAO-SISVALE** | `verify_classificacao_medicamentos_path` **29/29** | **NÃO** |

**Live antes:** **v27.06** @ `b539c712`  
**PREP:** `deploy/prep-checklist-1210n` @ `168151a2`  
**Dry-run:** `./scripts/cutover_loja_checklist_1210n.sh` → linha `OK: PREP v27.08`  
**Rollback:** tag `rollback/pre-checklist-1210n-v27.06-b539` @ `b539c712`  
**Cutover:** `./scripts/cutover_loja_checklist_1210n.sh`

**Não inclui** merge de `teste` inteiro — delta mínimo (15 arquivos). **PDV checkout / caixa / busca 230 intocados.**

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1210n.sh --exec
```

```bash
./scripts/post_deploy_checklist_1210n.sh
```
