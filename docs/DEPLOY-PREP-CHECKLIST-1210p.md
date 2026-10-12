# PREP — CHECKLIST **12/10p** · **v27.10**

| # | Pacote | Migrate | Prova |
| - | ------ | ------- | ----- |
| 1 | **CADASTRO-AGRO-VN-FANTASMA** | **NÃO** | `verify_cadastro_agro_vn_path.py` **19/19** |
| 2 | **MED-CLASSIFICACAO-SISVALE** | **NÃO** | `verify_classificacao_medicamentos_path.py` **PREP_FAILS=0** |
| 3 | **CLIENTE-LISTA-DESC-PDV** | **SIM** `0145` | `verify_cliente_lista_desc_pdv_path.py` **18/18** |

**Live antes:** **v27.09** @ `f6a4fa31`  
**PREP:** `deploy/prep-checklist-1210p`  
**Rollback:** tag `rollback/pre-checklist-1210p-v27.09-f6a4` @ `f6a4fa31`  
**Cutover:** `./scripts/cutover_loja_checklist_1210p.sh`

**Não inclui** merge de `teste` inteiro. **PDV:** só `pdv_state.js` (% desconto no subtotal) · sem wizard/busca 230/caixa.

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1210p.sh --exec
```

```bash
./scripts/post_deploy_checklist_1210p.sh
```

Render Shell (uma vez, após deploy): `python manage.py limpar_overlays_agro_vn_fantasma`
