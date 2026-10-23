# PREP — CHECKLIST 11/10j · **v27.00** (ETQ bulk + PWA)

| # | Pacote | Prova |
| - | ------ | ----- |
| 1 | **ETQ-SOL-IMPORT-ALL** | `verify_etq_solicitacao_vl_pin_path.py` **31/31** |
| 2 | **VL-PWA-INSTALL** | `verify_vendas_lojas_pwa_install.py` **19/19** |

**Live antes:** **v26.99** @ `fd1cd028`  
**PREP:** `deploy/prep-checklist-1110j` (= `producao` + cherry `3ca32946` · `2ee06b31` · `78f0fb87`)  
**Migrate:** **NÃO**  
**Rollback:** tag `rollback/pre-checklist-1110j-v26.99` @ `fd1cd028`  
**Cutover:** `scripts/cutover_loja_checklist_1110j.sh`

**Não inclui** merge de `teste` inteiro — só o delta acima (PDV/caixa/financeiro intocados).

## Pós-deploy (Shell Render — rápido)

```bash
./scripts/post_deploy_checklist_1110j.sh
```

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1110j.sh --exec
```

**Você na loja:** Ctrl+F5 nos PDVs (opcional; etiquetas desktop usa `produtos_etiquetas.js?v=35`).
