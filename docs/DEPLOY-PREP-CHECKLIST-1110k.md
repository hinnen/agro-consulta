# PREP — CHECKLIST 11/10k · **v27.03**

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **SISTAPP-PWA-AJUSTE** | `verify_vendas_lojas_pwa_install.py` **25/25** | **NÃO** |
| 2 | **META-POR-LOJA** | `verify_meta_mostruario_path.py` core (arquivos + CRUD + Zap) | **SIM** `0144` |
| 3 | **META-ZAP-ENXUTO** | (mesmo script · linhas `* R$ … bônus … ✅`) | **NÃO** |

**Live antes:** **v27.00** @ `14f5cad7`  
**PREP:** `deploy/prep-checklist-1110k`  
**Rollback:** tag `rollback/pre-checklist-1110k-v27.00` @ `14f5cad7`  
**Cutover:** `scripts/cutover_loja_checklist_1110k.sh`

**Não inclui** merge de `teste` inteiro — delta só GM Lojas / Ajuste / META. **PDV, caixa e busca 230 intocados.**

## Pós-deploy (Shell Render)

```bash
./scripts/post_deploy_checklist_1110k.sh
```

## Na senha (lojas pausadas ~2–3 min)

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1110k.sh --exec
```

Depois: conferir badge **v27.03** · `/meta/` (escolher Só Centro/Vila para cadastrar) · hub **Ajuste** verde · manifest **SistApp**.
