# PREP — CHECKLIST 11/10m · **v27.06**

| # | Pacote | Migrate |
| - | ------ | ------- |
| 1 | **VILA-ABERTURA-0108** (default `2026-08-01`) | **NÃO** |
| 2 | **VILA-DOMINGO-ZERO** (meta R$ 0 domingo) | **NÃO** |

**Live antes:** **v27.04** @ `f590b14b` (código `dc19f805`)  
**PREP:** `deploy/prep-checklist-1110m`  
**Rollback:** tag `rollback/pre-checklist-1110m-v27.04-f590` @ `f590b14b`  
**Cutover:** `./scripts/cutover_loja_checklist_1110m.sh`

**Render:** apagar `AGRO_VILA_ABERTURA=2026-07-20` se existir — ver `docs/ROLLBACK-BI-META-C-VILA.md`.

**Delta:** só `views.py` (Meta C Vila) + partial BI ajuda + scripts de prova. **PDV/caixa intocados.**

## Antes de pausar vendas

```bash
git fetch origin producao deploy/prep-checklist-1110m
./scripts/cutover_loja_checklist_1110m.sh
```

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1110m.sh --exec
```

```bash
./scripts/post_deploy_checklist_1110m.sh
```
