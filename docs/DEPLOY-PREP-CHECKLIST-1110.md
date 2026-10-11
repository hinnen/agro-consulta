# PREP — CHECKLIST 11/10 · alvo **v26.91**

**ETQ-SOLIC-MOBILE** + **VL-HUB-PIN-VENDAS-TAREFAS**

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **ETQ-SOLIC-MOBILE** | `verify_etq_solicitacao_vl_pin_path.py` **19/19** | **SIM** `0142` |
| 2 | **VL-HUB-PIN** | mesmo script + PIN vendas `/vendas/lojas/pin/` | **NÃO** |

**Live antes:** **v26.90** @ `b2a7b02d`  
**PREP:** `deploy/prep-checklist-1110`  
**Rollback:** `rollback/pre-checklist-1110-v26.90`  
**Backup:** `producao-backup-pre-v2691-checklist-1110`  
**Cutover:** `scripts/cutover_loja_checklist_1110.sh`

## Pós-deploy (obrigatório, Shell Render — logo após badge v26.91)

```bash
python manage.py migrate produtos 0142
```

## Smoke rápido (loja)

1. **Hub** (`/vendas/lojas/`): PIN **5** → **Vendas** e **Tarefas** bloqueados; **Fotos** e **Etiquetas** OK.
2. **Celular:** hub → Etiquetas → busca/bip → sacola → **Enviar solicitação**.
3. **PC:** `/produtos/etiquetas/` → **Solicitações** → importar → **Na fila**; busca manual mantém lista (bip limpa campo).

## Na senha (próximo chat)

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1110.sh --exec
```

**Reverter:** `git push origin rollback/pre-checklist-1110-v26.90:producao` (ou backup branch acima).
