# PREP — CHECKLIST **14/10d** itens **1+2** · **v27.17**

**Live antes:** **v27.16** @ `78a867a0` (ETQ-MOBILE-BIP já Live)

| Item | Pacote | Migrate |
| ---- | ------ | ------- |
| 1 | **PLANILHA-MED-ID-HOTFIX** | **NÃO** |
| 2 | **ETQ-FILA-MODAL-QTD** (só PC) | **NÃO** |

**Rollback:** `rollback/pre-1410d-v2716` @ `78a867a0`

```bash
./scripts/cutover_loja_checklist_1410d_planilha_etq_pc.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1410d_planilha_etq_pc.sh --exec
```

**Pós-deploy:** reimport planilha Med · export Med · Etiquetas PC (modal + qtd fila)
