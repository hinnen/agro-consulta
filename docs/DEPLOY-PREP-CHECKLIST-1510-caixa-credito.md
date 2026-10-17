# PREP — CHECKLIST **15/10** + **15/10b** · **v27.19**

**Live antes:** **v27.18** @ `9f18d10f` (RH-VALE-VILA já Live)

| Item | Pacote | Migrate |
| ---- | ------ | ------- |
| 15/10 | **CAIXA-FECHAR-PERF** | **NÃO** |
| 15/10b | **CREDITO-DESBLOQUEIO-LAB** | **NÃO** |

**Fora:** **15/10a** CLIENTE % PDV (`0145`) — cutover separado.

**Rollback:** `rollback/pre-v2718-caixa-credito` @ `9f18d10f`

```bash
./scripts/cutover_loja_checklist_1510_caixa_credito.sh
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1510_caixa_credito.sh --exec
```
