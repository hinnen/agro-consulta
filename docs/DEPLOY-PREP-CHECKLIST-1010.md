# PREP — CHECKLIST 10/10 · alvo **v26.79**

**Status:** 🟢 pronto · aguarda frase + senha.

| Pacote | Prova | Migrate |
| ------- | ----- | ------- |
| **PDV-BIP-230-NAO-BALANCA** | bip 10/10 · balança/JS 41/41 | **NÃO** |
| **CAD-230-SO-CLIQUE** | gerador 22/22 | **NÃO** |

PREP: `deploy/prep-checklist-1010`  
Base: Live **v26.78.1** @ `5f460cb6`  
Rollback: `rollback/pre-checklist-1010-v26.78.1`  
Backup: `producao-backup-pre-v2679-checklist-1010`

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1010.sh --exec
```

## Smoke

1. Ctrl+F5 · badge **v26.79**.
2. Bip `2300000001471` → GM4045, sem R$ 1,47.
3. Produto novo: barras vazio; botão **230** preenche.
4. Balança `2001000004812` continua R$ 4,81.
