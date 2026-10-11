# Rollback — CHECKLIST 11/10 · v26.94

| Item | Valor |
| ---- | ----- |
| **Live antes** | **v26.90** @ `b2a7b02d` |
| **Tag rollback** | `rollback/pre-checklist-1110-v26.90` |
| **Backup branch** (criada no cutover) | `producao-backup-pre-v2691-checklist-1110` |
| **PREP** | `deploy/prep-checklist-1110` → `producao` |
| **Migrate** | `0142` etiquetas · `0143` log GM Lojas |

**Merge `teste` inteiro?** **NÃO** — só o PREP (cherry-picks do pacote).

## Reverter produção (emergência)

```bash
git push origin rollback/pre-checklist-1110-v26.90:producao
# ou
git push origin producao-backup-pre-v2691-checklist-1110:producao
```

Migrações **não** revertem sozinhas; tabelas novas (`EtiquetaSolicitacaoPacoteAgro`, `GmLojasAppLogAgro`) podem ficar vazias — aceitável para rollback de código.

## O que o pacote muda na loja

- PWA GM Lojas: PIN único 7d; Vendas/Tarefas só PIN 1,2,3,4,8 **ao clicar**
- Etiquetas: solicitação celular + fila desktop
- Gestão → LOG (`/gestao/logs/`)
