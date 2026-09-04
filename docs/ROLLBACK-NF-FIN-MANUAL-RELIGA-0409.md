# Rollback — religa CP nota manual (`NF-FIN-MANUAL-RELIGA` · loja **v21.88**)

Ponto **antes** deste hotfix = loja **Live v21.87** (`55e9b6b`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `55e9b6b` (Live v21.87) |
| **Tag** | `rollback/pre-nf-fin-manual-religa-v21.87` |
| **Branch backup** | `producao-backup-pre-v2188-nf-fin-manual-20260904` |
| **O quê sobe** | casamento financeiro da Entrada NF (nota digitada) + observação com rascunho |
| **O quê NÃO sobe** | resto do `teste` (WhatsApp UI extra, PDV extra, etc.) |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-nf-fin-manual-religa-v21.87
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v21.87**.

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `NF-FIN-MANUAL-RELIGA` | **Não** | Só Entrada NF etapa financeiro. PDV/caixa iguais. |

Rotina: **Ctrl+F5** · Entrada NF · abrir nota já no CP · laranja some.

## Deploy

**Não** resetar `producao` para o branch `teste`.
