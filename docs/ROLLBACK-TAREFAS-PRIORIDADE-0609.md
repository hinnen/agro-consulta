# Rollback — TAREFAS-PRIORIDADE (loja alvo **v23.20**)

Ponto **antes** deste pacote = loja **Live v23.19** (`a3a22ff` · código útil `15c67a5`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `a3a22ff` (Live v23.19) |
| **Tag** | `rollback/pre-tarefas-prioridade-v23.19` |
| **Branch backup** | `producao-backup-pre-v2320-tarefas-prio-20260906` |
| **Branch PREP** | `deploy/prep-tarefas-prioridade-0609` · tip **v23.20** |
| **O quê sobe** | `TAREFAS-PRIORIDADE` — alta/média/baixa · badge · criar/editar · ordem na lista |
| **O quê NÃO sobe** | merge do `teste` · Zap / Excel / resto |
| **Migrate** | **SIM** `tarefas.0004` (AddField `prioridade` default `media`) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-tarefas-prioridade-v23.19
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.19**.

**Nota migrate:** o campo `prioridade` fica no Postgres; código antigo ignora. Se precisar limpar o campo depois, é opcional (não bloqueia PDV).

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `TAREFAS-PRIORIDADE` | **Não** | Só `/vendas/lojas/tarefas/` |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.20**
2. App Vendas → **Tarefas** → PIN → ver badge de prioridade · nova tarefa · alterar no detalhe
