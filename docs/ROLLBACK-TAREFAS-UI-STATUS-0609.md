# Rollback — TAREFAS-UI-STATUS (loja alvo **v23.19**)

Ponto **antes** = loja **Live v23.18** (`3ba41db` · VL-HUB-TAREFAS).

| Item | Valor |
| ---- | ----- |
| **Tag** | `rollback/pre-tarefas-ui-status-v23.18` |
| **Branch backup** | `producao-backup-pre-v2319-tarefas-ui-20260906` |
| **Branch PREP** | `deploy/prep-tarefas-ui-status-0609` |
| **O quê sobe** | UI Tarefas (blocos) · status Adiado permanente + Cancelados · migrate `0003` |
| **O quê NÃO sobe** | merge `teste` · WA · Excel · PDV |
| **Migrate** | **SIM** `tarefas.0003` (AlterField leve) |
| **Risco loja aberta** | **Baixo** — path só `/vendas/lojas/tarefas/` |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-tarefas-ui-status-v23.18
git push origin producao --force-with-lease
```

**Só** com frase + senha. Volta Live **v23.18**.

## Depois do deploy

1. **Ctrl+F5** · badge **v23.19**
2. App Vendas → Tarefas → PIN → conferir blocos e novos status
3. PDV/caixa **não** precisam de teste especial deste pacote
