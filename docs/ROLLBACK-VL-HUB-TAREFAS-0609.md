# Rollback — VL-HUB-TAREFAS (loja alvo **v23.18**)

Ponto **antes** deste pacote = loja **Live v23.07** (`4c59e54` · WA-PONTE-LEVE).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `4c59e54` (Live v23.07) |
| **Tag** | `rollback/pre-vl-hub-tarefas-v23.07` |
| **Branch backup** | `producao-backup-pre-v2318-vl-hub-tarefas-20260906` |
| **Branch PREP** | `deploy/prep-vl-hub-tarefas-0609` · tip **v23.18** |
| **O quê sobe** | `VL-HUB-TAREFAS` — hub `/vendas/lojas/` (Vendas / Tarefas) · app `tarefas/` + PIN · migrate seed |
| **O quê NÃO sobe** | merge do `teste` · Excel / WA extra / resto |
| **Migrate** | **SIM** `tarefas.0001` + `0002` |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-vl-hub-tarefas-v23.07
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.07**.

## Risco operacional

| Pacote | Afeta finalizar venda? | Nota |
| ------ | ---------------------- | ---- |
| `VL-HUB-TAREFAS` | **Não** | Só app Vendas/Tarefas no celular. Migrate cria tabelas + seed das 8 pendências |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.18**
2. Abrir app **Vendas** → 2 botões → **Tarefas** → PIN
3. Conferir lista seed (Equipe, Delivery, Billy Dog…)
