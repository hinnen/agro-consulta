# Rollback — lote checklist 2808b (loja alvo v18.64)

Ponto **antes** deste lote = loja **Live v18.50** (`4836ec1`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje** | `4836ec1` (Live v18.50) |
| **Tag (no deploy)** | `rollback/pre-lote-checklist-2808b-v18.50` |
| **Branch backup (no deploy)** | `producao-backup-pre-v1864-lote-checklist-20260828` |
| **O quê reverte** | CP busca Empresa/Credor · cofrinho acumulado + saldo inicial · PDV modo por forma · overlay repasse limpo |
| **O quê NÃO reverte** | lote v18.50 (preço digitado, cofrinho base, centavos, NF vínculo, etc.) |
| **Migrate** | `produtos.0102` — **no-op** no banco (só choice `saldo_inicial`). Reverter código **não** exige unmigrate. |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-2808b-v18.50
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v18.50**. Badge da loja volta a **v18.50**.
