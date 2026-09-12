# Rollback — lote checklist 2908b (loja alvo **v19.01**)

Ponto **antes** deste lote = loja **Live v18.83** (`d836982`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `d836982` (Live v18.83) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-2908b-v18.83` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v1901-lote-checklist-20260829` |
| **Branch PREP** | `deploy/prep-checklist-2908b` · tip **v19.01** |
| **O quê sobe** | 3 itens do CHECKLIST ÚNICO 29/08b (ver banana) |
| **O quê NÃO reverte** | lote v18.83 (dois cofres · forçar manual · caixa din · hero totais · …) e anteriores |
| **Migrate** | **SIM** — `produtos.0104` (tabelas % · seed **inativas**) · `estoque.0020` (`quantidade_pedida`). Reverter **código** não exige unmigrate; campos/tabelas novas ficam no PG sem uso. |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-2908b-v18.83
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v18.83**. Badge da loja volta a **v18.83**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| `REPASSE-HERO-LOTE` | **Não** (venda) | **Sim** no Repasse — só UX hero/cofres/Levar/totais. Pausar **Repasse** no deploy. |
| `TABELA-PRECO-FORMA` | **Potencial** | Toca JS do PDV/promo. Seed com tabelas **inativas** → **sem mudar preço** até alguém ativar. **Ctrl+F5** obrigatório após Live. |
| `PDV-PEDIR-CUPOM-QTD` | **Não** (finalizar venda) | Só overlay **Pedir loja** + migrate `0020`. |

Rotina no deploy: pausar vendas/finalizações ~2–3 min · Zap · Ctrl+F5 nos PDVs após Live · smoke: venda · forma pagamento · Pedir loja · Repasse.

## Deploy (próximo chat + senha)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-2908b
git push origin producao --force-with-lease
```

Render sobe · migrate `0104` + `0020` · conferir healthz / badge **v19.01**.
