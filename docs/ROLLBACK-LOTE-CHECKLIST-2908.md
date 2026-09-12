# Rollback — lote checklist 2908 (loja alvo **v18.83**)

Ponto **antes** deste lote = loja **Live v18.72** (`ae126d9`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `ae126d9` (Live v18.72) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-2908-v18.72` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v1883-lote-checklist-20260829` |
| **Branch PREP** | `deploy/prep-checklist-2908` · tip **v18.83** |
| **O quê sobe** | 8 itens do CHECKLIST ÚNICO 29/08 (ver banana) |
| **O quê NÃO reverte** | lote v18.72 (NS-ESCOLHA-EMP · overlay popup · CP-EMP-PG-FALLBACK) e anteriores |
| **Migrate** | **SIM** — `produtos.0103` (dois cofres). Reverter **código** não exige unmigrate; campos novos ficam no PG sem uso. |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-2908-v18.72
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v18.72**. Badge da loja volta a **v18.72**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| `REPASSE-DOIS-COFRES` | **Não** (venda) | **Sim** no Repasse — fórmula dos 2 cofres. Pausar **Repasse** no deploy. Migrate `0103` no build. |
| `REPASSE-FORCAR-MANUAL` | **Não** | Só overlay se digitar manual com dia zerado |
| `REPASSE-CAIXA-DIN` | **Não** | Só mostra saldo da gaveta |
| `REPASSE-HERO-TOTAIS` | **Não** | Só totais no card |
| `REPASSE-COFRE-CONFIRM` | **Não** | Modal no lugar do confirm do Chrome |
| `CP-EMP-ROW-TINT` | **Não** | Só cor na lista CP |
| `NE-SUCESSO-OK` | **Não** | Só modal empréstimo |
| `NS-ESCOLHA-MOLDURA` | **Não** | Só cards Nova saída |

Rotina no deploy: pausar vendas/finalizações ~2–3 min · Zap · Ctrl+F5 nos PDVs após Live · smoke: venda · Repasse · Nova saída.

## Deploy (próximo chat + senha)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-2908
git push origin producao --force-with-lease
```

Render sobe · migrate `0103` · conferir healthz / badge **v18.83**.
