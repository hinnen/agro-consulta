# Rollback — lote checklist 2908g (loja alvo **v19.60**)

Ponto **antes** deste lote = loja **Live v19.02** (`6b1eeed`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `6b1eeed` (Live v19.02 · Meta C Vila SOLO) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-2908g-v19.02` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v1960-lote-checklist-20260829` |
| **Branch PREP** | `deploy/prep-checklist-2908g` · tip **v19.60** |
| **O quê sobe** | 15 itens do CHECKLIST ÚNICO 29/08g (ver banana) |
| **O quê NÃO sobe** | `BI-META-C-VILA-RAMP` (continua SOLO / fora deste PREP) |
| **O quê NÃO reverte** | Meta C Vila v19.02 e lotes anteriores |
| **Migrate** | **SIM** — `produtos.0105` (Chat loja). Reverter **código** não exige unmigrate; tabela de chat fica no PG sem uso. |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-2908g-v19.02
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v19.02**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| REPASSE-* (1–7) | **Não** (venda) | Só Repasse — pausar Repasse no deploy |
| `PIN-OPERADOR-QUEM` | **Baixo** | Quem/PIN em operações — Ctrl+F5 |
| `NFCE-DESC-ITENS` | **Só com desconto** | XML NFC-e com vDesc — cupom fiscal com desconto |
| `PDV-CUPOM-DINHEIRO` | **Sim (Enter)** | Dinheiro+Enter passa a imprimir cupom |
| CAD-* | **Não** | Só cadastro / validade |
| `PDV-PEDIR-ESCRITO-UX` | **Não** (finalizar) | Só Pedir loja |
| `NF-ESTOQUE-BLOQUEIO-FALSO` | **Não** | Só Entrada NF etapa 5 |
| `PDV-CHAT-LOJA` | **Não** (finalizar) | Aba Chat + migrate `0105` |

Rotina no deploy: pausar vendas ~2–3 min · Zap · Ctrl+F5 nos PDVs · smoke: venda dinheiro+Enter · NFC-e c/ desconto · Pedir escrito · Chat · Repasse · Entrada NF etapa 5.

## Deploy (próximo chat + senha)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-2908g
git push origin producao --force-with-lease
```

Render sobe · migrate `0105` · conferir healthz / badge **v19.60**.
