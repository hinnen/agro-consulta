# Rollback — lote checklist 0109c (loja alvo **v20.58**)

Ponto **antes** deste lote = loja **Live v20.56** (`d30c5ca`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `d30c5ca` (Live v20.56) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-0109c-v20.56` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v2058-lote-checklist-20260901` |
| **Branch PREP** | `deploy/prep-checklist-0109c` · tip **v20.58** |
| **O quê sobe** | só `BI-DEVOL-PLANILHA` |
| **O quê NÃO sobe** | `WA-ATEND-QR` · `WA-FIADO-MSG` · `BI-META-C-VILA-RAMP` · resto do `teste` |
| **O quê NÃO reverte** | lote v20.56 (F3 · BI-DEVOL-DIA · via dinheiro) e anteriores |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-0109c-v20.56
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v20.56**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| `BI-DEVOL-PLANILHA` | **Não** | Só número do BI `/` (planilha deixa de esconder devolução). Carrinho, caixa, NFC-e, entrega **iguais**. |

Rotina no deploy: pausar vendas ~1–2 min · Ctrl+F5 · smoke: abrir BI hoje · PDV F7 uma venda.

## Deploy (próximo chat + senha)

**Não** resetar `producao` para o branch `teste` (lá tem WhatsApp QR e outras coisas **fora** deste lote).

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0109c
git push origin producao --force-with-lease
```

Render sobe · conferir healthz / badge **v20.58**.
