# Rollback — lote checklist 0109 (loja alvo **v20.56**)

Ponto **antes** deste lote = loja **Live v20.49** (`31941b8`).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `31941b8` (Live v20.49) |
| **Tag (criada no PREP)** | `rollback/pre-lote-checklist-0109-v20.49` |
| **Branch backup (criada no PREP)** | `producao-backup-pre-v2056-lote-checklist-20260901` |
| **Branch PREP** | `deploy/prep-checklist-0109` · tip **v20.56** |
| **O quê sobe** | `PDV-ENTREGA-F3` · `BI-DEVOL-DIA` · `ENT-VIA-DIN-SEM-MAQ` |
| **O quê NÃO sobe** | `WA-ATEND-QR` · `WA-FIADO-MSG` · `BI-META-C-VILA-RAMP` |
| **O quê NÃO reverte** | lote v20.49 (Zap cor + arredondar cofre) e anteriores |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-lote-checklist-0109-v20.49
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v20.49**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda PDV? | Nota |
| ------ | ---------------- | ---- |
| `PDV-ENTREGA-F3` | **Sim** (atalho) | Botão Entrega/F3 vai para etapa entrega; **Pagar/F7** continua pagamento. Carrinho e finalizar iguais. |
| `BI-DEVOL-DIA` | **Não** (venda) | Só gráfico/totais do BI `/`. |
| `ENT-VIA-DIN-SEM-MAQ` | **Não** (cobrança) | Só texto da **via do entregador** (dinheiro sem troco = COBRAR DINHEIRO). Cartão/Pix continua LEVAR MÁQUINA. |

Rotina no deploy: pausar vendas ~2–3 min · Ctrl+F5 · smoke: venda balcão F7 · entrega F3 · via dinheiro sem troco.

## Deploy (próximo chat + senha)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0109
git push origin producao --force-with-lease
```

Render sobe · conferir healthz / badge **v20.56**.
