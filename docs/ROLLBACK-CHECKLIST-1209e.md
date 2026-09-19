# Rollback — Checklist 12/09e (loja alvo **v23.98**)

Ponto **antes** deste lote = loja **Live v23.97** (`3c56734` / ETQ-A6-COLS).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `3c56734` (Live v23.97) |
| **Tag** | `rollback/pre-checklist-1209e-v23.97` |
| **Branch backup** | `producao-backup-pre-v2398-checklist-20260912e` |
| **Branch PREP** | `deploy/prep-checklist-1209e` · tip **v23.98** |
| **O quê sobe** | `REPASSE-ACUM-PCT-DIA` · `PDV-ENT-ALERTA-LOJA` |
| **O quê NÃO sobe** | merge do `teste` · DRE/Excel/WA WIP · outros |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-1209e-v23.97
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.97**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `REPASSE-ACUM-PCT-DIA` | **Não** (só Repasse) | 1ª abertura do calc reconstrói cache de deltas · números do acumulado mudam (é o conserto do crédito fantasma) · venda/caixa/Point intactos |
| `PDV-ENT-ALERTA-LOJA` | **Não** | Só bip/piscar de horário · Vila não toca alerta de saída Centro · não bloqueia venda |

**Piora venda/caixa?** Não esperado. Venda normal / Point / fechar caixa Live **v23.97** permanece.

## Provas (pré-envio)

| Pacote | Prova |
| ------ | ----- |
| `REPASSE-ACUM-PCT-DIA` | **16/16** · PIN **9973** · PG loja (Max% 11/09=50 · fantasma δ −418) |
| `PDV-ENT-ALERTA-LOJA` | **76/76** · PIN **9973** |
| Regressão | acum-extra **18/18** · pct-zero **20/20** · `manage.py check` OK |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.98**
2. **Repasse:** abrir overlay/calc (reconstrói sozinho) · acumulado **sem** −956 fantasma cobrindo o dia
3. **PDV Vila:** entrega com saída Centro urgente → **sem** bip · Centro → bip normal
4. Venda normal / caixa: sem mudança esperada
