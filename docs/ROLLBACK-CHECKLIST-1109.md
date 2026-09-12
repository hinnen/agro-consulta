# Rollback — Checklist 11/09 (loja alvo **v23.92**)

Ponto **antes** deste lote = loja **Live v23.91** (`22186fb` / checklist 10/09).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `22186fb` (Live v23.91) |
| **Tag (criar no deploy / já no prep)** | `rollback/pre-checklist-1109-v23.91` |
| **Branch backup (criar no deploy)** | `producao-backup-pre-v2392-checklist-20260911` |
| **Branch PREP** | `deploy/prep-checklist-1109` · tip **v23.92** |
| **O quê sobe** | `REPASSE-ACUM-EXTRA-BUG` |
| **O quê NÃO sobe** | merge do `teste` · WhatsApp · Excel · DRE WIP · outros |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-1109-v23.91
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.91**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `REPASSE-ACUM-EXTRA-BUG` | **Não** (venda) | Só cálculo do **acumulado** na tela/API do Repasse Vila→Centro depois de levar a mais no dia. Envelope já gravado **não muda**. Sem migrate. |

**Piora?** Não — a loja hoje mostra acumulado **inchado** (ex. 445 em vez de ~254). O fix só corrige o número amarelo.

## Provas (pré-envio · 11/09)

| Pacote | Prova |
| ------ | ----- |
| `REPASSE-ACUM-EXTRA-BUG` | path **18/18** · acum-net **29/29** · vila **268** · deep **103** · arredonda **41** · PIN **9973** |
| Django | `manage.py check` OK |
| Snapshot antes | `scripts/data/snapshot_repasse_acumulado_pre_fix_20260911.json` |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.92**
2. PDV → **Repasse** (hoje) → **Acumulado (dias anteriores)** ~**254** (não **445**)
3. Venda normal / caixa: sem mudança esperada
