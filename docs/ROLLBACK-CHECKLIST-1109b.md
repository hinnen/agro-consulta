# Rollback — Checklist 11/09b (loja alvo **v23.93**)

Ponto **antes** deste lote = loja **Live v23.92** (`78098c3` / checklist 11/09 acum).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `78098c3` (Live v23.92) |
| **Tag** | `rollback/pre-checklist-1109b-v23.92` |
| **Branch backup** | `producao-backup-pre-v2393-checklist-20260911b` |
| **Branch PREP** | `deploy/prep-checklist-1109b` · tip **v23.93** |
| **O quê sobe** | `REPASSE-PCT-ZERO` |
| **O quê NÃO sobe** | merge do `teste` · WhatsApp · Excel · DRE WIP · outros |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-1109b-v23.92
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.92**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `REPASSE-PCT-ZERO` | **Não** (venda) | Só o campo **% lucro** do Repasse Vila→Centro (PDV overlay + Gestão). Envelope já gravado **não muda**. Sem migrate. |

**Piora?** Não — hoje o PDV ignora 0% salvo e força 50%. O fix só honra o padrão da Gestão.

## Provas (pré-envio · 11/09)

| Pacote | Prova |
| ------ | ----- |
| `REPASSE-PCT-ZERO` | path **20/20** · PIN **9973** · meta/config/calc API · vila **271** · deep **103** · acum **18+29** · arredonda **41** · `check` OK |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.93**
2. PDV → **Repasse** → **% lucro bruto** = **0** (igual Gestão)
3. Mudar o campo e recalcular: **não** volta sozinho para 50
4. Venda normal / caixa: sem mudança esperada
