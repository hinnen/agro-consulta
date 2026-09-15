# Rollback — Checklist 07/09b (loja alvo **v23.45**)

Ponto **antes** deste lote = loja **Live v23.38** (`f13fb1e` / checklist 07/09).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `f13fb1e` (Live v23.38) |
| **Tag** | `rollback/pre-checklist-0709b-v23.38` |
| **Branch backup** | `producao-backup-pre-v2345-checklist-20260907` |
| **Branch PREP** | `deploy/prep-checklist-0709b` · tip **v23.45** |
| **O quê sobe** | `WA-UI-POLL-LEVE` + `CP-FORMAS-EXTRAVIO-SINAL` |
| **O quê NÃO sobe** | merge do `teste` · outros WIP |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-0709b-v23.38
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.38**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `WA-UI-POLL-LEVE` | **Não** (ajuda) | Só frequência do poll do Zap UI |
| `CP-FORMAS-EXTRAVIO-SINAL` | **Não** | Lista formas na baixa CP + Mini DRE Extravio ± |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.45**
2. Zap: aberto + vender no PDV (não engasgar)
3. CP: baixa → só **DINHEIRO** e **BANCO** · checkbox caixa no DINHEIRO
4. Resumo: Extravio = depósito − BANCO (**pode ser + ou −**)
