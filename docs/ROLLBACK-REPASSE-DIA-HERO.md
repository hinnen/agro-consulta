# Rollback — REPASSE-DIA-HERO (loja alvo **v23.99**)

Ponto **antes** = loja **Live v23.98** (`6106b91` / checklist 12/09e).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `6106b91` (Live v23.98) |
| **Tag** | `rollback/pre-repasse-dia-hero-v23.98` |
| **Branch backup** | `producao-backup-pre-v2399-dia-hero-20260912` |
| **Branch PREP** | `deploy/prep-repasse-dia-hero` · tip **v23.99** |
| **O quê sobe** | `REPASSE-DIA-HERO` (só JS hero «A SEPARAR») |
| **O quê NÃO sobe** | merge do `teste` |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-repasse-dia-hero-v23.98
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.98**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda / caixa? | Nota |
| ------ | -------------------- | ---- |
| `REPASSE-DIA-HERO` | **Não** | Só o número grande «A SEPARAR» no overlay Repasse · campo Levar / confirmar intactos |

## Provas

| Pacote | Prova |
| ------ | ----- |
| `REPASSE-DIA-HERO` | acum-pct-dia **22/22** · fundo-troco **61/61** · pct-zero **20/20** · PIN **9973** |

## Depois do deploy

1. **Ctrl+F5** · badge **v23.99**
2. Repasse: com crédito cobrindo o dia, **A SEPARAR** = dia real (não igual ao `|acumulado|`)
3. Campo **Levar ao Centro** continua **0,00** quando líquido é 0
