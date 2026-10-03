# Rollback — ETQ-53-QUOTA (hotfix etiquetas v26.02)

Volta a loja ao estado **antes** deste hotfix (Live **v26.00**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `af1944cd` · VERSION **26.00** |
| **Tag segurança** | `rollback/pre-etq-53-quota-v26.00` |
| **Branch backup** | `producao-backup-pre-v2602-etq-quota-20261003` |
| **Branch PREP** | `deploy/prep-etq-53-quota` · alvo loja **v26.02** |
| **Migrate** | **NÃO** |
| **Merge `teste`?** | **NÃO** — só cherry deste PREP |

## Pacote

| # | Pacote | Risco loja aberta |
| - | ------ | ----------------- |
| 1 | **ETQ-53-QUOTA** | Baixo — só tela `/produtos/etiquetas/` (JS + cache-bust). Não mexe venda/caixa/NF. |

## Como voltar (só frase + senha)

```bash
git fetch origin tag rollback/pre-etq-53-quota-v26.00
git checkout producao
git reset --hard rollback/pre-etq-53-quota-v26.00
git push origin producao --force-with-lease
```

## Provas (PREP · PIN 9973)

| Prova | Resultado |
| ----- | --------- |
| ETQ-53-QUOTA path | **34/34** |
| ETQ-53-UX path | **32/32** |
| Smoke local | **22/22** |
| Térmica várias | **39/39** |
