# Rollback — lote checklist 03/10 (`deploy/prep-checklist-0310`)

Volta a loja ao estado **antes** deste lote (Live **v25.86**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `f540081c` · VERSION **25.86** |
| **Tag sugerida** | `rollback/pre-checklist-0310-v25.86` |
| **Branch backup** | `producao-backup-pre-v2599-checklist-20261003` |
| **Branch PREP** | `deploy/prep-checklist-0310` · alvo loja **v25.99** |
| **Migrate** | **NÃO** |
| **Merge `teste`?** | **NÃO** — só cherry deste PREP |

## Pacotes do lote (7)

| # | Pacote | Risco loja aberta |
| - | ------ | ----------------- |
| 1 | **ETQ-EAN-LOJA** | Baixo — códigos `230…` + impressão |
| 2 | **PDV-EDIT-CB-ETQ** | Baixo — só lápis/etiqueta do produto |
| 3 | **BUG-28-ENT-LOJA-PIN** | Médio — **correção** loop PIN na entrega |
| 4 | **BUG-32-SO-ENT-OUTRA** | Médio — **correção** trava só-entrega outra loja |
| 5 | **ETQ-TERMICA-53X30** | Baixo — novo preset seed 53×30 |
| 6 | **PDV-PEDIR-ETQ53** | Baixo — botão Etiquetas 53 no Pedir loja |
| 7 | **PDV-PEDIR-BIP-30** | Baixo — Aceitar silencia bip 30 min |

## Como voltar (só frase + senha)

```bash
git fetch origin tag rollback/pre-checklist-0310-v25.86
git checkout producao
git reset --hard rollback/pre-checklist-0310-v25.86
git push origin producao --force-with-lease
```

## Provas (no PREP · PIN 9973)

| Prova | Resultado |
| ----- | --------- |
| ETQ-EAN-LOJA | **74/74** |
| PDV-EDIT-CB-ETQ | **74/74** |
| BUG-28 path | **10/10** |
| BUG-28/32 runtime | **16/16** |
| BUG-32 path | **8/8** |
| ent-loja lanc | **33/33** |
| Pedir loja | **80/80** |
| Pedir etq53 | **14/14** |
| térmica | **63/63** |
| térmica várias | **39/39** |

**PREP_FAILS=0** · tip PREP a confirmar no banana após push.
