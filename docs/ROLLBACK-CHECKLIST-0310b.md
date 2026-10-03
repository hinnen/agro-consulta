# Rollback — lote checklist 03/10b (`deploy/prep-checklist-0310b`)

Volta a loja ao estado **antes** deste lote (Live **v25.99**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `55f8fe79` · VERSION **25.99** |
| **Tag segurança** | `rollback/pre-checklist-0310b-v25.99` |
| **Branch backup** | `producao-backup-pre-v2600-checklist-20261003` |
| **Branch PREP** | `deploy/prep-checklist-0310b` @ `51a62353` · alvo loja **v26.00** |
| **Migrate** | **SIM** `produtos.0136` (só default do campo; **não** reescreve fichas) |
| **Merge `teste`?** | **NÃO** — só cherry deste PREP |

## Pacotes do lote (2)

| # | Pacote | Risco loja aberta |
| - | ------ | ----------------- |
| 1 | **ETQ-53-UX** | Baixo — só tela `/produtos/etiquetas/` (botão 53×30, preview, presets) |
| 2 | **CLIENTE-NOVO-LIMITE-001** | Baixo no PDV atual — **só cadastro novo** nasce 0,01; quem já tem **0** no limite continua com padrão R$ 5.000 |

**Não mexe de propósito:** finalizar venda, caixa, Point, fiado de cliente antigo, NFC-e.

## Como voltar (só frase + senha)

```bash
git fetch origin tag rollback/pre-checklist-0310b-v25.99
git checkout producao
git reset --hard rollback/pre-checklist-0310b-v25.99
git push origin producao --force-with-lease
```

Após voltar o código: se o migrate **0136** já rodou, o default do banco pode continuar 0,01 até alguém reverter o campo — fichas antigas com **0** **não** mudam.

## Provas (no PREP · PIN 9973)

| Prova | Resultado |
| ----- | --------- |
| ETQ-53-UX path | **29/29** |
| ETQ-53-UX smoke local | **16/16** |
| Cliente fonte única (+ limite 0,01) | **10/10** |

**PREP_FAILS=0** no tip PREP antes de confirmar no banana e push.
