# Rollback ÔÇö lote checklist 03/10b (`deploy/prep-checklist-0310b`)

Volta a loja ao estado **antes** deste lote (Live **v25.99**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `55f8fe79` ┬À VERSION **25.99** |
| **Tag seguran├ºa** | `rollback/pre-checklist-0310b-v25.99` |
| **Branch backup** | `producao-backup-pre-v2600-checklist-20261003` |
| **Branch PREP** | `deploy/prep-checklist-0310b` @ `af1944cd` ┬À alvo loja **v26.00** |
| **Migrate** | **SIM** `produtos.0136` (s├│ default do campo; **n├úo** reescreve fichas) |
| **Merge `teste`?** | **N├âO** ÔÇö s├│ cherry deste PREP |

## Pacotes do lote (2)

| # | Pacote | Risco loja aberta |
| - | ------ | ----------------- |
| 1 | **ETQ-53-UX** | Baixo ÔÇö s├│ tela `/produtos/etiquetas/` (bot├úo 53├ù30, preview, presets) |
| 2 | **CLIENTE-NOVO-LIMITE-001** | Baixo no PDV atual ÔÇö **s├│ cadastro novo** nasce 0,01; quem j├í tem **0** no limite continua com padr├úo R$ 5.000 |

**N├úo mexe de prop├│sito:** finalizar venda, caixa, Point, fiado de cliente antigo, NFC-e.

## Como voltar (s├│ frase + senha)

```bash
git fetch origin tag rollback/pre-checklist-0310b-v25.99
git checkout producao
git reset --hard rollback/pre-checklist-0310b-v25.99
git push origin producao --force-with-lease
```

Ap├│s voltar o c├│digo: se o migrate **0136** j├í rodou, o default do banco pode continuar 0,01 at├® algu├®m reverter o campo ÔÇö fichas antigas com **0** **n├úo** mudam.

## Provas (no PREP ┬À PIN 9973)

| Prova | Resultado |
| ----- | --------- |
| ETQ-53-UX path | **29/29** |
| ETQ-53-UX smoke local | **16/16** |
| Cliente fonte ├║nica (+ limite 0,01) | **10/10** |

**PREP_FAILS=0** no tip PREP antes de confirmar no banana e push.
