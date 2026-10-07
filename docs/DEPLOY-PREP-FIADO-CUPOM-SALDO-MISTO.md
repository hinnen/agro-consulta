# PREP deploy — cupom fiado misto (`deploy/prep-fiado-cupom-saldo-misto` · alvo **v26.56**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (1 pacote)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **FIADO-CUPOM-SALDO-MISTO** | path **34/34** · loja compra **48/48** | **NÃO** |

**O quê:** no comprovante fiado com outra forma (ex. dinheiro + fiado), o TOTAL fica menor e o **SALDO FIADO** aparece em caixa grande. 100% fiado não muda.

**Base Live:** v26.53 @ `1f127019` (ou tip atual de `producao` no PREP)  
**Branch PREP:** `deploy/prep-fiado-cupom-saldo-misto`  
**Rollback:** tag `rollback/pre-fiado-cupom-saldo-misto-v26.53` · `docs/ROLLBACK-FIADO-CUPOM-SALDO-MISTO.md`

**Não mexe:** finalizar venda, caixa, Point, NFC-e, títulos fiado (só layout do cupom 80mm + payload).

## Na senha (pausa curta ~2 min)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-fiado-cupom-saldo-misto
git push origin producao
```

Acompanhar até Live → Ctrl+F5 · badge **v26.56** · venda mista → cupom 2 vias.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-FIADO-CUPOM-SALDO-MISTO.md`.
