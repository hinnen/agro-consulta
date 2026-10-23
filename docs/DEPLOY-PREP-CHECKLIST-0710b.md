# PREP deploy — Checklist 07/10b (`deploy/prep-checklist-0710b` · alvo **v26.56**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (2 pacotes)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **BUG34-DEVOL-FRETE** | path **42/42** · PIN **9973** | **NÃO** |
| 2 | **FIADO-CUPOM-SALDO-MISTO** | path **34/34** · loja compra **48/48** | **NÃO** |

**Base Live:** v26.53 @ `32198975`  
**Branch PREP:** `deploy/prep-checklist-0710b`  
**Rollback:** tag `rollback/pre-checklist-0710b-v26.53` · branch `producao-backup-pre-v2656-checklist-20261007b` · `docs/ROLLBACK-CHECKLIST-0710b.md`

**Não mexe de propósito:** finalizar venda, caixa, Point, NFC-e. Só devolução (frete) + layout cupom fiado misto.

## Na senha (pausa curta ~2 min)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0710b
git push origin producao
```

Acompanhar até Live → Ctrl+F5 · badge **v26.56**.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-CHECKLIST-0710b.md`.
