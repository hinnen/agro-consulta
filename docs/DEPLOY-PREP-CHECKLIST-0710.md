# PREP deploy — Checklist 07/10 (`deploy/prep-checklist-0710` · alvo **v26.53**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (2 pacotes)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **ETQ-PONTE-TOPBAR-BIP** | path **23/23** · smoke **13/13** | **NÃO** |
| 2 | **FIADO-LOJA-COMPRA** | path **39/39** · recibos **68/68** | **SIM** `0139` |

**Base Live:** v26.46 @ `68ef04ce`  
**Branch PREP:** `deploy/prep-checklist-0710`  
**Rollback:** tag `rollback/pre-checklist-0710-v26.46` · branch `producao-backup-pre-v2653-checklist-20261007` · `docs/ROLLBACK-CHECKLIST-0710.md`

**Não mexe de propósito:** finalizar venda, caixa, Point, NFC-e, recibo de pagamento/baixa.

## Na senha (pausa curta ~2–3 min)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0710
git push origin producao
```

Render faz migrate `0139` no build. Acompanhar até Live → Ctrl+F5 · badge **v26.53**.

## Voltar (só frase + senha)

```bash
git fetch origin tag rollback/pre-checklist-0710-v26.46
git checkout producao
git reset --hard rollback/pre-checklist-0710-v26.46
git push origin producao --force-with-lease
```
