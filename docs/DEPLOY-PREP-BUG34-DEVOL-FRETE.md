# PREP deploy — BUG #34 devolução frete (`deploy/prep-bug34-devol-frete` · alvo **v26.55**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (1 pacote)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **BUG34-DEVOL-FRETE** | path **42/42** · PIN **9973** | **NÃO** |

**O quê:** devolução de entrega com frete — frete marcado por padrão; ao zerar todos os itens o frete acompanha; `frete_devolvido` grava de verdade (antes o refresh apagava); cura venda «presa» só com frete.

**Base Live:** v26.53 @ `32198975`  
**Branch PREP:** `deploy/prep-bug34-devol-frete`  
**Rollback:** tag `rollback/pre-bug34-devol-frete-v26.53` · `docs/ROLLBACK-BUG34-DEVOL-FRETE.md`

**Não mexe de propósito:** finalizar venda, Point, NFC-e emissão, caixa (só fluxo de devolução).

## Na senha (pausa curta ~2 min)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-bug34-devol-frete
git push origin producao
```

Acompanhar até Live → Ctrl+F5 · badge **v26.55**.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-BUG34-DEVOL-FRETE.md`.
