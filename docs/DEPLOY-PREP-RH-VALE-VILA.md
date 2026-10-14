# Deploy loja — RH-VALE-VILA (v27.18)

## O quê

Vale no caixa da **Vila Elias** para funcionário com perfil RH na empresa **Centro** (mesmo fluxo de folha/título; desconta gaveta Vila).

## Migrate

**Não.**

## Prova (antes do push produção)

```bash
export AGRO_PIN_TESTE=9973
# default PREP = origin/deploy/prep-rh-vale-vila (não usar origin/teste — não é ancestral de producao)
./scripts/cutover_loja_rh_vale_vila.sh
```

## Cutover produção

Renan: frase + senha no chat, depois:

```bash
export AGRO_LOJA_FRASE='pode subir para produção'
export AGRO_LOJA_SENHA='99738595'
./scripts/cutover_loja_rh_vale_vila.sh --exec
```

## Smoke na loja (~2 min, janela calma)

1. **Ctrl+F5** nos PCs da Vila.
2. Abrir caixa **Vila Elias** (não Centro).
3. **Retirada** → **Adiantamento de Salário (Vale)** → funcionário na **lista** (não «Outro») → **PIN** → valor teste baixo.
4. Conferir: saldo **dinheiro** do caixa Vila baixou · vale na **ficha RH** · sem erro de empresa.

## Rollback

Tag sugerida: `rollback/pre-v2717-rh-vale-vila` @ commit de produção imediatamente anterior ao deploy.

```bash
git push origin rollback/pre-v2717-rh-vale-vila:producao
```

## Fora do pacote

PDV venda, migrate 0145, lançamentos em lote, fechamento de caixa (só usa retirada existente).
