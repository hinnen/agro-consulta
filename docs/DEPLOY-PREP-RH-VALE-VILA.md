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

## Git ≠ Render (importante)

O script só faz **`git push` → branch `producao`**. A loja só muda quando o serviço **Sistvale - Produção** no Render **termina um build** dessa branch.

Se após o push **não aparecer deploy novo** no painel (ou o vale continuar com erro de empresa Vila), faça:

1. Render → projeto **SistVale** → **Production** → serviço **Sistvale - Produção**
2. **Manual Deploy** → **Deploy latest commit** (branch `producao`)
3. Aguarde status **Live** / verde (~3–8 min)
4. **Ctrl+F5** no Chrome da Vila e repita o smoke

Conferência: commit esperado **`9f18d10f4`** (v27.18 RH-VALE-VILA). Erro antigo *«Empresa 'Agro Mais Vila Elias' não encontrada — não foi possível vincular vale…»* = ainda no commit **`39337d12`**.

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
