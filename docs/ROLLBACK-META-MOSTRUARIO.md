# Rollback — META-MOSTRUARIO

Pacote: tela `/meta/` + botão META no menu Gestão + faixas `MetaVendaFaixaAgro` (migrate `0138`).

## Antes de subir

Anotar tip `producao` atual e criar tag:

```bash
git checkout producao
git pull
git tag rollback/pre-meta-mostruario-vXX.XX
git push origin rollback/pre-meta-mostruario-vXX.XX
```

## Reverter loja (só com frase + senha)

1. Voltar `producao` ao tip anterior ao pacote (ou cherry-pick reverso dos commits META).
2. **Não** apagar a tabela `produtos_metavendafaixaagro` na loja sem backup — só se Renan pedir limpar metas.
3. Deploy Render + Ctrl+F5.

## Sem migrate reverso automático

`0138` é CreateModel + seed. Remover modelo na loja só com decisão explícita.
