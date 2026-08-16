# Rollback — PDV Pedir loja

Ponto **antes** desta feature (Live v16.53, sem overlay Pedir loja):

| Item | Valor |
| ---- | ----- |
| **Commit** | `7f7b8022f9e9d7e1765ece3261f6191784071254` |
| **Tag** | `rollback/pre-pdv-pedir-loja` |
| **Branch backup** | `backup/pre-pdv-pedir-loja-20260815` |
| **Migrate desta feature** | `estoque/migrations/0018_solicitacao_transferencia_pdv.py` |

## Voltar o código (teste)

```bash
git fetch origin
git checkout teste
git reset --hard rollback/pre-pdv-pedir-loja
git push origin teste --force-with-lease
```

Só use `--force-with-lease` se **ninguém mais** tiver commits novos em `teste` que você queira manter. Alternativa sem force: revert dos commits da feature.

## Produção

**Não** sobe nem reverte produção sem frase + senha do Renan (ver `banana.md`).

Se a feature já estiver na loja:

```bash
git checkout producao
git reset --hard rollback/pre-pdv-pedir-loja
# ou revert dos commits da feature
```

## Banco (se a migration 0018 já rodou)

O rollback do código **não** apaga sozinho as tabelas. Se precisar limpar:

```bash
python manage.py migrate estoque 0017
```

Isso remove `SolicitacaoTransferenciaPdv` / itens / eventos. Pedidos já transferidos **não** desfazem o estoque — o movimento Agro fica.

## Conferir que voltou

- `/pdv/` **sem** botão **Pedir loja**
- `/transferencias/` igual a antes
- badge `VERSION` **16.53**
