# Rollback — PDV Pedir loja

## Produção (loja) — ponto **antes** deste pacote

| Item | Valor |
| ---- | ----- |
| **Commit** | `fc01036707b29968635382d465599b981c806cc7` (Live v16.53) |
| **Tag** | `rollback/pre-pdv-pedir-loja-prod-v16.53` |
| **Branch backup** | `producao-backup-pre-v1655-pedir-loja-20260815` |
| **Migrate desta feature** | `estoque/migrations/0018_solicitacao_transferencia_pdv.py` |
| **Live após pacote** | **v16.55** @ `5da53b3` |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-pdv-pedir-loja-prod-v16.53
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Depois: `migrate estoque 0017` se precisar limpar tabelas (estoque já transferido **não** volta sozinho).

## Teste (histórico)

| Item | Valor |
| ---- | ----- |
| **Commit** | `7f7b8022f9e9d7e1765ece3261f6191784071254` |
| **Tag** | `rollback/pre-pdv-pedir-loja` |
| **Branch backup** | `backup/pre-pdv-pedir-loja-20260815` |

## Banco (se a migration 0018 já rodou)

```bash
python manage.py migrate estoque 0017
```

Remove `SolicitacaoTransferenciaPdv` / itens / eventos. Pedidos já transferidos **não** desfazem o estoque.

## Conferir que voltou

- `/pdv/` **sem** botão **Pedir loja**
- `/transferencias/` igual a antes
- badge `VERSION` **16.53**
