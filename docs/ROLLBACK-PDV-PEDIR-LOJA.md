# Rollback — PDV Pedir loja

## Produção — Pedir loja UX5–UX7 (**v16.68**) — ponto **antes** deste pacote (Live v16.59)

| Item | Valor |
| ---- | ----- |
| **Commit** | `16663c7` (Live v16.59) |
| **Tag** | `rollback/pre-pdv-pedir-loja-ux7-v16.59` |
| **Branch backup** | `producao-backup-pre-v1668-pedir-loja-ux7-20260816` |
| **O quê reverte** | furado/bip · Ajustar na busca · aviso pós-PIN · GM/fonte · **sem migrate nova** |
| **Migrate** | **nenhuma** (já tinha `estoque.0018`) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-pdv-pedir-loja-ux7-v16.59
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v16.59**.

## Produção — Pedir loja UX3 (**v16.59**) — ponto **antes** deste pacote (Live v16.58)

| Item | Valor |
| ---- | ----- |
| **Commit** | `3964a07` (Live v16.58) |
| **Tag** | `rollback/pre-pdv-pedir-loja-ux3-v16.58` |
| **Branch backup** | `producao-backup-pre-v1659-pedir-loja-ux3-20260816` |
| **O quê reverte** | overlay um tamanho · colunas Produto\|Centro\|Vila · nome 1 linha · sem migrate |
| **Migrate** | **nenhuma** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-pdv-pedir-loja-ux3-v16.58
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v16.58**.

## Produção — Pedir loja UX2 (**v16.58**) — ponto **antes** deste pacote (Live v16.56)

| Item | Valor |
| ---- | ----- |
| **Commit** | `cd261d64e7ef569211ec0f5735e65c9e9d9a8438` (Live v16.56) |
| **Tag** | `rollback/pre-pdv-pedir-loja-ux2-v16.56` |
| **Branch backup** | `producao-backup-pre-v1658-pedir-loja-ux2-20260815` |
| **Prep (ainda não loja)** | `prep/producao-v1658-pedir-loja-ux2` @ `3009c03` |
| **O quê reverte** | layout busca\|pedido · API saldos Agro · sem migrate |
| **Migrate** | **nenhuma** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-pdv-pedir-loja-ux2-v16.56
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v16.56**.

## Produção — hotfix UX PC + rosa (**v16.56**) — ponto **antes** deste hotfix

| Item | Valor |
| ---- | ----- |
| **Commit** | `d3175b313115d8118b17c5df228767cb522bb23c` (Live v16.55) |
| **Tag** | `rollback/pre-pdv-pedir-loja-ux-v16.55` |
| **Branch backup** | `producao-backup-pre-v1656-pedir-loja-ux-20260815` |
| **O quê reverte** | layout PC coluna única + botão rosa + Transferir rosa |
| **Migrate** | **nenhuma** (só front/JS) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-pdv-pedir-loja-ux-v16.55
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v16.55** (Pedir loja ainda existe; layout antigo).

## Produção (loja) — ponto **antes** do pacote Pedir loja (feature inteira)

| Item | Valor |
| ---- | ----- |
| **Commit** | `fc01036707b29968635382d465599b981c806cc7` (Live v16.53) |
| **Tag** | `rollback/pre-pdv-pedir-loja-prod-v16.53` |
| **Branch backup** | `producao-backup-pre-v1655-pedir-loja-20260815` |
| **Migrate desta feature** | `estoque/migrations/0018_solicitacao_transferencia_pdv.py` |
| **Live após pacote** | **v16.55** @ `5da53b3` · depois hotfix **v16.56** |

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

- Hotfix UX: badge **v16.55** · Pedir loja ainda no PDV (layout antigo)
- Feature inteira: `/pdv/` **sem** botão **Pedir loja** · badge **v16.53** · `/transferencias/` igual
