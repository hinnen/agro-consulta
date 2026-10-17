# PREP deploy — Checklist 08/10c (`deploy/prep-checklist-0810c` · alvo **v26.70**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (3 pacotes)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **REL-HORA-ROTULO** | path **85/85** · unit **7/7** | **NÃO** |
| 2 | **PDV-BALANCA-ETQ-ENTER** | path **35/35** · unit **7/7** | **NÃO** |
| 3 | **CREDITO-LIMITE-REVISAO** | path **38/38** · PIN **9973** | **SIM** `0141` |

**Base Live:** v26.66 @ `f29d3f8e`  
**Branch PREP:** `deploy/prep-checklist-0810c`  
**Rollback:** tag `rollback/pre-checklist-0810c-v26.66` · branch `producao-backup-pre-v2670-checklist-20261008c` · `docs/ROLLBACK-CHECKLIST-0810c.md`

**Não mexe de propósito:** finalizar venda, caixa, Point, NFC-e. Hora a hora = rótulo do cartão. Balança = Enter/colar da etiqueta na busca. Crédito = só o laboratório (teto 20%).

## Na senha (pausa curta ~2–3 min)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0810c
git push origin producao
```

Render faz migrate `0141` (cria tabela; não apaga nada). Acompanhar até Live → Ctrl+F5 · badge **v26.70**.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-CHECKLIST-0810c.md`.
