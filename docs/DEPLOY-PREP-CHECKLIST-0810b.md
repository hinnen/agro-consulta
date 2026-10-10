# PREP deploy — Checklist 08/10b (`deploy/prep-checklist-0810b` · alvo **v26.66**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (4 pacotes)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CREDITO-SCORE-TRAVAS-V12** | shadow **101/101** · xlsx **90/90** · lab **34/34** | **NÃO** |
| 2 | **PDV-BALANCA-ETQ-PLU** | path **27/27** · unit **5/5** | **NÃO** |
| 3 | **META-LOJAS** | path **154/154** | **NÃO** |
| 4 | **REL-HORA** | path **67/67** · unit **7/7** | **NÃO** |

**Base Live:** v26.60 @ `e4206bbf`  
**Branch PREP:** `deploy/prep-checklist-0810b`  
**Rollback:** tag `rollback/pre-checklist-0810b-v26.60` · branch `producao-backup-pre-v2666-checklist-20261008b` · `docs/ROLLBACK-CHECKLIST-0810b.md`

Substitui o PREP `deploy/prep-checklist-0810` (v26.62): aquele não tinha META nem hora a hora. Na senha use **esta** branch.

**Não mexe de propósito:** finalizar venda, caixa, Point, NFC-e. Crédito = lab. Balança = leitura da etiqueta. META = tela `/meta/`. REL-HORA = relatório novo.

## Na senha (pausa curta ~2 min)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0810b
git push origin producao
```

Acompanhar até Live → Ctrl+F5 · badge **v26.66**.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-CHECKLIST-0810b.md`.
