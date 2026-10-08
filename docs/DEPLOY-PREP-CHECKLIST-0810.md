# PREP deploy — Checklist 08/10 (`deploy/prep-checklist-0810` · alvo **v26.62**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (2 pacotes)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CREDITO-SCORE-TRAVAS-V12** | shadow **101/101** · xlsx **90/90** · lab **34/34** | **NÃO** |
| 2 | **PDV-BALANCA-ETQ-PLU** | path **27/27** · unit **5/5** | **NÃO** |

**Base Live:** v26.60 @ `e4206bbf` (já tem o bip overlay/catálogo da balança; este PREP só acrescenta preferência PLU **0010** + travas do lab).  
**Branch PREP:** `deploy/prep-checklist-0810`  
**Rollback:** tag `rollback/pre-checklist-0810-v26.60` · branch `producao-backup-pre-v2662-checklist-20261008` · `docs/ROLLBACK-CHECKLIST-0810.md`

**Não usar** `deploy/prep-credito-v12` — essa branch é anterior ao bip da balança já na loja e apagaria esse fix.

**Não mexe de propósito:** finalizar venda, caixa, Point, NFC-e. Crédito = só lab shadow. Balança = leitura da etiqueta no PDV.

## Na senha (pausa curta ~2 min)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0810
git push origin producao
```

Acompanhar até Live → Ctrl+F5 · badge **v26.62**.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-CHECKLIST-0810.md`.
