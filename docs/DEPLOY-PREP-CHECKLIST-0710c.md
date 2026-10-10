# PREP deploy — Checklist 07/10c (`deploy/prep-checklist-0710c` · alvo **v26.60**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (4 pacotes)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **BUG31-CB-TABELA** | **38/38** · JS **6/6** · bug24 **15/15** | **NÃO** |
| 2 | **ETQ-PRESET-NF-MEM** | espelho **51/51** · quota **34/34** · sync **26/26** · ponte **23/23** | **NÃO** |
| 3 | **NF-SEM-NUMERO** | **16/16** · NF-FIN **15/15** | **NÃO** |
| 4 | **CP-CONTAS-LOJA-PG** | **49/49** · PIN **9973** | **SIM** `0140` |

**Base Live:** v26.56 @ `d08617b0`  
**Branch PREP:** `deploy/prep-checklist-0710c`  
**Rollback:** tag `rollback/pre-checklist-0710c-v26.56` · branch `producao-backup-pre-v2660-checklist-20261007c` · `docs/ROLLBACK-CHECKLIST-0710c.md`

**Extra no PREP:** restaura sync multi-PC em `produtos_etiquetas.js` (Live tinha perdido; senão presets da nota quebravam).

**Não mexe de propósito:** finalizar venda Point/NFC-e/caixa. BUG31 só corrige **qual tabela** no cashback misto. CP = contas a pagar. NF = entrada nota. ETQ = presets.

## Na senha (pausa curta ~2–3 min)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0710c
git push origin producao
```

Render faz migrate `0140`. Acompanhar até Live → Ctrl+F5 · badge **v26.60**.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-CHECKLIST-0710c.md`.
