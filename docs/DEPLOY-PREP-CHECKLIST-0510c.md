# PREP deploy — Checklist 05/10c (`deploy/prep-checklist-0510c` · alvo **v26.35**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (7 pacotes · migrate **NÃO**)

| # | Pacote | Prova |
| - | ------ | ----- |
| 1 | **PDV-PEDIR-PARCIAL-RESTO** | path **38/38** · smoke **36/36** |
| 2 | **PDV-PEDIR-PRONTO-TRANSF** | **42/42** · Pedir **80/80** |
| 3 | **META-MODO-AGORA** | **125/125** |
| 4 | **CREDITO-SCORE-XLSX-COLS** | **86/86** |
| 5 | **ETQ-PRESET-ESPELHO** | **40/40** · smoke **24/24** |
| 6 | **ETQ-PRINT-DIRETO** | full **68/68** |
| 7 | **PDV-PEDIR-PRINT-3** | **37/37** · smoke **23/23** |

**Base Live:** v26.08 @ `8b636b67`  
**Branch PREP:** `deploy/prep-checklist-0510c`  
**Rollback:** tag `rollback/pre-checklist-0510c-v26.08` · branch `producao-backup-pre-v2635-checklist-20261005` · este doc.

**Não mexe de propósito:** finalizar venda, caixa, Point, NFC-e.

## Na senha (pausa curta ~2–3 min)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0510c
git push origin producao
```

Acompanhar Render **Sistvale - Produção** até Live → Ctrl+F5 · badge **v26.35**.

## Voltar (só frase + senha)

```bash
git fetch origin tag rollback/pre-checklist-0510c-v26.08
git checkout producao
git reset --hard rollback/pre-checklist-0510c-v26.08
git push origin producao --force-with-lease
```
