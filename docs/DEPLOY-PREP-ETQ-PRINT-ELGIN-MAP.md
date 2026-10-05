> ✅ **Live v26.40** — producao @ 8be1f093 · Render dep-db20u9favr4c73a4ot70 (05/10).

# PREP deploy — ETQ-PRINT-ELGIN-MAP (`deploy/prep-etq-print-elgin-map` · alvo **v26.40**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (1 pacote · migrate **NÃO**)

| # | Pacote | Prova |
| - | ------ | ----- |
| 1 | **ETQ-PRINT-ELGIN-MAP** | full **74/74** · path **45/45** · dl **21/21** · print **40→Elgin 40x40** · **50→Elgin 50x30** |

**O quê:** mapa tamanho→impressora na ponte (LocalAppData). Elgin: 2 filas Windows (**Elgin 40x40** USER + **Elgin 50x30** GONDOLA). UI no card Etiquetas.

**Base Live:** v26.35 @ `c517af0a` (já tem ETQ-PRINT-DIRETO básico)  
**Branch PREP:** `deploy/prep-etq-print-elgin-map`  
**Rollback:** tag `rollback/pre-etq-print-elgin-map-v26.35` · branch `producao-backup-pre-v2640-etq-elgin-map` · `docs/ROLLBACK-ETQ-PRINT-ELGIN-MAP.md`

**Não mexe:** PDV venda · caixa · Point · NFC-e · financeiro · Pedir loja. Diff = ponte + etiquetas + cache `?v=` em NF/cadastro/lote + scripts de prova.

## Na senha (pausa curta ~1–2 min)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-etq-print-elgin-map
git push origin producao
```

Acompanhar Render **Sistvale - Produção** até Live → Ctrl+F5 · badge **v26.40**.  
PC etiqueta: reiniciar ponte se já estiver aberta · conferir mapa 40/50 no card.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-ETQ-PRINT-ELGIN-MAP.md`.
