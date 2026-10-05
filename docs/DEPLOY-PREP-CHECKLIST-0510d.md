# PREP deploy — Checklist 05/10d (`deploy/prep-checklist-0510d` · alvo **v26.46**)

**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## Conteúdo (2 pacotes · migrate **NÃO**)

| # | Pacote | Prova |
| - | ------ | ----- |
| 1 | **CREDITO-SCORE-TRAVAS** | shadow **98/98** · xlsx **90/90** |
| 2 | **ETQ-PONTE-1CLIQUE** (+ **NODE-FIX**) | **37/37** · bridge **22/22** · full **79/79** |

**O quê:**

- Travas `shadow_v1_1` no laboratório de crédito (flag off em produção — sem escrita financeira).
- ZIP da ponte: na raiz só **CLIQUE-AQUI-INSTALAR.bat** + Node embutido em `app/vendor` (absorve correção bat ASCII / fallback download Node).

**Base Live:** v26.40 @ `8be1f093`  
**Branch PREP:** `deploy/prep-checklist-0510d`  
**Rollback:** tag `rollback/pre-checklist-0510d-v26.40` · branch `producao-backup-pre-v2646-checklist-0510d` · `docs/ROLLBACK-CHECKLIST-0510d.md`

**Não mexe:** PDV venda · caixa · Point · NFC-e · financeiro · Pedir loja. Diff = score shadow + ponte/ZIP etiquetas + docs/scripts de prova.

## Na senha (pausa curta ~1–2 min)

```bash
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0510d
git push origin producao
```

Acompanhar Render **Sistvale - Produção** até Live → Ctrl+F5 · badge **v26.46**.  
PC etiqueta (se for reinstalir ponte): baixar ZIP de novo em Etiquetas → extrair → **CLIQUE-AQUI-INSTALAR.bat**.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-CHECKLIST-0510d.md`.
