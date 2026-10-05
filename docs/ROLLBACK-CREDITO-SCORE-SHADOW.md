# Rollback — CREDITO-SCORE-SHADOW (laboratório análise crédito · v26.07)

Volta a loja ao estado **antes** deste pacote (Live **v26.05**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ Live **v26.05** |
| **Tag segurança** | `rollback/pre-credito-score-shadow-v26.05` *(criar no deploy)* |
| **Branch PREP** | `teste` · alvo loja **v26.07** |
| **Migrate** | **SIM** `0137` — só CreateModel `ClienteAnaliseCreditoAgro` |
| **Merge `teste`?** | Preferir cherry/PREP só deste pacote se a loja estiver atrás do tip `teste` |

## Pacote

| # | Pacote | Risco loja aberta |
| - | ------ | ----------------- |
| 1 | **CREDITO-SCORE-SHADOW** | Baixo — tela admin isolada + tabela nova. Flag default **off**. **Não** mexe PDV, limite, venda, baixa, caixa, estoque. |

**O quê muda:** laboratório `/fiado/analise-credito/` (só quem tem flag + allowlist/superuser). Snapshots em tabela nova.

## Subida com pouca parada

1. Deploy código (flag ainda **false** no Render) → loja igual.
2. `migrate` `0137` (rápido, só CREATE TABLE).
3. No Render, setar `AGRO_CREDITO_SCORE_SHADOW_ENABLED=true` e `AGRO_CREDITO_SCORE_SHADOW_USERNAMES=renan` → restart.
4. Operadores **não** veem link; URL dá 404 sem allowlist.

## Como voltar (só frase + senha)

```bash
git fetch origin tag rollback/pre-credito-score-shadow-v26.05
git checkout producao
git reset --hard rollback/pre-credito-score-shadow-v26.05
git push origin producao --force-with-lease
```

Tabela `produtos_clienteanalisecreditoagro` pode ficar órfã (só snapshots). Remover só se quiser limpar — **não** afeta fiado operacional.

No Render: apagar ou zerar as duas env vars do shadow.

## Provas (local · PIN 9973)

| Prova | Resultado |
| ----- | --------- |
| CREDITO-SCORE-SHADOW path | **93/93** |
| PREP_FAILS | **0** |
