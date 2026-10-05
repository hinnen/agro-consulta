# Rollback — CREDITO-SCORE-SHADOW (laboratório análise crédito · v26.07)

Volta a loja ao estado **antes** deste pacote (Live **v26.05**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `41a6fdea` · VERSION **26.05** |
| **Tag segurança** | `rollback/pre-credito-score-shadow-v26.05` |
| **Branch backup** | `producao-backup-pre-v2607-credito-score-shadow-20261005` |
| **Branch PREP** | `deploy/prep-credito-score-shadow` · alvo loja **v26.07** |
| **Migrate** | **SIM** `0137` — só CreateModel `ClienteAnaliseCreditoAgro` |
| **Merge `teste`?** | **NÃO** — só cherry deste PREP (o `teste` tem outros pacotes que **não** sobem) |

## Pacote

| # | Pacote | Risco loja aberta |
| - | ------ | ----------------- |
| 1 | **CREDITO-SCORE-SHADOW** | Baixo — tela admin isolada + tabela nova. Flag default **off**. **Não** mexe PDV, limite, venda, baixa, caixa, estoque. |

**O quê muda no 1º deploy:** código + `CREATE TABLE`. Com a flag **desligada**, operadores **não** veem a tela (404). PDV/fiado iguais.

**Não ligar a flag no mesmo restart** (evita 2ª queda). Depois do Live, quando quiser o laboratório: no Render `AGRO_CREDITO_SCORE_SHADOW_ENABLED=true` e `AGRO_CREDITO_SCORE_SHADOW_USERNAMES=renan` (outro restart, pode ser fora do expediente).

## Subida (próximo chat · pausa + frase + senha)

1. Lojas pausam vendas.
2. `producao` ← `deploy/prep-credito-score-shadow` (sem merge `teste`).
3. Render: build já roda `migrate --noinput` (`0137`).
4. **Não** alterar env da flag neste passo.
5. Live → Ctrl+F5 PDV → venda teste se quiser.

## Como voltar (só frase + senha)

```bash
git fetch origin tag rollback/pre-credito-score-shadow-v26.05
git checkout producao
git reset --hard rollback/pre-credito-score-shadow-v26.05
git push origin producao --force-with-lease
```

Tabela `produtos_clienteanalisecreditoagro` pode ficar órfã (só snapshots). Remover só se quiser limpar — **não** afeta fiado operacional.

No Render: apagar ou zerar as duas env vars do shadow, se tiverem sido ligadas.

## Provas (PREP · PIN 9973)

| Prova | Resultado |
| ----- | --------- |
| CREDITO-SCORE-SHADOW path | **93/93** |
| PDV-FIADO-LIMITE-REFRESH (regressão) | **39/39** |
| PDV fiado card (regressão) | **39/39** |
| PREP_FAILS | **0** |
