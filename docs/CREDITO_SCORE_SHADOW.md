# Análise de Crédito — Laboratório (shadow)

## Objetivo

Ferramenta **administrativa** para observar score, confiança e limite **sugerido** dos clientes, com base no histórico de fiado já existente.

**Não altera** limite, saldo, título, pagamento, venda, caixa nem estoque.

## Ativar

No `.env` / Render:

```env
AGRO_CREDITO_SCORE_SHADOW_ENABLED=true
AGRO_CREDITO_SCORE_SHADOW_USERNAMES=renan
```

- Default: **desligado** (`False`).
- Acesso: **superuser** ou username na lista (vírgula).
- **Staff sozinho não entra.**
- Com flag off: URL responde **404** (mesmo para superuser).

## URLs

| Rota | Uso |
|------|-----|
| `/fiado/analise-credito/` | Lista + botão recalcular todos |
| `/fiado/analise-credito/cliente/<pk>/` | Detalhe + histórico de snapshots |

**Não** há link no menu do PDV nem na tela operacional `/fiado/`.

## Snapshots

Tabela Postgres: `ClienteAnaliseCreditoAgro` (migration `0137`).

Cada recalculo **cria uma linha nova** (histórico). Não sobrescreve.

Única escrita normal do módulo: `INSERT` nessa tabela.

## Recalcular

- Na tela: POST nos botões (todos / um cliente).
- CLI dry-run (não grava):

```bash
python manage.py analisar_credito_shadow
python manage.py analisar_credito_shadow --cliente-id 12
```

- Gravar snapshots:

```bash
python manage.py analisar_credito_shadow --persist
```

Abrir a página (GET) **não** recalcula.

Não há cron, signal nem cálculo no PDV.

## Fórmula atual

Versão: **`shadow_v1_1`** (pesos dos 5 componentes iguais ao v1; travas no score final)

| Componente | Máx |
|------------|-----|
| Pontualidade (12 meses, média ponderada por valor) | 45 |
| Situação atual (vencidos) | 25 |
| Quitação | 10 |
| Frequência (6 meses) | 10 |
| Relacionamento | 10 |

**Travas do score final** (após somar os componentes):

- % pago em dia < 50% → score máx **79**
- % pago em dia ≥ 50% e < 80% → score máx **84**
- atraso > 30 dias nos últimos 12 meses → score máx **79**

**Candidato à revisão** exige: score ≥ 85 · confiança Média/Alta · ≥ 6 títulos analisados · ≥ 80% em dia · maior atraso histórico ≤ 15 dias · sem vencido atual · sem inconsistência crítica (quitado sem baixas → **Revisar dados**) · sugerido > limite atual.

Sem histórico suficiente → `score = null`, classificação `SEM_HISTORICO`.

Limite sugerido = média fiado 3 meses × multiplicador do score (**só simulação**).

## Isolamento

Se o laboratório falhar ou estiver desligado, o PDV e o fiado operacional continuam iguais.

O score **nunca** é consultado em:

- `api_enviar_pedido_erp`
- `resumo_credito_fiado_cliente`
- `definir_limite_fiado_cliente`
- baixas / títulos / caixa

## Subir produção (pouca parada)

1. Deploy código com flag **ainda desligada** (default) → PDV/fiado iguais.
2. `python manage.py migrate` → só `0137` (CREATE TABLE).
3. No Render: `AGRO_CREDITO_SCORE_SHADOW_ENABLED=true` + `AGRO_CREDITO_SCORE_SHADOW_USERNAMES=renan` → restart.
4. Abrir `/fiado/analise-credito/` logado como Renan → **Recalcular análises**.

Operador sem allowlist continua com **404**. Sem link no menu.

Rollback: `docs/ROLLBACK-CREDITO-SCORE-SHADOW.md`.

## Testes

```bash
set AGRO_PIN_TESTE=9973
python scripts/verify_credito_score_shadow_path.py
python manage.py test produtos.tests_credito_score_shadow.CreditoScorePurezaTests
```
