# Deploy — leia só isto

## Duas branches, dois sites

| Você manda código para… | Atualiza… |
| ----------------------- | --------- |
| **`teste`** | staging (agro-consulta-staging) |
| **`producao`** | loja (Sistvale - Produção) |

## O que fazer no dia a dia

1. **Cursor / você testando** → push na branch **`teste`** → testa no staging.
2. **Gostou?** → no GitHub: **Pull request `teste` → `producao`** → Merge.  
   (Não use **`principal`** / `main` para deploy.)

## Erro de conflito no GitHub?

Feche o PR para `principal`. Só abra PR **`teste` → `producao`**.

Se pedir ajuda ao Cursor: **«pode ir para produção»**.

## Staging não pode alterar a loja (Mongo compartilhado)

No serviço **agro-consulta-staging** (Render → Environment):

| Variável | Valor |
| -------- | ----- |
| `AGRO_STAGING_READONLY` | `true` |
| `AGRO_ERP_PEDIDOS_DRY_RUN` | `true` |
| `AGRO_FONTE_CATALOGO` | `agro_pg` *(etapa 1 cadastro — só no staging)* |
| `DATABASE_URL` | Postgres **só do staging** (diferente da loja) |

**Etapa 1 cadastro (após deploy):** rodar import uma vez (Shell Render ou URL cron com token):

`python manage.py importar_catalogo_mongo_produto`

Conferir: `/api/agro/fonte-status/` → `catalogo_postgres: true`. Testar GM0027-1 → preço **R$ 20,90**.

Com isso o teste **lê** catálogo/preços do Mongo (espelho ERP) e **não grava** preço nem financeiro no Mongo. Vendas e overlay ficam no Postgres do staging.

Produção: **`AGRO_STAGING_READONLY=false`** (ou omita).

## PDV teste = fotocópia da loja (snapshot Postgres)

Objetivo: o **Render teste** mostrar os **mesmos preços/produtos** do PDV da loja, sem mexer na produção.

### 1. Variável extra (só no staging)

| Variável | Valor |
| -------- | ----- |
| `AGRO_SNAPSHOT_FONTE_DATABASE_URL` | **Internal Database URL** do Postgres **agro-db** (projeto SistVale — loja). **Nunca** no serviço da loja. |

### 2. Copiar dados (uma vez, ou quando quiser atualizar)

**Shell Render (teste):**

```bash
python manage.py copiar_snapshot_pdv_loja
```

**Ou HTTP** (mesmo token dos outros crons):

`GET /api/cron/copiar-snapshot-pdv-loja/?token=SEU_ALERTA_VENDAS_CRON_TOKEN`

Copia: `Produto`, `ProdutoGestaoOverlayAgro`, `AjusteRapidoEstoque`.

### 3. Conferir (antes de cortar Mongo no PDV)

- Ctrl+F5 no PDV teste
- Buscar `akiles` → GM0060-15 **R$ 70,00** · GM0061-15 **R$ 75,00** (igual loja)
- `/api/agro/fonte-status/` → `staging_readonly: true`

Nesta fase o PDV teste ainda **lê Mongo** para catálogo, mas aplica **overlay copiado da loja** (cache staging também recebe overlay).

### 4. Cortar Mongo no catálogo PDV (opcional — só staging)

Só depois do passo 3 OK:

| Variável | Valor |
| -------- | ----- |
| `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES` | `true` |

Estoque/médias podem continuar no Mongo. **Produção:** omitir (sempre `false`).

Revert rápido: `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=false` + redeploy.
