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
| `DATABASE_URL` | Postgres **só do staging** (diferente da loja) |

Com isso o teste **lê** catálogo/preços do Mongo (espelho ERP) e **não grava** preço nem financeiro no Mongo. Vendas e overlay ficam no Postgres do staging.

Produção: **`AGRO_STAGING_READONLY=false`** (ou omita).
