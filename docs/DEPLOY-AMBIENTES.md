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

## Staging (teste)

No serviço **agro-consulta-staging** (Render → Environment):

| Variável | Valor |
| -------- | ----- |
| `AGRO_STAGING_READONLY` | `true` |
| `AGRO_ERP_PEDIDOS_DRY_RUN` | `true` |
| `AGRO_FONTE_CATALOGO` | `agro_pg` |
| `DATABASE_URL` | Postgres **só do staging** |

Após deploy: `python manage.py importar_catalogo_mongo_produto` (1×).

## Produção — etapa 1 cadastro (2026-06-22)

No serviço **Sistvale - Produção** (Render → Environment):

| Variável | Valor |
| -------- | ----- |
| `AGRO_FONTE_CATALOGO` | `agro_pg` |
| `AGRO_STAGING_READONLY` | `false` (ou omita) |

**Após deploy:** Shell Render da loja → `python manage.py importar_catalogo_mongo_produto` (1×).

Conferir cadastro + busca GM/barras + salvar preço. **PDV ainda usa Mongo** nesta etapa.
