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
