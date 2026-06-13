# TESTE vs PRODUÇÃO — fluxo de deploy

Dois **branches com nome claro** no GitHub. O Cursor commita em **`teste`**; produção só quando você mergear em **`producao`**.

## No GitHub (lista de branches)

| Nome no GitHub | Ambiente | Render |
| -------------- | -------- | ------ |
| **`teste`** | TESTE (homolog) | `agro-consulta-teste` |
| **`producao`** | PRODUÇÃO (loja) | `agro-consulta` |

> **«principal»** no GitHub em português = branch **`main`**. Hoje ela costuma estar **igual** à `producao`. O deploy de produção pode usar `producao` ou `main` — no painel Render confira qual branch o serviço `agro-consulta` segue.

Push em **`teste`** **não** altera produção.

## Fluxo do dia a dia

```
Cursor commita → branch teste → push
       ↓
Render redeploya agro-consulta-teste
       ↓
Você testa (Ctrl+F5 no PDV)
       ↓
OK?  →  PR teste → producao  (ou: "pode ir para produção")
       ↓
Merge em producao → Render redeploya agro-consulta
```

Opcional: após merge em `producao`, faça o mesmo merge em `main` para manter **principal** alinhada.

## O que o Cursor faz

- Branch de trabalho: **`teste`**.
- **Nunca** commit direto em `producao` ou `main`.
- **Nunca** PR para produção sem você pedir.

## Ir para produção (você)

1. **GitHub:** Compare / Pull request **`teste`** → **`producao`** → Merge.
2. **Terminal:**
   ```bash
   git checkout producao
   git pull origin producao
   git merge teste
   git push origin producao
   ```

## Configurar Render (uma vez)

### Teste

- Serviço: **`agro-consulta-teste`**
- Branch: **`teste`**

### Produção

- Serviço: **`agro-consulta`**
- Branch: **`producao`** (ou **`main`** / principal, se ainda não trocou no painel)

Postgres **separado** no serviço de teste (recomendado).

## Branch antiga `feat/desvincula-erp`

Substituída por **`teste`**. Pode ignorar ou apagar depois que confirmar que tudo funciona na `teste`.
