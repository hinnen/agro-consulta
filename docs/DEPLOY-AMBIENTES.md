# TESTE vs PRODUÇÃO — fluxo de deploy

Dois ambientes fixos. O assistente (Cursor) **sempre** entrega em TESTE; PRODUÇÃO só quando **você** aprovar.

## Mapa rápido

| Ambiente   | Branch Git              | Render (serviço)      | Quem atualiza |
| ---------- | ----------------------- | --------------------- | ------------- |
| **TESTE**  | `feat/desvincula-erp`   | `agro-consulta-teste` | Cursor + você (push) |
| **PRODUÇÃO** | `main`                | `agro-consulta`       | **Só você**, após testar |

Push na branch **não** muda produção. Produção muda quando **`main`** recebe merge (PR ou merge local + push).

## Fluxo do dia a dia

```
Cursor edita → commit em feat/desvincula-erp → push
       ↓
Render redeploya agro-consulta-teste
       ↓
Você testa no URL de TESTE (F5 / Ctrl+F5 no PDV)
       ↓
OK?  →  abrir PR feat/desvincula-erp → main  (ou pedir ao Cursor: "pode ir para produção")
       ↓
Merge em main → Render redeploya agro-consulta (produção)
```

## O que o Cursor deve fazer

- Trabalhar na branch **`feat/desvincula-erp`** (TESTE).
- Commit e push **só** nessa branch, salvo pedido explícito seu.
- **Nunca** commitar direto em `main`.
- **Nunca** abrir PR / merge para `main` sem você pedir ("pode ir para produção", "merge na main", etc.).

## O que você faz para ir à produção

Escolha uma:

1. **GitHub:** Pull request `feat/desvincula-erp` → `main` → Revisar → Merge.
2. **Terminal:**
   ```bash
   git checkout main
   git pull origin main
   git merge feat/desvincula-erp
   git push origin main
   ```
3. **Pedir ao Cursor:** "Testei, pode mergear para produção" (ele abre PR ou orienta o merge).

Depois do deploy em produção: **Ctrl+F5** no PDV para limpar cache de JS.

## Configurar Render (uma vez)

### Produção (já existe)

- Serviço: **`agro-consulta`**
- Branch: **`main`**
- Auto Deploy: On

### Teste (criar se ainda não existir)

1. Render → **New** → **Web Service** → mesmo repositório.
2. Nome: **`agro-consulta-teste`**
3. Branch: **`feat/desvincula-erp`**
4. Build/start: iguais ao `render.yaml` (ou sync Blueprint).
5. **Postgres separado** para teste (recomendado) — não reutilize `DATABASE_URL` de produção.
6. Anote o URL (`https://agro-consulta-teste.onrender.com` ou similar) e use só para homologação.

Opcional no Blueprint: serviço `agro-consulta-teste` já está em `render.yaml`.

## GitHub (recomendado)

- **Branch protection** em `main`: exigir PR antes de merge (Settings → Branches).
- Assim ninguém (nem Cursor) sobe código em produção sem passo consciente.

## Resumo mental

- **TESTE** = laboratório; pode quebrar, você valida.
- **PRODUÇÃO** = loja real; só entra o que você aprovou na teste.
