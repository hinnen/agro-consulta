# Deploy — só duas branches

| Branch | Render | URL |
|--------|--------|-----|
| **`teste`** | agro-consulta-staging | staging.onrender.com |
| **`producao`** | Sistvale - Produção | www.sistvale.com.br |

- Push **`teste`** → deploy só no **staging**.
- Merge **`teste` → `producao`** → deploy só na **produção**.
- **`main` / principal** — ignore; não use para deploy.

## Render (Settings → Branch, uma vez)

1. **agro-consulta-staging** → branch **`teste`**
2. **Sistvale - Produção** → branch **`producao`** (trocar de `main`)

## Produção (quando aprovar o teste)

GitHub: PR **`teste` → `producao`** → Merge.
