# Teste local (PC do Renan) — a partir de 22/07/2026

**Regra:** validar no **Chrome em `http://127.0.0.1:8000`**.  
**Não** depender do Render teste (dorme no free).  
**Produção** só depois que você testou local **e** mandou frase + senha.

## 1. Uma vez (se ainda não tiver)

No PowerShell, pasta do repo `agro-consulta`:

```powershell
cd C:\Users\RenanHinnen\OneDrive\Documentos\GitHub\agro-consulta
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Arquivo `.env` (já existe aí): para espelhar a loja sem Mongo morto, confira:

```env
AGRO_FONTE_CATALOGO=agro_pg
AGRO_MONGO_ERP_DESLIGADO=true
AGRO_PDV_VENDA_SEM_MONGO_ERP=true
```

- **Sem** `DATABASE_URL` → Django usa **SQLite** (`db.sqlite3`) — bom para UI/fluxo.
- **Com** `DATABASE_URL` de um Postgres **só seu** (nunca o da loja) → dados mais parecidos com staging.

**Nunca** coloque o `DATABASE_URL` da loja no `.env` local.

## 2. Subir o site no PC

```powershell
cd C:\Users\RenanHinnen\OneDrive\Documentos\GitHub\agro-consulta
.\.venv\Scripts\Activate.ps1
python manage.py migrate
python manage.py runserver
```

Abra: **http://127.0.0.1:8000/**  
Login: usuário Django local (superuser). Se não tiver:

```powershell
python manage.py createsuperuser
```

## 3. O que testar (checklist curto)

| Tela | Ok se… |
|------|--------|
| PDV | PIN abre, busca produto responde |
| Cadastro | busca não fica minutos em «Buscando…» |
| Entrada NF | **Ler XML** não dá «Erro de rede» por Mongo |
| `/api/agro/fonte-status/` | `mongo_erp_desligado: true`, `catalogo_postgres: true` |

## 4. Fluxo com o assistente

1. Assistente **implementa** e deixa commit na branch `teste` (ou worktree).  
2. **Você** roda local e valida.  
3. Só então: *«pode subir para produção»* + senha — se quiser loja.  
4. Push no Render **teste** = **opcional** e **só se você pedir** (não é mais o gate padrão).

## 5. Se o local «parecer ok» e a loja diferente

Costuma ser: env diferente (`AGRO_FONTE_*`), banco vazio no SQLite, ou pacote que ainda não subiu na loja. Conferir `VERSION` e `/api/agro/fonte-status/` nos dois lados.
