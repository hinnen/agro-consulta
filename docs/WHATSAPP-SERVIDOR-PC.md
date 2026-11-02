# WhatsApp atendimento — servidor no PC da loja

Migrar o **host do Zap** do Render para um **Windows** (laboratório → PC da loja).  
**Não** altera o PDV Sistvale (`producao` / Render principal).

## O que fica onde

| Peça | Onde |
|------|------|
| Site + API Django + fotos/áudios | PC (`subir_servidor.bat`) |
| Ponte Baileys | Mesmo PC (`iniciar.bat` ou `iniciar-local.bat`) |
| Acesso das duas lojas + celular | Internet → **túnel HTTPS** (Cloudflare) |

Mídia grava em **`AGRO_WA_MEDIA_ROOT`** (padrão `C:\AgroWhatsApp\Dados\media`) — **não some** no `git pull`.

## Passo a passo (usuário)

1. **Git:** branch `whatsapp`, pasta do repo no PC (pode ser OneDrive; mídia fica **fora** do repo).
2. Duplo clique: `whatsapp_atendimento\servidor_pc\instalar_servidor.bat`  
   (Python, venv, `pip install`; instala Node se faltar.)
3. Duplo clique: `criar_env_servidor.bat` — responda as perguntas (Postgres local ou ajuste depois no `.env.wa-servidor`).
4. Instale **PostgreSQL** (winget / Setup). Depois: `criar_banco_agro_wa.bat` (cria user+banco `agro_wa`) — ou faça no pgAdmin.
5. Duplo clique: `subir_tudo.bat` **ou** `subir_servidor.bat` + `iniciar-local.bat`.
6. Chrome: **http://127.0.0.1:8000/atendimento-whatsapp/** — login Django (`createsuperuser` na primeira vez).
7. **Internet (teste):** com servidor rodando, `tunel_rapido.bat` → copie a URL `https://….trycloudflare.com` → rode de novo `criar_env_servidor.bat` com esse host **ou** edite `ALLOWED_HOSTS_EXTRA` / `CSRF_TRUSTED_ORIGINS_EXTRA` no `.env.wa-servidor` → reinicie `subir_servidor.bat`.
8. Ponte: em `whatsapp_atendimento\.env` use `AGRO_WA_DJANGO_URL=` **mesma URL pública** (ou `http://127.0.0.1:8000` só na loja).

## Laboratório (PC Renan · 02/11/2026)

| Item | Status |
|------|--------|
| Branch `whatsapp` | OK |
| Python + `.venv` + `pip` | OK |
| Node + `whatsapp_atendimento/node_modules` | OK |
| Pasta mídia `C:\AgroWhatsApp\Dados\media` | OK |
| `.env.wa-servidor` + ponte `.env` | OK (lab; senha PG `agro_wa_lab_2026`) |
| cloudflared | OK |
| Postgres 17 + banco `agro_wa` | OK |
| Django `runserver` + migrate | OK · login `/entrar/` user `renan` / `9973` |
| Ponte Baileys | OK · **conectado** `5513997673389` (auth antiga; Bad MAC residual ok) |
| Túnel rápido teste | OK (URL muda a cada restart) |
| Túnel fixo loja | Depois do lab |

**Como ligar de novo neste PC:** `whatsapp_atendimento\servidor_pc\subir_tudo.bat` (+ `tunel_rapido.bat` se precisar internet).  
**Fix migrate banco novo:** seed da `0065` usa modelo histórico (senão quebra em `exibir_pdv`).

## Repassar para o PC da loja

Mesmo repo + mesmos `.bat`. Só mudam:

- `.env.wa-servidor` (senha Postgres, pasta de mídia)
- `whatsapp_atendimento\.env` (URL pública final)
- Túnel Cloudflare **nome fixo** (produção; ver abaixo)

Opcional: copiar pasta `C:\AgroWhatsApp\Dados\media` e dump do Postgres `agro_wa`.

## Túnel fixo (produção, duas lojas)

1. Conta Cloudflare (grátis) + `cloudflared tunnel login`.
2. Crie um tunnel e aponte para `http://127.0.0.1:8000`.
3. Coloque o hostname em `ALLOWED_HOSTS_EXTRA` e `CSRF_TRUSTED_ORIGINS_EXTRA`.
4. Quando validar, na **Sistvale produção** ajuste `AGRO_WA_PUBLIC_ORIGIN` para essa URL (menu Z) **ou** bookmark direto no host do PC.

Doc Render antiga (rollback): `docs/DEPLOY-WHATSAPP-RENDER.md`.

## Variáveis importantes

Arquivo na **raiz do repo:** `.env.wa-servidor` (modelo: `whatsapp_atendimento/servidor_pc/env.servidor.exemplo`).

| Variável | Uso |
|----------|-----|
| `DATABASE_URL` | Postgres local só WhatsApp |
| `AGRO_WA_MEDIA_ROOT` | Disco persistente de mídia |
| `AGRO_WA_BRIDGE_TOKEN` | Igual em `whatsapp_atendimento/.env` |
| `ALLOWED_HOSTS_EXTRA` | Host do túnel + localhost |

Código: `AGRO_WA_MEDIA_ROOT` em `config/settings.py`.

## Limitações do laboratório

- `runserver` no `.bat` — ok para teste; na loja estável podemos trocar por **Waitress** depois.
- `trycloudflare.com` muda se reiniciar o túnel — só teste.
- Banco **vazio** no PC ≠ conversas do Render; para espelhar, export/import Postgres separado (nunca `DATABASE_URL` da loja PDV no PC de teste).

## Ordem com visual novo

Infra PC **primeiro** (este doc). Layout estilo WhatsApp Web **depois**, no mesmo host.
