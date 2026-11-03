# WhatsApp atendimento — servidor no PC da loja

Migrar o **host do Zap** do Render para um **Windows** (laboratório → PC da loja).  
**Não** altera o PDV Sistvale (`producao` / Render principal).

## O que fica onde

| Peça | Onde |
|------|------|
| Site + API Django + fotos/áudios | PC (`3-subir.bat`) |
| Ponte Baileys | Mesmo PC (`3-subir.bat` abre junto) |
| Acesso das duas lojas + celular | Internet → **túnel HTTPS** (Cloudflare) |

Mídia grava em **`AGRO_WA_MEDIA_ROOT`** (padrão `C:\AgroWhatsApp\Dados\media`) — **não some** no `git pull`.

## Passo a passo (usuário)

Abra a pasta **`whatsapp_atendimento\servidor_pc`** — só **4 arquivos** na raiz (+ `LEIA-ME.txt`):

| Arquivo | Quando |
|---------|--------|
| **`1-instalar.bat`** | 1ª vez no PC novo |
| **`2-configurar.bat`** | 1ª vez (`.env` + banco Postgres opcional) |
| **`3-subir.bat`** | Todo dia (servidor + ponte) |
| **`4-tunel-teste.bat`** | Internet temporária (outra loja/celular) |

Túnel **fixo** (produção): subpasta **`tunel_fixo\`**. Scripts internos: **`_interno\`** (ignore).

1. **Git:** branch `whatsapp`, `git pull`.
2. **`1-instalar.bat`** → **`2-configurar.bat`** → **`3-subir.bat`**
3. Chrome local: **http://127.0.0.1:8000/atendimento-whatsapp/** (`createsuperuser` na 1ª vez).
4. **`4-tunel-teste.bat`** com o `3` ligado — URL copiada; aceita `*.trycloudflare.com` no `.env`.

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
| Ponte Baileys | OK · **conectado** `5513997673389` |
| Prova Renan ~14:01 | ✅ msg (+foto) saiu e chegou no celular |
| Túnel rápido teste | OK (URL muda a cada restart) — **lab continua assim** |
| Túnel fixo loja | ❌ **C abortado** · Renan **02/11 ~14:32** pediu **URL fixa** → **B** (domínio só Zap, sem sistvale) |

**Como ligar de novo neste PC:** `3-subir.bat` (+ `4-tunel-teste.bat` se precisar internet).  
**Fix migrate banco novo:** seed da `0065` usa modelo histórico (senão quebra em `exibir_pdv`).

### Próximo passo — escolha Renan (túnel fixo)

| Opção | O que é | Quando |
|-------|---------|--------|
| **A** ✅ agora | Lab com `4-tunel-teste.bat` | Já funciona; URL **muda** se reiniciar o túnel. **Não** mexe PDV |
| **B** | Domínio **só do Zap** na Cloudflare | Não toca `sistvale.com.br` |
| **C** ❌ abortado | `zap.sistvale.com.br` | Zona CF removida; retomar só com checklist + OK Renan |

#### Checklist seguro se retomar **C** (não fazer no susto)

DNS **hoje** (Hostinger parking): NS `atlas`/`hyperion.dns-parking.com` · apex A `216.24.57.1` · www → `agro-consulta.onrender.com` · PDV healthz OK.

1. Na zona Cloudflare, **antes** de mudar NS no Hostinger, criar os mesmos registros (loja no ar):  
   - `@` A → `216.24.57.1` (e o 2º A do Render se o painel Render mostrar)  
   - `www` CNAME → `agro-consulta.onrender.com`  
   - copiar MX/TXT se existirem no painel Hostinger  
2. Só então no Hostinger trocar NS para os que a Cloudflare mostrar (`carter`/`kay` ou o par atual).  
3. Esperar propagar · testar `https://sistvale.com.br` e `/consulta/` na loja.  
4. Aí sim: `zap` → túnel · `tunel_fixo_*`.

**Agora:** fechar/pausar a tela «aguardando nameservers» · **não** trocar NS · PDV segue no ar.

Assistente **não** compra domínio e **não** mexe DNS de produção sozinho.

## Repassar para o PC da loja

Mesmo repo + mesmos `.bat`. Só mudam:

- `.env.wa-servidor` (senha Postgres, pasta de mídia)
- `whatsapp_atendimento\.env` (URL pública final)
- Túnel Cloudflare **nome fixo** (produção; ver abaixo)

Opcional: copiar pasta `C:\AgroWhatsApp\Dados\media` e dump do Postgres `agro_wa`.

## Túnel fixo (produção, duas lojas)

Precisa de **pelo menos um site/domínio na conta Cloudflare**. Sem zona, a tela «Autorizar o túnel» fica vazia e **não** gera `cert.pem`.

URL fica tipo `https://zap.SEUDOMINIO` e **não muda**.

1. Ter domínio na Cloudflare (ver opções A/B/C acima).
2. `tunel_fixo_login.bat` — autorizar → **selecionar a zona** → OK (`%USERPROFILE%\.cloudflared\cert.pem`).
3. `tunel_fixo_criar.bat` — nome `agro-wa`, host (ex. `zap.…`).
4. `.env.wa-servidor`: `ALLOWED_HOSTS_EXTRA` + `CSRF_TRUSTED_ORIGINS_EXTRA` → reinicie `subir_servidor.bat`.
5. `tunel_fixo_rodar.bat` (Django já em `:8000`).
6. Ponte `.env`: `AGRO_WA_DJANGO_URL=https://…` (mesmo host).
7. Sistvale loja (quando for): `AGRO_WA_PUBLIC_ORIGIN` **ou** bookmark.

`tunel_fixo/` = produção com domínio. **`4-tunel-teste.bat`** = lab sem domínio (URL muda).

## Variáveis importantes

Arquivo na **raiz do repo:** `.env.wa-servidor` (modelo: `whatsapp_atendimento/servidor_pc/env.exemplo`).

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

## Pasta fora do OneDrive (lab Renan � 02/11)

- Codigo: `C:\AgroWhatsApp\codigo\agro-consulta`
- Atalhos: `C:\AgroWhatsApp\SUBIR-TUDO-Zap.bat` (+ 1/2/3)
- Midia continua em `C:\AgroWhatsApp\Dados\media`
- OneDrive = nao usar para rodar o Zap (copia antiga / GitHub ok)

