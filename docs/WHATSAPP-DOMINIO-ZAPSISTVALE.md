# WhatsApp — domínio fixo `zapsistvale.online` (Hostinger + Cloudflare Tunnel)

URL final (recomendada): **https://zap.zapsistvale.online/atendimento-whatsapp/**

Não mexe no `sistvale.com.br`. Só o domínio novo.

---

## 1. Cloudflare (conta grátis, uma vez)

1. Crie conta em https://dash.cloudflare.com (se não tiver).
2. No PC servidor, pasta `whatsapp_atendimento\servidor_pc\tunel_fixo\`:
3. **`tunel_fixo_login.bat`** → login no navegador → **Authorize** (não precisa adicionar o domínio na Cloudflare se for DNS manual no Hostinger).

---

## 2. Criar túnel no PC

**`tunel_fixo_criar_zapsistvale.bat`**

Anote na tela:

- **URL fixa** (ex.: `https://zap.zapsistvale.online`)
- **CNAME** → algo como `xxxxxxxx.cfargotunnel.com`

---

## 3. Hostinger — DNS

Painel do domínio **zapsistvale.online**:

| Ação | Detalhe |
|------|---------|
| **Apagar** | Registro **A** `@` → `2.57.91.91` (estacionamento) |
| **Criar CNAME** | Nome: **`zap`** · Aponta para: **`UUID.cfargotunnel.com`** (copiado do passo 2) · TTL 300 |

Nameservers podem continuar **Hostinger** (`artemis` / `hermes`) — **não** precisa mudar NS agora.

Opcional: registro **CNAME** `www` → `zapsistvale.online` pode ficar ou remover.

Teste no navegador (após propagar):  
`https://zap.zapsistvale.online/healthz` → deve responder OK.

---

## 4. Token PDV (uma vez)

**`2 - Configurar…`** → `AGRO_WA_BRIDGE_TOKEN` = **mesmo** da Sistvale Produção (Render).

---

## 5. Todo dia

**`3 - Subir Zap - todo dia.bat`**

- Se existir `config-agro-wa.yml`, usa **túnel fixo** (não trycloudflare).
- Avisa o PDV com a URL fixa (**479** em produção).

Lojas: botão verde WhatsApp → **zap.zapsistvale.online** (não muda ao reiniciar).

---

## 6. Produção Render (opcional)

Quando estiver estável, pode definir na Sistvale:

`AGRO_WA_PUBLIC_ORIGIN=https://zap.zapsistvale.online`

O registro automático do PC **sobrescreve** no Postgres; a env vira fallback.

---

## Problemas

| Sintoma | O que fazer |
|---------|-------------|
| DNS não abre | Espere propagação; confira CNAME `zap` no Hostinger |
| 502 / erro túnel | Django rodando? Janela **Zap - servidor** com `Starting development server` |
| PDV abre URL velha trycloudflare | Rodar **3** com túnel fixo; deve aparecer aviso PDV; fechar aba antiga |
