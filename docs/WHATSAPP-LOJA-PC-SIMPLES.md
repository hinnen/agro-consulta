# WhatsApp no PC da loja — guia simples

Dois arquivos. Domínio fixo: **https://zap.zapsistvale.online**

---

## Antes (você / Renan, uma vez)

- [ ] Domínio **zapsistvale.online** **Ativo** na Cloudflare (nameservers Hostinger → Cloudflare)
- [ ] PR **479** + migrate **0155** na **Sistvale Produção**
- [ ] **`2 - Configurar…`** neste PC: `AGRO_WA_BRIDGE_TOKEN` = **igual** Render Produção

---

## PC da loja — Windows limpo (recomendado)

| Quando | Arquivo | O quê |
|--------|---------|--------|
| **1×** (~20 min) | **`1 - Instalar - primeira vez no PC.bat`** | Python, Postgres, banco, `.env` |
| **1×** (~5 min) | **`A - Loja - primeira vez PC servidor.bat`** | Cloudflare + túnel fixo + conferência |
| **Todo dia** | **`3 - Subir Zap - todo dia.bat`** | Só isto. Deixar janelas abertas. |

Operador da loja **só precisa aprender o `3`**.

---

## Copiar de um PC que já funciona (opcional)

No PC que já está OK, copie para o PC da loja (mesma pasta de usuário ou ajuste caminhos):

- `C:\AgroWhatsApp\Dados\` (mídia + senhas)
- `%USERPROFILE%\.cloudflared\` (túnel + login)
- Raiz do repo: `.env.wa-servidor`

Aí na loja: **`1 - Instalar`** (deps) → **`A - Loja`** → todo dia **`3`**.

---

## Lojas Centro / Vila / celular

- Botão **verde WhatsApp** no PDV → abre **zap.zapsistvale.online**
- **Não** instalam nada; só navegador.

---

## Desligar o Render **agro-consulta-wa** (US$ 7)

**Pode**, depois de validar:

1. **`3 - Subir`** no PC da loja + túnel fixo OK  
2. **`https://zap.zapsistvale.online/healthz`** responde  
3. Botão WhatsApp no PDV abre o Zap **neste domínio** (não trycloudflare, não onrender.com)  
4. Aí no Render: **Suspend** ou apague o serviço **agro-consulta-wa**

O PDV principal (**Sistvale / agro-consulta**) **continua** no ar — só desliga o serviço **extra** do WhatsApp.

Opcional no Render Produção (fallback):  
`AGRO_WA_PUBLIC_ORIGIN=https://zap.zapsistvale.online`  
(O PC ainda pode atualizar via API 479; a env vira reserva.)

---

## Problemas rápidos

| Problema | Ação |
|----------|------|
| Migrate / senha Postgres | **`5 - Corrigir Postgres…`** (PC “sujo”) ou PC Windows limpo |
| PDV abre link velho | **`3 - Subir`** de novo; espere “PDV atualizado” |
| Túnel caiu | Reinicie **`3`** |

Pasta dos `.bat`: `whatsapp_atendimento\servidor_pc\`
