# Render MCP no Cloud Agent (Cursor)

## Por que aparece o erro

No **Cloud Agent** (agente na nuvem), ao clicar **Autenticar** no plugin Render, pode surgir:

`Authentication URL unavailable for this cloud MCP server`

Isso é **esperado**: o agente na nuvem **não abre** o login OAuth no seu Chrome. Não é bug do Render da loja — é limite do OAuth no cloud.

**O que fazer na hora:** clique **Pular** (Skip). A loja continua normal.

## Configuração correta (uma vez)

Use **API key** do Render em vez de OAuth.

### 1. Criar chave no Render

1. [Render Dashboard → Account Settings → API Keys](https://dashboard.render.com/u/settings#api-keys)
2. **Create API Key** (nome sugerido: `Cursor Cloud`)
3. Copie o valor (`rnd_…`) — só aparece uma vez

### 2. Guardar no ambiente Cloud Agent

1. Abra o ambiente do repo: [Cloud Agents → Environments → hinnen/agro-consulta](https://cursor.com/dashboard/cloud-agents/environments/e/757e807a-9872-11f1-ba66-0e7d0216e441)
2. **Secrets** (runtime secrets)
3. Novo secret: nome **`RENDER_API_KEY`**, valor = a chave `rnd_…`

### 3. Repositório

O arquivo `.cursor/mcp.json` já aponta para `${env:RENDER_API_KEY}` (sem senha no Git).

### 4. Próximo agente

Inicie um **novo** Cloud Agent (ou nova mensagem após salvar o secret) para a VM carregar o secret.

Teste no chat: *«Liste meus serviços Render»* — deve responder sem pedir Autenticar.

## Cursor no PC (opcional)

No **Cursor Desktop** local, OAuth do plugin Render costuma funcionar. No **cloud**, prefira sempre API key.

Alternativa local: `~/.cursor/mcp.json` com o mesmo bloco `render-api-key` e `RENDER_API_KEY` no ambiente do Windows.

## Deploy loja sem MCP

Se não quiser configurar MCP: **Manual Deploy** no painel Render (**Sistvale - Produção**, branch `producao`) continua válido.
