# WhatsApp em serviço Render separado (PDV sem lentidão)

## Por quê

A **ponte** (`whatsapp_atendimento/iniciar.bat` no PC) faz poll a cada ~8–10 s nas APIs `api/atendimento-whatsapp/bridge/*`. No **mesmo** Gunicorn do PDV, isso deixa a loja lenta.

Solução: outro **Web Service** no Render só para WhatsApp (ponte + telas). O PDV fica no serviço principal.

## Reaproveitar o Render de teste (recomendado)

Você **não precisa criar** serviço novo. Use o que já existe e está parado:

| No painel Render (nome pode variar) | No `render.yaml` |
| ----------------------------------- | ---------------- |
| **agro-consulta-staging**           | `agro-consulta-teste` |

Plano **Starter (~US$ 7/mês)** basta — só WhatsApp + poll da ponte, sem PDV pesado.

### O que mudar nesse serviço (uma vez)

1. **Ligar** o serviço (Resume) no plano Starter.
2. **Branch de deploy:** `producao` (mesmo código da loja; não use `teste` para o Zap real).
3. **Build:** igual à loja (`producao_financeiro_pg_bootstrap` — já está no yaml).
4. **Environment** — copie da **Sistvale - Produção**, com estes ajustes:

   | Variável | No serviço WhatsApp |
   | -------- | ------------------- |
   | `DATABASE_URL` | **Igual à loja** (`agro-db` produção) — conversas WhatsApp estão no Postgres da loja |
   | `SECRET_KEY` | Igual à loja (login compatível) |
   | `AGRO_WA_BRIDGE_TOKEN` | Igual à loja |
   | `AGRO_WA_PUBLIC_ORIGIN` | **Não definir** |
   | `AGRO_CANONICAL_ORIGIN` | **Não definir** (Zap fica no `*.onrender.com` deste serviço) |
   | `AGRO_STAGING_READONLY` | **Remover** ou `false` |
   | `AGRO_ERP_PEDIDOS_DRY_RUN` | **Remover** ou `false` |
   | `AGRO_SNAPSHOT_FONTE_DATABASE_URL` | **Remover** (era só staging PDV) |

   Opcional no painel: renomear o serviço para **Agro WhatsApp** (só organização).

5. **Na Sistvale - Produção (PDV):** adicione  
   `AGRO_WA_PUBLIC_ORIGIN=https://<url-do-servico-reaproveitado>.onrender.com`  
   (a URL que aparece no topo do serviço depois do deploy).

6. **PC da ponte** — `whatsapp_atendimento/.env`:

   ```
   AGRO_WA_DJANGO_URL=https://<mesma-url-acima>.onrender.com
   AGRO_WA_BRIDGE_TOKEN=<igual Render loja>
   ```

   Abra o chat nessa URL (atalho **Z** na home já abre lá quando `AGRO_WA_PUBLIC_ORIGIN` está na loja). Login **uma vez** nesse host. **Uma** janela do `iniciar.bat`.

### Prova

- Ponte ligada + PDV em `www.sistvale.com.br` fluido.
- Mensagem de teste no chat do host `onrender.com` do WhatsApp.
- Ponte ainda apontando para a URL da **loja** → bridge `503` no PDV (proposital) até corrigir o `.env`.

## E o staging / branch `teste`?

- Esse host **deixa de ser** “PDV de teste no Render”.
- Testes de código continuam no **PC local** (`docs/TESTE-LOCAL.md`) ou, se um dia precisar de staging de novo, seria **outro** serviço — não misturar com o WhatsApp da loja.

## O que o código faz

- `AGRO_WA_PUBLIC_ORIGIN` no PDV: redireciona `/atendimento-whatsapp/*` e bloqueia barato `bridge/*` no worker da loja.
- Menu Z/Y: links absolutos para o host WhatsApp.

## Rollback

- Tire `AGRO_WA_PUBLIC_ORIGIN` da loja e suspenda o serviço WhatsApp. Dados não somem (Postgres da loja).

## Estado 30/10/2026 — pronto para o próximo chat autorizar a loja

Serviço já criado no painel (projeto **teste**): **agro-consulta-wa** · `https://agro-consulta-wa.onrender.com` · plano Starter · branch **`producao`** · auto-deploy **Off**. A ponte no PC já aponta para essa URL. O código deste pacote **ainda não está** na branch `producao` nem nesse serviço.

**Não** fazer merge de `teste` nem da branch `cursor/whatsapp-render-split-6637` em `producao` (milhares de commits à frente e atrás). O pacote é só o diff do WhatsApp em cima de `b91d34b79`.

Passos do próximo chat, **depois** da frase + senha na mesma mensagem:

1. Cherry-pick só os commits do Zap em `producao` (ou aplicar esse diff). Conferir que não entrou NF-e nem o resto do `teste`.
2. Manual Deploy em **agro-consulta-wa** (branch `producao`). Auto-deploy continua **Off**.
3. Na **Sistvale - Produção**, aí sim: `AGRO_WA_PUBLIC_ORIGIN=https://agro-consulta-wa.onrender.com`. **Não** colocar essa variável no serviço do Zap.
4. Build do Zap precisa de `collectstatic` (senão o JS do chat não carrega). Sem migrate nova.
5. Reiniciar o `iniciar.bat` no PC depois que o `index.js` novo estiver na pasta (o servidor já corrige o nome mesmo com a ponte antiga, porque ela manda `de_mim`).

Nome na lista: pushName «Agropecuária Agro Mais» não grava como cliente. Foto: cada poll devolve até 3 fotos pendentes; disco do Render é efêmero, então a ponte pede de novo depois de um deploy.
