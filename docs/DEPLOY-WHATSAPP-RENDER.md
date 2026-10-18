# WhatsApp em serviço Render separado (PDV sem lentidão)

## Por quê

A **ponte** (`whatsapp_atendimento/iniciar.bat` no PC) faz poll a cada ~8–10 s nas APIs `api/atendimento-whatsapp/bridge/*`. Com o **mesmo** Gunicorn do PDV, isso compete com venda, busca e caixa e deixa a loja lenta.

Solução: um **segundo Web Service** no Render com o **mesmo código e o mesmo Postgres**, só para WhatsApp. A ponte e o Chrome do atendimento usam a URL desse serviço; o PDV continua no serviço principal.

## Resumo

| Onde | Variável importante |
| ---- | ------------------- |
| **Sistvale - Produção** (PDV) | `AGRO_WA_PUBLIC_ORIGIN=https://<seu-agro-consulta-wa>.onrender.com` |
| **agro-consulta-wa** (novo) | **Não** definir `AGRO_WA_PUBLIC_ORIGIN` · mesmo `DATABASE_URL`, `SECRET_KEY`, `AGRO_WA_BRIDGE_TOKEN` que a loja |
| **PC — `whatsapp_atendimento/.env`** | `AGRO_WA_DJANGO_URL=https://<seu-agro-consulta-wa>.onrender.com` |

## Passo a passo no Render (produção)

1. **Criar o serviço WA**  
   - Blueprint / `render.yaml`: serviço `agro-consulta-wa`, branch `producao`.  
   - Ou duplicar manualmente o web da loja com outro nome e **sem** `AGRO_CANONICAL_ORIGIN` (o Zap fica no host `*.onrender.com` do WA ou num subdomínio futuro).

2. **Environment Group**  
   - Anexe o **mesmo** grupo da loja (Postgres, `SECRET_KEY`, `AGRO_WA_BRIDGE_TOKEN`, etc.).  
   - No serviço **loja**, adicione `AGRO_WA_PUBLIC_ORIGIN` com a URL HTTPS do `agro-consulta-wa` (copie do painel após o primeiro deploy).  
   - No serviço **WA**, confirme que **não** existe `AGRO_WA_PUBLIC_ORIGIN`.

3. **Plano**  
   - O WA pode usar um plano menor que o PDV (ex. Standard ~US$ 25–85 conforme RAM). O PDV pode voltar ao plano que fizer sentido só para balcão.

4. **Deploy**  
   - Deploy dos dois serviços (mesmo commit `producao`).

5. **PC da ponte**  
   - Em `whatsapp_atendimento/.env` (ao lado do `iniciar.bat`):

     ```
     AGRO_WA_DJANGO_URL=https://SEU-agro-consulta-wa.onrender.com
     AGRO_WA_BRIDGE_TOKEN=<igual ao Render da loja>
     ```

   - Abra o chat em **`https://SEU-agro-consulta-wa.onrender.com/atendimento-whatsapp/`** (atalho Z na home já abre essa URL quando `AGRO_WA_PUBLIC_ORIGIN` está na loja).  
   - Faça login **uma vez** nesse host (sessão é por domínio).  
   - Rode **uma** janela do `iniciar.bat`.

6. **Prova**  
   - Com a ponte ligada: PDV fluido na URL da loja (`www.sistvale.com.br`).  
   - Mensagem de teste cai no chat do serviço WA.  
   - Se a ponte ainda apontar para a URL da **loja**, o PDV responde `503` na bridge com texto pedindo para corrigir o `.env`.

## Staging (`teste`)

Opcional: criar `agro-consulta-wa-teste` na branch `teste`, `AGRO_WA_PUBLIC_ORIGIN` no staging PDV, e testar a ponte contra o WA de teste antes de repetir em produção.

## O que o código faz

- Middleware `AgroWhatsAppSplitMiddleware`: no serviço com `AGRO_WA_PUBLIC_ORIGIN`, redireciona `/atendimento-whatsapp/*` e bloqueia barato `/api/atendimento-whatsapp/bridge/*`.  
- Menu home / launcher: links Z e Y já usam a URL do serviço WA quando a variável está definida.

## Rollback

- Remova `AGRO_WA_PUBLIC_ORIGIN` do serviço loja e aponte a ponte de volta para a URL antiga (volta a lentidão se ponte + PDV no mesmo worker).  
- Pode desligar o serviço `agro-consulta-wa` no painel sem apagar dados (tudo está no Postgres compartilhado).
