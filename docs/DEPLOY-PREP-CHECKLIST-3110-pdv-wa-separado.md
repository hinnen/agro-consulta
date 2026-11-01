# PREP — checklist 31/10 · Zap separado no PDV · v27.26.45

Base: `producao` @ `925dd0cf` (VERSION **27.26.44**).

Entra só **PDV-WA-SEPARADO**:

- O ícone verde do PDV abre `https://agro-consulta-wa.onrender.com` em outra aba.
- O botão **Loja** manda o orçamento para esse serviço (`/api/pdv/zap-loja/orcamento/`).
- Se o serviço do Zap ainda não tiver a rota (404, 502 ou 503), o orçamento grava na Sistvale, como hoje.
- A URL antiga do Zap na Sistvale redireciona. `/healthz`, venda, caixa e checkout ficam no worker do PDV.
- No próprio serviço do Zap o middleware não redireciona (evita loop). Não definir `AGRO_WA_PUBLIC_ORIGIN` lá.

Migrate: **não**. NFC-e e rotas de NF-e: **não neste delta**.

Ainda não está na loja. O próximo chat sobe só com as lojas pausadas, frase e senha.

Revert: `git push origin rollback/pre-v272645-3110-pdv-wa-separado:producao` → `925dd0cf`.
