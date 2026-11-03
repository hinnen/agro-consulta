# PREP — PR 479 · URL do túnel no PDV · v27.26.49

Base: `producao` @ `39b447ad` (VERSION **27.26.48**).

Entra o que a Sistvale precisa do merge 479:

- Tabela nova `WhatsAppPublicOriginAgro` (migrate **0154**, depende da 0153 que já está na loja).
- O PC, com o token da ponte, grava a URL HTTPS do túnel em `/api/atendimento-whatsapp/registrar-origin-publica/`.
- Enquanto ninguém gravar, o ícone continua em `https://agro-consulta-wa.onrender.com`.
- Essa rota de registro **fica no PDV**. O restante do Zap continua saindo para o serviço separado.
- Venda, caixa, checkout e NF-e não mudam.

Não entra neste pacote o instalador do PC (`.bat` / túnel). Esses arquivos não existem na loja hoje; ficam na PR 479, no computador do Zap.

Migrate: **sim**, só esta tabela nova. Sem linha gravada, o comportamento do PDV não muda.

Revert: `git push origin rollback/pre-v272649-0311-wa-origin-pc:producao` → `39b447ad`.
