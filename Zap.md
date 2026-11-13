# Zap — contexto vivo (PC2)

Ler este arquivo **inteiro** antes de mexer no WhatsApp. Vale no Cursor da nuvem e no Cursor instalado. Renan chama com `@Zap`. Ao fechar o assunto, atualizar este arquivo sem perguntar se mudou algo que precisa continuar valendo.

Atualizado **12/11/2026**.

## O que é

O Zap da loja **não roda no Render**. Roda neste computador: **PC2-Centro** (Windows). As lojas abrem `https://zap.zapsistvale.online`.

O site da loja (Agro Consulta / Sistvale / branch `producao`) **não** é este Zap. Não subir nada na loja sem frase explícita + senha na mesma mensagem.

## Onde está

| Coisa | Lugar |
| --- | --- |
| Programa | `C:\AgroWhatsApp\repo` |
| Banco (conversas) | Postgres 17, banco `agro_wa` |
| Áudios e fotos | `C:\AgroWhatsApp\Dados\media` |
| Sessão do celular conectado | `whatsapp_atendimento\auth` |
| Configuração | `.env.wa-servidor` (não vai pro Git) |
| Cópia de segurança | OneDrive `Zap-copia` — **tudo**: conversas, áudios, fotos, código, senhas, sessão e túnel |
| Código no GitHub | branch `cursor/zap-boot-login-64bd` (não é `producao`) |

## Liga sozinho

No login do Windows sobe o Django (porta 8000) e a ponte. A tarefa `AgroZapTunel` espera o Zap responder e só então reinicia o túnel. Sem isso, depois de reiniciar o endereço dá erro 503.

O Cursor remoto deste PC também sobe sozinho, na pasta `C:\AgroWhatsApp\repo`.

## PIN e clientes

O código que lê PIN, nome de quem respondeu, cliente, fiado, vale e cashback **continua**. Conversas ficam no `agro_wa`. A segunda conexão `loja` (`AGRO_LOJA_DATABASE_URL`, router `config.loja_leitura`) está no programa e **não migra** a loja. Em 12/11 **ligada**: a senha ficou só no `.env.wa-servidor` deste PC (endereço externo do banco da loja). Conferido: 12 PINs e 2326 clientes. Em qualquer computador: sem PIN fresco o Zap **não deixa atender** e abre o teclado. O PIN vale **45 segundos** como no PDV e renova a cada mensagem. Abrir pelo botão do PDV já no PIN daquele computador só funciona quando a **loja** tiver esta atualização e a mesma chave `AGRO_WA_PIN_HANDOFF_KEY` (não vai no Git). Até lá, o Zap pede o PIN. O Zap antigo no Render é o serviço **agro-consulta-wa** (Renan suspendeu). A loja **Sistvale - Produção** continua no ar.

## Login da tela

- `renan` — o de sempre
- `agromais` — da loja, criado em 12/11 (senha no CHECKPOINT do `banana.md`)

Os dois entram em `/entrar/`.

## O que o Git guarda e o que não guarda

O Git guarda o **programa**. A cópia completa para trocar de PC fica no OneDrive, pasta `Zap-copia` (tarefa `AgroZapCopia`, todo dia 21h). Lá entram conversas, áudios, fotos, código, senhas, sessão do celular e túnel. O `LEIA-ME.txt` dessa pasta diz o que é cada parte.

Não colocar essa cópia dentro do Git.

## Arquivar entre aparelhos

Filtros da lista (Ativas, Resolvidas, relógio, Marcar) ficam na **mesma linha**. O PIN fica no topo da coluna, ao lado do número, largo o bastante para o nome inteiro numa linha. No SistApp o botão Zap abre `https://zap.zapsistvale.online` (o Zap deste PC). Não passa pela tela de entrar da Gestão.

Os PINs conhecidos ficam neste PC para abrir rápido. Se o PIN não está na lista, o Zap pergunta na hora para a loja. A lista sozinha não recusa ninguém. A loja é relida uma vez por dia.

A busca mostra a foto quando a conversa já tem. Se a conversa existe e a foto ainda não chegou, pede no máximo 3 por vez. Cadastro sem conversa continua só com a letra.

Marcar várias: a barra fica na coluna da lista, escura, e a conversa marcada fica verde-escuro (o texto continua legível).

Na lista, última mensagem **enviada** mostra o tracinho (um = foi, dois = lida). Resposta pelo celular ou pelo Zap **zera** o número verde. Conversa antiga só ganha o tracinho na próxima mensagem.

Ainda **não** é confiável nos dois sentidos (Zap ↔ WhatsApp Web/celular). Em 12/11 ~09:37 o log mostrou `[ARCHIVE MUTATION]` e o banco marcou Resolvidas, mas a conversa **continuou na lista** do celular e do WhatsApp Web. O aviso foi só interno. Não aplicar os snippets da Meta.

## Cuidado

- Dois processos da ponte ao mesmo tempo derrubam o envio (conflito 440).
- Túnel no ar com o Django desligado = 503. Religar o túnel **depois** do healthz.
- Mudança no programa deste PC a loja vê na hora, porque o endereço aponta para cá. Commit no Git, sozinho, não muda o que está ligado.
