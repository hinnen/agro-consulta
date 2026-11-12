# Zap — contexto vivo (PC2)

Ler este arquivo **inteiro** antes de mexer no WhatsApp. Vale no Cursor da nuvem e no Cursor instalado. Renan pode chamar com `@docs/WHATSAPP-PC2.md`.

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

## Login da tela

- `renan` — o de sempre
- `agromais` — da loja, criado em 12/11 (senha no CHECKPOINT do `banana.md`)

Os dois entram em `/entrar/`.

## O que o Git guarda e o que não guarda

O Git guarda o **programa**. A cópia completa para trocar de PC fica no OneDrive, pasta `Zap-copia` (tarefa `AgroZapCopia`, todo dia 21h). Lá entram conversas, áudios, fotos, código, senhas, sessão do celular e túnel. O `LEIA-ME.txt` dessa pasta diz o que é cada parte.

Não colocar essa cópia dentro do Git.

## Cuidado

- Dois processos da ponte ao mesmo tempo derrubam o envio (conflito 440).
- Túnel no ar com o Django desligado = 503. Religar o túnel **depois** do healthz.
- Mudança no programa deste PC a loja vê na hora, porque o endereço aponta para cá. Commit no Git, sozinho, não muda o que está ligado.
