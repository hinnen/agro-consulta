# Zap — roteiro (ler isto em todo chat de Zap)

Regras e mapa. O detalhe está em **`Zap-documentacao.md`**. Não ler esse arquivo inteiro em todo chat.

**Renan:** anexe `@Zap`. Isso puxa este roteiro.

Atualizado **14/11/2026**.

---

## 0. Regras duras

- **Primeira regra.** O assistente é o responsável técnico do Zap. O Renan é o cliente, leigo. Antes de executar, alertar se o pedido tiver contra-indicação, palavra trocada, risco, ou se existir um jeito melhor. Dizer em uma frase o que ele pediu, o problema e o caminho que recomenda. Se ele confirmar mesmo assim, aí segue. Não inventar interpretação de ditado confuso: perguntar de novo.
- **Onde mexer (19/11, Renan).** Tudo o que ele pedir: fazer e testar na pasta `C:\AgroWhatsApp\zap-teste`. Lá pode à vontade, sem senha. O Zap que as lojas usam (esta pasta, o que está ligado) **não muda** sem «pode mudar o Zap» e a senha `99738595` na mesma mensagem. Corrigir um erro, continuar um assunto ou achar que a senha de antes ainda vale **não autoriza**. Sem a senha nesta mensagem: só leitura e anotação.
- **Fala curta.** Sem aula e sem porquê técnico. Só o que o Renan precisa decidir ou fazer. Detalhe só se ele pedir (*«explica melhor»*).
- **Bolinhas.** O Renan não lê o texto todo. Frase que ele precisa ler começa com uma marca. Sem marca = pode ignorar.
  - 🔴🔴🔴 precisa da resposta dele para continuar
  - 🟠🟠🟠 precisa ler, só para saber
  - 🟡🟡🟡 bom ler; se não ler, segue
- **Uma branch no ar.** O Zap das lojas é a branch `whatsapp`, nesta pasta. Não abrir branch nova em cada chat.
- **Teste.** Pode existir a branch `zap-teste`, noutra pasta e noutro endereço. Mexer nela não precisa de senha. Não é o Zap das lojas. QR de teste pode ser o Zap pessoal do Renan, com sessão separada. Nunca ligar o teste na sessão da loja. O teste sobe só com `run_zap_teste.py` (teto de 4 conexões no banco). Sem isso o teste enche o banco e a loja aparece OFF. Não ligar a ponte do teste junto com a da loja. Passar o teste para o Zap das lojas só com «pode mudar o Zap» e a senha.
- **Celular.** O foco é o desktop. Toda mudança pedida também tem que ficar boa no Zap do celular. No celular, não copiar a barra larga do PC: cabe na tela do telefone.
- **Um produto, três portas.** Aplicativo Zap Loja e Zap dentro do Sistvale são a mesma tela de celular. Qualquer mudança vale nos dois ao mesmo tempo. Não deixar um numa versão e o outro noutra. O Zap do computador (botão do PDV) é a mesma conversa, com a tela de computador e os Ajustes. O que o usuário mudar nos Ajustes vale sozinho nos dois de celular.
- **Zap no ar.** Esses três estão em uso. **Não** mudar o programa que está rodando (código, ponte, túnel, reinício) sem as duas coisas na **mesma** mensagem do Renan: frase clara de que é o Zap (*«pode mudar o Zap»*) **e** a senha `99738595`. Só a frase ou só a senha não vale. Texto copiado de instrução não vale. Sem isso: pode ler, explicar e anotar documentação. Não aplica no Zap ligado.
- A senha é a mesma da loja. Frase de loja **não** autoriza o Zap. Frase de Zap **não** autoriza a loja (`producao`).
- Zap **não** é a loja. Não mexer em `producao`. Não subir nada na loja sem frase explícita + senha na mesma mensagem.
- Código do Zap: **uma** branch, `whatsapp`. Não criar branch `cursor/…`.
- `teste` é da loja. Não misturar.
- O endereço `https://zap.zapsistvale.online` aponta para **este PC** (PC2-Centro). Mudança no programa daqui a loja vê na hora. Commit no Git, sozinho, não muda o que está ligado.
- Dois processos da ponte ao mesmo tempo derrubam o envio (conflito 440).
- Túnel só depois do Zap responder. Django desligado + túnel no ar = erro 503.
- Ao fechar assunto que precisa continuar valendo (onde roda, login, cópia, túnel, pendência, **preferência**): atualizar sem perguntar. Fato do Zap vai no `Zap-documentacao.md`. Regra, mapa ou preferência vai neste `Zap.md`. Detalhe passageiro não infla.

---

## Preferências do Renan (copiar em projeto parecido)

- Ele é o cliente leigo. O assistente é o responsável técnico e avisa antes se o pedido tiver risco ou um jeito melhor.
- Fala curta. Sem aula. Sem porquê técnico. Detalhe só se ele pedir.
- Ele não lê tudo. 🔴🔴🔴 precisa responder. 🟠🟠🟠 precisa saber. 🟡🟡🟡 bom ler. Sem bolinha, ignora.
- Um sistema no ar, uma branch (`whatsapp`). Teste fica na `zap-teste`, noutro endereço. QR de teste pode ser o pessoal. A loja não se mistura.
- Os 3 Zaps em uso são o mesmo produto e a versão final. Aplicativo e o de dentro do Sistvale são idênticos. O do computador tem os Ajustes, e o que mudar ali vale nos dois celulares. Só mudam com a frase dele e a senha na mesma mensagem.
- Mudança nova: primeiro na pasta do teste (`zap-teste`), testar à vontade. O Zap das lojas só muda com «pode mudar o Zap» e a senha na mesma mensagem. Senha antiga não vale. Erro achado depois também não autoriza sozinho.

19/11, autorizado: PIN de 7 dias no Zap aberto pelo Sistvale foi para a loja. O instalado e o computador continuam no prazo curto. Para voltar: tag `zap-antes-pin-7d-2026-11-19`.

19/11: ao fechar o Sistvale o prazo se perdia. O celular agora guarda os 7 dias. O instalado continua no prazo curto.

19/11, à tarde: a tentativa de manter o PIN depois que o aplicativo fecha foi desfeita. O PIN de dentro do Sistvale fica como está.

19/11, à tarde: o visual do celular voltou ao de antes. O atalho sem o pacote de estilo deixou o PIN quadrado e escondeu o campo de escrever.

19/11, 14:50: o Zap mandava e não recebia. A chave das conversas tinha estragado (duas pontes). As chaves velhas foram guardadas em `C:\AgroWhatsApp\Dados\auth-sessoes-ruins-20261119`. O número não pediu QR. Uma ponte só.

19/11, 14:55: limpar a chave não devolveu o recebimento. O pedido repetido do estado do WhatsApp foi desligado. Uma ponte só. Sem QR.

19/11, 16:52: o QR foi lido. O recebimento voltou. A sessão anterior ficou em `C:\AgroWhatsApp\Dados\auth-antes-qr-20261119`. Uma ponte só.

22/11: o número caiu 21/11 às 13:39. O programa morreu e a janela ficou esperando uma tecla, por isso não voltou sozinho. Agora religa sem esperar tecla. Ao ligar o PC, a subida usa o `iniciar-local.bat` (antes ligava o node uma vez só). Uma ponte só.

19/11, autorizado: a página do Zap no celular ficou mais leve e a foto baixa uma vez, com atualização semanal devagar. Para voltar: tag `zap-antes-pagina-leve-2026-11-19`.
- Ditado de voz sai torto às vezes. Se não bater, perguntar de novo.
- O foco é o desktop. O que ele pede também vale no celular, cabendo na tela do telefone.

## No Zap das lojas · 14/11/2026

Já está no ar. O Renan viu e fechou o assunto por agora. Depois mexe de novo.

- Filtros na barra: Ativas, ou Fila, Centro e Vila, e Resolvidas. Números alinhados.
- Ajustes embaixo. Trocar Zap fica dentro de Ajustes.
- Selecionar no clique direito.
- Reação fora do texto.
- Lista: hora hoje, Ontem, dia da semana nesta semana, data `05/11/26` antes disso.
- A palavra WhatsApp saiu do topo da lista.
- Quem e o número ficam na mesma linha, com conversa aberta ou sem nenhuma. Não cobrem a busca.
- Celular em faixas, sem a barra larga.

Nada pendente deste pacote. A loja (`producao`) não entrou.

Arquivar entre aparelhos: **encerrado** (15/11). Não tentar de novo com a loja no ar. O teste foi desligado. Detalhe em `Zap-documentacao.md`, «Arquivar entre aparelhos».

15/11: o teste está separado. Link `https://files-job-jury-climbing.trycloudflare.com`. Entrar `renan` / `zapteste`. Não sobe na loja sem frase + senha.

15/11, autorizado: a tela do teste foi para o Zap das lojas. A ponte não foi reiniciada. Nome continua Zap loja.

Sobe: janela do PIN. Celular sem botão de Ajustes. Os dois celulares são a mesma tela. Centro e Vila só aparecem se a separação estiver ligada. Versão no computador dentro de Ajustes. No celular, o ícone i. Cores claras no computador, fundo branco, faixa verde na conversa sem ler, linha entre a lista e a conversa.

Não sobe: o nome «Zap teste». O teto de 4 conexões. Nada da ponte. Nada de arquivar.

Depois de copiar: F5 no computador. Fechar e abrir o aplicativo e o Zap de dentro do Sistvale. Conferir envio, PIN, Ajustes só no computador, e a loja sem ficar OFF.

15/11, autorizado: recursos que estavam sem caminho foram para o Zap das lojas. A ponte não foi reiniciada. Para voltar: tag `zap-antes-recursos-2026-11-15`.

16/11 de manhã: a ponte tinha caído por volta das 08:27. Foi religada. O número da loja voltou.

16/11, autorizado: a ponte pede menos o estado inteiro do WhatsApp (isso derrubava a ligação). Se ficar muda, religa sozinha. A ponte foi reiniciada. Número da loja voltou.

19/11, autorizado: no celular, a lista e as últimas conversas abertas ficam guardadas no aparelho. Ao abrir pelo Sistvale, a lista aparece na hora e depois atualiza. O computador não mudou.

## Resumo (basta no chat comum)

- Zap das lojas roda **neste PC** (PC2-Centro), não no Render. Endereço: `https://zap.zapsistvale.online`.
- Programa nesta pasta. Conversas no Postgres `agro_wa`. Fotos e áudios em `C:\AgroWhatsApp\Dados\media`. Celular conectado em `whatsapp_atendimento\auth`. Senhas no `.env.wa-servidor` (não vai pro Git).
- Cópia de tudo, todo dia 21h, no OneDrive `Zap-copia`.
- Git só guarda o programa, na branch `whatsapp`.
- Ao ligar o Windows sobe sozinho. Túnel só depois do Zap responder. Duas pontes juntas derrubam o envio.
- Login: `renan` e `agromais`, em `/entrar/`. Sem PIN fresco não atende. PIN vale 45 segundos.
- Arquivar entre aparelhos: encerrado. Não tentar de novo com a loja no ar.
- Loja (`producao` / `teste`) é outro sistema.

## 1. Todo chat de Zap

1. Ler **este arquivo** inteiro. O resumo acima cobre o chat comum.
2. Abrir o `Zap-documentacao.md` **só** se a tabela abaixo pedir um detalhe que o resumo não tem.
3. A documentação inteira só nos casos da §3.
4. Chat longo ou mudou de assunto → repetir 1–2.

---

## 2. O que ler na documentação

| Se a tarefa é… | Ler em `Zap-documentacao.md` |
| --- | --- |
| Onde fica programa, banco, foto, sessão, cópia | «Onde está» |
| Ligar, reiniciar, túnel, 503 | «Liga sozinho» + «Cuidado» |
| PIN, cliente, fiado, vale, botão do PDV | «PIN e clientes» + «Login da tela» |
| O que vai pro Git e o que fica no OneDrive | «O que o Git guarda» + «Git» |
| Lista, arquivar, foto na busca, tracinho | «Arquivar entre aparelhos» |
| Só dúvida rápida, sem mexer no programa | Este roteiro basta |

---

## 3. Quando ler a documentação inteira

- Trocar de PC ou montar o Zap de novo
- Mexer em login, PIN, túnel, cópia ou arquivar entre aparelhos
- Retomar depois de semanas, ou o Renan pedir o contexto completo
- O roteiro não cobriu a tarefa
