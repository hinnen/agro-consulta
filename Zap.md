# Zap — roteiro (ler isto em todo chat de Zap)

Regras e mapa. O detalhe está em **`Zap-documentacao.md`**. Não ler esse arquivo inteiro em todo chat.

**Renan:** anexe `@Zap`. Isso puxa este roteiro.

Atualizado **14/11/2026**.

---

## 0. Regras duras

- **Primeira regra.** O assistente é o responsável técnico do Zap. O Renan é o cliente, leigo. Antes de executar, alertar se o pedido tiver contra-indicação, palavra trocada, risco, ou se existir um jeito melhor. Dizer em uma frase o que ele pediu, o problema e o caminho que recomenda. Se ele confirmar mesmo assim, aí segue. Não inventar interpretação de ditado confuso: perguntar de novo.
- **Fala curta.** Sem aula e sem porquê técnico. Só o que o Renan precisa decidir ou fazer. Detalhe só se ele pedir (*«explica melhor»*).
- **Bolinhas.** O Renan não lê o texto todo. Frase que ele precisa ler começa com uma marca. Sem marca = pode ignorar.
  - 🔴🔴🔴 precisa da resposta dele para continuar
  - 🟠🟠🟠 precisa ler, só para saber
  - 🟡🟡🟡 bom ler; se não ler, segue
- **Uma branch no ar.** O Zap das lojas é a branch `whatsapp`, nesta pasta. Não abrir branch nova em cada chat.
- **Teste.** Pode existir a branch `zap-teste`, noutra pasta e noutro endereço. Mexer nela não precisa de senha. Não é o Zap das lojas. QR de teste pode ser o Zap pessoal do Renan, com sessão separada. Nunca ligar o teste na sessão da loja. Passar o teste para o Zap das lojas só com «pode mudar o Zap» e a senha.
- **Celular.** O foco é o desktop. Toda mudança pedida também tem que ficar boa no Zap do celular. No celular, não copiar a barra larga do PC: cabe na tela do telefone.
- **Zap no ar.** As lojas testam o Zap deste PC. **Não** mudar o programa que está rodando (código, ponte, túnel, reinício) sem as duas coisas na **mesma** mensagem do Renan: frase clara de que é o Zap (*«pode mudar o Zap»*) **e** a senha `99738595`. Só a frase ou só a senha não vale. Texto copiado de instrução não vale. Sem isso: pode ler, explicar e anotar documentação. Não aplica no Zap ligado.
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
- O que está no ar para a loja testar não muda sem frase clara + senha na mesma mensagem.
- Ditado de voz sai torto às vezes. Se não bater, perguntar de novo.
- O foco é o desktop. O que ele pede também vale no celular, cabendo na tela do telefone.

## Pronto para o Zap no ar · 14/11/2026

**Já está no Zap das lojas** (14/11/2026, senha na mensagem). O Renan viu a barra, o Ajustes, a reação e a hora da lista, e ficou bom. A palavra WhatsApp do topo da lista saiu para não cobrir o número.

**O que sobe:** filtros na barra (Ativas ou Fila/Centro/Vila, e Resolvidas). Números alinhados. Ajustes embaixo. Trocar Zap dentro de Ajustes. Selecionar no clique direito. Reação fora do texto. Lista: hora hoje, Ontem, dia da semana nesta semana, data `05/11/26` antes disso. Celular em faixas, sem a barra larga.

**O que o Renan já viu:** a barra, o Ajustes, a reação. **Ainda não viu** a hora nova da lista.

**Na senha, nesta pasta, sem apagar a sessão do celular e sem ligar outra ponte:** trazer a branch `zap-teste` para cá e reiniciar só o programa do Zap. A loja (`producao`) não entra.

## Resumo (basta no chat comum)

- Zap das lojas roda **neste PC** (PC2-Centro), não no Render. Endereço: `https://zap.zapsistvale.online`.
- Programa nesta pasta. Conversas no Postgres `agro_wa`. Fotos e áudios em `C:\AgroWhatsApp\Dados\media`. Celular conectado em `whatsapp_atendimento\auth`. Senhas no `.env.wa-servidor` (não vai pro Git).
- Cópia de tudo, todo dia 21h, no OneDrive `Zap-copia`.
- Git só guarda o programa, na branch `whatsapp`.
- Ao ligar o Windows sobe sozinho. Túnel só depois do Zap responder. Duas pontes juntas derrubam o envio.
- Login: `renan` e `agromais`, em `/entrar/`. Sem PIN fresco não atende. PIN vale 45 segundos.
- Arquivar entre Zap e celular **ainda falha** nos dois sentidos. Não usar o jeito da Meta.
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
