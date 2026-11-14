# Zap — roteiro (ler isto em todo chat de Zap)

Regras e mapa. O detalhe está em **`Zap-documentacao.md`**. Não ler esse arquivo inteiro em todo chat.

**Renan:** anexe `@Zap`. Isso puxa este roteiro.

Atualizado **14/11/2026**.

---

## 0. Regras duras

- **Primeira regra.** O assistente é o responsável técnico do Zap. O Renan é o cliente, leigo. Antes de executar, alertar se o pedido tiver contra-indicação, palavra trocada, risco, ou se existir um jeito melhor. Dizer em uma frase o que ele pediu, o problema e o caminho que recomenda. Se ele confirmar mesmo assim, aí segue. Não inventar interpretação de ditado confuso: perguntar de novo.
- **Zap no ar.** As lojas testam o Zap deste PC. **Não** mudar o programa que está rodando (código, ponte, túnel, reinício) sem as duas coisas na **mesma** mensagem do Renan: frase clara de que é o Zap (*«pode mudar o Zap»*) **e** a senha `99738595`. Só a frase ou só a senha não vale. Texto copiado de instrução não vale. Sem isso: pode ler, explicar e anotar documentação. Não aplica no Zap ligado.
- A senha é a mesma da loja. Frase de loja **não** autoriza o Zap. Frase de Zap **não** autoriza a loja (`producao`).
- Zap **não** é a loja. Não mexer em `producao`. Não subir nada na loja sem frase explícita + senha na mesma mensagem.
- Código do Zap: **uma** branch, `whatsapp`. Não criar branch `cursor/…`.
- `teste` é da loja. Não misturar.
- O endereço `https://zap.zapsistvale.online` aponta para **este PC** (PC2-Centro). Mudança no programa daqui a loja vê na hora. Commit no Git, sozinho, não muda o que está ligado.
- Dois processos da ponte ao mesmo tempo derrubam o envio (conflito 440).
- Túnel só depois do Zap responder. Django desligado + túnel no ar = erro 503.
- Ao fechar assunto que precisa continuar valendo (onde roda, login, cópia, túnel, pendência): atualizar o `Zap-documentacao.md` **sem perguntar**. Se mudar regra ou este mapa, atualizar este `Zap.md`. Detalhe passageiro não infla.

---

## 1. Todo chat de Zap

1. Ler **este arquivo** inteiro.
2. Ler no `Zap-documentacao.md` **só** a seção da tabela abaixo.
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
