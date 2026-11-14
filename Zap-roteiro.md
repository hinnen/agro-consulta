# ZAP ROTEIRO — ler isto **antes** do `Zap.md`

O `Zap.md` é o detalhe. Este arquivo é o mapa: o que fazer e **qual pedaço** ler. Não ler o `Zap.md` inteiro em todo chat.

**Renan:** na tarefa de Zap, diga que é Zap ou anexe `@Zap`. `@Zap-roteiro` ajuda, mas não substitui a leitura deste arquivo.

Atualizado **14/11/2026**.

---

## 0. Regras duras

- Zap **não** é a loja. Não mexer em `producao`. Não subir nada na loja sem frase explícita + senha na mesma mensagem.
- Código do Zap: **uma** branch, `whatsapp`. Não criar branch `cursor/…`.
- `teste` é da loja. Não misturar.
- O endereço `https://zap.zapsistvale.online` aponta para **este PC** (PC2-Centro). Mudança no programa daqui a loja vê na hora. Commit no Git, sozinho, não muda o que está ligado.
- Dois processos da ponte ao mesmo tempo derrubam o envio (conflito 440).
- Túnel só depois do Zap responder. Django desligado + túnel no ar = erro 503.
- Ao fechar assunto que precisa continuar valendo (onde roda, login, cópia, túnel, pendência): atualizar o `Zap.md` **sem perguntar**. Detalhe passageiro não infla.

---

## 1. Todo chat de Zap

1. Ler **este arquivo** inteiro.
2. Ler no `Zap.md` **só** a seção da tabela abaixo.
3. `Zap.md` inteiro só nos casos da §3.
4. Chat longo ou mudou de assunto → repetir 1–2.

---

## 2. O que ler no `Zap.md`

| Se a tarefa é… | Ler |
| --- | --- |
| Onde fica programa, banco, foto, sessão, cópia | «Onde está» |
| Ligar, reiniciar, túnel, 503 | «Liga sozinho» + «Cuidado» |
| PIN, cliente, fiado, vale, botão do PDV | «PIN e clientes» + «Login da tela» |
| O que vai pro Git e o que fica no OneDrive | «O que o Git guarda» + «Git» |
| Lista, arquivar, foto na busca, tracinho | «Arquivar entre aparelhos» |
| Só dúvida rápida, sem mexer no programa | Este roteiro basta |

---

## 3. Quando ler o `Zap.md` inteiro

- Trocar de PC ou montar o Zap de novo
- Mexer em login, PIN, túnel, cópia ou arquivar entre aparelhos
- Retomar depois de semanas, ou o Renan pedir o contexto completo
- O roteiro não cobriu a tarefa
