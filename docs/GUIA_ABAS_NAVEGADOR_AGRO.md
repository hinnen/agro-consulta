# Guia único — abas do sistema no navegador (Agro Consulta)

## O que são as três bolinhas à esquerda

Não são “atalhos que abrem outro programa” por um clique. São um **indicador** das **até 3 janelas/aba(s) do mesmo site** que o Agro permite abertas **neste navegador**, com **regra de segurança**:

- **No máximo 1 aba de PDV** (rota `/`, `/pdv/…`, ou fluxo equivalente ao PDV).
- **Até mais 2 abas “outras telas”** (ex.: `/lancamentos/`, dashboard, etc.).
- **Total: 3 abas**.

Cada bolinha tem **cor fixa por posição** (1 verde, 2 laranja, 3 violeta) e mostra se aquela “vaga” está **livre**, **ocupada por outra janela** ou **é a janela atual**.

## O que acontece ao clicar numa bolinha

- **Primeiro clique:** abre (fixa) o **painel de detalhe** ao lado — tipo de tela (PDV / outra), caminho (ex. `/pdv/`) e, se for a janela atual, o texto “Esta janela”.
- **Segundo clique na mesma bolinha:** fecha esse painel.
- **Clicar fora** da faixa lateral ou **tecla Esc:** fecha os painéis fixos.
- **Passar o rato** (hover) também mostra o painel enquanto o cursor estiver em cima (não precisa clicar).

**Isto não abre automaticamente um novo separador do Chrome/Edge.** Não há pedido de rede — é só estado na página.

## Como abrir “outra tela” de verdade (nova aba do navegador)

Use o próprio navegador:

- **Novo separador:** `Ctrl+T` (Windows) e depois cole o endereço, ou
- **Duplicar aba** no menu do separador, ou
- Use links do sistema que já digam **“abrir em nova aba”** (quando existirem).

### Chrome com o site instalado (“Abrir como janela” / atalho de app)

Nesse modo não há linha de **separadores** como no Chrome normal. O próprio navegador **não oferece** `Ctrl+T` como “nova aba” numa mesma janela (não há abas para abrir). Para outra sessão do Agro, use o botão **Nova janela** na própria faixa lateral (abre nova janela com o mesmo endereço), ou feche esse atalho e abra uma janela normal do Chrome com abas para ter `Ctrl+T`.

O limite de **3 abas** continua a valer: se tentar abrir uma **4.ª**, o Agro **bloqueia** com um aviso a tela inteira.

### Duplicar o mesmo endereço do PDV (ex.: dois separadores em `/`)

Já não bloqueamos a segunda aba só por estar no PDV. Uma ficou como sessão PDV principal; na outra, a lista lateral pode mostrar **Outra tela** com o mesmo caminho até mudar para outra rota. O papel (PDV / outra) **atualiza** ao navegar e a cada poucos segundos.

## Por que “cliquei e não notei nada”

- O efeito do clique é **só** abrir/fechar o **painel cinza/branco** ao lado da bolinha (e um **flash laranja** rápido na bolinha após correcção recente). Não muda o URL nem dispara API.
- Se o painel já estava aberto por **hover**, parece que “nada mudou” — **tire o rato** da bolinha e clique outra vez para ver o painel **manter-se** aberto (estado “fixo”).

## Desenvolvimento / duplicar ` _agro_consulta_ui.html`

Se o template incluir `produtos/_agro_consulta_ui.html` **duas vezes** na mesma página, o segundo bloco de script podia sair antes de definir o handler — **foi corrigido** para o `window.__agroConsultaTabDotToggle` ser sempre definido primeiro. Evite includes duplicados na mesma `<head>`.

## Onde está o código

- Comportamento e UI: `produtos/templates/produtos/_agro_open_external.html`
- Inclusão global: `produtos/templates/produtos/_agro_consulta_ui.html`

---

*Última revisão: alinhado ao handler `__agroConsultaTabDotToggle` antes do guard `__agroTabLimitInit` e `onclick` nas bolinhas.*
