# AGENTS.md — Agro Consulta (GM Agro)

Documento de contexto para humanos e para assistentes de IA. O Cursor pode carregar regras resumidas automaticamente via `.cursor/rules/agro-consulta.mdc`; este arquivo é a **fonte completa** (use `@AGENTS.md` quando a tarefa for ampla ou precisar do mapa inteiro).

---

## 1. Stack e deploy

- **Backend:** Django (`config/`), WSGI em `config/wsgi.py`.
- **Rotas raiz:** `config/urls.py` — `healthz`, APIs `financeiro`, `indicadores` (estoque), `transferencias`, e `**''` → `produtos.urls`** (maior parte do PDV e APIs web).
- **Dados:** Mongo em fluxos financeiros e integrações (ver `produtos/mongo_financeiro_util.py`, views que usam `obter_conexao_mongo`).
- **ERP / integrações:** pacote `integracoes/` (ex.: cliente venda ERP, financeiro opcional).
- **Hospedagem típica:** Render (health em `/healthz`), Gunicorn — ver `Procfile`, `render.yaml`.
- **Desktop:** Electron em `electron/main.js` + `electron/preload.js` (empacotado por `electron-builder`; ver `package.json`).

---

## 2. Apps Django no repositório


| App                                   | Papel (resumo)                                                                                                            |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `produtos`                            | PDV web, lançamentos, clientes, vendas, APIs de busca/estoque PDV, entrada NF, etc.                                       |
| `estoque`                             | APIs de indicadores, transferência, PIN, médias, impressão, separação — várias rotas em `config/urls.py` sob `/estoque/`. |
| `financeiro`                          | API REST sob `/api/financeiro/` (include de `financeiro.api.urls`).                                                       |
| `transferencias`                      | API sob `/api/transferencias/`.                                                                                           |
| `integracoes`                         | Pontes com ERP, notificações, etc.                                                                                        |
| `base`, `core`, `lojas`, `relatorios` | Suporte conforme cada módulo.                                                                                             |


---

## 3. Mapa de URLs principais (`produtos/urls.py`)

**Páginas (MPA / templates):**


| Caminho                          | Nome (name)                    | Nota                                          |
| -------------------------------- | ------------------------------ | --------------------------------------------- |
| `/`                              | `home`                         | Dashboard gerencial (BI + launchpad); alias `/dashboard/gerencial/` (`dashboard_gerencial`) |
| `/consulta/`                     | `consulta_produtos`            | PDV legado MPA (busca / orçamentos)           |
| `/historico/`                    | `historico_ajustes`            |                                               |
| `/transferencias/`               | `sugestao_transferencia`       |                                               |
| `/entregas/`                     | `entregas_painel`              | APIs sob `/entregas/api/...`                  |
| `/ajuste-mobile/`                | `ajuste_mobile`                |                                               |
| `/compras/`                      | `compras_view`                 | Pedido fornecedor, WhatsApp                   |
| `/compras/relatorio-a4/`         | `compras_relatorio_a4`         | Relatório A4 por fornecedor (página dedicada; link direto, mobile) |
| `/compras/relatorio-planilha-categoria/` | `compras_relatorio_planilha_categoria` | Lista por **categoria** · impressão A4 ou A6; sem coluna GM; «Contagem» e «Pedir» em branco |
| `/compras/relatorio-planilha-unidade/`   | `compras_relatorio_planilha_unidade` | Lista por **unidade** · mesmo layout e opções de papel que a planilha por categoria |
| `/entrada-nota/`                 | `entrada_nota`                 |                                               |
| `/lancamentos/`                  | `lancamentos_financeiros`      | Contas a pagar/receber                        |
| `/financeiro/resumo-gerencial/`  | `resumo_financeiro_gerencial`  |                                               |
| `/lancamentos/dre/`              | `lancamentos_dre`              |                                               |
| `/lancamentos/contas-pagar/`     | `lancamentos_contas_pagar`     |                                               |
| `/lancamentos/novo-manual/`      | `lancamentos_manual`           |                                               |
| `/lancamentos/fluxo-calendario/` | `lancamentos_fluxo_calendario` |                                               |
| `/estoque/sincronizacao/`        | `estoque_sincronizacao`        | Saúde leitura Mongo + divergência camada Agro |
| `/pdv/checkout/`                 | `pdv_checkout`                 |                                               |
| `/vendas/`                       | `vendas_lista`                 |                                               |
| `/venda/<pk>/`                   | `venda_agro_detalhe`           |                                               |
| `/clientes/` …                   | `clientes_`*, `cliente_*`      |                                               |
| `/rh/`                           | `rh_painel`                    |                                               |
| `/caixa/` …                      | `caixa_*`                      | Painel, saída, abrir, fechar                  |


**APIs (amostra; lista completa no arquivo):** `api/buscar/`, `api/lancamentos/`, export CSV/XLSX/PDF financeiro, `api/pdv/`*, `api/entrada-nota/*`, `api/ajustar/`, etc.

---

## 4. Partials e UI compartilhada (templates)

- `**produtos/templates/produtos/_agro_consulta_ui.html`** — tipografia/densidade GM Agro; inclui `**_agro_open_external.html`**.
- `**_agro_open_external.html`** — `agroAbrirUrlExterna`, uso de `window.agroShell.openExternal` no Electron; monkey-patch de `window.open` para WhatsApp/Maps/Waze/goo.gl.
- `**_head_perf_mpa.html`** — performance MPA onde usado.
- `**_gm_loading_bar.html`** — barra de loading em algumas telas.
- `**produtos/templates/produtos/includes/lancamentos_help_agents.html**` — textos longos de ajuda em Lançamentos (filtros / busca na lista), espelhados na **§10**; na tela ficam em bloco **«?»** (`<details>`), no padrão do RH (`rh_help_agents.html`).

---

## 5. Padrão visual e UX (loja)

- Telas **14" / 17" / 19"**: layout **compacto**, legível, sem depender de resolução alta. **APROVEITAR BEM A TELA,POR SER PEQUENA E SE TRATAR DE IDOSOS QUE IRÃO USAR É BEM DIFICIL DE ENXENGAR NATURAMENTE JÁ, ENTÃO PRECISA SER BEM APROVEITADA PARA MELHOR LEITURA.**
- **Contraste e legibilidade** (incl. idosos): fonte clara e GRANDE , botões grandes, ações previsíveis, sem sustos visuais.
- **Teclado e scanner** primeiro; mouse como apoio; **mínimo de cliques**.
- Tela **limpa**; textos longos só em tooltip, modal “Ajuda” ou “?”.
- Paleta **emerald / orange / slate**, cards simples, hierarquia forte.
- **Busca** onde existir: instantânea local + sync em background quando aplicável.
- **Barra de estoque** (PDV): atualização manual + horário da última atualização + automático em standby quando a tela tiver isso.
- **“Voltar ao PDV (F1)”** visível nas telas possíveis.
- Ao voltar ao PDV: **preservar contexto** (draft, filtros, lista, scroll).
- **Home administrativa (launcher):** sem **rolagem da página** (`overflow: hidden` no viewport, `100dvh` / `min-h-0` com flex); **tipografia e espaçamentos fluidos** (`clamp` com `vw` + `rem` na shell) e **colunas da grade por largura útil** (`container-type: inline-size` + `@container`), para o layout **acompanhar o zoom** sem empilhar cards; em **zoom muito alto**, se necessário há **rolagem só na área da grade** (sem sobreposição).

---

## 6. Como pedir alterações ao assistente

- **Modo econômico:** respostas curtas; só o necessário.
- **Escopo:** preferir **apenas os arquivos combinados**; se precisar alargar, **pedir autorização**.
- **Antes de editar:** **uma linha** com o plano.
- **Entrega:** um **patch coeso** por tarefa quando fizer sentido.

---

## 7. Decisões e implementações já registradas (changelog resumido)

**Compras (`compras.html` + JS inline)**  

- Sugestão de compra em destaque no **card**; horizonte em dias **independente** do período da média (média em `<details>` “Métricas avançadas”).  
- Opção **descontar ou não** estoque total **C+V** (localStorage).  
- F5 / textos: **atualizar métricas**; não confundir com horizonte da sugestão.
- **Relatório A4:** também em **`/compras/relatorio-a4/`** (`compras_relatorio_a4`) — mesma lógica do modal na tela Compras; layout pensado para **celular** (área segura, toques grandes, lista rolável). Na Compras entra pelo menu **«Folha Compras»** → **«Página só fornecedor»**.
- **Planilha impressa por categoria / unidade:** **`/compras/relatorio-planilha-categoria/`** e **`/compras/relatorio-planilha-unidade/`** — escolha A4 ou A6; lista sem coluna código (GM); colunas **Contagem** e **Pedir** vazias; **corpo da tabela** em tipografia grande (nome da 1ª coluna ~+20 % sobre a base anterior; demais colunas ~+35 % para leitura física). Quantidades «últ. pedido / compra» e «vendida desde última» calculadas por produto com **último evento de compra** no Mongo ERP (`_ultimas_compras_por_produto_ids`) + **DtoVenda** depois dessa data. Cabeçalho de impressão só com filtro e data gerada (legenda no rodapé). **Categoria e unidade: só seleção na lista** (sem digitar); `GET api/compras/relatorio-dim/?tipo=…&completa=1` monta options (scan Mongo maior + overlay). Filtro por **unidade** considera Mongo (`Unidade`, `SiglaUnidade`, campos de estoque) **e** a unidade salva no **overlay** PostgreSQL (como a coluna UNIDADE da gestão). APIs `api/compras/relatorio-dim/`, `api/compras/relatorio-categoria/`, `api/compras/relatorio-unidade/`. Na tela Compras: menu **«Folha Compras»** no cabeçalho agrupa fornecedor (modal + página dedicada), categoria e unidade; **painel ancorado à esquerda** do botão, **rótulos dos itens alinhados à esquerda** (leitura contínua, telas compactas).

**Entrada NF — pré-visualização XML (`entrada_nota.html`, botão Ler XML)**  

- Após o parse: **modal grande** em duas colunas (grade vs XML), com sugestão automática de vínculo (**EAN** / **código**), **arrastar e soltar** para casar, **descartar** por linha, **zona de lixo** (soltar o cartão) e **descartar todos os não vinculados** (com confirmação). Texto longo no bloco **Ajuda** (`<details>`), alinhado à UX do projeto.
- A **grade não muda** até **Confirmar na grade**; **clique fora do painel não fecha** o modal (evita fecho acidental). **Cancelar**, **Fechar** e **Esc** pedem confirmação antes de sair.
- Ao confirmar: aplica cabeçalho da NF, limpa a grade e monta linhas na ordem da nota (XML fundido com a grade quando há vínculo; em seguida linhas só da grade que não foram descartadas). Atualiza `ultimaNotaParse`. No fluxo de só pré-visualizar/parse **não** zera rascunho em edição, **não** limpa `entradaNfeExtraUltimo` e **não** reseta depósito (comportamento anterior fazia isso já ao ler o arquivo).

**Gestão / cadastro de produtos — lentidão após entrada NF (investigação aberta)**  

- **Sintoma:** primeira abertura aceitável; depois de **entrada de nota** e voltar à **gestão de produtos**, carga muito longa (minutos em casos extremos). Usuário reportou persistência após otimizações iniciais.  
- **Duas telas:** (1) **SisVale cadastro** `produtos_cadastro_erp.html` + `cadastro_erp_panel.js` → API **`api_produtos_cadastro`** em `produtos/views.py` (lista/busca Mongo **sem** saldo). (2) **Gestão operacional** `produtos_gestao.html` → **`api_produtos_gestao_lista`** (+ **`api_produtos_gestao_facetas`** no load). Não misturar as rotas ao depurar.  
- **Já feito (código):** projeção slim em **`api_produtos_gestao_lista`** e em **`motor_de_busca_agro`** quando usado pela gestão; **`_CADASTRO_LISTA_MONGO_PROJ`** em **`api_produtos_cadastro`** (find + motor); após propagar preços da NF, fila «pendentes ERP» em **batch** (`_erp_produto_pendentes_extend_batch`); no painel ERP, **lista + badge pendentes** em paralelo (`cadastro_erp_panel.js`).  
- **Se ainda estiver lento, próximo passo:** medir no browser/rede **qual URL** trava (`api_produtos_cadastro`, `api_produtos_gestao_facetas`, `api_produtos_gestao_erp_pendentes`, etc.); revisar **`api_produtos_gestao_facetas`** (vários `distinct` no Mongo); **`explain`** / índices em **`Nome`**, **`CadastroInativo`**, campos de **sort** do cadastro; pool **`obter_conexao_mongo`**; cache Redis.

**PDF financeiro (`produtos/lancamentos_financeiro_pdf.py`)**  

- Removidos blocos “OBSERVAÇÃO” / “ENTROU ALGUM DINHEIRO…”.  
- Sem coluna Observações; **QUAL CONTA** em branco; **Plano conta** sem grupo; coluna **Forma de pagamento**.  
- **Valor bruto** em fonte maior; quitação parcial → **bruto + linha Saldo**.  
- Tabela “Anotações e conferência” mais larga.

**Electron**  

- Build usa `**electron/main.js`** + `**electron/preload.js`**.  
- IPC `agro-open-external` → `shell.openExternal`; mesma origem do app pode abrir janela interna; demais http(s) e `whatsapp:` no SO.  
- Raiz: `electron-main.js` + `preload.js` alinhados para dev alternativo.

**Lançamentos — ordenação por coluna**  

- Backend hoje: principalmente `vencimento_asc` / `vencimento_desc` / `fluxo_desc` em `mongo_financeiro_util.py`. Ordenar **todas** as colunas no servidor exige estender o aggregate; sort só no cliente **não** substitui paginação global.

**Lançamentos — busca na lista (filtros)**  

- Texto longo da busca no modal Filtros em **«?»** (`<details>`); canônico em `produtos/templates/produtos/includes/lancamentos_help_agents.html`, espelho em **AGENTS.md §10**. Busca por **valor** (vírgula decimal / R$) alinhada a bruto, pago e saldo em aberto no Mongo (`mongo_financeiro_util.py`).

**Estoque — Agro como operação (espelho ERP + ajustes)**  

- Saldo operacional no PDV: referência ERP (Mongo) + correções em `AjusteRapidoEstoque` (`origem`, `usuario`, `observacao`). Painel `/estoque/sincronizacao/`, APIs `/api/estoque/sync-health/` e `/api/estoque/divergencia-ajustes/`, ping automático leve (`manage.py estoque_mongo_ping` no Cron Render ou HTTP `/api/cron/estoque-mongo-ping/`), comando `reconciliar_estoque_agro`. Doc interna: `docs/ESTOQUE_AGRO_FONTE_DA_VERDADE.md`.

**RH — fechamento e ficha (`rh_help_agents.html` + §9)**  

- Textos explicativos longos em **«?»** (`<details>`); conteúdo canônico espelhado em **AGENTS.md §9** e no include `rh/templates/rh/includes/rh_help_agents.html`.  
- Tela de fechamento: passos **1–3**, legenda e itens da folha em **guias recolhíveis** (`<details>`).

**RH — cancelar vale na ficha**  

- Botão **Cancelar** no extrato (`rh_funcionario_vale_cancelar`): `cancelado=True`, recalcula folhas abertas, tenta `sincronizar_valores_titulo_salario_mongo` na competência do vale. Alternativa: **Admin** Django em *Vales / adiantamentos*.

**Empréstimo interno (sócio) — pagamento em contas a pagar**  

- Cadastro do aporte continua só em `AgroEmprestimo`. Cada pagamento em **Consulta → Gerenciar** (`registrar_pagamento_emprestimo_interno_agro`) gera `DtoLancamento` quitado (plano dívida padrão, marca `EMP-INT` nas observações). Exclusão do pagamento remove o título vinculado. Na lista de Lançamentos use situação **Quitados** ou **Todos** (não aparece em **Em aberto**).

---

## 8. Manutenção deste arquivo

- Atualizar **§7** quando houver decisão de produto relevante.
- Atualizar **§3** se `produtos/urls.py` ganhar rotas importantes (ou referenciar “ver arquivo”).
- Atualizar **§9** quando mudar textos de ajuda do RH em tela (manter alinhado a `rh/templates/rh/includes/rh_help_agents.html`).
- Atualizar **§10** quando mudar textos de ajuda de **Lançamentos** em tela (manter alinhado a `produtos/templates/produtos/includes/lancamentos_help_agents.html`).
- Evitar duplicar **cada** template aqui — manter mapa enxuto.

---

## 9. Ajuda em tela — RH (fonte para o `?`)

Textos longos nas telas **Fechamento de folha** e **Ficha do funcionário** ficam em blocos **«?»** (elemento `<details>`). O HTML vivo está em `**rh/templates/rh/includes/rh_help_agents.html`**; esta seção é o espelho em Markdown para humanos e para `@AGENTS.md`.

### 9.0 Salário R$ 0 no fechamento

- Sem histórico salarial na ficha: cadastrar a primeira faixa na ficha (seção salário).
- Com histórico: conferir se a **vigência** cobre o **último dia do mês** da competência; corrigir datas se a faixa tiver terminado antes.

### 9.1 Legenda dos cartões (fechamento)

- Os quatro cartões mostram só a **folha** da competência (mês do título, até o último dia desse mês).
- O passo **2** (bloco verde) é **opcional**: usar só se quiser lançar o salário nas **contas a pagar** do financeiro (Mongo), com vencimento.

### 9.2 Itens da folha

- Lista = **composição da folha** (salário base, vales do mês, descontos/proventos do passo 1, etc.).
- Se vazia: no passo 1 (folha aberta), usar **Salvar e recalcular**.

### 9.3 Passo 1 — Ajustes da folha

- Campos para descontos extras, proventos, observações e ajustes de status / valor pago de controle.
- Após alterar: **Salvar e recalcular** para atualizar totais e itens.

### 9.4 Passo 2 — Conta a pagar no financeiro

- Opcional: faz o salário da competência aparecer nas **contas a pagar** (Mongo) com **data de vencimento** em Lançamentos.
- Cada **vale** (ficha ou caixa) é **pagamento parcial** do mesmo título; não cria outra despesa de “vale”.
- **Forma de pagamento** pode ficar em **branco** até quitar; **conta/banco** é obrigatória para gerar o título.
- Conta placeholder tipo **«ADICIONAR BANCO»** aparece no **topo** da lista quando configurada (`AGRO_FINANCEIRO_BANCO_PLACEHOLDER_ID` no `.env` se o ID do ERP for diferente do padrão embutido).

### 9.5 Passo 2 — Ajuda técnica (plano Mongo)

- Plano do título: variável de ambiente `**AGRO_RH_PLANO_SALARIO_FOLHA`** (texto igual ao cadastro no ERP/Mongo).
- Quando já existe lançamento, o **ID** do título é exibido na própria tela (ajuda técnica).

### 9.6 Passo 3 — Encerrar e correções

- **Fechar** e **Marcar pago** são só **controle interno** do RH; não substituem pagamento no banco nem baixa no ERP.
- **Reabrir folha** volta para Aberto e zera valor pago de controle se estava Pago.
- **Excluir competência** só sem título de salário vinculado no financeiro.

### 9.7 Ficha — onde estão salário e vales

- Formulários nas **faixas numeradas** abaixo dos cartões; **tocar na faixa** para expandir. Atalhos rolam e abrem a seção.

### 9.8 Ficha — histórico salarial

- Nova faixa **encerra** a vigência anterior na data informada e abre a nova; histórico anterior **não** é apagado.

### 9.9 Ficha — vales e financeiro

- Com **financeiro** marcado: precisa existir **título de salário** no **fechamento** daquele mês; vale = **baixa parcial** no Mongo.
- Sem financeiro: vale só no RH.
- Formas/contas como na **saída de caixa**.
- **Cancelar vale** (coluna Ações no extrato): exige **motivo** (mín. 3 caracteres); marca o vale como cancelado (não apaga a linha). Folhas **abertas** são recalculadas; se existir título Mongo na competência do vale, roda **sincronizar** para realinhar **ValorPago**. Conferir no financeiro se aparecer aviso de falha.

### 9.10 Ficha — lista de fechamentos vazia

- Competência costuma ser criada ao lançar **vale** (ficha ou caixa), ou pelo botão **Abrir folha do mês atual**.
- No fechamento: **vencimento** e título no financeiro no passo 2, se desejado.

---

## 10. Ajuda em tela — Lançamentos / filtros (fonte para o `?`)

Texto longo da **busca na lista** (modal Filtros em Contas a pagar / receber) fica no bloco **«?»** (`<details>`). O HTML canônico está em `**produtos/templates/produtos/includes/lancamentos_help_agents.html**` (slug `filtros_busca_lista`); esta seção espelha o conteúdo para humanos e para `@AGENTS.md`.

### 10.0 Busca na lista

- Separe termos com **espaço**; cada termo pode cair em favorecido, descrição, documento, plano, grupo, forma, banco, empresa, centro de custo, observações, IDs ou **valor** (bruto, pago ou saldo em aberto; use vírgula decimal ou R$).

---

*Última revisão estrutural: documento inicial + mapa de rotas a partir de `produtos/urls.py` e `config/urls.py`; §9 ajuda RH; §10 ajuda Lançamentos (filtros).*