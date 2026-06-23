# BANANA — contexto SisVale (anexe com `@banana`)

**Este é o único anexo obrigatório** no dia a dia. O `AGENTS.md` é enciclopédia (referência profunda); o Cursor já carrega um resumo via `.cursor/rules/agro-consulta.mdc`.


| Você quer…                            | Faça                                                  |
| ------------------------------------- | ----------------------------------------------------- |
| Começar qualquer tarefa               | Anexe `@banana` + descreva a tarefa                   |
| Detalhe fino de UX / RH / Lançamentos | Some `@AGENTS.md` (§5–11, §9, §10)                    |
| Registrar onde paramos                | Automático no fim da sessão; ou *"atualize a banana"* |
| Decisão permanente no changelog       | *"atualize o AGENTS"* (raro; só quando você pedir)    |


**Assistente:** não perguntar ao Renan se deve atualizar o `AGENTS.md`. WIP, roadmap e checkpoint vão no `banana.md` **sem pedir** quando for pertinente (ver CHECKPOINT).

**Comunicação com o Renan:** **sempre em português (BR).** Respostas **curtas e em linguagem de loja** — só o que for **estritamente importante** para decidir ou operar. **Evitar** nomes de arquivo, flag, API e detalhe de código **salvo se ele pedir** ou for indispensável numa instrução (ex.: uma linha no `.env`).

**Produção (regra dura — 2026-06-22):** **Nunca** `git push origin producao`, merge `teste`→`producao`, cherry-pick na loja ou deploy Render de produção **sem** o Renan escrever explicitamente *«pode subir (para produção)»* / *«pode ir para produção»* (ou equivalente claro). **Ordem:** commit + push em `**teste`** → Renan testa no Render **teste** → **só então** produção, se ele pedir. Corrigir bug ≠ autorização para loja. Loja operando = **zero** push produção por iniciativa do assistente.

**Produção — chat canônico (2026-06-22, Renan):** Push na loja (**SistVale** / branch `producao`) **só neste chat** daqui pra frente. Renan pode ter subido produção em **outro chat** ou **post** — o `banana.md` e o CHECKPOINT são a **fonte da verdade**; ao abrir este chat, o assistente **relê o CHECKPOINT** e **pergunta** se algo mudou antes de cherry-pick. **Não** assumir que «último commit deste chat» = produção.

**Teste (regra — 2026-06-22):** Renan **não testa localmente** (parou de usar — local parecia OK e produção quebrava). **Sempre** valida no **Render projeto «teste»** (branch Git `teste`). Quando ele fala *«teste»*, *«staging»* ou *«homologação»*, é **sempre** esse site no Render — **não** máquina local. **Assistente pode** commit + push em `teste` **automaticamente** (deploy Render segue sozinho); **não precisa pedir autorização** para subir no teste. Produção continua só com frase explícita acima.

**Registro no banana (regra — 2026-06-22):** **Toda alteração** que mude o sistema (fix, feature, deploy teste ou produção) → **registrar no `banana.md`** ao fechar a tarefa (CHECKPOINT: o quê mudou, commits, versão `VERSION`, teste OK ou pendente). Serve para **contexto do próximo chat**, **diagnosticar problema** e **saber o que reverter**. Assistente **não pergunta** se deve registrar — faz sempre que entregar código ou deploy. Detalhe passageiro ou chat só explicativo: não inflar o doc.

---

## 0. TL;DR (leia em 1 minuto)


| Item                         | Valor                                                                                                                                                                |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Produto**                  | **SisVale** / **Agro Consulta** — sistema web da **GM Agro** (loja agropecuária, Jacupiranga-SP)                                                                     |
| **Usuários**                 | Operadores de loja (PDV, caixa), gestão, financeiro, RH, compras                                                                                                     |
| **Stack**                    | Django + Postgres (Agro) + Mongo (espelho ERP) + Render + Electron opcional                                                                                          |
| **Branch dia a dia**         | `**teste`** (= staging Render; ver §3) · `**producao**` = loja · merge só quando Renan pedir                                                                         |
| **Tela inicial**             | `/` = BI gerencial · PDV principal em `/consulta/` e wizard `/pdv/checkout/`                                                                                         |
| **Regra de ouro**            | Operador usa **saldo do Agro**; ERP alimenta Mongo; Agro não devolve estoque ao ERP                                                                                  |
| **UX loja**                  | Compacto, alto contraste, teclado/scanner primeiro, paleta emerald/orange/slate                                                                                      |
| **Escala de tela**           | **Agro Display Scale** global (não zoom do Chrome) — ver AGENTS.md §11                                                                                               |
| **Cliente Renan (loja/dev)** | **Google Chrome** — navegação página a página (MPA). **Não** usar Electron no dia a dia (testou; **muito lento**). Assistente: **não perguntar** Chrome vs Electron. |


---

## 1. O negócio (não técnico)

**GM Agro** opera uma loja física. O SisVale substitui/complementa telas do ERP legado com foco em:

- **Balcão:** vender rápido (busca, scanner, formas de pagamento, entrega, cupom).
- **Estoque:** ver saldo confiável, ajustar, transferir, entrada de NF.
- **Gestão:** cadastro de produtos, preços, compras, validade.
- **Financeiro:** contas a pagar/receber espelhando Mongo do ERP + lançamentos manuais Agro.
- **Caixa:** turnos (gaveta / notebook / teste), sangria, fechamento.
- **RH:** folha, vales, ficha do funcionário.
- **Fiscal:** NFC-e modelo 65 emitida **pelo Agro** (série própria, separada do ERP).

**Operadores:** muitos são idosos — botões grandes, poucos cliques, sem textos longos na tela (ajuda em «?» ou modal).

**Renan (dono/dev):** testa **só no Render «teste»** (não local). Assistente: **commit + push automático em `teste`**; `**producao` só com «pode subir (produção)»** (ver topo).

**Como acessa o SisVale:** **Chrome** (aba normal ou instalado). **Electron** foi testado e **descartado na loja** — performance ruim. UX e perf (Lançamentos, BI, prefetch, bootstrap HTML) devem ser validados **no Chrome**, não no shell Electron/iframe.

---

## 2. Arquitetura técnica

```
[Navegador / Electron]
        ↓ HTTP
[Django config/] ──→ produtos/ (maioria das telas e APIs)
        │              ├── Postgres: vendas, clientes, overlay produto, caixa, NFC-e XML…
        │              └── Mongo (leitura + regras): catálogo ERP, financeiro, estoque espelho
        ├── estoque/ (APIs em /estoque/)
        ├── financeiro/ (/api/financeiro/)
        ├── integracoes/ (ponte ERP, venda ERP Mongo)
        └── rh/
[Render] Gunicorn · health /healthz
```

### 2.1 Duas bases de dados — papéis


| Base              | O quê fica aqui                                                                                                      |
| ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Mongo**         | Catálogo ERP (`DtoProduto`), estoque depósito, financeiro (`DtoLancamento`), vendas ERP históricas                   |
| **Postgres Agro** | `VendaAgro`, `ClienteAgro`, overlay cadastro (`ProdutoOverlayAgro`), caixa, `NfceDocumentoAgro`, ajustes estoque, RH |


**Overlay Agro:** camada PostgreSQL que sobrescreve/complementa campos do produto sem gravar de volta no ERP (preço loja, código NFe, flags).

### 2.2 Staging vs produção (Mongo compartilhado)

Staging **lê** o mesmo Mongo da loja mas **não pode escrever** nele:

- `AGRO_STAGING_READONLY=true`
- `AGRO_ERP_PEDIDOS_DRY_RUN=true`
- `DATABASE_URL` = Postgres **só do staging**

Detalhes: `docs/DEPLOY-AMBIENTES.md`.

---

## 3. Deploy e Git

**Renan — duas vertentes no Render (dois «sites»):**


| Como o Renan fala                 | Branch Git | Projeto Render (Dashboard) | Papel                                |
| --------------------------------- | ---------- | -------------------------- | ------------------------------------ |
| **Teste / staging / homologação** | `teste`    | **teste**                  | Onde **sempre** valida antes da loja |
| **Produção / loja / SistVale**    | `producao` | **SistVale**               | Loja de verdade                      |


**Não testa local** — o ambiente de prova é o Render **teste**. Deploy do `teste` é **automático** no Render após push.


| Ambiente (como falar) | Branch Git | Render (serviço)                               |
| --------------------- | ---------- | ---------------------------------------------- |
| **Teste / staging**   | `teste`    | projeto **teste** (ex.: agro-consulta-staging) |
| **Produção / loja**   | `producao` | projeto **SistVale** (Sistvale - Produção)     |


- `**main` / `principal` não entram no deploy.**
- **Assistente:** push `**teste` livre** (Renan testa no site teste). Merge/push `**producao` só** quando Renan autorizar.
- Após merge produção: `python manage.py migrate` no ambiente (Render faz no deploy).

### 3.1 Versão do sistema (contador único — 2026-06-22)


| O quê                    | Detalhe                                                                                                                                               |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Arquivo**              | `VERSION` na raiz (ex.: `1.92`)                                                                                                                       |
| **Badge BI**             | `v` + conteúdo de `VERSION`                                                                                                                           |
| **Contador**             | **Um só** — o número significa a **versão do pacote**, não o branch                                                                                   |
| **Branch `teste`**       | Hook sobe **+0,01** a cada commit (`1.92` → `1.93`)                                                                                                   |
| **Branch `producao`**    | No deploy: `**VERSION` = versão do pacote teste** que subiu (ex. deploy do `e98db0f` → loja fica `**1.92`**)                                          |
| **Cherry-pick em lote**  | Commits intermediários: `SKIP_VERSION_BUMP=1 git cherry-pick …` · **só o último** (ou commit `docs` pós-deploy) ajusta `VERSION` para bater com teste |
| **Erro passado**         | Pacote teste v1.92 virou prod v1.57–v1.59 (hook bumpou 3×) — **corrigido** para **1.92**                                                              |
| **Paridade**             | `teste` ≥ `producao` em número. Teste à frente = pacotes ainda não pedidos para loja                                                                  |
| **Como**                 | Hook `.githooks/pre-commit` → `scripts/bump_version.py --hook`                                                                                        |
| **Setup (1× por clone)** | `powershell scripts/setup_git_hooks.ps1`                                                                                                              |
| **Pular 1 commit**       | `SKIP_VERSION_BUMP=1 git commit …`                                                                                                                    |
| **Conferir diff real**   | `git diff origin/producao origin/teste --stat` (backend core costuma estar **igual**)                                                                 |


**Regra prática:** ao fechar deploy produção, CHECKPOINT registra `**teste` vX.XX → produção vX.XX** (mesmo número).

---

## 4. Módulos — mapa rápido

Cada bloco: **o que é · rotas · arquivos-chave · armadilhas**.

### 4.1 Home / BI (`/`)

- Dashboard gerencial SisVale BI; atalhos clássicos em `/atalhos/`.
- Versão do commit no Render (não hardcoded).
- Card **Validade** destaca vermelho se produto vencido.
- Gastos por plano de conta: oculto por padrão (`AGRO_DASHBOARD_GASTOS_PLANO=true` no `.env`).
- Template: `produtos/templates/produtos/dashboard_gerencial.html`.

### 4.2 PDV — ponto de venda

**Duas interfaces:**


| Tela                  | URL              | JS principal                    |
| --------------------- | ---------------- | ------------------------------- |
| PDV legado MPA        | `/consulta/`     | `consulta_produtos.js`          |
| PDV wizard (checkout) | `/pdv/checkout/` | `pdv_wizard.js`, `pdv_state.js` |


**Fluxo típico:** busca produto → carrinho → cliente (opcional) → pagamento → confirma → cupom/impressão.

**Regras UX já decididas:**

- **F1** volta ao PDV preservando draft/filtros/scroll.
- **Botão flutuante PDV** (2026-06-19): canto **inferior esquerdo**, tamanho médio, em quase todas as telas via `_agro_open_external.html` → `_agro_pdv_fab.html`. Destino `**/pdv/`** (wizard). Oculto no próprio PDV (`/pdv/`, `/consulta/`). **F1** global fora de campos. Sobe sutilmente se barra fixa embaixo encostar. No BI mantém PDV do topo + flutuante. **Visual (2026-06):** borda arco-íris + pulso + piscar (só CSS); para no hover; `prefers-reduced-motion` desliga animação. **Sobreposição (2026-06):** z-index **90** (abaixo de modais z 200+); **some** com modal/dialog aberto (Nova saída, `.sv-modal.show`, `role=dialog`); se encostar em botão no canto, sobe ou vai pro canto **direito**.
- **Perf. animações (decisão Renan, 2026-06):** acúmulo de efeitos no app inteiro *pode* pesar em PC fraco — mas **este FAB é impacto baixo** (1 elemento, CSS `transform`/`opacity`, sem JS extra nem rede). O que pesa mesmo: MPA página inteira, listas grandes, Mongo, JS do PDV/Lançamentos. Regra: poucos destaques globais (FAB, Validade vermelha); evitar animar tabelas/cards em massa.
- **Interruptor efeitos (2026-06-19):** botão minúsculo **«FX on / FX off»** acima do FAB PDV (`localStorage` `agro_reduzir_efeitos_v1`). **FX off** → classe `html.agro-fx-reduced`: desliga arco-íris/pulso do FAB, pulso do card **Validade** vencida, pulso decorativo PDV/Orçamento no BI. **Não** desliga: barra de loading, feedback de scanner, spinners de «salvando» (úteis). API JS: `agroSetFxReduced(true|false)`, `agroFxReduced()`.
- Entrega: fluxo inline na etapa (sem modais empilhados).
- Endereço de entrega oculto até concluir pagamento da entrega.
- Barra de estoque: atualização manual + horário + standby.

**APIs PDV (amostra):** `api/buscar/`, `api/pdv/*`, `api/promocoes/ativas-pdv/`, Mercado Pago Point em `views_mp_point.py`.

**Armadilha GM no barras (2026-06-18):** se «Código de barras» no cadastro tiver texto **GM** (ex. `GM1546-5S`), o leitor manda GM, não EAN. No **wizard** (`pdv_wizard.js`), o hífen do GM disparava atalho `**-`** = remover último item do carrinho (campo mostrava `GM15465S`). Patch: ignorar `-`/`+` durante SKU/GM + modo barcode para `GM…`. Legado `/consulta/`: F4 pós-bip + match alnum (`consulta_produtos.js`).

**Venda gravada em:** `VendaAgro` (Postgres) + tentativa sync ERP conforme config.

### 4.3 NFC-e (cupom fiscal 65)

**Emissão pelo Agro**, não pelo ERP. Série **21** no Agro; ERP continua série **20**.


| Modo `NFC_E_MODO` | Comportamento                                                                                                                                                                                                 |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `manual` (padrão) | **PIX / cartão** → NFC-e automática ao confirmar (sem popup CPF; sem identificação se cliente sem CPF). **Dinheiro / fiado / vale** → popup **NFC ou Venda**; se NFC → modal CPF grande ou «Sem CPF na nota». |
| `auto`            | NFC-e em toda venda                                                                                                                                                                                           |


**UX PDV (2026-06-18):** modal CPF **grande** (`max-w ~54rem`, fontes `clamp`). Reemissão em `/vendas/` → após autorizar pergunta **Imprimir cupom / Agora não**. Aviso pós-venda NFC-e falhou: toast **no topo**, depois da janela de impressão Windows.

**SEFAZ:** rejeição **225** corrigida com grupo `<card><tpIntegra>2</tpIntegra></card>` em PIX/cartão (NT 2024.003) + `vTotTrib` por item.

**Arquivos centrais:**


| Arquivo                            | Papel                                            |
| ---------------------------------- | ------------------------------------------------ |
| `produtos/nfce_config_util.py`     | Env vars, resumo config                          |
| `produtos/nfce_sp_emissao_util.py` | XML, SOAP SEFAZ SP, assinatura, QR Code          |
| `produtos/nfce_cupom_util.py`      | Cupom térmico 80 mm                              |
| `produtos/nfce_ibpt_util.py`       | Tributos Lei 12.741                              |
| `produtos/nfce_venda_util.py`      | Painel status por venda                          |
| `produtos/views_nfce.py`           | Rotas API e contabilidade                        |
| `produtos/models.py`               | `NfceDocumentoAgro`, `VendaAgro.nfce_solicitada` |


**Modelos:** `NfceDocumentoAgro` (1:1 com venda, guarda XML autorizado). Campo `nfce_solicitada` na venda = operador pediu cupom mesmo em forma manual.

**Rotas úteis:**

- `GET /api/nfce/status/`
- `POST /venda/<pk>/nfce/emitir/`
- `GET /venda/<pk>/nfce/cupom/`
- `GET /api/nfce/export-xml/?ano=&mes=` (ZIP contabilidade)
- `/contabilidade/` painel

**Produção GM Agro:** CNPJ `48900774000103`, município `3524600` Jacupiranga, `NFC_E_TP_AMB=1`. Checklist completo: `docs/NFCE-PRODUCAO.md`.

**Histórico de dor SEFAZ (já resolvido no código):** QR no XML antes da assinatura, SHA1 QR v2 SP, retry número duplicado 539/204, CA bundle ICP-Brasil no Render, SOAP 4.00 sem `nfeCabecMsg` errado.

### 4.4 Vendas

- Lista: `/vendas/` · detalhe: `/venda/<pk>/`
- Reimpressão cupom simples e fiscal (se NFC-e autorizada).
- Devolução, reenvio ERP, reversão ERP — APIs em `views.py`.
- Export CSV: `/vendas/exportar-csv/`.

### 4.5 Clientes

- Cadastro local: `ClienteAgro` (Postgres).
- Sync ERP/Mongo → Agro: `produtos/services_clientes_sync.py`, botão na lista, comando `sincronizar_clientes_agro`.
- `**editado_local=True` não é sobrescrito** na sync.
- PDV lista/busca clientes **só no Agro** (`api/listar-clientes/`, `api/buscar-clientes/`).
- IDs Mongo no JSON viram `local:{pk}` para não mandar ObjectId ao ERP.
- Contexto antigo detalhado: `docs/CONTEXTO_SESSAO_CLIENTES_PDV.md`.

### 4.6 Cadastro / gestão de produtos

**Duas telas — não confundir:**


| Tela                       | URL / API                                           | Uso                                                  |
| -------------------------- | --------------------------------------------------- | ---------------------------------------------------- |
| **Cadastro ERP** (SisVale) | `/produtos/cadastro-erp/`, `api_produtos_cadastro`  | Lista/busca Mongo **sem saldo**; Excel import/export |
| **Gestão operacional**     | `produtos_gestao.html`, `api_produtos_gestao_lista` | Saldo, facetas, operação loja                        |


**Excel fase 1:** export com colunas/categorias; import async com histórico e desfazer; ID oculta; Código GM editável; célula vazia não altera.

**Produto novo — código sistema + GM (decisão 2026-06-22, Renan OK no teste):**


| Campo              | Regra                                                                                            |
| ------------------ | ------------------------------------------------------------------------------------------------ |
| **Código sistema** | Exatamente **4 números**; sequência **4010–9999**; operador pode editar                          |
| **Sequência**      | Próximo livre após o **maior código sistema** já usado (só campo numérico — **GM não conta**)    |
| **Código GM**      | Sugestão automática `**GM` + código sistema**; operador **livre para editar** (sem formato fixo) |
| **Modal novo**     | Carregar códigos **sem repintar** o modal (não apagar nome/campos já digitados)                  |


Env opcional: `AGRO_NOVO_PRODUTO_COD_MIN` (piso da sequência; padrão **4010**).

**Lentidão pós-entrada NF (investigação aberta):** medir qual URL trava no browser; suspeitos: `api_produtos_gestao_facetas`, pool Mongo.

### 4.7 Entrada de nota fiscal

- `/entrada-nota/` — wizard 8 passos (fornecedor → … → financeiro → finalizar PIN).
- Pré-visualização XML: modal drag-and-drop, não fecha ao clicar fora; «Confirmar na grade» aplica de fato.
- **Financeiro desync (2026-06-19):** título já em Contas a pagar mas etapa 7 «Falta a pagar» + «Falha ao salvar» — rascunho sem `financeiro_lancado`. Fix local: sync ao abrir nota + «Salvar + a pagar» idempotente (`sincronizar_financeiro_rascunho_entrada_nfe`). **Workaround até deploy:** F5 na nota ou ir etapa 8 (título já existe).

### 4.8 Estoque Agro

- Saldo PDV = Mongo ERP + `AjusteRapidoEstoque`.
- Painel: `/estoque/sincronizacao/`.
- Doc: `docs/ESTOQUE_AGRO_FONTE_DA_VERDADE.md`.
- Cron: `estoque_mongo_ping` a cada 10 min no Render.

### 4.9 Compras

- `/compras/` — sugestão, horizonte em dias, métricas avançadas em `<details>`.
- Relatórios: A4 fornecedor, planilhas impressas por categoria/unidade (A4 ou A6).

### 4.10 Lançamentos / financeiro

- `/lancamentos/` — redirect → **Contas a pagar padrão:** `/lancamentos/contas-pagar/` (**layout novo**) · `/classico/` → redirect · `/teste/` → redirect
- Contas a receber: `/lancamentos/contas-receber/` (layout clássico)
- PDF: `lancamentos_financeiro_pdf.py` (sem coluna observações longas; forma pagamento; bruto destacado).
- Busca na lista: termos com espaço; valor em bruto/pago/saldo. Ajuda: `includes/lancamentos_help_agents.html`.
- **Layout novo CP:** `lancamentos_contas_pagar_teste.html` — API `/api/lancamentos/`; filtros na URL; recarga in-place preserva scroll/filtros/páginas.
- **Perf lista (2026-06-19):** projeção slim Mongo; `skip_totais` pág. 2+; cache sessionStorage; planos lazy.
- **Abertura CP — Chrome (2026-06-19, v1.48+):** prefetch BI/F7 · cache do dia · selo **Sincronizando…** · **bootstrap HTML** (lista hoje+abertos já no servidor, sem 2ª ida à API). Renan validou melhora **sutil** — esperado no Chrome MPA.
- **Teto sem refactor grande:** no Chrome cada clique = **página nova** + Mongo no bootstrap. **Roadmap adiado (2026-06-19):** próximo salto = Postgres financeiro **ou** lista no BI — ver CHECKPOINT.
- **Nova saída** (modal) + **Lote manual** (`/lancamentos/novo-manual/`): pseudo-plano **«Empréstimo (entrada + pagamento)»** — gera receita quitada (hoje) + despesa(s); se saída > entrada, diferença em **Juros de Empréstimos**. JS: `lancamento_emprestimo_dual.js`; backend: `expandir_linhas_emprestimo_dual_lote` em `mongo_financeiro_util.py`.

### 4.11 Caixa

- **Gaveta** = turno principal · **Notebook** = vínculo sem sessão própria · **Teste** = isolado (`ponto_caixa=teste`), fora do fechamento em lote.
- Layout **16:9**, shell `.caixa-shell`, `100dvh` — não coluna estreita.
- Util: `produtos/caixa_util.py`.

### 4.12 RH

- `/rh/` — folha, vales, ficha.
- Ajuda longa em `rh/templates/rh/includes/rh_help_agents.html` (espelho AGENTS.md §9).
- Vale com financeiro = baixa parcial no título de salário do mês (precisa folha fechada com título).
- Cancelar vale: motivo mín. 3 chars; recalcula folhas abertas.

### 4.13 Electron (desktop) — opcional; Renan não usa

- `electron/main.js` + `preload.js` — `shell.openExternal` para WhatsApp/Maps.
- Mesmo Agro Display Scale via `localStorage` (não `setZoomFactor` paralelo).
- **Renan:** testou e **não usa** (lento). Loja/dev = **Chrome**. Recursos só do shell (abas laterais `agro-open-inapp-tab`, iframes) **não se aplicam** ao fluxo dele — otimizar para **MPA no Chrome** (bootstrap HTML, prefetch, cache).

### 4.14 UI compartilhada

- `_agro_consulta_ui.html` — base visual.
- `_agro_display_scale.html` + `agro_display_scale.js` — escala global.
- `_agro_open_external.html` — links externos.

### 4.15 Desvinculação ERP (Mongo espelho → Postgres SisVale)

**Dois níveis:** (1) cortar **API ERP** (`Produtos/Salvar`, etc.); (2) parar de **ler/gravar no Mongo** na tela — ganho de **responsividade** e menos bugs (ex.: preço que “voltava”).

**Flags** (`produtos/agro_fonte_config.py`, `.env`): `AGRO_FONTE_CATALOGO=agro_pg` · `AGRO_FONTE_ESTOQUE=ledger` · `AGRO_FONTE_FINANCEIRO=agro_pg` · `AGRO_CADASTRO_PRODUTO_ERP_SYNC_HABILITADO=false` (padrão). Status debug: `GET /api/agro/fonte-status/`.


| Status                 | Tela / módulo                                                     | Nota                                                                                                                                            |
| ---------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **Feito**              | Clientes PDV, Vendas Agro, NFC-e, Caixa, RH (quase todo Postgres) | Sync ERP opcional onde existir                                                                                                                  |
| **Feito (staging OK)** | Cadastro SisVale `/produtos/cadastro-erp/`                        | `**AGRO_FONTE_CATALOGO=agro_pg`** no teste — import 3354 prod · Renan validou busca GM/barras + salvar preço + **sem piscadinha** (v1.84–v1.85) |
| **Infra pronta, off**  | Catálogo Postgres                                                 | `catalogo_agro.py`, modelo `Produto`, `importar_catalogo_mongo_produto` — **ligado no staging**; produção ainda Mongo                           |
| **Infra só flag**      | Estoque ledger, Financeiro Postgres                               | Flags existem; **não ligadas** nas views                                                                                                        |
| **Falta (alta)**       | PDV `/consulta/`, wizard `/pdv/checkout/`                         | `/api/buscar/`, cache catálogo, saldos, médias → Mongo                                                                                          |
| **Falta (alta)**       | Gestão operacional `produtos_gestao.html`                         | `gestao/lista` + `**gestao/facetas`** (vários `distinct`) — suspeito lentidão pós-entrada NF                                                    |
| **Falta (média)**      | Entrada NF, Compras, Estoque/transferências, Validade             | Buscas e agregações no espelho                                                                                                                  |
| **Falta (grande)**     | Lançamentos (todas), BI `/`, Fiado, resumo financeiro             | `mongo_financeiro_util` + `DtoVenda`                                                                                                            |


**Ordem sugerida:** cadastro lista → `agro_pg` → PDV `/api/buscar/` → gestão operacional → estoque ledger → **financeiro** (backup + checkpoint na tela Lançamentos).

**Estimativa (Renan — linguagem simples):**


| Etapa | O quê muda na prática                                                                                                  | Risco                                                        | Tempo (dev + teste no teste) |
| ----- | ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | ---------------------------- |
| **1** | Cadastro de produtos passa a ler/gravar **só no SisVale** (Postgres), sem depender do espelho na hora de listar/buscar | **Médio** — preço ou produto que “some” se import incompleto | **~1 semana**                |
| **2** | PDV + gestão de produtos na mesma base                                                                                 | **Alto** — balcão não acha produto ou preço errado           | **+2 a 3 semanas**           |
| **3** | Estoque, compras, financeiro, BI                                                                                       | **Muito alto** — impacto em caixa e contas                   | **vários meses**, por partes |


**Mitigação:** fazer só a etapa 1 no `teste`, conferir cadastro + busca + salvar preço, **só então** produção.

**Antes da etapa 1 (Renan — você faz; assistente implementa depois):**

1. Testar **só no ambiente teste** (Render staging) — **não** na loja ainda.
2. Anotar **2 ou 3 produtos** que você conhece: nome, preço de venda, código GM ou barras (para comparar depois).
3. Entrar no cadastro no teste com seu login e confirmar que abre `/produtos/cadastro-erp/`.
4. Confirmar no Render que o **staging tem banco Postgres próprio** (não o mesmo da produção).
5. **Não esperar** que o PDV mude nesta etapa — só a tela de cadastro.

**Depois que o assistente entregar etapa 1 (você testa):**

1. Aguardar deploy do branch `teste` no staging + **Ctrl+F5** no cadastro.
2. Conferir se a lista e a busca acham os produtos anotados.
3. Alterar preço de um produto → salvar → buscar de novo → preço **deve permanecer**.
4. Criar produto novo (se liberado no teste) ou pular se ainda bloqueado no staging.
5. Se algo falhar: anotar nome do produto + o que viu (sumiu, preço voltou, lista vazia).

**Etapa 1 — validada no teste (2026-06-22, Renan):** flag `agro_pg` + import · GM0027-1 a **R$ 21,00** persiste · busca nome/código OK · piscadinha preço resolvida (v1.84) · abertura lista mais rápida (v1.85 prefetch + Postgres sem count). **Próximo:** cherry-pick etapa 1 → produção **só quando Renan pedir**; PDV ainda Mongo.

**Produto de teste (Renan):** GM0027-1 · Ração Formula Natural adulto raças pequenas 1kg — **preço correto R$ 21,00** (staging/Agro); espelho Mongo R$ 19,90. Usar para validar busca + salvar + preço persistente.

---

### 4.16 Lançamentos — corte ERP (em preparação)

Fluxo **seguro** (só admin vê os botões):

1. **Backup ZIP** — CSV no PC (**só redundância**; SisVale não importa de volta).
2. **Checkpoint** — carimbo no Mongo; **não apaga nem muda valores**.
3. **Corte Agro→ERP** — após checkpoint, SisVale **não envia** lançamento/baixa pela API (`VendaERPAPIClient`); automático no código **v1.14**.
4. **Opcional Render** — `AGRO_FINANCEIRO_MONGO_CONGELADO=true` (reforço).

**Backup no PC** — cópia de segurança fora do sistema. Checkpoint = carimbo dentro do SisVale.

**Corte ERP:** é a **nossa** integração (API aberta WL), não ticket ao fornecedor. Padrão já era `AGRO_FINANCEIRO_ERP_SYNC_HABILITADO=false`; após checkpoint o bloqueio é garantido no código.

**Armadilha cherry-pick:** se `lancamentos_financeiros.html` incluir `lancamentos_pin_entrada.html`, o template **tem** que ir junto — senão **500** em Contas a pagar/receber.

- **PIN Lançamentos:** 1× por sessão ao entrar em qualquer tela `/lancamentos/*` (sem hub modal); navegação interna sem repetir; **modo descanso** (~3 min idle) pede de novo; sair para PDV/outra tela limpa a sessão.

Rotas: `backup-completo.xlsx` · `backup-abertos.zip` · `congelamento-status/` · `congelar-pre-corte/`. Painel na entrada `/lancamentos/`.

---

## 5. Variáveis de ambiente importantes


| Variável                      | Efeito                                   |
| ----------------------------- | ---------------------------------------- |
| `AGRO_STAGING_READONLY`       | Bloqueia escrita Mongo no staging        |
| `AGRO_ERP_PEDIDOS_DRY_RUN`    | Não grava pedidos ERP no staging         |
| `NFC_E_*`                     | Toda config NFC-e (ver NFCE-PRODUCAO.md) |
| `AGRO_DASHBOARD_GASTOS_PLANO` | Mostra gráfico gastos por plano no BI    |
| `AGRO_RH_PLANO_SALARIO_FOLHA` | Plano Mongo do título de salário         |
| `ALERTA_VENDAS_CRON_TOKEN`    | Token crons HTTP                         |


---

## 6. Como trabalhar com o assistente (Renan ↔ Cursor)

1. **Anexar contexto:** só `@banana` na maioria dos chats. `@AGENTS.md` é opcional (UX §11, RH §9, Lançamentos §10, tarefa muito ampla).
2. **Dois arquivos, papéis diferentes:** banana = memória viva (WIP, pendências, checkpoint). AGENTS = manual estável (não duplicar aqui).
3. **Registro de alterações:** **sempre** atualizar `banana.md` ao entregar mudança no sistema (fix, feature, deploy) — CHECKPOINT + § do módulo; commits e versão para rollback/contexto. Ver regra no topo (*Registro no banana*). Renan pode pedir *"atualize a banana"* também. AGENTS §7 **só** se Renan pedir.
4. **Escopo:** pedir arquivos ou módulo; assistente não amplia sem autorização.
5. **Antes de editar:** assistente deve dar **uma linha de plano**.
6. **Entrega:** um patch coeso por tarefa.
7. **Commits / teste:** push `**teste` automático** quando entregar fix (Renan valida no Render teste). Bump de `VERSION` (hook ou `python scripts/bump_version.py`). **Produção:** só quando Renan pedir (item 8).
8. **Produção:** **nunca** push/merge/deploy na loja (Render **SistVale**) sem *«pode subir (produção)»*. **2026-06-22:** assistente subiu PDV×cadastro em produção sem pedido — **não repetir**.
9. **Modo econômico:** Renan pode pedir respostas curtas.
10. **Cliente:** Renan usa **Chrome** — não perguntar Electron vs browser; Electron não é ambiente de teste dele.
11. **Retomar trabalho antigo:** módulo + este arquivo; chats anteriores não ficam na memória do assistente.

---

## 7. Documentos irmãos (não repetir aqui)


| Arquivo                                 | Conteúdo                                                                             |
| --------------------------------------- | ------------------------------------------------------------------------------------ |
| `AGENTS.md`                             | Enciclopédia — mapa URLs, UX §5–11, changelog §7. Referência, não anexo obrigatório. |
| `docs/DEPLOY-AMBIENTES.md`              | Staging readonly, fluxo Git                                                          |
| `docs/NFCE-PRODUCAO.md`                 | Checklist fiscal produção                                                            |
| `docs/ESTOQUE_AGRO_FONTE_DA_VERDADE.md` | Regra estoque                                                                        |
| `docs/CONTEXTO_SESSAO_CLIENTES_PDV.md`  | Sessão clientes (histórico)                                                          |
| `docs/GUIA_ABAS_NAVEGADOR_AGRO.md`      | Abas navegador                                                                       |
| `.cursor/rules/agro-consulta.mdc`       | Regra Cursor resumida (auto-carregada)                                               |


---

## 8. Linha do tempo recente (commits `teste`)

Últimos temas entregues (mais recente primeiro):

1. **FAB PDV — não cobrir modais/botões** — 2026-05-21: z-index 90, hide overlay/dialog, reposicionar canto direito (`_agro_pdv_fab.html`).
2. **Lançamentos CP — layout novo padrão + vista preservada + perf lista** — 2026-06-19 (CHECKPOINT).
3. **PDV wizard — GM no barras remove carrinho** — `e055761` · `pdv_wizard.js`: hífen `GM1546-5S` não remove carrinho; GM modo barcode.
4. **Lançamentos — Empréstimo (entrada + pagamento)** — pseudo-plano Nova saída + lote manual (`fbccf19`).
5. **Lançamentos — Nova saída (legibilidade)** — card expandido; fontes/campos maiores.
6. **Etiquetas faixa 230…** — `5c6590a` v1.20: CODE128 interno loja.
7. **PDV legado carrinho GM** — produção `59bdedc` v1.02.

*Para lista completa:* `git log teste --oneline -50`.

---







## CHECKPOINT DE ATUALIZAÇÃO

**Versão:** `1.0.45`  
**Última atualização:** `2026-06-22`  
**Atualizado por:** assistente Cursor (NFC-e cert teste→prod + Base64)  
**Versão app (`VERSION`):** **teste** v1.93 · **produção** v1.92 (`52cde10`)

### NFC-e — go-live em andamento (2026-06-22, Renan)


| Passo | O quê                                                                                      | Status                        |
| ----- | ------------------------------------------------------------------------------------------ | ----------------------------- |
| **1** | Variáveis Render **SistVale - Produção** (lista abaixo)                                    | **Renan agora**               |
| **2** | Conferir `GET /api/nfce/status/` logado: `ativo: true`, `tp_amb: 1`, `serie: 21`           | Após deploy Render            |
| **3** | Reteste no **teste** (PIX, dinheiro, reemissão) — opcional se confiar no histórico homolog | Renan                         |
| **4** | Cherry-pick pacote 1 (front PDV + vendas) → `producao` **neste chat**                      | Assistente quando Renan pedir |
| **5** | Venda teste PIX na loja + ajustar `NFC_E_PROXIMO_NUMERO` se 539                            | Renan na loja                 |


**Loja hoje:** backend NFC-e **sim** · modais PDV **não** (`eb2ce4c`). Vars com `ENABLED=true` **não emitem** sozinhas até passo 4.

**Vars obrigatórias (Render produção):** `NFC_E_ENABLED`, `NFC_E_TP_AMB=1`, `NFC_E_MODO=manual`, `NFC_E_SERIE=21`, `NFC_E_PROXIMO_NUMERO`, cert `NFC_E_CERT_BASE64`+`NFC_E_CERT_PASSWORD`, `NFC_E_CSC_ID`+`NFC_E_CSC_TOKEN`, emitente (`CNPJ`, `IE`, `RAZAO`, endereço, `CMUN=3524600`). Detalhe: CHECKPOINT + `docs/NFCE-PRODUCAO.md`.

### NFC-e — certificado e Base64 (2026-06-22, Renan)

**Pode copiar o certificado do teste?** **Sim**, se for o **mesmo arquivo .pfx A1** da loja (e-CNPJ `48900774000103`) — na GM Agro costuma ser **um certificado só** para homolog e produção. Copie no Render produção: `NFC_E_CERT_BASE64`, `NFC_E_CERT_PASSWORD`, razão, IE, endereço, etc.

**O que NÃO pode ser igual ao teste** (tem que ser **produção**):

| Variável | Teste (homolog) | Produção (loja) |
|----------|-----------------|-----------------|
| `NFC_E_TP_AMB` | `2` | **`1`** |
| `NFC_E_CSC_ID` / `NFC_E_CSC_TOKEN` | CSC do portal **homologação** | CSC do portal **produção** NFC-e SP |
| `NFC_E_PROXIMO_NUMERO` | numeração homolog (ex. já foi 13) | **próximo livre série 21 em produção** (pode ser `1` se nunca emitiu) |

**Comando PowerShell deu erro?** Quase sempre é **caminho errado** (`C:\caminho\...` era só exemplo). Jeito mais fácil:

1. Abra o Render **teste** → Environment → copie o valor inteiro de `NFC_E_CERT_BASE64` → cole na **produção** (se for o mesmo .pfx).
2. **Ou** no PowerShell, com o caminho **real** do arquivo (aspas se tiver espaço):

```powershell
$pfx = "C:\Users\RenanHinnen\Downloads\seu-certificado.pfx"
[Convert]::ToBase64String([IO.File]::ReadAllBytes($pfx)) | Set-Clipboard
```

Se der *«não encontrado»*, arraste o `.pfx` para a janela do PowerShell para colar o caminho certo.

**Senha:** copie `NFC_E_CERT_PASSWORD` do teste se for o mesmo arquivo.

### NFC-e — obter CSC (passo a passo, 2026-06-22)

Portal SP: [https://www.nfce.fazenda.sp.gov.br/NFCePortal/](https://www.nfce.fazenda.sp.gov.br/NFCePortal/)

| Passo | O quê | Status |
|-------|--------|--------|
| **1** | Abrir portal + certificado A1 da loja no PC | Renan |
| **2** | Credenciamento «com validade jurídica» (se ainda não fez) | pendente |
| **3** | Gerenciar Cód Segurança → «com validade jurídica» → Novo Cód Segurança | pendente |
| **4** | Copiar **ID** → `NFC_E_CSC_ID` e **Cód Segurança** → `NFC_E_CSC_TOKEN` no Render **produção** | pendente |

**Mapa:** ID Token = `NFC_E_CSC_ID` (número, ex. `1` ou `000001`) · CSC = `NFC_E_CSC_TOKEN` (texto longo). **Produção** = menu **«com validade jurídica»** (não só homologação). CSC do **teste** ≠ CSC da **loja**.

### Paridade prod × teste — auditoria Git (2026-06-22)

**Backend core idêntico** (`git diff origin/producao origin/teste` — **sem diferença**): `mongo_financeiro_util.py`, `views.py`, `catalogo_agro.py`, `cadastro_codigo_sequencial_util.py`, `nfe_entrada_util.py`, migrations NFC-e.

**Loja (`producao` v1.92)** = tudo que já subiu + pacote lançamentos v1.92 (`2d88ee4`). **Cadastro Postgres / código 4010+** já estavam na loja (`8889955`, `3d8ae08`, `f08da8c`) — **não** estão pendentes.

### Só no teste — ainda NÃO na loja (2026-06-22)

Conferência: `git diff origin/producao origin/teste --stat` · 36 arquivos · ~3,5k linhas (maioria front/PDV).


| #   | Pacote                                | Arquivos / nota                                                                                                           | Risco loja                                                             |
| --- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| 1   | **NFC-e PDV**                         | `pdv_wizard.js`, `pdv_wizard.html`, `venda_cupom_80mm.js`, `vendas_lista.html`, `venda_agro_detalhe.html`, `pdv/views.py` | **Alto** — foi **revertido** na loja (`eb2ce4c`); teste evoluiu depois |
| 2   | **Agro Display Scale**                | `agro_display_scale.js`, `_agro_display_scale.html`, `_agro_consulta_ui.html`, BI                                         | Médio — calibração global de tamanho de tela                           |
| 3   | **CP lista — perf/UX**                | `lancamentos_contas_pagar_teste.html` — bootstrap HTML, prefetch BI, badge «Sincronizando», colunas PIN, filtro hoje      | Baixo — só template; **API já igual** na loja                          |
| 4   | **Entrada NF — financeiro duplicado** | `entrada_nota.html` — pula etapa se título já existe; mensagens «já existia / recuperado»                                 | Baixo                                                                  |
| 5   | **BI — launchpad / escala rápida**    | `dashboard_gerencial.html`, `partials/dashboard_gerencial_body.html`, `home.html`                                         | Baixo                                                                  |
| 6   | **PDV entrega**                       | `partials/pdv/step_entrega.html`                                                                                          | Médio                                                                  |
| 7   | **Caixa — PIX MP QR**                 | `caixa_util.py` — normaliza «Mercado Pago … QR» como PIX                                                                  | Baixo                                                                  |
| 8   | **Config status staging**             | `agro_fonte_config.py` — `staging_readonly` no status; ERP sync só por env (sem auto-block checkpoint)                    | Baixo — prod mantém lógica checkpoint no sync                          |
| 9   | **Textos pré-corte ERP**              | `lancamentos_pre_corte_erp_panel.html`                                                                                    | Cosmético                                                              |
| 10  | **PIN calendário CP/DRE/fluxo**       | +1 linha include PIN em calendário/DRE/fluxo                                                                              | Baixo                                                                  |


**Removido da lista «pendente» (já estava na loja):** cadastro v1.87–v1.89, deploy fixes `bb27da1`/`nfe_entrada_util` — código **igual** nos dois branches.

**Como conferir de novo:** `git fetch origin` → `git diff origin/producao origin/teste --stat`

### Pacote teste v1.92 → **produção** (2026-06-22, Renan neste chat)

Renan autorizou *«1.92 do teste pode subir para produção»* (validou botão layout clássico sumiu; **exclusão** não testou no Render teste — `AGRO_STAGING_READONLY` bloqueia gravação Mongo; vai testar na **loja**).


| Pacote                                            | `teste`   | `producao` (cherry-pick)            |
| ------------------------------------------------- | --------- | ----------------------------------- |
| Nova saída — mês calendário, sucesso, volta BI/CP | `b5e499e` | `99ea1ae` v1.57                     |
| Excluir manual Agro **quitado** (CR/CP backend)   | `726b3ee` | `1d412e0` v1.58                     |
| CP — Excluir na lista nova + fim layout clássico  | `e98db0f` | `2d88ee4` (**VERSION loja = 1.92**) |


**Loja:** Ctrl+F5 após deploy Render → Contas a pagar → expandir linha → **Excluir** (sem ir ao clássico). CR: título manual quitado deve mostrar Excluir. **Renan:** conferir exclusão na loja real.

**Reverter:** revert dos 3 commits em `producao` (ordem inversa).

### Mapa loja vs teste — resumo (2026-06-22)


| Ambiente     | VERSION | HEAD      | Nota                                |
| ------------ | ------- | --------- | ----------------------------------- |
| **teste**    | v1.93   | `9ef18d4` | v1.92 + docs banana (auditoria)     |
| **produção** | v1.92   | `52cde10` | Pacote `2d88ee4` + VERSION alinhado |


**Próximos candidatos produção** (quando Renan pedir neste chat): ver tabela «Só no teste» acima — priorizar **#3 CP perf** ou **#4 Entrada NF** (baixo risco) antes de **NFC-e** (#1).

### Contas a pagar — Excluir na lista nova + fim do layout clássico (2026-06-22)


| Item                   | Detalhe                                                                                              |
| ---------------------- | ---------------------------------------------------------------------------------------------------- |
| **Sintoma**            | **Excluir** na lista nova abria a tela clássica (`/classico/?mongo_id=…`) e **não apagava** o título |
| **Causa**              | Botão era link para o layout antigo (corrigido em `b5e499e`; reforço neste patch)                    |
| **Fix**                | Excluir chama `api/lancamentos/excluir/` na própria tela; operador do PIN no payload                 |
| **Layout clássico CP** | `/lancamentos/contas-pagar/classico/` → **redirect** para `/lancamentos/contas-pagar/`               |
| **UI**                 | Removidos botões «Layout clássico» (lista + calendário)                                              |
| **Teste Render**       | Ctrl+F5 → expandir linha → **Excluir** → confirmar → título some sem mudar de tela                   |
| **Produção**           | `2d88ee4` v1.92 — Renan validou sumiço do clássico; exclusão a conferir na loja (staging readonly)   |


### Chat canônico — produção (2026-06-22, Renan)


| Regra                    | Detalhe                                                            |
| ------------------------ | ------------------------------------------------------------------ |
| **Onde sobe loja**       | **Este chat** — Renan fará push produção **daqui**                 |
| **Outros chats/posts**   | Pode ter havido deploy fora; **não** confiar só na memória do chat |
| **Fonte da verdade**     | CHECKPOINT + `git diff origin/producao origin/teste --stat`        |
| **Antes de cherry-pick** | Tabela «Só no teste»; Renan confirma o pacote                      |


### Cadastro — código sequencial produto novo (2026-06-22, Renan OK teste — **já na loja**)


| Item                | Detalhe                                                                                                                                         |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **Validação Renan** | OK no Render **teste**                                                                                                                          |
| **Commits `teste`** | `ffc82ba` v1.87 · `cea03c7` v1.88 · `a2303c7` v1.89                                                                                             |
| **Produção**        | `8889955` — **já na loja** (código **igual** nos branches)                                                                                      |
| **Reverter**        | Revert `8889955`; arquivos: `cadastro_codigo_sequencial_util.py`, `catalogo_agro.py`, `views.py`, `_modal_editar_produto_cadastro_erp.inc.html` |


**Regras (canônicas — §4.6):** código sistema **4 dígitos**, faixa **4010–9999**, sequência só pelo **código sistema** (GM não conta); GM = `GM` + número (editável livre); modal novo não repinta ao carregar códigos.

### Fluxo Render — teste automático, produção só com pedido (2026-06-22, Renan)


| Regra                     | Detalhe                                                                                                |
| ------------------------- | ------------------------------------------------------------------------------------------------------ |
| **Onde testa**            | Render projeto **teste** — **não** local                                                               |
| **Dois sites**            | **teste** = homologação · **SistVale** = loja                                                          |
| **Assistente → teste**    | Commit + push `**teste` automático** (deploy Render segue)                                             |
| **Assistente → produção** | **Só** com *«pode subir (produção)»* ou equivalente explícito (ex. *«pode subir se não for invasivo»*) |


### Registro no banana — toda entrega (2026-06-22, Renan)

Assistente **sempre** registra no CHECKPOINT (e § do módulo se couber): **o quê**, **commits**, **versão**, **teste/produção**, **como reverter** se relevante. Objetivo: próximo chat, diagnóstico, rollback.

### Cadastro — código sequencial (histórico curto)

**Sintoma inicial:** `__novo__` no código · depois número errado (9504) · piscada apagando nome.

**Fix (branch `teste`, validado v1.87–v1.89):** ver bloco «Cadastro — código sequencial produto novo» acima.

### Cadastro produtos — etapa 1 Postgres + perf lista (2026-06-22, Renan OK)


| Item                                     | Status                                                                                                                               |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `AGRO_FONTE_CATALOGO=agro_pg` staging    | Renan validou                                                                                                                        |
| Import `importar_catalogo_mongo_produto` | 3354 produtos                                                                                                                        |
| Busca nome + GM/barras                   | OK (v1.83+)                                                                                                                          |
| Piscadinha preço errado                  | **Resolvida** v1.84 — lista só servidor                                                                                              |
| Abertura lenta pós-fix                   | **Melhor** v1.85 — prefetch 1ª página, badge ERP em paralelo, Postgres sem `count`, busca local instantânea (preço «…» até servidor) |


**Commits:** `3c9ed24` v1.84 · `8d76146` v1.85 · branch `teste`.

**Próximo passo cadastro Postgres na loja:** flag `AGRO_FONTE_CATALOGO=agro_pg` + import — **já feito** (`3d8ae08`). Perf lista (v1.84–v1.85) **já na loja** (mesmo JS/backend).

**Cadastro × PDV (etapa 1 — linguagem de loja):**


| O que você faz no cadastro                        | Aparece no PDV?                                                        |
| ------------------------------------------------- | ---------------------------------------------------------------------- |
| **Muda preço/nome** de produto que **já existia** | **Sim** — overlay + merge Postgres na busca                            |
| **Cria produto novo** (Id `AGRO…`)                | **Sim** (após deploy v1.86+) — busca e catálogo local mesclam Postgres |
| **Mesmo PC**, cache antigo                        | Atualizar estoque/catálogo no PDV (botão sync) ou Ctrl+Shift+R         |


**Fix PDV×cadastro (2026-06-22):** `mesclar_prods_busca_pdv` + `mesclar_catalogo_pdv_cache` — produtos do Postgres entram na busca `/api/buscar/` e no download do catálogo local.

### FAB PDV — sobreposição com botões e modais (2026-05-21, Renan)

**Sintoma:** botão flutuante **PDV** ficava **na frente** de outros elementos conforme a tela — ex. **Cancelar** e «Preencher pela frase» no modal **Nova saída** (Lançamentos).

**Causa:**


| Item    | Detalhe                                                                           |
| ------- | --------------------------------------------------------------------------------- |
| z-index | FAB em **9000**; Nova saída `#agro-nova-saida-overlay` em **z 250** → FAB ganhava |
| Ocultar | `shouldHide()` só via `body.modal-open`; Nova saída usa `.hidden` + `aria-hidden` |


**Fix** (`produtos/templates/produtos/_agro_pdv_fab.html`):


| Ação                        | Comportamento                                                                                                              |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| z-index **90**              | Acima do conteúdo; **abaixo** de modais (200+)                                                                             |
| `hasOpenOverlay()`          | Some com `[role=dialog]`, `[aria-modal]`, `.sv-modal.show`, `#agro-nova-saida-overlay:not(.hidden)`                        |
| Colisão canto inf. esquerdo | Sobe sobre barras fixas; se encostar em **button/a**, sobe mais ou vai pro canto **direito** (`.agro-pdv-fab-wrap--right`) |
| Observer                    | `MutationObserver` debounced (class / `aria-hidden`) na árvore do `body`                                                   |


**F1** continua global quando o FAB está oculto. **FX on/off** inalterado.

**Teste Chrome:** Ctrl+F5 → Lançamentos → **Nova saída** → FAB não aparece; fechar → volta. Redimensionar janela estreita → não cobrir botões do rodapé.

### Perf — animações do botão PDV flutuante (2026-06-19, dúvida Renan)

Acúmulo de animações no app **pode** pesar em PC fraco ao longo do tempo — mas **este FAB é impacto baixo**: 1 elemento, só CSS (`transform`/`opacity`/gradiente), sem JS extra nem tráfego de rede. O que pesa de verdade: troca de página inteira (Chrome MPA), listas grandes, Mongo, JS do PDV/Lançamentos.

**Interruptor implementado:** mini-botão **FX on / FX off** acima do FAB (persiste no navegador). Desliga efeitos **decorativos** (FAB arco-íris, Validade pulsando, pulso PDV/Orçamento no BI). Mantém animações **funcionais** (loading, scanner, salvando).

### FAB PDV + Nova saída quitado → **produção** (2026-06-19, Renan pediu)

**Commit:** `f824944` · v1.48 · cherry-pick escopo fechado de `teste`.


| Pacote         | Detalhe                                                                                                              |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| **FAB PDV**    | `_agro_pdv_fab.html` + include em `_agro_open_external.html` — pulso/arco-íris, FX on/off, some em modal, z-index 90 |
| **Nova saída** | Quitado **por linha** (modo Parc.); forma opcional; API JSON segura; expandir empréstimo dual (`95474d5`)            |


**Loja:** Ctrl+F5 após deploy Render → FAB canto esquerdo; Nova saída → 3 parcelas + empréstimo dual.

### Nova saída → **produção** (2026-06-19, pacote anterior)

**Commit:** `be9558c` · v1.47 · cherry-pick escopo fechado de `teste` (`9ba11e4`…`cd5a6e0`).


| Pacote     | Detalhe                                                                |
| ---------- | ---------------------------------------------------------------------- |
| Parcelas   | Nº + intervalo → grade; backend `parcelas_saida`                       |
| Calendário | Popup grande (competência, vencimento, parcelas)                       |
| UI         | Grid 4 col; pílulas **Total | Parc.**; empréstimo saída abaixo entrada |


**Loja:** Ctrl+F5 após deploy Render → Nova saída.

### Bug produção — HTTP 500 ao Finalizar (2026-06-19)

| Sintoma | Alerta JS «Resposta inválida do servidor (HTTP 500)» — corpo HTML, não JSON |
| Cenário Renan | Empréstimo dual + 3 parcelas + quitado (836,95 / 836,94) |
| Causa provável | Exceção **fora** do `try` da API (`date.fromisoformat` no cabeçalho ou `expandir_linhas_emprestimo_dual_lote`) → página de erro Django |
| Fix v1.48 | `_d()` try/except · try expandir · quitado linha a linha · JS trecho HTML |
| Fix v1.49 | `sum(v for v, _, _ in parcelas_pag)` — tupla 3 itens |
| Fix v1.50 | «ADICIONAR CONTA» não vale para **quitado** |
| Empréstimo dual forma | **Forma entrada** (obrig.) + **Forma saída** (opc.) — 4ª coluna da grade; backend `_fin_ln_campo` / expandir dual |
| Layout Valor dual | **Renan não satisfeito** — tentativas v1.70–v1.71; **desistiu**; produção v1.51 |
| Juros dual parcelado | Saída > entrada → cada parcela gera **Pagamento** + **Juros** (proporcional); UI mostra «231,85 + 18,15» + **?** na grade |

### Empréstimo dual — juros parcelado (2026-06-19)

**Regra (Renan):** parcelas somam **valor saída** (ex. 4×250 = 1000). Diferença saída−entrada (72,60) vira **Juros de Empréstimos** **por parcela** (18,15), restante **Pagamento de Empréstimos** (231,85). Antes validava contra entrada e juros iam num título só.

**UI:** valor da parcela continua 250; abaixo «231,85 + 18,15» + **?** explicando planos. Modo Total: hint sob valores entrada/saída.

**Backend:** `expandir_linhas_emprestimo_dual_lote` + `split_decimal_proporcional`.

### Juros dual parcelado → **produção** (2026-06-19, Renan pediu teste real)

**Commit:** `4505805` · v1.52 · cherry-pick de `teste` (`0517525`).

**Loja:** Ctrl+F5 após deploy Render → Nova saída → Empréstimo dual → entrada 927,40 / saída 1000 → 4×250 → composição **231,85 + 18,15** → Finalizar.

### Nova saída — UX pós-gravar e intervalo mensal (2026-06-19, Renan)


| Item                 | Comportamento                                                                                             |
| -------------------- | --------------------------------------------------------------------------------------------------------- |
| **Intervalo mensal** | **Mensal / bimestral / trimestral** = mesmo dia no mês (19/06 → 19/07); semanal/quinzenal = dias corridos |
| **Sucesso**          | Painel verde no modal («N títulos gravados»); **sem** alerta ERP nem lista de IDs                         |
| **Volta**            | BI `/` → fica no BI · Contas a pagar → recarrega lista **in-place** (sem redirect)                        |


**Arquivos:** `lancamento_nova_saida.js`, `lancamento_nova_saida_modal.html`, `dashboard_gerencial.html`, `lancamentos_contas_pagar_teste.html`, `mongo_financeiro_util.py` (`_fin_vencimento_parcela`).

**Deploy:** **produção** `99ea1ae` v1.57 (cherry-pick `b5e499e`).

### Excluir entrada manual quitada (2026-06-19, Renan)

**Sintoma:** «Entrada de Empréstimo» quitada em Contas a receber — coluna Ações vazia, sem Excluir.

**Causa:** `_lancamento_pode_excluir_agro` barrava **quitado** antes de checar **Lote manual Agro**.

**Fix:** manual Agro (Nova saída / lote manual) pode excluir **mesmo quitado**; ERP continua bloqueado.

**Deploy:** **produção** `1d412e0` v1.58 (cherry-pick `726b3ee`).

| UX quitado Nova saída | **Produção** v1.48+: modo Parcela → botão **Quitado** em cada linha; Ctrl+F5 após deploy |

### Empréstimo dual forma + layout → **produção** (2026-06-19, Renan pediu)

**Commit:** `76e2a8b` · v1.51 · cherry-pick escopo fechado de `teste` (`7ef55b0`…`a47933b`).


| Pacote             | Detalhe                                                                                      |
| ------------------ | -------------------------------------------------------------------------------------------- |
| **Forma split**    | Entrada obrigatória · saída opcional · campos separados no modal e no Mongo                  |
| **Grade**          | Forma dual na 4ª coluna (sem linha extra); `.hidden` CSS Agro                                |
| **Valor dual**     | Entrada/saída lado a lado na coluna Valor — layout **imperfeito**; Renan desistiu de refinar |
| **Alerta parcial** | Gravação parcial mostra até 4 erros no alert                                                 |


**Loja:** Ctrl+F5 após deploy Render → Nova saída → Empréstimo (entrada + pagamento).

### Ambiente Renan — Chrome (não Electron)


| Item           | Valor                                                        |
| -------------- | ------------------------------------------------------------ |
| **Navegador**  | **Google Chrome** (MPA: cada tela = página nova)             |
| **Electron**   | Testado; **muito lento** — **não** é referência para UX/perf |
| **Assistente** | **Não perguntar** Chrome vs Electron; assumir Chrome         |


**Lançamentos / BI:** prefetch + bootstrap HTML (v1.48) valem no Chrome; aba lateral do shell **não** entra no fluxo dele.

### Lançamentos CP — abertura (Chrome, 2026-06-19)


| Feito (v1.48)                   | Efeito real no Chrome                                                         |
| ------------------------------- | ----------------------------------------------------------------------------- |
| Bootstrap HTML (hoje + abertos) | Elimina o “Carregando…” da **2ª** chamada API; lista aparece quando o JS roda |
| Prefetch + cache sessionStorage | Ajuda na **reabertura**; 1ª do dia ainda espera o servidor                    |
| Sincronizando…                  | Atualiza em background                                                        |


**Renan:** melhora **sutil** — OK para o desenho atual.

**O que ainda pesa (não dá para sumir com patch pequeno):**

1. Troca de página inteira BI → Lançamentos (branco + download HTML/JS).
2. PIN na 1ª entrada da sessão.
3. Mongo na montagem da página (bootstrap) — trocou API depois por consulta no Django.

**Conclusão:** para **Chrome MPA + Mongo**, o pacote atual é **quase o teto** (v1.48).

**Roadmap — adiado (Renan, 2026-06-19):** não investir agora no próximo salto. Retomar quando pedir:


| Prioridade futura | Opção                                     | Nota                               |
| ----------------- | ----------------------------------------- | ---------------------------------- |
| A                 | **Financeiro Postgres** (`agro_pg`)       | Alinha com §4.15; ganho estrutural |
| B                 | **Lista CP no BI** (sem trocar de página) | Ganho de percepção no Chrome       |
| C                 | Enxugar HTML/JS da página CP              | Ganho menor                        |


Até lá: manter bootstrap + prefetch + cache; **não** empilhar micro-otimizações.

### Nova saída — grid alinhado (`teste`, 2026-06-19)


| O quê                   | Detalhe                                                                |
| ----------------------- | ---------------------------------------------------------------------- |
| **Grid 2ª linha**       | 4 colunas iguais à 1ª linha (plano · valor · competência · vencimento) |
| **Chave Total/Parcela** | Pílulas **Total                                                        |
| **Empréstimo dual**     | Saída **abaixo** da entrada, mesma coluna do valor                     |



| O quê                       | Detalhe                                                                                 |
| --------------------------- | --------------------------------------------------------------------------------------- |
| **Calendário**              | Popup grande (competência, vencimento, parcelas) — células ~2,85rem, Limpar/Hoje        |
| **Chave Total→Parcela**     | Ao lado do valor; padrão **Total** (card Nº parcelas oculto); **Parcela** mostra o card |
| **Parcelas** (modo Parcela) | Nº + intervalo → grade; valor sempre **total**                                          |


**Teste:** Ctrl+F5 → Nova saída → clicar Competência/Vencimento (calendário grande) · chave Parcela → card aparece.

### Nova saída — parcelas + layout empréstimo (`teste`, 2026-06-19)


| O quê                 | Detalhe                                                                    |
| --------------------- | -------------------------------------------------------------------------- |
| **Layout empréstimo** | Só entrada + saída (sem Valor duplicado); grid 4 colunas                   |
| **Parcelas**          | Nº + intervalo → grade vencimento/valor                                    |
| **Empréstimo dual**   | Parcelas na **saída**; backend `parcelas_saida` em `mongo_financeiro_util` |


**Teste:** Ctrl+F5 → Nova saída → 3 parcelas mensais · empréstimo dual com saída parcelada.

### O que este documento já cobre (até aqui)

- [x] Negócio GM Agro / SisVale e perfil dos operadores
- [x] Stack Django + Postgres + Mongo + Render + Electron
- [x] Deploy teste/producao e staging readonly
- [x] Mapa dos módulos principais com arquivos e armadilhas
- [x] NFC-e: modo por forma, série 21, reemissão, schema 225, UX modal/toast
- [x] Clientes Agro, duas telas de cadastro produto, estoque, caixa, RH
- [x] Como Renan usa nos chats: `@banana` (único anexo obrigatório)
- [x] Regra: assistente não pergunta se atualiza AGENTS.md
- [x] Versão app: bump automático `VERSION` por commit (hook `.githooks/pre-commit`)
- [x] Arquivo: `banana.md` na raiz
- [x] Ponteiros para docs irmãos (sem duplicar AGENTS.md inteiro)
- [x] Linha do tempo recente de commits
- [x] §4.15 roadmap desvinculação ERP (Mongo → Postgres)
- [x] Regra: assistente atualiza banana automaticamente (sem perguntar)
- [x] Nova saída: tipografia maior + card expandido ocupa altura (sem vazio embaixo)
- [x] **Empréstimo (entrada + pagamento)** — pseudo-plano Nova saída + lote manual (2026-06-18)
- [x] PDV wizard: diagnóstico GM/hífen no barras (§4.2 + abaixo)
- [x] **Contas a pagar — layout novo padrão** + `/classico/` (2026-06-19)
- [x] **Lista CP — colunas por PIN** (visibilidade, ordem drag, save) (2026-06-19)
- [x] **Lista CP — perf + preservar filtros/vista** após baixa/NF/Nova saída (2026-06-19)
- [x] **PIN Lançamentos** — só na entrada do módulo + descanso; abertura **hoje / em aberto** (2026-06-19)
- [x] **Ambiente Renan:** Chrome (MPA); Electron testado e descartado — assistente não pergunta
- [x] **Botão flutuante PDV** — canto inferior esquerdo, `/pdv/`, F1 global (2026-06-19)
- [x] **FAB PDV — sobreposição** — some em modal; z-index 90; colisão → sobe ou canto direito (2026-05-21)

### Lançamentos — Contas a pagar layout novo (2026-06-19)


| URL                                   | Tela                     |
| ------------------------------------- | ------------------------ |
| `/lancamentos/contas-pagar/`          | **Layout novo** (padrão) |
| `/lancamentos/contas-pagar/classico/` | Redirect → layout padrão |
| `/lancamentos/contas-pagar/teste/`    | Redirect → padrão        |


**Mesma API** `/api/lancamentos/`. **Editar / Excluir** no layout novo: modal + APIs `alterar`/`excluir` (sem redirecionar); vista preservada após salvar/excluir. `**/classico/`** redireciona para o layout padrão (2026-06-22).

**Perf:** projeção slim Mongo; `skip_totais` pág. 2+; cache sessionStorage; planos lazy.

**Abertura rápida:** prefetch BI/F7; cache sessionStorage; selo **Sincronizando…**; **lista embutida no HTML** (bootstrap servidor, hoje+abertos); no app com abas, link do BI abre **aba lateral** (BI não some).

**Vista:** baixa, NF, Nova saída recarregam in-place (scroll, expandidos, «carregar mais», URL).

**Colunas (menu ▦ Colunas):** visibilidade + ordem por **operador do PIN** (`localStorage` `gm_fin_sv_cp_cols_v1`). Botão **Salvar no meu PIN** grava; persiste ao fechar o sistema. Arrastar **⋮⋮** na lista — quanto mais acima, mais à esquerda na tabela. **Padrão** (sem save): Vencimento · Fornecedor · Plano/grupo · Descrição · Saldo · Status · NF · Pagar.

**Planos de contas (filtros):** todos **marcados ao abrir**; desmarcar vale só na sessão (não grava — ao reabrir o sistema volta tudo marcado).

**Abertura:** lista padrão = **em aberto · vencimento hoje** (sem filtros na URL). Deep link / filtros salvos na URL respeitados.

**PIN:** uma vez ao entrar em Lançamentos (qualquer rota `/lancamentos/*`); navegação interna sem repetir; só de novo no **modo descanso** (idle). `/lancamentos/` redireciona direto para **Contas a pagar** (sem popup hub).

**Arquivos:** `lancamentos_contas_pagar_teste.html`, `mongo_financeiro_util.py`, `views.py`, `urls.py`, `lancamentos_financeiros.html`, `lancamentos_contas_pagar_calendario.html`, `includes/lancamentos_pin_entrada.html`, `_screensaver_pin.html`.

**Produção:** merge `teste`→`producao` 2026-06-19 (Renan pediu).

### NFC-e — status staging (2026-06-18)

- [x] **Reemissão** em Consultar vendas — Renan confirmou funcionando
- [x] Erro **225** — `card/tpIntegra=2` + IBPT por item
- [x] **PIX/cartão** — sem modal CPF; emite sem identificação
- [x] **Dinheiro** — popup NFC/Venda; modal CPF **grande** (v1.11+)
- [x] Toast falha fiscal **depois** da impressão Windows
- [ ] Merge `**producao`** — só quando Renan pedir + checklist `docs/NFCE-PRODUCAO.md`

### Lançamentos — Empréstimo (entrada + pagamento) — feito 2026-06-18

**Onde:** modal **Nova saída** em **Lançamentos** (`/lancamentos/`) + **Lote manual** (`/lancamentos/novo-manual/`). No **BI** (`/`): atalho só no card **Contas a Pagar** (sem botão na barra PDV/Orçamento/Menu).

**Plano na lista:** `Empréstimo (entrada + pagamento)` (buscar «emprest»).


| Campo        | Entrada (receita) | Saída / pagamento      |
| ------------ | ----------------- | ---------------------- |
| Loja, pessoa | sim               | sim                    |
| Conta, forma | não               | sim                    |
| Comp./venc.  | **hoje** (auto)   | o que preencher        |
| Quitado      | **sempre**        | chip «Quitado» só aqui |
| Recorrência  | desligada         | —                      |


**Valores:** entrada + saída. Se **saída > entrada** → título extra em **Juros de Empréstimos** (diferença). Pagamento principal = valor da entrada quando há juros.

**Toggle Pagar/Receber:** ignorado neste modo (gera receita + despesa no mesmo envio).

**Ajuda UI:** textos longos removidos do card; botão **?** no cabeçalho (só quando o plano empréstimo dual está ativo).

**Produção:** `d75c436` v1.10 (2026-06-18) — cherry-pick escopo fechado; **não** inclui PDV wizard nem merge `teste` inteiro.

### URGENTE — PDV carrinho some ao bipar GM no barras (2026-06-18)


| Onde                        | Sintoma                                                         | Status                                    |
| --------------------------- | --------------------------------------------------------------- | ----------------------------------------- |
| **Wizard** `/pdv/checkout/` | Cada bipe **GM1546-5S** remove **1 item** do carrinho (4→3→2→1) | **Staging `teste`** — aguarda teste Renan |
| **Legado** `/consulta/`     | Carrinho zerava ou perdia itens (F4 pós-bip, match parcial)     | **Produção** `59bdedc` v1.02              |


**Caso Renan (Ibiúna ensacada):** produto **1467** — GM `**GM1546-5S`** erroneamente no campo **Código de barras** (aba Fiscal). Leitor manda `GM1546-5S`; campo de busca mostra `**GM15465S`** (hífen engolido).

**Causa wizard:** atalho `**-`** na busca = `bumpLastCartItem(-1)` (menos qty do **último** item). O hífen do GM disparava remoção **sem** inserir o carácter.

**Patch wizard (`pdv_wizard.js`, em `teste`):**

- `-` / `+` ignorados enquanto digita GM/SKU ou janela pós-bip (1,5 s)
- Códigos `**GM…`** → modo **barcode** (auto-adiciona)
- Match **alnum** no cache (`GM15465S` = `GM1546-5S`)
- **F4** bloqueado após leitor (campo com código ou janela ativa)

**Teste pós-deploy (Renan):**

1. Ctrl+F5 no wizard · 4 itens no carrinho
2. Bipar **GM1546-5S** / **GM15465S** várias vezes
3. Carrinho **não** perde itens; idealmente adiciona Ibiúna
4. Corrigir cadastro: barras → EAN ou `230…` + reimprimir etiqueta

**PDV legado:** `consulta_produtos.js` + `_js_busca…` (produção v1.02).

**Produção wizard:** ainda **sem** este fix — cherry-pick só quando Renan pedir após OK no staging.

### Etiquetas — barras deve ser EAN, não GM (decisão 2026-06-18)

**Regra:** o leitor bipa **o que está codificado no barras** da etiqueta. **Correto:** EAN/GTIN numérico (ex. `7897030100427`) → PDV recebe número. **GM** (`GM1541-5S`, `GM1518-125-3`) só aparece quando a etiqueta foi gerada **sem** «Código de barras» válido no cadastro — aí o SisVale usa **CODE128 com texto GM** (`produtos_etiquetas_core.js` → `valorBarcodeProduto`).


| O quê                                             | Onde                                                                         |
| ------------------------------------------------- | ---------------------------------------------------------------------------- |
| Código GM                                         | **Texto** abaixo do barras (referência humana)                               |
| EAN 8/12/13                                       | **Dentro** do barras — é isso que o leitor deve mandar                       |
| Etiquetas antigas / já impressas com GM no barras | PDV aceita GM (patch carrinho); **reimprimir** quando houver EAN no cadastro |
| Ibiúna ensacada **GM1546-5S** (prod. 1467)        | Barras errado no cadastro — corrigir Fiscal → EAN/`230…` → reimprimir        |


**Operação:** antes de imprimir lote, abrir 🖨️ no cadastro — se aparecer aviso âmbar «Sem EAN», corrigir cadastro primeiro.

### Código de barras interno da loja (decisão 2026-06-18)

Nem todo item tem EAN de fábrica. Casos normais na GM Agro:


| Situação                                                                        | Barras no cadastro                                                     |
| ------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Embalagem do **fornecedor** (NF, saco fechado)                                  | EAN/GTIN **real** da embalagem quando existir                          |
| Saco **maior** com EAN, **unidade de dentro** sem barras (ex. ensacado na loja) | **Código interno** numérico criado pela loja — não é EAN do fornecedor |
| **Reembalagem** na loja (ração a granel, fracionado)                            | Idem — código interno (7 dígitos, 13 com DV válido, etc.)              |


**Não** confundir: «inventar» aqui = **identificador interno SisVale/loja**, não falsificar GTIN de fabricante. PDV e etiqueta tratam 4–14 dígitos como barras normal (CODE128); EAN-13 de **fábrica** exige DV correto.

**Faixa 230… (interno loja):** gerada pelo SisVale (`agro_codigo_barras_loja_util.py`) — 230 + 10 dígitos sequenciais. **Não é EAN-13** (último dígito é sequência, não DV). Etiqueta imprime **CODE128**; modal 🖨️ mostra «Barras interno loja». EAN real (ex. Abacate `789…`) continua EAN13.

**Diagnóstico Mongo:** `python manage.py pdv_diagnostico_codigo GM1546-5S GM1518-125-3 1813647`

### Código de barras curto (7 dígitos) + PDV — feito 2026-06-18

Match exato só-dígitos + fallback API (`_js_busca_produto_inteligente.html`, `consulta_produtos.js`). Etiquetas: CODE128 antes do fallback GM (`extrairCodigoBarrasCurto`).

### Etiqueta EAN-13 layout — feito 2026-06-18

**Sintoma:** barras 13 dígitos com DV errado → preview só preço cortado, sem barras.

**Correção:** `produtos_etiquetas_core.js` — valida/corrige DV, fallback CODE128, CSS 40×40 mm; modal cadastro avisa DV corrigido (âmbar).

**Staging (`teste`):** `5bcc05d` v1.19 · `5c6590a` v1.20 (230… CODE128) · `c234822` v1.22 (busca cadastro). **Ctrl+F5** cadastro + PDV após deploy Render.

### Cadastro produtos — busca GM/barras — feito 2026-06-18

**Sintoma:** lista cadastro não achava por código GM nem barras.

**Correção (`c234822` v1.22 + `…` v1.24):** modo **normal**, `api_produtos_cadastro`, prefixo GM (`GM1541` → `GM1541-5S`), Enter força busca, limpa «Continue digitando» ao buscar.

### Produção — patch PDV carrinho (feito 2026-06-18)

Renan validou no staging → subiu **só** o patch urgente (`59bdedc` em `producao`, v1.02). **Não** mergeou `teste` inteiro.

**Arquivos:**


| Arquivo                                       | Papel             |
| --------------------------------------------- | ----------------- |
| `consulta_produtos.js`                        | Fix carrinho + GM |
| `_js_busca_produto_inteligente.html`          | Match GM          |
| `pdv_diagnostico_codigo.py`                   | Diagnóstico Mongo |
| `produtos_etiquetas_core.js` + modal cadastro | EAN no barras     |


**Loja:** Ctrl+F5 no `/consulta/` após deploy Render.

### WIP / não commitado (snapshot 2026-06-19)


| Arquivo                                                | Tema                                          |
| ------------------------------------------------------ | --------------------------------------------- |
| `AGENTS.md`                                            | Nota `@banana` vs enciclopédia (local)        |
| `nfe_entrada_util.py`, `views.py`, `entrada_nota.html` | Entrada NF — sync financeiro desync (etapa 7) |


**Acabou de subir em `producao`:** Lançamentos CP (layout + perf + vista + PIN entrada + filtro hoje) · v1.33.

**Teste Renan (staging):** `/lancamentos/contas-pagar/` → filtrar → baixa/Nova saída → filtros e scroll mantidos.

### Pendências conhecidas (produto)

**Desvinculação ERP (responsividade)** — ver §4.15–4.16:

- [x] Backup ZIP + checkpoint — **feito** 2026-06-19 (~17 703 títulos)
- [x] Corte Agro→ERP (API) — **produção v1.14** (`372f90f`; automático após checkpoint)
- [ ] Próxima fase: `AGRO_FONTE_FINANCEIRO=agro_pg` (Lançamentos ler/escrever Postgres, não espelho Mongo)
- [ ] **Nunca** merge `teste` inteiro em `producao` — só cherry-pick do escopo combinado
- [x] Ativar catálogo Postgres no **teste** — **Renan OK 2026-06-22**
- [x] Ativar catálogo Postgres na **produção** — v1.53 deploy OK; loja testando (Renan avisa se falhar)
- [ ] PDV `/api/buscar/` e cache → Postgres
- [ ] Gestão operacional (lista + facetas) → Postgres
- [ ] Estoque `ledger` e financeiro `agro_pg` (implementar flags)
- [ ] Compras, entrada NF, BI, lançamentos (fases seguintes)

**Outras:**

- [x] **PDV legado carrinho GM** — produção `59bdedc` v1.02 (2026-06-18)
- [x] **Lançamentos CP — perf abertura Chrome** — bootstrap + prefetch + cache (`teste` v1.48); Renan OK melhora sutil; **próximo salto adiado**
- [ ] **Lançamentos CP perf — roadmap adiado** — retomar: (A) `agro_pg` financeiro **ou** (B) lista no BI sem navegar **ou** (C) enxugar página CP. **Não** micro-otimizar até Renan pedir.
- [ ] **Layout novo CP + perf lista** → produção (cherry-pick quando Renan pedir)
- [ ] **PDV wizard carrinho GM** — em staging `teste`; Renan validar → cherry-pick produção quando pedir
- [ ] Dedupe clientes Mongo vs ERP por CPF (futuro)
- [ ] Tela contabilidade ligada ao export XML mensal (usuário indicará layout)
- [ ] **Merge NFC-e → `producao`** após OK Renan + checklist `docs/NFCE-PRODUCAO.md`
- [ ] Testes automatizados sync clientes / NFC-e (futuro)

### Instruções para o assistente (próxima atualização)

**Atualização automática (padrão — não perguntar ao Renan):**

Ao **entregar** fix, feature ou deploy (teste ou produção) → **editar `banana.md`**: CHECKPOINT (o quê, commits, `VERSION`, teste OK, produção se houver, dica de revert) + § do módulo se for regra permanente. **Obrigatório** para qualquer mudança de comportamento do sistema. **Não** pedir autorização. **Não** registrar chat só explicativo ou detalhe passageiro.

**Quando Renan pedir *"atualize a banana"* (ou revisão explícita):**

1. Ler `git log teste --oneline -20` e `git status`.
2. Atualizar seções **4** (módulos afetados), **8** (linha do tempo) e este **CHECKPOINT**.
3. Incrementar versão do checkpoint: patch = pequenos fixes; minor = módulo novo; major = reestruturação do doc.
4. Mover itens de **WIP** para **coberto** após commit.
5. **Pendências:** manter lista viva — abrir/fechar conforme estado real (não só o que Renan disser na hora).
6. **Não** inflar o doc: manter tabelas; detalhe longo vai para doc irmão ou AGENTS.md §7.
7. **Nunca** perguntar ao Renan se deve atualizar o `AGENTS.md`.

### Fim do checkpoint v1.0.45

*Próxima edição começa abaixo desta linha ou substituindo o bloco CHECKPOINT acima.*