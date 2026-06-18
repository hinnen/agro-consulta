# BANANA — contexto SisVale (anexe com `@banana`)

**Este é o único anexo obrigatório** no dia a dia. O `AGENTS.md` é enciclopédia (referência profunda); o Cursor já carrega um resumo via `.cursor/rules/agro-consulta.mdc`.

| Você quer… | Faça |
|------------|------|
| Começar qualquer tarefa | Anexe `@banana` + descreva a tarefa |
| Detalhe fino de UX / RH / Lançamentos | Some `@AGENTS.md` (§5–11, §9, §10) |
| Registrar onde paramos | Automático no fim da sessão; ou *"atualize a banana"* |
| Decisão permanente no changelog | *"atualize o AGENTS"* (raro; só quando você pedir) |

**Assistente:** não perguntar ao Renan se deve atualizar o `AGENTS.md`. WIP, roadmap e checkpoint vão no `banana.md` **sem pedir** quando for pertinente (ver CHECKPOINT).

---

## 0. TL;DR (leia em 1 minuto)

| Item | Valor |
|------|--------|
| **Produto** | **SisVale** / **Agro Consulta** — sistema web da **GM Agro** (loja agropecuária, Jacupiranga-SP) |
| **Usuários** | Operadores de loja (PDV, caixa), gestão, financeiro, RH, compras |
| **Stack** | Django + Postgres (Agro) + Mongo (espelho ERP) + Render + Electron opcional |
| **Branch dia a dia** | `teste` → staging · merge `teste`→`producao` só quando Renan pedir |
| **Tela inicial** | `/` = BI gerencial · PDV principal em `/consulta/` e wizard `/pdv/checkout/` |
| **Regra de ouro** | Operador usa **saldo do Agro**; ERP alimenta Mongo; Agro não devolve estoque ao ERP |
| **UX loja** | Compacto, alto contraste, teclado/scanner primeiro, paleta emerald/orange/slate |
| **Escala de tela** | **Agro Display Scale** global (não zoom do Chrome) — ver AGENTS.md §11 |

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

**Renan (dono/dev):** testa no staging, aprova merge produção. O assistente **commita só em `teste`**; produção só com pedido explícito.

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

| Base | O quê fica aqui |
|------|-----------------|
| **Mongo** | Catálogo ERP (`DtoProduto`), estoque depósito, financeiro (`DtoLancamento`), vendas ERP históricas |
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

| Ambiente | Branch | Render |
|----------|--------|--------|
| Teste | `teste` | agro-consulta-staging |
| Loja | `producao` | Sistvale - Produção |

- **`main` / `principal` não entram no deploy.**
- Fluxo: push `teste` → testar staging → PR/merge `teste`→`producao` quando Renan autorizar.
- Após merge: `python manage.py migrate` no ambiente (Render faz no deploy).

### 3.1 Versão do sistema (automática — opção A)

| O quê | Detalhe |
|-------|---------|
| **Arquivo** | `VERSION` na raiz (ex.: `1.02`) |
| **Badge BI** | `v` + conteúdo de `VERSION` (sem terceiro número) |
| **Commit git** | Popup do badge no dashboard (automático no Render) |
| **Quando sobe** | **Cada commit** em `teste` ou `producao`: `1.01` → `1.02` → `1.03` |
| **Como** | Hook `.githooks/pre-commit` → `scripts/bump_version.py --hook` |
| **Setup (1× por clone)** | `powershell scripts/setup_git_hooks.ps1` |
| **Pular 1 commit** | `SKIP_VERSION_BUMP=1 git commit …` ou já incluir `VERSION` no stage |
| **Sem hook** | Assistente **deve** rodar `python scripts/bump_version.py` e incluir `VERSION` ao commitar |

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

| Tela | URL | JS principal |
|------|-----|--------------|
| PDV legado MPA | `/consulta/` | `consulta_produtos.js` |
| PDV wizard (checkout) | `/pdv/checkout/` | `pdv_wizard.js`, `pdv_state.js` |

**Fluxo típico:** busca produto → carrinho → cliente (opcional) → pagamento → confirma → cupom/impressão.

**Regras UX já decididas:**

- **F1** volta ao PDV preservando draft/filtros/scroll.
- Entrega: fluxo inline na etapa (sem modais empilhados).
- Endereço de entrega oculto até concluir pagamento da entrega.
- Barra de estoque: atualização manual + horário + standby.

**APIs PDV (amostra):** `api/buscar/`, `api/pdv/*`, `api/promocoes/ativas-pdv/`, Mercado Pago Point em `views_mp_point.py`.

**Armadilha GM no barras (2026-06-18):** se «Código de barras» no cadastro tiver texto **GM** (ex. `GM1546-5S`), o leitor manda GM, não EAN. No **wizard** (`pdv_wizard.js`), o hífen do GM disparava atalho **`-`** = remover último item do carrinho (campo mostrava `GM15465S`). Patch: ignorar `-`/`+` durante SKU/GM + modo barcode para `GM…`. Legado `/consulta/`: F4 pós-bip + match alnum (`consulta_produtos.js`).

**Venda gravada em:** `VendaAgro` (Postgres) + tentativa sync ERP conforme config.

### 4.3 NFC-e (cupom fiscal 65)

**Emissão pelo Agro**, não pelo ERP. Série **21** no Agro; ERP continua série **20**.

| Modo `NFC_E_MODO` | Comportamento |
|-------------------|---------------|
| `manual` (padrão) | **PIX / cartão** → NFC-e automática ao confirmar (sem popup CPF; sem identificação se cliente sem CPF). **Dinheiro / fiado / vale** → popup **NFC ou Venda**; se NFC → modal CPF grande ou «Sem CPF na nota». |
| `auto` | NFC-e em toda venda |

**UX PDV (2026-06-18):** modal CPF **grande** (`max-w ~54rem`, fontes `clamp`). Reemissão em `/vendas/` → após autorizar pergunta **Imprimir cupom / Agora não**. Aviso pós-venda NFC-e falhou: toast **no topo**, depois da janela de impressão Windows.

**SEFAZ:** rejeição **225** corrigida com grupo `<card><tpIntegra>2</tpIntegra></card>` em PIX/cartão (NT 2024.003) + `vTotTrib` por item.

**Arquivos centrais:**

| Arquivo | Papel |
|---------|--------|
| `produtos/nfce_config_util.py` | Env vars, resumo config |
| `produtos/nfce_sp_emissao_util.py` | XML, SOAP SEFAZ SP, assinatura, QR Code |
| `produtos/nfce_cupom_util.py` | Cupom térmico 80 mm |
| `produtos/nfce_ibpt_util.py` | Tributos Lei 12.741 |
| `produtos/nfce_venda_util.py` | Painel status por venda |
| `produtos/views_nfce.py` | Rotas API e contabilidade |
| `produtos/models.py` | `NfceDocumentoAgro`, `VendaAgro.nfce_solicitada` |

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
- **`editado_local=True` não é sobrescrito** na sync.
- PDV lista/busca clientes **só no Agro** (`api/listar-clientes/`, `api/buscar-clientes/`).
- IDs Mongo no JSON viram `local:{pk}` para não mandar ObjectId ao ERP.
- Contexto antigo detalhado: `docs/CONTEXTO_SESSAO_CLIENTES_PDV.md`.

### 4.6 Cadastro / gestão de produtos

**Duas telas — não confundir:**

| Tela | URL / API | Uso |
|------|-----------|-----|
| **Cadastro ERP** (SisVale) | `/produtos/cadastro-erp/`, `api_produtos_cadastro` | Lista/busca Mongo **sem saldo**; Excel import/export |
| **Gestão operacional** | `produtos_gestao.html`, `api_produtos_gestao_lista` | Saldo, facetas, operação loja |

**Excel fase 1:** export com colunas/categorias; import async com histórico e desfazer; ID oculta; Código GM editável; célula vazia não altera.

**Lentidão pós-entrada NF (investigação aberta):** medir qual URL trava no browser; suspeitos: `api_produtos_gestao_facetas`, pool Mongo.

### 4.7 Entrada de nota fiscal

- `/entrada-nota/` — wizard 7 passos (fornecedor → produtos → barras → lote/validade → estoque → financeiro → PIN).
- Pré-visualização XML: modal drag-and-drop, não fecha ao clicar fora; «Confirmar na grade» aplica de fato.

### 4.8 Estoque Agro

- Saldo PDV = Mongo ERP + `AjusteRapidoEstoque`.
- Painel: `/estoque/sincronizacao/`.
- Doc: `docs/ESTOQUE_AGRO_FONTE_DA_VERDADE.md`.
- Cron: `estoque_mongo_ping` a cada 10 min no Render.

### 4.9 Compras

- `/compras/` — sugestão, horizonte em dias, métricas avançadas em `<details>`.
- Relatórios: A4 fornecedor, planilhas impressas por categoria/unidade (A4 ou A6).

### 4.10 Lançamentos / financeiro

- `/lancamentos/` — hub · **Contas a pagar padrão:** `/lancamentos/contas-pagar/` (**layout novo**) · clássico: `/lancamentos/contas-pagar/classico/` · `/teste/` → redirect
- Contas a receber: `/lancamentos/contas-receber/` (layout clássico)
- PDF: `lancamentos_financeiro_pdf.py` (sem coluna observações longas; forma pagamento; bruto destacado).
- Busca na lista: termos com espaço; valor em bruto/pago/saldo. Ajuda: `includes/lancamentos_help_agents.html`.
- **Layout novo CP:** `lancamentos_contas_pagar_teste.html` — API `/api/lancamentos/`; filtros na URL; recarga in-place preserva scroll/filtros/páginas.
- **Perf lista (2026-06-19):** projeção slim Mongo; `skip_totais` pág. 2+; cache sessionStorage; planos lazy.
- **Nova saída** (modal) + **Lote manual** (`/lancamentos/novo-manual/`): pseudo-plano **«Empréstimo (entrada + pagamento)»** — gera receita quitada (hoje) + despesa(s); se saída &gt; entrada, diferença em **Juros de Empréstimos**. JS: `lancamento_emprestimo_dual.js`; backend: `expandir_linhas_emprestimo_dual_lote` em `mongo_financeiro_util.py`.

### 4.11 Caixa

- **Gaveta** = turno principal · **Notebook** = vínculo sem sessão própria · **Teste** = isolado (`ponto_caixa=teste`), fora do fechamento em lote.
- Layout **16:9**, shell `.caixa-shell`, `100dvh` — não coluna estreita.
- Util: `produtos/caixa_util.py`.

### 4.12 RH

- `/rh/` — folha, vales, ficha.
- Ajuda longa em `rh/templates/rh/includes/rh_help_agents.html` (espelho AGENTS.md §9).
- Vale com financeiro = baixa parcial no título de salário do mês (precisa folha fechada com título).
- Cancelar vale: motivo mín. 3 chars; recalcula folhas abertas.

### 4.13 Electron (desktop)

- `electron/main.js` + `preload.js` — `shell.openExternal` para WhatsApp/Maps.
- Mesmo Agro Display Scale via `localStorage` (não `setZoomFactor` paralelo).

### 4.14 UI compartilhada

- `_agro_consulta_ui.html` — base visual.
- `_agro_display_scale.html` + `agro_display_scale.js` — escala global.
- `_agro_open_external.html` — links externos.

### 4.15 Desvinculação ERP (Mongo espelho → Postgres SisVale)

**Dois níveis:** (1) cortar **API ERP** (`Produtos/Salvar`, etc.); (2) parar de **ler/gravar no Mongo** na tela — ganho de **responsividade** e menos bugs (ex.: preço que “voltava”).

**Flags** (`produtos/agro_fonte_config.py`, `.env`): `AGRO_FONTE_CATALOGO=agro_pg` · `AGRO_FONTE_ESTOQUE=ledger` · `AGRO_FONTE_FINANCEIRO=agro_pg` · `AGRO_CADASTRO_PRODUTO_ERP_SYNC_HABILITADO=false` (padrão). Status debug: `GET /api/agro/fonte-status/`.

| Status | Tela / módulo | Nota |
|--------|----------------|------|
| **Feito** | Clientes PDV, Vendas Agro, NFC-e, Caixa, RH (quase todo Postgres) | Sync ERP opcional onde existir |
| **Feito (parcial)** | Cadastro SisVale `/produtos/cadastro-erp/` | API ERP cortada; busca rápida (cache + `/api/buscar/`); preço espelha no Mongo ao salvar. **Lista/facetas ainda Mongo** |
| **Infra pronta, off** | Catálogo Postgres | `catalogo_agro.py`, modelo `Produto`, `importar_catalogo_mongo_produto` — flag `agro_pg` só em cadastro lista/detalhe/Excel |
| **Infra só flag** | Estoque ledger, Financeiro Postgres | Flags existem; **não ligadas** nas views |
| **Falta (alta)** | PDV `/consulta/`, wizard `/pdv/checkout/` | `/api/buscar/`, cache catálogo, saldos, médias → Mongo |
| **Falta (alta)** | Gestão operacional `produtos_gestao.html` | `gestao/lista` + **`gestao/facetas`** (vários `distinct`) — suspeito lentidão pós-entrada NF |
| **Falta (média)** | Entrada NF, Compras, Estoque/transferências, Validade | Buscas e agregações no espelho |
| **Falta (grande)** | Lançamentos (todas), BI `/`, Fiado, resumo financeiro | `mongo_financeiro_util` + `DtoVenda` |

**Ordem sugerida:** cadastro lista → `agro_pg` → PDV `/api/buscar/` → gestão operacional → estoque ledger → **financeiro** (backup + checkpoint na tela Lançamentos).

### 4.16 Lançamentos — corte ERP (em preparação)

Fluxo **seguro** (só admin vê os botões):

1. **Backup ZIP** — CSV no PC (**só redundância**; SisVale não importa de volta).
2. **Checkpoint** — carimbo no Mongo; **não apaga nem muda valores**.
3. **Corte Agro→ERP** — após checkpoint, SisVale **não envia** lançamento/baixa pela API (`VendaERPAPIClient`); automático no código **v1.14**.
4. **Opcional Render** — `AGRO_FINANCEIRO_MONGO_CONGELADO=true` (reforço).

**Backup no PC** — cópia de segurança fora do sistema. Checkpoint = carimbo dentro do SisVale.

**Corte ERP:** é a **nossa** integração (API aberta WL), não ticket ao fornecedor. Padrão já era `AGRO_FINANCEIRO_ERP_SYNC_HABILITADO=false`; após checkpoint o bloqueio é garantido no código.

**Armadilha cherry-pick:** se `lancamentos_financeiros.html` incluir `lancamentos_pin_entrada.html`, o template **tem** que ir junto — senão **500** em Contas a pagar/receber.

- **PIN Lançamentos:** 1× por sessão ao entrar no módulo (`sessionStorage`); navegação interna (CP, clássico, calendário…) sem novo PIN; **modo descanso** (~3 min idle) pede de novo; sair para PDV/outra tela limpa a sessão.

Rotas: `backup-completo.xlsx` · `backup-abertos.zip` · `congelamento-status/` · `congelar-pre-corte/`. Painel na entrada `/lancamentos/`.

---

## 5. Variáveis de ambiente importantes

| Variável | Efeito |
|----------|--------|
| `AGRO_STAGING_READONLY` | Bloqueia escrita Mongo no staging |
| `AGRO_ERP_PEDIDOS_DRY_RUN` | Não grava pedidos ERP no staging |
| `NFC_E_*` | Toda config NFC-e (ver NFCE-PRODUCAO.md) |
| `AGRO_DASHBOARD_GASTOS_PLANO` | Mostra gráfico gastos por plano no BI |
| `AGRO_RH_PLANO_SALARIO_FOLHA` | Plano Mongo do título de salário |
| `ALERTA_VENDAS_CRON_TOKEN` | Token crons HTTP |

---

## 6. Como trabalhar com o assistente (Renan ↔ Cursor)

1. **Anexar contexto:** só `@banana` na maioria dos chats. `@AGENTS.md` é opcional (UX §11, RH §9, Lançamentos §10, tarefa muito ampla).
2. **Dois arquivos, papéis diferentes:** banana = memória viva (WIP, pendências, checkpoint). AGENTS = manual estável (não duplicar aqui).
3. **Atualizar docs:** assistente **atualiza `banana.md` sozinho** quando houver decisão, pendência, roadmap ou WIP importante (ver CHECKPOINT — *atualização automática*). Renan pode pedir *"atualize a banana"* também. AGENTS §7 **só** se Renan pedir — assistente **nunca** pergunta no meio da tarefa.
4. **Escopo:** pedir arquivos ou módulo; assistente não amplia sem autorização.
5. **Antes de editar:** assistente deve dar **uma linha de plano**.
6. **Entrega:** um patch coeso por tarefa.
7. **Commits:** só quando Renan pedir; branch `teste`. **Incluir bump de `VERSION`** (hook faz sozinho se `setup_git_hooks` rodou; senão `python scripts/bump_version.py` + `git add VERSION`).
8. **Produção:** só merge quando Renan disser *"pode ir para produção"* ou similar.
9. **Modo econômico:** Renan pode pedir respostas curtas.
10. **Retomar trabalho antigo:** módulo + este arquivo; chats anteriores não ficam na memória do assistente.

---

## 7. Documentos irmãos (não repetir aqui)

| Arquivo | Conteúdo |
|---------|----------|
| `AGENTS.md` | Enciclopédia — mapa URLs, UX §5–11, changelog §7. Referência, não anexo obrigatório. |
| `docs/DEPLOY-AMBIENTES.md` | Staging readonly, fluxo Git |
| `docs/NFCE-PRODUCAO.md` | Checklist fiscal produção |
| `docs/ESTOQUE_AGRO_FONTE_DA_VERDADE.md` | Regra estoque |
| `docs/CONTEXTO_SESSAO_CLIENTES_PDV.md` | Sessão clientes (histórico) |
| `docs/GUIA_ABAS_NAVEGADOR_AGRO.md` | Abas navegador |
| `.cursor/rules/agro-consulta.mdc` | Regra Cursor resumida (auto-carregada) |

---

## 8. Linha do tempo recente (commits `teste`)

Últimos temas entregues (mais recente primeiro):

1. **Lançamentos CP — layout novo padrão + vista preservada + perf lista** — 2026-06-19 (CHECKPOINT).
2. **PDV wizard — GM no barras remove carrinho** — `e055761` · `pdv_wizard.js`: hífen `GM1546-5S` não remove carrinho; GM modo barcode.
3. **Lançamentos — Empréstimo (entrada + pagamento)** — pseudo-plano Nova saída + lote manual (`fbccf19`).
3. **Lançamentos — Nova saída (legibilidade)** — card expandido; fontes/campos maiores.
4. **Etiquetas faixa 230…** — `5c6590a` v1.20: CODE128 interno loja.
5. **PDV legado carrinho GM** — produção `59bdedc` v1.02.

*Para lista completa:* `git log teste --oneline -50`.

---

<!-- ═══════════════════════════════════════════════════════════════════ -->
<!-- CHECKPOINT — O ASSISTENTE ATUALIZA A PARTIR DAQUI PARA BAIXO      -->
<!-- ═══════════════════════════════════════════════════════════════════ -->

## CHECKPOINT DE ATUALIZAÇÃO

**Versão:** `1.0.13`  
**Última atualização:** `2026-06-19`  
**Atualizado por:** assistente Cursor (Lançamentos CP layout + perf + vista → `teste`)  
**Versão app (`VERSION`):** **produção v1.33** (Lançamentos CP layout + PIN + filtro hoje) · staging `teste` alinhado

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
- [x] **Lista CP — perf + preservar filtros/vista** após baixa/NF/Nova saída (2026-06-19)
- [x] **PIN Lançamentos** — só na entrada do módulo + descanso; abertura **hoje / em aberto** (2026-06-19)

### Lançamentos — Contas a pagar layout novo (2026-06-19)

| URL | Tela |
|-----|------|
| `/lancamentos/contas-pagar/` | **Layout novo** (padrão) |
| `/lancamentos/contas-pagar/classico/` | Tabela clássica |
| `/lancamentos/contas-pagar/teste/` | Redirect → padrão |

**Mesma API** `/api/lancamentos/`. Botões **Layout clássico** ↔ **Layout novo**. Editar/excluir no clássico: `?retorno=` mantém filtros ao voltar.

**Perf:** projeção slim Mongo; `skip_totais` pág. 2+; cache sessionStorage; planos lazy.

**Vista:** baixa, NF, Nova saída recarregam in-place (scroll, expandidos, «carregar mais», URL).

**Abertura:** lista padrão = **em aberto · vencimento hoje** (sem filtros na URL). Deep link / filtros salvos na URL respeitados.

**PIN:** uma vez ao entrar em Lançamentos (hub `/lancamentos/`); navegação interna sem repetir; só de novo no **modo descanso** (idle). Launcher BI não pede PIN antes do módulo.

**Arquivos:** `lancamentos_contas_pagar_teste.html`, `mongo_financeiro_util.py`, `views.py`, `urls.py`, `lancamentos_financeiros.html`, `lancamentos_contas_pagar_calendario.html`, `includes/lancamentos_pin_entrada.html`, `_screensaver_pin.html`.

**Produção:** merge `teste`→`producao` 2026-06-19 (Renan pediu).

### NFC-e — status staging (2026-06-18)

- [x] **Reemissão** em Consultar vendas — Renan confirmou funcionando
- [x] Erro **225** — `card/tpIntegra=2` + IBPT por item
- [x] **PIX/cartão** — sem modal CPF; emite sem identificação
- [x] **Dinheiro** — popup NFC/Venda; modal CPF **grande** (v1.11+)
- [x] Toast falha fiscal **depois** da impressão Windows
- [ ] Merge **`producao`** — só quando Renan pedir + checklist `docs/NFCE-PRODUCAO.md`

### Lançamentos — Empréstimo (entrada + pagamento) — feito 2026-06-18

**Onde:** modal **Nova saída** em **Lançamentos** (`/lancamentos/`) + **Lote manual** (`/lancamentos/novo-manual/`). No **BI** (`/`): atalho só no card **Contas a Pagar** (sem botão na barra PDV/Orçamento/Menu).

**Plano na lista:** `Empréstimo (entrada + pagamento)` (buscar «emprest»).

| Campo | Entrada (receita) | Saída / pagamento |
|-------|-------------------|-------------------|
| Loja, pessoa | sim | sim |
| Conta, forma | não | sim |
| Comp./venc. | **hoje** (auto) | o que preencher |
| Quitado | **sempre** | chip «Quitado» só aqui |
| Recorrência | desligada | — |

**Valores:** entrada + saída. Se **saída &gt; entrada** → título extra em **Juros de Empréstimos** (diferença). Pagamento principal = valor da entrada quando há juros.

**Toggle Pagar/Receber:** ignorado neste modo (gera receita + despesa no mesmo envio).

**Ajuda UI:** textos longos removidos do card; botão **?** no cabeçalho (só quando o plano empréstimo dual está ativo).

**Produção:** `d75c436` v1.10 (2026-06-18) — cherry-pick escopo fechado; **não** inclui PDV wizard nem merge `teste` inteiro.

### URGENTE — PDV carrinho some ao bipar GM no barras (2026-06-18)

| Onde | Sintoma | Status |
|------|---------|--------|
| **Wizard** `/pdv/checkout/` | Cada bipe **GM1546-5S** remove **1 item** do carrinho (4→3→2→1) | **Staging `teste`** — aguarda teste Renan |
| **Legado** `/consulta/` | Carrinho zerava ou perdia itens (F4 pós-bip, match parcial) | **Produção** `59bdedc` v1.02 |

**Caso Renan (Ibiúna ensacada):** produto **1467** — GM **`GM1546-5S`** erroneamente no campo **Código de barras** (aba Fiscal). Leitor manda `GM1546-5S`; campo de busca mostra **`GM15465S`** (hífen engolido).

**Causa wizard:** atalho **`-`** na busca = `bumpLastCartItem(-1)` (menos qty do **último** item). O hífen do GM disparava remoção **sem** inserir o carácter.

**Patch wizard (`pdv_wizard.js`, em `teste`):**

- `-` / `+` ignorados enquanto digita GM/SKU ou janela pós-bip (1,5 s)
- Códigos **`GM…`** → modo **barcode** (auto-adiciona)
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

| O quê | Onde |
|-------|------|
| Código GM | **Texto** abaixo do barras (referência humana) |
| EAN 8/12/13 | **Dentro** do barras — é isso que o leitor deve mandar |
| Etiquetas antigas / já impressas com GM no barras | PDV aceita GM (patch carrinho); **reimprimir** quando houver EAN no cadastro |
| Ibiúna ensacada **GM1546-5S** (prod. 1467) | Barras errado no cadastro — corrigir Fiscal → EAN/`230…` → reimprimir |

**Operação:** antes de imprimir lote, abrir 🖨️ no cadastro — se aparecer aviso âmbar «Sem EAN», corrigir cadastro primeiro.

### Código de barras interno da loja (decisão 2026-06-18)

Nem todo item tem EAN de fábrica. Casos normais na GM Agro:

| Situação | Barras no cadastro |
|----------|-------------------|
| Embalagem do **fornecedor** (NF, saco fechado) | EAN/GTIN **real** da embalagem quando existir |
| Saco **maior** com EAN, **unidade de dentro** sem barras (ex. ensacado na loja) | **Código interno** numérico criado pela loja — não é EAN do fornecedor |
| **Reembalagem** na loja (ração a granel, fracionado) | Idem — código interno (7 dígitos, 13 com DV válido, etc.) |

**Não** confundir: «inventar» aqui = **identificador interno SisVale/loja**, não falsificar GTIN de fabricante. PDV e etiqueta tratam 4–14 dígitos como barras normal (CODE128); EAN-13 de **fábrica** exige DV correto (SisVale corrige DV na impressão se estiver errado e avisa no modal).

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

| Arquivo | Papel |
|---------|--------|
| `consulta_produtos.js` | Fix carrinho + GM |
| `_js_busca_produto_inteligente.html` | Match GM |
| `pdv_diagnostico_codigo.py` | Diagnóstico Mongo |
| `produtos_etiquetas_core.js` + modal cadastro | EAN no barras |

**Loja:** Ctrl+F5 no `/consulta/` após deploy Render.

### WIP / não commitado (snapshot 2026-06-19)

| Arquivo | Tema |
|---------|------|
| `AGENTS.md` | Nota `@banana` vs enciclopédia (local) |

**Acabou de subir em `producao`:** Lançamentos CP (layout + perf + vista + PIN entrada + filtro hoje) · v1.33.

**Teste Renan (staging):** `/lancamentos/contas-pagar/` → filtrar → baixa/Nova saída → filtros e scroll mantidos.

### Pendências conhecidas (produto)

**Desvinculação ERP (responsividade)** — ver §4.15–4.16:

- [x] Backup ZIP + checkpoint — **feito** 2026-06-19 (~17 703 títulos)
- [x] Corte Agro→ERP (API) — **produção v1.14** (`372f90f`; automático após checkpoint)
- [ ] Próxima fase: `AGRO_FONTE_FINANCEIRO=agro_pg` (Lançamentos ler/escrever Postgres, não espelho Mongo)
- [ ] **Nunca** merge `teste` inteiro em `producao` — só cherry-pick do escopo combinado
- [ ] Ativar catálogo Postgres: `importar_catalogo_mongo_produto` + `AGRO_FONTE_CATALOGO=agro_pg`
- [ ] PDV `/api/buscar/` e cache → Postgres
- [ ] Gestão operacional (lista + facetas) → Postgres
- [ ] Estoque `ledger` e financeiro `agro_pg` (implementar flags)
- [ ] Compras, entrada NF, BI, lançamentos (fases seguintes)

**Outras:**

- [x] **PDV legado carrinho GM** — produção `59bdedc` v1.02 (2026-06-18)
- [ ] **Layout novo CP + perf lista** → produção (cherry-pick quando Renan pedir)
- [ ] **PDV wizard carrinho GM** — em staging `teste`; Renan validar → cherry-pick produção quando pedir
- [ ] Dedupe clientes Mongo vs ERP por CPF (futuro)
- [ ] Tela contabilidade ligada ao export XML mensal (usuário indicará layout)
- [ ] **Merge NFC-e → `producao`** após OK Renan + checklist `docs/NFCE-PRODUCAO.md`
- [ ] Testes automatizados sync clientes / NFC-e (futuro)

### Instruções para o assistente (próxima atualização)

**Atualização automática (padrão — não perguntar ao Renan):**

Ao **encerrar tarefa** ou **fechar tópico importante**, se a sessão alterou decisão de produto, pendência, WIP, roadmap (ex. desvinculação), diagnóstico de bug recorrente ou entrega em módulo grande → **editar `banana.md`** (CHECKPOINT + § do módulo). **Não** pedir autorização. **Não** atualizar por detalhe passageiro ou chat só explicativo.

**Quando Renan pedir *"atualize a banana"* (ou revisão explícita):**

1. Ler `git log teste --oneline -20` e `git status`.
2. Atualizar seções **4** (módulos afetados), **8** (linha do tempo) e este **CHECKPOINT**.
3. Incrementar versão do checkpoint: patch = pequenos fixes; minor = módulo novo; major = reestruturação do doc.
4. Mover itens de **WIP** para **coberto** após commit.
5. **Pendências:** manter lista viva — abrir/fechar conforme estado real (não só o que Renan disser na hora).
6. **Não** inflar o doc: manter tabelas; detalhe longo vai para doc irmão ou AGENTS.md §7.
7. **Nunca** perguntar ao Renan se deve atualizar o `AGENTS.md`.

### Fim do checkpoint v1.0.13

*Próxima edição começa abaixo desta linha ou substituindo o bloco CHECKPOINT acima.*
