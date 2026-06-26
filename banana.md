# BANANA — contexto SisVale (anexe com `@banana`)

**Este é o único anexo obrigatório** no dia a dia. O `AGENTS.md` é enciclopédia (referência profunda); o Cursor carrega regra em `.cursor/rules/agro-consulta.mdc` (**§0 = ler este arquivo inteiro com Read antes de qualquer coisa**).


| Você quer…                            | Faça                                                  |
| ------------------------------------- | ----------------------------------------------------- |
| Começar qualquer tarefa               | Anexe `@banana` + descreva a tarefa                   |
| Detalhe fino de UX / RH / Lançamentos | Some `@AGENTS.md` (§5–11, §9, §10)                    |
| Registrar onde paramos                | Automático no fim da sessão; ou *"atualize a banana"* |
| Decisão permanente no changelog       | *"atualize o AGENTS"* (raro; só quando você pedir)    |


**Assistente:** **sempre ler o `banana.md` inteiro** com **Read** (sem `limit`) **antes** de qualquer tarefa — **1ª ação do chat**; reler inteiro se o chat ficou longo ou houve resumo de contexto. Registrar alterações no `banana.md` quando necessário **sem pedir**. Não perguntar ao Renan se deve atualizar o `AGENTS.md`. WIP, roadmap e checkpoint vão no `banana.md` quando pertinente (ver CHECKPOINT). Detalhe na rule Cursor **§0**.

**Comunicação com o Renan:** **sempre em português (BR).** Respostas **curtas e em linguagem de loja** — só o que for **estritamente importante** para decidir ou operar. **Evitar** nomes de arquivo, flag, API e detalhe de código **salvo se ele pedir** ou for indispensável numa instrução (ex.: uma linha no `.env`).

**Produção (regra dura — 2026-06-22, senha 2026-06-24):** **Nunca** `git push origin producao`, merge `teste`→`producao`, cherry-pick na loja ou deploy Render de produção **sem as duas coisas abaixo na mesma mensagem do Renan:**

1. Frase explícita — *«pode subir (para produção)»* / *«pode ir para produção»* / *«sobe pacote vX.XX para produção»* (ou equivalente claro).
2. **Senha de autorização loja** — Renan digitou no chat **2026-06-24** (madrugada, chat PDV): **`99738595`**. Tem que aparecer **no mesmo pedido** que a frase acima. Só a frase **sem** senha = **não sobe**. Só a senha **sem** frase = **não sobe**.

**Ordem:** commit + push em `**teste`** → Renan testa no Render **teste** → **só então** produção, se ele pedir com frase + senha. Corrigir bug ≠ autorização para loja. Loja operando = **zero** push produção por iniciativa do assistente.

**Produção — chat canônico (2026-06-22, Renan):** Push na loja (**SistVale** / branch `producao`) **só neste chat** daqui pra frente. Renan pode ter subido produção em **outro chat** ou **post** — o `banana.md` e o CHECKPOINT são a **fonte da verdade**; ao abrir este chat, o assistente **relê o CHECKPOINT** e **pergunta** se algo mudou antes de cherry-pick. **Não** assumir que «último commit deste chat» = produção.

**Teste (regra — 2026-06-22, reforço Renan 2026-06-17):** Renan **não testa localmente** (parou de usar — local parecia OK e produção quebrava). **Sempre** valida no **Render projeto «teste»** (branch Git `teste`). Quando ele fala *«teste»*, *«staging»* ou *«homologação»*, é **sempre** esse site no Render — **não** máquina local.

**Render teste — assistente manda:** commit + push em `teste` **sempre que achar necessário** (feature pronta, fix, banana atualizada) — **não pedir autorização** ao Renan. Deploy Render segue sozinho; registrar no CHECKPOINT. Renan (2026-06-17): *«para teste pode subir sempre que você achar necessário»*.

**Produção** continua só com frase + senha (§ acima).

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

**Renan (dono/dev):** testa **só no Render «teste»** (não local). Assistente: **commit + push automático em `teste`**; `**producao` só com frase + senha** (ver topo).

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

**Regra Renan (resumo — 23/06/2026):** cada entrega de **código** no **teste** sobe **+0,01** no badge (`2.03` → `2.04` → `2.05` …). A **loja** fica parada no último pacote que você subiu (hoje **v2.03**) até pedir produção — aí a loja **pula** para o mesmo número do pacote (ex. teste já em **v2.05** → sobe pacote → loja vira **v2.05**). Não é **+0,1** (dez centésimos); é **+0,01** (um centésimo). Só `banana.md` / docs **não** contam (`SKIP_VERSION_BUMP=1`).

| O quê | Detalhe |
| ----- | ------- |
| **Arquivo** | `VERSION` na raiz (ex.: `1.93`) |
| **Badge BI** | `v` + conteúdo de `VERSION` em **cada** site (teste e loja têm arquivo próprio no branch) |
| **O que o número significa** | **Rótulo do último pacote** que aquele ambiente exibe — **não** significa que teste e loja rodam o **mesmo código** |
| **Branch `teste`** | Hook sobe **+0,01** a cada commit de **código** (`1.93` → `1.94`). Docs com `SKIP_VERSION_BUMP=1` não contam |
| **Branch `producao`** | No deploy: `VERSION` = número do **pacote que subiu** (ex. NFC-e → loja **v1.93**) |
| **Cherry-pick em lote** | `SKIP_VERSION_BUMP=1` nos intermediários · no fim um commit ajusta `VERSION` na loja |

#### Mesmo número teste e loja — não é o mesmo sistema

| Situação | O que significa |
| -------- | --------------- |
| **teste v2.77 · loja v2.28** (hoje) | Loja: Contabilidade v2.27 + **PDV autocomplete v2.28**. Teste **ainda tem** outros pacotes só no branch teste. |
| **Como saber o que falta na loja** | Tabela **«Só no teste»** no CHECKPOINT · `git diff origin/producao origin/teste --stat` — **não** olhar só o `VERSION` |
| **Regra intuitiva (ideal)** | **`teste` > `producao`** → loja atrás (ex. teste **v1.94**, loja **v1.93** = tem pacote pendente). **`teste` = `producao`** → acabou de subir pacote **ou** teste só ganhou docs sem bump |
| **Próximo fix de código no teste** | Deve virar **v2.06** no teste; loja fica **v2.03** até você pedir produção — aí fica óbvio que não são iguais |

**Por que confundiu:** no deploy alinhamos o **número** (1.93) nos dois lados; o **código** do teste nunca foi todo para a loja (só cherry-picks). VERSION = **etiqueta do pacote**, não diff completo entre branches.

| Erro corrigido (jun/26) | Pacote v1.92 tinha virado loja v1.57–v1.59 (hook no cherry-pick) → manual **1.92**, depois **1.93** NFC-e |
| Conferir | `git show origin/teste:VERSION` · `git show origin/producao:VERSION` · diff `--stat` |

**Regra prática:** deploy loja → CHECKPOINT: **pacote X** (commits) · **teste vX.XX → loja vX.XX**. Pendências → tabela «Só no teste», não só badge.

#### Subir só um pacote no meio (ex. teste já em v1.96, loja só v1.95)

O **número** no teste é histórico de commits; na **loja** você sobe **o pacote que escolher**, não “tudo até a última versão”.

| Exemplo | O que acontece |
| ------- | -------------- |
| Teste | v1.94 (fix A) → v1.95 (fix B) → v1.96 (fix C) — **três commits** |
| Você pede | *«sobe só o 1.95»* (fix B) |
| Assistente | Cherry-pick **só o(s) commit(s) do 1.95** — **não** leva o 1.96 |
| Loja depois | Badge **v1.95** (manual no commit final do deploy) |
| Teste continua | **v1.96** — fix C **ainda só no teste** |
| Leitura | **teste v1.96 > loja v1.95** = falta subir o pacote **1.96** |

**Importante:**

- **Não precisa** existir v1.94 na loja para subir v1.95 (pode pular: loja 1.93 → 1.95).
- O badge da loja = **número do pacote que você subiu**, não “cópia do VERSION do teste no momento”.
- No **banana** registramos: *pacote v1.95 · commit `abc123` · loja não recebeu v1.94 nem v1.96*.
- Se o **1.96 depende** do código do 1.95, o cherry-pick do 1.95 já leva o necessário; o 1.96 em si fica de fora.

**Na prática você diz:** *«pode subir para produção o pacote da v1.95»* (ou descreve o fix). O assistente acha o commit pelo `VERSION` / histórico / banana — **não** faz merge do `teste` inteiro.

#### Recomendação — manter ou mudar? (23/06/2026)

**Para o SisVale hoje: manter este esquema (+0,01, cherry-pick, banana).** Já está no hook, no badge do BI e na rotina teste → loja. **Não** vale trocar por semver grande ou data só por organização — mudaria pouco e geraria retrabalho.

| Camada | Papel |
| ------ | ----- |
| **`VERSION` (+0,01)** | Badge simples na loja (*v1.95*) — operador vê na home |
| **`banana.md` CHECKPOINT** | **Fonte da verdade** — qual commit = qual pacote, o que está só no teste, o que foi para a loja |
| **Cherry-pick** | Sobe **pacote escolhido**, não branch inteira |

**Três disciplinas** (isso é o que organiza de verdade):

1. **Um pacote lógico = um bump no teste** — não misturar dois fixes grandes no mesmo commit se forem pacotes de produção diferentes.
2. **Todo deploy loja** → linha no CHECKPOINT: `v1.95 · commit · o quê · revert como`.
3. **Pedido explícito** — *«sobe v1.95»* ou *«pode subir produção o fix X»*; assistente **não** merge `teste` inteiro.

**Opcional no futuro** (só se quiser mais rigor): `git tag v1.95` no commit do teste ao fechar pacote — facilita achar o cherry-pick. **Não obrigatório** agora.

**Não recomendado aqui:** merge `teste`→`producao` de uma vez; CalVer (`2026.06.23`); número de versão diferente por branch sem regra (voltaria confusão 1.59 vs 1.92).

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

#### Quando emite ou não (resumo operacional)

**Pré-requisito:** `NFC_E_ENABLED=true` e certificado/CSC configurados. Se desligado → **nunca** emite.

| Situação | Emite NFC-e? |
| -------- | ------------ |
| `NFC_E_MODO=auto` | **Sim**, em toda venda confirmada |
| Forma **PIX** ou **cartão** (débito / crédito / parcelado) | **Sim**, automático |
| **Dinheiro**, fiado, vale, cashback, etc. | **Só se o operador escolher** cupom fiscal |
| Operador escolhe **«Venda comum»** (popup de impressão) | **Não** — só cupom não fiscal |
| Venda **sem impressão** + forma manual | **Não** (salvo modo `auto`) |
| Venda **sem impressão** + PIX/cartão | **Sim**, automático |
| **Falha na SEFAZ** | Venda **grava igual**; reemitir em **Consultar vendas** |

#### Popups no PDV (wizard `/pdv/checkout/`)

| Passo | PIX / cartão | Dinheiro e demais |
| ----- | ------------ | ----------------- |
| Confirmar **com impressão** | Sem popup «NFC ou Venda» — vai direto ao fiscal | Popup **Cupom fiscal** ou **Venda comum** |
| Cliente **sem CPF** válido | Modal **Sem CPF na nota** / **Informar CPF** | Idem, se escolheu cupom fiscal |
| Cliente **com CPF** no cadastro | Usa o CPF, sem modal | Idem |

#### Devolução de venda

- Repõe estoque + saída no caixa (como antes).
- Se tinha NFC-e **autorizada** → sistema **tenta cancelar na SEFAZ** com motivo padrão: *«Devolucao de mercadoria registrada no sistema Agro.»*
- **Prazo SEFAZ (NFC-e mod. 65):** cancelamento por evento só até **~30 minutos** após a **autorização do cupom** (regra nacional — NT 2018.004 / Ajuste SINIEF 07/2018). O relógio **não** recomeça na devolução.
- **Erro 501** = prazo esgotado na SEFAZ. Devolução no Agro segue OK; cupom continua autorizado → **NF-e de devolução (mod. 55)** ou contador.
- Status local passa a **Cancelada** quando a SEFAZ aceita.
- Botão **Cancelar NFC-e** na tela da venda (retry manual).

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

**Fantasmas Mongo → Postgres (`agro_pg`, 2026-06-24):**

| O quê | Detalhe |
| ----- | ------- |
| **O que é** | Documento no espelho Mongo com `_id` 24 hex **sem** `Nome` — import virou linha `Produto` com nome «—» e `codigo_interno` = Id |
| **Caso loja** | 3× ibiuna **25 kg** (GM1541/42/46-25); demais variantes OK |
| **Evitar de novo** | `importar_catalogo_mongo_produto` **ignora** fantasma (`deve_ignorar_import_mongo_fantasma`) · nunca grava ObjectId como nome · novo cadastro só via Agro (não duplicar GM no Mongo |
| **Auditar (quantos?)** | `auditar_produtos_fantasma_pg` — **graves** (nome/GM quebrados); `--higiene` = só `codigo_interno`=Id |
| **Corrigir lote** | `python manage.py corrigir_produto_nome_objectid_pg --dry-run` depois sem `--dry-run` |
| **Quando rodar** | Após `importar_catalogo_mongo_produto`, após snapshot staging, se busca cadastro «sumir» produto que existe no PDV |
| **Código** | `catalogo_nome_util.py` · comandos `auditar_*` / `corrigir_*` · exibição API herda irmãos GM (teste v2.50+) |

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
- **Gráfico gastos por plano (2026-06-26):** `/financeiro/grafico-gastos/` — **100dvh sem scroll**; toolbar período simétrica; painel **Filtros | Planos**; **4 atalhos globais** Postgres. **Entrada BI:** botão laranja **Gráfico gastos** no card **Contas a Pagar** (`/`). Teste **v3.24**; loja **v3.02**.

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

**Resposta curta (Jun/2026):** **NÃO terminou.** Validamos **um fluxo operacional** no **teste** (catálogo PG, ledger estoque, Entrada NF wizard). **Loja (produção)** e vários módulos ainda **leem/gravam Mongo ERP** — **não** cancelar assinatura ERP ainda (§ abaixo Renan 2026-06-24).

**Dois níveis:** (1) cortar **API ERP** (`Produtos/Salvar`, etc.); (2) parar de **ler/gravar no Mongo** na tela — ganho de **responsividade** e menos bugs (ex.: preço que “voltava”).

**Flags** (`produtos/agro_fonte_config.py`, `.env`): `AGRO_FONTE_CATALOGO=agro_pg` · `AGRO_FONTE_ESTOQUE=ledger` · `AGRO_FONTE_FINANCEIRO=agro_pg` · `AGRO_CADASTRO_PRODUTO_ERP_SYNC_HABILITADO=false` (padrão). Status debug: `GET /api/agro/fonte-status/`.


| Status                 | Tela / módulo                                                     | Nota                                                                                                                                            |
| ---------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **Feito (Agro PG)**    | Clientes PDV, **Vendas**, **NFC-e**, **Caixa**, **RH**, **Fiado** (`/fiado/`, `FiadoTituloAgro`…) | **Fiado = Postgres nativo** — não usa `DtoLancamento`. Sync ERP opcional onde existir |
| **Teste ✅ / loja parcial** | Cadastro, PDV catálogo (`agro_pg`), Compras D4, ledger | **Loja ✅** desde merge 25/06 · **Gestão lista** ainda Mongo (sem `SOMENTE_POSTGRES`) |
| **Teste ✅**           | Compras D4 **A+B+C**, Entrada NF wizard, ledger saldo             | v2.79–v2.87 · D3 financeiro **dry-run** staging                                                                                                 |
| **Preparado / dados Mongo** | **Lançamentos** CP/CR, DRE, fluxo, Nova saída, lote manual   | **Muito feito** (layout, PIN, perf, backup, checkpoint, corte API v1.14) — **fonte ainda** `DtoLancamento` Mongo · falta `AGRO_FONTE_FINANCEIRO=agro_pg` |
| **Infra só flag**      | Estoque ledger, Financeiro Postgres                               | Ledger **ativo** no estoque Agro; flag `agro_pg` financeiro **não** nas views de Lançamentos                                                   |
| **Falta (alta)**       | PDV/gestão **na loja** (flags B+C)                                | Staging ✅                                                                                                                                       |
| **Falta (média)**      | Busca **GM** Compras/NF · Entrada NF título real (fora dry-run)   | Motor GM por último · NF título hoje Mongo na loja                                                                                              |
| **Falta (grande)**     | **Lançamentos → Postgres** · **BI `/`** · resumo gerencial       | `mongo_financeiro_util` + `DtoVenda` no BI — **não confundir com Fiado** (já Agro)                                                               |
| **Falta**              | Transferências, Validade, sync fornecedor NF                      | Ainda espelho Mongo                                                                                                                             |

**Lançamentos vs desvinculação (Renan 2026-06-25 — não confundir):**

| | **Já fizemos** | **Ainda falta (§4.15)** |
| --- | --- | --- |
| **Lançamentos** | Layout CP, PIN, filtros, Nova saída, empréstimo dual, editar/excluir, backup ZIP, **checkpoint** (~17 703 títulos), **corte sync API ERP** (v1.14), perf bootstrap | Títulos ler/gravar **Postgres Agro** em vez de `DtoLancamento` — flag `agro_pg` **existe, telas não usam** |
| **Fiado** | Gestão `/fiado/`, PDV limite/parcelas, títulos **`FiadoTituloAgro`** Postgres | Nada no pacote «desvincular Mongo catálogo/financeiro». BI ainda pode cruzar `DtoVenda` para não duplicar gráfico — **não é a tela Fiado** |


**Estimativa (Renan — linguagem simples):** pacotes de **dias**, não “meses inteiros” — o grosso já está no teste; falta **subir na loja** + **financeiro/BI** por fatias.


| Etapa | O quê muda na prática                                                                                                  | Risco                                                        | Tempo (dev + teste) |
| ----- | ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | ------------------- |
| **1** | Cadastro Postgres (`agro_pg`)                                                                                          | Médio                                                        | **✅ teste** · 1 dia loja |
| **2** | PDV + gestão mesma base + ledger                                                                                       | Alto no balcão                                               | **✅ teste** · 1–2 dias loja |
| **3** | Compras/NF busca + métricas + Lançamentos + BI                                                                         | Alto no caixa/contas                                         | **2–5 dias por fatia** (CP primeiro) |


**Ordem sprint (Renan — Jun/2026, ~dias):**

| # | Pacote | Por quê nesta ordem | Dias (ordem de grandeza) |
| - | ------ | ------------------- | ------------------------ |
| **1** | **Produção = teste** (flags B+C + ledger + `agro_pg` + Entrada NF v2.78) | Código **já validado** — **agendado loja fecha 25/06 noite** (ver CHECKPOINT) | **1** deploy |
| **2** | **Compras D4** (métricas + folhas) | Última compra / sugestão | **✅ teste** v2.79–v2.87 |
| **3** | **Entrada NF financeiro Agro** (título sem `DtoLancamento` loja) | Wizard OK; título real na loja | **2–3** |
| **4** | **Lançamentos — migração Postgres** (`agro_pg` financeiro) | Checkpoint/corte API **já feitos** · falta **fonte de dados** | **3–5** |
| **5** | **BI `/` + resumo** | VendaAgro Postgres | **2–3** |
| **6** | **Transferências · Validade · fornecedor NF** | Menor urgência | **1–2** cada |
| **7** | **Motor busca único** | **Não é desvinculação** — bug GM Compras/NF (`gm0050`); **por último** | **1–2** |
| **8** | **Checkpoint final + cancelar assinatura ERP** | Só após 1–6 + backup | **0.5** |

**Motor busca ≠ desvinculação:** catálogo/saldo já vêm do Postgres no teste; GM quebrado é **UX/paridade de busca** entre telas — **não bloqueia** cortar Mongo. Pode ficar **último**.

**Ordem técnica legada (referência):** cadastro → `agro_pg` → PDV busca → gestão → ledger → financeiro checkpoint.

---

### Renan — desvinculo: duas listas (28/05/2026)

**Cenário lista 1:** ERP para de enviar dados **ou** Mongo fica inacessível **de repente** (não é corte planejado).

#### 1) O que **para ou fica grave** (perde ERP/Mongo)

| Área | O que acontece |
| ---- | -------------- |
| **Entrada NF** | **Para** se Mongo cair (rascunho da nota). Título a pagar (PG) OK se Mongo só parou de sync |
| **Gestão produtos** | **Para ou fica vazia/lenta** — facetas e parte da lista ainda Mongo |
| **Compras** | Relatórios/dimensões e «última compra» — **param de atualizar** ou quebram sem Mongo |
| **Lançamentos** | **DRE, calendário, export PDF/Excel** — leem Mongo → **param** |
| **Gráfico gastos** | **Para** (só Mongo hoje) |
| **BI `/` + resumo gerencial** | Gráficos/histórico ERP — **param ou ficam velhos** |
| **Transferências / Validade** | **Param ou ficam velhas** |
| **Produtos novos no ERP** | **Não entram** no Agro até import/sync |
| **Listas auxiliares** | Fornecedor, plano de conta, formas — muitas telas ainda puxam Mongo |

**Continua funcionando** (Postgres Agro — operação do dia):

PDV venda · caixa · fiado · clientes · NFC-e · RH · **CP/CR Lançamentos** · saldo operacional (ledger/ajustes) · preço overlay PG · cadastro/PDV catálogo já importado.

---

#### 2) O que **ainda falta** para desvinculo completo (pode cancelar ERP)

| # | Pacote | Status |
| - | ------ | ------ |
| ✅ | Cadastro + PDV catálogo PG + ledger saldo | **Loja** |
| ✅ | Compras D4 (métricas/folhas) | **Loja** |
| ✅ | Entrada NF passo 7 título a pagar | **PG loja** |
| ✅ | Lançamentos CP/CR lista + gravar | **PG loja** |
| ✅ | Fiado, vendas, caixa, RH, NFC-e, clientes | **PG nativo** |
| ✅ | Checkpoint + corte sync API financeiro | **Feito** |
| 1 | **Gestão** 100 % PG na loja (`SOMENTE_POSTGRES`) | Falta |
| 2 | **DRE + calendário + export PDF/Excel** Lançamentos → PG | Falta |
| 3 | **BI `/` + resumo gerencial** → VendaAgro PG | Falta |
| 4 | **Gráfico gastos** → PG | Falta |
| 5 | **Transferências + Validade** | Falta |
| 6 | **Entrada NF rascunho** (nota em andamento) → PG | Opcional / grande |
| 7 | **Compras** dimensões 100 % PG (sem scan Mongo) | Parcial |
| 8 | **Congelar Mongo financeiro** + só histórico | Opcional |
| 9 | **Motor busca GM** Compras/NF | UX — por último |
| 10 | **Cancelar assinatura ERP** | Só após 1–8 estáveis + backup |

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

### Renan — Lançamentos: o que já fizemos e o que falta (linguagem leiga)

**Imagine dois armários de boletos/contas:**

| Armário | O que é |
| ------- | ------- |
| **Mongo (espelho ERP)** | Onde **hoje** ficam ~17 mil contas a pagar/receber que a loja usa |
| **Postgres Agro** | O armário **novo** do SisVale — já existe a gaveta (`TituloFinanceiroAgro`), mas a tela **ainda não abre nele** |

**Dois “cortes” diferentes (não confundir):**

| Corte | Significado | Status |
| ----- | ----------- | ------ |
| **1 — Parar de falar com o ERP** | SisVale **não envia** mais baixa/lançamento para o sistema antigo (API WL) | **✅ Feito** (checkpoint + código v1.14) |
| **2 — Mudar a tela Lançamentos** | Contas a pagar/receber passam a **ler e gravar no Postgres Agro**, não no Mongo | **🔥 HOJE** — escopo **CP primeiro** |

---

**O que JÁ fizemos (Lançamentos + corte ERP):**

1. **Tela nova** — Contas a pagar/receber mais rápida: filtros, PIN, Nova saída, excluir, empréstimo dual, etc.
2. **Backup** — Planilha/ZIP dos títulos guardada (cópia de segurança no PC).
3. **Checkpoint** — “Carimbo” nos ~17 mil títulos no Mongo (**não apaga nada**, só marca).
4. **Corte API ERP** — Depois do checkpoint, o SisVale **não manda** mais lançamento/baixa para o ERP por API.
5. **Gaveta nova no Postgres** — Tabela preparada + comando para **copiar** títulos do Mongo (hoje só **simulação** no teste, não ligou na loja).
6. **Loja hoje** — Operador usa Lançamentos **normal**; boletos e Entrada NF passo 7 **continuam no Mongo** e **funcionam** (Renan validou ✅).

**O que AINDA falta para “terminar o corte” da tela Lançamentos com Mongo:**

| Passo | Em português claro | Onde |
| ----- | ------------------ | ---- |
| **A** | **Copiar** os ~17 mil títulos do Mongo para o Postgres (comando `importar… --apply`) | Primeiro **teste**, depois **loja** |
| **B** | **Reprogramar a tela** — lista, busca, quitar, excluir, Nova saída, DRE, PDF, calendário — para usar o Postgres em vez do Mongo | Dev **teste** |
| **C** | **Entrada NF passo 7** — “Salvar + a pagar” gravar no Postgres, não só no Mongo | Junto com B |
| **D** | **Testar tudo** no staging (criar, pagar, excluir, conferir saldo) | Renan no **teste** |
| **E** | **Ligar na loja** — flag `AGRO_FONTE_FINANCEIRO=agro_pg` **só quando B+C estiverem prontos** | Deploy **produção** + senha |
| **F** | Mongo vira **só histórico** (não entra título novo) — opcional congelar de vez | Depois de E estável |

**Prioridade Renan (25/06 — ordem fixa):**

1. **Contas a pagar EM ABERTO** — máxima segurança · zero perda  
2. **Contas a pagar** (todas)  
3. **Contas a receber** — **por último** (quase não usam; fiado = outra tela)

**Regra:** Mongo **não some** até espelho PG conferido · leitura PG **só** com flag · gravação dupla ou rollback se falhar.

**Escopo HOJE:** CP em aberto → CP lista/filtros → CR depois (se der tempo).

**Fiado não entra** — já Postgres nativo.

---

### WIP HOJE — Lançamentos Postgres (CP primeiro) — **teste OK 26/06**

| Quem | O quê |
| ---- | ----- |
### CP Postgres — o que falta + quem faz (fechar o serviço)

| # | Quem | O quê | Quando |
| --- | ---- | ----- | ------ |
| **1** | **Assistente** | Código: **pagar, parcial, nova saída, editar, excluir** CP → Postgres (teste) | **✅ v3.40 teste** |
| **2** | **Renan** | No **teste**: pagar **1 título pequeno** (ou de teste) + conferir se sumiu da lista / saldo certo | **✅ Renan 26/06** — «Teste» R$ 1,00 quitado PG |
| **3** | **Assistente** | Import PG na **loja** + deploy pacote **teste→producao** | **✅ em curso v3.03** |
| **4** | **Renan** | Frase *«pode subir produção»* + senha **`99738595`** **no mesmo pedido** | **✅ Renan 26/06** |
| **5** | **Renan** | **~30 min** CP só consulta na loja (ou avisar equipe: **não pagar** na CP na hora H) | **✅ 26/06** |
| **6** | Loja: CP aberto **741** + total | **✅ 26/06** — pagamento real **dispensado** |
| **7 CR loja** | Conferência lista CR | **✅ 26/06** — **453** · **R$ 29.241,58** |
| **8 Painel backup** | Card amarelo recolhido (admin) | **✅ v3.08** deploy loja |

**Renan (26/06):** não precisa teste de pagar na CP na loja · **fiado** = operação principal (já Postgres nativo).

### Próximo — finalizar Lançamentos / financeiro (ordem)

| # | O quê | Urgência loja |
| --- | ----- | ------------- |
| **1** | **Entrada NF passo 7** — títulos «Salvar + a pagar» | **✅ já Postgres na loja** (`inserir_lancamentos_manual_lote_dispatch`) · rascunho NF (etapas 1–6) continua Mongo |
| **2** | **DRE / fluxo calendário / PDF** Lançamentos — ler Postgres | Baixa (telas secundárias) |
| **3** | **Esconder painel amarelo** checkpoint (admin) — migração CP/CR fechada | Cosmético |
| **4** | **Congelar Mongo financeiro** (opcional) — título novo só PG; Mongo só histórico | Só quando 1–2 estáveis |
| **—** | **Fiado** | **Nada** — já fora deste pacote |

**Renan NÃO precisa:** mexer Render · import manual · código · backup de novo (já tem no PC).

**Bloqueio CP Postgres (em aberto):** **fechado 26/06** — loja operando CP no Postgres.

### WIP — Contas a receber Postgres **26/06**

| Item | Status |
| ---- | ------ |
| Lista + filtros + planos CR | **✅ código teste** (mesmo PG dos 17,8 mil títulos) |
| Receber / parcial / editar / excluir / lote | **✅ código teste** |
| Fiado | **fora** — tela própria Postgres |
| Deploy loja | **✅ v3.07** — CR conferida **453** / **29.241,58** |


### Renan — intervenção (estritamente necessário)

| # | Status | Detalhe |
| --- | ------ | ------- |
| **1 Backup** | **✅ Renan 25/06** | ZIP abertos + todos no PC |
| **2 Conferir total aberto** | **✅ Renan 25/06** | CP **sem filtro de data** · tela **Qtd 741** · Excel **`01_a_pagar_em_aberto.csv` = 748 linhas** (+7) · total **~R$ 393.652,70** (tela) vs **~R$ 393.667,21** (Excel) · dif. **~R$ 14,51** · **causa: backup sem dedup** (tela funde dup. ERP; ZIP não) · bloco **Geraldo / acordo sat / R$ 600** = mesmo **ID ERP** repetido — linhas extras, saldo pode ser 0 |
| **3 Render env** | **✅ Renan — nada a fazer** | **Não** alterar variáveis no Render (loja nem teste) · passo = **ficar parada** até assistente avisar |
| **4 Pós-import teste** | **✅ Renan 26/06** | Staging CP aberto: **Qtd 741** · **A pagar R$ 393.652,70** — **bate loja** · amostra expandida OK (ex. Renan Hinnen **R$ 163,00**) |
| **4b Pagamento PG teste** | **✅ Renan 26/06** | Título manual **«Teste» R$ 1,00** · quitou no Postgres · sumiu de **Em aberto** · aparece em **Quitados** (saldo 0) · juros R$ 0,10 **não** lançou (conta «ADICIONAR CONTA» — regra esperada) |
| **5 Fiado / CR** | CR **✅ código 26/06** · fiado fora |

**Loja hoje (26/06):** **Contas a pagar liberada** — lista + pagar no **Postgres** (conferido **741** / **393.652,70**). Caixa/PDV normal.

### Por que Contas a pagar **não está 100 % concluída** ainda?

**Não é bug** — foi **de propósito**, em **etapas**, porque são **~17 mil títulos** e **~R$ 393 mil** em aberto.

| Etapa | O quê | Status |
| ----- | ----- | ------ |
| **A** | Backup + conferir total na **loja** | **✅ Renan** |
| **B** | Copiar títulos Mongo → Postgres | **✅ teste** (13,2 mil) |
| **C** | **Lista** CP (ver, filtrar, total) no Postgres | **✅ teste** (741 / 393.652,70 bateu) |
| **D** | **Gravar** no Postgres: pagar, parcial, nova saída, editar, excluir | **✅ teste v3.40** (deploy pendente Render) |
| **E** | Validar **D** no teste (pagar 1 título de teste) | **✅ Renan 26/06** |
| **F** | Import + flag na **loja** (senha) | **✅ v3.04** — **741** / **393.652,70** conferido Renan |

**Por que parou antes de D?**  
Primeiro garantimos: *«a cópia no Postgres é a mesma coisa que a tela da loja?»* — **sim** (passo 4).  
Só **depois** mexemos em **pagamento** — se errar, some dinheiro ou duplica título.

**Metáfora:** armário novo montado e **inventário conferido**; falta passar a **usar o armário de verdade** (cada pagamento sair do Mongo e ir pro Postgres).

**Loja:** CP **Postgres** (lista + pagamento). Mongo **não apagado** — só espelho/backup.

### Renan — como fazer (passos 3 e 4)

**Passo 3 — agora (só não mexer)**

| Faça | Não faça |
| ---- | -------- |
| **Nada** — pode usar a **loja** normal (PDV, CP, etc.) | Abrir Render → **Environment** → **Save** em variável nova |
| Se abrir o Render por curiosidade: **só olhar**, sem salvar | Criar/editar `AGRO_FONTE_FINANCEIRO`, `SOMENTE_POSTGRES`, `STAGING_READONLY`, etc. |
| | Pedir deploy **produção** / merge `teste`→`producao` hoje |

**Passo 3 = já feito** se você **não** alterou env no Render desde o backup.

---

**Passo 4 — quando o assistente avisar «import teste pronto»**

Site = **projeto teste** no Render (branch `teste`) — **não** `sistvale.com.br`.

1. Login **admin** no **teste**.
2. **Ctrl+F5** (recarregar sem cache).
3. Abrir **`/lancamentos/contas-pagar/`**.
4. **Filtros** → Situação **Em aberto** → **limpar** vencimento (de/até **vazios**) → **Marcar todos** planos → **Aplicar**.
5. Conferir rodapé — deve bater com a **loja** (referência anotada):
   - **Qtd: 741** (pode variar ±1 se alguém pagou entre ontem e hoje)
   - **A pagar: ~R$ 393.652,70**
6. **Opcional (amostra):** clique **em cima de 3 linhas** da lista (ex. Detran, Renan Hinnen, Caminhão) — a linha **abre** e mostra detalhes; confira se **saldo** = coluna Saldo (ex. R$ 234,78). **Não precisa pagar** nem editar.
7. Me avisar **«passo 4 OK»** — se **Qtd + A pagar** já bateram (como no print), **pode pular o item 6**.

**Passo 5:** fiado e contas a receber — **ignorar hoje**.

**Assistente (26/06 — técnico):**

| Item | Detalhe |
| ---- | ------- |
| **Código** | `lancamentos_financeiro_pg_util.py` — lista CP Postgres + dedup igual Mongo |
| **API** | `api/lancamentos/contas-pagar/` lê PG quando `AGRO_FONTE_FINANCEIRO=agro_pg` |
| **Import HTTP** | `GET /api/cron/importar-titulos-financeiro-mongo-pg/?token=…` (staging, `DRY_RUN=true`) |
| **Conferência** | `GET /api/agro/financeiro-pg-conferencia/` (superuser) — Mongo vs PG abertos |
| **Flag teste** | Automático no staging após import (`financeiro_postgres: true` no fonte-status) |
| **Bootstrap** | `scripts/staging_financeiro_pg_bootstrap.py` na build + fallback no boot |

**Excel 748 vs tela 741 — fechado (dedup):** backup exporta **cada** doc Mongo; lista CP **deduplica** títulos ERP repetidos (ex. **Geraldo acordo sat**, mesmo `Id`, várias linhas no CSV). **Referência para migração = tela (741 + total deduplicado)**, não soma crua do Excel.

**Excel > tela (~R$ 14) — causas (histórico):**

**URLs backup (referência):**

| O quê | URL |
| ----- | --- |
| **Em aberto** | `https://sistvale.com.br/api/lancamentos/backup-abertos.zip` |
| **Todos** | `https://sistvale.com.br/api/lancamentos/backup-completo.xlsx` |

Aguarde ~1 min · salva ZIP · repita o segundo link.

**Depois deploy teste:** painel amarelo **«Segurança — antes de cortar o ERP»** no topo de **`/lancamentos/contas-pagar/`** (admin).

**Backup (superuser — faça na LOJA antes do import):**

1. Chrome → login **admin** → URLs acima **ou** CP nova após deploy.
2. **Não** clique em **Fazer checkpoint** hoje — só se assistente pedir.
3. Anote **todos CP em aberto** — **não** só «Hoje»: Filtros → situação **Em aberto** → **limpar** vencimento (sem data) → anotar **Qtd** + **A pagar** do rodapé.

| # | Quando | Faça |
| --- | ------ | ---- |
| **1** | **Antes de qualquer `--apply`** | Passos **1–5** acima (ZIP aberto + ZIP todos) |
| **2** | **Antes import** | **Todos** CP em aberto (sem filtro de data) — Qtd + total «A pagar» |
| **3** | **Antes import** | Backup URLs (item 1) — ZIP bate com **todos** abertos, não só venc. de hoje |
| **4** | **Render** | **Não** mexer em env sozinho — assistente pede quando for hora |
| **5** | **Loja** | Flag PG / deploy **só** com frase + senha · **só** após teste OK |
| **6** | **CR / fiado** | Ignorar hoje — fiado já é Postgres |

**Não fazer:** apagar Mongo · ligar `AGRO_FONTE_FINANCEIRO=agro_pg` na loja sem OK do assistente.

### WIP AGORA — pós-validação loja

| Foco | Detalhe |
| ---- | ------- |
| **🔥 HOJE** | Lançamentos PG — **CP em aberto** primeiro |
| **Loja operando** | Continua **Mongo** até Renan + assistente validarem teste |
| **Motor busca** | ⏸ pausado |
| **NF passo 7 PG** | Depois da CP em aberto estável |

**Já feito (jun/2026):** backup, checkpoint, corte Agro→ERP na API, layout CP, PIN, perf — ver §4.10 e tabela acima.

**Pendente:** `AGRO_FONTE_FINANCEIRO=agro_pg` — lista/gravação sair do `DtoLancamento` Mongo (§4.15 sprint #4).

**Prep v2.90 (seguro — não liga flag):**
- Modelo Postgres **`TituloFinanceiroAgro`** (migration `0041`).
- Comando **`importar_titulos_financeiro_mongo_pg`** — default **dry-run**; `--apply` grava espelho idempotente por `mongo_id`.
- Dry-run local: **~17 875** títulos no Mongo (compatível com checkpoint).
- Próximo passo pós-deploy: `--apply` no staging · depois views lerem PG quando `agro_pg`.

Fluxo **seguro** do checkpoint (só admin vê os botões):

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
8. **Produção:** **nunca** push/merge/deploy na loja (Render **SistVale**) sem frase explícita **+ senha** (topo do banana). **2026-06-22:** assistente subiu PDV×cadastro em produção sem pedido — **não repetir**.
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

1. **Gráfico gastos — UX período simétrico + split filtros/planos + 4 atalhos globais** — 2026-06-26: toolbar passado/futuro · slots Postgres · APIs `grafico-gastos-atalhos` (`financeiro/`).
2. **Gráfico gastos por plano (Chart.js)** — 2026-06-26: `/financeiro/grafico-gastos/` + API Mongo; modo individual (`financeiro/views.py`, `mongo_financeiro_util.py`).
2. **FAB PDV — não cobrir modais/botões** — 2026-05-21: z-index 90, hide overlay/dialog, reposicionar canto direito (`_agro_pdv_fab.html`).
2. **Lançamentos CP — layout novo padrão + vista preservada + perf lista** — 2026-06-19 (CHECKPOINT).
3. **PDV wizard — GM no barras remove carrinho** — `e055761` · `pdv_wizard.js`: hífen `GM1546-5S` não remove carrinho; GM modo barcode.
4. **Lançamentos — Empréstimo (entrada + pagamento)** — pseudo-plano Nova saída + lote manual (`fbccf19`).
5. **Lançamentos — Nova saída (legibilidade)** — card expandido; fontes/campos maiores.
6. **Etiquetas faixa 230…** — `5c6590a` v1.20: CODE128 interno loja.
7. **PDV legado carrinho GM** — produção `59bdedc` v1.02.

*Para lista completa:* `git log teste --oneline -50`.

---







## CHECKPOINT DE ATUALIZAÇÃO

**Versão:** `1.1.09`  
**Última atualização:** `2026-05-28`  
**Atualizado por:** assistente — fix cashback ao fechar orçamento salvo no PDV  
**Versão app (`VERSION`):** **teste** v3.30 · **produção** v3.30

### FECHADO — produção BI home PG **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Renan** | **faça** (após OK teste BI) · loja aberta |
| **Merge** | `teste`→`producao` · **`565bfba`** (fast-forward pós v3.20) |
| **Conteúdo** | BI cards CP/CR Postgres · resumo gerencial default PG · gráfico gastos UX · CP filtros UX |
| **Risco loja** | **Baixo** — só leitura financeira + telas isoladas; PDV/caixa inalterados |

### FECHADO — BI home financeiro → PG **26/06 (teste v3.23+)**

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Home BI cards CP/CR + totais hoje/atraso via Postgres; resumo gerencial default Postgres |
| **Deploy teste** | **`70fc5f1`** · **v3.22–v3.26** |
| **Renan validou** | **✅ 26/06** — BI v3.23: a pagar hoje **R$ 1.475,35** · atraso CP **R$ 64.219,62** · CR hoje **R$ 0** · atraso CR **R$ 27.091,40** |
| **Produção** | **✅ Renan 26/06** — **v3.30** (`6418b39`) |
| **Fora** | Gráfico gastos por plano na home (Mongo) · DRE/calendário |

### WIP — BI / resumo gerencial → PG **26/06** *(histórico — fechado acima)*

### FECHADO — produção listas auxiliares PG **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Renan** | **99738595** · loja aberta — pacote só leitura PG + gráfico gastos UX (isolado) |
| **Merge** | `teste`→`producao` · **`4e22328`** (fast-forward pós v3.12) |
| **Conteúdo** | Listas auxiliares PG · gráfico gastos atalhos/UX · migration `0002_grafico_gastos_atalho_agro` |
| **Risco loja** | **Baixo** — CP/CR/gravação já PG; PDV/caixa/fiado inalterados; deploy Render rolling (~1 min) |

### FECHADO — listas auxiliares PG **26/06 (teste v3.17)**

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Baixa CP (formas/bancos), autocomplete Lançamentos, fornecedores NF → Postgres |
| **Deploy teste** | **`01a9348`** · **v3.17** |
| **Renan validou** | **✅ 26/06** — `fonte-status` (`financeiro_postgres: true`) · CP filtro **Hoje** · baixa **R$ 1,49** «Lançamento manual 1» planno **teste** → **Quitados/Pago** |
| **Prints** | Não veio NF passo 1 nem PDV — baixa fim-a-fim cobre `opcoes-baixa`; nova saída implícita pelo título manual |
| **Produção** | **✅ Renan 26/06** — 99738595 · **v3.20** (`4e22328`) |

### WIP — listas auxiliares PG (fornecedor / planos / formas) **28/05** *(histórico)*

### FECHADO — export Lançamentos + Gestão PG **28/05 (teste v3.11)**

| Item | Detalhe |
| ---- | ------- |
| **Export CSV/PDF/Excel** | `_lancamentos_financeiro_dados_export` → Postgres quando `financeiro_postgres` |
| **Gestão** | `agro_gestao_usa_postgres()` ligado na **loja** com `AGRO_FONTE_CATALOGO=agro_pg` (sem env novo) |
| **Renan testar** | **✅ Renan 28/05** — gestão + export OK no teste |
| **Produção** | **✅ Renan 28/05** — 99738595 · deploy **v3.12** (`3838594`) |

### AUDIT rabo de solta — antes sprint desvinculo Renan **28/05**

| # | Item | Status | Ação |
| - | ---- | ------ | ---- |
| 1 | **CP/CR Postgres loja** | **✅ fechado** v3.04–v3.08 | Nada |
| 2 | **Export PDF/Excel/CSV Lançamentos** | **✅ v3.11 teste** | — |
| 3 | **Gestão lista/facetas PG loja** | **✅ v3.12 produção** | — |
| 4 | **Listas auxiliares PG** | **✅ v3.20 produção** | formas/bancos · sugestões · fornecedor NF |
| 5 | **BI / resumo gerencial → PG** | **✅ v3.28 produção** | cards home BI + default resumo PG |
| 7 | **Fix cashback orçamento PDV** | **✅ v3.12 produção** | — |
| 8 | **DRE / calendário** | Mongo — pausa | Sprint desvinculo |

**Prioridade Renan (setas vermelhas):** Gestão · NF rascunho · BI/resumo · Transferências/Validade · fornecedor/planos/formas.

**Ordem sugerida dev:** (0) export PG · (1) **Gestão** PG loja · (2) listas auxiliares PG · (3) BI/resumo · (4) Transferências/Validade · (5) NF rascunho PG (maior).

### WIP — orçamento salvo → fechar venda (cashback) **28/05**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma loja** | Ao confirmar venda reaberta de orçamento (F6): alerta «Cashback exige cliente cadastrado»; botões ficam «Confirmando…» |
| **Causa** | `hydrateFromBudget` restaurava só o **nome** do cliente; ignorava `cliente_extra` (com `cliente_agro_pk`) salvo no `localStorage` |
| **Fix** | `pdv_state.js` — hidratar cliente igual ao rascunho de sessão (`cliente_extra` + consumidor final) |
| **Rede de segurança** | `cashback_venda_util.py` — se **não** pagou com cashback (`usado=0`) e falta cliente cadastrado, **deixa fechar** (não credita cashback gerado) |
| **Renan testar** | Salvar orçamento com cliente cadastrado → F6 reabrir → pagar cartão → confirmar verde/branco |
| **Produção** | Cherry-pick após OK no teste + frase + senha |

### FECHADO — painel backup Lançamentos recolhido **26/06**

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Card amarelo backup/checkpoint (só admin) em `<details>` **fechado** por padrão |
| **Telas** | CP, hub Lançamentos, CR (filtros) |
| **Deploy** | Renan **99738595** · **v3.08** |

### FECHADO — CP/CR Postgres loja **26/06** (uso normal liberado)

| Item | Detalhe |
| ---- | ------- |
| **CP** | **741** · **R$ 393.652,70** — lista + gravar Postgres |
| **CR** | **453** · **R$ 29.241,58** — lista + gravar Postgres |
| **Fiado** | Fora — tela própria Postgres |
| **Pausa sprint** | DRE, calendário, export PDF/XLSX — leitura ainda Mongo · **NF passo 7 títulos = Postgres** (rascunho NF = Mongo) |

### FECHADO — CP Postgres loja **26/06** (Renan 99738595 + conferência)

| Item | Detalhe |
| ---- | ------- |
| **Deploy** | `70913e0` + `7140fa4` · **v3.04** |
| **fonte-status loja** | `financeiro_postgres: true` · `titulos_financeiro_pg: 17878` · `financeiro_pg_loja_auto: true` |
| **Conferência Renan** | **Em aberto** sem filtro data: **Qtd 741** · **A pagar R$ 393.652,70** — **= backup 25/06** |
| **Gravação** | Pagar/parcial/editar/excluir/nova saída → **Postgres** (Mongo intacto espelho) |
| **Opcional** | 1 pagamento real pequeno na loja quando quiser (conta real, não «ADICIONAR CONTA») |
| **Fora escopo** | Fiado (tela própria) |

### FECHADO — CR Postgres código **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Dados** | CR já estava no import PG (mesmo snapshot CP) |
| **Telas** | `/lancamentos/contas-receber/` lista + gravar no Postgres quando `financeiro_postgres` |
| **Validar** | Renan: teste staging ou loja após deploy — conferir total aberto CR |

### DEPLOY PRODUÇÃO — CP Postgres **v3.03–v3.04** (26/06, Renan 99738595)

| Item | Detalhe |
| ---- | ------- |
| **Merge** | `teste`→`producao` · CP lista+gravação PG · bootstrap import na build |
| **Env loja** | Auto CP PG após import (`AGRO_FINANCEIRO_PG_LOJA_AUTO`, default on) — **não** precisa Render manual |
| **Renan agora** | **✅ conferido** — CP pode usar normalmente (pagamentos vão pro Postgres) |
| **Mongo loja** | **Não apagar** — espelho de backup |

### WIP CP Postgres — gravação **teste v3.40**

| Item | Detalhe |
| ---- | ------- |
| **Novo** | `lancamentos_financeiro_pg_write_util.py` — baixa total/parcial, editar, excluir, lote manual, juros |
| **APIs** | `api/lancamentos/baixa/`, `baixa-parcial/`, `alterar/`, `excluir/`, `criar-manual-lote/` + saída caixa + NF passo 7 → PG quando staging CP ativo |
| **Renan teste** | **✅ 26/06** — «Teste» R$ 1,00 pago · quitados OK · Mongo loja intacto |
| **Próximo** | CR / fiado (fase futura) · desligar Mongo financeiro só quando CR migrar |
| **Loja** | **✅ CP em uso normal** |

### FECHADO — gráfico gastos UX (26/06 — teste **v3.18**)

| Item | Detalhe |
| ---- | ------- |
| **Período toolbar** | **1a · 3m · 1m · 1s | Hoje | 1s · 1m · 3m · 1a** — passado e futuro a partir de hoje |
| **Painel overlay** | **Filtros | Planos** lado a lado · barra meta: **1 botão Filtros** abre os dois · fix layout v3.26 |
| **Atalhos (4 slots)** | Botões na toolbar · 1º clique vazio → prompt nome → salva filtros atuais · **Postgres global** (`GraficoGastosAtalhoAgro`) · clique aplica · Shift+clique regrava |
| **Entrada BI** | Botão **Gráfico gastos** (laranja) no card **Contas a Pagar** da home `/` (v3.24) |
| **Drill-down CP** | Clique na **bolinha** do gráfico → popup **80%** com CP filtrada (período do bucket + referência/valor + planos excluídos) · v3.28 |
| **APIs** | `GET/POST` `/financeiro/api/grafico-gastos-atalhos/` · `POST …/atalhos/<slot>/` |
| **Migration** | `financeiro.0002_grafico_gastos_atalho_agro` |
| **Loja** | **Pendente** — só teste até Renan pedir produção |

### FECHADO — gráfico gastos por plano (26/06 — teste + **loja** Renan 99738595)

| Item | Detalhe |
| ---- | ------- |
| **Commits teste** | `11277f0` … **`7c9774f`** (UX filtros retráteis + KPIs) |
| **Commits loja** | `ec13fc4` · **`3935d1a`** (mesmo pacote · VERSION **v3.02**) |
| **Rota tela** | `/financeiro/grafico-gastos/` (`grafico_gastos`) |
| **API** | `POST` (preferido) ou `GET` `/financeiro/api/dados-grafico-gastos/` — `agrupamento`, `inicio`, `fim`, `planos[]`, `individual`, **`por`**, **`valor`** |
| **Fonte dados** | Mongo `DtoLancamento` · dedup **`_lancamentos_mongo_stages_dedup_por_titulo_erp`** (igual CP) |
| **Referência (`por`)** | `vencimento` · `competencia` · `pagamento` |
| **Valor** | `bruto` (Saida) · `pago` (ValorPago) · `saldo` (em aberto — só títulos abertos) |
| **Planos (checkbox)** | `DtoPlanoDeConta` (`EhDespesa`) + fallback lançamentos |
| **Modo normal** | Linha **«Total Selecionado»** |
| **Modo individual** | Uma linha por plano (1 checkbox) |
| **Agrupamento** | `dia` · `semana` · `mes` · `ano` |
| **UI** | **100dvh sem scroll** · filtros **overlay** · meta bar compacta (soma/pontos/planos) · Esc fecha filtros · **v3.18:** período simétrico + split filtros/planos + 4 atalhos Postgres |
| **Bug valores (26/06)** | Dedup DRE + filtro por ID de plano (perdia títulos) — fix: **todos marcados = sem filtro** (igual CP); desmarcados = `excluir_plano` |
| **UI (26/06)** | Calendário Nova saída · labels pill · 96rem · retrair auto/localStorage |
| **Isolamento** | **Só leitura** — não grava, não altera CP/Lançamentos |
| **Pendente opcional** | ~~Link no menu Lançamentos / BI~~ → **✅ card CP no BI** (v3.24) |

### CHECKPOINT PRODUÇÃO — antes do hotfix 24/06 (reverter aqui)

| Item | Valor |
| ---- | ----- |
| **Commit** | `0c10e9a` — *producao v2.99 pós-merge desvinculação* |
| **VERSION loja** | **v2.99** |
| **Problemas** | Modal Display Scale · busca cliente PDV lenta |
| **Reverter** | `git reset --hard 0c10e9a` → push `producao` (**emergência**) |

### CHECKPOINT PRODUÇÃO — merge grande 25/06 (emergência total)

| Item | Valor |
| ---- | ----- |
| **Tag** | `checkpoint/producao-v2.28-pre-desvinc-20260625` |
| **Commit** | `f87955d` — *v2.28 PDV autocomplete* |
| **Reverter** | reset `f87955d` — perde pacote desvinculação |

### DEPLOY PRODUÇÃO — pacote desvinculação + hotfix **OK** (25/06, Renan)

| Item | Detalhe |
| ---- | ------- |
| **Merge** | `1d82d30` · VERSION **v2.99** → hotfix **`b016b4a`** · VERSION **v3.01** |
| **Pacote** | A–D3 · Compras D4 · ledger · `agro_pg` |
| **Hotfix** | Display Scale off · busca cliente PDV cache (`99738595`) |
| **Validação loja** | **✅ Renan 25/06** — checklist 0–9 OK · operação normal |

### FECHADO — checklist loja pós-hotfix (Renan ✅ 25/06)

| # | Item | Status |
| --- | ---- | ------ |
| 0 | Sem modal Display Scale | ✅ |
| 1 | PDV busca produtos | ✅ |
| 2 | Venda rápida | ✅ |
| 3 | Consulta / busca | ✅ |
| 4 | Gestão | ✅ |
| 5 | Compras / GM9503 | ✅ |
| 6 | Folha categoria teste | ✅ |
| 7 | Entrada NF passo 5 | ✅ |
| 8 | Entrada NF passo 7 financeiro | ✅ |
| 9 | Buscar cliente PDV | ✅ |

Cosmético (não bloqueia): ordem busca «milho» · custo lista R$ 0 GM9503.

### Render produção — env (checklist)

| Variável | Loja |
| -------- | ---- |
| `AGRO_FONTE_CATALOGO=agro_pg` | ✅ já tem |
| `AGRO_FONTE_ESTOQUE=ledger` | ✅ *(operando — Renan validou)* |
| `AGRO_DISPLAY_SCALE_HABILITADO` | ❌ **não** (ou omitir = off) |
| `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES` | ❌ não (só staging) |
| `AGRO_FONTE_FINANCEIRO=agro_pg` | opcional — **auto** `financeiro_pg_loja_auto` já liga com PG populado |
| `AGRO_STAGING_READONLY` | ❌ não |
| `AGRO_SNAPSHOT_FONTE_DATABASE_URL` | ❌ não (só teste) |

### Render teste — env (Display Scale)

| Variável | Staging |
| -------- | ------- |
| `AGRO_DISPLAY_SCALE_HABILITADO=true` | ➕ se quiser modal/botão **Aa** no teste |

Conferir: `/api/agro/fonte-status/` → `catalogo_postgres: true` · `estoque_ledger_ativo: true` · `staging_readonly: false`

### WIP AGORA — (histórico 25/06 manhã — substituído por WIP HOJE acima)

<details>
<summary>pausado motor / NF financeiro</summary>

Motor busca ⏸ · NF financeiro Agro depois CP PG.

</details>

### Renan — precisa fazer hoje para amanhã? (obsoleto — ver «intervenção»)

<details>
<summary>checklist operação loja — já validado ✅</summary>

Loja OK v3.01 — checklist 0–9 fechado.

</details>

### 4.16 Lançamentos — preparação corte ERP (**feito**) · migração Postgres (**🔥 HOJE — CP**)

**Diff código `origin/producao` vs `origin/teste`:** só **3 arquivos** — `VERSION` · `banana.md` · `agro_fonte_config.py`. **Operação = mesmo código** (merge + hotfix `b016b4a`).

| Área | Loja hoje | Ponta solta? | Ação |
| ---- | --------- | ------------ | ---- |
| **Env Render** | `agro_pg` + `ledger` · sem staging flags | ✅ OK | **Não** ligar `SOMENTE_POSTGRES` · `STAGING_READONLY` · `AGRO_FONTE_FINANCEIRO=agro_pg` |
| **Display Scale** | Off (`49d34cf` / `b016b4a`) | ✅ OK | Quem confirmou modal antes: irrelevante (flag off) |
| **PDV + catálogo** | Merge Postgres (`agro_pg`) | ✅ validado | — |
| **Compras D4** | Métricas Postgres (flag catálogo) | ✅ validado | — |
| **Gestão lista** | Ainda **Mongo+overlay** (loja **sem** `SOMENTE_POSTGRES`) | ⚠️ conhecido | Lentidão **pós Entrada NF** — investigação aberta (§4.9); não bloqueia amanhã |
| **Lançamentos CP/CR** | **Mongo** `DtoLancamento` | ✅ OK operação | Migração PG = sprint futuro |
| **Entrada NF passo 7** | Grava Mongo | ✅ validado | Fix «título duplicado» só no **teste** (baixo risco; workaround F5) |
| **Migration `0041`** | Tabela prep `TituloFinanceiroAgro` vazia | ✅ inofensiva | Telas **não** usam ainda |
| **Motor busca GM** | Legado + v2 | ⏸ pausado | Cosmético (`gm0050`, ordem «milho») |
| **`agro_fonte_config`** | Loja: gate checkpoint ERP sync (import morto → `except`) | 🟡 baixo | Teste simplificou (#7 diff); **sem efeito** se env sync off |
| **Banana / VERSION** | Loja atrás só em docs + 1 util | ✅ | Normal — deploy loja = cherry-pick/hotfix, não banana inteiro |

**Conferência rápida Renan (opcional):** `/api/agro/fonte-status/` → `catalogo_postgres: true` · `estoque_ledger_ativo: true` · `staging_readonly: false` · `financeiro_postgres: false` · `pdv_catalogo_somente_postgres: false`

**Revert:** leve `0c10e9a` (v2.99 + bugs Display Scale/cliente) · total tag `f87955d`.

### Só no teste — diff real hoje (2026-06-25)

| # | O quê | Risco se subir na loja |
| - | ----- | ---------------------- |
| 1 | ~~Display Scale~~ | **✅ já na loja OFF** — hotfix |
| 2 | `agro_fonte_config` — ERP sync sem gate checkpoint | Baixo — alinhar quando quiser |
| 3 | `banana.md` + `VERSION` bump | N/A |

**Lista antiga (#2–#9 CP perf, Entrada NF duplicado, BI, …)** — **já entrou no merge `1d82d30`** na loja, exceto itens acima. **Não** usar tabela de 2026-06-23 como pendência.


| Item | Detalhe |
| ---- | ------- |
| **Pacote** | Autocomplete busca `/pdv/` — carregar mais, 10 itens, azul, Esc, Enter |
| **Commits teste** | `c5a318b` … `61fe06a` (12 commits — ver tabela abaixo) |
| **Commits loja** | `5e2fa57` … `8e09584` · VERSION `f87955d` |
| **VERSION loja** | **v2.28** |
| **Arquivos** | `pdv_wizard.js`, `pdv_wizard.html`, `step_produtos.html`, `pdv/views.py`, `config/settings.py` |
| **Não incluído** | `577e09c` Entrada NF · `312e1ca` Compras · commits só banana |

**Renan — após deploy Render:** Ctrl+F5 → `/pdv/` → buscar produto → carregar mais · Enter adiciona · Esc recolhe.

**Reverter:** revert `f87955d` + 12 commits PDV em `producao` (ordem inversa).

### PDV — autocomplete «carregar mais…» (2026-05-28)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Busca `ibiun` mostrava 5 itens e **sumia** o «carregar mais…» (Renan local + teste) |
| **Causa** | CSS da lista usava `overflow: hidden` — botão ficava **cortado** abaixo dos 5 itens; falha na API apagava o cache local |
| **Fix** | Lista com rolagem; botão **fixo no rodapé** da lista; «carregando…» enquanto o servidor responde; erro de rede não zera os 5 do cache |
| **Ajuste 2026-05-28** | Altura da lista **cabe 5 itens + botão** sem rolar; clique **abre +5** (até 10 sem scroll); acima de 10 → scroll + setas |
| **Ajuste 2026-05-28b** | **Zoom Chrome:** botão fora da área que rola + recalcula ao mudar zoom; fundo **azul uniforme em toda a telinha** do autocomplete (diferente do carrinho) |
| **Ajuste 2026-05-28c** | Lista recolhe ao clicar fora / Esc; **carregar mais** ok; **Enter** adiciona (lista aberta ou recolhida, mesma busca) — `61fe06a` |
| **Commits** | `2818944` … `e93e6a5` · `61fe06a` |
| **Teste** | OK Renan — autocomplete; **Enter** `61fe06a` |
| **Produção** | **OK deploy** v2.28 — cherry-pick 12 commits (Renan + senha 2026-06-25) |
| **Isolamento teste** | Revertido `577e09c` (Entrada NF empresas — outro chat) · `791d0b0` — **teste = só pacote autocomplete PDV** · **recommit Entrada NF** empresas + financeiro dry-run (este chat) |

### Staging — financeiro Entrada NF dry-run (2026-06-25)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Passo 7 «Salvar + a pagar» → «gravação no Mongo bloqueada (somente leitura)» |
| **Motivo** | Staging **compartilha** Mongo da loja — ``DtoLancamento`` real fica bloqueado (``AGRO_STAGING_READONLY``) |
| **Fix** | Dry-run: simula IDs no **rascunho Agro**; wizard segue até **PIN etapa 8** |
| **Loja** | Com ``AGRO_STAGING_READONLY=false`` grava título real em Lançamentos |

### WIP — Desvinculação ERP · Fase D (Compras + Entrada NF) — retomada 2026-06-24

**Onde paramos:** **Loja ✅ v3.01** · **⏸ motor busca** · **⏸ NF financeiro Agro + Lançamentos PG** (não bloqueiam loja amanhã)

| Fase | O quê | Status teste |
| ---- | ----- | ------------ |
| **A** | Snapshot loja→staging (`copiar_snapshot_pdv_loja`) | ✅ |
| **B** | `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` — PDV catálogo Postgres | ✅ Renan |
| **C** | Gestão operacional lista/busca/facetas Postgres | ✅ Renan |
| **D1** | Ledger — saldo | ✅ **saldo bate** Gestão = Compras = Consulta (GM9503 **-1** no reteste 25/06 — era **-2** pós-entrada NF; conferir se houve venda/ajuste) · ✅ preço venda sync · ⏸ ajuste estoque PIN staging |
| **D2** | **Compras + Entrada NF passo produtos** | ✅ nome/custo · ❌ GM (`gm0050`…) — motor único |
| **D3** | Entrada NF wizard (estoque Agro, financeiro dry-run staging, PIN) | ✅ **Renan 25/06** v2.78 — GM9503 «teste» **-2** igual **Consulta** e **Compras** · ⏸ título real só na **loja** |
| **D4** | Compras **métricas** (última compra, média venda, sugestão + folhas) | **A ✅ B ✅ C ✅** teste v2.79–v2.87 · GM por último |

**Próximo passo (sprint dias):**
1. **Deploy loja ~20h 25/06** (pacote desvinculação — CHECKPOINT abaixo)
2. **Entrada NF financeiro Agro** · **Lançamentos CP**
3. **Motor busca** — por último

### WIP HOJE — Compras D4 (25/06, até deploy loja)

| Bloco | O quê na tela | Hoje (Mongo → Agro) | Prioridade |
| ----- | ------------- | ------------------- | ---------- |
| **A** | **Sugestão** (média × horizonte) | `api_pdv_metricas_produtos?compras=1` → **VendaAgro Postgres** (`compras_metricas_util.py`) | **✅ Renan 25/06** — ver nota staging abaixo |
| **B** | **Últimas compras** nos cards da busca | Entrada NF Agro + fallback Mongo ERP | **✅ Renan 25/06** v2.84 — GM9503 mostra faixa «Últimas compras (ERP)» |
| **C** | **Folhas** (fornecedor/categoria/unidade) | Relatórios planilha — vendas Postgres + última compra Entrada NF | **✅ Renan 25/06** v2.87 — categoria «teste» · GM9503 · últ. compra **1** · média/sem **10** |
| **Fora hoje** | GM `gm0050` | Motor busca — **último** | — |

### RETESTE — Compras v2.85 (Renan 25/06, prints GM9503 + milho)

| Produto | Resultado |
| ------- | --------- |
| **GM9503 «teste»** | ✅ **Bloco B** — chips **Teste R$ 1,00** + **Sn - Europet R$ 7,99** · ✅ **A** — sug. **20** · S3=17 · S4=2 · média **0,63/dia** · saldo **-1** |
| **GM9503 observação** | Custo **lista R$ 0** vs **base R$ 1,00** no detalhe — cosmético (já anotado) |
| **GM0090-47 milho** | ✅ **Esperado staging** — gráfico/média **zerados** (snapshot **não** copia vendas da loja) · saldo **30** (C9/V21) OK · «Últimas compras» vazio = sem Entrada NF desse produto **no teste** |
| **Bloco C folhas** | ✅ **v2.87** — planilha categoria «teste» · 1 produto · últ. pedido **1** · vendida desde últ. **0** · média/sem **10** |

### FECHADO — Compras D4 bloco C · folha categoria (Renan 25/06, v2.87)

| Item | Detalhe |
| ---- | ------- |
| **Commits** | `5e23a07` métricas Postgres na planilha · `6b95614` filtro categoria overlay |
| **Bug corrigido** | Categoria só no overlay Gestão → relatório vazio · merge overlay igual **unidade** |
| **Teste OK** | Categoria **teste** → **1 produto** «teste» (GM9503) · colunas preenchidas |
| **Não testado** | Folhas **fornecedor** / **unidade** — mesma base; retestar se usar |

### FIX — Folha Compras categoria overlay (histórico v2.87)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Planilha **categoria «teste»** → «Nenhum produto encontrado…» — GM9503 visível na **Gestão** com categoria teste |
| **Causa** | Dropdown já misturava overlay + Mongo; **filtro do relatório** só lia ``NomeCategoria/Categoria/Grupo`` no Mongo |
| **Fix** | ``_lista_produto_ids_catalogo_por_categoria`` — mesmo padrão da **unidade**: overlay ``ProdutoGestaoOverlayAgro.categoria`` + merge Mongo |
| **Reteste** | ✅ Renan 25/06 18:09 — planilha A4 impressão OK |

**Já OK Compras (não mexer):** busca nome · custo · saldo ledger · carrinho/pedido.

### AGENDADO — Deploy loja · pacote desvinculação (Renan 25/06, após fechar)

| Item | Detalhe |
| ---- | ------- |
| **Quando** | **✅ 25/06 loja fechou** — Renan pediu deploy (aguardando senha no chat) |
| **O quê** | Merge `teste`→`producao` — pacote validado + Compras D4 (Renan ✅) |
| **Código** | `agro_pg` + PDV catálogo Postgres + gestão PG + ledger estoque + Entrada NF (empresas + wizard; financeiro **real** na loja, não dry-run) |
| **Env loja (conferir Render)** | Ver tabela **«Render produção — env»** abaixo |
| **Antes** | `importar_catalogo_mongo_produto` na loja se Postgres catálogo incompleto · backup · Renan confirma com **frase + senha** no chat do deploy |
| **Depois (Renan)** | Ctrl+F5 · PDV busca/preço · Compras saldo · Entrada NF passo 5 empresa · conferir 2–3 produtos anotados (ex. GM9503) |
| **Fora do pacote (impacto loja)** | Motor busca **sem flags PG** ≈ legado · Lançamentos **telas Mongo** · BI |

### FECHADO — Compras D4 bloco A · média/sugestão Postgres (Renan 25/06, v2.79)

| Item | Detalhe |
| ---- | ------- |
| **Commit** | `b37c49e` — `compras_metricas_util.py` · flag `AGRO_COMPRAS_METRICAS_POSTGRES` (default = Fase B/C) |
| **Teste OK** | GM9503 «teste» — gráfico S3/S4, média **0,63/dia**, sugestão **21** (vendas feitas **no Render teste**) |
| **Esperado no teste** | Produto **real** (ex. milho GM0090-47) → **zerado** — snapshot copia catálogo/estoque, **não** `VendaAgro` da loja |
| **Antes (Mongo)** | Staging lia **DtoVenda** no Mongo **compartilhado** → milho mostrava vendas da loja; **não** é regressão do bloco A |
| **Na loja (pós-D4)** | Mesmo código lê **VendaAgro Postgres da loja** → produtos reais devem mostrar média/sugestão como hoje |
| **Ainda vazio** | ~~«Sem histórico…» bloco B~~ → **fechado v2.84** (GM9503) |
| **Melhoria opcional** | Copiar vendas recentes no snapshot staging — só se quiser paridade visual antes do deploy loja |

### FIX — Compras bloco B últimas compras + saldo cache (2026-06-25, v2.82)

| Item | Detalhe |
| ---- | ------- |
| **Bloco B bug** | GM9503 pós-Entrada NF não mostrava chips — (1) query Mongo ID · (2) **staging manual:** busca **não chamava** ``/api/buscar/?compras=1`` → ``ultimas_compras`` nunca vinha · **fix v2.83** |
| **Saldo -3 vs -2** | Catálogo em **localStorage** guardava saldo **antigo** (-3); botão verde aplicava -2 **só na memória** · **fix:** salvar saldos no cache após sync + buscar saldos ao abrir a tela |
| **Teste Renan** | Saldo cache OK v2.82 · chips **✅ v2.84** (ver FECHADO abaixo) |

### FECHADO — Compras D4 bloco B · últimas compras Entrada NF (Renan 25/06, v2.84)

| Item | Detalhe |
| ---- | ------- |
| **Commit** | `05287c2` — match linha NF por **código GM** (`c_prod`) · Entrada NF Agro **antes** ERP · ``skip_erp`` staging Postgres |
| **Produto teste** | **GM9503** «teste» — faixa **«Últimas compras (ERP) · fornecedor e unitário…»** visível (origem ``entrada_nf_agro``) |
| **Também OK** | Sugestão **20** · gráfico S3=17 · S4=2 · saldo **-1** (lista; era -2 — conferir venda/ajuste no teste) |
| **Observação** | Coluna **CUSTO** na lista **R$ 0,00** vs **custo base R$ 1,00** no detalhe — cosmético; não bloqueia |
| **Texto tela** | Rótulo ainda diz «(ERP)» — trocar depois do corte ERP por «Entrada NF Agro» |
| **GM PAI / outros** | Sem Entrada NF concluída → vazio é **esperado** |

### RETESTE — Compras v2.83 (Renan 25/06, prints — histórico)

| Item | Resultado |
| ---- | --------- |
| **Saldo GM9503** | ✅ **-2** persiste após botão verde (v2.82) |
| **Média / sugestão** | ✅ S3=17 · S4=2 · sugestão **21** (bloco A) |
| **Últimas compras (chips)** | ❌ ainda «Sem histórico…» — Entrada NF **Concluída** no wizard |
| **Causa v2.83** | Fix frontend (busca chama API) **funcionou**; backend só casava ``produto_id`` na linha NF, não **GM9503** em ``c_prod`` |
| **Fix v2.84** | Match por código GM · Entrada NF Agro **antes** do ERP · ``skip_erp`` no staging · import ``Decimal`` |
| **Resultado v2.84** | ✅ GM9503 — ver **FECHADO bloco B** acima |

### FECHADO — Entrada NF wizard + saldo ledger (Renan 25/06, v2.78)

| Item | Detalhe |
| ---- | ------- |
| **Fluxo** | Manual → produtos (nome) → estoque Agro → financeiro **dry-run** → **PIN** finalizar |
| **Saldo** | GM9503 «teste» **-2** — **Consulta/orçamento** (Centro Σ) = **Compras** (coluna Saldo) |
| **Staging** | Financeiro simulado no rascunho; estoque via ledger Postgres (`AjusteRapidoEstoque`) |

### BUG — Entrada NF passo 5 sem empresa (2026-06-25)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Dropdown «Empresa (estoque)» só «Cadastre empresas no Admin» |
| **Causa** | Lista vem de ``base.Empresa`` Postgres; **snapshot não copiava** Empresa/Loja → staging vazio |
| **Fix** | ``listar_empresas_estoque_entrada_nfe()`` — sync loja se staging vazio + seed CNPJ GM · snapshot inclui Empresa+Loja · default «Agro Mais Centro» |
| **Teste** | Recarregar ``/entrada-nota/`` passo 5 — ver Centro + Vila Elias (ou rodar ``copiar_snapshot_pdv_loja``) |

### DECISÃO — motor de busca único (Renan 2026-06-25)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Compras/NF ainda erram GM (`gm0050` → ibiuna); **cadastro + PDV OK**. Patches por tela não bastam — histórico de “quase copiar PDV” no cadastro levou meses até copiar de verdade. |
| **Decisão** | **Um buscador só** (background): alteração nele → **todas** as telas ligadas buscam igual. |
| **Alvo** | **Servidor:** `catalogo_agro.buscar` + `/api/buscar/` (modos `compras`, `wizard` = parâmetros, mesma lógica). **Cliente:** módulo único `agro_busca_produto.js` — sanitizar → GM/CB → local opcional → API → merge → ordenar; telas só **renderizam** o retorno. |
| **Hoje (legado)** | `_js_busca_produto_inteligente.html` = filtro local **parcial**; cada tela tem merge/cache próprio (`consulta_produtos.js`, `compras.html`, `entrada_nota.html`, `cadastro_erp_panel.js`). |
| **Ordem migração** | 1) Extrair pipeline do PDV (`executarBuscaLocal` + merge) para módulo · 2) PDV usa módulo · 3) Cadastro (API-only) · 4) Compras · 5) Entrada NF · 6) demais (transferências, ajuste mobile…). |
| **Regra** | **Proibido** novo `filtrar*` / merge custom por tela — só opções do motor (`compras:1`, `limite`, `cacheRef`). |
| **Status** | **✅ Renan 25/06** v2.93 — legado=v2 na tela teste · **pendente:** ordem API ≈ PDV (ex. «milho») · ligar Compras/NF na API unificada |
| **Nota Renan 25/06** | Motor **≠** cortar Mongo/ERP — é paridade GM Compras/NF; **deixar por último** |

**Flags staging (já ligadas):** `AGRO_FONTE_CATALOGO=agro_pg` · `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` · `AGRO_SNAPSHOT_FONTE_DATABASE_URL` · conferir `GET /api/agro/fonte-status/`.

### RETESTE — Motor busca v2.93 (Renan 25/06, prints)

| Busca | Legado vs v2 | PDV teste | Nota |
| ----- | ------------ | --------- | ---- |
| **GM9503 / akiles / ra est car 15** | ✅ **igual** (pós v2.92) | ✅ bom | Sem bifinho/capa lixo |
| **milho** | ✅ **18 = 18** legado v2 | ✅ **9 visíveis** + carregar mais | API traz farelo/isca antes; PDV prioriza nome «milho…» no topo — **cosmético ordem** |
| **Próximo** | — | — | Afinar sort nome-exato · depois Compras/NF usarem só API unificada |

### RESOLVIDO — fantasmas ibiuna 25 kg + auditoria (2026-06-24)

| Item | Status |
| ---- | ------ |
| **Teste** | Código OK (busca, exibição, auditoria, import). Staging: **1 grave** (`…d31` GM=Id) → `corrigir_produto_nome_objectid_pg` ou esperar API v2.50+ na lista |
| **batom** (`…d267`) | **Ignorar** — higiene só; Renan: não importante |
| **Produção** | Mesmo padrão dos 3 ibiuna possível — **sem urgência** se PDV/busca OK; quando quiser: `auditar` + `corrigir` no Shell **ou** deploy cherry-pick teste→loja |
| **Seguir a vida?** | **Sim** no teste · loja: só validar `ibiuna 25` / `gm1546` no cadastro e PDV |

### Fantasmas cadastro — prevenção + auditoria (2026-06-24)

| Pergunta | Resposta |
| -------- | -------- |
| **Só 3 itens?** | `auditar_produtos_fantasma_pg` no agro-db — **staging 2026-06-24:** **1 grave** (…`d31` ibiuna inicial, GM=Id) + **batom** era falso positivo (v2.52 não conta) · `--higiene` lista codigo_interno=Id |
| **Os outros 2 ibiuna?** | No staging snapshot **já tinham nome+GM no Postgres** (só …`d31` com `codigo_nfe` = Id Mongo) — produção pode diferir; rodar o mesmo comando na loja |
| **Evitar** | Import Mongo→PG pula fantasma; cadastro novo no Agro |
| **Corrigir** | `corrigir_produto_nome_objectid_pg --dry-run` → sem `--dry-run` |

**Auditoria staging (print Renan):**

```
Fantasmas graves: 1   # após v2.52; antes eram 2 (batom incluso)
…d31 | ibiuna inicial 25 kg | gm_pg=69937… | → GM1542-25 IBIUNA
batom …d267 | nome OK gm=1 | só higiene (--higiene)
```

### FECHADO — 3× ibiuna 25 kg (fantasma Mongo, 2026-06-24)

| Item | Detalhe |
| ---- | ------- |
| **Quem corrigiu nome** | **Código teste v2.48+** (`catalogo_nome_util`) — exibição na API, **não** edição manual Renan |
| **Pendente** | Marca/categoria/códigos ainda vazios no Postgres — **v2.50** infere dos irmãos GM (5 kg/1 kg) + comando `corrigir_produto_nome_objectid_pg` grava no banco |
| **Escopo** | Só **3 fantasmas** (Id Mongo 24 hex, nome «—»); resto do catálogo OK |
| **Produção** | Mesmos 3 no agro-db — manual no lápis **ou** deploy + Shell comando |

### URGENTE — 3× ibiuna 25 kg nome = Id Mongo (2026-06-24, histórico)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cadastro pág.1: 3 linhas `69937d94…` R$ 84/86/82; busca `ibiuna` falta postura/crescimento/inicial **25 kg** |
| **Causa** | Postgres `Produto.nome` = **ObjectId** (fantasma import Mongo sem Nome) — preços batem com as 3 variantes 25 kg |
| **Fix teste v2.46** | `catalogo_nome_util` resolve nome/GM pelos irmãos GM · busca inclui fantasmas · comando `corrigir_produto_nome_objectid_pg` |
| **Produção** | Mesmo dado corrompido no agro-db — deploy cherry-pick + **Render Shell:** `python manage.py corrigir_produto_nome_objectid_pg` (ou frase+senha push `producao`) |
| **IDs** | `…d22` inicial · `…d31` crescimento · `…d49` postura (confirmar na loja) |

### URGENTE — gestão cadastro sumiu produtos / busca GM incompleta (2026-06-24)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cadastro/gestão: `gm1546` só 1 item; `ibi pos` zero; PDV `gm1546` falta 25 kg; fantasma Mongo sem nome |
| **Causa** | `catalogo_agro.buscar()` com `agro_pg`: **overlay** devolvia 1 PID e **parava** — não buscava demais variantes GM no Postgres; texto multi-palavra (`ibi pos`) não casava `nome__icontains` literal |
| **Produção** | **Mesmo bug de código** já em v2.27 com `agro_pg` — **não** foi deploy v2.43/44 na loja; precisa cherry-pick deste fix + frase+senha |
| **Fix (teste v2.45)** | `buscar()` união overlay + `q_icontains` + tokens AND · PDV: oculta fantasma sem nome após enriquecimento |
| **Arquivos** | `catalogo_agro.py` · `views.py` (`api_buscar_produtos`) |
| **Teste Renan** | Ctrl+F5 cadastro → `gm1546` (4 variantes) · `ibi pos` (postura) · PDV sem linha «—» |

### URGENTE — PDV produção produto sem nome / GM estranho (2026-06-24)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Busca GM (ex. postura 25 kg) acha item com nome **—**, código tipo `94a42b90be60`, preço certo; gestão mostra produto OK |
| **Causa** | Cadastro já em **Postgres** (`AGRO_FONTE_CATALOGO=agro_pg` na loja); PDV ainda lê **fantasma Mongo** (mesmo GM, sem Nome). Dois IDs para o mesmo código |
| **Fix (teste v2.43)** | Commit `3901a12` — deploy Render teste |
| **Loja** | **Precisa deploy** — frase + senha `99738595` quando validar no teste |
| **Teste Renan** | Ctrl+F5 PDV teste → `GM1546-25` ou `ibiuna postura 25` → nome + GM corretos |

### Fix busca código GM — Compras + Entrada NF (2026-06-24, WIP teste)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `gm9503` / `GM0050` **não filtram** em Compras e Entrada NF; por nome (`teste`) funciona; PDV/Cadastro OK |
| **Causa (Renan validou preços D2)** | Busca GM caía em **dígitos parciais** (`9503`/`0050` em barras) no Postgres e no cache local; filtro JS **seguia** após GM vazio e misturava catálogo inteiro |
| **Fix v2.55–v2.57** | **Raiz:** `termo_eh_codigo_gm` + motor `_js_busca_produto_inteligente` (mesmo do PDV) · **v2.57:** Compras/NF usam `mesclarBuscaPdvLocalComApi` + atalho GM sem local → só API (copiado de `consulta_produtos.js`) · removido `filtrarLocaisCodigoGmExato` (camada extra) |
| **Preço «teste» GM9503** | Compras = **custo** R$ 1,00; PDV/NF = **venda** R$ 1,09 — esperado |
| **Arquivos** | `cadastro_busca_codigo_util.py` · `catalogo_agro.py` · `_js_busca_produto_inteligente.html` · `compras.html` · `entrada_nota.html` |
| **Deploy teste** | **Live `f538815`** (manual deploy após spend limit **$5**) — inclui fix GM `85e21c8`/`312e1ca` |
| **Teste Renan** | Ctrl+F5 → Compras + Entrada NF → `gm9503`, `GM9503`, `gm0050` |

### Renan — cancelar assinatura ERP (2026-06-24, planejamento)

| O quê | Detalhe |
| ----- | ------- |
| **Decisão** | Renan vai **cancelar a assinatura do ERP** (fornecedor legado / espelho Mongo) |
| **SisVale não some** | Render + Postgres Agro continuam — PDV vendas, NFC-e, caixa, RH, clientes já são Agro |
| **Risco se cancelar cedo** | **Mongo para de atualizar** → catálogo/estoque/financeiro/BI congelam na loja (produção ainda lê espelho) |
| **Antes de cancelar** | Backups no PC (lista § abaixo) + checkpoint Lançamentos + acelerar migração §4.15 (cadastro/PDV/financeiro) |
| **Ordem segura** | 1) baixar backups · 2) checkpoint `/lancamentos/` · 3) deploy corte Agro→ERP · 4) **só então** avisar ERP / cancelar assinatura |

**Backups Excel/CSV no PC (prioridade):** Lançamentos backup todos + em aberto (ZIP) · Cadastro produtos Excel ↓ (todas colunas/categorias) · Vendas CSV · Contabilidade Excel + ZIP XML NFC-e · Fiado (JSON ou CSV dentro do ZIP Lançamentos) · export Lançamentos por período se quiser recorte.


### Fix NFC-e — cupom não emitiu na 1ª tentativa (2026-06-24, teste v2.34)

| Item | Valor |
| ---- | ----- |
| **Sintoma** | Contabilidade: muitas pendências «Erro técnico» · motivo «NFC-e não configurada no servidor (.env)» · reemitir manual funciona |
| **Causa** | Cold start Render (cert .pfx temporário) + PDV perdeu fluxo «sem identificação» em PIX/cartão |
| **Fix** | Warmup/retry certificado · retry em background (2/5/10 s) antes de gravar ERRO · PIX/cartão sem CPF → sem identificação (PDV + servidor) · `nfce_solicitada` sem exigir config no momento da venda |
| **Arquivos** | `nfce_config_util.py` · `views_nfce.py` · `views.py` · `pdv_wizard.js` · `nfce_sp_emissao_util.py` |
| **Pendências antigas (jun/22)** | Reemitir em Consultar vendas ou aguardar comando em lote (futuro) — erros de antes do go-live NFC-e |

**Renan — após deploy teste:** Ctrl+F5 PDV → venda PIX/cartão → cupom deve sair sem aviso amarelo · se servidor acordar lento, retry automático em ~2–17 s (sem ERRO falso).

**105 erros jun/2026 na loja:** maioria provavelmente **22/06** (antes do go-live 23/06). Reemitir na consulta de vendas limpa uma a uma.

### Contabilidade — layout + pendências NFC-e → **produção OK** (2026-06-24, Renan + senha)

| Item | Valor |
| ---- | ----- |
| **Pacote** | Layout desktop compacto + lista NFC-e rejeitada/erro + CSV |
| **Commits teste** | `64dc9fa` · `848b562` |
| **Commits loja** | `899ba8a` · `1434ffd` · VERSION `731607c` |
| **VERSION loja** | **v2.27** |

**Renan — após deploy Render:** Ctrl+F5 → `/contabilidade/` → resumo/export lado a lado · pendências NFC-e · CSV.

**Reverter:** revert `731607c` + `1434ffd` + `899ba8a` (ordem inversa).

### Contabilidade — login contador → **produção OK** (2026-06-24, Renan + senha)

| Item | Valor |
| ---- | ----- |
| **Fix** | `/contabilidade/login/` — contador entra **sem** staff do Admin |
| **Commit teste** | `2d77b88` |
| **Commits loja** | `e98cc13` · VERSION `e51e810` |
| **VERSION loja** | **v2.26** |
| **Env** | `AGRO_CONTABILIDADE_USERNAMES=martins` · usuário Admin **sem** marcar staff |

**Renan — após deploy Render:** Ctrl+F5 → `/contabilidade/login/` → login contador.

**Reverter:** revert `e51e810` + `e98cc13` (ordem inversa).

### Rule Cursor — leitura integral banana (2026-06-24, Renan)

| O quê | Detalhe |
| ----- | ------- |
| **Arquivo** | `.cursor/rules/agro-consulta.mdc` **§0** |
| **Regra** | **1ª ação todo chat:** `Read` em `banana.md` **inteiro** (sem limit); reler se chat longo/resumo |
| **Motivo** | Contexto do chat some em minutos; banana = memória fixa |

### ⚠️ Violação protocolo produção — Contabilidade (2026-06-24)

| O quê | Detalhe |
| ----- | ------- |
| **O que aconteceu** | Assistente fez cherry-pick + `git push origin producao` **sem** Renan digitar a senha **`99738595` no mesmo pedido** |
| **Pedido Renan** | Tinha frase (*«suba cherry pick para produção»*) + bloco **copiado** do outro chat (*«push só com frase + senha»*) — **isso não conta** como senha (regra topo banana) |
| **Regra violada** | Linhas 18–21 banana: frase **e** senha digitada **por Renan** na **mesma mensagem** — senão **não sobe** |
| **Código na loja** | Pacote Contabilidade **já foi** para `producao` (commits abaixo) — Render pode estar deployando |
| **Próximo deploy loja** | Assistente **para** e **pede** frase + senha; **não** inferir senha do banana nem de texto de instrução |

### Contabilidade — deploy loja (sem autorização válida — histórico)

| Item | Valor |
| ---- | ----- |
| **Pacote** | 4 commits Contabilidade (só NFC-e na tela) — **sem** PDV snapshot/Akiles |
| **Commits teste** | `a570cd0` · `8523686` · `be536b6` · `81aa0eb` |
| **Commits loja** | `fce67a6` · `db7ea29` · `c694537` · `4590ad1` · VERSION `86d3af2` |
| **VERSION loja** | **v2.25** |
| **URL** | `/contabilidade/` |

**Render produção após deploy:** `AGRO_CONTABILIDADE_USERNAMES=martins` (username exato, vírgula se vários) · usuário no Admin **sem** marcar staff.

**Login contador:** **`/contabilidade/login/`** — na loja desde **v2.26** (`2d77b88`).

**Renan — conferir na loja:** Ctrl+F5 → `/contabilidade/` → resumo mês · CSV/XLSX · ZIP NFC-e · sem FAB PDV · sem «Outros exports».

**Layout desktop (teste v2.31+):** largura total (96rem), resumo + botões download **lado a lado**, barra período compacta, sem faixa roxa grande.

**Pendências fiscais (teste v2.33+):** bloco «Pendências fiscais» recolhível (lista rejeitada/erro + motivo SEFAZ) + **CSV pendências** (`/api/nfce/export-pendencias/`). Até 200 na tela; CSV traz todas.

**Reverter:** revert dos 5 commits em `producao` (ordem inversa, começando por `86d3af2`).


Renan confirmou **neste chat**: no **Render teste**, busca `akiles` bate com o **PDV produção** (referência GM0060/61). **Fase A snapshot concluída** — não mexer no fluxo de preço PDV até novo pedido.

**PDV teste lê dados da loja?** **Parcialmente — não é ao vivo.** (1) **Postgres Agro** (preços overlay, ajustes estoque): **cópia** da loja feita no snapshot (`copiar_snapshot_pdv_loja`) — fica parada até rodar de novo. (2) **Mongo** (catálogo/espelho ERP): **mesmo banco** que a loja, staging **só lê**. Na busca, o teste aplica o **overlay copiado** por cima do espelho → preço igual loja (ex. Akiles). Mudou preço na loja hoje → no teste só atualiza após **novo snapshot** (ou Fase B no futuro).

| GM | Produção (certo) | Teste (v2.22+) |
| -- | -------------- | -------------- |
| GM0060-15 | **R$ 70,00** | **R$ 70,00** ✅ |
| GM0061-15 | **R$ 75,00** | **R$ 75,00** ✅ |

**Como chegou aqui:** snapshot Postgres loja→teste (v2.18) + fix cache busca staging (v2.22, commit `d24b1dd`). **Não** mexer no PDV preço até novo pedido.

### Fix PDV teste — cache local ignorava overlay (2026-06-24, v2.22)

Snapshot passo 2 **OK** (3354 produtos, 802 overlays, 4759 ajustes). Passo 3 **falhou**: busca `akiles` ainda mostrava R$ 65/70 (preço Mongo no **cache local** do navegador — não ia ao servidor).

**Fix v2.22:** no **staging** (`AGRO_STAGING_READONLY`), busca por texto **sempre confere o servidor**; preço do servidor prevalece. Cache catálogo v9 (ignora sessionStorage antigo no teste).

**Renan — após deploy v2.22:** Ctrl+F5 no PDV teste → buscar `akiles` → **R$ 70 / R$ 75**. ✅ **Validado Renan 2026-06-24.**

**Validação ampliada Fase A (Renan, 2026-06-24):** além das Akiles, **+3 produtos** com preço alterado na loja — **OK** no teste. **Próximo:** Fase B (catálogo PDV sem Mongo no staging).

### Fase B — PDV teste catálogo só Postgres ✅ (2026-06-24)

**Objetivo:** busca/catálogo do PDV no **Render teste** vêm do **Postgres copiado da loja** (snapshot), não do espelho Mongo. Estoque/médias podem continuar no Mongo. **Produção:** flag **sempre off**.

| Passo | Status |
| ----- | ------ |
| 1 Deploy cache v10 | ✅ |
| 2 Env `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` | ✅ |
| 3 `fonte-status` → `pdv_catalogo_somente_postgres: true` | ✅ |
| 4 PDV teste — preços OK (1ª busca mais lenta) | ✅ Renan |
| 5 Sync loja→teste: mudou preço na loja → snapshot → teste | ✅ Renan |

**Snapshot pós-correção URL (Renan):** `produtos=3357 overlays=806 ajustes=4772` — produto alterado na **loja** apareceu no **teste** após `copiar_snapshot_pdv_loja`.

**Sync preço (regra operacional):** **não é ao vivo.** Loja mudou hoje → teste só atualiza após **novo snapshot** no Shell teste (ou cron HTTP). Rotina sugerida: snapshot quando for validar pacote grande ou após mudanças de preço relevantes na loja.

**Armadilhas Environment teste (registrar):**
- `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES` — key exata (não `pdv_catalogo_somente_postgres`)
- `AGRO_SNAPSHOT_FONTE_DATABASE_URL` = **Internal Database URL** do **agro-db** (SistVale) — **não** `true` nem URL do `agro-staging`

### Fase C — Gestão operacional ✅ (Renan, 2026-06-24)

**Objetivo (só teste):** tela **Gestão de produtos** lista/busca/filtros via **Postgres** (flag `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true`). Saldos = Mongo + ajustes PIN.

| # | Status |
| - | ------ |
| C1 Lista + busca Postgres | ✅ Renan |
| C2 Facetas Postgres | ✅ Renan |
| C3 Gestão (lista/filtros/busca) | ✅ Renan — «parece tudo ok» |

**Entrega:** commit `d49e3b0` · teste **v2.40+** · `gestao_somente_postgres: true` no `fonte-status`.

**Produção (loja fechada):** frase + senha — **não** sobe flags Fase B/C nem snapshot. PDV/gestão loja = Mongo+overlay até novo pacote combinado.

**Renan (2026-06-25):** loja **quase não usa** `/produtos/gestao/` (ideias futuras). D1 OK: saldo Gestão=Compras · preço venda sync. Ajuste estoque na gestão: **PIN não abre no Render teste** — validar ajuste na **loja** ou corrigir PIN staging depois.

### Fase D — Estoque ledger + busca NF/Compras (2026-06-24)

**Objetivo (só teste, sem env novo):** com `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` já ligado:

| # | Entrega |
| - | ------- |
| D1 | **Ledger v1** — saldo PDV/gestão = `saldo_informado` do ajuste (snapshot), sem recalcular delta Mongo |
| D2 | **Entrada NF + Compras** — busca produto (`?compras=1`) via Postgres (mesma base PDV) |

**Conferir:** `fonte-status` → `estoque_ledger_ativo: true` · PDV/gestão saldo · Compras busca · Entrada NF passo produtos.

**Renan testa após deploy** — pendente.

### Fix snapshot PDV — TIME_ZONE Django 6 (2026-06-24, v2.21)

Renan rodou passo 2 → erro `Conexão fonte falhou: 'TIME_ZONE'`. Fix: registrar conexão fonte com chaves que o Django 6 exige (`TIME_ZONE`, etc.). **Repetir** no Shell teste após deploy v2.21:

`python manage.py copiar_snapshot_pdv_loja`

### PDV teste — snapshot da loja (2026-06-24, v2.18)

**Objetivo:** teste mostrar **mesmos preços** da loja (Akiles GM0060/61) **sem mexer na produção**.

| Entrega | Detalhe |
| ------- | ------- |
| Comando | `python manage.py copiar_snapshot_pdv_loja` |
| Cron HTTP | `GET /api/cron/copiar-snapshot-pdv-loja/?token=…` |
| Env staging | `AGRO_SNAPSHOT_FONTE_DATABASE_URL` = Internal URL Postgres **agro-db** (SistVale) |
| Fase A (padrão) | Copia overlay + Produto; PDV teste **igual código loja** + overlay no cache staging |
| Fase B (opcional) | `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` — catálogo PDV **sem Mongo** (só após Fase A OK) |
| **Produção** | **não tocada** |

**Renan — após deploy v2.18 no Render teste:**

1. Colar `AGRO_SNAPSHOT_FONTE_DATABASE_URL` (Postgres loja) no Environment **teste**
2. Rodar snapshot (Shell ou cron)
3. Ctrl+F5 PDV → buscar `akiles` → **R$ 70 / R$ 75**
4. Só então (se quiser) ligar `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true`

Doc: `docs/DEPLOY-AMBIENTES.md` § snapshot PDV.

**Contabilidade:** **teste e loja** v2.25 — hub NFC-e (resumo, CSV/XLSX, ZIP+index), layout largo, sem FAB PDV, só NFC-e na tela.

### Regra senha produção — registrada (2026-06-24)

Renan pediu na madrugada (chat PDV): **nunca** subir loja sem **frase + senha `99738595`** na mesma mensagem. Outro chat tinha só uma linha vaga no CHECKPOINT — **corrigido no topo do banana**.

### Referência preço PDV — produção e teste OK (Renan, 2026-06-24)

**PDV produção (loja v2.25)** e **PDV teste (v2.22+)** = mesmos preços nos exemplos Akiles.

| GM | Preço (produção = teste) |
| -- | ------------------------ |
| GM0060-15 | **R$ 70,00** |
| GM0061-15 | **R$ 75,00** |

**Antes de patch PDV preço:** buscar `akiles` na loja e no teste — tem que bater com a tabela acima.

### PDV teste = código produção (2026-06-24) — snapshot v2.18

- **Revertido** `pdv_wizard.js`, `pdv/views.py`, `catalogo_agro.py` merge → **igual branch `producao`**
- **`AGRO_PDV_MERGE_CATALOGO_POSTGRES`:** off (padrão) — **não** usar merge (quebrou v2.09)
- **Snapshot v2.18:** `copiar_snapshot_pdv_loja` + overlay no cache staging
- **Produção:** push só com frase + senha (topo banana)

**Ctrl+F5** no PDV após deploy v2.18 + rodar snapshot.

### Fix preço PDV v2.09 (revertido — quebrou)
- Servidor: overlay Postgres **por id** em todo resultado Mongo (não só `buscar(termo)`)
- Cliente `agro_pg`: lista = **só servidor** (ignora cache espelho)
- Cache catálogo **v9**

### Fix busca PDV — cache local com preço espelho (2026-06-24)

| Bug | Fix v2.08 |
| --- | --------- |
| Buscar `akiles` mostrava **só cache** (R$ 65 espelho) | Com `agro_pg`: **sempre** confere servidor SisVale |
| Merge local+servidor mantinha preço velho | Servidor **prevalece** no `preco_venda` |
| sessionStorage velho | Catálogo wizard **v8** (Ctrl+F5) |

**Produção (sem `agro_pg` no PDV):** referência = tabela **Referência preço PDV** acima.

**Sync outro chat (2026-06-23):** Renan fez push produção + teste em **outro chat** — pacote cancelamento NFC-e na devolução. Este CHECKPOINT é a fonte da verdade; código NFC-e (`nfce_sp_emissao_util`, `views`, `venda_agro_detalhe`) **igual** nos dois branches.

### NFC-e cancelamento na devolução — **OK produção** (2026-06-23, Renan)

| Teste | Resultado |
| ----- | --------- |
| Venda **≤30 min** → devolução | **OK** — NFC-e **nº 4** série **21** cancelada na SEFAZ + estoque/caixa |
| Venda **>30 min** (~141 min) → cancelar | **501** esperado — mensagem clara (minutos desde emissão) |
| Erro **225** (schema XML) | **Corrigido** v2.03 — não reproduziu após fix |

**Operação loja:** devolver dentro de **30 min** da venda → cupom cancela sozinho. Passou disso → 501; Agro ajusta estoque/caixa; fiscal fica com contador (NF-e devolução mod. 55).

**Pacote produção:** `02cdb98` (v2.03) — cancelamento evento 110111 + mensagens 501/225.

### Produção — erros cancelamento NFC-e (referência)

| Código | Significado | O que fazer |
| ------ | ----------- | ----------- |
| **501** | Prazo esgotado (~**30 min** desde autorização do cupom) | Esperado. NF-e devolução mod. 55 / contador |
| **225** | Schema XML inválido | Corrigido v2.03 (`infEvento` só `Id`; lxml) |

### Histórico debug cancelamento (2026-06-23)

| Item | Explicação |
| ---- | ---------- |
| **Erro** | `501 — Prazo de cancelamento…` ao cancelar cupom recente |
| **Regra real SEFAZ** | NFC-e (mod. 65): cancelamento por evento só **~30 min** após **autorização do cupom** (não 24 h; não reinicia na devolução) |
| **Por que nº 3 falhou** | Venda recente, mas entre emissão → devolução → bugs (infEvento) → deploys passou **>30 min** desde a autorização |
| **O que fazer nº 3** | Cupom **permanece autorizado** na SEFAZ. Caminho fiscal: **NF-e devolução mod. 55** (contador) referenciando a chave da NFC-e |
| **Código** | Mensagem corrigida (30 min + minutos desde emissão); §4.3 banana atualizado |

### Produção — fix cancelamento «infEvento não encontrado» (2026-06-23)

| Item | Valor |
| ---- | ----- |
| **Problema** | Devolução OK, NFC-e nº 3 não cancelou — «infEvento não encontrado» |
| **Causa** | XML do evento perdia `xmlns` na serialização antes da assinatura |
| **Fix** | `c0f0ef0` / produção `c89f3ef` — xmlns em `<evento>` + botão **Cancelar NFC-e** na venda |
| **Venda nº 3** | Já devolvida — após deploy: abrir venda → **Cancelar NFC-e** |

### Produção — deploy NFC-e cancelamento (2026-06-23, Renan pediu)

| Item | Valor |
| ---- | ----- |
| **Branch** | `producao` ← cherry-pick `7227d83` + `7395c2a` |
| **Commits loja** | `1d09438` (CPF PIX) · `20c33bb` (cancelamento SEFAZ na devolução) |
| **VERSION loja** | **1.97** |
| **Bug pós-deploy** | «infEvento não encontrado» — **corrigido** v1.99 |
| **Fix loja** | `c89f3ef` |

### NFC-e — regras resumidas + cancelamento na devolução (2026-06-23)

- **§4.3** — tabela «quando emite», popups PDV e devolução (documentação operacional).
- **Devolução:** tenta **cancelar NFC-e na SEFAZ** (evento 110111); motivo padrão *Devolucao de mercadoria registrada no sistema Agro.*; status local **Cancelada** se OK; aviso se prazo **30 min** ou falha.
- **Arquivos:** `nfce_sp_emissao_util.py` (`cancelar_nfce_autorizada`), `views.py`, `models.py`, `nfce_venda_util.py`, `venda_agro_detalhe.html`.

### Fix pós go-live NFC-e (2026-06-23, Renan)

| Problema | Resposta / fix |
| -------- | -------------- |
| **Devolução cancela cupom?** | **Sim, automaticamente** (SEFAZ SP, até **~30 min** desde autorização). Fora do prazo → **501** + aviso; estoque/caixa ajustados igual. |
| **PIX + impressão, cliente sem CPF** | Modal CPF (commit `7227d83`). |

### NFC-e — go-live **OK** (2026-06-23, Renan)

| Passo | O quê | Status |
| ----- | ----- | ------ |
| **1** | Vars Render produção (`TP_AMB=1`, CSC, cert, série 21) | **OK** |
| **2** | `/api/nfce/status/` `ativo: true` | **OK** |
| **3** | PDV + vendas → `producao` v1.93 (`9cf4522`) | **OK** |
| **4** | Venda real na loja + QR na consulta SEFAZ | **OK** (Renan) |

**Primeira NFC-e produção validada (23/06/2026):**

| Campo | Valor |
| ----- | ----- |
| Número / série | **nº 2** · série **21** |
| Protocolo | `135264232730394` |
| Pagamento | PIX · R$ 1,00 (venda teste) |
| Consumidor | não identificado |
| Consulta QR | **OK** — Renan conferiu no site SEFAZ SP («quente») |

**Operação:** PIX/cartão → NFC-e automática · dinheiro → escolha NFC/Venda · reemissão `/vendas/`.

**Render:** se próxima nota der rejeição **539** (número duplicado), `NFC_E_PROXIMO_NUMERO` = **3** (última autorizada foi **2**). O sistema também avança pelo Postgres após cada emissão OK.

**Pacote deploy:** `9cf4522` · `2386eb2` · revert = revert dos 2 commits em `producao`.

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
| **1** | Abrir portal + certificado A1 da loja no PC | OK |
| **2–3** | CSC produção na tela SEFAZ (Gerenciamento Cód Segurança) | OK (Renan 22/06) |
| **4** | `NFC_E_CSC_ID` + `NFC_E_CSC_TOKEN` no Render **produção** | **OK** (Renan) |
| **5** | Conferir `/api/nfce/status/` logado: `ativo: true`, `tp_amb: 1` | **OK** (Renan 22/06) |

**Mapa:** ID Token = `NFC_E_CSC_ID` (número, ex. `1` ou `000001`) · CSC = `NFC_E_CSC_TOKEN` (texto longo). **Produção** = menu **«com validade jurídica»** (não só homologação). CSC do **teste** ≠ CSC da **loja**.

### Paridade prod × teste — auditoria Git (2026-06-22)

**Backend core idêntico** (`git diff origin/producao origin/teste` — **sem diferença**): `mongo_financeiro_util.py`, `views.py`, `catalogo_agro.py`, `cadastro_codigo_sequencial_util.py`, `nfe_entrada_util.py`, migrations NFC-e.

**Loja (`producao` v2.03)** = NFC-e emissão v1.93 + cancelamento devolução v2.03 (`02cdb98`). **Cadastro Postgres / código 4010+** já estavam na loja — **não** estão pendentes.

### Só no teste — ainda NÃO na loja (2026-06-23)

Conferência: `git diff origin/producao origin/teste --stat` · **28 arquivos** · ~2k linhas (maioria front/PDV/BI). **NFC-e** (emissão + cancelamento) **já na loja** — não entra na lista.


| #   | Pacote                                | Arquivos / nota                                                                                                           | Risco loja                                                             |
| --- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| 1   | **Agro Display Scale**                | `agro_display_scale.js`, `_agro_display_scale.html`, `_agro_consulta_ui.html`, BI                                         | Médio — calibração global de tamanho de tela                           |
| 2   | **CP lista — perf/UX**                | `lancamentos_contas_pagar_teste.html` — bootstrap HTML, prefetch BI, badge «Sincronizando», colunas PIN, filtro hoje      | Baixo — só template; **API já igual** na loja                          |
| 3   | **Entrada NF — financeiro duplicado** | `entrada_nota.html` — pula etapa se título já existe; mensagens «já existia / recuperado»                                 | Baixo                                                                  |
| 4   | **BI — launchpad / escala rápida**    | `dashboard_gerencial.html`, `partials/dashboard_gerencial_body.html`, `home.html`                                         | Baixo                                                                  |
| 5   | **PDV entrega**                       | `partials/pdv/step_entrega.html`                                                                                          | Médio                                                                  |
| 6   | **Caixa — PIX MP QR**                 | `caixa_util.py` — normaliza «Mercado Pago … QR» como PIX                                                                  | Baixo                                                                  |
| 7   | **Config status staging**             | `agro_fonte_config.py` — `staging_readonly` no status; ERP sync só por env (sem auto-block checkpoint)                    | Baixo — prod mantém lógica checkpoint no sync                          |
| 8   | **Textos pré-corte ERP**              | `lancamentos_pre_corte_erp_panel.html`                                                                                    | Cosmético                                                              |
| 9   | **PIN calendário CP/DRE/fluxo**       | +1 linha include PIN em calendário/DRE/fluxo                                                                              | Baixo                                                                  |


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

### Mapa loja vs teste — resumo (2026-06-23)


| Ambiente     | VERSION | HEAD      | Nota |
| ------------ | ------- | --------- | ---- |
| **teste**    | v2.05   | `df2d1f7` | Homologação — banana sync outro chat |
| **produção** | v2.03   | `02cdb98` | Loja — NFC-e emissão v1.93 + cancelamento devolução v2.03 |

**Pacote cancelamento NFC-e na loja (outro chat):** `1d09438` (CPF PIX) · `20c33bb` (cancelamento SEFAZ) · `c89f3ef` (xmlns/retry) · `4995c0e` (501/30 min) · `02cdb98` (fix **225**).

**Próximos candidatos produção:** pacotes **#1–#9** da tabela «Só no teste» (Display Scale, CP perf, Entrada NF, …). NFC-e **já na loja**.

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
| **Assistente → produção** | **Só** com frase explícita (*«pode subir (produção)»*, etc.) **+ senha `99738595`** na **mesma** mensagem |


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
- [x] **PIX/cartão** — sem popup NFC/Venda; modal CPF se cliente sem CPF
- [x] **Devolução** — cancelamento SEFAZ automático · **OK produção** nº 4 (Renan 23/06) · prazo **30 min**
- [x] **Dinheiro** — popup NFC/Venda; modal CPF **grande** (v1.11+)
- [x] Toast falha fiscal **depois** da impressão Windows
- [x] **Produção** — emissão v1.93 · nº 2 série 21 · QR SEFAZ OK · cancelamento devolução **v2.03** · nº 4 cancelado (Renan 23/06/2026)

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

- [x] **Fiado** — Postgres nativo (`FiadoTituloAgro`); **fora** do pacote Lançamentos→Mongo
- [x] Lançamentos — backup ZIP + checkpoint (~17 703 títulos) + layout/PIN/perf
- [x] Corte Agro→ERP (API) — **produção v1.14** (`372f90f`; automático após checkpoint)
- [ ] **Próxima fase financeiro:** `AGRO_FONTE_FINANCEIRO=agro_pg` — Lançamentos **dados** Postgres (não espelho Mongo)
- [ ] **Nunca** merge `teste` inteiro em `producao` — só cherry-pick do escopo combinado
- [x] Catálogo Postgres **teste** — Renan OK 2026-06-22
- [x] Catálogo Postgres **produção** — v1.53
- [x] PDV catálogo + Gestão + ledger + Compras D4 — **teste** (deploy loja ~20h)
- [x] BI home financeiro → PG (cards CP/CR teste v3.23+)
- [ ] BI / resumo gerencial — gráfico gastos home ainda Mongo
- [ ] Transferências, Validade, fornecedor NF

**Outras:**

- [x] **PDV legado carrinho GM** — produção `59bdedc` v1.02 (2026-06-18)
- [x] **Lançamentos CP — perf abertura Chrome** — bootstrap + prefetch + cache (`teste` v1.48); Renan OK melhora sutil; **próximo salto adiado**
- [ ] **Lançamentos CP perf — roadmap adiado** — retomar: (A) `agro_pg` financeiro **ou** (B) lista no BI sem navegar **ou** (C) enxugar página CP. **Não** micro-otimizar até Renan pedir.
- [ ] **Layout novo CP + perf lista** → produção (cherry-pick quando Renan pedir)
- [ ] **PDV wizard carrinho GM** — em staging `teste`; Renan validar → cherry-pick produção quando pedir
- [ ] Dedupe clientes Mongo vs ERP por CPF (futuro)
- [x] Tela contabilidade — itens **1,2,3,4,8** — **teste + produção** v2.25 (2026-06-24)

---

## Roadmap — tela Contabilidade (`/contabilidade/`)

**Implementado (teste + loja v2.25):** resumo mês, CSV/XLSX, ZIP com `index.csv` + autorizadas/canceladas, login dedicado (`AGRO_CONTABILIDADE_USERNAMES`). Ver CHECKPOINT 1.0.66.

**Fluxo Renan:** sempre **teste primeiro** → validar no Render teste → **replicar produção** (mesmo código; NFC-e série **21** já é produção) quando pedir frase + senha.

**Hoje (antes v2.14):** só ZIP XML autorizadas.

**Objetivo:** hub para o **escritório** baixar fiscal + espelhos gerenciais, sem entrar no PDV.

### Prioridade alta (provável uso real)

| Ferramenta | Para quê | Esforço | Notas |
| ---------- | -------- | ------- | ----- |
| **Resumo do mês NFC-e** (antes do ZIP) | Qtd autorizadas / canceladas, total R$, faixa numeração série 21 | Baixo | ✅ teste v2.14 |
| **Planilha CSV/XLSX NFC-e** | Nº, série, chave, data/hora, valor venda, CPF consumidor, status, venda # | Baixo | ✅ teste v2.14 |
| **ZIP incluir canceladas + índice** | XML autorizado + marcar **Cancelada** no `index.csv` dentro do ZIP | Médio | ✅ teste v2.14 |
| **Atalho Lançamentos export** | Mesmo mês → CSV/XLSX/PDF financeiro (APIs já existem em `/lancamentos/`) | Baixo | ✅ teste v2.14 |
| **Atalho Vendas CSV** | `/vendas/exportar-csv/` filtrado por período | Baixo | ✅ teste v2.14 |

### Prioridade média

| Ferramenta | Para quê | Esforço |
| ---------- | -------- | ------- |
| **Caixa — fechamentos do mês** | PDF/CSV por sessão (gaveta): entradas, sangrias, formas | Médio — `caixa_util`, `MovimentoCaixa` |
| **Entrada NF / compras** | Resumo entradas no período (Mongo + Agro) | Médio |
| **DRE / resumo gerencial** | Link `/lancamentos/dre/` + `/financeiro/resumo-gerencial/` com mês | Baixo |
| **Pendências fiscais** | Lista NFC-e rejeitada/erro no mês (não autorizadas) | Baixo | ✅ teste v2.33+ — bloco recolhível + CSV |

### Prioridade baixa / fase 2

| Ferramenta | Para quê | Esforço |
| ---------- | -------- | ------- |
| **Usuário «Contabilidade»** | Login só leitura + export (sem PDV/caixa) | Médio | ✅ teste v2.14 — `AGRO_CONTABILIDADE_USERNAMES` |
| **Log «quem baixou o ZIP»** | Auditoria | Baixo |
| **E-mail automático mensal** | Enviar ZIP dia 1 | Alto — Render + SMTP |
| **NF-e devolução mod. 55** | Quando NFC-e passou 30 min e foi devolvida | Alto — emissão própria |
| **XML cancelamento (evento)** | Guardar e exportar procEventoNFe | Médio — hoje só mudamos status local |

### O que **não** misturar na contabilidade (por enquanto)

- ERP série **20** (continua no ERP legado; Agro é série **21**).
- RH folha completa (dado sensível — link separado se um dia).
- Edição de lançamentos (contador só **exporta**, não lança).

**Próximo passo sugerido (Renan escolhe):** bloco **Resumo + CSV + ZIP melhorado** num único patch na tela atual.
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

### Fim do checkpoint v1.0.77

*Próxima edição começa abaixo desta linha ou substituindo o bloco CHECKPOINT acima.*