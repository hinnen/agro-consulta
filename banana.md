# BANANA Ã¢â‚¬â€ GM Agro / loja Jacupiranga (anexe com `@banana`)

**Loja principal GM Agro** Ã¢â‚¬â€ teste Render, produÃƒÂ§ÃƒÂ£o, pacotes, operaÃƒÂ§ÃƒÂ£o diÃƒÂ¡ria. O **produto SisVale** no geral estÃƒÂ¡ em **`SISTVALE.md`**; a instÃƒÂ¢ncia **delivery em branco** estÃƒÂ¡ em **`FOOD.md`**.

**Este ÃƒÂ© o anexo de contexto da loja GM.** Leitura guiada: **`banana-roteiro.md`** (fluxograma Ã¢â‚¬â€ ler **antes** deste arquivo). O `AGENTS.md` ÃƒÂ© enciclopÃƒÂ©dia; regra Cursor em `.cursor/rules/agro-consulta.mdc` (**Ã‚Â§0 = roteiro + trechos necessÃƒÂ¡rios**, nÃƒÂ£o o banana inteiro salvo exceÃƒÂ§ÃƒÂµes no roteiro Ã‚Â§5).


| VocÃƒÂª querÃ¢â‚¬Â¦                            | FaÃƒÂ§a                                                  |
| ------------------------------------- | ----------------------------------------------------- |
| **Loja GM Agro** (PDV, caixa, CPÃ¢â‚¬Â¦)    | Descreva a tarefa (roteiro auto) ou **`@banana-roteiro`** |
| **FOOD** (delivery em branco)         | Anexe **`@FOOD`** + `FOOD.md`                         |
| VisÃƒÂ£o produto SisVale / multi-cliente | `SISTVALE.md`                                         |
| Detalhe fino de UX / RH / LanÃƒÂ§amentos | Some `@AGENTS.md` (Ã‚Â§5Ã¢â‚¬â€œ11, Ã‚Â§9, Ã‚Â§10)                    |
| Registrar onde paramos (GM)           | AutomÃƒÂ¡tico no fim da sessÃƒÂ£o; ou *"atualize a banana"* |
| DecisÃƒÂ£o permanente no changelog       | *"atualize o AGENTS"* (raro; sÃƒÂ³ quando vocÃƒÂª pedir)    |


**Assistente:** **1Ã‚Âª aÃƒÂ§ÃƒÂ£o** = ler **`banana-roteiro.md` inteiro** e seguir o fluxograma (trechos do `banana.md`, nÃƒÂ£o o arquivo todo Ã¢â‚¬â€ ver roteiro Ã‚Â§5). Registrar alteraÃƒÂ§ÃƒÂµes no `banana.md` quando necessÃƒÂ¡rio **sem pedir**. WIP e checkpoint no CHECKPOINT. Modo econÃƒÂ´mico: rule `.cursor/rules/modo-economico.mdc`.

**ProduÃƒÂ§ÃƒÂ£o (regra dura Ã¢â‚¬â€ 2026-06-22, senha 2026-06-24):** **Nunca** `git push origin producao`, merge `teste`Ã¢â€ â€™`producao`, cherry-pick na loja ou deploy Render de produÃƒÂ§ÃƒÂ£o **sem as duas coisas abaixo na mesma mensagem do Renan:**

1. Frase explÃƒÂ­cita Ã¢â‚¬â€ *Ã‚Â«pode subir (para produÃƒÂ§ÃƒÂ£o)Ã‚Â»* / *Ã‚Â«pode ir para produÃƒÂ§ÃƒÂ£oÃ‚Â»* / *Ã‚Â«sobe pacote vX.XX para produÃƒÂ§ÃƒÂ£oÃ‚Â»* (ou equivalente claro).
2. **Senha de autorizaÃƒÂ§ÃƒÂ£o loja** Ã¢â‚¬â€ Renan digitou no chat **2026-06-24** (madrugada, chat PDV): **`99738595`**. Tem que aparecer **no mesmo pedido** que a frase acima. SÃƒÂ³ a frase **sem** senha = **nÃƒÂ£o sobe**. SÃƒÂ³ a senha **sem** frase = **nÃƒÂ£o sobe**.

**NÃƒÂ£o vale como autorizaÃƒÂ§ÃƒÂ£o:** *Ã‚Â«faÃƒÂ§aÃ‚Â»*, *Ã‚Â«segueÃ‚Â»*, *Ã‚Â«ok no testeÃ‚Â»*, *Ã‚Â«banana.md faÃƒÂ§aÃ‚Â»* Ã¢â‚¬â€ **mesmo em portuguÃƒÂªs**. Renan **nÃƒÂ£o fala inglÃƒÂªs**; respostas do assistente **sempre em portuguÃƒÂªs (BR)** (ver linha acima).

**Ordem:** ao **fechar entrega** â†’ commit na branch `teste` + **`git push origin teste`** (backup GitHub, **sem** pedir autorizaÃ§Ã£o) â†’ Renan valida no **PC local** (`docs/TESTE-LOCAL.md`) â†’ **sÃ³ entÃ£o** produÃ§Ã£o, se ele pedir com frase + senha. Corrigir bug â‰  autorizaÃ§Ã£o para loja. Loja operando = **zero** push produÃ§Ã£o por iniciativa do assistente.

**ProduÃ§Ã£o â€” chat canÃ´nico (2026-06-22, Renan):** Push na loja (**SistVale** / branch `producao`) **sÃ³ neste chat** daqui pra frente. Renan pode ter subido produÃ§Ã£o em **outro chat** ou **post** â€” o `banana.md` e o CHECKPOINT sÃ£o a **fonte da verdade**; ao abrir este chat, o assistente **relÃª o CHECKPOINT** e **pergunta** se algo mudou antes de cherry-pick. **NÃ£o** assumir que Â«Ãºltimo commit deste chatÂ» = produÃ§Ã£o.

**Teste / backup GitHub (regra â€” 2026-07-30, Renan Â· substitui 22/07 Â«nÃ£o push sozinhoÂ»):**  
**ValidaÃ§Ã£o** = **local no PC** (`runserver` + Chrome `http://127.0.0.1:8000`). Ver **`docs/TESTE-LOCAL.md`**.  
**Render staging (free) NÃƒO Ã© gate:** dorme â€” nÃ£o confiar nele para liberar loja.  
**Assistente â€” ao fechar entrega (fix/feature/pacote):** **sempre** `git commit` na branch `teste` **e** `git push origin teste` â€” **sem perguntar** e **sem** frase/senha. Motivo: se o PC estragar, o cÃ³digo jÃ¡ estÃ¡ no GitHub.  
**NÃ£o** push a cada meio caminho quebrado (sÃ³ entrega fechada / checkpoint Ãºtil). Bug no `teste` **nÃ£o** quebra a loja â€” histÃ³rico Git permite voltar.  
**ProduÃ§Ã£o:** continua **sÃ³** com frase + senha **depois** de Renan testar local. **Dois repos Git nÃ£o sÃ£o necessÃ¡rios** â€” `teste` (rascunho+backup) e `producao` (loja) bastam.

**Registro no banana (regra Ã¢â‚¬â€ 2026-06-22):** **Toda alteraÃƒÂ§ÃƒÂ£o** que mude o sistema (fix, feature, deploy teste ou produÃƒÂ§ÃƒÂ£o) Ã¢â€ â€™ **registrar no `banana.md`** ao fechar a tarefa (CHECKPOINT: o quÃƒÂª mudou, commits, versÃƒÂ£o `VERSION`, teste OK ou pendente). Serve para **contexto do prÃƒÂ³ximo chat**, **diagnosticar problema** e **saber o que reverter**. Assistente **nÃƒÂ£o pergunta** se deve registrar Ã¢â‚¬â€ faz sempre que entregar cÃƒÂ³digo ou deploy. Detalhe passageiro ou chat sÃƒÂ³ explicativo: nÃƒÂ£o inflar o doc.

---

## 0. TL;DR (leia em 1 minuto)


| Item                         | Valor                                                                                                                                                                |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Produto**                  | **SisVale** / **Agro Consulta** Ã¢â‚¬â€ sistema web da **GM Agro** (loja agropecuÃƒÂ¡ria, Jacupiranga-SP)                                                                     |
| **UsuÃƒÂ¡rios**                 | Operadores de loja (PDV, caixa), gestÃƒÂ£o, financeiro, RH, compras                                                                                                     |
| **Stack**                    | Django + Postgres (Agro) + Mongo (espelho ERP) + Render + Electron opcional                                                                                          |
| **Branch dia a dia**         | `**teste`** = commit + **push GitHub ao fechar entrega** (backup) Â· validaÃ§Ã£o = **local PC** Â· `**producao**` = loja **sÃ³** frase+senha |
| **Tela inicial**             | `/` = BI gerencial Ã‚Â· PDV principal em `/consulta/` e wizard `/pdv/checkout/`                                                                                         |
| **Regra de ouro**            | Operador usa **saldo do Agro**; ERP alimenta Mongo; Agro nÃƒÂ£o devolve estoque ao ERP                                                                                  |
| **UX loja**                  | Compacto, alto contraste, teclado/scanner primeiro, paleta emerald/orange/slate                                                                                      |
| **Escala de tela**           | **Agro Display Scale** global (nÃƒÂ£o zoom do Chrome) Ã¢â‚¬â€ ver AGENTS.md Ã‚Â§11                                                                                               |
| **Listagem loja (WhatsApp)** | **Completa** (CHECKPOINT) Ã‚Â· enviar p/ loja **a cada 2 dias** desde **29/06** Ã‚Â· prÃƒÂ³x.: **01/07**, **03/07**Ã¢â‚¬Â¦                                                          |
| **Cliente Renan (loja/dev)** | **Google Chrome** Ã‚Â· **teste = local** (`docs/TESTE-LOCAL.md`) Ã‚Â· Render staging free **nÃƒÂ£o** ÃƒÂ© gate Ã‚Â· Electron descartado |


---

## 1. O negÃƒÂ³cio (nÃƒÂ£o tÃƒÂ©cnico)

**GM Agro** opera uma loja fÃƒÂ­sica. O SisVale substitui/complementa telas do ERP legado com foco em:

- **BalcÃƒÂ£o:** vender rÃƒÂ¡pido (busca, scanner, formas de pagamento, entrega, cupom).
- **Estoque:** ver saldo confiÃƒÂ¡vel, ajustar, transferir, entrada de NF.
- **GestÃƒÂ£o:** cadastro de produtos, preÃƒÂ§os, compras, validade.
- **Financeiro:** contas a pagar/receber espelhando Mongo do ERP + lanÃƒÂ§amentos manuais Agro.
- **Caixa:** turnos (gaveta / notebook / teste), sangria, fechamento.
- **RH:** folha, vales, ficha do funcionÃƒÂ¡rio.
- **Fiscal:** NFC-e modelo 65 emitida **pelo Agro** (sÃƒÂ©rie prÃƒÂ³pria, separada do ERP).

**Operadores:** muitos sÃƒÂ£o idosos Ã¢â‚¬â€ botÃƒÂµes grandes, poucos cliques, sem textos longos na tela (ajuda em Ã‚Â«?Ã‚Â» ou modal).

**Renan (dono/dev):** valida no **PC local** (`docs/TESTE-LOCAL.md`). Assistente **sempre** push `origin teste` ao **fechar entrega** (backup GitHub); **produÃ§Ã£o sÃ³** com frase + senha **apÃ³s** teste local (ver topo Â· Â§0.2 roteiro).

**Como acessa o SisVale:** **Chrome** (loja e local). **Electron** descartado na loja Ã¢â‚¬â€ performance ruim. UX/perf validar no Chrome.

---

## 2. Arquitetura tÃƒÂ©cnica

```
[Navegador / Electron]
        Ã¢â€ â€œ HTTP
[Django config/] Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€ â€™ produtos/ (maioria das telas e APIs)
        Ã¢â€â€š              Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ Postgres: vendas, clientes, overlay produto, caixa, NFC-e XMLÃ¢â‚¬Â¦
        Ã¢â€â€š              Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ Mongo (leitura + regras): catÃƒÂ¡logo ERP, financeiro, estoque espelho
        Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ estoque/ (APIs em /estoque/)
        Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ financeiro/ (/api/financeiro/)
        Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ integracoes/ (ponte ERP, venda ERP Mongo)
        Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ rh/
[Render] Gunicorn Ã‚Â· health /healthz
```

### 2.1 Duas bases de dados Ã¢â‚¬â€ papÃƒÂ©is


| Base              | O quÃƒÂª fica aqui                                                                                                      |
| ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Mongo**         | CatÃƒÂ¡logo ERP (`DtoProduto`), estoque depÃƒÂ³sito, financeiro (`DtoLancamento`), vendas ERP histÃƒÂ³ricas                   |
| **Postgres Agro** | `VendaAgro`, `ClienteAgro`, overlay cadastro (`ProdutoOverlayAgro`), caixa, `NfceDocumentoAgro`, ajustes estoque, RH |


**Overlay Agro:** camada PostgreSQL que sobrescreve/complementa campos do produto sem gravar de volta no ERP (preÃƒÂ§o loja, cÃƒÂ³digo NFe, flags).

### 2.2 Staging vs produÃƒÂ§ÃƒÂ£o (Mongo compartilhado)

Staging **lÃƒÂª** o mesmo Mongo da loja mas **nÃƒÂ£o pode escrever** nele:

- `AGRO_STAGING_READONLY=true`
- `AGRO_ERP_PEDIDOS_DRY_RUN=true`
- `DATABASE_URL` = Postgres **sÃƒÂ³ do staging**

Detalhes: `docs/DEPLOY-AMBIENTES.md`.

---

## 3. Deploy e Git

**Renan Ã¢â‚¬â€ duas vertentes no Render (dois Ã‚Â«sitesÃ‚Â»):**


| Como o Renan fala                 | Branch Git | Projeto Render (Dashboard) | Papel                                |
| --------------------------------- | ---------- | -------------------------- | ------------------------------------ |
| **Teste / staging / homologaÃƒÂ§ÃƒÂ£o** | `teste`    | **teste**                  | Onde **sempre** valida antes da loja |
| **ProduÃƒÂ§ÃƒÂ£o / loja / SistVale**    | `producao` | **SistVale**               | Loja de verdade                      |


**NÃƒÂ£o testa local** Ã¢â‚¬â€ o ambiente de prova ÃƒÂ© o Render **teste**. Deploy do `teste` ÃƒÂ© **automÃƒÂ¡tico** no Render apÃƒÂ³s push.


| Ambiente (como falar) | Branch Git | Render (serviÃƒÂ§o)                               |
| --------------------- | ---------- | ---------------------------------------------- |
| **Teste / staging**   | `teste`    | projeto **teste** (ex.: agro-consulta-staging) |
| **ProduÃƒÂ§ÃƒÂ£o / loja**   | `producao` | projeto **SistVale** (Sistvale - ProduÃƒÂ§ÃƒÂ£o)     |


- `**main` / `principal` nÃƒÂ£o entram no deploy.**
- **Assistente:** push `**teste` livre** (Renan testa no site teste). Merge/push `**producao` sÃƒÂ³** quando Renan autorizar.
- ApÃƒÂ³s merge produÃƒÂ§ÃƒÂ£o: `python manage.py migrate` no ambiente (Render faz no deploy).

### 3.2 Pausa antes do deploy loja (Renan 30/06)

**Hoje nÃƒÂ£o existe** trava automÃƒÂ¡tica no SisVale antes de subir pacote na **loja**. O Render reinicia o serviÃƒÂ§o (~1Ã¢â‚¬â€œ3 min build + alguns segundos de troca); quem estiver **no meio** de finalizar venda pode ver erro de rede Ã¢â‚¬â€ em geral **Ctrl+F5** e tentar de novo.

| O quÃƒÂª | SituaÃƒÂ§ÃƒÂ£o |
| ----- | -------- |
| **Modo manutenÃƒÂ§ÃƒÂ£o no cÃƒÂ³digo** | **NÃƒÂ£o tem** (pendÃƒÂªncia futura Ã¢â‚¬â€ ver fila **FL-038** abaixo) |
| **PÃƒÂ¡gina Ã‚Â«Sistema em ManutenÃƒÂ§ÃƒÂ£oÃ‚Â»** | HTML legado em `produtos/templates/produtos/MANUTENÃƒâ€¡ÃƒÆ’O, BASTA RENOMEAR.txt` Ã¢â‚¬â€ **hack manual**, nÃƒÂ£o usado no fluxo normal |
| **`AGRO_STAGING_READONLY`** | SÃƒÂ³ **teste** Ã‚Â· bloqueia Mongo Ã‚Â· **nÃƒÂ£o** trava PDV da loja |
| **IdempotÃƒÂªncia venda** | Duplo clique / retry no **Finalizar** tende a **nÃƒÂ£o duplicar** venda (jÃƒÂ¡ no sistema) |
| **Gunicorn loja** | `--timeout 180` Ã¢â‚¬â€ requisiÃƒÂ§ÃƒÂ£o em andamento pode terminar antes do kill |

**Rotina recomendada (operacional Ã¢â‚¬â€ atÃƒÂ© existir trava no sistema):**

1. Escolher **janela calma** (evitar meio-dia cheio e fechamento de caixa se possÃƒÂ­vel).
2. **Aviso no Zap da loja:** *Ã‚Â«AtualizaÃƒÂ§ÃƒÂ£o em ~2 min Ã¢â‚¬â€ nÃƒÂ£o finalize venda agora; quem jÃƒÂ¡ clicou pode aguardar ou F5 e repetir.Ã‚Â»*
3. No Render **SistVale**: deploy **manual** (ou confirmar auto-deploy) e **acompanhar** atÃƒÂ© Ã‚Â«LiveÃ‚Â».
4. **Ctrl+F5** nos PDVs abertos Ã‚Â· venda teste rÃƒÂ¡pida (R$ 0,01) se quiser conferir.

**PendÃƒÂªncia produto (fila):** **FL-038** Ã¢â‚¬â€ **Ã‚Â§3.2.0** (leigo) Ã‚Â· **Ã‚Â§3.2.1+** (tÃƒÂ©cnico).

#### 3.2.0 FL-038 Ã¢â‚¬â€ em poucas palavras (leigo)

**O que ÃƒÂ©:** uns **2 minutos** antes de atualizar o sistema na **loja**, o SisVale entra num modo *Ã‚Â«estou atualizando Ã¢â‚¬â€ segura a mÃƒÂ£o no botÃƒÂ£o que grava dinheiroÃ‚Â»*.

**O que a loja vÃƒÂª:** faixa **laranja** no topo (PDV, caixa, financeiro Ã¢â‚¬â€ conforme fase). BotÃƒÂµes tipo **Finalizar venda**, **reforÃƒÂ§o/retirada**, **devoluÃƒÂ§ÃƒÂ£o**, **baixar fiado**, **baixar conta** ficam **desligados** por uns instantes.

**O que pode fazer normal:** buscar produto, montar carrinho, olhar listas, filtros, BI, histÃƒÂ³rico Ã¢â‚¬â€ **nada se perde** no rascunho.

**Se alguÃƒÂ©m jÃƒÂ¡ tinha clicado Ã‚Â«confirmarÃ‚Â»:** se o pedido **jÃƒÂ¡ entrou**, termina sozinho. Se deu erro por causa da atualizaÃƒÂ§ÃƒÂ£o, **clicar de novo no mesmo botÃƒÂ£o** (mesma operaÃƒÂ§ÃƒÂ£o) **nÃƒÂ£o duplica** venda Ã¢â‚¬â€ o sistema reconhece o retry.

**Se clicar Ã‚Â«confirmarÃ‚Â» depois que a faixa apareceu** (operaÃƒÂ§ÃƒÂ£o **nova**): aparece aviso Ã¢â‚¬â€ **espera ~2 min** e tenta de novo.

**Quem liga e desliga:** no deploy automÃƒÂ¡tico (Renan manda *pode subir produÃƒÂ§ÃƒÂ£o* + senha), o assistente **liga** a contingÃƒÂªncia Ã¢â€ â€™ sobe a versÃƒÂ£o no Render Ã¢â€ â€™ espera ficar **Live** Ã¢â€ â€™ **desliga**. Se o deploy **falhar**, desliga do mesmo jeito Ã¢â‚¬â€ **a loja nÃƒÂ£o fica travada**.

**Aviso Zap (opcional):** *Ã‚Â«Atualizando o sistema ~2 min Ã¢â‚¬â€ nÃƒÂ£o finalize venda nem baixa agora. Quem jÃƒÂ¡ confirmou, aguarde.Ã‚Â»*

**Por fases (nÃƒÂ£o tudo no dia 1):** primeiro **finalizar venda** Ã‚Â· depois **caixa, devoluÃƒÂ§ÃƒÂ£o e fiado** Ã‚Â· por ÃƒÂºltimo **contas a pagar** (baixa e lanÃƒÂ§amento novo), quando estiver redondo contra duplicata.

**Hoje (sem FL-038 cÃƒÂ³digo):** sÃƒÂ³ aviso no Zap + escolher horÃƒÂ¡rio calmo Ã¢â‚¬â€ ver rotina acima Ã‚Â· **Renan 30/06:** neste deploy **v5.44** usa **sÃƒÂ³ rotina manual**; implementar FL-038 (fases A+B) em deploy futuro.

**Renan 30/06 Ã¢â‚¬â€ fila offline:** ideia de vender no PC e subir depois Ã¢â€ â€™ **FL-041 P3** (projeto grande). **Curto prazo:** FL-038 manual + idempotÃƒÂªncia venda jÃƒÂ¡ existente.

#### 3.2.1 FL-038 Ã¢â‚¬â€ como funcionaria programado (rascunho tÃƒÂ©cnico)

**Ideia:** trava **sÃƒÂ³ o PDV** (Finalizar venda) por alguns minutos enquanto sobe pacote na **loja**. NÃƒÂ£o mexe preÃƒÂ§o, catÃƒÂ¡logo, estoque nem **Contas a pagar**.

**Importante Ã¢â‚¬â€ env no Render nÃƒÂ£o serve para ligar/desligar rÃƒÂ¡pido:** mudar `AGRO_PDV_PAUSA_DEPLOY` no painel **dispara outro restart** do serviÃƒÂ§o. Seriam **dois** reinÃƒÂ­cios (pausa + deploy). **DecisÃƒÂ£o v2:** flag de pausa em **Postgres** (leitura a cada request) Ã‚Â· ligar/desligar por **HTTP** sem redeploy extra.

| Mecanismo | Papel |
| --------- | ----- |
| **`AgroDeployPausaPdv`** (Postgres) ou linha ÃƒÂºnica em tabela de config | `ativo` + `desde` + `motivo` Ã¢â‚¬â€ todos os workers Gunicorn veem na hora |
| **`GET /api/cron/pdv-pausa-deploy/?token=Ã¢â‚¬Â¦&on=1`** | Liga pausa (mesmo token dos crons / token deploy) |
| **`Ã¢â‚¬Â¦&on=0`** | Desliga pausa |
| **`AGRO_PDV_PAUSA_MENSAGEM`** (env, opcional) | Texto fixo do banner |

**TrÃƒÂªs camadas (complementares):**

| Camada | O quÃƒÂª faz | Operador vÃƒÂª |
| ------ | --------- | ----------- |
| **1 Ã¢â‚¬â€ Banner** | Bootstrap PDV lÃƒÂª flag no servidor | Faixa laranja no topo |
| **2 Ã¢â‚¬â€ JS** | **F9 / Confirmar** desabilitado se pausa | NÃƒÂ£o clica Finalizar |
| **3 Ã¢â‚¬â€ API** | POSTs de **fechar venda PDV** Ã¢â€ â€™ **503** JSON claro | Protege PDV aberto antes da pausa |

**Rotas bloqueadas (v1 Ã¢â‚¬â€ sÃƒÂ³ PDV):**

| Rota | Motivo |
| ---- | ------ |
| `POST /api/enviar-pedido-erp/` | Finalizar venda wizard |
| `POST /api/pdv/mp-point/finalizar/` | MP Point |
| `POST /api/pdv/entrega-pendente/<id>/finalizar/` | Entrega pendente |

**O que continua liberado:**

- Montar carrinho, busca, F6 orÃƒÂ§amento, F8 relacionamento, **caixa**, **BI**
- **Contas a pagar** Ã¢â‚¬â€ listar, filtrar, **baixa**, editar tÃƒÂ­tulo (**fora** do FL-038 v1)
- Rascunho checkout Ã¢â‚¬â€ carrinho nÃƒÂ£o se perde

#### 3.2.2 E se ligar a pausa com alguÃƒÂ©m no meio da operaÃƒÂ§ÃƒÂ£o?

**Finalizando venda no PDV**

| Momento | O que acontece |
| ------- | -------------- |
| **Ainda montando** carrinho / pagamento | Pausa sÃƒÂ³ trava **Finalizar** Ã¢â‚¬â€ pode continuar montando |
| **Clicou Finalizar** e request **jÃƒÂ¡ entrou** no servidor | Termina **normalmente** (checagem ÃƒÂ© na **entrada** do POST) |
| **Clicou Finalizar** e deu erro no **restart** do deploy | **Mesmo** Finalizar de novo Ã¢â‚¬â€ **idempotÃƒÂªncia** (`client_request_id`) **nÃƒÂ£o duplica** venda; com pausa ligada, retry com **mesma chave** **passa** mesmo bloqueado |
| **Clicou Finalizar** **depois** da pausa (venda **nova**) | **503** Ã¢â‚¬â€ aguardar fim do deploy |
| PDV aberto **sem F5** | Banner pode demorar ~30s atÃƒÂ© prÃƒÂ³ximo bootstrap; **API** jÃƒÂ¡ bloqueia Finalizar |

**Baixa / lanÃƒÂ§amento em Contas a pagar**

| | |
|---|---|
| **v1 FL-038** | **NÃƒÂ£o bloqueia CP** Ã¢â‚¬â€ baixa segue |
| **Risco real** | SÃƒÂ³ o **restart** do Render (como hoje), nÃƒÂ£o a flag PDV |
| **Se no futuro** quiser travar CP tambÃƒÂ©m | FL-038 **v2** Ã¢â‚¬â€ rotas `api/lancamentos/baixa*` (decidir com Renan) |

**Caixa (fechar turno, sangria):** igual CP Ã¢â‚¬â€ **fora** do v1; risco sÃƒÂ³ restart.

#### 3.2.3 AutomaÃƒÂ§ÃƒÂ£o no deploy loja (decisÃƒÂ£o Renan 30/06)

**Sim Ã¢â‚¬â€ quando Renan mandar *Ã‚Â«pode subir produÃƒÂ§ÃƒÂ£oÃ‚Â»* + senha `99738595` na mesma mensagem**, o assistente **pode** rodar a sequÃƒÂªncia (apÃƒÂ³s FL-038 implementado + token configurado):

```
1. HTTP loja Ã¢â€ â€™ pausa ON   (/api/cron/pdv-pausa-deploy/?token=Ã¢â‚¬Â¦&on=1)
2. ~5Ã¢â‚¬â€œ10 s                  (propaga Postgres; opcional Zap Ã‚Â«atualizando ~2 minÃ‚Â»)
3. cherry-pick / push       branch producao (pacote combinado)
4. aguardar Render          Ã‚Â«LiveÃ‚Â» (MCP/API Render)
5. HTTP loja Ã¢â€ â€™ pausa OFF    (Ã¢â‚¬Â¦&on=0)
6. CHECKPOINT banana        pacote Ã‚Â· commits Ã‚Â· Ctrl+F5
```

| Item | Detalhe |
| ---- | ------- |
| **Script alvo** | `scripts/deploy_producao_com_pausa.ps1` (ou `.sh` no Render Ã¢â‚¬â€ hoje assistente roda da mÃƒÂ¡quina Renan) |
| **Token** | Reutilizar `ALERTA_VENDAS_CRON_TOKEN` ou env dedicado `AGRO_DEPLOY_PAUSE_TOKEN` |
| **PrÃƒÂ©-requisito** | FL-038 **cÃƒÂ³digo** jÃƒÂ¡ na loja (primeiro deploy **sem** pausa automÃƒÂ¡tica, ou pausa manual sÃƒÂ³ Zap) |
| **Falha no meio** | Se deploy falhar: **despausa** (`on=0`) mesmo assim Ã¢â‚¬â€ loja nÃƒÂ£o fica travada |
| **Renan manual** | Continua valendo Zap + janela calma; automaÃƒÂ§ÃƒÂ£o **substitui** ir no Render ligar env |

**Retry / idempotÃƒÂªncia (venda):** ver tabela em Ã‚Â§3.2.1 Ã¢â‚¬â€ chave **nova** bloqueada; **mesma** chave (retry) libera.

**O que a pausa NÃƒÆ’O cobre (v1):** CP Ã‚Â· caixa Ã‚Â· site inteiro offline Ã‚Â· legado `/consulta/` (incluir depois se precisar).

**Estimativa:** 1 patch FL-038 (Postgres + cron ON/OFF + 3 rotas PDV + banner + F9) + 1 script deploy automÃƒÂ¡tico Ã‚Â· testar no staging.

**Fluxo manual legado (sÃƒÂ³ env Ã¢â‚¬â€ descartado para toggle):** ~~Render env true/false~~ Ã¢â‚¬â€ gera restart duplo; manter env sÃƒÂ³ para **mensagem** opcional.

#### 3.2.4 ContingÃƒÂªncia ampla Ã¢â‚¬â€ Renan 30/06 (venda Ã‚Â· caixa Ã‚Â· fiado Ã‚Â· lanÃƒÂ§amentos)

**Pergunta Renan:** nÃƒÂ£o seria melhor **contingÃƒÂªncia** cobrindo tambÃƒÂ©m reforÃƒÂ§o, retirada, devoluÃƒÂ§ÃƒÂ£o, baixa fiado e lanÃƒÂ§amentos (baixa + novo)?

**OpiniÃƒÂ£o assistente Ã¢â‚¬â€ sim no conceito, em fases no cÃƒÂ³digo:**

| PrÃƒÂ³s | Contras |
| ---- | ------- |
| Protege **todo movimento de dinheiro** no minuto do restart | Janela de deploy **congela mais gente** (financeiro no meio da tarde) |
| Uma regra mental: *Ã‚Â«atualizando Ã¢â‚¬â€ nÃƒÂ£o grave nada crÃƒÂ­ticoÃ‚Â»* | **LanÃƒÂ§amentos** hoje tÃƒÂªm idempotÃƒÂªncia **menos uniforme** que o PDV Ã¢â‚¬â€ retry cego pode **duplicar baixa** se nÃƒÂ£o auditar antes |
| Alinha com automaÃƒÂ§ÃƒÂ£o *pode subir* + senha | Patch **maior** (mais rotas + banners em caixa/CP) |

**RecomendaÃƒÂ§ÃƒÂ£o:** um ÃƒÂºnico modo **`contingencia_deploy`** no Postgres (nÃƒÂ£o sÃƒÂ³ Ã‚Â«pausa PDVÃ‚Â»), com **escopos** ligados juntos no deploy automÃƒÂ¡tico:

| Escopo | O que trava (POST que grava) | Telas (banner laranja) |
| ------ | ---------------------------- | ---------------------- |
| **`pdv`** | Finalizar venda Ã‚Â· MP Point Ã‚Â· entrega pendente | Wizard PDV |
| **`caixa`** | `api/caixa/movimento/` (reforÃƒÂ§o + retirada) Ã‚Â· fechar caixa (se POST dedicado) | Painel caixa |
| **`devolucao`** | `api/venda/Ã¢â‚¬Â¦/devolver/` | Consulta venda / fluxo devoluÃƒÂ§ÃƒÂ£o |
| **`fiado`** | `api/fiado/baixa*` (3 rotas) | PDV fiado / caixa fiado |
| **`lancamentos`** | `api/lancamentos/baixa/` Ã‚Â· `baixa-parcial/` Ã‚Â· `criar-manual-lote/` Ã‚Â· `saida-caixa/` (avaliar lista final no cÃƒÂ³digo) | Contas a pagar / novo manual |

**Ordem de implementaÃƒÂ§ÃƒÂ£o sugerida:**

| Fase | Escopos | Por quÃƒÂª |
| ---- | ------- | ------- |
| **A** | `pdv` | Maior volume balcÃƒÂ£o Ã‚Â· idempotÃƒÂªncia **jÃƒÂ¡ forte** Ã‚Â· menor risco |
| **B** | `caixa` + `devolucao` + `fiado` | Mesmo Ã‚Â«balcÃƒÂ£o/caixaÃ‚Â» Ã‚Â· poucos POSTs Ã‚Â· Renan sente diferenÃƒÂ§a na loja |
| **C** | `lancamentos` | SÃƒÂ³ depois de **auditar idempotÃƒÂªncia** (ou chave por baixa) nas APIs financeiras |

**No deploy automÃƒÂ¡tico** (*pode subir* + senha): ligar **todos os escopos A+B** (e **C** quando fase C estiver pronta) num ÃƒÂºnico `on=1` Ã‚Â· desligar tudo no `on=0`.

**Quem jÃƒÂ¡ estava gravando quando liga contingÃƒÂªncia:**

| MÃƒÂ³dulo | Comportamento alvo (igual PDV) |
| ------ | ------------------------------ |
| Venda PDV | Request **dentro** Ã¢â€ â€™ termina Ã‚Â· **retry mesma chave** Ã¢â€ â€™ passa Ã‚Â· **nova** operaÃƒÂ§ÃƒÂ£o Ã¢â€ â€™ 503 |
| Caixa / fiado / devoluÃƒÂ§ÃƒÂ£o | Mesma regra **se** tiver idempotÃƒÂªncia; senÃƒÂ£o sÃƒÂ³ bloqueia **novo** POST (retry = operador espera despausa Ã¢â‚¬â€ aceitÃƒÂ¡vel 2 min) |
| LanÃƒÂ§amentos baixa/novo | **Fase C** Ã¢â‚¬â€ priorizar **nÃƒÂ£o duplicar** tÃƒÂ­tulo quitado |

**O que continua liberado sempre:** consultar listas, filtros, relatÃƒÂ³rios, montar carrinho, rascunhos, BI Ã¢â‚¬â€ **sÃƒÂ³ trava o Ã‚Â«confirmar/gravarÃ‚Â»**.

**Renomear fila:** FL-038 = **ContingÃƒÂªncia deploy** (nome amigÃƒÂ¡vel Zap: *Ã‚Â«Sistema em atualizaÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ aguarde 2 min para finalizar venda ou baixaÃ‚Â»*).

**DecisÃƒÂ£o pendente Renan:** fase **B** entra junto com **A** no primeiro pacote, ou **A** sozinha na loja e **B** no deploy seguinte?

**Regra Renan (resumo Ã¢â‚¬â€ 23/06/2026):** cada entrega de **cÃƒÂ³digo** no **teste** sobe **+0,01** no badge (`2.03` Ã¢â€ â€™ `2.04` Ã¢â€ â€™ `2.05` Ã¢â‚¬Â¦). A **loja** fica parada no ÃƒÂºltimo pacote que vocÃƒÂª subiu (hoje **v2.03**) atÃƒÂ© pedir produÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ aÃƒÂ­ a loja **pula** para o mesmo nÃƒÂºmero do pacote (ex. teste jÃƒÂ¡ em **v2.05** Ã¢â€ â€™ sobe pacote Ã¢â€ â€™ loja vira **v2.05**). NÃƒÂ£o ÃƒÂ© **+0,1** (dez centÃƒÂ©simos); ÃƒÂ© **+0,01** (um centÃƒÂ©simo). SÃƒÂ³ `banana.md` / docs **nÃƒÂ£o** contam (`SKIP_VERSION_BUMP=1`).

| O quÃƒÂª | Detalhe |
| ----- | ------- |
| **Arquivo** | `VERSION` na raiz (ex.: `1.93`) |
| **Badge BI** | `v` + conteÃƒÂºdo de `VERSION` em **cada** site (teste e loja tÃƒÂªm arquivo prÃƒÂ³prio no branch) |
| **O que o nÃƒÂºmero significa** | **RÃƒÂ³tulo do ÃƒÂºltimo pacote** que aquele ambiente exibe Ã¢â‚¬â€ **nÃƒÂ£o** significa que teste e loja rodam o **mesmo cÃƒÂ³digo** |
| **Branch `teste`** | Hook sobe **+0,01** a cada commit de **cÃƒÂ³digo** (`1.93` Ã¢â€ â€™ `1.94`). Docs com `SKIP_VERSION_BUMP=1` nÃƒÂ£o contam |
| **Branch `producao`** | No deploy: `VERSION` = nÃƒÂºmero do **pacote que subiu** (ex. NFC-e Ã¢â€ â€™ loja **v1.93**) |
| **Cherry-pick em lote** | `SKIP_VERSION_BUMP=1` nos intermediÃƒÂ¡rios Ã‚Â· no fim um commit ajusta `VERSION` na loja |

#### Mesmo nÃƒÂºmero teste e loja Ã¢â‚¬â€ nÃƒÂ£o ÃƒÂ© o mesmo sistema

| SituaÃƒÂ§ÃƒÂ£o | O que significa |
| -------- | --------------- |
| **teste v2.77 Ã‚Â· loja v2.28** (hoje) | Loja: Contabilidade v2.27 + **PDV autocomplete v2.28**. Teste **ainda tem** outros pacotes sÃƒÂ³ no branch teste. |
| **Como saber o que falta na loja** | Tabela **Ã‚Â«SÃƒÂ³ no testeÃ‚Â»** no CHECKPOINT Ã‚Â· `git diff origin/producao origin/teste --stat` Ã¢â‚¬â€ **nÃƒÂ£o** olhar sÃƒÂ³ o `VERSION` |
| **Regra intuitiva (ideal)** | **`teste` > `producao`** Ã¢â€ â€™ loja atrÃƒÂ¡s (ex. teste **v1.94**, loja **v1.93** = tem pacote pendente). **`teste` = `producao`** Ã¢â€ â€™ acabou de subir pacote **ou** teste sÃƒÂ³ ganhou docs sem bump |
| **PrÃƒÂ³ximo fix de cÃƒÂ³digo no teste** | Deve virar **v2.06** no teste; loja fica **v2.03** atÃƒÂ© vocÃƒÂª pedir produÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ aÃƒÂ­ fica ÃƒÂ³bvio que nÃƒÂ£o sÃƒÂ£o iguais |

**Por que confundiu:** no deploy alinhamos o **nÃƒÂºmero** (1.93) nos dois lados; o **cÃƒÂ³digo** do teste nunca foi todo para a loja (sÃƒÂ³ cherry-picks). VERSION = **etiqueta do pacote**, nÃƒÂ£o diff completo entre branches.

| Erro corrigido (jun/26) | Pacote v1.92 tinha virado loja v1.57Ã¢â‚¬â€œv1.59 (hook no cherry-pick) Ã¢â€ â€™ manual **1.92**, depois **1.93** NFC-e |
| Conferir | `git show origin/teste:VERSION` Ã‚Â· `git show origin/producao:VERSION` Ã‚Â· diff `--stat` |

**Regra prÃƒÂ¡tica:** deploy loja Ã¢â€ â€™ CHECKPOINT: **pacote X** (commits) Ã‚Â· **teste vX.XX Ã¢â€ â€™ loja vX.XX**. PendÃƒÂªncias Ã¢â€ â€™ tabela Ã‚Â«SÃƒÂ³ no testeÃ‚Â», nÃƒÂ£o sÃƒÂ³ badge.

#### Subir sÃƒÂ³ um pacote no meio (ex. teste jÃƒÂ¡ em v1.96, loja sÃƒÂ³ v1.95)

O **nÃƒÂºmero** no teste ÃƒÂ© histÃƒÂ³rico de commits; na **loja** vocÃƒÂª sobe **o pacote que escolher**, nÃƒÂ£o Ã¢â‚¬Å“tudo atÃƒÂ© a ÃƒÂºltima versÃƒÂ£oÃ¢â‚¬Â.

| Exemplo | O que acontece |
| ------- | -------------- |
| Teste | v1.94 (fix A) Ã¢â€ â€™ v1.95 (fix B) Ã¢â€ â€™ v1.96 (fix C) Ã¢â‚¬â€ **trÃƒÂªs commits** |
| VocÃƒÂª pede | *Ã‚Â«sobe sÃƒÂ³ o 1.95Ã‚Â»* (fix B) |
| Assistente | Cherry-pick **sÃƒÂ³ o(s) commit(s) do 1.95** Ã¢â‚¬â€ **nÃƒÂ£o** leva o 1.96 |
| Loja depois | Badge **v1.95** (manual no commit final do deploy) |
| Teste continua | **v1.96** Ã¢â‚¬â€ fix C **ainda sÃƒÂ³ no teste** |
| Leitura | **teste v1.96 > loja v1.95** = falta subir o pacote **1.96** |

**Importante:**

- **NÃƒÂ£o precisa** existir v1.94 na loja para subir v1.95 (pode pular: loja 1.93 Ã¢â€ â€™ 1.95).
- O badge da loja = **nÃƒÂºmero do pacote que vocÃƒÂª subiu**, nÃƒÂ£o Ã¢â‚¬Å“cÃƒÂ³pia do VERSION do teste no momentoÃ¢â‚¬Â.
- No **banana** registramos: *pacote v1.95 Ã‚Â· commit `abc123` Ã‚Â· loja nÃƒÂ£o recebeu v1.94 nem v1.96*.
- Se o **1.96 depende** do cÃƒÂ³digo do 1.95, o cherry-pick do 1.95 jÃƒÂ¡ leva o necessÃƒÂ¡rio; o 1.96 em si fica de fora.

**Na prÃƒÂ¡tica vocÃƒÂª diz:** *Ã‚Â«pode subir para produÃƒÂ§ÃƒÂ£o o pacote da v1.95Ã‚Â»* (ou descreve o fix). O assistente acha o commit pelo `VERSION` / histÃƒÂ³rico / banana Ã¢â‚¬â€ **nÃƒÂ£o** faz merge do `teste` inteiro.

#### RecomendaÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ manter ou mudar? (23/06/2026)

**Para o SisVale hoje: manter este esquema (+0,01, cherry-pick, banana).** JÃƒÂ¡ estÃƒÂ¡ no hook, no badge do BI e na rotina teste Ã¢â€ â€™ loja. **NÃƒÂ£o** vale trocar por semver grande ou data sÃƒÂ³ por organizaÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ mudaria pouco e geraria retrabalho.

| Camada | Papel |
| ------ | ----- |
| **`VERSION` (+0,01)** | Badge simples na loja (*v1.95*) Ã¢â‚¬â€ operador vÃƒÂª na home |
| **`banana.md` CHECKPOINT** | **Fonte da verdade** Ã¢â‚¬â€ qual commit = qual pacote, o que estÃƒÂ¡ sÃƒÂ³ no teste, o que foi para a loja |
| **Cherry-pick** | Sobe **pacote escolhido**, nÃƒÂ£o branch inteira |

**TrÃƒÂªs disciplinas** (isso ÃƒÂ© o que organiza de verdade):

1. **Um pacote lÃƒÂ³gico = um bump no teste** Ã¢â‚¬â€ nÃƒÂ£o misturar dois fixes grandes no mesmo commit se forem pacotes de produÃƒÂ§ÃƒÂ£o diferentes.
2. **Todo deploy loja** Ã¢â€ â€™ linha no CHECKPOINT: `v1.95 Ã‚Â· commit Ã‚Â· o quÃƒÂª Ã‚Â· revert como`.
3. **Pedido explÃƒÂ­cito** Ã¢â‚¬â€ *Ã‚Â«sobe v1.95Ã‚Â»* ou *Ã‚Â«pode subir produÃƒÂ§ÃƒÂ£o o fix XÃ‚Â»*; assistente **nÃƒÂ£o** merge `teste` inteiro.

**Opcional no futuro** (sÃƒÂ³ se quiser mais rigor): `git tag v1.95` no commit do teste ao fechar pacote Ã¢â‚¬â€ facilita achar o cherry-pick. **NÃƒÂ£o obrigatÃƒÂ³rio** agora.

**NÃƒÂ£o recomendado aqui:** merge `teste`Ã¢â€ â€™`producao` de uma vez; CalVer (`2026.06.23`); nÃƒÂºmero de versÃƒÂ£o diferente por branch sem regra (voltaria confusÃƒÂ£o 1.59 vs 1.92).

---

## 4. MÃƒÂ³dulos Ã¢â‚¬â€ mapa rÃƒÂ¡pido

Cada bloco: **o que ÃƒÂ© Ã‚Â· rotas Ã‚Â· arquivos-chave Ã‚Â· armadilhas**.

### 4.1 Home / BI (`/`)

- Dashboard gerencial SisVale BI; atalhos clÃƒÂ¡ssicos em `/atalhos/`.
- VersÃƒÂ£o do commit no Render (nÃƒÂ£o hardcoded).
- Card **Validade** destaca vermelho se produto vencido.
- Card **Lucro LÃƒÂ­quido** (no lugar de Novos Clientes): vencimento Ã‚Â· bruto + pago Ã‚Â· mesmo DRE do Resumo.
- **Filtro NÃºmeros** (10/08): **Centro + Vila** (padrÃ£o) Â· Centro Â· Vila â€” independente do seletor PDV (Centro/Vila do caixa).
- **Topo BI compacto (10/08):** sem Â«GestÃ£o EstratÃ©gicaÂ» Â· sem botÃ£o OrÃ§. (F2 no teclado/Menu) Â· **Trava** embaixo de Loja.
- Gastos por plano de conta: oculto por padrÃƒÂ£o (`AGRO_DASHBOARD_GASTOS_PLANO=true` no `.env`).
- Template: `produtos/templates/produtos/dashboard_gerencial.html`.

### 4.2 PDV Ã¢â‚¬â€ ponto de venda

**Duas interfaces:**


| Tela                  | URL              | JS principal                    |
| --------------------- | ---------------- | ------------------------------- |
| PDV legado MPA        | `/consulta/`     | `consulta_produtos.js`          |
| PDV wizard (checkout) | `/pdv/checkout/` | `pdv_wizard.js`, `pdv_state.js` |


**Fluxo tÃƒÂ­pico:** busca produto Ã¢â€ â€™ carrinho Ã¢â€ â€™ cliente (opcional) Ã¢â€ â€™ pagamento Ã¢â€ â€™ confirma Ã¢â€ â€™ cupom/impressÃƒÂ£o.

**Regras UX jÃƒÂ¡ decididas:**

- **F1** volta ao PDV preservando draft/filtros/scroll.
- **Estoque Vila (28/07):** atalho na topbar â†’ menu Folha Compras â†’ `/compras/?folha=` com overlay.
- **BotÃ£o flutuante PDV** (2026-06-19): canto **inferior esquerdo** por padrÃ£o; **reposiciona sozinho** (6 cantos: BL/BR/TL/TR/meio L/R) se encostar em botÃ£o â€” prioridade **BR** em `/caixa/`. **Aa** (Display Scale) idem: TR â†’ TL â†’ BR â†’ BL.
- **Perf. animaÃƒÂ§ÃƒÂµes (decisÃƒÂ£o Renan, 2026-06):** acÃƒÂºmulo de efeitos no app inteiro *pode* pesar em PC fraco Ã¢â‚¬â€ mas **este FAB ÃƒÂ© impacto baixo** (1 elemento, CSS `transform`/`opacity`, sem JS extra nem rede). O que pesa mesmo: MPA pÃƒÂ¡gina inteira, listas grandes, Mongo, JS do PDV/LanÃƒÂ§amentos. Regra: poucos destaques globais (FAB, Validade vermelha); evitar animar tabelas/cards em massa.
- **Interruptor efeitos (2026-06-19):** botÃƒÂ£o minÃƒÂºsculo **Ã‚Â«FX on / FX offÃ‚Â»** acima do FAB PDV (`localStorage` `agro_reduzir_efeitos_v1`). **FX off** Ã¢â€ â€™ classe `html.agro-fx-reduced`: desliga arco-ÃƒÂ­ris/pulso do FAB, pulso do card **Validade** vencida, pulso decorativo PDV/OrÃƒÂ§amento no BI. **NÃƒÂ£o** desliga: barra de loading, feedback de scanner, spinners de Ã‚Â«salvandoÃ‚Â» (ÃƒÂºteis). API JS: `agroSetFxReduced(true|false)`, `agroFxReduced()`.
- Entrega wizard **F3:** pagamento local Ã¢â€ â€™ endereÃƒÂ§o Ã¢â€ â€™ taxa Ã¢â€ â€™ meio Ã¢â€ â€™ troco Ã¢â€ â€™ **Conferir entrega** (resumo com Editar por bloco). Frete grÃƒÂ¡tis por endereÃƒÂ§o no futuro pula popup taxa.
- EndereÃƒÂ§o oculto atÃƒÂ© escolher pagamento na entrega ou na loja.
- Barra de estoque: atualizaÃƒÂ§ÃƒÂ£o manual + horÃƒÂ¡rio + standby.

**APIs PDV (amostra):** `api/buscar/`, `api/pdv/*`, `api/promocoes/ativas-pdv/`, Mercado Pago Point em `views_mp_point.py`.

**Uso loja (31/07 Â· v12.31):** botÃ£o topbar â†’ overlay Â· saÃ­da PG Â· quem = grade RH (toque avanÃ§a / Outros digita) Â· motivo Â· PIN Â· histÃ³rico/estorno Â· nÃ£o mexe no carrinho da venda.

**Cadastro rÃ¡pido PDV (04/08 Â· v13.82):** botÃ£o **+ Produto** na busca Â· bipar â†’ checa EAN â†’ lookup internet opcional Â· cria Agro (UN) Â· card **PDV conferir** no Cadastro ERP Â· VERIFY_OK.

**RaÃ§Ãµes PDV (09/08 Â· loja v15.26 Â· teste UX):** botÃ£o **RaÃ§Ãµes** â†’ tipo â†’ marca (ou Todas) â†’ tamanho â†’ **lista grande** (menorâ†’maior preÃ§o) â†’ Adicionar / Adicionar todas / Fechar. NÃ£o vai direto ao carrinho. Linha **zebra cinza fraca** (sem cor da marca) Â· foto miniatura (clique abre grande) Â· â€œNo carrinhoâ€ na coluna AÃ§Ã£o. Esc fecha (nÃ£o fecha se a foto estiver aberta). LÃª Categoria/Sub 1/Sub 2/Peso do Agro na hora. Cadastro: Cat. `RaÃ§Ãµes` Â· Sub 1 `CÃ£o`/`Gato` Â· Sub 2 + Peso `1`/`2,5`/`5`/`10`/`15`/`20`/`25`/`pacote`.

**Fiado Ã¢â‚¬â€ baixa (decisÃƒÂ£o 07/07):** cobranÃƒÂ§a de tÃƒÂ­tulo em aberto **nÃƒÂ£o** fica no modal de `/fiado/` Ã¢â‚¬â€ redireciona ao **PDV pagamento** com cliente + valor do tÃƒÂ­tulo (ou selecionados). Quita `FiadoTituloAgro` + caixa no confirmar. **Cupom fiscal na baixa** = **FL-052** (P1,1), depois do pacote pagamento.

**Armadilha GM no barras (2026-06-18):** se Ã‚Â«CÃƒÂ³digo de barrasÃ‚Â» no cadastro tiver texto **GM** (ex. `GM1546-5S`), o leitor manda GM, nÃƒÂ£o EAN. No **wizard** (`pdv_wizard.js`), o hÃƒÂ­fen do GM disparava atalho `**-`** = remover ÃƒÂºltimo item do carrinho (campo mostrava `GM15465S`). Patch: ignorar `-`/`+` durante SKU/GM + modo barcode para `GMÃ¢â‚¬Â¦`. Legado `/consulta/`: F4 pÃƒÂ³s-bip + match alnum (`consulta_produtos.js`).

**Venda gravada em:** `VendaAgro` (Postgres) + tentativa sync ERP conforme config.

### 4.3 NFC-e (cupom fiscal 65)

**EmissÃƒÂ£o pelo Agro**, nÃƒÂ£o pelo ERP. SÃƒÂ©rie **21** no Agro; ERP continua sÃƒÂ©rie **20**.

#### Quando emite ou nÃƒÂ£o (resumo operacional)

**PrÃƒÂ©-requisito:** `NFC_E_ENABLED=true` e certificado/CSC configurados. Se desligado Ã¢â€ â€™ **nunca** emite.

| SituaÃƒÂ§ÃƒÂ£o | Emite NFC-e? |
| -------- | ------------ |
| `NFC_E_MODO=auto` | **Sim**, em toda venda confirmada |
| Forma **PIX** ou **cartÃƒÂ£o** (dÃƒÂ©bito / crÃƒÂ©dito / parcelado) | **Sim**, automÃƒÂ¡tico |
| **Dinheiro**, fiado, vale, cashback, etc. | **SÃƒÂ³ se o operador escolher** cupom fiscal |
| Operador escolhe **Ã‚Â«Venda comumÃ‚Â»** (popup de impressÃƒÂ£o) | **NÃƒÂ£o** Ã¢â‚¬â€ sÃƒÂ³ cupom nÃƒÂ£o fiscal |
| Venda **sem impressÃƒÂ£o** + forma manual | **NÃƒÂ£o** (salvo modo `auto`) |
| Venda **sem impressÃƒÂ£o** + PIX/cartÃƒÂ£o | **Sim**, automÃƒÂ¡tico (background) |
| Venda **com impressÃƒÂ£o (F9)** + cupom fiscal | **Sim**, **sÃƒÂ­ncrono** Ã¢â‚¬â€ modal CPF Ã¢â€ â€™ aguarda SEFAZ Ã¢â€ â€™ imprime |
| Venda **sem impressÃƒÂ£o (Enter)** + PIX/cartÃƒÂ£o | **Sim**, background (sem modal se sem CPF no cliente) |
| **Falha na SEFAZ** | Venda **grava igual**; reemitir em **Consultar vendas** |

#### Popups no PDV (wizard `/pdv/checkout/`)

| Passo | PIX / cartÃƒÂ£o | Dinheiro e demais |
| ----- | ------------ | ----------------- |
| Confirmar **com impressÃƒÂ£o (F9)** | Modal CPF (se sem CPF no cliente) Ã¢â€ â€™ emissÃƒÂ£o **sÃƒÂ­ncrona** Ã¢â€ â€™ imprime fiscal | Popup **Cupom fiscal** ou **Venda comum** Ã¢â€ â€™ idem se fiscal |
| Confirmar **sem impressÃƒÂ£o (Enter)** | Sem modal Ã¢â‚¬â€ sem ID automÃƒÂ¡tico em PIX/cartÃƒÂ£o | NFC-e sÃƒÂ³ se operador pediu / forma auto |
| Cliente **sem CPF** vÃƒÂ¡lido | Modal **Sem CPF na nota** / **Informar CPF** | Idem, se escolheu cupom fiscal |
| Cliente **com CPF** no cadastro | Usa o CPF, sem modal | Idem |

#### DevoluÃƒÂ§ÃƒÂ£o de venda

- RepÃƒÂµe estoque + saÃƒÂ­da no caixa (como antes).
- Se tinha NFC-e **autorizada** Ã¢â€ â€™ sistema **tenta cancelar na SEFAZ** com motivo padrÃƒÂ£o: *Ã‚Â«Devolucao de mercadoria registrada no sistema Agro.Ã‚Â»*
- **Prazo SEFAZ (NFC-e mod. 65):** cancelamento por evento sÃƒÂ³ atÃƒÂ© **~30 minutos** apÃƒÂ³s a **autorizaÃƒÂ§ÃƒÂ£o do cupom** (regra nacional Ã¢â‚¬â€ NT 2018.004 / Ajuste SINIEF 07/2018). O relÃƒÂ³gio **nÃƒÂ£o** recomeÃƒÂ§a na devoluÃƒÂ§ÃƒÂ£o.
- **Erro 501** = prazo esgotado na SEFAZ. DevoluÃƒÂ§ÃƒÂ£o no Agro segue OK; cupom continua autorizado Ã¢â€ â€™ **NF-e de devoluÃƒÂ§ÃƒÂ£o (mod. 55)** ou contador.
- Status local passa a **Cancelada** quando a SEFAZ aceita.
- BotÃƒÂ£o **Cancelar NFC-e** na tela da venda (retry manual).

**UX PDV (2026-06-18):** modal CPF **grande** (`max-w ~54rem`, fontes `clamp`). ReemissÃƒÂ£o em `/vendas/` Ã¢â€ â€™ apÃƒÂ³s autorizar pergunta **Imprimir cupom / Agora nÃƒÂ£o**. Aviso pÃƒÂ³s-venda NFC-e falhou: toast **no topo**, depois da janela de impressÃƒÂ£o Windows.

**SEFAZ:** PIX/cartÃƒÂ£o Ã¢â€ â€™ grupo `<card><tpIntegra>2</tpIntegra></card>` (NT 2024.003) + `vTotTrib` por item. **NÃƒÂ£o** mandar `card` em fiado/`tPag=05` (rejeiÃƒÂ§ÃƒÂ£o **963**). NCM/CFOP/CEST sÃƒÂ³ dÃƒÂ­gitos no XML (rejeiÃƒÂ§ÃƒÂ£o **225** schema).

**Arquivos centrais:**


| Arquivo                            | Papel                                            |
| ---------------------------------- | ------------------------------------------------ |
| `produtos/nfce_config_util.py`     | Env vars, resumo config                          |
| `produtos/nfce_sp_emissao_util.py` | XML, SOAP SEFAZ SP, assinatura, QR Code          |
| `produtos/nfce_cupom_util.py`      | Cupom tÃƒÂ©rmico 80 mm                              |
| `produtos/nfce_ibpt_util.py`       | Tributos Lei 12.741                              |
| `produtos/nfce_venda_util.py`      | Painel status por venda                          |
| `produtos/views_nfce.py`           | Rotas API e contabilidade                        |
| `produtos/models.py`               | `NfceDocumentoAgro`, `VendaAgro.nfce_solicitada` |


**Modelos:** `NfceDocumentoAgro` (1:1 com venda, guarda XML autorizado). Campo `nfce_solicitada` na venda = operador pediu cupom mesmo em forma manual.

**Rotas ÃƒÂºteis:**

- `GET /api/nfce/status/`
- `POST /venda/<pk>/nfce/emitir/`
- `GET /venda/<pk>/nfce/cupom/`
- `GET /api/nfce/export-xml/?ano=&mes=` (ZIP contabilidade)
- `/contabilidade/` painel

**ProduÃƒÂ§ÃƒÂ£o GM Agro:** CNPJ `48900774000103`, municÃƒÂ­pio `3524600` Jacupiranga, `NFC_E_TP_AMB=1`. Checklist completo: `docs/NFCE-PRODUCAO.md`.

**HistÃƒÂ³rico de dor SEFAZ (jÃƒÂ¡ resolvido no cÃƒÂ³digo):** QR no XML antes da assinatura, SHA1 QR v2 SP, retry nÃƒÂºmero duplicado 539/204, CA bundle ICP-Brasil no Render, SOAP 4.00 sem `nfeCabecMsg` errado.

### 4.4 Vendas

- Lista: `/vendas/` Ã‚Â· detalhe: `/venda/<pk>/`
- ReimpressÃƒÂ£o cupom simples e fiscal (se NFC-e autorizada).
- DevoluÃƒÂ§ÃƒÂ£o, reenvio ERP, reversÃƒÂ£o ERP Ã¢â‚¬â€ APIs em `views.py`.
- Export CSV: `/vendas/exportar-csv/`.

### 4.5 Clientes

- Cadastro local: `ClienteAgro` (Postgres).
- Sync ERP/Mongo Ã¢â€ â€™ Agro: `produtos/services_clientes_sync.py`, botÃƒÂ£o na lista, comando `sincronizar_clientes_agro`.
- `**editado_local=True` nÃƒÂ£o ÃƒÂ© sobrescrito** na sync.
- PDV lista/busca clientes **sÃƒÂ³ no Agro** (`api/listar-clientes/`, `api/buscar-clientes/`).
- IDs Mongo no JSON viram `local:{pk}` para nÃƒÂ£o mandar ObjectId ao ERP.
- Contexto antigo detalhado: `docs/CONTEXTO_SESSAO_CLIENTES_PDV.md`.

### 4.6 Cadastro / gestÃƒÂ£o de produtos

- **Etiquetas `/produtos/etiquetas/`:** presets de layout = **Postgres** (`EtiquetaPresetAgro`) â€” multi-PC (01/08). localStorage sÃ³ cache + preset ativo + rodapÃ©.

**Duas telas Ã¢â‚¬â€ nÃƒÂ£o confundir:**


| Tela                       | URL / API                                           | Uso                                                  |
| -------------------------- | --------------------------------------------------- | ---------------------------------------------------- |
| **Cadastro ERP** (SisVale) | `/produtos/cadastro-erp/`, `api_produtos_cadastro`  | Lista/busca Mongo **sem saldo**; Excel import/export |
| **GestÃƒÂ£o operacional**     | `produtos_gestao.html`, `api_produtos_gestao_lista` | Saldo, facetas, operaÃƒÂ§ÃƒÂ£o loja                        |


**Excel fase 1:** export com colunas/categorias; import async com histÃƒÂ³rico e desfazer; ID oculta; CÃƒÂ³digo GM editÃƒÂ¡vel; cÃƒÂ©lula vazia nÃƒÂ£o altera. Colunas: Sub 2â€“4, Unidade, Modelo, Peso (alÃ©m das originais).

**Modal cadastro â€” marca/categoria (08/07):** Â«Salvar no AgroÂ» grava online (Postgres + overlay). BotÃ£o **+** sÃ³ preenche o campo â€” **nÃ£o** substitui salvar. Ao reabrir, detalhe da API prevalece sobre linha da lista (fix bug que Â«apagavaÂ» marca/cat).

**Faceta unidade/marca/cat (06/08 Â· FACETA-CACHE):** lista branca vinha de cache desatualizado (+ PIN nÃ£o limpava chave `v6`). Corrigido: invalidar cache ao salvar/+ Â· servidor inclui valores do produto e do PIN Â· JS nÃ£o sobrescreve o que jÃ¡ cadastrou na sessÃ£o.

**Foto produto (06/08 Â· FOTO-PDV Â· v14.68):** aba Gerais usa o mesmo anexar foto da aba Delivery (uma foto sÃ³) Â· jÃ¡ aparece no PDV (fluxo Delivery desde v11.45). Removido campo URL que nÃ£o gravava no Agro.

**Fantasmas Mongo Ã¢â€ â€™ Postgres (`agro_pg`, 2026-06-24):**

| O quÃƒÂª | Detalhe |
| ----- | ------- |
| **O que ÃƒÂ©** | Documento no espelho Mongo com `_id` 24 hex **sem** `Nome` Ã¢â‚¬â€ import virou linha `Produto` com nome Ã‚Â«Ã¢â‚¬â€Ã‚Â» e `codigo_interno` = Id |
| **Caso loja** | 3Ãƒâ€” ibiuna **25 kg** (GM1541/42/46-25); demais variantes OK |
| **Evitar de novo** | `importar_catalogo_mongo_produto` **ignora** fantasma (`deve_ignorar_import_mongo_fantasma`) Ã‚Â· nunca grava ObjectId como nome Ã‚Â· novo cadastro sÃƒÂ³ via Agro (nÃƒÂ£o duplicar GM no Mongo |
| **Auditar (quantos?)** | `auditar_produtos_fantasma_pg` Ã¢â‚¬â€ **graves** (nome/GM quebrados); `--higiene` = sÃƒÂ³ `codigo_interno`=Id |
| **Corrigir lote** | `python manage.py corrigir_produto_nome_objectid_pg --dry-run` depois sem `--dry-run` |
| **Quando rodar** | ApÃƒÂ³s `importar_catalogo_mongo_produto`, apÃƒÂ³s snapshot staging, se busca cadastro Ã‚Â«sumirÃ‚Â» produto que existe no PDV |
| **CÃƒÂ³digo** | `catalogo_nome_util.py` Ã‚Â· comandos `auditar_*` / `corrigir_*` Ã‚Â· exibiÃƒÂ§ÃƒÂ£o API herda irmÃƒÂ£os GM (teste v2.50+) |

**Produto novo Ã¢â‚¬â€ cÃƒÂ³digo sistema + GM (decisÃƒÂ£o 2026-06-22, Renan OK no teste):**


| Campo              | Regra                                                                                            |
| ------------------ | ------------------------------------------------------------------------------------------------ |
| **CÃƒÂ³digo sistema** | Exatamente **4 nÃƒÂºmeros**; sequÃƒÂªncia **4010Ã¢â‚¬â€œ9999**; operador pode editar                          |
| **SequÃƒÂªncia**      | PrÃƒÂ³ximo livre apÃƒÂ³s o **maior cÃƒÂ³digo sistema** jÃƒÂ¡ usado (sÃƒÂ³ campo numÃƒÂ©rico Ã¢â‚¬â€ **GM nÃƒÂ£o conta**)    |
| **CÃƒÂ³digo GM**      | SugestÃƒÂ£o automÃƒÂ¡tica `**GM` + cÃƒÂ³digo sistema**; operador **livre para editar** (sem formato fixo) |
| **Modal novo**     | Carregar cÃƒÂ³digos **sem repintar** o modal (nÃƒÂ£o apagar nome/campos jÃƒÂ¡ digitados)                  |


Env opcional: `AGRO_NOVO_PRODUTO_COD_MIN` (piso da sequÃƒÂªncia; padrÃƒÂ£o **4010**).

**LentidÃƒÂ£o pÃƒÂ³s-entrada NF (investigaÃƒÂ§ÃƒÂ£o aberta):** medir qual URL trava no browser; suspeitos: `api_produtos_gestao_facetas`, pool Mongo. **01/07 loja v6.04 Ã¢â‚¬â€ relato Ã‚Â«quase todas telasÃ‚Â»:** ver CHECKPOINT Ã‚Â«Perf multi-telaÃ‚Â»; loja jÃƒÂ¡ `agro_pg`+`ledger`+CP PG Ã¢â‚¬â€ foco em fila Gunicorn (1 worker), facetas no load, CP PG scan 25k, bootstrap+API dupla LanÃƒÂ§amentos.

### 4.7 Entrada de nota fiscal

- `/entrada-nota/` Ã¢â‚¬â€ wizard 8 passos (fornecedor Ã¢â€ â€™ Ã¢â‚¬Â¦ Ã¢â€ â€™ financeiro Ã¢â€ â€™ finalizar PIN).
- **Dist DF-e (31/07):** certificado `NFE_DIST_DFE_*` **ou** `NFC_E_*`. Cursor PG sÃ³ avanÃ§a em **137/138**. Caixa de entrada PG (~80): Buscar grava Â· Pendentes antigas primeiro Â· ConcluÃ­das. Recuperar por chave se precisar. **XML** se nota antiga.
- **SÃ³ resumo / CiÃªncia (05/08):** evento oficial **210210** no Ambiente Nacional; grava protocolo no PG, nÃ£o repete e tenta baixar o XML completo para Carregar na grade.
- **XML vs Aguarde 1h (12/08):** Buscar lista (distNSU) continua com 1h apÃ³s 137; **Buscar XML / chave** sÃ³ trava no **656**. ApÃ³s Buscar, sistema tenta CiÃªncia+XML sozinho nas Â«SÃ³ resumoÂ» â€” fica pronto para **Carregar na grade** (manual, uma nota por vez).
- **UI aba SEFAZ (04/08):** tela limpa (aÃ§Ãµes + lista); textos longos no **Â«?Â»** (contexto `sefaz` + bloco no modal). Status compacto (Pronto / Local off / Cursor).
- PrÃƒÂ©-visualizaÃƒÂ§ÃƒÂ£o XML: modal drag-and-drop, nÃƒÂ£o fecha ao clicar fora; Ã‚Â«Confirmar na gradeÃ‚Â» aplica de fato.
- **Busca produtos etapa 2 (16/07 Ã‚Â· loja v8.69):** BCA `/api/buscar/` igual cadastro/PDV Ã¢â‚¬â€ famÃƒÂ­lia GM completa (complemento Mongo); nÃƒÂ£o desligar Mongo no `entrada_nfe=1`.
- **AcrÃƒÂ©scimos no custo (14/07 Ã‚Â· loja v8.43):** checkbox Ã‚Â«Incluir no custo os acrÃƒÂ©scimos da notaÃ‚Â» (etapa 2) Ã¢â‚¬â€ rateia frete+ST+seguro+outras+IPIÃ¢Ë†â€™desconto no custo unitÃƒÂ¡rio proporcional ao `vProd`; mark/desmarca recalcula sem reupload. Nota sem esses totais = noop.
- **Mudar produto (21/07):** trocar vÃƒÂ­nculo no Ã‚Â«MudarÃ‚Â» **nÃƒÂ£o** troca o V. unit da NF (nem o rateio); sÃƒÂ³ cadastro/P.venda. Linha manual sem base NF ainda puxa custo do cadastro.
- **Nova nota (21/07):** botÃƒÂ£o Ã‚Â«NovaÃ‚Â» zera XML/cabeÃƒÂ§alho/financeiro/rateio Ã¢â‚¬â€ nÃƒÂ£o herda a nota anterior (autosave tambÃƒÂ©m).
- **HistÃƒÂ³rico C1Ã¢â‚¬â€œC3 + NF (18/07):** C1Ã¢â‚¬â€œC3 = sÃƒÂ³ compras **anteriores**; a NF aberta **nÃƒÂ£o** entra (evitava parecer 2 notas: data entrada vs emissÃƒÂ£o).
- **VÃ­nculo XML (30/07 Â· v12.10):** tabela Postgres `EntradaNfeVinculoAgro` = fonte da verdade multi-PC; Â«Ler XMLÂ» reaproveita cProd (R0151â€¦). Migrate `0069` Â· backfill `agro_backfill_c_prod_nf_entrada`.
- **Financeiro desync (2026-06-19 / reforÃ§o 29/07):** tÃ­tulo jÃ¡ em Contas a pagar mas etapa 7 laranja + Â«Salvar + a pagarÂ» morto â€” rascunho perdeu `financeiro_lancado`. Fix: sync ao abrir Â· botÃ£o religa sem reabrir Â· API nÃ£o gera 2Âº lote se achar NF Â· Reabrir estorna por rastro se ids sumiram. **NÃ£o** reabrir e confirmar tudo de novo (duplicava). Limpar duplicatas jÃ¡ feitas em Contas a pagar.
- **Reabrir â†’ estoque de novo (03/08):** ao reabrir, estornar se houver status/`estoque_aplicado_em`/carimbo/`ajuste_ids` (nÃ£o sÃ³ `estoque_aplicado`). Autosave nÃ£o ressuscita carimbo. Lista Â«reabrirÂ» encerrada chama o mesmo estorno.
- **Kardex ao reabrir (03/08):** reabrir **nÃ£o apaga** a Entrada NF â€” grava saÃ­da `estorno_entrada_nf_agro` (Â«Estorno NF (reabrir)Â»); ao concluir de novo, nova Entrada NF. `nf_qtd=` no ajuste para qtd confiÃ¡vel.
- **Trocar/remover produto com estoque lanÃ§ado (05/08 Â· v14.48):** exige **estorno** antes â€” modal Â«Estornar e trocarÂ» (PIN) chama a rotina de reabrir e joga o usuÃ¡rio de volta Ã  etapa 2; backend recusa salvar linhas com `produto_id` diferente enquanto houver carimbo de estoque (`requer_estorno`).
- **Custo do cadastro na etapa 2 (03/08 Â· v13.71):** V. unit puxa custo do Cadastro (overlay/PG) â€” JS ignora `preco_custo_final=0` do Mongo; overlay sincroniza final/acrÃ©scimo; `buscar-produto-id` fallback `Produto.custo`. Linha com custo da NF (`preservar`) continua sem sobrescrever.
- **Validade â†’ tela Validade (06/08):** ao **lanÃ§ar estoque**, se a linha tiver `lote_validade` (etapa 4), grava/soma `EstoqueLote` (antes sÃ³ ficava no rascunho). Reabrir reduz o lote se a entrada tinha `nf_lote`/`nf_val`. Notas **jÃ¡** lanÃ§adas antes do fix **nÃ£o** voltam sozinhas.
- **Etapa 3 cÃ³d. barras (12/08 Â· v16.06 `NF-BIP-ET3`):** bip casa com EAN da linha **e** barras do cadastro/overlay dos itens da NF; casado por EAN/bip na etapa 2 â†’ Ok verde; prova `verify_nf_bip_et3_path.py`.

### 4.8 Estoque Agro

- Saldo PDV = Mongo ERP + `AjusteRapidoEstoque`.
- Painel: `/estoque/sincronizacao/`.
- Doc: `docs/ESTOQUE_AGRO_FONTE_DA_VERDADE.md`.
- Cron: `estoque_mongo_ping` a cada 10 min no Render.
- **TransferÃªncia forÃ§ada â€” bip vs digitar (06/08):** bip (cÃ³digo de barras numÃ©rico) â†’ qtd 1 (+1 se jÃ¡ no carrinho) e foco volta na busca; digitar nome/GM â†’ foco na quantidade (como antes).
- **Contagem cÃ­clica (13/08 Â· v16.12):** Ajuste Mobile botÃ£o **CÃ­clica** â€” sessÃ£o PG multi-celular, cego, escopo loja/categoria/corredor, 2 passagens, grava `AjusteRapidoEstoque` sÃ³ no fechamento (`contagem_ciclica`).

### 4.9 Compras

- `/compras/` Ã¢â‚¬â€ sugestÃƒÂ£o, horizonte em dias, mÃƒÂ©tricas avanÃƒÂ§adas em `<details>`.
- **UI etapa 1 (18/07 Ã‚Â· v9.95):** painel resumo (horizonte + descontar estoque + KPIs) Ã‚Â· busca em destaque Ã‚Â· detalhe expandido em blocos Ã¢â‚¬â€ **sÃƒÂ³ visual**, regras/cÃƒÂ¡lculos iguais.
- **Fontes (Renan 08/07):** mÃƒÂ©dia/grÃƒÂ¡fico/sugestÃƒÂ£o = **vendas PDV Agro**; ÃƒÂºltima compra/chips/planilha = **sÃƒÂ³ Entrada NF Agro** (ERP cortado). RÃƒÂ³tulos na tela alinhados.
- **UX Compras (08/07):** coluna Ã‚Â«ComprarÃ‚Â»; estoque Centro+Vila por extenso; lucro sÃƒÂ³ com custo confiÃƒÂ¡vel; custo usa cadastro ou ÃƒÂºltima NF; F5 preenche Ã‚Â«ÃƒÅ¡lt. NFÃ‚Â» via Entrada NF Agro.
- RelatÃƒÂ³rios: A4 fornecedor, planilhas impressas por categoria/unidade (A4 ou A6).
- **Folha Compras (08/07):** fornecedor / categoria / unidade abrem em **popup na prÃƒÂ³pria tela** (nÃƒÂ£o nova aba) Ã¢â‚¬â€ evita limite de 3 abas SisVale e layout bugado. BotÃƒÂ£o **Nova aba** no popup se precisar. PÃƒÂ¡ginas planilha com `?embed=1` nÃƒÂ£o montam barra lateral.
- **Popup Folha (27/07):** modal compacto (padrÃƒÂ£o ERP) Ã‚Â· Folha de saldo com filtros **botÃƒÂ£o+chip** iguais ao cadastro Ã‚Â· facetas no HTML.
- **Folha por fornecedor (27/07 + 11/08):** catÃ¡logo PG â€” cadastro + Entrada NF Agro. **11/08 (`FOLHA-FORN-HIST`):** lista = cadastro **âˆª** histÃ³rico de NFs (nÃ£o sÃ³ Ãºltimo pedido); 1Âº token do nome (ex. ADIMAX).
- **Folha de saldo (28/07):** overlay grande Â· filtros salvos **online** (Postgres) com 1 padrÃ£o global Â· tipografia maior nos filtros.
- **Atalho PDV Estoque Vila (28/07):** botÃ£o no PDV (`/pdv/checkout/`) abre as 5 opÃ§Ãµes da Folha Compras e manda pra `/compras/?folha=â€¦` com overlay jÃ¡ aberto (rÃ³tulo sÃ³ â€” nÃ£o forÃ§a Vila).

### 4.10 LanÃƒÂ§amentos / financeiro

- `/lancamentos/` Ã¢â‚¬â€ redirect Ã¢â€ â€™ **Contas a pagar padrÃƒÂ£o:** `/lancamentos/contas-pagar/` (**layout novo**) Ã‚Â· `/classico/` Ã¢â€ â€™ redirect Ã‚Â· `/teste/` Ã¢â€ â€™ redirect
- Contas a receber: `/lancamentos/contas-receber/` (layout clÃƒÂ¡ssico)
- PDF: `lancamentos_financeiro_pdf.py` (sem coluna observaÃƒÂ§ÃƒÂµes longas; forma pagamento; bruto destacado).
- Busca na lista: termos com espaÃƒÂ§o; **valor** (bruto/pago/saldo); **nÃºmero da NF**; **data** digitada; boleto; parcela; CPF/CNPJ. Ajuda: `includes/lancamentos_help_agents.html`.
- **Layout novo CP:** `lancamentos_contas_pagar_teste.html` Ã¢â‚¬â€ API `/api/lancamentos/`; filtros na URL; recarga in-place preserva scroll/filtros/pÃƒÂ¡ginas. **Filtro de data:** vencimento (padrÃƒÂ£o) Ã‚Â· competÃƒÂªncia Ã‚Â· pagamento (`ref` + `venc_*` / `comp_*` / `pag_*` na URL e na API).
- **Perf lista (2026-06-19):** projeÃƒÂ§ÃƒÂ£o slim Mongo; `skip_totais` pÃƒÂ¡g. 2+; cache sessionStorage; planos lazy.
- **Abertura CP Ã¢â‚¬â€ Chrome (2026-06-19, v1.48+):** prefetch BI/F7 Ã‚Â· cache do dia Ã‚Â· selo **SincronizandoÃ¢â‚¬Â¦** Ã‚Â· **bootstrap HTML** (lista hoje+abertos jÃƒÂ¡ no servidor, sem 2Ã‚Âª ida ÃƒÂ  API). Renan validou melhora **sutil** Ã¢â‚¬â€ esperado no Chrome MPA.
- **Teto sem refactor grande:** no Chrome cada clique = **pÃƒÂ¡gina nova** + Mongo no bootstrap. **Roadmap adiado (2026-06-19):** prÃƒÂ³ximo salto = Postgres financeiro **ou** lista no BI Ã¢â‚¬â€ ver CHECKPOINT.
- **Nova saÃƒÂ­da** (modal) + **Lote manual** (`/lancamentos/novo-manual/`): pseudo-plano **Ã‚Â«EmprÃƒÂ©stimo (entrada + pagamento)Ã‚Â»** Ã¢â‚¬â€ gera receita quitada (hoje) + despesa(s); se saÃƒÂ­da > entrada, diferenÃƒÂ§a em **Juros de EmprÃƒÂ©stimos**. JS: `lancamento_emprestimo_dual.js`; backend: `expandir_linhas_emprestimo_dual_lote` em `mongo_financeiro_util.py`.
- **GrÃƒÂ¡fico gastos por plano (2026-06-26):** `/financeiro/grafico-gastos/` Ã¢â‚¬â€ **100dvh sem scroll**; toolbar perÃƒÂ­odo simÃƒÂ©trica; painel **Filtros | Planos**; **4 atalhos** Postgres (**Alt+clique** fixa padrÃƒÂ£o Ã°Å¸â€œÅ’); modos tempo real / histÃƒÂ³rico / comparar; drill-down CP popup. **Entrada BI:** botÃƒÂ£o laranja no card **Contas a Pagar** (`/`). Teste **v3.54+**; loja **v3.39**.
- **DRE Indicadores + Resumo â€” CMV (09/08, `DRE-CMV-TOGGLE` + `RG-CMV-TOGGLE`):** botÃ£o **Mercadoria vendida** (custo cadastro Ã— qtd) Ã— **Mercadoria paga** (lanÃ§amentos). Lucro bruto / margem / lÃ­quido / PE acompanham. Caixa nÃ£o muda. PadrÃ£o = vendida. Mesma chave `agro_dre_cmv_modo_v1`.
- **DRE visual prÃ©via (09/08 + Mini DRE soma + card emprÃ©stimos 10/08 + legÃ­vel 13/08):** Resumo 16:9. Mini DRE: Receita â†’ â€¦ â†’ **Saldo final** (= soma). Card EmprÃ©stimos: devido/juros/total/pago/emprestado. **BalÃ£o no mouse** (`data-rg-tip`) em cada nÃºmero + `?` para texto longo. PE = termÃ´metro (Vendeu / Precisa / Folga). Indicadores permanece atÃ© 100%.
- **Filtro loja (10/08):** **Centro + Vila** (padrÃ£o) Â· Centro Â· Vila. Vendas/CMV vendida = PDV da(s) loja(s). Despesas/emprÃ©stimos = empresa cadastrada (Vila sem empresa prÃ³pria usa Centro). BI `/` mesmo recorte no filtro **NÃºmeros** (nÃ£o troca o PDV).

### 4.11 Caixa

- **Gaveta (Centro)** = turno principal do Centro Ã‚Â· **Vila Elias** (`ponto_caixa=vila`) = turno prÃƒÂ³prio da Vila Ã‚Â· **Notebook** = satÃƒÂ©lite do pai da loja do aparelho Ã‚Â· **Teste** = isolado, fora do lote.
- Painel Ã‚Â«todosÃ‚Â» e fechamento em lote: **sÃƒÂ³ a loja do seletor** (Centro Ãƒâ€” Vila) Ã¢â‚¬â€ nÃƒÂ£o misturam.
- Abrir caixa alinha o seletor de loja do PDV; venda usa depÃƒÂ³sito do `ponto_caixa` da sessÃƒÂ£o.
- **Antiburro (v10.04):** abrir Gaveta/Vila e trocar Loja no BI exige digitar `centro` ou `vila`; com caixa aberto o seletor fica travado.
- **Trava loja (v10.56):** reforÃƒÂ§o, retirada/saÃƒÂ­da, devoluÃƒÂ§ÃƒÂ£o, fiado, assumir sessÃƒÂ£o e venda **sÃƒÂ³** no turno da loja do aparelho; caixa fechado = bloqueia.
- MP Point automÃƒÂ¡tico: sÃƒÂ³ Gaveta Centro / Teste (nÃƒÂ£o Vila).
- Layout **16:9**, shell `.caixa-shell`, `100dvh` Ã¢â‚¬â€ nÃƒÂ£o coluna estreita.
- Util: `produtos/caixa_util.py`.
- **Abrir Ã¢â‚¬â€ CÃƒÂ©dulas (21/07):** botÃƒÂ£o **CÃƒÂ©dulas** na abertura (igual fechar) Ã‚Â· campo valor comeÃƒÂ§a vazio Ã‚Â· sugestÃƒÂ£o sÃƒÂ³ no card azul / placeholder Ã‚Â· modal `includes/caixa_cedulas_abertura_modal.html`.
- **DiferenÃƒÂ§a abertura (21/07):** se contagem Ã¢â€°Â  ÃƒÂºltimo fechamento Ã¢â€ â€™ grava `diferenca_abertura` Ã‚Â· aparece em **ConferÃƒÂªncias Ã‚Â· diferenÃƒÂ§as** como Ã‚Â«Abertura Ã‚Â· DinheiroÃ‚Â» Ã‚Â· `usuario` (abriu) + `usuario_fechamento` (fechou) sempre.
- **Retirada / saÃƒÂ­da (2026-06-24):** botÃƒÂ£o do painel Ã¢â€ â€™ **`/caixa/retiradas/`** (histÃƒÂ³rico com filtros data Ã‚Â· plano Ã‚Â· quem levou; padrÃƒÂ£o **hoje**; calendÃƒÂ¡rio Agro Date Picker). BotÃƒÂ£o laranja **Nova saÃƒÂ­da** Ã¢â€ â€™ formulÃƒÂ¡rio existente (`?painel=retirada`). Popup fechar caixa tambÃƒÂ©m abre o histÃƒÂ³rico (`embed=1`). Layout **rem/clamp** + herda **Agro Display Scale** (perfil ÃƒÂºnico / iframe pai).
- **Retiradas Ã¢â‚¬â€ vales RH (01/07):** histÃƒÂ³rico `/caixa/retiradas/` inclui **ValeFuncionario** (adiantamento) para conferÃƒÂªncia mensal Ã‚Â· filtro plano aceita **label ou cÃƒÂ³digo** Ã‚Â· vale no caixa nÃƒÂ£o gera Ã‚Â«SaÃƒÂ­da caixaÃ‚Â» no financeiro (baixa parcial no salÃƒÂ¡rio) Ã‚Â· **loja v5.64** cherry-pick `2207fd6`.
- **Repasse Vila â†’ Centro (13/08 Â· v16.10):** `/repasse-vila/` + PDV **Repasse** Â· CMV + % lucro + fiado pago Vila Â· migrate `0087` Â· aviso na abertura Gaveta Centro.

### 4.12 RH

- `/rh/` Ã¢â‚¬â€ folha, vales, ficha.
- Ajuda longa em `rh/templates/rh/includes/rh_help_agents.html` (espelho AGENTS.md Ã‚Â§9).
- Vale com financeiro = baixa parcial no tÃƒÂ­tulo de salÃƒÂ¡rio do mÃƒÂªs (precisa folha fechada com tÃƒÂ­tulo).
- Cancelar vale: motivo mÃƒÂ­n. 3 chars; recalcula folhas abertas.

### 4.13 Electron (desktop) Ã¢â‚¬â€ opcional; Renan nÃƒÂ£o usa

- `electron/main.js` + `preload.js` Ã¢â‚¬â€ `shell.openExternal` para WhatsApp/Maps.
- Mesmo Agro Display Scale via `localStorage` (nÃƒÂ£o `setZoomFactor` paralelo).
- **Renan:** testou e **nÃƒÂ£o usa** (lento). Loja/dev = **Chrome**. Recursos sÃƒÂ³ do shell (abas laterais `agro-open-inapp-tab`, iframes) **nÃƒÂ£o se aplicam** ao fluxo dele Ã¢â‚¬â€ otimizar para **MPA no Chrome** (bootstrap HTML, prefetch, cache).

### 4.14 UI compartilhada

- `_agro_consulta_ui.html` Ã¢â‚¬â€ base visual.
- `_agro_display_scale.html` + `agro_display_scale.js` Ã¢â‚¬â€ escala global.
- `_agro_open_external.html` Ã¢â‚¬â€ links externos.

**Popups / modais (decisÃƒÂ£o 08/07 Ã¢â‚¬â€ Renan):** padrÃƒÂ£o do produto = **`<div>` + Tailwind + JavaScript puro** (MPA Django). Abrir/fechar = tirar/colocar classe `hidden` (+ `modal-open` no `body` quando precisar). **NÃƒÂ£o** usar biblioteca de modal (Bootstrap, SweetAlert, etc.). **`<dialog>` nativo:** avaliado Ã¢â‚¬â€ **nÃƒÂ£o** adotar em popup novo por padrÃƒÂ£o (dezenas de modais jÃƒÂ¡ no padrÃƒÂ£o `div`; operador nÃƒÂ£o ganha nada; CPU/memÃƒÂ³ria imperceptÃƒÂ­vel). **Regra:** popup novo numa tela que jÃƒÂ¡ tem modal Ã¢â€ â€™ **copiar o padrÃƒÂ£o da tela**; tela zerada / FOOD do zero Ã¢â€ â€™ pode usar `<dialog>` se padronizar a tela inteira. CanÃƒÂ´nico tambÃƒÂ©m em **`SISTVALE.md`** e **`FOOD.md`**.

### 4.15 DesvinculaÃƒÂ§ÃƒÂ£o ERP (Mongo espelho Ã¢â€ â€™ Postgres SisVale)

**Resposta curta (Jul/2026):** **~85 % operaÃƒÂ§ÃƒÂ£o diÃƒÂ¡ria jÃƒÂ¡ Postgres.** Mongo ERP = **espelho + relatÃƒÂ³rios que faltam**. **NÃƒÂ£o** cancelar assinatura ERP atÃƒÂ© fechar checklist abaixo.

**Dois nÃƒÂ­veis:** (1) cortar **API ERP** Ã¢â€ â€™ **Ã¢Å“â€¦ feito**; (2) parar de **ler/gravar Mongo** em **todas** as telas Ã¢â€ â€™ **em andamento**.

### Checklist Ã¢â‚¬â€ corte total Mongo (ordem de prioridade)

Marque na loja apÃƒÂ³s deploy + Renan OK. **NÃƒÂ£o apagar Mongo** atÃƒÂ© item **12**.

| # | Pacote | Por quÃƒÂª nesta ordem | Loja hoje | PrÃƒÂ³ximo passo |
| - | ------ | ------------------- | --------- | ------------- |
| Ã¢Å“â€¦ | **API ERP** (Agro nÃƒÂ£o grava no legado) | Corte WL | **OK** | Ã¢â‚¬â€ |
| Ã¢Å“â€¦ | **PDV Ã‚Â· vendas Ã‚Â· NFC-e Ã‚Â· caixa Ã‚Â· RH Ã‚Â· clientes Ã‚Â· fiado** | Nativo Postgres | **OK** | Ã¢â‚¬â€ |
| Ã¢Å“â€¦ | **Cadastro + catÃƒÂ¡logo PDV + ledger estoque** | BalcÃƒÂ£o | **OK** | Ã¢â‚¬â€ |
| Ã¢Å“â€¦ | **CP/CR** lista + pagar + nova saÃƒÂ­da + editar | Financeiro dia | **OK** | Ã¢â‚¬â€ |
| Ã¢Å“â€¦ | **Entrada NF** passo 7 + rascunho PG | Compras/estoque | **OK v4.20** | Ã¢â‚¬â€ |
| Ã¢Å“â€¦ | **Compras D4** (mÃƒÂ©tricas + folhas) | ReposiÃƒÂ§ÃƒÂ£o | **OK** | Ã¢â‚¬â€ |
| Ã¢Å“â€¦ | **BI cards CP/CR + resumo gerencial** | GestÃƒÂ£o rÃƒÂ¡pida | **OK** | Ã¢â‚¬â€ |
| Ã¢Å“â€¦ | **TransferÃƒÂªncias + Validade** (leitura) | Estoque | **OK** | Ã¢â‚¬â€ |
| Ã¢Å“â€¦ | **Listas auxiliares** (plano, forma, banco, fornecedor NF) | Baixa CP / NF | **OK** | Ã¢â‚¬â€ |
| **1** | **DRE + fluxo calendÃƒÂ¡rio + export PDF/Excel** LanÃƒÂ§amentos | Param se Mongo cair | **PG** (flag auto loja) | Validar loja |
| **2** | **GrÃƒÂ¡fico gastos** (dados, nÃƒÂ£o sÃƒÂ³ UX) | BI financeiro | **PG** (flag auto loja) | Validar vs CP |
| **3** | **BI `/` histÃƒÂ³rico vendas** Ã¢â€ â€™ `VendaAgro` | Dashboard completo | **Ã¢Å“â€¦ teste v4.36** Ã¢â‚¬â€ sem DtoVenda/`vendas_agro` com `agro_pg` | Validar BI |
| **4** | **GestÃƒÂ£o produtos 100 % PG** (facetas + perf pÃƒÂ³s-NF) | LentidÃƒÂ£o / lista | **Ã¢Å“â€¦ teste v4.36** Ã¢â‚¬â€ sem fallback Mongo; saldo ledger (v4.33) | Validar gestÃƒÂ£o |
| **5** | **Compras dimensÃƒÂµes** relatÃƒÂ³rio (categoria/unidade sem scan Mongo) | Folhas grandes | **Ã¢Å“â€¦ teste v4.36** Ã¢â‚¬â€ buscar + NF etapa 6 sem Mongo obrigatÃƒÂ³rio | Validar Compras |
| **6** | **Entrada NF auditoria financeiro** + recovery tÃƒÂ­tulos PG | BotÃƒÂ£o auditoria | **Ã¢Å“â€¦ teste 28/06** Renan | NF 112 Ã‚Â· alertas 0 |
| **7** | **PDV Ã¢â€ â€™ GestÃƒÂ£o saldo** pÃƒÂ³s-venda (v3.82) | Estoque gestÃƒÂ£o | **Ã¢Å“â€¦ teste 28/06** | GM9503 47Ã¢â€ â€™46 |
| **8** | **Congelar Mongo financeiro** (`AGRO_FINANCEIRO_MONGO_CONGELADO`) | SÃƒÂ³ histÃƒÂ³rico | Off | ApÃƒÂ³s 1Ã¢â‚¬â€œ6 OK |
| **9** | **Motor busca GM** Compras/NF | UX `gm0050` | Quebrado | **Por ÃƒÂºltimo** |
| **10** | **Backup PC** (CP/CR ZIP, cadastro Excel, vendas, NFC-e) | Seguro | Renan | Antes do 11 |
| **11** | **Checkpoint LanÃƒÂ§amentos** + conferÃƒÂªncia totais | Prova corte | Feito parcial | Renan + assistente |
| **12** | **Cancelar assinatura ERP** | Fim espelho | Ã¢â‚¬â€ | SÃƒÂ³ apÃƒÂ³s **1Ã¢â‚¬â€œ11** estÃƒÂ¡veis |

**Regra deploy loja:** teste OK Ã¢â€ â€™ Renan *Ã‚Â«pode subir produÃƒÂ§ÃƒÂ£oÃ‚Â»* + senha **99738595** Ã‚Â· pacote por pacote (nÃƒÂ£o merge inteiro).

**Status debug:** `GET /api/agro/fonte-status/`



| Status                 | Tela / mÃƒÂ³dulo                                                     | Nota                                                                                                                                            |
| ---------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **Feito (Agro PG)**    | Clientes PDV, **Vendas**, **NFC-e**, **Caixa**, **RH**, **Fiado** (`/fiado/`, `FiadoTituloAgro`Ã¢â‚¬Â¦) | **Fiado = Postgres nativo** Ã¢â‚¬â€ nÃƒÂ£o usa `DtoLancamento`. Sync ERP opcional onde existir |
| **Teste Ã¢Å“â€¦**           | Cadastro, PDV catÃƒÂ¡logo (`agro_pg` + `SOMENTE_POSTGRES`), gestÃƒÂ£o/lista PG, Compras D4 + dimensÃƒÂµes, BI vendas VendaAgro, pacotes corte v4.31Ã¢â‚¬â€œv4.43 | **Ã¢Å“â€¦ Renan 28/06** checklist 1Ã¢â‚¬â€œ4 + 6Ã¢â‚¬â€œ7 Ã‚Â· DRE Ã¢ÂÂ­ |
| **Loja Ã¢Å“â€¦ parcial**    | Cadastro, PDV merge PG, Compras D4, CP/CR, Entrada NF v4.20, ledger | **Pacote corte v4.36** deploy **28/06** Ã‚Â· GestÃƒÂ£o/BI/Compras corte Mongo |
| **PG loja Ã¢â‚¬â€ validar** | DRE, calendÃƒÂ¡rio, export PDF/Excel, grÃƒÂ¡fico gastos (dados)         | Flag financeiro PG auto Ã¢â‚¬â€ conferir totais vs CP                                                                                                 |
| **Falta (mÃƒÂ©dia)**      | Busca **GM** Compras/NF                                           | Motor GM por ÃƒÂºltimo Ã¢â‚¬â€ **nÃƒÂ£o bloqueia** corte Mongo no teste                                                                                      |
| **Falta (operacional)** | Backup PC Ã‚Â· checkpoint totais Ã‚Â· congelar Mongo Ã‚Â· cancelar ERP    | Itens **8Ã¢â‚¬â€œ12** Ã‚Â§4.15 Ã¢â‚¬â€ Renan + assistente                                                                                                        |

**LanÃƒÂ§amentos vs desvinculaÃƒÂ§ÃƒÂ£o (Renan 2026-06-25 Ã¢â‚¬â€ nÃƒÂ£o confundir):**

| | **JÃƒÂ¡ fizemos** | **Ainda falta (Ã‚Â§4.15)** |
| --- | --- | --- |
| **LanÃƒÂ§amentos** | Layout CP, PIN, filtros, Nova saÃƒÂ­da, emprÃƒÂ©stimo dual, editar/excluir, backup ZIP, **checkpoint** (~17Ã¢â‚¬Â¯703 tÃƒÂ­tulos), **corte sync API ERP** (v1.14), perf bootstrap, **CP/CR PG loja** | DRE/calendÃƒÂ¡rio/export Ã¢â‚¬â€ **PG** se financeiro PG; validar totais |
| **Fiado** | GestÃƒÂ£o `/fiado/`, PDV limite/parcelas, tÃƒÂ­tulos **`FiadoTituloAgro`** Postgres | Nada no pacote Ã‚Â«desvincular Mongo catÃƒÂ¡logo/financeiroÃ‚Â». BI ainda pode cruzar `DtoVenda` para nÃƒÂ£o duplicar grÃƒÂ¡fico Ã¢â‚¬â€ **nÃƒÂ£o ÃƒÂ© a tela Fiado** |


**Estimativa (Renan Ã¢â‚¬â€ linguagem simples):** pacotes de **dias**, nÃƒÂ£o Ã¢â‚¬Å“meses inteirosÃ¢â‚¬Â Ã¢â‚¬â€ o grosso jÃƒÂ¡ estÃƒÂ¡ no teste; falta **subir na loja** + **financeiro/BI** por fatias.


| Etapa | O quÃƒÂª muda na prÃƒÂ¡tica                                                                                                  | Risco                                                        | Tempo (dev + teste) |
| ----- | ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | ------------------- |
| **1** | Cadastro Postgres (`agro_pg`)                                                                                          | MÃƒÂ©dio                                                        | **Ã¢Å“â€¦ teste** Ã‚Â· 1 dia loja |
| **2** | PDV + gestÃƒÂ£o mesma base + ledger                                                                                       | Alto no balcÃƒÂ£o                                               | **Ã¢Å“â€¦ teste** Ã‚Â· 1Ã¢â‚¬â€œ2 dias loja |
| **3** | Compras/NF busca + mÃƒÂ©tricas + LanÃƒÂ§amentos + BI                                                                         | Alto no caixa/contas                                         | **2Ã¢â‚¬â€œ5 dias por fatia** (CP primeiro) |


**Ordem sprint (Renan Ã¢â‚¬â€ Jun/2026, ~dias):**

| # | Pacote | Por quÃƒÂª nesta ordem | Dias (ordem de grandeza) |
| - | ------ | ------------------- | ------------------------ |
| **1** | **ProduÃƒÂ§ÃƒÂ£o = teste** (flags B+C + ledger + `agro_pg` + Entrada NF v2.78) | CÃƒÂ³digo **jÃƒÂ¡ validado** Ã¢â‚¬â€ **agendado loja fecha 25/06 noite** (ver CHECKPOINT) | **1** deploy |
| **2** | **Compras D4** (mÃƒÂ©tricas + folhas) | ÃƒÅ¡ltima compra / sugestÃƒÂ£o | **Ã¢Å“â€¦ teste** v2.79Ã¢â‚¬â€œv2.87 |
| **3** | **Entrada NF financeiro Agro** (tÃƒÂ­tulo sem `DtoLancamento` loja) | Wizard OK; tÃƒÂ­tulo real na loja | **2Ã¢â‚¬â€œ3** |
| **4** | **LanÃƒÂ§amentos Ã¢â‚¬â€ migraÃƒÂ§ÃƒÂ£o Postgres** (`agro_pg` financeiro) | Checkpoint/corte API **jÃƒÂ¡ feitos** Ã‚Â· falta **fonte de dados** | **3Ã¢â‚¬â€œ5** |
| **5** | **BI `/` + resumo** | VendaAgro Postgres | **Ã¢Å“â€¦ teste v4.36** Ã¢â‚¬â€ validar Ã‚Â· loja pendente |
| **6** | **TransferÃƒÂªncias Ã‚Â· Validade Ã‚Â· fornecedor NF** | Menor urgÃƒÂªncia | **1Ã¢â‚¬â€œ2** cada |
| **7** | **Motor busca ÃƒÂºnico** | **NÃƒÂ£o ÃƒÂ© desvinculaÃƒÂ§ÃƒÂ£o** Ã¢â‚¬â€ bug GM Compras/NF (`gm0050`); **por ÃƒÂºltimo** | **1Ã¢â‚¬â€œ2** |
| **8** | **Checkpoint final + cancelar assinatura ERP** | SÃƒÂ³ apÃƒÂ³s 1Ã¢â‚¬â€œ6 + backup | **0.5** |

**Motor busca Ã¢â€°Â  desvinculaÃƒÂ§ÃƒÂ£o:** catÃƒÂ¡logo/saldo jÃƒÂ¡ vÃƒÂªm do Postgres no teste; GM quebrado ÃƒÂ© **UX/paridade de busca** entre telas Ã¢â‚¬â€ **nÃƒÂ£o bloqueia** cortar Mongo. Pode ficar **ÃƒÂºltimo**.

**Ordem tÃƒÂ©cnica legada (referÃƒÂªncia):** cadastro Ã¢â€ â€™ `agro_pg` Ã¢â€ â€™ PDV busca Ã¢â€ â€™ gestÃƒÂ£o Ã¢â€ â€™ ledger Ã¢â€ â€™ financeiro checkpoint.

---

### Renan Ã¢â‚¬â€ desvinculo: duas listas (28/05/2026)

**CenÃƒÂ¡rio lista 1:** ERP para de enviar dados **ou** Mongo fica inacessÃƒÂ­vel **de repente** (nÃƒÂ£o ÃƒÂ© corte planejado).

#### 1) O que **para ou fica grave** (perde ERP/Mongo)

| ÃƒÂrea | O que acontece |
| ---- | -------------- |
| **Entrada NF** | Rascunho **PG** no teste/loja v4.20+; se Mongo cair, fallback legado atÃƒÂ© PG popular |
| **GestÃƒÂ£o produtos** | **Teste Ã¢Å“â€¦** lista/facetas PG Ã‚Â· **Loja** ainda Mongo (sem `SOMENTE_POSTGRES`) |
| **Compras** | **Teste Ã¢Å“â€¦** dimensÃƒÂµes/ÃƒÂºltima compra sem scan ERP Ã‚Â· **Loja** D4 OK; folhas grandes dependem de histÃƒÂ³rico Entrada NF Agro |
| **LanÃƒÂ§amentos** | CP/CR **PG loja** Ã‚Â· DRE/calendÃƒÂ¡rio/export **PG** se financeiro PG Ã¢â‚¬â€ validar totais |
| **GrÃƒÂ¡fico gastos** | **PG loja** (flag auto) Ã¢â‚¬â€ validar vs CP |
| **BI `/` + resumo gerencial** | **Teste Ã¢Å“â€¦** VendaAgro (v4.36) Ã‚Â· cards CP/CR **OK loja** |
| **TransferÃƒÂªncias / Validade** | Leitura OK; sync profundo ainda espelho Mongo |
| **Produtos novos no ERP** | **NÃƒÂ£o entram** no Agro atÃƒÂ© import/sync |
| **Listas auxiliares** | Fornecedor, plano de conta, formas Ã¢â‚¬â€ muitas telas ainda puxam Mongo |

**Continua funcionando** (Postgres Agro Ã¢â‚¬â€ operaÃƒÂ§ÃƒÂ£o do dia):

PDV venda Ã‚Â· caixa Ã‚Â· fiado Ã‚Â· clientes Ã‚Â· NFC-e Ã‚Â· RH Ã‚Â· **CP/CR LanÃƒÂ§amentos** Ã‚Â· saldo operacional (ledger/ajustes) Ã‚Â· preÃƒÂ§o overlay PG Ã‚Â· cadastro/PDV catÃƒÂ¡logo jÃƒÂ¡ importado.

---

#### 2) O que **ainda falta** para desvinculo completo (pode cancelar ERP)

*Espelho da tabela **Checklist Ã¢â‚¬â€ corte total Mongo** (Ã‚Â§4.15 acima).*

| # | Pacote | Status |
| - | ------ | ------ |
| Ã¢Å“â€¦ | Cadastro + PDV catÃƒÂ¡logo PG + ledger saldo | **Loja** |
| Ã¢Å“â€¦ | Compras D4 (mÃƒÂ©tricas/folhas) | **Loja** |
| Ã¢Å“â€¦ | Entrada NF passo 7 + rascunho PG | **PG loja v4.20** |
| Ã¢Å“â€¦ | LanÃƒÂ§amentos CP/CR lista + gravar | **PG loja** |
| Ã¢Å“â€¦ | Fiado, vendas, caixa, RH, NFC-e, clientes | **PG nativo** |
| Ã¢Å“â€¦ | Checkpoint + corte sync API financeiro | **Feito** |
| **1** | DRE + calendÃƒÂ¡rio + export PDF/Excel LanÃƒÂ§amentos | **PG loja** Ã¢â‚¬â€ validar |
| **2** | GrÃƒÂ¡fico gastos (dados) | **PG loja** Ã¢â‚¬â€ validar |
| **3** | BI `/` histÃƒÂ³rico Ã¢â€ â€™ VendaAgro | **Ã¢Å“â€¦ teste v4.36** Ã¢â‚¬â€ validar BI |
| **4** | GestÃƒÂ£o 100 % PG | **Ã¢Å“â€¦ teste v4.36** Ã‚Â· loja ainda Mongo |
| **5** | Compras dimensÃƒÂµes sem scan Mongo | **Ã¢Å“â€¦ teste v4.36** Ã¢â‚¬â€ validar Compras |
| **6** | NF auditoria financeiro | **Ã¢Å“â€¦ teste v4.31** Ã¢â‚¬â€ validar NF |
| **7** | PDV Ã¢â€ â€™ GestÃƒÂ£o saldo pÃƒÂ³s-venda | **Ã¢Å“â€¦ teste 28/06** | GM9503 47Ã¢â€ â€™46 sem F5 |
| **8** | Congelar Mongo financeiro | Off Ã¢â‚¬â€ apÃƒÂ³s 1Ã¢â‚¬â€œ7 OK |
| **9** | Motor busca GM Compras/NF | Por ÃƒÂºltimo |
| **10** | Backup PC | Renan |
| **11** | Checkpoint LanÃƒÂ§amentos + totais | Parcial |
| **12** | Cancelar assinatura ERP | SÃƒÂ³ apÃƒÂ³s **1Ã¢â‚¬â€œ11** + backup |

**MitigaÃƒÂ§ÃƒÂ£o:** fazer sÃƒÂ³ a etapa 1 no `teste`, conferir cadastro + busca + salvar preÃƒÂ§o, **sÃƒÂ³ entÃƒÂ£o** produÃƒÂ§ÃƒÂ£o.

**Antes da etapa 1 (Renan Ã¢â‚¬â€ vocÃƒÂª faz; assistente implementa depois):**

1. Testar **sÃƒÂ³ no ambiente teste** (Render staging) Ã¢â‚¬â€ **nÃƒÂ£o** na loja ainda.
2. Anotar **2 ou 3 produtos** que vocÃƒÂª conhece: nome, preÃƒÂ§o de venda, cÃƒÂ³digo GM ou barras (para comparar depois).
3. Entrar no cadastro no teste com seu login e confirmar que abre `/produtos/cadastro-erp/`.
4. Confirmar no Render que o **staging tem banco Postgres prÃƒÂ³prio** (nÃƒÂ£o o mesmo da produÃƒÂ§ÃƒÂ£o).
5. **NÃƒÂ£o esperar** que o PDV mude nesta etapa Ã¢â‚¬â€ sÃƒÂ³ a tela de cadastro.

**Depois que o assistente entregar etapa 1 (vocÃƒÂª testa):**

1. Aguardar deploy do branch `teste` no staging + **Ctrl+F5** no cadastro.
2. Conferir se a lista e a busca acham os produtos anotados.
3. Alterar preÃƒÂ§o de um produto Ã¢â€ â€™ salvar Ã¢â€ â€™ buscar de novo Ã¢â€ â€™ preÃƒÂ§o **deve permanecer**.
4. Criar produto novo (se liberado no teste) ou pular se ainda bloqueado no staging.
5. Se algo falhar: anotar nome do produto + o que viu (sumiu, preÃƒÂ§o voltou, lista vazia).

**Etapa 1 Ã¢â‚¬â€ validada no teste (2026-06-22, Renan):** flag `agro_pg` + import Ã‚Â· GM0027-1 a **R$ 21,00** persiste Ã‚Â· busca nome/cÃƒÂ³digo OK Ã‚Â· piscadinha preÃƒÂ§o resolvida (v1.84) Ã‚Â· abertura lista mais rÃƒÂ¡pida (v1.85 prefetch + Postgres sem count). **PrÃƒÂ³ximo:** cherry-pick etapa 1 Ã¢â€ â€™ produÃƒÂ§ÃƒÂ£o **sÃƒÂ³ quando Renan pedir**; PDV ainda Mongo.

**Produto de teste (Renan):** GM0027-1 Ã‚Â· RaÃƒÂ§ÃƒÂ£o Formula Natural adulto raÃƒÂ§as pequenas 1kg Ã¢â‚¬â€ **preÃƒÂ§o correto R$ 21,00** (staging/Agro); espelho Mongo R$ 19,90. Usar para validar busca + salvar + preÃƒÂ§o persistente.

---

### Renan Ã¢â‚¬â€ LanÃƒÂ§amentos: o que jÃƒÂ¡ fizemos e o que falta (linguagem leiga)

**Imagine dois armÃƒÂ¡rios de boletos/contas:**

| ArmÃƒÂ¡rio | O que ÃƒÂ© |
| ------- | ------- |
| **Mongo (espelho ERP)** | Onde **hoje** ficam ~17 mil contas a pagar/receber que a loja usa |
| **Postgres Agro** | O armÃƒÂ¡rio **novo** do SisVale Ã¢â‚¬â€ jÃƒÂ¡ existe a gaveta (`TituloFinanceiroAgro`), mas a tela **ainda nÃƒÂ£o abre nele** |

**Dois Ã¢â‚¬Å“cortesÃ¢â‚¬Â diferentes (nÃƒÂ£o confundir):**

| Corte | Significado | Status |
| ----- | ----------- | ------ |
| **1 Ã¢â‚¬â€ Parar de falar com o ERP** | SisVale **nÃƒÂ£o envia** mais baixa/lanÃƒÂ§amento para o sistema antigo (API WL) | **Ã¢Å“â€¦ Feito** (checkpoint + cÃƒÂ³digo v1.14) |
| **2 Ã¢â‚¬â€ Mudar a tela LanÃƒÂ§amentos** | Contas a pagar/receber passam a **ler e gravar no Postgres Agro**, nÃƒÂ£o no Mongo | **Ã°Å¸â€Â¥ HOJE** Ã¢â‚¬â€ escopo **CP primeiro** |

---

**O que JÃƒÂ fizemos (LanÃƒÂ§amentos + corte ERP):**

1. **Tela nova** Ã¢â‚¬â€ Contas a pagar/receber mais rÃƒÂ¡pida: filtros, PIN, Nova saÃƒÂ­da, excluir, emprÃƒÂ©stimo dual, etc.
2. **Backup** Ã¢â‚¬â€ Planilha/ZIP dos tÃƒÂ­tulos guardada (cÃƒÂ³pia de seguranÃƒÂ§a no PC).
3. **Checkpoint** Ã¢â‚¬â€ Ã¢â‚¬Å“CarimboÃ¢â‚¬Â nos ~17 mil tÃƒÂ­tulos no Mongo (**nÃƒÂ£o apaga nada**, sÃƒÂ³ marca).
4. **Corte API ERP** Ã¢â‚¬â€ Depois do checkpoint, o SisVale **nÃƒÂ£o manda** mais lanÃƒÂ§amento/baixa para o ERP por API.
5. **Gaveta nova no Postgres** Ã¢â‚¬â€ Tabela preparada + comando para **copiar** tÃƒÂ­tulos do Mongo (hoje sÃƒÂ³ **simulaÃƒÂ§ÃƒÂ£o** no teste, nÃƒÂ£o ligou na loja).
6. **Loja hoje** Ã¢â‚¬â€ Operador usa LanÃƒÂ§amentos **normal**; boletos e Entrada NF passo 7 **continuam no Mongo** e **funcionam** (Renan validou Ã¢Å“â€¦).

**O que AINDA falta para Ã¢â‚¬Å“terminar o corteÃ¢â‚¬Â da tela LanÃƒÂ§amentos com Mongo:**

| Passo | Em portuguÃƒÂªs claro | Onde |
| ----- | ------------------ | ---- |
| **A** | **Copiar** os ~17 mil tÃƒÂ­tulos do Mongo para o Postgres (comando `importarÃ¢â‚¬Â¦ --apply`) | Primeiro **teste**, depois **loja** |
| **B** | **Reprogramar a tela** Ã¢â‚¬â€ lista, busca, quitar, excluir, Nova saÃƒÂ­da, DRE, PDF, calendÃƒÂ¡rio Ã¢â‚¬â€ para usar o Postgres em vez do Mongo | Dev **teste** |
| **C** | **Entrada NF passo 7** Ã¢â‚¬â€ Ã¢â‚¬Å“Salvar + a pagarÃ¢â‚¬Â gravar no Postgres, nÃƒÂ£o sÃƒÂ³ no Mongo | Junto com B |
| **D** | **Testar tudo** no staging (criar, pagar, excluir, conferir saldo) | Renan no **teste** |
| **E** | **Ligar na loja** Ã¢â‚¬â€ flag `AGRO_FONTE_FINANCEIRO=agro_pg` **sÃƒÂ³ quando B+C estiverem prontos** | Deploy **produÃƒÂ§ÃƒÂ£o** + senha |
| **F** | Mongo vira **sÃƒÂ³ histÃƒÂ³rico** (nÃƒÂ£o entra tÃƒÂ­tulo novo) Ã¢â‚¬â€ opcional congelar de vez | Depois de E estÃƒÂ¡vel |

**Prioridade Renan (25/06 Ã¢â‚¬â€ ordem fixa):**

1. **Contas a pagar EM ABERTO** Ã¢â‚¬â€ mÃƒÂ¡xima seguranÃƒÂ§a Ã‚Â· zero perda  
2. **Contas a pagar** (todas)  
3. **Contas a receber** Ã¢â‚¬â€ **por ÃƒÂºltimo** (quase nÃƒÂ£o usam; fiado = outra tela)

**Regra:** Mongo **nÃƒÂ£o some** atÃƒÂ© espelho PG conferido Ã‚Â· leitura PG **sÃƒÂ³** com flag Ã‚Â· gravaÃƒÂ§ÃƒÂ£o dupla ou rollback se falhar.

**Escopo HOJE:** CP em aberto Ã¢â€ â€™ CP lista/filtros Ã¢â€ â€™ CR depois (se der tempo).

**Fiado nÃƒÂ£o entra** Ã¢â‚¬â€ jÃƒÂ¡ Postgres nativo.

---

### WIP HOJE Ã¢â‚¬â€ LanÃƒÂ§amentos Postgres (CP primeiro) Ã¢â‚¬â€ **teste OK 26/06**

| Quem | O quÃƒÂª |
| ---- | ----- |
### CP Postgres Ã¢â‚¬â€ o que falta + quem faz (fechar o serviÃƒÂ§o)

| # | Quem | O quÃƒÂª | Quando |
| --- | ---- | ----- | ------ |
| **1** | **Assistente** | CÃƒÂ³digo: **pagar, parcial, nova saÃƒÂ­da, editar, excluir** CP Ã¢â€ â€™ Postgres (teste) | **Ã¢Å“â€¦ v3.40 teste** |
| **2** | **Renan** | No **teste**: pagar **1 tÃƒÂ­tulo pequeno** (ou de teste) + conferir se sumiu da lista / saldo certo | **Ã¢Å“â€¦ Renan 26/06** Ã¢â‚¬â€ Ã‚Â«TesteÃ‚Â» R$ 1,00 quitado PG |
| **3** | **Assistente** | Import PG na **loja** + deploy pacote **testeÃ¢â€ â€™producao** | **Ã¢Å“â€¦ em curso v3.03** |
| **4** | **Renan** | Frase *Ã‚Â«pode subir produÃƒÂ§ÃƒÂ£oÃ‚Â»* + senha **`99738595`** **no mesmo pedido** | **Ã¢Å“â€¦ Renan 26/06** |
| **5** | **Renan** | **~30 min** CP sÃƒÂ³ consulta na loja (ou avisar equipe: **nÃƒÂ£o pagar** na CP na hora H) | **Ã¢Å“â€¦ 26/06** |
| **6** | Loja: CP aberto **741** + total | **Ã¢Å“â€¦ 26/06** Ã¢â‚¬â€ pagamento real **dispensado** |
| **7 CR loja** | ConferÃƒÂªncia lista CR | **Ã¢Å“â€¦ 26/06** Ã¢â‚¬â€ **453** Ã‚Â· **R$ 29.241,58** |
| **8 Painel backup** | Card amarelo recolhido (admin) | **Ã¢Å“â€¦ v3.08** deploy loja |

**Renan (26/06):** nÃƒÂ£o precisa teste de pagar na CP na loja Ã‚Â· **fiado** = operaÃƒÂ§ÃƒÂ£o principal (jÃƒÂ¡ Postgres nativo).

### PrÃƒÂ³ximo Ã¢â‚¬â€ finalizar LanÃƒÂ§amentos / financeiro (ordem)

| # | O quÃƒÂª | UrgÃƒÂªncia loja |
| --- | ----- | ------------- |
| **1** | **Entrada NF passo 7** Ã¢â‚¬â€ tÃƒÂ­tulos Ã‚Â«Salvar + a pagarÃ‚Â» | **Ã¢Å“â€¦ jÃƒÂ¡ Postgres na loja** (`inserir_lancamentos_manual_lote_dispatch`) Ã‚Â· rascunho NF (etapas 1Ã¢â‚¬â€œ6) continua Mongo |
| **2** | **DRE / fluxo calendÃƒÂ¡rio / PDF** LanÃƒÂ§amentos Ã¢â‚¬â€ ler Postgres | Baixa (telas secundÃƒÂ¡rias) |
| **3** | **Esconder painel amarelo** checkpoint (admin) Ã¢â‚¬â€ migraÃƒÂ§ÃƒÂ£o CP/CR fechada | CosmÃƒÂ©tico |
| **4** | **Congelar Mongo financeiro** (opcional) Ã¢â‚¬â€ tÃƒÂ­tulo novo sÃƒÂ³ PG; Mongo sÃƒÂ³ histÃƒÂ³rico | SÃƒÂ³ quando 1Ã¢â‚¬â€œ2 estÃƒÂ¡veis |
| **Ã¢â‚¬â€** | **Fiado** | **Nada** Ã¢â‚¬â€ jÃƒÂ¡ fora deste pacote |

**Renan NÃƒÆ’O precisa:** mexer Render Ã‚Â· import manual Ã‚Â· cÃƒÂ³digo Ã‚Â· backup de novo (jÃƒÂ¡ tem no PC).

**Bloqueio CP Postgres (em aberto):** **fechado 26/06** Ã¢â‚¬â€ loja operando CP no Postgres.

### WIP Ã¢â‚¬â€ Contas a receber Postgres **26/06**

| Item | Status |
| ---- | ------ |
| Lista + filtros + planos CR | **Ã¢Å“â€¦ cÃƒÂ³digo teste** (mesmo PG dos 17,8 mil tÃƒÂ­tulos) |
| Receber / parcial / editar / excluir / lote | **Ã¢Å“â€¦ cÃƒÂ³digo teste** |
| Fiado | **fora** Ã¢â‚¬â€ tela prÃƒÂ³pria Postgres |
| Deploy loja | **Ã¢Å“â€¦ v3.07** Ã¢â‚¬â€ CR conferida **453** / **29.241,58** |


### Renan Ã¢â‚¬â€ intervenÃƒÂ§ÃƒÂ£o (estritamente necessÃƒÂ¡rio)

| # | Status | Detalhe |
| --- | ------ | ------- |
| **1 Backup** | **Ã¢Å“â€¦ Renan 25/06** | ZIP abertos + todos no PC |
| **2 Conferir total aberto** | **Ã¢Å“â€¦ Renan 25/06** | CP **sem filtro de data** Ã‚Â· tela **Qtd 741** Ã‚Â· Excel **`01_a_pagar_em_aberto.csv` = 748 linhas** (+7) Ã‚Â· total **~R$ 393.652,70** (tela) vs **~R$ 393.667,21** (Excel) Ã‚Â· dif. **~R$ 14,51** Ã‚Â· **causa: backup sem dedup** (tela funde dup. ERP; ZIP nÃƒÂ£o) Ã‚Â· bloco **Geraldo / acordo sat / R$ 600** = mesmo **ID ERP** repetido Ã¢â‚¬â€ linhas extras, saldo pode ser 0 |
| **3 Render env** | **Ã¢Å“â€¦ Renan Ã¢â‚¬â€ nada a fazer** | **NÃƒÂ£o** alterar variÃƒÂ¡veis no Render (loja nem teste) Ã‚Â· passo = **ficar parada** atÃƒÂ© assistente avisar |
| **4 PÃƒÂ³s-import teste** | **Ã¢Å“â€¦ Renan 26/06** | Staging CP aberto: **Qtd 741** Ã‚Â· **A pagar R$ 393.652,70** Ã¢â‚¬â€ **bate loja** Ã‚Â· amostra expandida OK (ex. Renan Hinnen **R$ 163,00**) |
| **4b Pagamento PG teste** | **Ã¢Å“â€¦ Renan 26/06** | TÃƒÂ­tulo manual **Ã‚Â«TesteÃ‚Â» R$ 1,00** Ã‚Â· quitou no Postgres Ã‚Â· sumiu de **Em aberto** Ã‚Â· aparece em **Quitados** (saldo 0) Ã‚Â· juros R$ 0,10 **nÃƒÂ£o** lanÃƒÂ§ou (conta Ã‚Â«ADICIONAR CONTAÃ‚Â» Ã¢â‚¬â€ regra esperada) |
| **5 Fiado / CR** | CR **Ã¢Å“â€¦ cÃƒÂ³digo 26/06** Ã‚Â· fiado fora |

**Loja hoje (26/06):** **Contas a pagar liberada** Ã¢â‚¬â€ lista + pagar no **Postgres** (conferido **741** / **393.652,70**). Caixa/PDV normal.

### Por que Contas a pagar **nÃƒÂ£o estÃƒÂ¡ 100 % concluÃƒÂ­da** ainda?

**NÃƒÂ£o ÃƒÂ© bug** Ã¢â‚¬â€ foi **de propÃƒÂ³sito**, em **etapas**, porque sÃƒÂ£o **~17 mil tÃƒÂ­tulos** e **~R$ 393 mil** em aberto.

| Etapa | O quÃƒÂª | Status |
| ----- | ----- | ------ |
| **A** | Backup + conferir total na **loja** | **Ã¢Å“â€¦ Renan** |
| **B** | Copiar tÃƒÂ­tulos Mongo Ã¢â€ â€™ Postgres | **Ã¢Å“â€¦ teste** (13,2 mil) |
| **C** | **Lista** CP (ver, filtrar, total) no Postgres | **Ã¢Å“â€¦ teste** (741 / 393.652,70 bateu) |
| **D** | **Gravar** no Postgres: pagar, parcial, nova saÃƒÂ­da, editar, excluir | **Ã¢Å“â€¦ teste v3.40** (deploy pendente Render) |
| **E** | Validar **D** no teste (pagar 1 tÃƒÂ­tulo de teste) | **Ã¢Å“â€¦ Renan 26/06** |
| **F** | Import + flag na **loja** (senha) | **Ã¢Å“â€¦ v3.04** Ã¢â‚¬â€ **741** / **393.652,70** conferido Renan |

**Por que parou antes de D?**  
Primeiro garantimos: *Ã‚Â«a cÃƒÂ³pia no Postgres ÃƒÂ© a mesma coisa que a tela da loja?Ã‚Â»* Ã¢â‚¬â€ **sim** (passo 4).  
SÃƒÂ³ **depois** mexemos em **pagamento** Ã¢â‚¬â€ se errar, some dinheiro ou duplica tÃƒÂ­tulo.

**MetÃƒÂ¡fora:** armÃƒÂ¡rio novo montado e **inventÃƒÂ¡rio conferido**; falta passar a **usar o armÃƒÂ¡rio de verdade** (cada pagamento sair do Mongo e ir pro Postgres).

**Loja:** CP **Postgres** (lista + pagamento). Mongo **nÃƒÂ£o apagado** Ã¢â‚¬â€ sÃƒÂ³ espelho/backup.

### Renan Ã¢â‚¬â€ como fazer (passos 3 e 4)

**Passo 3 Ã¢â‚¬â€ agora (sÃƒÂ³ nÃƒÂ£o mexer)**

| FaÃƒÂ§a | NÃƒÂ£o faÃƒÂ§a |
| ---- | -------- |
| **Nada** Ã¢â‚¬â€ pode usar a **loja** normal (PDV, CP, etc.) | Abrir Render Ã¢â€ â€™ **Environment** Ã¢â€ â€™ **Save** em variÃƒÂ¡vel nova |
| Se abrir o Render por curiosidade: **sÃƒÂ³ olhar**, sem salvar | Criar/editar `AGRO_FONTE_FINANCEIRO`, `SOMENTE_POSTGRES`, `STAGING_READONLY`, etc. |
| | Pedir deploy **produÃƒÂ§ÃƒÂ£o** / merge `teste`Ã¢â€ â€™`producao` hoje |

**Passo 3 = jÃƒÂ¡ feito** se vocÃƒÂª **nÃƒÂ£o** alterou env no Render desde o backup.

---

**Passo 4 Ã¢â‚¬â€ quando o assistente avisar Ã‚Â«import teste prontoÃ‚Â»**

Site = **projeto teste** no Render (branch `teste`) Ã¢â‚¬â€ **nÃƒÂ£o** `sistvale.com.br`.

1. Login **admin** no **teste**.
2. **Ctrl+F5** (recarregar sem cache).
3. Abrir **`/lancamentos/contas-pagar/`**.
4. **Filtros** Ã¢â€ â€™ SituaÃƒÂ§ÃƒÂ£o **Em aberto** Ã¢â€ â€™ **limpar** vencimento (de/atÃƒÂ© **vazios**) Ã¢â€ â€™ **Marcar todos** planos Ã¢â€ â€™ **Aplicar**.
5. Conferir rodapÃƒÂ© Ã¢â‚¬â€ deve bater com a **loja** (referÃƒÂªncia anotada):
   - **Qtd: 741** (pode variar Ã‚Â±1 se alguÃƒÂ©m pagou entre ontem e hoje)
   - **A pagar: ~R$ 393.652,70**
6. **Opcional (amostra):** clique **em cima de 3 linhas** da lista (ex. Detran, Renan Hinnen, CaminhÃƒÂ£o) Ã¢â‚¬â€ a linha **abre** e mostra detalhes; confira se **saldo** = coluna Saldo (ex. R$ 234,78). **NÃƒÂ£o precisa pagar** nem editar.
7. Me avisar **Ã‚Â«passo 4 OKÃ‚Â»** Ã¢â‚¬â€ se **Qtd + A pagar** jÃƒÂ¡ bateram (como no print), **pode pular o item 6**.

**Passo 5:** fiado e contas a receber Ã¢â‚¬â€ **ignorar hoje**.

**Assistente (26/06 Ã¢â‚¬â€ tÃƒÂ©cnico):**

| Item | Detalhe |
| ---- | ------- |
| **CÃƒÂ³digo** | `lancamentos_financeiro_pg_util.py` Ã¢â‚¬â€ lista CP Postgres + dedup igual Mongo |
| **API** | `api/lancamentos/contas-pagar/` lÃƒÂª PG quando `AGRO_FONTE_FINANCEIRO=agro_pg` |
| **Import HTTP** | `GET /api/cron/importar-titulos-financeiro-mongo-pg/?token=Ã¢â‚¬Â¦` (staging, `DRY_RUN=true`) |
| **ConferÃƒÂªncia** | `GET /api/agro/financeiro-pg-conferencia/` (superuser) Ã¢â‚¬â€ Mongo vs PG abertos |
| **Flag teste** | AutomÃƒÂ¡tico no staging apÃƒÂ³s import (`financeiro_postgres: true` no fonte-status) |
| **Bootstrap** | `scripts/staging_financeiro_pg_bootstrap.py` na build + fallback no boot |

**Excel 748 vs tela 741 Ã¢â‚¬â€ fechado (dedup):** backup exporta **cada** doc Mongo; lista CP **deduplica** tÃƒÂ­tulos ERP repetidos (ex. **Geraldo acordo sat**, mesmo `Id`, vÃƒÂ¡rias linhas no CSV). **ReferÃƒÂªncia para migraÃƒÂ§ÃƒÂ£o = tela (741 + total deduplicado)**, nÃƒÂ£o soma crua do Excel.

**Excel > tela (~R$ 14) Ã¢â‚¬â€ causas (histÃƒÂ³rico):**

**URLs backup (referÃƒÂªncia):**

| O quÃƒÂª | URL |
| ----- | --- |
| **Em aberto** | `https://sistvale.com.br/api/lancamentos/backup-abertos.zip` |
| **Todos** | `https://sistvale.com.br/api/lancamentos/backup-completo.xlsx` |

Aguarde ~1 min Ã‚Â· salva ZIP Ã‚Â· repita o segundo link.

**Depois deploy teste:** painel amarelo **Ã‚Â«SeguranÃƒÂ§a Ã¢â‚¬â€ antes de cortar o ERPÃ‚Â»** no topo de **`/lancamentos/contas-pagar/`** (admin).

**Backup (superuser Ã¢â‚¬â€ faÃƒÂ§a na LOJA antes do import):**

1. Chrome Ã¢â€ â€™ login **admin** Ã¢â€ â€™ URLs acima **ou** CP nova apÃƒÂ³s deploy.
2. **NÃƒÂ£o** clique em **Fazer checkpoint** hoje Ã¢â‚¬â€ sÃƒÂ³ se assistente pedir.
3. Anote **todos CP em aberto** Ã¢â‚¬â€ **nÃƒÂ£o** sÃƒÂ³ Ã‚Â«HojeÃ‚Â»: Filtros Ã¢â€ â€™ situaÃƒÂ§ÃƒÂ£o **Em aberto** Ã¢â€ â€™ **limpar** vencimento (sem data) Ã¢â€ â€™ anotar **Qtd** + **A pagar** do rodapÃƒÂ©.

| # | Quando | FaÃƒÂ§a |
| --- | ------ | ---- |
| **1** | **Antes de qualquer `--apply`** | Passos **1Ã¢â‚¬â€œ5** acima (ZIP aberto + ZIP todos) |
| **2** | **Antes import** | **Todos** CP em aberto (sem filtro de data) Ã¢â‚¬â€ Qtd + total Ã‚Â«A pagarÃ‚Â» |
| **3** | **Antes import** | Backup URLs (item 1) Ã¢â‚¬â€ ZIP bate com **todos** abertos, nÃƒÂ£o sÃƒÂ³ venc. de hoje |
| **4** | **Render** | **NÃƒÂ£o** mexer em env sozinho Ã¢â‚¬â€ assistente pede quando for hora |
| **5** | **Loja** | Flag PG / deploy **sÃƒÂ³** com frase + senha Ã‚Â· **sÃƒÂ³** apÃƒÂ³s teste OK |
| **6** | **CR / fiado** | Ignorar hoje Ã¢â‚¬â€ fiado jÃƒÂ¡ ÃƒÂ© Postgres |

**NÃƒÂ£o fazer:** apagar Mongo Ã‚Â· ligar `AGRO_FONTE_FINANCEIRO=agro_pg` na loja sem OK do assistente.

### PRÃƒâ€œXIMO AGORA Ã¢â‚¬â€ validaÃƒÂ§ÃƒÂ£o corte Mongo **v4.38** *(substitui bloco 27/06 abaixo)*

| Quem | AÃƒÂ§ÃƒÂ£o |
| ---- | ---- |
| **Renan** | Roteiro **7 passos** no CHECKPOINT acima Ã¢â‚¬â€ marcar OK ou reportar # + erro |
| **Assistente** | SÃƒÂ³ apÃƒÂ³s 7 OK + senha Ã¢â€ â€™ cherry-pick loja Ã‚Â· item **8Ã¢â‚¬â€œ12** Ã‚Â§4.15 depois |

**NÃƒÂ£o fazer agora:** merge `teste`Ã¢â€ â€™`producao` inteiro Ã‚Â· ligar `SOMENTE_POSTGRES` na **loja** sem pacote Ã‚Â· apagar Mongo.

<details>
<summary>histÃƒÂ³rico Ã¢â‚¬â€ retomada pÃƒÂ³s-sync CP v3.58 (27/06)</summary>

### PRÃƒâ€œXIMO AGORA Ã¢â‚¬â€ retomada pÃƒÂ³s-sync CP **v3.58** (27/06)

**Fechado hoje:** sync shell prod Ã‚Â· CP jul **145 Ã‚Â· 82.642,99** = Mongo/Excel/grÃƒÂ¡fico.

**Assistente avanÃƒÂ§ou (teste Ã¢â‚¬â€ Renan valida depois, um a um):**

| # | Entrega teste | Renan testar quando puder |
| - | ------------- | ------------------------- |
| **A** | **Resumo gerencial** Ã¢â€ â€™ Postgres (`TituloFinanceiroAgro`) quando financeiro PG | `/financeiro/resumo-gerencial/` Ã‚Â· fonte Postgres Ã‚Â· competÃƒÂªncia/vencimento |
| **B** | **PDV pÃƒÂ³s-venda** Ã¢â‚¬â€ API devolve `pdv_catalog_patches` Ã‚Â· cache local atualiza saldo | Vender 1 un. Ã¢â€ â€™ Consulta/GestÃƒÂ£o saldo desce sem F5 estoque |
| **C** | **Painel amarelo CP** Ã¢â‚¬â€ some para admin quando PG jÃƒÂ¡ tem tÃƒÂ­tulos | `/lancamentos/contas-pagar/` superuser |
| **D** | *(jÃƒÂ¡ no teste)* DRE Ã‚Â· fluxo calendÃƒÂ¡rio Ã‚Â· export PDF/XLSX PG | Ã¢ÂÂ­ DRE opcional |
| **E** | **Entrada NF rascunho PG** (cÃƒÂ³digo pronto teste) Ã‚Â· motor GM Ã‚Â· TransferÃƒÂªncias/Validade | `/entrada-nota/` wizard 1Ã¢â‚¬â€œ7 |

**Ordem sugerida (banana + checklist pacote corte ERP):**

| # | O quÃƒÂª | Quem | UrgÃƒÂªncia |
| - | ----- | ---- | -------- |
| **1** | **Ctrl+F5 loja** Ã¢â‚¬â€ CP jul aberto + grÃƒÂ¡fico jul = **82.643** (confirma na tela) | Renan | **Agora** Ã‚Â· 2 min |
| **2** | **GestÃƒÂ£o saldo pÃƒÂ³s-venda** Ã¢â‚¬â€ fix **v3.82** sÃƒÂ³ no **teste** Ã‚Â· vender 1 un. Ã¢â€ â€™ gestÃƒÂ£o deve baixar estoque | Renan teste | **Alta** Ã¢â‚¬â€ loja ainda **v3.54** (bug baixa sem Mongo) Ã‚Â· sobe **no pacote**, nÃƒÂ£o cherry avulso |
| **3** | **Pacote desvinculaÃƒÂ§ÃƒÂ£o restante** Ã¢â‚¬â€ baixa estoque PDV + flags B+C loja + Compras/NF onde falta | Assistente Ã¢â€ â€™ teste Ã¢â€ â€™ loja com senha | **Alta** Ã¢â‚¬â€ Renan disse estoque loja perdido Ã‚Â· recontar antes ou depois do pacote |
| **4** | **Entrada NF rascunho** etapas 1Ã¢â‚¬â€œ6 Ã¢â€ â€™ **Postgres** (teste v4.09) Ã‚Â· passo 7 financeiro **jÃƒÂ¡ PG** | Assistente Ã¢â€ â€™ Renan testa staging | MÃƒÂ©dia |
| **5** | **DRE / fluxo calendÃƒÂ¡rio / PDF** ler Postgres | Assistente | Baixa (Renan Ã¢ÂÂ­ DRE) |
| **6** | **BI card gastos-plano** | Opcional | SÃƒÂ³ se ligar `AGRO_DASHBOARD_GASTOS_PLANO=true` |
| **7** | **Motor busca GM** (`gm0050` Compras/NF) | Por ÃƒÂºltimo | NÃƒÂ£o bloqueia operaÃƒÂ§ÃƒÂ£o |
| **8** | **TransferÃƒÂªncias Ã‚Â· Validade Ã‚Â· fornecedor NF** | Fase 6 desvinculo | Baixa |

**Ignorar por ora (Renan):** Compras planilha recarrega Ã‚Â· calendÃƒÂ¡rio fluxo no **teste** mistura vendas staging.

**SÃƒÂ³ no teste (diff vs loja ~9 arquivos):** grÃƒÂ¡fico gastos UX Ã‚Â· PDV/views Ã‚Â· `catalogo_agro` Ã‚Â· diag CP Ã¢â‚¬â€ **nÃƒÂ£o** merge inteiro; cherry-pick por pacote.

**Assistente Ã¢â‚¬â€ prÃƒÂ³ximo cÃƒÂ³digo:** retomar item **2** (validar v3.82 teste) ou item **3** (pacote baixa estoque + desvinculo loja) Ã¢â‚¬â€ **Renan escolhe**.

</details>

### WIP AGORA Ã¢â‚¬â€ validaÃƒÂ§ÃƒÂ£o corte Mongo v4.38

| Foco | Detalhe |
| ---- | ------- |
| **Ã°Å¸â€Â¥ AGORA** | Renan Ã¢â‚¬â€ roteiro **7 passos** (CHECKPOINT) no **staging** |
| **Assistente** | Parado atÃƒÂ© OK ou reporte de bug (# + URL) |
| **Loja** | v4.21 Ã¢â‚¬â€ pacote corte **nÃƒÂ£o** entrou ainda |
| **Depois 7 OK** | Cherry-pick loja com senha Ã‚Â· itens **8Ã¢â‚¬â€œ12** Ã‚Â§4.15 |

<details>
<summary>histÃƒÂ³rico WIP Ã¢â‚¬â€ CP PG jun/2026</summary>

### WIP AGORA Ã¢â‚¬â€ pÃƒÂ³s-validaÃƒÂ§ÃƒÂ£o loja *(histÃƒÂ³rico Ã¢â‚¬â€ ver PRÃƒâ€œXIMO AGORA acima)*

| Foco | Detalhe |
| ---- | ------- |
| **Ã°Å¸â€Â¥ HOJE** | LanÃƒÂ§amentos PG Ã¢â‚¬â€ **CP em aberto** primeiro |
| **Loja operando** | Continua **Mongo** atÃƒÂ© Renan + assistente validarem teste |
| **Motor busca** | Ã¢ÂÂ¸ pausado |
| **NF passo 7 PG** | Depois da CP em aberto estÃƒÂ¡vel |

**JÃƒÂ¡ feito (jun/2026):** backup, checkpoint, corte AgroÃ¢â€ â€™ERP na API, layout CP, PIN, perf Ã¢â‚¬â€ ver Ã‚Â§4.10 e tabela acima.

**Pendente:** `AGRO_FONTE_FINANCEIRO=agro_pg` Ã¢â‚¬â€ lista/gravaÃƒÂ§ÃƒÂ£o sair do `DtoLancamento` Mongo (Ã‚Â§4.15 sprint #4).

**Prep v2.90 (seguro Ã¢â‚¬â€ nÃƒÂ£o liga flag):**
- Modelo Postgres **`TituloFinanceiroAgro`** (migration `0041`).
- Comando **`importar_titulos_financeiro_mongo_pg`** Ã¢â‚¬â€ default **dry-run**; `--apply` grava espelho idempotente por `mongo_id`.
- Dry-run local: **~17Ã¢â‚¬Â¯875** tÃƒÂ­tulos no Mongo (compatÃƒÂ­vel com checkpoint).
- PrÃƒÂ³ximo passo pÃƒÂ³s-deploy: `--apply` no staging Ã‚Â· depois views lerem PG quando `agro_pg`.

Fluxo **seguro** do checkpoint (sÃƒÂ³ admin vÃƒÂª os botÃƒÂµes):

1. **Backup ZIP** Ã¢â‚¬â€ CSV no PC (**sÃƒÂ³ redundÃƒÂ¢ncia**; SisVale nÃƒÂ£o importa de volta).
2. **Checkpoint** Ã¢â‚¬â€ carimbo no Mongo; **nÃƒÂ£o apaga nem muda valores**.
3. **Corte AgroÃ¢â€ â€™ERP** Ã¢â‚¬â€ apÃƒÂ³s checkpoint, SisVale **nÃƒÂ£o envia** lanÃƒÂ§amento/baixa pela API (`VendaERPAPIClient`); automÃƒÂ¡tico no cÃƒÂ³digo **v1.14**.
4. **Opcional Render** Ã¢â‚¬â€ `AGRO_FINANCEIRO_MONGO_CONGELADO=true` (reforÃƒÂ§o).

**Backup no PC** Ã¢â‚¬â€ cÃƒÂ³pia de seguranÃƒÂ§a fora do sistema. Checkpoint = carimbo dentro do SisVale.

**Corte ERP:** ÃƒÂ© a **nossa** integraÃƒÂ§ÃƒÂ£o (API aberta WL), nÃƒÂ£o ticket ao fornecedor. PadrÃƒÂ£o jÃƒÂ¡ era `AGRO_FINANCEIRO_ERP_SYNC_HABILITADO=false`; apÃƒÂ³s checkpoint o bloqueio ÃƒÂ© garantido no cÃƒÂ³digo.

**Armadilha cherry-pick:** se `lancamentos_financeiros.html` incluir `lancamentos_pin_entrada.html`, o template **tem** que ir junto Ã¢â‚¬â€ senÃƒÂ£o **500** em Contas a pagar/receber.

- **PIN LanÃƒÂ§amentos:** 1Ãƒâ€” por sessÃƒÂ£o ao entrar em qualquer tela `/lancamentos/*` (sem hub modal); navegaÃƒÂ§ÃƒÂ£o interna sem repetir; **modo descanso** (~3 min idle) pede de novo; sair para PDV/outra tela limpa a sessÃƒÂ£o.

Rotas: `backup-completo.xlsx` Ã‚Â· `backup-abertos.zip` Ã‚Â· `congelamento-status/` Ã‚Â· `congelar-pre-corte/`. Painel na entrada `/lancamentos/`.

</details>

---

## 5. VariÃƒÂ¡veis de ambiente importantes


| VariÃƒÂ¡vel                      | Efeito                                   |
| ----------------------------- | ---------------------------------------- |
| `AGRO_STAGING_READONLY`       | Bloqueia escrita Mongo no staging        |
| `AGRO_ERP_PEDIDOS_DRY_RUN`    | NÃƒÂ£o grava pedidos ERP no staging         |
| `NFC_E_*`                     | Toda config NFC-e (ver NFCE-PRODUCAO.md) |
| `AGRO_DASHBOARD_GASTOS_PLANO` | Mostra grÃƒÂ¡fico gastos por plano no BI    |
| `AGRO_RH_PLANO_SALARIO_FOLHA` | Plano Mongo do tÃƒÂ­tulo de salÃƒÂ¡rio         |
| `ALERTA_VENDAS_CRON_TOKEN`    | Token crons HTTP                         |


---

## 6. Como trabalhar com o assistente (Renan Ã¢â€ â€ Cursor)

1. **Anexar contexto:** na maioria dos chats sÃƒÂ³ descrever a tarefa (roteiro carrega sozinho). `@banana-roteiro` se quiser forÃƒÂ§ar. `@AGENTS.md` opcional (UX Ã‚Â§11, RH Ã‚Â§9, LanÃƒÂ§amentos Ã‚Â§10).
2. **Dois arquivos, papÃƒÂ©is diferentes:** banana = memÃƒÂ³ria viva (WIP, pendÃƒÂªncias, checkpoint). AGENTS = manual estÃƒÂ¡vel (nÃƒÂ£o duplicar aqui).
3. **Registro de alteraÃƒÂ§ÃƒÂµes:** **sempre** atualizar `banana.md` ao entregar mudanÃƒÂ§a no sistema (fix, feature, deploy) Ã¢â‚¬â€ CHECKPOINT + Ã‚Â§ do mÃƒÂ³dulo; commits e versÃƒÂ£o para rollback/contexto. Ver regra no topo (*Registro no banana*). Renan pode pedir *"atualize a banana"* tambÃƒÂ©m. AGENTS Ã‚Â§7 **sÃƒÂ³** se Renan pedir.
4. **Escopo:** pedir arquivos ou mÃƒÂ³dulo; assistente nÃƒÂ£o amplia sem autorizaÃƒÂ§ÃƒÂ£o.
5. **Antes de editar:** assistente deve dar **uma linha de plano**.
6. **Entrega:** um patch coeso por tarefa.
7. **Commits / teste (30/07):** ao **fechar entrega** â†’ commit na branch `teste` + **`git push origin teste` sempre** (backup GitHub, **sem** pedir). Bump de `VERSION`. Renan valida no **PC local** (`docs/TESTE-LOCAL.md`). Render free **nÃ£o** Ã© gate. **ProduÃ§Ã£o:** sÃ³ item 8, **depois** do teste local + frase/senha.
8. **ProduÃƒÂ§ÃƒÂ£o:** **nunca** push/merge/deploy na loja (Render **SistVale**) sem frase explÃƒÂ­cita **+ senha** (topo do banana) **e** sem Renan ter testado local. **2026-06-22:** assistente subiu PDVÃƒâ€”cadastro em produÃƒÂ§ÃƒÂ£o sem pedido Ã¢â‚¬â€ **nÃƒÂ£o repetir**.
9. **Modo econÃƒÂ´mico:** permanente Ã¢â‚¬â€ rule `modo-economico.mdc`; detalhe sÃƒÂ³ se Renan pedir.
10. **Cliente:** Renan usa **Chrome** (loja e local) Ã¢â‚¬â€ nÃƒÂ£o perguntar Electron vs browser.
11. **Retomar trabalho antigo:** `@banana-roteiro` + este arquivo; chats anteriores nÃƒÂ£o ficam na memÃƒÂ³ria do assistente.

---

## 7. Documentos irmÃƒÂ£os (nÃƒÂ£o repetir aqui)


| Arquivo                                 | ConteÃƒÂºdo                                                                             |
| --------------------------------------- | ------------------------------------------------------------------------------------ |
| `AGENTS.md`                             | EnciclopÃƒÂ©dia Ã¢â‚¬â€ mapa URLs, UX Ã‚Â§5Ã¢â‚¬â€œ11, changelog Ã‚Â§7. ReferÃƒÂªncia, nÃƒÂ£o anexo obrigatÃƒÂ³rio. |
| `docs/DEPLOY-AMBIENTES.md`              | Staging readonly, fluxo Git                                                          |
| `docs/NFCE-PRODUCAO.md`                 | Checklist fiscal produÃƒÂ§ÃƒÂ£o                                                            |
| `docs/ESTOQUE_AGRO_FONTE_DA_VERDADE.md` | Regra estoque                                                                        |
| `docs/CONTEXTO_SESSAO_CLIENTES_PDV.md`  | SessÃƒÂ£o clientes (histÃƒÂ³rico)                                                          |
| `docs/GUIA_ABAS_NAVEGADOR_AGRO.md`      | Abas navegador                                                                       |
| `banana-roteiro.md`                     | Fluxograma Ã¢â‚¬â€ o que ler no banana por tarefa (ler **antes** do banana)                |
| `.cursor/rules/agro-consulta.mdc`       | Regra Cursor resumida (auto-carregada)                                               |
| `.cursor/rules/modo-economico.mdc`      | Respostas curtas Ã¢â‚¬â€ permanente                                                        |


---

## 8. Linha do tempo recente (commits `teste`)

ÃƒÅ¡ltimos temas entregues (mais recente primeiro):

1. **GrÃƒÂ¡fico gastos Ã¢â‚¬â€ UX perÃƒÂ­odo simÃƒÂ©trico + split filtros/planos + 4 atalhos globais** Ã¢â‚¬â€ 2026-06-26: toolbar passado/futuro Ã‚Â· slots Postgres Ã‚Â· APIs `grafico-gastos-atalhos` (`financeiro/`).
2. **GrÃƒÂ¡fico gastos por plano (Chart.js)** Ã¢â‚¬â€ 2026-06-26: `/financeiro/grafico-gastos/` + API Mongo; modo individual (`financeiro/views.py`, `mongo_financeiro_util.py`).
2. **FAB PDV Ã¢â‚¬â€ nÃƒÂ£o cobrir modais/botÃƒÂµes** Ã¢â‚¬â€ 2026-05-21: z-index 90, hide overlay/dialog, reposicionar canto direito (`_agro_pdv_fab.html`).
2. **LanÃƒÂ§amentos CP Ã¢â‚¬â€ layout novo padrÃƒÂ£o + vista preservada + perf lista** Ã¢â‚¬â€ 2026-06-19 (CHECKPOINT).
3. **PDV wizard Ã¢â‚¬â€ GM no barras remove carrinho** Ã¢â‚¬â€ `e055761` Ã‚Â· `pdv_wizard.js`: hÃƒÂ­fen `GM1546-5S` nÃƒÂ£o remove carrinho; GM modo barcode.
4. **LanÃƒÂ§amentos Ã¢â‚¬â€ EmprÃƒÂ©stimo (entrada + pagamento)** Ã¢â‚¬â€ pseudo-plano Nova saÃƒÂ­da + lote manual (`fbccf19`).
5. **LanÃƒÂ§amentos Ã¢â‚¬â€ Nova saÃƒÂ­da (legibilidade)** Ã¢â‚¬â€ card expandido; fontes/campos maiores.
6. **Etiquetas faixa 230Ã¢â‚¬Â¦** Ã¢â‚¬â€ `5c6590a` v1.20: CODE128 interno loja.
7. **PDV legado carrinho GM** Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o `59bdedc` v1.02.

*Para lista completa:* `git log teste --oneline -50`.

---







## CHECKPOINT DE ATUALIZAÃƒâ€¡ÃƒÆ’O

### ðŸ“¦ PACOTE PRONTO â€” Contagem cÃ­clica (`AJUSTE-CICLICA` Â· **v16.14**)

> **Loja hoje:** âœ… **Live v16.07** Â· `producao` @ **262d460**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao` sem frase + senha.

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **pronto para envio Ã  produÃ§Ã£o** Â· `teste` **v16.14** |
| **O quÃª** | Ajuste Mobile **CÃ­clica**: cego Â· multi-celular (PG) Â· loja/categoria/corredor Â· pass1 â†’ recontagem â†’ grava sÃ³ no final Â· origem `contagem_ciclica` |
| **Prova** | path **VERIFY_OK** (42) Â· deep **VERIFY_DEEP_OK** (27) |
| **Migrate** | **SIM** â€” `estoque.0016_contagem_ciclica` |
| **VocÃª** | migrate 0016 Â· Ctrl+F5 `/ajuste-mobile/` Â· CÃ­clica Â· categoria pequena Â· 2 celulares |

### ðŸ“¦ PACOTE PRONTO â€” Repasse Vila â†’ Centro (`REPASSE-VILA` Â· **v16.11**)

> **Loja hoje:** âœ… **Live v16.07** Â· `producao` @ **262d460**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao` sem frase + senha.

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **pronto para envio Ã  produÃ§Ã£o** Â· `teste` **v16.11** Â· loja ainda **v16.07** |
| **O quÃª** | `/repasse-vila/` + PDV **Repasse** Â· CMV + % lucro (50) + fiado pago Vila Â· sai Vila Â· entra Centro (pendente+aviso se fechado) Â· migrate **0087** |
| **Prova** | path **VERIFY_OK** Â· deep **VERIFY_DEEP_OK** (37) Â· calc fiado fora Â· incremental zera Â· pendenteâ†’aplicado Â· APIs/tela OK |
| **Migrate** | **SIM** â€” `produtos.0087_repasse_vila_centro` |
| **Fora** | Card BI (adiado) |
| **VocÃª** | migrate 0087 Â· Ctrl+F5 Â· caixa Vila Â· Menu/PDV Repasse |

### ðŸ“¦ PACOTE PRONTO â€” DRE visual legÃ­vel (`DRE-VISUAL-LEGIVEL` Â· **v16.09**)

> **Loja hoje:** âœ… **Live v16.07** Â· `producao` @ **262d460**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao` sem frase + senha.

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **pronto para envio Ã  produÃ§Ã£o** Â· `teste` **v16.09** @ **5ae797b** Â· loja ainda **v16.07** |
| **O quÃª** | Resumo/DRE: balÃ£o no mouse Â· PE termÃ´metro Â· subtÃ­tulos KPIs Â· `?` mantido |
| **Prova** | `verify_dre_visual_path.py` **VERIFY_OK** (226) Â· unit DRE **30/30** |
| **Migrate** | **NÃƒO** |
| **VocÃª** | Ctrl+F5 Resumo |

### âœ… CHECKLIST ÃšNICO â€” pronto para envio Ã  produÃ§Ã£o (13/08)

> **Loja hoje:** âœ… **Live v16.07** Â· `producao` @ **262d460**  
> **NÃƒO** merge `teste`â†’`producao` sem frase + senha.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **AJUSTE-CICLICA** | âœ… **pronto para envio Ã  produÃ§Ã£o** / teste **v16.15** | **SIM** estoque.0016 |
| 2 | **REPASSE-VILA** | âœ… **pronto para envio Ã  produÃ§Ã£o** / teste **v16.11** | **SIM** 0087 |
| 3 | **DRE-VISUAL-LEGIVEL** | âœ… **pronto para envio Ã  produÃ§Ã£o** / teste **v16.09** @ **5ae797b** | nÃ£o |

### âœ… RevisÃ£o path â€” ENTRADA-NF-UX + PDV-PIX-SICREDI (13/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **jÃ¡ na loja** (desde lote ~v11.98) Â· loja hoje **v16.07** Â· **nada a subir** |
| **Prova** | boleto 44â†’47 OK Â· Novaâ†’reset Â· financeiro_ids Â· `pix_sicredi` no cÃ³digo `producao` |
| **Branches velhas** | `deploy/entrada-nf-ux-v11.95` / `deploy/pdv-pix-sicredi-v11.94` = **obsoletas** (nÃ£o re-cherry) |

### ðŸ“¦ PACOTE PRONTO LOJA â€” DF-e XML fora do Aguarde 1h (`DFE-XML-AGUARDE` Â· **v15.95+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **jÃ¡ Live** (lote v15.99) Â· prova refeita 13/08 Â· **nada pendente** deste path |
| **O quÃª** | Buscar lista = 1h no 137 Â· Buscar XML/chave sÃ³ trava no **656** Â· auto CiÃªncia+XML nas SÃ³ resumo Â· **Carregar na grade** manual |
| **Prova** | `python scripts/verify_dfe_xml_aguarde_path.py` â†’ **VERIFY_OK 31/31** Â· manifestaÃ§Ã£o **26/26** Â· NSU-656 **12/12** |
| **Migrate** | **NÃƒO** |
| **Fora** | PDV Â· caixa Â· distNSU (continua 1h) |
| **VocÃª** | Ctrl+F5 SEFAZ Â· Buscar Â· notas em **Carregar na grade** sem esperar 1h |

### âœ… CHECKLIST ÃšNICO (legado 13/08 â€” fila vazia)

> **SubstituÃ­do** pelo checklist **pronto para envio** com **DRE-VISUAL-LEGIVEL** no topo do CHECKPOINT. ENTRADA-NF / PIX / DFE-XML **jÃ¡ Live**.

### âœ… Deploy loja â€” lote checklist 12/08b (`deploy/lote-checklist-1208b` Â· **v16.07**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v16.07** Â· `producao` @ **262d460** Â· Render `dep-d9u8i7psrm7s73b67dhg` |
| **Pacotes** | CP-CAL-PG Â· NF-BIP-ET3 |
| **Prova** | VERIFY **31/31** Â· **40/40** (+ irmÃ£ 31/31) |
| **Migrate** | **NÃƒO** |
| **Rollback** | tag `rollback/pre-lote-checklist-1208b-v15.99` @ **ec76e89** |
| **Base anterior** | Live v15.99 @ **ec76e89** |
| **VocÃª** | **Ctrl+F5** Â· badge **v16.07** Â· calendÃ¡rio CP Â· Entrada NF etapa 3 |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (12/08 Â· loja v16.07) Â· **superado**

> Vigente: **CHECKLIST ÃšNICO â€” pronto envio (13/08)** no topo (fila vazia neste path).

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CP-CAL-PG** | âœ… enviado / Live v16.07 | nÃ£o |
| 2 | **NF-BIP-ET3** | âœ… enviado / Live v16.07 | nÃ£o |

### âœ… Deploy loja â€” lote checklist 12/08 (`deploy/lote-checklist-1208` Â· **v15.99**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.99** Â· `producao` @ **ec76e89** Â· Render `dep-d9u7bmu7bikc739ncamg` |
| **Pacotes** | COMPRAS-SLIM-DADOS Â· NF-BIP-CAD Â· DFE-XML-AGUARDE |
| **Prova** | VERIFY **41/41** Â· **31/31** Â· **26/26** |
| **Migrate** | **NÃƒO** |
| **Rollback** | tag `rollback/pre-lote-checklist-1208-v15.82` @ **5bbebbe** |
| **Base anterior** | Live v15.82 @ **5bbebbe** |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (12/08 Â· loja v15.99) Â· **superado**

> Vigente: **CHECKLIST ÃšNICO â€” pronto envio (12/08)** no topo. Loja permanece **v15.99** atÃ© o prÃ³ximo envio.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **COMPRAS-SLIM-DADOS** | âœ… enviado / v15.99 | nÃ£o |
| 2 | **NF-BIP-CAD** | âœ… enviado / v15.99 | nÃ£o |
| 3 | **DFE-XML-AGUARDE** | âœ… enviado / v15.99 | nÃ£o |

### âœ… Deploy loja â€” COMPRAS-SLIM-FORN (`deploy/compras-slim-forn-1108` Â· **v15.82**) Â· **superado**

> **Loja hoje:** âœ… **Live v15.82** Â· `producao` @ **5bbebbe** Â· Render `dep-d9tn7foae00c73bg8sb0`  
> Vigente checklist: **pronto envio (12/08)** no topo.

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.82** Â· `producao` @ **5bbebbe** |
| **O quÃª** | slim v4 com fornecedor Â· cache v4 |
| **Rollback** | tag `rollback/pre-compras-slim-forn-v15.77` @ `b226e66` |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (11/08 Â· loja v15.82) Â· **superado**

> Vigente: **CHECKLIST ÃšNICO â€” pronto envio (12/08)** no topo. Loja permanece **v15.82** atÃ© o prÃ³ximo envio.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **COMPRAS-SLIM-FORN** | âœ… enviado / Live v15.82 | nÃ£o |

### ðŸš€ PREP deploy loja â€” COMPRAS-SLIM-FORN (`deploy/compras-slim-forn-1108` Â· **v15.82**) Â· **superado**

> Vigente: **Deploy loja â€” COMPRAS-SLIM-FORN Live v15.82** no topo.

### âœ… Deploy loja â€” COMPRAS-SLIM-FALLBACK (`deploy/compras-slim-1108` Â· **v15.77**)

> **Loja hoje:** âœ… **Live v15.77** Â· `producao` @ **b226e66** Â· Render `dep-d9tn2heq1p3s73b2ai9g`  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **COMPRAS-SLIM-FALLBACK** | âœ… enviado / Live v15.77 | nÃ£o |

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.77** Â· `producao` @ **b226e66** Â· base era `75288b5` (v15.68) |
| **Branch** | `deploy/compras-slim-1108` (FF â†’ `producao`) |
| **O quÃª** | `/compras/` fallback slim com freio catalogo-full-off Â· cache v3 |
| **Arquivos** | sÃ³ compras.html + mobile_ajuste.html + VERSION |
| **Prova** | **44/44** Â· sem migrate |
| **Rollback** | tag `rollback/pre-compras-slim-v15.68` @ `75288b5` Â· frase+senha |
| **VocÃª agora** | **Ctrl+F5** `/compras/` Â· badge **v15.77** Â· digitar 2 letras |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (11/08 Â· loja v15.77)

> **Loja hoje:** âœ… **Live v15.77** Â· `producao` @ **b226e66**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **COMPRAS-SLIM-FALLBACK** | âœ… enviado / Live v15.77 | nÃ£o |

### ðŸš€ PREP deploy loja â€” COMPRAS-SLIM-FALLBACK (`deploy/compras-slim-1108` Â· **v15.77**) Â· **superado**

> Vigente: **Deploy loja â€” COMPRAS-SLIM-FALLBACK Live v15.77** no topo.

### âœ… Deploy loja â€” FOLHA-FAMILIA (`deploy/folha-familia-1108` Â· **v15.68**) Â· **superado**

> **Loja hoje:** âœ… **Live v15.68** Â· `producao` @ **75288b5** Â· Render `dep-d9tmfcoae00c73bfb9o0`  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **FOLHA-FAMILIA** | âœ… enviado / Live v15.68 | nÃ£o |

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.68** Â· `producao` @ **75288b5** Â· base era `e0d19a3` (v15.62) |
| **Branch** | `deploy/folha-familia-1108` (FF â†’ `producao`) |
| **O quÃª** | Folha: granel some; venda Ã— fator soma no saco Â· decimal |
| **Prova** | **42/42** Â· sem migrate |
| **Rollback** | tag `rollback/pre-folha-familia-v15.62` @ `e0d19a3` Â· frase+senha |
| **VocÃª agora** | **Ctrl+F5** Â· badge **v15.68** Â· Folha ADIMAX Â· granel fora Â· saco com fraÃ§Ã£o |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (11/08 Â· loja v15.68)

> **Loja hoje:** âœ… **Live v15.68** Â· `producao` @ **75288b5**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **FOLHA-FAMILIA** | âœ… enviado / Live v15.68 | nÃ£o |

### ðŸš€ PREP deploy loja â€” FOLHA-FAMILIA (`deploy/folha-familia-1108` Â· **v15.68**) Â· **superado**

> Vigente: **Deploy loja â€” FOLHA-FAMILIA Live v15.68** no topo.

### âœ… Deploy loja â€” lote checklist 11/08 (`deploy/lote-checklist-1108` Â· **v15.62**)

> **Loja hoje:** âœ… **Live v15.62** Â· `producao` @ **e0d19a3** Â· Render `dep-d9tm20hsrm7s73alh9g0`  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **BI-TOPBAR-COMPACT** | âœ… enviado / Live v15.62 | nÃ£o |
| 2 | **FIADO-RECIBO** | âœ… enviado / Live v15.62 | nÃ£o |
| 3 | **FOLHA-FORN-HIST** | âœ… enviado / Live v15.62 | nÃ£o |

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.62** Â· `producao` @ **e0d19a3** Â· base era `1f50976` (v15.55) |
| **Rollback** | tag `rollback/pre-lote-checklist-1108-v15.55` @ `1f50976` |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (11/08 Â· loja v15.62) Â· **superado**

> Vigente: **CHECKLIST ÃšNICO â€” pronto envio (11/08 Â· loja v15.62)** no topo.

### ðŸ› FIX â€” Folha Compras por fornecedor (`FOLHA-FORN-HIST` Â· **teste v15.61**) Â· VERIFY_OK

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` Â· prova **28/28** Â· no checklist envio |
| **Commit** | `9e0efc2` |
| **Sintoma** | Folha (PDV + Compras) parecia sÃ³ o **Ãºltimo pedido** (ex. ADIMAX) |
| **Fix** | Cadastro **âˆª** histÃ³rico Entrada NF Agro Â· 1Âº token do nome Â· prÃ©-filtro NF id **ou** nome |
| **Migrate** | **NÃƒO** |

### âœ… Deploy loja â€” filtro loja DRE + BI (`deploy/dre-loja-filtro-1008` Â· **v15.55**)

> **Loja hoje:** âœ… **Live v15.55** Â· `producao` @ **1f50976**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.55** Â· `producao` @ **1f50976** Â· base era `4bf3410` (v15.54) |
| **Pacote** | **DRE-LOJA-FILTRO** |
| **O quÃª** | DRE + BI: **Centro + Vila** (padrÃ£o) Â· Centro Â· Vila. Vendas/CMV = PDV. Despesas/emp. = empresa (Vila sem cadastro â†’ Centro). |
| **Rollback** | `git push origin 4bf3410:producao` ou tag `rollback/pre-dre-loja-filtro-v15.54` |
| **Migrate** | **NÃƒO** |
| **Fora** | PDV Â· caixa Â· wizard Â· Indicadores HTML |
| **VocÃª** | Ctrl+F5 DRE + BI `/` Â· badge **v15.55** Â· filtro **Loja** / **NÃºmeros** Â· padrÃ£o as duas |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (10/08 Â· loja v15.55) Â· **superado**

> **Vigente:** **CHECKLIST ÃšNICO â€” pronto envio (10/08)** no topo. Loja permanece **v15.55** atÃ© o prÃ³ximo envio.

### âœ… Deploy loja â€” DRE-EMP-CARD (`deploy/dre-emp-card-1008` Â· **v15.54**)

> **Loja hoje:** âœ… **Live v15.54** Â· `producao` @ **4bf3410**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.54** Â· `producao` @ **4bf3410** Â· base era `674901d` (v15.52) |
| **Pacote** | **DRE-EMP-CARD** |
| **O quÃª** | Card EmprÃ©stimos (devido bruto + juros + total + pago + emprestado) Â· Mini DRE **Saldo final = soma** Â· frases no **Â«?Â»** Â· rÃ³tulos sem quebra |
| **Rollback** | `git push origin 674901d:producao` ou tag `rollback/pre-dre-emp-card-v15.52` |
| **Migrate** | **NÃƒO** |
| **Fora** | PDV Â· caixa Â· wizard Â· Indicadores HTML |
| **VocÃª** | Ctrl+F5 DRE Â· badge **v15.54** Â· Jul/2026 Centro vencimento: emp. devido ~42.601 Â· juros ~4.658 |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (10/08 Â· loja v15.54) Â· **superado**

> **Vigente:** **CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (10/08 Â· loja v15.55)** no topo.

### âœ… Deploy loja â€” DRE cadastro oficial de planos (`deploy/dre-planos-cadastro-1008` Â· **v15.52**)

> **Loja hoje:** âœ… **Live v15.52** Â· `producao` @ **674901d**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.52** Â· `producao` @ **674901d** Â· base era `2f6d05e` (v15.50) |
| **Pacote** | **DRE-PLANOS-CADASTRO** |
| **Rollback** | `git push origin 2f6d05e:producao` ou tag `rollback/pre-dre-planos-cadastro-v15.50` |
| **Migrate** | **NÃƒO** |
| **Fora** | PDV Â· caixa Â· wizard Â· Indicadores HTML |
| **VocÃª** | Ctrl+F5 DRE Â· badge **v15.52** Â· despesas com nome oficial do cadastro |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (10/08 Â· loja v15.52)

> **Loja hoje:** âœ… **Live v15.52** Â· `producao` @ **674901d**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **DRE-PLANOS-CADASTRO** | âœ… enviado / Live v15.52 | nÃ£o |

### âœ… Deploy loja â€” DRE visual polish (`deploy/dre-visual-polish-0908` Â· **v15.50**)

> **Loja hoje:** âœ… **Live v15.50** Â· `producao` @ **2f6d05e**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.50** Â· `producao` @ **2f6d05e** Â· base era `9b212b0` (v15.46) |
| **Pacote** | **DRE-VISUAL-POLISH** |
| **Rollback** | `git push origin 9b212b0:producao` ou tag `rollback/pre-dre-visual-polish-v15.46` |
| **Migrate** | **NÃƒO** |
| **Fora** | PDV Â· caixa Â· wizard Â· Indicadores HTML |
| **VocÃª** | Ctrl+F5 DRE Â· badge **v15.50** Â· comparativos Â· Mini DRE juros/emprÃ©stimo Â· Saldo final |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (09/08 Â· loja v15.50)

> **Loja hoje:** âœ… **Live v15.50** Â· `producao` @ **2f6d05e**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **DRE-VISUAL-POLISH** | âœ… enviado / Live v15.50 | nÃ£o |

### ðŸ“¦ PACOTE PRONTO â€” DRE visual polish (`DRE-VISUAL-POLISH` Â· **v15.50**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.50** Â· `producao` @ **2f6d05e** |
| **O quÃª** | PE barras modernas Â· despesas por categoria no recorte Â· Mini DRE juros + emprÃ©stimo + **Saldo final** Â· cards **vs mÃªs passado** + **vs mÃ©dia 90d** |
| **Prova** | tests **34/34** Â· visual **191/191** Â· CMV **55/55** Â· PDV-DRE **33/33** Â· RG **79/79** |
| **Migrate** | **NÃƒO** |
| **Fora** | Indicadores HTML Â· API `geracao_caixa` Â· PDV/caixa |

### âœ… Deploy loja â€” lote UX+DRE+BI (`deploy/lote-ux-dre-bi-0908` Â· **v15.46**)

> **Loja hoje:** âœ… **Live v15.46** Â· `producao` @ **9b212b0**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.46** Â· `producao` @ **9b212b0** Â· base era `ebe9e9c` (v15.26) |
| **Pacotes** | **PDV-RACOES-LISTA-UX** + **DRE-VISUAL-PREVIA** + **BI-LUCRO-LIQUIDO** |
| **Rollback** | `git push origin ebe9e9c:producao` ou tag `rollback/pre-lote-ux-dre-bi-v15.26` |
| **Migrate** | **NÃƒO** |
| **Fora** | caixa, checkout, wizard wholesale, relatÃ³rios WIP, Indicadores HTML |
| **VocÃª** | Ctrl+F5 Â· BI `/` Lucro LÃ­quido Â· DRE visual Â· PDV RaÃ§Ãµes lista (foto+zebra) Â· badge **v15.46** |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (09/08 Â· loja v15.46)

> **Loja hoje:** âœ… **Live v15.46** Â· `producao` @ **9b212b0**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-RACOES-LISTA-UX** | âœ… enviado / Live v15.46 | nÃ£o |
| 2 | **DRE-VISUAL-PREVIA** | âœ… enviado / Live v15.46 | nÃ£o |
| 3 | **BI-LUCRO-LIQUIDO** | âœ… enviado / Live v15.46 | nÃ£o |

### ðŸ“¦ PACOTE PRONTO â€” BI Lucro LÃ­quido (`BI-LUCRO-LIQUIDO` Â· **v15.42**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.46** Â· `producao` @ **9b212b0** |
| **O quÃª** | BI `/` Â· card **Lucro LÃ­quido** no lugar de Novos Clientes Â· **vencimento** Â· Bruto + Pago Â· mesma conta do Resumo. Vila sem empresa prÃ³pria usa Agro Mais Centro. |
| **VocÃª** | Ctrl+F5 `/` Â· datas 12/07â€“10/08 Â· Bruto = **-R$ 2.480,17** Â· Pago = **R$ 1,29** |
| **Prova** | verify **73/73** Â· tests **4/4** Â· live PG = Indicadores |
| **Migrate** | **NÃƒO** |

### ðŸ“¦ PACOTE PRONTO â€” Lista RaÃ§Ãµes foto + zebra (`PDV-RACOES-LISTA-UX`)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.46** Â· `producao` @ **9b212b0** |
| **O quÃª** | Foto miniatura (clique abre grande) Â· â€œNo carrinhoâ€ junto do Adicionar Â· sem coluna Carrinho Â· linha **zebra cinza fraca** (sem cor da marca) |
| **Prova** | verify RaÃ§Ãµes **129/129** Â· tests **17/17** Â· `node --check` |
| **VocÃª** | Ctrl+F5 PDV â†’ RaÃ§Ãµes â†’ lista |
| **Migrate** | **NÃƒO** |

### ðŸ“¦ PACOTE PRONTO â€” DRE visual prÃ©via (`DRE-VISUAL-PREVIA` Â· **v15.46**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.46** Â· `producao` @ **9b212b0** |
| **O quÃª** | `/financeiro/resumo-gerencial/` â€” 16:9 Â· donut despesas + receita por categoria PDV Â· PE linha Â· mini DRE Â· emprÃ©stimos no filtro (nÃ£o total) Â· sem Estoque Â· grade sem sobrepor. Indicadores intacto. |
| **VocÃª** | Ctrl+F5 DRE Â· 1 dia â†’ devido â‰  total Â· cards sem tapar Â· zoom Aa |
| **Prova** | tests DRE+CMV **27/27** Â· verify visual **157/157** |
| **Migrate** | **NÃƒO** |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (09/08 Â· loja v15.26)

> **Loja hoje:** âœ… **Live v15.26** Â· `producao` @ **ebe9e9c** Â· Render `dep-d9sh2egae00c73anribg`  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-RACOES-LISTA-DENSE** | âœ… enviado / Live v15.26 | nÃ£o |
| 2 | **RG-CMV-TOGGLE** | âœ… enviado / Live v15.26 | nÃ£o |

### âœ… Deploy loja â€” lote DENSE + Resumo CMV (`deploy/lote-dense-rg-0908` Â· **v15.26**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.26** Â· `producao` @ **ebe9e9c** Â· Render `dep-d9sh2egae00c73anribg` Â· base era `5fc0ee8` (v15.24) |
| **Pacotes** | **PDV-RACOES-LISTA-DENSE** + **RG-CMV-TOGGLE** |
| **Fora** | resto do `teste` Â· **NÃƒO** merge `teste`â†’`producao` |
| **Migrate** | **NÃƒO** |
| **O quÃª** | Lista RaÃ§Ãµes compacta (sem quebra) + Resumo CMV vendida Ã— paga |
| **Provas** | teste **32/32** Â· worktree **32/32** Â· verify RaÃ§Ãµes **VERIFY_OK** Â· RG **78/78** Â· DRE **55/55** Â· PDV-DRE **33/33** |
| **VocÃª** | **Ctrl+F5** PDV â†’ RaÃ§Ãµes â†’ lista Â· Resumo gerencial â†’ julho Centro â†’ trocar os dois botÃµes |
| **Rollback** | tag `rollback/pre-lote-dense-rg-v15.24` @ `5fc0ee8` Â· branch `producao-backup-pre-v1526-dense-rg-20260809` Â· frase+senha |

### ðŸ“¦ PACOTE PRONTO â€” Lista RaÃ§Ãµes sem quebra (`PDV-RACOES-LISTA-DENSE` Â· **v15.26**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.26** Â· `producao` @ **ebe9e9c** |
| **O quÃª** | Lista numa linha sÃ³ (sem quebra em tamanho / botÃ£o / carrinho). Overlay um pouco maior. Zoom Agro **nÃ£o** muda. |
| **VocÃª** | Ctrl+F5 PDV â†’ RaÃ§Ãµes â†’ lista â†’ deve caber mais produtos |
| **Prova** | `tests_pdv_racoes` **16/16** Â· verify path lista+dense **VERIFY_OK** Â· `node --check` Â· `manage.py check` |
| **Migrate** | **NÃƒO** |

### ðŸ“¦ PACOTE PRONTO â€” Resumo CMV vendida Ã— paga (`RG-CMV-TOGGLE` Â· **v15.25**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.26** Â· `producao` @ **ebe9e9c** |
| **O quÃª** | `/financeiro/resumo-gerencial/` â€” botÃ£o **Mercadoria vendida** Ã— **Mercadoria paga** (igual Indicadores). Lucro, operacional, lÃ­quido e PE acompanham. Caixa nÃ£o muda. Markup % no card lucro bruto. |
| **VocÃª** | Ctrl+F5 Resumo gerencial â†’ julho Centro â†’ trocar os dois botÃµes |
| **Prova** | `tests_receita_pdv_dre` **16/16** Â· verify RG **78/78** Â· verify DRE **33/33** Â· verify CMV **55/55** Â· `node --check` Â· `manage.py check` |
| **Migrate** | **NÃƒO** |

### âœ… InauguraÃ§Ã£o Vila â€” 5% encerrado (09/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **encerrado** Â· janela era **07â€“08/08** Â· hoje **09/08** o 5% **jÃ¡ desliga sozinho** |
| **DecisÃ£o** | **Deixar o cÃ³digo** (nÃ£o apagar, nÃ£o deploy) |
| **Por quÃª** | Remover agora = risco no PDV sem ganho. Faixa sÃ³ liga na data. **CAMP-PROMO-MENOR** (promo da loja certa + GM) **fica** â€” Ã© correÃ§Ã£o permanente. |
| **Loja** | âœ… **Live v15.05** Â· sem faixa laranja Â· promoÃ§Ãµes normais |
| **Se 5% ainda aparecer** | Ctrl+F5 Â· se persistir: `AGRO_CAMPANHA_INAUGURACAO_OFF=1` + restart |
| **PrÃ³xima inauguraÃ§Ã£o** | Reusar o mesmo mÃ³dulo Â· sÃ³ mudar datas |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (09/08 Â· loja v15.24)

> **Loja hoje:** âœ… **Live v15.24** Â· `producao` @ **5fc0ee8** Â· Render `dep-d9sg5g37uimc73bncm1g`  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-RACOES-LISTA** | âœ… enviado / Live v15.24 | nÃ£o |

### âœ… Deploy loja â€” RaÃ§Ãµes lista (`deploy/pdv-racoes-lista` Â· **v15.24**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.24** Â· `producao` @ **5fc0ee8** Â· Render `dep-d9sg5g37uimc73bncm1g` Â· base era `2116b41` (v15.23) |
| **Pacotes** | **PDV-RACOES-LISTA** |
| **Fora** | resto do `teste` Â· **NÃƒO** merge `teste`â†’`producao` |
| **Migrate** | **NÃƒO** |
| **O quÃª** | Depois do tamanho: overlay grande, baratoâ†’caro, Adicionar / Adicionar todas / Fechar. Linha verde. Esc fecha. |
| **Provas** | `tests_pdv_racoes` **16/16** Â· verify **VERIFY_OK** Â· `node --check` Â· `manage.py check` |
| **VocÃª** | **Ctrl+F5** PDV â†’ RaÃ§Ãµes â†’ tipo â†’ marca â†’ tamanho â†’ lista â†’ Adicionar |
| **Rollback** | tag `rollback/pre-pdv-racoes-lista-v15.23` @ `2116b41` Â· branch `producao-backup-pre-v1524-racoes-lista-20260809` Â· frase+senha |

### ðŸ“¦ PACOTE PRONTO â€” RaÃ§Ãµes escolhe na lista (`PDV-RACOES-LISTA` Â· **v15.24**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.24** Â· `producao` @ **5fc0ee8** |
| **O quÃª** | Depois do tamanho: overlay grande, baratoâ†’caro, Adicionar / Adicionar todas / Fechar. Linha verde. Esc fecha. NÃ£o vai direto ao carrinho. |
| **VocÃª** | Ctrl+F5 PDV â†’ RaÃ§Ãµes â†’ tipo â†’ marca â†’ tamanho â†’ lista â†’ Adicionar |
| **Prova** | `tests_pdv_racoes` **16/16** Â· verify **VERIFY_OK** Â· `node --check` Â· `manage.py check` |
| **Migrate** | **NÃƒO** |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (09/08 Â· loja v15.23)

> **Loja hoje:** âœ… **Live v15.23** Â· `producao` @ **2116b41** Â· Render `dep-d9sflrgu01pc73e5vet0`  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **DRE-CMV-TOGGLE** | âœ… enviado / Live v15.23 | nÃ£o |

### âœ… Deploy loja â€” DRE CMV vendida Ã— paga (`deploy/dre-cmv-toggle-0908` Â· **v15.23**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.23** Â· `producao` @ **2116b41** Â· Render `dep-d9sflrgu01pc73e5vet0` Â· base era `759e435` (v15.20) |
| **Pacotes** | **DRE-CMV-TOGGLE** |
| **Fora** | resto do `teste` Â· **NÃƒO** merge `teste`â†’`producao` |
| **Migrate** | **NÃƒO** |
| **O quÃª** | Indicadores DRE: botÃ£o **Mercadoria vendida** Ã— **Mercadoria paga**. Lucro/margem/lÃ­quido acompanham. Caixa nÃ£o muda. |
| **Provas** | `tests_receita_pdv_dre` **12/12** Â· `tests_pdv_racoes` **15/15** Â· path **47/47** Â· verify DRE **24/24** Â· `manage.py check` |
| **VocÃª** | **Ctrl+F5** Indicadores â†’ DRE â†’ trocar os dois botÃµes |
| **Rollback** | tag `rollback/pre-dre-cmv-toggle-v15.20` @ `759e435` Â· branch `producao-backup-pre-v1521-dre-cmv-20260809` Â· frase+senha |

### ðŸ“¦ PACOTE PRONTO â€” DRE CMV vendida Ã— paga (`DRE-CMV-TOGGLE` Â· **v15.23**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.23** Â· `producao` @ **2116b41** |
| **O quÃª** | Indicadores DRE: botÃ£o **Mercadoria vendida** (custo Ã— qtd) ou **Mercadoria paga** (lanÃ§amentos). Lucro, margem e lÃ­quido acompanham. Caixa nÃ£o muda. PadrÃ£o = vendida. Se CMV vendida falhar, fica na paga. |
| **VocÃª** | Ctrl+F5 Indicadores â†’ DRE â†’ trocar os dois botÃµes |
| **Prova** | `tests_receita_pdv_dre` **12/12** Â· path **47/47 VERIFY_OK** |
| **Migrate** | **NÃƒO** |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (09/08 Â· loja v15.20)

> **Loja hoje:** âœ… **Live v15.20** Â· `producao` @ **759e435**  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-RACOES-FIX** | âœ… enviado / Live v15.20 | nÃ£o |
| 2 | **PDV-RACOES-25** | âœ… enviado / Live v15.20 | nÃ£o |

### âœ… Deploy loja â€” RaÃ§Ãµes fix + saco 2,5 kg (`deploy/pdv-racoes-fix-25` Â· **v15.20**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.20** Â· `producao` @ **759e435** Â· Render `dep-d9s9pujl550s73e4dc10` Â· base era `42faf6c` (v15.16) |
| **Pacotes** | **PDV-RACOES-FIX** + **PDV-RACOES-25** |
| **Fora** | resto do `teste` Â· **NÃƒO** merge `teste`â†’`producao` |
| **Migrate** | **NÃƒO** |
| **O quÃª** | BotÃ£o RaÃ§Ãµes lÃª cadastro na hora + saco **2,5 kg** |
| **Provas** | `tests_pdv_racoes` **15/15** Â· verify **VERIFY_OK** Â· `manage.py check` |
| **VocÃª** | **Ctrl+F5** PDV â†’ RaÃ§Ãµes â†’ **CÃ£o adulto** â†’ Origens Â· Peso `2,5` no cadastro |
| **Rollback** | tag `rollback/pre-pdv-racoes-fix-25-v15.16` @ `42faf6c` Â· branch `producao-backup-pre-v1520-racoes-20260809` Â· frase+senha |

### ðŸ“¦ PACOTE PRONTO â€” RaÃ§Ãµes lÃª cadastro ao vivo (`PDV-RACOES-FIX` Â· **v15.20**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.20** Â· `producao` @ **759e435** |
| **O quÃª** | BotÃ£o RaÃ§Ãµes puxa Categoria/Sub 1/Sub 2/Peso do Agro na hora (nÃ£o usa lista velha do PDV) |
| **VocÃª** | Ctrl+F5 no PDV â†’ RaÃ§Ãµes â†’ **CÃ£o adulto** â†’ deve aparecer Origens |
| **Prova** | `tests_pdv_racoes` **15/15** Â· verify path Origens+2,5 **VERIFY_OK** |
| **Migrate** | **NÃƒO** |

### ðŸ“¦ PACOTE PRONTO â€” RaÃ§Ãµes saco 2,5 kg (`PDV-RACOES-25` Â· **v15.18**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.20** Â· `producao` @ **759e435** |
| **O quÃª** | BotÃ£o RaÃ§Ãµes: tamanho **Saco 2,5 kg** Â· cadastro Peso (etiqueta) = **`2,5`** |
| **Cadastro** | SÃ³ `2,5` (aceita `2.5` / `2,50 kg`) Â· **nÃ£o** vira 25 kg nem pacote |
| **Prova** | `tests_pdv_racoes` **15/15** Â· verify path 2,5 **VERIFY_OK** Â· `manage.py check` |
| **Migrate** | **NÃƒO** |

### âœ… Deploy loja â€” lote checklist 09/08 (`deploy/lote-checklist-0908` Â· **v15.16**)

> **Loja hoje:** âœ… **Live v15.16** Â· `producao` @ **42faf6c** Â· Render `dep-d9s5ogjbc2fs73b36p8g`  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **DRE-PDV-RECEITA** | âœ… enviado / Live v15.16 | nÃ£o |
| 2 | **RG-AJUDA-MODAL** | âœ… enviado / Live v15.16 | nÃ£o |
| 3 | **PDV-RACOES** | âœ… enviado / Live v15.16 | nÃ£o |

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.16** Â· `producao` @ **42faf6c** |
| **Branch** | `deploy/lote-checklist-0908` (FF â†’ `producao`) Â· base era `9c45d74` (v15.05) |
| **Pacotes** | DRE-PDV-RECEITA Â· RG-AJUDA-MODAL Â· PDV-RACOES |
| **Fora** | resto do `teste` Â· **NÃƒO** merge `teste`â†’`producao` |
| **Migrate** | **NÃƒO** |
| **Provas** | tests **20/20** Â· verify DRE **15/15** Â· verify RaÃ§Ãµes **VERIFY_OK** Â· `manage.py check` |
| **Rollback** | tag `rollback/pre-lote-checklist-0908-v15.05` @ `9c45d74` Â· frase+senha |
| **VocÃª agora** | **Ctrl+F5** PDV (botÃ£o RaÃ§Ãµes) Â· Indicadores (receita â‰ˆ card PDV) Â· Resumo (ajuda sÃ³ no ?) |

### ðŸ“¦ PACOTE PRONTO LOJA â€” DRE usa venda do PDV (`DRE-PDV-RECEITA` Â· **v15.14**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.16** Â· `producao` @ **42faf6c** |
| **O quÃª** | Indicadores + Resumo: **receita operacional = faturamento PDV** (loja da empresa) Â· lÃ­quido recalcula Â· CR sÃ³ conferÃªncia Â· grupo soma cada loja (nÃ£o â€œtodasâ€) Â· caixa nÃ£o mistura |
| **Prova** | `tests_receita_pdv_dre` **7/7** Â· verify **VERIFY_OK** Â· `manage.py check` |
| **Migrate** | **NÃƒO** |
| **VocÃª** | Ctrl+F5 Indicadores Â· mÃªs atÃ© hoje Â· receita â‰ˆ card PDV Â· lÃ­quido deixa de ser âˆ’10 mil |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Resumo aviso ajuda (`RG-AJUDA-MODAL` Â· **v15.10**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.16** Â· `producao` @ **42faf6c** |
| **O quÃª** | `/financeiro/resumo-gerencial/` â€” modal ajuda sÃ³ no **?** |
| **Commit** | `9eabf62` |
| **Migrate** | **NÃƒO** |

### ðŸ“¦ PACOTE PRONTO â€” PDV atalho RaÃ§Ãµes (`PDV-RACOES` Â· **v15.11+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.16** Â· `producao` @ **42faf6c** |
| **O quÃª** | BotÃ£o **RaÃ§Ãµes** no PDV â†’ tipo â†’ marca (ou Todas) â†’ tamanho (ou Todos) â†’ carrinho |
| **Cadastro** | Cat. `RaÃ§Ãµes` Â· Sub 1 `CÃ£o`/`Gato` Â· Sub 2 + Peso `1`/`2,5`/`5`/`10`/`15`/`20`/`25`/`pacote` |
| **CatÃ¡logo** | Cache PDV **v3** Â· 1Âª abertura do dia reconstrÃ³i |
| **VocÃª** | Ctrl+F5 PDV Â· testar EstimaÃ§Ã£o 15 kg em CÃ£o adulto |
| **Prova** | `tests_pdv_racoes` **13/13** Â· verify path cadastroâ†’overlayâ†’catÃ¡logoâ†’JSâ†’carrinho **VERIFY_OK** (loja + teste) Â· `manage.py check` |
| **Migrate** | **NÃƒO** |
| **Commit** | `251d679` + verify |

### âœ… Deploy loja â€” CAD-XLSX-COLS (`deploy/cad-xlsx-cols` Â· **v15.05**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.05** Â· `producao` @ **9c45d74** Â· Render `dep-d9s5cum417fc73aov3fg` |
| **Loja hoje** | âœ… **Live v15.05** Â· `producao` @ **9c45d74** |
| **Branch** | `deploy/cad-xlsx-cols` (FF â†’ `producao`) Â· base era `14dc51a` (v15.04) |
| **Pacote** | **CAD-XLSX-COLS** sÃ³ |
| **Fora** | resto do `teste` Â· **NÃƒO** merge `teste`â†’`producao` Â· **NÃƒO** cherry `19c09bf`/`355b76d` |
| **Migrate** | **NÃƒO** |
| **O quÃª** | Excel Cadastro: checkboxes Sub 2â€“4, Unidade, Modelo, **Peso** Â· gravar `peso_etiqueta` Â· cÃ©lula vazia nÃ£o altera |
| **Provas** | `tests_cadastro_planilha_peso` **7/7** Â· verify **20/20** Â· `manage.py check` |
| **VocÃª** | Cadastro â†’ Ctrl+F5 â†’ Excel â†“ â†’ marcar Sub 2â€“4 / Unidade / Modelo / Peso |
| **Rollback** | tag `rollback/pre-cad-xlsx-cols-v15.04` @ `14dc51a` Â· branch `producao-backup-pre-v1505-cad-xlsx-20260809` Â· frase+senha |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (09/08 Â· loja v15.16) Â· **superado**

> **Vigente:** loja **Live v15.20**. `producao` @ **759e435**.  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **DRE-PDV-RECEITA** | âœ… enviado / Live v15.16 | nÃ£o |
| 2 | **RG-AJUDA-MODAL** | âœ… enviado / Live v15.16 | nÃ£o |
| 3 | **PDV-RACOES** | âœ… enviado / Live v15.16 | nÃ£o |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (09/08 Â· loja v15.05) Â· **superado**

> **Vigente:** loja **Live v15.16**. `producao` @ **42faf6c**.  
> **âš ï¸** **NÃƒO** merge `teste`â†’`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CAD-XLSX-COLS** | âœ… enviado / Live v15.05 | nÃ£o |

### CAD-XLSX-COLS â€” Excel cadastro (loja Â· 09/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.05** Â· `9c45d74` |
| **O quÃª** | Modal Excel â†“ mostra Sub 2â€“4, Unidade, Modelo, Peso Â· import grava Peso |
| **Regra** | CÃ©lula vazia **nÃ£o** altera Â· baixar Excel de novo |
| **Rollback** | `rollback/pre-cad-xlsx-cols-v15.04` â†’ v15.04 |
| **Migrate** | **NÃƒO** |

### âœ… Deploy loja â€” CAMP-PROMO-MENOR (`deploy/camp-promo-menor-0808` Â· **v15.04**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.04** Â· `producao` @ **14dc51a** Â· Render `dep-d9ra30bbc2fs73ahkii0` |
| **Loja hoje** | âœ… **Live v15.04** Â· `producao` @ **14dc51a** |
| **Branch** | `deploy/camp-promo-menor-0808` (FF â†’ `producao`) Â· base era `290e8b2` (v15.03) |
| **Pacote** | **CAMP-PROMO-MENOR** sÃ³ |
| **Fora** | resto do `teste` Â· GG-UX Â· **NÃƒO** merge `teste`â†’`producao` |
| **Migrate** | **NÃƒO** |
| **O quÃª** | PDV Vila pedia promo da **Centro** â†’ valor direto 54,90 nÃ£o entrava Â· 5% sozinho (60â†’57). Fix: pede promo da loja do caixa Â· casa pelo GM Â· recarrega ao voltar/salvar Â· cache v2 |
| **Provas** | `tests_promocoes_busca` **8/8** Â· `tests_campanha_pdv` **15/15** Â· verify campanha **45/45** Â· verify promo busca **54/54** Â· `manage.py check` OK |
| **VocÃª** | InauguraÃ§Ã£o **passou** Â· 5% off. Promo valor direto segue valendo (fix permanente). |
| **Kill** | sÃ³ se 5% ainda aparecer: `AGRO_CAMPANHA_INAUGURACAO_OFF=1` + restart |
| **Rollback** | tag `rollback/pre-camp-promo-menor-0808-v15.03` @ `290e8b2` |

### ðŸ“¦ PACOTE PRONTO â€” Promo valor direto prevalece no 5% Vila (`CAMP-PROMO-MENOR`)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.04** Â· `producao` @ **14dc51a** |
| **Sintoma** | Farelo GM1507-30: lista **R$ 60** Â· valor direto **R$ 54,90** Â· PDV Vila cobrou **R$ 57** (sÃ³ 5%) |
| **Causa** | Promo **estava gravada** (PG id 9, Vila, 07â€“08, 54,90). PDV da loja carrega promo sÃ³ da **Centro**. Campanha 5% rodava sozinha. |
| **Fix** | Recarrega promo ao voltar no PDV / salvar em outra aba Â· casa tambÃ©m pelo **GM** Â· cache v2 Â· caixa Vila manda na empresa da promo |
| **Prova** | `tests_promocoes_busca` **8/8** Â· `tests_campanha_pdv` **15/15** Â· verify campanha **45/45** Â· verify promo busca **54/54** |
| **Migrate** | **NÃƒO** |
| **VocÃª agora** | **Ctrl+F5** PDV Vila â†’ farelo = **R$ 54,90**. |

### âœ… Deploy loja â€” lote checklist 07/08 (`deploy/lote-checklist-0708` Â· **v15.03**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado / Live v15.03** Â· `producao` @ **290e8b2** Â· Render `dep-d9r9hne417fc73a776pg` |
| **Branch** | `deploy/lote-checklist-0708` (FF â†’ `producao`) Â· base era `e68187b` (v14.85) |
| **Pacotes** | CAMP-VILA-5 Â· PROMO-BUSCA-PG |
| **Fora** | GG-UX (P2,5) |
| **Migrate** | **NÃƒO** |
| **ApÃ³s Live** | Ctrl+F5 PDV **Vila** + **Centro** Â· PromoÃ§Ãµes â†’ Nova â†’ etapa 2 (GM/nome) |
| **Provas** | campanha **15/15** + verify **35/35** Â· promo busca **5/5** + verify **53/53** Â· `manage.py check` OK |
| **Risco loja aberta** | MÃ©dio **07 e 08/08 Vila** (5% no PDV) Â· **09/08 volta ao normal sozinho** Â· Centro sem faixa Â· busca promo sÃ³ tela PromoÃ§Ãµes |
| **Kill** | `AGRO_CAMPANHA_INAUGURACAO_OFF=1` + restart Render Â· **nÃ£o** ligar TEST na loja |
| **Rollback** | tag `rollback/pre-lote-checklist-0708-v14.85` @ `e68187b` |
| **âš ï¸** | **NÃƒO** merge `teste`â†’`producao` Â· sÃ³ esta branch Â· sem migrate |
| **VocÃª agora** | **Ctrl+F5** PDV Vila + Centro Â· PromoÃ§Ãµes etapa 2 |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (08/08 Â· loja v15.04) Â· **superado**

> **Vigente:** **CHECKLIST ÃšNICO â€” pronto envio (09/08)** no topo. Loja permanece **v15.04** atÃ© o prÃ³ximo envio.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CAMP-VILA-5** | âœ… enviado / Live v15.03 | nÃ£o |
| 2 | **PROMO-BUSCA-PG** | âœ… enviado / Live v15.03 | nÃ£o |
| 3 | **CAMP-PROMO-MENOR** | âœ… enviado / Live v15.04 | nÃ£o |

### ðŸ“¦ PACOTE PRONTO LOJA â€” InauguraÃ§Ã£o Vila 5% auto (`CAMP-VILA-5` Â· **v15.03**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **encerrado 09/08** Â· cÃ³digo ficou na loja (desliga sozinho) Â· Live desde v15.03 |
| **O quÃª** | **07 e 08/08/2026** sÃ³ **Vila Elias**: 5% off Â· menor vs promo Â· arredonda 5Â¢ Â· **09/08 volta ao normal** Â· nÃ£o mexe cadastro Â· faixa laranja |
| **Fix extra** | Recalc (mudou qtd) limpa marca da campanha â€” promo+5% nÃ£o fica com preÃ§o velho |
| **Prova** | `manage.py test produtos.tests_campanha_pdv` **15/15** Â· `scripts/verify_camp_vila_5_path.py` **35/35** |
| **Migrate** | **NÃƒO** |
| **Risco** | MÃ©dio â€” sÃ³ Vila nos dias 07â€“08 Â· kill `AGRO_CAMPANHA_INAUGURACAO_OFF=1` |
| **VocÃª** | **Ctrl+F5** PDV Vila **07 e 08** Â· milho 1kg â†’ 2,40 Â· Centro sem faixa Â· **09** sem 5% |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Promo busca produto Postgres (`PROMO-BUSCA-PG` Â· **v15.02**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v15.03** Â· VERIFY_OK **53/53** |
| **O quÃª** | Etapa 2 da promoÃ§Ã£o busca no **Postgres** (GM + nome); JS aceita GM com hÃ­fen (`GM1507-30`) |
| **Causa** | API ainda exigia Mongo; loja jÃ¡ Ã© `agro_pg` |
| **Prova** | `manage.py test produtos.tests_promocoes_busca` **5/5** Â· `scripts/verify_promo_busca_pg_path.py` **53/53** |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ tela promoÃ§Ãµes / estoque ajuste que reusa o motor |
| **VocÃª** | Ctrl+F5 PromoÃ§Ãµes â†’ Nova â†’ etapa 2 â†’ GM ou nome |
| **Autorizar** | *pode subir lote checklist 07/08 / deploy/lote-checklist-0708 para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Dispenser pet sem comer pelo branco (`DSP-PET-BG` Â· **v14.84**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.85** |
| **O quÃª** | + Adicionar pet nÃ£o limpa fundo branco/preto (peito branco intacto); ingredientes seguem iguais |
| **Prova** | `scripts/verify_dsp_pet_bg.py` **31/31** Â· `verify_dsp_png_bg.py` OK Â· `manage.py check` OK |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ `/interno/dispenser-a6*` Â· **zero** PDV |
| **Arquivo** | `dispenser_a6_studio.html` |
| **VocÃª** | Ctrl+F5 Dispenser â†’ Bicho â†’ apagar pet ruim â†’ subir de novo |
| **Autorizar** | *pode subir DSP-PET-BG / dispenser pet para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Uso loja donos + cards (`PDV-USO-DONOS` Â· **v14.82**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.85** |
| **O quÃª** | Motivos **Uso Geraldinho** / **Uso Geraldo** Â· cards de soma Â· **Uso Centro** / **Uso Vila Elias** Â· botÃ£o topbar verde |
| **Prova** | `scripts/verify_uso_loja_donos_path.py` **65/65** Â· `manage.py check` OK |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ Uso loja |
| **VocÃª** | Ctrl+F5 PDV Â· motivo dono Â· HistÃ³rico 4 cards Â· botÃ£o verde |
| **Autorizar** | *pode subir PDV-USO-DONOS / uso loja donos para produÃ§Ã£o* + **99738595** |

### âœ… Deploy loja **v14.85** â€” lote checklist 06/08b (frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.85** Â· `producao` @ **e68187b** Â· Render `dep-d9qlio0u01pc739jq0hg` |
| **Incluiu** | CX-EMP-LOJA Â· VAL-SALVAR Â· BI-VAL-LOJA Â· PDV-USO-DONOS Â· DSP-PET-BG |
| **Branch** | `deploy/lote-checklist-0608b` (FF â†’ `producao`) |
| **Migrate** | **NÃƒO** |
| **Fora** | GG-UX (P2,5) |
| **Rollback** | tag `rollback/pre-lote-checklist-0608b-v14.72` @ `008e361` |
| **VocÃª** | **Ctrl+F5** Retirada Vila Â· Validade Â· BI (TRAVA) Â· Uso loja Â· Dispenser |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (06/08b Â· loja v14.85) Â· **superado**

> **Vigente:** **CHECKLIST ÃšNICO â€” pronto envio (07/08)** no topo. Loja permanece **v14.85** atÃ© o prÃ³ximo envio.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CX-EMP-LOJA** | âœ… enviado | nÃ£o |
| 2 | **VAL-SALVAR** | âœ… enviado | nÃ£o |
| 3 | **BI-VAL-LOJA** | âœ… enviado | nÃ£o |
| 4 | **PDV-USO-DONOS** | âœ… enviado | nÃ£o |
| 5 | **DSP-PET-BG** | âœ… enviado | nÃ£o |
| 6 | **GG-UX** | ðŸŸ¡ P2,5 Â· fora | â€” |

### ðŸ“¦ PACOTE PRONTO LOJA â€” BI Validade segue loja travada (`BI-VAL-LOJA` Â· **v14.78**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.85** |
| **O quÃª** | Card Validade do BI respeita TRAVA Centro/Vila (`EstoqueLote.deposito`) Â· cache `v5` |
| **Prova** | `scripts/verify_bi_val_salvar_path.py` **17/17** |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ KPI do BI |
| **VocÃª** | Ctrl+F5 BI Â· TRAVA Vila Â· nÃºmeros â‰  Centro |
| **Autorizar** | *pode subir BI-VAL-LOJA / validade BI loja para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Validade Salvar na linha (`VAL-SALVAR` Â· **v14.77**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.85** |
| **O quÃª** | Sempre **Salvar** Â· grava data/lote no `EstoqueLote` Â· API com `lote_id` |
| **Prova** | `verify_bi_val_salvar_path` Â· sem Â«Usar cadastroÂ» |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” RelatÃ³rio Validade |
| **VocÃª** | Ctrl+F5 Validade Â· muda data Â· **Salvar** |
| **Autorizar** | *pode subir VAL-SALVAR / salvar validade para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” saÃ­da Vila = empresa Vila (`CX-EMP-LOJA` Â· **v14.75**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.85** |
| **O quÃª** | Retirada: empresa da loja do caixa Â· Vila â†’ **Agro Mais Vila Elias** Â· Centro â†’ **Agro Mais Centro** |
| **Commit** | `64c338b` |
| **Prova** | `verify_saida_empresa_loja` **10/10** |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” nÃ£o mexe PDV venda |
| **VocÃª** | Ctrl+F5 Retirada Vila Â· empresa certa Â· 1 saÃ­da |
| **Autorizar** | *pode subir CX-EMP-LOJA / empresa saÃ­da caixa para produÃ§Ã£o* + **99738595** |

### âœ… Deploy loja **v14.72** â€” lote checklist 06/08 (frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.72** Â· `producao` @ **008e361** Â· Render `dep-d9qasd0u01pc739asp30` |
| **Incluiu** | PLANOS-PDV Â· FACETA-CACHE Â· TRANSF-BIP Â· FOTO-PDV Â· NF-VAL-BCA |
| **Branch** | `deploy/lote-checklist-0608` (FF â†’ `producao`) |
| **Migrate** | **0085** (`exibir_pdv`, depsâ†’0083) Â· **0086** (`EstoqueLote.deposito`) â€” Render `migrate --noinput` no build |
| **Fora** | GG-UX (P2,5) |
| **Rollback** | tag `rollback/pre-lote-checklist-0608-v14.61` @ `47bb1de` |
| **VocÃª** | **Ctrl+F5** Cadastro Â· Validade Â· TransferÃªncias Â· Config Planos Â· Retirada |

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (06/08 Â· loja v14.72) Â· **superado**

> **Vigente:** **CHECKLIST ÃšNICO â€” pronto envio (06/08 Â· apÃ³s loja v14.72)** no topo. Loja permanece **v14.72** atÃ© o prÃ³ximo envio.

### ðŸ“¦ PACOTE PRONTO LOJA â€” Foto Gerais = Delivery = PDV (`FOTO-PDV` Â· **v14.68+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.72** |
| **O quÃª** | Aba Gerais: anexar foto (igual Delivery) Â· **mesma foto** nas duas abas Â· PDV jÃ¡ lÃª do overlay |
| **Prova** | `scripts/verify_foto_pdv.py` **13/13** Â· `manage.py check` OK |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ modal cadastro |
| **VocÃª** | Ctrl+F5 Cadastro Â· Gerais â†’ foto â†’ Salvar Â· Delivery igual Â· PDV |
| **Autorizar** | *pode subir FOTO-PDV / foto produto para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” TransferÃªncia forÃ§ada bip (`TRANSF-BIP` Â· **v14.67**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.72** |
| **O quÃª** | Bip barras â†’ +1 e cursor na busca; digitar nome/GM â†’ foco na qtd |
| **Prova** | `scripts/verify_transf_bip.py` **4/4** |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ `/transferencias/` forÃ§ada |
| **VocÃª** | Ctrl+F5 ForÃ§ada Â· bip Â· bip Â· digitar GM + Enter |
| **Autorizar** | *pode subir TRANSF-BIP / bip transferÃªncia para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Planos no PDV por checkbox (`PLANOS-PDV` Â· **v14.62+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.72** |
| **O quÃª** | Checkbox **PDV** em Planos de contas Â· 14 atuais prÃ©-marcados Â· saÃ­da/retirada lÃª Postgres Â· marcar **ManutenÃ§Ã£o do PrÃ©dio** libera no select |
| **Commits** | `65e0f6f` (+ bump `bbddd19`) |
| **Migrate** | **SIM** â€” loja: `0084` (no-op, schema jÃ¡ ok) + `0085` (`exibir_pdv` + seed) Â· **nÃ£o** subir `0082` |
| **Prova** | `verify_planos_conta` **28/28** Â· `manage.py check` OK Â· toggle ManutenÃ§Ã£o on/off Â· 14 marcados = lista antiga Â· IDs especiais vale/salÃ¡rio/outros/depÃ³sito OK Â· inativo some do PDV |
| **Risco** | Baixo-mÃ©dio â€” muda texto gravado do plano (nome oficial Agro em vez do cÃ³digo ERP); aliases jÃ¡ cobrem |
| **VocÃª** | Ctrl+F5 Config Planos Â· coluna PDV Â· marcar ManutenÃ§Ã£o Â· abrir Retirada e conferir |
| **Autorizar** | *pode subir PLANOS-PDV / planos no PDV para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Cadastro faceta unidade/marca/cat (`FACETA-CACHE` Â· **v14.66**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.72** |
| **Bug** | Cadastrou Caixa no produto Â· no outro a lista dizia Â«nÃ£o cadastradoÂ» (marca/cat igual) |
| **Fix** | Invalidar cache `v6` ao salvar/+ PIN Â· servidor junta produto+PIN Â· JS **soma** (nÃ£o apaga) Â· seed ao abrir/salvar |
| **Prova** | `scripts/verify_faceta_cache.py` **10/10** Â· integraÃ§Ã£o overlay+PIN OK Â· `manage.py check` OK |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ combobox do cadastro |
| **VocÃª** | Ctrl+F5 Cadastro Â· digitar **Caixa** no outro Natuverm â†’ tem que achar |
| **Autorizar** | *pode subir FACETA-CACHE / faceta cadastro para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Entrada NF validade â†’ Validade + BCA (`NF-VAL-BCA` Â· **v14.71**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.72** |
| **O quÃª** | Etapa 4 NF â†’ `EstoqueLote` (+ `deposito`) Â· estorno reduz lote Â· colunas **Centro/Vila** Â· filtro Loja por saldo/lote Â· BCA + linha editÃ¡vel sem data Â· backfill `manage.py backfill_validade_entrada_nf` |
| **Migrate** | **SIM** â€” `0086_estoque_lote_deposito` Â· loja: apÃ³s deploy rodar migrate Â· backfill com `--aplicar` se faltar lote antigo |
| **Prova** | `scripts/verify_validade_nf_path.py` **23/23** Â· `manage.py check` OK |
| **Risco** | MÃ©dio â€” Entrada NF estoque + tela Validade |
| **VocÃª** | Ctrl+F5 Validade Â· Todas/Centro/Vila Â· BCA produto sem data â†’ preencher Â· Entrada NF etapa 4 â†’ conferir lista |
| **Autorizar** | *pode subir NF-VAL-BCA / validade NF para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE â€” Planos no PDV por checkbox (`PLANOS-PDV` Â· detalhe)

> **Vigente:** **PACOTE PRONTO LOJA â€” PLANOS-PDV** no topo do CHECKPOINT.

### âœ… CHECKLIST ÃšNICO â€” enviado produÃ§Ã£o (05/08 Â· loja v14.61) Â· **superado**

> **Vigente:** **CHECKLIST ÃšNICO â€” pronto envio (06/08)** no topo. Loja permanece **v14.61** atÃ© o prÃ³ximo envio.

**JÃ¡ Live (v14.41):** BI-TOPBAR-TOTAL Â· SEFAZ-UI Â· COMP-UX Â· DFE-CIENCIA Â· CP-DUP-BACKUP Â· GG-GASTOS Â· PDV-CAD / CUSTO-FAMILIA / MODAL-UTF8.

**Prova (05/08 Â· revalidado):** `verify_planos_conta` **24/24** Â· `verify_nf_troca_estorno` **11/11** Â· `verify_cp_anti_dup_backup` **11/11** Â· `verify_bi_topbar_total` **35/35** Â· `verify_pacotes_pendentes_0508` **9/9** Â· `manage.py check` OK.

**SimulaÃ§Ã£o cherry (prep):** BI/CP/NF aplicam limpo (sÃ³ conflito `VERSION`/`banana.md` â€” normal). **PLANOS:** cherry `e109918`+`3ecc824`+`fe7746c` **sozinho falha** (arquivos da tela nÃ£o existem na loja) â†’ na branch deploy foi feito **prep** (UI+APIs+menu, **sem** `0082`/`0084`/`models`). Lote **nÃ£o toca PDV/caixa** Â· **sem migrate**.

**Deploy concluÃ­do (05/08):** autorizaÃ§Ã£o recebida Â· `producao` **47bb1de** Â· Render/HTTP Live Â· versÃ£o confirmada na home. Fazer **Ctrl+F5** nos PCs da loja.

### ðŸŸ¡ WIP â€” GrÃ¡fico gastos uso / clareza (`GG-UX` Â· **P2,5** Â· 05/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ðŸŸ¡ **fora do lote** Â· fila P2,5 Â· **sem pressa** |
| **Onde** | `/financeiro/grafico-gastos/` (jÃ¡ Live v14.41 com GG-GASTOS) |
| **Pediu** | Renan: tela confusa Â· revisÃ£o de clareza depois |

### âœ… ENVIADO LOJA â€” BI topbar filtros de data (`BI-TOPBAR-DATAS` Â· **Live v14.61**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.61** |
| **O quÃª** | Filtros `MÃªs atÃ© hoje`â€¦`Datas` nunca cortados Â· marca/rÃ³tulo Loja sÃ³ â‰¥1600px Â· badge **`Trava: Vila`** sÃ³ com caixa travado Â· JS mede largura e joga filtros p/ 2Âª linha se nÃ£o couber |
| **Arquivo** | `dashboard_gerencial.html` |
| **Commit** | `8d6976f` |
| **Prova** | `verify_pacotes_pendentes_0508.py` Â· `verify_bi_topbar_total.py` **35/35** Â· Chrome 820â€“1920 |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ BI `/` |
| **VocÃª** | Ctrl+F5 `/` Â· Â«DatasÂ» visÃ­vel Â· trava do caixa aparece sÃ³ quando trava |

### âœ… ENVIADO LOJA â€” Backup CP menu fecha (`CP-BACKUP-MENU` Â· **Live v14.61**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.61** |
| **O quÃª** | Overlay amarelo do **Backup** nÃ£o fica aberto sozinho Â· fecha fora / Esc / apÃ³s baixar ZIP |
| **Arquivo** | `lancamentos_contas_pagar_teste.html` |
| **Commit** | `e0652c5` |
| **Prova** | `verify_cp_anti_dup_backup.py` **11/11** Â· ZIP abertos 200 + `backup_ultimo` grava Â· CSS `[hidden]` depois do `display:flex` |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ UI do botÃ£o Backup (CP-DUP-BACKUP jÃ¡ Live) |
| **VocÃª** | Ctrl+F5 CP Â· Backup fechado Â· clica â†’ abre â†’ baixa â†’ fecha |

### âœ… ENVIADO LOJA â€” NF trocar produto exige estorno (`NF-TROCA-ESTORNO` Â· **Live v14.61**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.61** |
| **O quÃª** | Com estoque jÃ¡ lanÃ§ado, trocar/remover produto **ou mudar quantidade** pede **Estornar e trocar** (PIN) Â· back recusa salvar (`requer_estorno`) Â· XML Â«Confirmar na gradeÂ» e repontar id pela margem tambÃ©m respeitam o estorno |
| **Arquivos** | `entrada_nota.html` Â· `nfe_entrada_util.py` |
| **Commit** | `7f8a78d` + `eaec9a8` + `263a137` |
| **Prova** | `python scripts/verify_nf_troca_estorno.py` â†’ **VERIFY_OK 11/11** (banco real: troca/qtd recusadas, estorno libera; API 400 `requer_estorno`; `node --check` nos scripts da tela) Â· `verify_pacotes_pendentes_0508.py` **9/9** Â· `manage.py check` OK |
| **Migrate** | **NÃƒO** |
| **Risco** | MÃ©dio-baixo â€” mexe em Entrada NF jÃ¡ concluÃ­da; estorno usa API de reabrir jÃ¡ existente |
| **VocÃª** | Nota com estoque Â· Mudar produto â†’ modal PIN Â· saldo antigo some / novo entra |

### âœ… ENVIADO LOJA â€” Planos de contas Config (`PLANOS-CONTA` Â· **Live v14.61**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live v14.61** Â· enviado via **`deploy/lote-checklist-0508`** |
| **Onde** | ConfiguraÃ§Ã£o (F11) â†’ **Planos de contas** |
| **O quÃª** | Tela edita planos oficiais Â· seed se vazio Â· visual Â§11 |
| **Loja** | Prep na branch deploy (UI+APIs+menu) Â· modelo jÃ¡ existe (`0065`) |
| **NÃƒO** | cherry cru `e109918`â€¦ (arquivos nÃ£o existem na loja) Â· **nem** `c6757fd`/`0084` |
| **Prova** | `verify_planos_conta.py` **24/24** Â· reverse URL OK na branch deploy Â· `check` OK |
| **VocÃª** | Ctrl+F5 Â· F11 â†’ Planos Â· edita um dos 44 |

### ðŸž FIX â€” trocar produto na NF nÃ£o estornava o estoque (05/08 Â· **teste v14.48**)

> Empacotado em **NF-TROCA-ESTORNO** (acima).

### ðŸž FIX â€” painel Backup do CP ficava sempre aberto (05/08 Â· **teste v14.46**)

> Empacotado em **CP-BACKUP-MENU** (acima).

### ðŸ”§ BI topbar â€” filtros de data nunca escondidos (05/08 Â· **teste v14.45**)

> Empacotado em **BI-TOPBAR-DATAS** (acima).

### ðŸ“¦ PACOTE PRONTO LOJA â€” GrÃ¡fico gastos acerto + visual (`GG-GASTOS` Â· **v14.30+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live** v14.41 Â· VERIFY_OK 10/10 Â· ðŸŸ¡ **GG-UX P2,5** na fila (clareza / Renan acha que ainda nÃ£o estÃ¡ certa) |
| **O quÃª** | Soma sÃ³ planos marcados Â· bucket recorta no perÃ­odo Â· Â«Como eraÂ»/Comparar usa `as_of` Â· popup CP alinhado Â· visual Display Scale |
| **Arquivos** | `produtos/lancamentos_financeiro_pg_analytics_util.py` Â· `financeiro/templates/financeiro/grafico_gastos.html` |
| **Prova** | `python scripts/verify_grafico_gastos.py` â†’ **VERIFY_OK** |
| **Commits** | `69ab665` (filtro+clip) Â· `6756c02` (checkup+visual) Â· + script verify |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ leitura BI Â· nÃ£o grava CP |
| **VocÃª** | Ctrl+F5 Â· `/financeiro/grafico-gastos/` Â· 1 plano (ex. SalÃ¡rios) Â· ponto Jul = CP mesmo filtro |
| **Autorizar** | *pode subir GG-GASTOS / grÃ¡fico gastos para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” DF-e CiÃªncia + XML completo (`DFE-CIENCIA` Â· **v14.33**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live** v14.41 |
| **Onde** | Entrada NF â†’ aba SEFAZ Â· item **SÃ³ resumo** |
| **Fluxo** | **Dar ciÃªncia e buscar XML** â†’ (se precisar) **Buscar XML** â†’ **Carregar na grade** |
| **Inclui** | Evento **210210** no Ambiente Nacional Â· status/protocolo no Postgres Â· nÃ£o reenvia se jÃ¡ ciente |
| **Migrate** | **SIM** Â· `0083_dfe_manifestacao_ciencia` |
| **Prova** | `python scripts/verify_dfe_manifestacao.py` â†’ **VERIFY_OK 21/21** Â· unit cliente OK Â· `manage.py check` OK Â· migrate local OK |
| **Commit** | `aa2a9a3` (+ verify reforÃ§ado neste push) |
| **Risco** | Fiscal: evento oficial na Receita; XML pode demorar minutos apÃ³s a CiÃªncia |
| **âš ï¸** | Branch isolada + migrate na loja Â· **nÃ£o** merge `teste` inteiro |
| **VocÃª** | Ctrl+F5 Â· SEFAZ â†’ SÃ³ resumo â†’ Dar ciÃªncia Â· depois Carregar |
| **Autorizar** | *pode subir DFE-CIENCIA / ciÃªncia DF-e para produÃ§Ã£o* + **99738595** |

### ðŸ§¹ CHECKUP + visual â€” tela GrÃ¡fico gastos (`GG-CHECKUP`)

> âœ… Empacotado em **GG-GASTOS** (acima) Â· VERIFY_OK Â· pronto envio.

### ðŸž FIX â€” grÃ¡fico Gastos somava plano errado (`GG-FILTRO`)

> âœ… Empacotado em **GG-GASTOS** (acima) Â· filtro positivo + clip do bucket.

### ðŸ“¦ PACOTE PRONTO LOJA â€” Anti-duplicata CP + Backup (`CP-DUP-BACKUP` Â· **v14.34**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live** v14.41 Â· VERIFY_OK 11/11 |
| **Onde** | Entrada NF (Â«Salvar + a pagarÂ») Â· Contas a pagar â†’ **Backup** |
| **Inclui** | Bloqueio PG por chave NF / assinatura Â· guard Entrada NF Â· trava duplo clique Â· Backup Todos/Abertos + data/hora Ãºltimo |
| **Migrate** | **NÃƒO** |
| **Prova** | `python scripts/verify_cp_anti_dup_backup.py` â†’ **VERIFY_OK 11/11** Â· `manage.py check` OK |
| **Commit** | `4b28263` (+ verify neste push) |
| **Risco** | Baixo â€” nÃ£o apaga tÃ­tulo antigo; sÃ³ impede 2Âº lote novo |
| **Dados** | Maio quitado **deixar** Â· julho+ limpo Â· IbiÃºna lote errado jÃ¡ sumiu |
| **VocÃª** | Ctrl+F5 Â· CP: botÃ£o Backup Â· Entrada NF: nÃ£o gerar 2Âº lote na mesma chave |
| **Autorizar** | *pode subir CP-DUP-BACKUP / anti-duplicata CP para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ Plano de contas SisVale (`PLANOS-CONTA` Â· detalhe)

> Vigente: checklist + branch **`deploy/lote-checklist-0508`**. **NÃ£o** cherry cru dos commits do `teste`. **NÃ£o** `0084`.

### ðŸ“¦ CHECKLIST ÃšNICO â€” pÃ³s v14.41 (05/08) Â· **superado**

> **Vigente:** **CHECKLIST ÃšNICO â€” pronto envio (05/08 Â· apÃ³s loja v14.41)** no topo do CHECKPOINT.

### ðŸ“¦ ENVIO LOJA 05/08 â€” lote checklist (`LOTE-v14.41`)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** / Live v14.41 Â· `2efcc60` |
| **Antes** | v13.83 Â· `ed52234` Â· tag `checkpoint-loja-pre-lote-20260805` |
| **Incluiu** | SEFAZ-UI Â· DFE-CIENCIA Â· CP-DUP-BACKUP Â· BI-TOPBAR-TOTAL Â· COMP-UX Â· GG-GASTOS |
| **Omitiu** | **PLANOS-CONTA** (risco migrate vs 0065) |
| **Migrate** | **0083** sÃ³ (DFE) â€” Render build roda `migrate --noinput` |
| **Prova prÃ©-push** | verifies OK Â· check OK Â· 14 telas 200 |

### ðŸ“¦ PACOTE PRONTO LOJA â€” ComposiÃ§Ã£o Saco/Kit recolher (`COMP-UX` Â· **v14.23**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live** v14.41 |
| **Inclui** | â–¶/â–¼ em Saco e Kit Â· texto longo sÃ³ no Â«?Â» Â· aba ComposiÃ§Ã£o com scroll Â· saco comeÃ§a recolhido se kit ligado |
| **Arquivo** | `_modal_editar_produto_cadastro_erp.inc.html` (sÃ³ aba ComposiÃ§Ã£o) Â· `scripts/verify_comp_ux_recolher.py` |
| **Commit** | `645fc75` (+ verify neste push) |
| **Prova** | `python scripts/verify_comp_ux_recolher.py` â†’ **VERIFY_OK** Â· `verify_custo_familia.py` â†’ **VERIFY_OK** |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ UI Cadastro ComposiÃ§Ã£o Â· **zero** lÃ³gica PDV/estoque |
| **âš ï¸** | Subir **sobre loja v13.83** com patch isolado do modal â€” **nÃ£o** o modal completo do `teste` (tem WIP) |
| **VocÃª** | Ctrl+F5 Cadastro â†’ ComposiÃ§Ã£o Â· â–¶ no Saco Â· vÃª o Kit |
| **Autorizar** | *pode subir COMP-UX / composiÃ§Ã£o recolhimento para produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” BI topbar Sync + Total unidades (`BI-TOPBAR-TOTAL` Â· **v14.18**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live** v14.41 |
| **Inclui** | Sync ERP compacto na topbar Â· mais espaÃ§o aos filtros Â· grÃ¡fico **Faturamento por Unidade** com barra **Total** (Centro+Vila) + badge |
| **Arquivos** | `dashboard_gerencial.html` Â· `dashboard_gerencial_body.html` Â· `scripts/verify_bi_topbar_total.py` Â· VERSION |
| **Commit cÃ³digo** | `fcf1c49` Â· pacote+verify `739be93` Â· badge **14.20** |
| **Prova** | `python scripts/verify_bi_topbar_total.py` â†’ **VERIFY_OK 35/35** Â· home local **200** (Sync curto Â· Total push Â· stores Centro/Vila) |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” **sÃ³ BI `/`** Â· zero PDV/caixa/NF |
| **VocÃª** | Ctrl+F5 `/` Â· Sync pequeno Â· filtros legÃ­veis Â· Total = Centro+Vila |
| **Autorizar** | *pode subir BI-TOPBAR-TOTAL / produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Entrada NF aba SEFAZ limpa (`SEFAZ-UI` Â· **v14.19**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live** v14.41 |
| **Inclui** | Aba SEFAZ sÃ³ aÃ§Ãµes + lista Â· escritas no **?** Â· status compacto Â· chip Â«SÃ³ resumoÂ» |
| **Arquivo** | `entrada_nota.html` |
| **Commit** | `e83af8f` |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ UI |
| **VocÃª** | Ctrl+F5 `/entrada-nota/` â†’ SEFAZ â†’ **?** |
| **Autorizar** | *pode subir SEFAZ-UI / Entrada NF SEFAZ para produÃ§Ã£o* + **99738595** |

### ðŸŽ¨ Entrada NF â€” aba SEFAZ limpa + ajuda Â«?Â» (04/08 Â· **teste v14.19**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… teste Â· ver **PACOTE PRONTO SEFAZ-UI** |
| **Badge** | **14.19** Â· `e83af8f` |
| **Prova** | Ctrl+F5 `/entrada-nota/` â†’ SEFAZ â†’ **?** |

### ðŸ”§ BI â€” topbar sync + Total por unidade (04/08 Â· **teste v14.18**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… teste Â· ver **PACOTE PRONTO BI-TOPBAR-TOTAL** |
| **Commit** | `fcf1c49` Â· push `origin/teste` |

### ðŸ©¹ UX ComposiÃ§Ã£o â€” recolhimento Saco/Kit (04/08 Â· **v14.23**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ðŸ“‹ **PACOTE PRONTO** â€” ver **COMP-UX** no CHECKLIST |
| **Prova** | VERIFY_OK (`verify_comp_ux_recolher` + `verify_custo_familia`) |

### âœ… Deploy loja **v13.83** â€” MODAL-UTF8 acentos (04/08 Â· frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** Â· producao @ **ed52234** Â· badge **13.83** Â· Render auto |
| **AutorizaÃ§Ã£o** | *pode subir esse path para produÃ§Ã£o* + **99738595** |
| **Branch** | `deploy/hotfix-modal-utf8-v13.83` â†’ producao |
| **Diff** | **2 arquivos** (modal + VERSION) Â· VERIFY_OK |
| **Migrate** | **NÃƒO** |
| **Rollback** | `git push origin b28ce83:producao` Â· tag `rollback/pre-modal-utf8-v13.83` |
| **VocÃª** | Ctrl+F5 Cadastro â†’ abas PreÃ§os/ComposiÃ§Ã£o com Ã§/Ã£ ok |

### ðŸš‘ HOTFIX modal acentos (`MODAL-UTF8` Â· **v13.83**) â€” **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live loja** â€” ver Deploy v13.83 acima |
| **Rollback** | tag `rollback/pre-modal-utf8-v13.83` â†’ b28ce83 (v13.82) |

### âœ… Deploy loja **v13.82** â€” CUSTO-FAMILIA (04/08 Â· frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** Â· producao @ **b28ce83** Â· badge **13.82** Â· Render auto |
| **AutorizaÃ§Ã£o** | *pode subir para produÃ§Ã£o* + **99738595** |
| **Branch** | `deploy/custo-familia-v13.82` â†’ producao (**nÃ£o** teste inteiro) |
| **Diff** | **11 arquivos** Â· VERIFY_OK |
| **Migrate** | **NÃƒO** |
| **Rollback** | `git push origin 3381d0d:producao` Â· tag `rollback/pre-custo-familia-v13.82` |
| **VocÃª** | Ctrl+F5 Cadastro â†’ ComposiÃ§Ã£o Â· amarrar pacote no saco Â· reabrir Â· 1 venda Â· estoque do saco |

### ðŸš€ Custo famÃ­lia â€” saco â†’ pacote/granel (04/08 Â· **Live v13.82**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live loja** â€” ver Deploy v13.82 acima |
| **Rollback** | tag `rollback/pre-custo-familia-v13.82` â†’ 3381d0d |

### âœ… Deploy loja **v13.81** â€” PDV-CAD-RAPIDO (04/08 Â· frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** Â· producao @ **3381d0d** Â· badge **13.81** Â· Render auto |
| **AutorizaÃ§Ã£o** | *pode subir cadastro rÃ¡pido PDV / PDV-CAD-RAPIDO para produÃ§Ã£o* + **99738595** |
| **Branch** | deploy/pdv-cad-rapido-v13.99 â†’ producao (**nÃ£o** 	este inteiro) |
| **Diff** | **16 arquivos** / +2011 Â· FL-008 preservado |
| **Prova** | 21/21 + VERIFY_OK na branch isolada |
| **Migrate** | **NÃƒO** |
| **Rollback** | git push origin a0f0db2:producao Â· tag 
ollback/pre-pdv-cad-rapido-v13.81 |
| **VocÃª** | Ctrl+F5 PDV Â· busca Â· **+ Novo Produto** Â· Cadastro **PDV conferir** Â· 1 venda |
| **Cosmos** | opcional no Render: AGRO_COSMOS_TOKEN |

### ðŸ“¦ CHECKLIST ÃšNICO â€” pÃ³s envio (04/08 Â· apÃ³s v13.83) Â· **superado**

> **Vigente:** **CHECKLIST ÃšNICO â€” pronto envio (04/08)** no topo (BI-TOPBAR-TOTAL Â· SEFAZ-UI).

**Loja na Ã©poca:** badge **v13.83** Â· producao @ **ed52234**

| # | Pacote | Status |
| - | ------ | ------ |
| 1 | **PDV-CAD-RAPIDO** | âœ… **enviado** / Live v13.81 |
| 2 | **CUSTO-FAMILIA** (saco+kit) | âœ… **enviado** / Live v13.82 |
| 3 | **MODAL-UTF8** (acentos) | âœ… **enviado** / Live v13.83 |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Hotfix acentos modal (`MODAL-UTF8` Â· **v13.83**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live loja v13.83** Â· ed52234 |
| **Inclui** | Texto UTF-8 do modal Cadastro |
| **Rollback** | `b28ce83` / `rollback/pre-modal-utf8-v13.83` |
| **VocÃª** | Ctrl+F5 Cadastro â†’ PreÃ§os/ComposiÃ§Ã£o legÃ­veis |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Custo famÃ­lia saco+kit (`CUSTO-FAMILIA` Â· **v13.82**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live loja v13.82** Â· b28ce83 |
| **Inclui** | Bloco saco (custo + baixa estoque) Â· kit multi-insumo Â· propaga (cadastro/NF/Excel) |
| **Rollback** | `3381d0d` / `rollback/pre-custo-familia-v13.82` |
| **VocÃª** | Ctrl+F5 Cadastro â†’ ComposiÃ§Ã£o Â· 1 venda com pacote ligado ao saco |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Cadastro rÃ¡pido PDV (PDV-CAD-RAPIDO Â· **v13.81**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live loja v13.81** Â· 3381d0d |
| **Inclui** | + Novo/Produto Â· Cosmos/OFF Â· NCM silencioso Â· foto se existir Â· card **PDV conferir** Â· modal **?** Â· busca maior |
| **Rollback** | 0f0db2 / 
ollback/pre-pdv-cad-rapido-v13.81 |

### âš ï¸ Lembrete â€” NÃƒO subir 	este inteiro

	este vs loja ainda tem centenas de arquivos WIP. Loja sÃ³ recebe branch isolada.

### âœ… Deploy loja **v13.80** â€” lote CAD/NF + DSP Â· **histÃ³rico**

Base antes do PDV-CAD: 0f0db2.

### ðŸš€ DEPLOY PRONTO â€” lote CAD/NF + Dispenser (`deploy/lote-cad-nf-dsp-v13.80` Â· 03/08) Â· **enviado**

**Status:** âœ… **Live loja v13.80** Â· ver bloco Deploy acima. **NÃ£o inclui** PDV-CAD-RAPIDO.  
**Rollback:** `git push origin 6996fca:producao`

### ðŸ“¦ PACOTE PRONTO LOJA â€” Dispenser PNG fundo (`DSP-PNG-BG` Â· **v13.80**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** / Live loja v13.80 |
| **Inclui** | Moldura ingredientes sempre branca Â· upload PNG + limpa fundo branco/preto da borda |
| **Prova** | `python scripts/verify_dsp_png_bg.py` â†’ **VERIFY_OK** |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ `/interno/dispenser-a6*` Â· **zero** PDV |
| **Arquivo** | `dispenser_a6_studio.html` |
| **VocÃª** | Ctrl+F5 Dispenser â†’ Ingredientes â†’ PNG transparente Â· moldura **branca** Â· foto antiga Â«queimadaÂ» â†’ apagar e subir de novo |

### ðŸš€ PREP deploy â€” lote CAD/NF (`prep/lote-cad-nf-v13.75`) Â· **OBSOLETO**

| Item | Detalhe |
| ---- | ------- |
| **Status** | âŒ **NÃƒO usar** â€” `catalogo_agro.py` mojibake Â· substituÃ­do por `deploy/lote-cad-nf-dsp-v13.80` |
| **Provas** | (antigas) cherry sobre `6996fca` Â· CAD-CB 7/7 Â· ENTRADA-NF-CUSTO â€” **remonte limpo no deploy** |
| **Inclui** | NF custo Â· Duplicar Â· Barras opcionais (+ fallback busca) |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Barras opcionais (`CAD-CB-OPC` Â· **v13.75**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** / Live no lote v13.80 |
| **Inclui** | Lista barras opcionais no cadastro Â· grava PG Â· bip PDV acha qualquer EAN |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Duplicar cadastro (`CAD-DUP` Â· **v13.72**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** / Live no lote v13.80 |
| **Inclui** | BotÃ£o **Duplicar** no modal Cadastro Â· cÃ³digos/barras novos Â· sem estoque |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Entrada NF custo cadastro (`ENTRADA-NF-CUSTO` Â· **v13.71**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** / Live no lote v13.80 |
| **Inclui** | V. unit etapa 2 puxa custo do Cadastro (JS ignora 0 Â· overlay sync Â· PG fallback) |

### âœ… VERIFY â€” ENTRADA-NF-CUSTO (03/08)

| Item | Detalhe |
| ---- | ------- |
| **Resultado** | **VERIFY_OK** Â· **v13.77** |
| **Testes** | 10/10 `tests_entrada_nf_custo_cadastro` |
| **Path** | Mongo final=0 â†’ overlay OU `Produto.custo` PG â†’ JS `> 0` â†’ V. unit |
| **Caso real** | sal fino GM1821 Â· Cadastro R$ 27 Â· overlay sem `preco_custo_overlay` Â· depende fallback PG |
| **VocÃª** | Ctrl+F5 Entrada NF Â· incluir sal fino â†’ V. unit **27,00** |

### âœ… VERIFY â€” CAD-CB-OPC (03/08)

| Item | Detalhe |
| ---- | ------- |
| **Resultado** | **VERIFY_OK** |
| **Testes** | 7/7 `tests_codigos_barras_opcionais` |
| **VocÃª** | Ctrl+F5 cadastro: gravar EAN opcional â†’ bip no PDV |

### ðŸ“¦ CHECKLIST ÃšNICO â€” apÃ³s envio (03/08 Â· lote v13.64) Â· **histÃ³rico**

**Loja hoje:** badge **v13.64** Â· `producao` @ **`6996fca`**  
**Rollback:** tag `rollback/pre-lote-checklist-03ago-v13.39` (@ `94112a8` / v13.39)  
**Migrate:** `0081` (dispenser) Â· `estoque.0015` (estorno NF) â€” Render roda no boot

| Ordem | Pacote | Status |
| ----- | ------ | ------ |
| â€” | **LOTE CHECKLIST** (**v13.39**) | âœ… absorvido |
| **1** | **DSP-MIX** + **DSP-FOLHA-CLOUD** | âœ… **enviado** / Live (no lote) |
| **2** | **NF-REOPEN-ESTOQUE** | âœ… **enviado** / Live (no lote) |
| **3** | **BUGS-PROMPT** | âœ… **enviado** / Live (no lote) |
| **4** | **KARDEX-SALDO-COLS** | âœ… **enviado** / Live (no lote) |

### âœ… Deploy loja **v13.64** â€” lote checklist 03/08 (frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live** Render Â· `producao` @ **`6996fca`** Â· badge **13.64** Â· deploy `dep-d9odlmhsrm7s73fth000` |
| **AutorizaÃ§Ã£o** | *pode enviar para produÃ§Ã£o* + **99738595** (03/08) |
| **Cherry** | `96c7ed7` â†’ `0057e69` â†’ `7db7675` â†’ `be3715e`* â†’ `5a84b83` â†’ `19004d8` â†’ `0e65ecb` â†’ `e4c1828` |
| **\*** | `views.py` reabrir: manteve `_entrada_nfe_conexao` + `usuario_django` |
| **Diff** | 21 arquivos Â· **zero** PDV/caixa/checkout |
| **Rollback** | `rollback/pre-lote-checklist-03ago-v13.39` |
| **Loja** | **Ctrl+F5** nos PCs Â· badge BI deve mostrar **13.64** |

### ðŸš€ PREP deploy loja â€” lote 03/08 (**feito**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** â€” ver bloco Deploy v13.64 acima |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Kardex Centro/Vila/Total (`KARDEX-SALDO-COLS` Â· **v13.64**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** / Live (lote **v13.64** @ `6996fca`) |
| **Inclui** | Colunas **Centro Â· Vila Â· Total** Â· filtros chip Â· saldo 0 legÃ­vel |
| **Prova** | AST zero write Â· API GET Â· count ajustes inalterado Â· math mock |
| **Saldo atual** | **NÃƒO altera** â€” sÃ³ leitura/exibiÃ§Ã£o |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” modal cadastro aba Estoque |
| **Autorizar** | *pode subir kardex saldos / produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Dispenser Mix + folhas nuvem (`DSP-MIX` Â· **v13.63**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** / Live (lote **v13.64** @ `6996fca`) |
| **Inclui** | Mix Sabores Â· criar sabor Â· folhas **sÃ³ Postgres/RAM** (sem Â«memÃ³ria cheiaÂ») |
| **Prova** | PG upsert/list/delete folha Â· cloud sem `writeLocal(folhas)` Â· saveFolhas sem Quota Â· Mix PNG OK |
| **Migrate** | **SIM** `0081` |
| **Risco** | Baixo â€” sÃ³ `/interno/dispenser-a6*` Â· **zero** PDV |
| **Autorizar** | *pode subir dispenser / produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Entrada NF reabrir estoque (`NF-REOPEN-ESTOQUE` Â· **v13.60**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** / Live (lote **v13.64** @ `6996fca`) |
| **Commits** | `7db7675` Â· `be3715e` |
| **Inclui** | Reabrir estorna carimbo Â· kardex saÃ­da estorno (ao **reabrir** nota â€” mexe saldo de propÃ³sito) |
| **Migrate** | **SIM** `estoque.0015` |
| **Risco** | MÃ©dio â€” **sÃ³ ao reabrir** Entrada NF (nÃ£o Ã© path de venda PDV) |
| **Autorizar** | *pode subir NF reabrir estoque / produÃ§Ã£o* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Bugs Copiar prompt Cursor (`BUGS-PROMPT` Â· **v13.62**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** / Live (lote **v13.64** @ `6996fca`) |
| **Inclui** | Copiar prompt Cursor Â· JSON seguro Â· print via `reverse` |
| **Migrate** | **NÃƒO** |
| **Risco** | Baixo â€” sÃ³ tela Bugs |
| **Autorizar** | *pode subir bugs prompt / produÃ§Ã£o* + **99738595** |

### ðŸ”§ Dispenser â€” folhas sem Â«memÃ³ria cheiaÂ» (`DSP-FOLHA-CLOUD` Â· **teste v13.63**)

| Item | Detalhe |
| ---- | ------- |
| **Verify** | âœ… **PASS** Â· commit **`0057e69`** |
| **Fix** | Folhas sÃ³ RAM + Postgres Â· apaga `dsp_folhas_v1` Â· save espera API |
| **Pacote** | ver **DSP-MIX** acima |

### âœ… Deploy loja **v13.39** â€” LOTE CHECKLIST (03/08 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live** Â· `producao` @ **`94112a8`** Â· badge **13.39** |
| **Rollback** | `rollback/pre-lote-checklist-v13.38` (@ v13.36) |

### âœ… LOJA â€” **HIST-REVERTER-PIN** (**v13.36** Â· 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **absorvido** Â· loja agora **v13.39** Â· (este pacote foi **v13.36** @ `7cb3695`) |
| **Antes** | **v13.35** @ `87a5a78` |
| **Push** | `deploy/hist-reverter-pin-v13:producao` (autorizado frase + senha) |
| **O quÃª** | âœ• reverte contagem Â· PIN real / SESSAO Â· API `/api/deletar-ajuste/` |

### ðŸ“¦ CHECKLIST ÃšNICO â€” histÃ³rico (02/08 Â· superado por v13.39)

| Ordem | Pacote | Status |
| ----- | ------ | ------ |
| â€” | **AJUSTE-HIST-UX** (**v13.35**) | âœ… absorvido |
| **1** | **HIST-REVERTER-PIN** (**v13.36**) | âœ… absorvido Â· loja **v13.39** |

### ðŸ”§ HistÃ³rico â€” X reverte contagem (PIN) (**teste** Â· 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | âœ• pedia PIN `1234` e a API `/api/deletar-ajuste/` **nÃ£o existia** |
| **Fix** | PIN real (RH) ou `SESSAO` Â· API Â· saldo volta Â· sÃ³ `ajuste_pin` |
| **Arquivos** | `historico_ajustes.html` Â· `views.py` Â· `urls.py` |
| **Loja** | pacote **v13.36** pronto |

### âœ… LOJA â€” **AJUSTE-HIST-UX** (**v13.35** Â· 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **na loja** Â· `producao` @ **`87a5a78`** Â· badge **v13.35** |
| **Antes** | **v13.26** @ `c3ec890` |
| **Push** | `deploy/ajuste-hist-ux-v13:producao` (autorizado frase + senha) |
| **Rollback** | `git push origin rollback/pre-ajuste-hist-ux-v13:producao` â†’ volta **v13.26** |
| **O quÃª** | Bip+1 mantÃ©m card Â· Hist. cards Â· Hist. sem barra BI Â· GM (nÃ£o AGROE) |
| **Migrate** | **NÃƒO** |
| **VocÃª** | Ctrl+F5 Â· badge **13.35** Â· Bip+1 Â· Hist. sem barra Â· GM no card |

### ðŸ“¦ CHECKLIST ÃšNICO â€” apÃ³s envio (02/08)

**Loja hoje:** badge **v13.35** Â· `producao` @ `87a5a78`  
**Migrate:** NÃƒO

| Ordem | Pacote | Status |
| ----- | ------ | ------ |
| â€” | **AJUSTE-MOBILE-UX** (**v13.26**) | âœ… absorvido |
| **1** | **AJUSTE-HIST-UX** (**v13.35**) | âœ… **na loja** |

### ðŸ”§ HistÃ³rico â€” GM no card (nÃ£o ID AGROE) (**v13.33** Â· 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Card do Hist. mostrava `AGROEâ€¦` em vez do GM |
| **Fix** | Grava GM no ajuste PIN Â· Hist. busca GM no catÃ¡logo se antigo Â· nunca mostra ID longo |
| **Arquivos** | `views.py` Â· `historico_ajustes.html` Â· `mobile_ajuste.html` |
| **VocÃª** | Ctrl+F5 Hist. Â· deve aparecer **GM4000** (etc.) |

### ðŸ”§ HistÃ³rico â€” sem barra azul do BI (**teste** Â· 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Hist. cobria com sidebar do BI â€” nÃ£o dava pra ler |
| **Fix** | `/historico/` = tela cheia (igual Ajuste) Â· nÃ£o monta shell Â· link `target=_top` |
| **Arquivos** | `historico_ajustes.html` Â· `_agro_open_external.html` Â· `dashboard_gerencial.html` Â· `mobile_ajuste.html` |
| **Migrate** | NÃƒO |
| **VocÃª** | Ctrl+F5 Â· Hist. Â· **sem** barra azul Â· cards legÃ­veis |

### ðŸ”§ HistÃ³rico de ajustes â€” layout celular (**teste** Â· 02/08)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | `/historico/` em **cards** (nÃ£o tabela larga) Â· Voltar ao Ajuste em destaque |
| **Arquivo** | `historico_ajustes.html` (+ select_related no view) |
| **Migrate** | NÃƒO |
| **Loja** | **ainda nÃ£o** |
| **VocÃª** | Ctrl+F5 Hist. no celular Â· lista legÃ­vel Â· Voltar |

### ðŸ”§ Ajuste Mobile â€” Bip+1 mantÃ©m produto na tela (**v13.29** Â· 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | ApÃ³s +1 o card sumia â€” sÃ³ dava pra Ajustar pelo verde |
| **Fix** | Campo limpo (anti-cola EAN) Â· lista mostra **Ãºltimo bipado** clicÃ¡vel |
| **Arquivo** | **sÃ³** `mobile_ajuste.html` |
| **Migrate** | NÃƒO |
| **Teste** | `1ca5de9` Â· badge **13.29** |
| **Loja** | **ainda nÃ£o** |
| **VocÃª** | Ctrl+F5 `/ajuste-mobile/` Â· Bip+1 ON Â· bip Â· card fica Â· clique no card Â· 2Âº bip sem cola |

### ðŸ”§ DF-e â€” consulta SEFAZ off no runserver local (02/08)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Buscar / chave Dist **nÃ£o** falam com a Receita no PC local |
| **Por quÃª** | Mesmo CNPJ da loja â†’ 656 |
| **Loja** | Continua normal (RENDER) |
| **ExceÃ§Ã£o** | `NFE_DIST_DFE_PERMITIR_LOCAL=true` no `.env` |
| **Arquivos** | `sefaz_dfe_client.py` Â· status API Â· `entrada_nota.html` |

### âœ… Deploy loja **v13.26** â€” AJUSTE-MOBILE-UX (02/08 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** Â· `producao` @ **`c3ec890`** Â· Render auto |
| **Base** | loja **v13.11** @ `8d7f38b` |
| **Branch** | `deploy/ajuste-mobile-ux-v13` |
| **Rollback** | `git push origin rollback/pre-ajuste-mobile-ux-v13:producao` (@ **`8d7f38b`** / v13.11) |
| **Inclui** | Conferir/data Â· numpad BT Â· modal compacto Â· Hist. 200 Â· Scan estÃ¡vel |
| **Migrate** | **NÃƒO** |
| **NÃƒO** | merge `teste` Â· PDV/caixa |
| **VocÃª agora** | Esperar Live Â· Ctrl+F5 `/ajuste-mobile/` Â· badge **13.26** Â· 1 contagem Â· Hist. Â· Scan |

### â­ PRÃ“XIMO CHAT â€” deploy loja **AJUSTE-MOBILE-UX** (preparado 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** â€” ver bloco Deploy loja **v13.26** acima |
| **Rollback** | `git push origin rollback/pre-ajuste-mobile-ux-v13:producao` |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Ajuste Mobile UX (`AJUSTE-MOBILE-UX` Â· **v13.26**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado Â·** `producao` @ `c3ec890` |
| **Base loja** | era **v13.11** @ `8d7f38b` |
| **Deploy** | `deploy/ajuste-mobile-ux-v13` @ `c3ec890` |
| **Rollback** | `rollback/pre-ajuste-mobile-ux-v13` |
| **O quÃª** | (1) Â«ConferirÂ» nÃ£o apaga data Â· (2) numpad BT Â· (3) modal compacto Â· (4) Hist. Ãºltimos 200 Â· (5) Scan estÃ¡vel |
| **Migrate** | **NÃƒO** |

### ðŸ“¦ CHECKLIST ÃšNICO â€” pronto envio (02/08)

**Loja hoje:** badge **v13.26** Â· `producao` @ `c3ec890` (Render a finalizar)  
**Migrate:** NÃƒO

| Ordem | Pacote | Status |
| ----- | ------ | ------ |
| â€” | Lotes atÃ© **v13.11** | âœ… **jÃ¡ na loja** |
| **1** | **AJUSTE-MOBILE-UX** (**v13.26**) | âœ… **enviado** `c3ec890` |

### ðŸ”§ Ajuste Mobile â€” Hist. trava + Scan cÃ¢mera (**v13.21** Â· 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Hist.** | `/historico/` carregava **todos** os ajustes â†’ Chrome travava Â· agora **Ãºltimos 200** + botÃ£o Voltar ao Ajuste |
| **Scan** | Limpa cÃ¢mera ao fechar/reabrir Â· evita double-start Â· lib pinada 2.3.8 Â· mensagem se sem permissÃ£o |
| **Arquivos** | `views.py` Â· `historico_ajustes.html` Â· `mobile_ajuste.html` |
| **Migrate** | NÃƒO |
| **Pacote** | incluso em **AJUSTE-MOBILE-UX** |

### ðŸ”§ Ajuste Mobile â€” teclado numÃ©rico na tela (Bluetooth Â· **v13.16** Â· 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Leitor BT = teclado fÃ­sico â†’ celular **nÃ£o abre** teclado na hora de digitar qtd |
| **Fix** | Numpad grande no modal (1â€“9 Â· 0 Â· . Â· âŒ« Â· Limpar) Â· campo qtd sÃ³ leitura |
| **Arquivo** | **sÃ³** `mobile_ajuste.html` (+ VERSION) |
| **Migrate** | NÃƒO |
| **Teste** | `teste` **v13.18** (texto explicativo do numpad **removido**) |
| **Loja** | **ainda nÃ£o** â€” frase + senha |
| **VocÃª** | Ctrl+F5 `/ajuste-mobile/` Â· abrir produto Â· digitar qtd nos botÃµes Â· Somar/Trocar |
| **Dica Android** | Ajustes â†’ Teclado fÃ­sico â†’ **mostrar teclado na tela** (se quiser soft keyboard em outros campos) |

### ðŸ”§ Ajuste Mobile â€” Â«ConferirÂ» apaga data logo apÃ³s contar (**v13.14** Â· 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Contou hÃ¡ minutos Â· lista laranja **Conferir** (deveria ser `dd/mm/aa`) |
| **Causa** | Refresh de saldos vinha **sem** data e **zerava** a data local; catÃ¡logo slim tambÃ©m sobrescrevia |
| **Fix** | API vazia **nÃ£o** apaga data boa Â· ao trocar catÃ¡logo **mantÃ©m** contagem da sessÃ£o |
| **Arquivo** | **sÃ³** `mobile_ajuste.html` (+ VERSION) |
| **Migrate** | NÃƒO |
| **Teste** | `teste` v**13.14** Â· push `origin/teste` |
| **Loja** | **ainda nÃ£o** â€” falta frase + senha |
| **VocÃª** | Ctrl+F5 `/ajuste-mobile/` Â· 1 contagem Â· deve ficar **data de hoje**, nÃ£o Conferir Â· confira se a loja (Centro/Vila) Ã© a mesma da contagem |

### âœ… Ajuste Mobile â€” bip nÃ£o cola 2 EANs (**v13.11** Â· 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v13.11** Â· `producao` @ **`8d7f38b`** Â· Render auto |
| **O quÃª** | 2Âº bip **apaga** o cÃ³digo anterior no campo (nÃ£o fica EAN+EAN) |
| **Arquivo** | **sÃ³** `mobile_ajuste.html` (+ VERSION) |
| **Migrate** | NÃƒO |
| **Branch** | `deploy/ajuste-bip-clear-v13.11` |
| **Rollback** | `git push origin rollback/pre-v1311-ajuste-bip:producao` (@ **`ac5dfb3`** / v13.10) |
| **Risco** | **Muito baixo** â€” sÃ³ Ajuste Mobile Â· PDV/caixa intactos |
| **VocÃª** | Ctrl+F5 `/ajuste-mobile/` Â· bip 1 Â· bip 2 Â· campo sÃ³ com o 2Âº |

### âœ… Deploy loja **v13.10** â€” Etiquetas presets Postgres (`ETQ-PRESET-PG` Â· 01/08 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** Â· `producao` @ **`ac5dfb3`** Â· badge **13.10** Â· Render auto |
| **Base** | loja **v13.04** @ `72c6b6c` |
| **Branch** | `deploy/etq-preset-pg-v13.10` |
| **Rollback** | `git push origin rollback/pre-v1310-etq-preset:producao` (@ **`72c6b6c`** / v13.04) |
| **Inclui** | Presets etiqueta no Postgres Â· multi-PC Â· migrate `0079` |
| **Migrate** | **SIM** `0079_etiqueta_preset_agro` (Render no boot) |
| **NÃƒO** | merge `teste` Â· PDV/caixa |
| **VocÃª agora** | Esperar deploy Live Â· Ctrl+F5 Â· badge **13.10** Â· no PC do Â«box raÃ§Ã£oÂ» abrir etiquetas **logado** 1Ã— Â· outros PCs devem ver |

### ðŸ”§ Etiquetas â€” presets no Postgres multi-PC (`ETQ-PRESET-PG` Â· 01/08)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Preset Â«box raÃ§Ã£oÂ» (e qualquer novo) ficava **sÃ³ no PC** (`localStorage`) â€” viola roteiro Â§0.1 |
| **Fix** | Tabela `EtiquetaPresetAgro` Â· API `/api/produtos/etiquetas/presets/` Â· JS grava/lÃª PG Â· migrate **1Ã—** o que jÃ¡ estava no browser |
| **Migrate** | **SIM** `0079_etiqueta_preset_agro` |
| **Arquivos** | `models` Â· `0079` Â· `views`/`urls` Â· `produtos_etiquetas*.js` Â· template Â· `banana-roteiro` Â§0.1 |
| **VocÃª** | No PC que tem Â«box raÃ§Ã£oÂ»: login Â· abrir `/produtos/etiquetas/` Â· Ctrl+F5 Â· deve subir sozinho Â· outros PCs passam a ver |
| **Teste** | `9fbafdc` Â· badge **13.10** Â· push `origin/teste` |
| **Loja** | âœ… **v13.10** @ `ac5dfb3` Â· rollback `rollback/pre-v1310-etq-preset` |

### ðŸ”§ DF-e â€” faixa verde Cursor desatualizada apÃ³s Buscar (01/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | AvanÃ§ado dizia **2092** Â· faixa verde ainda **2086** |
| **Fix** | ApÃ³s Buscar, atualiza Cursor na faixa verde |
| **Arquivo** | `entrada_nota.html` |

### âœ… Deploy loja **v13.04** â€” CP-BUSCA-FORN + DFE-NSU-656 (01/08 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado Â· Live** Render `dep-d9n49nvlk1mc738vjmng` Â· badge **13.04** Â· `producao` @ **`72c6b6c`** |
| **Base** | loja **v12.95** @ `87aa52b` |
| **Branch** | `deploy/cp-dfe-lote-v13.04` |
| **Rollback** | `git push origin rollback/pre-cp-dfe-lote-v13.04:producao` (@ **`87aa52b`** / v12.95) |
| **Inclui** | CP busca fornecedor (cÃ³digo no nome + e-mail @) Â· DFE NSU no 656 |
| **Migrate** | **NÃƒO** |
| **NÃƒO** | merge `teste` Â· PDV/caixa |
| **VocÃª agora** | Ctrl+F5 Â· badge **13.04** Â· CP `Renan Hinnen 1403` Â· Entrada NF SEFAZ 1 Buscar |

### â­ PRÃ“XIMO CHAT â€” deploy loja **CP + DFE lote v13.04** (preparado 01/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** â€” ver bloco Deploy loja **v13.04** acima |
| **O quÃª** | **CP-BUSCA-FORN** + **DFE-NSU-656** (um restart) |
| **Branch** | `deploy/cp-dfe-lote-v13.04` @ **`72c6b6c`** â†’ `producao` |
| **Rollback** | `git push origin rollback/pre-cp-dfe-lote-v13.04:producao` |
| **Migrate** | **NÃƒO** |
| **Risco PDV/caixa** | **Baixo** â€” CP sÃ³ busca lista Â· DFE sÃ³ cursor NSU no 656 Â· **nÃ£o** mexe venda/caixa/finalize |
| **Provas** | DFE `verify_dfe_nsu_656.py` **12/12 VERIFY_OK** Â· CP smoke Q `1403`+e-mail Â· compile OK Â· cherry sÃ³ 8 arquivos |
| **VocÃª autoriza** | ~~Lojas pausam Â· frase+senha~~ âœ… **autorizado 01/08** |
| **Depois** | Ctrl+F5 Â· badge **13.04** Â· CP `Renan Hinnen 1403` Â· Entrada NF SEFAZ 1 Buscar |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Dist DF-e cursor 656 adota NSU maior (`DFE-NSU-656` Â· **v13.04**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado Â· Live** loja v13.04 Â· `72c6b6c` Â· `dep-d9n49nvlk1mc738vjmng` |
| **Fix** | No 656 **da SEFAZ**, se ultNSU **maior** â†’ grava Â· **nunca** maxNSU Â· **nunca** pra trÃ¡s Â· Aguarde local **nÃ£o** grava NSU |
| **Arquivos** | `dfe_inbox_util.py` Â· `sefaz_dfe_client.py` Â· `scripts/verify_dfe_nsu_656.py` |
| **Migrate** | NÃƒO |
| **Prova** | `verify_dfe_nsu_656.py` â†’ **12/12 VERIFY_OK** |
| **VocÃª** | deploy Â· 1 Buscar (pode 656) â†’ cursor sobe se SEFAZ mandar Â· 1h Â· Buscar de novo Â· buraco â†’ chave |
| **Zap** | *AtualizaÃ§Ã£o rÃ¡pida Entrada NF / SEFAZ (~1 min)* |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Contas a pagar busca fornecedor (`CP-BUSCA-FORN` Â· **v13.03**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado Â· Live** loja v13.04 Â· `72c6b6c` Â· `dep-d9n49nvlk1mc738vjmng` |
| **VERSION** | **13.03** (no lote sobe como **13.04** com DFE) Â· commit origem `006f871` |
| **Inclui** | NÃºmero no nome (`Renan Hinnen 1403`) Â· quem lanÃ§ou sÃ³ com `@` Â· ajuda `?` |
| **Arquivos** | `lancamentos_financeiro_pg_util.py` Â· `mongo_financeiro_util.py` Â· `lancamentos_help_agents.html` Â· AGENTS Â§10 |
| **Migrate** | **NÃƒO** |
| **Risco** | **Baixo** â€” sÃ³ busca lista CP Â· nÃ£o mexe pagar/PDV/caixa |
| **Prova 01/08** | smoke Q `1403` casa cliente Â· Renan sem @ nÃ£o polui Â· e-mail com @ ok |
| **NÃƒO** | merge inteiro `teste` |
| **Zap loja** | *AtualizaÃ§Ã£o rÃ¡pida Contas a pagar (~1â€“2 min)* |

### âœ… Deploy loja **v12.95** â€” BI-KPI-LOJA (01/08 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado Â· Live** Render `dep-d9n2huht0dsc738q0m60` Â· badge **12.95** Â· `producao` @ **`87aa52b`** |
| **Base** | loja **v12.88** @ `941446d` |
| **Branch** | `deploy/bi-kpi-loja-v12.95` |
| **Rollback** | `git push origin rollback/pre-bi-kpi-loja-v12.95:producao` (@ **`941446d`** / v12.88) |
| **Inclui** | KPIs por loja Â· % vs mesmo dia da semana Â· Validade alinhada ao relatÃ³rio |
| **Migrate** | **NÃƒO** |
| **NÃƒO** | merge `teste` Â· PDV/caixa Â· FL-058 |
| **VocÃª agora** | Ctrl+F5 home Â· badge **12.95** Â· Centro/Vila Â· Validade Â· selos Â«vs 1Âº â€¦Â» |

### â­ PRÃ“XIMO CHAT â€” deploy loja **BI-KPI-LOJA v12.95** (preparado 01/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** â€” ver bloco Deploy loja **v12.95** acima |
| **O quÃª** | SÃ³ BI home: loja nos KPIs Â· % vs mesmo dia da semana Â· Validade alinhada |
| **Branch** | `deploy/bi-kpi-loja-v12.95` @ **`87aa52b`** â†’ `producao` |
| **Rollback** | `git push origin rollback/pre-bi-kpi-loja-v12.95:producao` |
| **Migrate** | NÃƒO |
| **Risco PDV/caixa** | **NÃ£o piora** â€” pacote **nÃ£o** toca venda/caixa/NF/estoque |
| **Provas** | Validade **15/15 VERIFY_OK** Â· cherry sÃ³ BI Â· compile OK |
| **VocÃª autoriza** | ~~Lojas pausam Â· frase+senha~~ âœ… **autorizado 01/08** |
| **Depois** | Ctrl+F5 Â· badge **12.95** Â· home Centro/Vila Â· Validade |

### ðŸ”§ BI â€” card Validade 0/0 com lotes no relatÃ³rio (01/08 Â· **teste v12.93** Â· VERIFY_OK)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | RelatÃ³rio validade mostra vencidos / no mÃªs; card BI Centro **e** Vila ficavam **0 / 0** |
| **Causa** | Contagem por loja exigia sÃ³ saldo operacional e pegava **1 data** por produto; ignorava lote com qtd |
| **Fix** | `_contagem_validade_dashboard_por_loja` Â· cache `v4` Â· lote qtd>0 + todas as datas |
| **Prova** | `scripts/verify_validade_bi.py` **15/15 VERIFY_OK** (01/08) |
| **VocÃª** | Ctrl+F5 Â· badge **12.95** Â· Validade deve espelhar o relatÃ³rio |

### ðŸ“¦ PACOTE PRONTO LOJA â€” BI loja + KPIs + Validade (`BI-KPI-LOJA` Â· **v12.95**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado Â· Live** `dep-d9n2huht0dsc738q0m60` Â· `producao` @ **`87aa52b`** |
| **VERSION** | **12.95** (loja hoje **12.88**) |
| **Branch deploy** | `deploy/bi-kpi-loja-v12.95` @ **`87aa52b`** |
| **Rollback** | `rollback/pre-bi-kpi-loja-v12.95` @ **`941446d`** (v12.88) |
| **Inclui** | Filtro Centro/Vila em Vendas/Performance Â· % Vendas/Ticket/Novos vs **mesmo dia da semana do mÃªs ant.** Â· Novos = **hoje** Â· card Validade por loja alinhado ao relatÃ³rio (lote qtd + todas as datas) Â· Â«por unidadeÂ» compara as duas |
| **Arquivos** | `views.py` (sÃ³ dashboard) Â· `dashboard_vendas_historico_util.py` Â· `dashboard_gerencial_body.html` Â· `tests_validade_dashboard.py` Â· `scripts/verify_validade_bi.py` Â· VERSION |
| **Migrate** | **NÃƒO** |
| **Risco loja aberta** | **Baixo** â€” **sÃ³ BI `/`** Â· **nÃ£o** mexe PDV Â· caixa Â· Entrada NF Â· estoque Â· venda Â· finalize |
| **Prova 01/08** | `scripts/verify_validade_bi.py` â†’ **15/15 VERIFY_OK** Â· compile views OK Â· cherry limpo (sÃ³ conflito banana/VERSION resolvido) |
| **NÃƒO** | merge inteiro `teste` Â· FL-058 (ainda nÃ£o feito) Â· ENTRADA-NF-CUSTO |
| **PrÃ³ximo chat** | 1) lojas **pausam vendas** 2) *pode subir BI-KPI-LOJA / produÃ§Ã£o* + **99738595** 3) FF `deploy/bi-kpi-loja-v12.95` â†’ `producao` Â· Ctrl+F5 home Â· badge **12.95** Â· Validade â‰  0 se relatÃ³rio tem Â· selos Â«vs 1Âº â€¦Â» |
| **Zap loja** | *AtualizaÃ§Ã£o ~2 min â€” nÃ£o finalize venda agora* |

### ðŸ”§ BI â€” Ticket + Novos Clientes vs mesmo dia semana (01/08 Â· **teste v12.92**)

| Item | Detalhe |
| ---- | ------- |
| **Ticket** | % MÃªs e Hoje vs ticket do mesmo dia da semana do mÃªs ant. |
| **Novos Clientes** | NÃºmero = **hoje** Â· % vs cadastros no mesmo dia da semana do mÃªs ant. (30d sÃ³ no Â«?Â») |
| **VocÃª** | Ctrl+F5 Â· selos Â«vs 1Âº sÃ¡b.Â» nos 3 cards |

### ðŸ”§ BI â€” Vendas % vs mesmo dia da semana do mÃªs ant. (01/08 Â· **teste v12.91**)

| Item | Detalhe |
| ---- | ------- |
| **Antes** | Card Vendas: % vs **ontem** |
| **Agora** | % vs **mesma ocorrÃªncia** do dia da semana no mÃªs anterior (ex. 01/08 1Âº sÃ¡b. â†’ 1Âº sÃ¡b. de jul) |
| **VocÃª** | Ctrl+F5 Â· badge do card Vendas tipo Â«vs 1Âº sÃ¡b.Â» Â· tooltip com data (ex. 04/07) |

### ðŸ”§ BI â€” Vendas/Performance filtrados pela loja (01/08 Â· **teste v12.90**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Com Loja=Centro, card Vendas e Total da Performance somavam Centro+Vila |
| **Fix** | Total/KPI pela loja do aparelho Â· Â«por unidadeÂ» continua comparando as duas Â· cache meta/pdv v8/v6 Â· selo Â«Â· CentroÂ» no Total |
| **VocÃª** | Local Ctrl+F5 Â· Loja Centro â†’ Vendas â‰ˆ barra Centro Â· Total Performance â‰ˆ mesma Â· barras ao lado ainda mostram as duas |

### âœ… Deploy loja **v12.88** â€” lote checklist (01/08 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** `producao` @ **`941446d`** Â· badge **12.88** Â· aguardar Live Render |
| **Base** | loja **v12.51** @ `b977c9b` |
| **Branch** | `deploy/lote-checklist-v12.88-01ago` |
| **Rollback** | `git push origin rollback/pre-lote-checklist:producao` (@ **`b977c9b`** / v12.51) |
| **Inclui** | ETQ-GM Â· BUG-FAB Â· PDV-USO-UX Â· PDV-USO-BRINDE Â· DFE-CHAVE Â· AJUSTE-MOBILE |
| **Migrate** | **SIM** â€” `0076` + `0077` + `0078` (no build Render) |
| **NÃƒO** | merge `teste` Â· slim API (jÃ¡ na loja) |
| **VocÃª agora** | Ctrl+F5 PDVs Â· badge **12.88** Â· 1 venda Â· Uso loja Â· `/ajuste-mobile/` PIN+lista Â· etiquetas |

### ðŸ“¦ CHECKLIST ÃšNICO â€” pÃ³s-deploy (01/08) Â· **substituÃ­do**

> **Vigente:** bloco **CHECKLIST ÃšNICO â€” pronto envio (03/08)** no topo (BUGS-F10-FRETE Â· ETQ-LOTE-A4 Â· RELAT-INVENTARIO).

**Loja na Ã©poca:** badge **v13.04** Â· depois subiu **v13.10** / **v13.11**.  
Fila 1â€“9 abaixo = **jÃ¡ enviada** (arquivo histÃ³rico).

| Ordem | Pacote | Status |
| ----- | ------ | ------ |
| â€” | Lote **v12.51** | âœ… enviado antes |
| 1 | **AJUSTE-MOBILE** | âœ… **enviado loja v12.88** Â· migrate `0078` |
| 2 | **BUG-FAB-BARRA** | âœ… **enviado loja v12.88** |
| 3 | **PDV-USO-LOJA-UX** | âœ… **enviado loja v12.88** Â· migrate `0076` |
| 4 | **PDV-USO-LOJA-BRINDE** | âœ… **enviado loja v12.88** Â· migrate `0077` |
| 5 | **DFE-CHAVE** | âœ… **enviado loja v12.88** |
| 6 | **ETQ-GM-CAMPOS** | âœ… **enviado loja v12.88** |
| 7 | **BI-KPI-LOJA** | âœ… **enviado Â· Live** loja v12.95 Â· `87aa52b` |
| 8 | **CP-BUSCA-FORN** | âœ… **enviado Â· Live** loja v13.04 Â· `72c6b6c` |
| 9 | **DFE-NSU-656** | âœ… **enviado Â· Live** loja v13.04 Â· `72c6b6c` |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Dist DF-e baixar pela chave (`DFE-CHAVE` Â· **v12.71**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.88** Â· no lote `941446d` |
| **O quÃª** | Aba SEFAZ: campo chave 44 + **Baixar pela chave** Â· grava Pendentes Â· **nÃ£o** mexe no ultNSU |
| **API** | `POST /api/entrada-nota/dfe-por-chave/` |
| **Migrate** | NÃƒO (usa tabelas `0071`/`0072` jÃ¡ na loja) |
| **Base loja** | v12.51+ (DFE-INBOX) |
| **Prova** | rota Â· consChNFe Â· upsert sem gravar NSU Â· POST 400 chave curta Â· detalhe inbox Â· cursor estÃ¡vel |
| **Arquivos** | `views.py` Â· `urls.py` Â· `entrada_nota.html` Â· `sefaz_dfe_client.py` Â· `dfe_inbox_util.py` |
| **VocÃª** | deploy Â· fim do Aguarde 1h Â· colar chave Â· Baixar Â· Carregar na grade |
| **NÃƒO** | Usar no meio do timer 656 |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Bug ðŸž na barra GestÃ£o (`BUG-FAB-BARRA` Â· **v12.55**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.88** Â· no lote `941446d` |
| **O quÃª** | Na GestÃ£o o ðŸž fica centrado na barra azul Â· PDV sem barra = igual |
| **Migrate** | NÃƒO |
| **Commits** | `6905682` |
| **Base loja** | v12.51 (jÃ¡ tem BUG-REPORT) |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Uso loja bip + Outros (`PDV-USO-LOJA-UX` Â· **v12.55**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.88** Â· no lote `941446d` Â· migrate `0076` |
| **O quÃª** | Bip adiciona direto na lista Â· motivo **Outros** com campo livre |
| **Migrate** | SIM â€” `produtos.0076` |
| **Commits** | `b0645b0` Â· `ecc1adc` |
| **Base loja** | v12.51 (jÃ¡ tem PDV-USO-LOJA) |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Brinde cliente + F8 BÃ´nus (`PDV-USO-LOJA-BRINDE` Â· **v12.57**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.88** Â· no lote `941446d` Â· migrate `0077` |
| **O quÃª** | Motivo **Brinde cliente** â†’ busca cliente Â· grava no PG Â· F8 aba **BÃ´nus** (sai MÃ©tricas) |
| **Migrate** | SIM â€” `produtos.0077` |
| **Commits** | `8e8c5f4` (+ docs `fc61102`) |
| **Base loja** | v12.51+ Â· ideal apÃ³s **PDV-USO-LOJA-UX** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Dist DF-e caixa de entrada (`DFE-INBOX` Â· **v12.51**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.51** Â· Live `dep-d9mbqn8jo6nc73bkhrl0` |
| **O quÃª** | Buscar notas novas grava XML no PG Â· Pendentes (antigaâ†’nova) / ConcluÃ­das Â· ~80 Â· cursor sÃ³ 137/138 Â· NFC_E fallback Â· baixar por chave |
| **Migrate** | SIM â€” `produtos.0071` + `0072` (tabelas cursor/documento) |
| **Commits chave** | `07d9973` Â· `96629a1` Â· `cde8d0e` (+ docs banana) |
| **Prova local** | config OK Â· 2 pendentes FIFO Â· status/inbox/detalhe HTTP Â· trava 656 Â· cursor 2090 estÃ¡vel |
| **Loja apÃ³s deploy** | migrate Â· Ctrl+F5 Entrada NFâ†’SEFAZ Â· Buscar **ou** recuperar pelas chaves (Saframil/AstÃºrias) â€” local **nÃ£o** copia pra loja |
| **NÃƒO** | Pular NSU Â· spam Consultar |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Bug report flutuante (`BUG-REPORT` Â· **v12.49**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.51** Â· Live |
| **O quÃª** | BotÃ£o ðŸž flutuante Â· Alt+B Â· print automÃ¡tico Â· lista F10 GestÃ£o â†’ Bugs Â· PG |
| **Safe-zone** | Empurra rodapÃ©s (Voltar) Â· overlay â†’ canto direito Â· **GestÃ£o:** ðŸž centrado na barra azul Â· PDV sem barra = canto livre |
| **Migrate** | SIM â€” `produtos.0074` (`BugReportAgro` + `DispositivoLojaAgro`) |
| **Prova** | URLs/API criar+status+print+lista OK Â· form abre no shell Â· WhatsApp CallMeBot se configurado |
| **Base** | v12.32â€“12.49 |

### âœ… Uso loja â€” quem levou grade RH (**v12.31** Â· 31/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Pop Â«Quem levou?Â» = botÃµes de todos os funcionÃ¡rios ativos (RH) Â· **Outros** abre campo Â· toque no nome **avanÃ§a sozinho** |
| **API** | `meta` devolve `funcionarios` |
| **Prova** | Ctrl+F5 PDV Â· Uso loja Â· Confirmar saÃ­da Â· ver grade Â· clicar nome â†’ motivo |

### âœ… Uso loja â€” Enter = Confirmar saÃ­da (**v12.34** Â· 31/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Na lista da saÃ­da, **Enter** dispara Confirmar saÃ­da (exceto busca / autocomplete / pops) |

### âœ… Uso loja â€” Voltar nos pops (**v12.36** Â· 31/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | BotÃ£o **â† Voltar** em quem/motivo/PIN Â· Esc = mesmo (passo anterior; no 1Âº fecha o pop) |

### âœ… Uso loja â€” totais custo/venda no histÃ³rico (**v12.38** Â· 31/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | RodapÃ© HistÃ³rico: tags **Centro** e **Vila** com total custo + total venda (saÃ­das nÃ£o estornadas) |
| **Migrate** | `produtos.0075` (preÃ§o snapshot no item) |

### ðŸ“¦ PACOTE PRONTO LOJA â€” RelatÃ³rio vendas por marca (`RELAT-VENDAS-MARCA` Â· **v12.22**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.51** Â· Live |
| **O quÃª** | Card **Vendas por marca** Â· ordenar **valor total** / **quantidade** Â· Excel â†“ Â· ajuda ? |
| **Prova** | Totais = vendas por grupo Â· ordenar valorâ‰ qtd Â· view/xlsx/hub OK |
| **Migrate** | NÃƒO |
| **Arquivos** | `relatorios_vendas_util.py` Â· `relatorios_central_views.py` Â· hub/generico/help Â· `urls.py` |

### ðŸ“¦ PACOTE PRONTO LOJA â€” PDV Uso loja (`PDV-USO-LOJA` Â· **v12.48**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.51** Â· Live |
| **O quÃª** | Uso loja no PDV Â· quem grade RH Â· motivo toque avanÃ§a Â· Enter confirma Â· Voltar Â· hist. em linha Â· totais custo/venda por loja (Centro azul / Vila laranja) Â· PIN Â· estorno Â· kardex PG |
| **Migrate** | SIM â€” `estoque.0014` + `produtos.0073` + `0074` (cadeia) + `0075` (preÃ§os item) |
| **Prova** | URLs/meta/hist/totais OK Â· itens com ajuste Â· motivos alinhados Â· `{}` â†’ pede PIN |
| **Base** | v12.18â€“12.48 |

### ðŸ“¦ PACOTE â€” PDV Uso loja (**v12.18** Â· 31/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ver **PACOTE PRONTO PDV-USO-LOJA v12.48** |
| **O que Ã©** | BotÃ£o **Uso loja** no PDV Â· overlay (padrÃ£o Folha saldo) Â· busca = motor PDV Â· lista prÃ³pria Â· PIN Â· histÃ³rico + estorno |
| **Estoque** | Baixa no **Postgres** (`AjusteRapidoEstoque` origem `uso_loja`) Â· caixa aberto = depÃ³sito do turno Â· caixa fechado = operador escolhe Centro/Vila |
| **Opcional** | Quem levou = grade RH (toque avanÃ§a) Â· Outros digita Â· motivo Â· vazio = dono do PIN |
| **Kardex** | Aparece como Â«Uso lojaÂ» / Â«Estorno uso lojaÂ» |
| **Migrate** | `estoque.0014` + `produtos.0073` + `0074` + `0075` |
| **Arquivos** | `uso_loja_util.py` Â· `views_uso_loja.py` Â· `pdv_uso_loja.js` Â· `uso_loja_overlay.html` Â· topbar PDV |
| **Testar** | Ctrl+F5 PDV Â· Uso loja Â· quem/motivo toque Â· PIN Â· HistÃ³rico totais Â· estornar Â· kardex |


### âœ… Consertar nomes quebrados cadastro (31/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **gravado loja** Â· **41/41** Â· `broken_left=0` |
| **O quÃª** | Nome/marca/categoria/GM/EAN em PG (+ overlay) Â· **nÃ£o** mexeu custo/venda |
| **Fonte** | Mesmo `produto_externo_id` no staging (cÃ³pia boa) |
| **VocÃª** | Ctrl+F5 Cadastro Â· sumiu Â«NOME QUEBRADOÂ» / Â«Consertar nomeÂ» |


### âœ… Deploy loja **v12.51** â€” lote marca + uso loja + bugs + DF-e (31/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado Â· Live** Render `dep-d9mbqn8jo6nc73bkhrl0` Â· badge **12.51** |
| **Base** | loja **v12.12** @ `9d65e31` |
| **Lote** | `deploy/lote-v12.22-12.51-31jul` @ **`b977c9b`** â†’ `producao` (1 restart) |
| **Inclui** | RELAT-VENDAS-MARCA Â· PDV-USO-LOJA Â· BUG-REPORT Â· DFE-INBOX (API+UI) |
| **Migrate** | **SIM** â€” `estoque.0014` + `produtos.0071`â†’`0075` (no build Render) |
| **NÃƒO inclui** | merge `teste` Â· ENTRADA-NF-CUSTO Â· dispenser/catÃ¡logo delivery |
| **Rollback** | `git push origin rollback/pre-v1251-lote-31jul:producao` (@ **`9d65e31`** / v12.12) |
| **VocÃª agora** | Ctrl+F5 PDVs Â· badge **12.51** Â· smoke: 1 venda Â· Uso loja Â· ðŸž Â· RelatÃ³rios marca Â· Entrada NFâ†’SEFAZ inbox |

### âœ… Deploy loja **v12.12** â€” lote 12.08â€“12.12 (30/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado Â· Live** Render `dep-d9lmqg4s728c739fsgo0` Â· badge **12.12** |
| **Base** | loja **v12.07** @ `adcb750` |
| **Lote** | `deploy/lote-v12.08-12.12-30jul` @ **`9d65e31`** â†’ `producao` (1 restart) |
| **Inclui** | 12.08 lÃ¡pis cache Â· 12.09+12.10 Excel estoque + vÃ­nculo NF PG Â· 12.11 Somar/Trocar Â· 12.12 lembrete entrega |
| **Migrate** | **SIM** â€” `0068` + `0069` (no build Render) |
| **NÃƒO inclui** | Dist DF-e Â· merge `teste` Â· ENTRADA-NF-CUSTO |
| **Rollback** | `git push origin rollback/pre-v1212-lote-30jul:producao` (@ **`adcb750`** / v12.07) |
| **VocÃª agora** | Ctrl+F5 PDVs Â· badge **12.12** Â· smoke: lÃ¡pis preÃ§o Â· lembrete some ao finalizar Â· Ajuste Somar no celular |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Etiquetas GM curto + toggles (`ETQ-GM-CAMPOS` Â· **v12.52**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.88** Â· no lote `941446d` |
| **VERSION alvo** | **12.52** (loja **12.51**) |
| **Inclui** | GM curto (GM0050-1â†’GM50) Â· checkboxes Nome/R$/PreÃ§o/Peso/Logo/GM Â· fonte/cor/layout GM |
| **Arquivos** | `produtos_etiquetas.html` Â· `produtos_etiquetas.js` Â· `produtos_etiquetas_core.js` Â· VERSION |
| **Migrate** | **NÃƒO** |
| **Risco** | **Baixo** â€” sÃ³ `/produtos/etiquetas/` Â· preset antigo **igual** (GM off) Â· zero PDV/caixa |
| **Smoke** | check 0 Â· JS OK Â· fmtGmCurto cases OK Â· preset antigo nÃ£o mostra GM |
| **Autorizar** | *pode subir etiquetas GM / produÃ§Ã£o* + **99738595** |
| **Rollback** | `rollback/pre-v1252-etq-gm` @ HEAD `producao` antes do push |

### ðŸ“¦ PACOTE â€” Lembrete entrega (PDV-LEMBRETE-ENTREGA Â· **v12.12**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.12** Â· no lote `9d65e31` |
| **Branch** | `deploy/pdv-lembrete-entrega-v12.12` @ ffdf13e (incluÃ­da no lote) |


### PACOTE â€” Entrada NF vÃ­nculo + Excel estoque (**v12.10**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.12** Â· no lote `9d65e31` Â· migrate 0068+0069 |
| **Branch** | `deploy/entrada-nf-vinculo-estoque-xlsx-v12.10` @ 9f60fea |


### ðŸ› Fix â€” Lembrete entrega (30/07) â†’ âœ… loja v12.12


### ðŸ“¦ PACOTE â€” Excel estoque Cadastro (**v12.09**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** no lote v12.12 (com vÃ­nculo NF) |


### ðŸ“¦ PACOTE â€” PDV cache lÃ¡pis (**v12.08**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.12** Â· no lote `9d65e31` |
| **Branch** | `deploy/pdv-cache-lapis-v12.08` @ fd14a10 |


### PACOTE â€” Entrada NF custo cadastro (`ENTRADA-NF-CUSTO`)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **teste v13.71** (reaberto 03/08) Â· aguarda validaÃ§Ã£o local Â· **nÃ£o** loja |

### Entrada NF â€” custo cadastro nÃ£o puxa (29/07 Â· reaberto 03/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… fix **v13.71** no `teste` |
| **Sintoma** | Busca mostra venda ok Â· V. unit 0,00 ao incluir |
| **Fix** | overlay sync final Â· JS ignora custo 0 Â· PG em `buscar-produto-id` |
| **VocÃª** | Ctrl+F5 Entrada NF Â· incluir produto com custo no Cadastro |

### ðŸ“¦ PACOTE â€” Ajuste Mobile Somar+catÃ¡logo (`AJUSTE-MOBILE-SOMAR` Â· **v12.11**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.12** Â· no lote `9d65e31` |
| **Branch** | `deploy/ajuste-mobile-somar-v12.11` @ 2c5f66c |
| **Arquivo** | **sÃ³** `mobile_ajuste.html` |

### ðŸ› Ajuste Mobile â€” catÃ¡logo 0 no celular do funcionÃ¡rio (30/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… no pacote **AJUSTE-MOBILE-SOMAR** Â· pronto envio |
| **Sintoma** | Link pro Vitor Â· PIN ok Â· **CAT 0** |
| **Fix** | Erro + Tentar de novo Â· Atualizar baixa lista Â· cache apÃ³s sucesso |

### UX â€” Ajuste Mobile Somar Ã— Trocar (30/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… no pacote **AJUSTE-MOBILE-SOMAR** Â· pronto envio |
| **Fix** | Modal: **Somar** / **Trocar** Â· campo Quantidade vazio ao abrir |

### âœ… Deploy loja **v12.06** â€” AJUSTE-MOBILE-CEL (29/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** Â· push `producao` **`7623c3c`** Â· badge **12.06** Â· aguardar Live Render |
| **Base** | loja **v12.05** @ `30e79a9` |
| **Origem teste** | **`7cc95f5`** (cÃ³digo; banana do teste nÃ£o cherry â€” conflito) |
| **Inclui** | Ajuste Mobile tela cheia celular Â· fora shell BI Â· layout touch |
| **NÃƒO inclui** | DF-e Â· bug report Â· merge `teste` inteiro |
| **Migrate** | **NÃƒO** |
| **Rollback** | `git push origin rollback/pre-v1206-ajuste-mobile:producao` (@ **30e79a9** / v12.05) |
| **Validar agora** | Ctrl+F5 no celular Â· `/ajuste-mobile/` Â· PIN Â· busca Â· 1 contagem Â· BI Â«AjusteÂ» sai do BI Â· badge **12.06** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Ajuste Mobile celular (`AJUSTE-MOBILE-CEL` Â· 29/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v12.06** Â· `producao` **`7623c3c`** Â· teste **`7cc95f5`** |
| **Commit teste** | **`7cc95f5`** Â· branch `teste` |
| **VERSION loja** | **12.06** (era 12.05) |
| **Escopo** | SÃ³ `/ajuste-mobile/` (+ atalho BI / links `target=_top`) Â· **nÃ£o** mexe PDV venda / caixa / CP |
| **Risco loja aberta** | **Baixo** â€” shell/FAB/BI sÃ³ especializam path ajuste-mobile |
| **Inclui** | Tela cheia fora do BI Â· layout celular Â· scroll pÃ¡gina Â· cards/resumo Â· teclado visualViewport |
| **Arquivos** | `mobile_ajuste.html` Â· `ajuste_mobile_login.html` Â· `_agro_open_external.html` Â· `dashboard_gerencial.html` Â· `_agro_pdv_fab.html` Â· `context_processors.py` Â· `sidebar_nav.html` Â· `consulta_produtos_pre_layout_pdv.html` |
| **NÃƒO incluir** | WIP DF-e / bug report / entrada_nota / models / views / *-CAIXA* |

### UX â€” Ajuste Mobile tela cheia no celular (29/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **loja v12.06** Â· `7623c3c` |
| **Pedido** | Layout sÃ³ celular + link prÃ³prio (nÃ£o abrir dentro do BI com barra lateral) |
| **Causa** | Shell de abas (`agro-open-inapp-tab`) engolia `/ajuste-mobile/` Â· URL ficava no BI |
| **Fix** | Sem shell nesta rota Â· rompe iframe Â· BI atalho = `location.assign` full Â· layout touch Â· sem F1/PDV/Display Scale |
| **RevisÃ£o 29/07 b** | Scroll da **pÃ¡gina inteira** Â· tipografia sem inflar Â· teclado (`visualViewport`) |
| **RevisÃ£o 29/07 c** | CabeÃ§alho em **faixas** (estoque sozinho Â· horÃ¡rio + Atualizar na linha de baixo) |
| **RevisÃ£o 29/07 d** | Resumo 1 linha Â· cards compactos Â· tÃ­tulo 2 linhas c/ corte |
| **Link** | `â€¦/ajuste-mobile/` (PIN â†’ mesma URL) |
| **Arquivos** | ver pacote **AJUSTE-MOBILE-CEL** |
| **VocÃª** | Ctrl+F5 no celular Â· rolar Â· buscar Â· 1 contagem Â· BI Â«AjusteÂ» deve sair do BI |
| **NÃ£o** | Usar no PC / abrir pelo shell de abas |

### Ã°Å¸Ââ€º Dist DF-e â€” caixa de entrada salva (~80) (31/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… VERIFY_OK Â· pacote **DFE-INBOX v12.51** no checklist Â· aguarda produÃ§Ã£o |
| **O quÃª** | Lista PG Â· Buscar grava Â· Pendentes FIFO Â· cursor 137/138 |
| **VocÃª** | ProduÃ§Ã£o: frase+senha Â· depois Buscar/chave na loja |

### Ã°Å¸Ââ€º Dist DF-e â€” 656 avanÃ§ava o NSU sem puxar nota (31/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cursor ia 2086â†’2090 no **656** Â· sem XML Â· fio da meada perdido |
| **Causa** | API gravava `ult_nsu` da resposta SEFAZ em qualquer cStat |
| **Fix** | SÃ³ grava cursor em **137/138** Â· no **656** mantÃ©m o salvo Â· UI mostra Â«Cursor lojaÂ» |
| **Local** | Cursor resetado para **2086** |
| **VocÃª** | Ctrl+F5 Â· Atualizar status â†’ 2086 Â· **nÃ£o** Consultar atÃ© passar 1h do 656 Â· nota atrasada â†’ Ler XML |

### Ã°Å¸Ââ€º Dist DF-e â€” certificado na branch `teste` (30/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Aba SEFAZ amarela de novo Â· ultNSU 0 (voltou para `teste` sem o fix) |
| **Fix** | DF-e reusa `NFC_E_*` Â· cursor NSU PG **2086** Â· models + migrate `0071`/`0072` |
| **VocÃª** | Reiniciar runserver Â· Ctrl+F5 Â· **Atualizar status** â†’ verde Â· se 656 nÃ£o clicar |
| **Push** | `origin teste` |

### Ã°Å¸Ââ€º Dist DF-e â€” certificado Â«nÃ£o configuradoÂ» no local (29/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Aba SEFAZ Â· aviso amarelo Â· ultNSU 0 Â· Â«DistribuiÃ§Ã£o DF-e nÃ£o configuradaÂ» |
| **Causa** | Branch sem fallback NFC-e Â· sÃ³ lia `NFE_DIST_DFE_*` (vazio) Â· cursor NSU sÃ³ no Mongo |
| **Fix** | `sefaz_dfe_client` reusa `NFC_E_*` Â· cursor NSU no Postgres/SQLite (`AgroNfeDistDfeCursor` Â· jÃ¡ em **2086**) Â· status/API usam mesmo CNPJ |
| **VocÃª** | Reiniciar runserver Â· Ctrl+F5 Â· **Atualizar status** â†’ deve sumir o amarelo Â· **nÃ£o** pular NSU Â· se 656 â†’ esperar 1h Â· nota urgente â†’ **Ler XML** |
| **NÃ£o** | Consultar SEFAZ vÃ¡rias vezes seguidas Â· saltar ultNSU |


### Entrada NF â€” Sem a pagar + botÃ£o morto + duplicata (29/07 Â· NF 39407344)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **loja v12.04** |
| **Sintoma** | Lista ConcluÃ­da Â«Sem a pagarÂ» Â· tÃ­tulos existem no CP Â· etapa 7 laranja Â· Salvar+a pagar nÃ£o clica Â· reabrir+confirmar duplica |
| **Causa** | Flag `financeiro_lancado` sumiu Â· UI congelada (PIN) bloqueava o botÃ£o Â· gerar de novo sem achar o lote antigo |
| **Fix** | BotÃ£o religa mesmo com PIN Â· sync por NF antes de inserir Â· Reabrir estorna tÃ­tulos Ã³rfÃ£os pela NF |
| **VocÃª** | Ctrl+F5 Â· abrir a nota Â· etapa 7 Â· **Salvar + a pagar** â†’ deve religar (sem 2Âº lote). Na NF jÃ¡ com 6 linhas: apagar 3 duplicadas no CP |
| **NÃ£o** | Reabrir e confirmar todas as etapas Â«sÃ³ pra limpar o laranjaÂ» |

### Contas a pagar â€” busca por nÃºmero da NF (29/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **loja v12.04** |
| **Sintoma** | Digitar `013962` = 0 tÃ­tulos Â· Ctrl+F achava na descriÃ§Ã£o |
| **Causa** | NÃºmero puro 5â€“7 dÃ­gitos ia sÃ³ como **valor** + `numero_documento` Â· NF fica em Â«NF 013962Â» na descriÃ§Ã£o |
| **Fix** | TambÃ©m casa descriÃ§Ã£o/obs (+ variante sem zero Ã  esquerda) |
| **Arquivo** | `lancamentos_financeiro_pg_util.py` Â· ajuda `?` / AGENTS Â§10 |
| **VocÃª** | Ctrl+F5 Contas a pagar Â· busca `013962` â†’ deve listar as parcelas |

### Local â€” catÃ¡logo SQLite atualizado (29/07)

| Item | Detalhe |
| ---- | ------- |
| **Fonte** | staging Oregon (dump Produto+overlay; sem coluna peso_etiqueta lÃ¡ ainda) |
| **Destino** | db.sqlite3 Â· **3361** produtos Â· **819** overlays |
| **.env** | DATABASE_URL continua **comentado** (rÃ¡pido) |
| **VocÃª** | reiniciar 
unserver Â· Ctrl+F5 Â· limpar cache busca se precisar |


### âœ… PDV â€” carrinho itens travados (FL-008 Â· 28/07 Â· loja v11.99)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | GM0110-1 / GM0110-10 (e outros) no carrinho: +/âˆ’, lixeira e lÃ¡pis mortos |
| **Causa** | Lista busca `display:flex` ganhava do `.hidden` â€” cobria o carrinho (mesmo bug v6.16) |
| **Fix** | `pdv_wizard.html` + `pdv_wizard.js` Â· commit **`8f7610d`** |
| **Status** | âœ… **loja OK** â€” Renan validou 28/07 Â· badge **11.99** |
| **Rollback** | `rollback/pre-v1199-pdv-carrinho` â†’ v11.98 |

### Deploy loja **v11.98** â€” lote 11.94-11.98 (28/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** Â· push `producao` **`cb73d87`** Â· badge **11.98** Â· aguardar Live + migrate `0067` |
| **Inclui** | Pix Sicredi (11.94) Â· Entrada NF UX+Nova (11.95) Â· GÃ´ndola+0067 (11.96) Â· Transf forÃ§ada (11.97) Â· Estoque Vila (11.98) |
| **NÃƒO inclui** | DF-e inbox Â· merge inteiro `teste` |
| **Migrate** | **SIM** `0067_produto_overlay_peso_etiqueta` |
| **Rollback** | `git push origin rollback/pre-lote-v1194-1198:producao` (@ **3633011** / v11.93) |
| **Validar agora** | Ctrl+F5 Â· badge **11.98** Â· Pix Sicredi Â· Entrada NF Nova/boleto Â· etiquetas Â· transf forÃ§ada Â· PDV Estoque Vila Â· 1 venda |

### FILA DEPLOY â€” 11.94-11.98 (enviado loja v11.98)

Loja hoje **v11.93**. Ordem sugerida (um por vez ou lote, sempre com frase + `99738595`):

| Ordem | Loja | Ref | Branch pronta | Migrate | Rollback |
| ----- | ---- | --- | ------------- | ------- | -------- |
| 1 | **11.94** | PDV-PIX-SICREDI | `deploy/pdv-pix-sicredi-v11.94` @ `06db8c8` | NÃƒO | `rollback/pre-v1194-pdv-pix-sicredi` |
| 2 | **11.95** | ENTRADA-NF-UX (+ Nova limpa) | `deploy/entrada-nf-ux-v11.95` @ `425e16c` | NÃƒO | `rollback/pre-v1195-entrada-nf-ux` |
| 3 | **11.96** | ETQ-GONDOLA | `deploy/etq-gondola-v11.96` @ `3ab66e9` | **SIM** `0067` | `rollback/pre-v1196-etq-gondola` |
| 4 | **11.97** | TRANSF-FORCADA | `deploy/transf-forcada-v11.97` @ `cbc9a38` | NÃƒO | `rollback/pre-v1197-transf-forcada` |
| 5 | **11.98** | PDV-ESTOQUE-VILA | `deploy/pdv-estoque-vila-v11.98` @ `e792363` | NÃƒO | `rollback/pre-v1198-pdv-estoque-vila` |

**JÃ¡ resolvido (nÃ£o sobe de novo):**
- **Â«v10.89Â» MP automÃ¡tico pÃ³s-abrir caixa** â€” cÃ³digo **jÃ¡ na loja** (v11.93+); checklist antigo enganava.
- **Â«v10.90Â» Entrada NF Nova limpa** â€” **jÃ¡ dentro** do pacote **11.95** (`entradaNfeResetEditorParaNovaNota`).
- **Â«v9.92Â» kardex e-mail + Î”camada** â€” **jÃ¡ na loja desde v10.63** (`1e6fe3c`); checklist antigo enganava.

**Smoke 28/07:** `manage.py check` 0 Â· asserts pacote 10/10 Â· py_compile OK Â· DF-e **fora** de todos os deploys.

**Autorizar (prÃ³ximo chat):** pausa vendas se quiser Â· *Â«pode subir â€¦ / produÃ§Ã£oÂ»* + **99738595** Â· FF da branch `deploy/â€¦` â†’ `producao` Â· Ctrl+F5.


### âœ… Deploy loja **v12.05** â€” ETQ-BUSCA-FILTROS (29/07 Â· frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **HEAD** | `30e79a9` |
| **Rollback** | `rollback/pre-v1205-etq-busca` @ `72b204b` |
| **Validar** | Ctrl+F5 Â· badge **12.05** Â· filtros Â· add todos Â· lista nÃ£o some |

### PACOTE PRONTO LOJA â€” Etiquetas busca filtros + add todos (`ETQ-BUSCA-FILTROS` Â· **v12.05**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **ENVIADO loja v12.05** Â· `producao` @ **`30e79a9`** Â· rollback `rollback/pre-v1205-etq-busca` @ `72b204b` |
| **VERSION alvo loja** | **12.05** (loja hoje **12.04**) |
| **Inclui** | Filtros Folha na busca Â· lista **nÃ£o some** ao add (FL-009) Â· **Adicionar todos** |
| **Arquivos** | `produtos_etiquetas.html` Â· `produtos_etiquetas.js` Â· `produtos_etiquetas_view` (sÃ³ contexto facetas/presets) |
| **Migrate** | **NÃƒO** |
| **Risco loja aberta** | **Baixo** â€” sÃ³ `/produtos/etiquetas/` Â· **zero** PDV/caixa/CP |
| **Smoke** | `manage.py check` 0 Â· JS syntax OK Â· diff views = 7 linhas |
| **NÃƒO inclui** | DF-e Â· merge `teste` Â· Folha saldo Â· Entrada NF |
| **PrÃ³ximo chat** | 1) pausar vendas 2) *pode subir etiquetas busca / produÃ§Ã£o* + **99738595** 3) FF `deploy/etq-busca-filtros-v12.05` â†’ `producao` Â· Ctrl+F5 |
| **Rollback** | `rollback/pre-v1205-etq-busca` @ HEAD `producao` antes do push |

### PACOTE PRONTO LOJA â€” Folha polish + Etiquetas ajuste (`FOLHA-ETQ-V12` Â· 29/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **jÃ¡ na loja v12.03** (lote Folha+Etq) â€” nÃ£o sobe de novo |
| **VERSION alvo loja** | **12.00â†’12.03** |
| **Branch** | `deploy/folha-etq-v12.00` @ **`0a4a386`** |
| **Inclui** | 1) Folha saldo polish (fornecedor overlay Â· filtro no cupom Â· GM alinhado Â· saldo) 2) Etiquetas: borda 0,5 mm fora (91Ã—31) Â· fonte nome 1/2/3 linhas Â· corte na 3Âª |
| **Arquivos** | `compras_relatorio_planilha.html` Â· `compras_relatorio_saldo.html` Â· `produtos_etiquetas.js` Â· `produtos_etiquetas_core.js` Â· `produtos_etiquetas.html` |
| **Migrate** | **NÃƒO** |
| **Risco loja aberta** | **Baixo** â€” sÃ³ Folha saldo/planilha + etiquetas Â· **zero** PDV carrinho/caixa/CP |
| **Smoke** | `manage.py check` 0 Â· JS syntax OK Â· diffs sÃ³ 5 arquivos vs `producao` |
| **NÃƒO inclui** | DF-e Â· merge `teste` Â· transferÃªncia Â· Entrada NF Â· PDV |
| **PrÃ³ximo chat** | 1) lojas pausam vendas 2) *pode subir Folha+etiquetas / produÃ§Ã£o* + **99738595** 3) FF `deploy/folha-etq-v12.00` â†’ `producao` Â· Ctrl+F5 |
| **Rollback** | `rollback/pre-v1200-folha-etq` @ HEAD `producao` antes do push |

### PACOTE PRONTO LOJA â€” Etiqueta GÃ´ndola A4 (`ETQ-GONDOLA` Â· 28/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ðŸ“¦ **pronto** Â· branch `deploy/etq-gondola-v11.96` @ **`3ab66e9`** Â· espera frase + senha |
| **VERSION alvo loja** | **11.96** |
| **O quÃª** | Preset **GÃ´ndola A4** Â· 9Ã—3 cm Â· 18/folha Â· marcas de corte Â· logo Agro Mais Â· R$ separado Â· centavos menores Â· peso **sem** texto Â«PESO:Â» Â· campo peso no cadastro |
| **Migrate** | **SIM** â€” `0067_produto_overlay_peso_etiqueta` |
| **Fonte teste** | `27c8436` |
| **NÃƒO inclui** | TransferÃªncia Â· DF-e Â· Entrada NF Â· PDV Estoque Vila |
| **Risco loja aberta** | **Baixo** â€” sÃ³ etiquetas + 1 campo cadastro |
| **Autorizar** | *pode subir etiquetas gÃ´ndola / produÃ§Ã£o* + **99738595** |
| **Rollback** | `rollback/pre-v1196-etq-gondola` |

### PACOTE PRONTO LOJA â€” TransferÃªncia forÃ§ada Vilaâ†”Centro (`TRANSF-FORCADA` Â· 28/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ðŸ“¦ **pronto** Â· branch `deploy/transf-forcada-v11.97` @ **`cbc9a38`** Â· espera frase + senha |
| **VERSION alvo loja** | **11.97** |
| **O quÃª** | Modal **ForÃ§ada Vilaâ†”C** Â· carrinho (busca + colar) Â· direÃ§Ã£o **Vilaâ†’C** ou **Câ†’Vila** Â· qtd com foco/Enter Â· altura fixa Â· ajuda no Â«?Â» |
| **Migrate** | **NÃƒO** |
| **Fonte teste** | `0391ca7` |
| **NÃƒO inclui** | Etiquetas Â· DF-e Â· Entrada NF Â· PDV |
| **Risco loja aberta** | **Baixo** â€” sÃ³ /transferencias/ forÃ§ada |
| **Autorizar** | *pode subir transferÃªncia forÃ§ada / produÃ§Ã£o* + **99738595** |
| **Rollback** | `rollback/pre-v1197-transf-forcada` |

### PACOTE â€” Folha saldo polish â†’ absorvido em **FOLHA-ETQ-V12** (29/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âž¡ï¸ **absorvido** no pacote `FOLHA-ETQ-V12` (Folha + etiquetas juntos Â· loja **v12.00**) |
| **Autorizar** | ver bloco **FOLHA-ETQ-V12** acima |

### PACOTE PRONTO LOJA â€” Entrada NF boleto + lista ConcluÃ­da + Nova limpa (`ENTRADA-NF-UX` Â· 28/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **jÃ¡ loja ~v11.98** Â· reconfirmado 13/08 em `producao` v16.07 Â· **nÃ£o reenviar** |
| **VERSION alvo loja** | **11.95** |
| **Inclui** | 1) Boleto 44â†’47 Â· 2) Lista ConcluÃ­da (nome/XML/chip Â«Sem a pagarÂ») Â· 3) **Nova limpa** (antigo Â«v10.90Â») |
| **NÃƒO inclui** | DF-e inbox Â· TransferÃªncia Â· PDV Â· migrate |
| **Migrate** | **NÃƒO** |
| **Risco loja aberta** | **Baixo** |
| **Autorizar** | *pode subir Entrada NF UX / produÃ§Ã£o* + **99738595** |
| **Rollback** | `rollback/pre-v1195-entrada-nf-ux` |
| **Validar pÃ³s** | ConcluÃ­da Â· boleto 44â†’47 Â· **Nova** em branco Â· 1 venda PDV |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ PDV Pix mÃƒÂ¡quina Sicredi (`PDV-PIX-SICREDI` Ã‚Â· **v11.94**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **jÃ¡ loja ~v11.98** Â· reconfirmado 13/08 em `producao` v16.07 Â· **nÃ£o reenviar** |
| **VERSION alvo loja** | **11.94** |
| **Smoke OK** | lista + filtro notebook Ã‚Â· sÃƒÂ³ arquivos PDV Ã‚Â· migrate **NÃƒÆ’O** |
| **Risco loja aberta** | **Baixo** Ã¢â‚¬â€ sÃƒÂ³ adiciona opÃƒÂ§ÃƒÂ£o Sicredi Pix |
| **Autorizar** | *pode subir Sicredi Pix / PDV* + **99738595** |
| **Rollback** | `rollback/pre-v1194-pdv-pix-sicredi` |

### Ã¢Å“â€¦ Deploy loja **v11.93** Ã¢â‚¬â€ Folha saldo presets + transferÃƒÂªncia sem saldo+ (`FOLHA-TRANSF`) (28/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **enviado** Ã‚Â· push `producao` **`f83e804`** Ã‚Â· aguardar Live + migrate `0066` Ã‚Â· badge **11.93** |
| **VERSION** | **loja v11.93** (antes v11.92) |
| **Como** | FF `deploy/folha-transf-v11.93` Ã¢â€ â€™ `producao` (apÃƒÂ³s rollback) |
| **Inclui** | Folha saldo overlay + filtros salvos online (PadrÃƒÂ£o Ã¢Ëœâ€¦) Ã‚Â· TransferÃƒÂªncia VilaÃ¢â€ â€™C com saldo 0/negativo |
| **NÃƒÆ’O inclui** | merge inteiro `teste` Ã‚Â· PDV Ã‚Â· caixa Ã‚Â· CP |
| **Migrate** | **SIM** `0066_compras_folha_saldo_filtro_preset` (Render no deploy) |
| **Backup / reverter** | `rollback/pre-v1193-folha-transf` @ **`e7ab4d2`** Ã¢â€ â€™ `git push origin rollback/pre-v1193-folha-transf:producao` |
| **Validar agora** | Ctrl+F5 ComprasÃ¢â€ â€™Folha saldo (salvar/padrÃƒÂ£o) Ã‚Â· `/transferencias/` Vila saldo 0 Ã‚Â· 1 venda PDV |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ Folha saldo presets + transferÃƒÂªncia sem saldo+ (`FOLHA-TRANSF` Ã‚Â· **v11.93**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **enviado loja v11.93** Ã¢â‚¬â€ ver bloco Deploy acima |
| **Branch pronta** | `deploy/folha-transf-v11.93` @ **`f83e804`** (base `producao` + cÃƒÂ³digo limpo) |
| **Fonte teste** | `1dfd369` Ã‚Â· tip `037f67b` |
| **VERSION** | **loja 11.92 Ã¢â€ â€™ 11.93** |
| **Inclui** | 1) Folha saldo overlay maior + filtros salvos **online** + PadrÃƒÂ£o Ã¢Ëœâ€¦ Ã‚Â· 2) TransferÃƒÂªncia VilaÃ¢â€ â€™C com saldo 0/negativo |
| **NÃƒÆ’O inclui** | merge inteiro `teste` Ã‚Â· PDV Ã‚Â· caixa Ã‚Â· CP |
| **Migrate** | **SIM** `0066_compras_folha_saldo_filtro_preset` (tabela nova) |
| **Risco loja aberta** | PDV/caixa **intocados** Ã‚Â· TransferÃƒÂªncia sÃƒÂ³ mais permissiva Ã‚Â· Folha saldo sÃƒÂ³ Compras |
| **Smoke OK** | `check` Ã‚Â· URLs Ã‚Â· CRUD preset Ã‚Â· HTML Ã‚Â· cherry cÃƒÂ³digo limpo (sÃƒÂ³ VERSION/banana) |
| **Backup** | `rollback/pre-v1193-folha-transf` @ **`e7ab4d2`** |
| **Validar pÃƒÂ³s** | Ctrl+F5 Folha saldo (salvar/padrÃƒÂ£o) Ã‚Â· `/transferencias/` saldo 0 Ã‚Â· 1 venda PDV |

### Ã°Å¸â€œÂ¦ PACOTE Ã¢â‚¬â€ Folha saldo presets + transferÃƒÂªncia sem saldo+ (28/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ no `teste` Ã‚Â· pacote loja = branch **`deploy/folha-transf-v11.93`** (v11.93) Ã‚Â· **aguarda pausa + senha** |
| **1 Ã‚Â· Folha saldo** | Overlay maior + fontes Ã‚Â· filtros salvos **online** (Postgres) Ã‚Â· PadrÃƒÂ£o Ã¢Ëœâ€¦ global Ã‚Â· migrate **`0066`** |
| **2 Ã‚Â· TransferÃƒÂªncia** | VilaÃ¢â€ â€™C mesmo com saldo 0/negativo Ã‚Â· aviso/confirma se qtd > saldo |
| **Arquivos** | `compras_relatorio_saldo.html` Ã‚Â· `compras.html` Ã‚Â· `models.py` Ã‚Â· `views.py` Ã‚Â· `urls.py` Ã‚Â· `0066_Ã¢â‚¬Â¦` Ã‚Â· `transferencias.html` Ã‚Â· `estoque/views.py` |
| **VocÃƒÂª** | Ctrl+F5 ComprasÃ¢â€ â€™Folha saldo (salvar/padrÃƒÂ£o) Ã‚Â· `/transferencias/` VilaÃ¢â€ â€™C com saldo 0 |

### Ã°Å¸Ââ€º SEFAZ Dist DF-e Ã¢â‚¬â€ cStat 656 na 1Ã‚Âª consulta do dia (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Aba SEFAZ Ã‚Â· 1 clique Ã‚Â· **656 Consumo Indevido** Ã‚Â· botÃƒÂ£o Ã‚Â«Aguarde ~1hÃ‚Â» Ã‚Â· `ultNSU=Ã¢â‚¬Â¦2084` Ã‚Â· `maxNSU=0` |
| **Causa** | Bloqueio **da Receita no CNPJ** (nÃƒÂ£o ÃƒÂ© bug do botÃƒÂ£o). Quase sempre: **outro sistema** (ERP / contador / outro PC / staging) jÃƒÂ¡ consultou DF-e no mesmo CNPJ, **ou** NSU jÃƒÂ¡ no fim sem intervalo de 1h |
| **Agora** | Esperar a 1h do botÃƒÂ£o Ã‚Â· **nÃƒÂ£o** clicar de novo Ã‚Â· nota de hoje Ã¢â€ â€™ **Ler XML** (e-mail / portal / fornecedor) |
| **Depois** | 1 clique sÃƒÂ³ Ã‚Â· se vier **137** (nada novo) Ã¢â€ â€™ esperar 1h Ã‚Â· se **138** Ã¢â€ â€™ carregar na grade |
| **Conferir** | ERP legado ou software do contador ainda Ã¢â‚¬Å“baixa NFÃ¢â‚¬Â automÃƒÂ¡tico deste CNPJ? |

### Ã°Å¸Â©Â¹ FIX Ã¢â‚¬â€ Ajuste Mobile saldo Ã‚Â«tudo 0Ã‚Â» / filtro sÃƒÂ³ positivo (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ no pacote **CAD-COMPRAS-AJUSTE** (teste v11.80) Ã‚Â· validar local Ã‚Â· loja alvo v11.92 |
| **Sintoma** | HistÃƒÂ³rico grava ajuste (ex. +1000 Vila) Ã‚Â· lista com Ã‚Â«SÃƒÂ³ positivoÃ‚Â» = **0 itens** Ã‚Â· saldo na tela fica 0 |
| **Causa** | Lista usa cache do PDV com saldo 0 Ã‚Â· filtro positivo esconde tudo Ã‚Â· ATUALIZAR sÃƒÂ³ pedia saldo dos **visÃƒÂ­veis** (ninguÃƒÂ©m) |
| **Fix** | API `?positivos=1&deposito=` Ã‚Â· ao abrir / ATUALIZAR / Aplicar Ã‚Â«sÃƒÂ³ positivoÃ‚Â» hidrata saldo real do depÃƒÂ³sito |
| **Arquivos** | `mobile_ajuste.html` Ã‚Â· `views.py` (`api_pdv_saldos`) Ã‚Â· `estoque_saldo_agro_util.py` |
| **Validar** | Ctrl+F5 Ã‚Â· Vila Ã‚Â· filtro SÃƒÂ³ positivo Ã¢â€ â€™ deve listar o que ajustou hoje Ã‚Â· ATUALIZAR Ã‚Â· Hist. confere |
| **Workaround se ainda vazio** | Limpar filtros Ã¢â€ â€™ ATUALIZAR Ã¢â€ â€™ depois SÃƒÂ³ positivo de novo |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ Cadastro filtros + Folha saldo + Ajuste mobile (`CAD-COMPRAS-AJUSTE` Ã‚Â· 27/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã°Å¸â€œÂ¦ **pronto p/ prÃƒÂ³ximo chat** Ã¢â‚¬â€ push `teste` Ã‚Â· **nÃƒÂ£o** produÃƒÂ§ÃƒÂ£o ainda Ã‚Â· espera pausa loja + frase + senha `99738595` |
| **Pasta** | `_pacote_cadastro_compras_ajuste_20260727/LEIA-ME.txt` |
| **VERSION teste** | **11.80** (push) |
| **VERSION loja alvo** | **11.92** (loja hoje **11.91** Ã‚Â· no cherry **forÃƒÂ§ar 11.92**, nÃƒÂ£o 11.80) |
| **Commits (ordem cherry)** | `87d6557` (folha fornecedor PG) Ã¢â€ â€™ **`a812c78`** (pacote) Ã‚Â· docs `9a72d80` opcional |
| **Inclui** | Cadastro filtros/coluna estoque Ã‚Â· Folha saldo A4/A5/80mm Ã‚Â· Folha fornecedor PG Ã‚Â· Ajuste mobile `positivos=1` Ã‚Â· fix saldos c/ Mongo off |
| **RevisÃƒÂ£o 27/07 (assistente)** | Diffs revisados Ã‚Â· `manage.py check` 0 Ã‚Â· smoke 200: `/api/pdv/saldos/` (c/ e s/ ids) Ã‚Â· `/consulta/` Ã‚Â· `/caixa/` Ã‚Â· `/compras/` Ã‚Â· cadastro Ã‚Â· ajuste-mobile Ã‚Â· APIs saldo |
| **Risco loja aberta** | **Baixo** se cherry sÃƒÂ³ estes commits: PDV/saldos sem `positivos` **igual** Ã‚Â· `positivos=1` sÃƒÂ³ mobile Ã‚Â· Compras/Cadastro = additivo Ã‚Â· `agro_fonte` na loja **jÃƒÂ¡** tem o short-circuit Mongo (cherry pode no-op/conflito trivial) |
| **NÃƒÆ’O fazer** | merge `teste`Ã¢â€ â€™`producao` inteiro Ã‚Â· subir sem pausa PDV Ã‚Â· deixar VERSION 11.80 na loja |
| **PrÃƒÂ³ximo chat** | 1) lojas pausam vendas 2) Ã‚Â«pode subir produÃƒÂ§ÃƒÂ£oÃ‚Â» + `99738595` 3) cherry + VERSION 11.92 + push `producao` + Ctrl+F5 |

### Ã¢Å“Â¨ UX Ã¢â‚¬â€ Coluna estoque cadastro (27/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Total grande colorido (verde/vermelho/cinza) + chips **C** / **V** com cor prÃƒÂ³pria Ã‚Â· nÃƒÂºmero pt-BR |
| **Arquivos** | `cadastro_erp_panel.js` `?v=27` Ã‚Â· CSS em `produtos_cadastro_erp.html` |
| **VocÃƒÂª** | Ctrl+F5 cadastro Ã‚Â· olhar coluna Estoque |

### Ã¢Å“Â¨ UX Ã¢â‚¬â€ Filtros cadastro compactos (27/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Removidas abas/vitrines redundantes (Produtos/Marcas/Categorias/Fornecedores). Uma barra: Loja Ã‚Â· Estoque Ã‚Â· Marca/Cat/Sub/Forn/Unid/Modelo Ã‚Â· Aplicar. Data/sub2Ã¢â‚¬â€œ4/preÃƒÂ§o/NCM em Ã‚Â«MaisÃ‚Â». Mais altura p/ lista. |
| **Arquivos** | `produtos_cadastro_erp.html` Ã‚Â· `cadastro_erp_panel.js` `?v=26` |
| **VocÃƒÂª** | Ctrl+F5 cadastro Ã‚Â· botÃƒÂ£o **Filtros** |

### Ã°Å¸Â©Â¹ FIX Ã¢â‚¬â€ `/api/pdv/saldos/` 500 local (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Local lento Ã‚Â· log `Internal Server Error: /api/pdv/saldos/` Ã‚Â· body Ã‚Â«Erro conexaoÃ‚Â» |
| **Causa** | Mongo ERP desligado, mas `estoque_operacional_sem_mongo_erp` exigia ledger Ã¢â€ â€™ API recusava |
| **Fix** | Com `mongo_erp_desligado` Ã¢â€ â€™ saldo operacional sem Mongo (ajuste) |
| **Arquivo** | `agro_fonte_config.py` |
| **VocÃƒÂª** | Reinicia `runserver` Ã‚Â· Ctrl+F5 Ã‚Â· Compras/ajuste nÃƒÂ£o devem mais spammar 500 |

### WIP Ã¢â‚¬â€ Folha de saldo Compras (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ no pacote **CAD-COMPRAS-AJUSTE** (teste v11.80) Ã‚Â· validar local |
| **O quÃƒÂª** | ImpressÃƒÂ£o sÃƒÂ³ **GM Ã‚Â· produto Ã‚Â· saldo** Ã‚Â· papel **A4 / A5 / 80mm** Ã‚Â· checkbox Centro e/ou Vila (duas = soma) |
| **Filtros** | Mesmos do cadastro (marca/cat/sub/estoque/datas/modelo/unidade/extras) + esconder zerados + busca |
| **UX (27/07)** | Popup compacto padrÃƒÂ£o ERP Ã‚Â· filtros = **botÃƒÂµes + chips** (igual cadastro) Ã‚Â· listas jÃƒÂ¡ no HTML (nÃƒÂ£o fica branco) |
| **80mm Epson TM-T20X** | wrap **82%** + saldo fixo (90% ainda raspava) |
| **Arquivos** | `compras_relatorio_saldo.html` Ã‚Â· `views.py` Ã‚Â· `urls.py` Ã‚Â· `compras.html` |
| **API** | `/api/compras/relatorio-saldo/` |
| **VocÃƒÂª** | Ctrl+F5 Compras Ã‚Â· Folha de saldo Ã‚Â· clicar **Marca** deve listar opÃƒÂ§ÃƒÂµes |

### WIP Ã¢â‚¬â€ Cadastro ERP filtros completos (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ no pacote **CAD-COMPRAS-AJUSTE** (teste v11.80) Ã‚Â· validar local |
| **Pedido** | Filtros avanÃƒÂ§ados: estoque +/Ã¢Ë†â€™/0 Ã‚Â· loja Centro/Vila Ã‚Â· multi cat/sub 1Ã¢â‚¬â€œ4 Ã‚Â· datas (cadastro / 1Ã‚Âª NF / ÃƒÂºltima NF) Ã‚Â· modelo Ã‚Â· unidade Ã‚Â· extras |
| **UI** | BotÃƒÂ£o **Filtros** Ã‚Â· barra compacta (sem vitrines) Ã‚Â· Ã‚Â«MaisÃ‚Â» recolhido |
| **Exemplo** | Loja=Vila + Estoque=Positivo Ã¢â€ â€™ sÃƒÂ³ saldo Vila Elias |
| **Arquivos** | `cadastro_filtros_util.py` Ã‚Â· `estoque_saldo_agro_util.py` Ã‚Â· `catalogo_agro.py` Ã‚Â· `views.py` Ã‚Â· `produtos_cadastro_erp.html` Ã‚Â· `cadastro_erp_panel.js` Ã‚Â· `agro_busca_catalogo.js` |
| **Validar** | Vila+positivo Ã‚Â· multi cat/sub Ã‚Â· cada tipo de data Ã‚Â· Limpar Ã‚Â· lista com mais altura |

### Ã°Å¸ÂÅ’ Local lento Ã¢â‚¬â€ Postgres Oregon (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | `.env` com `DATABASE_URL` staging **Oregon** (~300Ã¢â‚¬â€œ600 ms/consulta) |
| **SoluÃƒÂ§ÃƒÂ£o Renan** | Snapshot catÃƒÂ¡logo Ã¢â€ â€™ SQLite local Ã‚Â· `DATABASE_URL` comentado Ã‚Â· **reiniciar runserver** |
| **Resultado** | ~3361 produtos congelados Ã‚Â· count ~5 ms |
| **Doc** | `docs/TESTE-LOCAL.md` Ã‚Â§0 |

### Ã°Å¸Ââ€º FIX Ã¢â‚¬â€ Folha Compras Ã‚Â«por fornecedorÃ‚Â» sem Mongo (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` Ã‚Â· validar no PC (Ctrl+F5 `/compras/`) |
| **Sintoma** | Alert Ã‚Â«serviÃƒÂ§o legado indisponÃƒÂ­velÃ‚Â» ao **Imprimir planilha** (por fornecedor) Ã¢â‚¬â€ Mongo morto |
| **Fix** | Caminho Postgres: catÃƒÂ¡logo por fornecedor + ÃƒÂºltima Entrada NF Agro + vendas PDV |
| **TambÃƒÂ©m** | Categoria/unidade jÃƒÂ¡ eram PG (sem mudanÃƒÂ§a) Ã‚Â· rÃƒÂ³tulo Ã‚Â«FORNECEDORÃ‚Â» duplicado Ã¢â€ â€™ Ã‚Â«Montar folhaÃ‚Â» |
| **Arquivos** | `views.py` Ã‚Â· `catalogo_agro.py` Ã‚Â· `compras_ultimas_compras_util.py` Ã‚Â· `compras_relatorio_planilha.html` |
| **Validar** | Folha Ã¢â€ â€™ por fornecedor (IBIUNA) Ã‚Â· por categoria Ã‚Â· por unidade |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ Dispenser +13 sabores (`DSP-SABORES` Ã‚Â· 24/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã°Å¸â€œÂ¦ **pronto** Ã‚Â· **NÃƒÆ’O subir** enquanto loja vende Ã‚Â· prÃƒÂ³ximo chat: pausar vendas + frase + senha `99738595` |
| **Commit teste** | **`ed43464`** `feat(dispenser): +13 sabores comuns (leite, ovo, bacalhauÃ¢â‚¬Â¦)` |
| **VERSION loja alvo** | **11.91** (loja hoje **11.90** Ã‚Â· no cherry **nÃƒÂ£o** usar 11.76 do teste) |
| **Arquivos** | `flavor_lib.js` Ã‚Â· `flavor-icons.svg` Ã‚Â· `dispenser_flavor_icons.html` Ã‚Â· 13 PNG em `lib/icons/` Ã‚Â· cache studio `?v=11.76` Ã‚Â· (+ `VERSION`/`banana` no resolve) |
| **Inclui** | Leite Ã‚Â· Ovo Ã‚Â· Bacalhau Ã‚Â· Veado Ã‚Â· CamarÃƒÂ£o Ã‚Â· Batata Ã‚Â· Milho Ã‚Â· LinhaÃƒÂ§a Ã‚Â· Cranberry Ã‚Â· Banana Ã‚Â· Couve Ã‚Â· Inhame Ã‚Â· Alecrim |
| **JÃƒÂ¡ tinha** | **Blueberry (Mirtilo)** Ã¢â‚¬â€ ÃƒÂ­cone + catÃƒÂ¡logo jÃƒÂ¡ existiam (`mirtilo.png`) Ã‚Â· **nÃƒÂ£o** faltava |
| **NÃƒÆ’O inclui** | migrate Ã‚Â· PDV Ã‚Â· caixa Ã‚Â· CP Ã‚Â· NFC-e Ã‚Â· merge inteiro `teste` Ã‚Â· kardex Ã‚Â· re-cherry zoom/cores |
| **Cherry** | **sÃƒÂ³** `ed43464` em `producao` Ã‚Â· conflito esperado sÃƒÂ³ `VERSION`+`banana` (ÃƒÂ­cones/JS aplicam limpo Ã¢â‚¬â€ dry-run OK 24/07) |
| **Backup** | criar `rollback/pre-v1191-dsp-sabores` @ HEAD `producao` antes do push |
| **Risco loja aberta** | **Baixo** se cherry sÃƒÂ³ esse commit Ã¢â‚¬â€ paths sÃƒÂ³ `/interno/dispenser-a6*` Ã‚Â· **zero** `/consulta/`, caixa, vendas |
| **Testado assistente** | dry-run cherry Ã‚Â· 13 PNG + blueberry presentes Ã‚Â· META/PROTEINS/SIDES OK Ã‚Â· **falta** Ctrl+F5 visual do Renan |
| **VocÃƒÂª antes de autorizar** | Local: Dispenser Ã¢â€ â€™ Sabores Ã¢â€ â€™ ver grade nova (Leite etc.) Ã‚Â· Blueberry jÃƒÂ¡ na lista |
| **PÃƒÂ³s-Live** | Ctrl+F5 Dispenser Ã‚Â· Sabores Ã‚Â· PDV smoke rÃƒÂ¡pido sÃƒÂ³ por precauÃƒÂ§ÃƒÂ£o |

### Ã¢Å“â€¦ Deploy loja **v11.90** Ã¢â‚¬â€ zoom logo (`DSP-LOGO-ZOOM`) Ã‚Â· Ã¢Å“â€¦ jÃƒÂ¡ enviado

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **Live** Ã‚Â· **nÃƒÂ£o** falta senha deste pacote |
| **Commit** | `2f004b5` / tip docs Live |

### WIP Ã¢â‚¬â€ Dispenser A6 (visual)

| Item | Detalhe |
| ---- | ------- |
| **URL** | `/interno/dispenser-a6/` |
| **JÃƒÂ¡ na loja** | Nuvem + Prontas + cores + zoom logo (**v11.90**) |
| **Novo no teste** | **Mix Sabores** + criar sabor (nome/desc/PNG) Â· `DSP-MIX-CRIAR` (03/08) |
| **Blueberry** | Ã¢Å“â€¦ jÃƒÂ¡ no catÃƒÂ¡logo (Mirtilo) |
| **Regra** | Cherry **sÃƒÂ³** o pacote Ã‚Â· **nunca** merge `teste`Ã¢â€ â€™`producao` |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ HistÃƒÂ³rico estoque / kardex ledger (**v11.68** Ã‚Â· 23/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **jÃ¡ na loja** (~v10.63+) Â· **nÃ£o** entra no lote v12.51 Â· Â«prontoÂ» antigo invalidado 31/07 |
| **Caso** | Venda **#3747** Ã‚Â· 1 un. ibiuna Ã‚Â· aba Estoque mostrou saÃƒÂ­da **~39,463** (bug em **qualquer** produto com ledger) |
| **O que ÃƒÂ©** | Kardex com ledger = ÃŽâ€ `saldo_informado` (nÃƒÂ£o ÃŽâ€ camada Mongo) Ã‚Â· baixa/estorno venda usa snapshot ledger + congela `erp_ref` |
| **VERSION** | **11.68** Ã‚Â· commit teste **`fddc5a2`** |
| **Arquivos** | `estoque_movimentos_cadastro_util.py` Ã‚Â· `views.py` (baixa/estorno) |
| **Risco** | Baixo Ã¢â‚¬â€ sÃƒÂ³ leitura do histÃƒÂ³rico + alinhamento da baixa; **nÃƒÂ£o** apaga estoque nem vendas |
| **Testar (teste)** | Ctrl+F5 Ã‚Â· cadastro ibiuna Ã¢â€ â€™ aba Estoque Ã¢â€ â€™ venda #3747 saÃƒÂ­da **1** Ã‚Â· 1 venda nova de 1 un. = saÃƒÂ­da 1 |
| **Loja** | frase + senha Ã‚Â· rollback `rollback/pre-v1168-kardex-ledger` @ HEAD `producao` antes do push |

### Ã¢Å“â€¦ VerificaÃƒÂ§ÃƒÂ£o remota loja Ã¢â‚¬â€ abrir/vender amanhÃƒÂ£ (22/07 ~23:20 Ã‚Â· Renan ausente AnyDesk)

| Item | Resultado |
| ---- | --------- |
| **Loja Live** | **v11.64** em `https://sistvale.com.br/` (badge home) Ã‚Â· commit **118acbc** |
| **healthz** | **200 ok** (~0,7s) |
| **PDV `/consulta/`** | HTML 200 Ã‚Â· JS `consulta_produtos` + `agro_busca_catalogo` com `?v=118acbcÃ¢â‚¬Â¦` (pacote novo) |
| **Busca `/api/buscar/`** | OK ~0,5Ã¢â‚¬â€œ1,4s Ã‚Â· leite/cÃƒÂ£o/gato/raÃƒÂ§ÃƒÂ£o/GM9503 retornam produtos |
| **Freio catÃƒÂ¡logo full** | **ligado** Ã¢â‚¬â€ `/api/todos-produtos/` e `Ã¢â‚¬Â¦/delta/` respondem vazio em &lt;1s (`catalogo-full-off`) Ã¢â€ â€™ PDV vende pela busca servidor (nÃƒÂ£o remonta catÃƒÂ¡logo gigante de manhÃƒÂ£) |
| **Freio no cÃƒÂ³digo** | `agro_pdv_catalogo_full_desligado` **mantido** na loja (nÃƒÂ£o veio removido do teste) |
| **AST Python** | views / fonte / settings / middleware OK |
| **Rollback pronto** | `rollback/pre-v1164-devolucao-bca-custo` @ **241782c** Ã¢â€ â€™ `git push origin rollback/pre-v1164-devolucao-bca-custo:producao` |
| **NÃƒÆ’O testado** (sem login) | Abrir caixa Ã‚Â· finalizar venda Ã‚Â· fiado Ã‚Â· devoluÃƒÂ§ÃƒÂ£o na loja Ã‚Â· NFC-e |
| **NÃƒÆ’O na loja ainda** | `--todas` reaplicar custos (sÃƒÂ³ **teste v11.65**) Ã¢â‚¬â€ **nÃƒÂ£o bloqueia vender** |
| **AmanhÃƒÂ£ de manhÃƒÂ£ (loja)** | 1) Ctrl+F5 no PDV Ã‚Â· 2) abrir caixa Ã‚Â· 3) buscar produto + 1 venda rÃƒÂ¡pida Ã‚Â· se site morto: ver healthz / Render Ã‚Â· se pacote ruim: rollback acima |

**Veredito assistente:** site e busca da loja **saudÃƒÂ¡veis** para abrir e vender; risco residual = fluxos autenticados (caixa/venda) nÃƒÂ£o exercitados daqui.

### Ã°Å¸â€œÅ’ Reaplicar custos NF em lote `--todas` (**teste v11.65** Ã‚Â· 22/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ `teste` **873d7fe** Ã‚Â· **ainda NÃƒÆ’O na loja** (loja v11.64 = 1 NF por vez) |
| **Uso** | dry-run: `python manage.py reaplicar_custos_entrada_nf --todas` Ã‚Â· gravar: `Ã¢â‚¬Â¦ --todas --aplicar` |
| **Opcional** | `--desde=2026-01-01` Ã‚Â· `--limite=50` Ã‚Â· ordem antigaÃ¢â€ â€™nova (ÃƒÂºltima NF manda no custo) |
| **Loja** | precisa frase + senha de novo |

### Ã°Å¸Â§Â­ Teste = LOCAL (22/07 Ã‚Â· Renan Ã‚Â· opÃƒÂ§ÃƒÂ£o B)

| Item | Detalhe |
| ---- | ------- |
| **Gate** | Chrome em `http://127.0.0.1:8000` Ã¢â‚¬â€ ver `docs/TESTE-LOCAL.md` |
| **Render teste** | Free dorme Ã¢â‚¬â€ **nÃƒÂ£o** usar como validaÃƒÂ§ÃƒÂ£o; assistente **nÃƒÂ£o** push `origin teste` sozinho |
| **ProduÃƒÂ§ÃƒÂ£o** | SÃƒÂ³ apÃƒÂ³s Renan testar local + frase + senha (ele nÃƒÂ£o sobe sem testar) |
| **ConfianÃƒÂ§a** | Renan: nÃƒÂ£o enviar ÃƒÂ  loja sem teste local; assistente respeita |

### Ã¢Å“Â¨ Kardex cadastro Ã¢â‚¬â€ Fornecedor/PreÃƒÂ§o na Entrada NF (**teste v11.59**)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Aba Estoque do modal: coluna Fornecedor/PreÃƒÂ§o ficava `---` na Entrada NF (custo jÃƒÂ¡ ok na aba PreÃƒÂ§os) |
| **Fix** | Grava `nf_forn`/`nf_custo` no `observacao` do ajuste Ã‚Â· lÃƒÂª no kardex Ã‚Â· enrich compras via rascunho Entrada NF Agro (PG, sem Mongo) |
| **NFs antigas** | Preenche se achar a NF no rascunho Agro (nÃ‚Âº) |
| **Arquivos** | `estoque_movimentos_cadastro_util.py` Ã‚Â· `views.py` Ã‚Â· `compras_ultimas_compras_util.py` |

### Ã°Å¸Ââ€º Cadastro BCA Ã¢â‚¬â€ Ã‚Â«lista localÃ‚Â» incompleta (**teste v11.60**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Busca Ã‚Â«testeÃ‚Â» mostra sÃƒÂ³ 2 (rodapÃƒÂ© **LISTA LOCAL**); GM9503 some; ÃƒÂ s vezes volta 7 |
| **Causa** | Fallback 2s do pacote local; servidor chegava depois e era **ignorado**; pacote nÃƒÂ£o ganhava produtos novos |
| **Fix** | Cadastro espera atÃƒÂ© 12s o servidor Ã‚Â· resposta tardia atualiza a grade Ã‚Â· merge de produtos novos no pacote local |
| **Agora** | Ctrl+F5 no Cadastro Ã‚Â· buscar Ã‚Â«testeÃ‚Â» de novo (pode demorar uns segundos) |
| **Arquivos** | `agro_busca_catalogo.js` Ã‚Â· `cadastro_erp_panel.js` |

### Ã°Å¸â€â€™ Cadastro BCA Ã¢â‚¬â€ sÃƒÂ³ servidor (**teste v11.61**)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Renan: em produÃƒÂ§ÃƒÂ£o nÃƒÂ£o pode oscilar 2 vs 10 produtos |
| **DecisÃƒÂ£o** | **Cadastro** `skipLocal: true` Ã¢â‚¬â€ grade sempre `/api/buscar` (servidor). Pacote local fica pro **PDV** (velocidade) |
| **Kardex NF 1111** | Sem rascunho Agro no PG (sÃƒÂ³ 112 e 222) Ã¢â‚¬â€ coluna `---` correta; notas novas gravam forn/custo no ajuste |
| **Por que 2 vs 7+** | Pacote Chrome **incompleto** (nÃƒÂ£o ÃƒÂ© cadastro diferente). Ã‚Â«HojeÃ‚Â» nÃƒÂ£o rebaixava delta Ã¢â€ â€™ local sÃƒÂ³ achava 2 lixos; servidor lÃƒÂª os ~7Ã¢â‚¬â€œ8 reais no PG. Fix: delta em background mesmo com pacote Ã‚Â«hojeÃ‚Â» |
| **Arquivos** | `cadastro_erp_panel.js` Ã‚Â· `agro_busca_catalogo.js` |

### Ã°Å¸â€â€™ PDV Ã¢â‚¬â€ lista nÃƒÂ£o cresce depois (**teste v11.62**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Busca TESTE: 2 itens Ã¢â€ â€™ 5 s depois aparecem mais (medo Renan) |
| **Causa** | `mesclarBuscaLocalComOnline` pintava local e depois **acrescentava** hits do servidor |
| **Fix** | Com lista local jÃƒÂ¡ na tela: servidor **nÃƒÂ£o** muda a sugestÃƒÂ£o; sÃƒÂ³ atualiza pacote em background |
| **Arquivos** | `consulta_produtos.js` |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ regra busca + preÃƒÂ§o no carrinho (**teste v11.63**)

| Item | Detalhe |
| ---- | ------- |
| **Busca** | Pacote Ã‚Â«lista de hojeÃ‚Â» bom + hits Ã¢â€ â€™ local na hora. Pacote fraco/vazio Ã¢â€ â€™ espera servidor **Ã¢â€°Â¤2s**, senÃƒÂ£o usa local. Sem pisca |
| **Carrinho** | Confirma preÃƒÂ§o via `/api/buscar/` **Ã¢â€°Â¤2s**; se demorar/falhar Ã¢â€ â€™ mantÃƒÂ©m cache. Atualiza silencioso (sem alert ERP) |
| **Realidade loja** | 2Ã¢â‚¬â€œ6 NF/dia Ã‚Â· bipÃ¢â€°Ë†nome Ã‚Â· PC abre todo dia |
| **Arquivos** | `consulta_produtos.js` |

### Ã°Å¸Å¡â€˜ PDV Ã¢â‚¬â€ lag 1Ã¢â‚¬â€œ2s em tudo (**teste v11.64**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Menu caixa / fechar venda dinheiro Ã¢â‚¬Å“engasgandoÃ¢â‚¬Â 1Ã¢â‚¬â€œ2 s (antes era na hora) |
| **Causa** | `ensurePacote` baixava catÃƒÂ¡logo **inteiro a cada busca** Ã¢â€ â€™ worker Django ocupado Ã¢â€ â€™ demais cliques na fila |
| **Fix** | Delta no mÃƒÂ¡x. **a cada 5 min** (ou pacote fraco / 1Ã‚Âª abertura); busca continua leve |
| **Commit** | `3f9683e` Ã‚Â· branch `teste` Ã‚Â· **aguarda** frase + senha produÃƒÂ§ÃƒÂ£o (lojas fecharem) |
| **Arquivos** | `agro_busca_catalogo.js` |

### Ã°Å¸Ââ€º DevoluÃƒÂ§ÃƒÂ£o loja Ã¢â‚¬â€ Ã‚Â«Falha de redeÃ‚Â» (#3798 Centro) (**teste v11.58**)

| Item | Detalhe |
| ---- | ------- |
| **Quando** | 22/07 Ã¢â‚¬â€ print loja Centro, venda **#3798**, PentabiÃƒÂ³tico, forma **Fiado** |
| **Sintoma** | Modal Ã‚Â«Falha de redeÃ‚Â» ao confirmar devoluÃƒÂ§ÃƒÂ£o |
| **Causa** | Estorno na devoluÃƒÂ§ÃƒÂ£o **exigia Mongo**; ping lento Ã¢â€ â€™ timeout/HTML Ã¢â€ â€™ front dizia Ã‚Â«redeÃ‚Â». Baixa na venda jÃƒÂ¡ rodava sem Mongo |
| **Fix** | Estorno alinhado ÃƒÂ  baixa (skip Mongo se ledger/sem ERP) Ã‚Â· NFC-e cancel try/except Ã‚Â· mensagem front com HTTP |
| **Loja agora** | Sem pacote: recarregar #3798; 2Ã‚Âª tentativa; se demorar ~30s = timeout. **Sobe** sÃƒÂ³ com frase + `99738595` apÃƒÂ³s teste local |
| **Arquivos** | `produtos/views.py` Ã‚Â· `venda_agro_detalhe.html` Ã‚Â· `VERSION` 11.58 |

### Ã°Å¸â€Âª Corte Mongo na Entrada NF (**teste v11.57**)

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | Parar de apagar Ã‚Â«Mongo indisponÃƒÂ­velÃ‚Â» tela a tela Ã¢â‚¬â€ eliminar de uma vez no fluxo NF |
| **O quÃƒÂª** | Wizard NF sem gate Mongo (rascunho/estoque/financeiro/margem/custo) Ã‚Â· `agro_financeiro_usa_postgres` auto se ERP off Ã‚Â· middleware `AgroSemMencaoMongoMiddleware` (nÃƒÂ£o exibe a palavra) |
| **Local** | `.env`: `AGRO_MONGO_ERP_DESLIGADO=true` Ã‚Â· `AGRO_FONTE_FINANCEIRO=agro_pg` |
| **Ainda legado** | Outras telas (BI/Compras antigo) podem ter cÃƒÂ³digo Mongo Ã¢â‚¬â€ middleware esconde a palavra; prÃƒÂ³ximo corte mÃƒÂ³dulo a mÃƒÂ³dulo |

### Ã¢Å“â€¦ Reparar GM/EAN/nome stagingÃ¢â€ â€™loja (22/07 Ã‚Â· **aplicado**)

| Item | Detalhe |
| ---- | ------- |
| **Cmd** | `reparar_codigos_catalogo_fonte_destino --aplicar` |
| **Resultado** | **96** gravados na loja Ã‚Â· 3262 iguais Ã‚Â· 2 sem destino Ã‚Â· **preÃƒÂ§o/estoque nÃƒÂ£o mexidos** |
| **ConferÃƒÂªncia** | dry-run apÃƒÂ³s apply: mudaria=0 |
| **Deploy** | **nÃƒÂ£o** precisou Ã¢â‚¬â€ grava direto no `agro-db` |

### Ã°Å¸Ââ€º Entrada NF Ã¢â‚¬â€ custo nÃƒÂ£o atualiza no Cadastro (**teste v11.54**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | NF 77846 finalizada c/ PIN (estoque OK) Ã‚Â· Cadastro ainda **75,58** (deveria ~**90,42**) |
| **Causa** | PÃƒÂ³s-PIN sÃƒÂ³ gravava `PrecoCusto` no **Mongo**; Cadastro `agro_pg` lÃƒÂª **Produto/overlay**. Se Mongo fora Ã¢â€ â€™ PIN OK + estoque OK + **custo ignorado** |
| **Fix** | `_entrada_nfe_gravar_custo_sisvale` + apply sempre (sem exigir Mongo) Ã‚Â· cmd `reaplicar_custos_entrada_nf --nf=77846` Ã‚Â· **v11.55** estoque Agro sem gate Ã‚Â«Mongo indisponÃƒÂ­velÃ‚Â» |
| **Loja agora** | Corrigir ÃƒÂ  mÃƒÂ£o no Cadastro **ou** checklist **P0** abaixo (pÃƒÂ³s-deploy) |
| **Checklist** | **P0** Ã¢â‚¬â€ apÃƒÂ³s subir pacote: `python manage.py reaplicar_custos_entrada_nf --nf=77846 --aplicar` |
| **Teste local** | Reiniciar `runserver` Ã‚Â· **v11.57** corte Mongo na Entrada NF + middleware sem palavra Ã‚Â«MongoÃ‚Â» + financeiro PG auto com ERP off |
| **HistÃƒÂ³rico Ã‚Â«Ã¢â‚¬â€Ã‚Â»** | Coluna Fornecedor/PreÃƒÂ§o vazia ÃƒÂ© match de compra (secundÃƒÂ¡rio) Ã¢â‚¬â€ nÃƒÂ£o ÃƒÂ© a causa do custo errado |

### Ã°Å¸Å¡Â¨ URGENTE Ã¢â‚¬â€ Mongo ERP Authentication failed (**teste v11.53**)

| Item | Detalhe |
| ---- | ------- |
| **Causa raiz** | Logs loja: `Authentication failed` + timeout em `aprondaerp.com.br` Ã¢â‚¬â€ assinatura ERP morta; cada tela que tocava Mongo **travava o 1 worker** |
| **Fix** | `agro_mongo_erp_desligado()` auto com `agro_pg` Ã‚Â· circuit breaker Ã‚Â· timeouts 3s Ã‚Â· parse/salvar XML **sem Mongo** (casa PG) Ã‚Â· rascunho NF em PG |
| **Loja** | **Aguarda** *Ã‚Â«pode subir para produÃƒÂ§ÃƒÂ£oÃ‚Â»* + `99738595` Ã¢â‚¬â€ lojas reclamando; pacote pronto no teste |

### Ã°Å¸â€œÂ¦ Pacote local BCA Ã¢â‚¬â€ ordem A (**teste v11.52**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Lista no PC: **hoje Ã¢â€ â€™ ÃƒÂºltimo bom Ã¢â€ â€™ servidor**; se servidor &lt;2s usa servidor; **nunca** apaga last good |
| **Arquivos** | `agro_busca_catalogo.js` Ã‚Â· PDV (`pdv_wizard` + chip) Ã‚Â· cadastro ERP |
| **UI** | Chip verde Ã‚Â«lista de hojeÃ‚Â» / amarelo Ã‚Â«lista antigaÃ‚Â» / vermelho Ã‚Â«sÃƒÂ³ servidorÃ‚Â» |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÆ’O** Ã¢â‚¬â€ sÃƒÂ³ `teste`. Renan pediu medo de loja; zero push `producao` |
| **Testar** | Ctrl+F5 no teste Ã‚Â· PDV + cadastro Ã‚Â· chip Ã‚Â· desligar rede mental: busca ainda acha se jÃƒÂ¡ tiver lista |

### Ã¢Å¡Â¡ BCA cache + sem Mongo no motor (22/07 Ã‚Â· **teste v11.51**)

| Item | Detalhe |
| ---- | ------- |
| **Escopo** | `/api/buscar` (PDV Ã‚Â· gestÃƒÂ£o Ã‚Â· cadastro Ã‚Â· compras Ã‚Â· NF Ã‚Â· ajuste) Ã¢â‚¬â€ **nÃƒÂ£o** sÃƒÂ³ PDV |
| **Cache** | `bca_busca_cache_util` Ã‚Â· TTL 90s Ã‚Â· chave `bca_busca_v2:` |
| **Mongo** | Com `agro_pg`: motor **nÃƒÂ£o** chama Mongo (ERP cancelado) |
| **PG** | Fallback `icontains` largo sÃƒÂ³ se &lt;3 hits (antes &lt;8) |
| **Teste Renan** | Ã¢Å“â€¦ apÃƒÂ³s Render acordar: PDV busca **instantÃƒÂ¢nea** Ã‚Â· cadastro **boa/rÃƒÂ¡pida** Ã‚Â· lentidÃƒÂ£o inicial = free/cold start, nÃƒÂ£o o pacote |
| **Loja** | Aguarda frase + senha Ã‚Â· console: digitar `allow pasting` antes de colar limpeza cache |

### Ã°Å¸Å¡â€˜ Import MongoÃ¢â€ â€™PG sÃƒÂ³ faltantes (22/07 Ã‚Â· **loja v11.04**)

| Item | Detalhe |
| ---- | ------- |
| **Mongo** | **Encerrado** Ã¢â‚¬â€ Renan: ERP cancelado, sem acesso. NÃƒÂ£o hÃƒÂ¡ import Mongo. CatÃƒÂ¡logo = **sÃƒÂ³ Postgres** |
| **Achado** | RecuperaÃƒÂ§ÃƒÂ£o vendas: kitekat frango/carne **jÃƒÂ¡ existiam** (`ja_existe`) Ã¢â‚¬â€ busca nÃƒÂ£o achava (nome/cÃƒÂ³digo PG quebrado) |
| **v11.04** | `/interno/recuperar-produtos-vendas/?dias=90` **repara** nome/cÃƒÂ³digo a partir da venda |
| **Inspecionar** | /interno/inspecionar-produto/?nome=kitekat |
| **OK loja** | v11.04: reparados=71 Ã‚Â· busca `sache kitekat` = **4 sabores** |

### Ã¢Å¡Â Ã¯Â¸Â Assistente Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o sem senha (22/07 Ã‚Â· Renan cobrou)

| Item | Detalhe |
| ---- | ------- |
| **Fato** | Push `producao` **v10.98** (`f7b6892`) **sem** frase+senha `99738595` na mensagem |
| **Erro** | Violou regra dura do banana (topo) Ã¢â‚¬â€ loja no meio da venda, sem aviso |
| **Combinado** | **Nunca mais** push `producao` sem frase explÃƒÂ­cita + senha na **mesma** mensagem |
| **Mongo** | DesvinculaÃƒÂ§ÃƒÂ£o: catÃƒÂ¡logo/BCA = **Postgres**. CÃƒÂ³digo legado ainda chamava Mongo na busca Ã¢â‚¬â€ isso era **bug**, nÃƒÂ£o polÃƒÂ­tica; v10.98 corta. **NÃƒÂ£o** voltar a depender do Mongo |

### Ã°Å¸â€œâ€¹ CHECKLIST decisÃƒÂ£o Ã¢â‚¬â€ pacote/BCA definitivo (22/07 Ã‚Â· conversa Renan)


**DecisÃƒÂ£o parcial jÃƒÂ¡ tomada:** ordem **A** (hoje Ã¢â€ â€™ ÃƒÂºltimo pacote local Ã¢â€ â€™ servidor) **+** se servidor <~2s, atualiza preÃƒÂ§o por cima.

**Escopo BCA (obrigatÃƒÂ³rio):** melhorias de busca/pacote valem para o motor **BCA** (`/api/buscar` / unificado), usado por: **PDV Ã‚Â· ajuste rÃƒÂ¡pido Ã‚Â· Entrada NF Ã‚Â· cadastro Ã‚Â· gestÃƒÂ£o Ã‚Â· compras** Ã¢â‚¬â€ nÃƒÂ£o sÃƒÂ³ PDV.

#### A. Pacote (lista pra busca)

| # | Decidir | OpÃƒÂ§ÃƒÂµes / nota |
| - | ------- | ------------- |
| A1 | ConteÃƒÂºdo do pacote | nome, marca, GM, EAN, preÃƒÂ§o venda, index_codigos, busca_texto Ã¢â‚¬â€ **sem saldo** |
| A2 | Quem monta | Cron madrugada + botÃƒÂ£o Ã‚Â«Atualizar listaÃ‚Â» + apÃƒÂ³s Entrada NF / mudanÃƒÂ§a preÃƒÂ§o (opcional) |
| A3 | Onde guarda no servidor | cache Redis/disco + endpoint slim (jÃƒÂ¡ existe `/api/pdv/catalogo-slim/`) |
| A4 | Onde guarda no PC | localStorage/IndexedDB Ã¢â‚¬â€ **ÃƒÂºltimo pacote bom** (nunca apagar no fail) |
| A5 | Ordem no PDV | **1** pacote hoje Ã‚Â· **2** ÃƒÂºltimo local Ã‚Â· **3** servidor; se servidor <2s Ã¢â€ â€™ patch preÃƒÂ§o |
| A6 | Se madrugada falhar | retry 2Ãƒâ€” Ã‚Â· alarme Ã‚Â· PDV segue com A5 Ã‚Â· **nunca** bloquear venda |
| A7 | Timeout montagem | ex. 20Ã¢â‚¬â€œ30s no slim; estoura Ã¢â€ â€™ aborta, mantÃƒÂ©m pacote anterior |
| A8 | Sinal na UI | verde Ã‚Â«lista de hojeÃ‚Â» / amarelo Ã‚Â«lista antigaÃ‚Â» / vermelho Ã‚Â«sÃƒÂ³ servidorÃ‚Â» |

#### B. PreÃƒÂ§o e saldo

| # | Decidir | OpÃƒÂ§ÃƒÂµes / nota |
| - | ------- | ------------- |
| B1 | PreÃƒÂ§o na venda | lista local pra achar; **confirmar servidor se <2s**; senÃƒÂ£o vende com preÃƒÂ§o da lista |
| B2 | Saldo | **sÃƒÂ³ sob demanda** (ajuste rÃƒÂ¡pido / ids=Ã¢â‚¬Â¦) Ã¢â‚¬â€ nÃƒÂ£o no pacote |
| B3 | CentroÃƒâ€”Vila | mesma API de saldo ao vivo do produto aberto |

#### C. Motor BCA (todas as telas)

| # | Decidir | OpÃƒÂ§ÃƒÂµes / nota |
| - | ------- | ------------- |
| C1 | Uma API | `/api/buscar` (BCA) = fonte ÃƒÂºnica; telas sÃƒÂ³ consomem |
| C2 | PG primeiro | loja `agro_pg`: texto/cÃƒÂ³digo no Postgres; Mongo sÃƒÂ³ exceÃƒÂ§ÃƒÂ£o (ex. famÃƒÂ­lia GM) |
| C3 | Frase longa | AND + fallback token (jÃƒÂ¡ no 10.95) Ã¢â‚¬â€ revisar se ainda corta sabor |
| C4 | Limite resultados | wizard 24/48 Ã¢â‚¬â€ pode esconder irmÃƒÂ£os? calibrar |
| C5 | Inativos | gestÃƒÂ£o Ã‚Â«somente ativosÃ‚Â» vs PDV Ã¢â‚¬â€ alinhar regra |

#### D. OperaÃƒÂ§ÃƒÂ£o / nunca parar

| # | Decidir | OpÃƒÂ§ÃƒÂµes / nota |
| - | ------- | ------------- |
| D1 | Fechar venda / entrega | nÃƒÂ£o depender do pacote; investigar lentidÃƒÂ£o atual (DB ainda pressionado) |
| D2 | Monitor | health: slim ok? buscar p95? alarme |
| D3 | NÃƒÂ£o reverter | 10.87/10.88 fora de cogitaÃƒÂ§ÃƒÂ£o agora |

#### E. Incidentes abertos loja (22/07 tarde)

| # | Sintoma | Achado |
| - | ------- | ------ |
| E1 | Ã‚Â«produtos sumindoÃ‚Â» (kitekat = 1 exemplo) | **NÃƒÂ£o apagou os 3000.** Slim = **3364**. Ontem a busca ainda olhava **Mongo**; hoje (hotfix) sÃƒÂ³ **Postgres** Ã¢â€ â€™ o que nÃƒÂ£o estÃƒÂ¡ completo no PG Ã‚Â«someÃ‚Â» na busca. **Voltar normal:** (1) reimport MongoÃ¢â€ â€™`Produto` em lote **ou** (2) restore ZIP em `/interno/pg-backup/` **ou** (3) temporÃƒÂ¡rio religar complemento Mongo na busca. **NÃƒÂ£o** caÃƒÂ§ar SKU a SKU |
| E2 | Busca lenta / fechar venda lento | `/api/buscar` ainda ~8Ã¢â‚¬â€œ25s em vÃƒÂ¡rios termos Ã¢â‚¬â€ banco sob carga; pacote local + BCA leve ÃƒÂ© o caminho |

**PrÃƒÂ³ximo:** Renan autoriza loja (frase+senha) Ã¢â€ â€™ **reimport catÃƒÂ¡logo MongoÃ¢â€ â€™PG** (ou restore backup) Ã‚Â· checklist A1Ã¢â‚¬â€œD3 depois.

### Ã°Å¸Â©Â¹ Recuperar produtos de itens de venda (22/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Por quÃƒÂª** | Frango/carne Kitekat vendidos ontem, sumiram do PG ativo Ã¢â‚¬â€ loja assustou Ã‚Â«perdemos 3000?Ã‚Â» |
| **Fato** | CatÃƒÂ¡logo slim = **3364** Ã‚Â· faltam SKUs pontuais, nÃƒÂ£o o cadastro inteiro |
| **Ferramenta** | `manage.py recuperar_produtos_itens_venda` + cron GET `api/cron/recuperar-produtos-itens-venda/` (token alerta) |
| **Loja** | Aguarda frase + senha `99738595` Ã¢â‚¬â€ **nÃƒÂ£o** push `producao` sem isso |

### Ã¢Å“â€¦ Loja v10.97 Ã¢â‚¬â€ bipagem OK + causa do Ã¢â‚¬Å“amanheceu quebradoÃ¢â‚¬Â (22/07 Ã‚Â· Renan)


| Item | Detalhe |
| ---- | ------- |
| **Bipagem** | Ã¢Å“â€¦ loja: cÃƒÂ³digo de barras **vai direto pro carrinho** (normal) |
| **Suspeito nÃ‚Âº 1** | **v10.87 + v10.88** (21/07 manhÃƒÂ£) Ã¢â‚¬â€ apertaram Postgres (`conn_max_age` 60s, menos slots/workers) |
| **Por quÃƒÂª** | O Ã¢â‚¬Å“carregamentozinhoÃ¢â‚¬Â da 1Ã‚Âª abertura **jÃƒÂ¡ existia**; com menos folga no banco, hoje de manhÃƒÂ£ **nÃƒÂ£o terminou** e travou tudo |
| **Quase inocente** | **v10.89** Entrada NF V.unit (21/07 13:35) Ã¢â‚¬â€ sÃƒÂ³ XML/Mudar; ÃƒÂ  tarde ainda vendia |
| **Inocente** | **v10.82** (19/07) Ã¢â‚¬â€ dias estÃƒÂ¡veis depois |
| **Defesa** | NÃƒÂ£o remontar catÃƒÂ¡logo gigante na abertura (slim / busca leve Ã¢â‚¬â€ v10.93Ã¢â‚¬â€œ10.97) |

### fix PDV busca v11.48 (= loja v10.92) Ã¢â‚¬â€ 22/07

Busca tecla sem esperar delta; mata N+1 overlay no batch catalogo_agro. Prova: buscar milho ok / delta 500 na loja.

### Ã°Å¸Â©Â¹ Cadastro Ã¢â‚¬â€ foto Delivery sumia + PDV (21/07 Ã‚Â· **teste v11.45**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Ã‚Â«Salvo no SisValeÃ‚Â» mas ao reabrir o modal a foto sumia |
| **Causa** | Server apagava `imagem_base64` > ~900k (arquivo ~700 KB Ã¢â€ â€™ base64 estoura) |
| **Fix** | Comprime no save (Pillow) + no browser (canvas JPEG) Ã‚Â· leitura nÃƒÂ£o descarta foto |
| **PDV** | Foto Delivery vira `imagem` do produto (busca / gestÃƒÂ£o / catÃƒÂ¡logo PG) |
| **Commit** | `2ca4347` |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· Delivery Ã¢â€ â€™ escolher foto Ã¢â€ â€™ Salvar Ã¢â€ â€™ fechar Ã¢â€ â€™ reabrir Ã‚Â· conferir PDV |

### Ã°Å¸Å¡â‚¬ CatÃƒÂ¡logo Ã¢â‚¬â€ famÃƒÂ­lia de embalagens granel+sacos (21/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Card com vÃƒÂ¡rios preÃƒÂ§os (Granel / Saco XXkg / Ã¢â‚¬Â¦) Ã‚Â· vÃƒÂ­nculo manual na aba Delivery Ã‚Â· `+ Add` abre escolha |
| **Dados** | `delivery.embalagens[]` no overlay (sem migrate) Ã‚Â· irmÃƒÂ£os nÃƒÂ£o duplicam card |
| **VocÃƒÂª** | Cadastro Ã¢â€ â€™ Delivery Ã¢â€ â€™ Embalagens no card Ã‚Â· vincular granel+sacos Ã‚Â· `/catalogo/` Ã‚Â· + Add escolhe SKU |

### Ã°Å¸â€œÂ¦ Contagem Ã¢â‚¬â€ data ÃƒÂºltimo AJUSTE PIN no mobile + PDV (21/07 Ã‚Â· **teste v11.40**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Ajuste mobile: coluna com data da ÃƒÂºltima contagem PIN Ã‚Â· PDV ediÃƒÂ§ÃƒÂ£o rÃƒÂ¡pida: data acima do Ã‚Â«NovoÃ‚Â» (Centro/Vila) |
| **Fonte** | ÃƒÅ¡ltimo `AjusteRapidoEstoque` com origem **AJUSTE PIN** Ã‚Â· formato `dd/mm/aa` Ã‚Â· sem data = **Conferir** |
| **Extra** | Salvar estoque no lÃƒÂ¡pis do PDV agora grava como AJUSTE PIN (passa a contar na data) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· `/ajuste-mobile/` e lÃƒÂ¡pis do PDV Ã‚Â· confira data / Conferir |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ Entrada NF Ã‚Â«NovaÃ‚Â» limpa (**v10.90**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã°Å¸â€œÂ¦ **PRONTO PARA ENVIO PARA PRODUÃƒâ€¡ÃƒÆ’O** |
| **Teste** | **v11.38** Ã‚Â· commit **`46add26`** |
| **VERSION loja alvo** | **10.90** (base loja **v10.89**) |
| **Inclui** | SÃƒÂ³ `entrada_nota.html` Ã¢â‚¬â€ botÃƒÂ£o **Nova** zera XML/cabeÃƒÂ§alho/financeiro/rateio (nÃƒÂ£o herda nota anterior) |
| **JÃƒÂ¡ na loja** | V. unit no Mudar + acrÃƒÂ©scimos (**v10.89**) |
| **NÃƒÆ’O inclui** | Dispenser Ã‚Â· merge inteiro do `teste` |
| **Backup no push** | `rollback/pre-entrada-nf-nova-limpa-v10.89` |
| **Autorizar** | *pode subir para produÃƒÂ§ÃƒÂ£o* + **99738595** |
| **Validar apÃƒÂ³s Live** | Ctrl+F5 Ã‚Â· abrir nota Ã‚Â· **Nova** Ã‚Â· em branco Ã‚Â· puxar XML novo |

### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ Nova herdava XML/dados da nota anterior (21/07 Ã‚Â· **teste v11.38**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Ao abrir **Nova**, campos e XML da nota anterior vinham preenchidos / gravavam no rascunho novo |
| **Causa** | BotÃƒÂ£o Nova limpava sÃƒÂ³ parte do cabeÃƒÂ§alho; `ultimaNotaParse` + financeiro + rateio ficavam |
| **Fix** | `entradaNfeResetEditorParaNovaNota` Ã¢â‚¬â€ zera tudo + cancela autosave pendente |
| **Arquivo** | `entrada_nota.html` |
| **Status** | Ã°Å¸â€œÂ¦ **PRONTO PARA ENVIO PARA PRODUÃƒâ€¡ÃƒÆ’O** Ã‚Â· pacote **v10.90** |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· abrir nota Ã‚Â· **Nova** Ã‚Â· deve nascer em branco Ã‚Â· puxar XML novo sem rastro da anterior |

### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ Mudar produto baixava V. unit com acrÃƒÂ©scimos (21/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Box acrÃƒÂ©scimos OK (ex. 75,77 Ã¢â€ â€™ 90,42); ao **Mudar** vÃƒÂ­nculo, V. unit caÃƒÂ­a (ex. ~87,9 = custo do cadastro) |
| **Causa** | `entradaNfeAplicarProdutoNaLinha` apagava `nfeCustNfPreservado` e sobrescrevia com custo do catÃƒÂ¡logo |
| **Fix** | Linha com custo da NF mantÃƒÂ©m V. unit + reaplica rateio; sÃƒÂ³ linha manual sem base NF puxa cadastro |
| **Arquivo** | `entrada_nota.html` |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· XML + acrÃƒÂ©scimos Ã‚Â· Mudar 1 item Ã‚Â· V. unit deve ficar igual ao pÃƒÂ³s-checkbox |

### Ã¢Å¡Â¡ Ajuste mobile Ã¢â‚¬â€ barra carregando sem parar (21/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | Download catÃƒÂ¡logo + poll de **todos** os saldos engolia o worker; barra podia ficar presa (show duplo) |
| **Fix** | Sem poll automÃƒÂ¡tico Ã‚Â· ATUALIZAR = sÃƒÂ³ itens na tela Ã‚Â· cache PDV se jÃƒÂ¡ tiver Ã‚Â· patch saldo no cache Ã‚Â· reset da barra |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· barra some Ã‚Â· salvar rÃƒÂ¡pido Ã‚Â· lista atualiza na hora |

### Ã¢Å¡Â¡ Ajuste mobile Ã¢â‚¬â€ salvar lento no teste (21/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | PÃƒÂ³s-salvar: invalidava catÃƒÂ¡logo + 2Ãƒâ€” refresh de **todos** os saldos; poll a cada 2 s engolia o worker |
| **Fix** | Salvar sÃƒÂ³ grava PG Ã‚Â· atualiza lista local Ã‚Â· poll 15 s Ã‚Â· saldos `?ids=` Ã‚Â· vÃƒÂ­rgula decimal BR |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· salvar um item deve fechar rÃƒÂ¡pido |

### Ã°Å¸ÂÂª Ajuste mobile Ã¢â‚¬â€ depÃƒÂ³sito inicial = loja do PDV (21/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Abre no estoque Centro/Vila do aparelho (mesmo do PDV; se caixa aberto, trava o select) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· confira filtro DepÃƒÂ³sito e faixa Ã‚Â«Estoque: Ã¢â‚¬Â¦Ã‚Â» |

### Ã°Å¸â€Â Ajuste mobile Ã¢â‚¬â€ PIN a cada abertura + operador nos ajustes (21/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Antes** | PIN do PDV/descanso liberava a tela; ajuste podia gravar usuÃƒÂ¡rio do login Django |
| **Agora** | PIN **sempre** ao abrir Ã‚Â· operador do PIN nos ajustes Ã‚Â· chip Ã‚Â«OperadorÃ‚Â» + Trocar |
| **VocÃƒÂª** | Ctrl+F5 `/ajuste-mobile/` Ã‚Â· deve pedir PIN Ã‚Â· Hist. mostra o nome |

### Ã°Å¸Â©Â¹ Ajuste mobile Ã¢â‚¬â€ lista sÃƒÂ³ 7 produtos (21/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | `AGRO_MANUAL_SYNC_ONLY` lia cache curto do PDV e **nÃƒÂ£o** baixava catÃƒÂ¡logo da API |
| **Fix** | Sempre baixa `/api/todos-produtos/` Ã‚Â· **Atualizar** = lista + saldos Ã‚Â· saldos API em PG/ledger sem exigir Mongo |
| **VocÃƒÂª** | Ctrl+F5 em `/ajuste-mobile/` Ã‚Â· resumo deve mostrar **catÃƒÂ¡logo** com milhares Ã‚Â· digite nome/GM se a lista truncar em 400 |

### Ã°Å¸Â©Â¹ Postgres slots Ã¢â‚¬â€ leak BI threads (21/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Causa raiz** | BI `ThreadPoolExecutor` (~14) + `close_old_connections` **nÃƒÂ£o** fecha conexÃƒÂ£o nova Ã¢â€ â€™ slots vazam |
| **Fix** | `connections.close_all()` no worker BI / ERP / NFC-e Ã‚Â· teto **4** workers no BI Ã‚Â· `conn_max_age=60` (rede) |
| **Ops loja** | **FL-057 P0,1** Ã¢â‚¬â€ Render agro-db Ã¢â€ â€™ PgBouncer (URL porta **6432**) Ã‚Â· ver CHECKLIST ÃƒÅ¡NICO |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· abrir BI em 2Ã¢â‚¬â€œ3 abas Ã‚Â· sem 500 |

### Ã°Å¸â€Â Incidente manhÃƒÂ£ 21/07 Ã¢â‚¬â€ site nÃƒÂ£o abria **antes** do deploy (confirmaÃƒÂ§ÃƒÂ£o Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Loja 500 / deploy v10.86 falhou no `migrate` Ã‚Â· `FATAL: remaining connection slots are reserved for SUPERUSER` |
| **NÃƒÂ£o foi** | Bug do pacote Caixa VilaÃƒâ€”Centro |
| **Foi** | Postgres (`agro-db`) **jÃƒÂ¡ sem slot livre** Ã¢â‚¬â€ site jÃƒÂ¡ falhava; o deploy sÃƒÂ³ **revelou** (migrate precisa de 1 conexÃƒÂ£o) |
| **Por quÃƒÂª enche** | BI abria ~14 threads ORM; `close_old_connections` nÃƒÂ£o fecha conexÃƒÂ£o nova Ã¢â€ â€™ vazamento de slots (+ deploy/cron) |
| **RecuperaÃƒÂ§ÃƒÂ£o** | Restart Database no PG + Restart web |
| **MitigaÃƒÂ§ÃƒÂ£o loja** | **v10.87** `conn_max_age` padrÃƒÂ£o **60s** (`DJANGO_CONN_MAX_AGE`) |
| **CorreÃƒÂ§ÃƒÂ£o raiz** | ver CHECKPOINT Ã‚Â«Postgres slots Ã¢â‚¬â€ leak BI threadsÃ‚Â» (`close_all` + teto 4) |

### Ã°Å¸Â©Â¹ Caixa Ã¢â‚¬â€ diferenÃƒÂ§a abertura + quem abriu/fechou (21/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Antes** | DiferenÃƒÂ§a sÃƒÂ³ no fechamento; abertura nÃƒÂ£o ia pra ConferÃƒÂªncias; fechamento sem usuÃƒÂ¡rio gravado |
| **Agora** | Abertura grava sugestÃƒÂ£o + diferenÃƒÂ§a Ã‚Â· linha **Abertura Ã‚Â· Dinheiro** em ConferÃƒÂªncias Ã‚Â· `usuario` + `usuario_fechamento` |
| **Migration** | `0062_sessaocaixa_diferenca_abertura_usuario_fechamento` |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· abrir com valor Ã¢â€°Â  card azul Ã‚Â· fechar Ã‚Â· ConferÃƒÂªncias Ã‚Â· ver linha + Ã‚Â«Abriu / FechouÃ‚Â» |

### Ã°Å¸Â©Â¹ Abrir caixa Ã¢â‚¬â€ botÃƒÂ£o CÃƒÂ©dulas (21/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | CÃƒÂ©dulas na abertura Ã‚Â· texto no **?** (ao lado do Menu) Ã‚Â· modal quase tela cheia |
| **Arquivos** | `caixa_abrir.html` Ã‚Â· `caixa_cedulas_abertura_modal.html` Ã‚Â· `agro_pdv_overlay.js` Ã‚Â· `caixa_pdv_overlay_mode.html` |
| **VocÃƒÂª** | Ctrl+F5 (PDV+abrir) Ã‚Â· **?** Ã‚Â· **CÃƒÂ©dulas** grande Ã‚Â· hitbox do botÃƒÂ£o sÃƒÂ³ no botÃƒÂ£o (v10.99) |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.87** Ã¢â‚¬â€ Caixa VilaÃƒâ€”Centro + conn (21/07 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢ÂÂ³ push \producao\ **8b802c1** Ã‚Â· aguardar Live |
| **Inclui** | pacote caixa v10.86 + conn_max_age 60s |
| **Backup** | rollback/pre-caixa-vila-centro-v10.82 |



**VersÃƒÂ£o app (VERSION):** **teste v11.42** Ã‚Â· **loja v10.87** (push 8b802c1) Ã‚Â· rollback `pre-caixa-vila-centro-v10.82`

### Ã°Å¸Â©Â¹ LogÃƒÂ­stica/TransferÃƒÂªncias Ã¢â‚¬â€ abertura lenta (21/07 Ã‚Â· teste)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | `/transferencias/` demorava em Ã‚Â«carregando sugestÃƒÂµesÃ‚Â» |
| **Causa** | API: N+1 overlay Ã‚Â· varria histÃƒÂ³rico inteiro de ajustes Vila Ã‚Â· buscava ajustes 2Ã¢â‚¬â€œ3Ãƒâ€” Ã‚Â· DELETE pedido um a um no GET |
| **Feito** | info leve + overlay em lote Ã‚Â· DISTINCT ON Vila Ã‚Â· saldos reutilizados Ã‚Â· limpeza de pedido em lote |
| **Arquivos** | `produtos/estoque_saldo_agro_util.py` Ã‚Â· `estoque/views.py` |
| **VocÃƒÂª** | Abrir LogÃƒÂ­stica no **teste** Ã¢â‚¬â€ deve abrir bem mais rÃƒÂ¡pido |
| **Status** | Ã¢ÂÂ³ push `teste` |

### WIP Ã¢â‚¬â€ Dispenser A6 (visual)

| Item | Detalhe |
| ---- | ------- |
| **URL** | `/interno/dispenser-a6/` (login) |
| **JÃƒÂ¡ na loja** | Nuvem + Prontas + cores + zoom logo (`v11.90`) |
| **Pacote pronto** | **`DSP-SABORES`** Ã‚Â· commit **`ed43464`** Ã‚Â· loja alvo **v11.91** Ã‚Â· ver topo CHECKPOINT |
| **Blueberry** | Ã¢Å“â€¦ jÃƒÂ¡ existia (Mirtilo) |
| **Status** | Ã¢Å“â€¦ revisado + dry-run cherry OK Ã‚Â· aguarda pausa vendas + frase+senha |
| **Regra** | Cherry sÃƒÂ³ `ed43464` Ã‚Â· sem merge inteiro `teste` |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.86** Ã¢â‚¬â€ Caixa Vila Ãƒâ€” Centro (21/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push \producao\ **7c11ad8** Ã‚Â· aguardar Live no Render |
| **VERSION** | **loja v10.86** (antes v10.82) |
| **Inclui** | (1) Vila nÃƒÂ£o adota Centro Ã‚Â· (2) fechar sÃƒÂ³ trava prÃƒÂ³pria loja Ã‚Â· (3) catÃƒÂ¡logo SEM DONO/Assumir Ã‚Â· (4) Quem PIN Ã‚Â· (5) rÃƒÂ³tulo Caixa Centro/Vila Ã‚Â· (6) sem TRAVADO duplicado Ã‚Â· (7) wizard fiado sÃƒÂ³ da prÃƒÂ³pria loja |
| **NÃƒÆ’O inclui** | Dispenser Ã‚Â· MP automÃƒÂ¡tico Ã‚Â· merge inteiro teste |
| **Backup / reverter** | ollback/pre-caixa-vila-centro-v10.82\ @ **1aa95dc** |
| **Como reverter** | \git push origin rollback/pre-caixa-vila-centro-v10.82:producao\ |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir para produÃƒÂ§ÃƒÂ£o* + **99738595** |
| **ApÃƒÂ³s Live** | Ctrl+F5 Vila: Caixa sem # Ã‚Â· sem TRAVADO Ã‚Â· fechar livre entrega Centro Ã‚Â· fiado sÃƒÂ³ da Vila |


### Ã°Å¸â€œÂ¦ Deploy loja **v10.82** Ã¢â‚¬â€ Hotfix entregas PIN + Imprimir (19/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push producao **1aa95dc** Ã‚Â· aguardar Live |
| **Inclui** | (1) nÃƒÂ£o abrir overlay sob Modo descanso Ã‚Â· abre apÃƒÂ³s PIN Ã‚Â· (2) popup Imprimir em `<dialog>` na frente |
| **NÃƒÆ’O inclui** | Merge inteiro `teste` Ã‚Â· sem migrate nova |
| **Base** | `73215fc` (loja v10.81) |
| **Backup / reverter** | `rollback/pre-pdv-entregas-pin-print-v10.81` @ **73215fc** |
| **Como reverter** | `git push origin rollback/pre-pdv-entregas-pin-print-v10.81:producao` |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir para produÃƒÂ§ÃƒÂ£o* + **99738595** Ã‚Â· seguro + checkpoint |
| **Risco** | MÃƒÂ­nimo Ã¢â‚¬â€ sÃƒÂ³ JS/HTML do PDV overlay |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v10.82** Ã‚Â· PIN Ã¢â€ â€™ Assumir Ã‚Â· Imprimir por cima do overlay |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ Entregas nÃƒÂ£o clicÃƒÂ¡veis sob Modo descanso/PIN (19/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Alerta do catÃƒÂ¡logo abria o modal **por cima** do PIN; `sspin-locked` zera `pointer-events` Ã¢â€ â€™ Assumir/Imprimir sem clique |
| **Fix** | Com PIN ativo: **nÃƒÂ£o** abre o modal (sÃƒÂ³ toast + badge); apÃƒÂ³s desbloquear PIN, abre sozinho Ã‚Â· CSS libera clique se o dialog jÃƒÂ¡ estiver aberto |
| **VocÃƒÂª** | Ctrl+F5 no **teste** Ã‚Â· PIN na tela + pedido catÃƒÂ¡logo Ã¢â€ â€™ digita PIN Ã¢â€ â€™ modal abre Ã‚Â· Assumir ok |
| **Loja** | **v10.82** **1aa95dc** |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ popup Imprimir atrÃƒÂ¡s do overlay entregas (19/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Ã‚Â«ImprimirÃ‚Â» no overlay abria escolha de vias **atrÃƒÂ¡s** do dialog Entregas (top layer) |
| **Fix** | Modal Ã‚Â«O que imprimir?Ã‚Â» virou `<dialog showModal>` Ã¢â‚¬â€ fica na frente do overlay |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· Entregas Ã¢â€ â€™ Imprimir Ã¢â€ â€™ escolher vias na frente |
| **Loja** | **v10.82** **1aa95dc** |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.81** Ã¢â‚¬â€ Assumir entrega catÃƒÂ¡logo CentroÃƒâ€”Vila (19/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push producao **73215fc** Ã‚Â· aguardar Live |
| **Inclui** | Assumir entrega Ã‚Â· filtro loja Ã‚Â· imprint 3 vias Ã‚Â· migrate `0061` |
| **NÃƒÆ’O inclui** | Merge inteiro `teste` |
| **Base** | `cbe97cb` (loja v10.80) |
| **Cherry** | `bce06da` Ã¢â€ â€™ pacote **73215fc** v10.81 |
| **Backup / reverter** | `rollback/pre-assumir-entrega-v10.80` @ **cbe97cb** |
| **Como reverter** | `git push origin rollback/pre-assumir-entrega-v10.80:producao` |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir para produÃƒÂ§ÃƒÂ£o* + **99738595** Ã‚Â· seguro + checkpoint |
| **Risco** | Baixo Ã¢â‚¬â€ PDV overlay + API + migrate additive (campos novos blank) |
| **VocÃƒÂª** | Ctrl+F5 PDV Ã‚Â· badge **v10.81** Ã‚Â· pedido catÃƒÂ¡logo Ã‚Â· Assumir numa loja Ã‚Â· Render migrate Live |

### Ã°Å¸Å¡â‚¬ PDV Ã¢â‚¬â€ Assumir entrega catÃƒÂ¡logo CentroÃƒâ€”Vila (19/07 Ã‚Â· **teste v10.82** Ã‚Â· **bce06da**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ teste **bce06da** Ã‚Â· **loja v10.81** **73215fc** |
| **O quÃƒÂª** | Pedido catÃƒÂ¡logo sem dono nas **duas** lojas Ã‚Â· badge/alerta forte Ã‚Â· **Assumir** = depÃƒÂ³sito PDV Ã‚Â· some da outra Ã‚Â· **imprime 3 vias** Ã‚Â· botÃƒÂ£o **Imprimir** no overlay |
| **Migrate** | `0061_pedido_entrega_loja` (`loja_entrega`, `loja_assumida_em/por`) Ã¢â‚¬â€ Render apply no deploy |
| **API** | `POST /api/pdv/entrega-pendente/<pk>/assumir/` Ã‚Â· lista `?loja=` |
| **VocÃƒÂª** | Pedido no `/catalogo/` Ã¢â€ â€™ PDV Centro e Vila veem Ã‚Â· Assumir na Vila Ã¢â€ â€™ some no Centro Ã‚Â· 3 vias Ã‚Â· Imprimir de novo Ã‚Â· Retomar PDV legado ok |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.80** Ã¢â‚¬â€ CatÃƒÂ¡logo sem texto extra do WhatsApp (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push producao **cbe97cb** Ã‚Â· aguardar Live |
| **Backup / reverter** | 
ollback/pre-catalogo-texto-wa-v10.67 @ **6e78f8b** |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.67** Ã¢â‚¬â€ CatÃƒÂ¡logo WA 1Ã‚Âº + OG delivery raÃƒÂ§ÃƒÂ£o (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push producao **6e78f8b** Ã‚Â· aguardar Live |
| **Backup / reverter** | 
ollback/pre-catalogo-wa-og-v10.66 @ **237a2b8** |
| **VocÃƒÂª** | badge **v10.67** Ã‚Â· Extrair novamente Ã‚Â· testar WA no checkout |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.66** Ã¢â‚¬â€ OG card 1200Ãƒâ€”630 sem cortar logo (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push producao **237a2b8** Ã‚Â· aguardar Live |
| **Backup / reverter** | 
ollback/pre-catalogo-og-card-v10.65 @ **bdb1749** |
| **VocÃƒÂª** | Debugger Ã‚Â«Raspar novamenteÃ‚Â» Ã‚Â· Zap conversa nova |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.65** Ã¢â‚¬â€ CatÃƒÂ¡logo preview WhatsApp / OG (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push producao **bdb1749** Ã‚Â· aguardar Live |
| **Inclui** | Meta Open Graph + /catalogo/og-image/ (logo da gestÃƒÂ£o) |
| **Backup / reverter** | 
ollback/pre-catalogo-og-v10.64 @ **e0471d0** |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir* + **99738595** |
| **VocÃƒÂª** | badge **v10.65** Ã‚Â· colar link Ã‚Â· Debugger se cache |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.64** Ã¢â‚¬â€ Renan sem NFC-e auto + fechar ordem (18/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push producao |
| **Backup / reverter** | 
ollback/pre-renan-nfce-fechar-ordem-v10.63 @ **3667759** |
| **Como reverter** | git push origin rollback/pre-renan-nfce-fechar-ordem-v10.63:producao |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v10.64** Ã‚Â· checklist manhÃƒÂ£ |

| **Smoke 18/07 noite** | Ã¢Å“â€¦ abrir caixa loja Ã‚Â· venda R\$ 0,01 dinheiro Ã‚Â· saÃƒÂ­da com caixa fechado bloqueia |


### Ã°Å¸Â©Â¹ Fechar caixa Ã¢â‚¬â€ ordem fixa das formas + sem Fiado (18/07 Ã‚Â· **teste v10.69**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push 	este Ã‚Â· validar no Render |
| **Ordem** | Dinheiro Ã¢â€ â€™ Pix MP Ã¢â€ â€™ PIX Ã¢â€ â€™ DÃƒÂ©bito MP Ã¢â€ â€™ DÃƒÂ©bito Ã¢â€ â€™ CrÃƒÂ©dito MP Ã¢â€ â€™ CrÃƒÂ©dito Ã¢â€ â€™ Vale Ã¢â€ â€™ Cashback Ã¢â€ â€™ Outro |
| **Fiado** | Fora da conferÃƒÂªncia por valor (wizard de notas continua) |
| **Antes** | Formas com movimento subiam pro topo |
| **VocÃƒÂª** | Ctrl+F5 /caixa/fechar/ Ã‚Â· lista nessa ordem Ã‚Â· sem linha Fiado |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ Mercado Pago Renan sem NFC-e automÃƒÂ¡tica (18/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ no teste Ã‚Â· validar Ã‚Â· loja sobe com senha |
| **Regra** | SÃƒÂ³ Renan sem cupom fiscal automÃƒÂ¡tico |


### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ Mercado Pago Renan sem NFC-e automÃƒÂ¡tica (18/07 Ã‚Â· **teste v10.68**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push 	este Ã‚Â· validar no Render |
| **Regra** | SÃƒÂ³ **Mercado Pago Renan** (mp_renan / pix_mp_renan) Ã¢â‚¬â€ dÃƒÂ©bito, crÃƒÂ©dito, parcelado, Pix **sem** cupom fiscal automÃƒÂ¡tico |
| **NÃƒÂ£o muda** | Cielo Ã‚Â· Sicredi Ã‚Â· MP BalcÃƒÂ£o/Pix automÃƒÂ¡tico |
| **Depois** | Se precisar NFC-e: Consultar vendas Ã¢â€ â€™ Reemitir |
| **Arquivos** | 
fce_config_util.py Ã‚Â· pdv_wizard.js |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· pagar na Renan Ã¢â€ â€™ venda OK Ã‚Â· **sem** NFC-e Ã‚Â· Cielo continua emitindo |


### Ã°Å¸â€œÂ¦ Deploy loja **v10.63** Ã¢â‚¬â€ kardex + C1Ã¢â‚¬â€œC3 + Zap legado (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push producao **1e6fe3c** |
| **Backup / reverter** | 
ollback/pre-kardex-c1c3-zap-v10.62 @ **bc911d5** |
| **Como reverter** | git push origin rollback/pre-kardex-c1c3-zap-v10.62:producao |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v10.63** Ã‚Â· Quem sem @ Ã‚Â· NF etapa 8 Ã‚Â· Zap AGROMAIS |


### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ v10.63 kardex + C1Ã¢â‚¬â€œC3 + Zap legado (18/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã°Å¸â€œÂ¦ **commit pronto** 1e6fe3c Ã‚Â· tag rollback 
ollback/pre-kardex-c1c3-zap-v10.62 @ bc911d5 Ã‚Â· **faltando senha p/ push** |
| **Inclui** | Kardex e-mail+ÃŽâ€camada Ã‚Â· Entrada NF C1Ã¢â‚¬â€œC3 Ã‚Â· Zap AGROMAIS no /consulta/ Ã‚Â· abas cadastro null-safe |
| **Conferido** | JÃƒÂ¡ na loja = Compras UI, aba 9, catÃƒÂ¡logo, BI Ã¢â‚¬â€ **nÃƒÂ£o** reenviados |
| **Autorizar** | *Ã‚Â«pode subir pacote v10.63Ã‚Â»* + **99738595** |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge v10.63 Ã‚Â· Quem sem @ Ã‚Â· NF etapa 8 Ã‚Â· Enviar Zap AGROMAIS |


### Ã°Å¸â€œÂ¦ Deploy loja **v10.62** Ã¢â‚¬â€ CatÃƒÂ¡logo GPS checkout (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push producao (este commit) Ã‚Â· aguardar Live |
| **Inclui** | GPS/Plus Code no finalizar pedido Ã‚Â· esconde Plus do cliente Ã‚Â· API /catalogo/api/localizacao/ |
| **NÃƒÆ’O inclui** | Compras UI Ã‚Â· merge inteiro teste Ã‚Â· horÃƒÂ¡rio/agendamento FOOD |
| **Base** | e38df10 (loja v10.61 aba 9) |
| **Backup / reverter** | 
ollback/pre-catalogo-gps-v10.61 @ **e38df10** |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir para produÃƒÂ§ÃƒÂ£o* + **99738595** Ã‚Â· pedido de checkpoint |
| **Risco** | Baixo Ã¢â‚¬â€ sÃƒÂ³ checkout /catalogo/ Ã‚Â· sem migrate Ã‚Â· PDV/caixa intactos |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v10.62** Ã‚Â· testar GPS no finalizar Ã‚Â· entregas com Plus |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.61** Ã¢â‚¬â€ aba 9 histÃƒÂ³rico sÃƒÂ³ o que mudou (18/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push e38df10 Ã‚Â· rollback 
ollback/pre-aba9-historico-v10.60 @ c88ce66 |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· lÃƒÂ¡pis 1 centavo Ã‚Â· aba 9 = 1 linha |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.60** Ã¢â‚¬â€ Compras UI etapa 1 (18/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push c88ce66 Ã‚Â· rollback 
ollback/pre-compras-ui-v10.59 @ c352575 |
| **VocÃƒÂª** | Ctrl+F5 /compras/ |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.59** Ã¢â‚¬â€ BI mensagem + validade + faturamento 2 barras (18/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push c352575 Ã‚Â· rollback 
ollback/pre-bi-validade-msg-v10.58 @ 1e211ae |
| **Print 1** | Frase amigÃƒÂ¡vel (sem .env) |
| **Print 2** | Conf. zera na loja Ã‚Â· filtro Loja na validade |
| **Print 3** | Faturamento mostra Centro + Vila |
| **VocÃƒÂª** | Ctrl+F5 BI Vila |

### Ã°Å¸Â©Â¹ BI Vila Ã¢â‚¬â€ top clientes + validade + faturamento (18/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Print 1** | Mensagem vazia amigÃƒÂ¡vel |
| **Print 2** | Conf. sÃƒÂ³ com saldo da loja Ã‚Â· tela validade com filtro |
| **Print 3** | Faturamento sempre as duas lojas |


### Ã°Å¸â€œÂ¦ Deploy loja **v10.58** Ã¢â‚¬â€ CatÃƒÂ¡logo delivery (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push producao **96e157a** Ã‚Â· aguardar Live + migrate 0057Ã¢â‚¬â€œ0060 |
| **Inclui** | Vitrine /catalogo/ Ã‚Â· gestÃƒÂ£o Ã‚Â· categorias/foto/logo Ã‚Â· WhatsApp Ã‚Â· Pillow |
| **NÃƒÆ’O inclui** | Compras UI Ã‚Â· Entrada NF grande Ã‚Â· merge inteiro |
| **Base** | 55231f0 (loja v10.57 FL-024) |
| **Backup / reverter** | 
ollback/pre-catalogo-delivery-v10.57 @ **55231f0** |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode enviar* + **99738595** |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v10.58** Ã‚Â· /catalogo/ |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.57** Ã¢â‚¬â€ FL-024 picklist (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push producao **55231f0** |
| **Backup / reverter** | 
ollback/pre-fl024-picklist-v10.56 @ **c030d07** |


### Ã°Å¸â€œÂ¦ Deploy loja **v10.56** Ã¢â‚¬â€ BI Vila zeros + trava caixa (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `producao` **c030d07** Ã‚Â· aguardar Live + smoke |
| **Inclui** | BI Vila sem vazamento Centro Ã‚Â· saÃƒÂ­da/reforÃƒÂ§o/devoluÃƒÂ§ÃƒÂ£o/fiado exigem caixa aberto Ã‚Â· sem CentroÃƒâ€”Vila cruzado |
| **NÃƒÆ’O inclui** | CatÃƒÂ¡logo delivery Ã‚Â· FL-024 Ã‚Â· Compras UI Ã‚Â· resto sÃƒÂ³ no teste |
| **MÃƒÂ©todo** | cherry-pick 3 commits Ã‚Â· **nÃƒÂ£o** merge inteiro |
| **Base** | `a79831c` (loja v10.48) |
| **Backup / reverter** | `rollback/pre-bi-caixa-lock-v10.48` @ **a79831c** |
| **Risco Centro** | Baixo Ã¢â‚¬â€ sÃƒÂ³ filtra BI e endurece regras de caixa |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir para produÃƒÂ§ÃƒÂ£o* + **99738595** |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v10.56** Ã‚Â· BI Vila limpo Ã‚Â· caixa fechado Ã¢â€ â€™ sem saÃƒÂ­da/reforÃƒÂ§o/devolver |

### Ã°Å¸â€Â PrÃƒÂ©-produÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ revisÃƒÂ£o catÃƒÂ¡logo + divergÃƒÂªncia (18/07 Ã‚Â· este chat)

| Item | Detalhe |
| ---- | ------- |
| **Loja hoje** | **v10.58** Ã¢â‚¬â€ catÃƒÂ¡logo delivery **no ar** (push `96e157a`) |
| **Teste hoje** | **v10.56+** Ã¢â‚¬â€ alinhado no mÃƒÂ³dulo catÃƒÂ¡logo |
| **Veredito** | Pacote isolado Ã¢Å“â€¦ Ã‚Â· migrate 0057Ã¢â‚¬â€œ0060 no deploy Ã‚Â· `publicado` comeÃƒÂ§a off |
| **Rollback** | `rollback/pre-catalogo-delivery-v10.57` @ **55231f0** |

### Ã°Å¸Å¡Â¨ Caixa Ã¢â‚¬â€ trava fechado + loja cruzada (18/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | SaÃƒÂ­da/reforÃƒÂ§o/devoluÃƒÂ§ÃƒÂ£o/fiado/assumir podiam rodar com caixa fechado ou bater no turno da **outra** loja |
| **Fix** | Regra ÃƒÂºnica: turno sÃƒÂ³ da loja do aparelho Ã‚Â· reforÃƒÂ§o/movimento/assumir/venda/entrega sem ID cruzado Ã‚Â· devoluÃƒÂ§ÃƒÂ£o exige caixa da **mesma loja da venda** Ã‚Â· fiado nÃƒÂ£o aceita Ã‚Â«sem caixaÃ‚Â» |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· caixa fechado Ã¢â€ â€™ reforÃƒÂ§o/saÃƒÂ­da/devolver/fiado recusam Ã‚Â· BI Vila + tentar Centro = bloqueio |

### Ã°Å¸Å¡Â¨ Retiradas Ã¢â‚¬â€ bloqueio com caixa fechado (18/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Com BI em Vila, aba Centro no histÃƒÂ³rico + **Nova saÃƒÂ­da** gravava no financeiro **sem caixa aberto**; legado caÃƒÂ­a na lista Centro |
| **Fix** | API exige caixa aberto da loja do aparelho Ã‚Â· formulÃƒÂ¡rio/botÃƒÂ£o travados se fechado Ã‚Â· chips Centro/Vila = **sÃƒÂ³ filtro da lista** |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· caixa fechado Ã¢â€ â€™ **Nova saÃƒÂ­da** cinza Ã‚Â· tentar URL direta Ã¢â€ â€™ volta pro histÃƒÂ³rico Ã‚Â· abrir caixa Ã¢â€ â€™ aÃƒÂ­ registra |

### Ã°Å¸Â©Â¹ BI Vila Ã¢â‚¬â€ zerar cards que vazavam Centro (18/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Vila com Vendas R$ 0 ainda mostrava ticket, ranking vendedor, top clientes, entregas, novos clientes, barra Centro no faturamento, validade |
| **Fix** | Sem fallback Mongo/ERP com loja filtrada Ã‚Â· ticket/ranking/top/entregas/validade por depÃƒÂ³sito Ã‚Â· novos clientes = 0 na Vila Ã‚Â· grÃƒÂ¡fico unidade sÃƒÂ³ a loja |
| **CP/CR** | Continuam da **empresa** (financeiro compartilhado) Ã¢â‚¬â€ nÃƒÂ£o mudou |
| **VocÃƒÂª** | Ctrl+F5 no **teste** Ã‚Â· Loja **Vila Elias** Ã¢â€ â€™ ranking vazio Ã‚Â· ticket 0 Ã‚Â· top clientes vazio Ã‚Â· entregas 0 Ã‚Â· novos 0 Ã‚Â· sem barra Centro |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.48** Ã¢â‚¬â€ BI + retiradas por loja (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `producao` **a79831c** Ã‚Â· aguardar Live + smoke |
| **Inclui** | BI filtra pela loja Ã‚Â· Retiradas chips Centro/Vila/Todas |
| **Base** | `eacbe2c` (v10.28) |
| **Backup / reverter** | `rollback/pre-bi-retiradas-v10.28` @ **eacbe2c** |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir para produÃƒÂ§ÃƒÂ£o* + **99738595** |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v10.48** Ã‚Â· BI Vila Ã¢â€ â€™ ~R$ 0 Ã‚Â· Retiradas Vila Ã¢â€ â€™ sem Centro |

### Ã°Å¸Â©Â¹ Retiradas/saÃƒÂ­das Ã¢â‚¬â€ filtro por loja Centro/Vila (18/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Com aparelho em Vila Elias, histÃƒÂ³rico de saÃƒÂ­das ainda listava Centro |
| **Fix** | Filtra pelo turno (GavetaÃƒâ€”Vila) Ã‚Â· chips Centro/Vila/Todas Ã‚Â· padrÃƒÂ£o = loja do aparelho |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· Retiradas Ã¢â€ â€™ chip **Vila Elias** Ã¢â€ â€™ lista vazia/sÃƒÂ³ Vila Ã‚Â· **Centro** Ã¢â€ â€™ saÃƒÂ­das do dia |

### Ã°Å¸Â©Â¹ BI Ã¢â‚¬â€ seletor loja filtra nÃƒÂºmeros + recarrega (18/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Trocar Loja no BI sÃƒÂ³ mudava badge/estoque; Performance/Vendas continuavam do Centro |
| **Fix** | KPI/sÃƒÂ©rie/ticket/ranking filtrados pelo depÃƒÂ³sito do aparelho Ã‚Â· ao confirmar loja Ã¢â€ â€™ **reload** Ã‚Â· grÃƒÂ¡fico Ã‚Â«por unidadeÃ‚Â» continua comparando as duas |
| **VocÃƒÂª** | No **teste**: Loja Ã¢â€ â€™ Vila Elias Ã¢â€ â€™ digitar `vila` Ã¢â€ â€™ tela recarrega Ã‚Â· Performance ~R$ 0 se ainda sem venda Vila |

### Ã°Å¸â€œÂ¦ Deploy loja **v10.28** Ã¢â‚¬â€ pacote Vila Elias / Centro (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `producao` **eacbe2c** Ã‚Â· Render migrate 0055/0056 Ã‚Â· aguardar Live + smoke |
| **Inclui** | DepÃƒÂ³sito CentroÃƒâ€”Vila Ã‚Â· caixa separado Ã‚Â· antiburro Ã‚Â· bloqueio sem caixa Ã‚Â· badge/aviso Ã‚Â· filtro vendas/relatÃƒÂ³rio Ã‚Â· MP Renan Ã‚Â· saldo cadastro recente |
| **NÃƒÆ’O inclui** | CatÃƒÂ¡logo delivery Ã‚Â· FL-024 picklist Ã‚Â· Compras UI Ã‚Â· resto sÃƒÂ³ no teste |
| **MÃƒÂ©todo** | cherry-pick 12 commits Ã‚Â· **nÃƒÂ£o** merge inteiro |
| **Base** | `c0620af` (loja v9.91) |
| **Backup / reverter** | `rollback/pre-vila-v9.91` (= `producao-backup-pre-v1028-vila-20260718` @ **c0620af**) |
| **Env** | var estoque **ausente** Ã¢Å“â€¦ |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir para produÃƒÂ§ÃƒÂ£o* + **99738595** |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v10.28** Ã‚Â· smoke Gaveta: abrir Ã¢â€ â€™ vender 1 Ã¢â€ â€™ fechar Ã¢â€ â€™ relatÃƒÂ³rio **Centro** |

### Ã°Å¸â€Â RevisÃƒÂ£o risco pacote Vila Ã¢â€ â€™ CENTRO (18/07 Ã‚Â· agente Opus)

| Item | Detalhe |
| ---- | ------- |
| **Veredito** | **SEGURO para o Centro** Ã¢â‚¬â€ venda segue o caixa aberto, nÃƒÂ£o o cookie |
| **Antes de abrir** | Env loja: var **ausente** Ã¢Å“â€¦ Ã‚Â· migrate no deploy Ã‚Â· **loja v10.28** push Ã‚Â· smoke Gaveta pendente |
| **AtenÃƒÂ§ÃƒÂ£o Centro** | Agora **exige caixa aberto** pra vender Ã‚Â· relatÃƒÂ³rio default = loja do PC (usar chip Centro/Todas) |
| **Vila** | Volume baixo OK Ã‚Â· Point auto **nÃƒÂ£o** Ã‚Â· NFC-e ainda CNPJ Centro |

### feat Ã¢â‚¬â€ Caixa relatÃƒÂ³rio/conferÃƒÂªncias por loja (18/07 Ã‚Â· **teste v10.28**)

| Item | Detalhe |
| ---- | ------- |
| **Achado** | Saldo / reforÃƒÂ§o / retirada / fechar jÃƒÂ¡ usavam o turno da loja Ã‚Â· relatÃƒÂ³rio e conferÃƒÂªncias misturavam Centro+Vila |
| **Fix** | Filtro Centro / Vila / Todas (padrÃƒÂ£o = loja do PC) Ã‚Â· MP Renan jÃƒÂ¡ soma nas linhas Ã‚Â«Ã¢â‚¬â€ Mercado PagoÃ‚Â» |
| **Validar** | RelatÃƒÂ³rio e ConferÃƒÂªncias Ã¢â€ â€™ chips Loja Ã‚Â· saldo do turno aberto |

### feat Ã¢â‚¬â€ PDV maquininha Mercado Pago Renan (18/07 Ã‚Â· **teste v10.26**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | OpÃƒÂ§ÃƒÂ£o **Mercado Pago Renan** no seletor de mÃƒÂ¡quina (cartÃƒÂ£o + Pix) |
| **Comportamento** | Manual (nÃƒÂ£o dispara Point automÃƒÂ¡tico) Ã‚Â· aparece no notebook tambÃƒÂ©m |
| **Validar** | PDV Ã¢â€ â€™ pagar cartÃƒÂ£o/Pix Ã¢â€ â€™ Selecionar mÃƒÂ¡quina Ã¢â€ â€™ 3Ã‚Âª opÃƒÂ§ÃƒÂ£o Renan |

### Ã°Å¸Â©Â¹ Cadastro Ã¢â‚¬â€ saldo busca ficava velho apÃƒÂ³s venda (18/07 Ã‚Â· **teste v10.23**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Venda baixava Vila 10Ã¢â€ â€™9 no banco, Cadastro busca ainda mostrava C10Ã‚Â·V10 |
| **Causa** | `/api/buscar/` pegava ajuste **qualquer** (ÃƒÂ s vezes o antigo), nÃƒÂ£o o mais recente |
| **Fix** | Mesma regra de `ajustes_mais_recentes` + Cadastro recalcula saldo no envelope |
| **Validar local** | Reinicia servidor Ã‚Â· busca Ã‚Â«teste divisÃƒÂ£oÃ‚Â» Ã¢â€ â€™ **V 9** |

### Ã°Å¸Â©Â¹ Cadastro Ã¢â‚¬â€ fase 2 picklist (gestÃƒÂ£o / overlay / unidade / PDV) (18/07 Ã‚Â· **teste v10.20**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Mesmo padrÃƒÂ£o FL-024 em: cadastro **unidade**, **gestÃƒÂ£o** drawer, **overlay** lista ERP, **PDV lÃƒÂ¡pis** unidade (Cadastrar exige PIN) |
| **JS** | `agro_picklist.js` compartilhado Ã‚Â· facetas incluem `unidades` |
| **Validar** | GestÃƒÂ£o: digitar marca inventada Ã¢â€ â€™ barra Ã‚Â· PDV lÃƒÂ¡pis: Cadastrar unidade Ã¢â€ â€™ PIN |

### Ã°Å¸Â©Â¹ Cadastro Ã¢â‚¬â€ marca/cat sÃƒÂ³ lista + PIN (FL-024) (18/07 Ã‚Â· **teste v10.19**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Marca / fornecedor / categoria / sub: buscar e **sÃƒÂ³ selecionar**; digitar solto **nÃƒÂ£o** grava; **+** abre popup com parecidos + **PIN** + log |
| **Busca** | Sem acento / caixa |
| **Validar** | Digitar marca inventada Ã¢â€ â€™ Salvar barra Ã‚Â· escolher da lista OK Ã‚Â· + com PIN cria |

### Ã°Å¸Â©Â¹ Cadastro Ã¢â‚¬â€ cÃƒÂ³digo sistema+GM no produto novo sem agro_pg (18/07 Ã‚Â· **teste v10.17**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Local (catÃƒÂ¡logo Mongo): Novo produto Ã¢â€ â€™ Fiscal sem cÃƒÂ³digo sistema/GM; loja `agro_pg` preenchia OK |
| **Fix** | Detalhe `__novo__` no caminho Mongo tambÃƒÂ©m aloca sequÃƒÂªncia 4010+ / `GM####` |
| **Validar** | Local: Novo produto Ã¢â€ â€™ aba Fiscal Ã¢â€ â€™ cÃƒÂ³digo + GM preenchidos |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ badge estoque + aviso caixa sem F5 (18/07 Ã‚Â· **teste v10.14**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Abrir Centro: badge ficava Ã‚Â«Vila EliasÃ‚Â» atÃƒÂ© F5 Ã‚Â· Fechar Centro no overlay: sumia o painel mas sem faixa Ã‚Â«Caixa fechadoÃ‚Â» e dava pra montar venda atÃƒÂ© o fim |
| **Fix** | Aviso sempre no HTML (sÃƒÂ³ some/aparece) Ã‚Â· refresh puxa `pdvDeposito` + badge Ã‚Â· iframe avisa `agro-pdv-caixa-changed` Ã‚Â· bloqueia item/avanÃƒÂ§ar com caixa fechado |
| **Validar** | Abrir Centro no PDV Ã¢â€ â€™ badge **Centro** na hora Ã‚Â· Fechar Ã¢â€ â€™ faixa ÃƒÂ¢mbar + nÃƒÂ£o deixa adicionar item |

### Ã°Å¸Â©Â¹ Caixa fechado Ã¢â‚¬â€ bloqueia venda + refresh no PDV (18/07 Ã‚Â· **teste v10.11**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | ApÃƒÂ³s fechar caixa no overlay, PDV ainda vendia (bootstrap velho) e servidor aceitava venda sem turno |
| **Fix** | API exige caixa aberto Ã‚Â· PDV revalida caixa antes de cada venda Ã‚Â· ao fechar overlay atualiza status Ã‚Â· redirects do Fechar mantÃƒÂªm overlay |
| **Validar local** | Abrir caixa Ã¢â€ â€™ fechar Ã¢â€ â€™ tentar vender Ã¢â€ â€™ deve barrar Ã‚Â· abrir de novo Ã¢â€ â€™ vende |

### Ã°Å¸Â©Â¹ Caixa no PDV Ã¢â‚¬â€ nÃƒÂ£o abrir BI no overlay (18/07 Ã‚Â· **teste v10.10**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | ApÃƒÂ³s abrir caixa, `redirect(home)` carregava o BI dentro do painel do PDV |
| **Fix** | No overlay (`agro_pdv_overlay`) Ã¢â€ â€™ volta ao **menu caixa** com os params; fora do PDV continua home |
| **Validar local** | PDV Ã¢â€ â€™ abrir caixa Vila Ã¢â€ â€™ fica no menu caixa, **nÃƒÂ£o** no BI |

### Ã°Å¸Â©Â¹ Abrir caixa Ã¢â‚¬â€ seleÃƒÂ§ÃƒÂ£o + digitar loja (18/07 Ã‚Â· **teste v10.08**)

| Item | Detalhe |
| ---- | ------- |
| **Feito** | CartÃƒÂ£o selecionado com anel + Ã‚Â«SelecionadoÃ‚Â» Ã‚Â· digitaÃƒÂ§ÃƒÂ£o errada = borda vermelha + texto |
| **Validar local** | Clicar Vila Ã¢â€ â€™ sÃƒÂ³ Vila destacada Ã‚Â· digitar `vil` Ã¢â€ â€™ aviso vermelho |

### Ã°Å¸Â©Â¹ Caixa fechar Ã¢â‚¬â€ 500 (fiado_baixas_wizard) (18/07 Ã‚Â· **teste v10.06**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Causa** | VariÃƒÂ¡vel `fiado_baixas_wizard` sumiu no pacote Vila Ã¢â€ â€™ NameError na tela Fechar |
| **Fix** | Restaurar `listar_fiado_baixas_conferencia_caixa(sessoes)` |
| **Validar** | Ctrl+F5 Ã¢â€ â€™ Fechar caixa (sessÃƒÂ£o antiga Centro) abre Ã‚Â· fechar libera seletor Loja no BI |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo Ã‚Â· WhatsApp 1Ã‚Âº no checkout **v10.78** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push 	este |
| **UX** | WhatsApp primeiro Ã‚Â· busca cadastro Ã‚Â· preenche nome/endereÃƒÂ§o Ã‚Â· lembra WA no aparelho |
| **ProduÃƒÂ§ÃƒÂ£o** | Ã¢Å“â€¦ loja **v10.67** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo Ã‚Â· OG Ã‚Â«Delivery de raÃƒÂ§ÃƒÂ£oÃ‚Â» **v10.77** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push 	este |
| **O quÃƒÂª** | TÃƒÂ­tulo/desc + faixa no cartÃƒÂ£o: *Delivery de raÃƒÂ§ÃƒÂ£o* Ã‚Â· cache card3 |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo Ã‚Â· OG card 1200x630 sem cortar logo **v10.74** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push 	este |
| **O quÃƒÂª** | /catalogo/og-image/ gera cartÃƒÂ£o 1200Ãƒâ€”630 Ã‚Â· logo contain Ã‚Â· ?v=card2-Ã¢â‚¬Â¦ quebra cache |
| **fb:app_id** | Aviso Facebook cosmÃƒÂ©tico Ã¢â‚¬â€ **nÃƒÂ£o** impede preview Zap |
| **ProduÃƒÂ§ÃƒÂ£o** | Ã¢Å“â€¦ loja **v10.66** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo Ã‚Â· preview WhatsApp (og:image) **v10.71** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push 	este |
| **O quÃƒÂª** | Meta Open Graph + /catalogo/og-image/ (logo da gestÃƒÂ£o) |
| **VocÃƒÂª** | Colar link no Zap Ã‚Â· se cache antigo: Facebook Sharing Debugger Ã‚Â«Raspar novamenteÃ‚Â» |
| **ProduÃƒÂ§ÃƒÂ£o** | Ã¢Å“â€¦ loja **v10.65** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo Ã‚Â· esconder Plus Code no checkout **v10.63** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push 	este Ã‚Â· Ã¢Å“â€¦ loja **v10.62** |
| **UX** | Cliente vÃƒÂª sÃƒÂ³ Ã¢Å“â€œ localizaÃƒÂ§ÃƒÂ£o OK Ã¢â‚¬â€ Plus Code fica oculto (grava no pedido) |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery Ã‚Â· GPS checkout **v10.61** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Origem** | FOOD `food.md` Ã‚Â§8.1 Ã¢â‚¬â€ GPS Ã¢â€ â€™ Plus Code no fechamento |
| **O quÃƒÂª** | BotÃƒÂ£o Ã‚Â«Usar minha localizaÃƒÂ§ÃƒÂ£oÃ‚Â» Ã‚Â· some endereÃƒÂ§o manual Ã‚Â· Ã‚Â«Digitar manualmenteÃ‚Â» Ã‚Â· API `/catalogo/api/localizacao/` Ã‚Â· grava plus/maps no pedido/cliente |
| **ProduÃƒÂ§ÃƒÂ£o** | Ã¢Å“â€¦ loja **v10.62** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.53** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **UX** | BotÃƒÂ£o **Ã‚Â«Salvar fotoÃ‚Â»** (antes Ã‚Â«Foto cardÃ‚Â») + texto: nÃƒÂ£o usar Ã‚Â«Salvar lojaÃ‚Â» |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.51** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.53)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Foto cat.** | Upload **AJAX** (comprime no celular) Ã‚Â· alerta OK/erro Ã‚Â· isento idempotÃƒÂªncia |
| **UI** | Ã‚Â«DesiÃ¢â‚¬Â¦.pngÃ‚Â» = nome abreviado do arquivo (normal); nÃƒÂ£o ÃƒÂ© erro |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.50** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.51)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Foto cat.** | Comprime no servidor (PillowÃ¢â€ â€™JPG) Ã‚Â· atÃƒÂ© ~4 MB bruto Ã‚Â· erro em **alerta** |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.49** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.50)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Foto cat.** | Limite **~1,2 MB** Ã‚Â· erro visÃƒÂ­vel no topo + alerta no navegador |
| **Causa** | PNG 860 KB > 700 KB antigo Ã‚Â· rejeitava sem aviso claro |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.48** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.49)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Hero** | DegradÃƒÂª mais branco na faixa Delivery Ã‚Â· cards endereÃƒÂ§o **#fff** |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.47** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.48)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Hero** | Textos **escuros** no degradÃƒÂª claro Ã‚Â· cards brancos Ã‚Â· CTA verde |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.46** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.47)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Hero** | Base do degradÃƒÂª um pouco mais **verde claro** (topo igual) |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.45** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.46)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Hero** | DegradÃƒÂª tipo ref.: **topo quase branco** (Ã‚Â½) Ã¢â€ â€™ teal suave |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.44** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.45)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Hero** | **Um** `linear-gradient` limpo (cremeÃ¢â€ â€™verde) Ã‚Â· sem vÃƒÂ©u/faixa |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.43** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.44)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Hero** | Fade em **vÃƒÂ©u** + faixa `hero-fade` Ã‚Â· transiÃƒÂ§ÃƒÂ£o bem mais suave |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.42** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.43)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Hero** | DegradÃƒÂª **longo/suave** (sem faixa dura) Ã‚Â· 100% mais abaixo |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.41** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.42)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Hero** | DegradÃƒÂª desce mais antes do verde 100% |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.40** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.41)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Hero** | Fundo logo: degradÃƒÂª verde/laranja **fraco** Ã¢â€ â€™ contraste sobe Ã¢â€ â€™ **100%** em Delivery |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.39** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.40)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Logo** | Corta sobra branca cima/baixo (`cover` + aspect mais baixo) |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.38** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.39)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Hero** | Logo **largura total** Ã‚Â· faixa branca fina Ã‚Â· **GestÃƒÂ£o** ao lado das boas-vindas |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.37** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.38)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Logo** | Largura **100%** do hero (sem caixa vazia) Ã‚Â· altura auto |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.36** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.37)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Hero** | Logo **maior** Ã‚Â· sem nome texto (jÃƒÂ¡ na arte) Ã‚Â· Ã‚Â«Delivery Ã‚Â· raÃƒÂ§ÃƒÂµesÃ‚Â» + boas-vindas **embaixo** |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.35** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.36)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Logo** | Formato **paisagem** na vitrine Ã‚Â· limite upload **~1,2 MB** |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.34** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.35)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **NÃƒÂ­veis** | Categoria Ã¢â€ â€™ sub Ã¢â€ â€™ **sub-sub** Ã¢â€ â€™ produtos (mÃƒÂ¡x. 3) |
| **Cadastro** | Aba Delivery: 3 selects + **+** em cada Ã‚Â· gestÃƒÂ£o cria sob raiz ou sob sub |
| **Vitrine** | Passos em cascata; Voltar sobe um nÃƒÂ­vel |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.33** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.35)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **UX** | CTA Ã‚Â«Como chegarÃ‚Â» nos endereÃƒÂ§os |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.29** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.30)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **EndereÃƒÂ§o** | Card clicÃƒÂ¡vel Ã¢â€ â€™ Google Maps rota |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.27** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.29)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **UX** | Logo maior Ã‚Â· WhatsApp balÃƒÂ£o flutuante |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.24** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.27)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` Ã‚Â· migrate `0060` logo loja |
| **Logo** | Upload em `/catalogo/gestao/` Ã‚Â· aparece antes do nome |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.22** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.24)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **Fluxo** | Home categoria Ã¢â€ â€™ **subcategoria** (se existir) Ã¢â€ â€™ produtos filtrados Ã‚Â· Voltar em cascata |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.21** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.22)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` Ã‚Â· migrate `0059` foto categoria |
| **Home `/catalogo/`** | Cards grandes de categoria com foto Ã¢â€ â€™ clique abre produtos Ã‚Â· Voltar |
| **GestÃƒÂ£o** | Em cada categoria raiz: upload **Foto card** (mÃƒÂ¡x. ~700 KB) |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.16** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.21)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` Ã‚Â· migrate `0058` |
| **UX aba 10** | Grade compacta + **botÃƒÂ£o +** cria categoria/sub no modal |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.15** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.16)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` |
| **UX aba 10** | Grade compacta no modal: cat+sub+estoque Ã‚Â· tÃƒÂ­tulo+peso+ordem Ã‚Â· desc+destaque+foto |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.12** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.13)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` Ã‚Â· migrate `0058` no deploy |
| **O quÃƒÂª** | `/catalogo/` pÃƒÂºblico Ã‚Â· aba **10. Delivery** Ã‚Â· pedidos Ã¢â€ â€™ `/entregas/` (`origem=catalogo`) |
| **GestÃƒÂ£o** | `/catalogo/gestao/` Ã¢â‚¬â€ publicar, WhatsApp, **2 endereÃƒÂ§os** (rÃƒÂ³tulo+rua), **CRUD categorias/sub** |
| **Categorias** | PadrÃƒÂ£o apps raÃƒÂ§ÃƒÂ£o: CÃƒÂ£es/Gatos/Ã¢â‚¬Â¦ + sub (Adulto/Filhote) Ã‚Â· seed na migrate Ã‚Â· select na aba Delivery |
| **Vitrine** | Mobile-first Ã‚Â· chips categoria Ã‚Â· 2 endereÃƒÂ§os no hero Ã‚Â· botÃƒÂ£o WhatsApp **fixo** (nÃƒÂ£o FAB) |
| **Migrate** | `0057` config Ã‚Â· `0058` categorias + endereÃƒÂ§os 1/2 |
| **Renan testar** | GestÃƒÂ£o: 2 endereÃƒÂ§os + criar cat/sub Ã¢â€ â€™ produto aba Delivery (cat+sub) Ã¢â€ â€™ vitrine celular chips + Zap |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o** Ã¢â‚¬â€ sÃƒÂ³ teste atÃƒÂ© Renan pedir + senha |

### WIP Ã¢â‚¬â€ CatÃƒÂ¡logo delivery GM Agro **v10.05** (18/07/2026) *(histÃƒÂ³rico Ã¢â‚¬â€ ver v10.12)*

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | CatÃƒÂ¡logo pÃƒÂºblico `/catalogo/` Ã‚Â· aba **10. Delivery** Ã‚Â· pedidos Ã¢â€ â€™ entregas |
| **GestÃƒÂ£o loja** | `/catalogo/gestao/` Ã¢â‚¬â€ publicar, WhatsApp, cores (sem cat/endereÃƒÂ§os 2 ainda) |


### Ã°Å¸â€â€™ Vila Elias Ã¢â‚¬â€ antiburro digitar loja (18/07 Ã‚Â· **teste v10.04**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` Ã‚Â· validar no Render |
| **Feito** | Abrir Gaveta/Vila: digitar **centro** ou **vila** Ã‚Â· trocar Loja no BI/atalhos: mesmo digitar Ã‚Â· com caixa aberto seletor **travado** |
| **Arquivos** | `pdv_deposito_util.py` Ã‚Â· `views.py` Ã‚Â· `caixa_abrir.html` Ã‚Â· `agro_loja_confirm.html` Ã‚Â· `dashboard_gerencial.html` Ã‚Â· `home.html` |
| **Validar** | Abrir caixa Vila digitando `vila` Ã‚Â· tentar trocar BI sem digitar Ã¢â€ â€™ bloqueia Ã‚Â· com caixa aberto seletor desabilitado |

### Ã°Å¸â€™Â° Vila Elias Ã¢â‚¬â€ Caixa separado Centro Ãƒâ€” Vila (18/07 Ã‚Â· **teste v10.03**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` Ã‚Â· validar no Render Ã‚Â· Ã°Å¸â€œÂ¦ produÃƒÂ§ÃƒÂ£o sÃƒÂ³ com frase + senha |
| **Feito** | `ponto_caixa=vila` (turno prÃƒÂ³prio) Ã‚Â· abrir 4 cartÃƒÂµes (Gaveta Centro / Vila / Notebook / Teste) Ã‚Â· painel Ã‚Â«todosÃ‚Â» e fechamento em lote **sÃƒÂ³ da loja do aparelho** Ã‚Â· venda herda depÃƒÂ³sito do ponto do caixa Ã‚Â· MP Point **nÃƒÂ£o** na Vila Ã‚Â· migration **0056** |
| **Arquivos** | `caixa_util.py` Ã‚Â· `views.py` Ã‚Â· `models.py` Ã‚Â· `0056_*` Ã‚Â· `caixa_abrir.html` Ã‚Â· `caixa_fechar.html` Ã‚Â· `caixa_painel.html` |
| **Validar teste** | BI Ã¢â€ â€™ Loja **Vila** Ã¢â€ â€™ Abrir **Caixa Vila** Ã¢â€ â€™ vender Ã¢â€ â€™ painel soma sÃƒÂ³ Vila Ã‚Â· Centro aberto no outro PC nÃƒÂ£o mistura Ã‚Â· fechar lote = sÃƒÂ³ Vila |
| **Ops** | PC Vila: seletor Loja Vila + abrir Caixa Vila. Centro continua Gaveta. Notebook satÃƒÂ©lite da loja do aparelho. |
| **Autorizar loja** | *Ã‚Â«pode subir Vila Elias caixa separadoÃ‚Â»* + **99738595** |

### Ã°Å¸ÂÂª Vila Elias Ã¢â‚¬â€ PDV baixa estoque por loja do aparelho (18/07 Ã‚Â· **teste v10.02**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` Ã‚Â· validar no Render Ã‚Â· Ã°Å¸â€œÂ¦ produÃƒÂ§ÃƒÂ£o sÃƒÂ³ com frase + senha |
| **Problema** | Seletor Ã‚Â«Vila EliasÃ‚Â» na home nÃƒÂ£o baixava estoque Vila Ã¢â‚¬â€ venda sempre usava env `PDV_VENDA_ESTOQUE_DEPOSITO` (Centro) |
| **Feito** | SessÃƒÂ£o/cookie `pdv_deposito` Ã‚Â· API `/api/pdv/deposito/` Ã‚Â· seletor no **BI** + atalhos Ã‚Â· badge no PDV Ã‚Â· `VendaAgro.deposito` + migration **0055** Ã‚Â· baixa/estorno usam depÃƒÂ³sito da venda |
| **Arquivos** | `pdv_deposito_util.py` Ã‚Â· `views.py` Ã‚Â· `pdv/views.py` Ã‚Â· `models.py` Ã‚Â· `0055_*` Ã‚Â· `dashboard_gerencial.html` Ã‚Â· `home.html` Ã‚Â· topbar/wizard JS |
| **Validar teste** | BI Ã¢â€ â€™ Loja **Vila Elias** Ã¢â€ â€™ badge Ã‚Â«Estoque: Vila EliasÃ‚Â» Ã¢â€ â€™ PDV venda R$ 0,01 Ã¢â€ â€™ saldo **Vila** cai Ã‚Â· devoluÃƒÂ§ÃƒÂ£o repÃƒÂµe Vila |
| **Ops abertura** | 1) Injetar estoque Vila (NF depÃƒÂ³sito Vila / transferÃƒÂªncia / PIN) 2) Conferir sync 3) PC Vila = Loja Vila no BI 4) Caixa sÃƒÂ³ naquele PC 5) **Sem NFC-e Vila** atÃƒÂ© cert CNPJ `0323Ã¢â‚¬Â¦` (PIX/cartÃƒÂ£o ainda = CNPJ Centro) |
| **Autorizar loja** | *Ã‚Â«pode subir Vila Elias PDV depÃƒÂ³sitoÃ‚Â»* + **99738595** |
| **Backup no envio** | `producao-backup-pre-v1002-vila-pdv-YYYYMMDD` @ HEAD loja |

### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ histÃƒÂ³rico C1Ã¢â‚¬â€œC3 nÃƒÂ£o duplica a prÃƒÂ³pria nota (18/07 Ã‚Â· **teste v9.97**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ push `teste` Ã‚Â· validar no Render (Ctrl+F5 etapa 8) |
| **Sintoma** | C3 e NF com mesmo preÃƒÂ§o e datas diferentes (entrada 14/07 vs emissÃƒÂ£o 30/06) Ã¢â‚¬â€ parecia 2 notas |
| **Causa** | ApÃƒÂ³s estoque, a NF aberta entrava em Ã‚Â«ÃƒÂºltimas comprasÃ‚Â» (data entrada) e de novo na coluna NF (emissÃƒÂ£o) |
| **Fix** | PrÃƒÂ©via exclui o rascunho atual do histÃƒÂ³rico; filtro extra por nÃ‚Âº+custo; rÃƒÂ³tulo Ã‚Â«Compras anteriores + NFÃ‚Â» |
| **Arquivos** | `views.py` Ã‚Â· `compras_ultimas_compras_util.py` Ã‚Â· `entrada_nota.html` |
| **Validar** | Abrir NF 320 (ou qualquer) Ã¢â€ â€™ etapa 8 Ã¢â€ â€™ C1Ã¢â‚¬â€œC3 sem a nota atual Ã‚Â· NF sÃƒÂ³ ÃƒÂ  direita |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ Compras etapa 1 UI (18/07 Ã‚Â· **teste v9.95+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **enviado loja v10.60** (`c88ce66`) Ã‚Â· 18/07 |
| **Escopo** | SÃƒÂ³ visual/organizaÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ **sem** mudanÃƒÂ§a de regra, API, Mongo, cÃƒÂ¡lculo de sugestÃƒÂ£o |
| **Feito** | Painel resumo (horizonte + descontar estoque + KPIs) Ã‚Â· busca em destaque Ã‚Â· filtros limpos Ã‚Â· lista + detalhe expandido em blocos |
| **Arquivo** | `produtos/templates/produtos/compras.html` |
| **Risco** | Baixo Ã¢â‚¬â€ sÃƒÂ³ layout; comportamento igual ao de antes |
| **Validar na loja** | Ctrl+F5 `/compras/` Ã‚Â· buscar Ã‚Â· expandir linha Ã‚Â· horizonte/checkbox Ã‚Â· Folha/F10/lista |
| **Autorizar** | *Ã‚Â«pode subir Compras UI etapa 1Ã‚Â»* + **99738595** |
| **PrÃƒÂ³ximo** | Etapas seguintes de evoluÃƒÂ§ÃƒÂ£o Compras (quando Renan pedir) |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ aba 9 histÃƒÂ³rico sÃƒÂ³ o que mudou (18/07 Ã‚Â· **teste v10.01**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **enviado loja v10.61** (`e38df10`) Ã‚Â· 18/07 |
| **Sintoma** | LÃƒÂ¡pis PDV (1 centavo) gerava vÃƒÂ¡rias linhas com **DE = Ã¢â‚¬â€** (nome, GM, barrasÃ¢â‚¬Â¦) Ã‚Â· preÃƒÂ§o sem valor anterior |
| **Causa** | Overlay vazio no 1Ã‚Âº save Ã‚Â· Ã‚Â«antesÃ‚Â» vazio Ã‚Â· `permite_venda` forÃƒÂ§ado True em todo save |
| **Fix** | Ã‚Â«AntesÃ‚Â» completa com catÃƒÂ¡logo Ã‚Â· nÃƒÂ£o forÃƒÂ§a permite_venda no lÃƒÂ¡pis |
| **Arquivos** | `cadastro_alteracao_historico_util.py` Ã‚Â· `views.py` |
| **Risco** | Baixo Ã¢â‚¬â€ sÃƒÂ³ histÃƒÂ³rico de cadastro; nÃƒÂ£o mexe estoque/venda |
| **Backup no envio** | `producao-backup-pre-v1001-aba9-YYYYMMDD` @ HEAD loja |
| **ApÃƒÂ³s enviar** | **ObrigatÃƒÂ³rio testar na loja** (Ctrl+F5 Ã‚Â· lÃƒÂ¡pis muda 1 centavo Ã‚Â· aba 9 = **1 linha** Ã‚Â· DE = preÃƒÂ§o antigo) |
| **Autorizar** | *Ã‚Â«pode subir aba 9 histÃƒÂ³ricoÃ‚Â»* + **99738595** |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ kardex e-mail + Entrada NF ÃŽâ€camada (18/07 Ã‚Â· **teste v9.92**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **jÃƒÂ¡ na loja desde v10.63** (`1e6fe3c`) Ã¢â‚¬â€ nÃƒÂ£o incluir na fila 11.94Ã¢â‚¬â€œ11.98 |
| **Sintoma** | Quem ainda com e-mail (`agromaisgm@Ã¢â‚¬Â¦`) Ã‚Â· Entrada NF (ex. 399636) ainda como **saÃƒÂ­da ~172** |
| **Causa** | Label da NF era e-mail do login Ã‚Â· qty usava ÃŽâ€ `saldo_informado` bruto (mistura salto ERP) |
| **Fix** | Resolve e-mailÃ¢â€ â€™nome Ã‚Â· movimento = ÃŽâ€(camada Agro = informadoÃ¢Ë†â€™ERP ref) Ã‚Â· saldo recomposto |
| **Arquivo** | `estoque_movimentos_cadastro_util.py` |
| **Risco** | Baixo Ã¢â‚¬â€ sÃƒÂ³ leitura/exibiÃƒÂ§ÃƒÂ£o do histÃƒÂ³rico |
| **Backup no envio** | `producao-backup-pre-v992-kardex-YYYYMMDD` @ HEAD loja |
| **ApÃƒÂ³s enviar** | **ObrigatÃƒÂ³rio testar na loja** (Ctrl+F5 Ã‚Â· badge Ã‚Â· milho Ã‚Â· Estoque Ã‚Â· Quem sem @ Ã‚Â· NF 399636 = entrada) |
| **Autorizar** | *Ã‚Â«pode subir kardex e-mail / Entrada NFÃ‚Â»* + **99738595** |

### Ã°Å¸â€œÂ¦ Deploy loja **v9.91** Ã¢â‚¬â€ kardex Quem + Entrada NF (18/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **Live** Ã‚Â· Render loja **v9.91** Ã‚Â· commit 372bb70 |
| **Inclui** | SÃƒÂ³ fix kardex: **Quem** = operador real Ã‚Â· **Entrada NF** sem saÃƒÂ­da fantasma Ã‚Â· fornecedor por nÃ‚Âº NF |
| **Arquivos** | `estoque_movimentos_cadastro_util.py` Ã‚Â· `views.py` (+9 linhas) Ã‚Â· **nÃƒÂ£o** merge inteiro |
| **HEAD loja** | *(este commit)* Ã‚Â· base **8418ba8** (v9.90) |
| **Backup** | `producao-backup-pre-v991-kardex-20260718` @ **8418ba8** Ã¢â‚¬â€ reverter: `git push origin producao-backup-pre-v991-kardex-20260718:producao` |
| **Risco** | Baixo Ã¢â‚¬â€ sÃƒÂ³ leitura/exibiÃƒÂ§ÃƒÂ£o do histÃƒÂ³rico; nÃƒÂ£o altera estoque nem venda |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir Ã¢â‚¬Â¦ correÃƒÂ§ÃƒÂ£o* + **99738595** |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.91** Ã‚Â· milho Ã‚Â· aba Estoque Ã‚Â· Quem + NF 398454 |

### Ã°Å¸Â©Â¹ Cadastro Ã¢â‚¬â€ kardex Quem + Entrada NF (17/07 Ã‚Â· **teste v9.91** Ã‚Â· **loja v9.91**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | 1) Coluna **Quem** quase sempre Ã‚Â«Geraldo HinnenÃ‚Â» Ã‚Â· 2) **Entrada NF** aparecia como **saÃƒÂ­da** (~170) e fornecedor **RBS** errado (NF 398454 era outro fornecedor / +20 milho) |
| **Causa** | Quem: priorizava usuÃƒÂ¡rio Django da sessÃƒÂ£o Chrome. Qtd: misturava saldo de IDs variantes no mesmo depÃƒÂ³sito. Fornecedor: casava sÃƒÂ³ por data + fallback 1Ã‚Âª compra |
| **Fix** | Operador do PIN/venda; delta por produto+depÃƒÂ³sito; casa NF por nÃƒÂºmero; sem fallback cego; rÃƒÂ³tulo Ã‚Â«NFÃ‚Â» sem duplicar |
| **Arquivos** | estoque_movimentos_cadastro_util.py Ã‚Â· 
iews.py (compras enrich) |
| **VocÃƒÂª** | Ctrl+F5 teste Ã‚Â· milho grande Ã‚Â· aba Estoque Ã‚Â· confere Quem nas vendas Ã‚Â· Entrada NF 398454 = **entrada** (nÃƒÂ£o saÃƒÂ­da) e fornecedor certo |

### Ã°Å¸â€œÂ¦ Deploy loja **v9.90** Ã¢â‚¬â€ Fecha PDV+Cadastro+NFC-e+RelatÃƒÂ³rios (17/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **Live** Ã‚Â· Render loja **v9.90** Ã‚Â· commit e40e898 |
| **Inclui** | **PDV Enviar WhatsApp** Ã‚Â· **PDV lÃƒÂ¡pis/editor** Ã‚Â· **Cadastro modal** (kardex + aba 9 + origem PDV) Ã‚Â· **FL-056** NFC-e 963/225 Ã‚Â· **RelatÃƒÂ³rios ajuda Ã‚Â«?Ã‚Â»** Ã‚Â· layout PDV etapa 1 (atÃƒÂ© v9.90) |
| **HEAD loja** | *(este commit)* Ã‚Â· base **4113492** (v9.16) |
| **Backup** | `producao-backup-pre-v990-fecha-pdv-cadastro-nfce-20260717` @ **4113492** Ã¢â‚¬â€ reverter: `git push origin producao-backup-pre-v990-fecha-pdv-cadastro-nfce-20260717:producao` |
| **MÃƒÂ©todo** | worktree Ã‚Â· checkout arquivos do `teste` Ã‚Â· **nÃƒÂ£o** merge inteiro |
| **Migration** | **0054** `ProdutoCadastroAlteracaoAgro` (Render migrate no deploy) |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *envie para produÃƒÂ§ÃƒÂ£o* + **99738595** (checklist Fecha) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.90** Ã‚Â· Enviar Zap Ã‚Â· lÃƒÂ¡pis Ã¢â€ â€™ aba 9 Ã‚Â· modal cadastro Ã‚Â· RelatÃƒÂ³rios **?** Ã‚Â· reemitir NFC-e **#2812**/**#3347** |

### Ã°Å¸â€œÂ¦ PDV Ã¢â‚¬â€ Enviar orÃƒÂ§amento WhatsApp Ã‚Â· pronto produÃƒÂ§ÃƒÂ£o (17/07 Ã‚Â· **teste v9.90**)


| | |
| --- | --- |
| **Status** | Ã¢Å“â€¦ **enviado loja v9.90** |
| **O quÃƒÂª** | BotÃƒÂ£o **Enviar** (Zap) acima do Subtotal Ã‚Â· texto Agromais Ã‚Â· grava em OrÃƒÂ§amentos Ã‚Â· ÃƒÂ­cone Zap na lista Ã‚Â· avisos toast central |
| **Commits** | c3ce765Ã¢â‚¬Â¦1b09f96 (pacote Zap PDV) Ã‚Â· HEAD 1b09f96 |
| **OK Renan** | 17/07 Ã¢â‚¬â€ fluxo + lista + ÃƒÂ­cone |
| **Loja** | Ã¢Å“â€¦ **v9.90** (pacote Fecha 17/07) |

### Ã¢Å“Â¨ PDV Ã¢â‚¬â€ ÃƒÂ­cone Zap nos orÃƒÂ§amentos enviados (17/07 Ã‚Â· **teste v9.90**)

| | |
| --- | --- |
| **O quÃƒÂª** | OrÃƒÂ§amentos salvos pelo **Enviar** WhatsApp mostram ÃƒÂ­cone verde do Zap na lista (e no Ver mais) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.90** Ã‚Â· Enviar de novo Ã¢â€ â€™ linha nova com ÃƒÂ­cone Zap |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ Enviar Zap tambÃƒÂ©m grava em OrÃƒÂ§amentos (17/07 Ã‚Â· **teste v9.89**)

| | |
| --- | --- |
| **O quÃƒÂª** | **Enviar** = mesma gravaÃƒÂ§ÃƒÂ£o do **Salvar orÃƒÂ§amento** (lista + servidor) Ã‚Â· depois abre o Zap |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.89** Ã‚Â· Enviar Ã¢â€ â€™ confere se aparece no card OrÃƒÂ§amentos |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ texto do aviso central maior (17/07 Ã‚Â· **teste v9.88**)

| | |
| --- | --- |
| **O quÃƒÂª** | TÃƒÂ­tulo/corpo/botÃƒÂ£o do toast prominent bem maiores (legÃƒÂ­vel na loja) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.88** Ã‚Â· Enviar sem telefone Ã¢â€ â€™ lÃƒÂª o texto grande |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ avisos Enviar Zap no meio da tela (17/07 Ã‚Â· **teste v9.87**)

| | |
| --- | --- |
| **O quÃƒÂª** | Avisos do Enviar WhatsApp no centro (toast prominent, grande) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.87** Ã‚Â· Enviar sem telefone Ã¢â€ â€™ aviso grande no meio |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ avisos Enviar Zap sem alert Chrome (17/07 Ã‚Â· **teste v9.86**)

| | |
| --- | --- |
| **O quÃƒÂª** | Carrinho vazio / sem cliente / sem telefone Ã¢â€ â€™ toast PDV (showPdvAviso), nÃƒÂ£o janela do navegador |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.86** Ã‚Â· clica Enviar nos 3 casos e confere o aviso SisVale |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ texto Zap orÃƒÂ§amento padrÃƒÂ£o Agromais (17/07 Ã‚Â· **teste v9.85**)

| | |
| --- | --- |
| **O quÃƒÂª** | Mensagem WhatsApp no formato do 2Ã‚Âº print: *ORÃƒâ€¡AMENTO AGROMAIS*, item em 1 linha 1x - nome  R$, linhas finas, sem losango/Ã°Å¸â€™Â° |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.85** Ã‚Â· Enviar de novo e conferir o Zap |

### Ã¢Å“Â¨ PDV Ã¢â‚¬â€ Enviar orÃƒÂ§amento WhatsApp (acima do Subtotal) (17/07 Ã‚Â· **teste v9.84**)

| | |
| --- | --- |
| **O quÃƒÂª** | BotÃƒÂ£o verde **Enviar** (ÃƒÂ­cone Zap) acima do Subtotal Ã‚Â· texto no padrÃƒÂ£o Consulta Ã‚Â· sem cliente abre busca F4 Ã‚Â· salva orÃƒÂ§amento antes Ã‚Â· abre Zap do cliente |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.84** Ã‚Â· testa: com cliente+itens; sem cliente; carrinho vazio |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ some azul de vez (painel+campo) (17/07 Ã‚Â· **teste v9.83**)

| | |
| --- | --- |
| **O quÃƒÂª** | Contorno do painel e borda do campo de busca saem do azul (cinza) Ã‚Â· sem linha entre busca e carrinho |
| **VocÃƒÂª** | Ctrl+F5 forte Ã‚Â· badge tem que ser **v9.83** (se for menor, ainda tÃƒÂ¡ cache) |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ some de vez a linha/faixa azul da busca (17/07 Ã‚Â· **teste v9.82**)

| | |
| --- | --- |
| **O quÃƒÂª** | Tira borda **e** fundo azul da faixa da busca (era isso que ainda aparecia) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.82** |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ tira linha azul sob a busca (17/07 Ã‚Â· **teste v9.81**)

| | |
| --- | --- |
| **O quÃƒÂª** | Remove a faixa/borda azul embaixo do campo de busca |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.81** |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ ÃƒÂ­cone Cliente fora do campo + texto Carrinho (17/07 Ã‚Â· **teste v9.80**)

| | |
| --- | --- |
| **O quÃƒÂª** | ÃƒÂcone do cliente **fora** do campo (igual Busca) Ã‚Â· volta escrita **Carrinho** |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.80** |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ sÃƒÂ³ ÃƒÂ­cones sem texto (teste visual) (17/07 Ã‚Â· **teste v9.79**)

| | |
| --- | --- |
| **O quÃƒÂª** | Tira as escritas Cliente/Busca/Carrinho Ã¢â‚¬â€ fica **sÃƒÂ³ o ÃƒÂ­cone** (pra Renan ver como fica) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.79** Ã‚Â· diz se mantÃƒÂ©m ou volta o texto |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ Cliente/Busca/Carrinho mesmo padrÃƒÂ£o (17/07 Ã‚Â· **teste v9.78**)

| | |
| --- | --- |
| **O quÃƒÂª** | TrÃƒÂªs linhas iguais: **ÃƒÂ­cone + tÃƒÂ­tulo** no tamanho da Busca. Cliente ganhou sÃƒÂ­mbolo (pessoa). Carrinho/ÃƒÂ­cones alinhados |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.78** |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ busca +1 ponto (17/07 Ã‚Â· **teste v9.77**)

| | |
| --- | --- |
| **O quÃƒÂª** | Busca/fonte digitada + altura mais um pouquinho |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.77** |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ busca fonte maior de novo (17/07 Ã‚Â· **teste v9.76**)

| | |
| --- | --- |
| **O quÃƒÂª** | Linha da busca + **Busca** + texto digitado bem maiores (mesmo tamanho, peso 900) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.76** |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ fonte digitada = Busca (17/07 Ã‚Â· **teste v9.75**)

| | |
| --- | --- |
| **O quÃƒÂª** | Texto digitado na busca no **mesmo tamanho** da fonte **Busca** |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.75** |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ busca um pouco maior (17/07 Ã‚Â· **teste v9.74**)

| | |
| --- | --- |
| **O quÃƒÂª** | Linha da busca um pouco mais alta Ã‚Â· fonte **Busca** + texto digitado maiores (proporcional) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.74** |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ busca na linha do ÃƒÂ­cone Busca (17/07 Ã‚Â· **teste v9.73**)

| | |
| --- | --- |
| **O quÃƒÂª** | Campo de busca sobe **na mesma linha** do ÃƒÂ­cone/tÃƒÂ­tulo **Busca** (vÃƒÂ£o vermelho do print) Ã‚Â· some a faixa de baixo Ã‚Â· **1 itens** ÃƒÂ  direita |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.73** Ã‚Â· confere se bate com o print |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ reverte busca na linha do Cliente (17/07 Ã‚Â· **teste v9.72**)

| | |
| --- | --- |
| **O quÃƒÂª** | Reverteu de novo Ã¢â‚¬â€ volta layout **v9.67** (busca embaixo Ã‚Â· Saldos no topo direito). PrÃƒÂ³ximo: confirmar no print antes de mexer |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.72** |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ reverte busca no header (17/07 Ã‚Â· **teste v9.71**)

| | |
| --- | --- |
| **O quÃƒÂª** | Reverteu busca no topo Ã¢â‚¬â€ volta layout **v9.67** (busca na coluna esquerda Ã‚Â· Saldos no topo direito) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.71** |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ Saldos sobe Ã‚Â· botÃƒÂµes cliente ÃƒÂ  esquerda (17/07 Ã‚Â· **teste v9.67**)

| | |
| --- | --- |
| **O quÃƒÂª** | Cliente + **Editar/Trocar/Hist** ficam na coluna da **busca** (esquerda). **Saldos** sobe no topo da coluna direita. OrÃƒÂ§amentos no mesmo lugar de sempre (abaixo do Saldos) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.67** Ã‚Â· confere |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ tira Etapa 1/Produtos Ã‚Â· ? sobe pro header (17/07 Ã‚Â· **teste v9.66**)

| | |
| --- | --- |
| **O quÃƒÂª** | SÃƒÂ³ isto: remove texto **Etapa 1 / Produtos** Ã‚Â· **?** vai para a barra de etapas (ao lado de 1Ã‚Â·2Ã‚Â·3) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.66** Ã‚Â· confere |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ PDV lÃƒÂ¡pis Ã¢â€ â€™ aba 9 AlteraÃƒÂ§ÃƒÂµes (17/07 Ã‚Â· Ã¢Å“â€¦ enviado v9.90)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **testado no teste** Ã‚Â· Ã°Å¸â€œÂ¦ **pronto pra envio** Ã¢â‚¬â€ espera frase + senha `99738595` na **mesma mensagem** |
| **O quÃƒÂª** | LÃƒÂ¡pis do PDV grava histÃƒÂ³rico na aba **9. AlteraÃƒÂ§ÃƒÂµes** com origem **Ã‚Â«PDV ediÃƒÂ§ÃƒÂ£o rÃƒÂ¡pidaÃ‚Â»** (mesmo overlay do cadastro). Saldo no lÃƒÂ¡pis continua sÃƒÂ³ na aba **4** |
| **VersÃƒÂ£o alvo loja** | **v9.62** |
| **Base loja hoje** | **v9.16** |
| **Arquivos** | `pdv_wizard.js` Ã‚Â· `cadastro_alteracao_historico_util.py` Ã‚Â· `models.py` (Origem.PDV) Ã‚Â· `_modal_editar_produto_cadastro_erp.inc.html` (`origem_historico=modal`) |
| **Sobe junto** | Ideal com pacote **Cadastro modal** (migration `0054` + aba 9) Ã¢â‚¬â€ se a aba 9 ainda nÃƒÂ£o estiver na loja, subir os dois |
| **Risco** | Baixo Ã¢â‚¬â€ sÃƒÂ³ marca origem + mesmo hook de histÃƒÂ³rico jÃƒÂ¡ existente |
| **MÃƒÂ©todo** | worktree `origin/producao` Ã‚Â· checkout desses arquivos de `teste` Ã‚Â· **nÃƒÂ£o** merge inteiro |
| **Autorizar com** | *Ã‚Â«pode subir histÃƒÂ³rico PDV lÃƒÂ¡pis / aba 9Ã‚Â»* + **99738595** Ã‚Â· ou junto: *Ã‚Â«pode subir cadastro modal / histÃƒÂ³ricoÃ‚Â»* + senha |
| **VocÃƒÂª apÃƒÂ³s** | Ctrl+F5 loja Ã‚Â· lÃƒÂ¡pis muda nome Ã¢â€ â€™ cadastro Ã¢â€ â€™ aba 9 Ã‚Â· origem Ã‚Â«PDV ediÃƒÂ§ÃƒÂ£o rÃƒÂ¡pidaÃ‚Â» |

### Ã°Å¸Â©Â¹ PDV + Cadastro Ã¢â‚¬â€ histÃƒÂ³rico aba 9 tambÃƒÂ©m no lÃƒÂ¡pis (17/07 Ã‚Â· **teste v9.62**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | EdiÃƒÂ§ÃƒÂ£o rÃƒÂ¡pida do PDV (lÃƒÂ¡pis) jÃƒÂ¡ salvava no mesmo overlay Ã¢â‚¬â€ agora marca origem **Ã‚Â«PDV ediÃƒÂ§ÃƒÂ£o rÃƒÂ¡pidaÃ‚Â»** na aba **9. AlteraÃƒÂ§ÃƒÂµes**. Cadastro modal marca **Ã‚Â«Modal cadastroÃ‚Â»**. MudanÃƒÂ§a de **saldo** no lÃƒÂ¡pis continua sÃƒÂ³ na aba **4 Estoque** (nÃƒÂ£o mistura) |
| **Status** | Ã°Å¸â€œÂ¦ **pronto pra envio** Ã¢â‚¬â€ pacote acima |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.62** Ã‚Â· lÃƒÂ¡pis muda nome/preÃƒÂ§o Ã¢â€ â€™ cadastro Ã¢â€ â€™ aba 9 Ã‚Â· confere origem |

### Ã°Å¸Â©Â¹ PDV etapa 1 Ã¢â‚¬â€ layout: tira ?, botÃƒÂµes no cliente, busca sobe (17/07 Ã‚Â· **teste v9.60**)

| | |
| --- | --- |
| **O quÃƒÂª** | Tira **?** Ã‚Â· **Editar / Trocar / Hist** colados no cliente (esquerda) Ã‚Â· busca sobe (sem tÃƒÂ­tulo Ã‚Â«BuscaÃ‚Â») Ã‚Â· Saldos sobe no painel Ã‚Â· contagem de itens no Carrinho |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.60** Ã‚Â· confere barra do cliente + busca + saldos |
| **Pacote lÃƒÂ¡pis** | Continua Ã°Å¸â€œÂ¦ pronto; este layout sobe junto se pedir PDV |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ Cadastro modal editar (UX + histÃƒÂ³ricos) (17/07 Ã‚Â· Ã¢Å“â€¦ enviado v9.90)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **testado no teste** Ã‚Â· Ã°Å¸â€œÂ¦ **pronto pra envio** Ã¢â‚¬â€ espera frase + senha `99738595` na **mesma mensagem** |
| **O quÃƒÂª** | Modal editar produto quase tela cheia Ã‚Â· aba **4 Estoque** = kardex (movimentaÃƒÂ§ÃƒÂ£o, sem PIN) Ã‚Â· aba **9 AlteraÃƒÂ§ÃƒÂµes** = histÃƒÂ³rico cadastro (antesÃ¢â€ â€™depois) Ã¢â‚¬â€ **inclui PDV lÃƒÂ¡pis** (origem Ã‚Â«PDV ediÃƒÂ§ÃƒÂ£o rÃƒÂ¡pidaÃ‚Â») Ã‚Â· PreÃƒÂ§os com nÃƒÂºmeros grandes Ã‚Â· Fiscal/Gerais fonte maior e campos juntos Ã‚Â· Config no topo Ã‚Â· URL da imagem com rÃƒÂ³tulo Ã‚Â· listas de histÃƒÂ³rico crescem atÃƒÂ© o rodapÃƒÂ© |
| **VersÃƒÂ£o alvo loja** | **v9.62** (ou badge do deploy) |
| **Base loja hoje** | **v9.16** |
| **Arquivos (nÃƒÂºcleo)** | `_modal_editar_produto_cadastro_erp.inc.html` Ã‚Â· `estoque_movimentos_cadastro_util.py` Ã‚Â· `cadastro_alteracao_historico_util.py` Ã‚Â· `migrations/0054_produto_cadastro_alteracao_agro.py` Ã‚Â· URLs/API cadastro (estoque-movimentos + alteracoes-historico) Ã‚Â· hooks salvar overlay/Excel Ã‚Â· `pdv_wizard.js` (origem PDV) Ã‚Â· `models.py` (Origem.PDV) |
| **Pente fino** | IDs dos campos intactos Ã‚Â· `edit-img` com rÃƒÂ³tulo Ã‚Â· kardex Ã¢â€°Â  alteraÃƒÂ§ÃƒÂµes Ã‚Â· PreÃƒÂ§os sem altura falsa Ã‚Â· lÃƒÂ¡pis PDV Ã¢â€ â€™ aba 9 com origem correta |
| **Risco** | MÃƒÂ©dio-baixo Ã¢â‚¬â€ UX + APIs de leitura + migration `0054` (tabela histÃƒÂ³rico) Ã‚Â· deploy precisa rodar migrate |
| **MÃƒÂ©todo** | worktree `origin/producao` Ã‚Â· checkout desses arquivos de `teste` Ã‚Â· **nÃƒÂ£o** merge inteiro |
| **Autorizar com** | *Ã‚Â«pode subir cadastro modal / histÃƒÂ³ricoÃ‚Â»* + **99738595** |
| **VocÃƒÂª apÃƒÂ³s** | Ctrl+F5 loja Ã‚Â· badge Ã‚Â· PreÃƒÂ§os Ã‚Â· Fiscal Ã‚Â· Estoque Ã‚Â· AlteraÃƒÂ§ÃƒÂµes Ã‚Â· **lÃƒÂ¡pis PDV** muda nome Ã¢â€ â€™ aba 9 origem PDV |

### Ã°Å¸Â©Â¹ Cadastro Ã¢â‚¬â€ PreÃƒÂ§os: nÃƒÂºmero grande de verdade (17/07 Ã‚Â· **teste v9.56**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Custo / MVA / comissÃƒÂ£o / margem: fonte **~2,75rem** no nÃƒÂºmero. Caixa sem altura artificial. PreÃƒÂ§o final **~3rem** |
| **Status** | Ã°Å¸â€œÂ¦ fecha no pacote **Cadastro modal** acima |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.56** Ã‚Â· aba PreÃƒÂ§os |

### Ã°Å¸Â©Â¹ Cadastro Ã¢â‚¬â€ Fiscal: fonte maior + campos mais juntos (17/07 Ã‚Â· **teste v9.55**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Aba Fiscal (e Gerais): campos mais prÃƒÂ³ximos e letra maior nos inputs |
| **Status** | Ã°Å¸â€œÂ¦ fecha no pacote **Cadastro modal** acima |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ PDV editor rÃƒÂ¡pido (lÃƒÂ¡pis) (17/07 Ã‚Â· Ã¢Å“â€¦ enviado v9.90)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **testado no teste** Ã‚Â· Ã°Å¸â€œÂ¦ **pronto pra envio** Ã¢â‚¬â€ espera frase + senha `99738595` na **mesma mensagem** |
| **O quÃƒÂª** | LÃƒÂ¡pis no carrinho Ã¢â€ â€™ ediÃƒÂ§ÃƒÂ£o rÃƒÂ¡pida (nome, GM, barras, unidade com busca, preÃƒÂ§os A/B, estoque Centro/Vila) Ã‚Â· popup grande sem scroll Ã‚Â· sem travar o PDV Ã‚Â· **histÃƒÂ³rico aba 9** com origem Ã‚Â«PDV ediÃƒÂ§ÃƒÂ£o rÃƒÂ¡pidaÃ‚Â» |
| **VersÃƒÂ£o alvo loja** | **v9.62** (ou badge do deploy) |
| **Base loja hoje** | **v9.16** |
| **Arquivos** | `pdv_wizard.html` Ã‚Â· `pdv_wizard.js` Ã‚Â· `pdv_state.js` Ã‚Â· `produtos/views.py` Ã‚Â· `produtos/urls.py` Ã‚Â· `pdv/views.py` Ã‚Â· `cadastro_alteracao_historico_util.py` Ã‚Â· `models.py` (Origem.PDV) |
| **Pente fino** | Unidades lazy Ã‚Â· Esc Ã‚Â· custo no carrinho Ã‚Â· API Mongo fallback Ã‚Â· **origem histÃƒÂ³rico PDV** |
| **Risco** | Baixo Ã¢â‚¬â€ overlay PDV + APIs; histÃƒÂ³rico depende da aba 9 / migration `0054` (pacote Cadastro modal) |
| **MÃƒÂ©todo** | worktree `origin/producao` Ã‚Â· checkout desses arquivos de `teste` Ã‚Â· **nÃƒÂ£o** merge inteiro |
| **Autorizar com** | *Ã‚Â«pode subir editor rÃƒÂ¡pido PDV / lÃƒÂ¡pisÃ‚Â»* + **99738595** |
| **VocÃƒÂª apÃƒÂ³s** | Ctrl+F5 loja Ã‚Â· badge Ã‚Â· lÃƒÂ¡pis no milho Ã¢â€ â€™ salva Ã¢â€ â€™ aba 9 no cadastro |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ pente fino editor rÃƒÂ¡pido (17/07 Ã‚Â· **teste v9.54**)

| | |
| --- | --- |
| **O quÃƒÂª** | Lazy unidade Ã‚Â· Esc correto Ã‚Â· custo no patch do carrinho Ã‚Â· scroll sÃƒÂ³ se Formas A/B estourar tela baixa |
| **Status** | Ã°Å¸â€œÂ¦ fecha no pacote acima |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ editor sem scroll: pop quase tela cheia (17/07 Ã‚Â· **teste v9.53**)

| | |
| --- | --- |
| **O quÃƒÂª** | Letras grandes iguais; **popup mais alto** (quase 98 % da tela, sem teto em rem) Ã¢â‚¬â€ estoque cabe **sem barra de rolagem** |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.53** Ã‚Â· lÃƒÂ¡pis Ã‚Â· confere se some o scroll |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ editor rÃƒÂ¡pido bem maior (+25 %) (17/07 Ã‚Â· **teste v9.51**)

| | |
| --- | --- |
| **O quÃƒÂª** | Popup ediÃƒÂ§ÃƒÂ£o rÃƒÂ¡pida com **zoom 1,25** (tudo cresce junto). Deve dar pra notar |
| **VocÃƒÂª** | **Ctrl+F5** Ã‚Â· badge **v9.51** Ã‚Â· lÃƒÂ¡pis |

### Ã°Å¸Â©Â¹ Cadastro Ã¢â‚¬â€ PreÃƒÂ§os bem legÃƒÂ­vel + preenche altura + URL imagem (17/07 Ã‚Â· **teste v9.50**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | NÃƒÂºmeros dos cards de PreÃƒÂ§os bem maiores (preenchem a tag). Fiscal/Gerais espalham na altura. ComposiÃƒÂ§ÃƒÂ£o: tabela cresce atÃƒÂ© o rodapÃƒÂ©. Campo sem nome = **URL da imagem** (agora com rÃƒÂ³tulo) |
| **VocÃƒÂª** | Ctrl+F5 forte Ã‚Â· badge **v9.50** Ã‚Â· PreÃƒÂ§os Ã‚Â· Fiscal Ã‚Â· ComposiÃƒÂ§ÃƒÂ£o Ã‚Â· Gerais |
| **Loja** | Ã¢ÂÂ³ |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ editor rÃƒÂ¡pido um degrau maior (17/07 Ã‚Â· **teste v9.49**)

| | |
| --- | --- |
| **O quÃƒÂª** | Popup ediÃƒÂ§ÃƒÂ£o rÃƒÂ¡pida **maior ~12 %** (caixa + rÃƒÂ³tulos + campos + botÃƒÂµes + saldos) Ã¢â‚¬â€ mesma disposiÃƒÂ§ÃƒÂ£o, melhor em tela pequena |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.49** Ã‚Â· lÃƒÂ¡pis Ã¢â€ â€™ confere se cabe e lÃƒÂª melhor |

### Ã°Å¸Â©Â¹ Cadastro Ã¢â‚¬â€ modal: proporÃƒÂ§ÃƒÂ£o PreÃƒÂ§os + listas atÃƒÂ© o rodapÃƒÂ© (17/07 Ã‚Â· **teste v9.48**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | PreÃƒÂ§os: tag e nÃƒÂºmero mais equilibrados. Config colado em cima. ComposiÃƒÂ§ÃƒÂ£o / AlteraÃƒÂ§ÃƒÂµes / Estoque: ÃƒÂ¡rea da lista cresce atÃƒÂ© o rodapÃƒÂ© (sem buraco branco) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.48** Ã‚Â· PreÃƒÂ§os Ã‚Â· ComposiÃƒÂ§ÃƒÂ£o Ã‚Â· Config Ã‚Â· AlteraÃƒÂ§ÃƒÂµes |
| **Loja** | Ã¢ÂÂ³ |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ estoque editor: nÃƒÂºmeros do mesmo tamanho (17/07 Ã‚Â· **teste v9.46**)

| | |
| --- | --- |
| **O quÃƒÂª** | Nos cards Centro/Vila, **agora** e **novo** ficam com a mesma fonte grande (campo nÃƒÂ£o fica miÃƒÂºdo ao lado do saldo) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.46** Ã‚Â· lÃƒÂ¡pis Ã¢â€ â€™ confere proporÃƒÂ§ÃƒÂ£o dos saldos |

### Ã¢Å“Â¨ Cadastro Ã¢â‚¬â€ abas do modal usam a altura (17/07 Ã‚Â· **teste v9.45**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Abas sem lista histÃƒÂ³rica (Gerais, Fiscal, PreÃƒÂ§os, ComposiÃƒÂ§ÃƒÂ£o, Config, Validade, Marcas) espalham campos / tabela maior Ã¢â‚¬â€ sem Ã¢â‚¬Å“buraco brancoÃ¢â‚¬Â embaixo. Estoque e AlteraÃƒÂ§ÃƒÂµes continuam com scroll na lista |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.45** Ã‚Â· abrir produto Ã¢â€ â€™ Fiscal / ComposiÃƒÂ§ÃƒÂ£o |
| **Loja** | Ã¢ÂÂ³ |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ editor: 2 tags estoque + unidade busca (17/07 Ã‚Â· **teste v9.44**)

| | |
| --- | --- |
| **O quÃƒÂª** | Estoque em **2 cards** (tag Centro / Vila). Unidade = busca nas jÃƒÂ¡ usadas (ignora acento/maiÃƒÂºscula) + **Cadastrar Ã‚Â«Ã¢â‚¬Â¦Ã‚Â»** se nÃƒÂ£o achar. Mensagem vermelha Ã‚Â«Produto nÃƒÂ£o encontradoÃ‚Â» sumiu: API agora lÃƒÂª Mongo se o produto ainda nÃƒÂ£o estÃƒÂ¡ no Postgres |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.44** Ã‚Â· lÃƒÂ¡pis no milho Ã‚Â· confere estoque + unidade |

### Ã¢Å“Â¨ Cadastro Ã¢â‚¬â€ modal editar maior (altura) (17/07 Ã‚Â· **teste v9.42**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Popup editar produto quase tela cheia (~98dvh, mais largo) Ã‚Â· ÃƒÂ¡rea das listas de histÃƒÂ³rico com altura mÃƒÂ­nima + scroll prÃƒÂ³prio |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.42** Ã‚Â· abrir produto Ã¢â€ â€™ aba Estoque / AlteraÃƒÂ§ÃƒÂµes |
| **Loja** | Ã¢ÂÂ³ |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ editor rÃƒÂ¡pido puxa GM/barras/custo certos (17/07 Ã‚Â· **teste v9.41**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Campo GM deixa de mostrar cÃƒÂ³digo sistema (ex. 9047) Ã‚Â· puxa **GM0090-47**, barras e custo (overlay/Mongo) |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.41** Ã‚Â· lÃƒÂ¡pis no milho Ã‚Â· confere GM + barras + custo 67 |
| **Loja** | Ã¢ÂÂ³ |

### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ formas A/B recolhidas no editor rÃƒÂ¡pido (17/07 Ã‚Â· **teste v9.40**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Tabela de formas A/B fica **escondida**; botÃƒÂ£o **Formas A/B** ao lado dos preÃƒÂ§os abre/fecha |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.40** Ã‚Â· lÃƒÂ¡pis Ã¢â€ â€™ sÃƒÂ³ preÃƒÂ§os A/B; clicar Formas A/B se precisar |
| **Loja** | Ã¢ÂÂ³ |

### Ã¢Å“Â¨ PDV Ã¢â‚¬â€ editor rÃƒÂ¡pido no lÃƒÂ¡pis (17/07 Ã‚Â· **teste v9.38**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | LÃƒÂ¡pis do carrinho: modal leve (nome, GM, barras, unidade, custo, venda, **A/B + formas**, estoque Centro/Vila). Salva e atualiza o item |
| **APIs** | pdv-edicao-rapida Ã‚Â· pdv-ajuste-estoque Ã‚Â· overlay parcial |
| **VocÃƒÂª** | Ctrl+F5 teste Ã‚Â· badge **v9.38** Ã‚Â· lÃƒÂ¡pis Ã¢â€ â€™ edita Ã¢â€ â€™ Salvar |
| **Loja** | Ã¢ÂÂ³ |

### Ã¢Å“Â¨ Cadastro Ã¢â‚¬â€ aba 9 AlteraÃƒÂ§ÃƒÂµes (histÃƒÂ³rico cadastro) (17/07 Ã‚Â· **teste v9.33**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Aba **9. AlteraÃƒÂ§ÃƒÂµes**: qualquer campo do cadastro (nome, preÃƒÂ§o, cÃƒÂ³digoÃ¢â‚¬Â¦) **antes Ã¢â€ â€™ depois + quem**. **NÃƒÂ£o** ÃƒÂ© movimentaÃƒÂ§ÃƒÂ£o de estoque (isso fica na aba 4). 40 + carregar mais Ã‚Â· botÃƒÂ£o vermelho histÃƒÂ³rico completo (atÃƒÂ© 500). SÃƒÂ³ daqui pra frente |
| **API** | `GET /api/produtos/cadastro/alteracoes-historico/` Ã‚Â· migration `0054` |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.34** Ã‚Â· editar produto Ã¢â€ â€™ mudar nome Ã¢â€ â€™ Salvar Ã¢â€ â€™ aba **AlteraÃƒÂ§ÃƒÂµes** |
| **Loja** | Ã¢ÂÂ³ |


### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ RelatÃƒÂ³rios ajuda Ã‚Â«?Ã‚Â» (17/07 Ã‚Â· Ã¢Å“â€¦ enviado v9.90)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã°Å¸â€œÂ¦ **pronto pra envio** Ã¢â‚¬â€ Renan OK no teste Ã‚Â· espera frase + senha `99738595` na **mesma mensagem** |
| **Inclui** | Ajuda **?** leiga em **todas** as telas de relatÃƒÂ³rios (hub, validade, ABC, ranking, margemÃ¢â‚¬Â¦) Ã‚Â· botÃƒÂ£o ÃƒÂ¢mbar ao lado de Imprimir A4 Ã‚Â· painel colorido por coluna Ã‚Â· **sem** scroll interno |
| **Commits teste** | `3828dcf` Ã‚Â· `6016bc1` Ã‚Â· `61f6137` (feat + alinhamento + restaura botÃƒÂ£o) |
| **Arquivos** | `relatorios_help_agents.html` Ã‚Â· `relatorios_generico.html` Ã‚Â· `relatorios_hub.html` Ã‚Â· `relatorios_validade.html` Ã‚Â· `relatorios_central_views.py` |
| **VersÃƒÂ£o alvo loja** | cherry-pick dos 3 commits (apÃƒÂ³s FL-056 ou junto, se pedir) |
| **Base loja hoje** | **v9.16** |
| **Autorizar com** | *Ã‚Â«pode subir ajuda relatÃƒÂ³riosÃ‚Â»* + **99738595** |
| **VocÃƒÂª apÃƒÂ³s** | Ctrl+F5 loja Ã‚Â· RelatÃƒÂ³rios Ã¢â€ â€™ Curva ABC Ã¢â€ â€™ **?** ao lado do azul |

### Ã¢Å“Â¨ Cadastro Ã¢â‚¬â€ kardex unificado na aba Estoque (17/07 Ã‚Â· **teste v9.30**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Remove Ã‚Â«HistÃƒÂ³rico de FornecedoresÃ‚Â» Ã‚Â· tabela **HistÃƒÂ³rico de movimentaÃƒÂ§ÃƒÂ£o** (venda, devoluÃƒÂ§ÃƒÂ£o, NF, transferÃƒÂªncia, ajuste) Ã‚Â· filtros depÃƒÂ³sito/tipo/perÃƒÂ­odo Ã‚Â· 50 + carregar mais (teto 200) Ã‚Â· link da venda Ã‚Â· **operador = nome, nunca PIN** |
| **API** | `GET /api/produtos/cadastro/estoque-movimentos/` |
| **Arquivos** | `estoque_movimentos_cadastro_util.py` Ã‚Â· `views.py` Ã‚Â· `_modal_editar_produto_cadastro_erp.inc.html` Ã‚Â· `produtos_cadastro_erp.html` |
| **VocÃƒÂª** | Ctrl+F5 teste Ã‚Â· badge **v9.30** Ã‚Â· abrir produto Ã¢â€ â€™ aba Estoque |
| **Loja** | Ã¢ÂÂ³ |

### Ã¢Å“Â¨ PDV Ã¢â‚¬â€ lixeira + lÃƒÂ¡pis no carrinho (17/07 Ã‚Â· **teste v9.29**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | AÃƒÂ§ÃƒÂµes do item: **lixeira em cima** + **lÃƒÂ¡pis embaixo** (menor, mesma altura de linha). LÃƒÂ¡pis = placeholder (`data-edit-item`) p/ ligar tela depois |
| **Arquivos** | `pdv_wizard.html` Ã‚Â· `pdv_wizard.js` |
| **VocÃƒÂª** | Ctrl+F5 teste Ã‚Â· badge **v9.29** Ã‚Â· carrinho com 2 itens Ã‚Â· conferir 2 botÃƒÂµes sem esticar a linha |
| **Loja** | Ã¢ÂÂ³ |

### fix Ã¢â‚¬â€ RelatÃƒÂ³rios Ã‚Â«?Ã‚Â» sumiu (17/07 Ã‚Â· **teste v9.28**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Troca `<details>` por **botÃƒÂ£o** ÃƒÂ¢mbar fixo (mesmo tamanho do Imprimir A4) Ã‚Â· painel abre/fecha no clique Ã‚Â· estilo inline pra nÃƒÂ£o sumir |
| **Arquivo** | `relatorios_help_agents.html` |
| **VocÃƒÂª** | Ctrl+F5 teste Ã‚Â· badge **v9.28** Ã‚Â· ? laranja ao lado do azul |
| **Loja** | Ã°Å¸â€œÂ¦ **pronto pra envio** (pacote RelatÃƒÂ³rios ajuda Ã‚Â«?Ã‚Â») |

### fix Ã¢â‚¬â€ RelatÃƒÂ³rios ajuda Ã‚Â«?Ã‚Â» alinhada + sem scroll no card (17/07 Ã‚Â· **teste v9.27**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | **?** ao lado de Imprimir A4 / Voltar (mesmo tamanho 44px) Ã‚Â· painel ÃƒÂ  direita **sem** max-height / scrollbar interna |
| **Arquivos** | `relatorios_help_agents.html` Ã‚Â· `relatorios_generico.html` Ã‚Â· `relatorios_hub.html` Ã‚Â· `relatorios_validade.html` |
| **VocÃƒÂª** | Ctrl+F5 teste Ã‚Â· badge **v9.27** Ã‚Â· RelatÃƒÂ³rios Ã¢â€ â€™ ? ao lado do azul |
| **Loja** | Ã°Å¸â€œÂ¦ **pronto pra envio** (pacote RelatÃƒÂ³rios ajuda Ã‚Â«?Ã‚Â») |

### Ã¢Å“Â¨ Cadastro ERP Ã¢â‚¬â€ busca + filtros na barra (17/07 Ã‚Â· **teste v9.24**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Sem botÃƒÂ£o CatÃƒÂ¡logo Ã‚Â· **Filtros avanÃƒÂ§ados** ao lado do Novo Ã‚Â· busca com borda verde forte + Ã‚Â«Digite aqui para buscarÃ¢â‚¬Â¦Ã‚Â» |
| **Arquivos** | `produtos_cadastro_erp.html` Ã‚Â· `cadastro_erp_panel.js` |
| **VocÃƒÂª** | Ctrl+F5 teste Ã‚Â· badge **v9.25** Ã‚Â· `/produtos/cadastro-erp/` |

### Ã¢Å“Â¨ RelatÃƒÂ³rios Ã¢â‚¬â€ ajuda Ã‚Â«?Ã‚Â» leiga em todas as telas (17/07 Ã‚Â· **teste v9.23** Ã‚Â· Ã¢Å“â€¦ OK Renan)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | BotÃƒÂ£o **?** em todas as telas de relatÃƒÂ³rios Ã‚Â· painel didÃƒÂ¡tico (colunas + o que o relatÃƒÂ³rio faz) Ã‚Â· hub, validade e todos os cards do genÃƒÂ©rico |
| **Arquivos** | `includes/relatorios_help_agents.html` Ã‚Â· `relatorios_generico.html` Ã‚Â· `relatorios_hub.html` Ã‚Â· `relatorios_validade.html` Ã‚Â· `relatorios_central_views.py` |
| **VocÃƒÂª** | Ã¢Å“â€¦ OK no teste (v9.28) |
| **Loja** | Ã°Å¸â€œÂ¦ **pronto pra envio** Ã¢â‚¬â€ ver pacote **RelatÃƒÂ³rios ajuda Ã‚Â«?Ã‚Â»** |

### Ã¢Å“Â¨ Cadastro ERP Ã¢â‚¬â€ layout header/toolbar (17/07 Ã‚Â· **teste v9.22**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | CatÃƒÂ¡logo + Ã‚Â«GestÃƒÂ£o de ProdutosÃ‚Â» / Somente ativos sobem pro header Ã‚Â· **Grupos** oculto Ã‚Â· barra: Busca Ã‚Â· Filtrar Ã‚Â· **+ Novo** Ã‚Â· (direita) ExcelÃ¢â€ â€œÃ¢â€ â€˜ Ã‚Â· HistÃƒÂ³rico Ã‚Â· Colunas |
| **Arquivo** | `produtos_cadastro_erp.html` |
| **VocÃƒÂª** | Ctrl+F5 teste Ã‚Â· badge **v9.22** Ã‚Â· `/produtos/cadastro-erp/` |
| **Loja** | ainda **v9.16** Ã¢â‚¬â€ sobe sÃƒÂ³ com frase+senha |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ NFC-e FL-056 (17/07 Ã‚Â· Ã¢Å“â€¦ enviado v9.90)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã°Å¸â€œÂ¦ **pronto pra envio** Ã¢â‚¬â€ espera frase + senha `99738595` na **mesma mensagem** |
| **Inclui** | **FL-056** Ã¢â‚¬â€ SEFAZ **963** + **225** (`nfce_sp_emissao_util.py` Ã‚Â· `nfce_fiscal_produto_util.py`) |
| **VersÃƒÂ£o alvo loja** | **v9.21** |
| **Base loja hoje** | **v9.16** |
| **Autorizar com** | *Ã‚Â«pode subir NFC-e 963/225Ã‚Â»* + **99738595** |
| **VocÃƒÂª apÃƒÂ³s** | Ctrl+F5 loja Ã‚Â· reemitir **#2812** e **#3347** |

### Ã°Å¸Ââ€º NFC-e Ã¢â‚¬â€ rejeiÃƒÂ§ÃƒÂµes 963 + 225 (17/07 Ã‚Â· teste)

| Item | Detalhe |
| ---- | ------- |
| **Casos** | Venda **#2812** Ã¢â€ â€™ **963** Ã‚Â· Venda **#3347** Ã¢â€ â€™ **225** (frete #23 jÃƒÂ¡ OK) |
| **963** | Fiado/`tPag=05` ia com grupo `<card>` Ã¢â‚¬â€ SEFAZ nÃƒÂ£o aceita. `_TPAG_REQUER_CARD` sÃƒÂ³ **03/04/10Ã¢â‚¬â€œ13/15/17/18** |
| **225** | CFOP/CEST/NCM com ponto/traÃƒÂ§o no XML (schema). Limpa sÃƒÂ³ dÃƒÂ­gitos + CEST sÃƒÂ³ se 7 dÃƒÂ­gitos |
| **Arquivos** | `nfce_sp_emissao_util.py` Ã‚Â· `nfce_fiscal_produto_util.py` |
| **Commits teste** | `fb6ba2f` Ã¢â‚¬Â¦ (**v9.21**) |
| **VocÃƒÂª** | Ctrl+F5 teste Ã‚Â· badge **v9.21** Ã‚Â· em Contabilidade **reemitir** #2812 e #3347 |
| **Status** | Ã°Å¸â€œÂ¦ **pronto pra envio** (loja ainda v9.16) |
| **Loja** | frase + senha na mesma mensagem |

### Ã°Å¸â€œÂ¦ Deploy loja **v9.16** Ã¢â‚¬â€ Fecha + RelatÃƒÂ³rios + NFC-e #23 (17/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | **#12** devoluÃƒÂ§ÃƒÂ£o Ã‚Â· **#17** busca CP Ã‚Â· giro ÃƒÂºltima venda Ã‚Â· **#23** NFC-e 535 |
| **HEAD loja** | **4113492** Ã‚Â· base **c61f7dd** (v9.14) |
| **Backup** | `producao-backup-pre-v916-fecha-relatorios-nfce-20260717` @ **c61f7dd** Ã¢â‚¬â€ reverter: `git push origin producao-backup-pre-v916-fecha-relatorios-nfce-20260717:producao` |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode enviar* + 99738595 |
| **VocÃƒÂª** | Ctrl+F5 Ã‚Â· badge **v9.16** Ã‚Â· devoluÃƒÂ§ÃƒÂ£o Ã‚Â· busca CP Ã‚Â· giro Ã‚Â· NFC-e c/ frete |

### Ã°Å¸â€œÂ¦ PACOTE PRONTO LOJA Ã¢â‚¬â€ Ã¢Å“â€¦ ENVIADO v9.16 (17/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **montado** Ã‚Â· **nÃƒÂ£o subiu** Ã¢â‚¬â€ espera frase + senha `99738595` na **mesma mensagem** |
| **VersÃƒÂ£o alvo loja** | **v9.16** |
| **Base loja hoje** | **c61f7dd** Ã‚Â· **v9.14** |
| **Backup (criar no envio)** | `producao-backup-pre-v916-fecha-relatorios-nfce-YYYYMMDD` @ HEAD loja atual |
| **Reverter** | `git push origin <backup>:producao` |
| **MÃƒÂ©todo** | worktree em `origin/producao` Ã‚Â· `git checkout origin/teste -- <arquivos>` Ã‚Â· **nÃƒÂ£o** merge inteiro testeÃ¢â€ â€™producao |
| **Risco** | MÃƒÂ©dio Ã¢â‚¬â€ #12 mexe venda/caixa/migraÃƒÂ§ÃƒÂ£o **0053** Ã‚Â· #17 sÃƒÂ³ busca CP Ã‚Â· relatÃƒÂ³rios sÃƒÂ³ leitura Ã‚Â· #23 NFC-e frete (pequeno) |
| **Autorizar com** | *Ã‚Â«pode subir para produÃƒÂ§ÃƒÂ£o o pacote fecha + relatÃƒÂ³rios + #23Ã‚Â»* + **99738595** |

#### A) Fecha (Ã¢Å“â€¦ Renan testou)

| Ref | O quÃƒÂª | Arquivos-chave |
| --- | ----- | -------------- |
| **#12** / FL-035 | DevoluÃƒÂ§ÃƒÂ£o parcial / por item no PDV | `devolucao_venda_util.py` Ã‚Â· migraÃƒÂ§ÃƒÂ£o **0053** Ã‚Â· `models.py` Ã‚Â· `views.py` Ã‚Â· `venda_agro_detalhe.html` Ã‚Â· `vendas_lista.html` Ã‚Â· `caixa_util.py` Ã‚Â· `caixa_relatorio_util.py` |
| **#12 cupom** | Risco + total restante na reimpressÃƒÂ£o | `venda_cupom_util.py` Ã‚Â· `venda_cupom_80mm.js` Ã‚Â· `context_processors.py` Ã‚Â· `views_mp_point.py` Ã‚Â· `config/settings.py` |
| **#17** / FL-022 | Busca CP inteligente (valor/data/boleto/parcela/CPF) | `lancamentos_financeiro_pg_util.py` Ã‚Â· `mongo_financeiro_util.py` Ã‚Â· `lancamentos_help_agents.html` Ã‚Â· `lancamentos_contas_pagar_teste.html` Ã‚Â· `AGENTS.md` |

#### B) RelatÃƒÂ³rios (tudo que falta na loja)

| Item | O quÃƒÂª |
| ---- | ----- |
| **v9.15** | Giro/parado: coluna **ÃƒÅ¡ltima venda** + filtro **Ordenar** por data (tela / A4 / Excel) |
| **Arquivos** | `relatorios_vendas_util.py` Ã‚Â· `relatorios_central_views.py` Ã‚Â· `relatorios_generico.html` |
| **JÃƒÂ¡ na loja (v9.14)** | Hub Ã‚Â· ABC categoria Ã‚Â· loading Ã‚Â· cÃƒÂ³digo GM Ã‚Â· Ver todos Ã‚Â· De/AtÃƒÂ© Ã¢â‚¬â€ **nÃƒÂ£o precisa subir de novo** |

#### C) NFC-e #23 (incluso)

| Ref | O quÃƒÂª | Arquivo |
| --- | ----- | ------- |
| **#23** / FL-055 | SEFAZ **535** Ã¢â‚¬â€ `vFrete` nos itens = total do frete (vendas #3418/#3380) | `nfce_sp_emissao_util.py` Ã‚Â· commit teste `31eb9dc` |

#### NÃƒÂ£o sobe neste pacote

| Item | Motivo |
| ---- | ------ |
| Merge inteiro `teste`Ã¢â€ â€™`producao` | Outras coisas do teste fora do pacote |
| **#7** notebook impressÃƒÂ£o | Ainda Ã°Å¸â€Â´ aberto |
| FL-008 / 016 / 024 / 049 / Aba 9 | âœ… jÃ¡ na loja (pente fino 01/08) â€” nÃ£o listar como pendente |
| FL-029 crÃ©dito Â· **FL-058** vale crÃ©dito PDV Â· 052 Â· 030 Â· 019 Â· 054 Â· 031 Â· 034? Â· 053 Â· 033 Â· Zap #7 | Ainda aberto / confirmar |

#### Checklist validar depois do deploy (Ctrl+F5 Ã‚Â· badge **v9.16**)

1. RelatÃƒÂ³rios Ã¢â€ â€™ Giro/parado Ã¢â€ â€™ coluna **ÃƒÅ¡ltima venda** + ordenar  
2. RelatÃƒÂ³rios Ã¢â€ â€™ ABC Ã¢â€ â€™ categoria / Excel / loading (jÃƒÂ¡ v9.14; smoke)  
3. Vendas Ã¢â€ â€™ devoluÃƒÂ§ÃƒÂ£o parcial (#12)  
4. Contas a pagar Ã¢â€ â€™ busca valor / parcela (#17)  
5. NFC-e com frete Ã¢â‚¬â€ sem rejeiÃƒÂ§ÃƒÂ£o **535** (#23)  

### feat Ã¢â‚¬â€ giro/parado com ÃƒÂºltima venda + ordenaÃƒÂ§ÃƒÂ£o (16/07 Ã‚Â· **teste v9.15**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Coluna **ÃƒÅ¡ltima venda** no giro/parado + filtro **Ordenar** por data |
| **Onde** | Tela, impressÃƒÂ£o A4 e Excel |

### Ã°Å¸â€œÂ¦ Deploy loja **v9.14** Ã¢â‚¬â€ RelatÃƒÂ³rios (pÃƒÂ³s-v9.07) (16/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | CÃƒÂ³digo GM Ã‚Â· De/AtÃƒÂ© personalizado Ã‚Â· ABC Ver todos Ã‚Â· loading (abrir/Excel/imprimir) Ã‚Â· ABC filtro categoria. **SÃƒÂ³ relatÃƒÂ³rios** Ã¢â‚¬â€ **nÃƒÂ£o** NFC-e nem #12/#17 |
| **HEAD loja** | **c61f7dd** (base **53c13fb** v9.07) |
| **Backup** | `producao-backup-pre-v914-relatorios-20260716` @ **53c13fb** (v9.07) Ã¢â‚¬â€ reverter: `git push origin producao-backup-pre-v914-relatorios-20260716:producao` |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir para produÃƒÂ§ÃƒÂ£o essas atualizaÃƒÂ§ÃƒÂµes nos relatÃƒÂ³rios* + 99738595 |
| **VocÃƒÂª** | Ctrl+F5 loja Ã‚Â· badge **v9.14** Ã‚Â· RelatÃƒÂ³rios Ã¢â€ â€™ ABC (categoria) Ã‚Â· Excel Ã‚Â· Imprimir |

### feat Ã¢â‚¬â€ ABC filtro por categoria (16/07 Ã‚Â· **teste v9.14** Ã‚Â· **loja v9.14**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Select **Categoria** na Curva ABC Ã‚Â· recalcula A/B/C sÃƒÂ³ da categoria Ã‚Â· coluna Categoria na tela / A4 / Excel |
| **Arquivos** | `relatorios_vendas_util.py` Ã‚Â· `relatorios_central_views.py` Ã‚Â· `relatorios_generico.html` |

### feat Ã¢â‚¬â€ loading Excel + impressÃƒÂ£o (16/07 Ã‚Â· **teste v9.13**)

| Item | Detalhe |
| ---- | ------- |
| **Excel** | Overlay Ã‚Â«Gerando ExcelÃ¢â‚¬Â¦Ã‚Â» atÃƒÂ© o arquivo baixar (nÃƒÂ£o some a tela) |
| **Imprimir** | Overlay Ã‚Â«Preparando impressÃƒÂ£oÃ¢â‚¬Â¦Ã‚Â» atÃƒÂ© abrir a caixa |

### feat Ã¢â‚¬â€ loading visual nos relatÃƒÂ³rios (16/07 Ã‚Â· **teste v9.11**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Overlay Ã‚Â«CarregandoÃ¢â‚¬Â¦Ã‚Â» ao clicar no card / Atualizar / Excel / Ver todos |
| **Arquivo** | `_relatorios_loading.html` |

### feat Ã¢â‚¬â€ ABC Ver todos + total perÃƒÂ­odo (16/07 Ã‚Â· **teste v9.10**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Lista padrÃƒÂ£o 500 Ã‚Â· botÃƒÂ£o **Ver todos** Ã‚Â· rodapÃƒÂ© = total do perÃƒÂ­odo (para bater com vendas) Ã‚Â· % sobre o perÃƒÂ­odo inteiro |
| **Excel** | Sempre exporta lista completa |

### fix Ã¢â‚¬â€ filtro De/AtÃƒÂ© vira personalizado (16/07 Ã‚Â· **teste v9.09**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Mudar De/AtÃƒÂ© Ã¢â€ â€™ perÃƒÂ­odo **Personalizado** (tela + servidor) |
| **Loja** | Ã¢Å“â€¦ **v9.14** (16/07) |

### fix Ã¢â‚¬â€ CÃƒÂ³digo GM nos relatÃƒÂ³rios (16/07 Ã‚Â· **teste v9.08**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Coluna CÃƒÂ³digo = **cÃƒÂ³digo GM** (`codigo_nfe`/overlay); nÃƒÂ£o mostra ObjectId Mongo |
| **Onde** | Todos os relatÃƒÂ³rios com produto (ABC, ranking, margem, ruptura, comissÃƒÂ£oÃ¢â‚¬Â¦) |
| **Loja** | Ã¢Å“â€¦ **v9.14** (16/07) |

### hotfix loja **v9.07** Ã¢â‚¬â€ relatorios 500 resolvido (16/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Mais vendidos / ABC / grupo / margem / comparativo / comissao = 500 |
| **Causa** | Agregacao ItemVendaAgro com ExpressionWrapper (incompativel na loja) |
| **Fix** | Mesmo padrao do giro (`Sum` quantidade/valor) Ã‚Â· `53c13fb` |
| **Validar** | Ctrl+F5 Ã‚Â· badge **v9.07** Ã‚Â· Mais vendidos + Curva ABC |

### Ã°Å¸â€œÂ¦ Deploy loja **v9.04** Ã¢â‚¬â€ Central de RelatÃƒÂ³rios (16/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | SÃƒÂ³ pacote **relatÃƒÂ³rios** (hub + todos os cards novos + Excel). **NÃƒÂ£o** sobe #12/#17 |
| **HEAD loja** | **da264a3** (cÃƒÂ³digo `1cb43f2` + docs) |
| **Backup** | `producao-backup-pre-v904-relatorios-20260716` @ **5c44084** (v8.69) Ã¢â‚¬â€ reverter: `git push origin producao-backup-pre-v904-relatorios-20260716:producao` |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir para produÃƒÂ§ÃƒÂ£o esse cherry-pick* + 99738595 |
| **VocÃƒÂª** | Ctrl+F5 loja Ã‚Â· badge **v9.04** Ã‚Â· RelatÃƒÂ³rios Ã‚Â· ABC |

### Ã¢Å“â€¦ #17 / FL-022 Ã¢â‚¬â€ Busca CP Ã‚Â· Renan testou Ã‚Â· pronto produÃƒÂ§ÃƒÂ£o (16/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | Ã¢Å“â€¦ **testado no teste** Ã‚Â· **Ã°Å¸â€œÂ¦ pronto para envio ÃƒÂ  produÃƒÂ§ÃƒÂ£o** (frase + senha) |
| **Pacote** | Fecha com **#12** / FL-035 |
| **Validou** | Valor progressivo (`234`Ã¢â‚¬Â¦`234,78`) Ã‚Â· parcela (`parcela 7` / `7/12` no texto) Ã‚Â· sem lixo |
| **Loja** | Ã¢ÂÂ³ aguarda frase + senha |

### Ã¢Å“â€¦ CHECKLIST ÃƒÅ¡NICO Ã¢â‚¬â€ por prioridade Ã‚Â· 22/07

**P:** P0 loja Ã¢â€ â€™ P1 grave Ã¢â€ â€™ P2 melhoria Ã¢â€ â€™ P3 depois Ã‚Â· decimal menor = mais urgente (P1,1 antes de P1,5).  
**Zap #** e **FL-** na mesma fila. JÃƒÂ¡ na loja Ã¢â€ â€™ sÃƒÂ³ em **Checklist concluÃƒÂ­do** (abaixo).

#### Agora

| Quando | O quÃª |
| ------ | ----- |
| **âœ… Loja** | **v13.04** CP-BUSCA-FORN + DFE-NSU-656 |
| **âœ… Loja** | **v12.95** BI-KPI-LOJA Â· KPIs por loja Â· % dia semana Â· Validade |
| **âœ… Loja** | **v12.88** lote checklist Â· AJUSTE-MOBILE Â· BUG-FAB Â· PDV-USO-UX Â· BRINDE Â· DFE-CHAVE Â· ETQ-GM |
| **âœ… Loja** | **v12.51** marca+uso+bugs+DF-e Â· **v12.12** 12.08â€“12.12 Â· **v12.06** AJUSTE-MOBILE-CEL Â· **v12.05** ETQ |
| **ðŸ“¦ Fila pacotes** | *(vazia â€” lote CP+DFE v13.04 enviado)* |
| **â›” Fora** | **ENTRADA-NF-CUSTO** |
| **P0,1** | **FL-057** PgBouncer (painel Render) |
| **P0,2** | **FL-058** Vale crÃ©dito no cliente pelo PDV *(novo 01/08)* |
| **Pente fino 01/08** | Checklist mentia: FL-008 Â· 016 Â· 024 Â· 049 Â· Aba 9 **jÃ¡ na loja** (ver abaixo) |


#### Fila aberta (por prioridade)

| P | Ref | Pedido | Status |
| - | --- | ------ | ------ |
| **P1** | **RELAT-VENDAS-MARCA** | Vendas por marca (ordenar valor/qtd + Excel) | âœ… **loja v12.51** |
| **P1** | **PDV-USO-LOJA** | Uso loja PDV + PIN + histÃ³rico + totais | âœ… **loja v12.51** |
| **P1** | **BUG-REPORT** | Joaninha flutuante + lista GestÃ£o | âœ… **loja v12.51** |
| **P1** | **DFE-INBOX** | Caixa entrada Dist DF-e no PG | âœ… **loja v12.51** |
| **P1** | **PDV-LEMBRETE-ENTREGA** | Lembrete caixa some ao entregue/finalizar/cancelar | âœ… **loja v12.12** |
| **P1** | **AJUSTE-MOBILE** | Slim + Bip +1 + popup carga + fila cÃ³digos | âœ… **loja v12.88** Â· migrate 0078 |
| **P1** | **AJUSTE-MOBILE-SOMAR** | Contagem 2 lugares (Somar/Trocar) + catÃ¡logo no celular | âœ… **loja v12.12** |
| **P1** | **ENTRADA-NF-VINCULO** | VÃ­nculo XML cProd no Postgres (multi-PC) | âœ… **loja v12.12** |
| **P1** | **CAD-ESTOQUE-XLSX** | Excel estoque no Cadastro (filtros + Ajuste +/- + prÃ©via) | âœ… **loja v12.12** |
| **P1** | **PDV-CACHE-LAPIS** | LÃ¡pis PDV nÃ£o perde cache local | âœ… **loja v12.12** |
| **P1** | **PDV-PIX-SICREDI** | Pix mÃ¡quina Sicredi no PDV | âœ… **loja v11.98** (lote) |
| **P1** | **ENTRADA-NF-UX** | Boleto 44â†’47 + lista ConcluÃ­da + Nova limpa | âœ… **loja v11.98** (lote) |
| **P1** | **ETQ-GONDOLA** | Etiqueta gÃ´ndola A4 | âœ… **loja v11.98** (lote Â· migrate 0067) |
| **P1** | **TRANSF-FORCADA** | TransferÃªncia forÃ§ada Vilaâ†”Centro | âœ… **loja v11.98** (lote) |
| **P1** | **ETQ-BUSCA-FILTROS** | Etiquetas: filtros Folha + manter busca + Adicionar todos | âœ… **loja v12.05** |
| **P1** | **FOLHA-ETQ** | Folha polish + etiquetas | âœ… **loja v12.03** |
| **P1** | **ENTRADA-NF-CUSTO** | Entrada NF: puxar custo cadastrado (V. unit 0) | â›” **fora da fila** |
| **P1** | **PDV-ESTOQUE-VILA** | Atalho PDV Estoque Vila + Folha saldo UX | âœ… **loja v11.98** (lote) |
| **P0** | Entrada NF Â· custo | PÃ³s-deploy **v11.54+**: `reaplicar_custos_entrada_nf --nf=77846 --aplicar` | ðŸ“‹ **ops** Â· GM0025 |
| **P0,1** | **FL-057** | Render loja: **PgBouncer** `agro-db` + `DATABASE_URL` **6432** + restart web | ðŸ“‹ **vocÃª no painel** |
| **P0,2** | **FL-058** | **Vale crÃ©dito** â€” adicionar crÃ©dito ao cliente **pelo PDV** | ðŸ“‹ **novo 01/08** Â· ligado a FL-029 crÃ©dito |
| **P1** | Kardex | E-mail no Quem + Entrada NF Î”camada | âœ… **jÃ¡ loja v10.63** |
| **P1** | Aba 9 | HistÃ³rico lÃ¡pis PDV DE=â€” | âœ… **loja v10.61** (`e38df10`) â€” checklist mentia Â«pronto envioÂ» |
| **P2** | Cadastro modal | Modal editar: UX Â· kardex Â· aba AlteraÃ§Ãµes Â· origem PDV | âœ… **loja v9.90** |
| **P1** | PDV lÃ¡pis | Editor rÃ¡pido + histÃ³rico aba 9 (origem PDV) | âœ… **loja v9.90** |
| **P2** | PDVâ†’aba 9 | LÃ¡pis registra em AlteraÃ§Ãµes | âœ… **loja v9.90** |
| **P0** | **FL-056** | NFC-e **963** + **225** â€” vendas #2812/#3347 | âœ… **loja v9.90** |
| **P2** | PDV Enviar Zap | Enviar orÃ§amento WhatsApp | âœ… **loja v9.90** |
| **P2** | RelatÃ³rios | Ajuda **?** leiga | âœ… **loja v9.90** |
| **P1** | **Zap #7** | Notebook demora impressÃ£o | ðŸ”´ **ainda aberto** |
| **P1** | **FL-008** | Carrinho trava (qtd/preÃ§o/remover) | âœ… **loja v11.99** (`8f7610d`) â€” checklist mentia |
| **P1** | **FL-016** | Reset contagem caixa (dia anterior) | âœ… **loja v6.75** â€” checklist mentia |
| **P1,1** | **FL-029** | Baixa parcial fiado + crÃ©dito | âš ï¸ **parcial**: baixa âœ… loja v7.61 Â· crÃ©dito â†’ ver **FL-058 P0,2** |
| **P1,1** | **FL-052** | NFC-e na baixa fiado | ðŸ“‹ **ainda aberto** |
| **P1,3** | **FL-030** | Ignorar bloqueio fiado vencido (PIN) | ðŸ“‹ **ainda aberto** |
| **P1,5** | **FL-019** | Recibo pagamento fiado | âœ… **loja v15.62** (FIADO-RECIBO) |
| **P1,5** | **Zap #20** Â· **FL-054** | Reimprimir papÃ©is entrega | ðŸ“‹ **ainda aberto** |
| **P1,5** | **FL-049** | CPF no cliente PDV â†’ NFC-e | âœ… **cÃ³digo na loja** (`6af5cac`) â€” checklist mentia Â«testeÂ» |
| **P1,6** | **Zap #22** Â· **FL-024** | Cadastro cat/sub/marca sÃ³ lista + PIN | âœ… **loja v10.57** â€” checklist mentia |
| **P1,6** | **FL-031** | Terminar tela `/entregas/` | ðŸ“‹ **ainda aberto** |
| **P1,9** | **FL-034** | HistÃ³rico F8 filtra cliente | âš ï¸ **cÃ³digo filtro na loja** â€” confirmar se fecha o pedido |
| **P2** | **Zap #19** Â· **FL-053** | HistÃ³rico de custo tick 2Ã— | ðŸ“‹ **ainda aberto** |
| **P3** | **Zap #21** Â· **FL-033** | BI comparativo N-Ã©simo dia semana | ðŸ“‹ **ainda aberto** |
| **P2+** | FL-005â€¦ | Resto P2/P3 na Â«Fila lojaÂ» completa | ðŸ“‹ |

#### Checklist concluÃƒÂ­do (jÃƒÂ¡ na loja)

| Ref | Pedido | Nota |
| --- | ------ | ---- |
| Zap **#12** Ã‚Â· FL-035 | DevoluÃƒÂ§ÃƒÂ£o parcial | Ã¢Å“â€¦ loja **v9.16** |
| Zap **#17** Ã‚Â· FL-022 | Busca CP inteligente | Ã¢Å“â€¦ loja **v9.16** |
| Zap **#23** Ã‚Â· FL-055 | NFC-e 535 frete | Ã¢Å“â€¦ loja **v9.16** |
| Zap **#1** | Entrada NF etapa 7 Ã¢â‚¬â€ valor | Ã¢Å“â€¦ |
| Zap **#2** | Entrada NF busca barras | Ã¢Å“â€¦ |
| Zap **#3** | Cadastro custo / prefixo GM | Ã¢Å“â€¦ |
| Zap **#4** | Cadastro modelo | Ã¢Å“â€¦ |
| Zap **#5** | Busca cadastro + NF leve | Ã¢Å“â€¦ |
| Zap **#6** | PIN mais rÃƒÂ¡pido | Ã¢Å“â€¦ |
| Zap **#8** | Produto novo na lista | Ã¢Å“â€¦ |
| Zap **#9** | Fiado MP forma no caixa | Ã¢Å“â€¦ |
| Zap **#10** | Cancelar cobranÃƒÂ§a fiado | Ã¢Å“â€¦ |
| Zap **#11** | Menu caixa/vendas fecha | Ã¢Å“â€¦ |
| Zap **#13** Ã‚Â· FL-027 | XML boleto Ã¢â€ â€™ CN | Ã¢Å“â€¦ |
| Zap **#14** Ã‚Â· FL-026 | Add produto barras/lote | Ã¢Å“â€¦ |
| Zap **#15** Ã‚Â· FL-025 | CÃƒÂ³digo interno 4010Ã¢â‚¬â€œ5999 | Ã¢Å“â€¦ P0,9 |
| Zap **#16** Ã‚Â· FL-023 | CP busca limpa datas | Ã¢Å“â€¦ P1,2 |
| Zap **#18** Ã‚Â· FL-018 Ã‚Â· FL-020 | Frete cupom / 3 vias | Ã¢Å“â€¦ |
| **+** | PIN ao abrir PDV | Ã¢Å“â€¦ |
| FL-017 Ã‚Â· FL-021 Ã‚Â· FL-028 Ã‚Â· FL-032 Ã‚Â· FL-051 Ã‚Â· FL-046 Ã‚Â· FL-047Ã¢â‚¬Â¦ | Outros FL jÃƒÂ¡ na loja | ver Ã‚Â«Fila lojaÃ‚Â» |

**Cadastro FL completo** (tags): Ã‚Â«Fila loja Ã¢â‚¬â€ pedidos Zap / melhoriasÃ‚Â» mais abaixo. Status do dia Ã¢â€ â€™ **esta seÃƒÂ§ÃƒÂ£o primeiro**.

### Ã°Å¸â€œÂ¦ Deploy loja **v8.69** Ã¢â‚¬â€ Entrada NF busca BCA (16/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | SÃƒÂ³ fix busca Entrada NF = BCA/famÃƒÂ­lia GM (`views.py` + comentÃƒÂ¡rio `entrada_nota.html`). **NÃƒÂ£o** sobe pacote do fecha (#12/#17/relatÃƒÂ³rios) |
| **Commit loja** | **5c44084** (origem teste `93ac5b3`) |
| **Backup** | branch `producao-backup-pre-v869-20260716` @ **45622b0** (v8.68) Ã¢â‚¬â€ reverter: `git push origin producao-backup-pre-v869-20260716:producao` |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir esse cherry-pick* + 99738595 |
| **VocÃƒÂª** | Ctrl+F5 loja Ã‚Â· badge **v8.69** Ã‚Â· Entrada NF Ã‚Â· `gm0008` Ã¢â€ â€™ **3** iguais ao cadastro |

### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ busca BCA = cadastro (16/07 Ã‚Â· **teste v8.94** Ã‚Â· **loja v8.69**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `gm0008` no cadastro = **3** (BCA); na Entrada NF = **1** |
| **Causa** | API com `entrada_nfe=1` desligava Mongo Ã¢â‚¬â€ famÃƒÂ­lia GM nÃƒÂ£o completava (cadastro completa) |
| **Fix** | Mesmo motor unificado + complemento Mongo famÃƒÂ­lia GM; cache busca NF **v2** |
| **Arquivos** | `views.py` Ã‚Â· `entrada_nota.html` (comentÃƒÂ¡rio) |
| **Validar** | Ctrl+F5 teste Ã‚Â· Entrada NF etapa 2 Ã‚Â· digitar `gm0008` Ã¢â€ â€™ **3** iguais ao cadastro |

### Ã°Å¸â€œÅ  Central de RelatÃƒÂ³rios Ã¢â‚¬â€ pacote completo (16/07 Ã‚Â· teste)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Hub com cards: mais/menos vendidos Ã‚Â· por grupo Ã‚Â· curva ABC Ã‚Â· giro/parado Ã‚Â· margem Ã‚Â· operador Ã‚Â· ranking clientes Ã‚Â· comparativo Ã‚Â· formas pagamento Ã‚Â· ruptura Ã‚Â· comissÃƒÂ£o estimada (+ validade/etiquetas jÃƒÂ¡ existentes) |
| **Extras** | PerÃƒÂ­odo Ã‚Â· print A4 Ã‚Â· **Excel Ã¢â€ â€œ** em cada relatÃƒÂ³rio novo |
| **Fonte** | `VendaAgro` / `ItemVendaAgro` (Postgres) Ã‚Â· giro/parado reusa `dashboard_estoque_financeiro_util` |
| **Arquivos** | `relatorios_vendas_util.py` Ã‚Â· `relatorios_central_views.py` Ã‚Â· `relatorios_hub.html` Ã‚Â· `relatorios_generico.html` Ã‚Â· `urls.py` |
| **Validar** | Ctrl+F5 Ã‚Â· BI Ã¢â€ â€™ RelatÃƒÂ³rios gerais Ã‚Â· abrir cada card Ã‚Â· Excel + perÃƒÂ­odo |
| **VersÃƒÂ£o** | **teste v8.93** |
| **Layout hub (16/07)** | Largura mÃƒÂ¡xima + **4 colunas** Ã‚Â· seÃƒÂ§ÃƒÂµes **Vendas / Estoque / Equipe e clientes** |

### Ã°Å¸â€œÂ¦ Pacote pronto loja Ã¢â‚¬â€ fechar depois (16/07 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Quando** | Depois que a **loja fechar** Ã¢â‚¬â€ frase + senha na mesma mensagem |
| **Inclui** | **#12** / **FL-035** Ã‚Â· **#17** / **FL-022** (ver CHECKLIST ÃƒÅ¡NICO) |
| **Teste** | **#12 Ã¢Å“â€¦ Renan** Ã‚Â· **#17 Ã¢Å“â€¦ Renan** (valor + parcela) Ã¢â‚¬â€ **pronto envio produÃƒÂ§ÃƒÂ£o** |
| **NÃƒÂ£o sobe** | **#7** impressÃƒÂ£o notebook |
| **Antes** | loja hoje **v8.69** |

### Ã°Å¸Â©Â¹ Cupom Ã¢â‚¬â€ risco sumia na impressÃƒÂ£o (16/07 Ã‚Â· **teste v8.89** Ã‚Â· **Ã¢Å“â€¦ Renan**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | TOTAL jÃƒÂ¡ R$ 1,30, item de R$ 5 sem risco |
| **Causa** | JS da lista em cache `?v=1` + risco no flex some no Chrome |
| **Fix** | `agro_pdv_assets_v` global + `<s>` no texto do item Ã‚Â· commits `6caed7d`+ |
| **Validar** | Ã¢Å“â€¦ Renan OK Ã‚Â· pronto loja (fecha) |

### Ã°Å¸Â©Â¹ Cupom Ã¢â‚¬â€ risco no item devolvido (16/07 Ã‚Â· **teste v8.85**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | ReimpressÃƒÂ£o 80mm: item/frete devolvido riscado + TOTAL restante + faixa Ã‚Â«devoluÃƒÂ§ÃƒÂ£o parcialÃ‚Â» |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.85** Ã‚Â· venda parcial Ã¢â€ â€™ Imprimir |

### Ã°Å¸Â©Â¹ #17 busca valor progressiva (16/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Prints** | `234` Ã¢â€ â€™ lixo R$ 600; `234,`/`234,7` Ã¢â€ â€™ vazio; `234,78` Ã¢â€ â€™ OK |
| **Causa** | Exato demais + Ã‚Â«234Ã‚Â» ainda misturava documento/parcela |
| **Fix** | Faixa ao digitar: `234`/`234,` = [234Ã¢â‚¬â€œ235); `234,7` = [234,70Ã¢â‚¬â€œ234,80); completo = exato. Sem parcela em nÃƒÂºmero solto |
| **Validar** | Ã¢Å“â€¦ Renan Ã‚Â· faixa valor OK |
| **Arquivo** | `lancamentos_financeiro_pg_util.py` |

### Ã°Å¸Â©Â¹ #17 busca CP Ã¢â‚¬â€ valor/parcela sem lixo (16/07 Ã‚Â· **teste v8.84**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `234,` / `234,7` trazia tÃƒÂ­tulos de R$ 600; parcela sem match trazia tÃƒÂ­tulos soltos |
| **Causa** | Termo numÃƒÂ©rico misturava texto + `mongo_id` (ObjectId contÃƒÂ©m Ã‚Â«234Ã‚Â») |
| **Fix** | Modos exclusivos: vÃƒÂ­rgula/R$ Ã¢â€ â€™ sÃƒÂ³ valor; `2/6` Ã¢â€ â€™ sÃƒÂ³ parcela; data Ã¢â€ â€™ sÃƒÂ³ data; nÃ‚Âº curto sem mongo_id |
| **Validar** | Ã¢Å“â€¦ Renan Ã‚Â· sem lixo R$ 600 |
| **Arquivo** | `lancamentos_financeiro_pg_util.py` |

### Ã¢Å“â€¦ #17 Busca CP inteligente P0+P1+P2 (16/07 Ã‚Â· **teste v8.82**)

| Item | Detalhe |
| ---- | ------- |
| **P0** | Valor + data digitada |
| **P1** | Boleto + doc flexÃƒÂ­vel |
| **P2** | Parcela + CPF/CNPJ |
| **Arquivos** | `lancamentos_financeiro_pg_util.py` Ã‚Â· `mongo_financeiro_util.py` Ã‚Â· help + placeholder |
| **Validar** | Ã¢Å“â€¦ Renan testou Ã‚Â· valor + parcela OK |
| **Loja** | Ã°Å¸â€œÂ¦ **pronto produÃƒÂ§ÃƒÂ£o** Ã¢â‚¬â€ frase + senha (pacote do fecha c/ #12) |

### Ã¢Å“Â¨ Vendas Ã¢â‚¬â€ total restante + risco devolvido + corta ERP (16/07 Ã‚Â· **teste v8.79** Ã‚Â· **Ã¢Å“â€¦ Renan**)

| Item | Detalhe |
| ---- | ------- |
| **1 Lista** | Parcial mostra **restante** (ex. R$ 1,30) + original riscado; total do perÃƒÂ­odo soma restantes |
| **2 Detalhe** | Item devolvido com **risco** + badge; total restante no card |
| **3 ERP** | Coluna/botÃƒÂµes ERP sumiram; `PDV_VENDA_ERP_ENVIO=False` (padrÃƒÂ£o) Ã¢â‚¬â€ nÃƒÂ£o manda Pedidos/Salvar |
| **Validar** | Ã¢Å“â€¦ Renan Ã‚Â· **Ã°Å¸â€œÂ¦ pronto loja (fecha)** |

### Ã°Å¸â€œÂ¦ Deploy loja **v8.68** Ã¢â‚¬â€ FL-021 hotfix match NF (16/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | SÃƒÂ³ `nfe_entrada_util.py` Ã¢â‚¬â€ match ORM por nÃ‚Âº NF na descriÃƒÂ§ÃƒÂ£o. **NÃƒÂ£o** sobe devoluÃƒÂ§ÃƒÂ£o |
| **Commits loja** | **02dc397** + docs **45622b0** |
| **Backup** | branch `producao-backup-pre-v868-20260716` @ **ed1698e** (v8.67) Ã¢â‚¬â€ reverter: `git push origin producao-backup-pre-v868-20260716:producao` |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir sÃƒÂ³ esse hotfix* + 99738595 |
| **VocÃƒÂª** | Ctrl+F5 loja Ã‚Â· badge **v8.68** Ã‚Â· CP Agromaia Ã¢â€ â€™ botÃƒÂ£o **NF** |


### Ã°Å¸Â©Â¹ CP Ã¢â‚¬â€ botÃƒÂ£o NF ainda vazio (FL-021 hotfix) (16/07 Ã‚Â· **teste v8.76** Ã‚Â· **loja v8.68**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Match do rascunho no Postgres via ORM (nÃƒÂºmero NF na descriÃƒÂ§ÃƒÂ£o + variantes) Ã¢â‚¬â€ o v8.67 sÃƒÂ³ ligava o enrich, mas a busca cortava notas antigas |
| **Validar** | Ctrl+F5 loja Ã‚Â· CP Agromaia Ã¢â€ â€™ botÃƒÂ£o **NF** |
| **Loja** | Ã¢Å“â€¦ v8.68 |


### Ã°Å¸Â©Â¹ DevoluÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ textos no ? (16/07 Ã‚Â· **teste v8.74**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Regras longas no **?** Ajuda do modal (padrÃƒÂ£o banana / tela limpa) |
| **Validar** | Ctrl+F5 Ã‚Â· badge **v8.74** Ã‚Â· Devolver Ã¢â€ â€™ ? |
| **Loja** | Ã¢ÂÂ³ |



### Ã°Å¸Â©Â¹ DevoluÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ fontes do modal um pouco maiores (16/07 Ã‚Â· **teste v8.72**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Textos/itens/formas/total no popup um degrau maior (sem exagero) |
| **Validar** | Ctrl+F5 Ã‚Â· badge **v8.72** Ã‚Â· Devolver |
| **Loja** | Ã¢ÂÂ³ |



### Ã°Å¸Â©Â¹ DevoluÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ confirmaÃƒÂ§ÃƒÂ£o bonita + valor igual aos itens (16/07 Ã‚Â· **teste v8.70**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Sem confirm do Chrome Ã‚Â· aviso no estilo SisVale Ã‚Â· formas = itens (centavo a mais bloqueia) |
| **Validar** | F5 Ã‚Â· 1,31 vs 1,30 bloqueia Ã‚Â· igualar Ã‚Â· confirmaÃƒÂ§ÃƒÂ£o grande |
| **Loja** | Ã¢ÂÂ³ |



### Ã°Å¸Â©Â¹ DevoluÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ modal maior (idosos) (16/07 Ã‚Â· **teste v8.69**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Popup devoluÃƒÂ§ÃƒÂ£o ~54rem Ã‚Â· fontes/botÃƒÂµes/checkbox maiores (padrÃƒÂ£o idosos / Display Scale) |
| **Validar** | Ctrl+F5 Ã‚Â· badge **v8.69** Ã‚Â· Devolver venda Ã¢â€ â€™ ler sem apertar os olhos |
| **Loja** | Ã¢ÂÂ³ |



### Ã°Å¸â€œÂ¦ Deploy loja **v8.67** Ã¢â‚¬â€ botÃƒÂ£o NF no CP (FL-021) (16/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | SÃƒÂ³ o fix do botÃƒÂ£o **NF** em Contas a pagar (Postgres) Ã¢â‚¬â€ **nÃƒÂ£o** sobe devoluÃƒÂ§ÃƒÂ£o parcial |
| **Commits loja** | cherry `5000678` Ã¢â€ â€™ **0a5b620** Ã‚Â· docs **ed1698e** |
| **Backup** | branch `producao-backup-pre-v867-20260716` @ **92b1804** (v8.65) Ã¢â‚¬â€ reverter: `git push origin producao-backup-pre-v867-20260716:producao` |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir* + 99738595 Ã‚Â· pediu backup/seguro |
| **VocÃƒÂª** | Ctrl+F5 loja Ã‚Â· badge **v8.67** Ã‚Â· CP busca Agromaia Ã¢â€ â€™ botÃƒÂ£o NF |


### Ã°Å¸Â©Â¹ CP Ã¢â‚¬â€ botÃƒÂ£o NF sumiu na lista (FL-021) (16/07 Ã‚Â· **teste v8.67** Ã‚Â· **loja v8.67**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Com financeiro Postgres, a API nÃƒÂ£o preenchia o link da Entrada NF Ã¢â€ â€™ coluna NF vazia. Agora enriquece na lista/bootstrap; match de rascunho PG mais confiÃƒÂ¡vel |
| **Validar** | Ctrl+F5 Ã‚Â· Contas a pagar Ã‚Â· busca Agromaia (ou RBS) Ã‚Â· botÃƒÂ£o **NF** na coluna Ã‚Â· abre overlay da nota |
| **Loja** | Ã¢Å“â€¦ v8.67 |


### Ã¢Å“Â¨ PDV Ã¢â‚¬â€ devoluÃƒÂ§ÃƒÂ£o por item (#12) (16/07 Ã‚Â· **teste v8.66**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Devolver itens escolhidos (+ frete opcional). Forma Fiado sÃƒÂ³ se a venda era fiado (abate dÃƒÂ­vida, sem caixa). Outras formas = RETIRADA. NFC-e cancela sÃƒÂ³ no total |
| **Validar** | Ctrl+F5 Ã‚Â· badge **v8.66** Ã‚Â· venda com 2+ itens Ã¢â€ â€™ devolver 1 Ã‚Â· badge Parcial Ã‚Â· devolver resto Ã‚Â· total |
| **Loja** | Ã¢ÂÂ³ |



### Ã°Å¸â€œÂ¦ Deploy loja **v8.65** Ã¢â‚¬â€ preÃƒÂ§os A/B + PDV (16/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | Cadastro busca leve Ã‚Â· preÃƒÂ§os por forma **ou** 2 grupos A/B Ã‚Â· visual lista/ediÃƒÂ§ÃƒÂ£o/NF Ã‚Â· layout PDV Ã‚Â· Esc/PAGAR consumidor Ã‚Â· toque A/B no carrinho (forma manda na etapa 3) |
| **Commits loja** | 16b576bÃ¢â‚¬Â¦0ed48dd (cherry dafa6c9Ã¢â‚¬Â¦f0f4e31) Ã‚Â· HEAD **0ed48dd** |
| **Backup** | branch producao-backup-pre-v865-20260716 @ **ccb4fd8** (v8.54) Ã¢â‚¬â€ reverter: git push origin producao-backup-pre-v865-20260716:producao |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir* + 99738595 Ã‚Â· pedido de backup |
| **VocÃƒÂª** | Ctrl+F5 loja Ã‚Â· badge **v8.65** Ã‚Â· se der errado avisar (dÃƒÂ¡ pra voltar ao backup) |


### Ã¢Å“Â¨ PDV Ã¢â‚¬â€ toque A/B no carrinho (prÃƒÂ©via; forma manda) (15/07 Ã‚Â· **teste v8.65**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | No carrinho, toque em A ou B muda o preÃƒÂ§o da etapa 1. Etapa 3: forma de pagamento **sempre** redefine o preÃƒÂ§o (com ou sem toque) |
| **Validar** | Ctrl+F5 Ã‚Â· badge **v8.65** Ã‚Â· toque A Ã¢â€ â€™ 92 Ã‚Â· PAGAR Ã‚Â· forma do grupo B Ã¢â€ â€™ valor volta p/ 87 |
| **Loja** | Ã¢Å“â€¦ v8.65 |


### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ PAGAR sem travar cliente + consumidor na busca (15/07 Ã‚Â· **teste v8.64**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Se cliente ficou Ã‚Â«unsetÃ‚Â», PAGAR grava consumidor final sozinho. Busca de cliente jÃƒÂ¡ traz botÃƒÂ£o laranja do consumidor |
| **Validar** | Ctrl+F5 Ã‚Â· badge **v8.64** Ã‚Â· montar carrinho e PAGAR sem alerta Ã‚Â· faixa mostra consumidor |
| **Loja** | Ã¢Å“â€¦ v8.65 |


### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ cliente Esc + valor grupo A/B (15/07 Ã‚Â· **teste v8.63**)

| Item | Detalhe |
| ---- | ------- |
| **Cliente** | Esc no inÃƒÂ­cio = consumidor final (antes sÃƒÂ³ fechava e a tela mentia). Sem cliente Ã¢â€ â€™ texto Ã‚Â«Toque para escolherÃ‚Â» |
| **Pagamento** | Ao escolher forma, recalcula preÃƒÂ§o do grupo **depois** preenche Ã‚Â«Valor desta formaÃ‚Â» (ex. 92, nÃƒÂ£o 87) |
| **Validar** | Ctrl+F5 Ã‚Â· badge **v8.61** Ã‚Â· milho grupos Ã‚Â· PAGAR Ã‚Â· cartÃƒÂ£o do grupo A Ã¢â€ â€™ valor 92 |
| **Loja** | Ã¢Å“â€¦ v8.65 |


### Ã°Å¸Â©Â¹ PDV carrinho Ã¢â‚¬â€ colunas fixas + total sem corte (15/07 Ã‚Â· **teste v8.60**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | GM/qtd/A-B/total/lixeira com largura fixa (alinha). Total cabe milhar (ex. 1.131,00) |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.60** Ã‚Â· milho misto + qtd alta no item com grupos |
| **Loja** | Ã¢Å“â€¦ v8.65 |


### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ alinhamento GM + 2 preÃƒÂ§os (lista e carrinho) (15/07 Ã‚Â· **teste v8.59**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Busca: Marca Ã¢â€ â€™ GM Ã¢â€ â€™ espaÃƒÂ§o fixo p/ 2 preÃƒÂ§os. Carrinho: coluna A/B sempre reservada (nÃƒÂ£o empurra lixeira) |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.59** Ã‚Â· digitar Ã‚Â«milhoÃ‚Â» Ã‚Â· carrinho misturando 1 e 2 preÃƒÂ§os |
| **Loja** | Ã¢Å“â€¦ v8.65 |


### Ã¢Å“Â¨ Visual A/B Ã¢â‚¬â€ lista cadastro + ediÃƒÂ§ÃƒÂ£o + Entrada NF (15/07 Ã‚Â· **teste v8.58**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Coluna VENDA com chips A/B Ã‚Â· preview Grupo A/B na aba preÃƒÂ§os Ã‚Â· chips A/B sob P.venda na Entrada NF |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.58** Ã‚Â· produto em 2 grupos: lista + lÃƒÂ¡pis + casar na NF |
| **Loja** | Ã¢Å“â€¦ v8.65 |


### Ã°Å¸Â©Â¹ PDV Ã¢â‚¬â€ grupos A/B aparecem + modal preÃƒÂ§os maior (15/07 Ã‚Â· **teste v8.57**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | Busca usava sÃƒÂ³ cache local e merge ignorava o servidor Ã¢â€ â€™ sem precos_grupos |
| **Fix** | Sempre confere servidor Ã‚Â· merge atualiza campos Ã‚Â· overlay no catÃƒÂ¡logo PDV Ã‚Â· modal preÃƒÂ§os ~90% |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.57** Ã‚Â· digitar produto com 2 grupos Ã¢â€ â€™ chips A/B na busca e no carrinho |
| **Loja** | Ã¢Å“â€¦ v8.65 |


### Ã¢Å“Â¨ PDV Ã¢â‚¬â€ preÃƒÂ§o por forma OU 2 grupos (15/07 Ã‚Â· **teste v8.56**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | No cadastro: modo **Por forma** (igual antes) ou **2 grupos** (preÃƒÂ§o A/B + formas em A/B). PDV mostra A e B na busca/carrinho; total sÃƒÂ³ muda na etapa 3 ao escolher a forma |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.56** Ã‚Â· produto em 2 grupos Ã¢â€ â€™ 2 chips no PDV Ã‚Â· pagar com forma do grupo A/B |
| **Loja** | Ã¢Å“â€¦ v8.65 |


### Ã°Å¸Â©Â¹ Cadastro Ã¢â‚¬â€ busca digitada mais leve (15/07 Ã‚Â· **teste v8.55**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | DigitaÃƒÂ§ÃƒÂ£o no cadastro: Mongo slim Ã‚Â· sem mÃƒÂ©dia/pedidos Ã‚Â· sem recalcular saldo 2Ãƒâ€” Ã‚Â· debounce texto 450 ms |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.55** Ã‚Â· digitar nome no cadastro deve responder mais rÃƒÂ¡pido |
| **Loja** | Ã¢Å“â€¦ v8.65 |


### Ã°Å¸â€œÂ¦ Deploy loja **v8.54** Ã¢â‚¬â€ Entrada NF UX restante (15/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | Nome SisVale sob descriÃƒÂ§ÃƒÂ£o Ã‚Â· Emb. fechada alarga CÃƒÂ³d./EAN Ã‚Â· alinhamento linha + bordas |
| **Commit loja** | ccb4fd8 Ã‚Â· backup producao-backup-pre-v854-20260715 @ 6f0df25 |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *enviarÃ¢â‚¬Â¦ produÃƒÂ§ÃƒÂ£o* + 99738595 |
| **VocÃƒÂª** | Ctrl+F5 loja Ã‚Â· badge **v8.54** |


### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ alinhamento linha + borda campos (15/07 Ã‚Â· **teste v8.54**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Nome SisVale sÃƒÂ³ empurra o fim da descriÃƒÂ§ÃƒÂ£o; CÃƒÂ³d./demais campos alinhados no topo Ã‚Â· borda dos campos um pouco mais forte |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.54** Ã‚Â· linha reta descriÃƒÂ§ÃƒÂ£oÃ¢â€ â€™cÃƒÂ³digo Ã‚Â· campos mais legÃƒÂ­veis |
| **Loja** | **Ã¢Å“â€¦** v8.54 |


### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ emb. recolhida alarga CÃƒÂ³d./EAN (15/07 Ã‚Â· **teste v8.53**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Com Embalagem fechada, espaÃƒÂ§o livre vai para **CÃƒÂ³d.** e **EAN** (nÃƒÂ£o fica buraco ÃƒÂ  direita) |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.52** Ã‚Â· Emb. fechada Ã¢â€ â€™ GM completo + EAN mais largo |
| **Loja** | **Ã¢Å“â€¦** v8.54 |


### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ nome do cadastro sob a descriÃƒÂ§ÃƒÂ£o (15/07 Ã‚Â· **teste v8.51**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Embaixo da descriÃƒÂ§ÃƒÂ£o da NF aparece o nome SisVale (verde), sem coluna nova |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.51** Ã‚Â· linha casada Ã¢â€ â€™ texto verde sob a descriÃƒÂ§ÃƒÂ£o |
| **Loja** | **Ã¢Å“â€¦** v8.54 |


### Ã°Å¸Å¡â€˜ Entrada NF Ã¢â‚¬â€ overlay R$ 0 apagava P.venda do Mongo (15/07 Ã‚Â· **teste v8.49**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Alguns itens do XML ficam P.venda 0,00 / margem -100 (ex.: 3 primeiros da NF) |
| **Causa** | Overlay/PG com preÃƒÂ§o 0 sobrescrevia `ValorVenda` do Mongo no casamento |
| **Fix** | SÃƒÂ³ aplica overlay/API quando preÃƒÂ§o **> 0** |
| **Loja** | **Ã¢Å“â€¦** v8.49 6f0df25 |

### Ã°Å¸â€œÂ¦ Deploy loja **v8.49** Ã¢â‚¬â€ UX Entrada NF + P.venda (15/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | v8.45Ã¢â‚¬â€œv8.48 layout (passos, abas, embalagem, topbar) + fix P.venda v8.49 |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *enviarÃ¢â‚¬Â¦ produÃƒÂ§ÃƒÂ£o* + `99738595` |


### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ abas/meta na barra do topo (15/07 Ã‚Â· **teste v8.48**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Manual/XML/SEFAZ + Origem/NF/datas sobem para a faixa do Ã‚Â«Ã¢â€ Â ListaÃ‚Â» (uma linha a menos) |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.48** Ã‚Â· editor sem faixa solta abaixo do topo |
| **Loja** | Ã¢ÂÂ³ |


### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ colunas embalagem recolhÃƒÂ­veis (15/07 Ã‚Â· **teste v8.47**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | BotÃƒÂ£o **Embalagem** (Ã¢â€“Â¸) esconde Un/emb Ã‚Â· Qtd est. Ã‚Â· R$ pacote Ã‚Â· DescriÃƒÂ§ÃƒÂ£o ganha largura Ã‚Â· abre sozinho se Un/emb &gt; 1 |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.47** Ã‚Â· colunas somem Ã‚Â· descriÃƒÂ§ÃƒÂ£o mais larga Ã‚Â· seta abre de novo |
| **Loja** | Ã¢ÂÂ³ |


### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ abas + meta numa linha (15/07 Ã‚Â· **teste v8.46**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Manual/XML/SEFAZ + Origem/NF/Linhas/datas na **mesma faixa** Ã‚Â· passos 1Ã¢â‚¬â€œ8 + Anterior/PrÃƒÂ³ximo sem 2Ã‚Âª fileira |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.46** Ã‚Â· topo do editor: uma linha sÃƒÂ³ |
| **Loja** | Ã¢ÂÂ³ |


### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ UX rateio + passos 1Ã¢â‚¬â€œ8 numa linha (15/07 Ã‚Â· **teste v8.45**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Caixa amarela do rateio compacta na linha Ã‚Â«2 Ã‚Â· ProdutosÃ‚Â» Ã‚Â· chips 1Ã¢â‚¬â€œ8 numa fileira (sem 7/8 sozinhos) |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.45** Ã‚Â· etapa 2: faixa amarela fina Ã‚Â· passo a passo: 8 tags na mesma linha |
| **Loja** | Ã¢ÂÂ³ |


### Ã°Å¸â€œÂ¦ Deploy loja **v8.43** Ã¢â‚¬â€ rateio frete/ST + hidrata GM paralelo (14/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode subir pra produÃƒÂ§ÃƒÂ£o* + `99738595` |
| **Commit loja** | `6f46143` (+ `e873147` hidrata paralelo) Ã‚Â· backup `producao-backup-pre-v843-20260714` @ `b6e64db` |
| **Pacote** | Rateio acrÃƒÂ©scimos no custo Ã‚Â· hidrata GM em paralelo |
| **Validar** | Ctrl+F5 loja Ã‚Â· badge **v8.43** Ã‚Â· XML frete/ST Ã¢â€ â€™ marcar Ã¢â€ â€™ custo sobe |
| **Revert** | `git reset --hard b6e64db` na `producao` + push (ou checkout backup) |


### Ã¢Å¡Â¡ Entrada NF Ã¢â‚¬â€ rateio frete/ST no custo (14/07 Ã‚Â· **loja v8.43**)

| | |
| --- | --- |
| **O quÃƒÂª** | Checkbox Ã‚Â«Incluir no custo os acrÃƒÂ©scimos da notaÃ‚Â» Ã¢â‚¬â€ rateia frete+ST(+seguro/outras/IPIÃ¢Ë†â€™desc) no V. unit; marca/desmarca sem reler XML |
| **Arquivos** | `nfe_entrada_util.py` (totais ICMSTot) Ã‚Â· `entrada_nota.html` |
| **Validar** | Ctrl+F5 teste Ã‚Â· badge **v8.43** Ã‚Â· XML com frete/ST Ã¢â€ â€™ marcar Ã¢â€ â€™ custo sobe Ã‚Â· desmarcar Ã¢â€ â€™ volta Ã‚Â· nota limpa Ã¢â€ â€™ opÃƒÂ§ÃƒÂ£o desligada |

### Ã¢Å¡Â¡ Entrada NF Ã¢â‚¬â€ hidrata GM em paralelo (14/07 Ã‚Â· **loja v8.43**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | GM nÃƒÂ£o pula de 2 em 2 s Ã¢â‚¬â€ busca do cadastro em paralelo |
| **Loja** | **Ã¢Å“â€¦** v8.43 e873147 |

### Ã°Å¸Å¡â€˜ Entrada NF Ã¢â‚¬â€ apÃƒÂ³s Confirmar XML sem GM / P.venda 0 (14/07 Ã‚Â· **loja v8.41**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Modal mostra **Cadastro**, mas na grade: CÃƒÂ³d. sÃƒÂ³ do fornecedor, P. venda **0,00**, margem **-100** |
| **Causa** | `casar_produtos_mongo` sÃƒÂ³ ia `produto_id`+nome Ã¢â‚¬â€ sem preÃƒÂ§o/GM |
| **Fix** | Parse traz **preÃƒÂ§o + GM** (Mongo+overlay) Ã‚Â· grade mostra GM Ã‚Â· hidrata pÃƒÂ³s-Confirmar via API se faltar |
| **Loja** | **Ã¢Å“â€¦** `b6e64db` Ã‚Â· backup `producao-backup-pre-v841-20260714` |
| **Validar** | Ctrl+F5 loja Ã‚Â· badge **v8.41** Ã‚Â· Ler XML Ã¢â€ â€™ Confirmar Ã¢â€ â€™ GM + P. venda |

### Ã°Å¸Â©Â¹ Entrada NF Ã¢â‚¬â€ modal XML Ã‚Â«Sem vÃƒÂ­nculoÃ‚Â» com grade vazia (14/07 Ã‚Â· **loja v8.40**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | ApÃƒÂ³s entrar NF, ao ler o **mesmo XML** de novo tudo Ã‚Â«Sem vÃƒÂ­nculoÃ‚Â» e esquerda vazia |
| **Causa** | Badge = par **gradeÃ¢â€ â€XML**, nÃƒÂ£o casamento com cadastro. Grade vazia = todos Ã‚Â«Sem vÃƒÂ­nculoÃ‚Â» mesmo com produto casado |
| **Fix** | Badge **Cadastro** (verde) quando `produto_id` veio do parse Ã‚Â· texto da grade vazia Ã‚Â· Confirmar tambÃƒÂ©m lembra EAN embalagem |
| **Loja** | **Ã¢Å“â€¦** `a4d60fc` Ã‚Â· backup `producao-backup-pre-v840-20260714` |
| **Validar** | Ctrl+F5 loja Ã‚Â· badge **v8.40** Ã‚Â· Ler XML grade vazia Ã¢â€ â€™ **Cadastro** nos casados |

### Ã°Å¸Å¡â€˜ Hotfix loja **v8.39** Ã¢â‚¬â€ merge migraÃƒÂ§ÃƒÂµes 0050+0051 (14/07)

| Item | Detalhe |
| ---- | ------- |
| **Erro** | `Conflicting migrations Ã¢â‚¬Â¦ (0050_vendaagro_frete, 0051_produto_modelo)` |
| **Fix** | `0052_merge_0050_frete_0051_modelo` |
| **Loja** | **Ã¢Å“â€¦** `c119f51` v8.39 Ã‚Â· teste `bcfdc8a` |

### Ã°Å¸â€œÂ¦ Deploy loja **v8.38** Ã¢â‚¬â€ lote Zap restante + PIN ao abrir (14/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *pode mandar tudo para produÃƒÂ§ÃƒÂ£o* + `99738595` |
| **Commit loja** | `93cbc67` Ã‚Â· backup `producao-backup-pre-v838-20260714` @ `74b95fc` |
| **Pacote** | **#5** busca leve Ã‚Â· **#16** aviso CP (texto + fonte) Ã‚Â· **#18** frete 3 vias (+ mig `0050_vendaagro_frete`) Ã‚Â· **#1 #2 #9 #11 #13 #14** Ã‚Â· **PIN ao abrir** PDV |
| **#16 texto** | *Busca por texto ativa: os campos de data foram limpos* Ã‚Â· fonte `1rem` |
| **Validar loja** | Ctrl+F5 Ã‚Â· badge **v8.38** Ã‚Â· abrir PDV Ã¢â€ â€™ pede PIN Ã‚Â· CP busca texto Ã¢â€ â€™ aviso Ã‚Â· entrega+frete nas 3 vias Ã‚Â· Entrada NF #1/#2 Ã‚Â· Esc fecha overlay menu |
| **Revert** | `git reset --hard 74b95fc` na `producao` + push (ou checkout backup) |

### Ã°Å¸â€Â PDV pede PIN ao abrir (14/07 Ã‚Â· **teste v8.37** Ã¢â€ â€™ **loja v8.38**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Fechar/reabrir PDV **nÃƒÂ£o** mantÃƒÂ©m operador Ã¢â‚¬â€ tela de PIN na entrada |
| **Validar** | Ctrl+F5 Ã‚Â· digita PIN Ã‚Â· fecha aba / volta do BI Ã‚Â· abre PDV Ã¢â€ â€™ pede PIN de novo |
| **Loja** | **Ã¢Å“â€¦** v8.38 `93cbc67` |

### Ã°Å¸â€œÂ¦ Deploy loja **v8.34** Ã¢â‚¬â€ #6 + #10 + PIN nome (14/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | *manda produÃƒÂ§ÃƒÂ£o* + `99738595` |
| **Commit loja** | `74b95fc` Ã‚Â· backup `producao-backup-pre-v834-20260714` @ `1cdad18` |
| **Pacote** | **#6** PIN 1 query Ã‚Â· **#10** Cancelar cobranÃƒÂ§a (+ MP fiado caixa) Ã‚Â· **PIN nome** online/sessÃƒÂ£o Ã‚Â· **chip** entre Nova venda e PIN |
| **NÃƒÆ’O veio** | frete #18 Ã‚Â· resto do `teste` |
| **Validar loja** | Ctrl+F5 Ã‚Â· badge **v8.34** Ã‚Â· digitar PIN Ã¢â€ â€™ nome no chip Ã‚Â· fiado BAIXA Ã¢â€ â€™ **Cancelar cobranÃƒÂ§a** Ã‚Â· venda no nome certo apÃƒÂ³s trocar PIN no descanso |
| **Revert** | `git reset --hard 1cdad18` na `producao` + push (ou checkout backup) |

### Ã°Å¸â€˜Â PDV Ã¢â‚¬â€ nome do PIN entre Nova venda e PIN (14/07 Ã‚Â· **teste v8.34**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Chip `gm-sspin-operador-chip` no topbar do wizard (entre **Nova venda** e **PIN**) |
| **Validar** | Ctrl+F5 Ã‚Â· digitar PIN Ã‚Â· nome aparece ali |

### Ã°Å¸Å¡â€˜ PIN / nome trocado (14/07 Ã‚Â· **teste v8.33**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | (1) desbloqueio pelo cache local **sem** gravar sessÃƒÂ£o no servidor Ã¢â€ â€™ reload trazia o nome anterior; (2) servidor confiava no nome do Chrome antes da sessÃƒÂ£o; (3) state/rascunho PDV podia guardar nome velho |
| **Fix** | PIN **sempre online** Ã‚Â· no descanso limpa nome no Chrome Ã‚Â· venda usa **sessÃƒÂ£o/PIN** antes do Chrome Ã‚Â· PDV limpa `operadorPdv` ao trocar/sair |
| **Validar** | Render **teste** Ã‚Â· Ctrl+F5 Ã‚Â· PC1: Geraldo PIN Ã¢â€ â€™ descanso Ã¢â€ â€™ Renan PIN Ã¢â€ â€™ venda no **seu** nome; 2 pessoas no mesmo Chrome sem trocar PIN ainda grudam (esperado atÃƒÂ© o descanso) |
| **Loja** | **Ã¢Å“â€¦** v8.34 `74b95fc` |

### Ã°Å¸â€Â PIN / nome trocado (Renan Ã¢â€ â€ Geraldo etc.) Ã¢â‚¬â€ 14/07 Ã‚Â· **sÃƒÂ³ diagnÃƒÂ³stico**

| Item | Detalhe |
| ---- | ------- |
| **Relato** | AÃƒÂ§ÃƒÂ£o com PIN do Renan (e outros) gravando nome de **outra pessoa** (ex. Geraldo Hinnen) |
| **Status** | Ã¢â€ â€™ virado fix **v8.33** acima |
| **Causa A (mais forte)** | Nome no Chrome + sessÃƒÂ£o desatualizada (atalho local sem API) |
| **Causa B** | PIN **nÃƒÂ£o ÃƒÂ© ÃƒÂºnico** no banco (`.first()`) Ã¢â‚¬â€ ainda existe; cadastro novo barra duplicata |

### Ã°Å¸Â©Â¹ #6 PIN Ã¢â‚¬â€ 1 query no rÃƒÂ³tulo (14/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | JÃƒÂ¡ tinha helper `_perfil_usuario_por_pin` (191e70b) Ã‚Â· **faltava**: `operador_label_de_pin` ainda batia 2Ãƒâ€” no banco |
| **Fix** | Valida + rÃƒÂ³tulo na **mesma** leitura |
| **Validar** | Render **teste** Ã‚Â· digitar PIN no PDV/caixa Ã‚Â· deve responder rÃƒÂ¡pido |
| **Loja** | **Ã¢Å“â€¦** v8.34 `74b95fc` |

### Ã°Å¸â€œÂ¦ Deploy loja **v8.30** (14/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Commits** | `96df934` GM0024 PG+Mongo Ã‚Â· `1cdad18` baixa fiado filtro nome |
| **Antes** | loja **v8.26** `780d964` |
| **Validar** | Ctrl+F5 Ã‚Â· Fiado Ã¢â€ â€™ BAIXA Ã‚Â· `gm0024` Ã¢â€ â€™ 3 |
| **Dados** | fiado: sÃƒÂ³ filtro Ã¢â‚¬â€ **nÃƒÂ£o** apaga tÃƒÂ­tulos |

### Ã°Å¸Å¡â€˜ Fiado BAIXA Ã‚Â«Nenhum tÃƒÂ­tulo em abertoÃ‚Â» (14/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Qualquer BAIXA no `/fiado/` Ã¢â€ â€™ *Nenhum tÃƒÂ­tulo em aberto para quitar* |
| **Causa** | Lista agrupa por **nome**; cobranÃƒÂ§a/baixa filtrava sÃƒÂ³ `cliente_agro_pk` (muitos tÃƒÂ­tulos sem FK / modo default `titulo` no PDV) |
| **Fix** | Mesmo filtro da gestÃƒÂ£o (`_q_titulos_cliente_gestao`) Ã‚Â· PDV default modo `cliente` |
| **Dados** | **SÃƒÂ³ leitura/filtro** Ã¢â‚¬â€ nÃƒÂ£o apaga tÃƒÂ­tulo, baixa nem saldo |
| **Loja** | **Ã¢Å“â€¦** v8.30 `1cdad18` |

### Ã°Å¸Å¡â€˜ Hotfix #3 GM0024 loja sÃƒÂ³ 1 (14/07 Ã‚Â· **v8.28**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Loja v8.26: `GM0093`/`GM0090` OK Ã‚Â· `GM0024` Ã¢â€ â€™ sÃƒÂ³ `-1` (nome Ã¢â€ â€™ 3) |
| **Causa** | Com `agro_pg`, 1 hit no Postgres fazia **pular Mongo** Ã¢â‚¬â€ `-10/-15` sÃƒÂ³ no Mongo |
| **Fix** | FamÃƒÂ­lia GM **sempre** complementa/mescla Mongo |
| **Validar** | Ctrl+F5 Ã‚Â· `gm0024` Ã¢â€ â€™ 3 magnus |
| **Loja** | Ã¢ÂÂ³ aguarda pacote maior (Renan 14/07: nÃƒÂ£o sobe sozinho) |

### Ã°Å¸â€œÂ¦ Deploy loja **v8.26** Ã¢â‚¬â€ busca GM CodigoNFe (14/07 Ã‚Â· Renan autorizou)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Motor Mongo prefixa `CodigoNFe`/`Codigo` (nÃƒÂ£o sÃƒÂ³ `index_codigos`) Ã‚Â· overlay famÃƒÂ­lia Ã‚Â· index apÃƒÂ³s overlay |
| **Validado** | Local: `GM0093` + mais 3 famÃƒÂ­lias OK |
| **Seed local** | `seed-gm0024-*` Teste Local Ã¢â‚¬â€ **apagado** (`--limpar`); loja nunca teve esses nomes |
| **Antes loja** | **v8.25** |
| **NÃƒÆ’O veio** | resto do `teste` (frete, lote ZapÃ¢â‚¬Â¦) |

### Ã°Å¸Å¡â€˜ #3 busca GM Ã¢â‚¬â€ causa real + teste local (14/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `gm0024`/`GM0093` Ã¢â€ â€™ 1; nome Ã¢â€ â€™ 3 |
| **Causa 1** | overlay / Mongo exact (hotfix anterior) |
| **Causa 2 (real quirera)** | Mongo: irmÃƒÂ£os **sem `index_codigos`**; motor sÃƒÂ³ buscava ÃƒÂ­ndice Ã¢â€ â€™ sÃƒÂ³ `GM0093-25`. `CodigoNFe` tinha os 3 |
| **Fix** | Prefixo em `CodigoNFe`/`Codigo` no motor + misto sem early-return no index Ã‚Â· Id=`_id` se faltar |
| **Validar local** | Ctrl+F5 Ã‚Â· `GM0093` Ã¢â€ â€™ quirera 1kg + 5kg + 25kg (Ã¢â€°Â¥3) Ã‚Â· nome Ã¢â‚¬Å“quirera finaÃ¢â‚¬Â igual |
| **Loja** | **Ã¢Å“â€¦** push `780d964` / teste `0033d39` Ã‚Â· badge **v8.26** Ã‚Â· Ctrl+F5 |

### Ã°Å¸Å¡â€˜ Hotfix #3 GM maiÃƒÂºsc/minÃƒÂºsc + famÃƒÂ­lia (14/07 Ã‚Â· **v8.25**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Loja v8.23: `gm0024` Ã¢â€ â€™ 1 item; nome Ã¢â€ â€™ 3 |
| **Causa** | No Postgres `istartswith` ÃƒÂ© **case-sensitive** (`gm` Ã¢â€°Â  `GM`) + famÃƒÂ­lia nÃƒÂ£o expandia |
| **Fix** | Prefixo CI (`iregex`/`GM`+`gm`) + busca explÃƒÂ­cita famÃƒÂ­lia `GM0024` / `GM0024-*` Ã‚Â· overlay igual |
| **Validar** | Ctrl+F5 loja Ã‚Â· `gm0024` Ã¢â€ â€™ **3** Ã‚Â· PDV igual |
| **Loja** | sobe junto (hotfx do pacote #3 jÃƒÂ¡ autorizado) |

### Ã°Å¸Å¡â€˜ Hotfix migraÃƒÂ§ÃƒÂ£o loja (14/07) Ã¢â‚¬â€ deploy v8.22 falhou no Render

| Item | Detalhe |
| ---- | ------- |
| **Erro** | `0051_produto_modelo` dependia de `0050_vendaagro_frete` (frete) Ã¢â‚¬â€ **nÃƒÂ£o** estava na loja |
| **Fix** | `0051` passa a depender de `0049` (irmÃƒÂ£ da 0050) |
| **Loja** | push hotfix Ã¢â€ â€™ badge **v8.23** |

### Ã°Å¸â€œÂ¦ Deploy loja **v8.22** Ã¢â‚¬â€ pacote #3 + #4 (14/07 Ã‚Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Pacote** | **#4** modelo persiste + **#3** prefixo GM (famÃƒÂ­lia) |
| **Commits loja** | `2c44d45` (#4) Ã‚Â· `a285d52` (#3) |
| **Antes** | loja **v8.15** `812226b` |
| **Backup** | `producao-backup-pre-v822-20260714` Ã‚Â· anterior `pre-v815` / `pre-v814` |
| **NÃƒÆ’O veio** | #5 #6 #16 #18 frete Ã‚Â· lote Zap restante Ã¢â‚¬â€ continua sÃƒÂ³ no teste |
| **Validar loja** | Ctrl+F5 Ã‚Â· `GM0024` Ã¢â€ â€™ 3 itens Ã‚Â· Modelo salvar/reabrir |
| **Revert** | `git reset --hard 812226b` na `producao` + push (ou checkout backup) |

### Ã°Å¸Ââ€º Hotfix #3 busca prefixo GM (teste **v8.22** Ã‚Â· **loja v8.22**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `GM0024` Ã¢â€ â€™ 1 resultado; pelo nome Ã¢â€ â€™ 3 (GM0024-1/10/15). Cadastro e PDV |
| **Causa** | Filtro GM do motor lia sÃƒÂ³ `Produto.codigo_nfe` e descartava irmÃƒÂ£os cujo GM estÃƒÂ¡ no **overlay** |
| **Fix** | Filtrar GM **depois** do overlay Ã‚Â· index/prefixo no motor Ã‚Â· PDV scanner `GM0024` (sem hÃƒÂ­fen) usa famÃƒÂ­lia |
| **Validar** | Ctrl+F5 loja Ã‚Â· buscar `GM0024` Ã¢â€ â€™ 3 itens Ã‚Â· PDV igual |
| **Loja** | **Ã¢Å“â€¦** v8.22 |

### Ã°Å¸Ââ€º Hotfix #4 modelo persiste (teste **v8.19+** Ã‚Â· **loja v8.22**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Campo **Modelo** no cadastro Ã¢â‚¬Å“salvavaÃ¢â‚¬Â e sumia ao reabrir |
| **Causa** | Modelo sÃƒÂ³ no JSON do overlay; resposta/lista/Mongo sem coluna; detalhe Mongo sobrescrevia com Modelo vazio |
| **Fix** | Coluna `Produto.modelo` + sync no salvar Ã‚Â· espelho Mongo `Modelo`/`NomeModelo` Ã‚Â· lista/detalhe/UI sempre devolvem o campo |
| **MigraÃƒÂ§ÃƒÂ£o** | `0051_produto_modelo` (copia do overlay Ã¢â€ â€™ coluna) |
| **Validar** | Ctrl+F5 loja Ã‚Â· editar modelo Ã‚Â· Salvar Ã‚Â· reabrir |
| **Loja** | **Ã¢Å“â€¦** v8.22 |

### Ã°Å¸â€â€ž Handoff Renan 14/07 Ã¢â‚¬â€ histÃƒÂ³rico (versÃƒÂ£o antiga)

> **Atualizado 16/07:** status vivo estÃƒÂ¡ no **CHECKLIST ÃƒÅ¡NICO** no topo do CHECKPOINT (Zap # + FL + P).

| Ambiente (14/07) | VersÃƒÂ£o | Notas |
| ---------------- | ------ | ----- |
| **Loja** | era v8.22 Ã¢â€ â€™ hoje **v8.69** | ver CHECKLIST ÃƒÅ¡NICO no topo |
| **Teste** | hoje **v8.97** | ver CHECKLIST ÃƒÅ¡NICO no topo |

**PrÃƒÂ³ximo:** fecha Ã¢â€ â€™ **#12+#17** Ã‚Â· aberto Zap Ã¢â€ â€™ **#7**.

### Ã¢Å“â€¦ #17 Busca CP inteligente Ã¢â‚¬â€ P0+P1+P2 (16/07 Ã‚Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **P0** | Valor (bruto/pago/restante; inteiro ou `1.500,00` / R$) + data digitada (venc./comp./pagto) |
| **P1** | Boleto (cÃƒÂ³digo/linha) + nÃ‚Âº documento sÃƒÂ³-dÃƒÂ­gitos |
| **P2** | Parcela (`2/6`, Ã‚Â«parcela 2Ã‚Â») + CPF/CNPJ com/sem mÃƒÂ¡scara |
| **Causa bug valor** | QS Postgres `_aplicar_texto_qs` nÃƒÂ£o buscava valor Ã¢â‚¬â€ sÃƒÂ³ texto |
| **Arquivos** | `lancamentos_financeiro_pg_util.py` Ã‚Â· `mongo_financeiro_util.py` Ã‚Â· help Ã‚Â§10 Ã‚Â· placeholder CP |
| **Validar** | Ã¢Å“â€¦ Renan testou Ã‚Â· valor + parcela OK |
| **Loja** | Ã°Å¸â€œÂ¦ **pronto produÃƒÂ§ÃƒÂ£o** Ã¢â‚¬â€ frase + senha (pacote do fecha c/ #12) |

### Ã°Å¸Ââ€º Pacote performance + UX (teste **v8.17**)

| Item | Detalhe |
| ---- | ------- |
| **#5** | Busca do cadastro e da Entrada NF mais leve no BCA |
| **#6** | ValidaÃƒÂ§ÃƒÂ£o de PIN com menos consultas ao banco |
| **#16** | Aviso da limpeza de datas no CP movido para o local pedido |
| **Arquivos** | `views.py` Ã‚Â· `caixa_util.py` Ã‚Â· `entrada_nota.html` Ã‚Â· `lancamentos_contas_pagar_teste.html` |
| **Validar** | Ctrl+F5 teste Ã‚Â· busca cadastro Ã‚Â· busca Entrada NF Ã‚Â· PIN Ã‚Â· posiÃƒÂ§ÃƒÂ£o do aviso no CP |
| **Loja** | **Ã¢ÂÂ³** sÃƒÂ³ com frase + senha |

### Ã°Å¸Ââ€º Hotfix #18 frete nas 3 vias e cupom fiscal (teste **v8.16**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Na `via do cliente` do fluxo de entrega a taxa nÃƒÂ£o saÃƒÂ­a no primeiro cupom, mas reaparecia na reimpressÃƒÂ£o |
| **Fix** | frete incluÃƒÂ­do no payload das 3 vias de entrega e fallback explÃƒÂ­cito no render do cupom 80mm / NFC-e |
| **Arquivos** | `pdv_wizard.js` Ã‚Â· `venda_cupom_80mm.js` Ã‚Â· `nfce_cupom_util.py` |
| **Validar** | Ctrl+F5 teste Ã‚Â· fluxo entrega com 3 vias Ã‚Â· via do cliente Ã‚Â· cupom fiscal |
| **Loja** | **Ã¢ÂÂ³** sÃƒÂ³ com frase + senha |

### Ã°Å¸Ââ€º Hotfix #3 custo via BCA (teste **v8.15**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cadastro e Entrada NF via busca BCA continuavam mostrando custo **R$ 0,00** mesmo com custo salvo no produto |
| **Causa** | `/api/buscar/` nÃƒÂ£o reaproveitava o custo salvo em `Produto.custo` quando o documento da busca vinha zerado |
| **Fix** | fallback de custo Postgres no `api_buscar_produtos` para fluxos `compras=1` / cadastro / Entrada NF |
| **Arquivo** | `produtos/views.py` |
| **Validar** | Ctrl+F5 teste Ã‚Â· cadastro lista custo Ã‚Â· busca da Entrada NF com o mesmo produto |
| **Loja** | **Ã¢ÂÂ³** sÃƒÂ³ com frase + senha |

### Ã°Å¸Ââ€º PÃƒÂ³s-validaÃƒÂ§ÃƒÂ£o bugs loja Zap 12/07 (teste **v8.14**) Ã¢â‚¬â€ ajustes amarelo/vermelho

| Item | Detalhe |
| ---- | ------- |
| **#3** | Cadastro lista mostra **preÃƒÂ§o de custo** com fallback mais robusto |
| **#4** | Busca cadastro passa a considerar **modelo** (`cadastro_extras.modelo`) |
| **#15** | **CÃƒÂ³digo interno** novo ocupa a **menor lacuna livre** a partir de **4010** |
| **#16** | **CP** continua limpando datas na busca e agora mostra **aviso visual** |
| **#18** | **Taxa de entrega** entra tambÃƒÂ©m na **via do cliente** do cupom |
| **Validar** | Ctrl+F5 teste Ã‚Â· #3 Ã‚Â· #4 Ã‚Â· #15 Ã‚Â· #16 Ã‚Â· #18 |
| **Loja** | **Ã¢ÂÂ³** sÃƒÂ³ com frase + senha |

### Ã°Å¸Ââ€º Lote bugs loja Zap 12/07 (teste **v8.12**) Ã¢â‚¬â€ pacote checklist #

| # | Item | Status |
| - | ---- | ------ |
| **1** | Entrada NF etapa 7 Ã¢â‚¬â€ valor recarregava a cada tecla | Ã¢Å“â€¦ nÃƒÂ£o rebuilda preview na validaÃƒÂ§ÃƒÂ£o |
| **2** | Entrada NF etapa 2 Ã¢â‚¬â€ busca barras | Ã¢Å“â€¦ nÃƒÂ£o bloqueia modo scanner |
| **3** | Cadastro lista Ã¢â‚¬â€ custo | Ã¢Å“â€¦ custos compra na row Mongo + overlay |
| **4** | Cadastro Ã¢â‚¬â€ campo modelo nÃƒÂ£o salvava | Ã¢Å“â€¦ body + `cadastro_extras.modelo` |
| **8** | Produto novo sumia da lista | Ã¢Å“â€¦ merge coloca no topo |
| **9** | Fiado MP Ã¢â€ â€™ forma genÃƒÂ©rica no caixa | Ã¢Å“â€¦ `[MP_POINT]` + split conferÃƒÂªncia |
| **10** | Fiado sem cancelar/voltar | Ã¢Å“â€¦ botÃƒÂ£o **Cancelar cobranÃƒÂ§a** |
| **11** | Menu caixa/vendas ÃƒÂ s vezes nÃƒÂ£o fecha | Ã¢Å“â€¦ Esc no iframe fecha overlay |
| **13** | XML boleto Ã¢â€ â€™ **Boleto BancÃƒÂ¡rio CN** | Ã¢Å“â€¦ (Renan confirmou CN) |
| **14** | Add produto perde barras/lote | Ã¢Å“â€¦ invalidate soft (FL-026) |
| **15** | CÃƒÂ³digo interno 9000+ Ã¢â€ â€™ 4xxx | Ã¢Å“â€¦ teto auto 5999 (FL-025) |
| **16** | CP busca limpa datas | Ã¢Å“â€¦ (FL-023) |
| **18** | Taxa entrega no cupom/NFC-e | Ã¢Å“â€¦ `VendaAgro.frete` + vFrete + linha cupom |
| **5Ã¢â‚¬â€œ7, 12, 17** | LentidÃƒÂ£o / PIN / impressÃƒÂ£o / devoluÃƒÂ§ÃƒÂ£o parcial / busca CP inteligente | Ã¢ÂÂ³ depois |

| Item | Detalhe |
| ---- | ------- |
| **Validar** | Ctrl+F5 teste Ã‚Â· nÃƒÂºmeros #1Ã¢â‚¬â€œ4, #8Ã¢â‚¬â€œ11, #13Ã¢â‚¬â€œ16, #18 |
| **MigraÃƒÂ§ÃƒÂ£o** | `0050_vendaagro_frete` (Render aplica no deploy) |
| **Loja** | **Ã¢ÂÂ³** sÃƒÂ³ com frase + senha |

### Entrada NF Ã¢â‚¬â€ BCA fix motor padrÃƒÂ£o (11/07 Ã‚Â· **teste v8.10**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Entrada NF usava cache local PDV + `entrada_nfe=1` Ã¢â‚¬â€ lista diferente do cadastro |
| **Fix** | **SÃƒÂ³ servidor** BCA Ã‚Â· `/api/buscar/?compras=1` Ã‚Â· sem merge cache |
| **Validar** | Ctrl+F5 teste Ã‚Â· `milho` / `#prova` = mesma ordem que cadastro |
| **Loja** | **Ã¢ÂÂ³** apÃƒÂ³s OK |

### Ã°Å¸Å¡â‚¬ Deploy loja **v8.07** Ã¢â‚¬â€ BCA + pacote teste (11/07 Ã¢â‚¬â€ Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Merge `teste` Ã¢â€ â€™ `producao`: **BCA** PDV + Cadastro + Consulta Ã‚Â· cadastro estoque/vitrines Ã‚Â· fiado Ã‚Â· indicadores |
| **Validado loja** | Renan OK PDV + cadastro + consulta |
| **Revert total BCA** | **`producao-backup-pre-bca-20260711`** Ã‚Â· **`rollback/pre-bca-v766`** @ **`e260c48`** (v7.66) |
| **Pendente produto** | Desativar `/produtos/gestao/` se cadastro OK |

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Card **Faturamento vendas (PDV)** Ã‚Â· **1 arquivo** `indicadores_gerencial_pg.py` |
| **Sem** | cadastro Ã‚Â· busca Ã‚Â· fix despesas CP (af328ba) |
| **Commit** | `e260c48` |

### Ã°Å¸Ââ€º FIX Ã¢â‚¬â€ Indicadores faturamento PDV Ã‚Â«Ã¢â‚¬â€Ã‚Â» (11/07 Ã‚Â· **v8.04 teste**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Card **Faturamento vendas (PDV)** = **R$ Ã¢â‚¬â€** (ex. mÃƒÂªs ant. jun/26) Ã‚Â· DRE e demais KPIs OK |
| **Causa** | v7.65 tinha card no HTML mas **faltava backend**; planilha histÃƒÂ³rica sem fallback |
| **Fix** | `_faturamento_pdv_periodo` Ã¢â€ â€™ mesma funÃƒÂ§ÃƒÂ£o do BI (`_dashboard_mongo_vendas_serie`) |
| **Loja** | **Ã¢Å“â€¦ v7.66** |

### Ã¢Å“â€¦ Deploy loja **v7.65** Ã¢â‚¬â€ Indicadores planilha + Estoque giro (11/07 Ã¢â‚¬â€ Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Tipo+Grupo planilha CP Ã‚Â· aba Estoque & giro Ã‚Â· **sÃƒÂ³ leitura BI** |
| **NÃƒÂ£o mexe** | PDV Ã‚Â· CP Ã‚Â· caixa Ã‚Â· fiado |
| **Pode mudar** | NÃƒÂºmeros Indicadores + Resumo gerencial (classificaÃƒÂ§ÃƒÂ£o) |

### Ã¢Å“â€¦ Indicadores Ã¢â‚¬â€ Tipo + Grupo + Estoque e giro (11/07 Ã‚Â· **v7.98+ teste**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Planilha oficial CP (Tipo/Grupo) na tabela despesas + aba **Estoque e giro** (parado 90d + top 5 giro 30d) |
| **URL aba** | `/financeiro/dashboard-gerencial/?aba=estoque` |
| **Validar** | Ctrl+F5 teste Ã¢â€ â€™ Indicadores Ã¢â€ â€™ aba Estoque e giro Ã‚Â· despesas agrupadas TipoÃ¢â€ â€™Grupo |
| **Fix v7.99** | Aba abre na hora + Ã‚Â«CarregandoÃ¢â‚¬Â¦Ã‚Â» Ã‚Â· dados via API (consulta pesada nÃƒÂ£o trava a pÃƒÂ¡gina) |
| **Fix 11/07** | Tabela despesas alinhada ÃƒÂ  **CP** (competÃƒÂªncia Ã‚Â· bruto Ã‚Â· todas empresas) Ã¢â‚¬â€ antes filtrava sÃƒÂ³ 1 empresa |

### Ã¢Å“â€¦ Deploy loja **v7.63** Ã¢â‚¬â€ Unificar planos despesa CP (11/07 Ã¢â‚¬â€ Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Mapa + URLs staff Ã‚Â· migrate **0049** Ã‚Â· sÃƒÂ³ renomeia plano_conta |
| **Pacote** | `pacote/planos-cp-producao` Ã¢â€ â€™ `producao` `d901763` (**sem** cadastro/busca) |
| **Apply PG loja** | **Ã¢Å“â€¦ 11/07** lote #1 Ã‚Â· **2483** tÃƒÂ­tulos Ã‚Â· pÃƒÂ³s-sim **0** a renomear Ã‚Â· **3286 Ã‚Â· R$ 1.144.537,97** Ã‚Â· fora mapa **0** |
| **Status** | **Ã¢Å“â€¦ CONCLUÃƒÂDO** loja |

### Ã¢Å“â€¦ Deploy loja **v7.61** Ã¢â‚¬â€ Fiado baixa parcial (11/07 Ã¢â‚¬â€ Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Baixa parcial Ã‚Â· popup Total/Parcial Ã‚Â· FIFO Ã‚Â· PDV pagamento |
| **Pacote** | `pacote/fiado-baixa-parcial-producao` Ã¢â€ â€™ `producao` `304a5fa` |
| **PÃƒÂ³s-deploy** | Render ~2Ã¢â‚¬â€œ5 min Ã‚Â· **Ctrl+F5** PDVs Ã‚Â· caixa aberto |

### Fiado Ã¢â‚¬â€ baixa parcial no PDV (11/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | **Ã¢Å“â€¦ loja v7.61** (cÃƒÂ³digo) Ã‚Â· docs checkpoint **v7.62** |
| **Teste** | Validado Ã‚Â· popup Total (Enter) / Parcial (P) |
| **Fora** | FL-029 crÃƒÂ©dito Ã‚Â· FL-052 NFC-e Ã‚Â· FL-019 recibo |

### Cadastro ERP Ã¢â‚¬â€ estoque + vitrines (11/07)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Trazer para `/produtos/cadastro-erp/` o que sÃƒÂ³ existia em `/produtos/gestao/` (coluna estoque + ajuste + vitrines marca/cat/forn) |
| **Feito teste v7.69Ã¢â‚¬â€œv7.76** | Busca Postgres (`catalogo_agro.buscar`) Ã‚Â· GM prefixo Ã‚Â· custo lista Ã‚Â· facetas marcas AÃ¢â‚¬â€œZ |
| **Feito teste v7.82** | **API ÃƒÂºnica** `/api/buscar/` Ã¢â‚¬â€ PDV padrÃƒÂ£o Ã‚Â· cadastro `?contexto=cadastro&compras=1` (+ custo/saldo/filtros). Lista AÃ¢â‚¬â€œZ continua `api_produtos_cadastro` sem `q` |
| **ValidaÃƒÂ§ÃƒÂ£o Renan 11/07** | Teste 1+2 busca unificada OK Ã‚Â· canÃƒÂ¡rio `[TESTE]` confirmou cadastro + wizard (cache local) |
| **Feito v7.69Ã¢â‚¬â€œv8.08** | **BCA** Ã¢â‚¬â€ PDV + cadastro + Consulta + **Entrada NF** |
| **ValidaÃƒÂ§ÃƒÂ£o Renan** | **Ã¢Å“â€¦ loja** PDV/cadastro/consulta + Entrada NF |

### FIX Ã¢â‚¬â€ Indicadores nÃƒÂºmeros vs BI (09/07 Ã‚Â· v7.61)

| Item | Detalhe |
| ---- | ------- |
| **Receita** | Card **Faturamento PDV** = mesmo dado do BI; **Receita financeira (DRE)** = lanÃƒÂ§amentos |
| **Despesas** | ClassificaÃƒÂ§ÃƒÂ£o fixa/variÃƒÂ¡vel/outra corrigida; hint Ã‚Â«role a tabelaÃ‚Â» para ver todos os grupos |
| **Loja** | **Ã¢Å“â€¦ v8.07** Ã¢â‚¬â€ Renan OK |

### Ã¢Å“â€¦ Deploy loja **v7.60** Ã¢â‚¬â€ Financeiro gerencial SisVale (09/07 Ã¢â‚¬â€ Renan senha OK Ã‚Â· loja aberta)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Resumo + Indicadores + GrÃƒÂ¡fico gastos Ã¢â‚¬â€ **sÃƒÂ³ leitura** SisVale; **nÃƒÂ£o** para uso gerencial ainda Ã¢â‚¬â€ **teste com dado real** |
| **Pacote** | v7.42Ã¢â‚¬â€œv7.60: financeiro gerencial + fixes Compras mÃƒÂ©tricas + barra lateral BI |
| **Risco operaÃƒÂ§ÃƒÂ£o** | PDV/CP/caixa **inalterados**; Compras/BI com fixes jÃƒÂ¡ validados no teste |
| **Como** | Merge `teste` Ã¢â€ â€™ `producao` (09/07) |
| **PÃƒÂ³s-deploy** | Render ~2Ã¢â‚¬â€œ5 min Ã‚Â· **Ctrl+F5** Ã‚Â· testar `/financeiro/dashboard-gerencial/` e Resumo |
| **Uso** | **NÃƒÂ£o** decidir gestÃƒÂ£o por essas telas atÃƒÂ© Renan fechar validaÃƒÂ§ÃƒÂ£o |

### UX Ã¢â‚¬â€ Financeiro gerencial 100 % SisVale (09/07 Ã‚Â· v7.60)

| Item | Detalhe |
| ---- | ------- |
| **Resumo** | `/financeiro/resumo-gerencial/` Ã¢â‚¬â€ saiu **Fonte Mongo/ERP**, **Filtro contas** e textos Postgres/Mongo; sÃƒÂ³ data base + valor |
| **API** | `/api/financeiro/resumo-operacional` e `gap-equilibrio` Ã¢â‚¬â€ sÃƒÂ³ lanÃƒÂ§amentos SisVale (`fonte=postgres`) |
| **GrÃƒÂ¡fico gastos** | `/financeiro/grafico-gastos/` Ã¢â‚¬â€ agregaÃƒÂ§ÃƒÂ£o sÃƒÂ³ SisVale; ajuda sem Mongo |
| **Menu** | Links Indicadores/Resumo sem Ã‚Â«PostgresÃ‚Â» no tooltip |
| **Validar** | Ctrl+F5 teste Ã¢â€ â€™ Resumo + Indicadores + GrÃƒÂ¡fico gastos Ã¢â‚¬â€ **Ctrl+F** nÃƒÂ£o deve achar ERP/Mongo/DtoLancamento |

### UX Ã¢â‚¬â€ Indicadores sÃƒÂ³ SisVale (09/07 Ã‚Â· v7.58)

| Item | Detalhe |
| ---- | ------- |
| **Tela** | `/financeiro/dashboard-gerencial/` Ã¢â‚¬â€ removido **Filtro contas** (ERP/.env); textos sem Mongo/Postgres/ERP |
| **Dados** | Sempre lanÃƒÂ§amentos SisVale; DRE usa sÃƒÂ³ contas de **operaÃƒÂ§ÃƒÂ£o da loja** (fixo no cÃƒÂ³digo) |

### Ã°Å¸Ââ€º FIX Ã¢â‚¬â€ Indicadores financeiros 500 (09/07 Ã‚Â· **v7.54**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `/financeiro/dashboard-gerencial/` Ã¢â€ â€™ Server Error 500 |
| **Causa** | `titulos_financeiro_montar_qs` passou a exigir `despesa`; DRE/indicadores buscam receita+despesa sem filtro |
| **Fix** | `despesa` opcional em `titulos_financeiro_montar_qs` Ã‚Â· template **Despesas por categoria** com grupos fixa/variÃƒÂ¡vel |
| **Validar** | Ctrl+F5 teste Ã¢â€ â€™ Indicadores Ã¢â€ â€™ KPIs + tabela categorias + grÃƒÂ¡fico |

### Ã°Å¸Ââ€º FIX Ã¢â‚¬â€ Indicadores 500 + despesas fixa/variÃƒÂ¡vel (09/07 Ã‚Â· **v7.54Ã¢â‚¬â€œv7.55**)

| Item | Detalhe |
| ---- | ------- |
| **500** | `titulos_financeiro_montar_qs` exigia `despesa`; DRE Indicadores lÃƒÂª receita+despesa Ã¢â€ â€™ `TypeError` |
| **Fix** | `despesa` opcional no PG Ã‚Â· rÃƒÂ³tulos **despesas por categoria** Ã‚Â· cards/tabela **fixa / variÃƒÂ¡vel / outras** + filtros |
| **Validar** | Ctrl+F5 `/financeiro/dashboard-gerencial/` |

### Ã¢Å“â€¦ Indicadores financeiros Ã¢â‚¬â€ tela nova PG (09/07 Ã‚Â· Renan Ã‚Â· **v7.53**)

| Item | Detalhe |
| ---- | ------- |
| **URL** | `/financeiro/dashboard-gerencial/` (`dashboard_financeiro_completo`) Ã¢â‚¬â€ **substituiu** tela Mongo |
| **Fonte** | 100 % **Postgres** (`TituloFinanceiroAgro`) Ã¢â‚¬â€ mesma base Resumo gerencial + CP |
| **KPIs** | Receita, margens, equilÃƒÂ­brio, caixa (pagamentoÃ‚Â·realizado), DRE, ref. mÃƒÂ©dia 60d |
| **Novo** | **Despesas por categoria** Ã¢â‚¬â€ fixa/variÃƒÂ¡vel/outra Ã‚Â· 3 meses ou 3 semanas Ã‚Â· tabela + grÃƒÂ¡fico top 10 Ã‚Â· ÃŽâ€% Ã‚Â· clique Ã¢â€ â€™ CP |
| **Arquivos** | `indicadores_gerencial.html` Ã‚Â· `indicadores_gerencial_pg.py` Ã‚Â· `gastos_variacao_pg.py` Ã‚Â· `financeiro/views.py` |
| **GrÃƒÂ¡fico gastos** | `/financeiro/grafico-gastos/` **mantido** (anÃƒÂ¡lise sÃƒÂ©rie longa) |
| **Validar** | Ctrl+F5 teste Ã¢â€ â€™ Indicadores Ã¢â€ â€™ KPIs batem Resumo gerencial Ã‚Â· trocar 3 meses/semanas |

### Ã°Å¸Ââ€º FIX Ã¢â‚¬â€ Compras nÃƒÂºmeros zerados no teste (08/07 Ã‚Â· Renan Ã‚Â· v7.51)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | SugestÃƒÂ£o Ã‚Â«Ã¢â‚¬â€Ã‚Â», vendas 0, custo R$ 0 na busca (ex. milho) |
| **Causa** | F5 no teste lia sÃƒÂ³ Postgres (poucas vendas); API zerava mÃƒÂ©dia/custo do catÃƒÂ¡logo |
| **Fix** | F5 hÃƒÂ­brido PG+Mongo; busca nÃƒÂ£o apaga valor bom com zero da API |
| **Barra lateral** | **NÃƒÂ£o mexido** (Renan: nÃƒÂ£o tocar) |
| **Validar** | Ctrl+F5 Compras Ã¢â€ â€™ buscar milho Ã¢â€ â€™ mÃƒÂ©dia/sugestÃƒÂ£o/custo |

### Ã°Å¸Ââ€º FIX Ã¢â‚¬â€ barra lateral sumiu (08/07 Ã‚Â· Renan Ã‚Â· v7.47)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Faixa escura ÃƒÂ  esquerda (PDV Ã‚Â· Dashboard Ã‚Â· +) **sumiu no teste**; produÃƒÂ§ÃƒÂ£o OK |
| **Causa** | `isPaginaAuxiliarSemShell` no script de limite de abas Ã¢â‚¬â€ `mountShell` no outro IIFE Ã¢â€ â€™ **ReferenceError** |
| **Fix** | Helpers em `window.__agroIsPaginaAuxiliarSemShell` / `__agroPathLookLikeFolhaPlanilha` (escopo compartilhado) |
| **Validar** | Ctrl+F5 na home teste Ã¢â€ â€™ barra igual print produÃƒÂ§ÃƒÂ£o |
| **ProduÃƒÂ§ÃƒÂ£o** | **NÃƒÂ£o mexido** |

### Ã°Å¸Ââ€º REVERT Ã¢â‚¬â€ barra lateral BI (08/07 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Erro** | Hotfixes v7.43Ã¢â‚¬â€œv7.45 no shell quebraram a **faixa de guias** da home |
| **AÃƒÂ§ÃƒÂ£o** | **Revertido** `_agro_open_external.html` + BI + `agro_dual_window.js` ao estado **prÃƒÂ©-v7.42** |
| **Mantido** | SÃƒÂ³ isenÃƒÂ§ÃƒÂ£o folha Compras (`embed=1` / planilha) Ã¢â‚¬â€ **nÃƒÂ£o** mexe na home |
| **Mantido** | Fix mÃƒÂ©tricas Compras (v7.44) Ã¢â‚¬â€ F5 nÃƒÂ£o zera catÃƒÂ¡logo |
| **Validar** | Ctrl+F5 na home Ã¢â€ â€™ faixa verde **PDV Ã‚Â· Dashboard Ã‚Â· +** ÃƒÂ  esquerda |

### Ã°Å¸Ââ€º HOTFIX Ã¢â‚¬â€ barra lateral nÃƒÂ£o monta (08/07 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | BI/Compras abrem mas **sem faixa verde** ÃƒÂ  esquerda (PDV Ã‚Â· Dashboard Ã‚Â· +) |
| **Causa** | `mountShell` quebrava na fase de rota e apagava o DOM; boot sÃƒÂ³ no `DOMContentLoaded` |
| **Fix** | DOM separado da rota; boot com retry + `load`/`pageshow`; gestÃƒÂ£o no `localStorage` ativa shell |
| **Arquivo** | `_agro_open_external.html` |

### Ã°Å¸Ââ€º HOTFIX Ã¢â‚¬â€ Compras mÃƒÂ©tricas zeradas + barra lateral (08/07 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Telas abrem mas sem guias laterais; produtos sem vendas/custo; planilha com mÃƒÂ©dia 0 |
| **Causa mÃƒÂ©tricas** | F5 (`aplicarMetricasCompraNaBase`) **zerava** todo produto fora do payload PG (sÃƒÂ³ ~6 com venda no teste) |
| **Fix mÃƒÂ©tricas** | F5 sÃƒÂ³ atualiza quem veio na API; busca `?compras=1` puxa mÃƒÂ©dia PG por produto |
| **Fix shell** | `mountShell` com retry; `__agroInAppAddTab` tenta remontar antes de `location.assign` |
| **Arquivos** | `compras.html` Ã‚Â· `compras_metricas_util.py` Ã‚Â· `views.py` Ã‚Â· `_agro_open_external.html` Ã‚Â· `dashboard_gerencial.html` |

### Ã°Å¸Ââ€º HOTFIX Ã¢â‚¬â€ barra lateral / navegaÃƒÂ§ÃƒÂ£o (08/07 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Sumiu barra lateral; F10/Compras/Produtos nÃƒÂ£o abrem Ã¢â‚¬â€ fica no BI |
| **Causa** | Patch folha Compras deixou shell lateral meio montado; `__agroInAppAddTab` falhava sem fallback |
| **Fix** | Remonta shell se incompleto; fallback `location.assign`; folha embed nÃƒÂ£o bloqueia shell do BI |
| **Arquivos** | `_agro_open_external.html` Ã‚Â· `agro_dual_window.js` Ã‚Â· `dashboard_gerencial.html` |

### Ã¢Å“â€¦ Compras Ã¢â‚¬â€ UX + NF Agro + custo (08/07 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Tudo sugerido: rÃƒÂ³tulos NF Agro (sem Ã‚Â«ERPÃ‚Â»); tela mais legÃƒÂ­vel; custo/lucro corrigidos; subir teste |
| **Custo** | Se Ã‚Â«finalÃ‚Â» zerado Ã¢â€ â€™ usa base cadastro ou ÃƒÂºltima Entrada NF |
| **Lucro** | SÃƒÂ³ exibe quando hÃƒÂ¡ custo confiÃƒÂ¡vel Ã¢â‚¬â€ senÃƒÂ£o Ã‚Â«Sem custo cadastradoÃ‚Â» |
| **Textos** | Ã‚Â«ComprarÃ‚Â», Centro+Vila, Ã‚Â«ÃƒÅ¡ltimas entradas NF AgroÃ‚Â», Ã‚Â«ÃƒÅ¡lt. NFÃ‚Â» no detalhe |
| **F5 / mÃƒÂ©tricas** | `compras_metricas_util` preenche ÃƒÂºltima entrada NF Agro (colunas 6Ã¢â‚¬â€œ7) |
| **Arquivos** | `compras.html` Ã‚Â· `compras_relatorio_planilha.html` Ã‚Â· `compras_metricas_util.py` Ã‚Â· `compras_ultimas_compras_util.py` |
| **Pacote** | Inclui tambÃƒÂ©m cadastro ERP marca/cat + folha popup (sessÃƒÂ£o 08/07) |

### Ã°Å¸Ââ€º Compras Ã¢â‚¬â€ Folha Compras popup (08/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Folha (fornecedor/cat/unidade) ÃƒÂ s vezes nÃƒÂ£o abria ou abria Ã‚Â«bugadaÃ‚Â» em nova aba |
| **Causa** | Limite 3 abas SisVale + barra lateral engolindo a pÃƒÂ¡gina da planilha |
| **Fix** | Popup com iframe na Compras; `?embed=1`; folha isenta do shell/limite de abas |
| **Arquivos** | `compras.html` Ã‚Â· `compras_relatorio_planilha.html` Ã‚Â· `_agro_open_external.html` |

### Ã°Å¸Ââ€º Cadastro ERP Ã¢â‚¬â€ marca/categoria sumindo (08/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Marca, categoria, subcategorias Ã‚Â«nÃƒÂ£o salvavamÃ‚Â» / sumiam ao reabrir ou em outro PC |
| **Causa** | Modal mesclava **lista velha** por cima do **detalhe da API**; popup Ã‚Â«texto localÃ‚Â» confundia |
| **Fix** | API prevalece na abertura; lista atualiza apÃƒÂ³s salvar; **sÃƒÂ³ Postgres** (overlay + Produto); cache facetas limpa |
| **Arquivos** | `_modal_editar_produto_cadastro_erp.inc.html` Ã‚Â· `cadastro_erp_panel.js` Ã‚Â· `views.py` |
| **Teste** | Cadastro Ã¢â€ â€™ editar Ã¢â€ â€™ marca/cat Ã¢â€ â€™ **Salvar no Agro** Ã¢â€ â€™ F5 Ã¢â€ â€™ outro PC Ã¢â‚¬â€ campos persistem |

### Ã°Å¸â€œâ€¹ DecisÃƒÂ£o Ã¢â‚¬â€ popups / modais (08/07 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Hoje** | `<div>` + Tailwind + JS (`hidden`) Ã¢â‚¬â€ sem lib de modal |
| **`<dialog>` nativo** | Avaliado Ã¢â‚¬â€ **nÃƒÂ£o** migrar nem obrigar em popup novo |
| **Motivo** | ConsistÃƒÂªncia com ~dezenas de modais existentes; zero ganho visÃƒÂ­vel/perf |
| **Regra assistente** | Novo popup Ã¢â€ â€™ padrÃƒÂ£o da tela; tela nova do zero Ã¢â€ â€™ `<dialog>` opcional |
| **Docs** | `banana.md` Ã‚Â§4.14 Ã‚Â· **`SISTVALE.md`** Ã‚Â· **`FOOD.md`** |

### Ã¢Å“â€¦ Deploy loja **v7.40** Ã¢â‚¬â€ FL-051 baixa fiado no PDV (08/07 Ã¢â‚¬â€ Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Baixa fiado no **PDV pagamento** Ã‚Â· painel fiado **fecha** antes do pagamento no PDV principal |
| **Como** | Merge `teste` Ã¢â€ â€™ `producao` `54564da` |
| **Status** | **Ã¢Å“â€¦ loja** |

### Ã¢Å“â€¦ FL-051 Ã¢â‚¬â€ Baixa fiado no PDV

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Baixa fiado no **PDV pagamento** (formas + maquininha + MP Point) Ã‚Â· painel fiado **fecha** antes do pagamento no PDV principal |
| **Como** | Merge `teste` Ã¢â€ â€™ `producao` |
| **PÃƒÂ³s-deploy** | Render ~2Ã¢â‚¬â€œ5 min Ã‚Â· **Ctrl+F5** nos PDVs Ã‚Â· testar Baixa no fiado (painel e `/fiado/`) |
| **Fora** | NFC-e na quitaÃƒÂ§ÃƒÂ£o = **FL-052** |
| **Fluxo overlay** | **Baixa** no painel fiado Ã¢â€ â€™ **fecha painel** Ã¢â€ â€™ pagamento no **PDV principal** |
| **Fluxo pÃƒÂ¡gina** | `/fiado/` fora do painel Ã¢â€ â€™ `/pdv/` cobranÃƒÂ§a normal |
| **Velocidade** | Caixa jÃƒÂ¡ aberto nÃƒÂ£o refaz consulta Ã‚Â· POST sem resumo pesado |

### Ã¢Å“â€¦ Deploy loja **v7.27** Ã¢â‚¬â€ PDV Nova venda + frete F7 (07/07 Ã¢â‚¬â€ Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | BotÃƒÂ£o **Nova venda F12** Ã‚Â· modal confirmaÃƒÂ§ÃƒÂ£o padrÃƒÂ£o PDV Ã‚Â· frete sÃƒÂ³ apÃƒÂ³s etapa Entrega Ã‚Â· **F7 direto** sem frete (fix CSS) |
| **Como** | Cherry-pick `2267c0a` + `9d2dac3` + `34abbbe` |
| **Status** | **Ã¢Å“â€¦ loja** |

### Ã¢Å“â€¦ Deploy loja **v7.24** Ã¢â‚¬â€ NFC-e timeout reemitir (07/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Reemitir nÃƒÂ£o estoura 30s Render Ã‚Â· sempre JSON Ã‚Â· mensagem clara se SEFAZ cair |
| **Commit loja** | `2a0dc35` |
| **Status** | **Ã¢Å“â€¦ loja** |

### Ã¢Å“â€¦ Deploy loja **v7.23** Ã¢â‚¬â€ NFC-e retry 403 SEFAZ (07/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Retry HTTP **403/5xx** Ã‚Â· timeout conexÃƒÂ£o 12s Ã‚Â· mensagem legÃƒÂ­vel (sem HTML) |
| **Como** | Cherry-pick `a2f252a` **sÃƒÂ³ NFC-e** |
| **Commit loja** | `db22c40` |
| **Status** | **Ã¢Å“â€¦ loja** |

### Ã¢Å“â€¦ Deploy loja **v7.22** Ã¢â‚¬â€ NFC-e retry SEFAZ (07/07 Ã¢â‚¬â€ Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Retry conexÃƒÂ£o SEFAZ (1/2/4/8 s) Ã‚Â· background nÃƒÂ£o desiste em falha de rede |
| **Como** | Cherry-pick `1223bb5` **sÃƒÂ³ NFC-e** |
| **Arquivos** | `nfce_sp_emissao_util.py` Ã‚Â· `sefaz_soap_util.py` Ã‚Â· `views_nfce.py` |
| **Status** | **Ã¢Å“â€¦ loja** |

### Ã°Å¸Ââ€º NFC-e Ã¢â‚¬â€ SEFAZ conexÃƒÂ£o recusada (07/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | 1Ã‚Âª venda OK; demais falham `Connection refused` em `nfce.fazenda.sp.gov.br` |
| **Causa** | Instabilidade SEFAZ/rede **+** cÃƒÂ³digo sem retry em falha de conexÃƒÂ£o |
| **Fix** | Retry HTTP SEFAZ (1/2/4/8 s) Ã‚Â· background segue em erro de rede |
| **OperaÃƒÂ§ÃƒÂ£o** | Vendas pendentes: **Reemitir NFC-e** em `/vendas/` |

### Ã¢Å“â€¦ Deploy loja **v7.21** Ã¢â‚¬â€ PDV entregas + confirmar venda (07/07 Ã¢â‚¬â€ Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Duplo clique confirmar Ã‚Â· badge Entregas Ã‚Â· entrega encerra sÃƒÂ³ no PDV Ã‚Â· idempotente |
| **Merge** | `teste` atÃƒÂ© `d186601` Ã¢â€ â€™ `producao` |
| **Status** | **Ã¢Å“â€¦ loja** |

### Ã°Å¸Ââ€º PDV v7.19 (07/07) Ã¢â‚¬â€ fluxo entrega

| DecisÃƒÂ£o | Detalhe |
| ------- | ------- |
| **Quem encerra** | PDV, ao confirmar venda/cobranÃƒÂ§a (`finalizar entrega`) |
| **Antes (bug)** | Servidor encerrava no ERP **antes** do PDV Ã¢â€ â€™ erro Ã‚Â«nÃƒÂ£o encontradaÃ‚Â» |

### Ã°Å¸Ââ€º PDV urgente v7.18 (07/07)

| Bug | Causa | Fix |
| --- | ----- | --- |
| Ã‚Â«Entrega pendente nÃƒÂ£o encontradaÃ‚Â» com venda OK | ERP jÃƒÂ¡ encerrava entrega; PDV chamava de novo | Finalizar idempotente (404 = jÃƒÂ¡ fechada) |

### Ã°Å¸Ââ€º PDV urgente v7.17 (07/07)

| Bug | Causa | Fix |
| --- | ----- | --- |
| Venda duplicada ao clicar vÃƒÂ¡rias vezes em Confirmar | BotÃƒÂ£o liberava antes de zerar carrinho | Trava atÃƒÂ© fim do fechamento + zera carrinho na hora |
| Entregas: badge Ã‚Â«1Ã‚Â» mas lista vazia | Cache local `total` Ã¢â€°Â  lista | Contador = tamanho da lista; refresh forÃƒÂ§ado ao abrir |

### Ã¢Å“â€¦ Deploy loja **v7.15** Ã¢â‚¬â€ Pix MP sem card duplicado (07/07 Ã¢â‚¬â€ Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Some card Ã‚Â«Mercado Pago Ã¢â‚¬â€ Pix automÃƒÂ¡ticoÃ‚Â» quando cobra na maquininha |
| **Merge** | `teste` Ã¢â€ â€™ `producao` |
| **Status** | **Ã¢Å“â€¦ loja** Ã¢â‚¬â€ Render deployando |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ Pix MP v7.15 (07/07)

| Item | Detalhe |
| ---- | ------- |
| **Pix MP auto** | Some card Ã‚Â«Mercado Pago Ã¢â‚¬â€ Pix automÃƒÂ¡ticoÃ‚Â» (sÃƒÂ³ botÃƒÂ£o verde) |

### Ã¢Å“â€¦ Deploy loja **v7.13** Ã¢â‚¬â€ MP Point PDV pagamento (07/07 Ã¢â‚¬â€ Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Fluxo TEF (cobrar na tranche) Ã‚Â· cancel PDVÃ¢â€ â€maquininha Ã‚Â· popups MP Ã‚Â· crÃƒÂ©dito ÃƒÂ  vista 1x Ã‚Â· layout pagamento |
| **Merge** | `teste` Ã¢â€ â€™ `producao` Ã‚Â· `2e920b1` |
| **Status** | **Ã¢Å“â€¦ loja** |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point popup v7.11Ã¢â‚¬â€œ12 (07/07)

| Item | Detalhe |
| ---- | ------- |
| **Maquininha** | Cancel/recusa usa o mesmo modal grande do cancel PDV |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point popup v7.10 (07/07)

| Item | Detalhe |
| ---- | ------- |
| **Popup** | Ainda maior |
| **Fix** | Overlay sumia sÃƒÂ³ visualmente Ã¢â‚¬â€ travava clique; Ã‚Â«EntendiÃ‚Â» libera PDV |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point popup v7.09 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Cancel** | SÃƒÂ³ modal centralizado; texto e Ã‚Â«EntendiÃ‚Â» no meio, maior |
| **Removido** | Faixa amarela atrÃƒÂ¡s do popup |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point v7.08 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Aviso cancel** | Modal grande + faixa fixa na tela pagamento (nÃƒÂ£o some sozinha) |
| **CrÃƒÂ©dito** | Envia `default_installments: 1` Ã¢â‚¬â€ ÃƒÂ  vista sem pergunta na MP |
| **Parcelado** | Sem mudanÃƒÂ§a |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point cancel bidirecional v7.07 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Cancel PDV** | API MP com header `at_terminal` quando valor jÃƒÂ¡ estÃƒÂ¡ na maquininha |
| **BotÃƒÂ£o espera** | Chama cancel na MP antes de abortar o poll |
| **Renan** | Pediu: cancelar no sistema tambÃƒÂ©m cancelar na maquininha |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ pagamento UX v7.06 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Topo** | Removida barra verde Ã‚Â«Etapa 4 PagamentoÃ‚Â» (mais ÃƒÂ¡rea ÃƒÂºtil) |
| **RodapÃƒÂ©** | Desconto/frete e botÃƒÂµes Confirmar maiores |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ pagamento UX v7.05 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Valor** | Card grande, nÃƒÂºmero centralizado, botÃƒÂ£o largo, passos em chips |
| **RevisÃƒÂ£o** | Ã‚Â«Pode confirmarÃ‚Â» centralizado; lanÃƒÂ§amento 2 linhas + botÃƒÂµes pequenos |
| **Renan** | MP ok nas formas; sem mais venda teste (cartÃƒÂ£o) |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ pagamento UX v7.04 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Tranche** | BotÃƒÂ£o verde Ã‚Â«Cobrar na maquininhaÃ‚Â» / Ã‚Â«LanÃƒÂ§ar pagamentoÃ‚Â» (Enter continua valendo) |
| **RevisÃƒÂ£o** | SÃƒÂ³ Ã‚Â«Resta pagarÃ‚Â» + total; detalhes sÃƒÂ³ se desconto/frete |
| **Teste Renan** | Formas corretas na MP; nÃƒÂ£o fechou mais venda (evitar cartÃƒÂ£o) |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ pagamento UX v7.03 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Direita** | Card grande Ã‚Â«Resta pagarÃ‚Â» / Ã‚Â«QuitadoÃ‚Â» + jÃƒÂ¡ lanÃƒÂ§ado |
| **Lista** | Badge verde **Pago** em cada lanÃƒÂ§amento; MP pago com fundo verde |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point fluxo TEF v7.02 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Tranche** | Enter no valor Ã¢â€ â€™ envia Point + espera + lanÃƒÂ§a Ã‚Â«Pago na maquininhaÃ‚Â» |
| **Confirmar** | SÃƒÂ³ grava venda/cupom (nÃƒÂ£o cobra de novo) |
| **API** | `confirmar-tranche` + status `PAID` Ã‚Â· finalizar aceita `erp_payload` completo |
| **Lista** | MP pago sem Alterar/Excluir |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ layout pagamento v7.01 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Barra inferior** | Campos e botÃƒÂµes maiores; borda escura nos inputs |
| **Pagamento** | Inputs com mais contraste (monitor claro) |
| **Parcelado MP** | Aviso se total &lt; R$ 10 |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ layout pagamento v6.98Ã¢â‚¬â€œ99 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Barra inferior** | Voltar + desconto + frete + confirmar (lado a lado) |
| **Removido** | ObservaÃƒÂ§ÃƒÂ£o final + footer duplicado na etapa pagamento |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point v6.97 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Avisos** | Confirmar venda / MP / caixa Ã¢â€ â€™ toast PDV (sem alert Chrome) |
| **Overlay gestÃƒÂ£o** | Borda/botÃƒÂ£o emerald (sem laranja) |
| **Render print** | Sem `MP_POINT_PRINT_ON_TERMINAL` Ã¢â€ â€™ cÃƒÂ³digo usa `seller_ticket` |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point v6.96 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Popup** | Maior, cores emerald/slate (sem laranja) |
| **Avisos** | Toast padrÃƒÂ£o PDV (nÃƒÂ£o alert do navegador) |
| **Recusa/cancel** | Poll detecta `failed`/`canceled` na maquininha |
| **Parcelas** | Envio reforÃƒÂ§ado + aviso se MP confirmar diferente do PDV |
| **Comprovante** | PadrÃƒÂ£o `seller_ticket` Ã¢â‚¬â€ **no Render** trocar `MP_POINT_PRINT_ON_TERMINAL` se ainda `no_ticket` |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point v6.95 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Parcelado** | `default_installments` como **inteiro** (fix erro MP property_type) |
| **Popup espera** | Painel maior; passos 1Ã¢â‚¬â€œ4; Ã‚Â«Travou?Ã‚Â» recolhido |
| **Cancel** | PDV chama API cancel MP; poll detecta cancel na maquininha |
| **Comprovante** | Maquininha: `MP_POINT_PRINT_ON_TERMINAL` no Render (padrÃƒÂ£o `no_ticket`) |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point v6.93 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Fix 500** | `MP_ORDERS_URL` restaurada + sintaxe corrigida |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point v6.92 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Fix 500** | Constante `MP_ORDERS_URL` restaurada em `mercado_pago_point.py` |
| **API** | Erro interno no criar Ã¢â€ â€™ JSON legÃƒÂ­vel (nÃƒÂ£o pÃƒÂ¡gina HTML) |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point v6.91 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Fix** | Token CSRF no PDV (`get_token`) Ã¢â‚¬â€ erro Ã‚Â«Unexpected token \<Ã‚Â» ao confirmar MP |
| **Erro legÃƒÂ­vel** | Se servidor falhar, mensagem em portuguÃƒÂªs (nÃƒÂ£o JSON quebrado) |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point v6.90 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Host MP** | SÃƒÂ³ navegador que **abriu Gaveta/Teste** manda Point (flag sessÃƒÂ£o; notebook bloqueado) |
| **Venda** | `pagamentos` gravam `maquinaId` no ERP/Agro (conferÃƒÂªncia MP) |
| **Som** | Um beep sÃƒÂ³ (ok ou erro se forma divergiu) |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point v6.89 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Fechar caixa** | Linhas MP separadas: Pix / DÃƒÂ©bito / CrÃƒÂ©dito **Ã¢â‚¬â€ Mercado Pago** vs demais maquininhas |
| **2Ã‚Âº PDV** | MP automÃƒÂ¡tico **sÃƒÂ³ Caixa Gaveta** (1Ã‚Âº aberto); Notebook = Cielo/Sicredi/Sicoob |
| **Venda** | `pagamentos_json` guarda `maquinaId` + `cobrarNoPointMp` para conferÃƒÂªncia |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point v6.88 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Garantia MP** | ApÃƒÂ³s pagar, lÃƒÂª forma real da maquininha; se divergir do PDV Ã¢â€ â€™ alerta + grava forma do MP |
| **Parcelado MP** | Envia N parcelas para a maquininha (`default_installments`) |
| **Fechar caixa** | Parcelado soma em **CrÃƒÂ©dito** (sÃƒÂ³ no fechamento; venda/ERP como antes) |
| **Painel espera** | Contador Ã‚Â· som ok/erro Ã‚Â· timeout Ã‚Â· fila na maquininha Ã‚Â· ajuda aberta |

### Ã¢Å“â€¦ PDV Ã¢â‚¬â€ MP Point v6.87 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **MP** | CartÃƒÂ£o e Pix **sÃƒÂ³ automÃƒÂ¡tico** (sem Manual) |
| **Pix** | MP auto Ã‚Â· Cielo manual Ã‚Â· Sicoob chave |
| **CartÃƒÂ£o** | MP auto Ã‚Â· Cielo Ã‚Â· Sicredi manual |
| **Fix** | Desconto/frete no Point Ã‚Â· forma na mÃƒÂ¡quina Ã‚Â· painel espera |

### Ã°Å¸â€Å’ MP Point Ã¢â‚¬â€ reativar conta nova (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Lado MP** | Maquininha **ativa** Ã‚Â· modo **vincular com o caixa** Ã¢Å“â€¦ Ã‚Â· app **pdvagromais** Ã‚Â· credencial **ProduÃƒÂ§ÃƒÂ£o** |
| **Lado Agro** | Render **teste** Ã¢â‚¬â€ `MP_POINT_*` configurado (Renan 06/07) |
| **Ordem** | Validar v6.87 no teste Ã¢â€ â€™ depois loja |
| **Status** | **Ã°Å¸Â§Âª teste v6.87** deploy |

### Ã¢Å“â€¦ Deploy loja **v6.86** Ã¢â‚¬â€ PIN PDV + contagem caixa (05/07 noite Ã¢â‚¬â€ Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | PIN manual/descanso PDV Ã‚Â· contagem fechar caixa ao reabrir Chrome Ã‚Â· overlay caixa nÃƒÂ£o trava |
| **Commits loja** | `8410ebc` Ã‚Â· `8df7e18` Ã‚Â· `ca78b88` Ã‚Â· `ae69827` |
| **Fora da loja** | Entregas PDV Ã‚Â· FL-049/050 (sÃƒÂ³ teste) |
| **Status** | **Ã¢Å“â€¦ loja** Ã¢â‚¬â€ validar PIN + contagem na operaÃƒÂ§ÃƒÂ£o |

### Ã°Å¸Ââ€º PDV Ã¢â‚¬â€ botÃƒÂ£o PIN nÃƒÂ£o abre (05/07 noite)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Descanso automÃƒÂ¡tico e botÃƒÂ£o **PIN** no PDV nÃƒÂ£o mostram tela |
| **Causa** | Fix de modal bloqueava `openLock` mesmo em abertura **manual** |
| **Fix** | PIN manual forÃƒÂ§a abertura Ã‚Â· overlay sÃƒÂ³ pausa se **visÃƒÂ­vel** |
| **Status** | **Ã°Å¸Â§Âª teste v6.86** |

### Ã°Å¸Ââ€º Caixa Ã¢â‚¬â€ contagem some ao fechar navegador (05/07 noite)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Digitou contagem Ã‚Â· fechou Chrome Ã‚Â· voltou zerado |
| **Causa** | Chave do turno exigia match exato de PKs e apagava localStorage |
| **Fix** | Guarda por **dia** Ã‚Â· grava turno ao digitar Ã‚Â· sÃƒÂ³ limpa se **mudou o dia** |
| **Status** | **Ã°Å¸Â§Âª teste v6.86** |

### Ã°Å¸Ââ€º PDV Ã¢â‚¬â€ descanso atrÃƒÂ¡s do modal Entregas (05/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Modal Ã‚Â«Pagamento na entregaÃ‚Â» aberto Ã‚Â· descanso/PIN aparece **atrÃƒÂ¡s** Ã‚Â· tela trava |
| **Causa** | `<dialog>` fica acima do PIN Ã‚Â· descanso nÃƒÂ£o pausava com modal PDV aberto |
| **Fix** | Pausa idle com modal/dialog PDV Ã‚Â· fecha modais antes do PIN Ã‚Â· **detecÃƒÂ§ÃƒÂ£o genÃƒÂ©rica** (`dialog[open]` + `aria-modal` + entrega wizard + overlay iframe) |
| **Status** | **Ã°Å¸Â§Âª teste v6.85** Ã¢â‚¬â€ validar pagamento, NFC-e, cliente, entrega |

### Ã°Å¸Ââ€º Entregas PDV vazio mas bloqueia fechar caixa (05/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Fechar caixa lista N entregas Ã‚Â«pagamento na entregaÃ‚Â» Ã‚Â· PDV Ã¢â€ â€™ Entregas: Ã‚Â«Nenhuma pendÃƒÂªnciaÃ‚Â» |
| **Causa** | Fechar caixa vÃƒÂª **todos caixas abertos** Ã‚Â· PDV filtrava sÃƒÂ³ o **caixa do navegador** (ex. Caixa #2 vs pendÃƒÂªncias no #1) |
| **Fix** | API PDV usa **mesmo critÃƒÂ©rio** do fechamento Ã‚Â· lista mostra **qual caixa** Ã‚Â· link laranja abre PDV com `?entregas=1` |
| **ProduÃƒÂ§ÃƒÂ£o** | Mesmo cÃƒÂ³digo na loja Ã¢â‚¬â€ pode afetar se houver notebook/teste + gaveta abertos com entrega pendente |
| **Status** | **Ã°Å¸Â§Âª teste v6.85** Ã¢â‚¬â€ validar botÃƒÂ£o laranja + lista com caixa |

### Ã°Å¸Ââ€º Caixa fechar Ã¢â‚¬â€ cache contagem sumindo (05/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Fechar/abrir navegador ou erro no fechamento Ã¢â€ â€™ contagem por forma e cÃƒÂ©dulas **zerada** |
| **Causa** | Patch turno limpava localStorage **no clique em fechar** (antes de confirmar) |
| **Fix** | Grava **na hora** no aparelho Ã‚Â· limpa sÃƒÂ³ apÃƒÂ³s **fechamento OK** ou **virou o dia** / **mudou turno** |
| **Status** | **Ã°Å¸Â§Âª teste v6.81** |

### Ã°Å¸Ââ€º Caixa fechar Ã¢â‚¬â€ tela trava com modal cÃƒÂ©dulas + descanso (05/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Parado na contagem (overlay cÃƒÂ©dulas / fiado): cliques e teclado morrem Ã¢â‚¬â€ igual PIN descanso |
| **Causa** | Modo descanso (~3 min) ativava **por baixo** dos overlays do caixa Ã‚Â· ou PDV overlay sem pausar idle |
| **Fix** | PIN descanso sempre **por cima** Ã‚Â· pausa idle com overlay PDV/caixa aberto Ã‚Â· cÃƒÂ©dulas acima do fiado |
| **Status** | **Ã°Å¸Â§Âª teste v6.81** Ã¢â‚¬â€ validar fechamento + contagem cÃƒÂ©dulas (PDV overlay e pÃƒÂ¡gina direta) |

### Ã¢Å“â€¦ FL-050 Ã¢â‚¬â€ `/vendas/` aguardar NFC-e background (04/07)

| Item | Detalhe |
| ---- | ------- |
| **Fix** | `venda_nfce_processando` (~120 s) Ã‚Â· botÃƒÂ£o **EmitindoÃ¢â‚¬Â¦** + alerta *aguarde* Ã‚Â· POST reemitir **409** se ainda processando Ã‚Â· detalhe venda igual |
| **Status** | **Ã°Å¸Â§Âª teste** Ã‚Â· **nÃƒÂ£o estava na loja** (v6.77 sÃƒÂ³ tinha fix CPF/F9 no PDV) |

### Ã¢Å“â€¦ FL-049 Ã¢â‚¬â€ CPF cliente no PDV + NFC-e (04/07)

| Item | Detalhe |
| ---- | ------- |
| **Fix** | Campo **CPF** nos modais PDV (editar Ã‚Â· cadastro rÃƒÂ¡pido Ã‚Â· entrega) Ã‚Â· grava `ClienteAgro` Ã‚Â· Enter/F9 usam CPF cadastrado sem modal |
| **Status** | **Ã°Å¸Â§Âª teste** |

### ~~Ã°Å¸â€œâ€¹ Fila Ã¢â‚¬â€ **FL-050**~~ *(fechado Ã¢â‚¬â€ ver acima)*

### ~~Ã°Å¸â€œâ€¹ Fila Ã¢â‚¬â€ **FL-049**~~ *(fechado Ã¢â‚¬â€ ver acima)*

### Ã¢Å“â€¦ Deploy loja **v6.77** (03/07 Ã¢â‚¬â€ Renan senha OK)

| Pacote | Commits / nota |
| ------ | -------------- |
| **NFC-e CPF + sync F9** | `3453968` Ã‚Â· merge `8495fb7` |
| **Status** | Render produÃƒÂ§ÃƒÂ£o deployando |

### NFC-e Ã¢â‚¬â€ CPF com impressÃƒÂ£o + sync SEFAZ (03/07)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Background emitia sem CPF antes do modal; F9 imprimia antes da SEFAZ autorizar |
| **DecisÃƒÂ£o** | **Enter** = NFC-e background (rÃƒÂ¡pido) Ã‚Â· **F9** = modal CPF + `nfce_sincrona` + imprime sÃƒÂ³ se autorizou |
| **Arquivos** | `pdv_wizard.js` Ã‚Â· `views_nfce.py` |
| **Status** | **Ã¢Å“â€¦ loja v6.77** |

### Ã¢Å“â€¦ Deploy loja **v6.75** (03/07 madrugada Ã¢â‚¬â€ Renan senha OK)

| Pacote | Commits / nota |
| ------ | -------------- |
| **Busca rÃƒÂ¡pida** | `908ff07` Ã¢â‚¬â€ entrada NF + cadastro |
| **Caixa contagem** | `21491cd` Ã¢â‚¬â€ zera rascunho ao mudar turno |
| **Fiado busca** | `21491cd` Ã¢â‚¬â€ cliente sem saldo na busca |
| **Roteiro Cursor** | `banana-roteiro.md` + modo econÃƒÂ´mico |
| **Merge** | `d26e913` `teste` Ã¢â€ â€™ `producao` |

### Ã¢Å¡Â¡ Busca produtos Ã¢â‚¬â€ entrada NF + cadastro (03/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Digitar Ã‚Â«milhoÃ‚Â»/GM/EAN na etapa produtos: 4Ã¢â‚¬â€œ14 s por tecla; requests empilhadas |
| **Fix** | Servidor: `catalogo_agro.buscar` sem scan `.iterator()` Ã‚Â· match exato antes do `icontains` Ã‚Â· GM/barras sÃƒÂ³ com tamanho mÃƒÂ­nimo Ã‚Â· sem fallback Mongo em `agro_pg` Ã‚Â· cache 45 s entrada NF. Cliente: abort Ã‚Â· debounce 400 ms Ã‚Â· cache PDV local. **Cadastro:** GM sÃƒÂ³ com 5+ chars Ã‚Â· barras 8+ Ã‚Â· debounce cÃƒÂ³digo 420 ms Ã‚Â· sem 2Ã‚Âª busca PDV |
| **Status** | **Ã¢Å“â€¦ loja v6.75** |

### Ã°Å¸Ââ€º Fechar caixa Ã¢â‚¬â€ contagem do dia anterior (03/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Contagem por forma ficava salva atÃƒÂ© o fechamento do dia seguinte |
| **Fix** | Rascunho amarrado ao turno (sessÃƒÂ£o) Ã‚Â· limpa localStorage ao mudar turno/fechar |
| **Status** | **Ã¢Å“â€¦ loja v6.75** |

### Ã°Å¸Ââ€º Fiado Ã¢â‚¬â€ busca sÃƒÂ³ com saldo (03/07)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Renan Ã¢â‚¬â€ na busca, ver cliente mesmo sem pendÃƒÂªncia |
| **Fix** | `apenas_saldo=0` quando digita busca Ã‚Â· cadastro ClienteAgro entra na lista |
| **Status** | **Ã¢Å“â€¦ loja v6.75** |

### Ã°Å¸Ââ€º Modo descanso Ã¢â‚¬â€ tela borrada ilegÃƒÂ­vel (03/07 Ã‚Â· loja)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | ApÃƒÂ³s ~3 min sem mexer: overlay escuro + tudo borrado; popup PIN ilegÃƒÂ­vel (Chrome/GPU) |
| **Causa** | `backdrop-blur` + `filter: blur` nos irmÃƒÂ£os do body Ã¢â‚¬â€ bug de composiÃƒÂ§ÃƒÂ£o no Chrome |
| **Fix** | `_screensaver_pin.html` Ã¢â‚¬â€ fundo sÃƒÂ³lido 94 %, sem blur no fundo; `#sspin-root` isolado no `body` |
| **Status** | **Ã¢Å“â€¦ loja v6.28** |

### Ã¢Å“â€¦ Deploy loja **v6.20** Ã¢â‚¬â€ perf PC fraco + RH vale CP (03/07)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Renan Ã¢â‚¬â€ loja fechou Ã‚Â· perf + fix RH vale Ã‚Â· senha OK |
| **Commits** | `c9c1ece`Ã¢â‚¬Â¦`6741ed2` em `producao` (7 commits cherry do `teste`) |
| **Perf** | Splash catÃƒÂ¡logo Ã‚Â· cache offline Ã‚Â· sync foco 5 min Ã‚Â· F11 animaÃƒÂ§ÃƒÂµes Ã‚Â· GestÃƒÂ£o sem iframe PDV Ã‚Â· BI lazy Ã‚Â· repouso abas 5/20 min |
| **RH** | Vale caixa **nÃƒÂ£o** duplica pagamento CP parcial na folha (`e557857` / `6741ed2`) |
| **Mantido** | Carrinho v6.16 |

### Ã°Å¸Ââ€º RH folha Ã¢â‚¬â€ vale duplicava pagamento CP (02/07 Ã‚Â· Renan) Ã¢â‚¬â€ **Ã¢Å“â€¦ loja v6.20**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Vale caixa + pagamento CP parcial iguais Ã¢â€ â€™ CP **Pago** inflado |
| **Fix** | Hook sÃƒÂ³ baixa direta em LanÃƒÂ§amentos Ã‚Â· sync Postgres Ã‚Â· aviso verde no fechamento |
| **Status** | **Na loja** Ã¢â‚¬â€ nÃƒÂ£o reabrir folhas jÃƒÂ¡ gambiarradas |
| **16/07** | Renan pediu cherry **sÃƒÂ³** deste bug + senha Ã¢â‚¬â€ **jÃƒÂ¡ estava** em `producao` (`e557857` / `6741ed2`) Ã‚Â· **nada a subir** |

### Ã°Å¸Ââ€º FL-048 Ã¢â‚¬â€ Ã‚Â«Baixar ZIP selecionadoÃ‚Â» nÃƒÂ£o baixava (02/07 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Ã‚Â«Kit recuperaÃƒÂ§ÃƒÂ£o zeroÃ‚Â» OK Ã‚Â· Ã‚Â«Baixar ZIP selecionadoÃ‚Â» recarrega a tela sem arquivo |
| **Causa** | `resumo.xlsx` Ã¢â‚¬â€ campo `categorias` (lista) no manifest; openpyxl nÃƒÂ£o aceita lista na cÃƒÂ©lula |
| **Fix** | `_excel_scalar()` em `pg_backup_util.py` Ã‚Â· mensagem de erro legÃƒÂ­vel na view |
| **Kit zero** | ConteÃƒÂºdo conferido OK (guias + `render-env-atual.env` + scripts) |

### Ã¢Å“â€¦ Deploy loja **v6.16** Ã¢â‚¬â€ carrinho PDV itens travados (02/07)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Renan Ã¢â‚¬â€ cherry **sÃƒÂ³** fix carrinho Ã‚Â· senha OK |
| **Commit** | `add4ce6` em `producao` |
| **O quÃƒÂª** | Lista busca sumia de verdade Ã‚Â· toque no carrinho fecha busca Ã‚Â· clique na linha por ÃƒÂ­ndice |
| **Validado** | Renan Ã¢â‚¬â€ **loja v6.16 OK** (GM6082 + GM6083) |
| **Fora** | Pacote perf Ã‚Â· RH vale CP Ã‚Â· entregas topbarÃ¢â€ â€™subtotal |

### Ã°Å¸Ââ€º PDV wizard Ã¢â‚¬â€ carrinho itens travados (02/07 Ã‚Â· Renan) Ã¢â‚¬â€ **Ã¢Å“â€¦ loja v6.16**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Alguns produtos no carrinho nÃƒÂ£o respondem a +/Ã¢Ë†â€™, preÃƒÂ§o nem remover; outros na mesma venda OK; sÃƒÂ³ **LIMPAR** tira |
| **Exemplos** | GM6082 (dobradiÃƒÂ§a) Ã‚Â· GM6083 (facÃƒÂ£o) |
| **Causa** | Lista da **busca** nÃƒÂ£o sumia (`#pdv-product-autocomplete` Ã¢â‚¬â€ `.hidden` perdia para `display:flex`) |
| **Fix** | `pdv_wizard.html` + `pdv_wizard.js` |
| **Status** | **Fechado** Ã¢â‚¬â€ loja confirmada Renan |

### WIP teste Ã¢â‚¬â€ perf CPU/RAM (02/07 Ã‚Â· Renan) Ã¢â‚¬â€ **Ã¢Å“â€¦ loja v6.20**

| Item | Detalhe |
| ---- | ------- |
| **Splash catÃƒÂ¡logo** | Atraso **400 ms** (cache rÃƒÂ¡pido **nÃƒÂ£o aparece**) Ã‚Â· mÃƒÂ­n. visÃƒÂ­vel **200 ms** |
| **Config F11** | Modal Ã‚Â· **Menos animaÃƒÂ§ÃƒÂµes** separado PDV / GestÃƒÂ£o (`agro_perf_fx.js`) Ã‚Â· tambÃƒÂ©m no Menu **F10** (GestÃƒÂ£o) |
| **GestÃƒÂ£o 2 apps** | Sem iframe PDV oculto Ã‚Â· Dashboard **sÃƒÂ³ carrega ao abrir** guia |
| **Abas livres** | **5 min** pausa animaÃƒÂ§ÃƒÂµes Ã‚Â· **20 min** descarrega iframe (volta ao clicar Ã¢â‚¬â€ nÃƒÂ£o recarrega ao trocar aba) |
| **BI** | Pausa grÃƒÂ¡ficos/pulsos quando guia Dashboard nÃƒÂ£o estÃƒÂ¡ ativa |
| **PDV sync foco** | Delta catÃƒÂ¡logo ao trocar janela: **8s Ã¢â€ â€™ 5 min** (busca local intacta) |
| **Overlay Caixa/Fiado** | JÃƒÂ¡ limpava iframe ao fechar (`agro_pdv_overlay.js`) |
| **Teste Renan (PC forte)** | SÃƒÂ³ PDV ~0,2Ã¢â‚¬â€œ2,8% Ã‚Â· sÃƒÂ³ GestÃƒÂ£o ~3,5Ã¢â‚¬â€œ7% Ã‚Â· ambos ~4Ã¢â‚¬â€œ10% CPU idle |

**WIP teste (antigo):** PDV splash 1Ã‚Âª carga Ã‚Â· cache offline Ã‚Â· wizard delta no foco.

### DecisÃƒÂ£o operaÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ Entregas Ãƒâ€” PDV (01/07, Renan)

| Item | Detalhe |
| ---- | ------- |
| **Fluxo loja** | PDV entrega Ã¢â€ â€™ entregador leva e cobra Ã¢â€ â€™ volta Ã¢â€ â€™ **retoma no PDV** e fecha venda com pagamento real |
| **Tela `/entregas/`** | Uso **raro** (ex. **terÃƒÂ§a** Ã¢â‚¬â€ rota sÃƒÂ­tio); status do meio **nÃƒÂ£o usados** Ã¢â‚¬â€ sÃƒÂ³ **pendente** e **entregue** |
| **BotÃƒÂ£o PDV** | SÃƒÂ³ **Ã‚Â«Entregas pendentesÃ‚Â»** embaixo (card Subtotal) Ã¢â‚¬â€ **removido** da barra de cima Ã‚Â· abre modal cobranÃƒÂ§a na volta |
| **Pagamento na entrega** | Fechar venda no PDV Ã¢â€ â€™ **entregue** em `/entregas/` + some da fila Ã‚Â«Entregas pendentesÃ‚Â» |
| **Pagamento na loja** | Venda fecha na hora Ã¢â€ â€™ **nÃƒÂ£o** entra na fila PDV Ã‚Â· **nÃƒÂ£o** bloqueia fechar caixa Ã‚Â· fica **pendente** em `/entregas/` atÃƒÂ© **fechar o caixa** Ã¢â€ â€™ aÃƒÂ­ vira **entregue** |
| **CÃƒÂ³digo** | `marcar_entrega_pendente_fechada` Ã‚Â· `finalizar_entregas_pagas_pendentes_ao_fechar_caixa` Ã‚Â· hook em `caixa_fechar` |

### Ã¢Å“â€¦ Deploy loja **v6.15** Ã¢â‚¬â€ fix botÃƒÂ£o **Pagar** clique morto (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o direto + **`99738595`** |
| **O quÃƒÂª** | Cherry **`399f2f2`** Ã¢â€ â€™ **`b8edc38`** Ã¢â‚¬â€ sÃƒÂ³ `pdv_wizard.js` + `pdv_wizard.html` |
| **Migrate** | Nenhuma |
| **Validar loja** | Ctrl+F5 badge **v6.15** Ã‚Â· finalizar venda Ã¢â€ â€™ nova venda Ã¢â€ â€™ clicar **Pagar** (sem sÃƒÂ³ F7) |

### Ã°Å¸Ââ€º PDV wizard Ã¢â‚¬â€ botÃƒÂ£o **Pagar** clique morto Ã‚Â· **Ã¢Å“â€¦ loja v6.15**

| Item | Detalhe |
| ---- | ------- |
| **Causa** | Toast pÃƒÂ³s-venda invisÃƒÂ­vel bloqueava cliques no canto inferior direito |
| **Fix** | `pointer-events-none` ao esconder toast Ã‚Â· limpar ao clicar Pagar |

### Ã°Å¸Å¡â‚¬ Deploy loja **v6.14** Ã¢â‚¬â€ FL-048 kit + env no ZIP + backup noturno (30/06)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ *pode subir* + **`99738595`** |
| **O quÃƒÂª** | Cherry **`c875945`** Ã‚Â· **`021a96e`** Ã‚Â· **`ff1f0d9`** Ã‚Â· **`d178536`** |
| **Migrate** | Nenhuma |
| **Validar loja** | Ctrl+F5 badge **v6.14** Ã‚Â· Admin Ã¢â€ â€™ Backup Ã¢â€ â€™ ZIP Ã¢â€ â€™ `kit/render-env-atual.env` |

### Ã¢Å“â€¦ **FL-048** Ã¢â‚¬â€ backup Postgres + recuperaÃƒÂ§ÃƒÂ£o zero

**Rotina:** marcar todas Ã¢â€ â€™ Ã‚Â«Baixar ZIPÃ‚Â» Ã¢â€ â€™ guardar no PC/nuvem. **Um ZIP basta** Ã¢â‚¬â€ sem site depois.

**Dentro do ZIP:** `data/*.jsonl` Ã‚Â· `manifest.json` Ã‚Â· `resumo.xlsx` Ã‚Â· pasta `kit/` com:
- `GUIA-BACKUP-PAINEL.txt` (espelho do painel)
- `render-env-atual.env` Ã¢â‚¬â€ **Environment real** do servidor (senhas, Mongo, NFC, MPÃ¢â‚¬Â¦)
- `LEIA-ME-RECUPERACAO-ZERO.txt` Ã‚Â· scripts

**resumo.xlsx (Renan 03/07):** **nÃƒÂ£o** ÃƒÂ© a lista completa Ã¢â‚¬â€ ÃƒÂ© **amostra legÃƒÂ­vel** para conferÃƒÂªncia rÃƒÂ¡pida. **Backup/restauraÃƒÂ§ÃƒÂ£o real** = `data/catalogo.jsonl` (tudo do **Postgres**). Aba **Contagens** = total por tabela; se o Excel tiver menos linhas, o resto estÃƒÂ¡ sÃƒÂ³ no JSONL. Planilha sÃƒÂ³ de cadastro: **Cadastro ERP Ã¢â€ â€™ Excel Ã¢â€ â€œ**.

**Excel v6.69+:** uma aba por tabela (ex. `catalogo_Produto` com cÃƒÂ³digo, nome, preÃƒÂ§oÃ¢â‚¬Â¦) Ã¢â‚¬â€ nÃƒÂ£o mistura overlay/promoÃƒÂ§ÃƒÂ£o na mesma folha.

**Fora do ZIP:** cÃƒÂ³digo Git Ã‚Â· dados *dentro* do Mongo ERP (credenciais vÃƒÂªm no .env).

**Parcial / restore parcial:** inalterado Ã¢â‚¬â€ checkbox por categoria.

**Backup noturno:** cron 04h Ã‚Â· webhook/S3 Ã‚Â· ver `pg_backup_nightly.py`.

**Desastre:** novo Render Ã¢â€ â€™ deploy `producao` Ã¢â€ â€™ colar `kit/render-env-atual.env` (trocar `DATABASE_URL`) Ã¢â€ â€™ migrate Ã¢â€ â€™ superuser Ã¢â€ â€™ Restore ZIP.

**Rollback noite:** restore = sÃƒÂ³ **dados** Ã‚Â· cÃƒÂ³digo ruim = **deploy versÃƒÂ£o antiga** (git/banana).

**Fonte checklist:** `pg_backup_render_checklist.py` Ã‚Â· `pg_backup_disaster_kit.py` Ã‚Â· `pg_backup_nightly.py` Ã‚Â· `pg_backup_upload.py`

**Ã¢Å“â€¦ Renan (03/07):** senha do **admin superuser** trocada Ã‚Â· usuÃƒÂ¡rio **novo para loja** criado **sem** superuser (nÃƒÂ£o vÃƒÂª backup/restore).

| Quem | Admin Django | Backup Postgres |
| ---- | ------------ | --------------- |
| **Superuser** (vocÃƒÂª) | Sim | Baixar + restaurar (com senha) |
| **UsuÃƒÂ¡rio loja** (staff, sem superuser) | SÃƒÂ³ o que o perfil permitir | **NÃƒÂ£o** aparece |

**Validar:** login superuser Ã¢â€ â€™ Admin Ã¢â€ â€™ **OperaÃƒÂ§ÃƒÂµes SisVale** Ã‚Â· login usuÃƒÂ¡rio loja Ã¢â€ â€™ **sem** bloco backup.

**Pendente operaÃƒÂ§ÃƒÂ£o loja (02/07):** replicar **2 apps Chrome PDV + GestÃƒÂ£o na barra** em **todos os PCs Win10** Ã¢â‚¬â€ roteiro em **Ã‚Â§ Atalhos Win10** abaixo.

### Ã¢Å“â€¦ Deploy loja **v6.11** Ã¢â‚¬â€ popup Caixa (overlay JS quebrado) (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ *arruma / pode mandar* (continuaÃƒÂ§ÃƒÂ£o deploy PDV) |
| **Commit** | **`8947c7f`** (sync **`agro_pdv_overlay.js`** + **`agro_dual_window.js`** de teste) |
| **Rollback** | Tag **`producao-rollback-v6.10-20260702`** @ **`8670f40`** |
| **Causa v6.10** | **`agro_pdv_overlay.js` na loja** tinha `options.force = true` **sem** `options` Ã¢â€ â€™ `ReferenceError` Ã‚Â· `openPdvPanel` engolia o erro Ã¢â€ â€™ Caixa ia para **janela GestÃƒÂ£o** (shell lateral) Ã‚Â· **teste** jÃƒÂ¡ tinha o JS completo desde v6.05 Ã¢â‚¬â€ cherry-pick v6.10 **nÃƒÂ£o copiou** esse arquivo |
| **Fix** | Overlay JS completo Ã‚Â· `navigateGestao` **nunca** `pulseGestaoFocus` para Caixa/Vendas/Fiado no PDV |
| **Validar loja** | Ctrl+Shift+R badge **v6.11** Ã‚Â· Caixa Ã¢â€ â€™ **popup laranja ~95%** (igual teste) |

**Se amanhÃƒÂ£ ainda falhar (checklist):**

| # | Conferir |
| - | -------- |
| 1 | Badge **v6.11** no PDV (senÃƒÂ£o deploy/cache HTML) |
| 2 | F12 Ã¢â€ â€™ Network Ã¢â€ â€™ `agro_pdv_overlay.js?v=` **commit** (nÃƒÂ£o `v=1`) |
| 3 | F12 Ã¢â€ â€™ Console Ã¢â‚¬â€ erro vermelho ao clicar Caixa? |
| 4 | Ctrl+Shift+R **no app PDV** (nÃƒÂ£o sÃƒÂ³ F5) |
| 5 | Paridade **prod=teste** nos 3 JS overlay Ã¢â‚¬â€ **OK pÃƒÂ³s-v6.11** (`agro_dual_window`, `agro_pdv_overlay`, `pdv_wizard`) |

**Riscos restantes (baixa):** cache agressivo Chrome Ã‚Â· app GestÃƒÂ£o aberto ao lado redirecionando foco (v6.11 bloqueia `pulseGestaoFocus` no PDV para Caixa/Vendas/Fiado).

**Renan 02/07 madrugada:** *Ã‚Â«parece que estÃƒÂ£o bomÃ‚Â»* Ã¢â‚¬â€ validaÃƒÂ§ÃƒÂ£o definitiva na **abertura da loja** (Caixa/Vendas/Fiado Ã¢â€ â€™ popup laranja Ã‚Â· badge **v6.11**).

### Ã¢Å¡Â Ã¯Â¸Â Deploy loja **v6.10** Ã¢â‚¬â€ incompleto (02/07)

| Item | Detalhe |
| ---- | ------- |
| **Commit** | **`8670f40`** |
| **Problema** | **`agro_pdv_overlay.js` nÃƒÂ£o foi copiado** Ã¢â‚¬â€ JS quebrado na loja Ã‚Â· substituÃƒÂ­do por **v6.11** |

### Ã¢Å“â€¦ PDV Caixa popup 95% Ã¢â‚¬â€ **v6.11 loja** (02/07 Ã‚Â· Renan)

### Ã°Å¸Ââ€º PDV topbar Ã¢â‚¬â€ ainda quebrado pÃƒÂ³s-v6.09 (02/07 madrugada Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Clique em Caixa/Vendas/Fiado no PDV nÃƒÂ£o abre overlay Ã‚Â· `/caixa/` em nova guia abre mas cards do menu nÃƒÂ£o navegam |
| **Causa extra** | Scripts `agro_dual_window.js` / `agro_pdv_overlay.js` com **`?v=1` fixo** (browser servia JS antigo) Ã‚Â· `isGestaoHost()` ainda tratava qualquer URL nÃƒÂ£o-PDV como gestÃƒÂ£o Ã¢â€ â€™ shell lateral engolia `/caixa/` |
| **Fix teste v6.51** | `agro_asset_v` no context (commit Render) Ã‚Â· `isGestaoHost` sÃƒÂ³ com papel gestÃƒÂ£o explÃƒÂ­cito Ã‚Â· roteador PDV + `openPdvPanel` reforÃƒÂ§ados Ã‚Â· fallbacks antes de `pulseGestaoFocus` silencioso |
| **Validar teste** | Ctrl+Shift+R no PDV Ã‚Â· DevTools Ã¢â€ â€™ `agro_dual_window.js?v=<commit>` (nÃƒÂ£o `v=1`) Ã‚Â· topbar Ã¢â€ â€™ overlay Ã‚Â· `/caixa/` guia separada Ã¢â€ â€™ cards navegam |

### Ã¢Å“â€¦ Deploy loja **v6.09** Ã¢â‚¬â€ PDV topbar overlay (fix isPdvHost) (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ *pode mandar* + senha **`99738595`** (sem reteste) |
| **Commit** | **`1591b63`** (cherry-pick **`bfd4de5`** de teste) |
| **Rollback** | Tag **`producao-rollback-v6.08-20260702`** @ **`6461974`** |
| **O quÃƒÂª** | **PDV:** Caixa / Vendas / Fiado abrem overlay Ã‚Â· submenus `/caixa/` em guia separada voltam a clicar |
| **Migrate** | Nenhuma |
| **Validar loja** | Ctrl+F5 badge **v6.09** Ã‚Â· topbar PDV Ã¢â€ â€™ overlay laranja Ã‚Â· cards do caixa navegam |

### Ã¢Å“â€¦ Deploy loja **v6.08** Ã¢â‚¬â€ PDV topbar overlay + busca cadastro/NF (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ *pode mandar para produÃƒÂ§ÃƒÂ£o ambos* + senha **`99738595`** |
| **Commit** | **`6461974`** (cherry-pick **`f012d42`** de teste) |
| **Rollback** | Tag **`producao-rollback-v6.07-20260702`** @ **`aa9f66e`** |
| **O quÃƒÂª** | **PDV:** Caixa / Consultar vendas / Fiado abrem overlay de novo Ã‚Â· **Perf:** busca Entrada NF etapa 2 + Cadastro ERP (motor lite, cache 45 s) |
| **Migrate** | Nenhuma |
| **Validar loja** | Ctrl+F5 badge **v6.08** Ã‚Â· PDV topbar Ã¢â€ â€™ overlay laranja Ã‚Â· Entrada NF etapa 2 + Cadastro busca mais rÃƒÂ¡pida |

### Ã¢Å“â€¦ Deploy loja **v6.07** Ã¢â‚¬â€ PDV botÃƒÂµes Pagar + ÃƒÂ­cone entrega (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ *manda* + senha **`99738595`** |
| **Rollback** | Tag **`producao-rollback-v6.06-20260702`** @ **`d6c271f`** |
| **Git** | **`teste` `0d5c094`** Ã‚Â· **`producao` `aa9f66e`** |
| **O quÃƒÂª** | Subtotal PDV: **Pagar** + F7 Ã‚Â· **Entrega** = ÃƒÂ­cone caminhÃƒÂ£o + F3 Ã‚Â· sem quebra de linha |
| **Validar loja** | Ctrl+F5 badge **v6.07** Ã‚Â· botÃƒÂµes subtotal em uma linha |

### Ã¢Å“â€¦ Deploy loja **v6.05Ã¢â‚¬â€œv6.06** Ã¢â‚¬â€ perf telas + Voltar PDV + overlay Caixa + scripts Win (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ *pode subir tudo para produÃƒÂ§ÃƒÂ£o* + senha **`99738595`** |
| **Rollback** | Tag **`producao-rollback-v6.04-20260702`** @ **`84541c2`** |
| **Git** | **`teste` `7b48e21`** Ã‚Â· **`producao` `ea5f972`** + docs **`d6c271f`** Ã‚Â· badge loja **`6.06`** |
| **O quÃƒÂª** | **Perf:** facetas gestÃƒÂ£o cache 15 min + adiado no load Ã‚Â· CP bootstrap sem totais Ã‚Â· staleRefresh +700 ms Ã‚Â· cap scan PG Ã‚Â· entrada NF rascunhos sem 503 Mongo Ã‚Â· **UX:** Ã‚Â«Voltar PDV F1Ã‚Â» some em GestÃƒÂ£o Ã‚Â· **Bug:** overlay Caixa no PDV nÃƒÂ£o abre BI Ã‚Â· **Ops:** scripts atalhos Chrome Win (`criar_atalhos` + `remover_apps`) |
| **Migrate** | Nenhuma |
| **Validar loja** | Ctrl+F5 badge **v6.06** Ã‚Â· **PDV** Caixa Ã¢â€ â€™ menu caixa (nÃƒÂ£o BI) Ã‚Â· **GestÃƒÂ£o** sem F1 Ã‚Â· gestÃƒÂ£o/cadastro/lanÃƒÂ§amentos/entrada NF mais rÃƒÂ¡pidos |

### Ã°Å¸Ââ€º PDV overlay Caixa abre BI GestÃƒÂ£o (01/07 Ã‚Â· Renan) Ã¢â‚¬â€ **Ã¢Å“â€¦ v6.05**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | App **SisVale PDV** Ã‚Â· botÃƒÂ£o **Caixa** abre overlay laranja mas iframe mostra **GestÃƒÂ£o EstratÃƒÂ©gica (BI)** Ã‚Â· persiste ao fechar/reabrir Ã‚Â· sÃƒÂ³ **Ctrl+F5** corrige |
| **Causa** | Dois apps Chrome compartilham `localStorage agro_app_role_v1` Ã‚Â· janela GestÃƒÂ£o manda `agro-open-inapp-tab` / foco BI Ã‚Â· overlay aceitava `/dashboard/` no iframe |
| **Fix** | `agro_dual_window.js` + `agro_pdv_overlay.js` (ver deploy **v6.05**) |
| **Deploy** | **Ã¢Å“â€¦ loja v6.05** |
| **Validar** | Ctrl+F5 no **PDV** Ã‚Â· Caixa fechado Ã¢â€ â€™ overlay **menu caixa** (nÃƒÂ£o BI) Ã‚Â· fechar/reabrir 3Ãƒâ€” OK Ã‚Â· com **GestÃƒÂ£o** aberta ao lado, Caixa continua certo |

### Ã°Å¸Ââ€º PDV topbar Ã¢â‚¬â€ Caixa / Vendas / Fiado nÃƒÂ£o abrem overlay (02/07 Ã‚Â· Renan) Ã¢â‚¬â€ **fix teste v6.51 Ã‚Â· loja ainda v6.09**

**Sintoma pÃƒÂ³s-v6.08/v6.09:** clique normal nÃƒÂ£o abre overlay; abrir em nova guia carrega `/caixa/` mas submenus tambÃƒÂ©m nÃƒÂ£o respondem.

**Causa:** (1) `readAppRole` / `window.name` PDV em URL errada Ã‚Â· (2) **`agro_asset_v` ausente** Ã¢â€ â€™ cache `?v=1` Ã‚Â· (3) `isGestaoHost()` amplo (`dualFlagOn && !isPdvPath`) montava shell em `/caixa/`.

**CorreÃƒÂ§ÃƒÂ£o v6.51:** `context_processors.agro_asset_v` Ã‚Â· `agro_dual_window.js` Ã¢â‚¬â€ `isGestaoHost` estrito Ã‚Â· `openPdvPanel`/`navigateGestao`/roteador com fallbacks Ã‚Â· `isPdvHost` + `isPdvPath` no router.

### Ã°Å¸â€Â§ Perf multi-tela Ã¢â‚¬â€ **Ã¢Å“â€¦ v6.05**

| Tela | API / view principal | Fix **v6.05** |
| ---- | -------------------- | ------------- |
| **GestÃƒÂ£o** | `api_produtos_gestao_facetas` | Cache 15 min Ã‚Â· facetas sÃƒÂ³ ao abrir Ã‚Â«Filtros avanÃƒÂ§adosÃ‚Â» |
| **LanÃƒÂ§amentos CP** | bootstrap + `/api/lancamentos/` | Bootstrap `skip_totais` Ã‚Â· staleRefresh +700 ms Ã‚Â· cap scan PG |
| **Entrada NF** | `api/entrada-nota/rascunhos/` | Sem 503 Mongo quando rascunho PG |

**Medir amanhÃƒÂ£ (Win10 loja, DevTools Ã¢â€ â€™ Network, Ctrl+F5):**

| URL | Esperado pÃƒÂ³s-fix (ordem de grandeza) |
| --- | ------------------------------------ |
| `api_produtos_gestao_lista?pagina=1` | **&lt; 1,5 s** (PG+ledger) |
| `api_produtos_gestao_facetas` | **0 ms** no load (adiado); **&lt; 2 s** 1Ã‚Âª vez ao abrir filtros (cache 15 min) |
| `api/produtos/cadastro/?pagina=1` | **&lt; 2 s** |
| `lancamentos/contas-pagar/` (document) | Lista visÃƒÂ­vel **&lt; 1 s** (bootstrap); API sync **+0,7 s** |
| `/api/lancamentos/?tipo=pagar&Ã¢â‚¬Â¦venc_de=HOJE` | **&lt; 1,5 s** PG |
| `api/entrada-nota/rascunhos/` | **&lt; 2 s** (sem 503 Mongo) |
| `/api/buscar/?q=Ã¢â‚¬Â¦&entrada_nfe=1` | **&lt; 1 s** busca 2+ chars |

### Ã°Å¸â€Â§ Perf busca produtos Ã¢â‚¬â€ Entrada NF etapa 2 + Cadastro ERP (02/07 Ã‚Â· Renan) Ã¢â‚¬â€ **Ã¢Å“â€¦ v6.08**

| Tela | O quÃƒÂª | Fix |
| ---- | ----- | --- |
| **Entrada NF Ã‚Â· produtos** | `/api/buscar/?entrada_nfe=1` ainda pesado (Mongo inteiro + ajustes estoque) | Motor **lite** (projeÃƒÂ§ÃƒÂ£o slim, regex cap 80, sem ajustes PIN) Ã‚Â· `limit=48` no front |
| **Cadastro ERP Ã‚Â· busca** | Lista demora ao digitar | Cache 45 s por termo Ã‚Â· limite 64 Ã‚Â· debounce 240 ms Ã‚Â· sem badge ERP pendentes durante busca |

**Arquivos:** `views.py` Ã‚Â· `motor_busca_unificado_util.py` Ã‚Â· `entrada_nota.html` Ã‚Â· `cadastro_erp_panel.js`

**Validar:** Ctrl+F5 Ã‚Â· Entrada NF etapa 2 Ã¢â‚¬â€ buscar Ã‚Â«raÃƒÂ§ÃƒÂ£oÃ‚Â» / GM Ã‚Â· Cadastro Ã¢â‚¬â€ mesma busca Ã‚Â· DevTools &lt; ~1 s na API

### Ã°Å¸â€Â§ Fix Ã¢â‚¬â€ Ã‚Â«Voltar ao PDV F1Ã‚Â» some em GestÃƒÂ£o Ã¢â‚¬â€ **Ã¢Å“â€¦ v6.05**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | App **SisVale GestÃƒÂ£o** (`agro_app_role=gestao`) ainda mostrava botÃƒÂ£o **VOLTAR AO PDV F1** no topo (ex.: Cadastro Produtos) Ã¢â‚¬â€ v6.00 sÃƒÂ³ escondia no BI |
| **Causa** | `_pdv_voltar_link.html` / F1 global nÃƒÂ£o checavam papel GestÃƒÂ£o; sÃƒÂ³ `dashboard_gerencial.html` usava `data-agro-hide-pdv` |
| **Fix** | `_agro_consulta_ui.html` Ã¢â‚¬â€ script + CSS global `data-agro-hide-pdv` quando `agro_app_role=gestao` ou `agro_inapp_embed` Ã‚Â· `_pdv_voltar_link.html` Ã¢â‚¬â€ classe `agro-pdv-voltar-link` + skip server-side gestÃƒÂ£o Ã‚Â· `_atalho_voltar_pdv.html` Ã¢â‚¬â€ F1 ignorado em gestÃƒÂ£o Ã‚Â· `mobile_ajuste.html` Ã¢â‚¬â€ mesma classe |
| **Arquivos** | `_agro_consulta_ui.html`, `_pdv_voltar_link.html`, `_atalho_voltar_pdv.html`, `mobile_ajuste.html` |
| **Deploy** | **Ã¢Å“â€¦ loja v6.05** |
| **Validar** | Ctrl+F5 no atalho **GestÃƒÂ£o** Ã¢â€ â€™ Cadastro ERP, Compras, LanÃƒÂ§amentos, RH Ã¢â‚¬â€ **sem** link F1 no topo Ã‚Â· atalho **PDV** mantÃƒÂ©m F1 onde existia |

### Ã¢Å“â€¦ Deploy loja **v6.04** Ã¢â‚¬â€ PIN descanso + cadastro 1234 online (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ *Pin manda tambem* + senha **`99738595`** |
| **Rollback** | Tag **`producao-rollback-v6.03-20260701`** @ **`e4232e8`** |
| **Git produÃƒÂ§ÃƒÂ£o (prÃƒÂ©/post)** | **`e4232e8`** Ã¢â€ â€™ **`84541c2`** |
| **Cherry-picks** | **`872bf96`** (PIN ÃƒÂºnico servidor) + **`135e785`** (1234 cadastro inicial online) |
| **O quÃƒÂª** | Modo descanso LanÃƒÂ§amentos (~3 min idle) Ã‚Â· PIN **sempre no servidor** Ã‚Â· **1234** abre cadastro 1Ã‚Âª vez Ã‚Â· RH Operadores continua gestÃƒÂ£o |
| **Migrate** | Nenhuma Ã‚Â· drift **base/estoque** prÃƒÂ©-existente |
| **DependÃƒÂªncia** | **`872bf96`** necessÃƒÂ¡rio antes de **`135e785`** |

**Validar loja:** Ctrl+F5 Ã‚Â· badge **v6.04** Ã‚Â· LanÃƒÂ§amentos idle ~3 min Ã¢â€ â€™ screensaver PIN Ã‚Â· operador sem PIN: **1234** Ã¢â€ â€™ cadastro Ã¢â€ â€™ PIN definitivo Ã‚Â· RH Ã¢â€ â€™ Operadores pins OK Ã‚Â· 2 PCs mesmo PIN.

### Ã¢Å“â€¦ Deploy loja **v6.00** Ã¢â‚¬â€ 2 janelas Chrome PDV/GestÃƒÂ£o (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ *pode enviar para produÃƒÂ§ÃƒÂ£o* + senha **`99738595`** |
| **Rollback** | Tag **`producao-rollback-v5.99-20260701`** @ **`81c485c`** |
| **Git produÃƒÂ§ÃƒÂ£o** | **`fe97096`** Ã‚Â· features **`bda42ca`** |
| **Cherry-picks** | **`0dbd799`** Ã¢â€ â€™ **`0f77603`** (6 commits) Ã‚Â· **`c1f9970`** jÃƒÂ¡ estava **`771ad00`** |
| **O quÃƒÂª** | PDV e GestÃƒÂ£o em **2 atalhos Chrome** Ã‚Â· GestÃƒÂ£o **sem** guia PDV na sidebar Ã‚Â· **sem** PDV F1 no topo do BI Ã‚Â· overlay consultas ~95% Ã‚Â· InÃƒÂ­cio no PDV foca janela GestÃƒÂ£o |
| **ExcluÃƒÂ­do** | **0048** orÃƒÂ§amentos PG Ã‚Â· PIN descanso **`135e785`** *(subiu no pacote **v6.04**)* |
| **Migrate** | Nenhuma Ã‚Â· drift **base/estoque** prÃƒÂ©-existente (igual pacotes 1Ã¢â‚¬â€œ4) |

**Validar loja:** Ctrl+F5 Ã‚Â· atalho **GestÃƒÂ£o** (`agro_app_role=gestao`) Ã¢â€ â€™ BI/caixa **sem** botÃƒÂ£o PDV Ã‚Â· atalho **PDV** Ã¢â€ â€™ balcÃƒÂ£o dedicado Ã‚Â· `scripts/criar_atalhos_sistvale.ps1` se faltar `.lnk`.

**Atalhos na barra Windows (Renan 01/07):** apps Chrome (`chrome_proxy`).

| Estado Renan (Win11 dev) | Detalhe |
| --- | --- |
| **Apps instalados** | PDV **`mcbdcdbnbbfijbkpclihamnpahafeigl`** Ã‚Â· GestÃƒÂ£o/BI **`beilkmpkajkdhaejggnppapepjkgajjp`** (Chrome gravou label **SisVale InteligÃƒÂªncia de NegÃƒÂ³cio**) |
| **Barra OK** | Script detecta `*PDV*` / `*Intelig*` Ã‚Â· atalhos **SisVale PDV** + **SisVale Gestao** na barra |
| **Remocao forcada** | `remover_apps_chrome_sistvale.ps1 -FecharChrome` Ã‚Â· registro Windows Apps OK |
| **Fantasma `chrome://apps`** | Icone **SistVale** antigo pode ficar ate **fechar Chrome por completo** ou reiniciar PC Ã¢â‚¬â€ **pode ignorar** se **Instalar pagina como app** ja apareceu |

### Ã°Å¸â€œâ€¹ Pendente loja Ã¢â‚¬â€ **Win10 amanhÃƒÂ£ (02/07)** Ã¢â‚¬â€ 2 apps PDV + GestÃƒÂ£o na barra

**Objetivo:** em **cada PC Win10 da loja** (balcÃƒÂ£o, caixa, gestÃƒÂ£oÃ¢â‚¬Â¦), mesmo setup do Renan: **2 janelas separadas** na barra Ã¢â‚¬â€ **sem** icone globo do Chrome Ã‚Â· **sem** app antigo **SistVale** misturando abas.

**Antes:** Ctrl+F5 no site Ã‚Â· badge loja **v6.04+** Ã‚Â· Chrome atualizado.

**Por PC (repetir):**

| # | O quÃƒÂª |
| --- | --- |
| 1 | Abrir PowerShell na pasta do repo **ou** copiar `scripts\criar_atalhos_sistvale.ps1` + `scripts\remover_apps_chrome_sistvale.ps1` para o PC |
| 2 | **Se existir app SistVale antigo** (menu so Ã‚Â«Abrir no appÃ¢â‚¬Â¦Ã‚Â»): `powershell -ExecutionPolicy Bypass -File .\scripts\remover_apps_chrome_sistvale.ps1 -FecharChrome` Ã‚Â· fechar Chrome Ã‚Â· ignorar fantasma em `chrome://apps` se **Instalar pagina como app** ja voltou |
| 3 | Abrir 2 janelas para instalar: `powershell -ExecutionPolicy Bypass -File .\scripts\criar_atalhos_sistvale.ps1 -BaseUrl "https://sistvale.com.br" -AbrirParaInstalar` |
| 4 | **Janela PDV** (`/pdv/`): menu Ã¢â€¹Â® Ã¢â€ â€™ **Instalar pagina como app** Ã¢â€ â€™ nome **SisVale PDV** |
| 5 | **Janela GestÃƒÂ£o** (BI): menu Ã¢â€¹Â® Ã¢â€ â€™ **Instalar pagina como app** Ã¢â€ â€™ nome **SisVale Gestao** *(Chrome pode gravar Ã‚Â«InteligÃƒÂªncia de NegÃƒÂ³cioÃ‚Â» Ã¢â‚¬â€ OK)* |
| 6 | Fixar barra: `powershell -ExecutionPolicy Bypass -File .\scripts\criar_atalhos_sistvale.ps1 -BaseUrl "https://sistvale.com.br" -FixarBarra -Desktop -LimparBarra` |
| 7 | **Win10:** se `-FixarBarra` nao aparecer na barra, arrastar `.lnk` da **Area de trabalho** ou **Iniciar Ã¢â€ â€™ SisVale** para a barra Ã‚Â· botao direito Ã¢â€ â€™ **Fixar na barra de tarefas** |
| 8 | Desfixar pins velhos: **SistVale**, **Consulta**, **Inteligencia** duplicada, icone **globo** (`chrome.exe`) |

**Validar no PC:**

| Atalho | Esperado |
| --- | --- |
| **SisVale PDV** | Abre **sÃƒÂ³ balcÃƒÂ£o** Ã‚Â· janela propria Ã‚Â· sem aba Chrome generica |
| **SisVale Gestao** | Abre **BI/caixa** Ã‚Â· **sem** botao PDV na lateral Ã‚Â· **sem** Ã‚Â«Voltar PDV F1Ã‚Â» no topo do BI |

**Notas:**

- **App-id e por maquina** Ã¢â‚¬â€ nao copiar IDs do PC do Renan; o script detecta sozinho apos instalar.
- **Perfil Chrome:** script usa perfil **Default** (Chrome normal da loja). Se a loja usar outro perfil, avisar antes.
- Scripts no repo: `scripts/criar_atalhos_sistvale.ps1` Ã‚Â· `scripts/remover_apps_chrome_sistvale.ps1`

**Comando rapido refazer barra:** `criar_atalhos_sistvale.ps1 -BaseUrl https://sistvale.com.br -FixarBarra -Desktop -LimparBarra`

### Ã¢Å“â€¦ Deploy loja **v5.77Ã¢â‚¬â€œv5.99** Ã¢â‚¬â€ pacotes 1Ã¢â‚¬â€œ4 cherry (01/07 noite)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ *pode mandar tudo para produÃƒÂ§ÃƒÂ£o* (loja fechada); **sem** merge `teste` inteiro |
| **Rollback** | Tag **`producao-rollback-v5.76-20260701`** @ **`7593664`** (HEAD anterior) |
| **Git produÃƒÂ§ÃƒÂ£o** | **`81c485c`** @ `producao` Ã‚Â· features **`594c1cd`** Ã‚Â· rollback anterior **`7593664`** |
| **Pacote 1** | Caixa overlay 2 apps, PDV lateral F7/F3 (**sem** migraÃƒÂ§ÃƒÂ£o **0048** orÃƒÂ§amentos PG) |
| **Pacote 2** | Retiradas Excel + operador + hÃƒÂ­fen ASCII |
| **Pacote 3** | RH ficha limpa, cancelar pagamento duplicado, sync CP, vale caixaÃ¢â€ â€™folha (**`ce775c2`** skip vazio Ã¢â‚¬â€ jÃƒÂ¡ na loja) |
| **Pacote 4** | Entregas pÃƒÂ³s-venda `venda_id` + fiado; painel sem rÃƒÂ³tulos ERP |
| **ExcluÃƒÂ­do teste (pacotes 1Ã¢â‚¬â€œ4)** | **0048** orÃƒÂ§amentos PG, PIN **135e785** *(subiu **v6.04**)*, dual window **0dbd799**, overlay **9896a90** *(subiu no pacote **v6.00**)* |
| **Migrate** | **Sem** 0048; `makemigrations --check` ainda aponta drift **base/estoque** (prÃƒÂ©-existente Ã¢â‚¬â€ nÃƒÂ£o gerado neste deploy) |
| **Render** | Push `producao` OK Ã‚Â· badge **v5.98** apÃƒÂ³s Ctrl+F5 |

**Validar ao voltar (checklist curto):** Ctrl+F5 Ã‚Â· **Caixa** overlay Menu/scroll Ã‚Â· **PDV** F7/F3 lateral Ã‚Â· **Retiradas** Excel + lista jun/2026 Ã‚Â· **RH** ficha + fechamento Igualar CP Ã‚Â· **Entregas** pÃƒÂ³s-venda fiado Ã‚Â· orÃƒÂ§amentos PG **nÃƒÂ£o** subiram (comportamento legado).

**Rollback (se der problema):** `git checkout producao-rollback-v5.76-20260701` Ã¢â€ â€™ push `producao` (ou redeploy tag no Render).

### SÃƒÂ³ no **teste** (loja **v6.04** nÃƒÂ£o tem)

| Item | Detalhe |
| ---- | ------- |
| **OrÃƒÂ§amentos PG** | Migration **`0048`** Ã‚Â· API `/api/pdv/orcamentos/` Ã‚Â· sync multi-PC Ã‚Â· GMORC bootstrap |

### Renan Ã¢â‚¬â€ desvinculaÃƒÂ§ÃƒÂ£o Mongo (resumo)

**API ERP cortada** Ã¢Å“â€¦ Ã‚Â· **~85 %** operaÃƒÂ§ÃƒÂ£o jÃƒÂ¡ Postgres Ã‚Â· Mongo restante = RH salÃƒÂ¡rio CP, calendÃƒÂ¡rio LanÃƒÂ§amentos, BI hÃƒÂ­brido, etc. Ã¢â‚¬â€ **nÃƒÂ£o trava PDV** (ver Ã‚Â§4.15).


### Ã°Å¸Ââ€º RH ficha Ã¢â‚¬â€ botÃƒÂ£o Ã‚Â«Abrir folhaÃ‚Â» sumia (01/07 Ã‚Â· teste)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Renan (Geraldo) Ã¢â‚¬â€ instruÃƒÂ§ÃƒÂ£o Ã‚Â«Caminho principalÃ‚Â» sem botÃƒÂ£o; **vale no mÃƒÂªs** funcionou |
| **Causa** | BotÃƒÂ£o sÃƒÂ³ no `{% empty %}` da lista Ã¢â‚¬â€ some quando jÃƒÂ¡ hÃƒÂ¡ fechamentos antigos |
| **Fix** | **Atalhos** + rodapÃƒÂ© da seÃƒÂ§ÃƒÂ£o **3** sempre visÃƒÂ­veis Ã‚Â· se mÃƒÂªs corrente jÃƒÂ¡ existe Ã¢â€ â€™ **Ir para folha MM/AAAA** |
| **Arquivos** | `funcionario_ficha.html` Ã‚Â· `rh/views.py` Ã‚Â· `rh_help_agents.html` |
| **OperaÃƒÂ§ÃƒÂ£o** | MÃƒÂªs novo = **Abrir folha** (atalho ou Ã‚Â§3) Ã‚Â· mÃƒÂªs passado = link na lista ou vale/caixa na data |

### Ã¢Å“â€¦ Deploy loja **v5.73Ã¢â‚¬â€œv5.75** Ã¢â‚¬â€ RH folha UX + reabrir (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã‚Â· produÃƒÂ§ÃƒÂ£o + senha **`99738595`** Ã‚Â· cherry **isolado** **`54aaa32`** Ã‚Â· **`a10faa1`** Ã‚Â· **`b825230`** (**sem** PDV/orÃƒÂ§amentos/caixa overlay) |
| **Pacote** | Lista fechamentos **todos status** Ã‚Â· tela fechamento **limpa** (ajuda no **?**) Ã‚Â· **Reabrir** mantÃƒÂ©m valor pago Ã‚Â· detalhe sem recalc a cada F5 |
| **Validado loja** | Igualar CP Ã‚Â· parcial CP Ã‚Â· caixa SalÃƒÂ¡rios Ã‚Â· Reabrir (bug Pago sumindo) |
| **PÃƒÂ³s-deploy** | Render ~2Ã¢â‚¬â€œ5 min Ã‚Â· **Ctrl+F5** RH fechamento |

### Ã¢Å“â€¦ Deploy loja **v5.71Ã¢â‚¬â€œv5.72** Ã¢â‚¬â€ hotfix migrate RH 0005 (01/07)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Deploy **`61e19c2`** (v5.70) falhou no **migrate** Ã¢â‚¬â€ `rh.0005` apontava `base.0010` inexistente na loja |
| **Fix** | DependÃƒÂªncia Ã¢â€ â€™ **`base.0009`** Ã‚Â· **`a138625`** produÃƒÂ§ÃƒÂ£o |
| **PÃƒÂ³s-deploy** | Render ~2Ã¢â‚¬â€œ5 min Ã‚Â· **Ctrl+F5** |

### Ã¢Å“â€¦ Deploy loja **v5.70** Ã¢â‚¬â€ RH pagamento salÃƒÂ¡rio CP + caixa (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã‚Â· *banana manda produÃƒÂ§ÃƒÂ£o* + senha **`99738595`** Ã‚Â· cherry **isolado** **`5434de0`** (**sem** PDV/outros do teste) |
| **Git produÃƒÂ§ÃƒÂ£o** | Cherry-pick **`5434de0`** Ã¢â€ â€™ **`producao`** |
| **O quÃƒÂª** | `PagamentoSalarioFuncionario` Ã‚Â· Pago sync = **vales + pagamentos** Ã‚Â· baixa CP Ã¢â€ â€™ RH Ã‚Â· caixa **SalÃƒÂ¡rios (pagamento folha)** |
| **Migrate** | **`0005_pagamento_salario_funcionario`** Ã¢â‚¬â€ Render roda no deploy |
| **PÃƒÂ³s-deploy** | Render ~2Ã¢â‚¬â€œ5 min Ã‚Â· **Ctrl+F5** Ã‚Â· baixa CP Ã¢â€ â€™ Ã‚Â«IgualarÃ‚Â» **nÃƒÂ£o apaga** Ã‚Â· caixa plano **SalÃƒÂ¡rios** |

### Ã¢Å“â€¦ Deploy loja **v5.67Ã¢â‚¬â€œv5.68** Ã¢â‚¬â€ RH folha espelha CP Postgres (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã‚Â· cherry-pick isolado + senha **`99738595`** |
| **Git produÃƒÂ§ÃƒÂ£o** | **`77bbd14`** Ã¢â‚¬â€ cherry-pick **`ce775c2`** (2 arquivos: espelho MongoÃ¢â€ â€™PG apÃƒÂ³s sync folha) |
| **O quÃƒÂª** | Ã‚Â«Igualar ao que estÃƒÂ¡ na folhaÃ‚Â» / Ã‚Â«Criar ou atualizarÃ‚Â» passa a refletir vales e descontos no CP |
| **Risco** | **Baixo** Ã¢â‚¬â€ sÃƒÂ³ 1 linha PG do tÃƒÂ­tulo sincronizado; nÃƒÂ£o apaga outros lanÃƒÂ§amentos |
| **PÃƒÂ³s-deploy** | Render ~2Ã¢â‚¬â€œ5 min Ã‚Â· Geraldo jun/2026: passo 1 Salvar e recalcular Ã¢â€ â€™ passo 2 Igualar Ã¢â€ â€™ **Ctrl+F5** CP |

### Ã¢Å“â€¦ Deploy loja **v5.66** Ã¢â‚¬â€ hotfix retiradas Adiantamento (01/07)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | `/caixa/retiradas/` jun/2026 + plano **Adiantamento** Ã¢â€ â€™ **500** (v5.65) |
| **Causa** | Cherry-pick vales perdeu `_op_exib` no merge Ã¢â‚¬â€ sÃƒÂ³ quebrava ao listar **ValeFuncionario** |
| **Fix** | Helpers inline em `caixa_retiradas_util.py` Ã‚Â· **`1c46fc7`** cherry-pick **`producao`** |
| **Validar** | Ctrl+F5 Ã‚Â· mesmo filtro Ã‚Â· lista vales jun/2026 |

### Ã¢Å“â€¦ Deploy loja **v5.65** Ã¢â‚¬â€ NF busca + retiradas vales (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã‚Â· *enviar para produÃƒÂ§ÃƒÂ£o* + senha **`99738595`** Ã‚Â· cherry-pick isolado (**sem** merge `teste`) |
| **Git** | **`de825f3`** (vales) Ã‚Â· **`f1453c3`** (NF) Ã‚Â· push **`producao`** |
| **Pacote** | NF passo 2 + vales RH no histÃƒÂ³rico retiradas |
| **Incidente** | Filtro Adiantamento 500 Ã¢â‚¬â€ corrigido **v5.66** |

### Ã¢Å“â€¦ Deploy loja **v5.62** Ã¢â‚¬â€ fix fiado PDV/F8 (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã‚Â· *manda direto produÃƒÂ§ÃƒÂ£o* + senha **`99738595`** Ã‚Â· teste sem fiado (zerado Ã¢â‚¬â€ nÃƒÂ£o dava para validar) |
| **Git** | Merge **`teste`Ã¢â€ â€™`producao`** **`545aad3`** Ã‚Â· fiado **`6875a3a`** + auditoria **`47c20e0`** |
| **Pacote** | Fiado: gestÃƒÂ£o/F8 lista tÃƒÂ­tulos por **nome** (igual grade) Ã‚Â· comando `fiado_auditar_cadastros_duplicados` |
| **PÃƒÂ³s-deploy** | Render ~2Ã¢â‚¬â€œ5 min Ã‚Â· **Ctrl+F5** Ã‚Â· **1 guia** Ã‚Â· Queila: abrir gestÃƒÂ£o pelo PDV = **todos** tÃƒÂ­tulos Ã‚Â· total = lateral **R$ 435,66** |
| **Auditoria** | Shell loja: `python manage.py fiado_auditar_cadastros_duplicados` |

### Ã¢Å“â€¦ Deploy loja **v5.61** Ã¢â‚¬â€ perf busca PDV (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã‚Â· correÃƒÂ§ÃƒÂ£o lentidÃƒÂ£o + senha **`99738595`** |
| **Git** | Cherry-pick **`bb5f1b6`** Ã¢â€ â€™ **`producao`** **`9fb0385`** Ã‚Â· **sem** fiado v5.58 |
| **Pacote** | Cache local PDV Ã‚Â· GM debounce Ã‚Â· busca wizard menos Mongo Ã‚Â· promo cache 90 s |
| **Arquivos** | `pdv_wizard.js` Ã‚Â· `motor_busca_unificado_util.py` Ã‚Â· `views.py` |
| **PÃƒÂ³s-deploy** | **Ctrl+F5** Ã‚Â· **1 guia** Ã‚Â· nÃƒÂ£o abrir BI+caixa+PDV juntos |

### Ã°Å¸â€Â´ INCIDENTE loja Ã¢â‚¬â€ tela cinza / fila **01/07** (histÃƒÂ³rico)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cinza ao abrir Ã‚Â· caixa/vendas/fiado lentos Ã‚Â· **1 worker** + vÃƒÂ¡rias guias |
| **Causa** | Fila Gunicorn (nÃƒÂ£o crash) Ã‚Â· logs tudo 200 |
| **OperaÃƒÂ§ÃƒÂ£o** | Fechar guias extras Ã‚Â· avaliar **2 workers** Render |

### Ã°Å¸Å¸Â  Fiado Ã¢â‚¬â€ sÃƒÂ³ 3 tÃƒÂ­tulos pelo PDV/F8 **v5.58Ã¢â€ â€™v5.62 loja** (01/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Queila Ã‚Â· lateral **R$ 435,66** Ã‚Â· gestÃƒÂ£o por cadastro **19+3** Ã‚Â· PDV/F8 sÃƒÂ³ **3** (R$ 25,60) |
| **Causa** | Cadastros duplicados mesmo nome Ã‚Â· filtro sÃƒÂ³ `cliente_agro_id` no atalho PDV |
| **Fix** | `_q_titulos_cliente_gestao` + F8 `_fiado_resumo` + `fiado_gestao.js` por **nome** |
| **Queila** | Ã‚Â«(nÃƒÂ£o usar mais)Ã‚Â» **R$ 410** + Ã‚Â«Hinnen aÃ‚Â» **R$ 25,60** = **R$ 435,66** |
| **Auditoria loja** | `python manage.py fiado_auditar_cadastros_duplicados` Ã‚Â· `--json` |

### Ã¢Å“â€¦ Deploy loja **v5.56** Ã¢â‚¬â€ fix Indisp. F8 (30/06)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã‚Â· *manda* + senha **`99738595`** |
| **Git** | `teste` Ã¢â€ â€™ **`producao`** fast-forward **`3467ea0`Ã¢â€ â€™`59ef94d`** Ã‚Â· push **`producao`** |
| **Pacote** | Fix **Indisp.** Ã¢â‚¬â€ catÃƒÂ¡logo Postgres (interno/barras/variaÃƒÂ§ÃƒÂµes) alinhado ao PDV Ã‚Â· fix core **`b0df2a7`** |
| **PÃƒÂ³s-deploy** | Render ~2Ã¢â‚¬â€œ5 min Ã‚Â· **Ctrl+F5** PDV Ã‚Â· badge **v5.56** Ã‚Â· F8 Queila Ã¢â€ â€™ **+1** nos itens que eram Indisp. |

### Ã°Å¸Å¸Â  F8 Ã‚Â«Indisp.Ã‚Â» na loja Ã¢â‚¬â€ fix catÃƒÂ¡logo Postgres **v5.55** (30/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Loja **v5.53** Ã‚Â· F8 Queila Ã‚Â· Ã‚Â«Itens mais compradosÃ‚Â» com **Indisp.** (ex. areia, sachÃƒÂª, raÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ cÃƒÂ³digos **2580**, **2236**, **818**Ã¢â‚¬Â¦) |
| **Causa** | Checagem **v5.51** sÃƒÂ³ `codigo_nfe` + Mongo Ã‚Â· na loja catÃƒÂ¡logo = **Postgres** (`codigo_interno`, barras, variaÃƒÂ§ÃƒÂµes) Ã¢â‚¬â€ vendas antigas gravam **cÃƒÂ³digo interno**, nÃƒÂ£o GM |
| **Fix v5.55** | `codigos_gm_ativos_no_catalogo` alinhado ao PDV: overlay (nfe+barras) Ã‚Â· **Produto** (nfe+interno+barras) Ã‚Â· **ProdutoMarcaVariacaoAgro** Ã‚Â· Mongo `index_codigos`/barras |
| **Arquivo** | `relacionamento_historico_erp_util.py` |
| **Deploy teste** | OK **`b0df2a7`** |
| **Deploy loja** | **`59ef94d`** **v5.56** |

### Ã¢Å“â€¦ Deploy loja **v5.53** (30/06)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã‚Â· *manda produÃƒÂ§ÃƒÂ£o* + senha **`99738595`** |
| **Git** | `teste` Ã¢â€ â€™ **`producao`** fast-forward **`8ff62ca`Ã¢â€ â€™`3467ea0`** Ã‚Â· push **`producao`** |
| **Pacote** | F8 perf (histÃƒÂ³rico paginado Ã‚Â· lazy ciclo/cross) Ã‚Â· **FL-042** (migration **0047** + comandos import/revert/probe) Ã‚Â· alertas aba Resumo/Ciclo **v5.52** Ã‚Â· Indisp. catÃƒÂ¡logo PG/Mongo **v5.51** Ã‚Â· fix perf batch Mongo **v5.53** |
| **PÃƒÂ³s-deploy loja** | Aguardar Render ~2Ã¢â‚¬â€œ5 min Ã‚Â· **Ctrl+F5** PDV Ã‚Â· conferir badge **v5.53** Ã‚Â· migration **0047** (Render costuma rodar sozinha) |
| **Import ERP na loja** | **NÃƒÂ£o** veio no deploy Ã¢â‚¬â€ dados histÃƒÂ³rico ERP sÃƒÂ³ no **teste** (`erp-hist-teste-3`). Loja: import manual no shell **sÃƒÂ³** quando Renan decidir (mesmo fluxo FL-042) |

### Ã°Å¸Å¸Â¢ Staging perf F8 Ã¢â‚¬â€ resolvido **v5.53** (30/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Render teste Ã‚Â«sÃƒÂ³ carregandoÃ‚Â» apÃƒÂ³s **v5.52** Ã‚Â· Gunicorn workers bloqueados |
| **v5.52 culpado?** | **NÃƒÂ£o** Ã¢â‚¬â€ sÃƒÂ³ JS (`pdv_relacionamento.js` alertas aba) + VERSION; **red herring** no timing |
| **Causa** | **v5.51** `codigos_gm_ativos_no_catalogo` (Mongo fallback) chamado **por venda** em `_serialize_venda_historico_*` Ã‚Â· F8 inicial = 12 vendas Ã¢â€ â€™ **12+ roundtrips Mongo** Ã‚Â· pior apÃƒÂ³s import **erp-hist-teste-3** (4309 vendas / 7501 itens) |
| **Fix v5.53** | Batch ÃƒÂºnico em `_historico_vendas_paginado` (todos codigos da pÃƒÂ¡gina) Ã‚Â· serializers aceitam `ativos_it` opcional Ã‚Â· `max_time_ms=8000` no find `DtoProduto` |
| **Arquivos** | `relacionamento_cliente_util.py` Ã‚Â· `relacionamento_historico_erp_util.py` |
| **Deploy teste** | OK **v5.53** Ã‚Â· Renan validou F8 rÃƒÂ¡pido |

### Ã°Å¸Â§Â­ Dois trilhos Ã¢â‚¬â€ **nÃƒÂ£o misturar** (Renan Ã‚Â· 30/06)

| Trilho | O quÃƒÂª | Onde testar | PrÃƒÂ³ximo passo |
| ------ | ----- | ----------- | ------------- |
| **A Ã¢â‚¬â€ F8 rÃƒÂ¡pido** | Modal abre sem travar Ã‚Â· histÃƒÂ³rico 12 + Carregar mais Ã‚Â· ciclo/cross lazy | PDV teste Ã‚Â· Ctrl+F5 Ã‚Â· badge **v5.48** Ã‚Â· F8 | Renan valida abertura rÃƒÂ¡pida |
| **B Ã¢â‚¬â€ Import ERP (FL-042)** | Mongo vendas Ã¢â€°Â¤26/05 Ã¢â€ â€™ Postgres Ã‚Â· merge no F8 | Shell Render teste | Live **v5.48** Ã¢â€ â€™ `relacionamento_import_historico_erp --lote erp-hist-teste-2` Ã‚Â· **Itens importados > 0** |

**Feito:** lote `erp-hist-teste-1` **revertido** (4309 cabeÃƒÂ§alhos vazios). **Commit teste:** `bdea194` **v5.48** (trilho A + fix join trilho B).

### F8 Relacionamento Ã¢â‚¬â€ perf abertura Ã‚Â· **teste v5.48**

| Item | Detalhe |
| ---- | ------- |
| **Problema** | F8 ficava em Ã‚Â«Carregando histÃƒÂ³ricoÃ¢â‚¬Â¦Ã‚Â» Ã¢â‚¬â€ API montava tudo de uma vez (`cross_sell` scan 8000 itens, ciclo, 500 vendas + merge 12 ERP) |
| **Fix** | Carga inicial **leve**: resumo + fiado (badge) + top produtos + **12 vendas** + pets/extras |
| **HistÃƒÂ³rico** | PÃƒÂ¡gina **12** vendas Ã‚Â· botÃƒÂ£o **Carregar mais** Ã‚Â· `GET ?secao=historico&historico_offset=` |
| **Lazy** | **Ciclo raÃƒÂ§ÃƒÂ£o** e **Cross-sell** sÃƒÂ³ ao abrir a aba (`?secao=ciclo_racao` / `cross_sell`) |
| **Ciclo alertas Resumo** | Prefetch ciclo **em background** apÃƒÂ³s abrir (nÃƒÂ£o bloqueia) |
| **Alertas aba (Renan Ã‚Â· 30/06)** | Caixa vermelha saiu do **Resumo** Ã‚Â· **Resumo** (>60d sumido) e **Ciclo** (nÃ‚Âº atrasados) no tÃƒÂ­tulo da aba Ã¢â‚¬â€ estilo Fiado |
| **Top produtos** | Amostra **150** vendas (antes 500) Ã¢â‚¬â€ suficiente para ranking |
| **Arquivos** | `relacionamento_cliente_util.py` Ã‚Â· `views.py` Ã‚Â· `pdv_relacionamento.js` |
| **Deploy** | **teste v5.48** Ã‚Â· commit `bdea194` Ã‚Â· push 30/06 |

### PendÃƒÂªncias fila Ã¢â‚¬â€ **FL-043** Ã‚Â· **FL-044** (Renan Ã‚Â· 30/06)

| ID | P | Pedido |
| -- | - | ------ |
| **FL-043** | **P2,8** | BotÃƒÂ£o desconto na baixa do fiado |
| **FL-044** | **P2,9** | Desconto automÃƒÂ¡tico funcionÃƒÂ¡rio (% prÃƒÂ©-definida) Ã¢â‚¬â€ provÃƒÂ¡vel junto **FL-001** (preÃƒÂ§o Ãƒâ€” forma ou grupo cliente) |

### FL-042 fix **v5.47** Ã¢â‚¬â€ dry-run deu 0 importÃƒÂ¡veis (Renan Ã‚Â· 30/06)

| Item | Detalhe |
| ---- | ------- |
| **Bug** | Leitura Mongo **sem** ClienteID/nome (projeÃƒÂ§ÃƒÂ£o slim) Ã¢â€ â€™ 6236 Ã‚Â«sem cliente AgroÃ‚Â» |
| **Fix** | ProjeÃƒÂ§ÃƒÂ£o completa + match ID variantes + ponte DtoPessoa + nome na venda |
| **Consumidor** | **CONSUMIDOR NÃƒÆ’O IDENTIFICADO** Ã¢â€ â€™ **ignorado** (contador `vendas_consumidor`) |
| **PrÃƒÂ©-import** | Se Ã‚Â«sem cliente AgroÃ‚Â» alto Ã¢â€ â€™ **`sincronizar_clientes_agro`** no teste |
| **PrÃƒÂ³ximo** | Import real teste Ã¢â€ â€™ validar F8 |

**Dry-run v5.47 OK (Renan Ã‚Â· 30/06):** **4309** importÃƒÂ¡veis Ã‚Â· **1264** consumidor ignorado Ã‚Â· **663** sem cliente Agro Ã‚Â· **876** clientes Ã‚Â· 6236 no corte.

**Sync clientes teste:** **1473 criados** Ã‚Â· **394** tel duplicado (ok).

**Import teste:** `python manage.py relacionamento_import_historico_erp --lote erp-hist-teste-1`

**Import real v1 (Renan Ã‚Â· 30/06) Ã¢â‚¬â€ bug 0 itens:**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | **4309** vendas gravadas Ã‚Â· **0** itens Ã‚Â· warnings `naive datetime` |
| **Causa** | Join cabeÃƒÂ§alhoÃ¢â€ â€™`DtoVendaProduto`: lookup usava sÃƒÂ³ 1Ã‚Âª chave (`Id`/`_id`); linhas Mongo usam `NumeroVenda`/outra variante |
| **Fix** | `_venda_join_keys_header` + `_itens_raw_venda` (todas chaves H2) Ã‚Â· `make_aware` em `data_venda` Ã‚Â· contador `vendas_sem_itens` |
| **Reverter** | `python manage.py relacionamento_reverter_historico_erp --lote erp-hist-teste-1` |
| **Revert OK (Renan Ã‚Â· 30/06)** | Shell teste: **4309** vendas removidas Ã‚Â· lote **erp-hist-teste-1** apagado |
| **Import v2 (erp-hist-teste-2)** | Ainda **0 itens** Ã‚Â· **4309 vendas sem linha** Ã¢â‚¬â€ Mongo `DtoVendaProduto` nÃƒÂ£o casou |
| **Fix v5.49** | FK ampliado + embutidos Ã¢â‚¬â€ ainda 0 (probe: **14068** linhas Mongo, **0** match) |
| **Causa v5.50** | `_id` ObjectId no cabeÃƒÂ§alho Ã¢â€ â€™ query sÃƒÂ³ ObjectId; `VendaID` na linha = **string** (tipo BSON) |
| **Fix v5.50** | `str(ObjectId)` em scalars + probe amostra Ã¢â€°Â¤ corte ERP + `item_mongo_amostra` |
| **Deploy teste v5.48** | F8 perf (carga inicial + histÃƒÂ³rico paginado) + fix join chaves |
| **Reverter v2** | `relacionamento_reverter_historico_erp --lote erp-hist-teste-2` |
| **Probe v5.50 OK (Renan Ã‚Â· 30/06)** | `itens_mongo` 1/4/1 Ã‚Â· `VendaID_tipo: str` Ã‚Â· amostra Ã¢â€°Â¤26/05 |
| **Import v3 OK (Renan Ã‚Â· 30/06)** | Lote **`erp-hist-teste-3`** Ã‚Â· **4309** vendas Ã‚Â· **7501** itens Ã‚Â· **0** vendas sem linha Ã‚Â· **4988** itens sem GM ativo (+1 Ã‚Â«Indisp.Ã‚Â») |
| **Validar** | F8 cliente com histÃƒÂ³rico ERP Ã‚Â· aba HistÃƒÂ³rico Ã‚Â· badge ERP Ã‚Â· +1 sÃƒÂ³ se GM ativo |
| **Indisp. fix v5.51** | `codigos_gm_ativos_no_catalogo` Ã¢â€ â€™ overlay + **Produto PG** + **Mongo** ativo (GM4856 etc.) Ã‚Â· **sem** reimport |
| **Indisp. (Renan Ã‚Â· 30/06)** | Regra antiga: sÃƒÂ³ overlay `codigo_nfe` Ã¢â€ â€™ muitos Ã‚Â«Indisp.Ã‚Â» com nome igual no SisVale |

### FL-042 Ã¢â‚¬â€ HistÃƒÂ³rico ERP no F8 **v5.46+** Ã‚Â· **teste**

| Item | Detalhe |
| ---- | ------- |
| **Escopo** | Import **1Ãƒâ€”** Mongo `DtoVenda` Ã¢â€ â€™ Postgres **tabelas separadas** Ã‚Â· merge sÃƒÂ³ no **F8** |
| **Corte** | ERP **Ã¢â€°Â¤ 26/05/2026** Ã‚Â· SisVale F8 **Ã¢â€°Â¥ 27/05/2026** Ã‚Â· testes PDV antes 27/05 **ignorados** |
| **Migration** | **`0047`** `RelacionamentoHistoricoImportLoteAgro` + venda/item histÃƒÂ³rico |
| **Comandos** | `relacionamento_import_historico_erp --dry-run` Ã‚Â· import real Ã‚Â· `relacionamento_reverter_historico_erp --lote X` ou `--tudo` |
| **`.env`** | `AGRO_REL_HISTORICO_ERP=true` (false = F8 sem merge, dados ficam) |
| **SeguranÃƒÂ§a** | **NÃƒÂ£o** mexe `VendaAgro`, fiado, cashback, vale, caixa, estoque |
| **Produto sumiu** | Snapshot nome/cÃƒÂ³digo Ã‚Â· **+1** sÃƒÂ³ se GM ativo no cadastro Ã‚Â· senÃƒÂ£o Ã‚Â«Indisp.Ã‚Â» |
| **Pendente teste** | ApÃƒÂ³s deploy: **dry-run** no Shell Render teste Ã¢â€ â€™ Renan ok Ã¢â€ â€™ import real |

---

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ *pode subir* + senha **99738595** Ã‚Â· loja estava **v5.22.1** |
| **Pacote** | Merge **`teste` Ã¢â€ â€™ `producao`** Ã‚Â· Relacionamento F8 + pets Postgres + menu caixa v5.24 + fiado link |
| **VERSION** | **5.44** UTF-8 (corrigido merge Ã¢â‚¬â€ evita build UTF-16) |
| **Migration** | **`0046`** `relacionamento_extras_json` |
| **ContingÃƒÂªncia** | Manual Ã‚Â§3.2 (Zap + pausa ~2 min) Ã¢â‚¬â€ sem FL-038 cÃƒÂ³digo |
| **Loja** | Ctrl+F5 Ã‚Â· badge **v5.44** Ã‚Â· F8 + pet + venda R$ 1 |
| **Revert** | redeploy commit **`producao` anterior ao merge** |

### Ã°Å¸â€œÂ¦ PACOTE LOJA Ã¢â‚¬â€ **v5.44** Ã‚Â· **SUBIU** 30/06

**DecisÃƒÂµes Renan (30/06):**

| Tema | DecisÃƒÂ£o |
| ---- | ------- |
| **Deploy seguro** | **FL-038** Ã¢â‚¬â€ neste deploy usa **rotina manual** Ã‚Â§3.2 (Zap + janela calma + parar de finalizar venda ~2 min) Ã‚Â· cÃƒÂ³digo FL-038 **depois** (P2) |
| **Fila offline** | **NÃƒÂ£o agora** Ã¢â‚¬â€ registrado **FL-041** (P3, projeto grande) |
| **Pets** | **OpÃƒÂ§ÃƒÂ£o A** JSON Postgres (**v5.44**) Ã‚Â· **FL-040** tabela Pet (P3) |

**ContingÃƒÂªncia neste deploy (sem cÃƒÂ³digo FL-038):**

1. Escolher **janela calma** (evitar pico e fechamento de caixa).
2. **Zap loja:** *Ã‚Â«Atualizando o sistema ~2 min Ã¢â‚¬â€ **nÃƒÂ£o finalize venda** agora. Quem jÃƒÂ¡ clicou Finalizar, aguarde. Depois: Ctrl+F5 no PDV.Ã‚Â»*
3. Renan avisa operadores: **parar de vender** ~2 min.
4. Assistente: merge + push **`producao`** Ã¢â€ â€™ acompanhar Render atÃƒÂ© **Live**.
5. **Ctrl+F5** em todos os PDVs Ã‚Â· badge **v5.44** Ã‚Â· venda teste R$ 0,01 (opcional).
6. Conferir **migration** `0046` rodou (Render deploy log).

**Script:** `scripts/preparar_deploy_loja_v544.ps1` Ã¢â‚¬â€ valida `VERSION` UTF-8 e diff Ã‚Â· push sÃƒÂ³ com `-ExecutarPush` apÃƒÂ³s autorizaÃƒÂ§ÃƒÂ£o.

---

#### O que entra na loja (resumo operador)

**Ã°Å¸â€ºâ€™ PDV Ã¢â‚¬â€ Relacionamento com cliente (F8 / botÃƒÂ£o Hist.)**

| Novidade | Detalhe |
| -------- | ------- |
| **Atalho F8** | Modal do cliente selecionado (nÃƒÂ£o consumidor final) |
| **Aba Resumo** | 9 indicadores **numa linha** (visitas, ticket, cashback, vale, total, freq., ÃƒÂºltima visita, pets, WhatsApp) |
| **Top produtos** | Tabela + botÃƒÂ£o **+1 un.** no balcÃƒÂ£o (nÃƒÂ£o fecha o modal) |
| **Fiado** | SÃƒÂ³ na aba **Fiado** (laranja/vermelho + valor) Ã‚Â· botÃƒÂ£o **LanÃƒÂ§amentos** abre gestÃƒÂ£o fiado do cliente |
| **HistÃƒÂ³rico** | Itens mais comprados + ÃƒÂºltimas vendas (sem cards duplicados do Resumo) |
| **Pets / saÃƒÂºde / anotaÃƒÂ§ÃƒÂµes** | Salvos no **cadastro Postgres** Ã¢â‚¬â€ **qualquer caixa** vÃƒÂª (nÃƒÂ£o fica sÃƒÂ³ no PC) |
| **Risco** | **Baixo** Ã¢â‚¬â€ consulta + cadastro auxiliar Ã‚Â· **nÃƒÂ£o muda** preÃƒÂ§o, estoque nem finalizar venda |

**Ã°Å¸â€™Âµ Caixa**

| Item | Nota |
| ---- | ---- |
| **Menu CAIXA mais rÃƒÂ¡pido** | Se a loja **jÃƒÂ¡** estiver na v5.24 (`eb9fcc9`), **nÃƒÂ£o repete** Ã‚Â· se Render ainda v5.22, entra neste pacote |

**Ã°Å¸â€”â€žÃ¯Â¸Â Banco (Postgres)**

| Migration | O quÃƒÂª |
| --------- | ----- |
| **`0046`** | Campo `relacionamento_extras_json` em **Cliente Agro** (pets, lembretes, anotaÃƒÂ§ÃƒÂµes) |

**Ã¢ÂÅ’ Fora deste pacote**

| Item | Motivo |
| ---- | ------ |
| **FL-038 cÃƒÂ³digo** | SÃƒÂ³ documentaÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ deploy usa Zap + pausa manual |
| **FL-039 / FL-040** | Ficha `/clientes/` e tabela Pet Ã¢â‚¬â€ P3 |
| **FL-041** | Fila vendas offline Ã¢â‚¬â€ P3 |
| **Demais FL-00x** | NÃƒÂ£o testados neste ciclo |

---

#### Checklist pÃƒÂ³s-deploy loja (Renan)

| # | O quÃƒÂª |
| - | ----- |
| 1 | Badge **v5.44** (Ctrl+F5) |
| 2 | PDV Ã‚Â· cliente cadastrado Ã‚Â· **F8** Ã¢â€ â€™ Resumo + abas |
| 3 | Pet no F8 Ã¢â€ â€™ outro PC vÃƒÂª o mesmo pet |
| 4 | Cliente com fiado Ã¢â€ â€™ aba Fiado + lanÃƒÂ§amentos |
| 5 | Menu **CAIXA** abre rÃƒÂ¡pido |
| 6 | Venda normal R$ 1 (sanidade) |

**Revert:** redeploy commit anterior em `producao` (anotar hash pÃƒÂ³s-merge).

---

**Anterior:** **17/06/2026** Ã‚Â· produÃƒÂ§ÃƒÂ£o **12Ã¢â‚¬â€œ16/jun** (PDV, Caixa, Entrada NF, Etiquetas, Cadastro Ã¢â‚¬â€ lista guardada pelo Renan).

**Esta lista:** produÃƒÂ§ÃƒÂ£o **17/06 Ã¢â‚¬â€œ 29/06/2026** Ã‚Â· badge loja **v5.22 live** (v5.24 menu caixa **nÃƒÂ£o subiu**) Ã‚Â· gerada **29/06**.

**Formato:** **lista completa** (PDV + caixa + NF + gestÃƒÂ£o + financeiro + BI + compras) Ã¢â‚¬â€ **nÃƒÂ£o** usar versÃƒÂ£o Ã‚Â«sÃƒÂ³ balcÃƒÂ£oÃ‚Â».

**CadÃƒÂªncia Renan (29/06):** copiar e enviar no **WhatsApp da loja** **a cada 2 dias**, a partir de **29/06/2026**.

| PrÃƒÂ³ximos envios |
| --------------- |
| **29/06** (inÃƒÂ­cio) Ã‚Â· **01/07** Ã‚Â· **03/07** Ã‚Â· **05/07** Ã‚Â· **07/07** Ã‚Â· Ã¢â‚¬Â¦ |

Entre deploys pode **reenviar a mesma**; quando subir pacote novo na **produÃƒÂ§ÃƒÂ£o**, atualizar data de corte + bullets + badge `VERSION`.

**Dica pÃƒÂ³s-deploy:** **Ctrl+F5** no Chrome apÃƒÂ³s atualizaÃƒÂ§ÃƒÂ£o.

---

Ã°Å¸Å¡â‚¬ **AtualizaÃƒÂ§ÃƒÂµes do Sistema Ã¢â‚¬â€ GM Agro** Ã°Å¸Å¡â‚¬  
Ã°Å¸â€œâ€¦ **ProduÃƒÂ§ÃƒÂ£o: 17 a 29 de junho**

**Ã°Å¸â€ºâ€™ PDV (Vendas)**  
Ã¢â‚¬Â¢ Autocomplete de produtos renovado: fundo azul, lista maior, **Carregar mais** e **Enter** adiciona sem fechar a busca.  
Ã¢â‚¬Â¢ Busca de **clientes** mais rÃƒÂ¡pida (lista guardada no navegador).  
Ã¢â‚¬Â¢ **Finalizar venda** mais rÃƒÂ¡pido: cupom fiscal sai em segundo plano; nÃƒÂ£o trava se o ERP estiver lento ou fora.  
Ã¢â‚¬Â¢ PDV **continua vendendo** mesmo com Mongo/ERP fora (produtos vÃƒÂªm do sistema Agro).  
Ã¢â‚¬Â¢ Etiquetas **GM com hÃƒÂ­fen**: bip nÃƒÂ£o apaga item do carrinho nem confunde cÃƒÂ³digos parecidos.  
Ã¢â‚¬Â¢ Modal de **CPF na NFC-e** maior e mais legÃƒÂ­vel.  
Ã¢â‚¬Â¢ **Entrega (F3):** telas e popups maiores; fluxo reorganizado (endereÃƒÂ§o Ã¢â€ â€™ taxa Ã¢â€ â€™ pagamento Ã¢â€ â€™ troco).  
Ã¢â‚¬Â¢ **Conferir entrega** mostra frete e total certos.  
Ã¢â‚¬Â¢ Ao **trocar cliente** na entrega, endereÃƒÂ§o nÃƒÂ£o Ã‚Â«grudaÃ‚Â» mais da venda anterior.  
Ã¢â‚¬Â¢ **Selos de promo** no carrinho: verde quando atingiu a promo; amarelo quando **faltam unidades**.  
Ã¢â‚¬Â¢ Promo **Ã‚Â«leve X pague YÃ‚Â»**: unidades extras voltam ao **preÃƒÂ§o normal** (ex.: 5Ã‚Âº item fora da promo).  
Ã¢â‚¬Â¢ **Promo mix** (vÃƒÂ¡rios produtos): total calculado certo; linhas da mesma promo juntas, com borda colorida e selo **MIX**.  
Ã¢â‚¬Â¢ **Remover** item virou ÃƒÂ­cone de lixeira (mais espaÃƒÂ§o na linha).

**Ã°Å¸â€™Âµ Caixa (Fechamento)**  
Ã¢â‚¬Â¢ Nova tela de **histÃƒÂ³rico de retiradas/saÃƒÂ­das**: filtros por data, plano e quem levou (padrÃƒÂ£o = hoje).  
Ã¢â‚¬Â¢ ApÃƒÂ³s registrar saÃƒÂ­da: aviso verde **Ã‚Â«Retirada concluÃƒÂ­daÃ‚Â»** e campos limpos.  
Ã¢â‚¬Â¢ **CorreÃƒÂ§ÃƒÂ£o importante:** devoluÃƒÂ§ÃƒÂ£o no mesmo dia **nÃƒÂ£o descontava em dobro** no fechamento nem no relatÃƒÂ³rio.  
Ã¢â‚¬Â¢ Menu **CAIXA** mais rÃƒÂ¡pido (detalhes pesados sÃƒÂ³ na tela **Saldo**) Ã¢â‚¬â€ **Ã°Å¸â€œâ€¹ fila** Ã‚Â· prÃƒÂ³ximo deploy loja (ainda **nÃƒÂ£o** na v5.22 live).

**Ã°Å¸Â§Â¾ Entrada de Nota Fiscal**  
Ã¢â‚¬Â¢ **Rascunhos** da nota salvos no sistema Agro (mais estÃƒÂ¡vel).  
Ã¢â‚¬Â¢ **Reabrir** nota finalizada: estorna tÃƒÂ­tulo no financeiro corretamente.  
Ã¢â‚¬Â¢ Estoque: aviso se ainda nÃƒÂ£o aplicou; reabrir limpa etapa de **lote/validade**.  
Ã¢â‚¬Â¢ **Auditoria financeiro** da NF (botÃƒÂ£o na lista de notas).

**Ã°Å¸ÂÂ·Ã¯Â¸Â Etiquetas de PreÃƒÂ§o**  
Ã¢â‚¬Â¢ CÃƒÂ³digo interno faixa **230** impresso como **CODE128** (leitura mais confiÃƒÂ¡vel no balcÃƒÂ£o).

**Ã°Å¸â€œÂ¦ Cadastro / GestÃƒÂ£o de Produtos**  
Ã¢â‚¬Â¢ Busca na lista por **cÃƒÂ³digo GM** e **cÃƒÂ³digo de barras**.  
Ã¢â‚¬Â¢ **GestÃƒÂ£o** mais rÃƒÂ¡pida e estÃƒÂ¡vel (menos dependÃƒÂªncia do ERP).  
Ã¢â‚¬Â¢ Estoque operacional no **Agro** Ã¢â‚¬â€ venda baixa saldo mesmo com Mongo fora.

**Ã°Å¸Å½Â PromoÃƒÂ§ÃƒÂµes (cadastro)**  
Ã¢â‚¬Â¢ **Salvar promoÃƒÂ§ÃƒÂ£o** corrigido (nÃƒÂ£o dava mais erro na etapa 2).  
Ã¢â‚¬Â¢ BotÃƒÂ£o **Excluir** na lista (remove duplicatas).  
Ã¢â‚¬Â¢ Etapa de produtos: **bip direto**, busca por GM ou nome; lista **continua aberta** apÃƒÂ³s adicionar.  
Ã¢â‚¬Â¢ BotÃƒÂ£o **Continuar Ã¢â‚¬â€ escolher produtos** corrigido.

**Ã°Å¸â€™Â° LanÃƒÂ§amentos / Contas a pagar**  
Ã¢â‚¬Â¢ Contas a pagar e receber no **sistema Agro** Ã¢â‚¬â€ mais rÃƒÂ¡pido e estÃƒÂ¡vel.  
Ã¢â‚¬Â¢ Filtros da lista carregam **mais rÃƒÂ¡pido**.  
Ã¢â‚¬Â¢ **Backup** em ZIP (sÃƒÂ³ em aberto) e Excel completo (admin).  
Ã¢â‚¬Â¢ **Nova saÃƒÂ­da** em tela cheia: emprÃƒÂ©stimo entrada + pagamento; quitar por item.  
Ã¢â‚¬Â¢ **Totais corrigidos** Ã¢â‚¬â€ sincronizaÃƒÂ§ÃƒÂ£o alinhou valores com o backup conferido.

**Ã°Å¸â€œÅ  Tela inicial (BI)**  
Ã¢â‚¬Â¢ Cards de **contas a pagar/receber** alinhados ao financeiro Agro.  
Ã¢â‚¬Â¢ **GrÃƒÂ¡fico de gastos** por plano de conta (botÃƒÂ£o laranja no card Contas a Pagar).  
Ã¢â‚¬Â¢ **Meta de vendas** do mÃƒÂªs: histÃƒÂ³rico da planilha (set/25Ã¢â‚¬â€œmai/26) + vendas PDV atuais Ã¢â‚¬â€ comparaÃƒÂ§ÃƒÂ£o mais realista.  
Ã¢â‚¬Â¢ Card de **validade** corrigido (produtos com data prÃƒÂ³xima aparecem certo).

**Ã°Å¸â€ºÂÃ¯Â¸Â Compras**  
Ã¢â‚¬Â¢ SugestÃƒÂ£o de compra usa **vendas do Agro** (mais rÃƒÂ¡pido).  
Ã¢â‚¬Â¢ **Folha Compras** por categoria/unidade inclui dados da gestÃƒÂ£o.  
Ã¢â‚¬Â¢ Card **Ã‚Â«ÃƒÂºltimas comprasÃ‚Â»** na busca (NF Agro + ERP).

**Ã°Å¸â€œÂ¦ TransferÃƒÂªncias e validade**  
Ã¢â‚¬Â¢ Telas de **transferÃƒÂªncia** e **relatÃƒÂ³rio de validade** usam estoque Agro (ajustes + vendas).

---

### WIP Ã¢â‚¬â€ Relacionamento PDV (F8 rascunho) **30/06**

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | Modal com abas Ã¢â‚¬â€ testar na **loja** com cliente cheio de compras; demais abas aos poucos |
| **Atalho** | **F8** ou botÃƒÂ£o **Hist.** (F5 voltou a ser refresh do navegador) |
| **Aba inicial** | **Resumo** (padrÃƒÂ£o ao abrir) Ã¢â‚¬â€ **v5.41** |
| **Modal** | Altura **fixa** (ref. aba HistÃƒÂ³rico) Ã¢â‚¬â€ troca de aba **nÃƒÂ£o redimensiona** Ã‚Â· scroll sÃƒÂ³ no miolo Ã¢â‚¬â€ **v5.35** |
| **Resumo** | **9 cards em linha** (1Ãƒâ€”9): Visitas Ã‚Â· Ticket Ã‚Â· Cashback Ã‚Â· Vale Ã‚Â· Total Ã‚Â· Freq. Ã‚Â· ÃƒÅ¡lt. visita Ã‚Â· Pets Ã‚Â· WhatsApp Ã¢â‚¬â€ **v5.43** |
| **HistÃƒÂ³rico** | Sem cards Visitas/Ticket/Total (sÃƒÂ³ no Resumo) Ã¢â‚¬â€ **v5.42** |
| **Fiado** | BotÃƒÂ£o **LanÃƒÂ§amentos** Ã¢â€ â€™ `/fiado/?from=pdv&cliente=PK` abre **modal do cliente** (fallback API se nÃƒÂ£o estiver na lista) Ã¢â‚¬â€ **v5.39** |
| **Carrinho** | BotÃƒÂ£o **+ 1 un.** Ã¢â‚¬â€ cache local primeiro (sem ida ao servidor) Ã‚Â· **v5.45 teste** |
| **Perf F8** | Abertura rÃƒÂ¡pida Ã‚Â· histÃƒÂ³rico **12 + Carregar mais** Ã‚Â· ciclo/cross **lazy** Ã‚Â· **WIP teste 30/06** |
| **Risco loja** | **Baixo** Ã¢â‚¬â€ sÃƒÂ³ consulta Ã‚Â· nÃƒÂ£o mexe venda/preÃƒÂ§o/estoque Ã‚Â· modal lento OK |
| **Deploy loja** | **Ã°Å¸â€œÂ¦ Pacote v5.44 pronto** Ã¢â‚¬â€ aguardando autorizaÃƒÂ§ÃƒÂ£o Renan (Ã‚Â§ CHECKPOINT pacote loja) |
| **API** | `GET /api/pdv/relacionamento-cliente/?cliente_agro_pk=` Ã‚Â· lazy `?secao=ciclo_racao|cross_sell|historico` |
| **Abas** | Resumo Ã‚Â· HistÃƒÂ³rico Ã‚Â· Ciclo raÃƒÂ§ÃƒÂ£o Ã‚Â· Cross-sell Ã‚Â· Fiado Ã‚Â· Cashback Ã‚Â· MÃƒÂ©tricas Ã‚Â· Pets Ã‚Â· SaÃƒÂºde Ã‚Â· AnotaÃƒÂ§ÃƒÂµes Ã‚Â· Contato |
| **Dados reais** | Vendas PDV + itens Ã‚Â· fiado Ã‚Â· cashback/vale Ã¢â‚¬â€ fonte **Postgres Agro** |
| **Extras cliente** | Pets Ã‚Â· saÃƒÂºde Ã‚Â· anotaÃƒÂ§ÃƒÂµes Ã¢â€ â€™ **`ClienteAgro.relacionamento_extras_json` (Postgres)** Ã‚Â· **v5.44** Ã‚Â· qualquer caixa vÃƒÂª |
| **API extras** | `GET` painel traz `extras` Ã‚Â· `POST /api/pdv/relacionamento-cliente/extras/` grava |
| **Regra assistente** | Relacionamento **cliente/pets/anotaÃƒÂ§ÃƒÂµes** = falar sÃƒÂ³ **Postgres** Ã¢â‚¬â€ nÃƒÂ£o misturar com ERP/Mongo nesse contexto |
| **PendÃƒÂªncia P3** | **FL-039** pets na ficha `/clientes/` Ã‚Â· **FL-040** tabela Pet normalizada (opÃƒÂ§ÃƒÂ£o B) |
| **Teste** | Render teste Ã‚Â· PDV Ã‚Â· cliente cadastrado Ã‚Â· **F8** ou **Hist.** |

#### FL-042 Ã¢â‚¬â€ histÃƒÂ³rico ERP no F8 (Renan Ã‚Â· 30/06)

**Hoje:** F8 sÃƒÂ³ vÃƒÂª vendas **SisVale** (`VendaAgro`). Vendas sÃƒÂ³ no ERP **nÃƒÂ£o entram**.

**DecisÃƒÂ£o seguranÃƒÂ§a (Renan):** **import ÃƒÂºnico** Ã¢â€ â€™ **Postgres somente leitura** Ã¢â€ â€™ **sem vÃƒÂ­nculo vivo com Mongo**. F8 **nunca** consulta Mongo em tempo real.

| OpÃƒÂ§ÃƒÂ£o | SeguranÃƒÂ§a SisVale | PrÃƒÂ³s | Contras |
| ----- | ----------------- | ---- | ------- |
| **A Ã¢â‚¬â€ Mongo 1Ãƒâ€” (recomendado)** | Alta, se gravar em **tabela histÃƒÂ³rica separada** | Completo (`DtoVenda` + itens); `ClienteID` casa com `externo_id`; sem planilha manual | Carga Mongo na hora do import; precisa **data corte** + dedup vs PDV |
| **B Ã¢â‚¬â€ Excel** | Alta (Renan revisa antes) | Controle humano; testa no staging com arquivo; zero carga Mongo | Trabalho manual; export ERP pode vir incompleto |
| **C Ã¢â‚¬â€ Mongo sempre ligado no F8** | **Evitar** | Ã¢â‚¬â€ | LentidÃƒÂ£o, dependÃƒÂªncia Mongo, risco duplicar venda ERP+PDV, quebra se espelho mudar |

**Recomendado:** **A + B** Ã¢â‚¬â€ comando **1Ãƒâ€”** lÃƒÂª Mongo (`DtoVenda` / `DtoVendaProduto`), grava Postgres, **encerra**. Excel sÃƒÂ³ para **conferÃƒÂªncia**, correÃƒÂ§ÃƒÂ£o ou o que o Mongo nÃƒÂ£o casar.

**Regras para nÃƒÂ£o quebrar loja:**
1. **NÃƒÂ£o** gravar histÃƒÂ³rico como venda Ã¢â‚¬Å“de balcÃƒÂ£oÃ¢â‚¬Â Ã¢â‚¬â€ tabela prÃƒÂ³pria (ex. `ClienteVendaHistoricoErpAgro`) **ou** `VendaAgro` com flag `origem=historico_erp` **excluÃƒÂ­da** de caixa, estoque, NFC-e e lista `/vendas/`.
2. **Data corte** = dia anterior ao PDV SisVale valer (ex. vendas Mongo **atÃƒÂ©** essa data; depois sÃƒÂ³ `VendaAgro`).
3. **Dedup:** mesmo `venda_id` ERP + cliente jÃƒÂ¡ no PDV Ã¢â€ â€™ nÃƒÂ£o somar duas vezes.
4. **Dry-run** primeiro (relatÃƒÂ³rio: quantas vendas, clientes sem match, duplicatas) Ã¢â€ â€™ Renan ok Ã¢â€ â€™ import real.
5. F8 sÃƒÂ³ faz **merge Postgres** (histÃƒÂ³rico importado + `VendaAgro`).

**Antes de codar:** dry-run no **teste** Ã¢â€ â€™ Renan ok Ã¢â€ â€™ import loja.

**Data corte PDV (Renan Ã‚Â· 30/06 Ã¢â‚¬â€ confirmado):** **27/05/2026** = inÃƒÂ­cio **permanente** no SisVale. Antes disso pode existir **vendinha de teste** no PDV Ã¢â‚¬â€ **nÃƒÂ£o conta** no F8 como venda SisVale.

| Fonte | PerÃƒÂ­odo no F8 | Nota |
| ----- | ------------- | ---- |
| **Mongo ERP (import 1Ãƒâ€”)** | data venda **Ã¢â€°Â¤ 26/05/2026** | HistÃƒÂ³rico antigo |
| **`VendaAgro` SisVale** | **Ã¢â€°Â¥ 27/05/2026** | ProduÃƒÂ§ÃƒÂ£o real; ignora testes anteriores |
| **Teste PDV antes 27/05** | **Fora** do relacionamento | Fica na `/vendas/` se existir, mas F8 nÃƒÂ£o soma |

**Produtos que nÃƒÂ£o existem mais no SisVale** (cadastro excluÃƒÂ­do, cÃƒÂ³digo mudou, GM diferente):

- Na importaÃƒÂ§ÃƒÂ£o grava **foto do item na ÃƒÂ©poca**: cÃƒÂ³digo ERP, cÃƒÂ³digo GM (se tinha), **descriÃƒÂ§ÃƒÂ£o**, qtd, valor Ã¢â‚¬â€ **sem depender** do cadastro atual.
- **Visitas, ticket, total, histÃƒÂ³rico de vendas** Ã¢â€ â€™ entram **normais** (ÃƒÂ© venda passada, nÃƒÂ£o precisa produto vivo).
- **Top produtos / ciclo raÃƒÂ§ÃƒÂ£o** Ã¢â€ â€™ agrupa pela **descriÃƒÂ§ÃƒÂ£o + cÃƒÂ³digo gravado no histÃƒÂ³rico** (texto da ÃƒÂ©poca).
- BotÃƒÂ£o **+1 no carrinho** (sÃƒÂ³ itens SisVale atuais hoje):
  - achou produto **ativo** no catÃƒÂ¡logo Ã¢â€ â€™ **+1** funciona;
  - **nÃƒÂ£o achou** (excluÃƒÂ­do / cÃƒÂ³digo mudou) Ã¢â€ â€™ mostra o nome **como estava**, **sem +1** ou com aviso *Ã‚Â«indisponÃƒÂ­vel no cadastroÃ‚Â»* Ã¢â‚¬â€ operador busca similar manualmente.
- **NÃƒÂ£o** recria produto, **nÃƒÂ£o** altera estoque, **nÃƒÂ£o** quebra a tela.

Dry-run do import tambÃƒÂ©m lista **quantos itens** ficaram sem match no catÃƒÂ¡logo atual (sÃƒÂ³ informativo).

**Consumidor nÃƒÂ£o identificado:** vendas em nome **CONSUMIDOR NÃƒÆ’O IDENTIFICADO** (ou similar) Ã¢â€ â€™ **nÃƒÂ£o importa** (contador `vendas_consumidor`). Se importar, tambÃƒÂ©m nÃƒÂ£o quebra.

**PrÃƒÂ©-import (teste/loja):** se dry-run mostrar muitas Ã‚Â«sem cliente AgroÃ‚Â», rodar **`sincronizar_clientes_agro`** antes.

**Garantias Renan (fiado / cashback / vale):**
- Import **nÃƒÂ£o grava** em `VendaAgro`, **nÃƒÂ£o abre** tÃƒÂ­tulo fiado, **nÃƒÂ£o mexe** `saldo_cashback` nem `saldo_vale_credito` do cliente.
- Fiado/cashback/vale no F8 continuam lendo **saldo real** (Postgres + regras atuais) Ã¢â‚¬â€ histÃƒÂ³rico ERP ÃƒÂ© **sÃƒÂ³ leitura decorativa** (visitas, ticket, top produtos, lista antiga).
- Se no ERP antigo a venda era fiado, no F8 pode **aparecer como informaÃƒÂ§ÃƒÂ£o** (Ã‚Â«comprou fiado em 2024Ã‚Â») Ã¢â‚¬â€ **nÃƒÂ£o altera** saldo em aberto de hoje.

**ReversÃƒÂ­vel (se der merda):**
1. Tabela **separada** (nÃƒÂ£o misturar com venda de balcÃƒÂ£o).
2. Cada import com **`lote_id`** (ex. `erp-hist-20260630`).
3. Comando **`reverter`** = apagar sÃƒÂ³ aquele lote (ou apagar tudo do histÃƒÂ³rico).
4. Flag **`.env`** liga/desliga merge no F8 **sem** apagar dados (`AGRO_REL_HISTORICO_ERP=false` Ã¢â€ â€™ F8 volta ao comportamento de hoje).
5. Ordem: **teste** dry-run Ã¢â€ â€™ import teste Ã¢â€ â€™ validar F8 Ã¢â€ â€™ **sÃƒÂ³ entÃƒÂ£o** loja (com frase + senha se produÃƒÂ§ÃƒÂ£o).

---

| # | O quÃƒÂª | Passou? |
| - | ----- | ------- |
| 0 | **F8** abre na aba **Resumo** (nÃƒÂ£o HistÃƒÂ³rico) | Ã¢ËœÂ |
| 0a | **Resumo:** 9 cards **numa linha sÃƒÂ³** (como abas) | Ã¢ËœÂ |
| 0b | Aba **HistÃƒÂ³rico** sem cards Visitas / Total / Ticket (sÃƒÂ³ itens + vendas) | Ã¢ËœÂ |
| 4 | Cadastrar **pet** no F8 Ã¢â€ â€™ fechar Ã¢â€ â€™ abrir em **outro PC** (ou outro Chrome) Ã¢â€ â€™ pet aparece | Ã¢ËœÂ |
| 5 | **SaÃƒÂºde** e **anotaÃƒÂ§ÃƒÂµes** tambÃƒÂ©m persistem no cadastro | Ã¢ËœÂ |
| 1 | **Resumo** sem card fiado (sÃƒÂ³ 3 cards) | Ã¢ËœÂ |
| 2 | Cliente com fiado Ã¢â€ â€™ aba **FIADO** laranja/vermelha + valor | Ã¢ËœÂ |
| 3 | Aba Fiado Ã¢â€ â€™ botÃƒÂ£o **LanÃƒÂ§amentos do cliente** abre gestÃƒÂ£o com modal do cliente | Ã¢ËœÂ |

**NÃƒÂ£o testar ainda:** **FL-038 contingÃƒÂªncia deploy** Ã¢â‚¬â€ sÃƒÂ³ documentaÃƒÂ§ÃƒÂ£o; **sem cÃƒÂ³digo**.

**Depois de OK:** Renan validou layout **v5.43** Ã¢â‚¬â€ para **loja**: pedir *Ã‚Â«pode subir para produÃƒÂ§ÃƒÂ£oÃ‚Â»* + senha **99738595** no mesmo chat Ã‚Â· assistente monta cherry-pick (Relacionamento + **v5.24** caixa, corrigir `VERSION` UTF-8 do build que falhou).

### DEPLOY LOJA Ã¢â‚¬â€ perf menu caixa **v5.24** (29/06) Ã¢ÂÅ’ build falhou

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ teste OK Ã‚Â· *pode subir* + senha **99738595** |
| **Pacote** | Cherry-pick teste **`11d8634`** Ã¢â€ â€™ loja **`eb9fcc9`** |
| **Render** | **29/06 ~20:54** Ã¢â‚¬â€ *Exited with status 1 while building* Ã‚Â· **live continua `0a0fd52` v5.22** |
| **Causa** | Arquivo **`VERSION`** no commit gravado em **UTF-16 (BOM)** Ã¢â€ â€™ `scripts/record_deploy.py` Ã¢â€ â€™ `read_app_version()` UTF-8 Ã¢â€ â€™ **UnicodeDecodeError** (1Ã‚Âº passo do build) |
| **CorreÃƒÂ§ÃƒÂ£o** | Recommit **`VERSION`** UTF-8 (`5.24\n`) + redeploy (cÃƒÂ³digo **`11d8634`** OK) Ã‚Â· opcional: `read_app_version` tolerante a UTF-16 |
| **Renan 30/06** | **Ã°Å¸â€œâ€¹ Fila** Ã¢â‚¬â€ **nÃƒÂ£o** redeploy isolado agora Ã‚Â· sobe **junto com o prÃƒÂ³ximo pacote** loja (fix `VERSION` no cherry-pick final) |
| **Revert loja** | jÃƒÂ¡ estÃƒÂ¡ em **`0a0fd52`** (v5.22) Ã¢â‚¬â€ botÃƒÂ£o Rollback no Render ÃƒÂ© redundante |

### DEPLOY LOJA Ã¢â‚¬â€ devoluÃƒÂ§ÃƒÂ£o caixa **FL-017** **v5.22** (29/06) Ã¢Å“â€¦

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ *pode enviar produÃƒÂ§ÃƒÂ£o* + senha **99738595** Ã‚Â· *com muito cuidado* |
| **Risco** | **Baixo** Ã¢â‚¬â€ sÃƒÂ³ `caixa_util.py` + `caixa_relatorio_util.py` Ã‚Â· **sem** migraÃƒÂ§ÃƒÂ£o Ã‚Â· **sem** PDV |
| **Pacote** | Cherry-pick cÃƒÂ³digo teste **`a936f97`** + **`bed21ee`** Ã¢â€ â€™ commit loja **`0a0fd52`** (nÃƒÂ£o merge inteiro `teste`) |
| **O quÃƒÂª** | Fechamento: devoluÃƒÂ§ÃƒÂ£o no mesmo turno nÃƒÂ£o desconta 2Ãƒâ€” Ã‚Â· RelatÃƒÂ³rio: vendas bruto + devoluÃƒÂ§ÃƒÂµes Ã¢â€ â€™ saldo certo |
| **Loja** | Ctrl+F5 apÃƒÂ³s Render Ã‚Â· badge **v5.22** Ã‚Â· falta fictÃƒÂ­cia (ex. 3Ãƒâ€” R$ 70 = R$ 210) **nÃƒÂ£o** deve voltar |
| **Revert** | redeploy **`a44422c`** (produÃƒÂ§ÃƒÂ£o v5.19 prÃƒÂ©-fix) |

### Ã¢Å“â€¦ FL-017 Ã¢â‚¬â€ validaÃƒÂ§ÃƒÂ£o Renan **teste v5.22** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **RelatÃƒÂ³rio caixa** | **Ã¢Å“â€¦** Ã¢â‚¬â€ Vendas + DevoluÃƒÂ§ÃƒÂµes batem Ã‚Â· saldo coerente (print: entradas R$ 26 Ã¢Ë†â€™ saÃƒÂ­das R$ 6 = **R$ 20**) |
| **Fechamento turno** | **Ã¢Å“â€¦ teste** Ã¢â‚¬â€ esperado deixa de Ã‚Â«inventar faltaÃ‚Â» em dobro na devoluÃƒÂ§ÃƒÂ£o |
| **Frete no relatÃƒÂ³rio** | Renan viu linha de frete que antes nÃƒÂ£o aparecia Ã¢â‚¬â€ efeito colateral do relatÃƒÂ³rio voltar a fechar certo (nÃƒÂ£o era foco do fix) |
| **Loja Ã¢â‚¬â€ relato operador** | 3 devoluÃƒÂ§ÃƒÂµes **R$ 70** Ã¢â€ â€™ fechamento mostrava **falta ~R$ 210** (70Ãƒâ€”3, desconto em dobro) Ã¢â‚¬â€ **fix v5.22 na loja** |
| **PrÃƒÂ³ximo** | Conferir loja pÃƒÂ³s-deploy (Ctrl+F5 Ã‚Â· badge v5.22) |

### Ã¢Å¡Â Ã¯Â¸Â FL-017 Ã¢â‚¬â€ confusÃƒÂ£o teste vs loja (29/06 Ã¢â‚¬â€ histÃƒÂ³rico)

| Onde | VersÃƒÂ£o | Fix devoluÃƒÂ§ÃƒÂ£o caixa |
| ---- | ------ | ------------------- |
| **Render teste** | **v5.22** | **Ã¢Å“â€¦** |
| **Render SistVale** | **v5.22** | **Ã¢Å“â€¦ deploy `0a0fd52`** |

### FIX Ã¢â‚¬â€ relatÃƒÂ³rio caixa saldo devoluÃƒÂ§ÃƒÂ£o **FL-017** **v5.22** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | 3Ãƒâ€” R$ 1,50 vendidas e devolvidas no dia Ã¢â€ â€™ relatÃƒÂ³rio **Vendas R$ 0** + **DevoluÃƒÂ§ÃƒÂµes Ã¢Ë†â€™R$ 4,50** Ã¢â€ â€™ saldo **Ã¢Ë†â€™R$ 4,50** (deveria **R$ 0**) |
| **Causa** | RelatÃƒÂ³rio **omitia** vendas devolvidas na seÃƒÂ§ÃƒÂ£o Vendas **e** somava DevoluÃƒÂ§ÃƒÂµes Ã¢â‚¬â€ desconto em dobro no saldo |
| **Fix** | Vendas no relatÃƒÂ³rio = **bruto do dia** (inclui depois devolvidas); DevoluÃƒÂ§ÃƒÂµes abatem Ã¢â€ â€™ saldo **0** |
| **BI card VENDAS** | **R$ 0 Ã‚Â· excl. 3 dev.** continua certo Ã¢â‚¬â€ ÃƒÂ© **lÃƒÂ­quido** do dia, nÃƒÂ£o o relatÃƒÂ³rio de movimentos |
| **Teste** | Ctrl+F5 Ã‚Â· RelatÃƒÂ³rio caixa hoje Ã¢â€ â€™ **Vendas +R$ 4,50** Ã‚Â· **DevoluÃƒÂ§ÃƒÂµes Ã¢Ë†â€™R$ 4,50** Ã‚Â· **Saldo R$ 0** |

### PERF Ã¢â‚¬â€ painel caixa menu abre mais rÃƒÂ¡pido **v5.24 teste** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Breve demora ao clicar **CAIXA** no PDV (menu do turno) |
| **Causa** | Menu carregava consultas do **Saldo** (vendas ÃƒÂ³rfÃƒÂ£s, tabela completa, planos saÃƒÂ­da) + cÃƒÂ¡lculo do turno **2Ãƒâ€”** |
| **Fix** | Menu: sÃƒÂ³ resumo (esperado dinheiro + qtd vendas) Ã‚Â· ÃƒÂ³rfÃƒÂ£s/movimentos sÃƒÂ³ em **Saldo caixa** Ã‚Â· agregaÃƒÂ§ÃƒÂ£o **1 passagem** |
| **Ainda pesa** | Tailwind CDN + fontes Google na 1Ã‚Âª abertura (padrÃƒÂ£o MPA) Ã¢â‚¬â€ nÃƒÂ£o mudou neste patch |

### FIX Ã¢â‚¬â€ devoluÃƒÂ§ÃƒÂ£o nÃƒÂ£o duplica saldo do turno **FL-017** **v5.21** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | DevoluÃƒÂ§ÃƒÂ£o no **mesmo turno**: venda some da lista **e** retirada desconta de novo Ã¢â€ â€™ saldo cai **2Ãƒâ€”** |
| **Causa** | `resumo_esperado_por_forma` excluÃƒÂ­a venda `devolvida_em` **e** subtraÃƒÂ­a retirada Ã‚Â«DevoluÃƒÂ§ÃƒÂ£o venda #Ã¢â‚¬Â¦Ã‚Â» |
| **Fix** | Retirada de devoluÃƒÂ§ÃƒÂ£o **sÃƒÂ³ ignora** no esperado se a venda era **deste turno**; outro turno continua sÃƒÂ³ pela retirada (igual relatÃƒÂ³rio caixa) |
| **Arquivos** | `caixa_util.py` Ã‚Â· `caixa_relatorio_util.py` (helper compartilhado) |
| **Teste staging** | Abrir caixa Ã‚Â· vender Dinheiro Ã‚Â· devolver Ã‚Â· **Esperado** deve cair **1Ãƒâ€”** (nÃƒÂ£o 2Ãƒâ€”) Ã‚Â· Ctrl+F5 painel caixa |

### DEPLOY LOJA Ã¢â‚¬â€ promo mix + selos carrinho **v5.19** (29/06) Ã¢Å“â€¦

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃƒÂ§ÃƒÂ£o** | Renan Ã¢â‚¬â€ staging lento Ã‚Â· *pode enviar produÃƒÂ§ÃƒÂ£o* + senha **99738595** |
| **Risco** | Baixo Ã¢â‚¬â€ sÃƒÂ³ **PDV wizard** (JS/CSS) + fix catÃƒÂ¡logo delta Ã‚Â· **sem** migraÃƒÂ§ÃƒÂ£o banco |
| **Pacote** | `teste` Ã¢â€ â€™ `producao` merge **`a44422c`** (v5.08 Ã¢â€ â€™ v5.19) |
| **O quÃƒÂª** | Promo mix (preÃƒÂ§o correto 3+2) Ã‚Â· selos MIX Ã‚Â· agrupa linhas Ã‚Â· fix import catÃƒÂ¡logo |
| **Loja** | **Ctrl+F5** no Chrome apÃƒÂ³s deploy Render Ã‚Â· vendas em andamento: OK continuar apÃƒÂ³s refresh |
| **Revert** | redeploy commit **`8ea8ac9`** (produÃƒÂ§ÃƒÂ£o prÃƒÂ©-merge) |

### FIX Ã¢â‚¬â€ busca PDV vazia no `runserver` local **v5.13** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `python manage.py runserver` Ã‚Â· busca nÃƒÂ£o acha produto |
| **Causa** | Import errado em `mesclar_catalogo_pdv_cache` Ã¢â€ â€™ delta HTTP 500 Ã‚Â· cache vazio |
| **Fix** | `integracoes.texto.normalizar` + fallback catÃƒÂ¡logo no wizard |
| **Local** | `.env` com Mongo (`VENDA_ERP_MONGO_*`) Ã‚Â· reiniciar runserver Ã‚Â· Ctrl+F5 PDV |

### FIX Ã¢â‚¬â€ promo mix 3+2 + indicador visual ligaÃƒÂ§ÃƒÂ£o **v5.15Ã¢â‚¬â€œ5.16** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Mix 3 un. produto A + 2 un. produto B Ã¢â€ â€™ preÃƒÂ§o/selo errado (ex. R$ 13,00 em vez de **R$ 12,90**) |
| **Causa** | Slots promocionais na ordem FIFO do carrinho (3Ãƒâ€” promo na 1Ã‚Âª linha) |
| **Fix** | Aloca slots promocionais priorizando **maior preÃƒÂ§o de tabela** (melhor desconto ao cliente) |
| **Visual** | Linhas da mesma promo mix: **borda colorida** + pill **MIX** no nome + selo **MIX / N de X** |
| **Arquivos** | `pdv_promocoes.js` Ã‚Â· `pdv_wizard.js` Ã‚Â· `pdv_wizard.html` |
| **Teste** | Ctrl+F5 Ã‚Â· GM1769 Ãƒâ€”3 + GM1771 Ãƒâ€”2 (promo leve 4) Ã¢â€ â€™ total **R$ 12,90** Ã‚Â· mesma cor nas 2 linhas |

### UX Ã¢â‚¬â€ selo mix menos confuso **v5.18** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Ã‚Â«MIX 3 de 4Ã‚Â» + Ã‚Â«MIX 1 de 4Ã‚Â» parecia **duas promos incompletas** (ex. 5+1 un.) |
| **Fix** | Selo mix ativo: **MIX 4 un.** (bloco fechado) + **N aqui** (quanto veio **desta linha**) |
| **Pendente mix** | **Faltam N Ã‚Â· 3/4** Ã¢â‚¬â€ mostra progresso no carrinho |
| **Exemplo 5+1** | Linha A: **MIX 4 un.** / **3 aqui** + **+2 normal** Ã‚Â· Linha B: **MIX 4 un.** / **1 aqui** |
| **Layout selo** | Coluna promo **largura/altura fixa** Ã¢â‚¬â€ GM e qtd alinhados mesmo com 1 ou 2 selos |
| **Cor mix** | Borda + gradiente **esquerda e direita** (mesma cor = mesma promo no carrinho) |
| **Ordem carrinho** | CritÃƒÂ©rio atingido Ã¢â€ â€™ linhas da **mesma promo ativa** **juntam** automaticamente |
| **Selo mix camadas** | **1Ã‚Âª linha do bloco:** `MIX 4 un.` + `N aqui` Ã‚Â· **demais:** sÃƒÂ³ `N aqui` (+ normal se houver) |

### FIX Ã¢â‚¬â€ promo mix (mesma promoÃƒÂ§ÃƒÂ£o, produtos diferentes) **v5.10+** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | 2+2 saches da promo Ã‚Â«testeÃ‚Â» Ã¢â€ â€™ cada linha Ã‚Â«Faltam 2Ã‚Â» Ã‚Â· total errado |
| **Era** | Contava qtd **por produto**, nÃƒÂ£o por promoÃƒÂ§ÃƒÂ£o |
| **Fix** | Soma unidades de **todos os produtos da mesma promo** (id) Ã‚Â· leve X e acima de X |
| **Exemplo** | GM1769 Ãƒâ€”2 + GM1771 Ãƒâ€”2 Ã¢â€ â€™ **R$ 10,00** Ã‚Â· selo **2 promo** em cada linha |
| **Teste** | Ctrl+F5 Ã‚Â· promo Ã‚Â«testeÃ‚Â» Ã‚Â· mix 4 un. |

### WIP Ã¢â‚¬â€ FL-003 fase 1: selo promo no carrinho PDV (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Onde** | Linha do carrinho Ã¢â‚¬â€ entre **GM** e **qtd** (ÃƒÂ¡rea indicada no print) |
| **Selo verde** | CritÃƒÂ©rio atingido Ã¢â‚¬â€ ex. **PROMO 4Ãƒâ€”** |
| **Selo amarelo** | Duas linhas: **PROMO** (cima) + **Faltam N** (baixo) Ã‚Â· N = mix no carrinho |
| **Mix ativo** | Borda colorida + **MIX** no nome Ã‚Â· **1Ã‚Âª linha:** `MIX 4 un.` + `N aqui` Ã‚Â· **demais:** sÃƒÂ³ `N aqui` |
| **Dois selos** | Passou do critÃƒÂ©rio Ã¢â‚¬â€ ex. **4 promo** + **+1 normal** (5 un. leve 4) |
| **Remover** | Texto virou **lixeira** (ÃƒÂ­cone) para ganhar espaÃƒÂ§o |
| **SÃƒÂ³ visual** | PreÃƒÂ§o jÃƒÂ¡ calculado antes; **nÃƒÂ£o** mexe em venda/caixa/fiscal |
| **Teste** | Ctrl+F5 PDV Ã‚Â· GM1787 Ã‚Â· qty 3/4/5/8/9 |

**FL-003 Ã¢â‚¬â€ fases restantes (depois):**

| Fase | Escopo | Status |
| ---- | ------ | ------ |
| **1** | Selo visual no carrinho PDV (wizard) | Ã°Å¸â€â€ž teste **v5.09** |
| **2** | Desconto promo no **DRE/relatÃƒÂ³rios** como Ã‚Â«desconto clientesÃ‚Â» | Ã°Å¸â€œâ€¹ pendente |
| **3** | Linha de desconto promo na **impressÃƒÂ£o** da venda (80 mm / PDF) | Ã°Å¸â€œâ€¹ pendente |

### Promo Ã‚Â«Leve X pague YÃ‚Â» Ã¢â‚¬â€ resto ao preÃƒÂ§o normal **v5.07+** (29/06) Ã¢Å“â€¦ loja

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | Leve 4 @ R$ 2,50 Ã¢â€ â€™ 5Ã‚Âº sache preÃƒÂ§o normal (12,90 nÃƒÂ£o 12,50) |
| **Era** | qty Ã¢â€°Â¥ X Ã¢â€ â€™ **todas** as unidades a R$ Y |
| **Fix** | Grupos completos de X a R$ Y Ã‚Â· resto ao preÃƒÂ§o tabela Ã‚Â· 4Ã¢â€ â€™10,00 Ã‚Â· 5Ã¢â€ â€™12,90 |
| **Deploy** | `teste`Ã¢â€ â€™`producao` **29/06** Ã‚Â· merge `fe3e9a6` Ã‚Â· checkpoint loja `8ea8ac9` |
| **Conferir loja** | Ctrl+F5 PDV Ã‚Â· GM1787 Ã‚Â· qty 5 Ã¢â€ â€™ total **R$ 12,90** |

### Fila loja Ã¢â‚¬â€ pedidos Zap / melhorias (Renan triagem)

> **Status operacional (Zap #+P+deploy):** ver **CHECKLIST ÃƒÅ¡NICO** no topo do CHECKPOINT. Esta tabela ÃƒÂ© o **cadastro FL** (P + mÃƒÂ³dulo + pedido). Ao mudar status, atualizar **os dois**.

**Pacotes prontos Ã¢â‚¬â€ aguardando deploy loja (Renan 30/06):**

| Pacote | O quÃƒÂª | ObservaÃƒÂ§ÃƒÂ£o |
| ------ | ----- | ---------- |
| **v5.44 Relacionamento F8** | Modal F8 + pets PG + fiado link | **Ã¢Å“â€¦ teste** Ã‚Â· **Ã°Å¸â€œÂ¦ pronto** Ã‚Â· ver CHECKPOINT Ã‚Â«PACOTE LOJA v5.44Ã‚Â» |
| ~~v5.24 perf caixa~~ | Menu caixa lazy | **Branch `producao` jÃƒÂ¡ tem `eb9fcc9`** Ã¢â‚¬â€ conferir se Render live |

**DecisÃƒÂ£o deploy (30/06):** Renan Ã¢â‚¬â€ **FL-038 manual** neste deploy Ã‚Â· demais atualizaÃƒÂ§ÃƒÂµes testadas sobem no **v5.44**.

**Como usar:** manda item a item no chat (`@banana` + prioridade + tela). Assistente registra aqui. **NÃƒÂ£o** vira cÃƒÂ³digo atÃƒÂ© vocÃƒÂª pedir ou subir de prioridade.

**Escala P (Renan):** **Px,y** = entre **Px** e **P(x+1)** Ã¢â‚¬â€ mais urgente que o de baixo, menos que o de cima. Decimal **menor** = mais perto do **P** inteiro de cima (ex. **P1,1** antes de **P1,5**). Inteiros: **P0** para a loja Ã‚Â· **P1** grave Ã‚Â· **P2** melhoria Ã‚Â· **P3** depois. **Conflito** de sub-prioridade (ex. jÃƒÂ¡ existe **P2,9**): **nÃƒÂ£o** rebaixar para **P2** Ã¢â‚¬â€ usar decimal mais fino (**P2,91**, **P2,92**Ã¢â‚¬Â¦).

**ConferÃƒÂªncia (29/06):** itens **P1,x** jÃƒÂ¡ na fila batem com a regra (entre **P1** e **P2**): **FL-021** Ã‚Â· **FL-022** = **P1,1** Ã‚Â· **FL-019** Ã‚Â· **FL-020** = **P1,5**. Nenhum precisou mudar de faixa. Ordem sugerida ao atacar: P1 Ã¢â€ â€™ P1,1 Ã¢â€ â€™ P1,5 Ã¢â€ â€™ P2.

**Lote Zap 29/06/2026 16:20** Ã¢â‚¬â€ neste envio: **FL-023Ã¢â‚¬Â¦FL-035** (fim da fila **FL-035**). Na conversa do Zap: procurar mensagens do **29/06** **atÃƒÂ© ~16:20** para achar o trecho; o ÃƒÂºltimo item deste lote ÃƒÂ© **FL-035**.

| # | P | MÃƒÂ³dulo | Pedido | Status | Desde |
| - | - | ------ | ------ | ------ | ----- |
| **FL-001** | **P3** | PreÃƒÂ§os / PDV | Tabelas de preÃƒÂ§o personalizÃƒÂ¡veis por **forma de pagamento** ou **grupo de cliente** | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-002** | **P3** | PromoÃƒÂ§ÃƒÂµes | Revisar **usabilidade** da tela de promoÃƒÂ§ÃƒÂ£o e **limpar textos inÃƒÂºteis** | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-003** | **P2** | PromoÃƒÂ§ÃƒÂµes / PDV / DRE | **Fase 1** selo promo no carrinho PDV Ã‚Â· **Fase 2** DRE Ã‚Â«desconto clientesÃ‚Â» Ã‚Â· **Fase 3** linha na impressÃƒÂ£o da venda | Ã°Å¸â€â€ž Fase 1 teste Ã‚Â· 2Ã¢â‚¬â€œ3 pendente | 29/06 |
| **FL-004** | **P3** | RH | **Batida de ponto** dos funcionÃƒÂ¡rios (registro entrada/saÃƒÂ­da) | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-005** | **P2** | Entrega / impressÃƒÂ£o | Na impressÃƒÂ£o (separaÃƒÂ§ÃƒÂ£o/entrega): **valor em R$ do troco a levar** na ida Ã¢â‚¬â€ **conferir antes** (hoje sÃƒÂ³ Ã‚Â«troco: sim/nÃƒÂ£oÃ‚Â») | Ã°Å¸â€œâ€¹ Pendente Ã‚Â· Ã°Å¸â€Â conferir | 29/06 |
| **FL-006** | **P2** | PDV / Entregas | **Ligar PDV** ao painel de entregas + **revisÃƒÂ£o visual** da tela `/entregas/` | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-007** | **P2** | UX geral | Revisar **tamanhos de layout** (Agro Display Scale) Ã¢â‚¬â€ **comeÃƒÂ§ar por** `/vendas/` (consulta de vendas) | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-008** | **P1** | PDV | Itens no carrinho **travam** â€” nÃ£o altera qtd, preÃ§o nem remove Â· ex. **GM6083** | âœ… **loja v11.99** | 28/07 |
| **FL-009** | **P2** | Etiquetas | Na tela de **impressÃƒÂ£o de etiquetas**: ao adicionar item, **nÃƒÂ£o fechar** o autocomplete (manter busca aberta para bipar/digitar o prÃƒÂ³ximo) | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-010** | **P2** | Vendas | **Consulta de vendas** (`/vendas/`): buscador e **filtros completos** (perÃƒÂ­odo, cliente, forma, status, textoÃ¢â‚¬Â¦) | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-011** | **P3** | Cashback | Revisar tela de **cashback** Ã¢â‚¬â€ melhorar usabilidade | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-012** | **P2** | Entrada NF | **Lixeira** para **desvincular produto da nota** Ã¢â‚¬â€ alinhar escopo com **Queila** antes de codar | Ã°Å¸â€œâ€¹ Pendente Ã‚Â· Ã¢Ââ€œ Queila | 29/06 |
| **FL-013** | **P2** | Etiquetas / impressÃƒÂ£o | CÃƒÂ³digo de barras em **PNG** (tipografia atual dificulta bip **1D e 2D**) | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-014** | **P3** | PDV | Projetar forma **mais prÃƒÂ¡tica** de alterar **quantidade** no carrinho | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-015** | **P2** | Etiquetas / PDV | **Regra bipagem etiqueta granel** Ã¢â‚¬â€ PDV nÃƒÂ£o leu direito; Renan fez **poucos testes** ainda | Ã°Å¸â€œâ€¹ Pendente Ã‚Â· Ã°Å¸â€Â validar | 29/06 |
| **FL-016** | **P1** | Caixa | **Reset da contagem** do caixa (dia anterior) | âœ… **loja v6.75** | 03/07 |
| **FL-017** | **P1** | Caixa / devoluÃƒÂ§ÃƒÂ£o | **DevoluÃƒÂ§ÃƒÂ£o duplicada** no caixa Ã¢â‚¬â€ apaga venda e ainda registra **saÃƒÂ­da** (dobra o efeito) | **Ã¢Å“â€¦ loja v5.22** Ã‚Â· validado teste | 29/06 |
| **FL-018** | **P2** | Vendas | **Frete** no total da venda (`VendaAgro.frete`) | Ã¢Å“â€¦ parcial 12/07 | 29/06 |
| **FL-019** | **P1,5** | Fiado | **Recibo de pagamentos** no fiado (comprovante ao cliente) | âœ… **teste v15.59** Â· PDV pergunta + Reimprimir `/fiado/` Â· 80 mm | 29/06 |
| **FL-020** | **P1,5** | PDV / fiscal | **Taxa de entrega** no cupom fiscal e cupom de venda (Renan 12/07: **deve sair**) | Ã¢Å“â€¦ 12/07 | 29/06 |
| **FL-021** | **P1,1** | CP | BotÃƒÂ£o **NF** nÃƒÂ£o aparece na lista Ã¢â‚¬â€ ex.: tÃƒÂ­tulo **RBS R$ 781,64** | Ã¢Å“â€¦ **loja v8.68** | 29/06 |
| **FL-022** | **P1,1** | CP | **Busca** no campo de filtros **inconsistente** (resultados variam / nÃƒÂ£o acha) | Ã¢Å“â€¦ **#17** Renan testou Ã‚Â· Ã°Å¸â€œÂ¦ pronto produÃƒÂ§ÃƒÂ£o (fecha) | 29/06 |
| **FL-023** | **P1,2** | CP | Ao **buscar** na lista: **limpar filtros de data** | Ã¢Å“â€¦ 12/07 | 29/06 16:20 |
| **FL-024** | **P1,6** | Cadastro | **Zap #22:** cat/sub/marca â€” sÃ³ selecionar se existir Â· Food+PIN+log Â· busca sem acento | âœ… **loja v10.57** | 18/07 |
| **FL-025** | **P0,9** | Cadastro ERP | **SequÃƒÂªncia cÃƒÂ³digo interno** 9000+ Ã¢â€ â€™ **4010Ã¢â‚¬â€œ5999** | Ã¢Å“â€¦ 12/07 | 29/06 16:20 |
| **FL-026** | **P2** | Entrada NF | Add produto novo perde barras/lote | Ã¢Å“â€¦ 12/07 | 29/06 16:20 |
| **FL-027** | **P2** | Entrada NF | XML forma boleto Ã¢â€ â€™ **Boleto BancÃƒÂ¡rio CN** | Ã¢Å“â€¦ 12/07 | 29/06 16:20 |
| **FL-028** | **P1** | Fiado | BotÃƒÂ£o **Baixa** manda quitar **total de notas** de uma vez e **dÃƒÂ¡ erro** | Ã¢Å“â€¦ TolerÃƒÂ¢ncia centavos v7.36 | 29/06 16:20 |
| **FL-029** | **P1,1** | Fiado | Baixa parcial âœ… loja v7.61 Â· **falta** opÃ§Ã£o deixar em **crÃ©dito** | âš ï¸ parcial | 11/07 |
| **FL-030** | **P1,3** | Fiado / PDV | Forma de **ignorar bloqueio** por cliente com **notinhas fiado vencidas** Ã¢â‚¬â€ **PIN Geraldo / Geraldinho** | Ã°Å¸â€œâ€¹ Pendente | 29/06 16:20 |
| **FL-031** | **P1,6** | Entregas | **Terminar** de arrumar tela **`/entregas/`** | Ã°Å¸â€œâ€¹ Pendente | 29/06 16:20 |
| **FL-032** | **P1,5** | PDV | BotÃƒÂ£o **reset** no PDV Ã¢â‚¬â€ zerar pedido e **comeÃƒÂ§ar nova venda** | **Ã¢Å“â€¦ loja v7.27** | 29/06 16:20 |
| **FL-051** | **P1** | Fiado / PDV | **Baixa fiado no PDV** Ã¢â‚¬â€ Baixa em `/fiado/` Ã¢â€ â€™ pagamento wizard (formas + maquininha + Point); substitui modal atual | Ã¢Å“â€¦ **loja v7.40** | 07/07 |
| **FL-052** | **P1,1** | Fiado / fiscal | **NFC-e na baixa fiado** Ã¢â‚¬â€ emitir cupom na **quitaÃƒÂ§ÃƒÂ£o** com forma real (venda original `venda_agro`); validar contador/SEFAZ | Ã°Å¸â€œâ€¹ Fila apÃƒÂ³s **FL-051** | 07/07 |
| **FL-033** | **P3** | BI / Home | **Zap #21:** indicador comparativo Ã¢â‚¬â€ **N-ÃƒÂ©simo** dia da semana vs mÃƒÂªs anterior (ex. **3Ã‚Âª terÃƒÂ§a** Ãƒâ€” **3Ã‚Âª terÃƒÂ§a**) | Ã°Å¸â€œâ€¹ Pendente Ã‚Â· foto Word | 16/07 |
| **FL-053** | **P2** | Entrada NF / custo | **Zap #19:** histÃƒÂ³rico custo ÃƒÂºltimos pedidos **duplica tick** (fim etapa 2 + finalizar NF) | Ã°Å¸â€œâ€¹ Pendente Ã‚Â· foto Word | 16/07 |
| **FL-054** | **P1,5** | Entregas / impressÃƒÂ£o | **Zap #20:** reimprimir papÃƒÂ©is (separaÃƒÂ§ÃƒÂ£o Ã‚Â· entregador Ã‚Â· cliente) | Ã°Å¸â€œâ€¹ Pendente Ã‚Â· foto Word | 16/07 |
| **FL-055** | **P0,1** | NFC-e / frete | **Zap #23:** rejeiÃƒÂ§ÃƒÂ£o **535** Ã¢â‚¬â€ frete no total sem `vFrete` nos itens | Ã¢Å“â€¦ **loja v9.16** | 16/07 |
| **FL-056** | **P0** | NFC-e / SEFAZ | RejeiÃƒÂ§ÃƒÂµes **963** (fiado+card) + **225** (CFOP/CEST pontuaÃƒÂ§ÃƒÂ£o) Ã¢â‚¬â€ vendas #2812/#3347 | Ã°Å¸â€œÂ¦ **pronto pra envio** Ã‚Â· teste **v9.21** | 17/07 |
| **FL-057** | **P0,1** | Ops / Render / Postgres | **PgBouncer** na loja â€” `agro-db` pooling + `DATABASE_URL` porta **6432** + restart web | ðŸ“‹ Pendente Â· Renan no painel | 21/07 |
| **FL-058** | **P0,2** | PDV / Clientes / crÃ©dito | **Adicionar vale crÃ©dito** ao cliente **pelo PDV** (lanÃ§ar crÃ©dito na conta do cliente na loja) Â· liga FL-029 crÃ©dito | ðŸ“‹ Novo | 01/08 |
| **FL-034** | **P1,9** | PDV / Clientes | BotÃƒÂ£o **HistÃƒÂ³rico** nÃƒÂ£o filtra vendas do **cliente selecionado** Ã¢â‚¬â€ deve filtrar (relacionamento / devoluÃƒÂ§ÃƒÂ£o) | Ã°Å¸â€â€ž **F8 modal rascunho** teste Ã‚Â· fila loja | 29/06 16:20 |
| **FL-035** | **P2** | DevoluÃƒÂ§ÃƒÂ£o | **DevoluÃƒÂ§ÃƒÂ£o parcial** da venda Ã¢â‚¬â€ ou **itens especÃƒÂ­ficos** | Ã°Å¸â€œÂ¦ **#12** pronto loja (fecha) Ã‚Â· Ã¢Å“â€¦ teste Renan | 29/06 16:20 |
| **FL-036** | **P3** | PDV / Promo | **Faixa vertical** ou chaves ligando selos do **mesmo mix** no carrinho (opÃƒÂ§ÃƒÂ£o visual 2) | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-037** | **P3** | PDV / Promo | **Selo mix ÃƒÂºnico** entre linhas (rowspan / bloco central Ã¢â‚¬â€ opÃƒÂ§ÃƒÂ£o 3 experimental) | Ã°Å¸â€œâ€¹ Pendente | 29/06 |
| **FL-038** | **P2** | Deploy | **ContingÃƒÂªncia deploy** Ã¢â‚¬â€ Ã‚Â§**3.2.0** leigo Ã‚Â· Ã‚Â§3.2.4 tÃƒÂ©cnico Ã‚Â· **este deploy = manual Ã‚Â§3.2** | Ã°Å¸â€œâ€¹ CÃƒÂ³digo pendente | 30/06 |
| **FL-039** | **P3** | Clientes | **Pets/saÃƒÂºde/anotaÃƒÂ§ÃƒÂµes** na **ficha** `/clientes/` (hoje sÃƒÂ³ no F8) | Ã°Å¸â€œâ€¹ Pendente | 30/06 |
| **FL-040** | **P3** | Clientes / PDV | **Tabela Pet** normalizada no Postgres (opÃƒÂ§ÃƒÂ£o B Ã¢â‚¬â€ evoluir do JSON) | Ã°Å¸â€œâ€¹ Pendente | 30/06 |
| **FL-041** | **P3** | PDV | **Fila vendas offline** Ã¢â‚¬â€ processar no PC e sync depois (Renan descartou curto prazo) | Ã°Å¸â€œâ€¹ Pendente | 30/06 |
| **FL-046** | **P2** | PDV / Clientes | **2 janelas Chrome** (PDV + gestÃƒÂ£o) Ã‚Â· atalhos Ã‚Â· foco sem 2Ã‚Âº PDV | Ã¢Å“â€¦ loja **v6.00** | 01/07 |
| **FL-047** | **P2** | UX gestÃƒÂ£o | **Sidebar abas:** recolhida **~48px** sÃƒÂ³ ÃƒÂ­cones Ã‚Â· clique troca Ã‚Â· seta expande | Ã¢Å“â€¦ loja **v6.00** | 01/07 |
| **FL-048** | **P2** | Ops / Postgres | **Painel backup Postgres** Ã¢â‚¬â€ ZIP+Excel+restore Ã‚Â· `/interno/pg-backup/` Ã‚Â· Admin | Ã°Å¸Â§Âª **teste** 03/07 | 03/07 |
| **FL-049** | **P1,5** | PDV / Clientes / fiscal | CPF no PDV + NFC-e usa CPF salvo | âœ… **na loja** (`6af5cac`) | 04/07 |
| **FL-050** | **P2** | Vendas / fiscal | **`/vendas/`** Ã¢â‚¬â€ apÃƒÂ³s venda, **nÃƒÂ£o forÃƒÂ§ar** reemissÃƒÂ£o ao clicar cupom fiscal enquanto NFC-e roda em **background** Ã‚Â· avisar *aguarde* Ã‚Â· rÃƒÂ³tulo do botÃƒÂ£o muda (emitindo Ã¢â€ â€™ **reimprimir** quando OK) | Ã°Å¸Â§Âª **teste** 04/07 | 03/07 |
| **FL-042** | **P2** | PDV / Clientes | **HistÃƒÂ³rico ERP no F8** Ã¢â‚¬â€ **v5.46 teste** Ã‚Â· import 1Ãƒâ€” Ã‚Â· corte ERP **Ã¢â€°Â¤26/05** Ã‚Â· SisVale **Ã¢â€°Â¥27/05** | Ã°Å¸Â§Âª Render teste Ã‚Â· dry-run Ã¢â€ â€™ import | 30/06 |
| **FL-043** | **P2,8** | Fiado | BotÃƒÂ£o **desconto** na **baixa** do fiado | Ã°Å¸â€œâ€¹ Pendente | 30/06 |
| **FL-044** | **P2,9** | PDV / PreÃƒÂ§os / RH | **Desconto automÃƒÂ¡tico funcionÃƒÂ¡rio** Ã¢â‚¬â€ % prÃƒÂ©-definida Ã‚Â· provÃƒÂ¡vel junto com **tabelas de preÃƒÂ§o Ãƒâ€” forma de pagamento ou grupo de cliente** (ver **FL-001**) | Ã°Å¸â€œâ€¹ Pendente | 30/06 |

**Notas assistente (cÃƒÂ³digo interno Ã¢â‚¬â€ Renan ignora se quiser):**

| ID | Tag | Escopo tÃƒÂ©cnico resumido |
| -- | --- | --------------------- |
| FL-001 | `preco-tabela-forma-grupo` | Cadastro tabelas preÃƒÂ§o Ãƒâ€” forma ou Ãƒâ€” grupo Ã¢â‚¬â€ fora do `precos_por_forma` atual |
| FL-002 | `promo-ux-copy` | `promocoes` form/wizard Ã¢â‚¬â€ UX + textos Ã‚Â«?Ã‚Â» / labels / ajuda |
| FL-003 | `pdv-promo-badge-dre-cupom` | **Fase 1 Ã¢Å“â€¦ teste:** selo carrinho `pdv_wizard` + `pdv_promocoes.js` Ã‚Â· **Fase 2 Ã°Å¸â€œâ€¹:** DRE/relatÃƒÂ³rio Ã‚Â«desconto clientesÃ‚Â» Ã‚Â· **Fase 3 Ã°Å¸â€œâ€¹:** cupom 80mm/PDF venda |
| FL-004 | `rh-batida-ponto` | MÃƒÂ³dulo novo em `rh/` Ã¢â‚¬â€ hoje sÃƒÂ³ folha/vales/ficha; definir: tablet/celular, PIN, export folha, integraÃƒÂ§ÃƒÂ£o fechamento |
| FL-005 | `entrega-print-troco-r$` | `wizardPrintHtmlSeparacao` Ã¢â‚¬â€ hoje `troco: sim/nÃƒÂ£o`; falta **R$ a levar** (troco calculado = paga com Ã¢Ë†â€™ total) |
| FL-006 | `pdv-entregas-painel-link` | Fluxo PDV Ã¢â€ â€™ `entregas_painel.html` + polish visual painel |
| FL-007 | `layout-scale-audit` | **1Ã‚Âª tela:** `vendas_lista` / `/vendas/` Ã¢â‚¬â€ depois PDV, caixa, entrega, CPÃ¢â‚¬Â¦ |
| FL-008 | `pdv-carrinho-item-travado` | Bug: linha carrinho sem editar qty/preÃƒÂ§o/remover Ã¢â‚¬â€ reproduzir + `pdv_wizard.js` Ã‚Â· ex. **GM6083** |
| FL-009 | `etiquetas-autocomplete-aberto` | `produtos_etiquetas.js` / core Ã¢â‚¬â€ apÃƒÂ³s add na fila, manter painel de busca + foco no campo |
| FL-010 | `vendas-lista-filtros` | `vendas_lista` view + template Ã¢â‚¬â€ busca texto + filtros avanÃƒÂ§ados |
| FL-011 | `cashback-ux` | Tela cashback Ã¢â‚¬â€ mapear rota/template atual; UX + textos |
| FL-012 | `entrada-nf-desvincular-lixeira` | `entrada_nota.html` Ã¢â‚¬â€ API jÃƒÂ¡ tem `*_desvincular_de`; falta UI lixeira + fluxo com Queila |
| FL-013 | `etiquetas-barcode-png` | ImpressÃƒÂ£o etiquetas: trocar render (SVG/fonte?) por **PNG** raster Ã¢â‚¬â€ leitura scanner 1D/2D |
| FL-014 | `pdv-qty-ux` | Design qty no carrinho Ã¢â‚¬â€ `pdv_wizard.js` +/- e campo; menos cliques/teclado |
| FL-015 | `pdv-etiqueta-granel-scan` | `consulta_produtos.js` / `pdv_wizard.js` + `produtos_etiquetas*` Ã¢â‚¬â€ regra granel vs cÃƒÂ³digo loja |
| FL-016 | `caixa-reset-contagem` | Fechamento/abertura Ã¢â‚¬â€ contagem dia anterior; `caixa_util` / painel fechar |
| FL-017 | `caixa-devolucao-duplicada` | DevoluÃƒÂ§ÃƒÂ£o: estorno venda + movimento saÃƒÂ­da em duplicidade Ã¢â‚¬â€ `devolucao_*` views |
| FL-018 | `vendas-detalhe-frete` | `/vendas/` detalhe/lista total sem frete; caixa relatÃƒÂ³rio OK Ã¢â‚¬â€ alinhar `VendaAgro.frete` |
| FL-019 | `fiado-recibo-pagamento` | Fiado: impressÃƒÂ£o/PDF recibo ao registrar pagamento parcial |
| FL-020 | `cupom-frete-separado` | `venda_cupom_80mm` + NFC-e Ã¢â‚¬â€ frete nÃƒÂ£o linha de produto / nÃƒÂ£o base tributÃƒÂ¡vel cupom |
| FL-021 | `cp-btn-nf-ausente` | `lancamentos_contas_pagar_teste.html` Ã¢â‚¬â€ `urlEntradaNfeEmbed` / vÃƒÂ­nculo NF entrada Ã‚Â· ex. **RBS 781,64** |
| FL-022 | `cp-busca-inconsistente` | Filtro busca lista CP Ã¢â‚¬â€ `mongo_financeiro_util` + JS modal filtros |
| FL-023 | `cp-busca-limpa-datas` | Ao buscar na lista CP: resetar filtros de **data** (perÃƒÂ­odo nÃƒÂ£o deve persistir na busca textual) |
| FL-024 | `cadastro-popup-cat-marca-food` | **#22:** nÃƒÂ£o auto-criar ao digitar Ã‚Â· select sÃƒÂ³ se existir Ã‚Â· popup Food+PIN+log Ã‚Â· busca CI sem acento Ã‚Â· cat/sub/marca |
| FL-025 | `codigo-interno-seq-4k` | SequÃƒÂªncia cÃƒÂ³digo interno GM Ã¢â‚¬â€ hoje **9000+**; alvo combinado **~4000Ã¢â‚¬â€œ5000** Ã¢â‚¬â€ `cadastro_erp` / overlay |
| FL-026 | `entrada-nf-perde-conferencia` | Add linha nova na NF: zera barras (passo 3) e lote/val (4Ã¢â‚¬â€œ5) dos itens jÃƒÂ¡ conferidos |
| FL-027 | `entrada-nf-xml-forma-boleto-cn` | Parse XML etapa 7: mapear forma pag. **Boleto BancÃƒÂ¡rio CN** (nÃƒÂ£o sÃƒÂ³ Ã‚Â«Boleto BancÃƒÂ¡rioÃ‚Â») |
| FL-028 | `fiado-baixa-lote-erro` | BotÃƒÂ£o baixa fiado tenta quitar **todas** as notas Ã¢â‚¬â€ erro; fluxo deve ser seletivo ou corrigir aggregate |
| FL-029 | `fiado-baixa-parcial-credito` | Validar baixa parcial + saldo em **crÃƒÂ©dito** cliente |
| FL-030 | `fiado-ignorar-vencido-pin` | Override bloqueio notinhas vencidas Ã¢â‚¬â€ PIN **Geraldo** / **Geraldinho** |
| FL-031 | `entregas-polish` | Continuar FL-006 Ã¢â‚¬â€ `entregas_painel.html` + APIs |
| FL-032 | `pdv-reset-nova-venda` | BotÃƒÂ£o explÃƒÂ­cito reset carrinho/contexto e nova venda |
| FL-033 | `bi-vendas-dia-nth-weekday` | **#21:** dashboard N-ÃƒÂ©simo weekday vs mÃƒÂªs anterior |
| FL-053 | `entrada-nf-historico-custo-duplo` | **#19:** tick histÃƒÂ³rico custo 2Ãƒâ€” (etapa 2 + finalize) Ã¢â‚¬â€ dedupe / um sÃƒÂ³ evento |
| FL-054 | `entregas-reimprimir-papeis` | **#20:** reimpressÃƒÂ£o separaÃƒÂ§ÃƒÂ£o / entregador / cliente no painel entregas |
| FL-055 | `nfce-frete-vfrete-itens-535` | **#23:** `det/prod/vFrete` = `ICMSTot/vFrete` (535) |
| FL-056 | `nfce-963-card-fiado-225-fiscal-digitos` | **963:** sem `card` em tPag 05 Ã‚Â· **225:** NCM/CFOP/CEST sÃƒÂ³ dÃƒÂ­gitos |
| FL-057 | `render-pgbouncer-agro-db-6432` | Render: Info do Postgres â†’ Connection pooling Â· web loja `DATABASE_URL` pooled **:6432** Â· redeploy Â· conferir healthz |
| FL-058 | `pdv-vale-credito-cliente` | PDV: lanÃ§ar / adicionar **vale crÃ©dito** ao cliente selecionado (Postgres) Â· uso depois na venda Â· overlap FL-029 crÃ©dito |
| FL-034 | `pdv-historico-cliente-filtro` | HistÃƒÂ³rico vendas deve respeitar **cliente selecionado** no PDV |
| FL-035 | `devolucao-parcial-itens` | DevoluÃƒÂ§ÃƒÂ£o por itens / parcial Ã¢â‚¬â€ hoje provavelmente venda inteira |
| FL-036 | `pdv-mix-selo-faixa-vertical` | Faixa/chaves CSS ligando coluna promo entre linhas do mesmo mix (opÃƒÂ§ÃƒÂ£o 2) |
| FL-037 | `pdv-mix-selo-rowspan` | Selo mix ÃƒÂºnico central entre linhas Ã¢â‚¬â€ experimental (opÃƒÂ§ÃƒÂ£o 3) |
| FL-038 | `deploy-contingencia` | Postgres escopos Ã‚Â· cron ON/OFF Ã‚Â· middleware POSTs por mÃƒÂ³dulo (pdv, caixa, fiado, devolucao, lancamentos) Ã‚Â· Ã‚Â§3.2.4 |
| FL-039 | `cliente-ficha-pets-relacionamento` | Exibir/editar `relacionamento_extras_json` na ficha `/clientes/` |
| FL-040 | `cliente-pet-tabela-normalizada` | Modelo `ClientePetAgro` (+ lembretes) Ã¢â‚¬â€ migrar do JSON quando priorizar |
| FL-041 | `pdv-fila-vendas-offline` | Projeto grande: fila local + sync + estoque/fiado Ã¢â‚¬â€ **nÃƒÂ£o** substitui FL-038 curto prazo |
| FL-042 | `relacionamento-import-erp-1x` | Mongo DtoVenda 1Ãƒâ€” Ã¢â€ â€™ Postgres histÃƒÂ³rico Ã‚Â· merge F8 Ã‚Â· **sem** leitura Mongo no PDV Ã‚Â· Excel = audit |
| FL-043 | `fiado-baixa-desconto` | UI + backend baixa fiado Ã¢â‚¬â€ aplicar **desconto** no pagamento (parcial ou total) |
| FL-044 | `pdv-desconto-funcionario-auto` | % desconto por funcionÃƒÂ¡rio/cliente grupo Ã‚Â· overlap **FL-001** (tabela preÃƒÂ§o Ãƒâ€” forma ou Ãƒâ€” grupo) |
| FL-048 | `pg-backup-painel-portavel` | Admin Ã¢â€ â€™ `/interno/pg-backup/` Ã‚Â· ZIP manifest+JSONL+**resumo.xlsx** Ã‚Â· checkbox Ã‚Â· restore+senha admin |
| FL-049 | `pdv-cliente-cpf-cadastro-nfce` | Campo **CPF** no cadastro cliente PDV (F8/modal) Ã‚Â· persistir `ClienteAgro` Ã‚Â· emissÃƒÂ£o NFC-e puxa CPF do cliente selecionado (hoje: modal sÃƒÂ³ se cadastro vazio) |
| FL-050 | `vendas-lista-nfce-background-ux` | `/vendas/` + detalhe: distinguir **processando** (`nfce_solicitada` sem doc / status PROCESSANDO) vs **erro** vs **autorizada** Ã‚Â· nÃƒÂ£o abrir modal reemitir no meio Ã‚Â· poll ou refresh Ã‚Â· rÃƒÂ³tulos: *EmitindoÃ¢â‚¬Â¦* / *Reimprimir cupom fiscal* Ã‚Â· `vendas_lista.html` Ã‚Â· `nfce_venda_util.painel_nfce_venda` Ã‚Â· `views_nfce` |
| FL-051 | `fiado-baixa-pdv-pagamento` | `/fiado/` Baixa Ã¢â€ â€™ `/pdv/checkout/` modo cobranÃƒÂ§a (`titulo_id`/`titulo_ids`) Ã‚Â· pagamento completo PDV Ã‚Â· confirmar quita `FiadoTituloAgro` + `MovimentoCaixa` Ã‚Â· deprecar modal `fiado-modal-baixa` |
| FL-052 | `fiado-baixa-nfce-quitacao` | ApÃƒÂ³s baixa: NFC-e da **venda original** com `tPag` da forma paga (nÃƒÂ£o crÃƒÂ©dito loja) Ã‚Â· CPF cliente Ã‚Â· conferÃƒÂªncia fiscal antes de auto |

**Notas FL-051 / FL-052 (07/07):** Renan escolheu **sempre PDV** (nÃƒÂ£o hÃƒÂ­brido). **FL-051** = **P1** (operacional). **FL-052** = **P1,1** (fiscal na fila, nÃƒÂ£o no 1Ã‚Âº pacote). Incluir fix **FL-028** se possÃƒÂ­vel no mesmo pacote pagamento.

**Notas FL-050 (03/07):** **P2** Ã¢â‚¬â€ Renan: operador vai direto em Consulta vendas apÃƒÂ³s Enter no PDV; clique trata como reemissÃƒÂ£o. Causa provÃƒÂ¡vel: `venda_nfce_pendente` = true com `nfce_solicitada` e sem `NfceDocumentoAgro` ainda. `views_nfce` jÃƒÂ¡ retorna Ã‚Â«em processamentoÃ‚Â» no POST duplicado Ã¢â‚¬â€ falta UX na **lista**.

**Notas FL-049 (03/07):** **P1,5** Ã¢â‚¬â€ junto **FL-019** Ã‚Â· **FL-020** Ã‚Â· **FL-032** na faixa. Objetivo: operador cadastra CPF uma vez no PDV; cupom fiscal usa automaticamente. Conferir se ficha `/clientes/` jÃƒÂ¡ tem CPF e espelhar no F8.

**Notas FL-043 / FL-044 (30/06):** **P2,8** desconto na baixa fiado Ã‚Â· **P2,9** desconto auto funcionÃƒÂ¡rio (% cadastro) Ã¢â‚¬â€ Renan acha que depende de **FL-001** (preÃƒÂ§o por forma/grupo). **FL-033** (BI vendas dia) ficou **P2,91** (liberou **P2,9** para **FL-044**).

**Notas lote 29/06 16:20:** **FL-025** **P0,9** (quase P1 Ã¢â‚¬â€ sequÃƒÂªncia cÃƒÂ³digo). **FL-028** **P1** fiado baixa em lote. **FL-029** reforÃƒÂ§a fiado (**P1,1**, junto FL-019 recibo). **FL-030** PINs nomeados Ã¢â‚¬â€ conferir usuÃƒÂ¡rios no admin. **FL-031** overlap com **FL-006** entregas. **FL-032** outro **P1,5** PDV (FL-020 = cupom frete).

**Notas FL-021 / FL-022 (CP Ã¢â‚¬â€ 29/06):** **P1,1**. FL-021: coluna/botÃƒÂ£o **NF** sÃƒÂ³ renderiza se `urlEntradaNfeEmbed(row)` achar vÃƒÂ­nculo entrada NF Ã¢â‚¬â€ reproduzir com fornecedor **RBS** Ã‚Â· valor **R$ 781,64**. FL-022: busca na lista (modal Filtros) Ã¢â‚¬â€ anotar termo que falha vs que acha.

**Notas FL-016 / FL-017 (caixa Ã¢â‚¬â€ 29/06):** dois **P1** operacionais. FL-017: conferir se devoluÃƒÂ§ÃƒÂ£o gera movimento caixa **e** reverte venda sem idempotÃƒÂªncia. Pedir nÃ‚Âº venda + print fechamento se repetir.

**Notas FL-018:** divergÃƒÂªncia **tela vendas** vs **relatÃƒÂ³rio caixa** Ã¢â‚¬â€ bug de exibiÃƒÂ§ÃƒÂ£o/cÃƒÂ¡lculo no detalhe, nÃƒÂ£o necessariamente no caixa.

**Notas FL-020:** frete hoje pode entrar no total do cupom/NFC-e; Renan quer **fora** do cupom fiscal e do cupom de venda (cobranÃƒÂ§a ÃƒÂ  parte ou sÃƒÂ³ entrega).

**Notas FL-001:** hoje o PDV jÃƒÂ¡ aplica preÃƒÂ§o por forma (`precos_por_forma` / promoÃƒÂ§ÃƒÂµes). Escopo novo = cadastro de **tabelas** (vÃƒÂ¡rios preÃƒÂ§os por produto Ãƒâ€” forma ou Ãƒâ€” grupo cliente) Ã¢â‚¬â€ projeto grande; definir regras com Renan antes de codar.

**Notas FL-007:** Renan (29/06) Ã¢â‚¬â€ **priorizar `/vendas/`** como piloto do ajuste de layout (Agro Display Scale Ã‚Â§11); usar como referÃƒÂªncia antes das demais telas.

**Notas FL-015:** poucos testes na loja (29/06). Conferir: cÃƒÂ³digo impresso na etiqueta granel vs parser PDV (`eh_granel`, prefixo GM, peso embutido). Pedir 2Ã¢â‚¬â€œ3 cÃƒÂ³digos que falharam + print da etiqueta.

**Notas FL-013:** hipÃƒÂ³tese Ã¢â‚¬â€ barras em SVG/fonte na folha saem finas/baixo contraste para leitor; gerar PNG (ou aumentar quiet zone / altura) na impressÃƒÂ£o de preÃƒÂ§o.

**Notas FL-012:** backend parcial em `entrada_nota` / overlay (`c_prod_nf_desvincular_de`, `ean_embalagem_nf_desvincular_de`). **Pendente:** conversa com **Queila** Ã¢â‚¬â€ quando usar lixeira, nota em qual etapa, confirmaÃƒÂ§ÃƒÂ£o.

**Notas FL-005:** prÃƒÂ©-anÃƒÂ¡lise 29/06 Ã¢â‚¬â€ via **SeparaÃƒÂ§ÃƒÂ£o** jÃƒÂ¡ imprime Ã‚Â«troco: sim/nÃƒÂ£oÃ‚Â»; **nÃƒÂ£o** imprime o **valor em reais** a levar. Campo `entrega.troco` no PDV = Ã‚Â«cliente paga comÃ‚Â». Implementar linha explÃƒÂ­cita tipo **Ã‚Â«Levar troco: R$ X,XXÃ‚Â»** nas vias separaÃƒÂ§ÃƒÂ£o + entregador (se dinheiro na entrega).

**Notas FL-008:** **P1** Ã¢â‚¬â€ impacto direto no balcÃƒÂ£o. **Caso reportado (29/06):** **GM6083** Ã¢â‚¬â€ apÃƒÂ³s add no carrinho, qtd/preÃƒÂ§o/remover nÃƒÂ£o respondem; sÃƒÂ³ Ã‚Â«Limpar carrinhoÃ‚Â». Reproduzir no staging com esse cÃƒÂ³digo; anotar se promo ou forma de pagamento estava ativa.

**Notas FL-004:** projeto **novo** Ã¢â‚¬â€ RH atual cobre folha, vales e ficha; **nÃƒÂ£o** tem ponto. Antes de codar: Renan define se ÃƒÂ© sÃƒÂ³ registro interno, se entra no fechamento da folha e quem bate (PIN, lista na loja, celular).

**Notas FL-003:** **P2**. **Fase 1 (29/06):** selo na linha do carrinho (entre GM e qtd) Ã¢â‚¬â€ verde Ã‚Â«PROMO 4Ãƒâ€”Ã‚Â», amarelo Ã‚Â«Faltam NÃ‚Â», misto Ã‚Â«4 promoÃ‚Â» + Ã‚Â«+1 normalÃ‚Â» quando passa do bloco leve X; lixeira no lugar de Ã‚Â«RemoverÃ‚Â». SÃƒÂ³ exibiÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ preÃƒÂ§o jÃƒÂ¡ vem da regra leve X. **Fase 2:** alinhar com Renan se Ã‚Â«desconto clientesÃ‚Â» = plano DRE existente ou campo novo na venda. **Fase 3:** cupom/impressÃƒÂ£o 80 mm.

### FECHADO + DEPLOY LOJA Ã¢â‚¬â€ hotfix promo Ã‚Â«ContinuarÃ‚Â» **v4.90** (29/06)

**Renan:** *Ã‚Â«mandaÃ‚Â»* + senha **99738595**.

**Loja (`producao`):** merge **`teste`** Ã¢â€ â€™ **`producao`** Ã‚Â· fast-forward **`54aa23b`** Ã‚Â· **push OK**.

**PÃƒÂ³s-deploy loja:** Ctrl+F5 Ã‚Â· Nova promoÃƒÂ§ÃƒÂ£o Ã¢â€ â€™ **Continuar Ã¢â‚¬â€ escolher produtos** Ã¢â€ â€™ etapa 2.

### INCIDENTE LOJA Ã¢â‚¬â€ promo Ã‚Â«ContinuarÃ‚Â» nÃƒÂ£o avanÃƒÂ§a etapa 1 **v4.90** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Clicar **Continuar Ã¢â‚¬â€ escolher produtos** nÃƒÂ£o vai para etapa 2 |
| **Causa** | Erro de sintaxe JS no v4.88 Ã¢â‚¬â€ script inteiro nÃƒÂ£o carregava |
| **Fix v4.90** | Restaurar `tentarAutoAdicionarBusca` + bind **Continuar** independente da etapa 2 |

### FECHADO + DEPLOY LOJA Ã¢â‚¬â€ promo UX + keep-warm staging **v4.86Ã¢â‚¬â€œv4.88** (29/06)

**Renan:** *Ã‚Â«pode subir para produÃƒÂ§ÃƒÂ£oÃ‚Â»* + senha **99738595** (nÃƒÂ£o perigoso para loja em uso).

**Loja (`producao`):** merge **`teste`** Ã¢â€ â€™ **`producao`** Ã‚Â· fast-forward **`871c4b4`** Ã‚Â· **push OK**.

**PÃƒÂ³s-deploy loja:** Ctrl+F5 Ã‚Â· PromoÃƒÂ§ÃƒÂµes etapa 2 Ã¢â‚¬â€ add vÃƒÂ¡rios da mesma busca Ã‚Â· bip/autocomplete GM/nome Ã‚Â· **Excluir** duplicatas.

| Pacote | O quÃƒÂª | Risco loja |
| ------ | ----- | ---------- |
| **v4.88** | + Add mantÃƒÂ©m lista de busca | SÃƒÂ³ tela promoÃƒÂ§ÃƒÂ£o |
| **v4.83Ã¢â‚¬â€œv4.84** | Bip + autocomplete GM/nome | SÃƒÂ³ tela promoÃƒÂ§ÃƒÂ£o |
| **v4.81** | BotÃƒÂ£o Excluir promoÃƒÂ§ÃƒÂ£o | PG |
| **v4.86Ã¢â‚¬â€œv4.87** | Cron ping Mongo 5 min + keep-warm **staging** | NÃƒÂ£o acelera nem desacelera PDV/caixa |

### FECHADO + DEPLOY LOJA Ã¢â‚¬â€ promoÃƒÂ§ÃƒÂµes bip + autocomplete GM/nome **v4.83Ã¢â‚¬â€œv4.84** (29/06)

**Renan:** *Ã‚Â«manda direto produÃƒÂ§ÃƒÂ£oÃ‚Â»* + senha **99738595** (Render teste lento na 1Ã‚Âª abertura Ã¢â‚¬â€ validaÃƒÂ§ÃƒÂ£o direto na loja).

**Loja (`producao`):** merge **`teste`** Ã¢â€ â€™ **`producao`** Ã‚Â· fast-forward **`536c5c5`** Ã‚Â· **push OK**.

**PÃƒÂ³s-deploy loja:** Ctrl+F5 Ã‚Â· PromoÃƒÂ§ÃƒÂµes etapa 2 Ã‚Â· bip barras Ã‚Â· autocomplete GM/nome Ã‚Â· **Excluir** duplicatas se ainda houver.

| Item | Detalhe |
| ---- | ------- |
| **v4.83Ã¢â‚¬â€œv4.84** | Barras entra direto Ã‚Â· GM/nome autocomplete Ã‚Â· GM completo match exato Ã‚Â· botÃƒÂ£o Excluir (v4.81) |
| **Dados** | PromoÃƒÂ§ÃƒÂµes 100 % Postgres |

### FECHADO + DEPLOY LOJA Ã¢â‚¬â€ promoÃƒÂ§ÃƒÂµes botÃƒÂ£o Excluir **v4.81** (29/06)

**Renan:** *Ã‚Â«manda produÃƒÂ§ÃƒÂ£oÃ‚Â»* + senha **99738595**.

**Loja (`producao`):** merge **`teste`** Ã¢â€ â€™ **`producao`** Ã‚Â· fast-forward **`536527a`** Ã‚Â· **push OK**.

**PÃƒÂ³s-deploy loja:** Ctrl+F5 Ã‚Â· PromoÃƒÂ§ÃƒÂµes Ã¢â€ â€™ **Excluir** duplicatas Ã‚Â· manter a de 4 produtos.

| Item | Detalhe |
| ---- | ------- |
| **v4.81** | BotÃƒÂ£o **Excluir** na lista Ã‚Â· confirmaÃƒÂ§ÃƒÂ£o Ã‚Â· PG (`PromocaoAgro` CASCADE) |
| **Dados** | PromoÃƒÂ§ÃƒÂµes 100 % Postgres Ã¢â‚¬â€ Mongo sÃƒÂ³ na busca de produto |

### FECHADO + DEPLOY LOJA Ã¢â‚¬â€ fix promoÃƒÂ§ÃƒÂ£o Ã‚Â«SalvarÃ‚Â» **v4.80** (29/06)

**Renan:** *Ã‚Â«pode subir diretoÃ‚Â»* + senha **99738595** (fix isolado Ã¢â‚¬â€ sÃƒÂ³ mensagem JS, sem banco/API).

**Loja (`producao`):** merge **`teste`** Ã¢â€ â€™ **`producao`** Ã‚Â· **push OK** (apÃƒÂ³s resolver conflito `banana.md`).

**PÃƒÂ³s-deploy loja:** Ctrl+F5 Ã‚Â· Nova promoÃƒÂ§ÃƒÂ£o Ã¢â€ â€™ produtos Ã¢â€ â€™ **Salvar promoÃƒÂ§ÃƒÂ£o** Ã¢â€ â€™ mensagem verde + lista.

### FIX Ã¢â‚¬â€ promoÃƒÂ§ÃƒÂ£o Ã‚Â«SalvarÃ‚Â» quebra com classList (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Etapa 2 Ã‚Â· adicionar produtos Ã‚Â· **Salvar promoÃƒÂ§ÃƒÂ£o** Ã¢â€ â€™ erro `DOMTokenList` / token com espaÃƒÂ§os |
| **Causa** | `showMsg` passava 3 classes Tailwind numa string sÃƒÂ³ para `classList.add()` |
| **Fix v4.80** | Cada classe como argumento separado |
| **Risco loja** | **Baixo** Ã¢â‚¬â€ 2 linhas JS na tela de promoÃƒÂ§ÃƒÂ£o; nÃƒÂ£o altera PDV, caixa, financeiro |

### FECHADO + DEPLOY LOJA Ã¢â‚¬â€ fix endereÃƒÂ§o entrega grudado **v4.78** (29/06)

**Renan:** *Ã‚Â«subaÃ‚Â»* + senha **99738595**.

**Loja (`producao`):** merge **`teste`** Ã¢â€ â€™ **`producao`** Ã‚Â· commit **`91dbc09`** Ã‚Â· merge **`6791b80`** Ã‚Â· **push OK**.

**PÃƒÂ³s-deploy loja:** Ctrl+F5 Ã‚Â· trocar cliente na entrega F3 Ã¢â€ â€™ endereÃƒÂ§o do cadastro (nÃƒÂ£o gruda o da venda anterior).

### INCIDENTE LOJA Ã¢â‚¬â€ endereÃƒÂ§o entrega grudado em todos os clientes (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Todo cliente na entrega F3 mostrava o mesmo endereÃƒÂ§o do teste (Av. AdhemarÃ¢â‚¬Â¦) |
| **Causa** | EndereÃƒÂ§o da venda anterior ficava no estado da sessÃƒÂ£o; ao trocar cliente nÃƒÂ£o limpava |
| **Fix v4.78** | Troca de cliente repÃƒÂµe endereÃƒÂ§o do cadastro; F3 entrega sincroniza; voltar a Produtos zera |
| **Agora na loja** | ApÃƒÂ³s deploy **v4.78** Ã‚Â· **Ctrl+F5** se ainda aparecer endereÃƒÂ§o antigo |

### FECHADO + DEPLOY LOJA Ã¢â‚¬â€ merge teste Ã¢â€ â€™ producao **v4.77** (29/06)

**Renan:** *Ã‚Â«manda para produÃƒÂ§ÃƒÂ£o - seja oque Deus quiserÃ‚Â»* + senha **99738595**.

**Loja (`producao`):** merge **`teste`** Ã¢â€ â€™ **`producao`** Ã‚Â· pacote PDV entrega **v4.59Ã¢â‚¬â€œv4.77** Ã‚Â· **push OK** Ã‚Â· commit **`987f75c`**.

**PÃƒÂ³s-deploy loja:** Ctrl+F5 Ã‚Â· PDV entrega F3 Ã¢â€ â€™ espaÃƒÂ§o no logradouro Ã‚Â· Conferir entrega (frete/total) Ã‚Â· popups entrega revisados.

### PDV entrega fluxo Ã¢â‚¬â€ sequÃƒÂªncia taxa/troco (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | Taxa fora do form de endereÃƒÂ§o; popup apÃƒÂ³s endereÃƒÂ§o; dinheiro/cartÃƒÂ£o depois da taxa; troco mostra total |
| **SequÃƒÂªncia** | F3 Ã¢â€ â€™ pagamento local Ã¢â€ â€™ endereÃƒÂ§o Ã¢â€ â€™ taxa+horÃƒÂ¡rio Ã¢â€ â€™ meio Ã¢â€ â€™ troco (total) Ã¢â€ â€™ enviar/ir pagamento |
| **Futuro** | `entregaTaxaDevePularAuto()` Ã¢â‚¬â€ frete grÃƒÂ¡tis por endereÃƒÂ§o omite popup taxa |
| **v4.77** | Entrega endereÃƒÂ§o: espaÃƒÂ§o no logradouro (trim sÃƒÂ³ ao sair do campo / F7 Ã¢â‚¬â€ nÃƒÂ£o a cada tecla) |
| **v4.76** | Conferir entrega: frete R$ 10 (e total) corrigido Ã¢â‚¬â€ nÃƒÂ£o zerava ao confirmar taxa |
| **v4.75** | Fix regressÃƒÂ£o: Conferir entrega vazava p/ Pagamento (div extra v4.65); JS+CSS forÃƒÂ§a esconder fora da etapa 2 |
| **v4.74** | Conferir entrega: sem vÃƒÂ£o vazio no meio; texto maior; partida visÃƒÂ­vel no resumo |
| **v4.73** | Popup Dinheiro/CartÃƒÂ£o (e pagamento na entrega/loja): card ~3Ãƒâ€” maior; botÃƒÂµes altos |
| **v4.72** | Tela endereÃƒÂ§o entrega: grade uniforme (sem vÃƒÂ£o vazio); labels/campos maiores; obs em 1 linha |
| **v4.71** | Popup pagamento local/meio mais largo (~40rem); botÃƒÂµes sem quebra de linha |
| **v4.70** | Fix: `</div>` sobrando em `step_entrega.html` (v4.65) exibia Conferir entrega na etapa Produtos; JS guarda visibilidade fora de entrega |
| **v4.69** | Popups entrega: tipografia maior (tÃƒÂ­tulo, total troco, campos, botÃƒÂµes) |
| **v4.68** | Popup troco: layout compacto; nÃƒÂ£o sobrepÃƒÂµe conferir entrega; corpo do card visÃƒÂ­vel |
| **v4.67** | Popup taxa/horÃƒÂ¡rio: grade uniforme (3 frete + valor|horÃƒÂ¡rio); confirma sÃƒÂ³ no rodapÃƒÂ© F7 |
| **v4.66** | Fix popup entrega sÃƒÂ³ cabeÃƒÂ§alho: sync nÃƒÂ£o esconde botÃƒÂµes ao ir pro endereÃƒÂ§o |
| **v4.65** | Popup entrega: card `fit-content` (sem faixa branca); overlay fora da etapa; clique no Voltar liberado |
| **v4.64** | Popups entrega compactos (altura = conteÃƒÂºdo); rodapÃƒÂ© Voltar/F7 clicÃƒÂ¡vel de novo |
| **v4.63** | Conferir entrega sem rolagem: 3 colunas, Partida oculta, total no topo, obs em 1 linha |
| **v4.62** | ImpressÃƒÂ£o mais leve: JsBarcode 1Ãƒâ€” na pÃƒÂ¡gina, sem pop-up extra no PDV, modais sem blur GPU |
| **v4.61** | tela **Conferir entrega** revisÃƒÂ£o ERP (grade label/valor, total lateral, painel ÃƒÂºnico) |
| **v4.60** | popups entrega overlay fixo 16:9 |
| **v4.59** | fix popup cadastro sÃƒÂ³ quando usuÃƒÂ¡rio altera dados na tela |

### PDV entrega fluxo Ã¢â‚¬â€ fix (29/06 madrugada)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | ApÃƒÂ³s **Entregar F3**, repetia Ã‚Â«Retirada ou entrega?Ã‚Â» e Ã‚Â«Taxa/horÃƒÂ¡rio/trocoÃ‚Â» antes do pagamento |
| **Fix** | F3 Ã¢â€ â€™ direto **Onde serÃƒÂ¡ o pagamento?**; taxa+horÃƒÂ¡rio no form de endereÃƒÂ§o; troco sÃƒÂ³ no fluxo dinheiro |
| **Tela cinza** | Etapa entrega sem wizard nem form Ã¢â‚¬â€ `return` no render bloqueava tela Ã‚Â· fix `a43a8fd` **v4.54** |
| **Teste** | Ctrl+F5 apÃƒÂ³s deploy Ã‚Â· Entregar F3 Ã¢â€ â€™ card Ã‚Â«Onde serÃƒÂ¡ o pagamento?Ã‚Â» |

### AGENDA AMANHÃƒÆ’ Ã¢â‚¬â€ itens **8Ã¢â‚¬â€œ11** Ã‚Â§4.15 (Renan **29/06 noite**)

**DecisÃƒÂ£o Renan:** *Ã‚Â«amanhÃƒÂ£ mexemos com issoÃ‚Â»* Ã¢â‚¬â€ retomar **backup + congelar + checkpoint CP** (trilha operacional antes do item **12** cancelar ERP). **Outro chat** aberto para **outro problema** (nÃƒÂ£o ÃƒÂ© CP) Ã¢â‚¬â€ este tÃƒÂ³pico continua aqui no CHECKPOINT.

**Por que mexer se CP jÃƒÂ¡ funciona no Postgres?** OperaÃƒÂ§ÃƒÂ£o diÃƒÂ¡ria **nÃƒÂ£o precisa** Ã¢â‚¬â€ 8Ã¢â‚¬â€œ11 ÃƒÂ© **fechamento seguro** (cÃƒÂ³pia no PC, carimbo Mongo, prova de totais) **antes** de cancelar assinatura ERP. NÃƒÂ£o ÃƒÂ© consertar CP.

**O que jÃƒÂ¡ estÃƒÂ¡ OK (nÃƒÂ£o refazer do zero):**

| Item | Status |
| ---- | ------ |
| CP/CR **lista + pagar** na **loja** | **Postgres** Ã‚Â· **741** em aberto Ã‚Â· **~R$ 393.652,70** (25Ã¢â‚¬â€œ26/06) |
| **Backup PC (#10)** | **Ã¢Å“â€¦ 25/06** Ã¢â‚¬â€ ZIP abertos + Excel no PC |
| **Checkpoint parcial (#11)** | **Ã¢Å“â€¦** ~17,7 mil tÃƒÂ­tulos carimbo + **corte AgroÃ¢â€ â€™ERP** API (v1.14) Ã¢â‚¬â€ SisVale **nÃƒÂ£o envia** baixa pro WL |
| **Congelar Mongo (#8)** | **Off** Ã¢â‚¬â€ flag `AGRO_FINANCEIRO_MONGO_CONGELADO` ainda **nÃƒÂ£o** na loja |
| CÃƒÂ³digo desvinculaÃƒÂ§ÃƒÂ£o | **teste v4.51** cadastro aux PG Ã‚Â· loja **v4.49** |

**Ordem amanhÃƒÂ£ (Renan + assistente):**

| Passo | # | O quÃƒÂª | Onde |
| ----- | - | ----- | ---- |
| **1** | **10** | Backup **atualizado** no PC (CP abertos ZIP + Excel completo + cadastro Excel se quiser) | **sistvale.com.br** (loja) Ã¢â‚¬â€ URLs abaixo |
| **2** | **11** | Conferir totais CP **deduplicados** (referÃƒÂªncia = **tela**, nÃƒÂ£o soma crua CSV) | Loja Ã‚Â· filtros Em aberto sem data |
| **3** | **8** | Congelar financeiro Mongo + `AGRO_FINANCEIRO_MONGO_CONGELADO=true` | **Render SistVale (produÃƒÂ§ÃƒÂ£o)** Ã¢â‚¬â€ **nÃƒÂ£o** ensaio no teste |
| **4** | **11** | Se faltar carimbo: botÃƒÂ£o **CONGELAR** em `/lancamentos/` (admin) ou `manage.py congelar_lancamentos_financeiro_agro` | **Loja** |
| **5** | Ã¢â‚¬â€ | Conferir `/api/agro/fonte-status/` Ã‚Â· `financeiro_mongo_congelado: true` | Loja |

**URLs backup (loja):**

| Arquivo | URL |
| ------- | --- |
| Em aberto | `https://sistvale.com.br/api/lancamentos/backup-abertos.zip` |
| Todos | `https://sistvale.com.br/api/lancamentos/backup-completo.xlsx` |

**Teste vs loja (importante Ã¢â‚¬â€ conversa 29/06):**

| Ambiente | CP Postgres | Mongo |
| -------- | ----------- | ----- |
| **Render teste** | Banco **isolado** Ã¢â‚¬â€ pagar/editar no teste **nÃƒÂ£o mexe** na loja | **LÃƒÂª** espelho compartilhado Ã‚Â· `AGRO_STAGING_READONLY` bloqueia quase toda gravaÃƒÂ§ÃƒÂ£o |
| **Render loja** | CP real da operaÃƒÂ§ÃƒÂ£o | Espelho compartilhado Ã‚Â· **congelar/checkpoint definitivo ÃƒÂ© aqui** |

**Cuidado:** botÃƒÂ£o **CONGELAR** no **teste** ainda **carimba** o Mongo compartilhado (nÃƒÂ£o apaga valores, mas ÃƒÂ© redundante). Ritual **8Ã¢â‚¬â€œ11 = loja**.

**ReferÃƒÂªncia totais (dedup):** Qtd **741** Ã‚Â· A pagar **~R$ 393.652,70** Ã‚Â· Excel abertos **748 linhas** (+7 dup. ERP, ex. Geraldo) Ã¢â‚¬â€ **usar tela**.

**Render amanhÃƒÂ£:** assistente **sÃƒÂ³ mexe env produÃƒÂ§ÃƒÂ£o** se Renan pedir **explÃƒÂ­cito** na sessÃƒÂ£o. **NÃƒÂ£o** push `producao` / cherry-pick loja sem frase + senha **99738595**.

**Depois de 8Ã¢â‚¬â€œ11 estÃƒÂ¡vel:** item **12** cancelar ERP (Renan decide) Ã‚Â· fase **D** cÃƒÂ³digo Compras fornecedor **continua pausada** atÃƒÂ© fechar CP ou outro chat.

**Novo chat (outro problema):** ler CHECKPOINT + Ã‚Â§4.15 Ã‚Â· **nÃƒÂ£o** misturar com passos 8Ã¢â‚¬â€œ11 salvo Renan pedir.

---

### WIP Ã¢â‚¬â€ desvinculaÃƒÂ§ÃƒÂ£o Mongo Ã‚Â§4.15 (**29/06**)

**Feito recente:** pacotes corte v4.31Ã¢â‚¬â€œv4.49 Ã‚Â· loja **v4.49** Ã‚Â· cadastro aux PG **v4.51 teste** (`17210f7`).

**Trilha (atualizada):**

| Fase | # | O quÃƒÂª | Status |
| ---- | - | ----- | ------ |
| **AmanhÃƒÂ£** | **10Ã¢â€ â€™8Ã¢â€ â€™11** | Backup + congelar + checkpoint CP | **Ã°Å¸â€œâ€¦ Renan 30/06** |
| ~~**C**~~ | cÃƒÂ³digo | Cadastro ERP aux PG | **Ã¢Å“â€¦ v4.51 teste** |
| **D** | cÃƒÂ³digo | Compras relatÃƒÂ³rio fornecedor sem Mongo | Pendente (outro chat) |
| **F** | **9** | Motor busca GM | Por ÃƒÂºltimo |
| **G** | **12** | Cancelar ERP | SÃƒÂ³ apÃƒÂ³s 8Ã¢â‚¬â€œ11 OK |

**Pausado atÃƒÂ© amanhÃƒÂ£:** nada Ã¢â‚¬â€ CP **volta** na agenda. **CÃƒÂ³digo fase D** pode seguir em chat separado se Renan quiser.

**Renan 29/06 Ã¢â‚¬â€ medo CP:** entendido Ã¢â‚¬â€ amanhÃƒÂ£ ÃƒÂ© **operacional fechamento**, nÃƒÂ£o remigrar tÃƒÂ­tulos. CP **jÃƒÂ¡ PG** na loja.

### FECHADO TESTE Ã¢â‚¬â€ cadastro ERP auxiliares Postgres **v4.51** (29/06)

| Rota / fluxo | Antes | Agora (`agro_pg`) |
| ------------ | ----- | ----------------- |
| `api_produtos_cadastro_proximo_cb_loja` | Mongo obrigatÃƒÂ³rio | Seq **230Ã¢â‚¬Â¦** sÃƒÂ³ Postgres + overlays |
| `api_produtos_cadastro_detalhe` (`__novo__`) | CÃƒÂ³digo seq consultava Mongo | Seq sÃƒÂ³ Postgres |
| `try_criar_produto_postgres_somente_agro` | Idem | Idem |
| `api_produtos_somente_agro_excluir` | Mongo obrigatÃƒÂ³rio | Apaga `Produto` PG + limpa overlay |
| Excel import/export prÃƒÂ©via/aplicar/reverter | Estado/import exigia Mongo | CatÃƒÂ¡logo + gravaÃƒÂ§ÃƒÂ£o via `Produto` |
| `api_produtos_cadastro_compras_historico` | 503 sem Mongo | Lista vazia + aviso (histÃƒÂ³rico ERP opcional) |

**Conferir staging:** Ctrl+F5 cadastro Ã‚Â· novo produto (cÃƒÂ³digo auto) Ã‚Â· botÃƒÂ£o **230Ã¢â‚¬Â¦** Ã‚Â· Excel Ã¢â€ â€˜Ã¢â€ â€œ Ã‚Â· `/api/agro/fonte-status/` Ã¢â€ â€™ `cadastro_somente_postgres: true`.

### FECHADO + DEPLOY LOJA Ã¢â‚¬â€ merge max planilha/PDV **v4.49** (29/06)

**Renan:** *Ã‚Â«mandaÃ‚Â»* + senha **99738595** Ã‚Â· cherry-pick **`f0c6f29`** Ã¢â€ â€™ **`98e4685`** Ã‚Â· **push OK**.

**PÃƒÂ³s-deploy loja:** Ctrl+F5 Ã‚Â· **MÃƒÂªs anterior (mai)** Ã‚Â· **21Ã¢â‚¬â€œ22/05** ~R$ 2.451 / R$ 2.870 (nÃƒÂ£o R$ 53 teste).

### FECHADO + DEPLOY LOJA Ã¢â‚¬â€ grÃƒÂ¡fico planilha **v4.47** (29/06)

**Renan:** *Ã‚Â«mandaÃ‚Â»* + senha **99738595** Ã‚Â· cherry-pick **`def40ba`** Ã¢â€ â€™ **`d67a1b3`** Ã‚Â· **push OK**.

**PÃƒÂ³s-deploy loja:** Ctrl+F5 Ã‚Â· **MÃƒÂªs anterior (mai)** Ã¢â€ â€™ barras mÃƒÂªs cheio Ã‚Â· jun+ sÃƒÂ³ PDV.

**Nota Renan 29/06 Ã¢â‚¬â€ mÃƒÂ©dia vs barras 21Ã¢â‚¬â€œ22/mai:** **mÃƒÂ©dia base R$ 2.882** OK Ã‚Â· barras 21Ã¢â‚¬â€œ22 eram R$ 53 (venda teste PDV) Ã¢â‚¬â€ **fix v4.49** merge **max(PDV, planilha)** Ã‚Â· **Ã¢Å“â€¦ loja 29/06**.

### WIP Ã¢â€ â€™ TESTE Ã¢â‚¬â€ meta C planilha + 3 meses (**29/06**)

**Pacote:** migration **`0045`** + **`DashboardVendaDiaHistoricoAgro`** Ã‚Â· import **`docs/dados/vendas_centro_nov2025.xlsx`** (272 dias Ã‚Â· set/25Ã¢â‚¬â€œmai/26 Centro) Ã‚Â· merge meta C **VendaAgro > planilha** Ã‚Â· fÃƒÂ³rmula **M-1+M-2+M-3** Ã‚Â· fix datas Excel (nov/dez 2025 + linha espÃƒÂºria 2027-01-01).

**ValidaÃƒÂ§ÃƒÂ£o Renan staging v4.45 (29/06 ~00:08):** **Ã¢Å“â€¦ parcial** Ã¢â‚¬â€ no **teste** quase nÃƒÂ£o hÃƒÂ¡ vendas PDV (mÃƒÂªs **R$ 84,62**), entÃƒÂ£o grÃƒÂ¡fico/cores do mÃƒÂªs **corrente** nÃƒÂ£o dÃƒÂ¡ para julgar direito. **MÃƒÂ©dia base ~R$ 104 mil** no tooltip **faz sentido** (planilha + 3 meses).

**ProduÃƒÂ§ÃƒÂ£o v4.45 (29/06):** Renan *Ã‚Â«pode enviarÃ‚Â»* + senha **99738595** Ã‚Â· cherry-pick **`d75927d`** Ã¢â€ â€™ **`d078b4e`** Ã‚Â· **push OK**. Conferir BI loja: Ctrl+F5 Ã‚Â· tooltip Ã‚Â«3 mesesÃ‚Â» Ã‚Â· jun/2026 sem +1394%.

**Ajuste grÃƒÂ¡fico (v4.47):** planilha preenche **barras** set/25Ã¢â‚¬â€œmai/26 Ã‚Â· **Ã¢Å“â€¦ loja 29/06**.

### FECHADO + DEPLOY LOJA Ã¢â‚¬â€ pacote corte Mongo **v4.31Ã¢â‚¬â€œv4.36** (28/06)

**Renan:** *Ã‚Â«pode subirÃ‚Â»* + senha **99738595** Ã‚Â· prints baseline produÃƒÂ§ÃƒÂ£o **antes** do deploy (comparar se precisar).

**Loja (`producao`):** cherry-pick **`bc805e7`** Ã‚Â· **`d21a718`** Ã‚Â· **`a516a64`** Ã¢â€ â€™ **`77f1254`** Ã‚Â· **push OK** Ã‚Â· **nÃƒÂ£o** merge inteiro `teste` Ã‚Â· banana loja **intacto** (sÃƒÂ³ cÃƒÂ³digo).

**PÃƒÂ³s-deploy loja:** Ctrl+F5 Ã‚Â· badge BI **v4.36+** Ã‚Â· `/api/agro/fonte-status/` Ã‚Â· smoke: vender 1 un. Ã¢â€ â€™ GestÃƒÂ£o saldo Ã‚Â· BI Fonte PDV.

**Smoke produÃƒÂ§ÃƒÂ£o pÃƒÂ³s v4.36 (28/06 ~22:27 Ã¢â‚¬â€ Renan):** **Ã¢Å“â€¦ PDV Ã¢â€ â€™ estoque desce**

| Item | Detalhe |
| ---- | ------- |
| **Produto** | GM9503 **teste** |
| **Venda** | **#2273** Ã‚Â· R$ 1,00 Ã‚Â· **BAIXA REGISTRADA** Ã‚Â· ERP ACEITO |
| **Cadastro ERP** | estoque **-5 Ã¢â€ â€™ -6** (C) |
| **GestÃƒÂ£o** | saldo acompanhou (print) |

**Incidente pÃƒÂ³s v4.36 Ã¢â‚¬â€ meta C / comparaÃƒÂ§ÃƒÂ£o BI (28/06 noite):**

| Sintoma | Causa |
| ------- | ----- |
| **MÃƒÂªs ant.** grÃƒÂ¡fico ~**R$ 18k** (sÃƒÂ³ fim de mai) Ã‚Â· tooltip meta Ã‚Â«**+1394%**Ã‚Â» Ã‚Â· cores estranhas | Pacote corte: **histÃƒÂ³rico da meta C** passou a usar **sÃƒÂ³ VendaAgro** (`agro_pg`). Loja tem PDV SisVale **sÃƒÂ³ desde ~fim/mai** Ã¢â‚¬â€ abr/mai quase vazios no PG |
| Card **-75,3%** hoje | **Normal** Ã¢â‚¬â€ ÃƒÂ© **hoje vs ontem**, nÃƒÂ£o meta C |
| GrÃƒÂ¡fico mÃƒÂªs **R$ 95.988** (Fonte PDV) | **OK** Ã¢â‚¬â€ vendas do mÃƒÂªs corrente no PG |

**Regra meta C (nÃƒÂ£o ÃƒÂ© 60 dias):** para cada dia, mÃƒÂ©dia de **M-1 + M-2 + M-3** (3 meses civis anteriores) Ã¢â‚¬â€ **Renan 29/06** (antes eram 2). Weekday + ocorrÃƒÂªncia no mÃƒÂªs. HistÃƒÂ³rico: planilha set/25Ã¢â‚¬â€œmai/26 + jun+ PG.

**CorreÃƒÂ§ÃƒÂ£o provisÃƒÂ³ria meta C Ã¢â‚¬â€ decisÃƒÂ£o Renan (28/06):** **opÃƒÂ§ÃƒÂ£o B Ã¢â‚¬â€ Excel manual** (nÃƒÂ£o hÃƒÂ­brido Mongo).

| # | Caminho | Status |
| - | ------- | ------ |
| ~~A~~ | HÃƒÂ­brido Mongo sÃƒÂ³ meta histÃƒÂ³rico | **Descartado** (Renan prefere planilha) |
| **B** | Tabela PG **`DashboardVendaDiaHistoricoAgro`** (data + total) Ã‚Â· Renan cola/importa Excel Ã‚Â· merge na meta C: **VendaAgro** > planilha > 0 | **Ã¢Å“â€¦ implementado teste v4.45** |
| C | Backfill Mongo Ã¢â€ â€™ PG | NÃƒÂ£o |

**Formato Excel (Renan enviar):**

| Campo | Exemplo | Nota |
| ----- | ------- | ---- |
| **Data** | `01/set` Ã‚Â· `02/set` Ã‚Â· Ã¢â‚¬Â¦ | **DD/mmm** (abr, mai, jun, jul, ago, **set**, out, nov, dez, jan, fev, mar) Ã¢â‚¬â€ **como no print** Ã¢Å“â€¦ |
| **Total** | `R$ 5.090,37` | Faturamento **do dia** (loja toda) Ã‚Â· ponto milhar + vÃƒÂ­rgula decimal OK |
| **PerÃƒÂ­odo planilha** | **set/2025 Ã¢â‚¬â€œ mai/2026** | Jun/2026+ **nÃƒÂ£o** entra no Excel Ã¢â‚¬â€ vendas **jÃƒÂ¡ no SisVale** (PDV) |
| **Arquivo Renan** | `vendas.cvs.xlsx` Ã¢â€ â€™ **`docs/dados/vendas_centro_nov2025.xlsx`** | **Ã¢Å“â€¦ 29/06** Ã‚Â· **272 linhas** (1 espÃƒÂºria omitida) Ã‚Â· **01/09/2025 Ã¢â‚¬â€œ 31/05/2026** Ã‚Â· **sÃƒÂ³ Centro** |
| **Merge meta C** | **max(PDV, planilha)** por dia | Venda teste PDV nÃƒÂ£o apaga planilha; jun+ sÃƒÂ³ PDV |
| **Ano** | Se a planilha **nÃƒÂ£o** tiver coluna ano | Assumimos **2025** para novÃ¢â‚¬â€œdez e **2026** para jan em diante (Renan corrige se errar) |
| **Dia sem venda** | `0` ou linha omitida | Omitir = sem venda naquele dia |

**Importante:** fase **1** = Excel + **meta C** (3 meses + merge PG/planilha). SES/clima **nÃƒÂ£o** entra neste pacote.

**PrÃƒÂ³ximo passo:** se Renan quiser **loja** Ã¢â€ â€™ cherry-pick **`d75927d`** (v4.45) com frase + senha Ã‚Â· na loja conferir jun/2026 (tooltip + % meta, sem +1394%).

**WIP Ã¢â‚¬â€ Renan quer revisar fÃƒÂ³rmula da meta (28/06 noite):**

| Tema | DecisÃƒÂ£o / nota |
| ---- | -------------- |
| **Dados** | Planilha **set/2025Ã¢â‚¬â€œmai/2026** + **jun+ PG** (merge) Ã¢â‚¬â€ meta C atual |
| **FÃƒÂ³rmula** | **Meta C = mÃƒÂ©dia M-1 + M-2 + M-3** (29/06 Renan; antes 2 meses) Ã‚Â· SES/clima **nÃƒÂ£o** agora |
| **Clima/chuva** | CÃƒÂ³digo Gemini (`previsao_mensal.py` + OpenWeather) Ã¢â‚¬â€ **fase separada**; nÃƒÂ£o substitui meta C atual |
| **Parecer assistente** | SES **Ã¢â€°Â ** meta C de hoje (weekday + ocorrÃƒÂªncia no mÃƒÂªs); ver CHECKPOINT ou chat 28/06 |

**Baseline produÃƒÂ§ÃƒÂ£o ANTES deploy (28/06 ~22:13 Ã¢â‚¬â€ prints Renan):**

| Tela | Valor / nota |
| ---- | ------------ |
| **BI `/`** v4.21.1 | Vendas hoje **R$ 1.310,04** Ã‚Â· **34** vendas Ã‚Â· grÃƒÂ¡fico mÃƒÂªs **R$ 95.988,00** Ã‚Â· a pagar hoje **R$ 2.911,24** Ã‚Â· atraso CP **R$ 63.754,44** |
| **CP** | **746** tÃƒÂ­t. abertos Ã‚Â· bruto **R$ 407.700,86** Ã‚Â· a pagar **R$ 396.515,99** |
| **`/vendas/` 30d** | **1920** vendas Ã‚Â· **R$ 104.990,11** |
| **`/vendas/` fiado pend. ERP** | **2122** Ã‚Â· **R$ 116.137,93** (filtro ativo no print) |

**Assistente:** checklist staging **6/6 + DRE Ã¢ÂÂ­** Ã¢Å“â€¦ Ã‚Â· deploy loja **feito**.

**Antes de comeÃƒÂ§ar:** Ctrl+F5 Ã‚Â· conferir `/api/agro/fonte-status/` Ã¢â€ â€™ `catalogo_postgres: true` Ã‚Â· `pdv_catalogo_somente_postgres: true` Ã‚Â· `gestao_somente_postgres: true`

| # | Tela | Passos | OK? | Nota |
| - | ---- | ------ | --- | ---- |
| **1** | **Entrada NF** | Busca **NF** (ex. `112`) Ã¢â€ â€™ **Auditar financeiro** | **Ã¢Å“â€¦ 28/06** | OK **1** Ã‚Â· alertas **0** Ã‚Â· tÃƒÂ­tulo CP PG conferido na NF 112 |
| **2** | **PDV Ã¢â€ â€™ GestÃƒÂ£o** | Vender **1 un.** Ã‚Â· GestÃƒÂ£o aberta Ã¢â€ â€™ saldo desce | **Ã¢Å“â€¦ 28/06** | GM9503 **teste** Ã‚Â· **47 Ã¢â€ â€™ 46** (C) |
| **3** | **BI `/`** | GrÃƒÂ¡fico vendas carrega Ã‚Â· meta C / ticket coerentes (sem erro Mongo) | **Ã¢Å“â€¦ 28/06** | Ctrl+F5 apÃƒÂ³s venda Ã‚Â· card **R$ 1,50 / 1 venda** Ã‚Â· `/vendas/` **#37** 21:03 Ã‚Â· grÃƒÂ¡fico **Fonte PDV** Ã‚Â· barra **28/06** Ã‚Â· tooltip meta C Ã‚Â«sem base 2 mesesÃ‚Â» = **staging sem histÃƒÂ³rico M-1/M-2** (esperado) Ã‚Â· ticket hoje **1,50** |
| **4** | **GrÃƒÂ¡fico gastos** | Abre Ã‚Â· totais **Ã¢â€°Ë† CP** mesmo perÃƒÂ­odo (competÃƒÂªncia ou vencimento) | **Ã¢Å“â€¦ 28/06** | Modo **Comparar** Ã‚Â· **Jul/2026** bolinha Ã¢â€ â€™ popup CP **119 tÃƒÂ­t.** Ã‚Â· bruto/a pagar **R$ 68.235,20** Ã¢â€°Ë† ponto grÃƒÂ¡fico **68.235** Ã‚Â· PG OK Ã‚Â· Ã‚Â±0 tempo real vs 26/06 = staging estÃƒÂ¡vel |
| **5** | **LanÃƒÂ§amentos DRE** | Abre perÃƒÂ­odo Ã‚Â· totais batem (sem tela vazia / 503) | **Ã¢ÂÂ­ 28/06** | **`LANCAMENTOS_DRE_ATIVO=false`** de propÃƒÂ³sito Ã¢â‚¬â€ Renan: incoerente no **Mongo** Ã‚Â· **fora** do pacote corte v4.31Ã¢â‚¬â€œv4.43 Ã‚Â· nÃƒÂ£o bloqueia cherry-pick |
| **6** | **GestÃƒÂ£o** | Lista abre rÃƒÂ¡pido Ã‚Â· filtros **marca / categoria** Ã‚Â· apÃƒÂ³s Entrada NF **nÃƒÂ£o** trava minutos | **Ã¢Å“â€¦ 28/06** | Renan: **tudo certo** Ã¢â‚¬â€ lista rÃƒÂ¡pida Ã‚Â· filtros OK |
| **7** | **Compras** | Card Ã‚Â«**ÃƒÂºltimas compras**Ã‚Â» Ã‚Â· **Folha Compras** Ã¢â€ â€™ categoria (planilha A4) | **Ã¢Å“â€¦ 28/06** | GM9503 Ã‚Â· **Teste R$ 1,00** Ã‚Â· folha **RaÃƒÂ§ÃƒÂµes** **232 prod.** A4 **27 pÃƒÂ¡g.** Ã‚Â· obs. micro recarga (nÃƒÂ£o bloqueia) |

**Incidente 28/06:** 1Ã‚Âª abertura staging = tela cinza (cold start Render) Ã‚Â· Ctrl+Shift+R Ã¢â€ â€™ BI OK. **BI passo 3:** card vendas **nÃƒÂ£o atualiza sozinho** Ã¢â‚¬â€ precisa **Ctrl+F5** apÃƒÂ³s venda (KPI hoje ÃƒÂ© live no servidor, mas a **pÃƒÂ¡gina** jÃƒÂ¡ estava aberta).

**Meta C no staging:** poucas vendas PDV Ã¢â€ â€™ mÃƒÂªs corrente **R$ 0Ã¢â‚¬â€œ84** e **-99% vs base** = **esperado**. A **mÃƒÂ©dia base ~R$ 104 mil** (planilha 3 meses) ÃƒÂ© o sinal de que import/merge **OK**. Cores e Ã‚Â«falta para bater meta %Ã‚Â» no mÃƒÂªs cheio = validar na **loja**.

**Passo 4 Ã¢â‚¬â€ GrÃƒÂ¡fico gastos (Renan agora):**

1. BI `/` Ã¢â€ â€™ card **Contas a Pagar** Ã¢â€ â€™ botÃƒÂ£o laranja **GrÃƒÂ¡fico gastos** (ou `/financeiro/grafico-gastos/`).
2. PerÃƒÂ­odo: **01/06/2026 Ã¢â‚¬â€œ 28/06/2026** (ou **MÃƒÂªs atÃƒÂ© hoje** na toolbar).
3. Filtros: **ReferÃƒÂªncia = Vencimento** Ã‚Â· **Valor = Bruto (tÃƒÂ­tulo)** Ã‚Â· **Tempo real** Ã‚Â· planos **Todos** marcados.
4. Anotar **total** do grÃƒÂ¡fico (faixa de totais / soma visÃƒÂ­vel).
5. Abrir **Contas a pagar** `/lancamentos/contas-pagar/` Ã¢â‚¬â€ **mesmo perÃƒÂ­odo** + filtro **Vencimento** + situaÃƒÂ§ÃƒÂ£o que bater com **bruto** (ex. **Todos** ou **Em aberto** Ã¢â‚¬â€ usar o par que o Ã‚Â«?Ã‚Â» do grÃƒÂ¡fico descreve).
6. **OK** se totais **Ã¢â€°Ë† iguais** (centavos). **DiferenÃƒÂ§a grande** Ã¢â€ â€™ anotar valor grÃƒÂ¡fico vs CP + print.

*GrÃƒÂ¡fico da **home BI** (card gastos-plano, se ligado) **Ã¢â€°Â ** esta tela Ã¢â‚¬â€ validar sÃƒÂ³ `/financeiro/grafico-gastos/`.*

**DRE (passo 5):** tela Ã‚Â«FunÃƒÂ§ÃƒÂ£o desativadaÃ‚Â» = **esperado**. Flag **`LANCAMENTOS_DRE_ATIVO`** off no Render (teste e loja). Motivo Renan: totais **incoerentes** quando lia Mongo Ã¢â‚¬â€ pausa atÃƒÂ© reprogramar PG. **NÃƒÂ£o entra** na validaÃƒÂ§ÃƒÂ£o do pacote corte; contagem **7 OK** = itens **1Ã¢â‚¬â€œ4 + 6Ã¢â‚¬â€œ7** (DRE Ã¢ÂÂ­).

**Passo 7 Ã¢â‚¬â€ Compras (ÃƒÂºltimo antes da loja):**

1. **`/compras/`** Ã¢â‚¬â€ card produto Ã¢â€ â€™ **Ã‚Â«ÃƒÅ¡ltimas compras (ERP)Ã‚Â»** Ã¢Å“â€¦ **GM9503** Ã‚Â· **Teste R$ 1,00** (entrada NF teste).
2. Menu **Folha Compras** Ã¢â€ â€™ **planilha por categoria** Ã¢â€ â€™ **RaÃƒÂ§ÃƒÂµes** + **A4** Ã¢Å“â€¦ **232 prod.** Ã‚Â· 27 pÃƒÂ¡gs. (28/06 22:12).

**Obs. Compras:** card detalhe pode **micro recarregar** / sensaÃƒÂ§ÃƒÂ£o de lentidÃƒÂ£o Ã¢â‚¬â€ **nÃƒÂ£o bloqueia** pacote corte; anotado para otimizar depois se incomodar.

**Auditoria NF Ã¢â‚¬â€ dois modos (Renan perguntou brecha):**

| Modo | Quando | O quÃƒÂª audita |
| ---- | ------ | ------------ |
| **Suspeitas** | Filtro ConcluÃƒÂ­da **sem busca** | SÃƒÂ³ notas concluÃƒÂ­das **sem** flag Ã‚Â«financeiro gravadoÃ‚Â» Ã¢â‚¬â€ caÃƒÂ§a esquecimento de Ã‚Â«Salvar + a pagarÃ‚Â» |
| **Ampliada** | **Com busca** (NF/fornecedor) ou filtro Financeiro | ConferÃƒÂªncia **nota a nota**: tÃƒÂ­tulo existe no CP Postgres + fornecedor bate |

**Brecha conhecida:** nota **ConcluÃƒÂ­da + financeiro gravado** nÃƒÂ£o reentra no modo suspeitas. Se a flag estiver errada (marcada sem tÃƒÂ­tulo real), passa batido **atÃƒÂ©** auditar com busca ou amostragem. **Teste staging:** buscar NF conhecida (112) Ã¢â€ â€™ OK:1. **OperaÃƒÂ§ÃƒÂ£o:** amostrar 2Ã¢â‚¬â€œ3 NFs/mÃƒÂªs com busca; comando `auditar_corte_mongo_pg` no CP (read-only).

**Se falhar:** anotar **# da linha** + mensagem na tela ou URL vermelha no DevTools (Rede).

**Depois dos 7 OK:** Renan manda *Ã‚Â«pode subir produÃƒÂ§ÃƒÂ£oÃ‚Â»* + senha **99738595** Ã¢â€ â€™ assistente cherry-pick **pacote corte** (nÃƒÂ£o merge inteiro `teste`). **DRE Ã¢ÂÂ­ nÃƒÂ£o conta como falha.**

**Commits pacote corte:** `bc805e7` v4.31 Ã‚Â· `d21a718` v4.33 Ã‚Â· `a516a64` v4.36 Ã‚Â· banana `57885fa` v4.38

### banana Ã¢â‚¬â€ alinhamento checklist Ã‚Â§4.15 (03/06 Ã‚Â· assistente)

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Tabelas duplicadas Ã‚Â§4.15 sincronizadas; status teste vs loja; rodapÃƒÂ© roadmap |
| **Deploy teste** | **`3fec909`** Ã‚Â· **v4.37** |

### Corte Mongo Ã¢â‚¬â€ pacote 3 parciais fechados (03/06 Ã‚Â· assistente)

| Item | Detalhe |
| ---- | ------- |
| **BI vendas (#3)** | Modo PDV forÃƒÂ§ado com `agro_pg`; meta C sem fallback DtoVenda; vÃƒÂ­nculos sem coleÃƒÂ§ÃƒÂ£o Mongo `vendas_agro` |
| **GestÃƒÂ£o (#4)** | Com catÃƒÂ¡logo PG: **sem fallback Mongo** em lista/facetas; saldo jÃƒÂ¡ ledger (pacote 2) |
| **Compras (#5)** | `api_buscar` ÃƒÂºltimas compras **sem exigir Mongo**; Entrada NF etapa 6 lÃƒÂª produto via Postgres |
| **Deploy teste** | **`a516a64`** (cÃƒÂ³digo) Ã‚Â· **`f986160`** (banana) Ã‚Â· **v4.36** |

**Renan Ã¢â‚¬â€ retestar no staging (Ctrl+F5):** BI `/` Ã‚Â· GestÃƒÂ£o lista/filtros Ã‚Â· Compras card Ã‚Â«ÃƒÂºltimas comprasÃ‚Â» Ã‚Â· folha categoria.

### Corte Mongo Ã¢â‚¬â€ pacote 2 (03/06 Ã‚Â· assistente)

| Item | Detalhe |
| ---- | ------- |
| **GestÃƒÂ£o saldo** | Lista PG nÃƒÂ£o lÃƒÂª estoque Mongo quando ledger operacional (`agro_estoque_operacional_sem_mongo_erp`) |
| **Compras ÃƒÂºltima compra** | Com `agro_pg`: sÃƒÂ³ **Entrada NF Agro** + vendas PG Ã¢â‚¬â€ sem scan DtoCompra* ERP |
| **Compras planilha** | MÃƒÂ©tricas/vendas pÃƒÂ³s-compra 100 % Postgres quando catÃƒÂ¡logo PG |
| **BI ticket** | Ticket mÃƒÂ©dio usa **VendaAgro** no modo PDV |
| **GrÃƒÂ¡fico gastos** | Mensagem erro amigÃƒÂ¡vel se PG ativo e Mongo cair |
| **Deploy teste** | **`d21a718`** Ã‚Â· **v4.33** |

**Renan Ã¢â‚¬â€ testar 1 a 1 no staging (Ctrl+F5):**

| # | Tela | O que conferir |
| - | ---- | -------------- |
| 1 | Entrada NF | Auditar financeiro (concluÃƒÂ­das) |
| 2 | PDV Ã¢â€ â€™ GestÃƒÂ£o | Vender 1 un. Ã‚Â· saldo desce na gestÃƒÂ£o aberta |
| 3 | BI `/` | GrÃƒÂ¡fico vendas + meta |
| 4 | GrÃƒÂ¡fico gastos | Abre Ã‚Â· totais vs CP mesmo perÃƒÂ­odo |
| 5 | LanÃƒÂ§amentos DRE | **Ã¢ÂÂ­** desligado (`LANCAMENTOS_DRE_ATIVO`) Ã¢â‚¬â€ fora do pacote |
| 6 | GestÃƒÂ£o | **Ã¢Å“â€¦** lista + filtros marca/categoria |
| 7 | Compras | **Ã¢Å“â€¦** ÃƒÂºltimas compras + folha **RaÃƒÂ§ÃƒÂµes** A4 |

### Corte Mongo Ã¢â‚¬â€ pacote 1 seguranÃƒÂ§a (03/06 Ã‚Â· assistente)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Renan: avanÃƒÂ§ar desvinculaÃƒÂ§ÃƒÂ£o com paridade segura; acionar sÃƒÂ³ se necessÃƒÂ¡rio |
| **NF auditoria** | TÃƒÂ­tulos CP via **Postgres** quando financeiro PG (`_titulos_pg_por_*`) |
| **BI meta C** | HistÃƒÂ³rico M-1/M-2 prefere **VendaAgro**; ERP sÃƒÂ³ se perÃƒÂ­odo vazio |
| **GestÃƒÂ£o saldo** | Ouve fila `agro_pdv_catalog_patch_queue_v1` Ã¢â‚¬â€ saldo atualiza apÃƒÂ³s venda PDV |
| **Comando** | `python manage.py auditar_corte_mongo_pg` Ã¢â‚¬â€ CP PG vs Mongo read-only |
| **Deploy teste** | **`bc805e7`** Ã‚Â· **v4.31** |
| **Renan testar** | (1) Entrada NF Ã¢â€ â€™ Auditar financeiro Ã‚Â· (2) Vender 1 un. Ã¢â€ â€™ GestÃƒÂ£o saldo desce Ã‚Â· (3) BI meta |

### CP Ã¢â‚¬â€ filtro por competÃƒÂªncia e pagamento (03/06 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Lista nova CP: alÃƒÂ©m de vencimento, filtrar por **competÃƒÂªncia** e **pagamento** (vencimento = padrÃƒÂ£o) |
| **UI** | Select **Filtrar por** no painel Filtros; labels De/AtÃƒÂ© dinÃƒÂ¢micos; aviso pagamento + em aberto |
| **API** | Reutiliza `ref` + `comp_de/ate` Ã‚Â· `pag_de/ate` Ã‚Â· `venc_de/ate` (jÃƒÂ¡ existentes) |
| **Atalhos Hoje/SÃƒÂ¡bÃ¢â‚¬Â¦** | Continuam no eixo **vencimento** |
| **GrÃƒÂ¡fico gastos** | Drill-down `embed=grafico` inalterado |
| **Arquivo** | `lancamentos_contas_pagar_teste.html` |
| **Deploy teste** | **`fc1abe8`** Ã‚Â· **v4.26** (hook pÃƒÂ³s-checkpoint) |
| **Deploy produÃƒÂ§ÃƒÂ£o** | **`69e5eb8`** Ã‚Â· **v4.21** Ã‚Â· Renan **99738595** Ã‚Â· **28/06** |

### Entrada NF Ã¢â‚¬â€ rascunho Postgres (24/06 Ã‚Â· assistente)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Rascunho assistente (etapas 1Ã¢â‚¬â€œ6) sair do Mongo `AgroEntradaNotaRascunho` |
| **Modelo** | `EntradaNotaRascunhoAgro` Ã‚Â· migration `0043` + `0044` |
| **Flag** | `AGRO_ENTRADA_NF_RASCUNHO_PG` Ã¢â‚¬â€ default **ligado** quando financeiro PG ativo |
| **Import legado** | `python manage.py importar_rascunhos_entrada_nota_mongo_pg` Ã‚Â· **auto** na listagem + boot (`maybe_bootstrap_rascunhos_entrada_nota_pg`) |
| **Incidente 28/06** | Loja lista vazia Ã¢â‚¬â€ PG ligado, dados ainda no Mongo; **fix v4.20** import auto no boot + listagem + **fallback Mongo** atÃƒÂ© PG popular |
| **Hotfix loja** | ~~Shell/env~~ Ã¢â€ â€™ **v4.20 no ar** Ã‚Â· 1Ã‚Âª abertura importa MongoÃ¢â€ â€™PG Ã‚Â· fallback se PG vazio |
| **Audit v4.17** | Fix `_object_id_rascunho` Ã‚Â· reabrir etapas Ã‚Â· msgs etapa 8 |
| **Deploy teste** | v4.09Ã¢â‚¬â€œv4.22 Ã‚Â· NF 112 GM9503 **47** Ã¢Å“â€¦ |
| **Deploy produÃƒÂ§ÃƒÂ£o v4.20** | **28/06** Ã‚Â· **`7834e66`** Ã‚Â· Renan autorizou (senha) Ã¢Å“â€¦ Ã‚Â· **Renan OK loja** Ã¢â‚¬â€ lista NF voltou (rascunhos + concluÃƒÂ­das) |
| **Deploy produÃƒÂ§ÃƒÂ£o v4.17** | **28/06** Ã‚Â· **`cdc198f`** Ã‚Â· Renan autorizou (senha) Ã¢Å“â€¦ |

### Caixa Ã¢â‚¬â€ histÃƒÂ³rico retiradas + feedback saÃƒÂ­da (24/06 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Ã‚Â«Retirada / saÃƒÂ­daÃ‚Â» no painel Ã¢â€ â€™ tela de **histÃƒÂ³rico** (nÃƒÂ£o form direto) |
| **Rota** | `/caixa/retiradas/` Ã‚Â· filtros: data (calendÃƒÂ¡rio Agro), plano, quem levou Ã‚Â· padrÃƒÂ£o **hoje** |
| **Nova saÃƒÂ­da** | BotÃƒÂ£o laranja grande Ã¢â€ â€™ `?painel=retirada` (form existente) |
| **Feedback saÃƒÂ­da** | ApÃƒÂ³s registrar: banner verde Ã‚Â«Retirada concluÃƒÂ­daÃ‚Â» + limpa todos os campos |
| **Deploy teste** | **`619fbba`** Ã‚Â· **v4.07** Ã‚Â· Renan OK |
| **Fix FAB (27/06)** | PDV + **Aa** reposicionam para canto livre (caixa prioriza inferior direito) |
| **Deploy produÃƒÂ§ÃƒÂ£o** | **27/06** Ã‚Â· merge `teste`Ã¢â€ â€™`producao` **`5568302`** Ã‚Â· **v4.07** Ã‚Â· Renan autorizou |

### PRODUTO Ã¢â‚¬â€ FOOD delivery em branco (27/06 Ã‚Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Nomes** | **SISTVALE** = produto Ã‚Â· **BANANA** = GM Agro Ã‚Â· **FOOD** = delivery em branco |
| **Repo FOOD** | GitHub **`hinnen/food`** (privado, vazio Ã¢â€ â€™ espelho cÃƒÂ³digo) |
| **Docs** | **`FOOD.md`** Ã‚Â· **`SISTVALE.md`** (mapa) Ã‚Â· `docs/FOOD-INSTANCIA-BRANCA.md` Ã‚Â· `.env.food.example` Ã‚Â· `render-food.yaml` Ã‚Â· `scripts/espelhar_repo_food.ps1` |
| **Chat** | Loja GM Ã¢â€ â€™ `@banana` Ã‚Â· FOOD Ã¢â€ â€™ `@FOOD` |
| **PrÃƒÂ³ximo Renan** | Clone FOOD local Ã¢Å“â€¦ Ã‚Â· Render **pausado** |
| **GM Agro** | Sem mudanÃƒÂ§a de deploy/dados |

### INCIDENTE Ã¢â‚¬â€ loja lenta geral (27/06) Ã‚Â· diagnÃƒÂ³stico fechado pelos grÃƒÂ¡ficos

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | SisVale demora abrir (tela cinza); balcÃƒÂ£o lento; vÃƒÂ­deo WhatsApp ~11:54 |
| **Gargalo** | **`agro-db`** (Postgres 256 MB) Ã¢â‚¬â€ **nÃƒÂ£o** o servidor web |
| **Web `Sistvale - ProduÃƒÂ§ÃƒÂ£o`** | 512 MB Ã‚Â· memÃƒÂ³ria **~40Ã¢â‚¬â€œ50%** Ã‚Â· CPU picos **~30Ã¢â‚¬â€œ40%** Ã‚Â· **1 worker** Ã¢â‚¬â€ saudÃƒÂ¡vel, **espera o banco** |
| **MemÃƒÂ³ria DB Ã¢â‚¬â€ 7 dias** | Working set **80Ã¢â‚¬â€œ95% a semana inteira** Ã¢â‚¬â€ pressÃƒÂ£o **crÃƒÂ´nica**, nÃƒÂ£o sÃƒÂ³ 27/06 |
| **Disco DB Ã¢â‚¬â€ 7 dias** | ~100 MB atÃƒÂ© **25/06** Ã¢â€ â€™ **~150Ã¢â‚¬â€œ180 MB a partir 26/06** (import CP/CR ~18k tÃƒÂ­tulos) |
| **CPU DB** | ~10Ã¢â‚¬â€œ12% Ã¢â‚¬â€ **nÃƒÂ£o** ÃƒÂ© gargalo |
| **Escrita DB** | **40Ã¢â‚¬â€œ60 KB/s estÃƒÂ¡vel a semana** + ops 1kÃ¢â‚¬â€œ3k/intervalo Ã¢â‚¬â€ loja + ledger + financeiro PG |
| **ConexÃƒÂµes DB** | Picos **70Ã¢â‚¬â€œ80 / limite 100** Ã¢â‚¬â€ fila quando memÃƒÂ³ria alta |
| **TransaÃƒÂ§ÃƒÂµes** | Pico **~6000** **22/06 ~17h** Ã¢â‚¬â€ provÃƒÂ¡vel import/bootstrap financeiro |
| **IndisponÃƒÂ­vel** | **24/06 19h10** e **26/06 16h51** Ã¢â‚¬â€ restart Render (coincide CP PG) |
| **Top query agora** | `TituloFinanceiroAgro` Ã¢â‚¬â€ **142k linhas** lidas Ã‚Â· ~115 ms/chamada (CP/BI pesado) |
| **Deploys 27/06 manhÃƒÂ£** | 3 seguidos (8h51Ã¢â‚¬â€œ10h) Ã¢â‚¬â€ piora momentÃƒÂ¢nea, **nÃƒÂ£o causa raiz** |
| **Por que Ã‚Â«atÃƒÂ© ontemÃ‚Â»** | **26/06** dados financeiros no Postgres + disco subiu; **27/06** loja aberta + deploys + Mongo ERP fora |
| **AÃƒÂ§ÃƒÂ£o** | **`agro-db` Ã¢â€ â€™ Basic-1gb** Ã¢Å“â€¦ **Renan 27/06** Ã¢â‚¬â€ pÃƒÂ³s-upgrade working set **&lt;20%** (limite **1 GB**; antes ~90% em 256 MB) |
| **Renan agora** | **Ctrl+F5** loja Ã‚Â· testar abrir SisVale + PDV + 1 venda Ã‚Â· gestÃƒÂ£o/DRE ainda podem ser Mongo |
| **Depois do upgrade** | Conferir working set **~30Ã¢â‚¬â€œ50%**; otimizar queries financeiro se CP/BI ainda pesado |

### PACOTE Ã¢â‚¬â€ corte ERP sem Mongo (26/06) Ã‚Â· **teste v3.77**

| Item | Detalhe |
| ---- | ------- |
| **Contexto** | ERP caiu (loja nÃƒÂ£o pagou); Renan pediu **fechar pendÃƒÂªncias do corte** no **teste** Ã¢â‚¬â€ validar um a um antes de produÃƒÂ§ÃƒÂ£o |
| **Commit** | **`7992b0a`** (cÃƒÂ³digo) Ã‚Â· push **`94616aa`** Ã¢â‚¬â€ analytics PG + Compras + gestÃƒÂ£o |
| **Migrate** | **Nenhuma** |
| **Flags Render teste** | `AGRO_FONTE_CATALOGO=agro_pg` Ã‚Â· `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` Ã‚Â· financeiro PG auto se tÃƒÂ­tulos existirem Ã‚Â· ledger estoque se jÃƒÂ¡ ligado |

**O que entrou neste pacote:**

| MÃƒÂ³dulo | MudanÃƒÂ§a |
| ------ | ------- |
| **DRE** | `/api/lancamentos/dre/resumo/` Ã¢â€ â€™ Postgres quando financeiro PG |
| **Fluxo calendÃƒÂ¡rio** | projeÃƒÂ§ÃƒÂ£o diÃƒÂ¡ria via `TituloFinanceiroAgro` + mÃƒÂ©dia `VendaAgro` |
| **BI card gastos-plano** | totais por plano no Postgres |
| **GrÃƒÂ¡fico gastos (sÃƒÂ©rie)** | API `/api/financeiro/grafico-gastos/dados/` + planos iniciais PG |
| **GestÃƒÂ£o produtos** | saldos via ledger/Agro quando Mongo off |
| **Compras planilha** | dim categoria/unidade + relatÃƒÂ³rios categoria/unidade **100% catÃƒÂ¡logo Postgres**; mÃƒÂ©tricas vendas via `VendaAgro` |

**PrÃƒÂ³ximas fases (fora deste pacote):**

- RelatÃƒÂ³rio Compras **por fornecedor** ainda exige Mongo catÃƒÂ¡logo
- Motor busca GM unificado (Compras/NF)
- Resumo gerencial financeiro completo PG

**Checklist Renan Ã¢â‚¬â€ testar no Render teste (Ctrl+F5):**

| # | Tela | Renan 27/06 | Nota |
| - | ---- | ----------- | ---- |
| 1 | PDV busca | **Ã¢Å“â€¦** teste + **Ã¢Å“â€¦** produÃƒÂ§ÃƒÂ£o | Ã¢â‚¬â€ |
| 2 | CP/CR | **Ã¢Å“â€¦** | Ã¢â‚¬â€ |
| 3 | DRE | **Ã¢ÂÂ­** desativado opcional | Ignorar no pacote |
| 4 | CalendÃƒÂ¡rio fluxo | **Ã¢Å¡Â Ã¯Â¸Â** vendas incluem vendas **do teste** | **Esperado no staging** Ã¢â‚¬â€ Postgres prÃƒÂ³prio Ã‚Â· na **loja** sÃƒÂ³ vendas loja |
| 5 | GrÃƒÂ¡fico gastos | **Ã¢Å“â€¦** teste + **Ã¢Å“â€¦ prod** (v3.55 grÃƒÂ¡fico Ã‚Â· v3.58 sync CP) | Renan conferir tela **82.643** |
| 6 | BI `/` card gastos-plano | **Ã¢ÂÂ­** Renan nÃƒÂ£o achou | Card **sÃƒÂ³** se `AGRO_DASHBOARD_GASTOS_PLANO=true` Ã¢â‚¬â€ **opcional** Ã‚Â· pode pular |
| 7 | GestÃƒÂ£o saldo pÃƒÂ³s-venda | **Ã¢ÂÅ’Ã¢â€ â€™fix v3.82** vendeu 7 Ã‚Â· ficou -3 | v3.54 nÃƒÂ£o baixava estoque sem Mongo Ã‚Â· **retestar venda** |
| 8 | Compras planilha | **Ã¢ÂÂ­** tela recarrega sozinha | Pouco usada Ã‚Â· dados PG depois Ã‚Â· **ignorar por ora** (Renan) |
| 9 | Cadastro ERP | **Ã¢Å“â€¦** lista OK Ã‚Â· um pouco mais lenta que loja | AceitÃƒÂ¡vel |

**ProduÃƒÂ§ÃƒÂ£o:** sÃƒÂ³ quando Renan pedir com frase + senha **99738595** Ã¢â‚¬â€ pacote ÃƒÂºnico (grÃƒÂ¡fico + baixa estoque + desvinculo); **sem** cherry-pick avulso.

### ANÃƒÂLISE item 5 Ã¢â‚¬â€ grÃƒÂ¡fico vs CP vs backup (Jul/2026 Ã‚Â· Renan 27/06)

| Fonte | Ambiente | Jul/2026 |
| ----- | -------- | -------- |
| GrÃƒÂ¡fico | ProduÃƒÂ§ÃƒÂ£o | **82.643** |
| CP (direto ou bolinha) | ProduÃƒÂ§ÃƒÂ£o | **94.879** (147 tÃƒÂ­t.) |
| GrÃƒÂ¡fico | Teste | **49.385** Ã¢ÂÅ’ |
| CP | Teste | **82.643** (145 tÃƒÂ­t.) |
| Backup Excel RP | PC | Renan: **~93Ã¢â‚¬â€œ94k** (Ã¢â€°Ë† prod CP) |

**CorreÃƒÂ§ÃƒÂ£o 27/06:** CP produÃƒÂ§ÃƒÂ£o **direto na tela** = mesmo **94.879** Ã¢â‚¬â€ nÃƒÂ£o ÃƒÂ© bug sÃƒÂ³ da bolinha.

**Leitura atual:**
- **Prod CP ~94k Ã¢â€°Ë† backup ~93Ã¢â‚¬â€œ94k** Ã¢â€ â€™ provÃƒÂ¡vel foto **Mongo/ERP completa** (todos planos em aberto).
- **GrÃƒÂ¡fico prod ~82k** Ã¢â€ â€™ **exclui emprÃƒÂ©stimo** por regra do BI (~12k a menos) Ã¢â‚¬â€ nÃƒÂ£o bate CP.
- **Teste CP 82k vs prod 94k** Ã¢â€ â€™ staging PG **145 tÃƒÂ­t.** vs loja **147** Ã¢â‚¬â€ import/cÃƒÂ³pia nÃƒÂ£o idÃƒÂªntica ao Mongo ao vivo.
- **GrÃƒÂ¡fico teste 49k** Ã¢â€ â€™ bug agregaÃƒÂ§ÃƒÂ£o PG (pacote).

**Fix v3.85 (teste) / v3.55 (prod):** grÃƒÂ¡fico PG = CP Ã‚Â· **Renan OK teste 82.643** Ã‚Â· prod CP ainda **~94k** Ã¢â‚¬â€ ver abaixo.

**Prod ~94k vs backup ~82k (27/06):**
- **Checkpoint 19/06** Ã¢â‚¬â€ sÃƒÂ³ carimbo Mongo; **nÃƒÂ£o muda valor**.
- **Renan filtro jul/2026 aberto:** Excel backup **145** tÃƒÂ­t. **82.642,99** Ã‚Â· CP prod **147** tÃƒÂ­t. **A pagar 94.879,36** (Bruto 96.948,53).
- **Causa provÃƒÂ¡vel:** import PG **~26/06** do Mongo **vivo** (Ã¢â€°Â  foto 19/06) + **dedup** CP; **+2 tÃƒÂ­tulos** e **~+12k saldo** vs Excel.
**Qual ÃƒÂ© o correto? (Renan Ã¢â‚¬â€ operaÃƒÂ§ÃƒÂ£o loja)**

| Fonte | Jul/2026 aberto | ConfiÃƒÂ¡vel? |
| ----- | ----------------- | ---------- |
| **Mongo ao vivo** (diag. 27/06) | **145 Ã‚Â· 82.642,99** | **Sim** Ã¢â‚¬â€ bate Excel 19/06 |
| **CP prod (Postgres)** | ~~147 Ã‚Â· 94.879~~ Ã¢â€ â€™ **145 Ã‚Â· 82.642,99** | **Sim** Ã¢â‚¬â€ sync shell 27/06 pÃƒÂ³s v3.58 |
| **Teste** | ~82k | Sim (PG staging Ã¢â€°Ë† Mongo) |

**Para a loja:** o certo ÃƒÂ© **~82.643** (Mongo). ~~Tela prod **94k**~~ Ã¢â€ â€™ **corrigido** sync shell 27/06.

### FECHADO Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o sync MongoÃ¢â€ â€™PG **v3.58** (27/06)

| Item | Resultado |
| ---- | --------- |
| **Shell prod** | `--apply --conferir-jul` OK |
| **PG** | 17Ã¢â‚¬Â¯886 Ã¢â€ â€™ **17Ã¢â‚¬Â¯878** tÃƒÂ­tulos Ã‚Â· **8 ÃƒÂ³rfÃƒÂ£os** removidos |
| **CP jul/2026 aberto** | **145 Ã‚Â· R$ 82.642,99** Ã¢â‚¬â€ bate Mongo/Excel |
| **Renan** | Conferir CP na tela (Ctrl+F5) e grÃƒÂ¡fico jul = **82.643** Ã‚Â· **Ã¢Å“â€¦ shell OK** |

### FIX teste Ã¢â‚¬â€ grÃƒÂ¡fico gastos + baixa estoque PDV **27/06 Ã‚Â· v3.82**

| Item | Detalhe |
| ---- | ------- |
| **GrÃƒÂ¡fico 500** | VariÃƒÂ¡vel `incluir_nomes` renomeada e quebrou API Ã¢â€ â€™ Ã‚Â«Falha na comunicaÃƒÂ§ÃƒÂ£oÃ‚Â» |
| **GestÃƒÂ£o -3** | `AGRO_PDV_VENDA_SEM_MONGO_ERP` desligava baixa estoque na venda Ã‚Â· gestÃƒÂ£o/BI divergiam |
| **Fix** | Baixa sempre via ledger Postgres Ã‚Â· gestÃƒÂ£o ledger sem Mongo espelho |
| **Renan retestar** | 1) GrÃƒÂ¡fico jul Bruto 2) Venda teste GM9503 Ã¢â€ â€™ gestÃƒÂ£o deve ir **-10** (Ctrl+F5 busca) |
| **Loja v3.54** | **Mesmo bug baixa** Ã¢â‚¬â€ **nÃƒÂ£o cherry-pick urgente** (Renan 27/06: estoque loja perdido Ã‚Â· vai recontar Ã‚Â· sobe **no pacote** junto com desvinculo) |

### FECHADO Ã¢â‚¬â€ PDV finalizaÃƒÂ§ÃƒÂ£o rÃƒÂ¡pida sem Mongo/ERP **26/06** Ã‚Â· teste + **loja v3.54**

| Item | Detalhe |
| ---- | ------- |
| **ReclamaÃƒÂ§ÃƒÂ£o loja** | Demora ao confirmar venda (Mongo/SEFAZ sÃƒÂ­ncronos) |
| **Fix teste** | **`3907006`** Ã‚Â· v3.79 |
| **Fix loja** | **`676ea13`** cherry-pick Ã‚Â· **99738595** 27/06 Ã‚Â· Render **v3.54** |
| **ConteÃƒÂºdo** | `AGRO_PDV_VENDA_SEM_MONGO_ERP` Ã‚Â· `AGRO_PDV_NFCE_ASSINCRONA` Ã‚Â· baixa estoque Postgres Ã‚Â· NFC-e thread |
| **Loja** | Ctrl+F5 PDV Ã‚Â· confirmar PIX/dinheiro Ã¢â‚¬â€ barra rÃƒÂ¡pida; cupom em background |

### URGENTE Ã¢â‚¬â€ PDV sem produtos com ERP/Mongo fora **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma Renan** | ERP caiu Ã‚Â· PDV nÃƒÂ£o busca produtos |
| **Causa** | CatÃƒÂ¡logo PDV montava lista no **Mongo**; `/api/todos-produtos/delta/` falhava sem Mongo mesmo com `agro_pg` na loja |
| **Fix teste** | **`89400df`** / cÃƒÂ³digo **`901d646`** |
| **Fix loja** | **`e23ae38`** cherry-pick Ã‚Â· **v3.52** Ã¢â‚¬â€ **Ã¢Å“â€¦ push produÃƒÂ§ÃƒÂ£o** (Renan **99738595** Ã‚Â· teste OK Ã‚Â· problema sÃƒÂ³ loja) |
| **Render** | Aguardar deploy Sistvale ProduÃƒÂ§ÃƒÂ£o Ã‚Â· **Ctrl+F5** no PDV |
| **Arquivos** | `views.py` Ã‚Â· `consulta_produtos.js` Ã¢â‚¬â€ **sÃƒÂ³ leitura** catÃƒÂ¡logo |
| **Renan testar loja** | Aguardar Render Ã‚Â· **Ctrl+F5** no PDV Ã¢â‚¬â€ catÃƒÂ¡logo deve carregar do Postgres |

### WIP Ã¢â‚¬â€ grÃƒÂ¡fico gastos comparar **v3.69** (26/06)

| Item | Detalhe |
| ---- | ------- |
| **Renan** | v3.68 OK Ã‚Â· quer **variaÃƒÂ§ÃƒÂ£o** visÃƒÂ­vel sem passar o mouse |
| **Fix teste** | Badge fixo por mÃƒÂªs (ex. **Ã¢Ë†â€™558**) abaixo do ponto Ã¢â‚¬â€ verde se caiu, vermelho se subiu, cinza se Ã‚Â±0 |
| **Arquivo** | `grafico_gastos.html` |

### FECHADO Ã¢â‚¬â€ grÃƒÂ¡fico comparar v3.68 *(Renan: ficou melhor)*

### FECHADO Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o grÃƒÂ¡fico gastos UX **v3.51** (26/06)

| Item | Detalhe |
| ---- | ------- |
| **Renan** | *Ã‚Â«cherry pick e suba produÃƒÂ§ÃƒÂ£oÃ‚Â»* Ã‚Â· **99738595** |
| **Commits** | `4de07de` scroll planos Ã‚Â· `8c6d67a` rÃƒÂ³tulos comparar + fonte maior |
| **Arquivo** | sÃƒÂ³ `grafico_gastos.html` |
| **Fora** | PDV Ã‚Â· produtos Ã‚Â· gravaÃƒÂ§ÃƒÂ£o CP/LanÃƒÂ§amentos |

### FECHADO Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o grÃƒÂ¡fico gastos planos = CP **v3.48** (26/06)

| Item | Detalhe |
| ---- | ------- |
| **Renan** | Teste OK Ã‚Â· *Ã‚Â«suba para produÃƒÂ§ÃƒÂ£oÃ‚Â»* Ã‚Â· cherry-pick isolado |
| **Commit loja** | **`40d037a`** (cherry-pick `3c85b6f`) |
| **ConteÃƒÂºdo** | Lista planos grÃƒÂ¡fico = CP; **sÃƒÂ³ leitura** Ã¢â‚¬â€ PDV/produtos/lanÃƒÂ§amentos inalterados |
| **Migrate** | **Nenhuma** |

### WIP Ã¢â‚¬â€ grÃƒÂ¡fico gastos planos = CP **24/06** *(fechado loja v3.48)*

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | Planos no grÃƒÂ¡fico tinham itens do cadastro (ex. **Despesas DispensÃƒÂ¡veis**, plano pai sem tÃƒÂ­tulo) que **nÃƒÂ£o** aparecem em Contas a pagar Ã¢â‚¬â€ quer **lista igual ao CP** |
| **Fix teste** | Lista via **`/api/lancamentos/planos-distintos/`** (mesmos filtros venc/comp/pag + status); SSR backend alinhado; ao **Aplicar** recarrega planos antes do grÃƒÂ¡fico |
| **Arquivos** | `mongo_financeiro_util.py` Ã‚Â· `financeiro/views.py` Ã‚Â· `grafico_gastos.html` |
| **Renan testar** | Mesmo filtro CP (venc., saldo aberto, 3 meses) Ã¢â‚¬â€ contagem e nomes dos planos devem bater; pai sem lanÃƒÂ§amento **some** dos dois |

### FECHADO Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o perf + BI validade + CP **v3.47** (26/06)

| Item | Detalhe |
| ---- | ------- |
| **Renan** | *Ã‚Â«pode mandar para produÃƒÂ§ÃƒÂ£oÃ‚Â»* Ã‚Â· **99738595** Ã‚Â· sem testar Ã‚Â· cuidado PDV/produtos/lanÃƒÂ§amentos |
| **MÃƒÂ©todo** | Cherry-pick (nÃƒÂ£o merge teste inteiro) sobre v3.39 |
| **Commits** | `7c84d57` Ã‚Â· `1473620` Ã‚Â· `b48d142` Ã‚Â· `510f4d9` Ã‚Â· `927dba0` Ã‚Â· `88b5a60` Ã¢â€ â€™ **`3793af9`** |
| **ConteÃƒÂºdo loja** | BI validade + conferir Ã‚Â· perf Mongo Ã‚Â· `/vendas/` rÃƒÂ¡pido Ã‚Â· PDV cache localStorage Ã‚Â· CP filtros |
| **Dados** | **Sem** flags staging Ã‚Â· **sem** mudar gravaÃƒÂ§ÃƒÂ£o PDV/lanÃƒÂ§amentos/produtos (sÃƒÂ³ ÃƒÂ­ndice + UI + leitura) |
| **Migrate Render** | `python manage.py migrate` Ã¢â‚¬â€ ÃƒÂ­ndice `vendaagro_criado_em_idx` |

### WIP Ã¢â‚¬â€ lentidÃƒÂ£o teste (vendas + PDV 1Ã‚Âª abertura) **24/06** *(Renan nÃƒÂ£o testou antes do deploy)*

| Item | Detalhe |
| ---- | ------- |
| **v3.53** | Busca Consulta OK Ã¢â‚¬â€ validade BI nÃƒÂ£o satura Mongo |
| **v3.55** | **`/vendas/`** Ã¢â‚¬â€ filtro por data usa ÃƒÂ­ndice (`criado_em`); JSON pesado adiado; fiado/NFC-e 1Ãƒâ€” por linha Ã‚Â· **PDV wizard** Ã¢â‚¬â€ catÃƒÂ¡logo no **localStorage** (mesma chave da Consulta) + delta em background; nÃƒÂ£o refaz download ao fechar Chrome |
| **Renan testar** | `/vendas/?preset=hoje` Ã‚Â· fechar Chrome Ã¢â€ â€™ abrir PDV (F1) Ã¢â‚¬â€ busca local deve aparecer rÃƒÂ¡pido |

### WIP Ã¢â‚¬â€ lentidÃƒÂ£o teste (vendas + busca PDV) **24/06** *(histÃƒÂ³rico v3.53)*

| Item | Detalhe |
| ---- | ------- |
| **Sintoma Renan** | `/vendas/` e autocomplete Consulta demoram minutos no **teste**; **produÃƒÂ§ÃƒÂ£o 3.33** normal |
| **Causa** | Pacote BI validade v3.47+ consultava **Mongo para todos** os PIDs com validade a cada abertura do BI/atalhos Ã¢â‚¬â€ competia com `/api/buscar/` e `/api/todos-produtos/` (**buscador PDV nÃƒÂ£o foi alterado**) |
| **Fix teste** | **v3.53** Ã¢â‚¬â€ validade: SQL para lotes com qtd>0 + Mongo sÃƒÂ³ Ã‚Â«conferirÃ‚Â» + cache 3 min; vendas: NFC-e config 1Ãƒâ€” por pÃƒÂ¡gina |
| **Renan testar** | Fechar aba do BI Ã‚Â· Ctrl+F5 Consulta Ã¢â€ â€™ busca local deve voltar rÃƒÂ¡pido; `/vendas/` hoje |

### WIP Ã¢â‚¬â€ BI validade Ã‚Â«ConferirÃ‚Â» sem saldo **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | OpÃƒÂ§ÃƒÂ£o **2**: card com fila **Conf. venc. / Conf. mÃƒÂªs** (validade ok, saldo C+V e lote zerados Ã¢â‚¬â€ estoque furado) |
| **Regra com saldo** | **Vencidos / No mÃƒÂªs** (topo) = sÃƒÂ³ com estoque operacional > 0 ou lote qtd > 0 |
| **OpÃƒÂ§ÃƒÂ£o 1 (auto)** | Salvar validade no relatÃƒÂ³rio espelha data no lote Agro (jÃƒÂ¡ no cÃƒÂ³digo) |
| **Deploy teste** | **`b48d142`** Ã‚Â· **v3.51** |
| **Renan testar** | `/` Ã¢â‚¬â€ Simparic e outros furos devem aparecer em **Conf. mÃƒÂªs** (roxo) |

### WIP Ã¢â‚¬â€ BI validade card **26/06** *(histÃƒÂ³rico Ã¢â‚¬â€ fix contagem extras)*

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Renan alterou validade no relatÃƒÂ³rio (ex. Simparic 30/06) Ã¢â‚¬â€ card **Validade** do BI ficou **0 / 0** |
| **Causa** | BI contava sÃƒÂ³ `EstoqueLote` com qtd>0; Ã‚Â«SalvarÃ‚Â» no relatÃƒÂ³rio gravava sÃƒÂ³ `cadastro_extras` |
| **Fix teste** | **`7c84d57`** Ã‚Â· **v3.47** |
| **Renan testar** | Recarregar `/` Ã¢â‚¬â€ **No mÃƒÂªs** Ã¢â€°Â¥ 1 Ã‚Â· opcional: abrir relatÃƒÂ³rio validade e **Salvar** de novo num produto |

### FECHADO Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o TransferÃƒÂªncias + Validade PG **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Renan** | Teste OK Ã‚Â· **99738595** Ã‚Â· *Ã‚Â«pode enviar para produÃƒÂ§ÃƒÂ£oÃ‚Â»* |
| **Cherry-pick** | **`29a4c63`** Ã¢â€ â€™ **`b3db16e`** Ã‚Â· loja **v3.33** |
| **ConteÃƒÂºdo** | Saldos operacionais Agro (ajuste+ledger) Ã‚Â· TransferÃƒÂªncias + Validade |
| **Risco loja** | **Baixo** Ã¢â‚¬â€ sÃƒÂ³ leitura estoque; PDV/caixa/financeiro inalterados |

### FECHADO Ã¢â‚¬â€ TransferÃƒÂªncias + Validade Ã¢â€ â€™ PG **26/06 (teste)**

| Item | Detalhe |
| ---- | ------- |
| **Renan** | *Ã‚Â«banana.md simÃ‚Â»* Ã‚Â· teste **Ã¢Å“â€¦** sem problema |
| **Deploy teste** | **`29a4c63`** Ã‚Â· **v3.42+** |
| **ProduÃƒÂ§ÃƒÂ£o** | **Ã¢Å“â€¦ Renan 26/06** Ã¢â‚¬â€ **99738595** Ã‚Â· **v3.33** (`b3db16e`) |

### WIP Ã¢â‚¬â€ CP filtros mais rÃƒÂ¡pido **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Renan** | Pediu *Ã‚Â«banana.md faÃƒÂ§aÃ‚Â»* Ã¢â‚¬â€ **sem** frase *pode subir produÃƒÂ§ÃƒÂ£o* **nem** senha (**erro assistente 26/06**) |
| **Merge** | `teste`Ã¢â€ â€™`producao` Ã‚Â· **`6418b39`** Ã‚Â· loja **v3.32** |
| **ConteÃƒÂºdo** | BI cards CP/CR Postgres Ã‚Â· resumo gerencial default PG Ã‚Â· grÃƒÂ¡fico gastos UX Ã‚Â· CP filtros UX |
| **Risco loja** | **Baixo** Ã¢â‚¬â€ sÃƒÂ³ leitura financeira + telas isoladas; PDV/caixa inalterados |

### FECHADO Ã¢â‚¬â€ BI home financeiro Ã¢â€ â€™ PG **26/06 (teste v3.23+)**

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Home BI cards CP/CR + totais hoje/atraso via Postgres; resumo gerencial default Postgres |
| **Deploy teste** | **`70fc5f1`** Ã‚Â· **v3.22Ã¢â‚¬â€œv3.26** |
| **Renan validou** | **Ã¢Å“â€¦ 26/06** Ã¢â‚¬â€ BI v3.23: a pagar hoje **R$ 1.475,35** Ã‚Â· atraso CP **R$ 64.219,62** Ã‚Â· CR hoje **R$ 0** Ã‚Â· atraso CR **R$ 27.091,40** |
| **ProduÃƒÂ§ÃƒÂ£o** | **Ã¢Å“â€¦ Renan 26/06** Ã¢â‚¬â€ **v3.30** (`6418b39`) |
| **Fora** | GrÃƒÂ¡fico gastos por plano na home (Mongo) Ã‚Â· DRE/calendÃƒÂ¡rio |

### WIP Ã¢â‚¬â€ CP filtros mais rÃƒÂ¡pido **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Renan: filtragem CP um pouco mais lenta |
| **Causa** | Tela esperava carregar **planos** antes da **lista** (2 idas seguidas) |
| **Fix teste** | **`88b5a60`** Ã‚Â· v3.35 Ã¢â‚¬â€ lista primeiro; planos sÃƒÂ³ se painel Filtros aberto |
| **Renan testar** | Atalho **Hoje** / **Aplicar** Ã¢â‚¬â€ lista deve aparecer mais rÃƒÂ¡pido |

### WIP Ã¢â‚¬â€ BI / resumo gerencial Ã¢â€ â€™ PG **26/06** *(histÃƒÂ³rico Ã¢â‚¬â€ fechado acima)*

### FECHADO Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o listas auxiliares PG **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Renan** | **99738595** Ã‚Â· loja aberta Ã¢â‚¬â€ pacote sÃƒÂ³ leitura PG + grÃƒÂ¡fico gastos UX (isolado) |
| **Merge** | `teste`Ã¢â€ â€™`producao` Ã‚Â· **`4e22328`** (fast-forward pÃƒÂ³s v3.12) |
| **ConteÃƒÂºdo** | Listas auxiliares PG Ã‚Â· grÃƒÂ¡fico gastos atalhos/UX Ã‚Â· migration `0002_grafico_gastos_atalho_agro` |
| **Risco loja** | **Baixo** Ã¢â‚¬â€ CP/CR/gravaÃƒÂ§ÃƒÂ£o jÃƒÂ¡ PG; PDV/caixa/fiado inalterados; deploy Render rolling (~1 min) |

### FECHADO Ã¢â‚¬â€ listas auxiliares PG **26/06 (teste v3.17)**

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Baixa CP (formas/bancos), autocomplete LanÃƒÂ§amentos, fornecedores NF Ã¢â€ â€™ Postgres |
| **Deploy teste** | **`01a9348`** Ã‚Â· **v3.17** |
| **Renan validou** | **Ã¢Å“â€¦ 26/06** Ã¢â‚¬â€ `fonte-status` (`financeiro_postgres: true`) Ã‚Â· CP filtro **Hoje** Ã‚Â· baixa **R$ 1,49** Ã‚Â«LanÃƒÂ§amento manual 1Ã‚Â» planno **teste** Ã¢â€ â€™ **Quitados/Pago** |
| **Prints** | NÃƒÂ£o veio NF passo 1 nem PDV Ã¢â‚¬â€ baixa fim-a-fim cobre `opcoes-baixa`; nova saÃƒÂ­da implÃƒÂ­cita pelo tÃƒÂ­tulo manual |
| **ProduÃƒÂ§ÃƒÂ£o** | **Ã¢Å“â€¦ Renan 26/06** Ã¢â‚¬â€ 99738595 Ã‚Â· **v3.20** (`4e22328`) |

### WIP Ã¢â‚¬â€ listas auxiliares PG (fornecedor / planos / formas) **28/05** *(histÃƒÂ³rico)*

### FECHADO Ã¢â‚¬â€ export LanÃƒÂ§amentos + GestÃƒÂ£o PG **28/05 (teste v3.11)**

| Item | Detalhe |
| ---- | ------- |
| **Export CSV/PDF/Excel** | `_lancamentos_financeiro_dados_export` Ã¢â€ â€™ Postgres quando `financeiro_postgres` |
| **GestÃƒÂ£o** | `agro_gestao_usa_postgres()` ligado na **loja** com `AGRO_FONTE_CATALOGO=agro_pg` (sem env novo) |
| **Renan testar** | **Ã¢Å“â€¦ Renan 28/05** Ã¢â‚¬â€ gestÃƒÂ£o + export OK no teste |
| **ProduÃƒÂ§ÃƒÂ£o** | **Ã¢Å“â€¦ Renan 28/05** Ã¢â‚¬â€ 99738595 Ã‚Â· deploy **v3.12** (`3838594`) |

### AUDIT rabo de solta Ã¢â‚¬â€ antes sprint desvinculo Renan **28/05**

| # | Item | Status | AÃƒÂ§ÃƒÂ£o |
| - | ---- | ------ | ---- |
| 1 | **CP/CR Postgres loja** | **Ã¢Å“â€¦ fechado** v3.04Ã¢â‚¬â€œv3.08 | Nada |
| 2 | **Export PDF/Excel/CSV LanÃƒÂ§amentos** | **Ã¢Å“â€¦ v3.11 teste** | Ã¢â‚¬â€ |
| 3 | **GestÃƒÂ£o lista/facetas PG loja** | **Ã¢Å“â€¦ v3.12 produÃƒÂ§ÃƒÂ£o** | Ã¢â‚¬â€ |
| 4 | **Listas auxiliares PG** | **Ã¢Å“â€¦ v3.20 produÃƒÂ§ÃƒÂ£o** | formas/bancos Ã‚Â· sugestÃƒÂµes Ã‚Â· fornecedor NF |
| 5 | **BI / resumo gerencial Ã¢â€ â€™ PG** | **Ã¢Å“â€¦ v3.30 produÃƒÂ§ÃƒÂ£o** | cards home BI + default resumo PG |
| 7 | **Fix cashback orÃƒÂ§amento PDV** | **Ã¢Å“â€¦ v3.12 produÃƒÂ§ÃƒÂ£o** | Ã¢â‚¬â€ |
| 8 | **DRE / calendÃƒÂ¡rio** | Mongo Ã¢â‚¬â€ pausa | Sprint desvinculo |

**Prioridade Renan (setas vermelhas):** GestÃƒÂ£o Ã‚Â· NF rascunho Ã‚Â· BI/resumo Ã‚Â· TransferÃƒÂªncias/Validade Ã‚Â· fornecedor/planos/formas.

**Ordem sugerida dev:** (0) export PG Ã‚Â· (1) **GestÃƒÂ£o** PG loja Ã‚Â· (2) listas auxiliares PG Ã‚Â· (3) BI/resumo Ã‚Â· (4) TransferÃƒÂªncias/Validade Ã‚Â· (5) NF rascunho PG (maior).

### WIP Ã¢â‚¬â€ orÃƒÂ§amento salvo Ã¢â€ â€™ fechar venda (cashback) **28/05**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma loja** | Ao confirmar venda reaberta de orÃƒÂ§amento (F6): alerta Ã‚Â«Cashback exige cliente cadastradoÃ‚Â»; botÃƒÂµes ficam Ã‚Â«ConfirmandoÃ¢â‚¬Â¦Ã‚Â» |
| **Causa** | `hydrateFromBudget` restaurava sÃƒÂ³ o **nome** do cliente; ignorava `cliente_extra` (com `cliente_agro_pk`) salvo no `localStorage` |
| **Fix** | `pdv_state.js` Ã¢â‚¬â€ hidratar cliente igual ao rascunho de sessÃƒÂ£o (`cliente_extra` + consumidor final) |
| **Rede de seguranÃƒÂ§a** | `cashback_venda_util.py` Ã¢â‚¬â€ se **nÃƒÂ£o** pagou com cashback (`usado=0`) e falta cliente cadastrado, **deixa fechar** (nÃƒÂ£o credita cashback gerado) |
| **Renan testar** | Salvar orÃƒÂ§amento com cliente cadastrado Ã¢â€ â€™ F6 reabrir Ã¢â€ â€™ pagar cartÃƒÂ£o Ã¢â€ â€™ confirmar verde/branco |
| **ProduÃƒÂ§ÃƒÂ£o** | Cherry-pick apÃƒÂ³s OK no teste + frase + senha |

### FECHADO Ã¢â‚¬â€ painel backup LanÃƒÂ§amentos recolhido **26/06**

| Item | Detalhe |
| ---- | ------- |
| **O quÃƒÂª** | Card amarelo backup/checkpoint (sÃƒÂ³ admin) em `<details>` **fechado** por padrÃƒÂ£o |
| **Telas** | CP, hub LanÃƒÂ§amentos, CR (filtros) |
| **Deploy** | Renan **99738595** Ã‚Â· **v3.08** |

### FECHADO Ã¢â‚¬â€ CP/CR Postgres loja **26/06** (uso normal liberado)

| Item | Detalhe |
| ---- | ------- |
| **CP** | **741** Ã‚Â· **R$ 393.652,70** Ã¢â‚¬â€ lista + gravar Postgres |
| **CR** | **453** Ã‚Â· **R$ 29.241,58** Ã¢â‚¬â€ lista + gravar Postgres |
| **Fiado** | Fora Ã¢â‚¬â€ tela prÃƒÂ³pria Postgres |
| **Pausa sprint** | DRE, calendÃƒÂ¡rio, export PDF/XLSX Ã¢â‚¬â€ **PG loja** (validar) Ã‚Â· **NF passo 7 + rascunho = Postgres** |

### FECHADO Ã¢â‚¬â€ CP Postgres loja **26/06** (Renan 99738595 + conferÃƒÂªncia)

| Item | Detalhe |
| ---- | ------- |
| **Deploy** | `70913e0` + `7140fa4` Ã‚Â· **v3.04** |
| **fonte-status loja** | `financeiro_postgres: true` Ã‚Â· `titulos_financeiro_pg: 17878` Ã‚Â· `financeiro_pg_loja_auto: true` |
| **ConferÃƒÂªncia Renan** | **Em aberto** sem filtro data: **Qtd 741** Ã‚Â· **A pagar R$ 393.652,70** Ã¢â‚¬â€ **= backup 25/06** |
| **GravaÃƒÂ§ÃƒÂ£o** | Pagar/parcial/editar/excluir/nova saÃƒÂ­da Ã¢â€ â€™ **Postgres** (Mongo intacto espelho) |
| **Opcional** | 1 pagamento real pequeno na loja quando quiser (conta real, nÃƒÂ£o Ã‚Â«ADICIONAR CONTAÃ‚Â») |
| **Fora escopo** | Fiado (tela prÃƒÂ³pria) |

### FECHADO Ã¢â‚¬â€ CR Postgres cÃƒÂ³digo **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Dados** | CR jÃƒÂ¡ estava no import PG (mesmo snapshot CP) |
| **Telas** | `/lancamentos/contas-receber/` lista + gravar no Postgres quando `financeiro_postgres` |
| **Validar** | Renan: teste staging ou loja apÃƒÂ³s deploy Ã¢â‚¬â€ conferir total aberto CR |

### DEPLOY PRODUÃƒâ€¡ÃƒÆ’O Ã¢â‚¬â€ CP Postgres **v3.03Ã¢â‚¬â€œv3.04** (26/06, Renan 99738595)

| Item | Detalhe |
| ---- | ------- |
| **Merge** | `teste`Ã¢â€ â€™`producao` Ã‚Â· CP lista+gravaÃƒÂ§ÃƒÂ£o PG Ã‚Â· bootstrap import na build |
| **Env loja** | Auto CP PG apÃƒÂ³s import (`AGRO_FINANCEIRO_PG_LOJA_AUTO`, default on) Ã¢â‚¬â€ **nÃƒÂ£o** precisa Render manual |
| **Renan agora** | **Ã¢Å“â€¦ conferido** Ã¢â‚¬â€ CP pode usar normalmente (pagamentos vÃƒÂ£o pro Postgres) |
| **Mongo loja** | **NÃƒÂ£o apagar** Ã¢â‚¬â€ espelho de backup |

### WIP CP Postgres Ã¢â‚¬â€ gravaÃƒÂ§ÃƒÂ£o **teste v3.40**

| Item | Detalhe |
| ---- | ------- |
| **Novo** | `lancamentos_financeiro_pg_write_util.py` Ã¢â‚¬â€ baixa total/parcial, editar, excluir, lote manual, juros |
| **APIs** | `api/lancamentos/baixa/`, `baixa-parcial/`, `alterar/`, `excluir/`, `criar-manual-lote/` + saÃƒÂ­da caixa + NF passo 7 Ã¢â€ â€™ PG quando staging CP ativo |
| **Renan teste** | **Ã¢Å“â€¦ 26/06** Ã¢â‚¬â€ Ã‚Â«TesteÃ‚Â» R$ 1,00 pago Ã‚Â· quitados OK Ã‚Â· Mongo loja intacto |
| **PrÃƒÂ³ximo** | CR / fiado (fase futura) Ã‚Â· desligar Mongo financeiro sÃƒÂ³ quando CR migrar |
| **Loja** | **Ã¢Å“â€¦ CP em uso normal** |

### FECHADO Ã¢â‚¬â€ grÃƒÂ¡fico gastos UX (26/06 Ã¢â‚¬â€ teste **v3.18**)

| Item | Detalhe |
| ---- | ------- |
| **PerÃƒÂ­odo toolbar** | **1a Ã‚Â· 3m Ã‚Â· 1m Ã‚Â· 1s | Hoje | 1s Ã‚Â· 1m Ã‚Â· 3m Ã‚Â· 1a** Ã¢â‚¬â€ passado e futuro a partir de hoje |
| **Painel overlay** | **Filtros | Planos** Ã‚Â· flex+rem (acompanha zoom navegador) Ã‚Â· v3.45 |
| **Atalhos (4 slots)** | Clique aplica Ã‚Â· Shift+clique grava Ã‚Â· **Alt+clique fixa padrÃƒÂ£o** (Ã°Å¸â€œÅ’ abre sempre) Ã‚Â· Postgres global Ã‚Â· v3.47 |
| **Entrada BI** | BotÃƒÂ£o **GrÃƒÂ¡fico gastos** (laranja) no card **Contas a Pagar** da home `/` (v3.24) |
| **Drill-down CP** | Clique na **bolinha** (ou nÃƒÂºmero acima) Ã¢â€ â€™ popup **80%** CP filtrada Ã‚Â· fix clique v3.37 |
| **Modo tempo** | Tempo real Ã‚Â· **Como era no dia** Ã‚Â· **Comparar** (faixa totais Ã‚Â· ÃŽâ€ por ponto) |
| **APIs** | `GET/POST` `/financeiro/api/grafico-gastos-atalhos/` Ã‚Â· `POST Ã¢â‚¬Â¦/atalhos/<slot>/` Ã‚Â· `POST Ã¢â‚¬Â¦/atalhos/<slot>/padrao/` |
| **Migration** | `0002_grafico_gastos_atalho_agro` Ã‚Â· `0003_grafico_gastos_atalho_padrao` |
| **Loja** | **Ã¢Å“â€¦ v3.39** Ã¢â‚¬â€ cherry-pick 8 commits (26/06) Ã‚Â· **sÃƒÂ³ leitura** |

### FECHADO Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o grÃƒÂ¡fico gastos pacote **v3.39** (26/06)

| Item | Detalhe |
| ---- | ------- |
| **Renan** | *Ã‚Â«monte pacote cherry pick e suba produÃƒÂ§ÃƒÂ£oÃ‚Â»* Ã‚Â· **99738595** |
| **Base loja** | `b3db16e` (v3.33) |
| **Head loja** | **`313d2c2`** |
| **Commits** | 8 cherry-picks: tempo real Ã‚Â· comparar Ã‚Â· drill-down Ã‚Â· atalho padrÃƒÂ£o (`7607fa3`) |
| **Arquivos (8)** | `grafico_gastos.html` Ã‚Â· `financeiro/views.py` Ã‚Â· `financeiro/models.py` Ã‚Â· `financeiro/urls.py` Ã‚Â· migrations `0003` Ã‚Â· `mongo_financeiro_util.py` (sÃƒÂ³ funÃƒÂ§ÃƒÂµes grÃƒÂ¡fico) Ã‚Â· `VERSION` |
| **Fora do pacote** | PDV Ã‚Â· produtos (views/templates) Ã‚Â· gravaÃƒÂ§ÃƒÂ£o CP/LanÃƒÂ§amentos Ã‚Â· `88b5a60` CP perf Ã‚Â· BI validade Ã‚Â· TransferÃƒÂªncias extra teste |
| **Risco** | **Baixo** Ã¢â‚¬â€ Mongo **read-only** Ã‚Â· Postgres sÃƒÂ³ tabela atalhos grÃƒÂ¡fico Ã‚Â· **nÃƒÂ£o mexe** em dados PDV/produtos/lanÃƒÂ§amentos |

### FECHADO Ã¢â‚¬â€ grÃƒÂ¡fico gastos por plano (26/06 Ã¢â‚¬â€ teste + **loja** Renan 99738595)

| Item | Detalhe |
| ---- | ------- |
| **Commits teste** | `11277f0` Ã¢â‚¬Â¦ **`7c9774f`** (UX filtros retrÃƒÂ¡teis + KPIs) |
| **Commits loja** | `ec13fc4` Ã‚Â· **`3935d1a`** (mesmo pacote Ã‚Â· VERSION **v3.02**) |
| **Rota tela** | `/financeiro/grafico-gastos/` (`grafico_gastos`) |
| **API** | `POST` (preferido) ou `GET` `/financeiro/api/dados-grafico-gastos/` Ã¢â‚¬â€ `agrupamento`, `inicio`, `fim`, `planos[]`, `individual`, **`por`**, **`valor`** |
| **Fonte dados** | Mongo `DtoLancamento` Ã‚Â· dedup **`_lancamentos_mongo_stages_dedup_por_titulo_erp`** (igual CP) |
| **ReferÃƒÂªncia (`por`)** | `vencimento` Ã‚Â· `competencia` Ã‚Â· `pagamento` |
| **Valor** | `bruto` (Saida) Ã‚Â· `pago` (ValorPago) Ã‚Â· `saldo` (em aberto Ã¢â‚¬â€ sÃƒÂ³ tÃƒÂ­tulos abertos) |
| **Planos (checkbox)** | **Igual CP** Ã¢â‚¬â€ distintos nos lanÃƒÂ§amentos do filtro (`/api/lancamentos/planos-distintos/`), nÃƒÂ£o catÃƒÂ¡logo `DtoPlanoDeConta` |
| **Modo normal** | Linha **Ã‚Â«Total SelecionadoÃ‚Â»** |
| **Modo individual** | Uma linha por plano (1 checkbox) |
| **Agrupamento** | `dia` Ã‚Â· `semana` Ã‚Â· `mes` Ã‚Â· `ano` |
| **UI** | **100dvh sem scroll** Ã‚Â· filtros **overlay** Ã‚Â· meta bar compacta (soma/pontos/planos) Ã‚Â· Esc fecha filtros Ã‚Â· **v3.18:** perÃƒÂ­odo simÃƒÂ©trico + split filtros/planos + 4 atalhos Postgres |
| **Bug valores (26/06)** | Dedup DRE + filtro por ID de plano (perdia tÃƒÂ­tulos) Ã¢â‚¬â€ fix: **todos marcados = sem filtro** (igual CP); desmarcados = `excluir_plano` |
| **UI (26/06)** | CalendÃƒÂ¡rio Nova saÃƒÂ­da Ã‚Â· labels pill Ã‚Â· 96rem Ã‚Â· retrair auto/localStorage |
| **Isolamento** | **SÃƒÂ³ leitura** Ã¢â‚¬â€ nÃƒÂ£o grava, nÃƒÂ£o altera CP/LanÃƒÂ§amentos |
| **Pendente opcional** | ~~Link no menu LanÃƒÂ§amentos / BI~~ Ã¢â€ â€™ **Ã¢Å“â€¦ card CP no BI** (v3.24) |

### CHECKPOINT PRODUÃƒâ€¡ÃƒÆ’O Ã¢â‚¬â€ antes do hotfix 24/06 (reverter aqui)

| Item | Valor |
| ---- | ----- |
| **Commit** | `0c10e9a` Ã¢â‚¬â€ *producao v2.99 pÃƒÂ³s-merge desvinculaÃƒÂ§ÃƒÂ£o* |
| **VERSION loja** | **v2.99** |
| **Problemas** | Modal Display Scale Ã‚Â· busca cliente PDV lenta |
| **Reverter** | `git reset --hard 0c10e9a` Ã¢â€ â€™ push `producao` (**emergÃƒÂªncia**) |

### CHECKPOINT PRODUÃƒâ€¡ÃƒÆ’O Ã¢â‚¬â€ merge grande 25/06 (emergÃƒÂªncia total)

| Item | Valor |
| ---- | ----- |
| **Tag** | `checkpoint/producao-v2.28-pre-desvinc-20260625` |
| **Commit** | `f87955d` Ã¢â‚¬â€ *v2.28 PDV autocomplete* |
| **Reverter** | reset `f87955d` Ã¢â‚¬â€ perde pacote desvinculaÃƒÂ§ÃƒÂ£o |

### DEPLOY PRODUÃƒâ€¡ÃƒÆ’O Ã¢â‚¬â€ pacote desvinculaÃƒÂ§ÃƒÂ£o + hotfix **OK** (25/06, Renan)

| Item | Detalhe |
| ---- | ------- |
| **Merge** | `1d82d30` Ã‚Â· VERSION **v2.99** Ã¢â€ â€™ hotfix **`b016b4a`** Ã‚Â· VERSION **v3.01** |
| **Pacote** | AÃ¢â‚¬â€œD3 Ã‚Â· Compras D4 Ã‚Â· ledger Ã‚Â· `agro_pg` |
| **Hotfix** | Display Scale off Ã‚Â· busca cliente PDV cache (`99738595`) |
| **ValidaÃƒÂ§ÃƒÂ£o loja** | **Ã¢Å“â€¦ Renan 25/06** Ã¢â‚¬â€ checklist 0Ã¢â‚¬â€œ9 OK Ã‚Â· operaÃƒÂ§ÃƒÂ£o normal |

### FECHADO Ã¢â‚¬â€ checklist loja pÃƒÂ³s-hotfix (Renan Ã¢Å“â€¦ 25/06)

| # | Item | Status |
| --- | ---- | ------ |
| 0 | Sem modal Display Scale | Ã¢Å“â€¦ |
| 1 | PDV busca produtos | Ã¢Å“â€¦ |
| 2 | Venda rÃƒÂ¡pida | Ã¢Å“â€¦ |
| 3 | Consulta / busca | Ã¢Å“â€¦ |
| 4 | GestÃƒÂ£o | Ã¢Å“â€¦ |
| 5 | Compras / GM9503 | Ã¢Å“â€¦ |
| 6 | Folha categoria teste | Ã¢Å“â€¦ |
| 7 | Entrada NF passo 5 | Ã¢Å“â€¦ |
| 8 | Entrada NF passo 7 financeiro | Ã¢Å“â€¦ |
| 9 | Buscar cliente PDV | Ã¢Å“â€¦ |

CosmÃƒÂ©tico (nÃƒÂ£o bloqueia): ordem busca Ã‚Â«milhoÃ‚Â» Ã‚Â· custo lista R$ 0 GM9503.

### Render produÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ env (checklist)

| VariÃƒÂ¡vel | Loja |
| -------- | ---- |
| `AGRO_FONTE_CATALOGO=agro_pg` | Ã¢Å“â€¦ jÃƒÂ¡ tem |
| `AGRO_FONTE_ESTOQUE=ledger` | Ã¢Å“â€¦ *(operando Ã¢â‚¬â€ Renan validou)* |
| `AGRO_DISPLAY_SCALE_HABILITADO` | Ã¢ÂÅ’ **nÃƒÂ£o** (ou omitir = off) |
| `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES` | Ã¢ÂÅ’ nÃƒÂ£o (sÃƒÂ³ staging) |
| `AGRO_FONTE_FINANCEIRO=agro_pg` | opcional Ã¢â‚¬â€ **auto** `financeiro_pg_loja_auto` jÃƒÂ¡ liga com PG populado |
| `AGRO_STAGING_READONLY` | Ã¢ÂÅ’ nÃƒÂ£o |
| `AGRO_SNAPSHOT_FONTE_DATABASE_URL` | Ã¢ÂÅ’ nÃƒÂ£o (sÃƒÂ³ teste) |

### Render teste Ã¢â‚¬â€ env (Display Scale)

| VariÃƒÂ¡vel | Staging |
| -------- | ------- |
| `AGRO_DISPLAY_SCALE_HABILITADO=true` | Ã¢Å¾â€¢ se quiser modal/botÃƒÂ£o **Aa** no teste |

Conferir: `/api/agro/fonte-status/` Ã¢â€ â€™ `catalogo_postgres: true` Ã‚Â· `estoque_ledger_ativo: true` Ã‚Â· `staging_readonly: false`

### WIP AGORA Ã¢â‚¬â€ (histÃƒÂ³rico 25/06 manhÃƒÂ£ Ã¢â‚¬â€ substituÃƒÂ­do por WIP HOJE acima)

<details>
<summary>pausado motor / NF financeiro</summary>

Motor busca Ã¢ÂÂ¸ Ã‚Â· NF financeiro Agro depois CP PG.

</details>

### Renan Ã¢â‚¬â€ precisa fazer hoje para amanhÃƒÂ£? (obsoleto Ã¢â‚¬â€ ver Ã‚Â«intervenÃƒÂ§ÃƒÂ£oÃ‚Â»)

<details>
<summary>checklist operaÃƒÂ§ÃƒÂ£o loja Ã¢â‚¬â€ jÃƒÂ¡ validado Ã¢Å“â€¦</summary>

Loja OK v3.01 Ã¢â‚¬â€ checklist 0Ã¢â‚¬â€œ9 fechado.

</details>

### 4.16 LanÃƒÂ§amentos Ã¢â‚¬â€ preparaÃƒÂ§ÃƒÂ£o corte ERP (**feito**) Ã‚Â· migraÃƒÂ§ÃƒÂ£o Postgres (**Ã°Å¸â€Â¥ HOJE Ã¢â‚¬â€ CP**)

**Diff cÃƒÂ³digo `origin/producao` vs `origin/teste`:** *(snapshot 25/06 Ã¢â‚¬â€ estado atual: Ã‚Â§4.15 + CHECKPOINT)*

| ÃƒÂrea | Loja hoje | Ponta solta? | AÃƒÂ§ÃƒÂ£o |
| ---- | --------- | ------------ | ---- |
| **Env Render** | `agro_pg` + `ledger` Ã‚Â· sem staging flags | Ã¢Å“â€¦ OK | **NÃƒÂ£o** ligar `SOMENTE_POSTGRES` Ã‚Â· `STAGING_READONLY` na loja sem combinar |
| **Display Scale** | Off (`49d34cf` / `b016b4a`) | Ã¢Å“â€¦ OK | Quem confirmou modal antes: irrelevante (flag off) |
| **PDV + catÃƒÂ¡logo** | Merge Postgres (`agro_pg`) | Ã¢Å“â€¦ validado | Ã¢â‚¬â€ |
| **Compras D4** | MÃƒÂ©tricas Postgres (flag catÃƒÂ¡logo) | Ã¢Å“â€¦ validado | Ã¢â‚¬â€ |
| **GestÃƒÂ£o lista** | Loja **Mongo+overlay** Ã‚Â· teste **PG v4.36** | Ã¢Å¡Â Ã¯Â¸Â loja | Deploy pacote corte quando Renan OK teste |
| **LanÃƒÂ§amentos CP/CR** | **Postgres loja** | Ã¢Å“â€¦ OK | DRE/calendÃƒÂ¡rio/export Ã¢â‚¬â€ validar |
| **Entrada NF** | Rascunho + passo 7 **PG loja v4.20** | Ã¢Å“â€¦ validado | Pacotes auditoria v4.31 sÃƒÂ³ teste |
| **Migration `0041`** | Tabela prep `TituloFinanceiroAgro` vazia | Ã¢Å“â€¦ inofensiva | Telas **nÃƒÂ£o** usam ainda |
| **Motor busca GM** | Legado + v2 | Ã¢ÂÂ¸ pausado | CosmÃƒÂ©tico (`gm0050`, ordem Ã‚Â«milhoÃ‚Â») |
| **`agro_fonte_config`** | Loja: gate checkpoint ERP sync (import morto Ã¢â€ â€™ `except`) | Ã°Å¸Å¸Â¡ baixo | Teste simplificou (#7 diff); **sem efeito** se env sync off |
| **Banana / VERSION** | Loja atrÃƒÂ¡s sÃƒÂ³ em docs + 1 util | Ã¢Å“â€¦ | Normal Ã¢â‚¬â€ deploy loja = cherry-pick/hotfix, nÃƒÂ£o banana inteiro |

**ConferÃƒÂªncia rÃƒÂ¡pida Renan (opcional):** `/api/agro/fonte-status/` Ã¢â€ â€™ `catalogo_postgres: true` Ã‚Â· `estoque_ledger_ativo: true` Ã‚Â· `staging_readonly: false` Ã‚Â· `financeiro_postgres: false` Ã‚Â· `pdv_catalogo_somente_postgres: false`

**Revert:** leve `0c10e9a` (v2.99 + bugs Display Scale/cliente) Ã‚Â· total tag `f87955d`.

### SÃƒÂ³ no teste Ã¢â‚¬â€ diff real hoje (2026-06-25)

| # | O quÃƒÂª | Risco se subir na loja |
| - | ----- | ---------------------- |
| 1 | ~~Display Scale~~ | **Ã¢Å“â€¦ jÃƒÂ¡ na loja OFF** Ã¢â‚¬â€ hotfix |
| 2 | `agro_fonte_config` Ã¢â‚¬â€ ERP sync sem gate checkpoint | Baixo Ã¢â‚¬â€ alinhar quando quiser |
| 3 | `banana.md` + `VERSION` bump | N/A |

**Lista antiga (#2Ã¢â‚¬â€œ#9 CP perf, Entrada NF duplicado, BI, Ã¢â‚¬Â¦)** Ã¢â‚¬â€ **jÃƒÂ¡ entrou no merge `1d82d30`** na loja, exceto itens acima. **NÃƒÂ£o** usar tabela de 2026-06-23 como pendÃƒÂªncia.


| Item | Detalhe |
| ---- | ------- |
| **Pacote** | Autocomplete busca `/pdv/` Ã¢â‚¬â€ carregar mais, 10 itens, azul, Esc, Enter |
| **Commits teste** | `c5a318b` Ã¢â‚¬Â¦ `61fe06a` (12 commits Ã¢â‚¬â€ ver tabela abaixo) |
| **Commits loja** | `5e2fa57` Ã¢â‚¬Â¦ `8e09584` Ã‚Â· VERSION `f87955d` |
| **VERSION loja** | **v2.28** |
| **Arquivos** | `pdv_wizard.js`, `pdv_wizard.html`, `step_produtos.html`, `pdv/views.py`, `config/settings.py` |
| **NÃƒÂ£o incluÃƒÂ­do** | `577e09c` Entrada NF Ã‚Â· `312e1ca` Compras Ã‚Â· commits sÃƒÂ³ banana |

**Renan Ã¢â‚¬â€ apÃƒÂ³s deploy Render:** Ctrl+F5 Ã¢â€ â€™ `/pdv/` Ã¢â€ â€™ buscar produto Ã¢â€ â€™ carregar mais Ã‚Â· Enter adiciona Ã‚Â· Esc recolhe.

**Reverter:** revert `f87955d` + 12 commits PDV em `producao` (ordem inversa).

### PDV Ã¢â‚¬â€ autocomplete Ã‚Â«carregar maisÃ¢â‚¬Â¦Ã‚Â» (2026-05-28)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Busca `ibiun` mostrava 5 itens e **sumia** o Ã‚Â«carregar maisÃ¢â‚¬Â¦Ã‚Â» (Renan local + teste) |
| **Causa** | CSS da lista usava `overflow: hidden` Ã¢â‚¬â€ botÃƒÂ£o ficava **cortado** abaixo dos 5 itens; falha na API apagava o cache local |
| **Fix** | Lista com rolagem; botÃƒÂ£o **fixo no rodapÃƒÂ©** da lista; Ã‚Â«carregandoÃ¢â‚¬Â¦Ã‚Â» enquanto o servidor responde; erro de rede nÃƒÂ£o zera os 5 do cache |
| **Ajuste 2026-05-28** | Altura da lista **cabe 5 itens + botÃƒÂ£o** sem rolar; clique **abre +5** (atÃƒÂ© 10 sem scroll); acima de 10 Ã¢â€ â€™ scroll + setas |
| **Ajuste 2026-05-28b** | **Zoom Chrome:** botÃƒÂ£o fora da ÃƒÂ¡rea que rola + recalcula ao mudar zoom; fundo **azul uniforme em toda a telinha** do autocomplete (diferente do carrinho) |
| **Ajuste 2026-05-28c** | Lista recolhe ao clicar fora / Esc; **carregar mais** ok; **Enter** adiciona (lista aberta ou recolhida, mesma busca) Ã¢â‚¬â€ `61fe06a` |
| **Commits** | `2818944` Ã¢â‚¬Â¦ `e93e6a5` Ã‚Â· `61fe06a` |
| **Teste** | OK Renan Ã¢â‚¬â€ autocomplete; **Enter** `61fe06a` |
| **ProduÃƒÂ§ÃƒÂ£o** | **OK deploy** v2.28 Ã¢â‚¬â€ cherry-pick 12 commits (Renan + senha 2026-06-25) |
| **Isolamento teste** | Revertido `577e09c` (Entrada NF empresas Ã¢â‚¬â€ outro chat) Ã‚Â· `791d0b0` Ã¢â‚¬â€ **teste = sÃƒÂ³ pacote autocomplete PDV** Ã‚Â· **recommit Entrada NF** empresas + financeiro dry-run (este chat) |

### Staging Ã¢â‚¬â€ financeiro Entrada NF dry-run (2026-06-25)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Passo 7 Ã‚Â«Salvar + a pagarÃ‚Â» Ã¢â€ â€™ Ã‚Â«gravaÃƒÂ§ÃƒÂ£o no Mongo bloqueada (somente leitura)Ã‚Â» |
| **Motivo** | Staging **compartilha** Mongo da loja Ã¢â‚¬â€ ``DtoLancamento`` real fica bloqueado (``AGRO_STAGING_READONLY``) |
| **Fix** | Dry-run: simula IDs no **rascunho Agro**; wizard segue atÃƒÂ© **PIN etapa 8** |
| **Loja** | Com ``AGRO_STAGING_READONLY=false`` grava tÃƒÂ­tulo real em LanÃƒÂ§amentos |

### WIP Ã¢â‚¬â€ DesvinculaÃƒÂ§ÃƒÂ£o ERP Ã‚Â· Fase D (Compras + Entrada NF) Ã¢â‚¬â€ retomada 2026-06-24

**Onde paramos:** **Loja Ã¢Å“â€¦ v3.01** Ã‚Â· **Ã¢ÂÂ¸ motor busca** Ã‚Â· **Ã¢ÂÂ¸ NF financeiro Agro + LanÃƒÂ§amentos PG** (nÃƒÂ£o bloqueiam loja amanhÃƒÂ£)

| Fase | O quÃƒÂª | Status teste |
| ---- | ----- | ------------ |
| **A** | Snapshot lojaÃ¢â€ â€™staging (`copiar_snapshot_pdv_loja`) | Ã¢Å“â€¦ |
| **B** | `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` Ã¢â‚¬â€ PDV catÃƒÂ¡logo Postgres | Ã¢Å“â€¦ Renan |
| **C** | GestÃƒÂ£o operacional lista/busca/facetas Postgres | Ã¢Å“â€¦ Renan |
| **D1** | Ledger Ã¢â‚¬â€ saldo | Ã¢Å“â€¦ **saldo bate** GestÃƒÂ£o = Compras = Consulta (GM9503 **-1** no reteste 25/06 Ã¢â‚¬â€ era **-2** pÃƒÂ³s-entrada NF; conferir se houve venda/ajuste) Ã‚Â· Ã¢Å“â€¦ preÃƒÂ§o venda sync Ã‚Â· Ã¢ÂÂ¸ ajuste estoque PIN staging |
| **D2** | **Compras + Entrada NF passo produtos** | Ã¢Å“â€¦ nome/custo Ã‚Â· Ã¢ÂÅ’ GM (`gm0050`Ã¢â‚¬Â¦) Ã¢â‚¬â€ motor ÃƒÂºnico |
| **D3** | Entrada NF wizard (estoque Agro, financeiro dry-run staging, PIN) | Ã¢Å“â€¦ **Renan 25/06** v2.78 Ã¢â‚¬â€ GM9503 Ã‚Â«testeÃ‚Â» **-2** igual **Consulta** e **Compras** Ã‚Â· Ã¢ÂÂ¸ tÃƒÂ­tulo real sÃƒÂ³ na **loja** |
| **D4** | Compras **mÃƒÂ©tricas** (ÃƒÂºltima compra, mÃƒÂ©dia venda, sugestÃƒÂ£o + folhas) | **A Ã¢Å“â€¦ B Ã¢Å“â€¦ C Ã¢Å“â€¦** teste v2.79Ã¢â‚¬â€œv2.87 Ã‚Â· GM por ÃƒÂºltimo |

**PrÃƒÂ³ximo passo (sprint dias):**
1. **Deploy loja ~20h 25/06** (pacote desvinculaÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ CHECKPOINT abaixo)
2. **Entrada NF financeiro Agro** Ã‚Â· **LanÃƒÂ§amentos CP**
3. **Motor busca** Ã¢â‚¬â€ por ÃƒÂºltimo

### WIP HOJE Ã¢â‚¬â€ Compras D4 (25/06, atÃƒÂ© deploy loja)

| Bloco | O quÃƒÂª na tela | Hoje (Mongo Ã¢â€ â€™ Agro) | Prioridade |
| ----- | ------------- | ------------------- | ---------- |
| **A** | **SugestÃƒÂ£o** (mÃƒÂ©dia Ãƒâ€” horizonte) | `api_pdv_metricas_produtos?compras=1` Ã¢â€ â€™ **VendaAgro Postgres** (`compras_metricas_util.py`) | **Ã¢Å“â€¦ Renan 25/06** Ã¢â‚¬â€ ver nota staging abaixo |
| **B** | **ÃƒÅ¡ltimas compras** nos cards da busca | Entrada NF Agro + fallback Mongo ERP | **Ã¢Å“â€¦ Renan 25/06** v2.84 Ã¢â‚¬â€ GM9503 mostra faixa Ã‚Â«ÃƒÅ¡ltimas compras (ERP)Ã‚Â» |
| **C** | **Folhas** (fornecedor/categoria/unidade) | RelatÃƒÂ³rios planilha Ã¢â‚¬â€ vendas Postgres + ÃƒÂºltima compra Entrada NF | **Ã¢Å“â€¦ Renan 25/06** v2.87 Ã¢â‚¬â€ categoria Ã‚Â«testeÃ‚Â» Ã‚Â· GM9503 Ã‚Â· ÃƒÂºlt. compra **1** Ã‚Â· mÃƒÂ©dia/sem **10** |
| **Fora hoje** | GM `gm0050` | Motor busca Ã¢â‚¬â€ **ÃƒÂºltimo** | Ã¢â‚¬â€ |

### RETESTE Ã¢â‚¬â€ Compras v2.85 (Renan 25/06, prints GM9503 + milho)

| Produto | Resultado |
| ------- | --------- |
| **GM9503 Ã‚Â«testeÃ‚Â»** | Ã¢Å“â€¦ **Bloco B** Ã¢â‚¬â€ chips **Teste R$ 1,00** + **Sn - Europet R$ 7,99** Ã‚Â· Ã¢Å“â€¦ **A** Ã¢â‚¬â€ sug. **20** Ã‚Â· S3=17 Ã‚Â· S4=2 Ã‚Â· mÃƒÂ©dia **0,63/dia** Ã‚Â· saldo **-1** |
| **GM9503 observaÃƒÂ§ÃƒÂ£o** | Custo **lista R$ 0** vs **base R$ 1,00** no detalhe Ã¢â‚¬â€ cosmÃƒÂ©tico (jÃƒÂ¡ anotado) |
| **GM0090-47 milho** | Ã¢Å“â€¦ **Esperado staging** Ã¢â‚¬â€ grÃƒÂ¡fico/mÃƒÂ©dia **zerados** (snapshot **nÃƒÂ£o** copia vendas da loja) Ã‚Â· saldo **30** (C9/V21) OK Ã‚Â· Ã‚Â«ÃƒÅ¡ltimas comprasÃ‚Â» vazio = sem Entrada NF desse produto **no teste** |
| **Bloco C folhas** | Ã¢Å“â€¦ **v2.87** Ã¢â‚¬â€ planilha categoria Ã‚Â«testeÃ‚Â» Ã‚Â· 1 produto Ã‚Â· ÃƒÂºlt. pedido **1** Ã‚Â· vendida desde ÃƒÂºlt. **0** Ã‚Â· mÃƒÂ©dia/sem **10** |

### FECHADO Ã¢â‚¬â€ Compras D4 bloco C Ã‚Â· folha categoria (Renan 25/06, v2.87)

| Item | Detalhe |
| ---- | ------- |
| **Commits** | `5e23a07` mÃƒÂ©tricas Postgres na planilha Ã‚Â· `6b95614` filtro categoria overlay |
| **Bug corrigido** | Categoria sÃƒÂ³ no overlay GestÃƒÂ£o Ã¢â€ â€™ relatÃƒÂ³rio vazio Ã‚Â· merge overlay igual **unidade** |
| **Teste OK** | Categoria **teste** Ã¢â€ â€™ **1 produto** Ã‚Â«testeÃ‚Â» (GM9503) Ã‚Â· colunas preenchidas |
| **NÃƒÂ£o testado** | Folhas **fornecedor** / **unidade** Ã¢â‚¬â€ mesma base; retestar se usar |

### FIX Ã¢â‚¬â€ Folha Compras categoria overlay (histÃƒÂ³rico v2.87)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Planilha **categoria Ã‚Â«testeÃ‚Â»** Ã¢â€ â€™ Ã‚Â«Nenhum produto encontradoÃ¢â‚¬Â¦Ã‚Â» Ã¢â‚¬â€ GM9503 visÃƒÂ­vel na **GestÃƒÂ£o** com categoria teste |
| **Causa** | Dropdown jÃƒÂ¡ misturava overlay + Mongo; **filtro do relatÃƒÂ³rio** sÃƒÂ³ lia ``NomeCategoria/Categoria/Grupo`` no Mongo |
| **Fix** | ``_lista_produto_ids_catalogo_por_categoria`` Ã¢â‚¬â€ mesmo padrÃƒÂ£o da **unidade**: overlay ``ProdutoGestaoOverlayAgro.categoria`` + merge Mongo |
| **Reteste** | Ã¢Å“â€¦ Renan 25/06 18:09 Ã¢â‚¬â€ planilha A4 impressÃƒÂ£o OK |

**JÃƒÂ¡ OK Compras (nÃƒÂ£o mexer):** busca nome Ã‚Â· custo Ã‚Â· saldo ledger Ã‚Â· carrinho/pedido.

### AGENDADO Ã¢â‚¬â€ Deploy loja Ã‚Â· pacote desvinculaÃƒÂ§ÃƒÂ£o (Renan 25/06, apÃƒÂ³s fechar)

| Item | Detalhe |
| ---- | ------- |
| **Quando** | **Ã¢Å“â€¦ 25/06 loja fechou** Ã¢â‚¬â€ Renan pediu deploy (aguardando senha no chat) |
| **O quÃƒÂª** | Merge `teste`Ã¢â€ â€™`producao` Ã¢â‚¬â€ pacote validado + Compras D4 (Renan Ã¢Å“â€¦) |
| **CÃƒÂ³digo** | `agro_pg` + PDV catÃƒÂ¡logo Postgres + gestÃƒÂ£o PG + ledger estoque + Entrada NF (empresas + wizard; financeiro **real** na loja, nÃƒÂ£o dry-run) |
| **Env loja (conferir Render)** | Ver tabela **Ã‚Â«Render produÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ envÃ‚Â»** abaixo |
| **Antes** | `importar_catalogo_mongo_produto` na loja se Postgres catÃƒÂ¡logo incompleto Ã‚Â· backup Ã‚Â· Renan confirma com **frase + senha** no chat do deploy |
| **Depois (Renan)** | Ctrl+F5 Ã‚Â· PDV busca/preÃƒÂ§o Ã‚Â· Compras saldo Ã‚Â· Entrada NF passo 5 empresa Ã‚Â· conferir 2Ã¢â‚¬â€œ3 produtos anotados (ex. GM9503) |
| **Fora do pacote (impacto loja)** | Motor busca **sem flags PG** Ã¢â€°Ë† legado Ã‚Â· LanÃƒÂ§amentos **telas Mongo** Ã‚Â· BI |

### FECHADO Ã¢â‚¬â€ Compras D4 bloco A Ã‚Â· mÃƒÂ©dia/sugestÃƒÂ£o Postgres (Renan 25/06, v2.79)

| Item | Detalhe |
| ---- | ------- |
| **Commit** | `b37c49e` Ã¢â‚¬â€ `compras_metricas_util.py` Ã‚Â· flag `AGRO_COMPRAS_METRICAS_POSTGRES` (default = Fase B/C) |
| **Teste OK** | GM9503 Ã‚Â«testeÃ‚Â» Ã¢â‚¬â€ grÃƒÂ¡fico S3/S4, mÃƒÂ©dia **0,63/dia**, sugestÃƒÂ£o **21** (vendas feitas **no Render teste**) |
| **Esperado no teste** | Produto **real** (ex. milho GM0090-47) Ã¢â€ â€™ **zerado** Ã¢â‚¬â€ snapshot copia catÃƒÂ¡logo/estoque, **nÃƒÂ£o** `VendaAgro` da loja |
| **Antes (Mongo)** | Staging lia **DtoVenda** no Mongo **compartilhado** Ã¢â€ â€™ milho mostrava vendas da loja; **nÃƒÂ£o** ÃƒÂ© regressÃƒÂ£o do bloco A |
| **Na loja (pÃƒÂ³s-D4)** | Mesmo cÃƒÂ³digo lÃƒÂª **VendaAgro Postgres da loja** Ã¢â€ â€™ produtos reais devem mostrar mÃƒÂ©dia/sugestÃƒÂ£o como hoje |
| **Ainda vazio** | ~~Ã‚Â«Sem histÃƒÂ³ricoÃ¢â‚¬Â¦Ã‚Â» bloco B~~ Ã¢â€ â€™ **fechado v2.84** (GM9503) |
| **Melhoria opcional** | Copiar vendas recentes no snapshot staging Ã¢â‚¬â€ sÃƒÂ³ se quiser paridade visual antes do deploy loja |

### FIX Ã¢â‚¬â€ Compras bloco B ÃƒÂºltimas compras + saldo cache (2026-06-25, v2.82)

| Item | Detalhe |
| ---- | ------- |
| **Bloco B bug** | GM9503 pÃƒÂ³s-Entrada NF nÃƒÂ£o mostrava chips Ã¢â‚¬â€ (1) query Mongo ID Ã‚Â· (2) **staging manual:** busca **nÃƒÂ£o chamava** ``/api/buscar/?compras=1`` Ã¢â€ â€™ ``ultimas_compras`` nunca vinha Ã‚Â· **fix v2.83** |
| **Saldo -3 vs -2** | CatÃƒÂ¡logo em **localStorage** guardava saldo **antigo** (-3); botÃƒÂ£o verde aplicava -2 **sÃƒÂ³ na memÃƒÂ³ria** Ã‚Â· **fix:** salvar saldos no cache apÃƒÂ³s sync + buscar saldos ao abrir a tela |
| **Teste Renan** | Saldo cache OK v2.82 Ã‚Â· chips **Ã¢Å“â€¦ v2.84** (ver FECHADO abaixo) |

### FECHADO Ã¢â‚¬â€ Compras D4 bloco B Ã‚Â· ÃƒÂºltimas compras Entrada NF (Renan 25/06, v2.84)

| Item | Detalhe |
| ---- | ------- |
| **Commit** | `05287c2` Ã¢â‚¬â€ match linha NF por **cÃƒÂ³digo GM** (`c_prod`) Ã‚Â· Entrada NF Agro **antes** ERP Ã‚Â· ``skip_erp`` staging Postgres |
| **Produto teste** | **GM9503** Ã‚Â«testeÃ‚Â» Ã¢â‚¬â€ faixa **Ã‚Â«ÃƒÅ¡ltimas compras (ERP) Ã‚Â· fornecedor e unitÃƒÂ¡rioÃ¢â‚¬Â¦Ã‚Â»** visÃƒÂ­vel (origem ``entrada_nf_agro``) |
| **TambÃƒÂ©m OK** | SugestÃƒÂ£o **20** Ã‚Â· grÃƒÂ¡fico S3=17 Ã‚Â· S4=2 Ã‚Â· saldo **-1** (lista; era -2 Ã¢â‚¬â€ conferir venda/ajuste no teste) |
| **ObservaÃƒÂ§ÃƒÂ£o** | Coluna **CUSTO** na lista **R$ 0,00** vs **custo base R$ 1,00** no detalhe Ã¢â‚¬â€ cosmÃƒÂ©tico; nÃƒÂ£o bloqueia |
| **Texto tela** | RÃƒÂ³tulo ainda diz Ã‚Â«(ERP)Ã‚Â» Ã¢â‚¬â€ trocar depois do corte ERP por Ã‚Â«Entrada NF AgroÃ‚Â» |
| **GM PAI / outros** | Sem Entrada NF concluÃƒÂ­da Ã¢â€ â€™ vazio ÃƒÂ© **esperado** |

### RETESTE Ã¢â‚¬â€ Compras v2.83 (Renan 25/06, prints Ã¢â‚¬â€ histÃƒÂ³rico)

| Item | Resultado |
| ---- | --------- |
| **Saldo GM9503** | Ã¢Å“â€¦ **-2** persiste apÃƒÂ³s botÃƒÂ£o verde (v2.82) |
| **MÃƒÂ©dia / sugestÃƒÂ£o** | Ã¢Å“â€¦ S3=17 Ã‚Â· S4=2 Ã‚Â· sugestÃƒÂ£o **21** (bloco A) |
| **ÃƒÅ¡ltimas compras (chips)** | Ã¢ÂÅ’ ainda Ã‚Â«Sem histÃƒÂ³ricoÃ¢â‚¬Â¦Ã‚Â» Ã¢â‚¬â€ Entrada NF **ConcluÃƒÂ­da** no wizard |
| **Causa v2.83** | Fix frontend (busca chama API) **funcionou**; backend sÃƒÂ³ casava ``produto_id`` na linha NF, nÃƒÂ£o **GM9503** em ``c_prod`` |
| **Fix v2.84** | Match por cÃƒÂ³digo GM Ã‚Â· Entrada NF Agro **antes** do ERP Ã‚Â· ``skip_erp`` no staging Ã‚Â· import ``Decimal`` |
| **Resultado v2.84** | Ã¢Å“â€¦ GM9503 Ã¢â‚¬â€ ver **FECHADO bloco B** acima |

### FECHADO Ã¢â‚¬â€ Entrada NF wizard + saldo ledger (Renan 25/06, v2.78)

| Item | Detalhe |
| ---- | ------- |
| **Fluxo** | Manual Ã¢â€ â€™ produtos (nome) Ã¢â€ â€™ estoque Agro Ã¢â€ â€™ financeiro **dry-run** Ã¢â€ â€™ **PIN** finalizar |
| **Saldo** | GM9503 Ã‚Â«testeÃ‚Â» **-2** Ã¢â‚¬â€ **Consulta/orÃƒÂ§amento** (Centro ÃŽÂ£) = **Compras** (coluna Saldo) |
| **Staging** | Financeiro simulado no rascunho; estoque via ledger Postgres (`AjusteRapidoEstoque`) |

### BUG Ã¢â‚¬â€ Entrada NF passo 5 sem empresa (2026-06-25)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Dropdown Ã‚Â«Empresa (estoque)Ã‚Â» sÃƒÂ³ Ã‚Â«Cadastre empresas no AdminÃ‚Â» |
| **Causa** | Lista vem de ``base.Empresa`` Postgres; **snapshot nÃƒÂ£o copiava** Empresa/Loja Ã¢â€ â€™ staging vazio |
| **Fix** | ``listar_empresas_estoque_entrada_nfe()`` Ã¢â‚¬â€ sync loja se staging vazio + seed CNPJ GM Ã‚Â· snapshot inclui Empresa+Loja Ã‚Â· default Ã‚Â«Agro Mais CentroÃ‚Â» |
| **Teste** | Recarregar ``/entrada-nota/`` passo 5 Ã¢â‚¬â€ ver Centro + Vila Elias (ou rodar ``copiar_snapshot_pdv_loja``) |

### DECISÃƒÆ’O Ã¢â‚¬â€ motor de busca ÃƒÂºnico (Renan 2026-06-25)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Compras/NF ainda erram GM (`gm0050` Ã¢â€ â€™ ibiuna); **cadastro + PDV OK**. Patches por tela nÃƒÂ£o bastam Ã¢â‚¬â€ histÃƒÂ³rico de Ã¢â‚¬Å“quase copiar PDVÃ¢â‚¬Â no cadastro levou meses atÃƒÂ© copiar de verdade. |
| **DecisÃƒÂ£o** | **Um buscador sÃƒÂ³** (background): alteraÃƒÂ§ÃƒÂ£o nele Ã¢â€ â€™ **todas** as telas ligadas buscam igual. |
| **Alvo** | **Servidor:** `catalogo_agro.buscar` + `/api/buscar/` (modos `compras`, `wizard` = parÃƒÂ¢metros, mesma lÃƒÂ³gica). **Cliente:** mÃƒÂ³dulo ÃƒÂºnico `agro_busca_produto.js` Ã¢â‚¬â€ sanitizar Ã¢â€ â€™ GM/CB Ã¢â€ â€™ local opcional Ã¢â€ â€™ API Ã¢â€ â€™ merge Ã¢â€ â€™ ordenar; telas sÃƒÂ³ **renderizam** o retorno. |
| **Hoje (legado)** | `_js_busca_produto_inteligente.html` = filtro local **parcial**; cada tela tem merge/cache prÃƒÂ³prio (`consulta_produtos.js`, `compras.html`, `entrada_nota.html`, `cadastro_erp_panel.js`). |
| **Ordem migraÃƒÂ§ÃƒÂ£o** | 1) Extrair pipeline do PDV (`executarBuscaLocal` + merge) para mÃƒÂ³dulo Ã‚Â· 2) PDV usa mÃƒÂ³dulo Ã‚Â· 3) Cadastro (API-only) Ã‚Â· 4) Compras Ã‚Â· 5) Entrada NF Ã‚Â· 6) demais (transferÃƒÂªncias, ajuste mobileÃ¢â‚¬Â¦). |
| **Regra** | **Proibido** novo `filtrar*` / merge custom por tela Ã¢â‚¬â€ sÃƒÂ³ opÃƒÂ§ÃƒÂµes do motor (`compras:1`, `limite`, `cacheRef`). |
| **Status** | **Ã¢Å“â€¦ Renan 25/06** v2.93 Ã¢â‚¬â€ legado=v2 na tela teste Ã‚Â· **pendente:** ordem API Ã¢â€°Ë† PDV (ex. Ã‚Â«milhoÃ‚Â») Ã‚Â· ligar Compras/NF na API unificada |
| **Nota Renan 25/06** | Motor **Ã¢â€°Â ** cortar Mongo/ERP Ã¢â‚¬â€ ÃƒÂ© paridade GM Compras/NF; **deixar por ÃƒÂºltimo** |

**Flags staging (jÃƒÂ¡ ligadas):** `AGRO_FONTE_CATALOGO=agro_pg` Ã‚Â· `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` Ã‚Â· `AGRO_SNAPSHOT_FONTE_DATABASE_URL` Ã‚Â· conferir `GET /api/agro/fonte-status/`.

### RETESTE Ã¢â‚¬â€ Motor busca v2.93 (Renan 25/06, prints)

| Busca | Legado vs v2 | PDV teste | Nota |
| ----- | ------------ | --------- | ---- |
| **GM9503 / akiles / ra est car 15** | Ã¢Å“â€¦ **igual** (pÃƒÂ³s v2.92) | Ã¢Å“â€¦ bom | Sem bifinho/capa lixo |
| **milho** | Ã¢Å“â€¦ **18 = 18** legado v2 | Ã¢Å“â€¦ **9 visÃƒÂ­veis** + carregar mais | API traz farelo/isca antes; PDV prioriza nome Ã‚Â«milhoÃ¢â‚¬Â¦Ã‚Â» no topo Ã¢â‚¬â€ **cosmÃƒÂ©tico ordem** |
| **PrÃƒÂ³ximo** | Ã¢â‚¬â€ | Ã¢â‚¬â€ | Afinar sort nome-exato Ã‚Â· depois Compras/NF usarem sÃƒÂ³ API unificada |

### RESOLVIDO Ã¢â‚¬â€ fantasmas ibiuna 25 kg + auditoria (2026-06-24)

| Item | Status |
| ---- | ------ |
| **Teste** | CÃƒÂ³digo OK (busca, exibiÃƒÂ§ÃƒÂ£o, auditoria, import). Staging: **1 grave** (`Ã¢â‚¬Â¦d31` GM=Id) Ã¢â€ â€™ `corrigir_produto_nome_objectid_pg` ou esperar API v2.50+ na lista |
| **batom** (`Ã¢â‚¬Â¦d267`) | **Ignorar** Ã¢â‚¬â€ higiene sÃƒÂ³; Renan: nÃƒÂ£o importante |
| **ProduÃƒÂ§ÃƒÂ£o** | Mesmo padrÃƒÂ£o dos 3 ibiuna possÃƒÂ­vel Ã¢â‚¬â€ **sem urgÃƒÂªncia** se PDV/busca OK; quando quiser: `auditar` + `corrigir` no Shell **ou** deploy cherry-pick testeÃ¢â€ â€™loja |
| **Seguir a vida?** | **Sim** no teste Ã‚Â· loja: sÃƒÂ³ validar `ibiuna 25` / `gm1546` no cadastro e PDV |

### Fantasmas cadastro Ã¢â‚¬â€ prevenÃƒÂ§ÃƒÂ£o + auditoria (2026-06-24)

| Pergunta | Resposta |
| -------- | -------- |
| **SÃƒÂ³ 3 itens?** | `auditar_produtos_fantasma_pg` no agro-db Ã¢â‚¬â€ **staging 2026-06-24:** **1 grave** (Ã¢â‚¬Â¦`d31` ibiuna inicial, GM=Id) + **batom** era falso positivo (v2.52 nÃƒÂ£o conta) Ã‚Â· `--higiene` lista codigo_interno=Id |
| **Os outros 2 ibiuna?** | No staging snapshot **jÃƒÂ¡ tinham nome+GM no Postgres** (sÃƒÂ³ Ã¢â‚¬Â¦`d31` com `codigo_nfe` = Id Mongo) Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o pode diferir; rodar o mesmo comando na loja |
| **Evitar** | Import MongoÃ¢â€ â€™PG pula fantasma; cadastro novo no Agro |
| **Corrigir** | `corrigir_produto_nome_objectid_pg --dry-run` Ã¢â€ â€™ sem `--dry-run` |

**Auditoria staging (print Renan):**

```
Fantasmas graves: 1   # apÃƒÂ³s v2.52; antes eram 2 (batom incluso)
Ã¢â‚¬Â¦d31 | ibiuna inicial 25 kg | gm_pg=69937Ã¢â‚¬Â¦ | Ã¢â€ â€™ GM1542-25 IBIUNA
batom Ã¢â‚¬Â¦d267 | nome OK gm=1 | sÃƒÂ³ higiene (--higiene)
```

### FECHADO Ã¢â‚¬â€ 3Ãƒâ€” ibiuna 25 kg (fantasma Mongo, 2026-06-24)

| Item | Detalhe |
| ---- | ------- |
| **Quem corrigiu nome** | **CÃƒÂ³digo teste v2.48+** (`catalogo_nome_util`) Ã¢â‚¬â€ exibiÃƒÂ§ÃƒÂ£o na API, **nÃƒÂ£o** ediÃƒÂ§ÃƒÂ£o manual Renan |
| **Pendente** | Marca/categoria/cÃƒÂ³digos ainda vazios no Postgres Ã¢â‚¬â€ **v2.50** infere dos irmÃƒÂ£os GM (5 kg/1 kg) + comando `corrigir_produto_nome_objectid_pg` grava no banco |
| **Escopo** | SÃƒÂ³ **3 fantasmas** (Id Mongo 24 hex, nome Ã‚Â«Ã¢â‚¬â€Ã‚Â»); resto do catÃƒÂ¡logo OK |
| **ProduÃƒÂ§ÃƒÂ£o** | Mesmos 3 no agro-db Ã¢â‚¬â€ manual no lÃƒÂ¡pis **ou** deploy + Shell comando |

### URGENTE Ã¢â‚¬â€ 3Ãƒâ€” ibiuna 25 kg nome = Id Mongo (2026-06-24, histÃƒÂ³rico)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cadastro pÃƒÂ¡g.1: 3 linhas `69937d94Ã¢â‚¬Â¦` R$ 84/86/82; busca `ibiuna` falta postura/crescimento/inicial **25 kg** |
| **Causa** | Postgres `Produto.nome` = **ObjectId** (fantasma import Mongo sem Nome) Ã¢â‚¬â€ preÃƒÂ§os batem com as 3 variantes 25 kg |
| **Fix teste v2.46** | `catalogo_nome_util` resolve nome/GM pelos irmÃƒÂ£os GM Ã‚Â· busca inclui fantasmas Ã‚Â· comando `corrigir_produto_nome_objectid_pg` |
| **ProduÃƒÂ§ÃƒÂ£o** | Mesmo dado corrompido no agro-db Ã¢â‚¬â€ deploy cherry-pick + **Render Shell:** `python manage.py corrigir_produto_nome_objectid_pg` (ou frase+senha push `producao`) |
| **IDs** | `Ã¢â‚¬Â¦d22` inicial Ã‚Â· `Ã¢â‚¬Â¦d31` crescimento Ã‚Â· `Ã¢â‚¬Â¦d49` postura (confirmar na loja) |

### URGENTE Ã¢â‚¬â€ gestÃƒÂ£o cadastro sumiu produtos / busca GM incompleta (2026-06-24)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cadastro/gestÃƒÂ£o: `gm1546` sÃƒÂ³ 1 item; `ibi pos` zero; PDV `gm1546` falta 25 kg; fantasma Mongo sem nome |
| **Causa** | `catalogo_agro.buscar()` com `agro_pg`: **overlay** devolvia 1 PID e **parava** Ã¢â‚¬â€ nÃƒÂ£o buscava demais variantes GM no Postgres; texto multi-palavra (`ibi pos`) nÃƒÂ£o casava `nome__icontains` literal |
| **ProduÃƒÂ§ÃƒÂ£o** | **Mesmo bug de cÃƒÂ³digo** jÃƒÂ¡ em v2.27 com `agro_pg` Ã¢â‚¬â€ **nÃƒÂ£o** foi deploy v2.43/44 na loja; precisa cherry-pick deste fix + frase+senha |
| **Fix (teste v2.45)** | `buscar()` uniÃƒÂ£o overlay + `q_icontains` + tokens AND Ã‚Â· PDV: oculta fantasma sem nome apÃƒÂ³s enriquecimento |
| **Arquivos** | `catalogo_agro.py` Ã‚Â· `views.py` (`api_buscar_produtos`) |
| **Teste Renan** | Ctrl+F5 cadastro Ã¢â€ â€™ `gm1546` (4 variantes) Ã‚Â· `ibi pos` (postura) Ã‚Â· PDV sem linha Ã‚Â«Ã¢â‚¬â€Ã‚Â» |

### URGENTE Ã¢â‚¬â€ PDV produÃƒÂ§ÃƒÂ£o produto sem nome / GM estranho (2026-06-24)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Busca GM (ex. postura 25 kg) acha item com nome **Ã¢â‚¬â€**, cÃƒÂ³digo tipo `94a42b90be60`, preÃƒÂ§o certo; gestÃƒÂ£o mostra produto OK |
| **Causa** | Cadastro jÃƒÂ¡ em **Postgres** (`AGRO_FONTE_CATALOGO=agro_pg` na loja); PDV ainda lÃƒÂª **fantasma Mongo** (mesmo GM, sem Nome). Dois IDs para o mesmo cÃƒÂ³digo |
| **Fix (teste v2.43)** | Commit `3901a12` Ã¢â‚¬â€ deploy Render teste |
| **Loja** | **Precisa deploy** Ã¢â‚¬â€ frase + senha `99738595` quando validar no teste |
| **Teste Renan** | Ctrl+F5 PDV teste Ã¢â€ â€™ `GM1546-25` ou `ibiuna postura 25` Ã¢â€ â€™ nome + GM corretos |

### Fix busca cÃƒÂ³digo GM Ã¢â‚¬â€ Compras + Entrada NF (2026-06-24, WIP teste)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `gm9503` / `GM0050` **nÃƒÂ£o filtram** em Compras e Entrada NF; por nome (`teste`) funciona; PDV/Cadastro OK |
| **Causa (Renan validou preÃƒÂ§os D2)** | Busca GM caÃƒÂ­a em **dÃƒÂ­gitos parciais** (`9503`/`0050` em barras) no Postgres e no cache local; filtro JS **seguia** apÃƒÂ³s GM vazio e misturava catÃƒÂ¡logo inteiro |
| **Fix v2.55Ã¢â‚¬â€œv2.57** | **Raiz:** `termo_eh_codigo_gm` + motor `_js_busca_produto_inteligente` (mesmo do PDV) Ã‚Â· **v2.57:** Compras/NF usam `mesclarBuscaPdvLocalComApi` + atalho GM sem local Ã¢â€ â€™ sÃƒÂ³ API (copiado de `consulta_produtos.js`) Ã‚Â· removido `filtrarLocaisCodigoGmExato` (camada extra) |
| **PreÃƒÂ§o Ã‚Â«testeÃ‚Â» GM9503** | Compras = **custo** R$ 1,00; PDV/NF = **venda** R$ 1,09 Ã¢â‚¬â€ esperado |
| **Arquivos** | `cadastro_busca_codigo_util.py` Ã‚Â· `catalogo_agro.py` Ã‚Â· `_js_busca_produto_inteligente.html` Ã‚Â· `compras.html` Ã‚Â· `entrada_nota.html` |
| **Deploy teste** | **Live `f538815`** (manual deploy apÃƒÂ³s spend limit **$5**) Ã¢â‚¬â€ inclui fix GM `85e21c8`/`312e1ca` |
| **Teste Renan** | Ctrl+F5 Ã¢â€ â€™ Compras + Entrada NF Ã¢â€ â€™ `gm9503`, `GM9503`, `gm0050` |

### Renan Ã¢â‚¬â€ cancelar assinatura ERP (2026-06-24, planejamento)

| O quÃƒÂª | Detalhe |
| ----- | ------- |
| **DecisÃƒÂ£o** | Renan vai **cancelar a assinatura do ERP** (fornecedor legado / espelho Mongo) |
| **SisVale nÃƒÂ£o some** | Render + Postgres Agro continuam Ã¢â‚¬â€ PDV vendas, NFC-e, caixa, RH, clientes jÃƒÂ¡ sÃƒÂ£o Agro |
| **Risco se cancelar cedo** | **Mongo para de atualizar** Ã¢â€ â€™ catÃƒÂ¡logo/estoque/financeiro/BI congelam na loja (produÃƒÂ§ÃƒÂ£o ainda lÃƒÂª espelho) |
| **Antes de cancelar** | Backups no PC (lista Ã‚Â§ abaixo) + checkpoint LanÃƒÂ§amentos + acelerar migraÃƒÂ§ÃƒÂ£o Ã‚Â§4.15 (cadastro/PDV/financeiro) |
| **Ordem segura** | 1) baixar backups Ã‚Â· 2) checkpoint `/lancamentos/` Ã‚Â· 3) deploy corte AgroÃ¢â€ â€™ERP Ã‚Â· 4) **sÃƒÂ³ entÃƒÂ£o** avisar ERP / cancelar assinatura |

**Backups Excel/CSV no PC (prioridade):** LanÃƒÂ§amentos backup todos + em aberto (ZIP) Ã‚Â· Cadastro produtos Excel Ã¢â€ â€œ (todas colunas/categorias) Ã‚Â· Vendas CSV Ã‚Â· Contabilidade Excel + ZIP XML NFC-e Ã‚Â· Fiado (JSON ou CSV dentro do ZIP LanÃƒÂ§amentos) Ã‚Â· export LanÃƒÂ§amentos por perÃƒÂ­odo se quiser recorte.


### Fix NFC-e Ã¢â‚¬â€ cupom nÃƒÂ£o emitiu na 1Ã‚Âª tentativa (2026-06-24, teste v2.34)

| Item | Valor |
| ---- | ----- |
| **Sintoma** | Contabilidade: muitas pendÃƒÂªncias Ã‚Â«Erro tÃƒÂ©cnicoÃ‚Â» Ã‚Â· motivo Ã‚Â«NFC-e nÃƒÂ£o configurada no servidor (.env)Ã‚Â» Ã‚Â· reemitir manual funciona |
| **Causa** | Cold start Render (cert .pfx temporÃƒÂ¡rio) + PDV perdeu fluxo Ã‚Â«sem identificaÃƒÂ§ÃƒÂ£oÃ‚Â» em PIX/cartÃƒÂ£o |
| **Fix** | Warmup/retry certificado Ã‚Â· retry em background (2/5/10 s) antes de gravar ERRO Ã‚Â· PIX/cartÃƒÂ£o sem CPF Ã¢â€ â€™ sem identificaÃƒÂ§ÃƒÂ£o (PDV + servidor) Ã‚Â· `nfce_solicitada` sem exigir config no momento da venda |
| **Arquivos** | `nfce_config_util.py` Ã‚Â· `views_nfce.py` Ã‚Â· `views.py` Ã‚Â· `pdv_wizard.js` Ã‚Â· `nfce_sp_emissao_util.py` |
| **PendÃƒÂªncias antigas (jun/22)** | Reemitir em Consultar vendas ou aguardar comando em lote (futuro) Ã¢â‚¬â€ erros de antes do go-live NFC-e |

**Renan Ã¢â‚¬â€ apÃƒÂ³s deploy teste:** Ctrl+F5 PDV Ã¢â€ â€™ venda PIX/cartÃƒÂ£o Ã¢â€ â€™ cupom deve sair sem aviso amarelo Ã‚Â· se servidor acordar lento, retry automÃƒÂ¡tico em ~2Ã¢â‚¬â€œ17 s (sem ERRO falso).

**105 erros jun/2026 na loja:** maioria provavelmente **22/06** (antes do go-live 23/06). Reemitir na consulta de vendas limpa uma a uma.

### Contabilidade Ã¢â‚¬â€ layout + pendÃƒÂªncias NFC-e Ã¢â€ â€™ **produÃƒÂ§ÃƒÂ£o OK** (2026-06-24, Renan + senha)

| Item | Valor |
| ---- | ----- |
| **Pacote** | Layout desktop compacto + lista NFC-e rejeitada/erro + CSV |
| **Commits teste** | `64dc9fa` Ã‚Â· `848b562` |
| **Commits loja** | `899ba8a` Ã‚Â· `1434ffd` Ã‚Â· VERSION `731607c` |
| **VERSION loja** | **v2.27** |

**Renan Ã¢â‚¬â€ apÃƒÂ³s deploy Render:** Ctrl+F5 Ã¢â€ â€™ `/contabilidade/` Ã¢â€ â€™ resumo/export lado a lado Ã‚Â· pendÃƒÂªncias NFC-e Ã‚Â· CSV.

**Reverter:** revert `731607c` + `1434ffd` + `899ba8a` (ordem inversa).

### Contabilidade Ã¢â‚¬â€ login contador Ã¢â€ â€™ **produÃƒÂ§ÃƒÂ£o OK** (2026-06-24, Renan + senha)

| Item | Valor |
| ---- | ----- |
| **Fix** | `/contabilidade/login/` Ã¢â‚¬â€ contador entra **sem** staff do Admin |
| **Commit teste** | `2d77b88` |
| **Commits loja** | `e98cc13` Ã‚Â· VERSION `e51e810` |
| **VERSION loja** | **v2.26** |
| **Env** | `AGRO_CONTABILIDADE_USERNAMES=martins` Ã‚Â· usuÃƒÂ¡rio Admin **sem** marcar staff |

**Renan Ã¢â‚¬â€ apÃƒÂ³s deploy Render:** Ctrl+F5 Ã¢â€ â€™ `/contabilidade/login/` Ã¢â€ â€™ login contador.

**Reverter:** revert `e51e810` + `e98cc13` (ordem inversa).

### Rule Cursor Ã¢â‚¬â€ leitura integral banana (2026-06-24, Renan)

| O quÃƒÂª | Detalhe |
| ----- | ------- |
| **Arquivo** | `.cursor/rules/agro-consulta.mdc` **Ã‚Â§0** |
| **Regra** | **1Ã‚Âª aÃƒÂ§ÃƒÂ£o todo chat:** `Read` em `banana.md` **inteiro** (sem limit); reler se chat longo/resumo |
| **Motivo** | Contexto do chat some em minutos; banana = memÃƒÂ³ria fixa |

### Ã¢Å¡Â Ã¯Â¸Â ViolaÃƒÂ§ÃƒÂ£o protocolo produÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ Contabilidade (2026-06-24)

| O quÃƒÂª | Detalhe |
| ----- | ------- |
| **O que aconteceu** | Assistente fez cherry-pick + `git push origin producao` **sem** Renan digitar a senha **`99738595` no mesmo pedido** |
| **Pedido Renan** | Tinha frase (*Ã‚Â«suba cherry pick para produÃƒÂ§ÃƒÂ£oÃ‚Â»*) + bloco **copiado** do outro chat (*Ã‚Â«push sÃƒÂ³ com frase + senhaÃ‚Â»*) Ã¢â‚¬â€ **isso nÃƒÂ£o conta** como senha (regra topo banana) |
| **Regra violada** | Linhas 18Ã¢â‚¬â€œ21 banana: frase **e** senha digitada **por Renan** na **mesma mensagem** Ã¢â‚¬â€ senÃƒÂ£o **nÃƒÂ£o sobe** |
| **CÃƒÂ³digo na loja** | Pacote Contabilidade **jÃƒÂ¡ foi** para `producao` (commits abaixo) Ã¢â‚¬â€ Render pode estar deployando |
| **PrÃƒÂ³ximo deploy loja** | Assistente **para** e **pede** frase + senha; **nÃƒÂ£o** inferir senha do banana nem de texto de instruÃƒÂ§ÃƒÂ£o |

### Contabilidade Ã¢â‚¬â€ deploy loja (sem autorizaÃƒÂ§ÃƒÂ£o vÃƒÂ¡lida Ã¢â‚¬â€ histÃƒÂ³rico)

| Item | Valor |
| ---- | ----- |
| **Pacote** | 4 commits Contabilidade (sÃƒÂ³ NFC-e na tela) Ã¢â‚¬â€ **sem** PDV snapshot/Akiles |
| **Commits teste** | `a570cd0` Ã‚Â· `8523686` Ã‚Â· `be536b6` Ã‚Â· `81aa0eb` |
| **Commits loja** | `fce67a6` Ã‚Â· `db7ea29` Ã‚Â· `c694537` Ã‚Â· `4590ad1` Ã‚Â· VERSION `86d3af2` |
| **VERSION loja** | **v2.25** |
| **URL** | `/contabilidade/` |

**Render produÃƒÂ§ÃƒÂ£o apÃƒÂ³s deploy:** `AGRO_CONTABILIDADE_USERNAMES=martins` (username exato, vÃƒÂ­rgula se vÃƒÂ¡rios) Ã‚Â· usuÃƒÂ¡rio no Admin **sem** marcar staff.

**Login contador:** **`/contabilidade/login/`** Ã¢â‚¬â€ na loja desde **v2.26** (`2d77b88`).

**Renan Ã¢â‚¬â€ conferir na loja:** Ctrl+F5 Ã¢â€ â€™ `/contabilidade/` Ã¢â€ â€™ resumo mÃƒÂªs Ã‚Â· CSV/XLSX Ã‚Â· ZIP NFC-e Ã‚Â· sem FAB PDV Ã‚Â· sem Ã‚Â«Outros exportsÃ‚Â».

**Layout desktop (teste v2.31+):** largura total (96rem), resumo + botÃƒÂµes download **lado a lado**, barra perÃƒÂ­odo compacta, sem faixa roxa grande.

**PendÃƒÂªncias fiscais (teste v2.33+):** bloco Ã‚Â«PendÃƒÂªncias fiscaisÃ‚Â» recolhÃƒÂ­vel (lista rejeitada/erro + motivo SEFAZ) + **CSV pendÃƒÂªncias** (`/api/nfce/export-pendencias/`). AtÃƒÂ© 200 na tela; CSV traz todas.

**Reverter:** revert dos 5 commits em `producao` (ordem inversa, comeÃƒÂ§ando por `86d3af2`).


Renan confirmou **neste chat**: no **Render teste**, busca `akiles` bate com o **PDV produÃƒÂ§ÃƒÂ£o** (referÃƒÂªncia GM0060/61). **Fase A snapshot concluÃƒÂ­da** Ã¢â‚¬â€ nÃƒÂ£o mexer no fluxo de preÃƒÂ§o PDV atÃƒÂ© novo pedido.

**PDV teste lÃƒÂª dados da loja?** **Parcialmente Ã¢â‚¬â€ nÃƒÂ£o ÃƒÂ© ao vivo.** (1) **Postgres Agro** (preÃƒÂ§os overlay, ajustes estoque): **cÃƒÂ³pia** da loja feita no snapshot (`copiar_snapshot_pdv_loja`) Ã¢â‚¬â€ fica parada atÃƒÂ© rodar de novo. (2) **Mongo** (catÃƒÂ¡logo/espelho ERP): **mesmo banco** que a loja, staging **sÃƒÂ³ lÃƒÂª**. Na busca, o teste aplica o **overlay copiado** por cima do espelho Ã¢â€ â€™ preÃƒÂ§o igual loja (ex. Akiles). Mudou preÃƒÂ§o na loja hoje Ã¢â€ â€™ no teste sÃƒÂ³ atualiza apÃƒÂ³s **novo snapshot** (ou Fase B no futuro).

| GM | ProduÃƒÂ§ÃƒÂ£o (certo) | Teste (v2.22+) |
| -- | -------------- | -------------- |
| GM0060-15 | **R$ 70,00** | **R$ 70,00** Ã¢Å“â€¦ |
| GM0061-15 | **R$ 75,00** | **R$ 75,00** Ã¢Å“â€¦ |

**Como chegou aqui:** snapshot Postgres lojaÃ¢â€ â€™teste (v2.18) + fix cache busca staging (v2.22, commit `d24b1dd`). **NÃƒÂ£o** mexer no PDV preÃƒÂ§o atÃƒÂ© novo pedido.

### Fix PDV teste Ã¢â‚¬â€ cache local ignorava overlay (2026-06-24, v2.22)

Snapshot passo 2 **OK** (3354 produtos, 802 overlays, 4759 ajustes). Passo 3 **falhou**: busca `akiles` ainda mostrava R$ 65/70 (preÃƒÂ§o Mongo no **cache local** do navegador Ã¢â‚¬â€ nÃƒÂ£o ia ao servidor).

**Fix v2.22:** no **staging** (`AGRO_STAGING_READONLY`), busca por texto **sempre confere o servidor**; preÃƒÂ§o do servidor prevalece. Cache catÃƒÂ¡logo v9 (ignora sessionStorage antigo no teste).

**Renan Ã¢â‚¬â€ apÃƒÂ³s deploy v2.22:** Ctrl+F5 no PDV teste Ã¢â€ â€™ buscar `akiles` Ã¢â€ â€™ **R$ 70 / R$ 75**. Ã¢Å“â€¦ **Validado Renan 2026-06-24.**

**ValidaÃƒÂ§ÃƒÂ£o ampliada Fase A (Renan, 2026-06-24):** alÃƒÂ©m das Akiles, **+3 produtos** com preÃƒÂ§o alterado na loja Ã¢â‚¬â€ **OK** no teste. **PrÃƒÂ³ximo:** Fase B (catÃƒÂ¡logo PDV sem Mongo no staging).

### Fase B Ã¢â‚¬â€ PDV teste catÃƒÂ¡logo sÃƒÂ³ Postgres Ã¢Å“â€¦ (2026-06-24)

**Objetivo:** busca/catÃƒÂ¡logo do PDV no **Render teste** vÃƒÂªm do **Postgres copiado da loja** (snapshot), nÃƒÂ£o do espelho Mongo. Estoque/mÃƒÂ©dias podem continuar no Mongo. **ProduÃƒÂ§ÃƒÂ£o:** flag **sempre off**.

| Passo | Status |
| ----- | ------ |
| 1 Deploy cache v10 | Ã¢Å“â€¦ |
| 2 Env `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` | Ã¢Å“â€¦ |
| 3 `fonte-status` Ã¢â€ â€™ `pdv_catalogo_somente_postgres: true` | Ã¢Å“â€¦ |
| 4 PDV teste Ã¢â‚¬â€ preÃƒÂ§os OK (1Ã‚Âª busca mais lenta) | Ã¢Å“â€¦ Renan |
| 5 Sync lojaÃ¢â€ â€™teste: mudou preÃƒÂ§o na loja Ã¢â€ â€™ snapshot Ã¢â€ â€™ teste | Ã¢Å“â€¦ Renan |

**Snapshot pÃƒÂ³s-correÃƒÂ§ÃƒÂ£o URL (Renan):** `produtos=3357 overlays=806 ajustes=4772` Ã¢â‚¬â€ produto alterado na **loja** apareceu no **teste** apÃƒÂ³s `copiar_snapshot_pdv_loja`.

**Sync preÃƒÂ§o (regra operacional):** **nÃƒÂ£o ÃƒÂ© ao vivo.** Loja mudou hoje Ã¢â€ â€™ teste sÃƒÂ³ atualiza apÃƒÂ³s **novo snapshot** no Shell teste (ou cron HTTP). Rotina sugerida: snapshot quando for validar pacote grande ou apÃƒÂ³s mudanÃƒÂ§as de preÃƒÂ§o relevantes na loja.

**Armadilhas Environment teste (registrar):**
- `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES` Ã¢â‚¬â€ key exata (nÃƒÂ£o `pdv_catalogo_somente_postgres`)
- `AGRO_SNAPSHOT_FONTE_DATABASE_URL` = **Internal Database URL** do **agro-db** (SistVale) Ã¢â‚¬â€ **nÃƒÂ£o** `true` nem URL do `agro-staging`

### Fase C Ã¢â‚¬â€ GestÃƒÂ£o operacional Ã¢Å“â€¦ (Renan, 2026-06-24)

**Objetivo (sÃƒÂ³ teste):** tela **GestÃƒÂ£o de produtos** lista/busca/filtros via **Postgres** (flag `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true`). Saldos = Mongo + ajustes PIN.

| # | Status |
| - | ------ |
| C1 Lista + busca Postgres | Ã¢Å“â€¦ Renan |
| C2 Facetas Postgres | Ã¢Å“â€¦ Renan |
| C3 GestÃƒÂ£o (lista/filtros/busca) | Ã¢Å“â€¦ Renan Ã¢â‚¬â€ Ã‚Â«parece tudo okÃ‚Â» |

**Entrega:** commit `d49e3b0` Ã‚Â· teste **v2.40+** Ã‚Â· `gestao_somente_postgres: true` no `fonte-status`.

**ProduÃƒÂ§ÃƒÂ£o (loja fechada):** frase + senha Ã¢â‚¬â€ **nÃƒÂ£o** sobe flags Fase B/C nem snapshot. PDV/gestÃƒÂ£o loja = Mongo+overlay atÃƒÂ© novo pacote combinado.

**Renan (2026-06-25):** loja **quase nÃƒÂ£o usa** `/produtos/gestao/` (ideias futuras). D1 OK: saldo GestÃƒÂ£o=Compras Ã‚Â· preÃƒÂ§o venda sync. Ajuste estoque na gestÃƒÂ£o: **PIN nÃƒÂ£o abre no Render teste** Ã¢â‚¬â€ validar ajuste na **loja** ou corrigir PIN staging depois.

### Fase D Ã¢â‚¬â€ Estoque ledger + busca NF/Compras (2026-06-24)

**Objetivo (sÃƒÂ³ teste, sem env novo):** com `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` jÃƒÂ¡ ligado:

| # | Entrega |
| - | ------- |
| D1 | **Ledger v1** Ã¢â‚¬â€ saldo PDV/gestÃƒÂ£o = `saldo_informado` do ajuste (snapshot), sem recalcular delta Mongo |
| D2 | **Entrada NF + Compras** Ã¢â‚¬â€ busca produto (`?compras=1`) via Postgres (mesma base PDV) |

**Conferir:** `fonte-status` Ã¢â€ â€™ `estoque_ledger_ativo: true` Ã‚Â· PDV/gestÃƒÂ£o saldo Ã‚Â· Compras busca Ã‚Â· Entrada NF passo produtos.

**Renan testa apÃƒÂ³s deploy** Ã¢â‚¬â€ pendente.

### Fix snapshot PDV Ã¢â‚¬â€ TIME_ZONE Django 6 (2026-06-24, v2.21)

Renan rodou passo 2 Ã¢â€ â€™ erro `ConexÃƒÂ£o fonte falhou: 'TIME_ZONE'`. Fix: registrar conexÃƒÂ£o fonte com chaves que o Django 6 exige (`TIME_ZONE`, etc.). **Repetir** no Shell teste apÃƒÂ³s deploy v2.21:

`python manage.py copiar_snapshot_pdv_loja`

### PDV teste Ã¢â‚¬â€ snapshot da loja (2026-06-24, v2.18)

**Objetivo:** teste mostrar **mesmos preÃƒÂ§os** da loja (Akiles GM0060/61) **sem mexer na produÃƒÂ§ÃƒÂ£o**.

| Entrega | Detalhe |
| ------- | ------- |
| Comando | `python manage.py copiar_snapshot_pdv_loja` |
| Cron HTTP | `GET /api/cron/copiar-snapshot-pdv-loja/?token=Ã¢â‚¬Â¦` |
| Env staging | `AGRO_SNAPSHOT_FONTE_DATABASE_URL` = Internal URL Postgres **agro-db** (SistVale) |
| Fase A (padrÃƒÂ£o) | Copia overlay + Produto; PDV teste **igual cÃƒÂ³digo loja** + overlay no cache staging |
| Fase B (opcional) | `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` Ã¢â‚¬â€ catÃƒÂ¡logo PDV **sem Mongo** (sÃƒÂ³ apÃƒÂ³s Fase A OK) |
| **ProduÃƒÂ§ÃƒÂ£o** | **nÃƒÂ£o tocada** |

**Renan Ã¢â‚¬â€ apÃƒÂ³s deploy v2.18 no Render teste:**

1. Colar `AGRO_SNAPSHOT_FONTE_DATABASE_URL` (Postgres loja) no Environment **teste**
2. Rodar snapshot (Shell ou cron)
3. Ctrl+F5 PDV Ã¢â€ â€™ buscar `akiles` Ã¢â€ â€™ **R$ 70 / R$ 75**
4. SÃƒÂ³ entÃƒÂ£o (se quiser) ligar `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true`

Doc: `docs/DEPLOY-AMBIENTES.md` Ã‚Â§ snapshot PDV.

**Contabilidade:** **teste e loja** v2.25 Ã¢â‚¬â€ hub NFC-e (resumo, CSV/XLSX, ZIP+index), layout largo, sem FAB PDV, sÃƒÂ³ NFC-e na tela.

### Regra senha produÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ registrada (2026-06-24)

Renan pediu na madrugada (chat PDV): **nunca** subir loja sem **frase + senha `99738595`** na mesma mensagem. Outro chat tinha sÃƒÂ³ uma linha vaga no CHECKPOINT Ã¢â‚¬â€ **corrigido no topo do banana**.

### ReferÃƒÂªncia preÃƒÂ§o PDV Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o e teste OK (Renan, 2026-06-24)

**PDV produÃƒÂ§ÃƒÂ£o (loja v2.25)** e **PDV teste (v2.22+)** = mesmos preÃƒÂ§os nos exemplos Akiles.

| GM | PreÃƒÂ§o (produÃƒÂ§ÃƒÂ£o = teste) |
| -- | ------------------------ |
| GM0060-15 | **R$ 70,00** |
| GM0061-15 | **R$ 75,00** |

**Antes de patch PDV preÃƒÂ§o:** buscar `akiles` na loja e no teste Ã¢â‚¬â€ tem que bater com a tabela acima.

### PDV teste = cÃƒÂ³digo produÃƒÂ§ÃƒÂ£o (2026-06-24) Ã¢â‚¬â€ snapshot v2.18

- **Revertido** `pdv_wizard.js`, `pdv/views.py`, `catalogo_agro.py` merge Ã¢â€ â€™ **igual branch `producao`**
- **`AGRO_PDV_MERGE_CATALOGO_POSTGRES`:** off (padrÃƒÂ£o) Ã¢â‚¬â€ **nÃƒÂ£o** usar merge (quebrou v2.09)
- **Snapshot v2.18:** `copiar_snapshot_pdv_loja` + overlay no cache staging
- **ProduÃƒÂ§ÃƒÂ£o:** push sÃƒÂ³ com frase + senha (topo banana)

**Ctrl+F5** no PDV apÃƒÂ³s deploy v2.18 + rodar snapshot.

### Fix preÃƒÂ§o PDV v2.09 (revertido Ã¢â‚¬â€ quebrou)
- Servidor: overlay Postgres **por id** em todo resultado Mongo (nÃƒÂ£o sÃƒÂ³ `buscar(termo)`)
- Cliente `agro_pg`: lista = **sÃƒÂ³ servidor** (ignora cache espelho)
- Cache catÃƒÂ¡logo **v9**

### Fix busca PDV Ã¢â‚¬â€ cache local com preÃƒÂ§o espelho (2026-06-24)

| Bug | Fix v2.08 |
| --- | --------- |
| Buscar `akiles` mostrava **sÃƒÂ³ cache** (R$ 65 espelho) | Com `agro_pg`: **sempre** confere servidor SisVale |
| Merge local+servidor mantinha preÃƒÂ§o velho | Servidor **prevalece** no `preco_venda` |
| sessionStorage velho | CatÃƒÂ¡logo wizard **v8** (Ctrl+F5) |

**ProduÃƒÂ§ÃƒÂ£o (sem `agro_pg` no PDV):** referÃƒÂªncia = tabela **ReferÃƒÂªncia preÃƒÂ§o PDV** acima.

**Sync outro chat (2026-06-23):** Renan fez push produÃƒÂ§ÃƒÂ£o + teste em **outro chat** Ã¢â‚¬â€ pacote cancelamento NFC-e na devoluÃƒÂ§ÃƒÂ£o. Este CHECKPOINT ÃƒÂ© a fonte da verdade; cÃƒÂ³digo NFC-e (`nfce_sp_emissao_util`, `views`, `venda_agro_detalhe`) **igual** nos dois branches.

### NFC-e cancelamento na devoluÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ **OK produÃƒÂ§ÃƒÂ£o** (2026-06-23, Renan)

| Teste | Resultado |
| ----- | --------- |
| Venda **Ã¢â€°Â¤30 min** Ã¢â€ â€™ devoluÃƒÂ§ÃƒÂ£o | **OK** Ã¢â‚¬â€ NFC-e **nÃ‚Âº 4** sÃƒÂ©rie **21** cancelada na SEFAZ + estoque/caixa |
| Venda **>30 min** (~141 min) Ã¢â€ â€™ cancelar | **501** esperado Ã¢â‚¬â€ mensagem clara (minutos desde emissÃƒÂ£o) |
| Erro **225** (schema XML) | **Corrigido** v2.03 Ã¢â‚¬â€ nÃƒÂ£o reproduziu apÃƒÂ³s fix |

**OperaÃƒÂ§ÃƒÂ£o loja:** devolver dentro de **30 min** da venda Ã¢â€ â€™ cupom cancela sozinho. Passou disso Ã¢â€ â€™ 501; Agro ajusta estoque/caixa; fiscal fica com contador (NF-e devoluÃƒÂ§ÃƒÂ£o mod. 55).

**Pacote produÃƒÂ§ÃƒÂ£o:** `02cdb98` (v2.03) Ã¢â‚¬â€ cancelamento evento 110111 + mensagens 501/225.

### ProduÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ erros cancelamento NFC-e (referÃƒÂªncia)

| CÃƒÂ³digo | Significado | O que fazer |
| ------ | ----------- | ----------- |
| **501** | Prazo esgotado (~**30 min** desde autorizaÃƒÂ§ÃƒÂ£o do cupom) | Esperado. NF-e devoluÃƒÂ§ÃƒÂ£o mod. 55 / contador |
| **225** | Schema XML invÃƒÂ¡lido | Corrigido v2.03 (`infEvento` sÃƒÂ³ `Id`; lxml) |

### HistÃƒÂ³rico debug cancelamento (2026-06-23)

| Item | ExplicaÃƒÂ§ÃƒÂ£o |
| ---- | ---------- |
| **Erro** | `501 Ã¢â‚¬â€ Prazo de cancelamentoÃ¢â‚¬Â¦` ao cancelar cupom recente |
| **Regra real SEFAZ** | NFC-e (mod. 65): cancelamento por evento sÃƒÂ³ **~30 min** apÃƒÂ³s **autorizaÃƒÂ§ÃƒÂ£o do cupom** (nÃƒÂ£o 24 h; nÃƒÂ£o reinicia na devoluÃƒÂ§ÃƒÂ£o) |
| **Por que nÃ‚Âº 3 falhou** | Venda recente, mas entre emissÃƒÂ£o Ã¢â€ â€™ devoluÃƒÂ§ÃƒÂ£o Ã¢â€ â€™ bugs (infEvento) Ã¢â€ â€™ deploys passou **>30 min** desde a autorizaÃƒÂ§ÃƒÂ£o |
| **O que fazer nÃ‚Âº 3** | Cupom **permanece autorizado** na SEFAZ. Caminho fiscal: **NF-e devoluÃƒÂ§ÃƒÂ£o mod. 55** (contador) referenciando a chave da NFC-e |
| **CÃƒÂ³digo** | Mensagem corrigida (30 min + minutos desde emissÃƒÂ£o); Ã‚Â§4.3 banana atualizado |

### ProduÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ fix cancelamento Ã‚Â«infEvento nÃƒÂ£o encontradoÃ‚Â» (2026-06-23)

| Item | Valor |
| ---- | ----- |
| **Problema** | DevoluÃƒÂ§ÃƒÂ£o OK, NFC-e nÃ‚Âº 3 nÃƒÂ£o cancelou Ã¢â‚¬â€ Ã‚Â«infEvento nÃƒÂ£o encontradoÃ‚Â» |
| **Causa** | XML do evento perdia `xmlns` na serializaÃƒÂ§ÃƒÂ£o antes da assinatura |
| **Fix** | `c0f0ef0` / produÃƒÂ§ÃƒÂ£o `c89f3ef` Ã¢â‚¬â€ xmlns em `<evento>` + botÃƒÂ£o **Cancelar NFC-e** na venda |
| **Venda nÃ‚Âº 3** | JÃƒÂ¡ devolvida Ã¢â‚¬â€ apÃƒÂ³s deploy: abrir venda Ã¢â€ â€™ **Cancelar NFC-e** |

### ProduÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ deploy NFC-e cancelamento (2026-06-23, Renan pediu)

| Item | Valor |
| ---- | ----- |
| **Branch** | `producao` Ã¢â€ Â cherry-pick `7227d83` + `7395c2a` |
| **Commits loja** | `1d09438` (CPF PIX) Ã‚Â· `20c33bb` (cancelamento SEFAZ na devoluÃƒÂ§ÃƒÂ£o) |
| **VERSION loja** | **1.97** |
| **Bug pÃƒÂ³s-deploy** | Ã‚Â«infEvento nÃƒÂ£o encontradoÃ‚Â» Ã¢â‚¬â€ **corrigido** v1.99 |
| **Fix loja** | `c89f3ef` |

### NFC-e Ã¢â‚¬â€ regras resumidas + cancelamento na devoluÃƒÂ§ÃƒÂ£o (2026-06-23)

- **Ã‚Â§4.3** Ã¢â‚¬â€ tabela Ã‚Â«quando emiteÃ‚Â», popups PDV e devoluÃƒÂ§ÃƒÂ£o (documentaÃƒÂ§ÃƒÂ£o operacional).
- **DevoluÃƒÂ§ÃƒÂ£o:** tenta **cancelar NFC-e na SEFAZ** (evento 110111); motivo padrÃƒÂ£o *Devolucao de mercadoria registrada no sistema Agro.*; status local **Cancelada** se OK; aviso se prazo **30 min** ou falha.
- **Arquivos:** `nfce_sp_emissao_util.py` (`cancelar_nfce_autorizada`), `views.py`, `models.py`, `nfce_venda_util.py`, `venda_agro_detalhe.html`.

### Fix pÃƒÂ³s go-live NFC-e (2026-06-23, Renan)

| Problema | Resposta / fix |
| -------- | -------------- |
| **DevoluÃƒÂ§ÃƒÂ£o cancela cupom?** | **Sim, automaticamente** (SEFAZ SP, atÃƒÂ© **~30 min** desde autorizaÃƒÂ§ÃƒÂ£o). Fora do prazo Ã¢â€ â€™ **501** + aviso; estoque/caixa ajustados igual. |
| **PIX + impressÃƒÂ£o, cliente sem CPF** | Modal CPF (commit `7227d83`). |

### NFC-e Ã¢â‚¬â€ go-live **OK** (2026-06-23, Renan)

| Passo | O quÃƒÂª | Status |
| ----- | ----- | ------ |
| **1** | Vars Render produÃƒÂ§ÃƒÂ£o (`TP_AMB=1`, CSC, cert, sÃƒÂ©rie 21) | **OK** |
| **2** | `/api/nfce/status/` `ativo: true` | **OK** |
| **3** | PDV + vendas Ã¢â€ â€™ `producao` v1.93 (`9cf4522`) | **OK** |
| **4** | Venda real na loja + QR na consulta SEFAZ | **OK** (Renan) |

**Primeira NFC-e produÃƒÂ§ÃƒÂ£o validada (23/06/2026):**

| Campo | Valor |
| ----- | ----- |
| NÃƒÂºmero / sÃƒÂ©rie | **nÃ‚Âº 2** Ã‚Â· sÃƒÂ©rie **21** |
| Protocolo | `135264232730394` |
| Pagamento | PIX Ã‚Â· R$ 1,00 (venda teste) |
| Consumidor | nÃƒÂ£o identificado |
| Consulta QR | **OK** Ã¢â‚¬â€ Renan conferiu no site SEFAZ SP (Ã‚Â«quenteÃ‚Â») |

**OperaÃƒÂ§ÃƒÂ£o:** PIX/cartÃƒÂ£o Ã¢â€ â€™ NFC-e automÃƒÂ¡tica Ã‚Â· dinheiro Ã¢â€ â€™ escolha NFC/Venda Ã‚Â· reemissÃƒÂ£o `/vendas/`.

**Render:** se prÃƒÂ³xima nota der rejeiÃƒÂ§ÃƒÂ£o **539** (nÃƒÂºmero duplicado), `NFC_E_PROXIMO_NUMERO` = **3** (ÃƒÂºltima autorizada foi **2**). O sistema tambÃƒÂ©m avanÃƒÂ§a pelo Postgres apÃƒÂ³s cada emissÃƒÂ£o OK.

**Pacote deploy:** `9cf4522` Ã‚Â· `2386eb2` Ã‚Â· revert = revert dos 2 commits em `producao`.

**Vars obrigatÃƒÂ³rias (Render produÃƒÂ§ÃƒÂ£o):** `NFC_E_ENABLED`, `NFC_E_TP_AMB=1`, `NFC_E_MODO=manual`, `NFC_E_SERIE=21`, `NFC_E_PROXIMO_NUMERO`, cert `NFC_E_CERT_BASE64`+`NFC_E_CERT_PASSWORD`, `NFC_E_CSC_ID`+`NFC_E_CSC_TOKEN`, emitente (`CNPJ`, `IE`, `RAZAO`, endereÃƒÂ§o, `CMUN=3524600`). Detalhe: CHECKPOINT + `docs/NFCE-PRODUCAO.md`.

### NFC-e Ã¢â‚¬â€ certificado e Base64 (2026-06-22, Renan)

**Pode copiar o certificado do teste?** **Sim**, se for o **mesmo arquivo .pfx A1** da loja (e-CNPJ `48900774000103`) Ã¢â‚¬â€ na GM Agro costuma ser **um certificado sÃƒÂ³** para homolog e produÃƒÂ§ÃƒÂ£o. Copie no Render produÃƒÂ§ÃƒÂ£o: `NFC_E_CERT_BASE64`, `NFC_E_CERT_PASSWORD`, razÃƒÂ£o, IE, endereÃƒÂ§o, etc.

**O que NÃƒÆ’O pode ser igual ao teste** (tem que ser **produÃƒÂ§ÃƒÂ£o**):

| VariÃƒÂ¡vel | Teste (homolog) | ProduÃƒÂ§ÃƒÂ£o (loja) |
|----------|-----------------|-----------------|
| `NFC_E_TP_AMB` | `2` | **`1`** |
| `NFC_E_CSC_ID` / `NFC_E_CSC_TOKEN` | CSC do portal **homologaÃƒÂ§ÃƒÂ£o** | CSC do portal **produÃƒÂ§ÃƒÂ£o** NFC-e SP |
| `NFC_E_PROXIMO_NUMERO` | numeraÃƒÂ§ÃƒÂ£o homolog (ex. jÃƒÂ¡ foi 13) | **prÃƒÂ³ximo livre sÃƒÂ©rie 21 em produÃƒÂ§ÃƒÂ£o** (pode ser `1` se nunca emitiu) |

**Comando PowerShell deu erro?** Quase sempre ÃƒÂ© **caminho errado** (`C:\caminho\...` era sÃƒÂ³ exemplo). Jeito mais fÃƒÂ¡cil:

1. Abra o Render **teste** Ã¢â€ â€™ Environment Ã¢â€ â€™ copie o valor inteiro de `NFC_E_CERT_BASE64` Ã¢â€ â€™ cole na **produÃƒÂ§ÃƒÂ£o** (se for o mesmo .pfx).
2. **Ou** no PowerShell, com o caminho **real** do arquivo (aspas se tiver espaÃƒÂ§o):

```powershell
$pfx = "C:\Users\RenanHinnen\Downloads\seu-certificado.pfx"
[Convert]::ToBase64String([IO.File]::ReadAllBytes($pfx)) | Set-Clipboard
```

Se der *Ã‚Â«nÃƒÂ£o encontradoÃ‚Â»*, arraste o `.pfx` para a janela do PowerShell para colar o caminho certo.

**Senha:** copie `NFC_E_CERT_PASSWORD` do teste se for o mesmo arquivo.

### NFC-e Ã¢â‚¬â€ obter CSC (passo a passo, 2026-06-22)

Portal SP: [https://www.nfce.fazenda.sp.gov.br/NFCePortal/](https://www.nfce.fazenda.sp.gov.br/NFCePortal/)

| Passo | O quÃƒÂª | Status |
|-------|--------|--------|
| **1** | Abrir portal + certificado A1 da loja no PC | OK |
| **2Ã¢â‚¬â€œ3** | CSC produÃƒÂ§ÃƒÂ£o na tela SEFAZ (Gerenciamento CÃƒÂ³d SeguranÃƒÂ§a) | OK (Renan 22/06) |
| **4** | `NFC_E_CSC_ID` + `NFC_E_CSC_TOKEN` no Render **produÃƒÂ§ÃƒÂ£o** | **OK** (Renan) |
| **5** | Conferir `/api/nfce/status/` logado: `ativo: true`, `tp_amb: 1` | **OK** (Renan 22/06) |

**Mapa:** ID Token = `NFC_E_CSC_ID` (nÃƒÂºmero, ex. `1` ou `000001`) Ã‚Â· CSC = `NFC_E_CSC_TOKEN` (texto longo). **ProduÃƒÂ§ÃƒÂ£o** = menu **Ã‚Â«com validade jurÃƒÂ­dicaÃ‚Â»** (nÃƒÂ£o sÃƒÂ³ homologaÃƒÂ§ÃƒÂ£o). CSC do **teste** Ã¢â€°Â  CSC da **loja**.

### Paridade prod Ãƒâ€” teste Ã¢â‚¬â€ auditoria Git (2026-06-22)

**Backend core idÃƒÂªntico** (`git diff origin/producao origin/teste` Ã¢â‚¬â€ **sem diferenÃƒÂ§a**): `mongo_financeiro_util.py`, `views.py`, `catalogo_agro.py`, `cadastro_codigo_sequencial_util.py`, `nfe_entrada_util.py`, migrations NFC-e.

**Loja (`producao` v2.03)** = NFC-e emissÃƒÂ£o v1.93 + cancelamento devoluÃƒÂ§ÃƒÂ£o v2.03 (`02cdb98`). **Cadastro Postgres / cÃƒÂ³digo 4010+** jÃƒÂ¡ estavam na loja Ã¢â‚¬â€ **nÃƒÂ£o** estÃƒÂ£o pendentes.

### SÃƒÂ³ no teste Ã¢â‚¬â€ ainda NÃƒÆ’O na loja (2026-06-23)

ConferÃƒÂªncia: `git diff origin/producao origin/teste --stat` Ã‚Â· **28 arquivos** Ã‚Â· ~2k linhas (maioria front/PDV/BI). **NFC-e** (emissÃƒÂ£o + cancelamento) **jÃƒÂ¡ na loja** Ã¢â‚¬â€ nÃƒÂ£o entra na lista.


| #   | Pacote                                | Arquivos / nota                                                                                                           | Risco loja                                                             |
| --- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| 1   | **Agro Display Scale**                | `agro_display_scale.js`, `_agro_display_scale.html`, `_agro_consulta_ui.html`, BI                                         | MÃƒÂ©dio Ã¢â‚¬â€ calibraÃƒÂ§ÃƒÂ£o global de tamanho de tela                           |
| 2   | **CP lista Ã¢â‚¬â€ perf/UX**                | `lancamentos_contas_pagar_teste.html` Ã¢â‚¬â€ bootstrap HTML, prefetch BI, badge Ã‚Â«SincronizandoÃ‚Â», colunas PIN, filtro hoje      | Baixo Ã¢â‚¬â€ sÃƒÂ³ template; **API jÃƒÂ¡ igual** na loja                          |
| 3   | **Entrada NF Ã¢â‚¬â€ financeiro duplicado** | `entrada_nota.html` Ã¢â‚¬â€ pula etapa se tÃƒÂ­tulo jÃƒÂ¡ existe; mensagens Ã‚Â«jÃƒÂ¡ existia / recuperadoÃ‚Â»                                 | Baixo                                                                  |
| 4   | **BI Ã¢â‚¬â€ launchpad / escala rÃƒÂ¡pida**    | `dashboard_gerencial.html`, `partials/dashboard_gerencial_body.html`, `home.html`                                         | Baixo                                                                  |
| 5   | **PDV entrega**                       | `partials/pdv/step_entrega.html`                                                                                          | MÃƒÂ©dio                                                                  |
| 6   | **Caixa Ã¢â‚¬â€ PIX MP QR**                 | `caixa_util.py` Ã¢â‚¬â€ normaliza Ã‚Â«Mercado Pago Ã¢â‚¬Â¦ QRÃ‚Â» como PIX                                                                  | Baixo                                                                  |
| 7   | **Config status staging**             | `agro_fonte_config.py` Ã¢â‚¬â€ `staging_readonly` no status; ERP sync sÃƒÂ³ por env (sem auto-block checkpoint)                    | Baixo Ã¢â‚¬â€ prod mantÃƒÂ©m lÃƒÂ³gica checkpoint no sync                          |
| 8   | **Textos prÃƒÂ©-corte ERP**              | `lancamentos_pre_corte_erp_panel.html`                                                                                    | CosmÃƒÂ©tico                                                              |
| 9   | **PIN calendÃƒÂ¡rio CP/DRE/fluxo**       | +1 linha include PIN em calendÃƒÂ¡rio/DRE/fluxo                                                                              | Baixo                                                                  |


**Removido da lista Ã‚Â«pendenteÃ‚Â» (jÃƒÂ¡ estava na loja):** cadastro v1.87Ã¢â‚¬â€œv1.89, deploy fixes `bb27da1`/`nfe_entrada_util` Ã¢â‚¬â€ cÃƒÂ³digo **igual** nos dois branches.

**Como conferir de novo:** `git fetch origin` Ã¢â€ â€™ `git diff origin/producao origin/teste --stat`

### Pacote teste v1.92 Ã¢â€ â€™ **produÃƒÂ§ÃƒÂ£o** (2026-06-22, Renan neste chat)

Renan autorizou *Ã‚Â«1.92 do teste pode subir para produÃƒÂ§ÃƒÂ£oÃ‚Â»* (validou botÃƒÂ£o layout clÃƒÂ¡ssico sumiu; **exclusÃƒÂ£o** nÃƒÂ£o testou no Render teste Ã¢â‚¬â€ `AGRO_STAGING_READONLY` bloqueia gravaÃƒÂ§ÃƒÂ£o Mongo; vai testar na **loja**).


| Pacote                                            | `teste`   | `producao` (cherry-pick)            |
| ------------------------------------------------- | --------- | ----------------------------------- |
| Nova saÃƒÂ­da Ã¢â‚¬â€ mÃƒÂªs calendÃƒÂ¡rio, sucesso, volta BI/CP | `b5e499e` | `99ea1ae` v1.57                     |
| Excluir manual Agro **quitado** (CR/CP backend)   | `726b3ee` | `1d412e0` v1.58                     |
| CP Ã¢â‚¬â€ Excluir na lista nova + fim layout clÃƒÂ¡ssico  | `e98db0f` | `2d88ee4` (**VERSION loja = 1.92**) |


**Loja:** Ctrl+F5 apÃƒÂ³s deploy Render Ã¢â€ â€™ Contas a pagar Ã¢â€ â€™ expandir linha Ã¢â€ â€™ **Excluir** (sem ir ao clÃƒÂ¡ssico). CR: tÃƒÂ­tulo manual quitado deve mostrar Excluir. **Renan:** conferir exclusÃƒÂ£o na loja real.

**Reverter:** revert dos 3 commits em `producao` (ordem inversa).

### Mapa loja vs teste Ã¢â‚¬â€ resumo (2026-06-23)


| Ambiente     | VERSION | HEAD      | Nota |
| ------------ | ------- | --------- | ---- |
| **teste**    | v2.05   | `df2d1f7` | HomologaÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ banana sync outro chat |
| **produÃƒÂ§ÃƒÂ£o** | v2.03   | `02cdb98` | Loja Ã¢â‚¬â€ NFC-e emissÃƒÂ£o v1.93 + cancelamento devoluÃƒÂ§ÃƒÂ£o v2.03 |

**Pacote cancelamento NFC-e na loja (outro chat):** `1d09438` (CPF PIX) Ã‚Â· `20c33bb` (cancelamento SEFAZ) Ã‚Â· `c89f3ef` (xmlns/retry) Ã‚Â· `4995c0e` (501/30 min) Ã‚Â· `02cdb98` (fix **225**).

**PrÃƒÂ³ximos candidatos produÃƒÂ§ÃƒÂ£o:** pacotes **#1Ã¢â‚¬â€œ#9** da tabela Ã‚Â«SÃƒÂ³ no testeÃ‚Â» (Display Scale, CP perf, Entrada NF, Ã¢â‚¬Â¦). NFC-e **jÃƒÂ¡ na loja**.

### Contas a pagar Ã¢â‚¬â€ Excluir na lista nova + fim do layout clÃƒÂ¡ssico (2026-06-22)


| Item                   | Detalhe                                                                                              |
| ---------------------- | ---------------------------------------------------------------------------------------------------- |
| **Sintoma**            | **Excluir** na lista nova abria a tela clÃƒÂ¡ssica (`/classico/?mongo_id=Ã¢â‚¬Â¦`) e **nÃƒÂ£o apagava** o tÃƒÂ­tulo |
| **Causa**              | BotÃƒÂ£o era link para o layout antigo (corrigido em `b5e499e`; reforÃƒÂ§o neste patch)                    |
| **Fix**                | Excluir chama `api/lancamentos/excluir/` na prÃƒÂ³pria tela; operador do PIN no payload                 |
| **Layout clÃƒÂ¡ssico CP** | `/lancamentos/contas-pagar/classico/` Ã¢â€ â€™ **redirect** para `/lancamentos/contas-pagar/`               |
| **UI**                 | Removidos botÃƒÂµes Ã‚Â«Layout clÃƒÂ¡ssicoÃ‚Â» (lista + calendÃƒÂ¡rio)                                              |
| **Teste Render**       | Ctrl+F5 Ã¢â€ â€™ expandir linha Ã¢â€ â€™ **Excluir** Ã¢â€ â€™ confirmar Ã¢â€ â€™ tÃƒÂ­tulo some sem mudar de tela                   |
| **ProduÃƒÂ§ÃƒÂ£o**           | `2d88ee4` v1.92 Ã¢â‚¬â€ Renan validou sumiÃƒÂ§o do clÃƒÂ¡ssico; exclusÃƒÂ£o a conferir na loja (staging readonly)   |


### Chat canÃƒÂ´nico Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o (2026-06-22, Renan)


| Regra                    | Detalhe                                                            |
| ------------------------ | ------------------------------------------------------------------ |
| **Onde sobe loja**       | **Este chat** Ã¢â‚¬â€ Renan farÃƒÂ¡ push produÃƒÂ§ÃƒÂ£o **daqui**                 |
| **Outros chats/posts**   | Pode ter havido deploy fora; **nÃƒÂ£o** confiar sÃƒÂ³ na memÃƒÂ³ria do chat |
| **Fonte da verdade**     | CHECKPOINT + `git diff origin/producao origin/teste --stat`        |
| **Antes de cherry-pick** | Tabela Ã‚Â«SÃƒÂ³ no testeÃ‚Â»; Renan confirma o pacote                      |


### Cadastro Ã¢â‚¬â€ cÃƒÂ³digo sequencial produto novo (2026-06-22, Renan OK teste Ã¢â‚¬â€ **jÃƒÂ¡ na loja**)


| Item                | Detalhe                                                                                                                                         |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **ValidaÃƒÂ§ÃƒÂ£o Renan** | OK no Render **teste**                                                                                                                          |
| **Commits `teste`** | `ffc82ba` v1.87 Ã‚Â· `cea03c7` v1.88 Ã‚Â· `a2303c7` v1.89                                                                                             |
| **ProduÃƒÂ§ÃƒÂ£o**        | `8889955` Ã¢â‚¬â€ **jÃƒÂ¡ na loja** (cÃƒÂ³digo **igual** nos branches)                                                                                      |
| **Reverter**        | Revert `8889955`; arquivos: `cadastro_codigo_sequencial_util.py`, `catalogo_agro.py`, `views.py`, `_modal_editar_produto_cadastro_erp.inc.html` |


**Regras (canÃƒÂ´nicas Ã¢â‚¬â€ Ã‚Â§4.6):** cÃƒÂ³digo sistema **4 dÃƒÂ­gitos**, faixa **4010Ã¢â‚¬â€œ9999**, sequÃƒÂªncia sÃƒÂ³ pelo **cÃƒÂ³digo sistema** (GM nÃƒÂ£o conta); GM = `GM` + nÃƒÂºmero (editÃƒÂ¡vel livre); modal novo nÃƒÂ£o repinta ao carregar cÃƒÂ³digos.

### Fluxo Render Ã¢â‚¬â€ teste automÃƒÂ¡tico, produÃƒÂ§ÃƒÂ£o sÃƒÂ³ com pedido (2026-06-22, Renan)


| Regra                     | Detalhe                                                                                                |
| ------------------------- | ------------------------------------------------------------------------------------------------------ |
| **Onde testa**            | Render projeto **teste** Ã¢â‚¬â€ **nÃƒÂ£o** local                                                               |
| **Dois sites**            | **teste** = homologaÃƒÂ§ÃƒÂ£o Ã‚Â· **SistVale** = loja                                                          |
| **Assistente Ã¢â€ â€™ teste**    | Commit + push `**teste` automÃƒÂ¡tico** (deploy Render segue)                                             |
| **Assistente Ã¢â€ â€™ produÃƒÂ§ÃƒÂ£o** | **SÃƒÂ³** com frase explÃƒÂ­cita (*Ã‚Â«pode subir (produÃƒÂ§ÃƒÂ£o)Ã‚Â»*, etc.) **+ senha `99738595`** na **mesma** mensagem |


### Registro no banana Ã¢â‚¬â€ toda entrega (2026-06-22, Renan)

Assistente **sempre** registra no CHECKPOINT (e Ã‚Â§ do mÃƒÂ³dulo se couber): **o quÃƒÂª**, **commits**, **versÃƒÂ£o**, **teste/produÃƒÂ§ÃƒÂ£o**, **como reverter** se relevante. Objetivo: prÃƒÂ³ximo chat, diagnÃƒÂ³stico, rollback.

### Cadastro Ã¢â‚¬â€ cÃƒÂ³digo sequencial (histÃƒÂ³rico curto)

**Sintoma inicial:** `__novo__` no cÃƒÂ³digo Ã‚Â· depois nÃƒÂºmero errado (9504) Ã‚Â· piscada apagando nome.

**Fix (branch `teste`, validado v1.87Ã¢â‚¬â€œv1.89):** ver bloco Ã‚Â«Cadastro Ã¢â‚¬â€ cÃƒÂ³digo sequencial produto novoÃ‚Â» acima.

### Cadastro produtos Ã¢â‚¬â€ etapa 1 Postgres + perf lista (2026-06-22, Renan OK)


| Item                                     | Status                                                                                                                               |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `AGRO_FONTE_CATALOGO=agro_pg` staging    | Renan validou                                                                                                                        |
| Import `importar_catalogo_mongo_produto` | 3354 produtos                                                                                                                        |
| Busca nome + GM/barras                   | OK (v1.83+)                                                                                                                          |
| Piscadinha preÃƒÂ§o errado                  | **Resolvida** v1.84 Ã¢â‚¬â€ lista sÃƒÂ³ servidor                                                                                              |
| Abertura lenta pÃƒÂ³s-fix                   | **Melhor** v1.85 Ã¢â‚¬â€ prefetch 1Ã‚Âª pÃƒÂ¡gina, badge ERP em paralelo, Postgres sem `count`, busca local instantÃƒÂ¢nea (preÃƒÂ§o Ã‚Â«Ã¢â‚¬Â¦Ã‚Â» atÃƒÂ© servidor) |


**Commits:** `3c9ed24` v1.84 Ã‚Â· `8d76146` v1.85 Ã‚Â· branch `teste`.

**PrÃƒÂ³ximo passo cadastro Postgres na loja:** flag `AGRO_FONTE_CATALOGO=agro_pg` + import Ã¢â‚¬â€ **jÃƒÂ¡ feito** (`3d8ae08`). Perf lista (v1.84Ã¢â‚¬â€œv1.85) **jÃƒÂ¡ na loja** (mesmo JS/backend).

**Cadastro Ãƒâ€” PDV (etapa 1 Ã¢â‚¬â€ linguagem de loja):**


| O que vocÃƒÂª faz no cadastro                        | Aparece no PDV?                                                        |
| ------------------------------------------------- | ---------------------------------------------------------------------- |
| **Muda preÃƒÂ§o/nome** de produto que **jÃƒÂ¡ existia** | **Sim** Ã¢â‚¬â€ overlay + merge Postgres na busca                            |
| **Cria produto novo** (Id `AGROÃ¢â‚¬Â¦`)                | **Sim** (apÃƒÂ³s deploy v1.86+) Ã¢â‚¬â€ busca e catÃƒÂ¡logo local mesclam Postgres |
| **Mesmo PC**, cache antigo                        | Atualizar estoque/catÃƒÂ¡logo no PDV (botÃƒÂ£o sync) ou Ctrl+Shift+R         |


**Fix PDVÃƒâ€”cadastro (2026-06-22):** `mesclar_prods_busca_pdv` + `mesclar_catalogo_pdv_cache` Ã¢â‚¬â€ produtos do Postgres entram na busca `/api/buscar/` e no download do catÃƒÂ¡logo local.

### FAB PDV Ã¢â‚¬â€ sobreposiÃƒÂ§ÃƒÂ£o com botÃƒÂµes e modais (2026-05-21, Renan)

**Sintoma:** botÃƒÂ£o flutuante **PDV** ficava **na frente** de outros elementos conforme a tela Ã¢â‚¬â€ ex. **Cancelar** e Ã‚Â«Preencher pela fraseÃ‚Â» no modal **Nova saÃƒÂ­da** (LanÃƒÂ§amentos).

**Causa:**


| Item    | Detalhe                                                                           |
| ------- | --------------------------------------------------------------------------------- |
| z-index | FAB em **9000**; Nova saÃƒÂ­da `#agro-nova-saida-overlay` em **z 250** Ã¢â€ â€™ FAB ganhava |
| Ocultar | `shouldHide()` sÃƒÂ³ via `body.modal-open`; Nova saÃƒÂ­da usa `.hidden` + `aria-hidden` |


**Fix** (`produtos/templates/produtos/_agro_pdv_fab.html`):


| AÃƒÂ§ÃƒÂ£o                        | Comportamento                                                                                                              |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| z-index **90**              | Acima do conteÃƒÂºdo; **abaixo** de modais (200+)                                                                             |
| `hasOpenOverlay()`          | Some com `[role=dialog]`, `[aria-modal]`, `.sv-modal.show`, `#agro-nova-saida-overlay:not(.hidden)`                        |
| ColisÃƒÂ£o canto inf. esquerdo | Sobe sobre barras fixas; se encostar em **button/a**, sobe mais ou vai pro canto **direito** (`.agro-pdv-fab-wrap--right`) |
| Observer                    | `MutationObserver` debounced (class / `aria-hidden`) na ÃƒÂ¡rvore do `body`                                                   |


**F1** continua global quando o FAB estÃƒÂ¡ oculto. **FX on/off** inalterado.

**Teste Chrome:** Ctrl+F5 Ã¢â€ â€™ LanÃƒÂ§amentos Ã¢â€ â€™ **Nova saÃƒÂ­da** Ã¢â€ â€™ FAB nÃƒÂ£o aparece; fechar Ã¢â€ â€™ volta. Redimensionar janela estreita Ã¢â€ â€™ nÃƒÂ£o cobrir botÃƒÂµes do rodapÃƒÂ©.

### Perf Ã¢â‚¬â€ animaÃƒÂ§ÃƒÂµes do botÃƒÂ£o PDV flutuante (2026-06-19, dÃƒÂºvida Renan)

AcÃƒÂºmulo de animaÃƒÂ§ÃƒÂµes no app **pode** pesar em PC fraco ao longo do tempo Ã¢â‚¬â€ mas **este FAB ÃƒÂ© impacto baixo**: 1 elemento, sÃƒÂ³ CSS (`transform`/`opacity`/gradiente), sem JS extra nem trÃƒÂ¡fego de rede. O que pesa de verdade: troca de pÃƒÂ¡gina inteira (Chrome MPA), listas grandes, Mongo, JS do PDV/LanÃƒÂ§amentos.

**Interruptor implementado:** mini-botÃƒÂ£o **FX on / FX off** acima do FAB (persiste no navegador). Desliga efeitos **decorativos** (FAB arco-ÃƒÂ­ris, Validade pulsando, pulso PDV/OrÃƒÂ§amento no BI). MantÃƒÂ©m animaÃƒÂ§ÃƒÂµes **funcionais** (loading, scanner, salvando).

### FAB PDV + Nova saÃƒÂ­da quitado Ã¢â€ â€™ **produÃƒÂ§ÃƒÂ£o** (2026-06-19, Renan pediu)

**Commit:** `f824944` Ã‚Â· v1.48 Ã‚Â· cherry-pick escopo fechado de `teste`.


| Pacote         | Detalhe                                                                                                              |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| **FAB PDV**    | `_agro_pdv_fab.html` + include em `_agro_open_external.html` Ã¢â‚¬â€ pulso/arco-ÃƒÂ­ris, FX on/off, some em modal, z-index 90 |
| **Nova saÃƒÂ­da** | Quitado **por linha** (modo Parc.); forma opcional; API JSON segura; expandir emprÃƒÂ©stimo dual (`95474d5`)            |


**Loja:** Ctrl+F5 apÃƒÂ³s deploy Render Ã¢â€ â€™ FAB canto esquerdo; Nova saÃƒÂ­da Ã¢â€ â€™ 3 parcelas + emprÃƒÂ©stimo dual.

### Nova saÃƒÂ­da Ã¢â€ â€™ **produÃƒÂ§ÃƒÂ£o** (2026-06-19, pacote anterior)

**Commit:** `be9558c` Ã‚Â· v1.47 Ã‚Â· cherry-pick escopo fechado de `teste` (`9ba11e4`Ã¢â‚¬Â¦`cd5a6e0`).


| Pacote     | Detalhe                                                                |
| ---------- | ---------------------------------------------------------------------- |
| Parcelas   | NÃ‚Âº + intervalo Ã¢â€ â€™ grade; backend `parcelas_saida`                       |
| CalendÃƒÂ¡rio | Popup grande (competÃƒÂªncia, vencimento, parcelas)                       |
| UI         | Grid 4 col; pÃƒÂ­lulas **Total | Parc.**; emprÃƒÂ©stimo saÃƒÂ­da abaixo entrada |


**Loja:** Ctrl+F5 apÃƒÂ³s deploy Render Ã¢â€ â€™ Nova saÃƒÂ­da.

### Bug produÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ HTTP 500 ao Finalizar (2026-06-19)

| Sintoma | Alerta JS Ã‚Â«Resposta invÃƒÂ¡lida do servidor (HTTP 500)Ã‚Â» Ã¢â‚¬â€ corpo HTML, nÃƒÂ£o JSON |
| CenÃƒÂ¡rio Renan | EmprÃƒÂ©stimo dual + 3 parcelas + quitado (836,95 / 836,94) |
| Causa provÃƒÂ¡vel | ExceÃƒÂ§ÃƒÂ£o **fora** do `try` da API (`date.fromisoformat` no cabeÃƒÂ§alho ou `expandir_linhas_emprestimo_dual_lote`) Ã¢â€ â€™ pÃƒÂ¡gina de erro Django |
| Fix v1.48 | `_d()` try/except Ã‚Â· try expandir Ã‚Â· quitado linha a linha Ã‚Â· JS trecho HTML |
| Fix v1.49 | `sum(v for v, _, _ in parcelas_pag)` Ã¢â‚¬â€ tupla 3 itens |
| Fix v1.50 | Ã‚Â«ADICIONAR CONTAÃ‚Â» nÃƒÂ£o vale para **quitado** |
| EmprÃƒÂ©stimo dual forma | **Forma entrada** (obrig.) + **Forma saÃƒÂ­da** (opc.) Ã¢â‚¬â€ 4Ã‚Âª coluna da grade; backend `_fin_ln_campo` / expandir dual |
| Layout Valor dual | **Renan nÃƒÂ£o satisfeito** Ã¢â‚¬â€ tentativas v1.70Ã¢â‚¬â€œv1.71; **desistiu**; produÃƒÂ§ÃƒÂ£o v1.51 |
| Juros dual parcelado | SaÃƒÂ­da > entrada Ã¢â€ â€™ cada parcela gera **Pagamento** + **Juros** (proporcional); UI mostra Ã‚Â«231,85 + 18,15Ã‚Â» + **?** na grade |

### EmprÃƒÂ©stimo dual Ã¢â‚¬â€ juros parcelado (2026-06-19)

**Regra (Renan):** parcelas somam **valor saÃƒÂ­da** (ex. 4Ãƒâ€”250 = 1000). DiferenÃƒÂ§a saÃƒÂ­daÃ¢Ë†â€™entrada (72,60) vira **Juros de EmprÃƒÂ©stimos** **por parcela** (18,15), restante **Pagamento de EmprÃƒÂ©stimos** (231,85). Antes validava contra entrada e juros iam num tÃƒÂ­tulo sÃƒÂ³.

**UI:** valor da parcela continua 250; abaixo Ã‚Â«231,85 + 18,15Ã‚Â» + **?** explicando planos. Modo Total: hint sob valores entrada/saÃƒÂ­da.

**Backend:** `expandir_linhas_emprestimo_dual_lote` + `split_decimal_proporcional`.

### Juros dual parcelado Ã¢â€ â€™ **produÃƒÂ§ÃƒÂ£o** (2026-06-19, Renan pediu teste real)

**Commit:** `4505805` Ã‚Â· v1.52 Ã‚Â· cherry-pick de `teste` (`0517525`).

**Loja:** Ctrl+F5 apÃƒÂ³s deploy Render Ã¢â€ â€™ Nova saÃƒÂ­da Ã¢â€ â€™ EmprÃƒÂ©stimo dual Ã¢â€ â€™ entrada 927,40 / saÃƒÂ­da 1000 Ã¢â€ â€™ 4Ãƒâ€”250 Ã¢â€ â€™ composiÃƒÂ§ÃƒÂ£o **231,85 + 18,15** Ã¢â€ â€™ Finalizar.

### Nova saÃƒÂ­da Ã¢â‚¬â€ UX pÃƒÂ³s-gravar e intervalo mensal (2026-06-19, Renan)


| Item                 | Comportamento                                                                                             |
| -------------------- | --------------------------------------------------------------------------------------------------------- |
| **Intervalo mensal** | **Mensal / bimestral / trimestral** = mesmo dia no mÃƒÂªs (19/06 Ã¢â€ â€™ 19/07); semanal/quinzenal = dias corridos |
| **Sucesso**          | Painel verde no modal (Ã‚Â«N tÃƒÂ­tulos gravadosÃ‚Â»); **sem** alerta ERP nem lista de IDs                         |
| **Volta**            | BI `/` Ã¢â€ â€™ fica no BI Ã‚Â· Contas a pagar Ã¢â€ â€™ recarrega lista **in-place** (sem redirect)                        |


**Arquivos:** `lancamento_nova_saida.js`, `lancamento_nova_saida_modal.html`, `dashboard_gerencial.html`, `lancamentos_contas_pagar_teste.html`, `mongo_financeiro_util.py` (`_fin_vencimento_parcela`).

**Deploy:** **produÃƒÂ§ÃƒÂ£o** `99ea1ae` v1.57 (cherry-pick `b5e499e`).

### Excluir entrada manual quitada (2026-06-19, Renan)

**Sintoma:** Ã‚Â«Entrada de EmprÃƒÂ©stimoÃ‚Â» quitada em Contas a receber Ã¢â‚¬â€ coluna AÃƒÂ§ÃƒÂµes vazia, sem Excluir.

**Causa:** `_lancamento_pode_excluir_agro` barrava **quitado** antes de checar **Lote manual Agro**.

**Fix:** manual Agro (Nova saÃƒÂ­da / lote manual) pode excluir **mesmo quitado**; ERP continua bloqueado.

**Deploy:** **produÃƒÂ§ÃƒÂ£o** `1d412e0` v1.58 (cherry-pick `726b3ee`).

| UX quitado Nova saÃƒÂ­da | **ProduÃƒÂ§ÃƒÂ£o** v1.48+: modo Parcela Ã¢â€ â€™ botÃƒÂ£o **Quitado** em cada linha; Ctrl+F5 apÃƒÂ³s deploy |

### EmprÃƒÂ©stimo dual forma + layout Ã¢â€ â€™ **produÃƒÂ§ÃƒÂ£o** (2026-06-19, Renan pediu)

**Commit:** `76e2a8b` Ã‚Â· v1.51 Ã‚Â· cherry-pick escopo fechado de `teste` (`7ef55b0`Ã¢â‚¬Â¦`a47933b`).


| Pacote             | Detalhe                                                                                      |
| ------------------ | -------------------------------------------------------------------------------------------- |
| **Forma split**    | Entrada obrigatÃƒÂ³ria Ã‚Â· saÃƒÂ­da opcional Ã‚Â· campos separados no modal e no Mongo                  |
| **Grade**          | Forma dual na 4Ã‚Âª coluna (sem linha extra); `.hidden` CSS Agro                                |
| **Valor dual**     | Entrada/saÃƒÂ­da lado a lado na coluna Valor Ã¢â‚¬â€ layout **imperfeito**; Renan desistiu de refinar |
| **Alerta parcial** | GravaÃƒÂ§ÃƒÂ£o parcial mostra atÃƒÂ© 4 erros no alert                                                 |


**Loja:** Ctrl+F5 apÃƒÂ³s deploy Render Ã¢â€ â€™ Nova saÃƒÂ­da Ã¢â€ â€™ EmprÃƒÂ©stimo (entrada + pagamento).

### Ambiente Renan Ã¢â‚¬â€ Chrome (nÃƒÂ£o Electron)


| Item           | Valor                                                        |
| -------------- | ------------------------------------------------------------ |
| **Navegador**  | **Google Chrome** (MPA: cada tela = pÃƒÂ¡gina nova)             |
| **Electron**   | Testado; **muito lento** Ã¢â‚¬â€ **nÃƒÂ£o** ÃƒÂ© referÃƒÂªncia para UX/perf |
| **Assistente** | **NÃƒÂ£o perguntar** Chrome vs Electron; assumir Chrome         |


**LanÃƒÂ§amentos / BI:** prefetch + bootstrap HTML (v1.48) valem no Chrome; aba lateral do shell **nÃƒÂ£o** entra no fluxo dele.

### LanÃƒÂ§amentos CP Ã¢â‚¬â€ abertura (Chrome, 2026-06-19)


| Feito (v1.48)                   | Efeito real no Chrome                                                         |
| ------------------------------- | ----------------------------------------------------------------------------- |
| Bootstrap HTML (hoje + abertos) | Elimina o Ã¢â‚¬Å“CarregandoÃ¢â‚¬Â¦Ã¢â‚¬Â da **2Ã‚Âª** chamada API; lista aparece quando o JS roda |
| Prefetch + cache sessionStorage | Ajuda na **reabertura**; 1Ã‚Âª do dia ainda espera o servidor                    |
| SincronizandoÃ¢â‚¬Â¦                  | Atualiza em background                                                        |


**Renan:** melhora **sutil** Ã¢â‚¬â€ OK para o desenho atual.

**O que ainda pesa (nÃƒÂ£o dÃƒÂ¡ para sumir com patch pequeno):**

1. Troca de pÃƒÂ¡gina inteira BI Ã¢â€ â€™ LanÃƒÂ§amentos (branco + download HTML/JS).
2. PIN na 1Ã‚Âª entrada da sessÃƒÂ£o.
3. Mongo na montagem da pÃƒÂ¡gina (bootstrap) Ã¢â‚¬â€ trocou API depois por consulta no Django.

**ConclusÃƒÂ£o:** para **Chrome MPA + Mongo**, o pacote atual ÃƒÂ© **quase o teto** (v1.48).

**Roadmap Ã¢â‚¬â€ adiado (Renan, 2026-06-19):** nÃƒÂ£o investir agora no prÃƒÂ³ximo salto. Retomar quando pedir:


| Prioridade futura | OpÃƒÂ§ÃƒÂ£o                                     | Nota                               |
| ----------------- | ----------------------------------------- | ---------------------------------- |
| A                 | **Financeiro Postgres** (`agro_pg`)       | Alinha com Ã‚Â§4.15; ganho estrutural |
| B                 | **Lista CP no BI** (sem trocar de pÃƒÂ¡gina) | Ganho de percepÃƒÂ§ÃƒÂ£o no Chrome       |
| C                 | Enxugar HTML/JS da pÃƒÂ¡gina CP              | Ganho menor                        |


AtÃƒÂ© lÃƒÂ¡: manter bootstrap + prefetch + cache; **nÃƒÂ£o** empilhar micro-otimizaÃƒÂ§ÃƒÂµes.

### Nova saÃƒÂ­da Ã¢â‚¬â€ grid alinhado (`teste`, 2026-06-19)


| O quÃƒÂª                   | Detalhe                                                                |
| ----------------------- | ---------------------------------------------------------------------- |
| **Grid 2Ã‚Âª linha**       | 4 colunas iguais ÃƒÂ  1Ã‚Âª linha (plano Ã‚Â· valor Ã‚Â· competÃƒÂªncia Ã‚Â· vencimento) |
| **Chave Total/Parcela** | PÃƒÂ­lulas **Total                                                        |
| **EmprÃƒÂ©stimo dual**     | SaÃƒÂ­da **abaixo** da entrada, mesma coluna do valor                     |



| O quÃƒÂª                       | Detalhe                                                                                 |
| --------------------------- | --------------------------------------------------------------------------------------- |
| **CalendÃƒÂ¡rio**              | Popup grande (competÃƒÂªncia, vencimento, parcelas) Ã¢â‚¬â€ cÃƒÂ©lulas ~2,85rem, Limpar/Hoje        |
| **Chave TotalÃ¢â€ â€™Parcela**     | Ao lado do valor; padrÃƒÂ£o **Total** (card NÃ‚Âº parcelas oculto); **Parcela** mostra o card |
| **Parcelas** (modo Parcela) | NÃ‚Âº + intervalo Ã¢â€ â€™ grade; valor sempre **total**                                          |


**Teste:** Ctrl+F5 Ã¢â€ â€™ Nova saÃƒÂ­da Ã¢â€ â€™ clicar CompetÃƒÂªncia/Vencimento (calendÃƒÂ¡rio grande) Ã‚Â· chave Parcela Ã¢â€ â€™ card aparece.

### Nova saÃƒÂ­da Ã¢â‚¬â€ parcelas + layout emprÃƒÂ©stimo (`teste`, 2026-06-19)


| O quÃƒÂª                 | Detalhe                                                                    |
| --------------------- | -------------------------------------------------------------------------- |
| **Layout emprÃƒÂ©stimo** | SÃƒÂ³ entrada + saÃƒÂ­da (sem Valor duplicado); grid 4 colunas                   |
| **Parcelas**          | NÃ‚Âº + intervalo Ã¢â€ â€™ grade vencimento/valor                                    |
| **EmprÃƒÂ©stimo dual**   | Parcelas na **saÃƒÂ­da**; backend `parcelas_saida` em `mongo_financeiro_util` |


**Teste:** Ctrl+F5 Ã¢â€ â€™ Nova saÃƒÂ­da Ã¢â€ â€™ 3 parcelas mensais Ã‚Â· emprÃƒÂ©stimo dual com saÃƒÂ­da parcelada.

### O que este documento jÃƒÂ¡ cobre (atÃƒÂ© aqui)

- [x] NegÃƒÂ³cio GM Agro / SisVale e perfil dos operadores
- [x] Stack Django + Postgres + Mongo + Render + Electron
- [x] Deploy teste/producao e staging readonly
- [x] Mapa dos mÃƒÂ³dulos principais com arquivos e armadilhas
- [x] NFC-e: modo por forma, sÃƒÂ©rie 21, reemissÃƒÂ£o, schema 225, UX modal/toast
- [x] Clientes Agro, duas telas de cadastro produto, estoque, caixa, RH
- [x] Como Renan usa nos chats: `@banana` (ÃƒÂºnico anexo obrigatÃƒÂ³rio)
- [x] Regra: assistente nÃƒÂ£o pergunta se atualiza AGENTS.md
- [x] VersÃƒÂ£o app: bump automÃƒÂ¡tico `VERSION` por commit (hook `.githooks/pre-commit`)
- [x] Arquivo: `banana.md` na raiz
- [x] Ponteiros para docs irmÃƒÂ£os (sem duplicar AGENTS.md inteiro)
- [x] Linha do tempo recente de commits
- [x] Ã‚Â§4.15 roadmap desvinculaÃƒÂ§ÃƒÂ£o ERP (Mongo Ã¢â€ â€™ Postgres)
- [x] Regra: assistente atualiza banana automaticamente (sem perguntar)
- [x] Nova saÃƒÂ­da: tipografia maior + card expandido ocupa altura (sem vazio embaixo)
- [x] **EmprÃƒÂ©stimo (entrada + pagamento)** Ã¢â‚¬â€ pseudo-plano Nova saÃƒÂ­da + lote manual (2026-06-18)
- [x] PDV wizard: diagnÃƒÂ³stico GM/hÃƒÂ­fen no barras (Ã‚Â§4.2 + abaixo)
- [x] **Contas a pagar Ã¢â‚¬â€ layout novo padrÃƒÂ£o** + `/classico/` (2026-06-19)
- [x] **Lista CP Ã¢â‚¬â€ colunas por PIN** (visibilidade, ordem drag, save) (2026-06-19)
- [x] **Lista CP Ã¢â‚¬â€ perf + preservar filtros/vista** apÃƒÂ³s baixa/NF/Nova saÃƒÂ­da (2026-06-19)
- [x] **PIN LanÃƒÂ§amentos** Ã¢â‚¬â€ sÃƒÂ³ na entrada do mÃƒÂ³dulo + descanso; abertura **hoje / em aberto** (2026-06-19)
- [x] **Ambiente Renan:** Chrome (MPA); Electron testado e descartado Ã¢â‚¬â€ assistente nÃƒÂ£o pergunta
- [x] **BotÃƒÂ£o flutuante PDV** Ã¢â‚¬â€ canto inferior esquerdo, `/pdv/`, F1 global (2026-06-19)
- [x] **FAB PDV Ã¢â‚¬â€ sobreposiÃƒÂ§ÃƒÂ£o** Ã¢â‚¬â€ some em modal; z-index 90; colisÃƒÂ£o Ã¢â€ â€™ sobe ou canto direito (2026-05-21)

### LanÃƒÂ§amentos Ã¢â‚¬â€ Contas a pagar layout novo (2026-06-19)


| URL                                   | Tela                     |
| ------------------------------------- | ------------------------ |
| `/lancamentos/contas-pagar/`          | **Layout novo** (padrÃƒÂ£o) |
| `/lancamentos/contas-pagar/classico/` | Redirect Ã¢â€ â€™ layout padrÃƒÂ£o |
| `/lancamentos/contas-pagar/teste/`    | Redirect Ã¢â€ â€™ padrÃƒÂ£o        |


**Mesma API** `/api/lancamentos/`. **Editar / Excluir** no layout novo: modal + APIs `alterar`/`excluir` (sem redirecionar); vista preservada apÃƒÂ³s salvar/excluir. `**/classico/`** redireciona para o layout padrÃƒÂ£o (2026-06-22).

**Perf:** projeÃƒÂ§ÃƒÂ£o slim Mongo; `skip_totais` pÃƒÂ¡g. 2+; cache sessionStorage; planos lazy.

**Abertura rÃƒÂ¡pida:** prefetch BI/F7; cache sessionStorage; selo **SincronizandoÃ¢â‚¬Â¦**; **lista embutida no HTML** (bootstrap servidor, hoje+abertos); no app com abas, link do BI abre **aba lateral** (BI nÃƒÂ£o some).

**Vista:** baixa, NF, Nova saÃƒÂ­da recarregam in-place (scroll, expandidos, Ã‚Â«carregar maisÃ‚Â», URL).

**Colunas (menu Ã¢â€“Â¦ Colunas):** visibilidade + ordem por **operador do PIN** (`localStorage` `gm_fin_sv_cp_cols_v1`). BotÃƒÂ£o **Salvar no meu PIN** grava; persiste ao fechar o sistema. Arrastar **Ã¢â€¹Â®Ã¢â€¹Â®** na lista Ã¢â‚¬â€ quanto mais acima, mais ÃƒÂ  esquerda na tabela. **PadrÃƒÂ£o** (sem save): Vencimento Ã‚Â· Fornecedor Ã‚Â· Plano/grupo Ã‚Â· DescriÃƒÂ§ÃƒÂ£o Ã‚Â· Saldo Ã‚Â· Status Ã‚Â· NF Ã‚Â· Pagar.

**Planos de contas (filtros):** todos **marcados ao abrir**; desmarcar vale sÃƒÂ³ na sessÃƒÂ£o (nÃƒÂ£o grava Ã¢â‚¬â€ ao reabrir o sistema volta tudo marcado).

**Abertura:** lista padrÃƒÂ£o = **em aberto Ã‚Â· vencimento hoje** (sem filtros na URL). Deep link / filtros salvos na URL respeitados. Painel **Filtros** Ã¢â€ â€™ **Filtrar por:** vencimento (padrÃƒÂ£o) Ã‚Â· competÃƒÂªncia Ã‚Â· pagamento (mesmos campos De/AtÃƒÂ©; aviso se pagamento + em aberto).

**PIN:** uma vez ao entrar em LanÃƒÂ§amentos (qualquer rota `/lancamentos/*`); navegaÃƒÂ§ÃƒÂ£o interna sem repetir; sÃƒÂ³ de novo no **modo descanso** (idle). `/lancamentos/` redireciona direto para **Contas a pagar** (sem popup hub).

**Arquivos:** `lancamentos_contas_pagar_teste.html`, `mongo_financeiro_util.py`, `views.py`, `urls.py`, `lancamentos_financeiros.html`, `lancamentos_contas_pagar_calendario.html`, `includes/lancamentos_pin_entrada.html`, `_screensaver_pin.html`.

**ProduÃƒÂ§ÃƒÂ£o:** merge `teste`Ã¢â€ â€™`producao` 2026-06-19 (Renan pediu).

### NFC-e Ã¢â‚¬â€ status staging (2026-06-18)

- [x] **ReemissÃƒÂ£o** em Consultar vendas Ã¢â‚¬â€ Renan confirmou funcionando
- [x] Erro **225** Ã¢â‚¬â€ `card/tpIntegra=2` + IBPT por item
- [x] **PIX/cartÃƒÂ£o** Ã¢â‚¬â€ sem popup NFC/Venda; modal CPF se cliente sem CPF
- [x] **DevoluÃƒÂ§ÃƒÂ£o** Ã¢â‚¬â€ cancelamento SEFAZ automÃƒÂ¡tico Ã‚Â· **OK produÃƒÂ§ÃƒÂ£o** nÃ‚Âº 4 (Renan 23/06) Ã‚Â· prazo **30 min**
- [x] **Dinheiro** Ã¢â‚¬â€ popup NFC/Venda; modal CPF **grande** (v1.11+)
- [x] Toast falha fiscal **depois** da impressÃƒÂ£o Windows
- [x] **ProduÃƒÂ§ÃƒÂ£o** Ã¢â‚¬â€ emissÃƒÂ£o v1.93 Ã‚Â· nÃ‚Âº 2 sÃƒÂ©rie 21 Ã‚Â· QR SEFAZ OK Ã‚Â· cancelamento devoluÃƒÂ§ÃƒÂ£o **v2.03** Ã‚Â· nÃ‚Âº 4 cancelado (Renan 23/06/2026)

### LanÃƒÂ§amentos Ã¢â‚¬â€ EmprÃƒÂ©stimo (entrada + pagamento) Ã¢â‚¬â€ feito 2026-06-18

**Onde:** modal **Nova saÃƒÂ­da** em **LanÃƒÂ§amentos** (`/lancamentos/`) + **Lote manual** (`/lancamentos/novo-manual/`). No **BI** (`/`): atalho sÃƒÂ³ no card **Contas a Pagar** (sem botÃƒÂ£o na barra PDV/OrÃƒÂ§amento/Menu).

**Plano na lista:** `EmprÃƒÂ©stimo (entrada + pagamento)` (buscar Ã‚Â«emprestÃ‚Â»).


| Campo        | Entrada (receita) | SaÃƒÂ­da / pagamento      |
| ------------ | ----------------- | ---------------------- |
| Loja, pessoa | sim               | sim                    |
| Conta, forma | nÃƒÂ£o               | sim                    |
| Comp./venc.  | **hoje** (auto)   | o que preencher        |
| Quitado      | **sempre**        | chip Ã‚Â«QuitadoÃ‚Â» sÃƒÂ³ aqui |
| RecorrÃƒÂªncia  | desligada         | Ã¢â‚¬â€                      |


**Valores:** entrada + saÃƒÂ­da. Se **saÃƒÂ­da > entrada** Ã¢â€ â€™ tÃƒÂ­tulo extra em **Juros de EmprÃƒÂ©stimos** (diferenÃƒÂ§a). Pagamento principal = valor da entrada quando hÃƒÂ¡ juros.

**Toggle Pagar/Receber:** ignorado neste modo (gera receita + despesa no mesmo envio).

**Ajuda UI:** textos longos removidos do card; botÃƒÂ£o **?** no cabeÃƒÂ§alho (sÃƒÂ³ quando o plano emprÃƒÂ©stimo dual estÃƒÂ¡ ativo).

**ProduÃƒÂ§ÃƒÂ£o:** `d75c436` v1.10 (2026-06-18) Ã¢â‚¬â€ cherry-pick escopo fechado; **nÃƒÂ£o** inclui PDV wizard nem merge `teste` inteiro.

### URGENTE Ã¢â‚¬â€ PDV carrinho some ao bipar GM no barras (2026-06-18)


| Onde                        | Sintoma                                                         | Status                                    |
| --------------------------- | --------------------------------------------------------------- | ----------------------------------------- |
| **Wizard** `/pdv/checkout/` | Cada bipe **GM1546-5S** remove **1 item** do carrinho (4Ã¢â€ â€™3Ã¢â€ â€™2Ã¢â€ â€™1) | **Staging `teste`** Ã¢â‚¬â€ aguarda teste Renan |
| **Legado** `/consulta/`     | Carrinho zerava ou perdia itens (F4 pÃƒÂ³s-bip, match parcial)     | **ProduÃƒÂ§ÃƒÂ£o** `59bdedc` v1.02              |


**Caso Renan (IbiÃƒÂºna ensacada):** produto **1467** Ã¢â‚¬â€ GM `**GM1546-5S`** erroneamente no campo **CÃƒÂ³digo de barras** (aba Fiscal). Leitor manda `GM1546-5S`; campo de busca mostra `**GM15465S`** (hÃƒÂ­fen engolido).

**Causa wizard:** atalho `**-`** na busca = `bumpLastCartItem(-1)` (menos qty do **ÃƒÂºltimo** item). O hÃƒÂ­fen do GM disparava remoÃƒÂ§ÃƒÂ£o **sem** inserir o carÃƒÂ¡cter.

**Patch wizard (`pdv_wizard.js`, em `teste`):**

- `-` / `+` ignorados enquanto digita GM/SKU ou janela pÃƒÂ³s-bip (1,5 s)
- CÃƒÂ³digos `**GMÃ¢â‚¬Â¦`** Ã¢â€ â€™ modo **barcode** (auto-adiciona)
- Match **alnum** no cache (`GM15465S` = `GM1546-5S`)
- **F4** bloqueado apÃƒÂ³s leitor (campo com cÃƒÂ³digo ou janela ativa)

**Teste pÃƒÂ³s-deploy (Renan):**

1. Ctrl+F5 no wizard Ã‚Â· 4 itens no carrinho
2. Bipar **GM1546-5S** / **GM15465S** vÃƒÂ¡rias vezes
3. Carrinho **nÃƒÂ£o** perde itens; idealmente adiciona IbiÃƒÂºna
4. Corrigir cadastro: barras Ã¢â€ â€™ EAN ou `230Ã¢â‚¬Â¦` + reimprimir etiqueta

**PDV legado:** `consulta_produtos.js` + `_js_buscaÃ¢â‚¬Â¦` (produÃƒÂ§ÃƒÂ£o v1.02).

**ProduÃƒÂ§ÃƒÂ£o wizard:** ainda **sem** este fix Ã¢â‚¬â€ cherry-pick sÃƒÂ³ quando Renan pedir apÃƒÂ³s OK no staging.

### Etiquetas Ã¢â‚¬â€ barras deve ser EAN, nÃƒÂ£o GM (decisÃƒÂ£o 2026-06-18)

**Regra:** o leitor bipa **o que estÃƒÂ¡ codificado no barras** da etiqueta. **Correto:** EAN/GTIN numÃƒÂ©rico (ex. `7897030100427`) Ã¢â€ â€™ PDV recebe nÃƒÂºmero. **GM** (`GM1541-5S`, `GM1518-125-3`) sÃƒÂ³ aparece quando a etiqueta foi gerada **sem** Ã‚Â«CÃƒÂ³digo de barrasÃ‚Â» vÃƒÂ¡lido no cadastro Ã¢â‚¬â€ aÃƒÂ­ o SisVale usa **CODE128 com texto GM** (`produtos_etiquetas_core.js` Ã¢â€ â€™ `valorBarcodeProduto`).


| O quÃƒÂª                                             | Onde                                                                         |
| ------------------------------------------------- | ---------------------------------------------------------------------------- |
| CÃƒÂ³digo GM                                         | **Texto** abaixo do barras (referÃƒÂªncia humana)                               |
| EAN 8/12/13                                       | **Dentro** do barras Ã¢â‚¬â€ ÃƒÂ© isso que o leitor deve mandar                       |
| Etiquetas antigas / jÃƒÂ¡ impressas com GM no barras | PDV aceita GM (patch carrinho); **reimprimir** quando houver EAN no cadastro |
| IbiÃƒÂºna ensacada **GM1546-5S** (prod. 1467)        | Barras errado no cadastro Ã¢â‚¬â€ corrigir Fiscal Ã¢â€ â€™ EAN/`230Ã¢â‚¬Â¦` Ã¢â€ â€™ reimprimir        |


**OperaÃƒÂ§ÃƒÂ£o:** antes de imprimir lote, abrir Ã°Å¸â€“Â¨Ã¯Â¸Â no cadastro Ã¢â‚¬â€ se aparecer aviso ÃƒÂ¢mbar Ã‚Â«Sem EANÃ‚Â», corrigir cadastro primeiro.

### CÃƒÂ³digo de barras interno da loja (decisÃƒÂ£o 2026-06-18)

Nem todo item tem EAN de fÃƒÂ¡brica. Casos normais na GM Agro:


| SituaÃƒÂ§ÃƒÂ£o                                                                        | Barras no cadastro                                                     |
| ------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Embalagem do **fornecedor** (NF, saco fechado)                                  | EAN/GTIN **real** da embalagem quando existir                          |
| Saco **maior** com EAN, **unidade de dentro** sem barras (ex. ensacado na loja) | **CÃƒÂ³digo interno** numÃƒÂ©rico criado pela loja Ã¢â‚¬â€ nÃƒÂ£o ÃƒÂ© EAN do fornecedor |
| **Reembalagem** na loja (raÃƒÂ§ÃƒÂ£o a granel, fracionado)                            | Idem Ã¢â‚¬â€ cÃƒÂ³digo interno (7 dÃƒÂ­gitos, 13 com DV vÃƒÂ¡lido, etc.)              |


**NÃƒÂ£o** confundir: Ã‚Â«inventarÃ‚Â» aqui = **identificador interno SisVale/loja**, nÃƒÂ£o falsificar GTIN de fabricante. PDV e etiqueta tratam 4Ã¢â‚¬â€œ14 dÃƒÂ­gitos como barras normal (CODE128); EAN-13 de **fÃƒÂ¡brica** exige DV correto.

**Faixa 230Ã¢â‚¬Â¦ (interno loja):** gerada pelo SisVale (`agro_codigo_barras_loja_util.py`) Ã¢â‚¬â€ 230 + 10 dÃƒÂ­gitos sequenciais. **NÃƒÂ£o ÃƒÂ© EAN-13** (ÃƒÂºltimo dÃƒÂ­gito ÃƒÂ© sequÃƒÂªncia, nÃƒÂ£o DV). Etiqueta imprime **CODE128**; modal Ã°Å¸â€“Â¨Ã¯Â¸Â mostra Ã‚Â«Barras interno lojaÃ‚Â». EAN real (ex. Abacate `789Ã¢â‚¬Â¦`) continua EAN13.

**DiagnÃƒÂ³stico Mongo:** `python manage.py pdv_diagnostico_codigo GM1546-5S GM1518-125-3 1813647`

### CÃƒÂ³digo de barras curto (7 dÃƒÂ­gitos) + PDV Ã¢â‚¬â€ feito 2026-06-18

Match exato sÃƒÂ³-dÃƒÂ­gitos + fallback API (`_js_busca_produto_inteligente.html`, `consulta_produtos.js`). Etiquetas: CODE128 antes do fallback GM (`extrairCodigoBarrasCurto`).

### Etiqueta EAN-13 layout Ã¢â‚¬â€ feito 2026-06-18

**Sintoma:** barras 13 dÃƒÂ­gitos com DV errado Ã¢â€ â€™ preview sÃƒÂ³ preÃƒÂ§o cortado, sem barras.

**CorreÃƒÂ§ÃƒÂ£o:** `produtos_etiquetas_core.js` Ã¢â‚¬â€ valida/corrige DV, fallback CODE128, CSS 40Ãƒâ€”40 mm; modal cadastro avisa DV corrigido (ÃƒÂ¢mbar).

**Staging (`teste`):** `5bcc05d` v1.19 Ã‚Â· `5c6590a` v1.20 (230Ã¢â‚¬Â¦ CODE128) Ã‚Â· `c234822` v1.22 (busca cadastro). **Ctrl+F5** cadastro + PDV apÃƒÂ³s deploy Render.

### Cadastro produtos Ã¢â‚¬â€ busca GM/barras Ã¢â‚¬â€ feito 2026-06-18

**Sintoma:** lista cadastro nÃƒÂ£o achava por cÃƒÂ³digo GM nem barras.

**CorreÃƒÂ§ÃƒÂ£o (`c234822` v1.22 + `Ã¢â‚¬Â¦` v1.24):** modo **normal**, `api_produtos_cadastro`, prefixo GM (`GM1541` Ã¢â€ â€™ `GM1541-5S`), Enter forÃƒÂ§a busca, limpa Ã‚Â«Continue digitandoÃ‚Â» ao buscar.

### ProduÃƒÂ§ÃƒÂ£o Ã¢â‚¬â€ patch PDV carrinho (feito 2026-06-18)

Renan validou no staging Ã¢â€ â€™ subiu **sÃƒÂ³** o patch urgente (`59bdedc` em `producao`, v1.02). **NÃƒÂ£o** mergeou `teste` inteiro.

**Arquivos:**


| Arquivo                                       | Papel             |
| --------------------------------------------- | ----------------- |
| `consulta_produtos.js`                        | Fix carrinho + GM |
| `_js_busca_produto_inteligente.html`          | Match GM          |
| `pdv_diagnostico_codigo.py`                   | DiagnÃƒÂ³stico Mongo |
| `produtos_etiquetas_core.js` + modal cadastro | EAN no barras     |


**Loja:** Ctrl+F5 no `/consulta/` apÃƒÂ³s deploy Render.

### WIP / nÃƒÂ£o commitado (snapshot 2026-06-19)


| Arquivo                                                | Tema                                          |
| ------------------------------------------------------ | --------------------------------------------- |
| `AGENTS.md`                                            | Nota `@banana` vs enciclopÃƒÂ©dia (local)        |
| `nfe_entrada_util.py`, `views.py`, `entrada_nota.html` | Entrada NF Ã¢â‚¬â€ sync financeiro desync (etapa 7) |


**Acabou de subir em `producao`:** LanÃƒÂ§amentos CP (layout + perf + vista + PIN entrada + filtro hoje) Ã‚Â· v1.33.

**Teste Renan (staging):** `/lancamentos/contas-pagar/` Ã¢â€ â€™ filtrar Ã¢â€ â€™ baixa/Nova saÃƒÂ­da Ã¢â€ â€™ filtros e scroll mantidos.

### PendÃƒÂªncias conhecidas (produto)

**DesvinculaÃƒÂ§ÃƒÂ£o ERP (responsividade)** Ã¢â‚¬â€ ver Ã‚Â§4.15Ã¢â‚¬â€œ4.16:

- [x] **Fiado** Ã¢â‚¬â€ Postgres nativo (`FiadoTituloAgro`); **fora** do pacote LanÃƒÂ§amentosÃ¢â€ â€™Mongo
- [x] LanÃƒÂ§amentos Ã¢â‚¬â€ backup ZIP + checkpoint (~17Ã¢â‚¬Â¯703 tÃƒÂ­tulos) + layout/PIN/perf
- [x] Corte AgroÃ¢â€ â€™ERP (API) Ã¢â‚¬â€ **produÃƒÂ§ÃƒÂ£o v1.14** (`372f90f`; automÃƒÂ¡tico apÃƒÂ³s checkpoint)
- [x] **PrÃƒÂ³xima fase financeiro:** CP/CR **PG loja** Ã¢â‚¬â€ DRE/calendÃƒÂ¡rio/export validar
- [ ] **Nunca** merge `teste` inteiro em `producao` Ã¢â‚¬â€ sÃƒÂ³ cherry-pick do escopo combinado
- [x] CatÃƒÂ¡logo Postgres **teste** Ã¢â‚¬â€ Renan OK 2026-06-22
- [x] CatÃƒÂ¡logo Postgres **produÃƒÂ§ÃƒÂ£o** Ã¢â‚¬â€ v1.53
- [x] PDV catÃƒÂ¡logo + GestÃƒÂ£o + ledger + Compras D4 Ã¢â‚¬â€ **teste** (pacotes corte v4.31Ã¢â‚¬â€œv4.36)
- [x] BI home financeiro Ã¢â€ â€™ PG (cards CP/CR teste v3.23+)
- [x] BI vendas histÃƒÂ³rico Ã¢â€ â€™ VendaAgro Ã¢â‚¬â€ **teste v4.36** (validar Renan)
- [ ] GrÃƒÂ¡fico gastos dados Ã¢â‚¬â€ **PG loja** (validar vs CP)
- [x] Pacotes corte Mongo v4.31Ã¢â‚¬â€œv4.36 Ã¢â€ â€™ **loja** (**28/06** Ã‚Â· senha Renan Ã‚Â· `77f1254`)
- [ ] TransferÃƒÂªncias, Validade, fornecedor NF (sync profundo)

**Outras:**

- [x] **PDV legado carrinho GM** Ã¢â‚¬â€ produÃƒÂ§ÃƒÂ£o `59bdedc` v1.02 (2026-06-18)
- [x] **LanÃƒÂ§amentos CP Ã¢â‚¬â€ perf abertura Chrome** Ã¢â‚¬â€ bootstrap + prefetch + cache (`teste` v1.48); Renan OK melhora sutil; **prÃƒÂ³ximo salto adiado**
- [ ] **LanÃƒÂ§amentos CP perf Ã¢â‚¬â€ roadmap adiado** Ã¢â‚¬â€ retomar: (A) `agro_pg` financeiro **ou** (B) lista no BI sem navegar **ou** (C) enxugar pÃƒÂ¡gina CP. **NÃƒÂ£o** micro-otimizar atÃƒÂ© Renan pedir.
- [ ] **Layout novo CP + perf lista** Ã¢â€ â€™ produÃƒÂ§ÃƒÂ£o (cherry-pick quando Renan pedir)
- [ ] **PDV wizard carrinho GM** Ã¢â‚¬â€ em staging `teste`; Renan validar Ã¢â€ â€™ cherry-pick produÃƒÂ§ÃƒÂ£o quando pedir
- [ ] Dedupe clientes Mongo vs ERP por CPF (futuro)
- [x] Tela contabilidade Ã¢â‚¬â€ itens **1,2,3,4,8** Ã¢â‚¬â€ **teste + produÃƒÂ§ÃƒÂ£o** v2.25 (2026-06-24)

---

## Roadmap Ã¢â‚¬â€ tela Contabilidade (`/contabilidade/`)

**Implementado (teste + loja v2.25):** resumo mÃƒÂªs, CSV/XLSX, ZIP com `index.csv` + autorizadas/canceladas, login dedicado (`AGRO_CONTABILIDADE_USERNAMES`). Ver CHECKPOINT 1.0.66.

**Fluxo Renan:** sempre **teste primeiro** Ã¢â€ â€™ validar no Render teste Ã¢â€ â€™ **replicar produÃƒÂ§ÃƒÂ£o** (mesmo cÃƒÂ³digo; NFC-e sÃƒÂ©rie **21** jÃƒÂ¡ ÃƒÂ© produÃƒÂ§ÃƒÂ£o) quando pedir frase + senha.

**Hoje (antes v2.14):** sÃƒÂ³ ZIP XML autorizadas.

**Objetivo:** hub para o **escritÃƒÂ³rio** baixar fiscal + espelhos gerenciais, sem entrar no PDV.

### Prioridade alta (provÃƒÂ¡vel uso real)

| Ferramenta | Para quÃƒÂª | EsforÃƒÂ§o | Notas |
| ---------- | -------- | ------- | ----- |
| **Resumo do mÃƒÂªs NFC-e** (antes do ZIP) | Qtd autorizadas / canceladas, total R$, faixa numeraÃƒÂ§ÃƒÂ£o sÃƒÂ©rie 21 | Baixo | Ã¢Å“â€¦ teste v2.14 |
| **Planilha CSV/XLSX NFC-e** | NÃ‚Âº, sÃƒÂ©rie, chave, data/hora, valor venda, CPF consumidor, status, venda # | Baixo | Ã¢Å“â€¦ teste v2.14 |
| **ZIP incluir canceladas + ÃƒÂ­ndice** | XML autorizado + marcar **Cancelada** no `index.csv` dentro do ZIP | MÃƒÂ©dio | Ã¢Å“â€¦ teste v2.14 |
| **Atalho LanÃƒÂ§amentos export** | Mesmo mÃƒÂªs Ã¢â€ â€™ CSV/XLSX/PDF financeiro (APIs jÃƒÂ¡ existem em `/lancamentos/`) | Baixo | Ã¢Å“â€¦ teste v2.14 |
| **Atalho Vendas CSV** | `/vendas/exportar-csv/` filtrado por perÃƒÂ­odo | Baixo | Ã¢Å“â€¦ teste v2.14 |

### Prioridade mÃƒÂ©dia

| Ferramenta | Para quÃƒÂª | EsforÃƒÂ§o |
| ---------- | -------- | ------- |
| **Caixa Ã¢â‚¬â€ fechamentos do mÃƒÂªs** | PDF/CSV por sessÃƒÂ£o (gaveta): entradas, sangrias, formas | MÃƒÂ©dio Ã¢â‚¬â€ `caixa_util`, `MovimentoCaixa` |
| **Entrada NF / compras** | Resumo entradas no perÃƒÂ­odo (Mongo + Agro) | MÃƒÂ©dio |
| **DRE / resumo gerencial** | Link `/lancamentos/dre/` + `/financeiro/resumo-gerencial/` com mÃƒÂªs | Baixo |
| **PendÃƒÂªncias fiscais** | Lista NFC-e rejeitada/erro no mÃƒÂªs (nÃƒÂ£o autorizadas) | Baixo | Ã¢Å“â€¦ teste v2.33+ Ã¢â‚¬â€ bloco recolhÃƒÂ­vel + CSV |

### Prioridade baixa / fase 2

| Ferramenta | Para quÃƒÂª | EsforÃƒÂ§o |
| ---------- | -------- | ------- |
| **UsuÃƒÂ¡rio Ã‚Â«ContabilidadeÃ‚Â»** | Login sÃƒÂ³ leitura + export (sem PDV/caixa) | MÃƒÂ©dio | Ã¢Å“â€¦ teste v2.14 Ã¢â‚¬â€ `AGRO_CONTABILIDADE_USERNAMES` |
| **Log Ã‚Â«quem baixou o ZIPÃ‚Â»** | Auditoria | Baixo |
| **E-mail automÃƒÂ¡tico mensal** | Enviar ZIP dia 1 | Alto Ã¢â‚¬â€ Render + SMTP |
| **NF-e devoluÃƒÂ§ÃƒÂ£o mod. 55** | Quando NFC-e passou 30 min e foi devolvida | Alto Ã¢â‚¬â€ emissÃƒÂ£o prÃƒÂ³pria |
| **XML cancelamento (evento)** | Guardar e exportar procEventoNFe | MÃƒÂ©dio Ã¢â‚¬â€ hoje sÃƒÂ³ mudamos status local |

### O que **nÃƒÂ£o** misturar na contabilidade (por enquanto)

- ERP sÃƒÂ©rie **20** (continua no ERP legado; Agro ÃƒÂ© sÃƒÂ©rie **21**).
- RH folha completa (dado sensÃƒÂ­vel Ã¢â‚¬â€ link separado se um dia).
- EdiÃƒÂ§ÃƒÂ£o de lanÃƒÂ§amentos (contador sÃƒÂ³ **exporta**, nÃƒÂ£o lanÃƒÂ§a).

**PrÃƒÂ³ximo passo sugerido (Renan escolhe):** bloco **Resumo + CSV + ZIP melhorado** num ÃƒÂºnico patch na tela atual.
- [ ] **Merge NFC-e Ã¢â€ â€™ `producao`** apÃƒÂ³s OK Renan + checklist `docs/NFCE-PRODUCAO.md`
- [ ] Testes automatizados sync clientes / NFC-e (futuro)

### InstruÃƒÂ§ÃƒÂµes para o assistente (prÃƒÂ³xima atualizaÃƒÂ§ÃƒÂ£o)

**AtualizaÃƒÂ§ÃƒÂ£o automÃƒÂ¡tica (padrÃƒÂ£o Ã¢â‚¬â€ nÃƒÂ£o perguntar ao Renan):**

Ao **entregar** fix, feature ou deploy (teste ou produÃƒÂ§ÃƒÂ£o) Ã¢â€ â€™ **editar `banana.md`**: CHECKPOINT (o quÃƒÂª, commits, `VERSION`, teste OK, produÃƒÂ§ÃƒÂ£o se houver, dica de revert) + Ã‚Â§ do mÃƒÂ³dulo se for regra permanente. **ObrigatÃƒÂ³rio** para qualquer mudanÃƒÂ§a de comportamento do sistema. **NÃƒÂ£o** pedir autorizaÃƒÂ§ÃƒÂ£o. **NÃƒÂ£o** registrar chat sÃƒÂ³ explicativo ou detalhe passageiro.

**Quando Renan pedir *"atualize a banana"* (ou revisÃƒÂ£o explÃƒÂ­cita):**

1. Ler `git log teste --oneline -20` e `git status`.
2. Atualizar seÃƒÂ§ÃƒÂµes **4** (mÃƒÂ³dulos afetados), **8** (linha do tempo) e este **CHECKPOINT**.
3. Incrementar versÃƒÂ£o do checkpoint: patch = pequenos fixes; minor = mÃƒÂ³dulo novo; major = reestruturaÃƒÂ§ÃƒÂ£o do doc.
4. Mover itens de **WIP** para **coberto** apÃƒÂ³s commit.
5. **PendÃƒÂªncias:** manter lista viva Ã¢â‚¬â€ abrir/fechar conforme estado real (nÃƒÂ£o sÃƒÂ³ o que Renan disser na hora).
6. **NÃƒÂ£o** inflar o doc: manter tabelas; detalhe longo vai para doc irmÃƒÂ£o ou AGENTS.md Ã‚Â§7.
7. **Nunca** perguntar ao Renan se deve atualizar o `AGENTS.md`.

### Fim do checkpoint v1.0.77

*PrÃƒÂ³xima ediÃƒÂ§ÃƒÂ£o comeÃƒÂ§a abaixo desta linha ou substituindo o bloco CHECKPOINT acima.*
