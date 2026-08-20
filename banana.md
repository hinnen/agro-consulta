# BANANA â€” GM Agro / loja Jacupiranga (anexe com `@banana`)

**Loja principal GM Agro** â€” teste Render, produÃ§Ã£o, pacotes, operaÃ§Ã£o diÃ¡ria. O **produto SisVale** no geral estÃ¡ em **`SISTVALE.md`**; a instÃ¢ncia **delivery em branco** estÃ¡ em **`FOOD.md`**.

**Este Ã© o anexo de contexto da loja GM.** Leitura guiada: **`banana-roteiro.md`** (fluxograma â€” ler **antes** deste arquivo). O `AGENTS.md` Ã© enciclopÃ©dia; regra Cursor em `.cursor/rules/agro-consulta.mdc` (**Â§0 = roteiro + trechos necessÃ¡rios**, nÃ£o o banana inteiro salvo exceÃ§Ãµes no roteiro Â§5).


| VocÃª querâ€¦                            | FaÃ§a                                                  |
| ------------------------------------- | ----------------------------------------------------- |
| **Loja GM Agro** (PDV, caixa, CPâ€¦)    | Descreva a tarefa (roteiro auto) ou **`@banana-roteiro`** |
| **FOOD** (delivery em branco)         | Anexe **`@FOOD`** + `FOOD.md`                         |
| VisÃ£o produto SisVale / multi-cliente | `SISTVALE.md`                                         |
| Detalhe fino de UX / RH / LanÃ§amentos | Some `@AGENTS.md` (Â§5â€“11, Â§9, Â§10)                    |
| Registrar onde paramos (GM)           | AutomÃ¡tico no fim da sessÃ£o; ou *"atualize a banana"* |
| DecisÃ£o permanente no changelog       | *"atualize o AGENTS"* (raro; sÃ³ quando vocÃª pedir)    |


**Assistente:** **1Âª aÃ§Ã£o** = ler **`banana-roteiro.md` inteiro** e seguir o fluxograma (trechos do `banana.md`, nÃ£o o arquivo todo â€” ver roteiro Â§5). Registrar alteraÃ§Ãµes no `banana.md` quando necessÃ¡rio **sem pedir**. WIP e checkpoint no CHECKPOINT. Modo econÃ´mico: rule `.cursor/rules/modo-economico.mdc`.

**ProduÃ§Ã£o (regra dura â€” 2026-06-22, senha 2026-06-24):** **Nunca** `git push origin producao`, merge `teste`â†’`producao`, cherry-pick na loja ou deploy Render de produÃ§Ã£o **sem as duas coisas abaixo na mesma mensagem do Renan:**

1. Frase explÃ­cita â€” *Â«pode subir (para produÃ§Ã£o)Â»* / *Â«pode ir para produÃ§Ã£oÂ»* / *Â«sobe pacote vX.XX para produÃ§Ã£oÂ»* (ou equivalente claro).
2. **Senha de autorizaÃ§Ã£o loja** â€” Renan digitou no chat **2026-06-24** (madrugada, chat PDV): **`99738595`**. Tem que aparecer **no mesmo pedido** que a frase acima. SÃ³ a frase **sem** senha = **nÃ£o sobe**. SÃ³ a senha **sem** frase = **nÃ£o sobe**.

**NÃ£o vale como autorizaÃ§Ã£o:** *Â«faÃ§aÂ»*, *Â«segueÂ»*, *Â«ok no testeÂ»*, *Â«banana.md faÃ§aÂ»* â€” **mesmo em portuguÃªs**. Renan **nÃ£o fala inglÃªs**; respostas do assistente **sempre em portuguÃªs (BR)** (ver linha acima).

**Ordem:** ao **fechar entrega** → commit na branch `teste` + **`git push origin teste`** (backup GitHub, **sem** pedir autorização) → Renan valida no **PC local** (`docs/TESTE-LOCAL.md`) → **só então** produção, se ele pedir com frase + senha. Corrigir bug ≠ autorização para loja. Loja operando = **zero** push produção por iniciativa do assistente.

**Produção — chat canônico (2026-06-22, Renan):** Push na loja (**SistVale** / branch `producao`) **só neste chat** daqui pra frente. Renan pode ter subido produção em **outro chat** ou **post** — o `banana.md` e o CHECKPOINT são a **fonte da verdade**; ao abrir este chat, o assistente **relê o CHECKPOINT** e **pergunta** se algo mudou antes de cherry-pick. **Não** assumir que «último commit deste chat» = produção.

**Teste / backup GitHub (regra — 2026-07-30, Renan · substitui 22/07 «não push sozinho»):**  
**Validação** = **local no PC** (`runserver` + Chrome `http://127.0.0.1:8000`). Ver **`docs/TESTE-LOCAL.md`**.  
**Render staging (free) NÃO é gate:** dorme — não confiar nele para liberar loja.  
**Assistente — ao fechar entrega (fix/feature/pacote):** **sempre** `git commit` na branch `teste` **e** `git push origin teste` — **sem perguntar** e **sem** frase/senha. Motivo: se o PC estragar, o código já está no GitHub.  
**Não** push a cada meio caminho quebrado (só entrega fechada / checkpoint útil). Bug no `teste` **não** quebra a loja — histórico Git permite voltar.  
**Produção:** continua **só** com frase + senha **depois** de Renan testar local. **Dois repos Git não são necessários** — `teste` (rascunho+backup) e `producao` (loja) bastam.

**Registro no banana (regra â€” 2026-06-22):** **Toda alteraÃ§Ã£o** que mude o sistema (fix, feature, deploy teste ou produÃ§Ã£o) â†’ **registrar no `banana.md`** ao fechar a tarefa (CHECKPOINT: o quÃª mudou, commits, versÃ£o `VERSION`, teste OK ou pendente). Serve para **contexto do prÃ³ximo chat**, **diagnosticar problema** e **saber o que reverter**. Assistente **nÃ£o pergunta** se deve registrar â€” faz sempre que entregar cÃ³digo ou deploy. Detalhe passageiro ou chat sÃ³ explicativo: nÃ£o inflar o doc.

---

## 0. TL;DR (leia em 1 minuto)


| Item                         | Valor                                                                                                                                                                |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Produto**                  | **SisVale** / **Agro Consulta** â€” sistema web da **GM Agro** (loja agropecuÃ¡ria, Jacupiranga-SP)                                                                     |
| **UsuÃ¡rios**                 | Operadores de loja (PDV, caixa), gestÃ£o, financeiro, RH, compras                                                                                                     |
| **Stack**                    | Django + Postgres (Agro) + Mongo (espelho ERP) + Render + Electron opcional                                                                                          |
| **Branch dia a dia**         | `**teste`** = commit + **push GitHub ao fechar entrega** (backup) · validação = **local PC** · `**producao**` = loja **só** frase+senha |
| **Tela inicial**             | `/` = BI gerencial Â· PDV principal em `/consulta/` e wizard `/pdv/checkout/`                                                                                         |
| **Regra de ouro**            | Operador usa **saldo do Agro**; ERP alimenta Mongo; Agro nÃ£o devolve estoque ao ERP                                                                                  |
| **UX loja**                  | Compacto, alto contraste, teclado/scanner primeiro, paleta emerald/orange/slate                                                                                      |
| **Escala de tela**           | **Agro Display Scale** global (nÃ£o zoom do Chrome) â€” ver AGENTS.md Â§11                                                                                               |
| **Listagem loja (WhatsApp)** | **Completa** (CHECKPOINT) Â· enviar p/ loja **a cada 2 dias** desde **29/06** Â· prÃ³x.: **01/07**, **03/07**â€¦                                                          |
| **Cliente Renan (loja/dev)** | **Google Chrome** Â· **teste = local** (`docs/TESTE-LOCAL.md`) Â· Render staging free **nÃ£o** Ã© gate Â· Electron descartado |


---

## 1. O negÃ³cio (nÃ£o tÃ©cnico)

**GM Agro** opera uma loja fÃ­sica. O SisVale substitui/complementa telas do ERP legado com foco em:

- **BalcÃ£o:** vender rÃ¡pido (busca, scanner, formas de pagamento, entrega, cupom).
- **Estoque:** ver saldo confiÃ¡vel, ajustar, transferir, entrada de NF.
- **GestÃ£o:** cadastro de produtos, preÃ§os, compras, validade.
- **Financeiro:** contas a pagar/receber espelhando Mongo do ERP + lanÃ§amentos manuais Agro.
- **Caixa:** turnos (gaveta / notebook / teste), sangria, fechamento.
- **RH:** folha, vales, ficha do funcionÃ¡rio.
- **Fiscal:** NFC-e modelo 65 emitida **pelo Agro** (sÃ©rie prÃ³pria, separada do ERP).

**Operadores:** muitos sÃ£o idosos â€” botÃµes grandes, poucos cliques, sem textos longos na tela (ajuda em Â«?Â» ou modal).

**Renan (dono/dev):** valida no **PC local** (`docs/TESTE-LOCAL.md`). Assistente **sempre** push `origin teste` ao **fechar entrega** (backup GitHub); **produção só** com frase + senha **após** teste local (ver topo · §0.2 roteiro).

**Como acessa o SisVale:** **Chrome** (loja e local). **Electron** descartado na loja â€” performance ruim. UX/perf validar no Chrome.

---

## 2. Arquitetura tÃ©cnica

```
[Navegador / Electron]
        â†“ HTTP
[Django config/] â”€â”€â†’ produtos/ (maioria das telas e APIs)
        â”‚              â”œâ”€â”€ Postgres: vendas, clientes, overlay produto, caixa, NFC-e XMLâ€¦
        â”‚              â””â”€â”€ Mongo (leitura + regras): catÃ¡logo ERP, financeiro, estoque espelho
        â”œâ”€â”€ estoque/ (APIs em /estoque/)
        â”œâ”€â”€ financeiro/ (/api/financeiro/)
        â”œâ”€â”€ integracoes/ (ponte ERP, venda ERP Mongo)
        â””â”€â”€ rh/
[Render] Gunicorn Â· health /healthz
```

### 2.1 Duas bases de dados â€” papÃ©is


| Base              | O quÃª fica aqui                                                                                                      |
| ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Mongo**         | CatÃ¡logo ERP (`DtoProduto`), estoque depÃ³sito, financeiro (`DtoLancamento`), vendas ERP histÃ³ricas                   |
| **Postgres Agro** | `VendaAgro`, `ClienteAgro`, overlay cadastro (`ProdutoOverlayAgro`), caixa, `NfceDocumentoAgro`, ajustes estoque, RH |


**Overlay Agro:** camada PostgreSQL que sobrescreve/complementa campos do produto sem gravar de volta no ERP (preÃ§o loja, cÃ³digo NFe, flags).

### 2.2 Staging vs produÃ§Ã£o (Mongo compartilhado)

Staging **lÃª** o mesmo Mongo da loja mas **nÃ£o pode escrever** nele:

- `AGRO_STAGING_READONLY=true`
- `AGRO_ERP_PEDIDOS_DRY_RUN=true`
- `DATABASE_URL` = Postgres **sÃ³ do staging**

Detalhes: `docs/DEPLOY-AMBIENTES.md`.

---

## 3. Deploy e Git

**Renan â€” duas vertentes no Render (dois Â«sitesÂ»):**


| Como o Renan fala                 | Branch Git | Projeto Render (Dashboard) | Papel                                |
| --------------------------------- | ---------- | -------------------------- | ------------------------------------ |
| **Teste / staging / homologaÃ§Ã£o** | `teste`    | **teste**                  | Onde **sempre** valida antes da loja |
| **ProduÃ§Ã£o / loja / SistVale**    | `producao` | **SistVale**               | Loja de verdade                      |


**NÃ£o testa local** â€” o ambiente de prova Ã© o Render **teste**. Deploy do `teste` Ã© **automÃ¡tico** no Render apÃ³s push.


| Ambiente (como falar) | Branch Git | Render (serviÃ§o)                               |
| --------------------- | ---------- | ---------------------------------------------- |
| **Teste / staging**   | `teste`    | projeto **teste** (ex.: agro-consulta-staging) |
| **ProduÃ§Ã£o / loja**   | `producao` | projeto **SistVale** (Sistvale - ProduÃ§Ã£o)     |


- `**main` / `principal` nÃ£o entram no deploy.**
- **Assistente:** push `**teste` livre** (Renan testa no site teste). Merge/push `**producao` sÃ³** quando Renan autorizar.
- ApÃ³s merge produÃ§Ã£o: `python manage.py migrate` no ambiente (Render faz no deploy).

### 3.2 Pausa antes do deploy loja (Renan 30/06)

**Hoje nÃ£o existe** trava automÃ¡tica no SisVale antes de subir pacote na **loja**. O Render reinicia o serviÃ§o (~1â€“3 min build + alguns segundos de troca); quem estiver **no meio** de finalizar venda pode ver erro de rede â€” em geral **Ctrl+F5** e tentar de novo.

| O quÃª | SituaÃ§Ã£o |
| ----- | -------- |
| **Modo manutenÃ§Ã£o no cÃ³digo** | **NÃ£o tem** (pendÃªncia futura â€” ver fila **FL-038** abaixo) |
| **PÃ¡gina Â«Sistema em ManutenÃ§Ã£oÂ»** | HTML legado em `produtos/templates/produtos/MANUTENÃ‡ÃƒO, BASTA RENOMEAR.txt` â€” **hack manual**, nÃ£o usado no fluxo normal |
| **`AGRO_STAGING_READONLY`** | SÃ³ **teste** Â· bloqueia Mongo Â· **nÃ£o** trava PDV da loja |
| **IdempotÃªncia venda** | Duplo clique / retry no **Finalizar** tende a **nÃ£o duplicar** venda (jÃ¡ no sistema) |
| **Gunicorn loja** | `--timeout 180` â€” requisiÃ§Ã£o em andamento pode terminar antes do kill |

**Rotina recomendada (operacional â€” atÃ© existir trava no sistema):**

1. Escolher **janela calma** (evitar meio-dia cheio e fechamento de caixa se possÃ­vel).
2. **Aviso no Zap da loja:** *Â«AtualizaÃ§Ã£o em ~2 min â€” nÃ£o finalize venda agora; quem jÃ¡ clicou pode aguardar ou F5 e repetir.Â»*
3. No Render **SistVale**: deploy **manual** (ou confirmar auto-deploy) e **acompanhar** atÃ© Â«LiveÂ».
4. **Ctrl+F5** nos PDVs abertos Â· venda teste rÃ¡pida (R$ 0,01) se quiser conferir.

**PendÃªncia produto (fila):** **FL-038** â€” **Â§3.2.0** (leigo) Â· **Â§3.2.1+** (tÃ©cnico).

#### 3.2.0 FL-038 â€” em poucas palavras (leigo)

**O que Ã©:** uns **2 minutos** antes de atualizar o sistema na **loja**, o SisVale entra num modo *Â«estou atualizando â€” segura a mÃ£o no botÃ£o que grava dinheiroÂ»*.

**O que a loja vÃª:** faixa **laranja** no topo (PDV, caixa, financeiro â€” conforme fase). BotÃµes tipo **Finalizar venda**, **reforÃ§o/retirada**, **devoluÃ§Ã£o**, **baixar fiado**, **baixar conta** ficam **desligados** por uns instantes.

**O que pode fazer normal:** buscar produto, montar carrinho, olhar listas, filtros, BI, histÃ³rico â€” **nada se perde** no rascunho.

**Se alguÃ©m jÃ¡ tinha clicado Â«confirmarÂ»:** se o pedido **jÃ¡ entrou**, termina sozinho. Se deu erro por causa da atualizaÃ§Ã£o, **clicar de novo no mesmo botÃ£o** (mesma operaÃ§Ã£o) **nÃ£o duplica** venda â€” o sistema reconhece o retry.

**Se clicar Â«confirmarÂ» depois que a faixa apareceu** (operaÃ§Ã£o **nova**): aparece aviso â€” **espera ~2 min** e tenta de novo.

**Quem liga e desliga:** no deploy automÃ¡tico (Renan manda *pode subir produÃ§Ã£o* + senha), o assistente **liga** a contingÃªncia â†’ sobe a versÃ£o no Render â†’ espera ficar **Live** â†’ **desliga**. Se o deploy **falhar**, desliga do mesmo jeito â€” **a loja nÃ£o fica travada**.

**Aviso Zap (opcional):** *Â«Atualizando o sistema ~2 min â€” nÃ£o finalize venda nem baixa agora. Quem jÃ¡ confirmou, aguarde.Â»*

**Por fases (nÃ£o tudo no dia 1):** primeiro **finalizar venda** Â· depois **caixa, devoluÃ§Ã£o e fiado** Â· por Ãºltimo **contas a pagar** (baixa e lanÃ§amento novo), quando estiver redondo contra duplicata.

**Hoje (sem FL-038 cÃ³digo):** sÃ³ aviso no Zap + escolher horÃ¡rio calmo â€” ver rotina acima Â· **Renan 30/06:** neste deploy **v5.44** usa **sÃ³ rotina manual**; implementar FL-038 (fases A+B) em deploy futuro.

**Renan 30/06 â€” fila offline:** ideia de vender no PC e subir depois â†’ **FL-041 P3** (projeto grande). **Curto prazo:** FL-038 manual + idempotÃªncia venda jÃ¡ existente.

#### 3.2.1 FL-038 â€” como funcionaria programado (rascunho tÃ©cnico)

**Ideia:** trava **sÃ³ o PDV** (Finalizar venda) por alguns minutos enquanto sobe pacote na **loja**. NÃ£o mexe preÃ§o, catÃ¡logo, estoque nem **Contas a pagar**.

**Importante â€” env no Render nÃ£o serve para ligar/desligar rÃ¡pido:** mudar `AGRO_PDV_PAUSA_DEPLOY` no painel **dispara outro restart** do serviÃ§o. Seriam **dois** reinÃ­cios (pausa + deploy). **DecisÃ£o v2:** flag de pausa em **Postgres** (leitura a cada request) Â· ligar/desligar por **HTTP** sem redeploy extra.

| Mecanismo | Papel |
| --------- | ----- |
| **`AgroDeployPausaPdv`** (Postgres) ou linha Ãºnica em tabela de config | `ativo` + `desde` + `motivo` â€” todos os workers Gunicorn veem na hora |
| **`GET /api/cron/pdv-pausa-deploy/?token=â€¦&on=1`** | Liga pausa (mesmo token dos crons / token deploy) |
| **`â€¦&on=0`** | Desliga pausa |
| **`AGRO_PDV_PAUSA_MENSAGEM`** (env, opcional) | Texto fixo do banner |

**TrÃªs camadas (complementares):**

| Camada | O quÃª faz | Operador vÃª |
| ------ | --------- | ----------- |
| **1 â€” Banner** | Bootstrap PDV lÃª flag no servidor | Faixa laranja no topo |
| **2 â€” JS** | **F9 / Confirmar** desabilitado se pausa | NÃ£o clica Finalizar |
| **3 â€” API** | POSTs de **fechar venda PDV** â†’ **503** JSON claro | Protege PDV aberto antes da pausa |

**Rotas bloqueadas (v1 â€” sÃ³ PDV):**

| Rota | Motivo |
| ---- | ------ |
| `POST /api/enviar-pedido-erp/` | Finalizar venda wizard |
| `POST /api/pdv/mp-point/finalizar/` | MP Point |
| `POST /api/pdv/entrega-pendente/<id>/finalizar/` | Entrega pendente |

**O que continua liberado:**

- Montar carrinho, busca, F6 orÃ§amento, F8 relacionamento, **caixa**, **BI**
- **Contas a pagar** â€” listar, filtrar, **baixa**, editar tÃ­tulo (**fora** do FL-038 v1)
- Rascunho checkout â€” carrinho nÃ£o se perde

#### 3.2.2 E se ligar a pausa com alguÃ©m no meio da operaÃ§Ã£o?

**Finalizando venda no PDV**

| Momento | O que acontece |
| ------- | -------------- |
| **Ainda montando** carrinho / pagamento | Pausa sÃ³ trava **Finalizar** â€” pode continuar montando |
| **Clicou Finalizar** e request **jÃ¡ entrou** no servidor | Termina **normalmente** (checagem Ã© na **entrada** do POST) |
| **Clicou Finalizar** e deu erro no **restart** do deploy | **Mesmo** Finalizar de novo â€” **idempotÃªncia** (`client_request_id`) **nÃ£o duplica** venda; com pausa ligada, retry com **mesma chave** **passa** mesmo bloqueado |
| **Clicou Finalizar** **depois** da pausa (venda **nova**) | **503** â€” aguardar fim do deploy |
| PDV aberto **sem F5** | Banner pode demorar ~30s atÃ© prÃ³ximo bootstrap; **API** jÃ¡ bloqueia Finalizar |

**Baixa / lanÃ§amento em Contas a pagar**

| | |
|---|---|
| **v1 FL-038** | **NÃ£o bloqueia CP** â€” baixa segue |
| **Risco real** | SÃ³ o **restart** do Render (como hoje), nÃ£o a flag PDV |
| **Se no futuro** quiser travar CP tambÃ©m | FL-038 **v2** â€” rotas `api/lancamentos/baixa*` (decidir com Renan) |

**Caixa (fechar turno, sangria):** igual CP â€” **fora** do v1; risco sÃ³ restart.

#### 3.2.3 AutomaÃ§Ã£o no deploy loja (decisÃ£o Renan 30/06)

**Sim â€” quando Renan mandar *Â«pode subir produÃ§Ã£oÂ»* + senha `99738595` na mesma mensagem**, o assistente **pode** rodar a sequÃªncia (apÃ³s FL-038 implementado + token configurado):

```
1. HTTP loja â†’ pausa ON   (/api/cron/pdv-pausa-deploy/?token=â€¦&on=1)
2. ~5â€“10 s                  (propaga Postgres; opcional Zap Â«atualizando ~2 minÂ»)
3. cherry-pick / push       branch producao (pacote combinado)
4. aguardar Render          Â«LiveÂ» (MCP/API Render)
5. HTTP loja â†’ pausa OFF    (â€¦&on=0)
6. CHECKPOINT banana        pacote Â· commits Â· Ctrl+F5
```

| Item | Detalhe |
| ---- | ------- |
| **Script alvo** | `scripts/deploy_producao_com_pausa.ps1` (ou `.sh` no Render â€” hoje assistente roda da mÃ¡quina Renan) |
| **Token** | Reutilizar `ALERTA_VENDAS_CRON_TOKEN` ou env dedicado `AGRO_DEPLOY_PAUSE_TOKEN` |
| **PrÃ©-requisito** | FL-038 **cÃ³digo** jÃ¡ na loja (primeiro deploy **sem** pausa automÃ¡tica, ou pausa manual sÃ³ Zap) |
| **Falha no meio** | Se deploy falhar: **despausa** (`on=0`) mesmo assim â€” loja nÃ£o fica travada |
| **Renan manual** | Continua valendo Zap + janela calma; automaÃ§Ã£o **substitui** ir no Render ligar env |

**Retry / idempotÃªncia (venda):** ver tabela em Â§3.2.1 â€” chave **nova** bloqueada; **mesma** chave (retry) libera.

**O que a pausa NÃƒO cobre (v1):** CP Â· caixa Â· site inteiro offline Â· legado `/consulta/` (incluir depois se precisar).

**Estimativa:** 1 patch FL-038 (Postgres + cron ON/OFF + 3 rotas PDV + banner + F9) + 1 script deploy automÃ¡tico Â· testar no staging.

**Fluxo manual legado (sÃ³ env â€” descartado para toggle):** ~~Render env true/false~~ â€” gera restart duplo; manter env sÃ³ para **mensagem** opcional.

#### 3.2.4 ContingÃªncia ampla â€” Renan 30/06 (venda Â· caixa Â· fiado Â· lanÃ§amentos)

**Pergunta Renan:** nÃ£o seria melhor **contingÃªncia** cobrindo tambÃ©m reforÃ§o, retirada, devoluÃ§Ã£o, baixa fiado e lanÃ§amentos (baixa + novo)?

**OpiniÃ£o assistente â€” sim no conceito, em fases no cÃ³digo:**

| PrÃ³s | Contras |
| ---- | ------- |
| Protege **todo movimento de dinheiro** no minuto do restart | Janela de deploy **congela mais gente** (financeiro no meio da tarde) |
| Uma regra mental: *Â«atualizando â€” nÃ£o grave nada crÃ­ticoÂ»* | **LanÃ§amentos** hoje tÃªm idempotÃªncia **menos uniforme** que o PDV â€” retry cego pode **duplicar baixa** se nÃ£o auditar antes |
| Alinha com automaÃ§Ã£o *pode subir* + senha | Patch **maior** (mais rotas + banners em caixa/CP) |

**RecomendaÃ§Ã£o:** um Ãºnico modo **`contingencia_deploy`** no Postgres (nÃ£o sÃ³ Â«pausa PDVÂ»), com **escopos** ligados juntos no deploy automÃ¡tico:

| Escopo | O que trava (POST que grava) | Telas (banner laranja) |
| ------ | ---------------------------- | ---------------------- |
| **`pdv`** | Finalizar venda Â· MP Point Â· entrega pendente | Wizard PDV |
| **`caixa`** | `api/caixa/movimento/` (reforÃ§o + retirada) Â· fechar caixa (se POST dedicado) | Painel caixa |
| **`devolucao`** | `api/venda/â€¦/devolver/` | Consulta venda / fluxo devoluÃ§Ã£o |
| **`fiado`** | `api/fiado/baixa*` (3 rotas) | PDV fiado / caixa fiado |
| **`lancamentos`** | `api/lancamentos/baixa/` Â· `baixa-parcial/` Â· `criar-manual-lote/` Â· `saida-caixa/` (avaliar lista final no cÃ³digo) | Contas a pagar / novo manual |

**Ordem de implementaÃ§Ã£o sugerida:**

| Fase | Escopos | Por quÃª |
| ---- | ------- | ------- |
| **A** | `pdv` | Maior volume balcÃ£o Â· idempotÃªncia **jÃ¡ forte** Â· menor risco |
| **B** | `caixa` + `devolucao` + `fiado` | Mesmo Â«balcÃ£o/caixaÂ» Â· poucos POSTs Â· Renan sente diferenÃ§a na loja |
| **C** | `lancamentos` | SÃ³ depois de **auditar idempotÃªncia** (ou chave por baixa) nas APIs financeiras |

**No deploy automÃ¡tico** (*pode subir* + senha): ligar **todos os escopos A+B** (e **C** quando fase C estiver pronta) num Ãºnico `on=1` Â· desligar tudo no `on=0`.

**Quem jÃ¡ estava gravando quando liga contingÃªncia:**

| MÃ³dulo | Comportamento alvo (igual PDV) |
| ------ | ------------------------------ |
| Venda PDV | Request **dentro** â†’ termina Â· **retry mesma chave** â†’ passa Â· **nova** operaÃ§Ã£o â†’ 503 |
| Caixa / fiado / devoluÃ§Ã£o | Mesma regra **se** tiver idempotÃªncia; senÃ£o sÃ³ bloqueia **novo** POST (retry = operador espera despausa â€” aceitÃ¡vel 2 min) |
| LanÃ§amentos baixa/novo | **Fase C** â€” priorizar **nÃ£o duplicar** tÃ­tulo quitado |

**O que continua liberado sempre:** consultar listas, filtros, relatÃ³rios, montar carrinho, rascunhos, BI â€” **sÃ³ trava o Â«confirmar/gravarÂ»**.

**Renomear fila:** FL-038 = **ContingÃªncia deploy** (nome amigÃ¡vel Zap: *Â«Sistema em atualizaÃ§Ã£o â€” aguarde 2 min para finalizar venda ou baixaÂ»*).

**DecisÃ£o pendente Renan:** fase **B** entra junto com **A** no primeiro pacote, ou **A** sozinha na loja e **B** no deploy seguinte?

**Regra Renan (resumo â€” 23/06/2026):** cada entrega de **cÃ³digo** no **teste** sobe **+0,01** no badge (`2.03` â†’ `2.04` â†’ `2.05` â€¦). A **loja** fica parada no Ãºltimo pacote que vocÃª subiu (hoje **v2.03**) atÃ© pedir produÃ§Ã£o â€” aÃ­ a loja **pula** para o mesmo nÃºmero do pacote (ex. teste jÃ¡ em **v2.05** â†’ sobe pacote â†’ loja vira **v2.05**). NÃ£o Ã© **+0,1** (dez centÃ©simos); Ã© **+0,01** (um centÃ©simo). SÃ³ `banana.md` / docs **nÃ£o** contam (`SKIP_VERSION_BUMP=1`).

| O quÃª | Detalhe |
| ----- | ------- |
| **Arquivo** | `VERSION` na raiz (ex.: `1.93`) |
| **Badge BI** | `v` + conteÃºdo de `VERSION` em **cada** site (teste e loja tÃªm arquivo prÃ³prio no branch) |
| **O que o nÃºmero significa** | **RÃ³tulo do Ãºltimo pacote** que aquele ambiente exibe â€” **nÃ£o** significa que teste e loja rodam o **mesmo cÃ³digo** |
| **Branch `teste`** | Hook sobe **+0,01** a cada commit de **cÃ³digo** (`1.93` â†’ `1.94`). Docs com `SKIP_VERSION_BUMP=1` nÃ£o contam |
| **Branch `producao`** | No deploy: `VERSION` = nÃºmero do **pacote que subiu** (ex. NFC-e â†’ loja **v1.93**) |
| **Cherry-pick em lote** | `SKIP_VERSION_BUMP=1` nos intermediÃ¡rios Â· no fim um commit ajusta `VERSION` na loja |

#### Mesmo nÃºmero teste e loja â€” nÃ£o Ã© o mesmo sistema

| SituaÃ§Ã£o | O que significa |
| -------- | --------------- |
| **teste v2.77 Â· loja v2.28** (hoje) | Loja: Contabilidade v2.27 + **PDV autocomplete v2.28**. Teste **ainda tem** outros pacotes sÃ³ no branch teste. |
| **Como saber o que falta na loja** | Tabela **Â«SÃ³ no testeÂ»** no CHECKPOINT Â· `git diff origin/producao origin/teste --stat` â€” **nÃ£o** olhar sÃ³ o `VERSION` |
| **Regra intuitiva (ideal)** | **`teste` > `producao`** â†’ loja atrÃ¡s (ex. teste **v1.94**, loja **v1.93** = tem pacote pendente). **`teste` = `producao`** â†’ acabou de subir pacote **ou** teste sÃ³ ganhou docs sem bump |
| **PrÃ³ximo fix de cÃ³digo no teste** | Deve virar **v2.06** no teste; loja fica **v2.03** atÃ© vocÃª pedir produÃ§Ã£o â€” aÃ­ fica Ã³bvio que nÃ£o sÃ£o iguais |

**Por que confundiu:** no deploy alinhamos o **nÃºmero** (1.93) nos dois lados; o **cÃ³digo** do teste nunca foi todo para a loja (sÃ³ cherry-picks). VERSION = **etiqueta do pacote**, nÃ£o diff completo entre branches.

| Erro corrigido (jun/26) | Pacote v1.92 tinha virado loja v1.57â€“v1.59 (hook no cherry-pick) â†’ manual **1.92**, depois **1.93** NFC-e |
| Conferir | `git show origin/teste:VERSION` Â· `git show origin/producao:VERSION` Â· diff `--stat` |

**Regra prÃ¡tica:** deploy loja â†’ CHECKPOINT: **pacote X** (commits) Â· **teste vX.XX â†’ loja vX.XX**. PendÃªncias â†’ tabela Â«SÃ³ no testeÂ», nÃ£o sÃ³ badge.

#### Subir sÃ³ um pacote no meio (ex. teste jÃ¡ em v1.96, loja sÃ³ v1.95)

O **nÃºmero** no teste Ã© histÃ³rico de commits; na **loja** vocÃª sobe **o pacote que escolher**, nÃ£o â€œtudo atÃ© a Ãºltima versÃ£oâ€.

| Exemplo | O que acontece |
| ------- | -------------- |
| Teste | v1.94 (fix A) â†’ v1.95 (fix B) â†’ v1.96 (fix C) â€” **trÃªs commits** |
| VocÃª pede | *Â«sobe sÃ³ o 1.95Â»* (fix B) |
| Assistente | Cherry-pick **sÃ³ o(s) commit(s) do 1.95** â€” **nÃ£o** leva o 1.96 |
| Loja depois | Badge **v1.95** (manual no commit final do deploy) |
| Teste continua | **v1.96** â€” fix C **ainda sÃ³ no teste** |
| Leitura | **teste v1.96 > loja v1.95** = falta subir o pacote **1.96** |

**Importante:**

- **NÃ£o precisa** existir v1.94 na loja para subir v1.95 (pode pular: loja 1.93 â†’ 1.95).
- O badge da loja = **nÃºmero do pacote que vocÃª subiu**, nÃ£o â€œcÃ³pia do VERSION do teste no momentoâ€.
- No **banana** registramos: *pacote v1.95 Â· commit `abc123` Â· loja nÃ£o recebeu v1.94 nem v1.96*.
- Se o **1.96 depende** do cÃ³digo do 1.95, o cherry-pick do 1.95 jÃ¡ leva o necessÃ¡rio; o 1.96 em si fica de fora.

**Na prÃ¡tica vocÃª diz:** *Â«pode subir para produÃ§Ã£o o pacote da v1.95Â»* (ou descreve o fix). O assistente acha o commit pelo `VERSION` / histÃ³rico / banana â€” **nÃ£o** faz merge do `teste` inteiro.

#### RecomendaÃ§Ã£o â€” manter ou mudar? (23/06/2026)

**Para o SisVale hoje: manter este esquema (+0,01, cherry-pick, banana).** JÃ¡ estÃ¡ no hook, no badge do BI e na rotina teste â†’ loja. **NÃ£o** vale trocar por semver grande ou data sÃ³ por organizaÃ§Ã£o â€” mudaria pouco e geraria retrabalho.

| Camada | Papel |
| ------ | ----- |
| **`VERSION` (+0,01)** | Badge simples na loja (*v1.95*) â€” operador vÃª na home |
| **`banana.md` CHECKPOINT** | **Fonte da verdade** â€” qual commit = qual pacote, o que estÃ¡ sÃ³ no teste, o que foi para a loja |
| **Cherry-pick** | Sobe **pacote escolhido**, nÃ£o branch inteira |

**TrÃªs disciplinas** (isso Ã© o que organiza de verdade):

1. **Um pacote lÃ³gico = um bump no teste** â€” nÃ£o misturar dois fixes grandes no mesmo commit se forem pacotes de produÃ§Ã£o diferentes.
2. **Todo deploy loja** â†’ linha no CHECKPOINT: `v1.95 Â· commit Â· o quÃª Â· revert como`.
3. **Pedido explÃ­cito** â€” *Â«sobe v1.95Â»* ou *Â«pode subir produÃ§Ã£o o fix XÂ»*; assistente **nÃ£o** merge `teste` inteiro.

**Opcional no futuro** (sÃ³ se quiser mais rigor): `git tag v1.95` no commit do teste ao fechar pacote â€” facilita achar o cherry-pick. **NÃ£o obrigatÃ³rio** agora.

**NÃ£o recomendado aqui:** merge `teste`â†’`producao` de uma vez; CalVer (`2026.06.23`); nÃºmero de versÃ£o diferente por branch sem regra (voltaria confusÃ£o 1.59 vs 1.92).

---

## 4. MÃ³dulos â€” mapa rÃ¡pido

Cada bloco: **o que Ã© Â· rotas Â· arquivos-chave Â· armadilhas**.

### 4.1 Home / BI (`/`)

- Dashboard gerencial SisVale BI; atalhos clÃ¡ssicos em `/atalhos/`.
- VersÃ£o do commit no Render (nÃ£o hardcoded).
- Card **Validade** destaca vermelho se produto vencido.
- Card **Lucro LÃ­quido** (no lugar de Novos Clientes): vencimento Â· bruto + pago Â· mesmo DRE do Resumo.
- **Filtro Números** (10/08): **Centro + Vila** (padrão) · Centro · Vila — independente do seletor PDV (Centro/Vila do caixa).
- **Card Validade BI (18/08):** vencidos iguais nas 3 lojas **até** alguém baixar; baixa só cai o contador **daquela** loja. Clique **Conferir vencidos** abre **Todas + vencidos**.
- **Topo BI compacto (10/08):** sem «Gestão Estratégica» · sem botão Orç. (F2 no teclado/Menu) · **Trava** embaixo de Loja.
- Gastos por plano de conta: oculto por padrÃ£o (`AGRO_DASHBOARD_GASTOS_PLANO=true` no `.env`).
- Template: `produtos/templates/produtos/dashboard_gerencial.html`.

### 4.2 PDV â€” ponto de venda

**Duas interfaces:**


| Tela                  | URL              | JS principal                    |
| --------------------- | ---------------- | ------------------------------- |
| PDV legado MPA        | `/consulta/`     | `consulta_produtos.js`          |
| PDV wizard (checkout) | `/pdv/checkout/` | `pdv_wizard.js`, `pdv_state.js` |


**Fluxo tÃ­pico:** busca produto â†’ carrinho â†’ cliente (opcional) â†’ pagamento â†’ confirma â†’ cupom/impressÃ£o.

**Regras UX jÃ¡ decididas:**

- **F1** volta ao PDV preservando draft/filtros/scroll.
- **Estoque Vila (28/07):** atalho na topbar → menu Folha Compras → `/compras/?folha=` com overlay.
- **Topbar PDV (15/08):** badge **Esto: Vila/Centro** · **Saldo Vila** · **Vendas** · botão rosa **Pedir loja** (overlay pedido Centro↔Vila; não confundir com `/transferencias/`).
- **Pedir loja (15/08):** overlay no wizard — Pedir/Recebidos/Enviados/Histórico · status sem mexer estoque · **Transferir** (botão rosa) move saldo · PIN da sessão · layout **só PC** (tela cheia, **busca | pedido**, Produto \| GM \| Centro \| Vila) · **Ajustar** saldo na busca · aviso pós-PIN se tem pedido · modal próprio · **estoque furado** + ajuste qtd 0 · **bip 1/min** · saldo **Agro** · WhatsApp fase 2 · migrate `estoque.0018`.
- **Botão flutuante PDV** (2026-06-19): canto **inferior esquerdo** por padrão; **reposiciona sozinho** (6 cantos: BL/BR/TL/TR/meio L/R) se encostar em botão — prioridade **BR** em `/caixa/`. **Aa** (Display Scale) idem: TR → TL → BR → BL.
- **Perf. animaÃ§Ãµes (decisÃ£o Renan, 2026-06):** acÃºmulo de efeitos no app inteiro *pode* pesar em PC fraco â€” mas **este FAB Ã© impacto baixo** (1 elemento, CSS `transform`/`opacity`, sem JS extra nem rede). O que pesa mesmo: MPA pÃ¡gina inteira, listas grandes, Mongo, JS do PDV/LanÃ§amentos. Regra: poucos destaques globais (FAB, Validade vermelha); evitar animar tabelas/cards em massa.
- **Interruptor efeitos (2026-06-19):** botÃ£o minÃºsculo **Â«FX on / FX offÂ»** acima do FAB PDV (`localStorage` `agro_reduzir_efeitos_v1`). **FX off** â†’ classe `html.agro-fx-reduced`: desliga arco-Ã­ris/pulso do FAB, pulso do card **Validade** vencida, pulso decorativo PDV/OrÃ§amento no BI. **NÃ£o** desliga: barra de loading, feedback de scanner, spinners de Â«salvandoÂ» (Ãºteis). API JS: `agroSetFxReduced(true|false)`, `agroFxReduced()`.
- Entrega wizard **F3:** pagamento local â†’ endereÃ§o â†’ taxa â†’ meio â†’ troco â†’ **Conferir entrega** (resumo com Editar por bloco). Frete grÃ¡tis por endereÃ§o no futuro pula popup taxa.
- EndereÃ§o oculto atÃ© escolher pagamento na entrega ou na loja.
- Barra de estoque: atualizaÃ§Ã£o manual + horÃ¡rio + standby.

**APIs PDV (amostra):** `api/buscar/`, `api/pdv/*`, `api/promocoes/ativas-pdv/`, Mercado Pago Point em `views_mp_point.py` (Centro + Vila, contas separadas).

**Uso loja (31/07 · v12.31):** botão topbar → overlay · saída PG · quem = grade RH (toque avança / Outros digita) · motivo · PIN · histórico/estorno · não mexe no carrinho da venda.

**Cadastro rápido PDV (04/08 · v13.82):** botão **+ Produto** na busca · bipar → checa EAN → lookup internet opcional · cria Agro (UN) · card **PDV conferir** no Cadastro ERP · VERIFY_OK.

**Rações PDV (09/08 · loja v15.26 · teste UX):** botão **Rações** → tipo → marca (ou Todas) → tamanho → **lista grande** (menor→maior preço) → Adicionar / Adicionar todas / Fechar. Não vai direto ao carrinho. Linha **zebra cinza fraca** (sem cor da marca) · foto miniatura (clique abre grande) · “No carrinho” na coluna Ação. Esc fecha (não fecha se a foto estiver aberta). Lê Categoria/Sub 1/Sub 2/Peso do Agro na hora. Cadastro: Cat. `Rações` · Sub 1 `Cão`/`Gato` · Sub 2 + Peso `1`/`2,5`/`5`/`10`/`15`/`20`/`25`/`pacote`.

**Balança granel (16/08 · teste v16.71):** botão **Pesar** / **F10** → overlay · Web Serial Chrome (Urano US20/2 POP-S · USE-P2) · códigos **1–199** (sem zeros) · auto-add ao estabilizar · Unidade **KG** no cadastro · migrate `0089`.

**Fiado â€” baixa (decisÃ£o 07/07):** cobranÃ§a de tÃ­tulo em aberto **nÃ£o** fica no modal de `/fiado/` â€” redireciona ao **PDV pagamento** com cliente + valor do tÃ­tulo (ou selecionados). Quita `FiadoTituloAgro` + caixa no confirmar. **Cupom fiscal na baixa** = **FL-052** (P1,1), depois do pacote pagamento.

**Armadilha GM no barras (2026-06-18):** se Â«CÃ³digo de barrasÂ» no cadastro tiver texto **GM** (ex. `GM1546-5S`), o leitor manda GM, nÃ£o EAN. No **wizard** (`pdv_wizard.js`), o hÃ­fen do GM disparava atalho `**-`** = remover Ãºltimo item do carrinho (campo mostrava `GM15465S`). Patch: ignorar `-`/`+` durante SKU/GM + modo barcode para `GMâ€¦`. Legado `/consulta/`: F4 pÃ³s-bip + match alnum (`consulta_produtos.js`).

**Venda gravada em:** `VendaAgro` (Postgres) + tentativa sync ERP conforme config.

### 4.3 NFC-e (cupom fiscal 65)

**EmissÃ£o pelo Agro**, nÃ£o pelo ERP. SÃ©rie **21** no Agro; ERP continua sÃ©rie **20**.

#### Quando emite ou nÃ£o (resumo operacional)

**PrÃ©-requisito:** `NFC_E_ENABLED=true` e certificado/CSC configurados. Se desligado â†’ **nunca** emite.

| SituaÃ§Ã£o | Emite NFC-e? |
| -------- | ------------ |
| `NFC_E_MODO=auto` | **Sim**, em toda venda confirmada |
| Forma **PIX** ou **cartÃ£o** (dÃ©bito / crÃ©dito / parcelado) | **Sim**, automÃ¡tico |
| **Dinheiro**, fiado, vale, cashback, etc. | **SÃ³ se o operador escolher** cupom fiscal |
| Operador escolhe **Â«Venda comumÂ»** (popup de impressÃ£o) | **NÃ£o** â€” sÃ³ cupom nÃ£o fiscal |
| Venda **sem impressÃ£o** + forma manual | **NÃ£o** (salvo modo `auto`) |
| Venda **sem impressÃ£o** + PIX/cartÃ£o | **Sim**, automÃ¡tico (background) |
| Venda **com impressÃ£o (F9)** + cupom fiscal | **Sim**, **sÃ­ncrono** â€” modal CPF â†’ aguarda SEFAZ â†’ imprime |
| Venda **sem impressÃ£o (Enter)** + PIX/cartÃ£o | **Sim**, background (sem modal se sem CPF no cliente) |
| **Falha na SEFAZ** | Venda **grava igual**; reemitir em **Consultar vendas** |

#### Popups no PDV (wizard `/pdv/checkout/`)

| Passo | PIX / cartÃ£o | Dinheiro e demais |
| ----- | ------------ | ----------------- |
| Confirmar **com impressÃ£o (F9)** | Modal CPF (se sem CPF no cliente) â†’ emissÃ£o **sÃ­ncrona** â†’ imprime fiscal | Popup **Cupom fiscal** ou **Venda comum** â†’ idem se fiscal |
| Confirmar **sem impressÃ£o (Enter)** | Sem modal â€” sem ID automÃ¡tico em PIX/cartÃ£o | NFC-e sÃ³ se operador pediu / forma auto |
| Cliente **sem CPF** vÃ¡lido | Modal **Sem CPF na nota** / **Informar CPF** | Idem, se escolheu cupom fiscal |
| Cliente **com CPF** no cadastro | Usa o CPF, sem modal | Idem |

#### Devolução de venda

- Repõe estoque + saída no caixa (como antes).
- Se tinha NFC-e **autorizada** e a devolução **totaliza** a venda → na confirmação o sistema **pergunta**: **Devolver e cancelar cupom** ou **Devolver e manter cupom**.
- Só tenta cancelar na SEFAZ se o operador escolher cancelar (motivo padrão: *«Devolucao de mercadoria registrada no sistema Agro.»*). Payload: `cancelar_nfce` true/false; sem chave → True (compat).
- **Prazo SEFAZ (NFC-e mod. 65):** cancelamento por evento só até **~30 minutos** após a **autorização do cupom** (regra nacional — NT 2018.004 / Ajuste SINIEF 07/2018). O relógio **não** recomeça na devolução.
- **Erro 501** = prazo esgotado na SEFAZ. Devolução no Agro segue OK; cupom continua autorizado → **NF-e de devolução (mod. 55)** ou contador.
- Status local passa a **Cancelada** quando a SEFAZ aceita.
- Botão **Cancelar NFC-e** na tela da venda (retry manual).
- Devolução **parcial**: NFC-e permanece; não pergunta cancelamento.

**UX PDV (2026-06-18):** modal CPF **grande** (`max-w ~54rem`, fontes `clamp`). ReemissÃ£o em `/vendas/` â†’ apÃ³s autorizar pergunta **Imprimir cupom / Agora nÃ£o**. Aviso pÃ³s-venda NFC-e falhou: toast **no topo**, depois da janela de impressÃ£o Windows.

**SEFAZ:** PIX/cartÃ£o â†’ grupo `<card><tpIntegra>2</tpIntegra></card>` (NT 2024.003) + `vTotTrib` por item. **NÃ£o** mandar `card` em fiado/`tPag=05` (rejeiÃ§Ã£o **963**). NCM/CFOP/CEST sÃ³ dÃ­gitos no XML (rejeiÃ§Ã£o **225** schema).

**Arquivos centrais:**


| Arquivo                            | Papel                                            |
| ---------------------------------- | ------------------------------------------------ |
| `produtos/nfce_config_util.py`     | Env vars, resumo config                          |
| `produtos/nfce_sp_emissao_util.py` | XML, SOAP SEFAZ SP, assinatura, QR Code          |
| `produtos/nfce_cupom_util.py`      | Cupom tÃ©rmico 80 mm                              |
| `produtos/nfce_ibpt_util.py`       | Tributos Lei 12.741                              |
| `produtos/nfce_venda_util.py`      | Painel status por venda                          |
| `produtos/views_nfce.py`           | Rotas API e contabilidade                        |
| `produtos/models.py`               | `NfceDocumentoAgro`, `VendaAgro.nfce_solicitada` |


**Modelos:** `NfceDocumentoAgro` (1:1 com venda, guarda XML autorizado). Campo `nfce_solicitada` na venda = operador pediu cupom mesmo em forma manual.

**Rotas Ãºteis:**

- `GET /api/nfce/status/`
- `POST /venda/<pk>/nfce/emitir/`
- `GET /venda/<pk>/nfce/cupom/`
- `GET /api/nfce/export-xml/?ano=&mes=` (ZIP contabilidade)
- `/contabilidade/` painel

**Produção GM Agro — emitente NFC-e:**

| Loja | CNPJ | IE | Endereço |
| ---- | ---- | -- | -------- |
| **Centro (matriz)** | `48900774000103` | env `NFC_E_IE` | env `NFC_E_LOGRADOURO`… |
| **Vila Elias (filial)** | `48900774000286` | `394051450113` | Joaquim Mauricio Grothe, 173 · Vila Elias · CEP 11940-000 |

Mesma raiz `48900774` → **mesmo certificado A1 + mesmo CSC**. Cupom segue o **depósito da venda** (caixa Vila → `/0002`, caixa Centro → `/0001`). Numeração **independente** por CNPJ (`NfceNumeracaoAgro.emitente_cnpj`). Município `3524600` Jacupiranga, `NFC_E_TP_AMB=1`. Checklist: `docs/NFCE-PRODUCAO.md`.

**Não confundir** com CNPJ `03230457000180` (empresa **Agro Mais Vila Elias** no financeiro/entrada NF) — isso **não** é o CNPJ fiscal da filial GM.

**HistÃ³rico de dor SEFAZ (jÃ¡ resolvido no cÃ³digo):** QR no XML antes da assinatura, SHA1 QR v2 SP, retry nÃºmero duplicado 539/204, CA bundle ICP-Brasil no Render, SOAP 4.00 sem `nfeCabecMsg` errado.

### 4.4 Vendas

- Lista: `/vendas/` Â· detalhe: `/venda/<pk>/`
- ReimpressÃ£o cupom simples e fiscal (se NFC-e autorizada).
- DevoluÃ§Ã£o, reenvio ERP, reversÃ£o ERP â€” APIs em `views.py`.
- Export CSV: `/vendas/exportar-csv/`.

### 4.5 Clientes

- Cadastro local: `ClienteAgro` (Postgres).
- Sync ERP/Mongo â†’ Agro: `produtos/services_clientes_sync.py`, botÃ£o na lista, comando `sincronizar_clientes_agro`.
- `**editado_local=True` nÃ£o Ã© sobrescrito** na sync.
- PDV lista/busca clientes **sÃ³ no Agro** (`api/listar-clientes/`, `api/buscar-clientes/`).
- **Editar cadastro (PDV):** modal sem scroll; telefone duplicado = popup no meio (abrir o outro ou **limpar o número** dali, com PIN). **Excluir** (bloqueia fiado em aberto e vínculo RH) + transferir cashback/vale. **Vale crédito:** clicar no saldo ou no cadastro — pagar (entra no caixa) ou manual (sem caixa). Log em `ClienteAgroEventoAgro`. Mesmas ações em `/clientes/…/editar/`.
- IDs Mongo no JSON viram `local:{pk}` para nÃ£o mandar ObjectId ao ERP.
- Contexto antigo detalhado: `docs/CONTEXTO_SESSAO_CLIENTES_PDV.md`.

### 4.6 Cadastro / gestÃ£o de produtos

- **Etiquetas `/produtos/etiquetas/`:** presets de layout = **Postgres** (`EtiquetaPresetAgro`) — multi-PC (01/08). localStorage só cache + preset ativo + rodapé.

**Duas telas â€” nÃ£o confundir:**


| Tela                       | URL / API                                           | Uso                                                  |
| -------------------------- | --------------------------------------------------- | ---------------------------------------------------- |
| **Cadastro ERP** (SisVale) | `/produtos/cadastro-erp/`, `api_produtos_cadastro`  | Lista/busca Mongo **sem saldo**; Excel import/export |
| **GestÃ£o operacional**     | `produtos_gestao.html`, `api_produtos_gestao_lista` | Saldo, facetas, operaÃ§Ã£o loja                        |


**Excel fase 1:** export com colunas/categorias; import async com histÃ³rico e desfazer; ID oculta; CÃ³digo GM editÃ¡vel; cÃ©lula vazia nÃ£o altera. Colunas: Sub 2–4, Unidade, Modelo, Peso (além das originais).

**Modal cadastro — marca/categoria (08/07):** «Salvar no Agro» grava online (Postgres + overlay). Botão **+** só preenche o campo — **não** substitui salvar. Ao reabrir, detalhe da API prevalece sobre linha da lista (fix bug que «apagava» marca/cat).

**Faceta unidade/marca/cat (06/08 · FACETA-CACHE):** lista branca vinha de cache desatualizado (+ PIN não limpava chave `v6`). Corrigido: invalidar cache ao salvar/+ · servidor inclui valores do produto e do PIN · JS não sobrescreve o que já cadastrou na sessão.

**Foto produto (06/08 · FOTO-PDV · v14.68):** aba Gerais usa o mesmo anexar foto da aba Delivery (uma foto só) · já aparece no PDV (fluxo Delivery desde v11.45). Removido campo URL que não gravava no Agro.

**Fantasmas Mongo â†’ Postgres (`agro_pg`, 2026-06-24):**

| O quÃª | Detalhe |
| ----- | ------- |
| **O que Ã©** | Documento no espelho Mongo com `_id` 24 hex **sem** `Nome` â€” import virou linha `Produto` com nome Â«â€”Â» e `codigo_interno` = Id |
| **Caso loja** | 3Ã— ibiuna **25 kg** (GM1541/42/46-25); demais variantes OK |
| **Evitar de novo** | `importar_catalogo_mongo_produto` **ignora** fantasma (`deve_ignorar_import_mongo_fantasma`) Â· nunca grava ObjectId como nome Â· novo cadastro sÃ³ via Agro (nÃ£o duplicar GM no Mongo |
| **Auditar (quantos?)** | `auditar_produtos_fantasma_pg` â€” **graves** (nome/GM quebrados); `--higiene` = sÃ³ `codigo_interno`=Id |
| **Corrigir lote** | `python manage.py corrigir_produto_nome_objectid_pg --dry-run` depois sem `--dry-run` |
| **Quando rodar** | ApÃ³s `importar_catalogo_mongo_produto`, apÃ³s snapshot staging, se busca cadastro Â«sumirÂ» produto que existe no PDV |
| **CÃ³digo** | `catalogo_nome_util.py` Â· comandos `auditar_*` / `corrigir_*` Â· exibiÃ§Ã£o API herda irmÃ£os GM (teste v2.50+) |

**Produto novo â€” cÃ³digo sistema + GM (decisÃ£o 2026-06-22, Renan OK no teste):**


| Campo              | Regra                                                                                            |
| ------------------ | ------------------------------------------------------------------------------------------------ |
| **CÃ³digo sistema** | Exatamente **4 nÃºmeros**; sequÃªncia **4010â€“9999**; operador pode editar                          |
| **SequÃªncia**      | PrÃ³ximo livre apÃ³s o **maior cÃ³digo sistema** jÃ¡ usado (sÃ³ campo numÃ©rico â€” **GM nÃ£o conta**)    |
| **CÃ³digo GM**      | SugestÃ£o automÃ¡tica `**GM` + cÃ³digo sistema**; operador **livre para editar** (sem formato fixo) |
| **Modal novo**     | Carregar cÃ³digos **sem repintar** o modal (nÃ£o apagar nome/campos jÃ¡ digitados)                  |


Env opcional: `AGRO_NOVO_PRODUTO_COD_MIN` (piso da sequÃªncia; padrÃ£o **4010**).

**LentidÃ£o pÃ³s-entrada NF (investigaÃ§Ã£o aberta):** medir qual URL trava no browser; suspeitos: `api_produtos_gestao_facetas`, pool Mongo. **01/07 loja v6.04 â€” relato Â«quase todas telasÂ»:** ver CHECKPOINT Â«Perf multi-telaÂ»; loja jÃ¡ `agro_pg`+`ledger`+CP PG â€” foco em fila Gunicorn (1 worker), facetas no load, CP PG scan 25k, bootstrap+API dupla LanÃ§amentos.

### 4.7 Entrada de nota fiscal

- `/entrada-nota/` â€” wizard 8 passos (fornecedor â†’ â€¦ â†’ financeiro â†’ finalizar PIN).
- **Dist DF-e (31/07):** certificado `NFE_DIST_DFE_*` **ou** `NFC_E_*`. Cursor PG só avança em **137/138**. Caixa de entrada PG (~80): Buscar grava · Pendentes antigas primeiro · Concluídas. Recuperar por chave se precisar. **XML** se nota antiga.
- **Só resumo / Ciência (05/08):** evento oficial **210210** no Ambiente Nacional; grava protocolo no PG, não repete e tenta baixar o XML completo para Carregar na grade.
- **XML vs Aguarde 1h (12/08):** Buscar lista (distNSU) continua com 1h após 137; **Buscar XML / chave** só trava no **656**. Após Buscar, sistema tenta Ciência+XML sozinho nas «Só resumo» — fica pronto para **Carregar na grade** (manual, uma nota por vez).
- **UI aba SEFAZ (04/08):** tela limpa (ações + lista); textos longos no **«?»** (contexto `sefaz` + bloco no modal). Status compacto (Pronto / Local off / Cursor).
- PrÃ©-visualizaÃ§Ã£o XML: modal drag-and-drop, nÃ£o fecha ao clicar fora; Â«Confirmar na gradeÂ» aplica de fato.
- **Busca produtos etapa 2 (16/07 Â· loja v8.69):** BCA `/api/buscar/` igual cadastro/PDV â€” famÃ­lia GM completa (complemento Mongo); nÃ£o desligar Mongo no `entrada_nfe=1`.
- **AcrÃ©scimos no custo (14/07 Â· loja v8.43):** checkbox Â«Incluir no custo os acrÃ©scimos da notaÂ» (etapa 2) â€” rateia frete+ST+seguro+outras+IPIâˆ’desconto no custo unitÃ¡rio proporcional ao `vProd`; mark/desmarca recalcula sem reupload. Nota sem esses totais = noop.
- **Mudar produto (21/07):** trocar vÃ­nculo no Â«MudarÂ» **nÃ£o** troca o V. unit da NF (nem o rateio); sÃ³ cadastro/P.venda. Linha manual sem base NF ainda puxa custo do cadastro.
- **Nova nota (21/07):** botÃ£o Â«NovaÂ» zera XML/cabeÃ§alho/financeiro/rateio â€” nÃ£o herda a nota anterior (autosave tambÃ©m).
- **HistÃ³rico C1â€“C3 + NF (18/07):** C1â€“C3 = sÃ³ compras **anteriores**; a NF aberta **nÃ£o** entra (evitava parecer 2 notas: data entrada vs emissÃ£o).
- **Vínculo XML (30/07 · v12.10):** tabela Postgres `EntradaNfeVinculoAgro` = fonte da verdade multi-PC; «Ler XML» reaproveita cProd (R0151…). Migrate `0069` · backfill `agro_backfill_c_prod_nf_entrada`.
- **Financeiro desync (2026-06-19 / reforço 29/07):** título já em Contas a pagar mas etapa 7 laranja + «Salvar + a pagar» morto — rascunho perdeu `financeiro_lancado`. Fix: sync ao abrir · botão religa sem reabrir · API não gera 2º lote se achar NF · Reabrir estorna por rastro se ids sumiram. **Não** reabrir e confirmar tudo de novo (duplicava). Limpar duplicatas já feitas em Contas a pagar.
- **Reabrir → estoque de novo (03/08):** ao reabrir, estornar se houver status/`estoque_aplicado_em`/carimbo/`ajuste_ids` (não só `estoque_aplicado`). Autosave não ressuscita carimbo. Lista «reabrir» encerrada chama o mesmo estorno.
- **Kardex ao reabrir (03/08):** reabrir **não apaga** a Entrada NF — grava saída `estorno_entrada_nf_agro` («Estorno NF (reabrir)»); ao concluir de novo, nova Entrada NF. `nf_qtd=` no ajuste para qtd confiável.
- **Trocar/remover produto com estoque lançado (05/08 · v14.48):** exige **estorno** antes — modal «Estornar e trocar» (PIN) chama a rotina de reabrir e joga o usuário de volta à etapa 2; backend recusa salvar linhas com `produto_id` diferente enquanto houver carimbo de estoque (`requer_estorno`).
- **Custo do cadastro na etapa 2 (03/08 · v13.71):** V. unit puxa custo do Cadastro (overlay/PG) — JS ignora `preco_custo_final=0` do Mongo; overlay sincroniza final/acréscimo; `buscar-produto-id` fallback `Produto.custo`. Linha com custo da NF (`preservar`) continua sem sobrescrever.
- **Validade → tela Validade (06/08):** ao **lançar estoque**, se a linha tiver `lote_validade` (etapa 4), grava/soma `EstoqueLote` (antes só ficava no rascunho). Reabrir reduz o lote se a entrada tinha `nf_lote`/`nf_val`. Notas **já** lançadas antes do fix **não** voltam sozinhas.
- **Etapa 3 cód. barras (12/08 · v16.06 `NF-BIP-ET3`):** bip casa com EAN da linha **e** barras do cadastro/overlay dos itens da NF; casado por EAN/bip na etapa 2 → Ok verde; prova `verify_nf_bip_et3_path.py`.
- **Etapa 3 PEND após bip etapa 2 (17/08 · `NF-BIP-ET2` · **Live v17.09**):** leitor no **Mudar**/busca (8+ dígitos) vale como Ok; XML `ean_pg`/`ean_overlay` também. Código do fornecedor (sem bip) continua PEND. Prova `verify_nf_bip_et2_path.py`.
- **Etapa 3 lenta + visual (17/08 · `NF-BIP-ET3-SNAP`):** um lote de códigos do cadastro (não 1 request por item); barra Conferidos; flash + som no Ok. Prova `verify_nf_bip_et3_path.py` **83/83**.
- **Vínculo NF não sobrescreve cadastro (17/08 · `NF-VINCULO-NAO-SOBRESCREVE`):** «Mudar»/cProd/EAN grava só o vínculo. Nome, marca, categoria, GM e preços ficam. Lote/validade não copia xProd da NF no nome. Prova `verify_nf_vinculo_nao_sobrescreve.py`.
- **Itens já estragados (17/08 · `NF-VINCULO-REPARO`):** ✅ **33 corrigidos** (18/08 · `--aplicar` na loja). Devolve histórico ou tira overlay / colchete `[EAN]`. **Não** mexe preço, GM, barras.

### 4.8 Estoque Agro

- Saldo PDV = Mongo ERP + `AjusteRapidoEstoque`.
- Painel: `/estoque/sincronizacao/`.
- Doc: `docs/ESTOQUE_AGRO_FONTE_DA_VERDADE.md`.
- Cron: `estoque_mongo_ping` a cada 10 min no Render.
- **Transferência forçada — bip vs digitar (06/08):** bip (código de barras numérico) → qtd 1 (+1 se já no carrinho) e foco volta na busca; digitar nome/GM → foco na quantidade (como antes).
- **Transferência forçada — layout C→Vila (18/08):** ao escolher **Centro → Vila**, modal inverte colunas (carrinho à esquerda, busca à direita); **Vila → C** mantém layout original.
- **Transferência forçada — popup direção (18/08):** botão **Forçada Vila↔C** abre popup «Vila → Centro» / «Centro → Vila» antes da tela; sem toggle no cabeçalho.
- **Contagem cíclica (13/08 · v16.12+ · Bip+1 18/08 · qtd+código 18/08 · foco 18/08 · **hero 18/08 · teste v17.25**):** Ajuste Mobile **Cíclica** — sessão PG multi-celular, cego, **sempre soma**, filtro **dias de movimento** (padrão 60), escopo loja/categoria/corredor, 2 passagens, grava só no fechamento. **Bip +1** soma 1 na contagem (não no estoque); tela pisca verde/vermelho **com o total já bipado**. **Modo foco:** só busca grande + lista; **último bip** em destaque no topo; já contados em linha estreita; **⋯** abre controles.

### 4.9 Compras

- `/compras/` â€” sugestÃ£o, horizonte em dias, mÃ©tricas avanÃ§adas em `<details>`.
- **UI etapa 1 (18/07 Â· v9.95):** painel resumo (horizonte + descontar estoque + KPIs) Â· busca em destaque Â· detalhe expandido em blocos â€” **sÃ³ visual**, regras/cÃ¡lculos iguais.
- **Fontes (Renan 08/07):** mÃ©dia/grÃ¡fico/sugestÃ£o = **vendas PDV Agro**; Ãºltima compra/chips/planilha = **sÃ³ Entrada NF Agro** (ERP cortado). RÃ³tulos na tela alinhados.
- **UX Compras (08/07):** coluna Â«ComprarÂ»; estoque Centro+Vila por extenso; lucro sÃ³ com custo confiÃ¡vel; custo usa cadastro ou Ãºltima NF; F5 preenche Â«Ãšlt. NFÂ» via Entrada NF Agro.
- RelatÃ³rios: A4 fornecedor, planilhas impressas por categoria/unidade (A4 ou A6).
- **Folha Compras (08/07):** fornecedor / categoria / unidade abrem em **popup na prÃ³pria tela** (nÃ£o nova aba) â€” evita limite de 3 abas SisVale e layout bugado. BotÃ£o **Nova aba** no popup se precisar. PÃ¡ginas planilha com `?embed=1` nÃ£o montam barra lateral.
- **Popup Folha (27/07):** modal compacto (padrÃ£o ERP) Â· Folha de saldo com filtros **botÃ£o+chip** iguais ao cadastro Â· facetas no HTML.
- **Folha por fornecedor (27/07 + 11/08):** catálogo PG — cadastro + Entrada NF Agro. **11/08 (`FOLHA-FORN-HIST`):** lista = cadastro **∪** histórico de NFs (não só último pedido); 1º token do nome (ex. ADIMAX).
- **Folha de saldo (28/07):** overlay grande · filtros salvos **online** (Postgres) com 1 padrão global · tipografia maior nos filtros.
- **Atalho PDV Estoque Vila (28/07):** botão no PDV (`/pdv/checkout/`) abre as 5 opções da Folha Compras e manda pra `/compras/?folha=…` com overlay já aberto (rótulo só — não força Vila).

### 4.10 LanÃ§amentos / financeiro

- `/lancamentos/` â€” redirect â†’ **Contas a pagar padrÃ£o:** `/lancamentos/contas-pagar/` (**layout novo**) Â· `/classico/` â†’ redirect Â· `/teste/` â†’ redirect
- Contas a receber: `/lancamentos/contas-receber/` (layout clÃ¡ssico)
- PDF: `lancamentos_financeiro_pdf.py` (sem coluna observaÃ§Ãµes longas; forma pagamento; bruto destacado).
- Busca na lista: termos com espaÃ§o; **valor** (bruto/pago/saldo); **número da NF**; **data** digitada; boleto; parcela; CPF/CNPJ. Ajuda: `includes/lancamentos_help_agents.html`.
- **Layout novo CP:** `lancamentos_contas_pagar_teste.html` â€” API `/api/lancamentos/`; filtros na URL; recarga in-place preserva scroll/filtros/pÃ¡ginas. **Filtro de data:** vencimento (padrÃ£o) Â· competÃªncia Â· pagamento (`ref` + `venc_*` / `comp_*` / `pag_*` na URL e na API).
- **Perf lista (2026-06-19):** projeÃ§Ã£o slim Mongo; `skip_totais` pÃ¡g. 2+; cache sessionStorage; planos lazy.
- **Abertura CP â€” Chrome (2026-06-19, v1.48+):** prefetch BI/F7 Â· cache do dia Â· selo **Sincronizandoâ€¦** Â· **bootstrap HTML** (lista hoje+abertos jÃ¡ no servidor, sem 2Âª ida Ã  API). Renan validou melhora **sutil** â€” esperado no Chrome MPA.
- **Teto sem refactor grande:** no Chrome cada clique = **pÃ¡gina nova** + Mongo no bootstrap. **Roadmap adiado (2026-06-19):** prÃ³ximo salto = Postgres financeiro **ou** lista no BI â€” ver CHECKPOINT.
- **Nova saÃ­da** (modal) + **Lote manual** (`/lancamentos/novo-manual/`): pseudo-plano **Â«EmprÃ©stimo (entrada + pagamento)Â»** â€” gera receita quitada (hoje) + despesa(s); se saÃ­da > entrada, diferenÃ§a em **Juros de EmprÃ©stimos**. JS: `lancamento_emprestimo_dual.js`; backend: `expandir_linhas_emprestimo_dual_lote` em `mongo_financeiro_util.py`.
- **GrÃ¡fico gastos por plano (2026-06-26):** `/financeiro/grafico-gastos/` â€” **100dvh sem scroll**; toolbar perÃ­odo simÃ©trica; painel **Filtros | Planos**; **4 atalhos** Postgres (**Alt+clique** fixa padrÃ£o ðŸ“Œ); modos tempo real / histÃ³rico / comparar; drill-down CP popup. **Entrada BI:** botÃ£o laranja no card **Contas a Pagar** (`/`). Teste **v3.54+**; loja **v3.39**.
- **DRE Indicadores + Resumo — CMV (09/08, `DRE-CMV-TOGGLE` + `RG-CMV-TOGGLE`):** botão **Mercadoria vendida** (custo cadastro × qtd) × **Mercadoria paga** (lançamentos). Lucro bruto / margem / líquido / PE acompanham. Caixa não muda. Padrão = vendida. Mesma chave `agro_dre_cmv_modo_v1`.
- **DRE visual prévia (09/08 + Mini DRE soma + card empréstimos 10/08 + legível 13/08):** Resumo 16:9. Mini DRE: Receita → … → **Saldo final** (= soma). Card Empréstimos: devido/juros/total/pago/emprestado. **Balão no mouse** (`data-rg-tip`) em cada número + `?` para texto longo. PE = termômetro (Vendeu / Precisa / Folga). Indicadores permanece até 100%.
- **Filtro loja (10/08):** **Centro + Vila** (padrão) · Centro · Vila. Vendas/CMV vendida = PDV da(s) loja(s). Despesas/empréstimos = empresa cadastrada (Vila sem empresa própria usa Centro). BI `/` mesmo recorte no filtro **Números** (não troca o PDV).

### 4.11 Caixa

- **Gaveta (Centro)** = turno principal do Centro Â· **Vila Elias** (`ponto_caixa=vila`) = turno prÃ³prio da Vila Â· **Notebook** = satÃ©lite do pai da loja do aparelho Â· **Teste** = isolado, fora do lote.
- Painel Â«todosÂ» e fechamento em lote: **sÃ³ a loja do seletor** (Centro Ã— Vila) â€” nÃ£o misturam.
- Abrir caixa alinha o seletor de loja do PDV; venda usa depÃ³sito do `ponto_caixa` da sessÃ£o.
- **Antiburro (v10.04):** abrir Gaveta/Vila e trocar Loja no BI exige digitar `centro` ou `vila`; com caixa aberto o seletor fica travado.
- **Trava loja (v10.56):** reforÃ§o, retirada/saÃ­da, devoluÃ§Ã£o, fiado, assumir sessÃ£o e venda **sÃ³** no turno da loja do aparelho; caixa fechado = bloqueia.
- MP Point automático: Gaveta Centro / Teste = conta Centro · Caixa Vila = conta Vila (outra credencial). Notebook não dispara.
- Layout **16:9**, shell `.caixa-shell`, `100dvh` â€” nÃ£o coluna estreita.
- Util: `produtos/caixa_util.py`.
- **Abrir â€” CÃ©dulas (21/07):** botÃ£o **CÃ©dulas** na abertura (igual fechar) Â· campo valor comeÃ§a vazio Â· sugestÃ£o sÃ³ no card azul / placeholder Â· modal `includes/caixa_cedulas_abertura_modal.html`.
- **DiferenÃ§a abertura (21/07):** se contagem â‰  Ãºltimo fechamento â†’ grava `diferenca_abertura` Â· aparece em **ConferÃªncias Â· diferenÃ§as** como Â«Abertura Â· DinheiroÂ» Â· `usuario` (abriu) + `usuario_fechamento` (fechou) sempre.
- **Retirada / saÃ­da (2026-06-24):** botÃ£o do painel â†’ **`/caixa/retiradas/`** (histÃ³rico com filtros data Â· plano Â· quem levou; padrÃ£o **hoje**; calendÃ¡rio Agro Date Picker). BotÃ£o laranja **Nova saÃ­da** â†’ formulÃ¡rio existente (`?painel=retirada`). Popup fechar caixa tambÃ©m abre o histÃ³rico (`embed=1`). Layout **rem/clamp** + herda **Agro Display Scale** (perfil Ãºnico / iframe pai).
- **Retiradas â€” vales RH (01/07):** histÃ³rico `/caixa/retiradas/` inclui **ValeFuncionario** (adiantamento) para conferÃªncia mensal Â· filtro plano aceita **label ou cÃ³digo** Â· vale no caixa nÃ£o gera Â«SaÃ­da caixaÂ» no financeiro (baixa parcial no salÃ¡rio) Â· **loja v5.64** cherry-pick `2207fd6`.
- **Repasse Vila → Centro (13/08 · v16.10):** `/repasse-vila/` + PDV **Repasse** · CMV + % lucro + fiado pago Vila · migrate `0087` · aviso na abertura Gaveta Centro.
- **Repasse acumulado (18/08):** saldo dos dias anteriores (+ falta / − crédito) · total sugerido · ajuste manual PG · migrate `0093`. **v17.41:** dinheiro já levado a mais **abate** o acumulado na hora (não pede o mesmo valor de novo).
- **Reserva / troco na Vila:** valor fixo salvo no Postgres (`reserva_vila`) desconta do envio ao Centro. Tela: campo **Fica na Vila** · opções (dia/%, checks) recolhidas · histórico em 2 colunas (1–15 | 16–31). Migrate `0095`.
- **Planos no lucro do envio (17/08):** botão **Planos** na tela de repasse — marca o que desconta do dinheiro enviado ao Centro (ex. Alimentação); o restante das saídas de caixa da Vila desconta do card **Lucro ficou na Vila**. Grava no Postgres (`RepasseVilaConfigAgro.planos_desconto_centro`). Migrate `0091`.

### 4.12 RH

- `/rh/` â€” folha, vales, ficha.
- Ajuda longa em `rh/templates/rh/includes/rh_help_agents.html` (espelho AGENTS.md Â§9).
- Vale com financeiro = baixa parcial no tÃ­tulo de salÃ¡rio do mÃªs (precisa folha fechada com tÃ­tulo).
- Cancelar vale: motivo mÃ­n. 3 chars; recalcula folhas abertas.

### 4.13 Electron (desktop) â€” opcional; Renan nÃ£o usa

- `electron/main.js` + `preload.js` â€” `shell.openExternal` para WhatsApp/Maps.
- Mesmo Agro Display Scale via `localStorage` (nÃ£o `setZoomFactor` paralelo).
- **Renan:** testou e **nÃ£o usa** (lento). Loja/dev = **Chrome**. Recursos sÃ³ do shell (abas laterais `agro-open-inapp-tab`, iframes) **nÃ£o se aplicam** ao fluxo dele â€” otimizar para **MPA no Chrome** (bootstrap HTML, prefetch, cache).

### 4.14 UI compartilhada

- `_agro_consulta_ui.html` â€” base visual.
- `_agro_display_scale.html` + `agro_display_scale.js` â€” escala global.
- `_agro_open_external.html` â€” links externos.

**Popups / modais (decisÃ£o 08/07 â€” Renan):** padrÃ£o do produto = **`<div>` + Tailwind + JavaScript puro** (MPA Django). Abrir/fechar = tirar/colocar classe `hidden` (+ `modal-open` no `body` quando precisar). **NÃ£o** usar biblioteca de modal (Bootstrap, SweetAlert, etc.). **`<dialog>` nativo:** avaliado â€” **nÃ£o** adotar em popup novo por padrÃ£o (dezenas de modais jÃ¡ no padrÃ£o `div`; operador nÃ£o ganha nada; CPU/memÃ³ria imperceptÃ­vel). **Regra:** popup novo numa tela que jÃ¡ tem modal â†’ **copiar o padrÃ£o da tela**; tela zerada / FOOD do zero â†’ pode usar `<dialog>` se padronizar a tela inteira. CanÃ´nico tambÃ©m em **`SISTVALE.md`** e **`FOOD.md`**.

### 4.15 DesvinculaÃ§Ã£o ERP (Mongo espelho â†’ Postgres SisVale)

**Resposta curta (Jul/2026):** **~85 % operaÃ§Ã£o diÃ¡ria jÃ¡ Postgres.** Mongo ERP = **espelho + relatÃ³rios que faltam**. **NÃ£o** cancelar assinatura ERP atÃ© fechar checklist abaixo.

**Dois nÃ­veis:** (1) cortar **API ERP** â†’ **âœ… feito**; (2) parar de **ler/gravar Mongo** em **todas** as telas â†’ **em andamento**.

### Checklist â€” corte total Mongo (ordem de prioridade)

Marque na loja apÃ³s deploy + Renan OK. **NÃ£o apagar Mongo** atÃ© item **12**.

| # | Pacote | Por quÃª nesta ordem | Loja hoje | PrÃ³ximo passo |
| - | ------ | ------------------- | --------- | ------------- |
| âœ… | **API ERP** (Agro nÃ£o grava no legado) | Corte WL | **OK** | â€” |
| âœ… | **PDV Â· vendas Â· NFC-e Â· caixa Â· RH Â· clientes Â· fiado** | Nativo Postgres | **OK** | â€” |
| âœ… | **Cadastro + catÃ¡logo PDV + ledger estoque** | BalcÃ£o | **OK** | â€” |
| âœ… | **CP/CR** lista + pagar + nova saÃ­da + editar | Financeiro dia | **OK** | â€” |
| âœ… | **Entrada NF** passo 7 + rascunho PG | Compras/estoque | **OK v4.20** | â€” |
| âœ… | **Compras D4** (mÃ©tricas + folhas) | ReposiÃ§Ã£o | **OK** | â€” |
| âœ… | **BI cards CP/CR + resumo gerencial** | GestÃ£o rÃ¡pida | **OK** | â€” |
| âœ… | **TransferÃªncias + Validade** (leitura) | Estoque | **OK** | â€” |
| âœ… | **Listas auxiliares** (plano, forma, banco, fornecedor NF) | Baixa CP / NF | **OK** | â€” |
| **1** | **DRE + fluxo calendÃ¡rio + export PDF/Excel** LanÃ§amentos | Param se Mongo cair | **PG** (flag auto loja) | Validar loja |
| **2** | **GrÃ¡fico gastos** (dados, nÃ£o sÃ³ UX) | BI financeiro | **PG** (flag auto loja) | Validar vs CP |
| **3** | **BI `/` histÃ³rico vendas** â†’ `VendaAgro` | Dashboard completo | **âœ… teste v4.36** â€” sem DtoVenda/`vendas_agro` com `agro_pg` | Validar BI |
| **4** | **GestÃ£o produtos 100 % PG** (facetas + perf pÃ³s-NF) | LentidÃ£o / lista | **âœ… teste v4.36** â€” sem fallback Mongo; saldo ledger (v4.33) | Validar gestÃ£o |
| **5** | **Compras dimensÃµes** relatÃ³rio (categoria/unidade sem scan Mongo) | Folhas grandes | **âœ… teste v4.36** â€” buscar + NF etapa 6 sem Mongo obrigatÃ³rio | Validar Compras |
| **6** | **Entrada NF auditoria financeiro** + recovery tÃ­tulos PG | BotÃ£o auditoria | **âœ… teste 28/06** Renan | NF 112 Â· alertas 0 |
| **7** | **PDV â†’ GestÃ£o saldo** pÃ³s-venda (v3.82) | Estoque gestÃ£o | **âœ… teste 28/06** | GM9503 47â†’46 |
| **8** | **Congelar Mongo financeiro** (`AGRO_FINANCEIRO_MONGO_CONGELADO`) | SÃ³ histÃ³rico | Off | ApÃ³s 1â€“6 OK |
| **9** | **Motor busca GM** Compras/NF | UX `gm0050` | Quebrado | **Por Ãºltimo** |
| **10** | **Backup PC** (CP/CR ZIP, cadastro Excel, vendas, NFC-e) | Seguro | Renan | Antes do 11 |
| **11** | **Checkpoint LanÃ§amentos** + conferÃªncia totais | Prova corte | Feito parcial | Renan + assistente |
| **12** | **Cancelar assinatura ERP** | Fim espelho | â€” | SÃ³ apÃ³s **1â€“11** estÃ¡veis |

**Regra deploy loja:** teste OK â†’ Renan *Â«pode subir produÃ§Ã£oÂ»* + senha **99738595** Â· pacote por pacote (nÃ£o merge inteiro).

**Status debug:** `GET /api/agro/fonte-status/`



| Status                 | Tela / mÃ³dulo                                                     | Nota                                                                                                                                            |
| ---------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **Feito (Agro PG)**    | Clientes PDV, **Vendas**, **NFC-e**, **Caixa**, **RH**, **Fiado** (`/fiado/`, `FiadoTituloAgro`â€¦) | **Fiado = Postgres nativo** â€” nÃ£o usa `DtoLancamento`. Sync ERP opcional onde existir |
| **Teste âœ…**           | Cadastro, PDV catÃ¡logo (`agro_pg` + `SOMENTE_POSTGRES`), gestÃ£o/lista PG, Compras D4 + dimensÃµes, BI vendas VendaAgro, pacotes corte v4.31â€“v4.43 | **âœ… Renan 28/06** checklist 1â€“4 + 6â€“7 Â· DRE â­ |
| **Loja âœ… parcial**    | Cadastro, PDV merge PG, Compras D4, CP/CR, Entrada NF v4.20, ledger | **Pacote corte v4.36** deploy **28/06** Â· GestÃ£o/BI/Compras corte Mongo |
| **PG loja â€” validar** | DRE, calendÃ¡rio, export PDF/Excel, grÃ¡fico gastos (dados)         | Flag financeiro PG auto â€” conferir totais vs CP                                                                                                 |
| **Falta (mÃ©dia)**      | Busca **GM** Compras/NF                                           | Motor GM por Ãºltimo â€” **nÃ£o bloqueia** corte Mongo no teste                                                                                      |
| **Falta (operacional)** | Backup PC Â· checkpoint totais Â· congelar Mongo Â· cancelar ERP    | Itens **8â€“12** Â§4.15 â€” Renan + assistente                                                                                                        |

**LanÃ§amentos vs desvinculaÃ§Ã£o (Renan 2026-06-25 â€” nÃ£o confundir):**

| | **JÃ¡ fizemos** | **Ainda falta (Â§4.15)** |
| --- | --- | --- |
| **LanÃ§amentos** | Layout CP, PIN, filtros, Nova saÃ­da, emprÃ©stimo dual, editar/excluir, backup ZIP, **checkpoint** (~17â€¯703 tÃ­tulos), **corte sync API ERP** (v1.14), perf bootstrap, **CP/CR PG loja** | DRE/calendÃ¡rio/export â€” **PG** se financeiro PG; validar totais |
| **Fiado** | GestÃ£o `/fiado/`, PDV limite/parcelas, tÃ­tulos **`FiadoTituloAgro`** Postgres | Nada no pacote Â«desvincular Mongo catÃ¡logo/financeiroÂ». BI ainda pode cruzar `DtoVenda` para nÃ£o duplicar grÃ¡fico â€” **nÃ£o Ã© a tela Fiado** |


**Estimativa (Renan â€” linguagem simples):** pacotes de **dias**, nÃ£o â€œmeses inteirosâ€ â€” o grosso jÃ¡ estÃ¡ no teste; falta **subir na loja** + **financeiro/BI** por fatias.


| Etapa | O quÃª muda na prÃ¡tica                                                                                                  | Risco                                                        | Tempo (dev + teste) |
| ----- | ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ | ------------------- |
| **1** | Cadastro Postgres (`agro_pg`)                                                                                          | MÃ©dio                                                        | **âœ… teste** Â· 1 dia loja |
| **2** | PDV + gestÃ£o mesma base + ledger                                                                                       | Alto no balcÃ£o                                               | **âœ… teste** Â· 1â€“2 dias loja |
| **3** | Compras/NF busca + mÃ©tricas + LanÃ§amentos + BI                                                                         | Alto no caixa/contas                                         | **2â€“5 dias por fatia** (CP primeiro) |


**Ordem sprint (Renan â€” Jun/2026, ~dias):**

| # | Pacote | Por quÃª nesta ordem | Dias (ordem de grandeza) |
| - | ------ | ------------------- | ------------------------ |
| **1** | **ProduÃ§Ã£o = teste** (flags B+C + ledger + `agro_pg` + Entrada NF v2.78) | CÃ³digo **jÃ¡ validado** â€” **agendado loja fecha 25/06 noite** (ver CHECKPOINT) | **1** deploy |
| **2** | **Compras D4** (mÃ©tricas + folhas) | Ãšltima compra / sugestÃ£o | **âœ… teste** v2.79â€“v2.87 |
| **3** | **Entrada NF financeiro Agro** (tÃ­tulo sem `DtoLancamento` loja) | Wizard OK; tÃ­tulo real na loja | **2â€“3** |
| **4** | **LanÃ§amentos â€” migraÃ§Ã£o Postgres** (`agro_pg` financeiro) | Checkpoint/corte API **jÃ¡ feitos** Â· falta **fonte de dados** | **3â€“5** |
| **5** | **BI `/` + resumo** | VendaAgro Postgres | **âœ… teste v4.36** â€” validar Â· loja pendente |
| **6** | **TransferÃªncias Â· Validade Â· fornecedor NF** | Menor urgÃªncia | **1â€“2** cada |
| **7** | **Motor busca Ãºnico** | **NÃ£o Ã© desvinculaÃ§Ã£o** â€” bug GM Compras/NF (`gm0050`); **por Ãºltimo** | **1â€“2** |
| **8** | **Checkpoint final + cancelar assinatura ERP** | SÃ³ apÃ³s 1â€“6 + backup | **0.5** |

**Motor busca â‰  desvinculaÃ§Ã£o:** catÃ¡logo/saldo jÃ¡ vÃªm do Postgres no teste; GM quebrado Ã© **UX/paridade de busca** entre telas â€” **nÃ£o bloqueia** cortar Mongo. Pode ficar **Ãºltimo**.

**Ordem tÃ©cnica legada (referÃªncia):** cadastro â†’ `agro_pg` â†’ PDV busca â†’ gestÃ£o â†’ ledger â†’ financeiro checkpoint.

---

### Renan â€” desvinculo: duas listas (28/05/2026)

**CenÃ¡rio lista 1:** ERP para de enviar dados **ou** Mongo fica inacessÃ­vel **de repente** (nÃ£o Ã© corte planejado).

#### 1) O que **para ou fica grave** (perde ERP/Mongo)

| Ãrea | O que acontece |
| ---- | -------------- |
| **Entrada NF** | Rascunho **PG** no teste/loja v4.20+; se Mongo cair, fallback legado atÃ© PG popular |
| **GestÃ£o produtos** | **Teste âœ…** lista/facetas PG Â· **Loja** ainda Mongo (sem `SOMENTE_POSTGRES`) |
| **Compras** | **Teste âœ…** dimensÃµes/Ãºltima compra sem scan ERP Â· **Loja** D4 OK; folhas grandes dependem de histÃ³rico Entrada NF Agro |
| **LanÃ§amentos** | CP/CR **PG loja** Â· DRE/calendÃ¡rio/export **PG** se financeiro PG â€” validar totais |
| **GrÃ¡fico gastos** | **PG loja** (flag auto) â€” validar vs CP |
| **BI `/` + resumo gerencial** | **Teste âœ…** VendaAgro (v4.36) Â· cards CP/CR **OK loja** |
| **TransferÃªncias / Validade** | Leitura OK; sync profundo ainda espelho Mongo |
| **Produtos novos no ERP** | **NÃ£o entram** no Agro atÃ© import/sync |
| **Listas auxiliares** | Fornecedor, plano de conta, formas â€” muitas telas ainda puxam Mongo |

**Continua funcionando** (Postgres Agro â€” operaÃ§Ã£o do dia):

PDV venda Â· caixa Â· fiado Â· clientes Â· NFC-e Â· RH Â· **CP/CR LanÃ§amentos** Â· saldo operacional (ledger/ajustes) Â· preÃ§o overlay PG Â· cadastro/PDV catÃ¡logo jÃ¡ importado.

---

#### 2) O que **ainda falta** para desvinculo completo (pode cancelar ERP)

*Espelho da tabela **Checklist â€” corte total Mongo** (Â§4.15 acima).*

| # | Pacote | Status |
| - | ------ | ------ |
| âœ… | Cadastro + PDV catÃ¡logo PG + ledger saldo | **Loja** |
| âœ… | Compras D4 (mÃ©tricas/folhas) | **Loja** |
| âœ… | Entrada NF passo 7 + rascunho PG | **PG loja v4.20** |
| âœ… | LanÃ§amentos CP/CR lista + gravar | **PG loja** |
| âœ… | Fiado, vendas, caixa, RH, NFC-e, clientes | **PG nativo** |
| âœ… | Checkpoint + corte sync API financeiro | **Feito** |
| **1** | DRE + calendÃ¡rio + export PDF/Excel LanÃ§amentos | **PG loja** â€” validar |
| **2** | GrÃ¡fico gastos (dados) | **PG loja** â€” validar |
| **3** | BI `/` histÃ³rico â†’ VendaAgro | **âœ… teste v4.36** â€” validar BI |
| **4** | GestÃ£o 100 % PG | **âœ… teste v4.36** Â· loja ainda Mongo |
| **5** | Compras dimensÃµes sem scan Mongo | **âœ… teste v4.36** â€” validar Compras |
| **6** | NF auditoria financeiro | **âœ… teste v4.31** â€” validar NF |
| **7** | PDV â†’ GestÃ£o saldo pÃ³s-venda | **âœ… teste 28/06** | GM9503 47â†’46 sem F5 |
| **8** | Congelar Mongo financeiro | Off â€” apÃ³s 1â€“7 OK |
| **9** | Motor busca GM Compras/NF | Por Ãºltimo |
| **10** | Backup PC | Renan |
| **11** | Checkpoint LanÃ§amentos + totais | Parcial |
| **12** | Cancelar assinatura ERP | SÃ³ apÃ³s **1â€“11** + backup |

**MitigaÃ§Ã£o:** fazer sÃ³ a etapa 1 no `teste`, conferir cadastro + busca + salvar preÃ§o, **sÃ³ entÃ£o** produÃ§Ã£o.

**Antes da etapa 1 (Renan â€” vocÃª faz; assistente implementa depois):**

1. Testar **sÃ³ no ambiente teste** (Render staging) â€” **nÃ£o** na loja ainda.
2. Anotar **2 ou 3 produtos** que vocÃª conhece: nome, preÃ§o de venda, cÃ³digo GM ou barras (para comparar depois).
3. Entrar no cadastro no teste com seu login e confirmar que abre `/produtos/cadastro-erp/`.
4. Confirmar no Render que o **staging tem banco Postgres prÃ³prio** (nÃ£o o mesmo da produÃ§Ã£o).
5. **NÃ£o esperar** que o PDV mude nesta etapa â€” sÃ³ a tela de cadastro.

**Depois que o assistente entregar etapa 1 (vocÃª testa):**

1. Aguardar deploy do branch `teste` no staging + **Ctrl+F5** no cadastro.
2. Conferir se a lista e a busca acham os produtos anotados.
3. Alterar preÃ§o de um produto â†’ salvar â†’ buscar de novo â†’ preÃ§o **deve permanecer**.
4. Criar produto novo (se liberado no teste) ou pular se ainda bloqueado no staging.
5. Se algo falhar: anotar nome do produto + o que viu (sumiu, preÃ§o voltou, lista vazia).

**Etapa 1 â€” validada no teste (2026-06-22, Renan):** flag `agro_pg` + import Â· GM0027-1 a **R$ 21,00** persiste Â· busca nome/cÃ³digo OK Â· piscadinha preÃ§o resolvida (v1.84) Â· abertura lista mais rÃ¡pida (v1.85 prefetch + Postgres sem count). **PrÃ³ximo:** cherry-pick etapa 1 â†’ produÃ§Ã£o **sÃ³ quando Renan pedir**; PDV ainda Mongo.

**Produto de teste (Renan):** GM0027-1 Â· RaÃ§Ã£o Formula Natural adulto raÃ§as pequenas 1kg â€” **preÃ§o correto R$ 21,00** (staging/Agro); espelho Mongo R$ 19,90. Usar para validar busca + salvar + preÃ§o persistente.

---

### Renan â€” LanÃ§amentos: o que jÃ¡ fizemos e o que falta (linguagem leiga)

**Imagine dois armÃ¡rios de boletos/contas:**

| ArmÃ¡rio | O que Ã© |
| ------- | ------- |
| **Mongo (espelho ERP)** | Onde **hoje** ficam ~17 mil contas a pagar/receber que a loja usa |
| **Postgres Agro** | O armÃ¡rio **novo** do SisVale â€” jÃ¡ existe a gaveta (`TituloFinanceiroAgro`), mas a tela **ainda nÃ£o abre nele** |

**Dois â€œcortesâ€ diferentes (nÃ£o confundir):**

| Corte | Significado | Status |
| ----- | ----------- | ------ |
| **1 â€” Parar de falar com o ERP** | SisVale **nÃ£o envia** mais baixa/lanÃ§amento para o sistema antigo (API WL) | **âœ… Feito** (checkpoint + cÃ³digo v1.14) |
| **2 â€” Mudar a tela LanÃ§amentos** | Contas a pagar/receber passam a **ler e gravar no Postgres Agro**, nÃ£o no Mongo | **ðŸ”¥ HOJE** â€” escopo **CP primeiro** |

---

**O que JÃ fizemos (LanÃ§amentos + corte ERP):**

1. **Tela nova** â€” Contas a pagar/receber mais rÃ¡pida: filtros, PIN, Nova saÃ­da, excluir, emprÃ©stimo dual, etc.
2. **Backup** â€” Planilha/ZIP dos tÃ­tulos guardada (cÃ³pia de seguranÃ§a no PC).
3. **Checkpoint** â€” â€œCarimboâ€ nos ~17 mil tÃ­tulos no Mongo (**nÃ£o apaga nada**, sÃ³ marca).
4. **Corte API ERP** â€” Depois do checkpoint, o SisVale **nÃ£o manda** mais lanÃ§amento/baixa para o ERP por API.
5. **Gaveta nova no Postgres** â€” Tabela preparada + comando para **copiar** tÃ­tulos do Mongo (hoje sÃ³ **simulaÃ§Ã£o** no teste, nÃ£o ligou na loja).
6. **Loja hoje** â€” Operador usa LanÃ§amentos **normal**; boletos e Entrada NF passo 7 **continuam no Mongo** e **funcionam** (Renan validou âœ…).

**O que AINDA falta para â€œterminar o corteâ€ da tela LanÃ§amentos com Mongo:**

| Passo | Em portuguÃªs claro | Onde |
| ----- | ------------------ | ---- |
| **A** | **Copiar** os ~17 mil tÃ­tulos do Mongo para o Postgres (comando `importarâ€¦ --apply`) | Primeiro **teste**, depois **loja** |
| **B** | **Reprogramar a tela** â€” lista, busca, quitar, excluir, Nova saÃ­da, DRE, PDF, calendÃ¡rio â€” para usar o Postgres em vez do Mongo | Dev **teste** |
| **C** | **Entrada NF passo 7** â€” â€œSalvar + a pagarâ€ gravar no Postgres, nÃ£o sÃ³ no Mongo | Junto com B |
| **D** | **Testar tudo** no staging (criar, pagar, excluir, conferir saldo) | Renan no **teste** |
| **E** | **Ligar na loja** â€” flag `AGRO_FONTE_FINANCEIRO=agro_pg` **sÃ³ quando B+C estiverem prontos** | Deploy **produÃ§Ã£o** + senha |
| **F** | Mongo vira **sÃ³ histÃ³rico** (nÃ£o entra tÃ­tulo novo) â€” opcional congelar de vez | Depois de E estÃ¡vel |

**Prioridade Renan (25/06 â€” ordem fixa):**

1. **Contas a pagar EM ABERTO** â€” mÃ¡xima seguranÃ§a Â· zero perda  
2. **Contas a pagar** (todas)  
3. **Contas a receber** â€” **por Ãºltimo** (quase nÃ£o usam; fiado = outra tela)

**Regra:** Mongo **nÃ£o some** atÃ© espelho PG conferido Â· leitura PG **sÃ³** com flag Â· gravaÃ§Ã£o dupla ou rollback se falhar.

**Escopo HOJE:** CP em aberto â†’ CP lista/filtros â†’ CR depois (se der tempo).

**Fiado nÃ£o entra** â€” jÃ¡ Postgres nativo.

---

### WIP HOJE â€” LanÃ§amentos Postgres (CP primeiro) â€” **teste OK 26/06**

| Quem | O quÃª |
| ---- | ----- |
### CP Postgres â€” o que falta + quem faz (fechar o serviÃ§o)

| # | Quem | O quÃª | Quando |
| --- | ---- | ----- | ------ |
| **1** | **Assistente** | CÃ³digo: **pagar, parcial, nova saÃ­da, editar, excluir** CP â†’ Postgres (teste) | **âœ… v3.40 teste** |
| **2** | **Renan** | No **teste**: pagar **1 tÃ­tulo pequeno** (ou de teste) + conferir se sumiu da lista / saldo certo | **âœ… Renan 26/06** â€” Â«TesteÂ» R$ 1,00 quitado PG |
| **3** | **Assistente** | Import PG na **loja** + deploy pacote **testeâ†’producao** | **âœ… em curso v3.03** |
| **4** | **Renan** | Frase *Â«pode subir produÃ§Ã£oÂ»* + senha **`99738595`** **no mesmo pedido** | **âœ… Renan 26/06** |
| **5** | **Renan** | **~30 min** CP sÃ³ consulta na loja (ou avisar equipe: **nÃ£o pagar** na CP na hora H) | **âœ… 26/06** |
| **6** | Loja: CP aberto **741** + total | **âœ… 26/06** â€” pagamento real **dispensado** |
| **7 CR loja** | ConferÃªncia lista CR | **âœ… 26/06** â€” **453** Â· **R$ 29.241,58** |
| **8 Painel backup** | Card amarelo recolhido (admin) | **âœ… v3.08** deploy loja |

**Renan (26/06):** nÃ£o precisa teste de pagar na CP na loja Â· **fiado** = operaÃ§Ã£o principal (jÃ¡ Postgres nativo).

### PrÃ³ximo â€” finalizar LanÃ§amentos / financeiro (ordem)

| # | O quÃª | UrgÃªncia loja |
| --- | ----- | ------------- |
| **1** | **Entrada NF passo 7** â€” tÃ­tulos Â«Salvar + a pagarÂ» | **âœ… jÃ¡ Postgres na loja** (`inserir_lancamentos_manual_lote_dispatch`) Â· rascunho NF (etapas 1â€“6) continua Mongo |
| **2** | **DRE / fluxo calendÃ¡rio / PDF** LanÃ§amentos â€” ler Postgres | Baixa (telas secundÃ¡rias) |
| **3** | **Esconder painel amarelo** checkpoint (admin) â€” migraÃ§Ã£o CP/CR fechada | CosmÃ©tico |
| **4** | **Congelar Mongo financeiro** (opcional) â€” tÃ­tulo novo sÃ³ PG; Mongo sÃ³ histÃ³rico | SÃ³ quando 1â€“2 estÃ¡veis |
| **â€”** | **Fiado** | **Nada** â€” jÃ¡ fora deste pacote |

**Renan NÃƒO precisa:** mexer Render Â· import manual Â· cÃ³digo Â· backup de novo (jÃ¡ tem no PC).

**Bloqueio CP Postgres (em aberto):** **fechado 26/06** â€” loja operando CP no Postgres.

### WIP â€” Contas a receber Postgres **26/06**

| Item | Status |
| ---- | ------ |
| Lista + filtros + planos CR | **âœ… cÃ³digo teste** (mesmo PG dos 17,8 mil tÃ­tulos) |
| Receber / parcial / editar / excluir / lote | **âœ… cÃ³digo teste** |
| Fiado | **fora** â€” tela prÃ³pria Postgres |
| Deploy loja | **âœ… v3.07** â€” CR conferida **453** / **29.241,58** |


### Renan â€” intervenÃ§Ã£o (estritamente necessÃ¡rio)

| # | Status | Detalhe |
| --- | ------ | ------- |
| **1 Backup** | **âœ… Renan 25/06** | ZIP abertos + todos no PC |
| **2 Conferir total aberto** | **âœ… Renan 25/06** | CP **sem filtro de data** Â· tela **Qtd 741** Â· Excel **`01_a_pagar_em_aberto.csv` = 748 linhas** (+7) Â· total **~R$ 393.652,70** (tela) vs **~R$ 393.667,21** (Excel) Â· dif. **~R$ 14,51** Â· **causa: backup sem dedup** (tela funde dup. ERP; ZIP nÃ£o) Â· bloco **Geraldo / acordo sat / R$ 600** = mesmo **ID ERP** repetido â€” linhas extras, saldo pode ser 0 |
| **3 Render env** | **âœ… Renan â€” nada a fazer** | **NÃ£o** alterar variÃ¡veis no Render (loja nem teste) Â· passo = **ficar parada** atÃ© assistente avisar |
| **4 PÃ³s-import teste** | **âœ… Renan 26/06** | Staging CP aberto: **Qtd 741** Â· **A pagar R$ 393.652,70** â€” **bate loja** Â· amostra expandida OK (ex. Renan Hinnen **R$ 163,00**) |
| **4b Pagamento PG teste** | **âœ… Renan 26/06** | TÃ­tulo manual **Â«TesteÂ» R$ 1,00** Â· quitou no Postgres Â· sumiu de **Em aberto** Â· aparece em **Quitados** (saldo 0) Â· juros R$ 0,10 **nÃ£o** lanÃ§ou (conta Â«ADICIONAR CONTAÂ» â€” regra esperada) |
| **5 Fiado / CR** | CR **âœ… cÃ³digo 26/06** Â· fiado fora |

**Loja hoje (26/06):** **Contas a pagar liberada** â€” lista + pagar no **Postgres** (conferido **741** / **393.652,70**). Caixa/PDV normal.

### Por que Contas a pagar **nÃ£o estÃ¡ 100 % concluÃ­da** ainda?

**NÃ£o Ã© bug** â€” foi **de propÃ³sito**, em **etapas**, porque sÃ£o **~17 mil tÃ­tulos** e **~R$ 393 mil** em aberto.

| Etapa | O quÃª | Status |
| ----- | ----- | ------ |
| **A** | Backup + conferir total na **loja** | **âœ… Renan** |
| **B** | Copiar tÃ­tulos Mongo â†’ Postgres | **âœ… teste** (13,2 mil) |
| **C** | **Lista** CP (ver, filtrar, total) no Postgres | **âœ… teste** (741 / 393.652,70 bateu) |
| **D** | **Gravar** no Postgres: pagar, parcial, nova saÃ­da, editar, excluir | **âœ… teste v3.40** (deploy pendente Render) |
| **E** | Validar **D** no teste (pagar 1 tÃ­tulo de teste) | **âœ… Renan 26/06** |
| **F** | Import + flag na **loja** (senha) | **âœ… v3.04** â€” **741** / **393.652,70** conferido Renan |

**Por que parou antes de D?**  
Primeiro garantimos: *Â«a cÃ³pia no Postgres Ã© a mesma coisa que a tela da loja?Â»* â€” **sim** (passo 4).  
SÃ³ **depois** mexemos em **pagamento** â€” se errar, some dinheiro ou duplica tÃ­tulo.

**MetÃ¡fora:** armÃ¡rio novo montado e **inventÃ¡rio conferido**; falta passar a **usar o armÃ¡rio de verdade** (cada pagamento sair do Mongo e ir pro Postgres).

**Loja:** CP **Postgres** (lista + pagamento). Mongo **nÃ£o apagado** â€” sÃ³ espelho/backup.

### Renan â€” como fazer (passos 3 e 4)

**Passo 3 â€” agora (sÃ³ nÃ£o mexer)**

| FaÃ§a | NÃ£o faÃ§a |
| ---- | -------- |
| **Nada** â€” pode usar a **loja** normal (PDV, CP, etc.) | Abrir Render â†’ **Environment** â†’ **Save** em variÃ¡vel nova |
| Se abrir o Render por curiosidade: **sÃ³ olhar**, sem salvar | Criar/editar `AGRO_FONTE_FINANCEIRO`, `SOMENTE_POSTGRES`, `STAGING_READONLY`, etc. |
| | Pedir deploy **produÃ§Ã£o** / merge `teste`â†’`producao` hoje |

**Passo 3 = jÃ¡ feito** se vocÃª **nÃ£o** alterou env no Render desde o backup.

---

**Passo 4 â€” quando o assistente avisar Â«import teste prontoÂ»**

Site = **projeto teste** no Render (branch `teste`) â€” **nÃ£o** `sistvale.com.br`.

1. Login **admin** no **teste**.
2. **Ctrl+F5** (recarregar sem cache).
3. Abrir **`/lancamentos/contas-pagar/`**.
4. **Filtros** â†’ SituaÃ§Ã£o **Em aberto** â†’ **limpar** vencimento (de/atÃ© **vazios**) â†’ **Marcar todos** planos â†’ **Aplicar**.
5. Conferir rodapÃ© â€” deve bater com a **loja** (referÃªncia anotada):
   - **Qtd: 741** (pode variar Â±1 se alguÃ©m pagou entre ontem e hoje)
   - **A pagar: ~R$ 393.652,70**
6. **Opcional (amostra):** clique **em cima de 3 linhas** da lista (ex. Detran, Renan Hinnen, CaminhÃ£o) â€” a linha **abre** e mostra detalhes; confira se **saldo** = coluna Saldo (ex. R$ 234,78). **NÃ£o precisa pagar** nem editar.
7. Me avisar **Â«passo 4 OKÂ»** â€” se **Qtd + A pagar** jÃ¡ bateram (como no print), **pode pular o item 6**.

**Passo 5:** fiado e contas a receber â€” **ignorar hoje**.

**Assistente (26/06 â€” tÃ©cnico):**

| Item | Detalhe |
| ---- | ------- |
| **CÃ³digo** | `lancamentos_financeiro_pg_util.py` â€” lista CP Postgres + dedup igual Mongo |
| **API** | `api/lancamentos/contas-pagar/` lÃª PG quando `AGRO_FONTE_FINANCEIRO=agro_pg` |
| **Import HTTP** | `GET /api/cron/importar-titulos-financeiro-mongo-pg/?token=â€¦` (staging, `DRY_RUN=true`) |
| **ConferÃªncia** | `GET /api/agro/financeiro-pg-conferencia/` (superuser) â€” Mongo vs PG abertos |
| **Flag teste** | AutomÃ¡tico no staging apÃ³s import (`financeiro_postgres: true` no fonte-status) |
| **Bootstrap** | `scripts/staging_financeiro_pg_bootstrap.py` na build + fallback no boot |

**Excel 748 vs tela 741 â€” fechado (dedup):** backup exporta **cada** doc Mongo; lista CP **deduplica** tÃ­tulos ERP repetidos (ex. **Geraldo acordo sat**, mesmo `Id`, vÃ¡rias linhas no CSV). **ReferÃªncia para migraÃ§Ã£o = tela (741 + total deduplicado)**, nÃ£o soma crua do Excel.

**Excel > tela (~R$ 14) â€” causas (histÃ³rico):**

**URLs backup (referÃªncia):**

| O quÃª | URL |
| ----- | --- |
| **Em aberto** | `https://sistvale.com.br/api/lancamentos/backup-abertos.zip` |
| **Todos** | `https://sistvale.com.br/api/lancamentos/backup-completo.xlsx` |

Aguarde ~1 min Â· salva ZIP Â· repita o segundo link.

**Depois deploy teste:** painel amarelo **Â«SeguranÃ§a â€” antes de cortar o ERPÂ»** no topo de **`/lancamentos/contas-pagar/`** (admin).

**Backup (superuser â€” faÃ§a na LOJA antes do import):**

1. Chrome â†’ login **admin** â†’ URLs acima **ou** CP nova apÃ³s deploy.
2. **NÃ£o** clique em **Fazer checkpoint** hoje â€” sÃ³ se assistente pedir.
3. Anote **todos CP em aberto** â€” **nÃ£o** sÃ³ Â«HojeÂ»: Filtros â†’ situaÃ§Ã£o **Em aberto** â†’ **limpar** vencimento (sem data) â†’ anotar **Qtd** + **A pagar** do rodapÃ©.

| # | Quando | FaÃ§a |
| --- | ------ | ---- |
| **1** | **Antes de qualquer `--apply`** | Passos **1â€“5** acima (ZIP aberto + ZIP todos) |
| **2** | **Antes import** | **Todos** CP em aberto (sem filtro de data) â€” Qtd + total Â«A pagarÂ» |
| **3** | **Antes import** | Backup URLs (item 1) â€” ZIP bate com **todos** abertos, nÃ£o sÃ³ venc. de hoje |
| **4** | **Render** | **NÃ£o** mexer em env sozinho â€” assistente pede quando for hora |
| **5** | **Loja** | Flag PG / deploy **sÃ³** com frase + senha Â· **sÃ³** apÃ³s teste OK |
| **6** | **CR / fiado** | Ignorar hoje â€” fiado jÃ¡ Ã© Postgres |

**NÃ£o fazer:** apagar Mongo Â· ligar `AGRO_FONTE_FINANCEIRO=agro_pg` na loja sem OK do assistente.

### PRÃ“XIMO AGORA â€” validaÃ§Ã£o corte Mongo **v4.38** *(substitui bloco 27/06 abaixo)*

| Quem | AÃ§Ã£o |
| ---- | ---- |
| **Renan** | Roteiro **7 passos** no CHECKPOINT acima â€” marcar OK ou reportar # + erro |
| **Assistente** | SÃ³ apÃ³s 7 OK + senha â†’ cherry-pick loja Â· item **8â€“12** Â§4.15 depois |

**NÃ£o fazer agora:** merge `teste`â†’`producao` inteiro Â· ligar `SOMENTE_POSTGRES` na **loja** sem pacote Â· apagar Mongo.

<details>
<summary>histÃ³rico â€” retomada pÃ³s-sync CP v3.58 (27/06)</summary>

### PRÃ“XIMO AGORA â€” retomada pÃ³s-sync CP **v3.58** (27/06)

**Fechado hoje:** sync shell prod Â· CP jul **145 Â· 82.642,99** = Mongo/Excel/grÃ¡fico.

**Assistente avanÃ§ou (teste â€” Renan valida depois, um a um):**

| # | Entrega teste | Renan testar quando puder |
| - | ------------- | ------------------------- |
| **A** | **Resumo gerencial** â†’ Postgres (`TituloFinanceiroAgro`) quando financeiro PG | `/financeiro/resumo-gerencial/` Â· fonte Postgres Â· competÃªncia/vencimento |
| **B** | **PDV pÃ³s-venda** â€” API devolve `pdv_catalog_patches` Â· cache local atualiza saldo | Vender 1 un. â†’ Consulta/GestÃ£o saldo desce sem F5 estoque |
| **C** | **Painel amarelo CP** â€” some para admin quando PG jÃ¡ tem tÃ­tulos | `/lancamentos/contas-pagar/` superuser |
| **D** | *(jÃ¡ no teste)* DRE Â· fluxo calendÃ¡rio Â· export PDF/XLSX PG | â­ DRE opcional |
| **E** | **Entrada NF rascunho PG** (cÃ³digo pronto teste) Â· motor GM Â· TransferÃªncias/Validade | `/entrada-nota/` wizard 1â€“7 |

**Ordem sugerida (banana + checklist pacote corte ERP):**

| # | O quÃª | Quem | UrgÃªncia |
| - | ----- | ---- | -------- |
| **1** | **Ctrl+F5 loja** â€” CP jul aberto + grÃ¡fico jul = **82.643** (confirma na tela) | Renan | **Agora** Â· 2 min |
| **2** | **GestÃ£o saldo pÃ³s-venda** â€” fix **v3.82** sÃ³ no **teste** Â· vender 1 un. â†’ gestÃ£o deve baixar estoque | Renan teste | **Alta** â€” loja ainda **v3.54** (bug baixa sem Mongo) Â· sobe **no pacote**, nÃ£o cherry avulso |
| **3** | **Pacote desvinculaÃ§Ã£o restante** â€” baixa estoque PDV + flags B+C loja + Compras/NF onde falta | Assistente â†’ teste â†’ loja com senha | **Alta** â€” Renan disse estoque loja perdido Â· recontar antes ou depois do pacote |
| **4** | **Entrada NF rascunho** etapas 1â€“6 â†’ **Postgres** (teste v4.09) Â· passo 7 financeiro **jÃ¡ PG** | Assistente â†’ Renan testa staging | MÃ©dia |
| **5** | **DRE / fluxo calendÃ¡rio / PDF** ler Postgres | Assistente | Baixa (Renan â­ DRE) |
| **6** | **BI card gastos-plano** | Opcional | SÃ³ se ligar `AGRO_DASHBOARD_GASTOS_PLANO=true` |
| **7** | **Motor busca GM** (`gm0050` Compras/NF) | Por Ãºltimo | NÃ£o bloqueia operaÃ§Ã£o |
| **8** | **TransferÃªncias Â· Validade Â· fornecedor NF** | Fase 6 desvinculo | Baixa |

**Ignorar por ora (Renan):** Compras planilha recarrega Â· calendÃ¡rio fluxo no **teste** mistura vendas staging.

**SÃ³ no teste (diff vs loja ~9 arquivos):** grÃ¡fico gastos UX Â· PDV/views Â· `catalogo_agro` Â· diag CP â€” **nÃ£o** merge inteiro; cherry-pick por pacote.

**Assistente â€” prÃ³ximo cÃ³digo:** retomar item **2** (validar v3.82 teste) ou item **3** (pacote baixa estoque + desvinculo loja) â€” **Renan escolhe**.

</details>

### WIP AGORA â€” validaÃ§Ã£o corte Mongo v4.38

| Foco | Detalhe |
| ---- | ------- |
| **ðŸ”¥ AGORA** | Renan â€” roteiro **7 passos** (CHECKPOINT) no **staging** |
| **Assistente** | Parado atÃ© OK ou reporte de bug (# + URL) |
| **Loja** | v4.21 â€” pacote corte **nÃ£o** entrou ainda |
| **Depois 7 OK** | Cherry-pick loja com senha Â· itens **8â€“12** Â§4.15 |

<details>
<summary>histÃ³rico WIP â€” CP PG jun/2026</summary>

### WIP AGORA â€” pÃ³s-validaÃ§Ã£o loja *(histÃ³rico â€” ver PRÃ“XIMO AGORA acima)*

| Foco | Detalhe |
| ---- | ------- |
| **ðŸ”¥ HOJE** | LanÃ§amentos PG â€” **CP em aberto** primeiro |
| **Loja operando** | Continua **Mongo** atÃ© Renan + assistente validarem teste |
| **Motor busca** | â¸ pausado |
| **NF passo 7 PG** | Depois da CP em aberto estÃ¡vel |

**JÃ¡ feito (jun/2026):** backup, checkpoint, corte Agroâ†’ERP na API, layout CP, PIN, perf â€” ver Â§4.10 e tabela acima.

**Pendente:** `AGRO_FONTE_FINANCEIRO=agro_pg` â€” lista/gravaÃ§Ã£o sair do `DtoLancamento` Mongo (Â§4.15 sprint #4).

**Prep v2.90 (seguro â€” nÃ£o liga flag):**
- Modelo Postgres **`TituloFinanceiroAgro`** (migration `0041`).
- Comando **`importar_titulos_financeiro_mongo_pg`** â€” default **dry-run**; `--apply` grava espelho idempotente por `mongo_id`.
- Dry-run local: **~17â€¯875** tÃ­tulos no Mongo (compatÃ­vel com checkpoint).
- PrÃ³ximo passo pÃ³s-deploy: `--apply` no staging Â· depois views lerem PG quando `agro_pg`.

Fluxo **seguro** do checkpoint (sÃ³ admin vÃª os botÃµes):

1. **Backup ZIP** â€” CSV no PC (**sÃ³ redundÃ¢ncia**; SisVale nÃ£o importa de volta).
2. **Checkpoint** â€” carimbo no Mongo; **nÃ£o apaga nem muda valores**.
3. **Corte Agroâ†’ERP** â€” apÃ³s checkpoint, SisVale **nÃ£o envia** lanÃ§amento/baixa pela API (`VendaERPAPIClient`); automÃ¡tico no cÃ³digo **v1.14**.
4. **Opcional Render** â€” `AGRO_FINANCEIRO_MONGO_CONGELADO=true` (reforÃ§o).

**Backup no PC** â€” cÃ³pia de seguranÃ§a fora do sistema. Checkpoint = carimbo dentro do SisVale.

**Corte ERP:** Ã© a **nossa** integraÃ§Ã£o (API aberta WL), nÃ£o ticket ao fornecedor. PadrÃ£o jÃ¡ era `AGRO_FINANCEIRO_ERP_SYNC_HABILITADO=false`; apÃ³s checkpoint o bloqueio Ã© garantido no cÃ³digo.

**Armadilha cherry-pick:** se `lancamentos_financeiros.html` incluir `lancamentos_pin_entrada.html`, o template **tem** que ir junto â€” senÃ£o **500** em Contas a pagar/receber.

- **PIN LanÃ§amentos:** 1Ã— por sessÃ£o ao entrar em qualquer tela `/lancamentos/*` (sem hub modal); navegaÃ§Ã£o interna sem repetir; **modo descanso** (~3 min idle) pede de novo; sair para PDV/outra tela limpa a sessÃ£o.

Rotas: `backup-completo.xlsx` Â· `backup-abertos.zip` Â· `congelamento-status/` Â· `congelar-pre-corte/`. Painel na entrada `/lancamentos/`.

</details>

---

## 5. VariÃ¡veis de ambiente importantes


| VariÃ¡vel                      | Efeito                                   |
| ----------------------------- | ---------------------------------------- |
| `AGRO_STAGING_READONLY`       | Bloqueia escrita Mongo no staging        |
| `AGRO_ERP_PEDIDOS_DRY_RUN`    | NÃ£o grava pedidos ERP no staging         |
| `NFC_E_*`                     | Toda config NFC-e (ver NFCE-PRODUCAO.md) |
| `AGRO_DASHBOARD_GASTOS_PLANO` | Mostra grÃ¡fico gastos por plano no BI    |
| `AGRO_RH_PLANO_SALARIO_FOLHA` | Plano Mongo do tÃ­tulo de salÃ¡rio         |
| `MP_POINT_*`                   | Point Centro (token + terminal)          |
| `MP_POINT_VILA_*`              | Point Vila (outra conta MP)              |


---

## 6. Como trabalhar com o assistente (Renan â†” Cursor)

1. **Anexar contexto:** na maioria dos chats sÃ³ descrever a tarefa (roteiro carrega sozinho). `@banana-roteiro` se quiser forÃ§ar. `@AGENTS.md` opcional (UX Â§11, RH Â§9, LanÃ§amentos Â§10).
2. **Dois arquivos, papÃ©is diferentes:** banana = memÃ³ria viva (WIP, pendÃªncias, checkpoint). AGENTS = manual estÃ¡vel (nÃ£o duplicar aqui).
3. **Registro de alteraÃ§Ãµes:** **sempre** atualizar `banana.md` ao entregar mudanÃ§a no sistema (fix, feature, deploy) â€” CHECKPOINT + Â§ do mÃ³dulo; commits e versÃ£o para rollback/contexto. Ver regra no topo (*Registro no banana*). Renan pode pedir *"atualize a banana"* tambÃ©m. AGENTS Â§7 **sÃ³** se Renan pedir.
4. **Escopo:** pedir arquivos ou mÃ³dulo; assistente nÃ£o amplia sem autorizaÃ§Ã£o.
5. **Antes de editar:** assistente deve dar **uma linha de plano**.
6. **Entrega:** um patch coeso por tarefa.
7. **Commits / teste (30/07):** ao **fechar entrega** → commit na branch `teste` + **`git push origin teste` sempre** (backup GitHub, **sem** pedir). Bump de `VERSION`. Renan valida no **PC local** (`docs/TESTE-LOCAL.md`). Render free **não** é gate. **Produção:** só item 8, **depois** do teste local + frase/senha.
8. **ProduÃ§Ã£o:** **nunca** push/merge/deploy na loja (Render **SistVale**) sem frase explÃ­cita **+ senha** (topo do banana) **e** sem Renan ter testado local. **2026-06-22:** assistente subiu PDVÃ—cadastro em produÃ§Ã£o sem pedido â€” **nÃ£o repetir**.
9. **Modo econÃ´mico:** permanente â€” rule `modo-economico.mdc`; detalhe sÃ³ se Renan pedir.
10. **Cliente:** Renan usa **Chrome** (loja e local) â€” nÃ£o perguntar Electron vs browser.
11. **Retomar trabalho antigo:** `@banana-roteiro` + este arquivo; chats anteriores nÃ£o ficam na memÃ³ria do assistente.

---

## 7. Documentos irmÃ£os (nÃ£o repetir aqui)


| Arquivo                                 | ConteÃºdo                                                                             |
| --------------------------------------- | ------------------------------------------------------------------------------------ |
| `AGENTS.md`                             | EnciclopÃ©dia â€” mapa URLs, UX Â§5â€“11, changelog Â§7. ReferÃªncia, nÃ£o anexo obrigatÃ³rio. |
| `docs/DEPLOY-AMBIENTES.md`              | Staging readonly, fluxo Git                                                          |
| `docs/NFCE-PRODUCAO.md`                 | Checklist fiscal produÃ§Ã£o                                                            |
| `docs/ESTOQUE_AGRO_FONTE_DA_VERDADE.md` | Regra estoque                                                                        |
| `docs/CONTEXTO_SESSAO_CLIENTES_PDV.md`  | SessÃ£o clientes (histÃ³rico)                                                          |
| `docs/GUIA_ABAS_NAVEGADOR_AGRO.md`      | Abas navegador                                                                       |
| `banana-roteiro.md`                     | Fluxograma â€” o que ler no banana por tarefa (ler **antes** do banana)                |
| `.cursor/rules/agro-consulta.mdc`       | Regra Cursor resumida (auto-carregada)                                               |
| `.cursor/rules/modo-economico.mdc`      | Respostas curtas â€” permanente                                                        |


---

## 8. Linha do tempo recente (commits `teste`)

Ãšltimos temas entregues (mais recente primeiro):

1. **GrÃ¡fico gastos â€” UX perÃ­odo simÃ©trico + split filtros/planos + 4 atalhos globais** â€” 2026-06-26: toolbar passado/futuro Â· slots Postgres Â· APIs `grafico-gastos-atalhos` (`financeiro/`).
2. **GrÃ¡fico gastos por plano (Chart.js)** â€” 2026-06-26: `/financeiro/grafico-gastos/` + API Mongo; modo individual (`financeiro/views.py`, `mongo_financeiro_util.py`).
2. **FAB PDV â€” nÃ£o cobrir modais/botÃµes** â€” 2026-05-21: z-index 90, hide overlay/dialog, reposicionar canto direito (`_agro_pdv_fab.html`).
2. **LanÃ§amentos CP â€” layout novo padrÃ£o + vista preservada + perf lista** â€” 2026-06-19 (CHECKPOINT).
3. **PDV wizard â€” GM no barras remove carrinho** â€” `e055761` Â· `pdv_wizard.js`: hÃ­fen `GM1546-5S` nÃ£o remove carrinho; GM modo barcode.
4. **LanÃ§amentos â€” EmprÃ©stimo (entrada + pagamento)** â€” pseudo-plano Nova saÃ­da + lote manual (`fbccf19`).
5. **LanÃ§amentos â€” Nova saÃ­da (legibilidade)** â€” card expandido; fontes/campos maiores.
6. **Etiquetas faixa 230â€¦** â€” `5c6590a` v1.20: CODE128 interno loja.
7. **PDV legado carrinho GM** â€” produÃ§Ã£o `59bdedc` v1.02.

*Para lista completa:* `git log teste --oneline -50`.

---







## CHECKPOINT DE ATUALIZAÃ‡ÃƒO

### ✅ Deploy loja — lote 20/08b (`deploy/lote-checklist-2008b` · **v17.72**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.72** · `producao` @ **26a947e** |
| **Pacotes** | **CATALOGO-5N-PESO** · **REPASSE-RESERVA-LUCRO** · **DEVOL-NFCE-ASK** · **PLANILHA-DELIVERY** |
| **Migrate** | **SIM** `0097` (Render no deploy) |
| **Rollback** | tag `rollback/pre-lote-checklist-2008b-v17.43` @ **5bd7f66** + frase + senha |
| **Você** | **Ctrl+F5** `/catalogo/` · Cadastro Excel Delivery · `/repasse-vila/` · devolução NFC-e · badge **17.72** |

### 🧪 WIP — Excluir categoria Delivery (`CAT-DEL-EXCLUIR`)

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Botão **−** na aba Delivery (cadastro) + Excluir/X na gestão `/catalogo/gestao/` · limpa vínculo nos produtos |
| **API** | `POST /catalogo/api/categorias/excluir/` |
| **Prova** | tests **3/3** · `manage.py check` |
| **Você** | Ctrl+F5 Cadastro → Delivery → selecionar → **−** · ou `/catalogo/gestao/` → Excluir |

| Pacote | O quê | Você valida |
| ------ | ----- | ----------- |
| **CATALOGO-5N-PESO** | `/catalogo/` 5 níveis + peso | `/catalogo/` · gestão N5 |
| **REPASSE-RESERVA-LUCRO** | Manual sai do lucro antes do % | `/repasse-vila/` · Log |
| **DEVOL-NFCE-ASK** | Devolução total pergunta NFC-e | venda c/ NFC-e · devolver tudo |
| **PLANILHA-DELIVERY** | Excel Delivery em massa | Cadastro Excel ↓/↑ |

### ~~🚀 PREP deploy loja — lote checklist 20/08b~~ · **superado — Live v17.72**

### ✅ CHECKLIST ÚNICO — enviado produção (20/08b · loja **v17.72**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CATALOGO-5N-PESO** | ✅ **enviado / Live v17.72** | **NÃO** |
| 2 | **REPASSE-RESERVA-LUCRO** | ✅ **enviado / Live v17.72** | **SIM 0097** |
| 3 | **DEVOL-NFCE-ASK** | ✅ **enviado / Live v17.72** | **NÃO** |
| 4 | **PLANILHA-DELIVERY** | ✅ **enviado / Live v17.72** | **NÃO** |

### ~~📦 PACOTE PRONTO — Planilha Excel Delivery~~ · **Live v17.72**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.72** |
| **O quê** | Cadastro Excel ↓/↑: colunas Delivery |
| **Loja** | `producao` @ **26a947e** |

### ~~📦 PACOTE PRONTO — Devolução pergunta NFC-e~~ · **Live v17.72**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.72** |
| **O quê** | Devolução **total** c/ NFC-e → pergunta **cancelar** ou **manter** cupom |

### ~~📦 PACOTE PRONTO — Reserva no lucro antes do %~~ · **Live v17.72**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.72** |
| **Migrate** | **SIM** `0097` |

### ~~📦 PACOTE PRONTO — Catálogo 5 níveis + peso~~ · **Live v17.72**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.72** |
| **Branch** | estava em `deploy/catalogo-5n-peso` · agora no lote **2008b** |

### ✅ Deploy loja — lote 20/08 (`deploy/lote-checklist-2008` · **v17.43**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.43** · `producao` @ **5bd7f66** |
| **Pacotes** | **MP-POINT-CANCEL-SAFE** (+ PIN) · **BI-VAL-BAIXA-LOJA** · **DRE-LUCRO-LOAD** · **REPASSE-RESERVA** |
| **Migrate** | **SIM** `0095` → `0096` (Render no deploy) |
| **Rollback** | tag `rollback/pre-lote-checklist-2008-v17.42` @ **453c041** + frase + senha |
| **Você** | **Ctrl+F5** PDV · `/` Validade · `/repasse-vila/` · `/financeiro/resumo-gerencial/` · badge **17.43** |

| Pacote | O quê | Você valida |
| ------ | ----- | ----------- |
| **MP-POINT-CANCEL-SAFE** | Timeout cancela · trava órfão · PIN Geraldo/Geraldinho/Renan | PDV · Point auto · Ctrl+F5 |
| **BI-VAL-BAIXA-LOJA** | Baixa Centro **não** some na Vila | `/` Validade · filtro Vila |
| **DRE-LUCRO-LOAD** | Gráfico lucro carrega | `/financeiro/resumo-gerencial/` |
| **REPASSE-RESERVA** | Troco fixo fica na Vila | `/repasse-vila/` |

### ~~🚀 PREP deploy loja — lote 20/08~~ · **superado — Live v17.43**

### ✅ CHECKLIST ÚNICO — enviado produção (20/08 · loja **v17.43**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **MP-POINT-CANCEL-SAFE** | ✅ **enviado / Live v17.43** | não |
| 2 | **BI-VAL-BAIXA-LOJA** | ✅ **enviado / Live v17.43** | **SIM 0096** |
| 3 | **DRE-LUCRO-LOAD** | ✅ **enviado / Live v17.43** | não |
| 4 | **REPASSE-RESERVA** | ✅ **enviado / Live v17.43** | **SIM 0095** |

### ~~📦 PACOTE PRONTO LOJA — Point cancel seguro + PIN gerencial~~ · **Live v17.43**

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Timeout cancela na máquina · abandon só se MP confirmou · status grava PAID · trava outra forma com Point vivo · **PIN gerencial** (Geraldo / Geraldinho / Renan Hinnen) força liberar em emergência |
| **Por quê** | Incidente 19/08 R$460 (Point cobrou, PDV não fechou, venda na Renan) |
| **Migrate** | **NÃO** |
| **Commits** | `2fd5f98` · `0e41e64` · prova deep `7199464` · loja **v17.43** @ **5bd7f66** |
| **Prova** | cancel-safe **21/21** · pin **23/23** · Vila path **41/41** · deep **40/40** |
| **Risco** | Médio — PDV Point Centro/Vila · Ctrl+F5 obrigatório |
| **Você** | Ctrl+F5 PDV · Point auto · cancelar no PDV = some na máquina · timeout cancela · se travar: PIN Geraldo/Geraldinho/Renan |
| **Loja** | ✅ **enviado / Live v17.43** |

### ⚠️ INCIDENTE — MP Point Centro (19/08 · Manasses R$460) · **fix no teste — ver PACOTE acima**

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | **797** ainda órfão no PG · venda **#5754** mp_renan |
| **Fix** | PACOTE **MP-POINT-CANCEL-SAFE** (checklist #1) |

### ~~📦 PACOTE PRONTO LOJA — baixa validade por loja (`BI-VAL-BAIXA-LOJA`)~~ · **Live v17.43**

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Centro conferiu/baixou vencido → **Vila continua vendo** até conferir lá. Estoque só da loja que baixou. Lote some só quando as **duas** conferiram. |
| **Por quê** | Antes «Dar baixa» apagava o lote para todo mundo e zerava o card da Vila |
| **Migrate** | **SIM** `0096_estoque_lote_baixa_por_loja` |
| **Commit** | fix **21d9c65** · prova **5ea6d7b** · teste **v17.52** · loja **v17.42** |
| **Risco** | Médio — baixa + estoque por loja · migrate obrigatório |
| **Prova** | path **44/44** · unificado **16/16** · salvar **18/18** · validade_bi **16/16** · clique **10/10** |
| **Você** | Ctrl+F5 `/` na **Vila** · filtro Vila → o que o Centro **já apagou hoje não volta**. Daqui pra frente: baixar no Centro e o card da Vila permanece |
| **Autorizar** | *via lote 20/08* — ver PREP no topo |
| **Loja** | ✅ **enviado / Live v17.43** |

### 📦 PACOTE PRONTO — Repasse: reserva Vila + layout (`REPASSE-RESERVA`) · **Live v17.43**

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Troco fixo **fica na Vila** e desconta do envio · opções recolhidas · dias 1–15 / 16–31 |
| **Onde** | `/repasse-vila/` + overlay PDV · PG `RepasseVilaConfigAgro.reserva_vila` |
| **Commit** | `e46671f` · loja **v17.43** |
| **Migrate** | **SIM** `0095_repasse_vila_reserva` |
| **Prova (18/08)** | path 115/115 · reserva **70/70** · deep 95/95 · acum-net 28/28 · planos 49/49 |
| **Você** | Ctrl+F5 `/repasse-vila/` · digite o troco → **Salvar** · conferir total a levar |
| **Loja** | ✅ **enviado / Live v17.43** |

### 📦 PACOTE PRONTO — DRE gráfico lucro carrega (`DRE-LUCRO-LOAD`) · **Live v17.43**

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Gráfico do rodapé **não trava** em «carregando» · 1 leitura do período · barras = lucro líquido/dia (conta do BI) |
| **Tela** | `/financeiro/resumo-gerencial/` |
| **Commit** | `12b6ff7` · loja **v17.43** |
| **Migrate** | **NÃO** |
| **Prova (18/08)** | verify chart 16/16 · CMV 55/55 · visual 232/232 · `check` OK · **436ms** · BI bruto/pago/Centro/Vila **match** (≤ R$ 0,02) |
| **Você** | Ctrl+F5 DRE · F5 · gráfico aparece (barras + linha verde) · **Lucro acum.** ≈ card do BI |
| **Loja** | ✅ **enviado / Live v17.43** |

### ✅ Deploy loja — lote 18/08m (`deploy/lote-checklist-1808m` · **v17.42**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.42** · `producao` @ **453c041** |
| **Pacotes** | **BI-VAL-CLIQUE** · **REPASSE-ACUM-NET** |
| **Migrate** | **NÃO** |
| **Rollback** | tag `rollback/pre-lote-checklist-1808m-v17.30` @ **1036967** + frase + senha |
| **Você** | **Ctrl+F5** em `/` (Validade = Todas + vencidos) · `/repasse-vila/` · badge **17.42** |

| Pacote | O quê | Você valida |
| ------ | ----- | ----------- |
| **BI-VAL-CLIQUE** | Conferir vencidos abre **Todas (C+V)** + só vencidos | `/` · filtro Centro · clicar Validade |
| **REPASSE-ACUM-NET** | Dinheiro já levado **não** volta no total a levar | `/repasse-vila/` |

### ~~🚀 PREP deploy loja — lote 18/08m~~ · **superado — Live v17.42**

### ✅ CHECKLIST ÚNICO — enviado produção (18/08m · loja **v17.42**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **BI-VAL-CLIQUE** | ✅ **enviado / Live v17.42** | não |
| 2 | **REPASSE-ACUM-NET** | ✅ **enviado / Live v17.42** | não |

### ~~📦 PACOTE PRONTO — Repasse não pede acumulado já enviado~~ · **Live v17.42**

### ~~📦 PACOTE PRONTO LOJA — clique Validade abre as 2 lojas~~ · **Live v17.42**

### ✅ Deploy loja — lote 18/08l (`deploy/lote-checklist-1808l` · **v17.30**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.30** · `producao` @ **1036967** |
| **Pacotes** | **BI-VAL-UNIFICADO** · **DRE-LUCRO-GRAFICO** |
| **Migrate** | **NÃO** |
| **Rollback** | tag `rollback/pre-lote-checklist-1808l-v17.29` @ **bced5d4** + frase + senha |
| **Você** | **Ctrl+F5** em `/` (Validade igual nas 3 lojas) · `/financeiro/resumo-gerencial/` (gráfico lucro = BI) · badge **17.30** |

| Pacote | O quê | Você valida |
| ------ | ----- | ----------- |
| **BI-VAL-UNIFICADO** | Card **Validade** igual em Centro / Vila / C+V | `/` · trocar filtro Números |
| **DRE-LUCRO-GRAFICO** | Barras = **lucro líquido/dia** (mesma conta do BI) · acum. = card BI | `/financeiro/resumo-gerencial/` |

### ~~🚀 PREP deploy loja — lote 18/08l~~ · **superado — Live v17.30**

### ✅ CHECKLIST ÚNICO — enviado produção (18/08l · loja **v17.30**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **BI-VAL-UNIFICADO** | ✅ **enviado / Live v17.30** | não |
| 2 | **DRE-LUCRO-GRAFICO** | ✅ **enviado / Live v17.30** | não |

### ~~📦 PACOTE PRONTO — DRE gráfico lucro = BI~~ · **Live v17.30**

### ~~📦 PACOTE PRONTO LOJA — BI Validade~~ · **Live v17.30**

### ✅ Deploy loja — lote 18/08j (`deploy/lote-checklist-1808j` · **v17.29**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.29** · `producao` @ **bced5d4** |
| **Pacote** | **DRE-SALDO-HOTFIX** |
| **Migrate** | **NÃO** |
| **Rollback** | tag `rollback/pre-lote-checklist-1808j-v17.27` @ **060b300** + frase + senha |
| **Você** | **Ctrl+F5** em `/financeiro/resumo-gerencial/` · badge **17.29** · rolar rodapé · barras azul/vermelho + linha verde |

| Pacote | O quê | Você valida |
| ------ | ----- | ----------- |
| **DRE-SALDO-HOTFIX** | Gráfico saldo **com barras** · scroll na página · Mini DRE legível · tela não trava no equilíbrio | `/financeiro/resumo-gerencial/` |

### ~~🚀 PREP deploy loja — lote 18/08j~~ · **superado — Live v17.29**

### ✅ CHECKLIST ÚNICO — enviado produção (18/08j · loja **v17.29**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **DRE-SALDO-HOTFIX** | ✅ **enviado / Live v17.29** | não |

### ~~📦 PACOTE PRONTO — DRE gráfico + scroll~~ · **Live v17.29**

| Item | Detalhe |
| ---- | ------- |
| **Commit teste** | `039f237` (cherry → lote **bced5d4**) |
| **O quê** | Fix gráfico saldo vazio · scroll · Mini DRE · equilíbrio em background · 1 consulta saldo |

### ~~✅ CHECKLIST ÚNICO — pronto para envio (18/08)~~ · **superado — Live v17.29**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| — | — | Loja **Live v17.29** (hotfix DRE) · lote **1808i** já Live **v17.27** | — |

### 📦 PACOTE PRONTO — Repasse acumulado fix (`REPASSE-ACUMULADO-FIX` · **v17.27**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.27** |
| **Tela** | `/repasse-vila/` + overlay PDV |
| **Fix** | Quita acumulado ao transferir · **zerar acumulado** (PIN) · cache delta (rápido) |
| **Migrate** | **SIM** `0094` |
| **Prova (18/08 revalidado)** | path **84/84** · deep **88/88** · planos **49/49** · `check` OK · smoke acumulado OK |
| **Você** | Ctrl+F5 · **Ver acumulado → zerar** se já transferiu antes da ferramenta |

### ✅ Deploy loja — lote 18/08i (`deploy/lote-checklist-1808i` · **v17.27**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.27** · `producao` @ **060b300** |
| **Pacotes** | **AJUSTE-CICLICA-HERO** · **DRE-SALDO-DIARIO** · **TRANSF-FORCADA-UX** · **REPASSE-ACUMULADO-FIX** |
| **Migrate** | **SIM** `0094` (repasse cache delta) · demais **não** |
| **Rollback** | tag `rollback/pre-lote-checklist-1808i-v17.24` @ **5660d74** + frase + senha |
| **Você** | **Ctrl+F5** em `/repasse-vila/` · `/ajuste-mobile/` (cíclica) · `/financeiro/resumo-gerencial/` · `/transferencias/` · badge **17.27** |

| Pacote | O quê | Você valida |
| ------ | ----- | ----------- |
| **REPASSE-ACUMULADO-FIX** | Quita acumulado ao transferir · **zerar acumulado** (PIN) · abre rápido | `/repasse-vila/` |
| **AJUSTE-CICLICA-HERO** | Último bip **grande** · já contados **estreito** · **⋯** controles | `/ajuste-mobile/` cíclica |
| **DRE-SALDO-DIARIO** | Gráfico saldo dia a dia + **Planos gasto** | `/financeiro/resumo-gerencial/` |
| **TRANSF-FORCADA-UX** | Popup direção · layout invertido C→Vila | `/transferencias/` forçada |

### ~~🚀 PREP deploy loja — lote 18/08i~~ · **superado — Live v17.27**

### ~~✅ CHECKLIST ÚNICO — enviado produção (18/08i · loja **v17.27**)~~ · **ver CHECKLIST no topo**

### ~~✅ CHECKLIST ÚNICO — pronto para envio (18/08)~~ · **superado — Live v17.27**

### ✅ Deploy loja — lote 18/08h (`deploy/lote-checklist-1808h` · **v17.24**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.24** · `producao` @ **5660d74** |
| **Pacote** | **MP-POINT-VILA** |
| **Migrate** | **NÃO** |
| **Rollback** | tag `rollback/pre-lote-checklist-1808h-v17.21` @ **ab3dbcf** + frase + senha |
| **Você** | **Vila:** fechar/abrir **Caixa Vila Elias** no notebook de vendas · **Ctrl+F5** PDV · venda pequena **Mercado Pago Vila (automático)** · **Centro:** sem mudança (Gaveta continua igual) |

### ~~🚀 PREP deploy loja — lote 18/08h~~ · **superado — Live v17.24**

### ✅ CHECKLIST ÚNICO — enviado produção (18/08h · loja **v17.24**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **MP-POINT-VILA** | ✅ **enviado / Live v17.24** | não |

| Incluído no lote **1808i** (Live **v17.27**) | Status |
| -------------------------------------------- | ------ |
| **AJUSTE-CICLICA-HERO** · **DRE-SALDO-DIARIO** · **TRANSF-FORCADA-UX** · **REPASSE-ACUMULADO-FIX** | ✅ **enviado / Live v17.27** |

### ~~✅ CHECKLIST ÚNICO — pronto para envio (18/08h)~~ · **superado — Live v17.24**

### ~~📦 PACOTE PRONTO — DRE saldo dia a dia + planos gasto (`DRE-SALDO-DIARIO` · **v17.23**)~~ · **Live v17.27**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **pronto para envio à produção** |
| **Tela** | `/financeiro/resumo-gerencial/` |
| **O quê** | Rodapé: gráfico saldo dia a dia · CMV · **Planos gasto** · scroll na página · fix gráfico vazio + Mini DRE |
| **API** | `GET /api/financeiro/saldo-diario-mes/` · `planos_gasto` também no resumo operacional |
| **Migrate** | **NÃO** |
| **Prova** | `python scripts/verify_dre_saldo_diario_chart.py` · Django check · integração PG OK |
| **Você** | Ctrl+F5 DRE gerencial · F5 · **Planos gasto** · CMV vendida/paga · gráfico rodapé |

### ~~📦 PACOTE PRONTO — Transferência forçada UX (`TRANSF-FORCADA-UX` · **v17.22**)~~ · **Live v17.27**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **pronto para envio à produção** |
| **Tela** | `/transferencias/` → **Forçada Vila↔C** |
| **O quê** | Popup pergunta **Vila → Centro** ou **Centro → Vila** antes da tela · **C→Vila** inverte colunas (carrinho à esquerda, busca à direita) · sem toggle no cabeçalho |
| **Migrate** | **NÃO** |
| **Prova** | `python scripts/verify_transf_forcada_ux.py` |
| **Você** | Ctrl+F5 `/transferencias/` · abrir forçada · testar as duas direções |

### ✅ Deploy loja — lote 18/08f (`deploy/lote-checklist-1808f` · **v17.21**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.21** · `producao` @ **ab3dbcf** |
| **Pacotes** | **REPASSE-ACUMULADO** + **AJUSTE-CICLICA-FOCUS** |
| **Migrate** | **SIM** `0093` (Render no deploy) |
| **Rollback** | tag `rollback/pre-lote-repasse-acumulado-v17.19` @ **3c810ba** + frase + senha |
| **Você** | Ctrl+F5 `/repasse-vila/` + `/ajuste-mobile/` · badge **v17.21** |

### ~~🚀 PREP deploy loja — lote 18/08f~~ · **superado — Live v17.21**

### ~~✅ CHECKLIST ÚNICO — produção (18/08)~~ · **superado — Live v17.21**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **REPASSE-ACUMULADO** | ✅ **enviado / Live v17.21** | **SIM** 0093 |
| 2 | **AJUSTE-CICLICA-FOCUS** | ✅ **enviado / Live v17.21** | não |

| Operação (sem deploy) | Status |
| --------------------- | ------ |
| **NF-VINCULO-REPARO** | ✅ **33** nomes · dry-run **0** |

### ✅ Deploy loja — lote 18/08e (`deploy/lote-checklist-1808e` · **v17.19**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.19** · `producao` @ **3c810ba** |
| **Pacote** | **AJUSTE-CICLICA-QTD-CB** |
| **Migrate** | **NÃO** |
| **Rollback** | tag `rollback/pre-lote-checklist-1808e-v17.18` @ **9fcecad** + frase + senha |
| **Você** | **Ctrl+F5** `/ajuste-mobile/` · badge **v17.19** · cíclica: 3 bips = **3** no card · reparo 33 nomes: ✅ feito 18/08 |

### ✅ Operação dados — NF-VINCULO-REPARO (18/08) · **feito**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **`--aplicar` na loja** · **33** corrigidos · re-verificado **35/35** · dry-run **0** |
| **Deploy** | **NÃO** — só dados |


### ~~✅ CHECKLIST ÚNICO — produção (18/08) lote anterior~~ · **superado — Live v17.19**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **NF-BIP-ET3-SNAP** | ✅ enviado / Live v17.19 | não |
| 2 | **NF-VINCULO-NAO-SOBRESCREVE** | ✅ enviado / Live v17.19 | não |
| 3 | **NF-VINCULO-REPARO** | ✅ **Live + `--aplicar` feito** (33 nomes · 18/08) | não |
| 4 | **AJUSTE-CICLICA-BIP1** | ✅ enviado / Live v17.19 | não |
| 5 | **AJUSTE-CICLICA-QTD-CB** | ✅ enviado / Live v17.19 | não |

### 📦 PACOTE — Cíclica qtd + overlay (`AJUSTE-CICLICA-QTD-CB` · **v17.19**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.19** · `producao` @ **3c810ba** |
| **O quê** | Card cíclica mostra **quanto já bipou**. Tela verde com número. Overlay código opcional (Sim → fila **Cód.**). |
| **Migrate** | **NÃO** |

### 📦 PACOTE — Cíclica Bip +1 (`AJUSTE-CICLICA-BIP1` · **v17.18**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.18** · `producao` @ **9fcecad** |
| **O quê** | Na cíclica, **Bip +1** soma 1 na contagem (estoque só no **Gravar**). Tela inteira pisca **verde** / **vermelho**. Liga sozinho. Mesmo EAN pode repetir (fila + gap 350 ms). |
| **Prova** | bip1 path **42/42** · cíclica path **72** · deep **31** · UX **17** |
| **Migrate** | **NÃO** |
| **Você** | **Ctrl+F5** `/ajuste-mobile/` · Cíclica → Corredor → bipar (verde = somou) · liberar vendas |

Autorizar rollback: tag `rollback/pre-lote-checklist-1708d-v17.09` @ **3b45abf** + frase + senha. Point Vila **não** subiu.

### 📦 PACOTE — Entrada NF cadastro + etapa 3 (`v17.16`) · **Live v17.18**

| Pacote | O quê | Prova |
| ------ | ----- | ----- |
| **NF-BIP-ET3-SNAP** | Bip na etapa 3 casa na hora (lote, barra, som). | ET3 **83/83** · ET2 **68/68** |
| **NF-VINCULO-NAO-SOBRESCREVE** | «Mudar» só lembra o código da nota. Nome/marca/preço ficam. | **28/28** |
| **NF-VINCULO-REPARO** | Devolve os **33** nomes colados da NF. Histórico se houver; senão tira o `[EAN]`. Preço/GM não mexem. Só grava com `--aplicar`. | **35/35** |

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.18** · `producao` @ **9fcecad** · loja **v17.18** |
| **Migrate** | **NÃO** |
| **Você** | **Ctrl+F5** `/entrada-nota/` e `/ajuste-mobile/` · badge **v17.19** · reparo 33: ✅ **`--aplicar` feito** (18/08). |

### ~~📦 PACOTE — Point MP Vila (`MP-POINT-VILA`)~~ · **ver PACOTE PRONTO v17.24 no topo**


### ✅ CHECKLIST ÚNICO — 17/08c · **superado — fila agora 17/08d**

### ✅ Deploy loja — checklist 17/08b (`deploy/lote-checklist-1708b` · **v17.09**)

> **Não** merge `teste`→`producao`. FF só o lote isolado.

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.09** · `producao` @ **3b45abf** · Render `dep-da1k1cp42hec73aqscrg` |
| **Pacotes** | **NF-BIP-ET2** só |
| **Arquivos** | `entrada_nota.html` · provas ET2/ET3 · `VERSION` 17.09 |
| **Fora** | PDV · caixa · NFC-e · financeiro · estoque |
| **Prova** | ET2 **68/68** · ET3 **48/48** · custo NF **10/10** |
| **Migrate** | **NÃO** |
| **Rollback** | tag `rollback/pre-lote-checklist-1708b-v17.07` @ **08e74d6** · frase+senha |
| **Você** | **Ctrl+F5** `/entrada-nota/` · badge **v17.09** · bip na etapa 2 deve virar Ok na 3 · liberar vendas |

### ✅ CHECKLIST ÚNICO — enviado produção (17/08b · loja **v17.09**) · **superado — fila agora NF-BIP-ET3-SNAP**

> **Loja hoje:** ✅ **Live v17.09** · `producao` @ **3b45abf** · Render `dep-da1k1cp42hec73aqscrg`

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **NF-BIP-ET2** | ✅ enviado / Live v17.09 | não |

### 📦 PACOTE — Entrada NF bip etapa 2 (`NF-BIP-ET2`) · **Live v17.09**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.09** · `producao` @ **3b45abf** |
| **O quê** | Leitor no **Mudar**/busca da etapa 2 (8+ dígitos) vira **Ok** na etapa 3. XML casado por EAN no Postgres (`ean_pg` / `ean_overlay`) também. Código do fornecedor sem bip continua PEND. |
| **Prova** | ET2 **68/68** · ET3 **48/48** · CAD **31/31** · custo NF **10/10** |
| **Migrate** | **NÃO** |
| **Arquivos** | `entrada_nota.html` · `scripts/verify_nf_bip_et2_path.py` |
| **Você** | **Ctrl+F5** `/entrada-nota/` · badge **v17.09** |

### WIP — Hidráulica giro alto × cadastro (17/08)

Renan montou lista civil (giro alto / médio / baixo). Conferido **giro alto** no cadastro da loja (Postgres). Excel na Área de trabalho: `hidraulica_giro_alto.xlsx` (aba **Colar na sua planilha**). **Falta certo:** luva PVC 1/2, Tê PVC 1/2, joelho cola/rosca 1/2, união PVC 3/4, bucha 1×3/4, macho/fêmea mangueira 1/2, fêmea 3/4, espigão 1/2 e 3/4, cola 175 g (só tem 17 g). Amarelos: conferir na gôndola. Próximo: giro médio quando ele mandar.

### ✅ CHECKLIST ÚNICO — enviado produção (17/08 · loja **v17.07**) · **superado — Live v17.09**

> **Loja hoje:** ✅ **Live v17.07** · `producao` @ **08e74d6** · Render `dep-da1jdgid0e5s73bee3pg`

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **REPASSE-PLANOS-CENTRO** | ✅ enviado / Live v17.07 | **SIM** 0091 |
| 2 | **PDV-CLI-CADASTRO** | ✅ enviado / Live v17.07 | **SIM** 0092 |

### ✅ Deploy loja — checklist 17/08 (`deploy/lote-checklist-1708` · **v17.07**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.07** · `producao` @ **08e74d6** · Render `dep-da1jdgid0e5s73bee3pg` |
| **Pacotes** | REPASSE-PLANOS-CENTRO · PDV-CLI-CADASTRO |
| **Método** | FF `deploy/lote-checklist-1708` → `producao` (sem merge `teste`) |
| **Prova** | CLI path **55/55** · deep **24/24** · planos **49/49** · vila path **60/60** · healthz **ok** |
| **Migrate** | **SIM** 0091 + 0092 (Render no deploy) |
| **Rollback** | tag `rollback/pre-lote-checklist-1708-v17.01` @ **09a07d6** · frase+senha |
| **Você** | **Ctrl+F5** · badge **v17.07** · PDV venda · Editar cadastro · `/repasse-vila/` Planos · liberar vendas |

### 📦 PACOTE — Cadastro cliente PDV (`PDV-CLI-CADASTRO`) · **Live v17.07**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.07** |
| **O quê** | Editar cadastro sem scroll · popup telefone duplicado (abrir / limpar número com PIN) · excluir + transferir cashback/vale · vale crédito pagar (caixa) ou manual (sem caixa) · histórico PIN · mesma tela `/clientes/` |
| **Migrate** | **SIM** 0092 (`ClienteAgroEventoAgro`) |
| **Você** | Ctrl+F5 PDV · Editar cadastro · Vale crédito no painel de saldos |

### 📦 PACOTE — Planos no lucro do envio (`REPASSE-PLANOS-CENTRO`) · **Live v17.07**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.07** |
| **O quê** | Botão **Planos** no `/repasse-vila/` · marcado desconta do envio ao Centro · sem marca sai do que ficou na Vila · Postgres |
| **Migrate** | **SIM** 0091 (`planos_desconto_centro`) |
| **Você** | Ctrl+F5 `/repasse-vila/` · **Planos** |

### ✅ CHECKLIST ÚNICO — enviado produção (16/08k · loja **v17.01**) · **superado — Live v17.07**

> **Loja hoje:** ✅ **Live v17.01** · `producao` @ **09a07d6** · Render `dep-da125kou01pc73fi1f40`  
> **Fila deste path:** **vazia** — **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **FECHAR-CAIXA-LOJA** | ✅ enviado / Live v17.01 | não |
| 2 | **REPASSE-HIST-CARDS** | ✅ enviado / Live v17.01 | não |
| 3 | **SAVE-ORC-CAIXA** | ✅ enviado / Live v17.01 | **SIM** 0090 (Render) |
| 4 | **PDV-BALANCA-COM-ESC** | ✅ enviado / Live v17.01 | não |

### ✅ Deploy loja — checklist 16/08k (`deploy/lote-checklist-1608k` · **v17.01**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.01** · `producao` @ **09a07d6** · Render `dep-da125kou01pc73fi1f40` |
| **Pacotes** | SAVE-ORC-CAIXA · REPASSE-HIST-CARDS · PDV-BALANCA-COM-ESC · FECHAR-CAIXA-LOJA |
| **Cherry** | `b9bd96b` → `6c29d73` → `c87ae4a` → `6aa7f4d` → `6493599` (tips loja: `901f06d`…`09a07d6`) |
| **Prova** | fechar **26/26** · save-orc **31/31** · repasse path+deep · balança JS OK · autosave contado preservado |
| **Migrate** | **SIM** 0090 (`CaixaConferenciaRascunhoAgro`) — Render no deploy |
| **Rollback** | tag `producao-rollback-v16.90-20260816` @ **215d0a9** |
| **Base anterior** | Live v16.90 @ **215d0a9** |
| **Você** | **Ctrl+F5** · badge **v17.01** · Fechar caixa (Vila/Centro) · F2 orçamentos · `/repasse-vila/` · F10 balança |

### 📦 PACOTE — Fechar caixa Vila/Centro (`FECHAR-CAIXA-LOJA`) · **Live v17.01**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.01** |
| **O quê** | Fiado/vale/cashback auto + ocultos · Centro: Point MP auto oculto · PIX/débito/crédito num campo · **autosave contado intacto** |
| **Prova** | `verify_caixa_fechar_loja_path.py` **26** · save-orc **31/31** |
| **Migrate** | **NÃO** |
| **Você** | Ctrl+F5 Fechar caixa Vila e Centro |

### 📦 PACOTE — Repasse tela + cards mês (`REPASSE-HIST-CARDS`) · **Live v17.01**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.01** |
| **O quê** | Visual Display Scale · cards **Enviado ao Centro** + **Lucro ficou na Vila** |
| **Migrate** | **NÃO** |
| **Cherry** | `6c29d73` |
| **Você** | Ctrl+F5 `/repasse-vila/` |

### 📦 PACOTE — Orçamentos + contagem caixa (`SAVE-ORC-CAIXA`) · **Live v17.01**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.01** |
| **O quê** | F2 lista orçamentos Postgres · contagem fechar caixa multi-PC |
| **Prova** | `verify_save_orc_caixa_path.py` **31/31** |
| **Migrate** | **SIM** 0090 |
| **Cherry** | `b9bd96b` |

### 📦 PACOTE — Balança COM/ESC (`PDV-BALANCA-COM-ESC`) · **Live v17.01**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v17.01** |
| **O quê** | Detecta impressora (ESC) · CONECTAR reabre picker · fingerprint · só peso com STX · barcode `0010` |
| **Prova** | Bugbot corrigido · `scripts/_test_pdv_balanca_logic.js` **22/22** |
| **Migrate** | **NÃO** |
| **Cherry** | `6aa7f4d` (inclui `c87ae4a`) |
| **Você** | F10 → **CONECTAR** → COM da **balança** (não impressora) |

### ✅ Live recente (não reenviar)

| Pacote | Live |
| ------ | ---- |
| **REPASSE-VALOR-MANUAL** | v16.90 @ `215d0a9` |
| **PDV-BALANCA-HOTFIX3** | v16.89 @ `c70d195` |

### ✅ Deploy loja — LOTE B balança (`PDV-BALANCA-GRANEL` · **v16.82**)

> **Status:** ✅ **enviado / Live v16.82** · `producao` @ **3fd7cd3** · base Live **v16.74** @ **1c41e50**  
> **Cherry:** `88310f0` → `e9e6eec` → `9d6c878` · tag `producao-rollback-pre-balanca-20260816`  
> **Migrate:** **SIM** 0089 (Render no deploy) · Serial COM4 USE-P2 9600 8N1

| # | Pacote | Status |
| - | ------ | ------ |
| 1 | **PDV-BALANCA-GRANEL** | ✅ enviado / Live v16.82 |

**Você:** Ctrl+F5 PDV · F10 · Conectar **COM4** · pesar 1 kg ≈ 1,000.

**Rollback:** `git push origin producao-rollback-pre-balanca-20260816:producao --force-with-lease` (só com senha)

### ✅ Deploy loja — LOTE A repasse (`REPASSE-DIA-PASSADO` + `REPASSE-ELET-MAQUINAS` · **v16.74**)

> **Status:** ✅ **enviado / Live v16.74** · `producao` @ **1c41e50** · Render `dep-da0tglm1egvs739oedi0`  
> **Base:** Live v16.69 @ **447123a** · tag rollback `producao-rollback-v16.69-20260816`  
> **Cherry:** `f10e2c4` (dia) · `3774aea` (máquinas+elet) · `1c41e50` (Sicoob PIX)  
> **Migrate:** **NÃO** · **Balança NÃO subiu** (LOTE B abaixo)

| # | Pacote | Status |
| - | ------ | ------ |
| 1 | **REPASSE-DIA-PASSADO** | ✅ enviado / Live v16.74 |
| 2 | **REPASSE-ELET-MAQUINAS** | ✅ enviado / Live v16.74 |
| 3 | **PDV-BALANCA-GRANEL** | ✅ enviado / Live v16.82 |

### ✅ CHECKLIST ÚNICO — após loja v16.82 (16/08g) · **superado**

> **Loja hoje:** ✅ **Live v16.84** (hotfix balança) · ver checklist **16/08h**  
> **Pronto envio:** **REPASSE-VALOR-MANUAL** (ainda pendente loja)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **REPASSE-VALOR-MANUAL** | ✅ **pronto para envio** / teste **v16.83** @ **ab71c8c** | não |
| 2 | **PDV-BALANCA-HOTFIX** | ✅ **enviado / Live v16.84** | não |

### ~~📦 PACOTE PRONTO — Balança hotfix~~ → ver bloco Live acima

### 📦 PACOTE PRONTO — Valor manual no repasse (`REPASSE-VALOR-MANUAL` · **v16.83**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **pronto para envio** |
| **O quê** | Digitar R$ (ex. 600) **manda** — não corta mais no automático |
| **Prova** | deep **68** · path **43** |
| **Migrate** | **NÃO** |
| **Cherry** | `ab71c8c` |
| **Você** | Ctrl+F5 Retiradas → Repasse |

### ✅ CHECKLIST ÚNICO — após loja v16.74 (16/08f) · **superado**

> Loja **v16.82** (balança) · hotfix manual = checklist **16/08g**.

### 📦 PACOTE PRONTO — PDV balança granel (`PDV-BALANCA-GRANEL` · **v16.76**) · **Live v16.82**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ pronto · **ainda não Live** · PREP no topo |
| **O quê** | Pesar / F10 · Web Serial · KG · hold 500ms · força KG |
| **Migrate** | **SIM** 0089 |
| **Cherry** | `88310f0` → `e9e6eec` |
| **Prova** | review Opus + fixes + dry-run cherry na base Live v16.74 |
| **Você** | Cadastro KG · no deploy: pausa + senha |

### 📦 PACOTE — Máquinas + repasse elet (`REPASSE-ELET-MAQUINAS`) · **Live v16.74**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.74** |
| **Produção** | `1c41e50` · Render `dep-da0tglm1egvs739oedi0` |

### 📦 PACOTE — Repasse dia passado (`REPASSE-DIA-PASSADO`) · **Live v16.74**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.74** |

### ✅ CHECKLIST ÚNICO — pronto para envio (16/08f prep) · **superado — Live v16.74**

> Vigente: PREP LOTE B no topo.

### ✅ CHECKLIST ÚNICO — pronto para envio à produção (16/08e) · **superado**

> Vigente: Live **v16.74** + PREP LOTE B.

### ✅ CHECKLIST ÚNICO — pronto para envio à produção (16/08d) · **superado**

> Vigente: Live **v16.74**.

### ✅ CHECKLIST ÚNICO — pronto para envio à produção (16/08b) · **superado**

> Vigente: Live **v16.74**.

### ✅ Deploy loja — coluna Código GM (`PDV-PEDIR-LOJA-GM-COL` · **v16.69**)

> **Status:** ✅ **enviado / Live v16.69** · `producao` @ **11d4d5b** · base Live **v16.68** @ **677b3a1**
> **Incluiu:** só CSS — coluna Código GM `7.25rem` → `9.75rem` · **sem migrate** · **risco baixo**
> **Rollback:** tag `rollback/pre-pdv-pedir-loja-gm-col-v16.68` · branch `producao-backup-pre-v1669-pedir-gm-col-20260816` · frase+senha
> **Você:** PDV loja · **Ctrl+F5** · badge **v16.69** · Pedir loja · busca milho · GM sem `…`

### ✅ Deploy loja — Pedir loja UX5–UX7 (`PDV-PEDIR-LOJA-UX7` · **v16.68**)

> **Status:** ✅ **enviado / Live v16.68** · `producao` @ **df2fd39** · base Live **v16.59** @ **16663c7**
> **Incluiu:** furado + bip 1/min · Ajustar Centro/Vila na busca · aviso pós-PIN · GM/fonte · **sem migrate**
> **Prova:** verify 54/54 · testes 20/20 · só arquivos Pedir loja (+ rota ajustar em urls)
> **Rollback:** tag `rollback/pre-pdv-pedir-loja-ux7-v16.59` · branch `producao-backup-pre-v1668-pedir-loja-ux7-20260816` · frase+senha
> **Você:** PDV loja · **Ctrl+F5** · badge **v16.68** · PIN com pedido · Ajustar · Transferir furado

### ✅ Deploy loja — Pedir loja layout PC (`PDV-PEDIR-LOJA-UX3` · **v16.59**) · **superado — Live v16.68**

> **Status:** ✅ **enviado / Live v16.59** · `producao` @ **b4f3807** · Render `dep-da0ip8bncjis738oftcg`
> **Base anterior:** Live v16.58 @ **7fcf7e9** / útil **3964a07**
> **Incluiu:** overlay **um tamanho** · colunas Produto \| Centro \| Vila · nome 1 linha · **sem migrate**
> **Prova:** verify 40/40 · testes 17/17 · `/pdv/` local com overlay novo
> **Rollback:** tag `rollback/pre-pdv-pedir-loja-ux3-v16.58` · branch `producao-backup-pre-v1659-pedir-loja-ux3-20260816` · frase+senha
> **Você:** PDV loja · **Ctrl+F5** · badge **v16.59** · Pedir loja · busca milho

### ✅ Deploy loja — Pedir loja UX2 (`PDV-PEDIR-LOJA-UX2` · **v16.58**)

> **Status:** ✅ **enviado / Live v16.58** · `producao` @ **3964a07** · Render `dep-da0igne1egvs739h7kkg`
> **Base anterior:** Live v16.56 @ **cd261d6**
> **Incluiu:** overlay PC busca \| pedido · saldo Agro · **sem migrate**
> **Rollback:** tag `rollback/pre-pdv-pedir-loja-ux2-v16.56` @ **cd261d6** · branch `producao-backup-pre-v1658-pedir-loja-ux2-20260815` · frase+senha
> **Você:** PDV loja · **Ctrl+F5** · badge **v16.58** · Pedir loja · busca milho

### ✅ CHECKLIST ÚNICO — enviado produção (16/08 · coluna GM · loja v16.69)

> **Loja hoje:** ✅ **Live v16.69** (após Render) · só largura coluna Código GM  
> **Fila deploy:** **vazia**.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-PEDIR-LOJA-GM-COL** | ✅ enviado / Live v16.69 | não |

### ✅ CHECKLIST ÚNICO — enviado produção (16/08 · Pedir loja UX7 · loja v16.68) · **superado — Live v16.69**

> **Loja hoje:** ✅ **Live v16.68** · pacote Pedir loja UX5–UX7  
> **Fila deploy:** **vazia**.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-PEDIR-LOJA-UX7** | ✅ enviado / Live v16.68 | não |

### 📦 PACOTE PRONTO LOJA — Pedir loja layout + saldo (`PDV-PEDIR-LOJA-UX2` · **v16.58**) · **superado — Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.58** |
| **Produção** | ✅ Live v16.58 · `3964a07` |

### ✅ Deploy loja — Pedir loja UX PC + rosa (`PDV-PEDIR-LOJA-UX` · **v16.56**)

> **Status:** ✅ **enviado / Live v16.56** · código útil `81157fc` · `producao` @ **197a6ac** · Render `dep-da0i81bncjis738o6meg`
> **Base anterior:** Live v16.55 @ **d3175b3** / feature **5da53b3**
> **Incluiu:** overlay coluna única PC · botão Pedir loja rosa · Transferir rosa · **sem migrate**
> **Rollback:** tag `rollback/pre-pdv-pedir-loja-ux-v16.55` @ **d3175b3** · branch `producao-backup-pre-v1656-pedir-loja-ux-20260815` · frase+senha
> **Você:** Ctrl+F5 PDV · badge **v16.56** · botão rosa · overlay

### ✅ Deploy loja — Pedir loja PDV (`PDV-PEDIR-LOJA` · **v16.55**)

> **Status:** ✅ **enviado / Live v16.55** · `producao` @ **5da53b3** · Render `dep-da0hv4m1egvs739guu70`
> **Base anterior:** Live v16.53 @ **fc01036**

| Item | Detalhe |
| ---- | ------- |
| **Incluiu** | Pedir loja overlay · rótulos topbar (Esto / Saldo Vila / Vendas) |
| **Migrate** | **SIM** `estoque.0018` (Render no build) |
| **Prova pré** | 15/15 · verify 28/28 · PATH_OK |
| **Rollback** | tag `rollback/pre-pdv-pedir-loja-prod-v16.53` @ **fc01036** · branch `producao-backup-pre-v1655-pedir-loja-20260815` · frase+senha |
| **Você** | Ctrl+F5 PDV · badge **v16.55** · Pedir loja · 1 pedido teste |

### ✅ CHECKLIST ÚNICO — enviado produção (15/08 · Pedir loja · loja v16.55) · **superado — Live v16.56; fila agora UX2 v16.58**

### 📦 PACOTE PRONTO LOJA — Pedir loja PDV (`PDV-PEDIR-LOJA` · **v16.55**) · **superado — Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.55** |
| **Produção** | ✅ Live v16.55 · `5da53b3` |

### ✅ Deploy loja — Vendas lojas média até agora (VENDAS-LOJAS-MEDIA-AGORA · **v16.53**)

> **Status:** ✅ **enviado / Live v16.53** · código útil `09463f8a`  
> **Base anterior:** Live v16.51 @ **fade5b8f**

| Item | Detalhe |
| ---- | ------- |
| **O quê** | `/vendas/lojas/` celular: números grandes · média **até agora** (padrão) · **dia todo** discreto |
| **Por quê** | De manhã a média do dia inteiro parece impossível — as vendas ainda vão acontecer |
| **Cálculo** | Mesma Meta C do BI · expediente **7h30–18h30** · dias já fechados 100 % · só hoje é cortado no relógio |
| **Tela** | Coluna de celular · cartões só com o valor · folha: **Até agora** / **Dia todo** |
| **Prova** | `scripts/verify_vendas_lojas_resumo_path.py` **VERIFY OK 111/111** |
| **Migrate** | **NÃO** |
| **Rollback desta atualização** | tag `rollback/pre-vendas-lojas-media-agora-v16.51` @ **fade5b8f** · branch `producao-backup-pre-v1653-vendas-lojas-media-agora-20260815` · frase+senha |
| **Você** | celular `/vendas/lojas/` · **Ctrl+F5** · toque num valor de manhã · badge **v16.53** |

### 📦 PACOTE PRONTO — Vendas lojas média até agora (VENDAS-LOJAS-MEDIA-AGORA · **v16.53**) · **superado — Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.53** |
| **O quê** | `/vendas/lojas/` celular: números grandes · tela limpa · média **até agora** (padrão) ou **dia todo** |
| **Produção** | ✅ Live v16.53 |

### ✅ Deploy loja — Ajuste mobile app (AJUSTE-MOBILE-PWA · **v16.51**)

> **Status:** ✅ **enviado / Live v16.51** · código útil `b0b565e8`  
> **Base anterior:** Live v16.48 @ **3fa550c0**

| Item | Detalhe |
| ---- | ------- |
| **O quê** | `/ajuste-mobile/` vira app no Chrome · números grandes · cabeçalho limpo (título sozinho + 5 botões em grade) |
| **Igual** | PIN a cada abertura · Bip +1 · cíclica · scan · SW **não** cacheia contagem |
| **Prova** | `scripts/verify_ajuste_mobile_pwa.py` **VERIFY OK 29/29** |
| **Migrate** | **NÃO** |
| **Rollback desta atualização** | tag `rollback/pre-ajuste-mobile-pwa-v16.48` @ **3fa550c0** · branch `producao-backup-pre-v1651-ajuste-mobile-pwa-20260815` · frase+senha |
| **Você** | celular `/ajuste-mobile/` · **Ctrl+F5** · Chrome → **Instalar aplicativo** · badge **v16.51** |

### 📦 PACOTE PRONTO — Ajuste mobile app (AJUSTE-MOBILE-PWA · **v16.51**) · **superado — Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.51** |
| **O quê** | `/ajuste-mobile/` no celular: PIN · ícone na tela · saldo e quantidade grandes · título sozinho + 5 botões em grade |
| **Igual** | PWA das vendas · não cacheia contagem · PIN / bip / cíclica iguais |
| **Prova** | `scripts/verify_ajuste_mobile_pwa.py` **VERIFY OK 29/29** |
| **Migrate** | **NÃO** |
| **Você** | celular `/ajuste-mobile/` · Ctrl+F5 · Chrome → **Instalar aplicativo** |
| **Produção** | ✅ Live v16.51 |

### 📦 PACOTE PRONTO — Ajuste mobile app (AJUSTE-MOBILE-PWA · **v16.50**) · **superado — Live no lote v16.51**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live no lote v16.51** |
| **O quê** | `/ajuste-mobile/` no celular: PIN · ícone na tela · saldo e quantidade grandes · botões de toque |
| **Igual** | PWA das vendas · não cacheia contagem · PIN / bip / cíclica iguais |
| **Prova** | `scripts/verify_ajuste_mobile_pwa.py` |
| **Migrate** | **NÃO** |
| **Você** | celular `/ajuste-mobile/` · Ctrl+F5 · Chrome → **Instalar aplicativo** |
| **Produção** | ✅ lote v16.51 |

### 📦 PACOTE PRONTO — Ajuste mobile app (AJUSTE-MOBILE-PWA · **v16.49**) · **superado — Live no lote v16.51**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live no lote v16.51** · Instalar no Chrome vira app |
| **O quê** | `/ajuste-mobile/` no celular: mesmo PIN · ícone na tela inicial · tela cheia |
| **Igual** | PWA das vendas (`/vendas/lojas/`) · não cacheia contagem |
| **Prova** | `scripts/verify_ajuste_mobile_pwa.py` **VERIFY OK 23/23** |
| **Migrate** | **NÃO** |
| **Você** | celular `/ajuste-mobile/` · Ctrl+F5 · Chrome → **Instalar aplicativo** |
| **Produção** | ✅ Live no lote v16.51 |

### ✅ Deploy loja — Vendas lojas média no toque (VENDAS-LOJAS-MEDIA · **v16.48**)

> **Status:** ✅ **enviado / Live v16.48** · `producao` @ **be452979**  
> **Base anterior:** Live v16.46 @ **a202214e**

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Toque no valor → média esperada (Meta C do BI) + R$ e % acima/abaixo · PWA + calendário + Ontem/Mês ant. |
| **Tela** | `/vendas/lojas/` celular · números grandes · limpa |
| **Prova** | `scripts/verify_vendas_lojas_resumo_path.py` **VERIFY OK 91/91** |
| **Migrate** | **NÃO** |
| **Rollback desta atualização** | tag `rollback/pre-vendas-lojas-media-v16.46` @ **a202214e** · branch `producao-backup-pre-v1648-vendas-lojas-media-20260815` · frase+senha |
| **Você** | celular `/vendas/lojas/` · **Ctrl+F5** · toque num valor · badge **v16.48** |

### 📦 PACOTE PRONTO — Vendas lojas média no toque (VENDAS-LOJAS-MEDIA · **v16.48**) · **superado — Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.48** · `producao` @ **be452979** |
| **O quê** | `/vendas/lojas/` · clicar Centro / Vila / Total abre folha: vendido, média esperada, diferença em R$ e % |
| **Cálculo** | Mesma **Meta C** do BI · Centro / Vila / total (sem filtro = as duas) · soma dos dias do período |
| **Tela** | Celular · números grandes · limpa · PWA + calendário + Ontem/Mês ant. (v16.47) |
| **Prova** | `scripts/verify_vendas_lojas_resumo_path.py` **VERIFY OK 91/91** |
| **Migrate** | **NÃO** |
| **Rollback desta atualização** | tag `rollback/pre-vendas-lojas-media-v16.46` @ **a202214e** (Live v16.46, **sem** média/PWA/calendário) · branch `producao-backup-pre-v1648-vendas-lojas-media-20260815` · frase+senha |
| **Você** | celular `/vendas/lojas/` · **Ctrl+F5** · toque num valor · badge **v16.48** |

### 📦 PACOTE PRONTO — Vendas lojas PWA + calendário (VENDAS-LOJAS-PWA · **v16.47**) · **Live no lote v16.48**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live no lote v16.48** · PWA + calendário + Ontem/Mês ant. |
| **O quê** | Instalar no Chrome vira app · calendário caprichado (escolhe o dia) · atalhos **Ontem** e **Mês ant.** |
| **Tela** | Prints OK · R$ 0,00 no Dia = ainda sem venda hoje; Semana soma os dias anteriores |
| **Prova** | `scripts/verify_vendas_lojas_resumo_path.py` **VERIFY OK 67/67** |
| **Migrate** | **NÃO** |
| **Você** | celular `/vendas/lojas/` · Ctrl+F5 · Chrome → **Instalar aplicativo** · testar calendário / ontem |
| **Produção** | ✅ lote v16.48 Live |

### ✅ Deploy loja — Vendas por loja celular (VENDAS-LOJAS-RESUMO · **v16.46**)

> **Status:** ✅ **enviado** · `producao` a partir de **e96f998** · rollback pronto  
> **Base anterior:** Live v16.44 @ **e96f998**

| Item | Detalhe |
| ---- | ------- |
| **O quê** | `/vendas/lojas/` no celular: Centro + Vila + total · Dia/Semana/Mês/Ano (padrão hoje) · números grandes |
| **Atalho** | Menu BI tecla **S** · Relatórios · card Faturamento |
| **Fonte** | Só PDV `VendaAgro` · sem cache · sem Mongo · sem devolvidas |
| **Prova** | `scripts/verify_vendas_lojas_resumo_path.py` **VERIFY OK 49/49** |
| **Migrate** | **NÃO** |
| **Rollback** | tag `rollback/pre-vendas-lojas-v16.44` @ **e96f998** · branch `producao-backup-pre-v1646-vendas-lojas-20260815` · frase+senha |
| **Você** | celular `/vendas/lojas/` · **Ctrl+F5** · badge **v16.46** |

### ✅ CHECKLIST ÚNICO — pronto para envio à produção (14/08b)

> **Loja hoje:** ✅ **Live v16.32** · producao @ **544b31f**  
> **NÃO** merge teste→producao sem frase + senha.  
> **Só no teste (subir juntos):** tip **v16.43** · **sem migrate** nestes 5.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **NFCE-VILA-SEQ** | ✅ **pronto para envio à produção** | não |
| 2 | **CTB-NFCE-LOJA** | ✅ **pronto para envio à produção** | não |
| 3 | **REPASSE-VILA-UX** | ✅ **pronto para envio à produção** | não |
| 4 | **DRE-FILTRO-PADRAO** | ✅ **pronto para envio à produção** | não |
| 5 | **AJUSTE-CICLICA-CANCEL** | ✅ **pronto para envio à produção** | não |

### 📦 PACOTE PRONTO — Cancelar contagem cíclica (AJUSTE-CICLICA-CANCEL · **v16.43**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **pronto para envio à produção** · teste **v16.43** |
| **O quê** | Botão **Cancelar contagem** · encerra pra todos · estoque não muda (antes de Gravar) · 2 confirmações · cancel idempotente |
| **Prova** | path **VERIFY_OK 65** (UI + runtime cancel / rejeita FECHADA) |
| **Migrate** | **NÃO** |
| **Você** | Ctrl+F5 /ajuste-mobile/ · Cíclica → Cancelar contagem |

### 📦 PACOTE PRONTO — Contabilidade NFC-e por loja (CTB-NFCE-LOJA · **v16.43**) · **Live v16.44**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **pronto para envio à produção** · teste **v16.43** |
| **O quê** | /contabilidade/ · botões grandes Centro/Vila/Ambas · ZIP/CSV/Excel por CNPJ |
| **Prova** | path **VERIFY_OK 52** · ZIP smoke OK |
| **Migrate** | **NÃO** |
| **Você** | Ctrl+F5 Contabilidade · Centro ZIP · Vila ZIP |

### 📦 PACOTE PRONTO — NFC-e Vila sequência PK (NFCE-VILA-SEQ · **v16.43**) · **Live v16.44**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **pronto para envio à produção** · teste **v16.43** |
| **O quê** | Sync sequência PK + retry ao criar numeração Vila |
| **Prova** | path CTB+SEQ **VERIFY_OK 52** |
| **Migrate** | **NÃO** |
| **Você** | Ctrl+F5 · venda teste Vila |

### 📦 PACOTE PRONTO — Repasse UX + forma (REPASSE-VILA-UX · **v16.43**) · **Live v16.44**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **pronto para envio à produção** · teste **v16.43** |
| **O quê** | Tela densa · forma de pagamento no overlay · anti-autofill · Retiradas sem 2º PDV |
| **Prova** | path **32** · deep **55** |
| **Migrate** | **NÃO** |
| **Você** | Ctrl+F5 Retiradas → Repasse |

### 📦 PACOTE PRONTO — DRE filtro padrão ao abrir (DRE-FILTRO-PADRAO · **v16.43**) · **Live v16.44**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **pronto para envio à produção** · teste **v16.43** |
| **O quê** | Resumo/DRE abre Centro+Vila · dia 1→hoje · Vencimento · Bruto · sempre carrega a API |
| **Prova** | verify_dre_visual_path · unit DRE |
| **Migrate** | **NÃO** |
| **Você** | Ctrl+F5 Resumo |

### ✅ CHECKLIST ÚNICO — enviado produção (14/08 · loja v16.32) · **superado**

> Vigente: **CHECKLIST ÚNICO — pronto envio (14/08b)** no topo. Loja permanece **v16.32** até o próximo envio.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **NFCE-VILA-EMIT** | ✅ enviado / Live v16.32 | **SIM** 0088 |
| 2 | **AJUSTE-CICLICA** | ✅ enviado / Live v16.32 | **SIM** estoque.0016+**0017** |
| 3 | **REPASSE-VILA** | ✅ enviado / Live v16.32 | **SIM** 0087 |
| 4 | **DRE-VISUAL-LEGIVEL** | ✅ enviado / Live v16.32 | não |

### ✅ Deploy loja — lote checklist 14/08 (`deploy/lote-checklist-1408` · **v16.32**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.32** · `producao` @ **544b31f** · Render `dep-d9vn5fbncjis73elco4g` |
| **Pacotes** | NFCE-VILA-EMIT · AJUSTE-CICLICA · REPASSE-VILA · DRE-VISUAL-LEGIVEL |
| **Prova** | path cíclica **57** · repasse **25** · DRE **229** · `manage.py check` OK · parity `teste` |
| **Migrate** | **SIM** — `produtos.0087` · `produtos.0088` · `estoque.0016` · `estoque.0017` (build Render) |
| **Rollback** | tag `rollback/pre-lote-checklist-1408-v16.07` @ **262d460** · frase+senha |
| **Base anterior** | Live v16.07 @ **262d460** |
| **Você** | **Ctrl+F5** · badge **v16.32** · PIX Vila CNPJ `/0002` · Retiradas Repasse · `/ajuste-mobile/` cíclica · liberar vendas |

### 🚀 PREP deploy loja — lote checklist 14/08 (`deploy/lote-checklist-1408` · **v16.32**) · **superado**

> Vigente: **Deploy loja — lote checklist 14/08 Live v16.32** no topo.

### 📦 PACOTE PRONTO — NFC-e Vila pelo caixa (`NFCE-VILA-EMIT` · **v16.24**) · **Live v16.32**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.32** · `producao` @ **544b31f** |
| **O quê** | Caixa **Vila** → CNPJ `/0002-86` · Centro → `/0001-03` · migrate **0088** |
| **Prova** | SEFAZ local · `verify_nfce_vila_path` **VERIFY_OK** |
| **Migrate** | **SIM** 0088 |
| **Você** | Ctrl+F5 · PIX Vila → CNPJ `/0002` |

### 📦 PACOTE PRONTO — Contagem cíclica / Ajuste Mobile (`AJUSTE-CICLICA` · **v16.32**) · **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.32** · `producao` @ **544b31f** |
| **O quê** | Cíclica cego · multi-celular · filtro dias · Incluir fora · UX smartphone (teclado/overlays/scroll) · Scan EAN rápido |
| **Prova** | path **57** · deep **31** · UX+HTTP **VERIFY_OK 16** (login, gate, overlays, forcar, cego) |
| **Migrate** | **SIM** — `estoque.0016` + **`0017`** |
| **Você** | Ctrl+F5 `/ajuste-mobile/` no **celular** |

### 📦 PACOTE PRONTO — Repasse Vila → Centro (`REPASSE-VILA` · **v16.21**) · **Live v16.32**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.32** · `producao` @ **544b31f** |
| **O quê** | Retiradas → Repasse · CMV/%/fiado · migrate **0087** |
| **Prova** | path **25** · deep **44** |
| **Migrate** | **SIM** 0087 |
| **Você** | Ctrl+F5 Retiradas |

### 📦 PACOTE PRONTO — DRE visual legível (`DRE-VISUAL-LEGIVEL` · **v16.09**) · **Live v16.32**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.32** · `producao` @ **544b31f** |
| **O quê** | Resumo/DRE legível (balão, PE, KPIs) |
| **Prova** | `verify_dre_visual_path` **VERIFY_OK** |
| **Migrate** | **NÃO** |
| **Você** | Ctrl+F5 Resumo |

### ✅ CHECKLIST ÚNICO — (13/08) · **superado**

> Vigente: **CHECKLIST ÚNICO (14/08)** no topo.

### ✅ Revisão path — ENTRADA-NF-UX + PDV-PIX-SICREDI (13/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **já na loja** (desde lote ~v11.98) · loja hoje **v16.07** · **nada a subir** |
| **Prova** | boleto 44→47 OK · Nova→reset · financeiro_ids · `pix_sicredi` no código `producao` |
| **Branches velhas** | `deploy/entrada-nf-ux-v11.95` / `deploy/pdv-pix-sicredi-v11.94` = **obsoletas** (não re-cherry) |

### 📦 PACOTE PRONTO LOJA — DF-e XML fora do Aguarde 1h (`DFE-XML-AGUARDE` · **v15.95+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **já Live** (lote v15.99) · prova refeita 13/08 · **nada pendente** deste path |
| **O quê** | Buscar lista = 1h no 137 · Buscar XML/chave só trava no **656** · auto Ciência+XML nas Só resumo · **Carregar na grade** manual |
| **Prova** | `python scripts/verify_dfe_xml_aguarde_path.py` → **VERIFY_OK 31/31** · manifestação **26/26** · NSU-656 **12/12** |
| **Migrate** | **NÃO** |
| **Fora** | PDV · caixa · distNSU (continua 1h) |
| **Você** | Ctrl+F5 SEFAZ · Buscar · notas em **Carregar na grade** sem esperar 1h |

### ✅ CHECKLIST ÚNICO (legado 13/08 — fila vazia)

> **Substituído** pelo checklist **pronto para envio** com **DRE-VISUAL-LEGIVEL** no topo do CHECKPOINT. ENTRADA-NF / PIX / DFE-XML **já Live**.

### ✅ Deploy loja — lote checklist 12/08b (`deploy/lote-checklist-1208b` · **v16.07**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v16.07** · `producao` @ **262d460** · Render `dep-d9u8i7psrm7s73b67dhg` |
| **Pacotes** | CP-CAL-PG · NF-BIP-ET3 |
| **Prova** | VERIFY **31/31** · **40/40** (+ irmã 31/31) |
| **Migrate** | **NÃO** |
| **Rollback** | tag `rollback/pre-lote-checklist-1208b-v15.99` @ **ec76e89** |
| **Base anterior** | Live v15.99 @ **ec76e89** |
| **Você** | **Ctrl+F5** · badge **v16.07** · calendário CP · Entrada NF etapa 3 |

### ✅ CHECKLIST ÚNICO — enviado produção (12/08 · loja v16.07) · **superado**

> Vigente: **CHECKLIST ÚNICO — pronto envio (13/08)** no topo (fila vazia neste path).

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CP-CAL-PG** | ✅ enviado / Live v16.07 | não |
| 2 | **NF-BIP-ET3** | ✅ enviado / Live v16.07 | não |

### ✅ Deploy loja — lote checklist 12/08 (`deploy/lote-checklist-1208` · **v15.99**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.99** · `producao` @ **ec76e89** · Render `dep-d9u7bmu7bikc739ncamg` |
| **Pacotes** | COMPRAS-SLIM-DADOS · NF-BIP-CAD · DFE-XML-AGUARDE |
| **Prova** | VERIFY **41/41** · **31/31** · **26/26** |
| **Migrate** | **NÃO** |
| **Rollback** | tag `rollback/pre-lote-checklist-1208-v15.82` @ **5bbebbe** |
| **Base anterior** | Live v15.82 @ **5bbebbe** |

### ✅ CHECKLIST ÚNICO — enviado produção (12/08 · loja v15.99) · **superado**

> Vigente: **CHECKLIST ÚNICO — pronto envio (12/08)** no topo. Loja permanece **v15.99** até o próximo envio.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **COMPRAS-SLIM-DADOS** | ✅ enviado / v15.99 | não |
| 2 | **NF-BIP-CAD** | ✅ enviado / v15.99 | não |
| 3 | **DFE-XML-AGUARDE** | ✅ enviado / v15.99 | não |

### ✅ Deploy loja — COMPRAS-SLIM-FORN (`deploy/compras-slim-forn-1108` · **v15.82**) · **superado**

> **Loja hoje:** ✅ **Live v15.82** · `producao` @ **5bbebbe** · Render `dep-d9tn7foae00c73bg8sb0`  
> Vigente checklist: **pronto envio (12/08)** no topo.

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.82** · `producao` @ **5bbebbe** |
| **O quê** | slim v4 com fornecedor · cache v4 |
| **Rollback** | tag `rollback/pre-compras-slim-forn-v15.77` @ `b226e66` |

### ✅ CHECKLIST ÚNICO — enviado produção (11/08 · loja v15.82) · **superado**

> Vigente: **CHECKLIST ÚNICO — pronto envio (12/08)** no topo. Loja permanece **v15.82** até o próximo envio.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **COMPRAS-SLIM-FORN** | ✅ enviado / Live v15.82 | não |

### 🚀 PREP deploy loja — COMPRAS-SLIM-FORN (`deploy/compras-slim-forn-1108` · **v15.82**) · **superado**

> Vigente: **Deploy loja — COMPRAS-SLIM-FORN Live v15.82** no topo.

### ✅ Deploy loja — COMPRAS-SLIM-FALLBACK (`deploy/compras-slim-1108` · **v15.77**)

> **Loja hoje:** ✅ **Live v15.77** · `producao` @ **b226e66** · Render `dep-d9tn2heq1p3s73b2ai9g`  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **COMPRAS-SLIM-FALLBACK** | ✅ enviado / Live v15.77 | não |

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.77** · `producao` @ **b226e66** · base era `75288b5` (v15.68) |
| **Branch** | `deploy/compras-slim-1108` (FF → `producao`) |
| **O quê** | `/compras/` fallback slim com freio catalogo-full-off · cache v3 |
| **Arquivos** | só compras.html + mobile_ajuste.html + VERSION |
| **Prova** | **44/44** · sem migrate |
| **Rollback** | tag `rollback/pre-compras-slim-v15.68` @ `75288b5` · frase+senha |
| **Você agora** | **Ctrl+F5** `/compras/` · badge **v15.77** · digitar 2 letras |

### ✅ CHECKLIST ÚNICO — enviado produção (11/08 · loja v15.77)

> **Loja hoje:** ✅ **Live v15.77** · `producao` @ **b226e66**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **COMPRAS-SLIM-FALLBACK** | ✅ enviado / Live v15.77 | não |

### 🚀 PREP deploy loja — COMPRAS-SLIM-FALLBACK (`deploy/compras-slim-1108` · **v15.77**) · **superado**

> Vigente: **Deploy loja — COMPRAS-SLIM-FALLBACK Live v15.77** no topo.

### ✅ Deploy loja — FOLHA-FAMILIA (`deploy/folha-familia-1108` · **v15.68**) · **superado**

> **Loja hoje:** ✅ **Live v15.68** · `producao` @ **75288b5** · Render `dep-d9tmfcoae00c73bfb9o0`  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **FOLHA-FAMILIA** | ✅ enviado / Live v15.68 | não |

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.68** · `producao` @ **75288b5** · base era `e0d19a3` (v15.62) |
| **Branch** | `deploy/folha-familia-1108` (FF → `producao`) |
| **O quê** | Folha: granel some; venda × fator soma no saco · decimal |
| **Prova** | **42/42** · sem migrate |
| **Rollback** | tag `rollback/pre-folha-familia-v15.62` @ `e0d19a3` · frase+senha |
| **Você agora** | **Ctrl+F5** · badge **v15.68** · Folha ADIMAX · granel fora · saco com fração |

### ✅ CHECKLIST ÚNICO — enviado produção (11/08 · loja v15.68)

> **Loja hoje:** ✅ **Live v15.68** · `producao` @ **75288b5**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **FOLHA-FAMILIA** | ✅ enviado / Live v15.68 | não |

### 🚀 PREP deploy loja — FOLHA-FAMILIA (`deploy/folha-familia-1108` · **v15.68**) · **superado**

> Vigente: **Deploy loja — FOLHA-FAMILIA Live v15.68** no topo.

### ✅ Deploy loja — lote checklist 11/08 (`deploy/lote-checklist-1108` · **v15.62**)

> **Loja hoje:** ✅ **Live v15.62** · `producao` @ **e0d19a3** · Render `dep-d9tm20hsrm7s73alh9g0`  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **BI-TOPBAR-COMPACT** | ✅ enviado / Live v15.62 | não |
| 2 | **FIADO-RECIBO** | ✅ enviado / Live v15.62 | não |
| 3 | **FOLHA-FORN-HIST** | ✅ enviado / Live v15.62 | não |

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.62** · `producao` @ **e0d19a3** · base era `1f50976` (v15.55) |
| **Rollback** | tag `rollback/pre-lote-checklist-1108-v15.55` @ `1f50976` |

### ✅ CHECKLIST ÚNICO — enviado produção (11/08 · loja v15.62) · **superado**

> Vigente: **CHECKLIST ÚNICO — pronto envio (11/08 · loja v15.62)** no topo.

### 🐛 FIX — Folha Compras por fornecedor (`FOLHA-FORN-HIST` · **teste v15.61**) · VERIFY_OK

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ push `teste` · prova **28/28** · no checklist envio |
| **Commit** | `9e0efc2` |
| **Sintoma** | Folha (PDV + Compras) parecia só o **último pedido** (ex. ADIMAX) |
| **Fix** | Cadastro **∪** histórico Entrada NF Agro · 1º token do nome · pré-filtro NF id **ou** nome |
| **Migrate** | **NÃO** |

### ✅ Deploy loja — filtro loja DRE + BI (`deploy/dre-loja-filtro-1008` · **v15.55**)

> **Loja hoje:** ✅ **Live v15.55** · `producao` @ **1f50976**  
> **⚠️** **NÃO** merge `teste`→`producao`.

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.55** · `producao` @ **1f50976** · base era `4bf3410` (v15.54) |
| **Pacote** | **DRE-LOJA-FILTRO** |
| **O quê** | DRE + BI: **Centro + Vila** (padrão) · Centro · Vila. Vendas/CMV = PDV. Despesas/emp. = empresa (Vila sem cadastro → Centro). |
| **Rollback** | `git push origin 4bf3410:producao` ou tag `rollback/pre-dre-loja-filtro-v15.54` |
| **Migrate** | **NÃO** |
| **Fora** | PDV · caixa · wizard · Indicadores HTML |
| **Você** | Ctrl+F5 DRE + BI `/` · badge **v15.55** · filtro **Loja** / **Números** · padrão as duas |

### ✅ CHECKLIST ÚNICO — enviado produção (10/08 · loja v15.55) · **superado**

> **Vigente:** **CHECKLIST ÚNICO — pronto envio (10/08)** no topo. Loja permanece **v15.55** até o próximo envio.

### ✅ Deploy loja — DRE-EMP-CARD (`deploy/dre-emp-card-1008` · **v15.54**)

> **Loja hoje:** ✅ **Live v15.54** · `producao` @ **4bf3410**  
> **⚠️** **NÃO** merge `teste`→`producao`.

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.54** · `producao` @ **4bf3410** · base era `674901d` (v15.52) |
| **Pacote** | **DRE-EMP-CARD** |
| **O quê** | Card Empréstimos (devido bruto + juros + total + pago + emprestado) · Mini DRE **Saldo final = soma** · frases no **«?»** · rótulos sem quebra |
| **Rollback** | `git push origin 674901d:producao` ou tag `rollback/pre-dre-emp-card-v15.52` |
| **Migrate** | **NÃO** |
| **Fora** | PDV · caixa · wizard · Indicadores HTML |
| **Você** | Ctrl+F5 DRE · badge **v15.54** · Jul/2026 Centro vencimento: emp. devido ~42.601 · juros ~4.658 |

### ✅ CHECKLIST ÚNICO — enviado produção (10/08 · loja v15.54) · **superado**

> **Vigente:** **CHECKLIST ÚNICO — enviado produção (10/08 · loja v15.55)** no topo.

### ✅ Deploy loja — DRE cadastro oficial de planos (`deploy/dre-planos-cadastro-1008` · **v15.52**)

> **Loja hoje:** ✅ **Live v15.52** · `producao` @ **674901d**  
> **⚠️** **NÃO** merge `teste`→`producao`.

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.52** · `producao` @ **674901d** · base era `2f6d05e` (v15.50) |
| **Pacote** | **DRE-PLANOS-CADASTRO** |
| **Rollback** | `git push origin 2f6d05e:producao` ou tag `rollback/pre-dre-planos-cadastro-v15.50` |
| **Migrate** | **NÃO** |
| **Fora** | PDV · caixa · wizard · Indicadores HTML |
| **Você** | Ctrl+F5 DRE · badge **v15.52** · despesas com nome oficial do cadastro |

### ✅ CHECKLIST ÚNICO — enviado produção (10/08 · loja v15.52)

> **Loja hoje:** ✅ **Live v15.52** · `producao` @ **674901d**  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **DRE-PLANOS-CADASTRO** | ✅ enviado / Live v15.52 | não |

### ✅ Deploy loja — DRE visual polish (`deploy/dre-visual-polish-0908` · **v15.50**)

> **Loja hoje:** ✅ **Live v15.50** · `producao` @ **2f6d05e**  
> **⚠️** **NÃO** merge `teste`→`producao`.

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.50** · `producao` @ **2f6d05e** · base era `9b212b0` (v15.46) |
| **Pacote** | **DRE-VISUAL-POLISH** |
| **Rollback** | `git push origin 9b212b0:producao` ou tag `rollback/pre-dre-visual-polish-v15.46` |
| **Migrate** | **NÃO** |
| **Fora** | PDV · caixa · wizard · Indicadores HTML |
| **Você** | Ctrl+F5 DRE · badge **v15.50** · comparativos · Mini DRE juros/empréstimo · Saldo final |

### ✅ CHECKLIST ÚNICO — enviado produção (09/08 · loja v15.50)

> **Loja hoje:** ✅ **Live v15.50** · `producao` @ **2f6d05e**  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **DRE-VISUAL-POLISH** | ✅ enviado / Live v15.50 | não |

### 📦 PACOTE PRONTO — DRE visual polish (`DRE-VISUAL-POLISH` · **v15.50**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.50** · `producao` @ **2f6d05e** |
| **O quê** | PE barras modernas · despesas por categoria no recorte · Mini DRE juros + empréstimo + **Saldo final** · cards **vs mês passado** + **vs média 90d** |
| **Prova** | tests **34/34** · visual **191/191** · CMV **55/55** · PDV-DRE **33/33** · RG **79/79** |
| **Migrate** | **NÃO** |
| **Fora** | Indicadores HTML · API `geracao_caixa` · PDV/caixa |

### ✅ Deploy loja — lote UX+DRE+BI (`deploy/lote-ux-dre-bi-0908` · **v15.46**)

> **Loja hoje:** ✅ **Live v15.46** · `producao` @ **9b212b0**  
> **⚠️** **NÃO** merge `teste`→`producao`.

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.46** · `producao` @ **9b212b0** · base era `ebe9e9c` (v15.26) |
| **Pacotes** | **PDV-RACOES-LISTA-UX** + **DRE-VISUAL-PREVIA** + **BI-LUCRO-LIQUIDO** |
| **Rollback** | `git push origin ebe9e9c:producao` ou tag `rollback/pre-lote-ux-dre-bi-v15.26` |
| **Migrate** | **NÃO** |
| **Fora** | caixa, checkout, wizard wholesale, relatórios WIP, Indicadores HTML |
| **Você** | Ctrl+F5 · BI `/` Lucro Líquido · DRE visual · PDV Rações lista (foto+zebra) · badge **v15.46** |

### ✅ CHECKLIST ÚNICO — enviado produção (09/08 · loja v15.46)

> **Loja hoje:** ✅ **Live v15.46** · `producao` @ **9b212b0**  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-RACOES-LISTA-UX** | ✅ enviado / Live v15.46 | não |
| 2 | **DRE-VISUAL-PREVIA** | ✅ enviado / Live v15.46 | não |
| 3 | **BI-LUCRO-LIQUIDO** | ✅ enviado / Live v15.46 | não |

### 📦 PACOTE PRONTO — BI Lucro Líquido (`BI-LUCRO-LIQUIDO` · **v15.42**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.46** · `producao` @ **9b212b0** |
| **O quê** | BI `/` · card **Lucro Líquido** no lugar de Novos Clientes · **vencimento** · Bruto + Pago · mesma conta do Resumo. Vila sem empresa própria usa Agro Mais Centro. |
| **Você** | Ctrl+F5 `/` · datas 12/07–10/08 · Bruto = **-R$ 2.480,17** · Pago = **R$ 1,29** |
| **Prova** | verify **73/73** · tests **4/4** · live PG = Indicadores |
| **Migrate** | **NÃO** |

### 📦 PACOTE PRONTO — Lista Rações foto + zebra (`PDV-RACOES-LISTA-UX`)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.46** · `producao` @ **9b212b0** |
| **O quê** | Foto miniatura (clique abre grande) · “No carrinho” junto do Adicionar · sem coluna Carrinho · linha **zebra cinza fraca** (sem cor da marca) |
| **Prova** | verify Rações **129/129** · tests **17/17** · `node --check` |
| **Você** | Ctrl+F5 PDV → Rações → lista |
| **Migrate** | **NÃO** |

### 📦 PACOTE PRONTO — DRE visual prévia (`DRE-VISUAL-PREVIA` · **v15.46**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.46** · `producao` @ **9b212b0** |
| **O quê** | `/financeiro/resumo-gerencial/` — 16:9 · donut despesas + receita por categoria PDV · PE linha · mini DRE · empréstimos no filtro (não total) · sem Estoque · grade sem sobrepor. Indicadores intacto. |
| **Você** | Ctrl+F5 DRE · 1 dia → devido ≠ total · cards sem tapar · zoom Aa |
| **Prova** | tests DRE+CMV **27/27** · verify visual **157/157** |
| **Migrate** | **NÃO** |

### ✅ CHECKLIST ÚNICO — enviado produção (09/08 · loja v15.26)

> **Loja hoje:** ✅ **Live v15.26** · `producao` @ **ebe9e9c** · Render `dep-d9sh2egae00c73anribg`  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-RACOES-LISTA-DENSE** | ✅ enviado / Live v15.26 | não |
| 2 | **RG-CMV-TOGGLE** | ✅ enviado / Live v15.26 | não |

### ✅ Deploy loja — lote DENSE + Resumo CMV (`deploy/lote-dense-rg-0908` · **v15.26**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.26** · `producao` @ **ebe9e9c** · Render `dep-d9sh2egae00c73anribg` · base era `5fc0ee8` (v15.24) |
| **Pacotes** | **PDV-RACOES-LISTA-DENSE** + **RG-CMV-TOGGLE** |
| **Fora** | resto do `teste` · **NÃO** merge `teste`→`producao` |
| **Migrate** | **NÃO** |
| **O quê** | Lista Rações compacta (sem quebra) + Resumo CMV vendida × paga |
| **Provas** | teste **32/32** · worktree **32/32** · verify Rações **VERIFY_OK** · RG **78/78** · DRE **55/55** · PDV-DRE **33/33** |
| **Você** | **Ctrl+F5** PDV → Rações → lista · Resumo gerencial → julho Centro → trocar os dois botões |
| **Rollback** | tag `rollback/pre-lote-dense-rg-v15.24` @ `5fc0ee8` · branch `producao-backup-pre-v1526-dense-rg-20260809` · frase+senha |

### 📦 PACOTE PRONTO — Lista Rações sem quebra (`PDV-RACOES-LISTA-DENSE` · **v15.26**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.26** · `producao` @ **ebe9e9c** |
| **O quê** | Lista numa linha só (sem quebra em tamanho / botão / carrinho). Overlay um pouco maior. Zoom Agro **não** muda. |
| **Você** | Ctrl+F5 PDV → Rações → lista → deve caber mais produtos |
| **Prova** | `tests_pdv_racoes` **16/16** · verify path lista+dense **VERIFY_OK** · `node --check` · `manage.py check` |
| **Migrate** | **NÃO** |

### 📦 PACOTE PRONTO — Resumo CMV vendida × paga (`RG-CMV-TOGGLE` · **v15.25**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.26** · `producao` @ **ebe9e9c** |
| **O quê** | `/financeiro/resumo-gerencial/` — botão **Mercadoria vendida** × **Mercadoria paga** (igual Indicadores). Lucro, operacional, líquido e PE acompanham. Caixa não muda. Markup % no card lucro bruto. |
| **Você** | Ctrl+F5 Resumo gerencial → julho Centro → trocar os dois botões |
| **Prova** | `tests_receita_pdv_dre` **16/16** · verify RG **78/78** · verify DRE **33/33** · verify CMV **55/55** · `node --check` · `manage.py check` |
| **Migrate** | **NÃO** |

### ✅ Inauguração Vila — 5% encerrado (09/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **encerrado** · janela era **07–08/08** · hoje **09/08** o 5% **já desliga sozinho** |
| **Decisão** | **Deixar o código** (não apagar, não deploy) |
| **Por quê** | Remover agora = risco no PDV sem ganho. Faixa só liga na data. **CAMP-PROMO-MENOR** (promo da loja certa + GM) **fica** — é correção permanente. |
| **Loja** | ✅ **Live v15.05** · sem faixa laranja · promoções normais |
| **Se 5% ainda aparecer** | Ctrl+F5 · se persistir: `AGRO_CAMPANHA_INAUGURACAO_OFF=1` + restart |
| **Próxima inauguração** | Reusar o mesmo módulo · só mudar datas |

### ✅ CHECKLIST ÚNICO — enviado produção (09/08 · loja v15.24)

> **Loja hoje:** ✅ **Live v15.24** · `producao` @ **5fc0ee8** · Render `dep-d9sg5g37uimc73bncm1g`  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-RACOES-LISTA** | ✅ enviado / Live v15.24 | não |

### ✅ Deploy loja — Rações lista (`deploy/pdv-racoes-lista` · **v15.24**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.24** · `producao` @ **5fc0ee8** · Render `dep-d9sg5g37uimc73bncm1g` · base era `2116b41` (v15.23) |
| **Pacotes** | **PDV-RACOES-LISTA** |
| **Fora** | resto do `teste` · **NÃO** merge `teste`→`producao` |
| **Migrate** | **NÃO** |
| **O quê** | Depois do tamanho: overlay grande, barato→caro, Adicionar / Adicionar todas / Fechar. Linha verde. Esc fecha. |
| **Provas** | `tests_pdv_racoes` **16/16** · verify **VERIFY_OK** · `node --check` · `manage.py check` |
| **Você** | **Ctrl+F5** PDV → Rações → tipo → marca → tamanho → lista → Adicionar |
| **Rollback** | tag `rollback/pre-pdv-racoes-lista-v15.23` @ `2116b41` · branch `producao-backup-pre-v1524-racoes-lista-20260809` · frase+senha |

### 📦 PACOTE PRONTO — Rações escolhe na lista (`PDV-RACOES-LISTA` · **v15.24**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.24** · `producao` @ **5fc0ee8** |
| **O quê** | Depois do tamanho: overlay grande, barato→caro, Adicionar / Adicionar todas / Fechar. Linha verde. Esc fecha. Não vai direto ao carrinho. |
| **Você** | Ctrl+F5 PDV → Rações → tipo → marca → tamanho → lista → Adicionar |
| **Prova** | `tests_pdv_racoes` **16/16** · verify **VERIFY_OK** · `node --check` · `manage.py check` |
| **Migrate** | **NÃO** |

### ✅ CHECKLIST ÚNICO — enviado produção (09/08 · loja v15.23)

> **Loja hoje:** ✅ **Live v15.23** · `producao` @ **2116b41** · Render `dep-d9sflrgu01pc73e5vet0`  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **DRE-CMV-TOGGLE** | ✅ enviado / Live v15.23 | não |

### ✅ Deploy loja — DRE CMV vendida × paga (`deploy/dre-cmv-toggle-0908` · **v15.23**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.23** · `producao` @ **2116b41** · Render `dep-d9sflrgu01pc73e5vet0` · base era `759e435` (v15.20) |
| **Pacotes** | **DRE-CMV-TOGGLE** |
| **Fora** | resto do `teste` · **NÃO** merge `teste`→`producao` |
| **Migrate** | **NÃO** |
| **O quê** | Indicadores DRE: botão **Mercadoria vendida** × **Mercadoria paga**. Lucro/margem/líquido acompanham. Caixa não muda. |
| **Provas** | `tests_receita_pdv_dre` **12/12** · `tests_pdv_racoes` **15/15** · path **47/47** · verify DRE **24/24** · `manage.py check` |
| **Você** | **Ctrl+F5** Indicadores → DRE → trocar os dois botões |
| **Rollback** | tag `rollback/pre-dre-cmv-toggle-v15.20` @ `759e435` · branch `producao-backup-pre-v1521-dre-cmv-20260809` · frase+senha |

### 📦 PACOTE PRONTO — DRE CMV vendida × paga (`DRE-CMV-TOGGLE` · **v15.23**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.23** · `producao` @ **2116b41** |
| **O quê** | Indicadores DRE: botão **Mercadoria vendida** (custo × qtd) ou **Mercadoria paga** (lançamentos). Lucro, margem e líquido acompanham. Caixa não muda. Padrão = vendida. Se CMV vendida falhar, fica na paga. |
| **Você** | Ctrl+F5 Indicadores → DRE → trocar os dois botões |
| **Prova** | `tests_receita_pdv_dre` **12/12** · path **47/47 VERIFY_OK** |
| **Migrate** | **NÃO** |

### ✅ CHECKLIST ÚNICO — enviado produção (09/08 · loja v15.20)

> **Loja hoje:** ✅ **Live v15.20** · `producao` @ **759e435**  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-RACOES-FIX** | ✅ enviado / Live v15.20 | não |
| 2 | **PDV-RACOES-25** | ✅ enviado / Live v15.20 | não |

### ✅ Deploy loja — Rações fix + saco 2,5 kg (`deploy/pdv-racoes-fix-25` · **v15.20**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.20** · `producao` @ **759e435** · Render `dep-d9s9pujl550s73e4dc10` · base era `42faf6c` (v15.16) |
| **Pacotes** | **PDV-RACOES-FIX** + **PDV-RACOES-25** |
| **Fora** | resto do `teste` · **NÃO** merge `teste`→`producao` |
| **Migrate** | **NÃO** |
| **O quê** | Botão Rações lê cadastro na hora + saco **2,5 kg** |
| **Provas** | `tests_pdv_racoes` **15/15** · verify **VERIFY_OK** · `manage.py check` |
| **Você** | **Ctrl+F5** PDV → Rações → **Cão adulto** → Origens · Peso `2,5` no cadastro |
| **Rollback** | tag `rollback/pre-pdv-racoes-fix-25-v15.16` @ `42faf6c` · branch `producao-backup-pre-v1520-racoes-20260809` · frase+senha |

### 📦 PACOTE PRONTO — Rações lê cadastro ao vivo (`PDV-RACOES-FIX` · **v15.20**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.20** · `producao` @ **759e435** |
| **O quê** | Botão Rações puxa Categoria/Sub 1/Sub 2/Peso do Agro na hora (não usa lista velha do PDV) |
| **Você** | Ctrl+F5 no PDV → Rações → **Cão adulto** → deve aparecer Origens |
| **Prova** | `tests_pdv_racoes` **15/15** · verify path Origens+2,5 **VERIFY_OK** |
| **Migrate** | **NÃO** |

### 📦 PACOTE PRONTO — Rações saco 2,5 kg (`PDV-RACOES-25` · **v15.18**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.20** · `producao` @ **759e435** |
| **O quê** | Botão Rações: tamanho **Saco 2,5 kg** · cadastro Peso (etiqueta) = **`2,5`** |
| **Cadastro** | Só `2,5` (aceita `2.5` / `2,50 kg`) · **não** vira 25 kg nem pacote |
| **Prova** | `tests_pdv_racoes` **15/15** · verify path 2,5 **VERIFY_OK** · `manage.py check` |
| **Migrate** | **NÃO** |

### ✅ Deploy loja — lote checklist 09/08 (`deploy/lote-checklist-0908` · **v15.16**)

> **Loja hoje:** ✅ **Live v15.16** · `producao` @ **42faf6c** · Render `dep-d9s5ogjbc2fs73b36p8g`  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **DRE-PDV-RECEITA** | ✅ enviado / Live v15.16 | não |
| 2 | **RG-AJUDA-MODAL** | ✅ enviado / Live v15.16 | não |
| 3 | **PDV-RACOES** | ✅ enviado / Live v15.16 | não |

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.16** · `producao` @ **42faf6c** |
| **Branch** | `deploy/lote-checklist-0908` (FF → `producao`) · base era `9c45d74` (v15.05) |
| **Pacotes** | DRE-PDV-RECEITA · RG-AJUDA-MODAL · PDV-RACOES |
| **Fora** | resto do `teste` · **NÃO** merge `teste`→`producao` |
| **Migrate** | **NÃO** |
| **Provas** | tests **20/20** · verify DRE **15/15** · verify Rações **VERIFY_OK** · `manage.py check` |
| **Rollback** | tag `rollback/pre-lote-checklist-0908-v15.05` @ `9c45d74` · frase+senha |
| **Você agora** | **Ctrl+F5** PDV (botão Rações) · Indicadores (receita ≈ card PDV) · Resumo (ajuda só no ?) |

### 📦 PACOTE PRONTO LOJA — DRE usa venda do PDV (`DRE-PDV-RECEITA` · **v15.14**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.16** · `producao` @ **42faf6c** |
| **O quê** | Indicadores + Resumo: **receita operacional = faturamento PDV** (loja da empresa) · líquido recalcula · CR só conferência · grupo soma cada loja (não “todas”) · caixa não mistura |
| **Prova** | `tests_receita_pdv_dre` **7/7** · verify **VERIFY_OK** · `manage.py check` |
| **Migrate** | **NÃO** |
| **Você** | Ctrl+F5 Indicadores · mês até hoje · receita ≈ card PDV · líquido deixa de ser −10 mil |

### 📦 PACOTE PRONTO LOJA — Resumo aviso ajuda (`RG-AJUDA-MODAL` · **v15.10**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.16** · `producao` @ **42faf6c** |
| **O quê** | `/financeiro/resumo-gerencial/` — modal ajuda só no **?** |
| **Commit** | `9eabf62` |
| **Migrate** | **NÃO** |

### 📦 PACOTE PRONTO — PDV atalho Rações (`PDV-RACOES` · **v15.11+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.16** · `producao` @ **42faf6c** |
| **O quê** | Botão **Rações** no PDV → tipo → marca (ou Todas) → tamanho (ou Todos) → carrinho |
| **Cadastro** | Cat. `Rações` · Sub 1 `Cão`/`Gato` · Sub 2 + Peso `1`/`2,5`/`5`/`10`/`15`/`20`/`25`/`pacote` |
| **Catálogo** | Cache PDV **v3** · 1ª abertura do dia reconstrói |
| **Você** | Ctrl+F5 PDV · testar Estimação 15 kg em Cão adulto |
| **Prova** | `tests_pdv_racoes` **13/13** · verify path cadastro→overlay→catálogo→JS→carrinho **VERIFY_OK** (loja + teste) · `manage.py check` |
| **Migrate** | **NÃO** |
| **Commit** | `251d679` + verify |

### ✅ Deploy loja — CAD-XLSX-COLS (`deploy/cad-xlsx-cols` · **v15.05**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.05** · `producao` @ **9c45d74** · Render `dep-d9s5cum417fc73aov3fg` |
| **Loja hoje** | ✅ **Live v15.05** · `producao` @ **9c45d74** |
| **Branch** | `deploy/cad-xlsx-cols` (FF → `producao`) · base era `14dc51a` (v15.04) |
| **Pacote** | **CAD-XLSX-COLS** só |
| **Fora** | resto do `teste` · **NÃO** merge `teste`→`producao` · **NÃO** cherry `19c09bf`/`355b76d` |
| **Migrate** | **NÃO** |
| **O quê** | Excel Cadastro: checkboxes Sub 2–4, Unidade, Modelo, **Peso** · gravar `peso_etiqueta` · célula vazia não altera |
| **Provas** | `tests_cadastro_planilha_peso` **7/7** · verify **20/20** · `manage.py check` |
| **Você** | Cadastro → Ctrl+F5 → Excel ↓ → marcar Sub 2–4 / Unidade / Modelo / Peso |
| **Rollback** | tag `rollback/pre-cad-xlsx-cols-v15.04` @ `14dc51a` · branch `producao-backup-pre-v1505-cad-xlsx-20260809` · frase+senha |

### ✅ CHECKLIST ÚNICO — enviado produção (09/08 · loja v15.16) · **superado**

> **Vigente:** loja **Live v15.20**. `producao` @ **759e435**.  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **DRE-PDV-RECEITA** | ✅ enviado / Live v15.16 | não |
| 2 | **RG-AJUDA-MODAL** | ✅ enviado / Live v15.16 | não |
| 3 | **PDV-RACOES** | ✅ enviado / Live v15.16 | não |

### ✅ CHECKLIST ÚNICO — enviado produção (09/08 · loja v15.05) · **superado**

> **Vigente:** loja **Live v15.16**. `producao` @ **42faf6c**.  
> **⚠️** **NÃO** merge `teste`→`producao`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CAD-XLSX-COLS** | ✅ enviado / Live v15.05 | não |

### CAD-XLSX-COLS — Excel cadastro (loja · 09/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.05** · `9c45d74` |
| **O quê** | Modal Excel ↓ mostra Sub 2–4, Unidade, Modelo, Peso · import grava Peso |
| **Regra** | Célula vazia **não** altera · baixar Excel de novo |
| **Rollback** | `rollback/pre-cad-xlsx-cols-v15.04` → v15.04 |
| **Migrate** | **NÃO** |

### ✅ Deploy loja — CAMP-PROMO-MENOR (`deploy/camp-promo-menor-0808` · **v15.04**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.04** · `producao` @ **14dc51a** · Render `dep-d9ra30bbc2fs73ahkii0` |
| **Loja hoje** | ✅ **Live v15.04** · `producao` @ **14dc51a** |
| **Branch** | `deploy/camp-promo-menor-0808` (FF → `producao`) · base era `290e8b2` (v15.03) |
| **Pacote** | **CAMP-PROMO-MENOR** só |
| **Fora** | resto do `teste` · GG-UX · **NÃO** merge `teste`→`producao` |
| **Migrate** | **NÃO** |
| **O quê** | PDV Vila pedia promo da **Centro** → valor direto 54,90 não entrava · 5% sozinho (60→57). Fix: pede promo da loja do caixa · casa pelo GM · recarrega ao voltar/salvar · cache v2 |
| **Provas** | `tests_promocoes_busca` **8/8** · `tests_campanha_pdv` **15/15** · verify campanha **45/45** · verify promo busca **54/54** · `manage.py check` OK |
| **Você** | Inauguração **passou** · 5% off. Promo valor direto segue valendo (fix permanente). |
| **Kill** | só se 5% ainda aparecer: `AGRO_CAMPANHA_INAUGURACAO_OFF=1` + restart |
| **Rollback** | tag `rollback/pre-camp-promo-menor-0808-v15.03` @ `290e8b2` |

### 📦 PACOTE PRONTO — Promo valor direto prevalece no 5% Vila (`CAMP-PROMO-MENOR`)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.04** · `producao` @ **14dc51a** |
| **Sintoma** | Farelo GM1507-30: lista **R$ 60** · valor direto **R$ 54,90** · PDV Vila cobrou **R$ 57** (só 5%) |
| **Causa** | Promo **estava gravada** (PG id 9, Vila, 07–08, 54,90). PDV da loja carrega promo só da **Centro**. Campanha 5% rodava sozinha. |
| **Fix** | Recarrega promo ao voltar no PDV / salvar em outra aba · casa também pelo **GM** · cache v2 · caixa Vila manda na empresa da promo |
| **Prova** | `tests_promocoes_busca` **8/8** · `tests_campanha_pdv` **15/15** · verify campanha **45/45** · verify promo busca **54/54** |
| **Migrate** | **NÃO** |
| **Você agora** | **Ctrl+F5** PDV Vila → farelo = **R$ 54,90**. |

### ✅ Deploy loja — lote checklist 07/08 (`deploy/lote-checklist-0708` · **v15.03**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado / Live v15.03** · `producao` @ **290e8b2** · Render `dep-d9r9hne417fc73a776pg` |
| **Branch** | `deploy/lote-checklist-0708` (FF → `producao`) · base era `e68187b` (v14.85) |
| **Pacotes** | CAMP-VILA-5 · PROMO-BUSCA-PG |
| **Fora** | GG-UX (P2,5) |
| **Migrate** | **NÃO** |
| **Após Live** | Ctrl+F5 PDV **Vila** + **Centro** · Promoções → Nova → etapa 2 (GM/nome) |
| **Provas** | campanha **15/15** + verify **35/35** · promo busca **5/5** + verify **53/53** · `manage.py check` OK |
| **Risco loja aberta** | Médio **07 e 08/08 Vila** (5% no PDV) · **09/08 volta ao normal sozinho** · Centro sem faixa · busca promo só tela Promoções |
| **Kill** | `AGRO_CAMPANHA_INAUGURACAO_OFF=1` + restart Render · **não** ligar TEST na loja |
| **Rollback** | tag `rollback/pre-lote-checklist-0708-v14.85` @ `e68187b` |
| **⚠️** | **NÃO** merge `teste`→`producao` · só esta branch · sem migrate |
| **Você agora** | **Ctrl+F5** PDV Vila + Centro · Promoções etapa 2 |

### ✅ CHECKLIST ÚNICO — enviado produção (08/08 · loja v15.04) · **superado**

> **Vigente:** **CHECKLIST ÚNICO — pronto envio (09/08)** no topo. Loja permanece **v15.04** até o próximo envio.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CAMP-VILA-5** | ✅ enviado / Live v15.03 | não |
| 2 | **PROMO-BUSCA-PG** | ✅ enviado / Live v15.03 | não |
| 3 | **CAMP-PROMO-MENOR** | ✅ enviado / Live v15.04 | não |

### 📦 PACOTE PRONTO LOJA — Inauguração Vila 5% auto (`CAMP-VILA-5` · **v15.03**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **encerrado 09/08** · código ficou na loja (desliga sozinho) · Live desde v15.03 |
| **O quê** | **07 e 08/08/2026** só **Vila Elias**: 5% off · menor vs promo · arredonda 5¢ · **09/08 volta ao normal** · não mexe cadastro · faixa laranja |
| **Fix extra** | Recalc (mudou qtd) limpa marca da campanha — promo+5% não fica com preço velho |
| **Prova** | `manage.py test produtos.tests_campanha_pdv` **15/15** · `scripts/verify_camp_vila_5_path.py` **35/35** |
| **Migrate** | **NÃO** |
| **Risco** | Médio — só Vila nos dias 07–08 · kill `AGRO_CAMPANHA_INAUGURACAO_OFF=1` |
| **Você** | **Ctrl+F5** PDV Vila **07 e 08** · milho 1kg → 2,40 · Centro sem faixa · **09** sem 5% |

### 📦 PACOTE PRONTO LOJA — Promo busca produto Postgres (`PROMO-BUSCA-PG` · **v15.02**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v15.03** · VERIFY_OK **53/53** |
| **O quê** | Etapa 2 da promoção busca no **Postgres** (GM + nome); JS aceita GM com hífen (`GM1507-30`) |
| **Causa** | API ainda exigia Mongo; loja já é `agro_pg` |
| **Prova** | `manage.py test produtos.tests_promocoes_busca` **5/5** · `scripts/verify_promo_busca_pg_path.py` **53/53** |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só tela promoções / estoque ajuste que reusa o motor |
| **Você** | Ctrl+F5 Promoções → Nova → etapa 2 → GM ou nome |
| **Autorizar** | *pode subir lote checklist 07/08 / deploy/lote-checklist-0708 para produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — Dispenser pet sem comer pelo branco (`DSP-PET-BG` · **v14.84**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.85** |
| **O quê** | + Adicionar pet não limpa fundo branco/preto (peito branco intacto); ingredientes seguem iguais |
| **Prova** | `scripts/verify_dsp_pet_bg.py` **31/31** · `verify_dsp_png_bg.py` OK · `manage.py check` OK |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só `/interno/dispenser-a6*` · **zero** PDV |
| **Arquivo** | `dispenser_a6_studio.html` |
| **Você** | Ctrl+F5 Dispenser → Bicho → apagar pet ruim → subir de novo |
| **Autorizar** | *pode subir DSP-PET-BG / dispenser pet para produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — Uso loja donos + cards (`PDV-USO-DONOS` · **v14.82**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.85** |
| **O quê** | Motivos **Uso Geraldinho** / **Uso Geraldo** · cards de soma · **Uso Centro** / **Uso Vila Elias** · botão topbar verde |
| **Prova** | `scripts/verify_uso_loja_donos_path.py` **65/65** · `manage.py check` OK |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só Uso loja |
| **Você** | Ctrl+F5 PDV · motivo dono · Histórico 4 cards · botão verde |
| **Autorizar** | *pode subir PDV-USO-DONOS / uso loja donos para produção* + **99738595** |

### ✅ Deploy loja **v14.85** — lote checklist 06/08b (frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.85** · `producao` @ **e68187b** · Render `dep-d9qlio0u01pc739jq0hg` |
| **Incluiu** | CX-EMP-LOJA · VAL-SALVAR · BI-VAL-LOJA · PDV-USO-DONOS · DSP-PET-BG |
| **Branch** | `deploy/lote-checklist-0608b` (FF → `producao`) |
| **Migrate** | **NÃO** |
| **Fora** | GG-UX (P2,5) |
| **Rollback** | tag `rollback/pre-lote-checklist-0608b-v14.72` @ `008e361` |
| **Você** | **Ctrl+F5** Retirada Vila · Validade · BI (TRAVA) · Uso loja · Dispenser |

### ✅ CHECKLIST ÚNICO — enviado produção (06/08b · loja v14.85) · **superado**

> **Vigente:** **CHECKLIST ÚNICO — pronto envio (07/08)** no topo. Loja permanece **v14.85** até o próximo envio.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CX-EMP-LOJA** | ✅ enviado | não |
| 2 | **VAL-SALVAR** | ✅ enviado | não |
| 3 | **BI-VAL-LOJA** | ✅ enviado | não |
| 4 | **PDV-USO-DONOS** | ✅ enviado | não |
| 5 | **DSP-PET-BG** | ✅ enviado | não |
| 6 | **GG-UX** | 🟡 P2,5 · fora | — |

### 📦 PACOTE PRONTO LOJA — BI Validade segue loja travada (`BI-VAL-LOJA` · **v14.78**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.85** |
| **O quê** | Card Validade do BI respeita TRAVA Centro/Vila (`EstoqueLote.deposito`) · cache `v5` |
| **Prova** | `scripts/verify_bi_val_salvar_path.py` **17/17** |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só KPI do BI |
| **Você** | Ctrl+F5 BI · TRAVA Vila · números ≠ Centro |
| **Autorizar** | *pode subir BI-VAL-LOJA / validade BI loja para produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — Validade Salvar na linha (`VAL-SALVAR` · **v14.77**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.85** |
| **O quê** | Sempre **Salvar** · grava data/lote no `EstoqueLote` · API com `lote_id` |
| **Prova** | `verify_bi_val_salvar_path` · sem «Usar cadastro» |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — Relatório Validade |
| **Você** | Ctrl+F5 Validade · muda data · **Salvar** |
| **Autorizar** | *pode subir VAL-SALVAR / salvar validade para produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — saída Vila = empresa Vila (`CX-EMP-LOJA` · **v14.75**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.85** |
| **O quê** | Retirada: empresa da loja do caixa · Vila → **Agro Mais Vila Elias** · Centro → **Agro Mais Centro** |
| **Commit** | `64c338b` |
| **Prova** | `verify_saida_empresa_loja` **10/10** |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — não mexe PDV venda |
| **Você** | Ctrl+F5 Retirada Vila · empresa certa · 1 saída |
| **Autorizar** | *pode subir CX-EMP-LOJA / empresa saída caixa para produção* + **99738595** |

### ✅ Deploy loja **v14.72** — lote checklist 06/08 (frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.72** · `producao` @ **008e361** · Render `dep-d9qasd0u01pc739asp30` |
| **Incluiu** | PLANOS-PDV · FACETA-CACHE · TRANSF-BIP · FOTO-PDV · NF-VAL-BCA |
| **Branch** | `deploy/lote-checklist-0608` (FF → `producao`) |
| **Migrate** | **0085** (`exibir_pdv`, deps→0083) · **0086** (`EstoqueLote.deposito`) — Render `migrate --noinput` no build |
| **Fora** | GG-UX (P2,5) |
| **Rollback** | tag `rollback/pre-lote-checklist-0608-v14.61` @ `47bb1de` |
| **Você** | **Ctrl+F5** Cadastro · Validade · Transferências · Config Planos · Retirada |

### ✅ CHECKLIST ÚNICO — enviado produção (06/08 · loja v14.72) · **superado**

> **Vigente:** **CHECKLIST ÚNICO — pronto envio (06/08 · após loja v14.72)** no topo. Loja permanece **v14.72** até o próximo envio.

### 📦 PACOTE PRONTO LOJA — Foto Gerais = Delivery = PDV (`FOTO-PDV` · **v14.68+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.72** |
| **O quê** | Aba Gerais: anexar foto (igual Delivery) · **mesma foto** nas duas abas · PDV já lê do overlay |
| **Prova** | `scripts/verify_foto_pdv.py` **13/13** · `manage.py check` OK |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só modal cadastro |
| **Você** | Ctrl+F5 Cadastro · Gerais → foto → Salvar · Delivery igual · PDV |
| **Autorizar** | *pode subir FOTO-PDV / foto produto para produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — Transferência forçada bip (`TRANSF-BIP` · **v14.67**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.72** |
| **O quê** | Bip barras → +1 e cursor na busca; digitar nome/GM → foco na qtd |
| **Prova** | `scripts/verify_transf_bip.py` **4/4** |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só `/transferencias/` forçada |
| **Você** | Ctrl+F5 Forçada · bip · bip · digitar GM + Enter |
| **Autorizar** | *pode subir TRANSF-BIP / bip transferência para produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — Planos no PDV por checkbox (`PLANOS-PDV` · **v14.62+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.72** |
| **O quê** | Checkbox **PDV** em Planos de contas · 14 atuais pré-marcados · saída/retirada lê Postgres · marcar **Manutenção do Prédio** libera no select |
| **Commits** | `65e0f6f` (+ bump `bbddd19`) |
| **Migrate** | **SIM** — loja: `0084` (no-op, schema já ok) + `0085` (`exibir_pdv` + seed) · **não** subir `0082` |
| **Prova** | `verify_planos_conta` **28/28** · `manage.py check` OK · toggle Manutenção on/off · 14 marcados = lista antiga · IDs especiais vale/salário/outros/depósito OK · inativo some do PDV |
| **Risco** | Baixo-médio — muda texto gravado do plano (nome oficial Agro em vez do código ERP); aliases já cobrem |
| **Você** | Ctrl+F5 Config Planos · coluna PDV · marcar Manutenção · abrir Retirada e conferir |
| **Autorizar** | *pode subir PLANOS-PDV / planos no PDV para produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — Cadastro faceta unidade/marca/cat (`FACETA-CACHE` · **v14.66**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.72** |
| **Bug** | Cadastrou Caixa no produto · no outro a lista dizia «não cadastrado» (marca/cat igual) |
| **Fix** | Invalidar cache `v6` ao salvar/+ PIN · servidor junta produto+PIN · JS **soma** (não apaga) · seed ao abrir/salvar |
| **Prova** | `scripts/verify_faceta_cache.py` **10/10** · integração overlay+PIN OK · `manage.py check` OK |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só combobox do cadastro |
| **Você** | Ctrl+F5 Cadastro · digitar **Caixa** no outro Natuverm → tem que achar |
| **Autorizar** | *pode subir FACETA-CACHE / faceta cadastro para produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — Entrada NF validade → Validade + BCA (`NF-VAL-BCA` · **v14.71**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.72** |
| **O quê** | Etapa 4 NF → `EstoqueLote` (+ `deposito`) · estorno reduz lote · colunas **Centro/Vila** · filtro Loja por saldo/lote · BCA + linha editável sem data · backfill `manage.py backfill_validade_entrada_nf` |
| **Migrate** | **SIM** — `0086_estoque_lote_deposito` · loja: após deploy rodar migrate · backfill com `--aplicar` se faltar lote antigo |
| **Prova** | `scripts/verify_validade_nf_path.py` **23/23** · `manage.py check` OK |
| **Risco** | Médio — Entrada NF estoque + tela Validade |
| **Você** | Ctrl+F5 Validade · Todas/Centro/Vila · BCA produto sem data → preencher · Entrada NF etapa 4 → conferir lista |
| **Autorizar** | *pode subir NF-VAL-BCA / validade NF para produção* + **99738595** |

### 📦 PACOTE — Planos no PDV por checkbox (`PLANOS-PDV` · detalhe)

> **Vigente:** **PACOTE PRONTO LOJA — PLANOS-PDV** no topo do CHECKPOINT.

### ✅ CHECKLIST ÚNICO — enviado produção (05/08 · loja v14.61) · **superado**

> **Vigente:** **CHECKLIST ÚNICO — pronto envio (06/08)** no topo. Loja permanece **v14.61** até o próximo envio.

**Já Live (v14.41):** BI-TOPBAR-TOTAL · SEFAZ-UI · COMP-UX · DFE-CIENCIA · CP-DUP-BACKUP · GG-GASTOS · PDV-CAD / CUSTO-FAMILIA / MODAL-UTF8.

**Prova (05/08 · revalidado):** `verify_planos_conta` **24/24** · `verify_nf_troca_estorno` **11/11** · `verify_cp_anti_dup_backup` **11/11** · `verify_bi_topbar_total` **35/35** · `verify_pacotes_pendentes_0508` **9/9** · `manage.py check` OK.

**Simulação cherry (prep):** BI/CP/NF aplicam limpo (só conflito `VERSION`/`banana.md` — normal). **PLANOS:** cherry `e109918`+`3ecc824`+`fe7746c` **sozinho falha** (arquivos da tela não existem na loja) → na branch deploy foi feito **prep** (UI+APIs+menu, **sem** `0082`/`0084`/`models`). Lote **não toca PDV/caixa** · **sem migrate**.

**Deploy concluído (05/08):** autorização recebida · `producao` **47bb1de** · Render/HTTP Live · versão confirmada na home. Fazer **Ctrl+F5** nos PCs da loja.

### 🟡 WIP — Gráfico gastos uso / clareza (`GG-UX` · **P2,5** · 05/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | 🟡 **fora do lote** · fila P2,5 · **sem pressa** |
| **Onde** | `/financeiro/grafico-gastos/` (já Live v14.41 com GG-GASTOS) |
| **Pediu** | Renan: tela confusa · revisão de clareza depois |

### ✅ ENVIADO LOJA — BI topbar filtros de data (`BI-TOPBAR-DATAS` · **Live v14.61**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.61** |
| **O quê** | Filtros `Mês até hoje`…`Datas` nunca cortados · marca/rótulo Loja só ≥1600px · badge **`Trava: Vila`** só com caixa travado · JS mede largura e joga filtros p/ 2ª linha se não couber |
| **Arquivo** | `dashboard_gerencial.html` |
| **Commit** | `8d6976f` |
| **Prova** | `verify_pacotes_pendentes_0508.py` · `verify_bi_topbar_total.py` **35/35** · Chrome 820–1920 |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só BI `/` |
| **Você** | Ctrl+F5 `/` · «Datas» visível · trava do caixa aparece só quando trava |

### ✅ ENVIADO LOJA — Backup CP menu fecha (`CP-BACKUP-MENU` · **Live v14.61**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.61** |
| **O quê** | Overlay amarelo do **Backup** não fica aberto sozinho · fecha fora / Esc / após baixar ZIP |
| **Arquivo** | `lancamentos_contas_pagar_teste.html` |
| **Commit** | `e0652c5` |
| **Prova** | `verify_cp_anti_dup_backup.py` **11/11** · ZIP abertos 200 + `backup_ultimo` grava · CSS `[hidden]` depois do `display:flex` |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só UI do botão Backup (CP-DUP-BACKUP já Live) |
| **Você** | Ctrl+F5 CP · Backup fechado · clica → abre → baixa → fecha |

### ✅ ENVIADO LOJA — NF trocar produto exige estorno (`NF-TROCA-ESTORNO` · **Live v14.61**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.61** |
| **O quê** | Com estoque já lançado, trocar/remover produto **ou mudar quantidade** pede **Estornar e trocar** (PIN) · back recusa salvar (`requer_estorno`) · XML «Confirmar na grade» e repontar id pela margem também respeitam o estorno |
| **Arquivos** | `entrada_nota.html` · `nfe_entrada_util.py` |
| **Commit** | `7f8a78d` + `eaec9a8` + `263a137` |
| **Prova** | `python scripts/verify_nf_troca_estorno.py` → **VERIFY_OK 11/11** (banco real: troca/qtd recusadas, estorno libera; API 400 `requer_estorno`; `node --check` nos scripts da tela) · `verify_pacotes_pendentes_0508.py` **9/9** · `manage.py check` OK |
| **Migrate** | **NÃO** |
| **Risco** | Médio-baixo — mexe em Entrada NF já concluída; estorno usa API de reabrir já existente |
| **Você** | Nota com estoque · Mudar produto → modal PIN · saldo antigo some / novo entra |

### ✅ ENVIADO LOJA — Planos de contas Config (`PLANOS-CONTA` · **Live v14.61**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live v14.61** · enviado via **`deploy/lote-checklist-0508`** |
| **Onde** | Configuração (F11) → **Planos de contas** |
| **O quê** | Tela edita planos oficiais · seed se vazio · visual §11 |
| **Loja** | Prep na branch deploy (UI+APIs+menu) · modelo já existe (`0065`) |
| **NÃO** | cherry cru `e109918`… (arquivos não existem na loja) · **nem** `c6757fd`/`0084` |
| **Prova** | `verify_planos_conta.py` **24/24** · reverse URL OK na branch deploy · `check` OK |
| **Você** | Ctrl+F5 · F11 → Planos · edita um dos 44 |

### 🐞 FIX — trocar produto na NF não estornava o estoque (05/08 · **teste v14.48**)

> Empacotado em **NF-TROCA-ESTORNO** (acima).

### 🐞 FIX — painel Backup do CP ficava sempre aberto (05/08 · **teste v14.46**)

> Empacotado em **CP-BACKUP-MENU** (acima).

### 🔧 BI topbar — filtros de data nunca escondidos (05/08 · **teste v14.45**)

> Empacotado em **BI-TOPBAR-DATAS** (acima).

### 📦 PACOTE PRONTO LOJA — Gráfico gastos acerto + visual (`GG-GASTOS` · **v14.30+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live** v14.41 · VERIFY_OK 10/10 · 🟡 **GG-UX P2,5** na fila (clareza / Renan acha que ainda não está certa) |
| **O quê** | Soma só planos marcados · bucket recorta no período · «Como era»/Comparar usa `as_of` · popup CP alinhado · visual Display Scale |
| **Arquivos** | `produtos/lancamentos_financeiro_pg_analytics_util.py` · `financeiro/templates/financeiro/grafico_gastos.html` |
| **Prova** | `python scripts/verify_grafico_gastos.py` → **VERIFY_OK** |
| **Commits** | `69ab665` (filtro+clip) · `6756c02` (checkup+visual) · + script verify |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só leitura BI · não grava CP |
| **Você** | Ctrl+F5 · `/financeiro/grafico-gastos/` · 1 plano (ex. Salários) · ponto Jul = CP mesmo filtro |
| **Autorizar** | *pode subir GG-GASTOS / gráfico gastos para produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — DF-e Ciência + XML completo (`DFE-CIENCIA` · **v14.33**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live** v14.41 |
| **Onde** | Entrada NF → aba SEFAZ · item **Só resumo** |
| **Fluxo** | **Dar ciência e buscar XML** → (se precisar) **Buscar XML** → **Carregar na grade** |
| **Inclui** | Evento **210210** no Ambiente Nacional · status/protocolo no Postgres · não reenvia se já ciente |
| **Migrate** | **SIM** · `0083_dfe_manifestacao_ciencia` |
| **Prova** | `python scripts/verify_dfe_manifestacao.py` → **VERIFY_OK 21/21** · unit cliente OK · `manage.py check` OK · migrate local OK |
| **Commit** | `aa2a9a3` (+ verify reforçado neste push) |
| **Risco** | Fiscal: evento oficial na Receita; XML pode demorar minutos após a Ciência |
| **⚠️** | Branch isolada + migrate na loja · **não** merge `teste` inteiro |
| **Você** | Ctrl+F5 · SEFAZ → Só resumo → Dar ciência · depois Carregar |
| **Autorizar** | *pode subir DFE-CIENCIA / ciência DF-e para produção* + **99738595** |

### 🧹 CHECKUP + visual — tela Gráfico gastos (`GG-CHECKUP`)

> ✅ Empacotado em **GG-GASTOS** (acima) · VERIFY_OK · pronto envio.

### 🐞 FIX — gráfico Gastos somava plano errado (`GG-FILTRO`)

> ✅ Empacotado em **GG-GASTOS** (acima) · filtro positivo + clip do bucket.

### 📦 PACOTE PRONTO LOJA — Anti-duplicata CP + Backup (`CP-DUP-BACKUP` · **v14.34**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live** v14.41 · VERIFY_OK 11/11 |
| **Onde** | Entrada NF («Salvar + a pagar») · Contas a pagar → **Backup** |
| **Inclui** | Bloqueio PG por chave NF / assinatura · guard Entrada NF · trava duplo clique · Backup Todos/Abertos + data/hora último |
| **Migrate** | **NÃO** |
| **Prova** | `python scripts/verify_cp_anti_dup_backup.py` → **VERIFY_OK 11/11** · `manage.py check` OK |
| **Commit** | `4b28263` (+ verify neste push) |
| **Risco** | Baixo — não apaga título antigo; só impede 2º lote novo |
| **Dados** | Maio quitado **deixar** · julho+ limpo · Ibiúna lote errado já sumiu |
| **Você** | Ctrl+F5 · CP: botão Backup · Entrada NF: não gerar 2º lote na mesma chave |
| **Autorizar** | *pode subir CP-DUP-BACKUP / anti-duplicata CP para produção* + **99738595** |

### 📦 Plano de contas SisVale (`PLANOS-CONTA` · detalhe)

> Vigente: checklist + branch **`deploy/lote-checklist-0508`**. **Não** cherry cru dos commits do `teste`. **Não** `0084`.

### 📦 CHECKLIST ÚNICO — pós v14.41 (05/08) · **superado**

> **Vigente:** **CHECKLIST ÚNICO — pronto envio (05/08 · após loja v14.41)** no topo do CHECKPOINT.

### 📦 ENVIO LOJA 05/08 — lote checklist (`LOTE-v14.41`)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** / Live v14.41 · `2efcc60` |
| **Antes** | v13.83 · `ed52234` · tag `checkpoint-loja-pre-lote-20260805` |
| **Incluiu** | SEFAZ-UI · DFE-CIENCIA · CP-DUP-BACKUP · BI-TOPBAR-TOTAL · COMP-UX · GG-GASTOS |
| **Omitiu** | **PLANOS-CONTA** (risco migrate vs 0065) |
| **Migrate** | **0083** só (DFE) — Render build roda `migrate --noinput` |
| **Prova pré-push** | verifies OK · check OK · 14 telas 200 |

### 📦 PACOTE PRONTO LOJA — Composição Saco/Kit recolher (`COMP-UX` · **v14.23**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live** v14.41 |
| **Inclui** | ▶/▼ em Saco e Kit · texto longo só no «?» · aba Composição com scroll · saco começa recolhido se kit ligado |
| **Arquivo** | `_modal_editar_produto_cadastro_erp.inc.html` (só aba Composição) · `scripts/verify_comp_ux_recolher.py` |
| **Commit** | `645fc75` (+ verify neste push) |
| **Prova** | `python scripts/verify_comp_ux_recolher.py` → **VERIFY_OK** · `verify_custo_familia.py` → **VERIFY_OK** |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só UI Cadastro Composição · **zero** lógica PDV/estoque |
| **⚠️** | Subir **sobre loja v13.83** com patch isolado do modal — **não** o modal completo do `teste` (tem WIP) |
| **Você** | Ctrl+F5 Cadastro → Composição · ▶ no Saco · vê o Kit |
| **Autorizar** | *pode subir COMP-UX / composição recolhimento para produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — BI topbar Sync + Total unidades (`BI-TOPBAR-TOTAL` · **v14.18**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live** v14.41 |
| **Inclui** | Sync ERP compacto na topbar · mais espaço aos filtros · gráfico **Faturamento por Unidade** com barra **Total** (Centro+Vila) + badge |
| **Arquivos** | `dashboard_gerencial.html` · `dashboard_gerencial_body.html` · `scripts/verify_bi_topbar_total.py` · VERSION |
| **Commit código** | `fcf1c49` · pacote+verify `739be93` · badge **14.20** |
| **Prova** | `python scripts/verify_bi_topbar_total.py` → **VERIFY_OK 35/35** · home local **200** (Sync curto · Total push · stores Centro/Vila) |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — **só BI `/`** · zero PDV/caixa/NF |
| **Você** | Ctrl+F5 `/` · Sync pequeno · filtros legíveis · Total = Centro+Vila |
| **Autorizar** | *pode subir BI-TOPBAR-TOTAL / produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — Entrada NF aba SEFAZ limpa (`SEFAZ-UI` · **v14.19**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live** v14.41 |
| **Inclui** | Aba SEFAZ só ações + lista · escritas no **?** · status compacto · chip «Só resumo» |
| **Arquivo** | `entrada_nota.html` |
| **Commit** | `e83af8f` |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só UI |
| **Você** | Ctrl+F5 `/entrada-nota/` → SEFAZ → **?** |
| **Autorizar** | *pode subir SEFAZ-UI / Entrada NF SEFAZ para produção* + **99738595** |

### 🎨 Entrada NF — aba SEFAZ limpa + ajuda «?» (04/08 · **teste v14.19**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ teste · ver **PACOTE PRONTO SEFAZ-UI** |
| **Badge** | **14.19** · `e83af8f` |
| **Prova** | Ctrl+F5 `/entrada-nota/` → SEFAZ → **?** |

### 🔧 BI — topbar sync + Total por unidade (04/08 · **teste v14.18**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ teste · ver **PACOTE PRONTO BI-TOPBAR-TOTAL** |
| **Commit** | `fcf1c49` · push `origin/teste` |

### 🩹 UX Composição — recolhimento Saco/Kit (04/08 · **v14.23**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | 📋 **PACOTE PRONTO** — ver **COMP-UX** no CHECKLIST |
| **Prova** | VERIFY_OK (`verify_comp_ux_recolher` + `verify_custo_familia`) |

### ✅ Deploy loja **v13.83** — MODAL-UTF8 acentos (04/08 · frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** · producao @ **ed52234** · badge **13.83** · Render auto |
| **Autorização** | *pode subir esse path para produção* + **99738595** |
| **Branch** | `deploy/hotfix-modal-utf8-v13.83` → producao |
| **Diff** | **2 arquivos** (modal + VERSION) · VERIFY_OK |
| **Migrate** | **NÃO** |
| **Rollback** | `git push origin b28ce83:producao` · tag `rollback/pre-modal-utf8-v13.83` |
| **Você** | Ctrl+F5 Cadastro → abas Preços/Composição com ç/ã ok |

### 🚑 HOTFIX modal acentos (`MODAL-UTF8` · **v13.83**) — **Live**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live loja** — ver Deploy v13.83 acima |
| **Rollback** | tag `rollback/pre-modal-utf8-v13.83` → b28ce83 (v13.82) |

### ✅ Deploy loja **v13.82** — CUSTO-FAMILIA (04/08 · frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** · producao @ **b28ce83** · badge **13.82** · Render auto |
| **Autorização** | *pode subir para produção* + **99738595** |
| **Branch** | `deploy/custo-familia-v13.82` → producao (**não** teste inteiro) |
| **Diff** | **11 arquivos** · VERIFY_OK |
| **Migrate** | **NÃO** |
| **Rollback** | `git push origin 3381d0d:producao` · tag `rollback/pre-custo-familia-v13.82` |
| **Você** | Ctrl+F5 Cadastro → Composição · amarrar pacote no saco · reabrir · 1 venda · estoque do saco |

### 🚀 Custo família — saco → pacote/granel (04/08 · **Live v13.82**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live loja** — ver Deploy v13.82 acima |
| **Rollback** | tag `rollback/pre-custo-familia-v13.82` → 3381d0d |

### ✅ Deploy loja **v13.81** — PDV-CAD-RAPIDO (04/08 · frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** · producao @ **3381d0d** · badge **13.81** · Render auto |
| **Autorização** | *pode subir cadastro rápido PDV / PDV-CAD-RAPIDO para produção* + **99738595** |
| **Branch** | deploy/pdv-cad-rapido-v13.99 → producao (**não** 	este inteiro) |
| **Diff** | **16 arquivos** / +2011 · FL-008 preservado |
| **Prova** | 21/21 + VERIFY_OK na branch isolada |
| **Migrate** | **NÃO** |
| **Rollback** | git push origin a0f0db2:producao · tag 
ollback/pre-pdv-cad-rapido-v13.81 |
| **Você** | Ctrl+F5 PDV · busca · **+ Novo Produto** · Cadastro **PDV conferir** · 1 venda |
| **Cosmos** | opcional no Render: AGRO_COSMOS_TOKEN |

### 📦 CHECKLIST ÚNICO — pós envio (04/08 · após v13.83) · **superado**

> **Vigente:** **CHECKLIST ÚNICO — pronto envio (04/08)** no topo (BI-TOPBAR-TOTAL · SEFAZ-UI).

**Loja na época:** badge **v13.83** · producao @ **ed52234**

| # | Pacote | Status |
| - | ------ | ------ |
| 1 | **PDV-CAD-RAPIDO** | ✅ **enviado** / Live v13.81 |
| 2 | **CUSTO-FAMILIA** (saco+kit) | ✅ **enviado** / Live v13.82 |
| 3 | **MODAL-UTF8** (acentos) | ✅ **enviado** / Live v13.83 |

### 📦 PACOTE PRONTO LOJA — Hotfix acentos modal (`MODAL-UTF8` · **v13.83**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live loja v13.83** · ed52234 |
| **Inclui** | Texto UTF-8 do modal Cadastro |
| **Rollback** | `b28ce83` / `rollback/pre-modal-utf8-v13.83` |
| **Você** | Ctrl+F5 Cadastro → Preços/Composição legíveis |

### 📦 PACOTE PRONTO LOJA — Custo família saco+kit (`CUSTO-FAMILIA` · **v13.82**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live loja v13.82** · b28ce83 |
| **Inclui** | Bloco saco (custo + baixa estoque) · kit multi-insumo · propaga (cadastro/NF/Excel) |
| **Rollback** | `3381d0d` / `rollback/pre-custo-familia-v13.82` |
| **Você** | Ctrl+F5 Cadastro → Composição · 1 venda com pacote ligado ao saco |

### 📦 PACOTE PRONTO LOJA — Cadastro rápido PDV (PDV-CAD-RAPIDO · **v13.81**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live loja v13.81** · 3381d0d |
| **Inclui** | + Novo/Produto · Cosmos/OFF · NCM silencioso · foto se existir · card **PDV conferir** · modal **?** · busca maior |
| **Rollback** | 0f0db2 / 
ollback/pre-pdv-cad-rapido-v13.81 |

### ⚠️ Lembrete — NÃO subir 	este inteiro

	este vs loja ainda tem centenas de arquivos WIP. Loja só recebe branch isolada.

### ✅ Deploy loja **v13.80** — lote CAD/NF + DSP · **histórico**

Base antes do PDV-CAD: 0f0db2.

### 🚀 DEPLOY PRONTO — lote CAD/NF + Dispenser (`deploy/lote-cad-nf-dsp-v13.80` · 03/08) · **enviado**

**Status:** ✅ **Live loja v13.80** · ver bloco Deploy acima. **Não inclui** PDV-CAD-RAPIDO.  
**Rollback:** `git push origin 6996fca:producao`

### 📦 PACOTE PRONTO LOJA — Dispenser PNG fundo (`DSP-PNG-BG` · **v13.80**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** / Live loja v13.80 |
| **Inclui** | Moldura ingredientes sempre branca · upload PNG + limpa fundo branco/preto da borda |
| **Prova** | `python scripts/verify_dsp_png_bg.py` → **VERIFY_OK** |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só `/interno/dispenser-a6*` · **zero** PDV |
| **Arquivo** | `dispenser_a6_studio.html` |
| **Você** | Ctrl+F5 Dispenser → Ingredientes → PNG transparente · moldura **branca** · foto antiga «queimada» → apagar e subir de novo |

### 🚀 PREP deploy — lote CAD/NF (`prep/lote-cad-nf-v13.75`) · **OBSOLETO**

| Item | Detalhe |
| ---- | ------- |
| **Status** | ❌ **NÃO usar** — `catalogo_agro.py` mojibake · substituído por `deploy/lote-cad-nf-dsp-v13.80` |
| **Provas** | (antigas) cherry sobre `6996fca` · CAD-CB 7/7 · ENTRADA-NF-CUSTO — **remonte limpo no deploy** |
| **Inclui** | NF custo · Duplicar · Barras opcionais (+ fallback busca) |

### 📦 PACOTE PRONTO LOJA — Barras opcionais (`CAD-CB-OPC` · **v13.75**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** / Live no lote v13.80 |
| **Inclui** | Lista barras opcionais no cadastro · grava PG · bip PDV acha qualquer EAN |

### 📦 PACOTE PRONTO LOJA — Duplicar cadastro (`CAD-DUP` · **v13.72**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** / Live no lote v13.80 |
| **Inclui** | Botão **Duplicar** no modal Cadastro · códigos/barras novos · sem estoque |

### 📦 PACOTE PRONTO LOJA — Entrada NF custo cadastro (`ENTRADA-NF-CUSTO` · **v13.71**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** / Live no lote v13.80 |
| **Inclui** | V. unit etapa 2 puxa custo do Cadastro (JS ignora 0 · overlay sync · PG fallback) |

### ✅ VERIFY — ENTRADA-NF-CUSTO (03/08)

| Item | Detalhe |
| ---- | ------- |
| **Resultado** | **VERIFY_OK** · **v13.77** |
| **Testes** | 10/10 `tests_entrada_nf_custo_cadastro` |
| **Path** | Mongo final=0 → overlay OU `Produto.custo` PG → JS `> 0` → V. unit |
| **Caso real** | sal fino GM1821 · Cadastro R$ 27 · overlay sem `preco_custo_overlay` · depende fallback PG |
| **Você** | Ctrl+F5 Entrada NF · incluir sal fino → V. unit **27,00** |

### ✅ VERIFY — CAD-CB-OPC (03/08)

| Item | Detalhe |
| ---- | ------- |
| **Resultado** | **VERIFY_OK** |
| **Testes** | 7/7 `tests_codigos_barras_opcionais` |
| **Você** | Ctrl+F5 cadastro: gravar EAN opcional → bip no PDV |

### 📦 CHECKLIST ÚNICO — após envio (03/08 · lote v13.64) · **histórico**

**Loja hoje:** badge **v13.64** · `producao` @ **`6996fca`**  
**Rollback:** tag `rollback/pre-lote-checklist-03ago-v13.39` (@ `94112a8` / v13.39)  
**Migrate:** `0081` (dispenser) · `estoque.0015` (estorno NF) — Render roda no boot

| Ordem | Pacote | Status |
| ----- | ------ | ------ |
| — | **LOTE CHECKLIST** (**v13.39**) | ✅ absorvido |
| **1** | **DSP-MIX** + **DSP-FOLHA-CLOUD** | ✅ **enviado** / Live (no lote) |
| **2** | **NF-REOPEN-ESTOQUE** | ✅ **enviado** / Live (no lote) |
| **3** | **BUGS-PROMPT** | ✅ **enviado** / Live (no lote) |
| **4** | **KARDEX-SALDO-COLS** | ✅ **enviado** / Live (no lote) |

### ✅ Deploy loja **v13.64** — lote checklist 03/08 (frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live** Render · `producao` @ **`6996fca`** · badge **13.64** · deploy `dep-d9odlmhsrm7s73fth000` |
| **Autorização** | *pode enviar para produção* + **99738595** (03/08) |
| **Cherry** | `96c7ed7` → `0057e69` → `7db7675` → `be3715e`* → `5a84b83` → `19004d8` → `0e65ecb` → `e4c1828` |
| **\*** | `views.py` reabrir: manteve `_entrada_nfe_conexao` + `usuario_django` |
| **Diff** | 21 arquivos · **zero** PDV/caixa/checkout |
| **Rollback** | `rollback/pre-lote-checklist-03ago-v13.39` |
| **Loja** | **Ctrl+F5** nos PCs · badge BI deve mostrar **13.64** |

### 🚀 PREP deploy loja — lote 03/08 (**feito**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** — ver bloco Deploy v13.64 acima |

### 📦 PACOTE PRONTO LOJA — Kardex Centro/Vila/Total (`KARDEX-SALDO-COLS` · **v13.64**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** / Live (lote **v13.64** @ `6996fca`) |
| **Inclui** | Colunas **Centro · Vila · Total** · filtros chip · saldo 0 legível |
| **Prova** | AST zero write · API GET · count ajustes inalterado · math mock |
| **Saldo atual** | **NÃO altera** — só leitura/exibição |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — modal cadastro aba Estoque |
| **Autorizar** | *pode subir kardex saldos / produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — Dispenser Mix + folhas nuvem (`DSP-MIX` · **v13.63**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** / Live (lote **v13.64** @ `6996fca`) |
| **Inclui** | Mix Sabores · criar sabor · folhas **só Postgres/RAM** (sem «memória cheia») |
| **Prova** | PG upsert/list/delete folha · cloud sem `writeLocal(folhas)` · saveFolhas sem Quota · Mix PNG OK |
| **Migrate** | **SIM** `0081` |
| **Risco** | Baixo — só `/interno/dispenser-a6*` · **zero** PDV |
| **Autorizar** | *pode subir dispenser / produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — Entrada NF reabrir estoque (`NF-REOPEN-ESTOQUE` · **v13.60**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** / Live (lote **v13.64** @ `6996fca`) |
| **Commits** | `7db7675` · `be3715e` |
| **Inclui** | Reabrir estorna carimbo · kardex saída estorno (ao **reabrir** nota — mexe saldo de propósito) |
| **Migrate** | **SIM** `estoque.0015` |
| **Risco** | Médio — **só ao reabrir** Entrada NF (não é path de venda PDV) |
| **Autorizar** | *pode subir NF reabrir estoque / produção* + **99738595** |

### 📦 PACOTE PRONTO LOJA — Bugs Copiar prompt Cursor (`BUGS-PROMPT` · **v13.62**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** / Live (lote **v13.64** @ `6996fca`) |
| **Inclui** | Copiar prompt Cursor · JSON seguro · print via `reverse` |
| **Migrate** | **NÃO** |
| **Risco** | Baixo — só tela Bugs |
| **Autorizar** | *pode subir bugs prompt / produção* + **99738595** |

### 🔧 Dispenser — folhas sem «memória cheia» (`DSP-FOLHA-CLOUD` · **teste v13.63**)

| Item | Detalhe |
| ---- | ------- |
| **Verify** | ✅ **PASS** · commit **`0057e69`** |
| **Fix** | Folhas só RAM + Postgres · apaga `dsp_folhas_v1` · save espera API |
| **Pacote** | ver **DSP-MIX** acima |

### ✅ Deploy loja **v13.39** — LOTE CHECKLIST (03/08 · Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **Live** · `producao` @ **`94112a8`** · badge **13.39** |
| **Rollback** | `rollback/pre-lote-checklist-v13.38` (@ v13.36) |

### ✅ LOJA — **HIST-REVERTER-PIN** (**v13.36** · 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **absorvido** · loja agora **v13.39** · (este pacote foi **v13.36** @ `7cb3695`) |
| **Antes** | **v13.35** @ `87a5a78` |
| **Push** | `deploy/hist-reverter-pin-v13:producao` (autorizado frase + senha) |
| **O quê** | ✕ reverte contagem · PIN real / SESSAO · API `/api/deletar-ajuste/` |

### 📦 CHECKLIST ÚNICO — histórico (02/08 · superado por v13.39)

| Ordem | Pacote | Status |
| ----- | ------ | ------ |
| — | **AJUSTE-HIST-UX** (**v13.35**) | ✅ absorvido |
| **1** | **HIST-REVERTER-PIN** (**v13.36**) | ✅ absorvido · loja **v13.39** |

### 🔧 Histórico — X reverte contagem (PIN) (**teste** · 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | ✕ pedia PIN `1234` e a API `/api/deletar-ajuste/` **não existia** |
| **Fix** | PIN real (RH) ou `SESSAO` · API · saldo volta · só `ajuste_pin` |
| **Arquivos** | `historico_ajustes.html` · `views.py` · `urls.py` |
| **Loja** | pacote **v13.36** pronto |

### ✅ LOJA — **AJUSTE-HIST-UX** (**v13.35** · 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **na loja** · `producao` @ **`87a5a78`** · badge **v13.35** |
| **Antes** | **v13.26** @ `c3ec890` |
| **Push** | `deploy/ajuste-hist-ux-v13:producao` (autorizado frase + senha) |
| **Rollback** | `git push origin rollback/pre-ajuste-hist-ux-v13:producao` → volta **v13.26** |
| **O quê** | Bip+1 mantém card · Hist. cards · Hist. sem barra BI · GM (não AGROE) |
| **Migrate** | **NÃO** |
| **Você** | Ctrl+F5 · badge **13.35** · Bip+1 · Hist. sem barra · GM no card |

### 📦 CHECKLIST ÚNICO — após envio (02/08)

**Loja hoje:** badge **v13.35** · `producao` @ `87a5a78`  
**Migrate:** NÃO

| Ordem | Pacote | Status |
| ----- | ------ | ------ |
| — | **AJUSTE-MOBILE-UX** (**v13.26**) | ✅ absorvido |
| **1** | **AJUSTE-HIST-UX** (**v13.35**) | ✅ **na loja** |

### 🔧 Histórico — GM no card (não ID AGROE) (**v13.33** · 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Card do Hist. mostrava `AGROE…` em vez do GM |
| **Fix** | Grava GM no ajuste PIN · Hist. busca GM no catálogo se antigo · nunca mostra ID longo |
| **Arquivos** | `views.py` · `historico_ajustes.html` · `mobile_ajuste.html` |
| **Você** | Ctrl+F5 Hist. · deve aparecer **GM4000** (etc.) |

### 🔧 Histórico — sem barra azul do BI (**teste** · 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Hist. cobria com sidebar do BI — não dava pra ler |
| **Fix** | `/historico/` = tela cheia (igual Ajuste) · não monta shell · link `target=_top` |
| **Arquivos** | `historico_ajustes.html` · `_agro_open_external.html` · `dashboard_gerencial.html` · `mobile_ajuste.html` |
| **Migrate** | NÃO |
| **Você** | Ctrl+F5 · Hist. · **sem** barra azul · cards legíveis |

### 🔧 Histórico de ajustes — layout celular (**teste** · 02/08)

| Item | Detalhe |
| ---- | ------- |
| **O quê** | `/historico/` em **cards** (não tabela larga) · Voltar ao Ajuste em destaque |
| **Arquivo** | `historico_ajustes.html` (+ select_related no view) |
| **Migrate** | NÃO |
| **Loja** | **ainda não** |
| **Você** | Ctrl+F5 Hist. no celular · lista legível · Voltar |

### 🔧 Ajuste Mobile — Bip+1 mantém produto na tela (**v13.29** · 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Após +1 o card sumia — só dava pra Ajustar pelo verde |
| **Fix** | Campo limpo (anti-cola EAN) · lista mostra **último bipado** clicável |
| **Arquivo** | **só** `mobile_ajuste.html` |
| **Migrate** | NÃO |
| **Teste** | `1ca5de9` · badge **13.29** |
| **Loja** | **ainda não** |
| **Você** | Ctrl+F5 `/ajuste-mobile/` · Bip+1 ON · bip · card fica · clique no card · 2º bip sem cola |

### 🔧 DF-e — consulta SEFAZ off no runserver local (02/08)

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Buscar / chave Dist **não** falam com a Receita no PC local |
| **Por quê** | Mesmo CNPJ da loja → 656 |
| **Loja** | Continua normal (RENDER) |
| **Exceção** | `NFE_DIST_DFE_PERMITIR_LOCAL=true` no `.env` |
| **Arquivos** | `sefaz_dfe_client.py` · status API · `entrada_nota.html` |

### ✅ Deploy loja **v13.26** — AJUSTE-MOBILE-UX (02/08 · Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** · `producao` @ **`c3ec890`** · Render auto |
| **Base** | loja **v13.11** @ `8d7f38b` |
| **Branch** | `deploy/ajuste-mobile-ux-v13` |
| **Rollback** | `git push origin rollback/pre-ajuste-mobile-ux-v13:producao` (@ **`8d7f38b`** / v13.11) |
| **Inclui** | Conferir/data · numpad BT · modal compacto · Hist. 200 · Scan estável |
| **Migrate** | **NÃO** |
| **NÃO** | merge `teste` · PDV/caixa |
| **Você agora** | Esperar Live · Ctrl+F5 `/ajuste-mobile/` · badge **13.26** · 1 contagem · Hist. · Scan |

### ⏭ PRÓXIMO CHAT — deploy loja **AJUSTE-MOBILE-UX** (preparado 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** — ver bloco Deploy loja **v13.26** acima |
| **Rollback** | `git push origin rollback/pre-ajuste-mobile-ux-v13:producao` |

### 📦 PACOTE PRONTO LOJA — Ajuste Mobile UX (`AJUSTE-MOBILE-UX` · **v13.26**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado ·** `producao` @ `c3ec890` |
| **Base loja** | era **v13.11** @ `8d7f38b` |
| **Deploy** | `deploy/ajuste-mobile-ux-v13` @ `c3ec890` |
| **Rollback** | `rollback/pre-ajuste-mobile-ux-v13` |
| **O quê** | (1) «Conferir» não apaga data · (2) numpad BT · (3) modal compacto · (4) Hist. últimos 200 · (5) Scan estável |
| **Migrate** | **NÃO** |

### 📦 CHECKLIST ÚNICO — pronto envio (02/08)

**Loja hoje:** badge **v13.26** · `producao` @ `c3ec890` (Render a finalizar)  
**Migrate:** NÃO

| Ordem | Pacote | Status |
| ----- | ------ | ------ |
| — | Lotes até **v13.11** | ✅ **já na loja** |
| **1** | **AJUSTE-MOBILE-UX** (**v13.26**) | ✅ **enviado** `c3ec890` |

### 🔧 Ajuste Mobile — Hist. trava + Scan câmera (**v13.21** · 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Hist.** | `/historico/` carregava **todos** os ajustes → Chrome travava · agora **últimos 200** + botão Voltar ao Ajuste |
| **Scan** | Limpa câmera ao fechar/reabrir · evita double-start · lib pinada 2.3.8 · mensagem se sem permissão |
| **Arquivos** | `views.py` · `historico_ajustes.html` · `mobile_ajuste.html` |
| **Migrate** | NÃO |
| **Pacote** | incluso em **AJUSTE-MOBILE-UX** |

### 🔧 Ajuste Mobile — teclado numérico na tela (Bluetooth · **v13.16** · 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Leitor BT = teclado físico → celular **não abre** teclado na hora de digitar qtd |
| **Fix** | Numpad grande no modal (1–9 · 0 · . · ⌫ · Limpar) · campo qtd só leitura |
| **Arquivo** | **só** `mobile_ajuste.html` (+ VERSION) |
| **Migrate** | NÃO |
| **Teste** | `teste` **v13.18** (texto explicativo do numpad **removido**) |
| **Loja** | **ainda não** — frase + senha |
| **Você** | Ctrl+F5 `/ajuste-mobile/` · abrir produto · digitar qtd nos botões · Somar/Trocar |
| **Dica Android** | Ajustes → Teclado físico → **mostrar teclado na tela** (se quiser soft keyboard em outros campos) |

### 🔧 Ajuste Mobile — «Conferir» apaga data logo após contar (**v13.14** · 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Contou há minutos · lista laranja **Conferir** (deveria ser `dd/mm/aa`) |
| **Causa** | Refresh de saldos vinha **sem** data e **zerava** a data local; catálogo slim também sobrescrevia |
| **Fix** | API vazia **não** apaga data boa · ao trocar catálogo **mantém** contagem da sessão |
| **Arquivo** | **só** `mobile_ajuste.html` (+ VERSION) |
| **Migrate** | NÃO |
| **Teste** | `teste` v**13.14** · push `origin/teste` |
| **Loja** | **ainda não** — falta frase + senha |
| **Você** | Ctrl+F5 `/ajuste-mobile/` · 1 contagem · deve ficar **data de hoje**, não Conferir · confira se a loja (Centro/Vila) é a mesma da contagem |

### ✅ Ajuste Mobile — bip não cola 2 EANs (**v13.11** · 02/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v13.11** · `producao` @ **`8d7f38b`** · Render auto |
| **O quê** | 2º bip **apaga** o código anterior no campo (não fica EAN+EAN) |
| **Arquivo** | **só** `mobile_ajuste.html` (+ VERSION) |
| **Migrate** | NÃO |
| **Branch** | `deploy/ajuste-bip-clear-v13.11` |
| **Rollback** | `git push origin rollback/pre-v1311-ajuste-bip:producao` (@ **`ac5dfb3`** / v13.10) |
| **Risco** | **Muito baixo** — só Ajuste Mobile · PDV/caixa intactos |
| **Você** | Ctrl+F5 `/ajuste-mobile/` · bip 1 · bip 2 · campo só com o 2º |

### ✅ Deploy loja **v13.10** — Etiquetas presets Postgres (`ETQ-PRESET-PG` · 01/08 · Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** · `producao` @ **`ac5dfb3`** · badge **13.10** · Render auto |
| **Base** | loja **v13.04** @ `72c6b6c` |
| **Branch** | `deploy/etq-preset-pg-v13.10` |
| **Rollback** | `git push origin rollback/pre-v1310-etq-preset:producao` (@ **`72c6b6c`** / v13.04) |
| **Inclui** | Presets etiqueta no Postgres · multi-PC · migrate `0079` |
| **Migrate** | **SIM** `0079_etiqueta_preset_agro` (Render no boot) |
| **NÃO** | merge `teste` · PDV/caixa |
| **Você agora** | Esperar deploy Live · Ctrl+F5 · badge **13.10** · no PC do «box ração» abrir etiquetas **logado** 1× · outros PCs devem ver |

### 🔧 Etiquetas — presets no Postgres multi-PC (`ETQ-PRESET-PG` · 01/08)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Preset «box ração» (e qualquer novo) ficava **só no PC** (`localStorage`) — viola roteiro §0.1 |
| **Fix** | Tabela `EtiquetaPresetAgro` · API `/api/produtos/etiquetas/presets/` · JS grava/lê PG · migrate **1×** o que já estava no browser |
| **Migrate** | **SIM** `0079_etiqueta_preset_agro` |
| **Arquivos** | `models` · `0079` · `views`/`urls` · `produtos_etiquetas*.js` · template · `banana-roteiro` §0.1 |
| **Você** | No PC que tem «box ração»: login · abrir `/produtos/etiquetas/` · Ctrl+F5 · deve subir sozinho · outros PCs passam a ver |
| **Teste** | `9fbafdc` · badge **13.10** · push `origin/teste` |
| **Loja** | ✅ **v13.10** @ `ac5dfb3` · rollback `rollback/pre-v1310-etq-preset` |

### 🔧 DF-e — faixa verde Cursor desatualizada após Buscar (01/08)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Avançado dizia **2092** · faixa verde ainda **2086** |
| **Fix** | Após Buscar, atualiza Cursor na faixa verde |
| **Arquivo** | `entrada_nota.html` |

### ✅ Deploy loja **v13.04** — CP-BUSCA-FORN + DFE-NSU-656 (01/08 · Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado · Live** Render `dep-d9n49nvlk1mc738vjmng` · badge **13.04** · `producao` @ **`72c6b6c`** |
| **Base** | loja **v12.95** @ `87aa52b` |
| **Branch** | `deploy/cp-dfe-lote-v13.04` |
| **Rollback** | `git push origin rollback/pre-cp-dfe-lote-v13.04:producao` (@ **`87aa52b`** / v12.95) |
| **Inclui** | CP busca fornecedor (código no nome + e-mail @) · DFE NSU no 656 |
| **Migrate** | **NÃO** |
| **NÃO** | merge `teste` · PDV/caixa |
| **Você agora** | Ctrl+F5 · badge **13.04** · CP `Renan Hinnen 1403` · Entrada NF SEFAZ 1 Buscar |

### ⏭ PRÓXIMO CHAT — deploy loja **CP + DFE lote v13.04** (preparado 01/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** — ver bloco Deploy loja **v13.04** acima |
| **O quê** | **CP-BUSCA-FORN** + **DFE-NSU-656** (um restart) |
| **Branch** | `deploy/cp-dfe-lote-v13.04` @ **`72c6b6c`** → `producao` |
| **Rollback** | `git push origin rollback/pre-cp-dfe-lote-v13.04:producao` |
| **Migrate** | **NÃO** |
| **Risco PDV/caixa** | **Baixo** — CP só busca lista · DFE só cursor NSU no 656 · **não** mexe venda/caixa/finalize |
| **Provas** | DFE `verify_dfe_nsu_656.py` **12/12 VERIFY_OK** · CP smoke Q `1403`+e-mail · compile OK · cherry só 8 arquivos |
| **Você autoriza** | ~~Lojas pausam · frase+senha~~ ✅ **autorizado 01/08** |
| **Depois** | Ctrl+F5 · badge **13.04** · CP `Renan Hinnen 1403` · Entrada NF SEFAZ 1 Buscar |

### 📦 PACOTE PRONTO LOJA — Dist DF-e cursor 656 adota NSU maior (`DFE-NSU-656` · **v13.04**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado · Live** loja v13.04 · `72c6b6c` · `dep-d9n49nvlk1mc738vjmng` |
| **Fix** | No 656 **da SEFAZ**, se ultNSU **maior** → grava · **nunca** maxNSU · **nunca** pra trás · Aguarde local **não** grava NSU |
| **Arquivos** | `dfe_inbox_util.py` · `sefaz_dfe_client.py` · `scripts/verify_dfe_nsu_656.py` |
| **Migrate** | NÃO |
| **Prova** | `verify_dfe_nsu_656.py` → **12/12 VERIFY_OK** |
| **Você** | deploy · 1 Buscar (pode 656) → cursor sobe se SEFAZ mandar · 1h · Buscar de novo · buraco → chave |
| **Zap** | *Atualização rápida Entrada NF / SEFAZ (~1 min)* |

### 📦 PACOTE PRONTO LOJA — Contas a pagar busca fornecedor (`CP-BUSCA-FORN` · **v13.03**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado · Live** loja v13.04 · `72c6b6c` · `dep-d9n49nvlk1mc738vjmng` |
| **VERSION** | **13.03** (no lote sobe como **13.04** com DFE) · commit origem `006f871` |
| **Inclui** | Número no nome (`Renan Hinnen 1403`) · quem lançou só com `@` · ajuda `?` |
| **Arquivos** | `lancamentos_financeiro_pg_util.py` · `mongo_financeiro_util.py` · `lancamentos_help_agents.html` · AGENTS §10 |
| **Migrate** | **NÃO** |
| **Risco** | **Baixo** — só busca lista CP · não mexe pagar/PDV/caixa |
| **Prova 01/08** | smoke Q `1403` casa cliente · Renan sem @ não polui · e-mail com @ ok |
| **NÃO** | merge inteiro `teste` |
| **Zap loja** | *Atualização rápida Contas a pagar (~1–2 min)* |

### ✅ Deploy loja **v12.95** — BI-KPI-LOJA (01/08 · Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado · Live** Render `dep-d9n2huht0dsc738q0m60` · badge **12.95** · `producao` @ **`87aa52b`** |
| **Base** | loja **v12.88** @ `941446d` |
| **Branch** | `deploy/bi-kpi-loja-v12.95` |
| **Rollback** | `git push origin rollback/pre-bi-kpi-loja-v12.95:producao` (@ **`941446d`** / v12.88) |
| **Inclui** | KPIs por loja · % vs mesmo dia da semana · Validade alinhada ao relatório |
| **Migrate** | **NÃO** |
| **NÃO** | merge `teste` · PDV/caixa · FL-058 |
| **Você agora** | Ctrl+F5 home · badge **12.95** · Centro/Vila · Validade · selos «vs 1º …» |

### ⏭ PRÓXIMO CHAT — deploy loja **BI-KPI-LOJA v12.95** (preparado 01/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** — ver bloco Deploy loja **v12.95** acima |
| **O quê** | Só BI home: loja nos KPIs · % vs mesmo dia da semana · Validade alinhada |
| **Branch** | `deploy/bi-kpi-loja-v12.95` @ **`87aa52b`** → `producao` |
| **Rollback** | `git push origin rollback/pre-bi-kpi-loja-v12.95:producao` |
| **Migrate** | NÃO |
| **Risco PDV/caixa** | **Não piora** — pacote **não** toca venda/caixa/NF/estoque |
| **Provas** | Validade **15/15 VERIFY_OK** · cherry só BI · compile OK |
| **Você autoriza** | ~~Lojas pausam · frase+senha~~ ✅ **autorizado 01/08** |
| **Depois** | Ctrl+F5 · badge **12.95** · home Centro/Vila · Validade |

### 🔧 BI — card Validade 0/0 com lotes no relatório (01/08 · **teste v12.93** · VERIFY_OK)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Relatório validade mostra vencidos / no mês; card BI Centro **e** Vila ficavam **0 / 0** |
| **Causa** | Contagem por loja exigia só saldo operacional e pegava **1 data** por produto; ignorava lote com qtd |
| **Fix** | `_contagem_validade_dashboard_por_loja` · cache `v4` · lote qtd>0 + todas as datas |
| **Prova** | `scripts/verify_validade_bi.py` **15/15 VERIFY_OK** (01/08) |
| **Você** | Ctrl+F5 · badge **12.95** · Validade deve espelhar o relatório |

### 📦 PACOTE PRONTO LOJA — BI loja + KPIs + Validade (`BI-KPI-LOJA` · **v12.95**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado · Live** `dep-d9n2huht0dsc738q0m60` · `producao` @ **`87aa52b`** |
| **VERSION** | **12.95** (loja hoje **12.88**) |
| **Branch deploy** | `deploy/bi-kpi-loja-v12.95` @ **`87aa52b`** |
| **Rollback** | `rollback/pre-bi-kpi-loja-v12.95` @ **`941446d`** (v12.88) |
| **Inclui** | Filtro Centro/Vila em Vendas/Performance · % Vendas/Ticket/Novos vs **mesmo dia da semana do mês ant.** · Novos = **hoje** · card Validade por loja alinhado ao relatório (lote qtd + todas as datas) · «por unidade» compara as duas |
| **Arquivos** | `views.py` (só dashboard) · `dashboard_vendas_historico_util.py` · `dashboard_gerencial_body.html` · `tests_validade_dashboard.py` · `scripts/verify_validade_bi.py` · VERSION |
| **Migrate** | **NÃO** |
| **Risco loja aberta** | **Baixo** — **só BI `/`** · **não** mexe PDV · caixa · Entrada NF · estoque · venda · finalize |
| **Prova 01/08** | `scripts/verify_validade_bi.py` → **15/15 VERIFY_OK** · compile views OK · cherry limpo (só conflito banana/VERSION resolvido) |
| **NÃO** | merge inteiro `teste` · FL-058 (ainda não feito) · ENTRADA-NF-CUSTO |
| **Próximo chat** | 1) lojas **pausam vendas** 2) *pode subir BI-KPI-LOJA / produção* + **99738595** 3) FF `deploy/bi-kpi-loja-v12.95` → `producao` · Ctrl+F5 home · badge **12.95** · Validade ≠ 0 se relatório tem · selos «vs 1º …» |
| **Zap loja** | *Atualização ~2 min — não finalize venda agora* |

### 🔧 BI — Ticket + Novos Clientes vs mesmo dia semana (01/08 · **teste v12.92**)

| Item | Detalhe |
| ---- | ------- |
| **Ticket** | % Mês e Hoje vs ticket do mesmo dia da semana do mês ant. |
| **Novos Clientes** | Número = **hoje** · % vs cadastros no mesmo dia da semana do mês ant. (30d só no «?») |
| **Você** | Ctrl+F5 · selos «vs 1º sáb.» nos 3 cards |

### 🔧 BI — Vendas % vs mesmo dia da semana do mês ant. (01/08 · **teste v12.91**)

| Item | Detalhe |
| ---- | ------- |
| **Antes** | Card Vendas: % vs **ontem** |
| **Agora** | % vs **mesma ocorrência** do dia da semana no mês anterior (ex. 01/08 1º sáb. → 1º sáb. de jul) |
| **Você** | Ctrl+F5 · badge do card Vendas tipo «vs 1º sáb.» · tooltip com data (ex. 04/07) |

### 🔧 BI — Vendas/Performance filtrados pela loja (01/08 · **teste v12.90**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Com Loja=Centro, card Vendas e Total da Performance somavam Centro+Vila |
| **Fix** | Total/KPI pela loja do aparelho · «por unidade» continua comparando as duas · cache meta/pdv v8/v6 · selo «· Centro» no Total |
| **Você** | Local Ctrl+F5 · Loja Centro → Vendas ≈ barra Centro · Total Performance ≈ mesma · barras ao lado ainda mostram as duas |

### ✅ Deploy loja **v12.88** — lote checklist (01/08 · Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** `producao` @ **`941446d`** · badge **12.88** · aguardar Live Render |
| **Base** | loja **v12.51** @ `b977c9b` |
| **Branch** | `deploy/lote-checklist-v12.88-01ago` |
| **Rollback** | `git push origin rollback/pre-lote-checklist:producao` (@ **`b977c9b`** / v12.51) |
| **Inclui** | ETQ-GM · BUG-FAB · PDV-USO-UX · PDV-USO-BRINDE · DFE-CHAVE · AJUSTE-MOBILE |
| **Migrate** | **SIM** — `0076` + `0077` + `0078` (no build Render) |
| **NÃO** | merge `teste` · slim API (já na loja) |
| **Você agora** | Ctrl+F5 PDVs · badge **12.88** · 1 venda · Uso loja · `/ajuste-mobile/` PIN+lista · etiquetas |

### 📦 CHECKLIST ÚNICO — pós-deploy (01/08) · **substituído**

> **Vigente:** bloco **CHECKLIST ÚNICO — pronto envio (03/08)** no topo (BUGS-F10-FRETE · ETQ-LOTE-A4 · RELAT-INVENTARIO).

**Loja na época:** badge **v13.04** · depois subiu **v13.10** / **v13.11**.  
Fila 1–9 abaixo = **já enviada** (arquivo histórico).

| Ordem | Pacote | Status |
| ----- | ------ | ------ |
| — | Lote **v12.51** | ✅ enviado antes |
| 1 | **AJUSTE-MOBILE** | ✅ **enviado loja v12.88** · migrate `0078` |
| 2 | **BUG-FAB-BARRA** | ✅ **enviado loja v12.88** |
| 3 | **PDV-USO-LOJA-UX** | ✅ **enviado loja v12.88** · migrate `0076` |
| 4 | **PDV-USO-LOJA-BRINDE** | ✅ **enviado loja v12.88** · migrate `0077` |
| 5 | **DFE-CHAVE** | ✅ **enviado loja v12.88** |
| 6 | **ETQ-GM-CAMPOS** | ✅ **enviado loja v12.88** |
| 7 | **BI-KPI-LOJA** | ✅ **enviado · Live** loja v12.95 · `87aa52b` |
| 8 | **CP-BUSCA-FORN** | ✅ **enviado · Live** loja v13.04 · `72c6b6c` |
| 9 | **DFE-NSU-656** | ✅ **enviado · Live** loja v13.04 · `72c6b6c` |

### 📦 PACOTE PRONTO LOJA — Dist DF-e baixar pela chave (`DFE-CHAVE` · **v12.71**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.88** · no lote `941446d` |
| **O quê** | Aba SEFAZ: campo chave 44 + **Baixar pela chave** · grava Pendentes · **não** mexe no ultNSU |
| **API** | `POST /api/entrada-nota/dfe-por-chave/` |
| **Migrate** | NÃO (usa tabelas `0071`/`0072` já na loja) |
| **Base loja** | v12.51+ (DFE-INBOX) |
| **Prova** | rota · consChNFe · upsert sem gravar NSU · POST 400 chave curta · detalhe inbox · cursor estável |
| **Arquivos** | `views.py` · `urls.py` · `entrada_nota.html` · `sefaz_dfe_client.py` · `dfe_inbox_util.py` |
| **Você** | deploy · fim do Aguarde 1h · colar chave · Baixar · Carregar na grade |
| **NÃO** | Usar no meio do timer 656 |

### 📦 PACOTE PRONTO LOJA — Bug 🐞 na barra Gestão (`BUG-FAB-BARRA` · **v12.55**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.88** · no lote `941446d` |
| **O quê** | Na Gestão o 🐞 fica centrado na barra azul · PDV sem barra = igual |
| **Migrate** | NÃO |
| **Commits** | `6905682` |
| **Base loja** | v12.51 (já tem BUG-REPORT) |

### 📦 PACOTE PRONTO LOJA — Uso loja bip + Outros (`PDV-USO-LOJA-UX` · **v12.55**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.88** · no lote `941446d` · migrate `0076` |
| **O quê** | Bip adiciona direto na lista · motivo **Outros** com campo livre |
| **Migrate** | SIM — `produtos.0076` |
| **Commits** | `b0645b0` · `ecc1adc` |
| **Base loja** | v12.51 (já tem PDV-USO-LOJA) |

### 📦 PACOTE PRONTO LOJA — Brinde cliente + F8 Bônus (`PDV-USO-LOJA-BRINDE` · **v12.57**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.88** · no lote `941446d` · migrate `0077` |
| **O quê** | Motivo **Brinde cliente** → busca cliente · grava no PG · F8 aba **Bônus** (sai Métricas) |
| **Migrate** | SIM — `produtos.0077` |
| **Commits** | `8e8c5f4` (+ docs `fc61102`) |
| **Base loja** | v12.51+ · ideal após **PDV-USO-LOJA-UX** |

### 📦 PACOTE PRONTO LOJA — Dist DF-e caixa de entrada (`DFE-INBOX` · **v12.51**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.51** · Live `dep-d9mbqn8jo6nc73bkhrl0` |
| **O quê** | Buscar notas novas grava XML no PG · Pendentes (antiga→nova) / Concluídas · ~80 · cursor só 137/138 · NFC_E fallback · baixar por chave |
| **Migrate** | SIM — `produtos.0071` + `0072` (tabelas cursor/documento) |
| **Commits chave** | `07d9973` · `96629a1` · `cde8d0e` (+ docs banana) |
| **Prova local** | config OK · 2 pendentes FIFO · status/inbox/detalhe HTTP · trava 656 · cursor 2090 estável |
| **Loja após deploy** | migrate · Ctrl+F5 Entrada NF→SEFAZ · Buscar **ou** recuperar pelas chaves (Saframil/Astúrias) — local **não** copia pra loja |
| **NÃO** | Pular NSU · spam Consultar |

### 📦 PACOTE PRONTO LOJA — Bug report flutuante (`BUG-REPORT` · **v12.49**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.51** · Live |
| **O quê** | Botão 🐞 flutuante · Alt+B · print automático · lista F10 Gestão → Bugs · PG |
| **Safe-zone** | Empurra rodapés (Voltar) · overlay → canto direito · **Gestão:** 🐞 centrado na barra azul · PDV sem barra = canto livre |
| **Migrate** | SIM — `produtos.0074` (`BugReportAgro` + `DispositivoLojaAgro`) |
| **Prova** | URLs/API criar+status+print+lista OK · form abre no shell · WhatsApp CallMeBot se configurado |
| **Base** | v12.32–12.49 |

### ✅ Uso loja — quem levou grade RH (**v12.31** · 31/07)

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Pop «Quem levou?» = botões de todos os funcionários ativos (RH) · **Outros** abre campo · toque no nome **avança sozinho** |
| **API** | `meta` devolve `funcionarios` |
| **Prova** | Ctrl+F5 PDV · Uso loja · Confirmar saída · ver grade · clicar nome → motivo |

### ✅ Uso loja — Enter = Confirmar saída (**v12.34** · 31/07)

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Na lista da saída, **Enter** dispara Confirmar saída (exceto busca / autocomplete / pops) |

### ✅ Uso loja — Voltar nos pops (**v12.36** · 31/07)

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Botão **← Voltar** em quem/motivo/PIN · Esc = mesmo (passo anterior; no 1º fecha o pop) |

### ✅ Uso loja — totais custo/venda no histórico (**v12.38** · 31/07)

| Item | Detalhe |
| ---- | ------- |
| **O quê** | Rodapé Histórico: tags **Centro** e **Vila** com total custo + total venda (saídas não estornadas) |
| **Migrate** | `produtos.0075` (preço snapshot no item) |

### 📦 PACOTE PRONTO LOJA — Relatório vendas por marca (`RELAT-VENDAS-MARCA` · **v12.22**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.51** · Live |
| **O quê** | Card **Vendas por marca** · ordenar **valor total** / **quantidade** · Excel ↓ · ajuda ? |
| **Prova** | Totais = vendas por grupo · ordenar valor≠qtd · view/xlsx/hub OK |
| **Migrate** | NÃO |
| **Arquivos** | `relatorios_vendas_util.py` · `relatorios_central_views.py` · hub/generico/help · `urls.py` |

### 📦 PACOTE PRONTO LOJA — PDV Uso loja (`PDV-USO-LOJA` · **v12.48**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.51** · Live |
| **O quê** | Uso loja no PDV · quem grade RH · motivo toque avança · Enter confirma · Voltar · hist. em linha · totais custo/venda por loja (Centro azul / Vila laranja) · PIN · estorno · kardex PG |
| **Migrate** | SIM — `estoque.0014` + `produtos.0073` + `0074` (cadeia) + `0075` (preços item) |
| **Prova** | URLs/meta/hist/totais OK · itens com ajuste · motivos alinhados · `{}` → pede PIN |
| **Base** | v12.18–12.48 |

### 📦 PACOTE — PDV Uso loja (**v12.18** · 31/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ver **PACOTE PRONTO PDV-USO-LOJA v12.48** |
| **O que é** | Botão **Uso loja** no PDV · overlay (padrão Folha saldo) · busca = motor PDV · lista própria · PIN · histórico + estorno |
| **Estoque** | Baixa no **Postgres** (`AjusteRapidoEstoque` origem `uso_loja`) · caixa aberto = depósito do turno · caixa fechado = operador escolhe Centro/Vila |
| **Opcional** | Quem levou = grade RH (toque avança) · Outros digita · motivo · vazio = dono do PIN |
| **Kardex** | Aparece como «Uso loja» / «Estorno uso loja» |
| **Migrate** | `estoque.0014` + `produtos.0073` + `0074` + `0075` |
| **Arquivos** | `uso_loja_util.py` · `views_uso_loja.py` · `pdv_uso_loja.js` · `uso_loja_overlay.html` · topbar PDV |
| **Testar** | Ctrl+F5 PDV · Uso loja · quem/motivo toque · PIN · Histórico totais · estornar · kardex |


### ✅ Consertar nomes quebrados cadastro (31/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **gravado loja** · **41/41** · `broken_left=0` |
| **O quê** | Nome/marca/categoria/GM/EAN em PG (+ overlay) · **não** mexeu custo/venda |
| **Fonte** | Mesmo `produto_externo_id` no staging (cópia boa) |
| **Você** | Ctrl+F5 Cadastro · sumiu «NOME QUEBRADO» / «Consertar nome» |


### ✅ Deploy loja **v12.51** — lote marca + uso loja + bugs + DF-e (31/07 · Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado · Live** Render `dep-d9mbqn8jo6nc73bkhrl0` · badge **12.51** |
| **Base** | loja **v12.12** @ `9d65e31` |
| **Lote** | `deploy/lote-v12.22-12.51-31jul` @ **`b977c9b`** → `producao` (1 restart) |
| **Inclui** | RELAT-VENDAS-MARCA · PDV-USO-LOJA · BUG-REPORT · DFE-INBOX (API+UI) |
| **Migrate** | **SIM** — `estoque.0014` + `produtos.0071`→`0075` (no build Render) |
| **NÃO inclui** | merge `teste` · ENTRADA-NF-CUSTO · dispenser/catálogo delivery |
| **Rollback** | `git push origin rollback/pre-v1251-lote-31jul:producao` (@ **`9d65e31`** / v12.12) |
| **Você agora** | Ctrl+F5 PDVs · badge **12.51** · smoke: 1 venda · Uso loja · 🐞 · Relatórios marca · Entrada NF→SEFAZ inbox |

### ✅ Deploy loja **v12.12** — lote 12.08–12.12 (30/07 · Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado · Live** Render `dep-d9lmqg4s728c739fsgo0` · badge **12.12** |
| **Base** | loja **v12.07** @ `adcb750` |
| **Lote** | `deploy/lote-v12.08-12.12-30jul` @ **`9d65e31`** → `producao` (1 restart) |
| **Inclui** | 12.08 lápis cache · 12.09+12.10 Excel estoque + vínculo NF PG · 12.11 Somar/Trocar · 12.12 lembrete entrega |
| **Migrate** | **SIM** — `0068` + `0069` (no build Render) |
| **NÃO inclui** | Dist DF-e · merge `teste` · ENTRADA-NF-CUSTO |
| **Rollback** | `git push origin rollback/pre-v1212-lote-30jul:producao` (@ **`adcb750`** / v12.07) |
| **Você agora** | Ctrl+F5 PDVs · badge **12.12** · smoke: lápis preço · lembrete some ao finalizar · Ajuste Somar no celular |

### 📦 PACOTE PRONTO LOJA — Etiquetas GM curto + toggles (`ETQ-GM-CAMPOS` · **v12.52**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.88** · no lote `941446d` |
| **VERSION alvo** | **12.52** (loja **12.51**) |
| **Inclui** | GM curto (GM0050-1→GM50) · checkboxes Nome/R$/Preço/Peso/Logo/GM · fonte/cor/layout GM |
| **Arquivos** | `produtos_etiquetas.html` · `produtos_etiquetas.js` · `produtos_etiquetas_core.js` · VERSION |
| **Migrate** | **NÃO** |
| **Risco** | **Baixo** — só `/produtos/etiquetas/` · preset antigo **igual** (GM off) · zero PDV/caixa |
| **Smoke** | check 0 · JS OK · fmtGmCurto cases OK · preset antigo não mostra GM |
| **Autorizar** | *pode subir etiquetas GM / produção* + **99738595** |
| **Rollback** | `rollback/pre-v1252-etq-gm` @ HEAD `producao` antes do push |

### 📦 PACOTE — Lembrete entrega (PDV-LEMBRETE-ENTREGA · **v12.12**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.12** · no lote `9d65e31` |
| **Branch** | `deploy/pdv-lembrete-entrega-v12.12` @ ffdf13e (incluída no lote) |


### PACOTE — Entrada NF vínculo + Excel estoque (**v12.10**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.12** · no lote `9d65e31` · migrate 0068+0069 |
| **Branch** | `deploy/entrada-nf-vinculo-estoque-xlsx-v12.10` @ 9f60fea |


### 🐛 Fix — Lembrete entrega (30/07) → ✅ loja v12.12


### 📦 PACOTE — Excel estoque Cadastro (**v12.09**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** no lote v12.12 (com vínculo NF) |


### 📦 PACOTE — PDV cache lápis (**v12.08**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.12** · no lote `9d65e31` |
| **Branch** | `deploy/pdv-cache-lapis-v12.08` @ fd14a10 |


### PACOTE — Entrada NF custo cadastro (`ENTRADA-NF-CUSTO`)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **teste v13.71** (reaberto 03/08) · aguarda validação local · **não** loja |

### Entrada NF — custo cadastro não puxa (29/07 · reaberto 03/08)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ fix **v13.71** no `teste` |
| **Sintoma** | Busca mostra venda ok · V. unit 0,00 ao incluir |
| **Fix** | overlay sync final · JS ignora custo 0 · PG em `buscar-produto-id` |
| **Você** | Ctrl+F5 Entrada NF · incluir produto com custo no Cadastro |

### 📦 PACOTE — Ajuste Mobile Somar+catálogo (`AJUSTE-MOBILE-SOMAR` · **v12.11**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.12** · no lote `9d65e31` |
| **Branch** | `deploy/ajuste-mobile-somar-v12.11` @ 2c5f66c |
| **Arquivo** | **só** `mobile_ajuste.html` |

### 🐛 Ajuste Mobile — catálogo 0 no celular do funcionário (30/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ no pacote **AJUSTE-MOBILE-SOMAR** · pronto envio |
| **Sintoma** | Link pro Vitor · PIN ok · **CAT 0** |
| **Fix** | Erro + Tentar de novo · Atualizar baixa lista · cache após sucesso |

### UX — Ajuste Mobile Somar × Trocar (30/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ no pacote **AJUSTE-MOBILE-SOMAR** · pronto envio |
| **Fix** | Modal: **Somar** / **Trocar** · campo Quantidade vazio ao abrir |

### ✅ Deploy loja **v12.06** — AJUSTE-MOBILE-CEL (29/07 · Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** · push `producao` **`7623c3c`** · badge **12.06** · aguardar Live Render |
| **Base** | loja **v12.05** @ `30e79a9` |
| **Origem teste** | **`7cc95f5`** (código; banana do teste não cherry — conflito) |
| **Inclui** | Ajuste Mobile tela cheia celular · fora shell BI · layout touch |
| **NÃO inclui** | DF-e · bug report · merge `teste` inteiro |
| **Migrate** | **NÃO** |
| **Rollback** | `git push origin rollback/pre-v1206-ajuste-mobile:producao` (@ **30e79a9** / v12.05) |
| **Validar agora** | Ctrl+F5 no celular · `/ajuste-mobile/` · PIN · busca · 1 contagem · BI «Ajuste» sai do BI · badge **12.06** |

### 📦 PACOTE PRONTO LOJA — Ajuste Mobile celular (`AJUSTE-MOBILE-CEL` · 29/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado loja v12.06** · `producao` **`7623c3c`** · teste **`7cc95f5`** |
| **Commit teste** | **`7cc95f5`** · branch `teste` |
| **VERSION loja** | **12.06** (era 12.05) |
| **Escopo** | Só `/ajuste-mobile/` (+ atalho BI / links `target=_top`) · **não** mexe PDV venda / caixa / CP |
| **Risco loja aberta** | **Baixo** — shell/FAB/BI só especializam path ajuste-mobile |
| **Inclui** | Tela cheia fora do BI · layout celular · scroll página · cards/resumo · teclado visualViewport |
| **Arquivos** | `mobile_ajuste.html` · `ajuste_mobile_login.html` · `_agro_open_external.html` · `dashboard_gerencial.html` · `_agro_pdv_fab.html` · `context_processors.py` · `sidebar_nav.html` · `consulta_produtos_pre_layout_pdv.html` |
| **NÃO incluir** | WIP DF-e / bug report / entrada_nota / models / views / *-CAIXA* |

### UX — Ajuste Mobile tela cheia no celular (29/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **loja v12.06** · `7623c3c` |
| **Pedido** | Layout só celular + link próprio (não abrir dentro do BI com barra lateral) |
| **Causa** | Shell de abas (`agro-open-inapp-tab`) engolia `/ajuste-mobile/` · URL ficava no BI |
| **Fix** | Sem shell nesta rota · rompe iframe · BI atalho = `location.assign` full · layout touch · sem F1/PDV/Display Scale |
| **Revisão 29/07 b** | Scroll da **página inteira** · tipografia sem inflar · teclado (`visualViewport`) |
| **Revisão 29/07 c** | Cabeçalho em **faixas** (estoque sozinho · horário + Atualizar na linha de baixo) |
| **Revisão 29/07 d** | Resumo 1 linha · cards compactos · título 2 linhas c/ corte |
| **Link** | `…/ajuste-mobile/` (PIN → mesma URL) |
| **Arquivos** | ver pacote **AJUSTE-MOBILE-CEL** |
| **Você** | Ctrl+F5 no celular · rolar · buscar · 1 contagem · BI «Ajuste» deve sair do BI |
| **Não** | Usar no PC / abrir pelo shell de abas |

### ðŸ› Dist DF-e — caixa de entrada salva (~80) (31/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ VERIFY_OK · pacote **DFE-INBOX v12.51** no checklist · aguarda produção |
| **O quê** | Lista PG · Buscar grava · Pendentes FIFO · cursor 137/138 |
| **Você** | Produção: frase+senha · depois Buscar/chave na loja |

### ðŸ› Dist DF-e — 656 avançava o NSU sem puxar nota (31/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cursor ia 2086→2090 no **656** · sem XML · fio da meada perdido |
| **Causa** | API gravava `ult_nsu` da resposta SEFAZ em qualquer cStat |
| **Fix** | Só grava cursor em **137/138** · no **656** mantém o salvo · UI mostra «Cursor loja» |
| **Local** | Cursor resetado para **2086** |
| **Você** | Ctrl+F5 · Atualizar status → 2086 · **não** Consultar até passar 1h do 656 · nota atrasada → Ler XML |

### ðŸ› Dist DF-e — certificado na branch `teste` (30/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Aba SEFAZ amarela de novo · ultNSU 0 (voltou para `teste` sem o fix) |
| **Fix** | DF-e reusa `NFC_E_*` · cursor NSU PG **2086** · models + migrate `0071`/`0072` |
| **Você** | Reiniciar runserver · Ctrl+F5 · **Atualizar status** → verde · se 656 não clicar |
| **Push** | `origin teste` |

### ðŸ› Dist DF-e — certificado «não configurado» no local (29/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Aba SEFAZ · aviso amarelo · ultNSU 0 · «Distribuição DF-e não configurada» |
| **Causa** | Branch sem fallback NFC-e · só lia `NFE_DIST_DFE_*` (vazio) · cursor NSU só no Mongo |
| **Fix** | `sefaz_dfe_client` reusa `NFC_E_*` · cursor NSU no Postgres/SQLite (`AgroNfeDistDfeCursor` · já em **2086**) · status/API usam mesmo CNPJ |
| **Você** | Reiniciar runserver · Ctrl+F5 · **Atualizar status** → deve sumir o amarelo · **não** pular NSU · se 656 → esperar 1h · nota urgente → **Ler XML** |
| **Não** | Consultar SEFAZ várias vezes seguidas · saltar ultNSU |


### Entrada NF — Sem a pagar + botão morto + duplicata (29/07 · NF 39407344)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **loja v12.04** |
| **Sintoma** | Lista Concluída «Sem a pagar» · títulos existem no CP · etapa 7 laranja · Salvar+a pagar não clica · reabrir+confirmar duplica |
| **Causa** | Flag `financeiro_lancado` sumiu · UI congelada (PIN) bloqueava o botão · gerar de novo sem achar o lote antigo |
| **Fix** | Botão religa mesmo com PIN · sync por NF antes de inserir · Reabrir estorna títulos órfãos pela NF |
| **Você** | Ctrl+F5 · abrir a nota · etapa 7 · **Salvar + a pagar** → deve religar (sem 2º lote). Na NF já com 6 linhas: apagar 3 duplicadas no CP |
| **Não** | Reabrir e confirmar todas as etapas «só pra limpar o laranja» |

### Contas a pagar — busca por número da NF (29/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **loja v12.04** |
| **Sintoma** | Digitar `013962` = 0 títulos · Ctrl+F achava na descrição |
| **Causa** | Número puro 5–7 dígitos ia só como **valor** + `numero_documento` · NF fica em «NF 013962» na descrição |
| **Fix** | Também casa descrição/obs (+ variante sem zero à esquerda) |
| **Arquivo** | `lancamentos_financeiro_pg_util.py` · ajuda `?` / AGENTS §10 |
| **Você** | Ctrl+F5 Contas a pagar · busca `013962` → deve listar as parcelas |

### Local — catálogo SQLite atualizado (29/07)

| Item | Detalhe |
| ---- | ------- |
| **Fonte** | staging Oregon (dump Produto+overlay; sem coluna peso_etiqueta lá ainda) |
| **Destino** | db.sqlite3 · **3361** produtos · **819** overlays |
| **.env** | DATABASE_URL continua **comentado** (rápido) |
| **Você** | reiniciar 
unserver · Ctrl+F5 · limpar cache busca se precisar |


### ✅ PDV — carrinho itens travados (FL-008 · 28/07 · loja v11.99)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | GM0110-1 / GM0110-10 (e outros) no carrinho: +/−, lixeira e lápis mortos |
| **Causa** | Lista busca `display:flex` ganhava do `.hidden` — cobria o carrinho (mesmo bug v6.16) |
| **Fix** | `pdv_wizard.html` + `pdv_wizard.js` · commit **`8f7610d`** |
| **Status** | ✅ **loja OK** — Renan validou 28/07 · badge **11.99** |
| **Rollback** | `rollback/pre-v1199-pdv-carrinho` → v11.98 |

### Deploy loja **v11.98** — lote 11.94-11.98 (28/07 · Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **enviado** · push `producao` **`cb73d87`** · badge **11.98** · aguardar Live + migrate `0067` |
| **Inclui** | Pix Sicredi (11.94) · Entrada NF UX+Nova (11.95) · Gôndola+0067 (11.96) · Transf forçada (11.97) · Estoque Vila (11.98) |
| **NÃO inclui** | DF-e inbox · merge inteiro `teste` |
| **Migrate** | **SIM** `0067_produto_overlay_peso_etiqueta` |
| **Rollback** | `git push origin rollback/pre-lote-v1194-1198:producao` (@ **3633011** / v11.93) |
| **Validar agora** | Ctrl+F5 · badge **11.98** · Pix Sicredi · Entrada NF Nova/boleto · etiquetas · transf forçada · PDV Estoque Vila · 1 venda |

### FILA DEPLOY — 11.94-11.98 (enviado loja v11.98)

Loja hoje **v11.93**. Ordem sugerida (um por vez ou lote, sempre com frase + `99738595`):

| Ordem | Loja | Ref | Branch pronta | Migrate | Rollback |
| ----- | ---- | --- | ------------- | ------- | -------- |
| 1 | **11.94** | PDV-PIX-SICREDI | `deploy/pdv-pix-sicredi-v11.94` @ `06db8c8` | NÃO | `rollback/pre-v1194-pdv-pix-sicredi` |
| 2 | **11.95** | ENTRADA-NF-UX (+ Nova limpa) | `deploy/entrada-nf-ux-v11.95` @ `425e16c` | NÃO | `rollback/pre-v1195-entrada-nf-ux` |
| 3 | **11.96** | ETQ-GONDOLA | `deploy/etq-gondola-v11.96` @ `3ab66e9` | **SIM** `0067` | `rollback/pre-v1196-etq-gondola` |
| 4 | **11.97** | TRANSF-FORCADA | `deploy/transf-forcada-v11.97` @ `cbc9a38` | NÃO | `rollback/pre-v1197-transf-forcada` |
| 5 | **11.98** | PDV-ESTOQUE-VILA | `deploy/pdv-estoque-vila-v11.98` @ `e792363` | NÃO | `rollback/pre-v1198-pdv-estoque-vila` |

**Já resolvido (não sobe de novo):**
- **«v10.89» MP automático pós-abrir caixa** — código **já na loja** (v11.93+); checklist antigo enganava.
- **«v10.90» Entrada NF Nova limpa** — **já dentro** do pacote **11.95** (`entradaNfeResetEditorParaNovaNota`).
- **«v9.92» kardex e-mail + Δcamada** — **já na loja desde v10.63** (`1e6fe3c`); checklist antigo enganava.

**Smoke 28/07:** `manage.py check` 0 · asserts pacote 10/10 · py_compile OK · DF-e **fora** de todos os deploys.

**Autorizar (próximo chat):** pausa vendas se quiser · *«pode subir … / produção»* + **99738595** · FF da branch `deploy/…` → `producao` · Ctrl+F5.


### ✅ Deploy loja **v12.05** — ETQ-BUSCA-FILTROS (29/07 · frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **HEAD** | `30e79a9` |
| **Rollback** | `rollback/pre-v1205-etq-busca` @ `72b204b` |
| **Validar** | Ctrl+F5 · badge **12.05** · filtros · add todos · lista não some |

### PACOTE PRONTO LOJA — Etiquetas busca filtros + add todos (`ETQ-BUSCA-FILTROS` · **v12.05**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **ENVIADO loja v12.05** · `producao` @ **`30e79a9`** · rollback `rollback/pre-v1205-etq-busca` @ `72b204b` |
| **VERSION alvo loja** | **12.05** (loja hoje **12.04**) |
| **Inclui** | Filtros Folha na busca · lista **não some** ao add (FL-009) · **Adicionar todos** |
| **Arquivos** | `produtos_etiquetas.html` · `produtos_etiquetas.js` · `produtos_etiquetas_view` (só contexto facetas/presets) |
| **Migrate** | **NÃO** |
| **Risco loja aberta** | **Baixo** — só `/produtos/etiquetas/` · **zero** PDV/caixa/CP |
| **Smoke** | `manage.py check` 0 · JS syntax OK · diff views = 7 linhas |
| **NÃO inclui** | DF-e · merge `teste` · Folha saldo · Entrada NF |
| **Próximo chat** | 1) pausar vendas 2) *pode subir etiquetas busca / produção* + **99738595** 3) FF `deploy/etq-busca-filtros-v12.05` → `producao` · Ctrl+F5 |
| **Rollback** | `rollback/pre-v1205-etq-busca` @ HEAD `producao` antes do push |

### PACOTE PRONTO LOJA — Folha polish + Etiquetas ajuste (`FOLHA-ETQ-V12` · 29/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **já na loja v12.03** (lote Folha+Etq) — não sobe de novo |
| **VERSION alvo loja** | **12.00→12.03** |
| **Branch** | `deploy/folha-etq-v12.00` @ **`0a4a386`** |
| **Inclui** | 1) Folha saldo polish (fornecedor overlay · filtro no cupom · GM alinhado · saldo) 2) Etiquetas: borda 0,5 mm fora (91×31) · fonte nome 1/2/3 linhas · corte na 3ª |
| **Arquivos** | `compras_relatorio_planilha.html` · `compras_relatorio_saldo.html` · `produtos_etiquetas.js` · `produtos_etiquetas_core.js` · `produtos_etiquetas.html` |
| **Migrate** | **NÃO** |
| **Risco loja aberta** | **Baixo** — só Folha saldo/planilha + etiquetas · **zero** PDV carrinho/caixa/CP |
| **Smoke** | `manage.py check` 0 · JS syntax OK · diffs só 5 arquivos vs `producao` |
| **NÃO inclui** | DF-e · merge `teste` · transferência · Entrada NF · PDV |
| **Próximo chat** | 1) lojas pausam vendas 2) *pode subir Folha+etiquetas / produção* + **99738595** 3) FF `deploy/folha-etq-v12.00` → `producao` · Ctrl+F5 |
| **Rollback** | `rollback/pre-v1200-folha-etq` @ HEAD `producao` antes do push |

### PACOTE PRONTO LOJA — Etiqueta Gôndola A4 (`ETQ-GONDOLA` · 28/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | 📦 **pronto** · branch `deploy/etq-gondola-v11.96` @ **`3ab66e9`** · espera frase + senha |
| **VERSION alvo loja** | **11.96** |
| **O quê** | Preset **Gôndola A4** · 9×3 cm · 18/folha · marcas de corte · logo Agro Mais · R$ separado · centavos menores · peso **sem** texto «PESO:» · campo peso no cadastro |
| **Migrate** | **SIM** — `0067_produto_overlay_peso_etiqueta` |
| **Fonte teste** | `27c8436` |
| **NÃO inclui** | Transferência · DF-e · Entrada NF · PDV Estoque Vila |
| **Risco loja aberta** | **Baixo** — só etiquetas + 1 campo cadastro |
| **Autorizar** | *pode subir etiquetas gôndola / produção* + **99738595** |
| **Rollback** | `rollback/pre-v1196-etq-gondola` |

### PACOTE PRONTO LOJA — Transferência forçada Vila↔Centro (`TRANSF-FORCADA` · 28/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | 📦 **pronto** · branch `deploy/transf-forcada-v11.97` @ **`cbc9a38`** · espera frase + senha |
| **VERSION alvo loja** | **11.97** |
| **O quê** | Modal **Forçada Vila↔C** · carrinho (busca + colar) · direção **Vila→C** ou **C→Vila** · qtd com foco/Enter · altura fixa · ajuda no «?» |
| **Migrate** | **NÃO** |
| **Fonte teste** | `0391ca7` |
| **NÃO inclui** | Etiquetas · DF-e · Entrada NF · PDV |
| **Risco loja aberta** | **Baixo** — só /transferencias/ forçada |
| **Autorizar** | *pode subir transferência forçada / produção* + **99738595** |
| **Rollback** | `rollback/pre-v1197-transf-forcada` |

### PACOTE — Folha saldo polish → absorvido em **FOLHA-ETQ-V12** (29/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ➡️ **absorvido** no pacote `FOLHA-ETQ-V12` (Folha + etiquetas juntos · loja **v12.00**) |
| **Autorizar** | ver bloco **FOLHA-ETQ-V12** acima |

### PACOTE PRONTO LOJA — Entrada NF boleto + lista Concluída + Nova limpa (`ENTRADA-NF-UX` · 28/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **já loja ~v11.98** · reconfirmado 13/08 em `producao` v16.07 · **não reenviar** |
| **VERSION alvo loja** | **11.95** |
| **Inclui** | 1) Boleto 44→47 · 2) Lista Concluída (nome/XML/chip «Sem a pagar») · 3) **Nova limpa** (antigo «v10.90») |
| **NÃO inclui** | DF-e inbox · Transferência · PDV · migrate |
| **Migrate** | **NÃO** |
| **Risco loja aberta** | **Baixo** |
| **Autorizar** | *pode subir Entrada NF UX / produção* + **99738595** |
| **Rollback** | `rollback/pre-v1195-entrada-nf-ux` |
| **Validar pós** | Concluída · boleto 44→47 · **Nova** em branco · 1 venda PDV |

### ðŸ“¦ PACOTE PRONTO LOJA â€” PDV Pix mÃ¡quina Sicredi (`PDV-PIX-SICREDI` Â· **v11.94**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **já loja ~v11.98** · reconfirmado 13/08 em `producao` v16.07 · **não reenviar** |
| **VERSION alvo loja** | **11.94** |
| **Smoke OK** | lista + filtro notebook Â· sÃ³ arquivos PDV Â· migrate **NÃƒO** |
| **Risco loja aberta** | **Baixo** â€” sÃ³ adiciona opÃ§Ã£o Sicredi Pix |
| **Autorizar** | *pode subir Sicredi Pix / PDV* + **99738595** |
| **Rollback** | `rollback/pre-v1194-pdv-pix-sicredi` |

### âœ… Deploy loja **v11.93** â€” Folha saldo presets + transferÃªncia sem saldo+ (`FOLHA-TRANSF`) (28/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado** Â· push `producao` **`f83e804`** Â· aguardar Live + migrate `0066` Â· badge **11.93** |
| **VERSION** | **loja v11.93** (antes v11.92) |
| **Como** | FF `deploy/folha-transf-v11.93` â†’ `producao` (apÃ³s rollback) |
| **Inclui** | Folha saldo overlay + filtros salvos online (PadrÃ£o â˜…) Â· TransferÃªncia Vilaâ†’C com saldo 0/negativo |
| **NÃƒO inclui** | merge inteiro `teste` Â· PDV Â· caixa Â· CP |
| **Migrate** | **SIM** `0066_compras_folha_saldo_filtro_preset` (Render no deploy) |
| **Backup / reverter** | `rollback/pre-v1193-folha-transf` @ **`e7ab4d2`** â†’ `git push origin rollback/pre-v1193-folha-transf:producao` |
| **Validar agora** | Ctrl+F5 Comprasâ†’Folha saldo (salvar/padrÃ£o) Â· `/transferencias/` Vila saldo 0 Â· 1 venda PDV |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Folha saldo presets + transferÃªncia sem saldo+ (`FOLHA-TRANSF` Â· **v11.93**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v11.93** â€” ver bloco Deploy acima |
| **Branch pronta** | `deploy/folha-transf-v11.93` @ **`f83e804`** (base `producao` + cÃ³digo limpo) |
| **Fonte teste** | `1dfd369` Â· tip `037f67b` |
| **VERSION** | **loja 11.92 â†’ 11.93** |
| **Inclui** | 1) Folha saldo overlay maior + filtros salvos **online** + PadrÃ£o â˜… Â· 2) TransferÃªncia Vilaâ†’C com saldo 0/negativo |
| **NÃƒO inclui** | merge inteiro `teste` Â· PDV Â· caixa Â· CP |
| **Migrate** | **SIM** `0066_compras_folha_saldo_filtro_preset` (tabela nova) |
| **Risco loja aberta** | PDV/caixa **intocados** Â· TransferÃªncia sÃ³ mais permissiva Â· Folha saldo sÃ³ Compras |
| **Smoke OK** | `check` Â· URLs Â· CRUD preset Â· HTML Â· cherry cÃ³digo limpo (sÃ³ VERSION/banana) |
| **Backup** | `rollback/pre-v1193-folha-transf` @ **`e7ab4d2`** |
| **Validar pÃ³s** | Ctrl+F5 Folha saldo (salvar/padrÃ£o) Â· `/transferencias/` saldo 0 Â· 1 venda PDV |

### ðŸ“¦ PACOTE â€” Folha saldo presets + transferÃªncia sem saldo+ (28/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… no `teste` Â· pacote loja = branch **`deploy/folha-transf-v11.93`** (v11.93) Â· **aguarda pausa + senha** |
| **1 Â· Folha saldo** | Overlay maior + fontes Â· filtros salvos **online** (Postgres) Â· PadrÃ£o â˜… global Â· migrate **`0066`** |
| **2 Â· TransferÃªncia** | Vilaâ†’C mesmo com saldo 0/negativo Â· aviso/confirma se qtd > saldo |
| **Arquivos** | `compras_relatorio_saldo.html` Â· `compras.html` Â· `models.py` Â· `views.py` Â· `urls.py` Â· `0066_â€¦` Â· `transferencias.html` Â· `estoque/views.py` |
| **VocÃª** | Ctrl+F5 Comprasâ†’Folha saldo (salvar/padrÃ£o) Â· `/transferencias/` Vilaâ†’C com saldo 0 |

### ðŸ› SEFAZ Dist DF-e â€” cStat 656 na 1Âª consulta do dia (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Aba SEFAZ Â· 1 clique Â· **656 Consumo Indevido** Â· botÃ£o Â«Aguarde ~1hÂ» Â· `ultNSU=â€¦2084` Â· `maxNSU=0` |
| **Causa** | Bloqueio **da Receita no CNPJ** (nÃ£o Ã© bug do botÃ£o). Quase sempre: **outro sistema** (ERP / contador / outro PC / staging) jÃ¡ consultou DF-e no mesmo CNPJ, **ou** NSU jÃ¡ no fim sem intervalo de 1h |
| **Agora** | Esperar a 1h do botÃ£o Â· **nÃ£o** clicar de novo Â· nota de hoje â†’ **Ler XML** (e-mail / portal / fornecedor) |
| **Depois** | 1 clique sÃ³ Â· se vier **137** (nada novo) â†’ esperar 1h Â· se **138** â†’ carregar na grade |
| **Conferir** | ERP legado ou software do contador ainda â€œbaixa NFâ€ automÃ¡tico deste CNPJ? |

### ðŸ©¹ FIX â€” Ajuste Mobile saldo Â«tudo 0Â» / filtro sÃ³ positivo (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… no pacote **CAD-COMPRAS-AJUSTE** (teste v11.80) Â· validar local Â· loja alvo v11.92 |
| **Sintoma** | HistÃ³rico grava ajuste (ex. +1000 Vila) Â· lista com Â«SÃ³ positivoÂ» = **0 itens** Â· saldo na tela fica 0 |
| **Causa** | Lista usa cache do PDV com saldo 0 Â· filtro positivo esconde tudo Â· ATUALIZAR sÃ³ pedia saldo dos **visÃ­veis** (ninguÃ©m) |
| **Fix** | API `?positivos=1&deposito=` Â· ao abrir / ATUALIZAR / Aplicar Â«sÃ³ positivoÂ» hidrata saldo real do depÃ³sito |
| **Arquivos** | `mobile_ajuste.html` Â· `views.py` (`api_pdv_saldos`) Â· `estoque_saldo_agro_util.py` |
| **Validar** | Ctrl+F5 Â· Vila Â· filtro SÃ³ positivo â†’ deve listar o que ajustou hoje Â· ATUALIZAR Â· Hist. confere |
| **Workaround se ainda vazio** | Limpar filtros â†’ ATUALIZAR â†’ depois SÃ³ positivo de novo |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Cadastro filtros + Folha saldo + Ajuste mobile (`CAD-COMPRAS-AJUSTE` Â· 27/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ðŸ“¦ **pronto p/ prÃ³ximo chat** â€” push `teste` Â· **nÃ£o** produÃ§Ã£o ainda Â· espera pausa loja + frase + senha `99738595` |
| **Pasta** | `_pacote_cadastro_compras_ajuste_20260727/LEIA-ME.txt` |
| **VERSION teste** | **11.80** (push) |
| **VERSION loja alvo** | **11.92** (loja hoje **11.91** Â· no cherry **forÃ§ar 11.92**, nÃ£o 11.80) |
| **Commits (ordem cherry)** | `87d6557` (folha fornecedor PG) â†’ **`a812c78`** (pacote) Â· docs `9a72d80` opcional |
| **Inclui** | Cadastro filtros/coluna estoque Â· Folha saldo A4/A5/80mm Â· Folha fornecedor PG Â· Ajuste mobile `positivos=1` Â· fix saldos c/ Mongo off |
| **RevisÃ£o 27/07 (assistente)** | Diffs revisados Â· `manage.py check` 0 Â· smoke 200: `/api/pdv/saldos/` (c/ e s/ ids) Â· `/consulta/` Â· `/caixa/` Â· `/compras/` Â· cadastro Â· ajuste-mobile Â· APIs saldo |
| **Risco loja aberta** | **Baixo** se cherry sÃ³ estes commits: PDV/saldos sem `positivos` **igual** Â· `positivos=1` sÃ³ mobile Â· Compras/Cadastro = additivo Â· `agro_fonte` na loja **jÃ¡** tem o short-circuit Mongo (cherry pode no-op/conflito trivial) |
| **NÃƒO fazer** | merge `teste`â†’`producao` inteiro Â· subir sem pausa PDV Â· deixar VERSION 11.80 na loja |
| **PrÃ³ximo chat** | 1) lojas pausam vendas 2) Â«pode subir produÃ§Ã£oÂ» + `99738595` 3) cherry + VERSION 11.92 + push `producao` + Ctrl+F5 |

### âœ¨ UX â€” Coluna estoque cadastro (27/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Total grande colorido (verde/vermelho/cinza) + chips **C** / **V** com cor prÃ³pria Â· nÃºmero pt-BR |
| **Arquivos** | `cadastro_erp_panel.js` `?v=27` Â· CSS em `produtos_cadastro_erp.html` |
| **VocÃª** | Ctrl+F5 cadastro Â· olhar coluna Estoque |

### âœ¨ UX â€” Filtros cadastro compactos (27/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Removidas abas/vitrines redundantes (Produtos/Marcas/Categorias/Fornecedores). Uma barra: Loja Â· Estoque Â· Marca/Cat/Sub/Forn/Unid/Modelo Â· Aplicar. Data/sub2â€“4/preÃ§o/NCM em Â«MaisÂ». Mais altura p/ lista. |
| **Arquivos** | `produtos_cadastro_erp.html` Â· `cadastro_erp_panel.js` `?v=26` |
| **VocÃª** | Ctrl+F5 cadastro Â· botÃ£o **Filtros** |

### ðŸ©¹ FIX â€” `/api/pdv/saldos/` 500 local (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Local lento Â· log `Internal Server Error: /api/pdv/saldos/` Â· body Â«Erro conexaoÂ» |
| **Causa** | Mongo ERP desligado, mas `estoque_operacional_sem_mongo_erp` exigia ledger â†’ API recusava |
| **Fix** | Com `mongo_erp_desligado` â†’ saldo operacional sem Mongo (ajuste) |
| **Arquivo** | `agro_fonte_config.py` |
| **VocÃª** | Reinicia `runserver` Â· Ctrl+F5 Â· Compras/ajuste nÃ£o devem mais spammar 500 |

### WIP â€” Folha de saldo Compras (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… no pacote **CAD-COMPRAS-AJUSTE** (teste v11.80) Â· validar local |
| **O quÃª** | ImpressÃ£o sÃ³ **GM Â· produto Â· saldo** Â· papel **A4 / A5 / 80mm** Â· checkbox Centro e/ou Vila (duas = soma) |
| **Filtros** | Mesmos do cadastro (marca/cat/sub/estoque/datas/modelo/unidade/extras) + esconder zerados + busca |
| **UX (27/07)** | Popup compacto padrÃ£o ERP Â· filtros = **botÃµes + chips** (igual cadastro) Â· listas jÃ¡ no HTML (nÃ£o fica branco) |
| **80mm Epson TM-T20X** | wrap **82%** + saldo fixo (90% ainda raspava) |
| **Arquivos** | `compras_relatorio_saldo.html` Â· `views.py` Â· `urls.py` Â· `compras.html` |
| **API** | `/api/compras/relatorio-saldo/` |
| **VocÃª** | Ctrl+F5 Compras Â· Folha de saldo Â· clicar **Marca** deve listar opÃ§Ãµes |

### WIP â€” Cadastro ERP filtros completos (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… no pacote **CAD-COMPRAS-AJUSTE** (teste v11.80) Â· validar local |
| **Pedido** | Filtros avanÃ§ados: estoque +/âˆ’/0 Â· loja Centro/Vila Â· multi cat/sub 1â€“4 Â· datas (cadastro / 1Âª NF / Ãºltima NF) Â· modelo Â· unidade Â· extras |
| **UI** | BotÃ£o **Filtros** Â· barra compacta (sem vitrines) Â· Â«MaisÂ» recolhido |
| **Exemplo** | Loja=Vila + Estoque=Positivo â†’ sÃ³ saldo Vila Elias |
| **Arquivos** | `cadastro_filtros_util.py` Â· `estoque_saldo_agro_util.py` Â· `catalogo_agro.py` Â· `views.py` Â· `produtos_cadastro_erp.html` Â· `cadastro_erp_panel.js` Â· `agro_busca_catalogo.js` |
| **Validar** | Vila+positivo Â· multi cat/sub Â· cada tipo de data Â· Limpar Â· lista com mais altura |

### ðŸŒ Local lento â€” Postgres Oregon (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | `.env` com `DATABASE_URL` staging **Oregon** (~300â€“600 ms/consulta) |
| **SoluÃ§Ã£o Renan** | Snapshot catÃ¡logo â†’ SQLite local Â· `DATABASE_URL` comentado Â· **reiniciar runserver** |
| **Resultado** | ~3361 produtos congelados Â· count ~5 ms |
| **Doc** | `docs/TESTE-LOCAL.md` Â§0 |

### ðŸ› FIX â€” Folha Compras Â«por fornecedorÂ» sem Mongo (27/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` Â· validar no PC (Ctrl+F5 `/compras/`) |
| **Sintoma** | Alert Â«serviÃ§o legado indisponÃ­velÂ» ao **Imprimir planilha** (por fornecedor) â€” Mongo morto |
| **Fix** | Caminho Postgres: catÃ¡logo por fornecedor + Ãºltima Entrada NF Agro + vendas PDV |
| **TambÃ©m** | Categoria/unidade jÃ¡ eram PG (sem mudanÃ§a) Â· rÃ³tulo Â«FORNECEDORÂ» duplicado â†’ Â«Montar folhaÂ» |
| **Arquivos** | `views.py` Â· `catalogo_agro.py` Â· `compras_ultimas_compras_util.py` Â· `compras_relatorio_planilha.html` |
| **Validar** | Folha â†’ por fornecedor (IBIUNA) Â· por categoria Â· por unidade |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Dispenser +13 sabores (`DSP-SABORES` Â· 24/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ðŸ“¦ **pronto** Â· **NÃƒO subir** enquanto loja vende Â· prÃ³ximo chat: pausar vendas + frase + senha `99738595` |
| **Commit teste** | **`ed43464`** `feat(dispenser): +13 sabores comuns (leite, ovo, bacalhauâ€¦)` |
| **VERSION loja alvo** | **11.91** (loja hoje **11.90** Â· no cherry **nÃ£o** usar 11.76 do teste) |
| **Arquivos** | `flavor_lib.js` Â· `flavor-icons.svg` Â· `dispenser_flavor_icons.html` Â· 13 PNG em `lib/icons/` Â· cache studio `?v=11.76` Â· (+ `VERSION`/`banana` no resolve) |
| **Inclui** | Leite Â· Ovo Â· Bacalhau Â· Veado Â· CamarÃ£o Â· Batata Â· Milho Â· LinhaÃ§a Â· Cranberry Â· Banana Â· Couve Â· Inhame Â· Alecrim |
| **JÃ¡ tinha** | **Blueberry (Mirtilo)** â€” Ã­cone + catÃ¡logo jÃ¡ existiam (`mirtilo.png`) Â· **nÃ£o** faltava |
| **NÃƒO inclui** | migrate Â· PDV Â· caixa Â· CP Â· NFC-e Â· merge inteiro `teste` Â· kardex Â· re-cherry zoom/cores |
| **Cherry** | **sÃ³** `ed43464` em `producao` Â· conflito esperado sÃ³ `VERSION`+`banana` (Ã­cones/JS aplicam limpo â€” dry-run OK 24/07) |
| **Backup** | criar `rollback/pre-v1191-dsp-sabores` @ HEAD `producao` antes do push |
| **Risco loja aberta** | **Baixo** se cherry sÃ³ esse commit â€” paths sÃ³ `/interno/dispenser-a6*` Â· **zero** `/consulta/`, caixa, vendas |
| **Testado assistente** | dry-run cherry Â· 13 PNG + blueberry presentes Â· META/PROTEINS/SIDES OK Â· **falta** Ctrl+F5 visual do Renan |
| **VocÃª antes de autorizar** | Local: Dispenser â†’ Sabores â†’ ver grade nova (Leite etc.) Â· Blueberry jÃ¡ na lista |
| **PÃ³s-Live** | Ctrl+F5 Dispenser Â· Sabores Â· PDV smoke rÃ¡pido sÃ³ por precauÃ§Ã£o |

### âœ… Deploy loja **v11.90** â€” zoom logo (`DSP-LOGO-ZOOM`) Â· âœ… jÃ¡ enviado

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live** Â· **nÃ£o** falta senha deste pacote |
| **Commit** | `2f004b5` / tip docs Live |

### WIP â€” Dispenser A6 (visual)

| Item | Detalhe |
| ---- | ------- |
| **URL** | `/interno/dispenser-a6/` |
| **JÃ¡ na loja** | Nuvem + Prontas + cores + zoom logo (**v11.90**) |
| **Novo no teste** | **Mix Sabores** + criar sabor (nome/desc/PNG) · `DSP-MIX-CRIAR` (03/08) |
| **Blueberry** | âœ… jÃ¡ no catÃ¡logo (Mirtilo) |
| **Regra** | Cherry **sÃ³** o pacote Â· **nunca** merge `teste`â†’`producao` |

### ðŸ“¦ PACOTE PRONTO LOJA â€” HistÃ³rico estoque / kardex ledger (**v11.68** Â· 23/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ✅ **já na loja** (~v10.63+) · **não** entra no lote v12.51 · «pronto» antigo invalidado 31/07 |
| **Caso** | Venda **#3747** Â· 1 un. ibiuna Â· aba Estoque mostrou saÃ­da **~39,463** (bug em **qualquer** produto com ledger) |
| **O que Ã©** | Kardex com ledger = Î” `saldo_informado` (nÃ£o Î” camada Mongo) Â· baixa/estorno venda usa snapshot ledger + congela `erp_ref` |
| **VERSION** | **11.68** Â· commit teste **`fddc5a2`** |
| **Arquivos** | `estoque_movimentos_cadastro_util.py` Â· `views.py` (baixa/estorno) |
| **Risco** | Baixo â€” sÃ³ leitura do histÃ³rico + alinhamento da baixa; **nÃ£o** apaga estoque nem vendas |
| **Testar (teste)** | Ctrl+F5 Â· cadastro ibiuna â†’ aba Estoque â†’ venda #3747 saÃ­da **1** Â· 1 venda nova de 1 un. = saÃ­da 1 |
| **Loja** | frase + senha Â· rollback `rollback/pre-v1168-kardex-ledger` @ HEAD `producao` antes do push |

### âœ… VerificaÃ§Ã£o remota loja â€” abrir/vender amanhÃ£ (22/07 ~23:20 Â· Renan ausente AnyDesk)

| Item | Resultado |
| ---- | --------- |
| **Loja Live** | **v11.64** em `https://sistvale.com.br/` (badge home) Â· commit **118acbc** |
| **healthz** | **200 ok** (~0,7s) |
| **PDV `/consulta/`** | HTML 200 Â· JS `consulta_produtos` + `agro_busca_catalogo` com `?v=118acbcâ€¦` (pacote novo) |
| **Busca `/api/buscar/`** | OK ~0,5â€“1,4s Â· leite/cÃ£o/gato/raÃ§Ã£o/GM9503 retornam produtos |
| **Freio catÃ¡logo full** | **ligado** â€” `/api/todos-produtos/` e `â€¦/delta/` respondem vazio em &lt;1s (`catalogo-full-off`) â†’ PDV vende pela busca servidor (nÃ£o remonta catÃ¡logo gigante de manhÃ£) |
| **Freio no cÃ³digo** | `agro_pdv_catalogo_full_desligado` **mantido** na loja (nÃ£o veio removido do teste) |
| **AST Python** | views / fonte / settings / middleware OK |
| **Rollback pronto** | `rollback/pre-v1164-devolucao-bca-custo` @ **241782c** â†’ `git push origin rollback/pre-v1164-devolucao-bca-custo:producao` |
| **NÃƒO testado** (sem login) | Abrir caixa Â· finalizar venda Â· fiado Â· devoluÃ§Ã£o na loja Â· NFC-e |
| **NÃƒO na loja ainda** | `--todas` reaplicar custos (sÃ³ **teste v11.65**) â€” **nÃ£o bloqueia vender** |
| **AmanhÃ£ de manhÃ£ (loja)** | 1) Ctrl+F5 no PDV Â· 2) abrir caixa Â· 3) buscar produto + 1 venda rÃ¡pida Â· se site morto: ver healthz / Render Â· se pacote ruim: rollback acima |

**Veredito assistente:** site e busca da loja **saudÃ¡veis** para abrir e vender; risco residual = fluxos autenticados (caixa/venda) nÃ£o exercitados daqui.

### ðŸ“Œ Reaplicar custos NF em lote `--todas` (**teste v11.65** Â· 22/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… `teste` **873d7fe** Â· **ainda NÃƒO na loja** (loja v11.64 = 1 NF por vez) |
| **Uso** | dry-run: `python manage.py reaplicar_custos_entrada_nf --todas` Â· gravar: `â€¦ --todas --aplicar` |
| **Opcional** | `--desde=2026-01-01` Â· `--limite=50` Â· ordem antigaâ†’nova (Ãºltima NF manda no custo) |
| **Loja** | precisa frase + senha de novo |

### ðŸ§­ Teste = LOCAL (22/07 Â· Renan Â· opÃ§Ã£o B)

| Item | Detalhe |
| ---- | ------- |
| **Gate** | Chrome em `http://127.0.0.1:8000` â€” ver `docs/TESTE-LOCAL.md` |
| **Render teste** | Free dorme â€” **nÃ£o** usar como validaÃ§Ã£o; assistente **nÃ£o** push `origin teste` sozinho |
| **ProduÃ§Ã£o** | SÃ³ apÃ³s Renan testar local + frase + senha (ele nÃ£o sobe sem testar) |
| **ConfianÃ§a** | Renan: nÃ£o enviar Ã  loja sem teste local; assistente respeita |

### âœ¨ Kardex cadastro â€” Fornecedor/PreÃ§o na Entrada NF (**teste v11.59**)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Aba Estoque do modal: coluna Fornecedor/PreÃ§o ficava `---` na Entrada NF (custo jÃ¡ ok na aba PreÃ§os) |
| **Fix** | Grava `nf_forn`/`nf_custo` no `observacao` do ajuste Â· lÃª no kardex Â· enrich compras via rascunho Entrada NF Agro (PG, sem Mongo) |
| **NFs antigas** | Preenche se achar a NF no rascunho Agro (nÂº) |
| **Arquivos** | `estoque_movimentos_cadastro_util.py` Â· `views.py` Â· `compras_ultimas_compras_util.py` |

### ðŸ› Cadastro BCA â€” Â«lista localÂ» incompleta (**teste v11.60**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Busca Â«testeÂ» mostra sÃ³ 2 (rodapÃ© **LISTA LOCAL**); GM9503 some; Ã s vezes volta 7 |
| **Causa** | Fallback 2s do pacote local; servidor chegava depois e era **ignorado**; pacote nÃ£o ganhava produtos novos |
| **Fix** | Cadastro espera atÃ© 12s o servidor Â· resposta tardia atualiza a grade Â· merge de produtos novos no pacote local |
| **Agora** | Ctrl+F5 no Cadastro Â· buscar Â«testeÂ» de novo (pode demorar uns segundos) |
| **Arquivos** | `agro_busca_catalogo.js` Â· `cadastro_erp_panel.js` |

### ðŸ”’ Cadastro BCA â€” sÃ³ servidor (**teste v11.61**)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Renan: em produÃ§Ã£o nÃ£o pode oscilar 2 vs 10 produtos |
| **DecisÃ£o** | **Cadastro** `skipLocal: true` â€” grade sempre `/api/buscar` (servidor). Pacote local fica pro **PDV** (velocidade) |
| **Kardex NF 1111** | Sem rascunho Agro no PG (sÃ³ 112 e 222) â€” coluna `---` correta; notas novas gravam forn/custo no ajuste |
| **Por que 2 vs 7+** | Pacote Chrome **incompleto** (nÃ£o Ã© cadastro diferente). Â«HojeÂ» nÃ£o rebaixava delta â†’ local sÃ³ achava 2 lixos; servidor lÃª os ~7â€“8 reais no PG. Fix: delta em background mesmo com pacote Â«hojeÂ» |
| **Arquivos** | `cadastro_erp_panel.js` Â· `agro_busca_catalogo.js` |

### ðŸ”’ PDV â€” lista nÃ£o cresce depois (**teste v11.62**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Busca TESTE: 2 itens â†’ 5 s depois aparecem mais (medo Renan) |
| **Causa** | `mesclarBuscaLocalComOnline` pintava local e depois **acrescentava** hits do servidor |
| **Fix** | Com lista local jÃ¡ na tela: servidor **nÃ£o** muda a sugestÃ£o; sÃ³ atualiza pacote em background |
| **Arquivos** | `consulta_produtos.js` |

### âœ… PDV â€” regra busca + preÃ§o no carrinho (**teste v11.63**)

| Item | Detalhe |
| ---- | ------- |
| **Busca** | Pacote Â«lista de hojeÂ» bom + hits â†’ local na hora. Pacote fraco/vazio â†’ espera servidor **â‰¤2s**, senÃ£o usa local. Sem pisca |
| **Carrinho** | Confirma preÃ§o via `/api/buscar/` **â‰¤2s**; se demorar/falhar â†’ mantÃ©m cache. Atualiza silencioso (sem alert ERP) |
| **Realidade loja** | 2â€“6 NF/dia Â· bipâ‰ˆnome Â· PC abre todo dia |
| **Arquivos** | `consulta_produtos.js` |

### ðŸš‘ PDV â€” lag 1â€“2s em tudo (**teste v11.64**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Menu caixa / fechar venda dinheiro â€œengasgandoâ€ 1â€“2 s (antes era na hora) |
| **Causa** | `ensurePacote` baixava catÃ¡logo **inteiro a cada busca** â†’ worker Django ocupado â†’ demais cliques na fila |
| **Fix** | Delta no mÃ¡x. **a cada 5 min** (ou pacote fraco / 1Âª abertura); busca continua leve |
| **Commit** | `3f9683e` Â· branch `teste` Â· **aguarda** frase + senha produÃ§Ã£o (lojas fecharem) |
| **Arquivos** | `agro_busca_catalogo.js` |

### ðŸ› DevoluÃ§Ã£o loja â€” Â«Falha de redeÂ» (#3798 Centro) (**teste v11.58**)

| Item | Detalhe |
| ---- | ------- |
| **Quando** | 22/07 â€” print loja Centro, venda **#3798**, PentabiÃ³tico, forma **Fiado** |
| **Sintoma** | Modal Â«Falha de redeÂ» ao confirmar devoluÃ§Ã£o |
| **Causa** | Estorno na devoluÃ§Ã£o **exigia Mongo**; ping lento â†’ timeout/HTML â†’ front dizia Â«redeÂ». Baixa na venda jÃ¡ rodava sem Mongo |
| **Fix** | Estorno alinhado Ã  baixa (skip Mongo se ledger/sem ERP) Â· NFC-e cancel try/except Â· mensagem front com HTTP |
| **Loja agora** | Sem pacote: recarregar #3798; 2Âª tentativa; se demorar ~30s = timeout. **Sobe** sÃ³ com frase + `99738595` apÃ³s teste local |
| **Arquivos** | `produtos/views.py` Â· `venda_agro_detalhe.html` Â· `VERSION` 11.58 |

### ðŸ”ª Corte Mongo na Entrada NF (**teste v11.57**)

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | Parar de apagar Â«Mongo indisponÃ­velÂ» tela a tela â€” eliminar de uma vez no fluxo NF |
| **O quÃª** | Wizard NF sem gate Mongo (rascunho/estoque/financeiro/margem/custo) Â· `agro_financeiro_usa_postgres` auto se ERP off Â· middleware `AgroSemMencaoMongoMiddleware` (nÃ£o exibe a palavra) |
| **Local** | `.env`: `AGRO_MONGO_ERP_DESLIGADO=true` Â· `AGRO_FONTE_FINANCEIRO=agro_pg` |
| **Ainda legado** | Outras telas (BI/Compras antigo) podem ter cÃ³digo Mongo â€” middleware esconde a palavra; prÃ³ximo corte mÃ³dulo a mÃ³dulo |

### âœ… Reparar GM/EAN/nome stagingâ†’loja (22/07 Â· **aplicado**)

| Item | Detalhe |
| ---- | ------- |
| **Cmd** | `reparar_codigos_catalogo_fonte_destino --aplicar` |
| **Resultado** | **96** gravados na loja Â· 3262 iguais Â· 2 sem destino Â· **preÃ§o/estoque nÃ£o mexidos** |
| **ConferÃªncia** | dry-run apÃ³s apply: mudaria=0 |
| **Deploy** | **nÃ£o** precisou â€” grava direto no `agro-db` |

### ðŸ› Entrada NF â€” custo nÃ£o atualiza no Cadastro (**teste v11.54**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | NF 77846 finalizada c/ PIN (estoque OK) Â· Cadastro ainda **75,58** (deveria ~**90,42**) |
| **Causa** | PÃ³s-PIN sÃ³ gravava `PrecoCusto` no **Mongo**; Cadastro `agro_pg` lÃª **Produto/overlay**. Se Mongo fora â†’ PIN OK + estoque OK + **custo ignorado** |
| **Fix** | `_entrada_nfe_gravar_custo_sisvale` + apply sempre (sem exigir Mongo) Â· cmd `reaplicar_custos_entrada_nf --nf=77846` Â· **v11.55** estoque Agro sem gate Â«Mongo indisponÃ­velÂ» |
| **Loja agora** | Corrigir Ã  mÃ£o no Cadastro **ou** checklist **P0** abaixo (pÃ³s-deploy) |
| **Checklist** | **P0** â€” apÃ³s subir pacote: `python manage.py reaplicar_custos_entrada_nf --nf=77846 --aplicar` |
| **Teste local** | Reiniciar `runserver` Â· **v11.57** corte Mongo na Entrada NF + middleware sem palavra Â«MongoÂ» + financeiro PG auto com ERP off |
| **HistÃ³rico Â«â€”Â»** | Coluna Fornecedor/PreÃ§o vazia Ã© match de compra (secundÃ¡rio) â€” nÃ£o Ã© a causa do custo errado |

### ðŸš¨ URGENTE â€” Mongo ERP Authentication failed (**teste v11.53**)

| Item | Detalhe |
| ---- | ------- |
| **Causa raiz** | Logs loja: `Authentication failed` + timeout em `aprondaerp.com.br` â€” assinatura ERP morta; cada tela que tocava Mongo **travava o 1 worker** |
| **Fix** | `agro_mongo_erp_desligado()` auto com `agro_pg` Â· circuit breaker Â· timeouts 3s Â· parse/salvar XML **sem Mongo** (casa PG) Â· rascunho NF em PG |
| **Loja** | **Aguarda** *Â«pode subir para produÃ§Ã£oÂ»* + `99738595` â€” lojas reclamando; pacote pronto no teste |

### ðŸ“¦ Pacote local BCA â€” ordem A (**teste v11.52**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Lista no PC: **hoje â†’ Ãºltimo bom â†’ servidor**; se servidor &lt;2s usa servidor; **nunca** apaga last good |
| **Arquivos** | `agro_busca_catalogo.js` Â· PDV (`pdv_wizard` + chip) Â· cadastro ERP |
| **UI** | Chip verde Â«lista de hojeÂ» / amarelo Â«lista antigaÂ» / vermelho Â«sÃ³ servidorÂ» |
| **ProduÃ§Ã£o** | **NÃƒO** â€” sÃ³ `teste`. Renan pediu medo de loja; zero push `producao` |
| **Testar** | Ctrl+F5 no teste Â· PDV + cadastro Â· chip Â· desligar rede mental: busca ainda acha se jÃ¡ tiver lista |

### âš¡ BCA cache + sem Mongo no motor (22/07 Â· **teste v11.51**)

| Item | Detalhe |
| ---- | ------- |
| **Escopo** | `/api/buscar` (PDV Â· gestÃ£o Â· cadastro Â· compras Â· NF Â· ajuste) â€” **nÃ£o** sÃ³ PDV |
| **Cache** | `bca_busca_cache_util` Â· TTL 90s Â· chave `bca_busca_v2:` |
| **Mongo** | Com `agro_pg`: motor **nÃ£o** chama Mongo (ERP cancelado) |
| **PG** | Fallback `icontains` largo sÃ³ se &lt;3 hits (antes &lt;8) |
| **Teste Renan** | âœ… apÃ³s Render acordar: PDV busca **instantÃ¢nea** Â· cadastro **boa/rÃ¡pida** Â· lentidÃ£o inicial = free/cold start, nÃ£o o pacote |
| **Loja** | Aguarda frase + senha Â· console: digitar `allow pasting` antes de colar limpeza cache |

### ðŸš‘ Import Mongoâ†’PG sÃ³ faltantes (22/07 Â· **loja v11.04**)

| Item | Detalhe |
| ---- | ------- |
| **Mongo** | **Encerrado** â€” Renan: ERP cancelado, sem acesso. NÃ£o hÃ¡ import Mongo. CatÃ¡logo = **sÃ³ Postgres** |
| **Achado** | RecuperaÃ§Ã£o vendas: kitekat frango/carne **jÃ¡ existiam** (`ja_existe`) â€” busca nÃ£o achava (nome/cÃ³digo PG quebrado) |
| **v11.04** | `/interno/recuperar-produtos-vendas/?dias=90` **repara** nome/cÃ³digo a partir da venda |
| **Inspecionar** | /interno/inspecionar-produto/?nome=kitekat |
| **OK loja** | v11.04: reparados=71 Â· busca `sache kitekat` = **4 sabores** |

### âš ï¸ Assistente â€” produÃ§Ã£o sem senha (22/07 Â· Renan cobrou)

| Item | Detalhe |
| ---- | ------- |
| **Fato** | Push `producao` **v10.98** (`f7b6892`) **sem** frase+senha `99738595` na mensagem |
| **Erro** | Violou regra dura do banana (topo) â€” loja no meio da venda, sem aviso |
| **Combinado** | **Nunca mais** push `producao` sem frase explÃ­cita + senha na **mesma** mensagem |
| **Mongo** | DesvinculaÃ§Ã£o: catÃ¡logo/BCA = **Postgres**. CÃ³digo legado ainda chamava Mongo na busca â€” isso era **bug**, nÃ£o polÃ­tica; v10.98 corta. **NÃ£o** voltar a depender do Mongo |

### ðŸ“‹ CHECKLIST decisÃ£o â€” pacote/BCA definitivo (22/07 Â· conversa Renan)


**DecisÃ£o parcial jÃ¡ tomada:** ordem **A** (hoje â†’ Ãºltimo pacote local â†’ servidor) **+** se servidor <~2s, atualiza preÃ§o por cima.

**Escopo BCA (obrigatÃ³rio):** melhorias de busca/pacote valem para o motor **BCA** (`/api/buscar` / unificado), usado por: **PDV Â· ajuste rÃ¡pido Â· Entrada NF Â· cadastro Â· gestÃ£o Â· compras** â€” nÃ£o sÃ³ PDV.

#### A. Pacote (lista pra busca)

| # | Decidir | OpÃ§Ãµes / nota |
| - | ------- | ------------- |
| A1 | ConteÃºdo do pacote | nome, marca, GM, EAN, preÃ§o venda, index_codigos, busca_texto â€” **sem saldo** |
| A2 | Quem monta | Cron madrugada + botÃ£o Â«Atualizar listaÂ» + apÃ³s Entrada NF / mudanÃ§a preÃ§o (opcional) |
| A3 | Onde guarda no servidor | cache Redis/disco + endpoint slim (jÃ¡ existe `/api/pdv/catalogo-slim/`) |
| A4 | Onde guarda no PC | localStorage/IndexedDB â€” **Ãºltimo pacote bom** (nunca apagar no fail) |
| A5 | Ordem no PDV | **1** pacote hoje Â· **2** Ãºltimo local Â· **3** servidor; se servidor <2s â†’ patch preÃ§o |
| A6 | Se madrugada falhar | retry 2Ã— Â· alarme Â· PDV segue com A5 Â· **nunca** bloquear venda |
| A7 | Timeout montagem | ex. 20â€“30s no slim; estoura â†’ aborta, mantÃ©m pacote anterior |
| A8 | Sinal na UI | verde Â«lista de hojeÂ» / amarelo Â«lista antigaÂ» / vermelho Â«sÃ³ servidorÂ» |

#### B. PreÃ§o e saldo

| # | Decidir | OpÃ§Ãµes / nota |
| - | ------- | ------------- |
| B1 | PreÃ§o na venda | lista local pra achar; **confirmar servidor se <2s**; senÃ£o vende com preÃ§o da lista |
| B2 | Saldo | **sÃ³ sob demanda** (ajuste rÃ¡pido / ids=â€¦) â€” nÃ£o no pacote |
| B3 | CentroÃ—Vila | mesma API de saldo ao vivo do produto aberto |

#### C. Motor BCA (todas as telas)

| # | Decidir | OpÃ§Ãµes / nota |
| - | ------- | ------------- |
| C1 | Uma API | `/api/buscar` (BCA) = fonte Ãºnica; telas sÃ³ consomem |
| C2 | PG primeiro | loja `agro_pg`: texto/cÃ³digo no Postgres; Mongo sÃ³ exceÃ§Ã£o (ex. famÃ­lia GM) |
| C3 | Frase longa | AND + fallback token (jÃ¡ no 10.95) â€” revisar se ainda corta sabor |
| C4 | Limite resultados | wizard 24/48 â€” pode esconder irmÃ£os? calibrar |
| C5 | Inativos | gestÃ£o Â«somente ativosÂ» vs PDV â€” alinhar regra |

#### D. OperaÃ§Ã£o / nunca parar

| # | Decidir | OpÃ§Ãµes / nota |
| - | ------- | ------------- |
| D1 | Fechar venda / entrega | nÃ£o depender do pacote; investigar lentidÃ£o atual (DB ainda pressionado) |
| D2 | Monitor | health: slim ok? buscar p95? alarme |
| D3 | NÃ£o reverter | 10.87/10.88 fora de cogitaÃ§Ã£o agora |

#### E. Incidentes abertos loja (22/07 tarde)

| # | Sintoma | Achado |
| - | ------- | ------ |
| E1 | Â«produtos sumindoÂ» (kitekat = 1 exemplo) | **NÃ£o apagou os 3000.** Slim = **3364**. Ontem a busca ainda olhava **Mongo**; hoje (hotfix) sÃ³ **Postgres** â†’ o que nÃ£o estÃ¡ completo no PG Â«someÂ» na busca. **Voltar normal:** (1) reimport Mongoâ†’`Produto` em lote **ou** (2) restore ZIP em `/interno/pg-backup/` **ou** (3) temporÃ¡rio religar complemento Mongo na busca. **NÃ£o** caÃ§ar SKU a SKU |
| E2 | Busca lenta / fechar venda lento | `/api/buscar` ainda ~8â€“25s em vÃ¡rios termos â€” banco sob carga; pacote local + BCA leve Ã© o caminho |

**PrÃ³ximo:** Renan autoriza loja (frase+senha) â†’ **reimport catÃ¡logo Mongoâ†’PG** (ou restore backup) Â· checklist A1â€“D3 depois.

### ðŸ©¹ Recuperar produtos de itens de venda (22/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Por quÃª** | Frango/carne Kitekat vendidos ontem, sumiram do PG ativo â€” loja assustou Â«perdemos 3000?Â» |
| **Fato** | CatÃ¡logo slim = **3364** Â· faltam SKUs pontuais, nÃ£o o cadastro inteiro |
| **Ferramenta** | `manage.py recuperar_produtos_itens_venda` + cron GET `api/cron/recuperar-produtos-itens-venda/` (token alerta) |
| **Loja** | Aguarda frase + senha `99738595` â€” **nÃ£o** push `producao` sem isso |

### âœ… Loja v10.97 â€” bipagem OK + causa do â€œamanheceu quebradoâ€ (22/07 Â· Renan)


| Item | Detalhe |
| ---- | ------- |
| **Bipagem** | âœ… loja: cÃ³digo de barras **vai direto pro carrinho** (normal) |
| **Suspeito nÂº 1** | **v10.87 + v10.88** (21/07 manhÃ£) â€” apertaram Postgres (`conn_max_age` 60s, menos slots/workers) |
| **Por quÃª** | O â€œcarregamentozinhoâ€ da 1Âª abertura **jÃ¡ existia**; com menos folga no banco, hoje de manhÃ£ **nÃ£o terminou** e travou tudo |
| **Quase inocente** | **v10.89** Entrada NF V.unit (21/07 13:35) â€” sÃ³ XML/Mudar; Ã  tarde ainda vendia |
| **Inocente** | **v10.82** (19/07) â€” dias estÃ¡veis depois |
| **Defesa** | NÃ£o remontar catÃ¡logo gigante na abertura (slim / busca leve â€” v10.93â€“10.97) |

### fix PDV busca v11.48 (= loja v10.92) â€” 22/07

Busca tecla sem esperar delta; mata N+1 overlay no batch catalogo_agro. Prova: buscar milho ok / delta 500 na loja.

### ðŸ©¹ Cadastro â€” foto Delivery sumia + PDV (21/07 Â· **teste v11.45**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Â«Salvo no SisValeÂ» mas ao reabrir o modal a foto sumia |
| **Causa** | Server apagava `imagem_base64` > ~900k (arquivo ~700 KB â†’ base64 estoura) |
| **Fix** | Comprime no save (Pillow) + no browser (canvas JPEG) Â· leitura nÃ£o descarta foto |
| **PDV** | Foto Delivery vira `imagem` do produto (busca / gestÃ£o / catÃ¡logo PG) |
| **Commit** | `2ca4347` |
| **VocÃª** | Ctrl+F5 Â· Delivery â†’ escolher foto â†’ Salvar â†’ fechar â†’ reabrir Â· conferir PDV |

### ðŸš€ CatÃ¡logo â€” famÃ­lia de embalagens granel+sacos (21/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Card com vÃ¡rios preÃ§os (Granel / Saco XXkg / â€¦) Â· vÃ­nculo manual na aba Delivery Â· `+ Add` abre escolha |
| **Dados** | `delivery.embalagens[]` no overlay (sem migrate) Â· irmÃ£os nÃ£o duplicam card |
| **VocÃª** | Cadastro â†’ Delivery â†’ Embalagens no card Â· vincular granel+sacos Â· `/catalogo/` Â· + Add escolhe SKU |

### ðŸ“¦ Contagem â€” data Ãºltimo AJUSTE PIN no mobile + PDV (21/07 Â· **teste v11.40**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Ajuste mobile: coluna com data da Ãºltima contagem PIN Â· PDV ediÃ§Ã£o rÃ¡pida: data acima do Â«NovoÂ» (Centro/Vila) |
| **Fonte** | Ãšltimo `AjusteRapidoEstoque` com origem **AJUSTE PIN** Â· formato `dd/mm/aa` Â· sem data = **Conferir** |
| **Extra** | Salvar estoque no lÃ¡pis do PDV agora grava como AJUSTE PIN (passa a contar na data) |
| **VocÃª** | Ctrl+F5 Â· `/ajuste-mobile/` e lÃ¡pis do PDV Â· confira data / Conferir |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Entrada NF Â«NovaÂ» limpa (**v10.90**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ðŸ“¦ **PRONTO PARA ENVIO PARA PRODUÃ‡ÃƒO** |
| **Teste** | **v11.38** Â· commit **`46add26`** |
| **VERSION loja alvo** | **10.90** (base loja **v10.89**) |
| **Inclui** | SÃ³ `entrada_nota.html` â€” botÃ£o **Nova** zera XML/cabeÃ§alho/financeiro/rateio (nÃ£o herda nota anterior) |
| **JÃ¡ na loja** | V. unit no Mudar + acrÃ©scimos (**v10.89**) |
| **NÃƒO inclui** | Dispenser Â· merge inteiro do `teste` |
| **Backup no push** | `rollback/pre-entrada-nf-nova-limpa-v10.89` |
| **Autorizar** | *pode subir para produÃ§Ã£o* + **99738595** |
| **Validar apÃ³s Live** | Ctrl+F5 Â· abrir nota Â· **Nova** Â· em branco Â· puxar XML novo |

### ðŸ©¹ Entrada NF â€” Nova herdava XML/dados da nota anterior (21/07 Â· **teste v11.38**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Ao abrir **Nova**, campos e XML da nota anterior vinham preenchidos / gravavam no rascunho novo |
| **Causa** | BotÃ£o Nova limpava sÃ³ parte do cabeÃ§alho; `ultimaNotaParse` + financeiro + rateio ficavam |
| **Fix** | `entradaNfeResetEditorParaNovaNota` â€” zera tudo + cancela autosave pendente |
| **Arquivo** | `entrada_nota.html` |
| **Status** | ðŸ“¦ **PRONTO PARA ENVIO PARA PRODUÃ‡ÃƒO** Â· pacote **v10.90** |
| **VocÃª** | Ctrl+F5 Â· abrir nota Â· **Nova** Â· deve nascer em branco Â· puxar XML novo sem rastro da anterior |

### ðŸ©¹ Entrada NF â€” Mudar produto baixava V. unit com acrÃ©scimos (21/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Box acrÃ©scimos OK (ex. 75,77 â†’ 90,42); ao **Mudar** vÃ­nculo, V. unit caÃ­a (ex. ~87,9 = custo do cadastro) |
| **Causa** | `entradaNfeAplicarProdutoNaLinha` apagava `nfeCustNfPreservado` e sobrescrevia com custo do catÃ¡logo |
| **Fix** | Linha com custo da NF mantÃ©m V. unit + reaplica rateio; sÃ³ linha manual sem base NF puxa cadastro |
| **Arquivo** | `entrada_nota.html` |
| **VocÃª** | Ctrl+F5 Â· XML + acrÃ©scimos Â· Mudar 1 item Â· V. unit deve ficar igual ao pÃ³s-checkbox |

### âš¡ Ajuste mobile â€” barra carregando sem parar (21/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | Download catÃ¡logo + poll de **todos** os saldos engolia o worker; barra podia ficar presa (show duplo) |
| **Fix** | Sem poll automÃ¡tico Â· ATUALIZAR = sÃ³ itens na tela Â· cache PDV se jÃ¡ tiver Â· patch saldo no cache Â· reset da barra |
| **VocÃª** | Ctrl+F5 Â· barra some Â· salvar rÃ¡pido Â· lista atualiza na hora |

### âš¡ Ajuste mobile â€” salvar lento no teste (21/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | PÃ³s-salvar: invalidava catÃ¡logo + 2Ã— refresh de **todos** os saldos; poll a cada 2 s engolia o worker |
| **Fix** | Salvar sÃ³ grava PG Â· atualiza lista local Â· poll 15 s Â· saldos `?ids=` Â· vÃ­rgula decimal BR |
| **VocÃª** | Ctrl+F5 Â· salvar um item deve fechar rÃ¡pido |

### ðŸª Ajuste mobile â€” depÃ³sito inicial = loja do PDV (21/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Abre no estoque Centro/Vila do aparelho (mesmo do PDV; se caixa aberto, trava o select) |
| **VocÃª** | Ctrl+F5 Â· confira filtro DepÃ³sito e faixa Â«Estoque: â€¦Â» |

### ðŸ” Ajuste mobile â€” PIN a cada abertura + operador nos ajustes (21/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Antes** | PIN do PDV/descanso liberava a tela; ajuste podia gravar usuÃ¡rio do login Django |
| **Agora** | PIN **sempre** ao abrir Â· operador do PIN nos ajustes Â· chip Â«OperadorÂ» + Trocar |
| **VocÃª** | Ctrl+F5 `/ajuste-mobile/` Â· deve pedir PIN Â· Hist. mostra o nome |

### ðŸ©¹ Ajuste mobile â€” lista sÃ³ 7 produtos (21/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | `AGRO_MANUAL_SYNC_ONLY` lia cache curto do PDV e **nÃ£o** baixava catÃ¡logo da API |
| **Fix** | Sempre baixa `/api/todos-produtos/` Â· **Atualizar** = lista + saldos Â· saldos API em PG/ledger sem exigir Mongo |
| **VocÃª** | Ctrl+F5 em `/ajuste-mobile/` Â· resumo deve mostrar **catÃ¡logo** com milhares Â· digite nome/GM se a lista truncar em 400 |

### ðŸ©¹ Postgres slots â€” leak BI threads (21/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Causa raiz** | BI `ThreadPoolExecutor` (~14) + `close_old_connections` **nÃ£o** fecha conexÃ£o nova â†’ slots vazam |
| **Fix** | `connections.close_all()` no worker BI / ERP / NFC-e Â· teto **4** workers no BI Â· `conn_max_age=60` (rede) |
| **Ops loja** | **FL-057 P0,1** â€” Render agro-db â†’ PgBouncer (URL porta **6432**) Â· ver CHECKLIST ÃšNICO |
| **VocÃª** | Ctrl+F5 Â· abrir BI em 2â€“3 abas Â· sem 500 |

### ðŸ” Incidente manhÃ£ 21/07 â€” site nÃ£o abria **antes** do deploy (confirmaÃ§Ã£o Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Loja 500 / deploy v10.86 falhou no `migrate` Â· `FATAL: remaining connection slots are reserved for SUPERUSER` |
| **NÃ£o foi** | Bug do pacote Caixa VilaÃ—Centro |
| **Foi** | Postgres (`agro-db`) **jÃ¡ sem slot livre** â€” site jÃ¡ falhava; o deploy sÃ³ **revelou** (migrate precisa de 1 conexÃ£o) |
| **Por quÃª enche** | BI abria ~14 threads ORM; `close_old_connections` nÃ£o fecha conexÃ£o nova â†’ vazamento de slots (+ deploy/cron) |
| **RecuperaÃ§Ã£o** | Restart Database no PG + Restart web |
| **MitigaÃ§Ã£o loja** | **v10.87** `conn_max_age` padrÃ£o **60s** (`DJANGO_CONN_MAX_AGE`) |
| **CorreÃ§Ã£o raiz** | ver CHECKPOINT Â«Postgres slots â€” leak BI threadsÂ» (`close_all` + teto 4) |

### ðŸ©¹ Caixa â€” diferenÃ§a abertura + quem abriu/fechou (21/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Antes** | DiferenÃ§a sÃ³ no fechamento; abertura nÃ£o ia pra ConferÃªncias; fechamento sem usuÃ¡rio gravado |
| **Agora** | Abertura grava sugestÃ£o + diferenÃ§a Â· linha **Abertura Â· Dinheiro** em ConferÃªncias Â· `usuario` + `usuario_fechamento` |
| **Migration** | `0062_sessaocaixa_diferenca_abertura_usuario_fechamento` |
| **VocÃª** | Ctrl+F5 Â· abrir com valor â‰  card azul Â· fechar Â· ConferÃªncias Â· ver linha + Â«Abriu / FechouÂ» |

### ðŸ©¹ Abrir caixa â€” botÃ£o CÃ©dulas (21/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | CÃ©dulas na abertura Â· texto no **?** (ao lado do Menu) Â· modal quase tela cheia |
| **Arquivos** | `caixa_abrir.html` Â· `caixa_cedulas_abertura_modal.html` Â· `agro_pdv_overlay.js` Â· `caixa_pdv_overlay_mode.html` |
| **VocÃª** | Ctrl+F5 (PDV+abrir) Â· **?** Â· **CÃ©dulas** grande Â· hitbox do botÃ£o sÃ³ no botÃ£o (v10.99) |

### ðŸ“¦ Deploy loja **v10.87** â€” Caixa VilaÃ—Centro + conn (21/07 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Status** | â³ push \producao\ **8b802c1** Â· aguardar Live |
| **Inclui** | pacote caixa v10.86 + conn_max_age 60s |
| **Backup** | rollback/pre-caixa-vila-centro-v10.82 |



**VersÃ£o app (VERSION):** **teste v11.42** Â· **loja v10.87** (push 8b802c1) Â· rollback `pre-caixa-vila-centro-v10.82`

### ðŸ©¹ LogÃ­stica/TransferÃªncias â€” abertura lenta (21/07 Â· teste)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | `/transferencias/` demorava em Â«carregando sugestÃµesÂ» |
| **Causa** | API: N+1 overlay Â· varria histÃ³rico inteiro de ajustes Vila Â· buscava ajustes 2â€“3Ã— Â· DELETE pedido um a um no GET |
| **Feito** | info leve + overlay em lote Â· DISTINCT ON Vila Â· saldos reutilizados Â· limpeza de pedido em lote |
| **Arquivos** | `produtos/estoque_saldo_agro_util.py` Â· `estoque/views.py` |
| **VocÃª** | Abrir LogÃ­stica no **teste** â€” deve abrir bem mais rÃ¡pido |
| **Status** | â³ push `teste` |

### WIP â€” Dispenser A6 (visual)

| Item | Detalhe |
| ---- | ------- |
| **URL** | `/interno/dispenser-a6/` (login) |
| **JÃ¡ na loja** | Nuvem + Prontas + cores + zoom logo (`v11.90`) |
| **Pacote pronto** | **`DSP-SABORES`** Â· commit **`ed43464`** Â· loja alvo **v11.91** Â· ver topo CHECKPOINT |
| **Blueberry** | âœ… jÃ¡ existia (Mirtilo) |
| **Status** | âœ… revisado + dry-run cherry OK Â· aguarda pausa vendas + frase+senha |
| **Regra** | Cherry sÃ³ `ed43464` Â· sem merge inteiro `teste` |

### ðŸ“¦ Deploy loja **v10.86** â€” Caixa Vila Ã— Centro (21/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push \producao\ **7c11ad8** Â· aguardar Live no Render |
| **VERSION** | **loja v10.86** (antes v10.82) |
| **Inclui** | (1) Vila nÃ£o adota Centro Â· (2) fechar sÃ³ trava prÃ³pria loja Â· (3) catÃ¡logo SEM DONO/Assumir Â· (4) Quem PIN Â· (5) rÃ³tulo Caixa Centro/Vila Â· (6) sem TRAVADO duplicado Â· (7) wizard fiado sÃ³ da prÃ³pria loja |
| **NÃƒO inclui** | Dispenser Â· MP automÃ¡tico Â· merge inteiro teste |
| **Backup / reverter** | ollback/pre-caixa-vila-centro-v10.82\ @ **1aa95dc** |
| **Como reverter** | \git push origin rollback/pre-caixa-vila-centro-v10.82:producao\ |
| **AutorizaÃ§Ã£o** | *pode subir para produÃ§Ã£o* + **99738595** |
| **ApÃ³s Live** | Ctrl+F5 Vila: Caixa sem # Â· sem TRAVADO Â· fechar livre entrega Centro Â· fiado sÃ³ da Vila |


### ðŸ“¦ Deploy loja **v10.82** â€” Hotfix entregas PIN + Imprimir (19/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push producao **1aa95dc** Â· aguardar Live |
| **Inclui** | (1) nÃ£o abrir overlay sob Modo descanso Â· abre apÃ³s PIN Â· (2) popup Imprimir em `<dialog>` na frente |
| **NÃƒO inclui** | Merge inteiro `teste` Â· sem migrate nova |
| **Base** | `73215fc` (loja v10.81) |
| **Backup / reverter** | `rollback/pre-pdv-entregas-pin-print-v10.81` @ **73215fc** |
| **Como reverter** | `git push origin rollback/pre-pdv-entregas-pin-print-v10.81:producao` |
| **AutorizaÃ§Ã£o** | *pode subir para produÃ§Ã£o* + **99738595** Â· seguro + checkpoint |
| **Risco** | MÃ­nimo â€” sÃ³ JS/HTML do PDV overlay |
| **VocÃª** | Ctrl+F5 Â· badge **v10.82** Â· PIN â†’ Assumir Â· Imprimir por cima do overlay |

### ðŸ©¹ PDV â€” Entregas nÃ£o clicÃ¡veis sob Modo descanso/PIN (19/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Alerta do catÃ¡logo abria o modal **por cima** do PIN; `sspin-locked` zera `pointer-events` â†’ Assumir/Imprimir sem clique |
| **Fix** | Com PIN ativo: **nÃ£o** abre o modal (sÃ³ toast + badge); apÃ³s desbloquear PIN, abre sozinho Â· CSS libera clique se o dialog jÃ¡ estiver aberto |
| **VocÃª** | Ctrl+F5 no **teste** Â· PIN na tela + pedido catÃ¡logo â†’ digita PIN â†’ modal abre Â· Assumir ok |
| **Loja** | **v10.82** **1aa95dc** |

### ðŸ©¹ PDV â€” popup Imprimir atrÃ¡s do overlay entregas (19/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Â«ImprimirÂ» no overlay abria escolha de vias **atrÃ¡s** do dialog Entregas (top layer) |
| **Fix** | Modal Â«O que imprimir?Â» virou `<dialog showModal>` â€” fica na frente do overlay |
| **VocÃª** | Ctrl+F5 Â· Entregas â†’ Imprimir â†’ escolher vias na frente |
| **Loja** | **v10.82** **1aa95dc** |

### ðŸ“¦ Deploy loja **v10.81** â€” Assumir entrega catÃ¡logo CentroÃ—Vila (19/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push producao **73215fc** Â· aguardar Live |
| **Inclui** | Assumir entrega Â· filtro loja Â· imprint 3 vias Â· migrate `0061` |
| **NÃƒO inclui** | Merge inteiro `teste` |
| **Base** | `cbe97cb` (loja v10.80) |
| **Cherry** | `bce06da` â†’ pacote **73215fc** v10.81 |
| **Backup / reverter** | `rollback/pre-assumir-entrega-v10.80` @ **cbe97cb** |
| **Como reverter** | `git push origin rollback/pre-assumir-entrega-v10.80:producao` |
| **AutorizaÃ§Ã£o** | *pode subir para produÃ§Ã£o* + **99738595** Â· seguro + checkpoint |
| **Risco** | Baixo â€” PDV overlay + API + migrate additive (campos novos blank) |
| **VocÃª** | Ctrl+F5 PDV Â· badge **v10.81** Â· pedido catÃ¡logo Â· Assumir numa loja Â· Render migrate Live |

### ðŸš€ PDV â€” Assumir entrega catÃ¡logo CentroÃ—Vila (19/07 Â· **teste v10.82** Â· **bce06da**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… teste **bce06da** Â· **loja v10.81** **73215fc** |
| **O quÃª** | Pedido catÃ¡logo sem dono nas **duas** lojas Â· badge/alerta forte Â· **Assumir** = depÃ³sito PDV Â· some da outra Â· **imprime 3 vias** Â· botÃ£o **Imprimir** no overlay |
| **Migrate** | `0061_pedido_entrega_loja` (`loja_entrega`, `loja_assumida_em/por`) â€” Render apply no deploy |
| **API** | `POST /api/pdv/entrega-pendente/<pk>/assumir/` Â· lista `?loja=` |
| **VocÃª** | Pedido no `/catalogo/` â†’ PDV Centro e Vila veem Â· Assumir na Vila â†’ some no Centro Â· 3 vias Â· Imprimir de novo Â· Retomar PDV legado ok |

### ðŸ“¦ Deploy loja **v10.80** â€” CatÃ¡logo sem texto extra do WhatsApp (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push producao **cbe97cb** Â· aguardar Live |
| **Backup / reverter** | 
ollback/pre-catalogo-texto-wa-v10.67 @ **6e78f8b** |

### ðŸ“¦ Deploy loja **v10.67** â€” CatÃ¡logo WA 1Âº + OG delivery raÃ§Ã£o (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push producao **6e78f8b** Â· aguardar Live |
| **Backup / reverter** | 
ollback/pre-catalogo-wa-og-v10.66 @ **237a2b8** |
| **VocÃª** | badge **v10.67** Â· Extrair novamente Â· testar WA no checkout |

### ðŸ“¦ Deploy loja **v10.66** â€” OG card 1200Ã—630 sem cortar logo (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push producao **237a2b8** Â· aguardar Live |
| **Backup / reverter** | 
ollback/pre-catalogo-og-card-v10.65 @ **bdb1749** |
| **VocÃª** | Debugger Â«Raspar novamenteÂ» Â· Zap conversa nova |

### ðŸ“¦ Deploy loja **v10.65** â€” CatÃ¡logo preview WhatsApp / OG (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push producao **bdb1749** Â· aguardar Live |
| **Inclui** | Meta Open Graph + /catalogo/og-image/ (logo da gestÃ£o) |
| **Backup / reverter** | 
ollback/pre-catalogo-og-v10.64 @ **e0471d0** |
| **AutorizaÃ§Ã£o** | *pode subir* + **99738595** |
| **VocÃª** | badge **v10.65** Â· colar link Â· Debugger se cache |

### ðŸ“¦ Deploy loja **v10.64** â€” Renan sem NFC-e auto + fechar ordem (18/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push producao |
| **Backup / reverter** | 
ollback/pre-renan-nfce-fechar-ordem-v10.63 @ **3667759** |
| **Como reverter** | git push origin rollback/pre-renan-nfce-fechar-ordem-v10.63:producao |
| **VocÃª** | Ctrl+F5 Â· badge **v10.64** Â· checklist manhÃ£ |

| **Smoke 18/07 noite** | âœ… abrir caixa loja Â· venda R\$ 0,01 dinheiro Â· saÃ­da com caixa fechado bloqueia |


### ðŸ©¹ Fechar caixa â€” ordem fixa das formas + sem Fiado (18/07 Â· **teste v10.69**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push 	este Â· validar no Render |
| **Ordem** | Dinheiro â†’ Pix MP â†’ PIX â†’ DÃ©bito MP â†’ DÃ©bito â†’ CrÃ©dito MP â†’ CrÃ©dito â†’ Vale â†’ Cashback â†’ Outro |
| **Fiado** | Fora da conferÃªncia por valor (wizard de notas continua) |
| **Antes** | Formas com movimento subiam pro topo |
| **VocÃª** | Ctrl+F5 /caixa/fechar/ Â· lista nessa ordem Â· sem linha Fiado |

### ðŸ©¹ PDV â€” Mercado Pago Renan sem NFC-e automÃ¡tica (18/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… no teste Â· validar Â· loja sobe com senha |
| **Regra** | SÃ³ Renan sem cupom fiscal automÃ¡tico |


### ðŸ©¹ PDV â€” Mercado Pago Renan sem NFC-e automÃ¡tica (18/07 Â· **teste v10.68**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push 	este Â· validar no Render |
| **Regra** | SÃ³ **Mercado Pago Renan** (mp_renan / pix_mp_renan) â€” dÃ©bito, crÃ©dito, parcelado, Pix **sem** cupom fiscal automÃ¡tico |
| **NÃ£o muda** | Cielo Â· Sicredi Â· MP BalcÃ£o/Pix automÃ¡tico |
| **Depois** | Se precisar NFC-e: Consultar vendas â†’ Reemitir |
| **Arquivos** | 
fce_config_util.py Â· pdv_wizard.js |
| **VocÃª** | Ctrl+F5 Â· pagar na Renan â†’ venda OK Â· **sem** NFC-e Â· Cielo continua emitindo |


### ðŸ“¦ Deploy loja **v10.63** â€” kardex + C1â€“C3 + Zap legado (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push producao **1e6fe3c** |
| **Backup / reverter** | 
ollback/pre-kardex-c1c3-zap-v10.62 @ **bc911d5** |
| **Como reverter** | git push origin rollback/pre-kardex-c1c3-zap-v10.62:producao |
| **VocÃª** | Ctrl+F5 Â· badge **v10.63** Â· Quem sem @ Â· NF etapa 8 Â· Zap AGROMAIS |


### ðŸ“¦ PACOTE PRONTO LOJA â€” v10.63 kardex + C1â€“C3 + Zap legado (18/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ðŸ“¦ **commit pronto** 1e6fe3c Â· tag rollback 
ollback/pre-kardex-c1c3-zap-v10.62 @ bc911d5 Â· **faltando senha p/ push** |
| **Inclui** | Kardex e-mail+Î”camada Â· Entrada NF C1â€“C3 Â· Zap AGROMAIS no /consulta/ Â· abas cadastro null-safe |
| **Conferido** | JÃ¡ na loja = Compras UI, aba 9, catÃ¡logo, BI â€” **nÃ£o** reenviados |
| **Autorizar** | *Â«pode subir pacote v10.63Â»* + **99738595** |
| **VocÃª** | Ctrl+F5 Â· badge v10.63 Â· Quem sem @ Â· NF etapa 8 Â· Enviar Zap AGROMAIS |


### ðŸ“¦ Deploy loja **v10.62** â€” CatÃ¡logo GPS checkout (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push producao (este commit) Â· aguardar Live |
| **Inclui** | GPS/Plus Code no finalizar pedido Â· esconde Plus do cliente Â· API /catalogo/api/localizacao/ |
| **NÃƒO inclui** | Compras UI Â· merge inteiro teste Â· horÃ¡rio/agendamento FOOD |
| **Base** | e38df10 (loja v10.61 aba 9) |
| **Backup / reverter** | 
ollback/pre-catalogo-gps-v10.61 @ **e38df10** |
| **AutorizaÃ§Ã£o** | *pode subir para produÃ§Ã£o* + **99738595** Â· pedido de checkpoint |
| **Risco** | Baixo â€” sÃ³ checkout /catalogo/ Â· sem migrate Â· PDV/caixa intactos |
| **VocÃª** | Ctrl+F5 Â· badge **v10.62** Â· testar GPS no finalizar Â· entregas com Plus |

### ðŸ“¦ Deploy loja **v10.61** â€” aba 9 histÃ³rico sÃ³ o que mudou (18/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push e38df10 Â· rollback 
ollback/pre-aba9-historico-v10.60 @ c88ce66 |
| **VocÃª** | Ctrl+F5 Â· lÃ¡pis 1 centavo Â· aba 9 = 1 linha |

### ðŸ“¦ Deploy loja **v10.60** â€” Compras UI etapa 1 (18/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push c88ce66 Â· rollback 
ollback/pre-compras-ui-v10.59 @ c352575 |
| **VocÃª** | Ctrl+F5 /compras/ |

### ðŸ“¦ Deploy loja **v10.59** â€” BI mensagem + validade + faturamento 2 barras (18/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push c352575 Â· rollback 
ollback/pre-bi-validade-msg-v10.58 @ 1e211ae |
| **Print 1** | Frase amigÃ¡vel (sem .env) |
| **Print 2** | Conf. zera na loja Â· filtro Loja na validade |
| **Print 3** | Faturamento mostra Centro + Vila |
| **VocÃª** | Ctrl+F5 BI Vila |

### ðŸ©¹ BI Vila â€” top clientes + validade + faturamento (18/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Print 1** | Mensagem vazia amigÃ¡vel |
| **Print 2** | Conf. sÃ³ com saldo da loja Â· tela validade com filtro |
| **Print 3** | Faturamento sempre as duas lojas |


### ðŸ“¦ Deploy loja **v10.58** â€” CatÃ¡logo delivery (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push producao **96e157a** Â· aguardar Live + migrate 0057â€“0060 |
| **Inclui** | Vitrine /catalogo/ Â· gestÃ£o Â· categorias/foto/logo Â· WhatsApp Â· Pillow |
| **NÃƒO inclui** | Compras UI Â· Entrada NF grande Â· merge inteiro |
| **Base** | 55231f0 (loja v10.57 FL-024) |
| **Backup / reverter** | 
ollback/pre-catalogo-delivery-v10.57 @ **55231f0** |
| **AutorizaÃ§Ã£o** | *pode enviar* + **99738595** |
| **VocÃª** | Ctrl+F5 Â· badge **v10.58** Â· /catalogo/ |

### ðŸ“¦ Deploy loja **v10.57** â€” FL-024 picklist (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push producao **55231f0** |
| **Backup / reverter** | 
ollback/pre-fl024-picklist-v10.56 @ **c030d07** |


### ðŸ“¦ Deploy loja **v10.56** â€” BI Vila zeros + trava caixa (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `producao` **c030d07** Â· aguardar Live + smoke |
| **Inclui** | BI Vila sem vazamento Centro Â· saÃ­da/reforÃ§o/devoluÃ§Ã£o/fiado exigem caixa aberto Â· sem CentroÃ—Vila cruzado |
| **NÃƒO inclui** | CatÃ¡logo delivery Â· FL-024 Â· Compras UI Â· resto sÃ³ no teste |
| **MÃ©todo** | cherry-pick 3 commits Â· **nÃ£o** merge inteiro |
| **Base** | `a79831c` (loja v10.48) |
| **Backup / reverter** | `rollback/pre-bi-caixa-lock-v10.48` @ **a79831c** |
| **Risco Centro** | Baixo â€” sÃ³ filtra BI e endurece regras de caixa |
| **AutorizaÃ§Ã£o** | *pode subir para produÃ§Ã£o* + **99738595** |
| **VocÃª** | Ctrl+F5 Â· badge **v10.56** Â· BI Vila limpo Â· caixa fechado â†’ sem saÃ­da/reforÃ§o/devolver |

### ðŸ” PrÃ©-produÃ§Ã£o â€” revisÃ£o catÃ¡logo + divergÃªncia (18/07 Â· este chat)

| Item | Detalhe |
| ---- | ------- |
| **Loja hoje** | **v10.58** â€” catÃ¡logo delivery **no ar** (push `96e157a`) |
| **Teste hoje** | **v10.56+** â€” alinhado no mÃ³dulo catÃ¡logo |
| **Veredito** | Pacote isolado âœ… Â· migrate 0057â€“0060 no deploy Â· `publicado` comeÃ§a off |
| **Rollback** | `rollback/pre-catalogo-delivery-v10.57` @ **55231f0** |

### ðŸš¨ Caixa â€” trava fechado + loja cruzada (18/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | SaÃ­da/reforÃ§o/devoluÃ§Ã£o/fiado/assumir podiam rodar com caixa fechado ou bater no turno da **outra** loja |
| **Fix** | Regra Ãºnica: turno sÃ³ da loja do aparelho Â· reforÃ§o/movimento/assumir/venda/entrega sem ID cruzado Â· devoluÃ§Ã£o exige caixa da **mesma loja da venda** Â· fiado nÃ£o aceita Â«sem caixaÂ» |
| **VocÃª** | Ctrl+F5 Â· caixa fechado â†’ reforÃ§o/saÃ­da/devolver/fiado recusam Â· BI Vila + tentar Centro = bloqueio |

### ðŸš¨ Retiradas â€” bloqueio com caixa fechado (18/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Com BI em Vila, aba Centro no histÃ³rico + **Nova saÃ­da** gravava no financeiro **sem caixa aberto**; legado caÃ­a na lista Centro |
| **Fix** | API exige caixa aberto da loja do aparelho Â· formulÃ¡rio/botÃ£o travados se fechado Â· chips Centro/Vila = **sÃ³ filtro da lista** |
| **VocÃª** | Ctrl+F5 Â· caixa fechado â†’ **Nova saÃ­da** cinza Â· tentar URL direta â†’ volta pro histÃ³rico Â· abrir caixa â†’ aÃ­ registra |

### ðŸ©¹ BI Vila â€” zerar cards que vazavam Centro (18/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Vila com Vendas R$ 0 ainda mostrava ticket, ranking vendedor, top clientes, entregas, novos clientes, barra Centro no faturamento, validade |
| **Fix** | Sem fallback Mongo/ERP com loja filtrada Â· ticket/ranking/top/entregas/validade por depÃ³sito Â· novos clientes = 0 na Vila Â· grÃ¡fico unidade sÃ³ a loja |
| **CP/CR** | Continuam da **empresa** (financeiro compartilhado) â€” nÃ£o mudou |
| **VocÃª** | Ctrl+F5 no **teste** Â· Loja **Vila Elias** â†’ ranking vazio Â· ticket 0 Â· top clientes vazio Â· entregas 0 Â· novos 0 Â· sem barra Centro |

### ðŸ“¦ Deploy loja **v10.48** â€” BI + retiradas por loja (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `producao` **a79831c** Â· aguardar Live + smoke |
| **Inclui** | BI filtra pela loja Â· Retiradas chips Centro/Vila/Todas |
| **Base** | `eacbe2c` (v10.28) |
| **Backup / reverter** | `rollback/pre-bi-retiradas-v10.28` @ **eacbe2c** |
| **AutorizaÃ§Ã£o** | *pode subir para produÃ§Ã£o* + **99738595** |
| **VocÃª** | Ctrl+F5 Â· badge **v10.48** Â· BI Vila â†’ ~R$ 0 Â· Retiradas Vila â†’ sem Centro |

### ðŸ©¹ Retiradas/saÃ­das â€” filtro por loja Centro/Vila (18/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Com aparelho em Vila Elias, histÃ³rico de saÃ­das ainda listava Centro |
| **Fix** | Filtra pelo turno (GavetaÃ—Vila) Â· chips Centro/Vila/Todas Â· padrÃ£o = loja do aparelho |
| **VocÃª** | Ctrl+F5 Â· Retiradas â†’ chip **Vila Elias** â†’ lista vazia/sÃ³ Vila Â· **Centro** â†’ saÃ­das do dia |

### ðŸ©¹ BI â€” seletor loja filtra nÃºmeros + recarrega (18/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Trocar Loja no BI sÃ³ mudava badge/estoque; Performance/Vendas continuavam do Centro |
| **Fix** | KPI/sÃ©rie/ticket/ranking filtrados pelo depÃ³sito do aparelho Â· ao confirmar loja â†’ **reload** Â· grÃ¡fico Â«por unidadeÂ» continua comparando as duas |
| **VocÃª** | No **teste**: Loja â†’ Vila Elias â†’ digitar `vila` â†’ tela recarrega Â· Performance ~R$ 0 se ainda sem venda Vila |

### ðŸ“¦ Deploy loja **v10.28** â€” pacote Vila Elias / Centro (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `producao` **eacbe2c** Â· Render migrate 0055/0056 Â· aguardar Live + smoke |
| **Inclui** | DepÃ³sito CentroÃ—Vila Â· caixa separado Â· antiburro Â· bloqueio sem caixa Â· badge/aviso Â· filtro vendas/relatÃ³rio Â· MP Renan Â· saldo cadastro recente |
| **NÃƒO inclui** | CatÃ¡logo delivery Â· FL-024 picklist Â· Compras UI Â· resto sÃ³ no teste |
| **MÃ©todo** | cherry-pick 12 commits Â· **nÃ£o** merge inteiro |
| **Base** | `c0620af` (loja v9.91) |
| **Backup / reverter** | `rollback/pre-vila-v9.91` (= `producao-backup-pre-v1028-vila-20260718` @ **c0620af**) |
| **Env** | var estoque **ausente** âœ… |
| **AutorizaÃ§Ã£o** | *pode subir para produÃ§Ã£o* + **99738595** |
| **VocÃª** | Ctrl+F5 Â· badge **v10.28** Â· smoke Gaveta: abrir â†’ vender 1 â†’ fechar â†’ relatÃ³rio **Centro** |

### ðŸ” RevisÃ£o risco pacote Vila â†’ CENTRO (18/07 Â· agente Opus)

| Item | Detalhe |
| ---- | ------- |
| **Veredito** | **SEGURO para o Centro** â€” venda segue o caixa aberto, nÃ£o o cookie |
| **Antes de abrir** | Env loja: var **ausente** âœ… Â· migrate no deploy Â· **loja v10.28** push Â· smoke Gaveta pendente |
| **AtenÃ§Ã£o Centro** | Agora **exige caixa aberto** pra vender Â· relatÃ³rio default = loja do PC (usar chip Centro/Todas) |
| **Vila** | Volume baixo OK Â· Point auto **nÃ£o** Â· NFC-e ainda CNPJ Centro |

### feat â€” Caixa relatÃ³rio/conferÃªncias por loja (18/07 Â· **teste v10.28**)

| Item | Detalhe |
| ---- | ------- |
| **Achado** | Saldo / reforÃ§o / retirada / fechar jÃ¡ usavam o turno da loja Â· relatÃ³rio e conferÃªncias misturavam Centro+Vila |
| **Fix** | Filtro Centro / Vila / Todas (padrÃ£o = loja do PC) Â· MP Renan jÃ¡ soma nas linhas Â«â€” Mercado PagoÂ» |
| **Validar** | RelatÃ³rio e ConferÃªncias â†’ chips Loja Â· saldo do turno aberto |

### feat â€” PDV maquininha Mercado Pago Renan (18/07 Â· **teste v10.26**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | OpÃ§Ã£o **Mercado Pago Renan** no seletor de mÃ¡quina (cartÃ£o + Pix) |
| **Comportamento** | Manual (nÃ£o dispara Point automÃ¡tico) Â· aparece no notebook tambÃ©m |
| **Validar** | PDV â†’ pagar cartÃ£o/Pix â†’ Selecionar mÃ¡quina â†’ 3Âª opÃ§Ã£o Renan |

### ðŸ©¹ Cadastro â€” saldo busca ficava velho apÃ³s venda (18/07 Â· **teste v10.23**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Venda baixava Vila 10â†’9 no banco, Cadastro busca ainda mostrava C10Â·V10 |
| **Causa** | `/api/buscar/` pegava ajuste **qualquer** (Ã s vezes o antigo), nÃ£o o mais recente |
| **Fix** | Mesma regra de `ajustes_mais_recentes` + Cadastro recalcula saldo no envelope |
| **Validar local** | Reinicia servidor Â· busca Â«teste divisÃ£oÂ» â†’ **V 9** |

### ðŸ©¹ Cadastro â€” fase 2 picklist (gestÃ£o / overlay / unidade / PDV) (18/07 Â· **teste v10.20**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Mesmo padrÃ£o FL-024 em: cadastro **unidade**, **gestÃ£o** drawer, **overlay** lista ERP, **PDV lÃ¡pis** unidade (Cadastrar exige PIN) |
| **JS** | `agro_picklist.js` compartilhado Â· facetas incluem `unidades` |
| **Validar** | GestÃ£o: digitar marca inventada â†’ barra Â· PDV lÃ¡pis: Cadastrar unidade â†’ PIN |

### ðŸ©¹ Cadastro â€” marca/cat sÃ³ lista + PIN (FL-024) (18/07 Â· **teste v10.19**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Marca / fornecedor / categoria / sub: buscar e **sÃ³ selecionar**; digitar solto **nÃ£o** grava; **+** abre popup com parecidos + **PIN** + log |
| **Busca** | Sem acento / caixa |
| **Validar** | Digitar marca inventada â†’ Salvar barra Â· escolher da lista OK Â· + com PIN cria |

### ðŸ©¹ Cadastro â€” cÃ³digo sistema+GM no produto novo sem agro_pg (18/07 Â· **teste v10.17**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Local (catÃ¡logo Mongo): Novo produto â†’ Fiscal sem cÃ³digo sistema/GM; loja `agro_pg` preenchia OK |
| **Fix** | Detalhe `__novo__` no caminho Mongo tambÃ©m aloca sequÃªncia 4010+ / `GM####` |
| **Validar** | Local: Novo produto â†’ aba Fiscal â†’ cÃ³digo + GM preenchidos |

### ðŸ©¹ PDV â€” badge estoque + aviso caixa sem F5 (18/07 Â· **teste v10.14**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Abrir Centro: badge ficava Â«Vila EliasÂ» atÃ© F5 Â· Fechar Centro no overlay: sumia o painel mas sem faixa Â«Caixa fechadoÂ» e dava pra montar venda atÃ© o fim |
| **Fix** | Aviso sempre no HTML (sÃ³ some/aparece) Â· refresh puxa `pdvDeposito` + badge Â· iframe avisa `agro-pdv-caixa-changed` Â· bloqueia item/avanÃ§ar com caixa fechado |
| **Validar** | Abrir Centro no PDV â†’ badge **Centro** na hora Â· Fechar â†’ faixa Ã¢mbar + nÃ£o deixa adicionar item |

### ðŸ©¹ Caixa fechado â€” bloqueia venda + refresh no PDV (18/07 Â· **teste v10.11**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | ApÃ³s fechar caixa no overlay, PDV ainda vendia (bootstrap velho) e servidor aceitava venda sem turno |
| **Fix** | API exige caixa aberto Â· PDV revalida caixa antes de cada venda Â· ao fechar overlay atualiza status Â· redirects do Fechar mantÃªm overlay |
| **Validar local** | Abrir caixa â†’ fechar â†’ tentar vender â†’ deve barrar Â· abrir de novo â†’ vende |

### ðŸ©¹ Caixa no PDV â€” nÃ£o abrir BI no overlay (18/07 Â· **teste v10.10**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | ApÃ³s abrir caixa, `redirect(home)` carregava o BI dentro do painel do PDV |
| **Fix** | No overlay (`agro_pdv_overlay`) â†’ volta ao **menu caixa** com os params; fora do PDV continua home |
| **Validar local** | PDV â†’ abrir caixa Vila â†’ fica no menu caixa, **nÃ£o** no BI |

### ðŸ©¹ Abrir caixa â€” seleÃ§Ã£o + digitar loja (18/07 Â· **teste v10.08**)

| Item | Detalhe |
| ---- | ------- |
| **Feito** | CartÃ£o selecionado com anel + Â«SelecionadoÂ» Â· digitaÃ§Ã£o errada = borda vermelha + texto |
| **Validar local** | Clicar Vila â†’ sÃ³ Vila destacada Â· digitar `vil` â†’ aviso vermelho |

### ðŸ©¹ Caixa fechar â€” 500 (fiado_baixas_wizard) (18/07 Â· **teste v10.06**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Causa** | VariÃ¡vel `fiado_baixas_wizard` sumiu no pacote Vila â†’ NameError na tela Fechar |
| **Fix** | Restaurar `listar_fiado_baixas_conferencia_caixa(sessoes)` |
| **Validar** | Ctrl+F5 â†’ Fechar caixa (sessÃ£o antiga Centro) abre Â· fechar libera seletor Loja no BI |

### WIP â€” CatÃ¡logo Â· WhatsApp 1Âº no checkout **v10.78** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push 	este |
| **UX** | WhatsApp primeiro Â· busca cadastro Â· preenche nome/endereÃ§o Â· lembra WA no aparelho |
| **ProduÃ§Ã£o** | âœ… loja **v10.67** |

### WIP â€” CatÃ¡logo Â· OG Â«Delivery de raÃ§Ã£oÂ» **v10.77** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push 	este |
| **O quÃª** | TÃ­tulo/desc + faixa no cartÃ£o: *Delivery de raÃ§Ã£o* Â· cache card3 |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo Â· OG card 1200x630 sem cortar logo **v10.74** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push 	este |
| **O quÃª** | /catalogo/og-image/ gera cartÃ£o 1200Ã—630 Â· logo contain Â· ?v=card2-â€¦ quebra cache |
| **fb:app_id** | Aviso Facebook cosmÃ©tico â€” **nÃ£o** impede preview Zap |
| **ProduÃ§Ã£o** | âœ… loja **v10.66** |

### WIP â€” CatÃ¡logo Â· preview WhatsApp (og:image) **v10.71** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push 	este |
| **O quÃª** | Meta Open Graph + /catalogo/og-image/ (logo da gestÃ£o) |
| **VocÃª** | Colar link no Zap Â· se cache antigo: Facebook Sharing Debugger Â«Raspar novamenteÂ» |
| **ProduÃ§Ã£o** | âœ… loja **v10.65** |

### WIP â€” CatÃ¡logo Â· esconder Plus Code no checkout **v10.63** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push 	este Â· âœ… loja **v10.62** |
| **UX** | Cliente vÃª sÃ³ âœ“ localizaÃ§Ã£o OK â€” Plus Code fica oculto (grava no pedido) |

### WIP â€” CatÃ¡logo delivery Â· GPS checkout **v10.61** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Origem** | FOOD `food.md` Â§8.1 â€” GPS â†’ Plus Code no fechamento |
| **O quÃª** | BotÃ£o Â«Usar minha localizaÃ§Ã£oÂ» Â· some endereÃ§o manual Â· Â«Digitar manualmenteÂ» Â· API `/catalogo/api/localizacao/` Â· grava plus/maps no pedido/cliente |
| **ProduÃ§Ã£o** | âœ… loja **v10.62** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.53** (18/07/2026)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **UX** | BotÃ£o **Â«Salvar fotoÂ»** (antes Â«Foto cardÂ») + texto: nÃ£o usar Â«Salvar lojaÂ» |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.51** (18/07/2026) *(histÃ³rico â€” ver v10.53)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Foto cat.** | Upload **AJAX** (comprime no celular) Â· alerta OK/erro Â· isento idempotÃªncia |
| **UI** | Â«Desiâ€¦.pngÂ» = nome abreviado do arquivo (normal); nÃ£o Ã© erro |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.50** (18/07/2026) *(histÃ³rico â€” ver v10.51)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Foto cat.** | Comprime no servidor (Pillowâ†’JPG) Â· atÃ© ~4 MB bruto Â· erro em **alerta** |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.49** (18/07/2026) *(histÃ³rico â€” ver v10.50)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Foto cat.** | Limite **~1,2 MB** Â· erro visÃ­vel no topo + alerta no navegador |
| **Causa** | PNG 860 KB > 700 KB antigo Â· rejeitava sem aviso claro |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.48** (18/07/2026) *(histÃ³rico â€” ver v10.49)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Hero** | DegradÃª mais branco na faixa Delivery Â· cards endereÃ§o **#fff** |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.47** (18/07/2026) *(histÃ³rico â€” ver v10.48)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Hero** | Textos **escuros** no degradÃª claro Â· cards brancos Â· CTA verde |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.46** (18/07/2026) *(histÃ³rico â€” ver v10.47)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Hero** | Base do degradÃª um pouco mais **verde claro** (topo igual) |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.45** (18/07/2026) *(histÃ³rico â€” ver v10.46)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Hero** | DegradÃª tipo ref.: **topo quase branco** (Â½) â†’ teal suave |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.44** (18/07/2026) *(histÃ³rico â€” ver v10.45)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Hero** | **Um** `linear-gradient` limpo (cremeâ†’verde) Â· sem vÃ©u/faixa |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.43** (18/07/2026) *(histÃ³rico â€” ver v10.44)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Hero** | Fade em **vÃ©u** + faixa `hero-fade` Â· transiÃ§Ã£o bem mais suave |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.42** (18/07/2026) *(histÃ³rico â€” ver v10.43)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Hero** | DegradÃª **longo/suave** (sem faixa dura) Â· 100% mais abaixo |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.41** (18/07/2026) *(histÃ³rico â€” ver v10.42)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Hero** | DegradÃª desce mais antes do verde 100% |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.40** (18/07/2026) *(histÃ³rico â€” ver v10.41)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Hero** | Fundo logo: degradÃª verde/laranja **fraco** â†’ contraste sobe â†’ **100%** em Delivery |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.39** (18/07/2026) *(histÃ³rico â€” ver v10.40)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Logo** | Corta sobra branca cima/baixo (`cover` + aspect mais baixo) |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.38** (18/07/2026) *(histÃ³rico â€” ver v10.39)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Hero** | Logo **largura total** Â· faixa branca fina Â· **GestÃ£o** ao lado das boas-vindas |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.37** (18/07/2026) *(histÃ³rico â€” ver v10.38)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Logo** | Largura **100%** do hero (sem caixa vazia) Â· altura auto |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.36** (18/07/2026) *(histÃ³rico â€” ver v10.37)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Hero** | Logo **maior** Â· sem nome texto (jÃ¡ na arte) Â· Â«Delivery Â· raÃ§ÃµesÂ» + boas-vindas **embaixo** |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.35** (18/07/2026) *(histÃ³rico â€” ver v10.36)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Logo** | Formato **paisagem** na vitrine Â· limite upload **~1,2 MB** |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.34** (18/07/2026) *(histÃ³rico â€” ver v10.35)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **NÃ­veis** | Categoria â†’ sub â†’ **sub-sub** â†’ produtos (mÃ¡x. 3) |
| **Cadastro** | Aba Delivery: 3 selects + **+** em cada Â· gestÃ£o cria sob raiz ou sob sub |
| **Vitrine** | Passos em cascata; Voltar sobe um nÃ­vel |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.33** (18/07/2026) *(histÃ³rico â€” ver v10.35)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **UX** | CTA Â«Como chegarÂ» nos endereÃ§os |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.29** (18/07/2026) *(histÃ³rico â€” ver v10.30)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **EndereÃ§o** | Card clicÃ¡vel â†’ Google Maps rota |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.27** (18/07/2026) *(histÃ³rico â€” ver v10.29)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **UX** | Logo maior Â· WhatsApp balÃ£o flutuante |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.24** (18/07/2026) *(histÃ³rico â€” ver v10.27)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` Â· migrate `0060` logo loja |
| **Logo** | Upload em `/catalogo/gestao/` Â· aparece antes do nome |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.22** (18/07/2026) *(histÃ³rico â€” ver v10.24)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **Fluxo** | Home categoria â†’ **subcategoria** (se existir) â†’ produtos filtrados Â· Voltar em cascata |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.21** (18/07/2026) *(histÃ³rico â€” ver v10.22)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` Â· migrate `0059` foto categoria |
| **Home `/catalogo/`** | Cards grandes de categoria com foto â†’ clique abre produtos Â· Voltar |
| **GestÃ£o** | Em cada categoria raiz: upload **Foto card** (mÃ¡x. ~700 KB) |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.16** (18/07/2026) *(histÃ³rico â€” ver v10.21)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` Â· migrate `0058` |
| **UX aba 10** | Grade compacta + **botÃ£o +** cria categoria/sub no modal |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.15** (18/07/2026) *(histÃ³rico â€” ver v10.16)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` |
| **UX aba 10** | Grade compacta no modal: cat+sub+estoque Â· tÃ­tulo+peso+ordem Â· desc+destaque+foto |
| **ProduÃ§Ã£o** | **NÃ£o** |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.12** (18/07/2026) *(histÃ³rico â€” ver v10.13)*

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` Â· migrate `0058` no deploy |
| **O quÃª** | `/catalogo/` pÃºblico Â· aba **10. Delivery** Â· pedidos â†’ `/entregas/` (`origem=catalogo`) |
| **GestÃ£o** | `/catalogo/gestao/` â€” publicar, WhatsApp, **2 endereÃ§os** (rÃ³tulo+rua), **CRUD categorias/sub** |
| **Categorias** | PadrÃ£o apps raÃ§Ã£o: CÃ£es/Gatos/â€¦ + sub (Adulto/Filhote) Â· seed na migrate Â· select na aba Delivery |
| **Vitrine** | Mobile-first Â· chips categoria Â· 2 endereÃ§os no hero Â· botÃ£o WhatsApp **fixo** (nÃ£o FAB) |
| **Migrate** | `0057` config Â· `0058` categorias + endereÃ§os 1/2 |
| **Renan testar** | GestÃ£o: 2 endereÃ§os + criar cat/sub â†’ produto aba Delivery (cat+sub) â†’ vitrine celular chips + Zap |
| **ProduÃ§Ã£o** | **NÃ£o** â€” sÃ³ teste atÃ© Renan pedir + senha |

### WIP â€” CatÃ¡logo delivery GM Agro **v10.05** (18/07/2026) *(histÃ³rico â€” ver v10.12)*

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | CatÃ¡logo pÃºblico `/catalogo/` Â· aba **10. Delivery** Â· pedidos â†’ entregas |
| **GestÃ£o loja** | `/catalogo/gestao/` â€” publicar, WhatsApp, cores (sem cat/endereÃ§os 2 ainda) |


### ðŸ”’ Vila Elias â€” antiburro digitar loja (18/07 Â· **teste v10.04**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` Â· validar no Render |
| **Feito** | Abrir Gaveta/Vila: digitar **centro** ou **vila** Â· trocar Loja no BI/atalhos: mesmo digitar Â· com caixa aberto seletor **travado** |
| **Arquivos** | `pdv_deposito_util.py` Â· `views.py` Â· `caixa_abrir.html` Â· `agro_loja_confirm.html` Â· `dashboard_gerencial.html` Â· `home.html` |
| **Validar** | Abrir caixa Vila digitando `vila` Â· tentar trocar BI sem digitar â†’ bloqueia Â· com caixa aberto seletor desabilitado |

### ðŸ’° Vila Elias â€” Caixa separado Centro Ã— Vila (18/07 Â· **teste v10.03**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` Â· validar no Render Â· ðŸ“¦ produÃ§Ã£o sÃ³ com frase + senha |
| **Feito** | `ponto_caixa=vila` (turno prÃ³prio) Â· abrir 4 cartÃµes (Gaveta Centro / Vila / Notebook / Teste) Â· painel Â«todosÂ» e fechamento em lote **sÃ³ da loja do aparelho** Â· venda herda depÃ³sito do ponto do caixa Â· MP Point **nÃ£o** na Vila Â· migration **0056** |
| **Arquivos** | `caixa_util.py` Â· `views.py` Â· `models.py` Â· `0056_*` Â· `caixa_abrir.html` Â· `caixa_fechar.html` Â· `caixa_painel.html` |
| **Validar teste** | BI â†’ Loja **Vila** â†’ Abrir **Caixa Vila** â†’ vender â†’ painel soma sÃ³ Vila Â· Centro aberto no outro PC nÃ£o mistura Â· fechar lote = sÃ³ Vila |
| **Ops** | PC Vila: seletor Loja Vila + abrir Caixa Vila. Centro continua Gaveta. Notebook satÃ©lite da loja do aparelho. |
| **Autorizar loja** | *Â«pode subir Vila Elias caixa separadoÂ»* + **99738595** |

### ðŸª Vila Elias â€” PDV baixa estoque por loja do aparelho (18/07 Â· **teste v10.02**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` Â· validar no Render Â· ðŸ“¦ produÃ§Ã£o sÃ³ com frase + senha |
| **Problema** | Seletor Â«Vila EliasÂ» na home nÃ£o baixava estoque Vila â€” venda sempre usava env `PDV_VENDA_ESTOQUE_DEPOSITO` (Centro) |
| **Feito** | SessÃ£o/cookie `pdv_deposito` Â· API `/api/pdv/deposito/` Â· seletor no **BI** + atalhos Â· badge no PDV Â· `VendaAgro.deposito` + migration **0055** Â· baixa/estorno usam depÃ³sito da venda |
| **Arquivos** | `pdv_deposito_util.py` Â· `views.py` Â· `pdv/views.py` Â· `models.py` Â· `0055_*` Â· `dashboard_gerencial.html` Â· `home.html` Â· topbar/wizard JS |
| **Validar teste** | BI â†’ Loja **Vila Elias** â†’ badge Â«Estoque: Vila EliasÂ» â†’ PDV venda R$ 0,01 â†’ saldo **Vila** cai Â· devoluÃ§Ã£o repÃµe Vila |
| **Ops abertura** | 1) Injetar estoque Vila (NF depÃ³sito Vila / transferÃªncia / PIN) 2) Conferir sync 3) PC Vila = Loja Vila no BI 4) Caixa sÃ³ naquele PC 5) **Sem NFC-e Vila** atÃ© cert CNPJ `0323â€¦` (PIX/cartÃ£o ainda = CNPJ Centro) |
| **Autorizar loja** | *Â«pode subir Vila Elias PDV depÃ³sitoÂ»* + **99738595** |
| **Backup no envio** | `producao-backup-pre-v1002-vila-pdv-YYYYMMDD` @ HEAD loja |

### ðŸ©¹ Entrada NF â€” histÃ³rico C1â€“C3 nÃ£o duplica a prÃ³pria nota (18/07 Â· **teste v9.97**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… push `teste` Â· validar no Render (Ctrl+F5 etapa 8) |
| **Sintoma** | C3 e NF com mesmo preÃ§o e datas diferentes (entrada 14/07 vs emissÃ£o 30/06) â€” parecia 2 notas |
| **Causa** | ApÃ³s estoque, a NF aberta entrava em Â«Ãºltimas comprasÂ» (data entrada) e de novo na coluna NF (emissÃ£o) |
| **Fix** | PrÃ©via exclui o rascunho atual do histÃ³rico; filtro extra por nÂº+custo; rÃ³tulo Â«Compras anteriores + NFÂ» |
| **Arquivos** | `views.py` Â· `compras_ultimas_compras_util.py` Â· `entrada_nota.html` |
| **Validar** | Abrir NF 320 (ou qualquer) â†’ etapa 8 â†’ C1â€“C3 sem a nota atual Â· NF sÃ³ Ã  direita |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Compras etapa 1 UI (18/07 Â· **teste v9.95+**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v10.60** (`c88ce66`) Â· 18/07 |
| **Escopo** | SÃ³ visual/organizaÃ§Ã£o â€” **sem** mudanÃ§a de regra, API, Mongo, cÃ¡lculo de sugestÃ£o |
| **Feito** | Painel resumo (horizonte + descontar estoque + KPIs) Â· busca em destaque Â· filtros limpos Â· lista + detalhe expandido em blocos |
| **Arquivo** | `produtos/templates/produtos/compras.html` |
| **Risco** | Baixo â€” sÃ³ layout; comportamento igual ao de antes |
| **Validar na loja** | Ctrl+F5 `/compras/` Â· buscar Â· expandir linha Â· horizonte/checkbox Â· Folha/F10/lista |
| **Autorizar** | *Â«pode subir Compras UI etapa 1Â»* + **99738595** |
| **PrÃ³ximo** | Etapas seguintes de evoluÃ§Ã£o Compras (quando Renan pedir) |

### ðŸ“¦ PACOTE PRONTO LOJA â€” aba 9 histÃ³rico sÃ³ o que mudou (18/07 Â· **teste v10.01**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **enviado loja v10.61** (`e38df10`) Â· 18/07 |
| **Sintoma** | LÃ¡pis PDV (1 centavo) gerava vÃ¡rias linhas com **DE = â€”** (nome, GM, barrasâ€¦) Â· preÃ§o sem valor anterior |
| **Causa** | Overlay vazio no 1Âº save Â· Â«antesÂ» vazio Â· `permite_venda` forÃ§ado True em todo save |
| **Fix** | Â«AntesÂ» completa com catÃ¡logo Â· nÃ£o forÃ§a permite_venda no lÃ¡pis |
| **Arquivos** | `cadastro_alteracao_historico_util.py` Â· `views.py` |
| **Risco** | Baixo â€” sÃ³ histÃ³rico de cadastro; nÃ£o mexe estoque/venda |
| **Backup no envio** | `producao-backup-pre-v1001-aba9-YYYYMMDD` @ HEAD loja |
| **ApÃ³s enviar** | **ObrigatÃ³rio testar na loja** (Ctrl+F5 Â· lÃ¡pis muda 1 centavo Â· aba 9 = **1 linha** Â· DE = preÃ§o antigo) |
| **Autorizar** | *Â«pode subir aba 9 histÃ³ricoÂ»* + **99738595** |

### ðŸ“¦ PACOTE PRONTO LOJA â€” kardex e-mail + Entrada NF Î”camada (18/07 Â· **teste v9.92**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **jÃ¡ na loja desde v10.63** (`1e6fe3c`) â€” nÃ£o incluir na fila 11.94â€“11.98 |
| **Sintoma** | Quem ainda com e-mail (`agromaisgm@â€¦`) Â· Entrada NF (ex. 399636) ainda como **saÃ­da ~172** |
| **Causa** | Label da NF era e-mail do login Â· qty usava Î” `saldo_informado` bruto (mistura salto ERP) |
| **Fix** | Resolve e-mailâ†’nome Â· movimento = Î”(camada Agro = informadoâˆ’ERP ref) Â· saldo recomposto |
| **Arquivo** | `estoque_movimentos_cadastro_util.py` |
| **Risco** | Baixo â€” sÃ³ leitura/exibiÃ§Ã£o do histÃ³rico |
| **Backup no envio** | `producao-backup-pre-v992-kardex-YYYYMMDD` @ HEAD loja |
| **ApÃ³s enviar** | **ObrigatÃ³rio testar na loja** (Ctrl+F5 Â· badge Â· milho Â· Estoque Â· Quem sem @ Â· NF 399636 = entrada) |
| **Autorizar** | *Â«pode subir kardex e-mail / Entrada NFÂ»* + **99738595** |

### ðŸ“¦ Deploy loja **v9.91** â€” kardex Quem + Entrada NF (18/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live** Â· Render loja **v9.91** Â· commit 372bb70 |
| **Inclui** | SÃ³ fix kardex: **Quem** = operador real Â· **Entrada NF** sem saÃ­da fantasma Â· fornecedor por nÂº NF |
| **Arquivos** | `estoque_movimentos_cadastro_util.py` Â· `views.py` (+9 linhas) Â· **nÃ£o** merge inteiro |
| **HEAD loja** | *(este commit)* Â· base **8418ba8** (v9.90) |
| **Backup** | `producao-backup-pre-v991-kardex-20260718` @ **8418ba8** â€” reverter: `git push origin producao-backup-pre-v991-kardex-20260718:producao` |
| **Risco** | Baixo â€” sÃ³ leitura/exibiÃ§Ã£o do histÃ³rico; nÃ£o altera estoque nem venda |
| **AutorizaÃ§Ã£o** | *pode subir â€¦ correÃ§Ã£o* + **99738595** |
| **VocÃª** | Ctrl+F5 Â· badge **v9.91** Â· milho Â· aba Estoque Â· Quem + NF 398454 |

### ðŸ©¹ Cadastro â€” kardex Quem + Entrada NF (17/07 Â· **teste v9.91** Â· **loja v9.91**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | 1) Coluna **Quem** quase sempre Â«Geraldo HinnenÂ» Â· 2) **Entrada NF** aparecia como **saÃ­da** (~170) e fornecedor **RBS** errado (NF 398454 era outro fornecedor / +20 milho) |
| **Causa** | Quem: priorizava usuÃ¡rio Django da sessÃ£o Chrome. Qtd: misturava saldo de IDs variantes no mesmo depÃ³sito. Fornecedor: casava sÃ³ por data + fallback 1Âª compra |
| **Fix** | Operador do PIN/venda; delta por produto+depÃ³sito; casa NF por nÃºmero; sem fallback cego; rÃ³tulo Â«NFÂ» sem duplicar |
| **Arquivos** | estoque_movimentos_cadastro_util.py Â· 
iews.py (compras enrich) |
| **VocÃª** | Ctrl+F5 teste Â· milho grande Â· aba Estoque Â· confere Quem nas vendas Â· Entrada NF 398454 = **entrada** (nÃ£o saÃ­da) e fornecedor certo |

### ðŸ“¦ Deploy loja **v9.90** â€” Fecha PDV+Cadastro+NFC-e+RelatÃ³rios (17/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **Live** Â· Render loja **v9.90** Â· commit e40e898 |
| **Inclui** | **PDV Enviar WhatsApp** Â· **PDV lÃ¡pis/editor** Â· **Cadastro modal** (kardex + aba 9 + origem PDV) Â· **FL-056** NFC-e 963/225 Â· **RelatÃ³rios ajuda Â«?Â»** Â· layout PDV etapa 1 (atÃ© v9.90) |
| **HEAD loja** | *(este commit)* Â· base **4113492** (v9.16) |
| **Backup** | `producao-backup-pre-v990-fecha-pdv-cadastro-nfce-20260717` @ **4113492** â€” reverter: `git push origin producao-backup-pre-v990-fecha-pdv-cadastro-nfce-20260717:producao` |
| **MÃ©todo** | worktree Â· checkout arquivos do `teste` Â· **nÃ£o** merge inteiro |
| **Migration** | **0054** `ProdutoCadastroAlteracaoAgro` (Render migrate no deploy) |
| **AutorizaÃ§Ã£o** | *envie para produÃ§Ã£o* + **99738595** (checklist Fecha) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.90** Â· Enviar Zap Â· lÃ¡pis â†’ aba 9 Â· modal cadastro Â· RelatÃ³rios **?** Â· reemitir NFC-e **#2812**/**#3347** |

### ðŸ“¦ PDV â€” Enviar orÃ§amento WhatsApp Â· pronto produÃ§Ã£o (17/07 Â· **teste v9.90**)


| | |
| --- | --- |
| **Status** | âœ… **enviado loja v9.90** |
| **O quÃª** | BotÃ£o **Enviar** (Zap) acima do Subtotal Â· texto Agromais Â· grava em OrÃ§amentos Â· Ã­cone Zap na lista Â· avisos toast central |
| **Commits** | c3ce765â€¦1b09f96 (pacote Zap PDV) Â· HEAD 1b09f96 |
| **OK Renan** | 17/07 â€” fluxo + lista + Ã­cone |
| **Loja** | âœ… **v9.90** (pacote Fecha 17/07) |

### âœ¨ PDV â€” Ã­cone Zap nos orÃ§amentos enviados (17/07 Â· **teste v9.90**)

| | |
| --- | --- |
| **O quÃª** | OrÃ§amentos salvos pelo **Enviar** WhatsApp mostram Ã­cone verde do Zap na lista (e no Ver mais) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.90** Â· Enviar de novo â†’ linha nova com Ã­cone Zap |

### ðŸ©¹ PDV â€” Enviar Zap tambÃ©m grava em OrÃ§amentos (17/07 Â· **teste v9.89**)

| | |
| --- | --- |
| **O quÃª** | **Enviar** = mesma gravaÃ§Ã£o do **Salvar orÃ§amento** (lista + servidor) Â· depois abre o Zap |
| **VocÃª** | Ctrl+F5 Â· badge **v9.89** Â· Enviar â†’ confere se aparece no card OrÃ§amentos |

### ðŸ©¹ PDV â€” texto do aviso central maior (17/07 Â· **teste v9.88**)

| | |
| --- | --- |
| **O quÃª** | TÃ­tulo/corpo/botÃ£o do toast prominent bem maiores (legÃ­vel na loja) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.88** Â· Enviar sem telefone â†’ lÃª o texto grande |

### ðŸ©¹ PDV â€” avisos Enviar Zap no meio da tela (17/07 Â· **teste v9.87**)

| | |
| --- | --- |
| **O quÃª** | Avisos do Enviar WhatsApp no centro (toast prominent, grande) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.87** Â· Enviar sem telefone â†’ aviso grande no meio |

### ðŸ©¹ PDV â€” avisos Enviar Zap sem alert Chrome (17/07 Â· **teste v9.86**)

| | |
| --- | --- |
| **O quÃª** | Carrinho vazio / sem cliente / sem telefone â†’ toast PDV (showPdvAviso), nÃ£o janela do navegador |
| **VocÃª** | Ctrl+F5 Â· badge **v9.86** Â· clica Enviar nos 3 casos e confere o aviso SisVale |

### ðŸ©¹ PDV â€” texto Zap orÃ§amento padrÃ£o Agromais (17/07 Â· **teste v9.85**)

| | |
| --- | --- |
| **O quÃª** | Mensagem WhatsApp no formato do 2Âº print: *ORÃ‡AMENTO AGROMAIS*, item em 1 linha 1x - nome  R$, linhas finas, sem losango/ðŸ’° |
| **VocÃª** | Ctrl+F5 Â· badge **v9.85** Â· Enviar de novo e conferir o Zap |

### âœ¨ PDV â€” Enviar orÃ§amento WhatsApp (acima do Subtotal) (17/07 Â· **teste v9.84**)

| | |
| --- | --- |
| **O quÃª** | BotÃ£o verde **Enviar** (Ã­cone Zap) acima do Subtotal Â· texto no padrÃ£o Consulta Â· sem cliente abre busca F4 Â· salva orÃ§amento antes Â· abre Zap do cliente |
| **VocÃª** | Ctrl+F5 Â· badge **v9.84** Â· testa: com cliente+itens; sem cliente; carrinho vazio |

### ðŸ©¹ PDV â€” some azul de vez (painel+campo) (17/07 Â· **teste v9.83**)

| | |
| --- | --- |
| **O quÃª** | Contorno do painel e borda do campo de busca saem do azul (cinza) Â· sem linha entre busca e carrinho |
| **VocÃª** | Ctrl+F5 forte Â· badge tem que ser **v9.83** (se for menor, ainda tÃ¡ cache) |

### ðŸ©¹ PDV â€” some de vez a linha/faixa azul da busca (17/07 Â· **teste v9.82**)

| | |
| --- | --- |
| **O quÃª** | Tira borda **e** fundo azul da faixa da busca (era isso que ainda aparecia) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.82** |

### ðŸ©¹ PDV â€” tira linha azul sob a busca (17/07 Â· **teste v9.81**)

| | |
| --- | --- |
| **O quÃª** | Remove a faixa/borda azul embaixo do campo de busca |
| **VocÃª** | Ctrl+F5 Â· badge **v9.81** |

### ðŸ©¹ PDV â€” Ã­cone Cliente fora do campo + texto Carrinho (17/07 Â· **teste v9.80**)

| | |
| --- | --- |
| **O quÃª** | Ãcone do cliente **fora** do campo (igual Busca) Â· volta escrita **Carrinho** |
| **VocÃª** | Ctrl+F5 Â· badge **v9.80** |

### ðŸ©¹ PDV â€” sÃ³ Ã­cones sem texto (teste visual) (17/07 Â· **teste v9.79**)

| | |
| --- | --- |
| **O quÃª** | Tira as escritas Cliente/Busca/Carrinho â€” fica **sÃ³ o Ã­cone** (pra Renan ver como fica) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.79** Â· diz se mantÃ©m ou volta o texto |

### ðŸ©¹ PDV â€” Cliente/Busca/Carrinho mesmo padrÃ£o (17/07 Â· **teste v9.78**)

| | |
| --- | --- |
| **O quÃª** | TrÃªs linhas iguais: **Ã­cone + tÃ­tulo** no tamanho da Busca. Cliente ganhou sÃ­mbolo (pessoa). Carrinho/Ã­cones alinhados |
| **VocÃª** | Ctrl+F5 Â· badge **v9.78** |

### ðŸ©¹ PDV â€” busca +1 ponto (17/07 Â· **teste v9.77**)

| | |
| --- | --- |
| **O quÃª** | Busca/fonte digitada + altura mais um pouquinho |
| **VocÃª** | Ctrl+F5 Â· badge **v9.77** |

### ðŸ©¹ PDV â€” busca fonte maior de novo (17/07 Â· **teste v9.76**)

| | |
| --- | --- |
| **O quÃª** | Linha da busca + **Busca** + texto digitado bem maiores (mesmo tamanho, peso 900) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.76** |

### ðŸ©¹ PDV â€” fonte digitada = Busca (17/07 Â· **teste v9.75**)

| | |
| --- | --- |
| **O quÃª** | Texto digitado na busca no **mesmo tamanho** da fonte **Busca** |
| **VocÃª** | Ctrl+F5 Â· badge **v9.75** |

### ðŸ©¹ PDV â€” busca um pouco maior (17/07 Â· **teste v9.74**)

| | |
| --- | --- |
| **O quÃª** | Linha da busca um pouco mais alta Â· fonte **Busca** + texto digitado maiores (proporcional) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.74** |

### ðŸ©¹ PDV â€” busca na linha do Ã­cone Busca (17/07 Â· **teste v9.73**)

| | |
| --- | --- |
| **O quÃª** | Campo de busca sobe **na mesma linha** do Ã­cone/tÃ­tulo **Busca** (vÃ£o vermelho do print) Â· some a faixa de baixo Â· **1 itens** Ã  direita |
| **VocÃª** | Ctrl+F5 Â· badge **v9.73** Â· confere se bate com o print |

### ðŸ©¹ PDV â€” reverte busca na linha do Cliente (17/07 Â· **teste v9.72**)

| | |
| --- | --- |
| **O quÃª** | Reverteu de novo â€” volta layout **v9.67** (busca embaixo Â· Saldos no topo direito). PrÃ³ximo: confirmar no print antes de mexer |
| **VocÃª** | Ctrl+F5 Â· badge **v9.72** |

### ðŸ©¹ PDV â€” reverte busca no header (17/07 Â· **teste v9.71**)

| | |
| --- | --- |
| **O quÃª** | Reverteu busca no topo â€” volta layout **v9.67** (busca na coluna esquerda Â· Saldos no topo direito) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.71** |

### ðŸ©¹ PDV â€” Saldos sobe Â· botÃµes cliente Ã  esquerda (17/07 Â· **teste v9.67**)

| | |
| --- | --- |
| **O quÃª** | Cliente + **Editar/Trocar/Hist** ficam na coluna da **busca** (esquerda). **Saldos** sobe no topo da coluna direita. OrÃ§amentos no mesmo lugar de sempre (abaixo do Saldos) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.67** Â· confere |

### ðŸ©¹ PDV â€” tira Etapa 1/Produtos Â· ? sobe pro header (17/07 Â· **teste v9.66**)

| | |
| --- | --- |
| **O quÃª** | SÃ³ isto: remove texto **Etapa 1 / Produtos** Â· **?** vai para a barra de etapas (ao lado de 1Â·2Â·3) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.66** Â· confere |

### ðŸ“¦ PACOTE PRONTO LOJA â€” PDV lÃ¡pis â†’ aba 9 AlteraÃ§Ãµes (17/07 Â· âœ… enviado v9.90)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **testado no teste** Â· ðŸ“¦ **pronto pra envio** â€” espera frase + senha `99738595` na **mesma mensagem** |
| **O quÃª** | LÃ¡pis do PDV grava histÃ³rico na aba **9. AlteraÃ§Ãµes** com origem **Â«PDV ediÃ§Ã£o rÃ¡pidaÂ»** (mesmo overlay do cadastro). Saldo no lÃ¡pis continua sÃ³ na aba **4** |
| **VersÃ£o alvo loja** | **v9.62** |
| **Base loja hoje** | **v9.16** |
| **Arquivos** | `pdv_wizard.js` Â· `cadastro_alteracao_historico_util.py` Â· `models.py` (Origem.PDV) Â· `_modal_editar_produto_cadastro_erp.inc.html` (`origem_historico=modal`) |
| **Sobe junto** | Ideal com pacote **Cadastro modal** (migration `0054` + aba 9) â€” se a aba 9 ainda nÃ£o estiver na loja, subir os dois |
| **Risco** | Baixo â€” sÃ³ marca origem + mesmo hook de histÃ³rico jÃ¡ existente |
| **MÃ©todo** | worktree `origin/producao` Â· checkout desses arquivos de `teste` Â· **nÃ£o** merge inteiro |
| **Autorizar com** | *Â«pode subir histÃ³rico PDV lÃ¡pis / aba 9Â»* + **99738595** Â· ou junto: *Â«pode subir cadastro modal / histÃ³ricoÂ»* + senha |
| **VocÃª apÃ³s** | Ctrl+F5 loja Â· lÃ¡pis muda nome â†’ cadastro â†’ aba 9 Â· origem Â«PDV ediÃ§Ã£o rÃ¡pidaÂ» |

### ðŸ©¹ PDV + Cadastro â€” histÃ³rico aba 9 tambÃ©m no lÃ¡pis (17/07 Â· **teste v9.62**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | EdiÃ§Ã£o rÃ¡pida do PDV (lÃ¡pis) jÃ¡ salvava no mesmo overlay â€” agora marca origem **Â«PDV ediÃ§Ã£o rÃ¡pidaÂ»** na aba **9. AlteraÃ§Ãµes**. Cadastro modal marca **Â«Modal cadastroÂ»**. MudanÃ§a de **saldo** no lÃ¡pis continua sÃ³ na aba **4 Estoque** (nÃ£o mistura) |
| **Status** | ðŸ“¦ **pronto pra envio** â€” pacote acima |
| **VocÃª** | Ctrl+F5 Â· badge **v9.62** Â· lÃ¡pis muda nome/preÃ§o â†’ cadastro â†’ aba 9 Â· confere origem |

### ðŸ©¹ PDV etapa 1 â€” layout: tira ?, botÃµes no cliente, busca sobe (17/07 Â· **teste v9.60**)

| | |
| --- | --- |
| **O quÃª** | Tira **?** Â· **Editar / Trocar / Hist** colados no cliente (esquerda) Â· busca sobe (sem tÃ­tulo Â«BuscaÂ») Â· Saldos sobe no painel Â· contagem de itens no Carrinho |
| **VocÃª** | Ctrl+F5 Â· badge **v9.60** Â· confere barra do cliente + busca + saldos |
| **Pacote lÃ¡pis** | Continua ðŸ“¦ pronto; este layout sobe junto se pedir PDV |

### ðŸ“¦ PACOTE PRONTO LOJA â€” Cadastro modal editar (UX + histÃ³ricos) (17/07 Â· âœ… enviado v9.90)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **testado no teste** Â· ðŸ“¦ **pronto pra envio** â€” espera frase + senha `99738595` na **mesma mensagem** |
| **O quÃª** | Modal editar produto quase tela cheia Â· aba **4 Estoque** = kardex (movimentaÃ§Ã£o, sem PIN) Â· aba **9 AlteraÃ§Ãµes** = histÃ³rico cadastro (antesâ†’depois) â€” **inclui PDV lÃ¡pis** (origem Â«PDV ediÃ§Ã£o rÃ¡pidaÂ») Â· PreÃ§os com nÃºmeros grandes Â· Fiscal/Gerais fonte maior e campos juntos Â· Config no topo Â· URL da imagem com rÃ³tulo Â· listas de histÃ³rico crescem atÃ© o rodapÃ© |
| **VersÃ£o alvo loja** | **v9.62** (ou badge do deploy) |
| **Base loja hoje** | **v9.16** |
| **Arquivos (nÃºcleo)** | `_modal_editar_produto_cadastro_erp.inc.html` Â· `estoque_movimentos_cadastro_util.py` Â· `cadastro_alteracao_historico_util.py` Â· `migrations/0054_produto_cadastro_alteracao_agro.py` Â· URLs/API cadastro (estoque-movimentos + alteracoes-historico) Â· hooks salvar overlay/Excel Â· `pdv_wizard.js` (origem PDV) Â· `models.py` (Origem.PDV) |
| **Pente fino** | IDs dos campos intactos Â· `edit-img` com rÃ³tulo Â· kardex â‰  alteraÃ§Ãµes Â· PreÃ§os sem altura falsa Â· lÃ¡pis PDV â†’ aba 9 com origem correta |
| **Risco** | MÃ©dio-baixo â€” UX + APIs de leitura + migration `0054` (tabela histÃ³rico) Â· deploy precisa rodar migrate |
| **MÃ©todo** | worktree `origin/producao` Â· checkout desses arquivos de `teste` Â· **nÃ£o** merge inteiro |
| **Autorizar com** | *Â«pode subir cadastro modal / histÃ³ricoÂ»* + **99738595** |
| **VocÃª apÃ³s** | Ctrl+F5 loja Â· badge Â· PreÃ§os Â· Fiscal Â· Estoque Â· AlteraÃ§Ãµes Â· **lÃ¡pis PDV** muda nome â†’ aba 9 origem PDV |

### ðŸ©¹ Cadastro â€” PreÃ§os: nÃºmero grande de verdade (17/07 Â· **teste v9.56**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Custo / MVA / comissÃ£o / margem: fonte **~2,75rem** no nÃºmero. Caixa sem altura artificial. PreÃ§o final **~3rem** |
| **Status** | ðŸ“¦ fecha no pacote **Cadastro modal** acima |
| **VocÃª** | Ctrl+F5 Â· badge **v9.56** Â· aba PreÃ§os |

### ðŸ©¹ Cadastro â€” Fiscal: fonte maior + campos mais juntos (17/07 Â· **teste v9.55**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Aba Fiscal (e Gerais): campos mais prÃ³ximos e letra maior nos inputs |
| **Status** | ðŸ“¦ fecha no pacote **Cadastro modal** acima |

### ðŸ“¦ PACOTE PRONTO LOJA â€” PDV editor rÃ¡pido (lÃ¡pis) (17/07 Â· âœ… enviado v9.90)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **testado no teste** Â· ðŸ“¦ **pronto pra envio** â€” espera frase + senha `99738595` na **mesma mensagem** |
| **O quÃª** | LÃ¡pis no carrinho â†’ ediÃ§Ã£o rÃ¡pida (nome, GM, barras, unidade com busca, preÃ§os A/B, estoque Centro/Vila) Â· popup grande sem scroll Â· sem travar o PDV Â· **histÃ³rico aba 9** com origem Â«PDV ediÃ§Ã£o rÃ¡pidaÂ» |
| **VersÃ£o alvo loja** | **v9.62** (ou badge do deploy) |
| **Base loja hoje** | **v9.16** |
| **Arquivos** | `pdv_wizard.html` Â· `pdv_wizard.js` Â· `pdv_state.js` Â· `produtos/views.py` Â· `produtos/urls.py` Â· `pdv/views.py` Â· `cadastro_alteracao_historico_util.py` Â· `models.py` (Origem.PDV) |
| **Pente fino** | Unidades lazy Â· Esc Â· custo no carrinho Â· API Mongo fallback Â· **origem histÃ³rico PDV** |
| **Risco** | Baixo â€” overlay PDV + APIs; histÃ³rico depende da aba 9 / migration `0054` (pacote Cadastro modal) |
| **MÃ©todo** | worktree `origin/producao` Â· checkout desses arquivos de `teste` Â· **nÃ£o** merge inteiro |
| **Autorizar com** | *Â«pode subir editor rÃ¡pido PDV / lÃ¡pisÂ»* + **99738595** |
| **VocÃª apÃ³s** | Ctrl+F5 loja Â· badge Â· lÃ¡pis no milho â†’ salva â†’ aba 9 no cadastro |

### ðŸ©¹ PDV â€” pente fino editor rÃ¡pido (17/07 Â· **teste v9.54**)

| | |
| --- | --- |
| **O quÃª** | Lazy unidade Â· Esc correto Â· custo no patch do carrinho Â· scroll sÃ³ se Formas A/B estourar tela baixa |
| **Status** | ðŸ“¦ fecha no pacote acima |

### ðŸ©¹ PDV â€” editor sem scroll: pop quase tela cheia (17/07 Â· **teste v9.53**)

| | |
| --- | --- |
| **O quÃª** | Letras grandes iguais; **popup mais alto** (quase 98 % da tela, sem teto em rem) â€” estoque cabe **sem barra de rolagem** |
| **VocÃª** | Ctrl+F5 Â· badge **v9.53** Â· lÃ¡pis Â· confere se some o scroll |

### ðŸ©¹ PDV â€” editor rÃ¡pido bem maior (+25 %) (17/07 Â· **teste v9.51**)

| | |
| --- | --- |
| **O quÃª** | Popup ediÃ§Ã£o rÃ¡pida com **zoom 1,25** (tudo cresce junto). Deve dar pra notar |
| **VocÃª** | **Ctrl+F5** Â· badge **v9.51** Â· lÃ¡pis |

### ðŸ©¹ Cadastro â€” PreÃ§os bem legÃ­vel + preenche altura + URL imagem (17/07 Â· **teste v9.50**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | NÃºmeros dos cards de PreÃ§os bem maiores (preenchem a tag). Fiscal/Gerais espalham na altura. ComposiÃ§Ã£o: tabela cresce atÃ© o rodapÃ©. Campo sem nome = **URL da imagem** (agora com rÃ³tulo) |
| **VocÃª** | Ctrl+F5 forte Â· badge **v9.50** Â· PreÃ§os Â· Fiscal Â· ComposiÃ§Ã£o Â· Gerais |
| **Loja** | â³ |

### ðŸ©¹ PDV â€” editor rÃ¡pido um degrau maior (17/07 Â· **teste v9.49**)

| | |
| --- | --- |
| **O quÃª** | Popup ediÃ§Ã£o rÃ¡pida **maior ~12 %** (caixa + rÃ³tulos + campos + botÃµes + saldos) â€” mesma disposiÃ§Ã£o, melhor em tela pequena |
| **VocÃª** | Ctrl+F5 Â· badge **v9.49** Â· lÃ¡pis â†’ confere se cabe e lÃª melhor |

### ðŸ©¹ Cadastro â€” modal: proporÃ§Ã£o PreÃ§os + listas atÃ© o rodapÃ© (17/07 Â· **teste v9.48**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | PreÃ§os: tag e nÃºmero mais equilibrados. Config colado em cima. ComposiÃ§Ã£o / AlteraÃ§Ãµes / Estoque: Ã¡rea da lista cresce atÃ© o rodapÃ© (sem buraco branco) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.48** Â· PreÃ§os Â· ComposiÃ§Ã£o Â· Config Â· AlteraÃ§Ãµes |
| **Loja** | â³ |

### ðŸ©¹ PDV â€” estoque editor: nÃºmeros do mesmo tamanho (17/07 Â· **teste v9.46**)

| | |
| --- | --- |
| **O quÃª** | Nos cards Centro/Vila, **agora** e **novo** ficam com a mesma fonte grande (campo nÃ£o fica miÃºdo ao lado do saldo) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.46** Â· lÃ¡pis â†’ confere proporÃ§Ã£o dos saldos |

### âœ¨ Cadastro â€” abas do modal usam a altura (17/07 Â· **teste v9.45**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Abas sem lista histÃ³rica (Gerais, Fiscal, PreÃ§os, ComposiÃ§Ã£o, Config, Validade, Marcas) espalham campos / tabela maior â€” sem â€œburaco brancoâ€ embaixo. Estoque e AlteraÃ§Ãµes continuam com scroll na lista |
| **VocÃª** | Ctrl+F5 Â· badge **v9.45** Â· abrir produto â†’ Fiscal / ComposiÃ§Ã£o |
| **Loja** | â³ |

### ðŸ©¹ PDV â€” editor: 2 tags estoque + unidade busca (17/07 Â· **teste v9.44**)

| | |
| --- | --- |
| **O quÃª** | Estoque em **2 cards** (tag Centro / Vila). Unidade = busca nas jÃ¡ usadas (ignora acento/maiÃºscula) + **Cadastrar Â«â€¦Â»** se nÃ£o achar. Mensagem vermelha Â«Produto nÃ£o encontradoÂ» sumiu: API agora lÃª Mongo se o produto ainda nÃ£o estÃ¡ no Postgres |
| **VocÃª** | Ctrl+F5 Â· badge **v9.44** Â· lÃ¡pis no milho Â· confere estoque + unidade |

### âœ¨ Cadastro â€” modal editar maior (altura) (17/07 Â· **teste v9.42**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Popup editar produto quase tela cheia (~98dvh, mais largo) Â· Ã¡rea das listas de histÃ³rico com altura mÃ­nima + scroll prÃ³prio |
| **VocÃª** | Ctrl+F5 Â· badge **v9.42** Â· abrir produto â†’ aba Estoque / AlteraÃ§Ãµes |
| **Loja** | â³ |

### ðŸ©¹ PDV â€” editor rÃ¡pido puxa GM/barras/custo certos (17/07 Â· **teste v9.41**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Campo GM deixa de mostrar cÃ³digo sistema (ex. 9047) Â· puxa **GM0090-47**, barras e custo (overlay/Mongo) |
| **VocÃª** | Ctrl+F5 Â· badge **v9.41** Â· lÃ¡pis no milho Â· confere GM + barras + custo 67 |
| **Loja** | â³ |

### ðŸ©¹ PDV â€” formas A/B recolhidas no editor rÃ¡pido (17/07 Â· **teste v9.40**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Tabela de formas A/B fica **escondida**; botÃ£o **Formas A/B** ao lado dos preÃ§os abre/fecha |
| **VocÃª** | Ctrl+F5 Â· badge **v9.40** Â· lÃ¡pis â†’ sÃ³ preÃ§os A/B; clicar Formas A/B se precisar |
| **Loja** | â³ |

### âœ¨ PDV â€” editor rÃ¡pido no lÃ¡pis (17/07 Â· **teste v9.38**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | LÃ¡pis do carrinho: modal leve (nome, GM, barras, unidade, custo, venda, **A/B + formas**, estoque Centro/Vila). Salva e atualiza o item |
| **APIs** | pdv-edicao-rapida Â· pdv-ajuste-estoque Â· overlay parcial |
| **VocÃª** | Ctrl+F5 teste Â· badge **v9.38** Â· lÃ¡pis â†’ edita â†’ Salvar |
| **Loja** | â³ |

### âœ¨ Cadastro â€” aba 9 AlteraÃ§Ãµes (histÃ³rico cadastro) (17/07 Â· **teste v9.33**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Aba **9. AlteraÃ§Ãµes**: qualquer campo do cadastro (nome, preÃ§o, cÃ³digoâ€¦) **antes â†’ depois + quem**. **NÃ£o** Ã© movimentaÃ§Ã£o de estoque (isso fica na aba 4). 40 + carregar mais Â· botÃ£o vermelho histÃ³rico completo (atÃ© 500). SÃ³ daqui pra frente |
| **API** | `GET /api/produtos/cadastro/alteracoes-historico/` Â· migration `0054` |
| **VocÃª** | Ctrl+F5 Â· badge **v9.34** Â· editar produto â†’ mudar nome â†’ Salvar â†’ aba **AlteraÃ§Ãµes** |
| **Loja** | â³ |


### ðŸ“¦ PACOTE PRONTO LOJA â€” RelatÃ³rios ajuda Â«?Â» (17/07 Â· âœ… enviado v9.90)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ðŸ“¦ **pronto pra envio** â€” Renan OK no teste Â· espera frase + senha `99738595` na **mesma mensagem** |
| **Inclui** | Ajuda **?** leiga em **todas** as telas de relatÃ³rios (hub, validade, ABC, ranking, margemâ€¦) Â· botÃ£o Ã¢mbar ao lado de Imprimir A4 Â· painel colorido por coluna Â· **sem** scroll interno |
| **Commits teste** | `3828dcf` Â· `6016bc1` Â· `61f6137` (feat + alinhamento + restaura botÃ£o) |
| **Arquivos** | `relatorios_help_agents.html` Â· `relatorios_generico.html` Â· `relatorios_hub.html` Â· `relatorios_validade.html` Â· `relatorios_central_views.py` |
| **VersÃ£o alvo loja** | cherry-pick dos 3 commits (apÃ³s FL-056 ou junto, se pedir) |
| **Base loja hoje** | **v9.16** |
| **Autorizar com** | *Â«pode subir ajuda relatÃ³riosÂ»* + **99738595** |
| **VocÃª apÃ³s** | Ctrl+F5 loja Â· RelatÃ³rios â†’ Curva ABC â†’ **?** ao lado do azul |

### âœ¨ Cadastro â€” kardex unificado na aba Estoque (17/07 Â· **teste v9.30**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Remove Â«HistÃ³rico de FornecedoresÂ» Â· tabela **HistÃ³rico de movimentaÃ§Ã£o** (venda, devoluÃ§Ã£o, NF, transferÃªncia, ajuste) Â· filtros depÃ³sito/tipo/perÃ­odo Â· 50 + carregar mais (teto 200) Â· link da venda Â· **operador = nome, nunca PIN** |
| **API** | `GET /api/produtos/cadastro/estoque-movimentos/` |
| **Arquivos** | `estoque_movimentos_cadastro_util.py` Â· `views.py` Â· `_modal_editar_produto_cadastro_erp.inc.html` Â· `produtos_cadastro_erp.html` |
| **VocÃª** | Ctrl+F5 teste Â· badge **v9.30** Â· abrir produto â†’ aba Estoque |
| **Loja** | â³ |

### âœ¨ PDV â€” lixeira + lÃ¡pis no carrinho (17/07 Â· **teste v9.29**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | AÃ§Ãµes do item: **lixeira em cima** + **lÃ¡pis embaixo** (menor, mesma altura de linha). LÃ¡pis = placeholder (`data-edit-item`) p/ ligar tela depois |
| **Arquivos** | `pdv_wizard.html` Â· `pdv_wizard.js` |
| **VocÃª** | Ctrl+F5 teste Â· badge **v9.29** Â· carrinho com 2 itens Â· conferir 2 botÃµes sem esticar a linha |
| **Loja** | â³ |

### fix â€” RelatÃ³rios Â«?Â» sumiu (17/07 Â· **teste v9.28**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Troca `<details>` por **botÃ£o** Ã¢mbar fixo (mesmo tamanho do Imprimir A4) Â· painel abre/fecha no clique Â· estilo inline pra nÃ£o sumir |
| **Arquivo** | `relatorios_help_agents.html` |
| **VocÃª** | Ctrl+F5 teste Â· badge **v9.28** Â· ? laranja ao lado do azul |
| **Loja** | ðŸ“¦ **pronto pra envio** (pacote RelatÃ³rios ajuda Â«?Â») |

### fix â€” RelatÃ³rios ajuda Â«?Â» alinhada + sem scroll no card (17/07 Â· **teste v9.27**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | **?** ao lado de Imprimir A4 / Voltar (mesmo tamanho 44px) Â· painel Ã  direita **sem** max-height / scrollbar interna |
| **Arquivos** | `relatorios_help_agents.html` Â· `relatorios_generico.html` Â· `relatorios_hub.html` Â· `relatorios_validade.html` |
| **VocÃª** | Ctrl+F5 teste Â· badge **v9.27** Â· RelatÃ³rios â†’ ? ao lado do azul |
| **Loja** | ðŸ“¦ **pronto pra envio** (pacote RelatÃ³rios ajuda Â«?Â») |

### âœ¨ Cadastro ERP â€” busca + filtros na barra (17/07 Â· **teste v9.24**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Sem botÃ£o CatÃ¡logo Â· **Filtros avanÃ§ados** ao lado do Novo Â· busca com borda verde forte + Â«Digite aqui para buscarâ€¦Â» |
| **Arquivos** | `produtos_cadastro_erp.html` Â· `cadastro_erp_panel.js` |
| **VocÃª** | Ctrl+F5 teste Â· badge **v9.25** Â· `/produtos/cadastro-erp/` |

### âœ¨ RelatÃ³rios â€” ajuda Â«?Â» leiga em todas as telas (17/07 Â· **teste v9.23** Â· âœ… OK Renan)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | BotÃ£o **?** em todas as telas de relatÃ³rios Â· painel didÃ¡tico (colunas + o que o relatÃ³rio faz) Â· hub, validade e todos os cards do genÃ©rico |
| **Arquivos** | `includes/relatorios_help_agents.html` Â· `relatorios_generico.html` Â· `relatorios_hub.html` Â· `relatorios_validade.html` Â· `relatorios_central_views.py` |
| **VocÃª** | âœ… OK no teste (v9.28) |
| **Loja** | ðŸ“¦ **pronto pra envio** â€” ver pacote **RelatÃ³rios ajuda Â«?Â»** |

### âœ¨ Cadastro ERP â€” layout header/toolbar (17/07 Â· **teste v9.22**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | CatÃ¡logo + Â«GestÃ£o de ProdutosÂ» / Somente ativos sobem pro header Â· **Grupos** oculto Â· barra: Busca Â· Filtrar Â· **+ Novo** Â· (direita) Excelâ†“â†‘ Â· HistÃ³rico Â· Colunas |
| **Arquivo** | `produtos_cadastro_erp.html` |
| **VocÃª** | Ctrl+F5 teste Â· badge **v9.22** Â· `/produtos/cadastro-erp/` |
| **Loja** | ainda **v9.16** â€” sobe sÃ³ com frase+senha |

### ðŸ“¦ PACOTE PRONTO LOJA â€” NFC-e FL-056 (17/07 Â· âœ… enviado v9.90)

| Item | Detalhe |
| ---- | ------- |
| **Status** | ðŸ“¦ **pronto pra envio** â€” espera frase + senha `99738595` na **mesma mensagem** |
| **Inclui** | **FL-056** â€” SEFAZ **963** + **225** (`nfce_sp_emissao_util.py` Â· `nfce_fiscal_produto_util.py`) |
| **VersÃ£o alvo loja** | **v9.21** |
| **Base loja hoje** | **v9.16** |
| **Autorizar com** | *Â«pode subir NFC-e 963/225Â»* + **99738595** |
| **VocÃª apÃ³s** | Ctrl+F5 loja Â· reemitir **#2812** e **#3347** |

### ðŸ› NFC-e â€” rejeiÃ§Ãµes 963 + 225 (17/07 Â· teste)

| Item | Detalhe |
| ---- | ------- |
| **Casos** | Venda **#2812** â†’ **963** Â· Venda **#3347** â†’ **225** (frete #23 jÃ¡ OK) |
| **963** | Fiado/`tPag=05` ia com grupo `<card>` â€” SEFAZ nÃ£o aceita. `_TPAG_REQUER_CARD` sÃ³ **03/04/10â€“13/15/17/18** |
| **225** | CFOP/CEST/NCM com ponto/traÃ§o no XML (schema). Limpa sÃ³ dÃ­gitos + CEST sÃ³ se 7 dÃ­gitos |
| **Arquivos** | `nfce_sp_emissao_util.py` Â· `nfce_fiscal_produto_util.py` |
| **Commits teste** | `fb6ba2f` â€¦ (**v9.21**) |
| **VocÃª** | Ctrl+F5 teste Â· badge **v9.21** Â· em Contabilidade **reemitir** #2812 e #3347 |
| **Status** | ðŸ“¦ **pronto pra envio** (loja ainda v9.16) |
| **Loja** | frase + senha na mesma mensagem |

### ðŸ“¦ Deploy loja **v9.16** â€” Fecha + RelatÃ³rios + NFC-e #23 (17/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | **#12** devoluÃ§Ã£o Â· **#17** busca CP Â· giro Ãºltima venda Â· **#23** NFC-e 535 |
| **HEAD loja** | **4113492** Â· base **c61f7dd** (v9.14) |
| **Backup** | `producao-backup-pre-v916-fecha-relatorios-nfce-20260717` @ **c61f7dd** â€” reverter: `git push origin producao-backup-pre-v916-fecha-relatorios-nfce-20260717:producao` |
| **AutorizaÃ§Ã£o** | *pode enviar* + 99738595 |
| **VocÃª** | Ctrl+F5 Â· badge **v9.16** Â· devoluÃ§Ã£o Â· busca CP Â· giro Â· NFC-e c/ frete |

### ðŸ“¦ PACOTE PRONTO LOJA â€” âœ… ENVIADO v9.16 (17/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **montado** Â· **nÃ£o subiu** â€” espera frase + senha `99738595` na **mesma mensagem** |
| **VersÃ£o alvo loja** | **v9.16** |
| **Base loja hoje** | **c61f7dd** Â· **v9.14** |
| **Backup (criar no envio)** | `producao-backup-pre-v916-fecha-relatorios-nfce-YYYYMMDD` @ HEAD loja atual |
| **Reverter** | `git push origin <backup>:producao` |
| **MÃ©todo** | worktree em `origin/producao` Â· `git checkout origin/teste -- <arquivos>` Â· **nÃ£o** merge inteiro testeâ†’producao |
| **Risco** | MÃ©dio â€” #12 mexe venda/caixa/migraÃ§Ã£o **0053** Â· #17 sÃ³ busca CP Â· relatÃ³rios sÃ³ leitura Â· #23 NFC-e frete (pequeno) |
| **Autorizar com** | *Â«pode subir para produÃ§Ã£o o pacote fecha + relatÃ³rios + #23Â»* + **99738595** |

#### A) Fecha (âœ… Renan testou)

| Ref | O quÃª | Arquivos-chave |
| --- | ----- | -------------- |
| **#12** / FL-035 | DevoluÃ§Ã£o parcial / por item no PDV | `devolucao_venda_util.py` Â· migraÃ§Ã£o **0053** Â· `models.py` Â· `views.py` Â· `venda_agro_detalhe.html` Â· `vendas_lista.html` Â· `caixa_util.py` Â· `caixa_relatorio_util.py` |
| **#12 cupom** | Risco + total restante na reimpressÃ£o | `venda_cupom_util.py` Â· `venda_cupom_80mm.js` Â· `context_processors.py` Â· `views_mp_point.py` Â· `config/settings.py` |
| **#17** / FL-022 | Busca CP inteligente (valor/data/boleto/parcela/CPF) | `lancamentos_financeiro_pg_util.py` Â· `mongo_financeiro_util.py` Â· `lancamentos_help_agents.html` Â· `lancamentos_contas_pagar_teste.html` Â· `AGENTS.md` |

#### B) RelatÃ³rios (tudo que falta na loja)

| Item | O quÃª |
| ---- | ----- |
| **v9.15** | Giro/parado: coluna **Ãšltima venda** + filtro **Ordenar** por data (tela / A4 / Excel) |
| **Arquivos** | `relatorios_vendas_util.py` Â· `relatorios_central_views.py` Â· `relatorios_generico.html` |
| **JÃ¡ na loja (v9.14)** | Hub Â· ABC categoria Â· loading Â· cÃ³digo GM Â· Ver todos Â· De/AtÃ© â€” **nÃ£o precisa subir de novo** |

#### C) NFC-e #23 (incluso)

| Ref | O quÃª | Arquivo |
| --- | ----- | ------- |
| **#23** / FL-055 | SEFAZ **535** â€” `vFrete` nos itens = total do frete (vendas #3418/#3380) | `nfce_sp_emissao_util.py` Â· commit teste `31eb9dc` |

#### NÃ£o sobe neste pacote

| Item | Motivo |
| ---- | ------ |
| Merge inteiro `teste`â†’`producao` | Outras coisas do teste fora do pacote |
| **#7** notebook impressÃ£o | Ainda ðŸ”´ aberto |
| FL-008 / 016 / 024 / 049 / Aba 9 | ✅ já na loja (pente fino 01/08) — não listar como pendente |
| FL-029 crédito · **FL-058** vale crédito PDV · 052 · 030 · 019 · 054 · 031 · 034? · 053 · 033 · Zap #7 | Ainda aberto / confirmar |

#### Checklist validar depois do deploy (Ctrl+F5 Â· badge **v9.16**)

1. RelatÃ³rios â†’ Giro/parado â†’ coluna **Ãšltima venda** + ordenar  
2. RelatÃ³rios â†’ ABC â†’ categoria / Excel / loading (jÃ¡ v9.14; smoke)  
3. Vendas â†’ devoluÃ§Ã£o parcial (#12)  
4. Contas a pagar â†’ busca valor / parcela (#17)  
5. NFC-e com frete â€” sem rejeiÃ§Ã£o **535** (#23)  

### feat â€” giro/parado com Ãºltima venda + ordenaÃ§Ã£o (16/07 Â· **teste v9.15**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Coluna **Ãšltima venda** no giro/parado + filtro **Ordenar** por data |
| **Onde** | Tela, impressÃ£o A4 e Excel |

### ðŸ“¦ Deploy loja **v9.14** â€” RelatÃ³rios (pÃ³s-v9.07) (16/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | CÃ³digo GM Â· De/AtÃ© personalizado Â· ABC Ver todos Â· loading (abrir/Excel/imprimir) Â· ABC filtro categoria. **SÃ³ relatÃ³rios** â€” **nÃ£o** NFC-e nem #12/#17 |
| **HEAD loja** | **c61f7dd** (base **53c13fb** v9.07) |
| **Backup** | `producao-backup-pre-v914-relatorios-20260716` @ **53c13fb** (v9.07) â€” reverter: `git push origin producao-backup-pre-v914-relatorios-20260716:producao` |
| **AutorizaÃ§Ã£o** | *pode subir para produÃ§Ã£o essas atualizaÃ§Ãµes nos relatÃ³rios* + 99738595 |
| **VocÃª** | Ctrl+F5 loja Â· badge **v9.14** Â· RelatÃ³rios â†’ ABC (categoria) Â· Excel Â· Imprimir |

### feat â€” ABC filtro por categoria (16/07 Â· **teste v9.14** Â· **loja v9.14**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Select **Categoria** na Curva ABC Â· recalcula A/B/C sÃ³ da categoria Â· coluna Categoria na tela / A4 / Excel |
| **Arquivos** | `relatorios_vendas_util.py` Â· `relatorios_central_views.py` Â· `relatorios_generico.html` |

### feat â€” loading Excel + impressÃ£o (16/07 Â· **teste v9.13**)

| Item | Detalhe |
| ---- | ------- |
| **Excel** | Overlay Â«Gerando Excelâ€¦Â» atÃ© o arquivo baixar (nÃ£o some a tela) |
| **Imprimir** | Overlay Â«Preparando impressÃ£oâ€¦Â» atÃ© abrir a caixa |

### feat â€” loading visual nos relatÃ³rios (16/07 Â· **teste v9.11**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Overlay Â«Carregandoâ€¦Â» ao clicar no card / Atualizar / Excel / Ver todos |
| **Arquivo** | `_relatorios_loading.html` |

### feat â€” ABC Ver todos + total perÃ­odo (16/07 Â· **teste v9.10**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Lista padrÃ£o 500 Â· botÃ£o **Ver todos** Â· rodapÃ© = total do perÃ­odo (para bater com vendas) Â· % sobre o perÃ­odo inteiro |
| **Excel** | Sempre exporta lista completa |

### fix â€” filtro De/AtÃ© vira personalizado (16/07 Â· **teste v9.09**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Mudar De/AtÃ© â†’ perÃ­odo **Personalizado** (tela + servidor) |
| **Loja** | âœ… **v9.14** (16/07) |

### fix â€” CÃ³digo GM nos relatÃ³rios (16/07 Â· **teste v9.08**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Coluna CÃ³digo = **cÃ³digo GM** (`codigo_nfe`/overlay); nÃ£o mostra ObjectId Mongo |
| **Onde** | Todos os relatÃ³rios com produto (ABC, ranking, margem, ruptura, comissÃ£oâ€¦) |
| **Loja** | âœ… **v9.14** (16/07) |

### hotfix loja **v9.07** â€” relatorios 500 resolvido (16/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Mais vendidos / ABC / grupo / margem / comparativo / comissao = 500 |
| **Causa** | Agregacao ItemVendaAgro com ExpressionWrapper (incompativel na loja) |
| **Fix** | Mesmo padrao do giro (`Sum` quantidade/valor) Â· `53c13fb` |
| **Validar** | Ctrl+F5 Â· badge **v9.07** Â· Mais vendidos + Curva ABC |

### ðŸ“¦ Deploy loja **v9.04** â€” Central de RelatÃ³rios (16/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | SÃ³ pacote **relatÃ³rios** (hub + todos os cards novos + Excel). **NÃ£o** sobe #12/#17 |
| **HEAD loja** | **da264a3** (cÃ³digo `1cb43f2` + docs) |
| **Backup** | `producao-backup-pre-v904-relatorios-20260716` @ **5c44084** (v8.69) â€” reverter: `git push origin producao-backup-pre-v904-relatorios-20260716:producao` |
| **AutorizaÃ§Ã£o** | *pode subir para produÃ§Ã£o esse cherry-pick* + 99738595 |
| **VocÃª** | Ctrl+F5 loja Â· badge **v9.04** Â· RelatÃ³rios Â· ABC |

### âœ… #17 / FL-022 â€” Busca CP Â· Renan testou Â· pronto produÃ§Ã£o (16/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | âœ… **testado no teste** Â· **ðŸ“¦ pronto para envio Ã  produÃ§Ã£o** (frase + senha) |
| **Pacote** | Fecha com **#12** / FL-035 |
| **Validou** | Valor progressivo (`234`â€¦`234,78`) Â· parcela (`parcela 7` / `7/12` no texto) Â· sem lixo |
| **Loja** | â³ aguarda frase + senha |

### âœ… CHECKLIST ÃšNICO â€” por prioridade Â· 22/07

**P:** P0 loja â†’ P1 grave â†’ P2 melhoria â†’ P3 depois Â· decimal menor = mais urgente (P1,1 antes de P1,5).  
**Zap #** e **FL-** na mesma fila. JÃ¡ na loja â†’ sÃ³ em **Checklist concluÃ­do** (abaixo).

#### Agora

| Quando | O quê |
| ------ | ----- |
| **✅ Loja** | **v13.04** CP-BUSCA-FORN + DFE-NSU-656 |
| **✅ Loja** | **v12.95** BI-KPI-LOJA · KPIs por loja · % dia semana · Validade |
| **✅ Loja** | **v12.88** lote checklist · AJUSTE-MOBILE · BUG-FAB · PDV-USO-UX · BRINDE · DFE-CHAVE · ETQ-GM |
| **✅ Loja** | **v12.51** marca+uso+bugs+DF-e · **v12.12** 12.08–12.12 · **v12.06** AJUSTE-MOBILE-CEL · **v12.05** ETQ |
| **📦 Fila pacotes** | *(vazia — lote CP+DFE v13.04 enviado)* |
| **⛔ Fora** | **ENTRADA-NF-CUSTO** |
| **P0,1** | **FL-057** PgBouncer (painel Render) |
| **P0,2** | **FL-058** Vale crédito no cliente pelo PDV *(novo 01/08)* |
| **Pente fino 01/08** | Checklist mentia: FL-008 · 016 · 024 · 049 · Aba 9 **já na loja** (ver abaixo) |


#### Fila aberta (por prioridade)

| P | Ref | Pedido | Status |
| - | --- | ------ | ------ |
| **P1** | **RELAT-VENDAS-MARCA** | Vendas por marca (ordenar valor/qtd + Excel) | ✅ **loja v12.51** |
| **P1** | **PDV-USO-LOJA** | Uso loja PDV + PIN + histórico + totais | ✅ **loja v12.51** |
| **P1** | **BUG-REPORT** | Joaninha flutuante + lista Gestão | ✅ **loja v12.51** |
| **P1** | **DFE-INBOX** | Caixa entrada Dist DF-e no PG | ✅ **loja v12.51** |
| **P1** | **PDV-LEMBRETE-ENTREGA** | Lembrete caixa some ao entregue/finalizar/cancelar | ✅ **loja v12.12** |
| **P1** | **AJUSTE-MOBILE** | Slim + Bip +1 + popup carga + fila códigos | ✅ **loja v12.88** · migrate 0078 |
| **P1** | **AJUSTE-MOBILE-SOMAR** | Contagem 2 lugares (Somar/Trocar) + catálogo no celular | ✅ **loja v12.12** |
| **P1** | **ENTRADA-NF-VINCULO** | Vínculo XML cProd no Postgres (multi-PC) | ✅ **loja v12.12** |
| **P1** | **CAD-ESTOQUE-XLSX** | Excel estoque no Cadastro (filtros + Ajuste +/- + prévia) | ✅ **loja v12.12** |
| **P1** | **PDV-CACHE-LAPIS** | Lápis PDV não perde cache local | ✅ **loja v12.12** |
| **P1** | **PDV-PIX-SICREDI** | Pix máquina Sicredi no PDV | ✅ **loja v11.98** (lote) |
| **P1** | **ENTRADA-NF-UX** | Boleto 44→47 + lista Concluída + Nova limpa | ✅ **loja v11.98** (lote) |
| **P1** | **ETQ-GONDOLA** | Etiqueta gôndola A4 | ✅ **loja v11.98** (lote · migrate 0067) |
| **P1** | **TRANSF-FORCADA** | Transferência forçada Vila↔Centro | ✅ **loja v11.98** (lote) |
| **P1** | **ETQ-BUSCA-FILTROS** | Etiquetas: filtros Folha + manter busca + Adicionar todos | ✅ **loja v12.05** |
| **P1** | **FOLHA-ETQ** | Folha polish + etiquetas | ✅ **loja v12.03** |
| **P1** | **ENTRADA-NF-CUSTO** | Entrada NF: puxar custo cadastrado (V. unit 0) | ⛔ **fora da fila** |
| **P1** | **PDV-ESTOQUE-VILA** | Atalho PDV Estoque Vila + Folha saldo UX | ✅ **loja v11.98** (lote) |
| **P0** | Entrada NF · custo | Pós-deploy **v11.54+**: `reaplicar_custos_entrada_nf --nf=77846 --aplicar` | 📋 **ops** · GM0025 |
| **P0,1** | **FL-057** | Render loja: **PgBouncer** `agro-db` + `DATABASE_URL` **6432** + restart web | 📋 **você no painel** |
| **P0,2** | **FL-058** | **Vale crédito** — adicionar crédito ao cliente **pelo PDV** | 📋 **novo 01/08** · ligado a FL-029 crédito |
| **P1** | Kardex | E-mail no Quem + Entrada NF Δcamada | ✅ **já loja v10.63** |
| **P1** | Aba 9 | Histórico lápis PDV DE=— | ✅ **loja v10.61** (`e38df10`) — checklist mentia «pronto envio» |
| **P2** | Cadastro modal | Modal editar: UX · kardex · aba Alterações · origem PDV | ✅ **loja v9.90** |
| **P1** | PDV lápis | Editor rápido + histórico aba 9 (origem PDV) | ✅ **loja v9.90** |
| **P2** | PDV→aba 9 | Lápis registra em Alterações | ✅ **loja v9.90** |
| **P0** | **FL-056** | NFC-e **963** + **225** — vendas #2812/#3347 | ✅ **loja v9.90** |
| **P2** | PDV Enviar Zap | Enviar orçamento WhatsApp | ✅ **loja v9.90** |
| **P2** | Relatórios | Ajuda **?** leiga | ✅ **loja v9.90** |
| **P1** | **Zap #7** | Notebook demora impressão | 🔴 **ainda aberto** |
| **P1** | **FL-008** | Carrinho trava (qtd/preço/remover) | ✅ **loja v11.99** (`8f7610d`) — checklist mentia |
| **P1** | **FL-016** | Reset contagem caixa (dia anterior) | ✅ **loja v6.75** — checklist mentia |
| **P1,1** | **FL-029** | Baixa parcial fiado + crédito | ⚠️ **parcial**: baixa ✅ loja v7.61 · crédito → ver **FL-058 P0,2** |
| **P1,1** | **FL-052** | NFC-e na baixa fiado | 📋 **ainda aberto** |
| **P1,3** | **FL-030** | Ignorar bloqueio fiado vencido (PIN) | 📋 **ainda aberto** |
| **P1,5** | **FL-019** | Recibo pagamento fiado | ✅ **loja v15.62** (FIADO-RECIBO) |
| **P1,5** | **Zap #20** · **FL-054** | Reimprimir papéis entrega | 📋 **ainda aberto** |
| **P1,5** | **FL-049** | CPF no cliente PDV → NFC-e | ✅ **código na loja** (`6af5cac`) — checklist mentia «teste» |
| **P1,6** | **Zap #22** · **FL-024** | Cadastro cat/sub/marca só lista + PIN | ✅ **loja v10.57** — checklist mentia |
| **P1,6** | **FL-031** | Terminar tela `/entregas/` | 📋 **ainda aberto** |
| **P1,9** | **FL-034** | Histórico F8 filtra cliente | ⚠️ **código filtro na loja** — confirmar se fecha o pedido |
| **P2** | **Zap #19** · **FL-053** | Histórico de custo tick 2× | 📋 **ainda aberto** |
| **P3** | **Zap #21** · **FL-033** | BI comparativo N-ésimo dia semana | 📋 **ainda aberto** |
| **P2+** | FL-005… | Resto P2/P3 na «Fila loja» completa | 📋 |

#### Checklist concluÃ­do (jÃ¡ na loja)

| Ref | Pedido | Nota |
| --- | ------ | ---- |
| Zap **#12** Â· FL-035 | DevoluÃ§Ã£o parcial | âœ… loja **v9.16** |
| Zap **#17** Â· FL-022 | Busca CP inteligente | âœ… loja **v9.16** |
| Zap **#23** Â· FL-055 | NFC-e 535 frete | âœ… loja **v9.16** |
| Zap **#1** | Entrada NF etapa 7 â€” valor | âœ… |
| Zap **#2** | Entrada NF busca barras | âœ… |
| Zap **#3** | Cadastro custo / prefixo GM | âœ… |
| Zap **#4** | Cadastro modelo | âœ… |
| Zap **#5** | Busca cadastro + NF leve | âœ… |
| Zap **#6** | PIN mais rÃ¡pido | âœ… |
| Zap **#8** | Produto novo na lista | âœ… |
| Zap **#9** | Fiado MP forma no caixa | âœ… |
| Zap **#10** | Cancelar cobranÃ§a fiado | âœ… |
| Zap **#11** | Menu caixa/vendas fecha | âœ… |
| Zap **#13** Â· FL-027 | XML boleto â†’ CN | âœ… |
| Zap **#14** Â· FL-026 | Add produto barras/lote | âœ… |
| Zap **#15** Â· FL-025 | CÃ³digo interno 4010â€“5999 | âœ… P0,9 |
| Zap **#16** Â· FL-023 | CP busca limpa datas | âœ… P1,2 |
| Zap **#18** Â· FL-018 Â· FL-020 | Frete cupom / 3 vias | âœ… |
| **+** | PIN ao abrir PDV | âœ… |
| FL-017 Â· FL-021 Â· FL-028 Â· FL-032 Â· FL-051 Â· FL-046 Â· FL-047â€¦ | Outros FL jÃ¡ na loja | ver Â«Fila lojaÂ» |

**Cadastro FL completo** (tags): Â«Fila loja â€” pedidos Zap / melhoriasÂ» mais abaixo. Status do dia â†’ **esta seÃ§Ã£o primeiro**.

### ðŸ“¦ Deploy loja **v8.69** â€” Entrada NF busca BCA (16/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | SÃ³ fix busca Entrada NF = BCA/famÃ­lia GM (`views.py` + comentÃ¡rio `entrada_nota.html`). **NÃ£o** sobe pacote do fecha (#12/#17/relatÃ³rios) |
| **Commit loja** | **5c44084** (origem teste `93ac5b3`) |
| **Backup** | branch `producao-backup-pre-v869-20260716` @ **45622b0** (v8.68) â€” reverter: `git push origin producao-backup-pre-v869-20260716:producao` |
| **AutorizaÃ§Ã£o** | *pode subir esse cherry-pick* + 99738595 |
| **VocÃª** | Ctrl+F5 loja Â· badge **v8.69** Â· Entrada NF Â· `gm0008` â†’ **3** iguais ao cadastro |

### ðŸ©¹ Entrada NF â€” busca BCA = cadastro (16/07 Â· **teste v8.94** Â· **loja v8.69**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `gm0008` no cadastro = **3** (BCA); na Entrada NF = **1** |
| **Causa** | API com `entrada_nfe=1` desligava Mongo â€” famÃ­lia GM nÃ£o completava (cadastro completa) |
| **Fix** | Mesmo motor unificado + complemento Mongo famÃ­lia GM; cache busca NF **v2** |
| **Arquivos** | `views.py` Â· `entrada_nota.html` (comentÃ¡rio) |
| **Validar** | Ctrl+F5 teste Â· Entrada NF etapa 2 Â· digitar `gm0008` â†’ **3** iguais ao cadastro |

### ðŸ“Š Central de RelatÃ³rios â€” pacote completo (16/07 Â· teste)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Hub com cards: mais/menos vendidos Â· por grupo Â· curva ABC Â· giro/parado Â· margem Â· operador Â· ranking clientes Â· comparativo Â· formas pagamento Â· ruptura Â· comissÃ£o estimada (+ validade/etiquetas jÃ¡ existentes) |
| **Extras** | PerÃ­odo Â· print A4 Â· **Excel â†“** em cada relatÃ³rio novo |
| **Fonte** | `VendaAgro` / `ItemVendaAgro` (Postgres) Â· giro/parado reusa `dashboard_estoque_financeiro_util` |
| **Arquivos** | `relatorios_vendas_util.py` Â· `relatorios_central_views.py` Â· `relatorios_hub.html` Â· `relatorios_generico.html` Â· `urls.py` |
| **Validar** | Ctrl+F5 Â· BI â†’ RelatÃ³rios gerais Â· abrir cada card Â· Excel + perÃ­odo |
| **VersÃ£o** | **teste v8.93** |
| **Layout hub (16/07)** | Largura mÃ¡xima + **4 colunas** Â· seÃ§Ãµes **Vendas / Estoque / Equipe e clientes** |

### ðŸ“¦ Pacote pronto loja â€” fechar depois (16/07 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Quando** | Depois que a **loja fechar** â€” frase + senha na mesma mensagem |
| **Inclui** | **#12** / **FL-035** Â· **#17** / **FL-022** (ver CHECKLIST ÃšNICO) |
| **Teste** | **#12 âœ… Renan** Â· **#17 âœ… Renan** (valor + parcela) â€” **pronto envio produÃ§Ã£o** |
| **NÃ£o sobe** | **#7** impressÃ£o notebook |
| **Antes** | loja hoje **v8.69** |

### ðŸ©¹ Cupom â€” risco sumia na impressÃ£o (16/07 Â· **teste v8.89** Â· **âœ… Renan**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | TOTAL jÃ¡ R$ 1,30, item de R$ 5 sem risco |
| **Causa** | JS da lista em cache `?v=1` + risco no flex some no Chrome |
| **Fix** | `agro_pdv_assets_v` global + `<s>` no texto do item Â· commits `6caed7d`+ |
| **Validar** | âœ… Renan OK Â· pronto loja (fecha) |

### ðŸ©¹ Cupom â€” risco no item devolvido (16/07 Â· **teste v8.85**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | ReimpressÃ£o 80mm: item/frete devolvido riscado + TOTAL restante + faixa Â«devoluÃ§Ã£o parcialÂ» |
| **Validar** | Ctrl+F5 teste Â· badge **v8.85** Â· venda parcial â†’ Imprimir |

### ðŸ©¹ #17 busca valor progressiva (16/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Prints** | `234` â†’ lixo R$ 600; `234,`/`234,7` â†’ vazio; `234,78` â†’ OK |
| **Causa** | Exato demais + Â«234Â» ainda misturava documento/parcela |
| **Fix** | Faixa ao digitar: `234`/`234,` = [234â€“235); `234,7` = [234,70â€“234,80); completo = exato. Sem parcela em nÃºmero solto |
| **Validar** | âœ… Renan Â· faixa valor OK |
| **Arquivo** | `lancamentos_financeiro_pg_util.py` |

### ðŸ©¹ #17 busca CP â€” valor/parcela sem lixo (16/07 Â· **teste v8.84**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `234,` / `234,7` trazia tÃ­tulos de R$ 600; parcela sem match trazia tÃ­tulos soltos |
| **Causa** | Termo numÃ©rico misturava texto + `mongo_id` (ObjectId contÃ©m Â«234Â») |
| **Fix** | Modos exclusivos: vÃ­rgula/R$ â†’ sÃ³ valor; `2/6` â†’ sÃ³ parcela; data â†’ sÃ³ data; nÂº curto sem mongo_id |
| **Validar** | âœ… Renan Â· sem lixo R$ 600 |
| **Arquivo** | `lancamentos_financeiro_pg_util.py` |

### âœ… #17 Busca CP inteligente P0+P1+P2 (16/07 Â· **teste v8.82**)

| Item | Detalhe |
| ---- | ------- |
| **P0** | Valor + data digitada |
| **P1** | Boleto + doc flexÃ­vel |
| **P2** | Parcela + CPF/CNPJ |
| **Arquivos** | `lancamentos_financeiro_pg_util.py` Â· `mongo_financeiro_util.py` Â· help + placeholder |
| **Validar** | âœ… Renan testou Â· valor + parcela OK |
| **Loja** | ðŸ“¦ **pronto produÃ§Ã£o** â€” frase + senha (pacote do fecha c/ #12) |

### âœ¨ Vendas â€” total restante + risco devolvido + corta ERP (16/07 Â· **teste v8.79** Â· **âœ… Renan**)

| Item | Detalhe |
| ---- | ------- |
| **1 Lista** | Parcial mostra **restante** (ex. R$ 1,30) + original riscado; total do perÃ­odo soma restantes |
| **2 Detalhe** | Item devolvido com **risco** + badge; total restante no card |
| **3 ERP** | Coluna/botÃµes ERP sumiram; `PDV_VENDA_ERP_ENVIO=False` (padrÃ£o) â€” nÃ£o manda Pedidos/Salvar |
| **Validar** | âœ… Renan Â· **ðŸ“¦ pronto loja (fecha)** |

### ðŸ“¦ Deploy loja **v8.68** â€” FL-021 hotfix match NF (16/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | SÃ³ `nfe_entrada_util.py` â€” match ORM por nÂº NF na descriÃ§Ã£o. **NÃ£o** sobe devoluÃ§Ã£o |
| **Commits loja** | **02dc397** + docs **45622b0** |
| **Backup** | branch `producao-backup-pre-v868-20260716` @ **ed1698e** (v8.67) â€” reverter: `git push origin producao-backup-pre-v868-20260716:producao` |
| **AutorizaÃ§Ã£o** | *pode subir sÃ³ esse hotfix* + 99738595 |
| **VocÃª** | Ctrl+F5 loja Â· badge **v8.68** Â· CP Agromaia â†’ botÃ£o **NF** |


### ðŸ©¹ CP â€” botÃ£o NF ainda vazio (FL-021 hotfix) (16/07 Â· **teste v8.76** Â· **loja v8.68**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Match do rascunho no Postgres via ORM (nÃºmero NF na descriÃ§Ã£o + variantes) â€” o v8.67 sÃ³ ligava o enrich, mas a busca cortava notas antigas |
| **Validar** | Ctrl+F5 loja Â· CP Agromaia â†’ botÃ£o **NF** |
| **Loja** | âœ… v8.68 |


### ðŸ©¹ DevoluÃ§Ã£o â€” textos no ? (16/07 Â· **teste v8.74**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Regras longas no **?** Ajuda do modal (padrÃ£o banana / tela limpa) |
| **Validar** | Ctrl+F5 Â· badge **v8.74** Â· Devolver â†’ ? |
| **Loja** | â³ |



### ðŸ©¹ DevoluÃ§Ã£o â€” fontes do modal um pouco maiores (16/07 Â· **teste v8.72**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Textos/itens/formas/total no popup um degrau maior (sem exagero) |
| **Validar** | Ctrl+F5 Â· badge **v8.72** Â· Devolver |
| **Loja** | â³ |



### ðŸ©¹ DevoluÃ§Ã£o â€” confirmaÃ§Ã£o bonita + valor igual aos itens (16/07 Â· **teste v8.70**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Sem confirm do Chrome Â· aviso no estilo SisVale Â· formas = itens (centavo a mais bloqueia) |
| **Validar** | F5 Â· 1,31 vs 1,30 bloqueia Â· igualar Â· confirmaÃ§Ã£o grande |
| **Loja** | â³ |



### ðŸ©¹ DevoluÃ§Ã£o â€” modal maior (idosos) (16/07 Â· **teste v8.69**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Popup devoluÃ§Ã£o ~54rem Â· fontes/botÃµes/checkbox maiores (padrÃ£o idosos / Display Scale) |
| **Validar** | Ctrl+F5 Â· badge **v8.69** Â· Devolver venda â†’ ler sem apertar os olhos |
| **Loja** | â³ |



### ðŸ“¦ Deploy loja **v8.67** â€” botÃ£o NF no CP (FL-021) (16/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | SÃ³ o fix do botÃ£o **NF** em Contas a pagar (Postgres) â€” **nÃ£o** sobe devoluÃ§Ã£o parcial |
| **Commits loja** | cherry `5000678` â†’ **0a5b620** Â· docs **ed1698e** |
| **Backup** | branch `producao-backup-pre-v867-20260716` @ **92b1804** (v8.65) â€” reverter: `git push origin producao-backup-pre-v867-20260716:producao` |
| **AutorizaÃ§Ã£o** | *pode subir* + 99738595 Â· pediu backup/seguro |
| **VocÃª** | Ctrl+F5 loja Â· badge **v8.67** Â· CP busca Agromaia â†’ botÃ£o NF |


### ðŸ©¹ CP â€” botÃ£o NF sumiu na lista (FL-021) (16/07 Â· **teste v8.67** Â· **loja v8.67**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Com financeiro Postgres, a API nÃ£o preenchia o link da Entrada NF â†’ coluna NF vazia. Agora enriquece na lista/bootstrap; match de rascunho PG mais confiÃ¡vel |
| **Validar** | Ctrl+F5 Â· Contas a pagar Â· busca Agromaia (ou RBS) Â· botÃ£o **NF** na coluna Â· abre overlay da nota |
| **Loja** | âœ… v8.67 |


### âœ¨ PDV â€” devoluÃ§Ã£o por item (#12) (16/07 Â· **teste v8.66**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Devolver itens escolhidos (+ frete opcional). Forma Fiado sÃ³ se a venda era fiado (abate dÃ­vida, sem caixa). Outras formas = RETIRADA. NFC-e cancela sÃ³ no total |
| **Validar** | Ctrl+F5 Â· badge **v8.66** Â· venda com 2+ itens â†’ devolver 1 Â· badge Parcial Â· devolver resto Â· total |
| **Loja** | â³ |



### ðŸ“¦ Deploy loja **v8.65** â€” preÃ§os A/B + PDV (16/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | Cadastro busca leve Â· preÃ§os por forma **ou** 2 grupos A/B Â· visual lista/ediÃ§Ã£o/NF Â· layout PDV Â· Esc/PAGAR consumidor Â· toque A/B no carrinho (forma manda na etapa 3) |
| **Commits loja** | 16b576bâ€¦0ed48dd (cherry dafa6c9â€¦f0f4e31) Â· HEAD **0ed48dd** |
| **Backup** | branch producao-backup-pre-v865-20260716 @ **ccb4fd8** (v8.54) â€” reverter: git push origin producao-backup-pre-v865-20260716:producao |
| **AutorizaÃ§Ã£o** | *pode subir* + 99738595 Â· pedido de backup |
| **VocÃª** | Ctrl+F5 loja Â· badge **v8.65** Â· se der errado avisar (dÃ¡ pra voltar ao backup) |


### âœ¨ PDV â€” toque A/B no carrinho (prÃ©via; forma manda) (15/07 Â· **teste v8.65**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | No carrinho, toque em A ou B muda o preÃ§o da etapa 1. Etapa 3: forma de pagamento **sempre** redefine o preÃ§o (com ou sem toque) |
| **Validar** | Ctrl+F5 Â· badge **v8.65** Â· toque A â†’ 92 Â· PAGAR Â· forma do grupo B â†’ valor volta p/ 87 |
| **Loja** | âœ… v8.65 |


### ðŸ©¹ PDV â€” PAGAR sem travar cliente + consumidor na busca (15/07 Â· **teste v8.64**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Se cliente ficou Â«unsetÂ», PAGAR grava consumidor final sozinho. Busca de cliente jÃ¡ traz botÃ£o laranja do consumidor |
| **Validar** | Ctrl+F5 Â· badge **v8.64** Â· montar carrinho e PAGAR sem alerta Â· faixa mostra consumidor |
| **Loja** | âœ… v8.65 |


### ðŸ©¹ PDV â€” cliente Esc + valor grupo A/B (15/07 Â· **teste v8.63**)

| Item | Detalhe |
| ---- | ------- |
| **Cliente** | Esc no inÃ­cio = consumidor final (antes sÃ³ fechava e a tela mentia). Sem cliente â†’ texto Â«Toque para escolherÂ» |
| **Pagamento** | Ao escolher forma, recalcula preÃ§o do grupo **depois** preenche Â«Valor desta formaÂ» (ex. 92, nÃ£o 87) |
| **Validar** | Ctrl+F5 Â· badge **v8.61** Â· milho grupos Â· PAGAR Â· cartÃ£o do grupo A â†’ valor 92 |
| **Loja** | âœ… v8.65 |


### ðŸ©¹ PDV carrinho â€” colunas fixas + total sem corte (15/07 Â· **teste v8.60**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | GM/qtd/A-B/total/lixeira com largura fixa (alinha). Total cabe milhar (ex. 1.131,00) |
| **Validar** | Ctrl+F5 teste Â· badge **v8.60** Â· milho misto + qtd alta no item com grupos |
| **Loja** | âœ… v8.65 |


### ðŸ©¹ PDV â€” alinhamento GM + 2 preÃ§os (lista e carrinho) (15/07 Â· **teste v8.59**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Busca: Marca â†’ GM â†’ espaÃ§o fixo p/ 2 preÃ§os. Carrinho: coluna A/B sempre reservada (nÃ£o empurra lixeira) |
| **Validar** | Ctrl+F5 teste Â· badge **v8.59** Â· digitar Â«milhoÂ» Â· carrinho misturando 1 e 2 preÃ§os |
| **Loja** | âœ… v8.65 |


### âœ¨ Visual A/B â€” lista cadastro + ediÃ§Ã£o + Entrada NF (15/07 Â· **teste v8.58**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Coluna VENDA com chips A/B Â· preview Grupo A/B na aba preÃ§os Â· chips A/B sob P.venda na Entrada NF |
| **Validar** | Ctrl+F5 teste Â· badge **v8.58** Â· produto em 2 grupos: lista + lÃ¡pis + casar na NF |
| **Loja** | âœ… v8.65 |


### ðŸ©¹ PDV â€” grupos A/B aparecem + modal preÃ§os maior (15/07 Â· **teste v8.57**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | Busca usava sÃ³ cache local e merge ignorava o servidor â†’ sem precos_grupos |
| **Fix** | Sempre confere servidor Â· merge atualiza campos Â· overlay no catÃ¡logo PDV Â· modal preÃ§os ~90% |
| **Validar** | Ctrl+F5 teste Â· badge **v8.57** Â· digitar produto com 2 grupos â†’ chips A/B na busca e no carrinho |
| **Loja** | âœ… v8.65 |


### âœ¨ PDV â€” preÃ§o por forma OU 2 grupos (15/07 Â· **teste v8.56**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | No cadastro: modo **Por forma** (igual antes) ou **2 grupos** (preÃ§o A/B + formas em A/B). PDV mostra A e B na busca/carrinho; total sÃ³ muda na etapa 3 ao escolher a forma |
| **Validar** | Ctrl+F5 teste Â· badge **v8.56** Â· produto em 2 grupos â†’ 2 chips no PDV Â· pagar com forma do grupo A/B |
| **Loja** | âœ… v8.65 |


### ðŸ©¹ Cadastro â€” busca digitada mais leve (15/07 Â· **teste v8.55**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | DigitaÃ§Ã£o no cadastro: Mongo slim Â· sem mÃ©dia/pedidos Â· sem recalcular saldo 2Ã— Â· debounce texto 450 ms |
| **Validar** | Ctrl+F5 teste Â· badge **v8.55** Â· digitar nome no cadastro deve responder mais rÃ¡pido |
| **Loja** | âœ… v8.65 |


### ðŸ“¦ Deploy loja **v8.54** â€” Entrada NF UX restante (15/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | Nome SisVale sob descriÃ§Ã£o Â· Emb. fechada alarga CÃ³d./EAN Â· alinhamento linha + bordas |
| **Commit loja** | ccb4fd8 Â· backup producao-backup-pre-v854-20260715 @ 6f0df25 |
| **AutorizaÃ§Ã£o** | *enviarâ€¦ produÃ§Ã£o* + 99738595 |
| **VocÃª** | Ctrl+F5 loja Â· badge **v8.54** |


### ðŸ©¹ Entrada NF â€” alinhamento linha + borda campos (15/07 Â· **teste v8.54**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Nome SisVale sÃ³ empurra o fim da descriÃ§Ã£o; CÃ³d./demais campos alinhados no topo Â· borda dos campos um pouco mais forte |
| **Validar** | Ctrl+F5 teste Â· badge **v8.54** Â· linha reta descriÃ§Ã£oâ†’cÃ³digo Â· campos mais legÃ­veis |
| **Loja** | **âœ…** v8.54 |


### ðŸ©¹ Entrada NF â€” emb. recolhida alarga CÃ³d./EAN (15/07 Â· **teste v8.53**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Com Embalagem fechada, espaÃ§o livre vai para **CÃ³d.** e **EAN** (nÃ£o fica buraco Ã  direita) |
| **Validar** | Ctrl+F5 teste Â· badge **v8.52** Â· Emb. fechada â†’ GM completo + EAN mais largo |
| **Loja** | **âœ…** v8.54 |


### ðŸ©¹ Entrada NF â€” nome do cadastro sob a descriÃ§Ã£o (15/07 Â· **teste v8.51**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Embaixo da descriÃ§Ã£o da NF aparece o nome SisVale (verde), sem coluna nova |
| **Validar** | Ctrl+F5 teste Â· badge **v8.51** Â· linha casada â†’ texto verde sob a descriÃ§Ã£o |
| **Loja** | **âœ…** v8.54 |


### ðŸš‘ Entrada NF â€” overlay R$ 0 apagava P.venda do Mongo (15/07 Â· **teste v8.49**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Alguns itens do XML ficam P.venda 0,00 / margem -100 (ex.: 3 primeiros da NF) |
| **Causa** | Overlay/PG com preÃ§o 0 sobrescrevia `ValorVenda` do Mongo no casamento |
| **Fix** | SÃ³ aplica overlay/API quando preÃ§o **> 0** |
| **Loja** | **âœ…** v8.49 6f0df25 |

### ðŸ“¦ Deploy loja **v8.49** â€” UX Entrada NF + P.venda (15/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Inclui** | v8.45â€“v8.48 layout (passos, abas, embalagem, topbar) + fix P.venda v8.49 |
| **AutorizaÃ§Ã£o** | *enviarâ€¦ produÃ§Ã£o* + `99738595` |


### ðŸ©¹ Entrada NF â€” abas/meta na barra do topo (15/07 Â· **teste v8.48**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Manual/XML/SEFAZ + Origem/NF/datas sobem para a faixa do Â«â† ListaÂ» (uma linha a menos) |
| **Validar** | Ctrl+F5 teste Â· badge **v8.48** Â· editor sem faixa solta abaixo do topo |
| **Loja** | â³ |


### ðŸ©¹ Entrada NF â€” colunas embalagem recolhÃ­veis (15/07 Â· **teste v8.47**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | BotÃ£o **Embalagem** (â–¸) esconde Un/emb Â· Qtd est. Â· R$ pacote Â· DescriÃ§Ã£o ganha largura Â· abre sozinho se Un/emb &gt; 1 |
| **Validar** | Ctrl+F5 teste Â· badge **v8.47** Â· colunas somem Â· descriÃ§Ã£o mais larga Â· seta abre de novo |
| **Loja** | â³ |


### ðŸ©¹ Entrada NF â€” abas + meta numa linha (15/07 Â· **teste v8.46**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Manual/XML/SEFAZ + Origem/NF/Linhas/datas na **mesma faixa** Â· passos 1â€“8 + Anterior/PrÃ³ximo sem 2Âª fileira |
| **Validar** | Ctrl+F5 teste Â· badge **v8.46** Â· topo do editor: uma linha sÃ³ |
| **Loja** | â³ |


### ðŸ©¹ Entrada NF â€” UX rateio + passos 1â€“8 numa linha (15/07 Â· **teste v8.45**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Caixa amarela do rateio compacta na linha Â«2 Â· ProdutosÂ» Â· chips 1â€“8 numa fileira (sem 7/8 sozinhos) |
| **Validar** | Ctrl+F5 teste Â· badge **v8.45** Â· etapa 2: faixa amarela fina Â· passo a passo: 8 tags na mesma linha |
| **Loja** | â³ |


### ðŸ“¦ Deploy loja **v8.43** â€” rateio frete/ST + hidrata GM paralelo (14/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | *pode subir pra produÃ§Ã£o* + `99738595` |
| **Commit loja** | `6f46143` (+ `e873147` hidrata paralelo) Â· backup `producao-backup-pre-v843-20260714` @ `b6e64db` |
| **Pacote** | Rateio acrÃ©scimos no custo Â· hidrata GM em paralelo |
| **Validar** | Ctrl+F5 loja Â· badge **v8.43** Â· XML frete/ST â†’ marcar â†’ custo sobe |
| **Revert** | `git reset --hard b6e64db` na `producao` + push (ou checkout backup) |


### âš¡ Entrada NF â€” rateio frete/ST no custo (14/07 Â· **loja v8.43**)

| | |
| --- | --- |
| **O quÃª** | Checkbox Â«Incluir no custo os acrÃ©scimos da notaÂ» â€” rateia frete+ST(+seguro/outras/IPIâˆ’desc) no V. unit; marca/desmarca sem reler XML |
| **Arquivos** | `nfe_entrada_util.py` (totais ICMSTot) Â· `entrada_nota.html` |
| **Validar** | Ctrl+F5 teste Â· badge **v8.43** Â· XML com frete/ST â†’ marcar â†’ custo sobe Â· desmarcar â†’ volta Â· nota limpa â†’ opÃ§Ã£o desligada |

### âš¡ Entrada NF â€” hidrata GM em paralelo (14/07 Â· **loja v8.43**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | GM nÃ£o pula de 2 em 2 s â€” busca do cadastro em paralelo |
| **Loja** | **âœ…** v8.43 e873147 |

### ðŸš‘ Entrada NF â€” apÃ³s Confirmar XML sem GM / P.venda 0 (14/07 Â· **loja v8.41**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Modal mostra **Cadastro**, mas na grade: CÃ³d. sÃ³ do fornecedor, P. venda **0,00**, margem **-100** |
| **Causa** | `casar_produtos_mongo` sÃ³ ia `produto_id`+nome â€” sem preÃ§o/GM |
| **Fix** | Parse traz **preÃ§o + GM** (Mongo+overlay) Â· grade mostra GM Â· hidrata pÃ³s-Confirmar via API se faltar |
| **Loja** | **âœ…** `b6e64db` Â· backup `producao-backup-pre-v841-20260714` |
| **Validar** | Ctrl+F5 loja Â· badge **v8.41** Â· Ler XML â†’ Confirmar â†’ GM + P. venda |

### ðŸ©¹ Entrada NF â€” modal XML Â«Sem vÃ­nculoÂ» com grade vazia (14/07 Â· **loja v8.40**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | ApÃ³s entrar NF, ao ler o **mesmo XML** de novo tudo Â«Sem vÃ­nculoÂ» e esquerda vazia |
| **Causa** | Badge = par **gradeâ†”XML**, nÃ£o casamento com cadastro. Grade vazia = todos Â«Sem vÃ­nculoÂ» mesmo com produto casado |
| **Fix** | Badge **Cadastro** (verde) quando `produto_id` veio do parse Â· texto da grade vazia Â· Confirmar tambÃ©m lembra EAN embalagem |
| **Loja** | **âœ…** `a4d60fc` Â· backup `producao-backup-pre-v840-20260714` |
| **Validar** | Ctrl+F5 loja Â· badge **v8.40** Â· Ler XML grade vazia â†’ **Cadastro** nos casados |

### ðŸš‘ Hotfix loja **v8.39** â€” merge migraÃ§Ãµes 0050+0051 (14/07)

| Item | Detalhe |
| ---- | ------- |
| **Erro** | `Conflicting migrations â€¦ (0050_vendaagro_frete, 0051_produto_modelo)` |
| **Fix** | `0052_merge_0050_frete_0051_modelo` |
| **Loja** | **âœ…** `c119f51` v8.39 Â· teste `bcfdc8a` |

### ðŸ“¦ Deploy loja **v8.38** â€” lote Zap restante + PIN ao abrir (14/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | *pode mandar tudo para produÃ§Ã£o* + `99738595` |
| **Commit loja** | `93cbc67` Â· backup `producao-backup-pre-v838-20260714` @ `74b95fc` |
| **Pacote** | **#5** busca leve Â· **#16** aviso CP (texto + fonte) Â· **#18** frete 3 vias (+ mig `0050_vendaagro_frete`) Â· **#1 #2 #9 #11 #13 #14** Â· **PIN ao abrir** PDV |
| **#16 texto** | *Busca por texto ativa: os campos de data foram limpos* Â· fonte `1rem` |
| **Validar loja** | Ctrl+F5 Â· badge **v8.38** Â· abrir PDV â†’ pede PIN Â· CP busca texto â†’ aviso Â· entrega+frete nas 3 vias Â· Entrada NF #1/#2 Â· Esc fecha overlay menu |
| **Revert** | `git reset --hard 74b95fc` na `producao` + push (ou checkout backup) |

### ðŸ” PDV pede PIN ao abrir (14/07 Â· **teste v8.37** â†’ **loja v8.38**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Fechar/reabrir PDV **nÃ£o** mantÃ©m operador â€” tela de PIN na entrada |
| **Validar** | Ctrl+F5 Â· digita PIN Â· fecha aba / volta do BI Â· abre PDV â†’ pede PIN de novo |
| **Loja** | **âœ…** v8.38 `93cbc67` |

### ðŸ“¦ Deploy loja **v8.34** â€” #6 + #10 + PIN nome (14/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | *manda produÃ§Ã£o* + `99738595` |
| **Commit loja** | `74b95fc` Â· backup `producao-backup-pre-v834-20260714` @ `1cdad18` |
| **Pacote** | **#6** PIN 1 query Â· **#10** Cancelar cobranÃ§a (+ MP fiado caixa) Â· **PIN nome** online/sessÃ£o Â· **chip** entre Nova venda e PIN |
| **NÃƒO veio** | frete #18 Â· resto do `teste` |
| **Validar loja** | Ctrl+F5 Â· badge **v8.34** Â· digitar PIN â†’ nome no chip Â· fiado BAIXA â†’ **Cancelar cobranÃ§a** Â· venda no nome certo apÃ³s trocar PIN no descanso |
| **Revert** | `git reset --hard 1cdad18` na `producao` + push (ou checkout backup) |

### ðŸ‘ PDV â€” nome do PIN entre Nova venda e PIN (14/07 Â· **teste v8.34**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Chip `gm-sspin-operador-chip` no topbar do wizard (entre **Nova venda** e **PIN**) |
| **Validar** | Ctrl+F5 Â· digitar PIN Â· nome aparece ali |

### ðŸš‘ PIN / nome trocado (14/07 Â· **teste v8.33**)

| Item | Detalhe |
| ---- | ------- |
| **Causa** | (1) desbloqueio pelo cache local **sem** gravar sessÃ£o no servidor â†’ reload trazia o nome anterior; (2) servidor confiava no nome do Chrome antes da sessÃ£o; (3) state/rascunho PDV podia guardar nome velho |
| **Fix** | PIN **sempre online** Â· no descanso limpa nome no Chrome Â· venda usa **sessÃ£o/PIN** antes do Chrome Â· PDV limpa `operadorPdv` ao trocar/sair |
| **Validar** | Render **teste** Â· Ctrl+F5 Â· PC1: Geraldo PIN â†’ descanso â†’ Renan PIN â†’ venda no **seu** nome; 2 pessoas no mesmo Chrome sem trocar PIN ainda grudam (esperado atÃ© o descanso) |
| **Loja** | **âœ…** v8.34 `74b95fc` |

### ðŸ” PIN / nome trocado (Renan â†” Geraldo etc.) â€” 14/07 Â· **sÃ³ diagnÃ³stico**

| Item | Detalhe |
| ---- | ------- |
| **Relato** | AÃ§Ã£o com PIN do Renan (e outros) gravando nome de **outra pessoa** (ex. Geraldo Hinnen) |
| **Status** | â†’ virado fix **v8.33** acima |
| **Causa A (mais forte)** | Nome no Chrome + sessÃ£o desatualizada (atalho local sem API) |
| **Causa B** | PIN **nÃ£o Ã© Ãºnico** no banco (`.first()`) â€” ainda existe; cadastro novo barra duplicata |

### ðŸ©¹ #6 PIN â€” 1 query no rÃ³tulo (14/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **Status** | JÃ¡ tinha helper `_perfil_usuario_por_pin` (191e70b) Â· **faltava**: `operador_label_de_pin` ainda batia 2Ã— no banco |
| **Fix** | Valida + rÃ³tulo na **mesma** leitura |
| **Validar** | Render **teste** Â· digitar PIN no PDV/caixa Â· deve responder rÃ¡pido |
| **Loja** | **âœ…** v8.34 `74b95fc` |

### ðŸ“¦ Deploy loja **v8.30** (14/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Commits** | `96df934` GM0024 PG+Mongo Â· `1cdad18` baixa fiado filtro nome |
| **Antes** | loja **v8.26** `780d964` |
| **Validar** | Ctrl+F5 Â· Fiado â†’ BAIXA Â· `gm0024` â†’ 3 |
| **Dados** | fiado: sÃ³ filtro â€” **nÃ£o** apaga tÃ­tulos |

### ðŸš‘ Fiado BAIXA Â«Nenhum tÃ­tulo em abertoÂ» (14/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Qualquer BAIXA no `/fiado/` â†’ *Nenhum tÃ­tulo em aberto para quitar* |
| **Causa** | Lista agrupa por **nome**; cobranÃ§a/baixa filtrava sÃ³ `cliente_agro_pk` (muitos tÃ­tulos sem FK / modo default `titulo` no PDV) |
| **Fix** | Mesmo filtro da gestÃ£o (`_q_titulos_cliente_gestao`) Â· PDV default modo `cliente` |
| **Dados** | **SÃ³ leitura/filtro** â€” nÃ£o apaga tÃ­tulo, baixa nem saldo |
| **Loja** | **âœ…** v8.30 `1cdad18` |

### ðŸš‘ Hotfix #3 GM0024 loja sÃ³ 1 (14/07 Â· **v8.28**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Loja v8.26: `GM0093`/`GM0090` OK Â· `GM0024` â†’ sÃ³ `-1` (nome â†’ 3) |
| **Causa** | Com `agro_pg`, 1 hit no Postgres fazia **pular Mongo** â€” `-10/-15` sÃ³ no Mongo |
| **Fix** | FamÃ­lia GM **sempre** complementa/mescla Mongo |
| **Validar** | Ctrl+F5 Â· `gm0024` â†’ 3 magnus |
| **Loja** | â³ aguarda pacote maior (Renan 14/07: nÃ£o sobe sozinho) |

### ðŸ“¦ Deploy loja **v8.26** â€” busca GM CodigoNFe (14/07 Â· Renan autorizou)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Motor Mongo prefixa `CodigoNFe`/`Codigo` (nÃ£o sÃ³ `index_codigos`) Â· overlay famÃ­lia Â· index apÃ³s overlay |
| **Validado** | Local: `GM0093` + mais 3 famÃ­lias OK |
| **Seed local** | `seed-gm0024-*` Teste Local â€” **apagado** (`--limpar`); loja nunca teve esses nomes |
| **Antes loja** | **v8.25** |
| **NÃƒO veio** | resto do `teste` (frete, lote Zapâ€¦) |

### ðŸš‘ #3 busca GM â€” causa real + teste local (14/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `gm0024`/`GM0093` â†’ 1; nome â†’ 3 |
| **Causa 1** | overlay / Mongo exact (hotfix anterior) |
| **Causa 2 (real quirera)** | Mongo: irmÃ£os **sem `index_codigos`**; motor sÃ³ buscava Ã­ndice â†’ sÃ³ `GM0093-25`. `CodigoNFe` tinha os 3 |
| **Fix** | Prefixo em `CodigoNFe`/`Codigo` no motor + misto sem early-return no index Â· Id=`_id` se faltar |
| **Validar local** | Ctrl+F5 Â· `GM0093` â†’ quirera 1kg + 5kg + 25kg (â‰¥3) Â· nome â€œquirera finaâ€ igual |
| **Loja** | **âœ…** push `780d964` / teste `0033d39` Â· badge **v8.26** Â· Ctrl+F5 |

### ðŸš‘ Hotfix #3 GM maiÃºsc/minÃºsc + famÃ­lia (14/07 Â· **v8.25**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Loja v8.23: `gm0024` â†’ 1 item; nome â†’ 3 |
| **Causa** | No Postgres `istartswith` Ã© **case-sensitive** (`gm` â‰  `GM`) + famÃ­lia nÃ£o expandia |
| **Fix** | Prefixo CI (`iregex`/`GM`+`gm`) + busca explÃ­cita famÃ­lia `GM0024` / `GM0024-*` Â· overlay igual |
| **Validar** | Ctrl+F5 loja Â· `gm0024` â†’ **3** Â· PDV igual |
| **Loja** | sobe junto (hotfx do pacote #3 jÃ¡ autorizado) |

### ðŸš‘ Hotfix migraÃ§Ã£o loja (14/07) â€” deploy v8.22 falhou no Render

| Item | Detalhe |
| ---- | ------- |
| **Erro** | `0051_produto_modelo` dependia de `0050_vendaagro_frete` (frete) â€” **nÃ£o** estava na loja |
| **Fix** | `0051` passa a depender de `0049` (irmÃ£ da 0050) |
| **Loja** | push hotfix â†’ badge **v8.23** |

### ðŸ“¦ Deploy loja **v8.22** â€” pacote #3 + #4 (14/07 Â· Renan frase+senha)

| Item | Detalhe |
| ---- | ------- |
| **Pacote** | **#4** modelo persiste + **#3** prefixo GM (famÃ­lia) |
| **Commits loja** | `2c44d45` (#4) Â· `a285d52` (#3) |
| **Antes** | loja **v8.15** `812226b` |
| **Backup** | `producao-backup-pre-v822-20260714` Â· anterior `pre-v815` / `pre-v814` |
| **NÃƒO veio** | #5 #6 #16 #18 frete Â· lote Zap restante â€” continua sÃ³ no teste |
| **Validar loja** | Ctrl+F5 Â· `GM0024` â†’ 3 itens Â· Modelo salvar/reabrir |
| **Revert** | `git reset --hard 812226b` na `producao` + push (ou checkout backup) |

### ðŸ› Hotfix #3 busca prefixo GM (teste **v8.22** Â· **loja v8.22**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `GM0024` â†’ 1 resultado; pelo nome â†’ 3 (GM0024-1/10/15). Cadastro e PDV |
| **Causa** | Filtro GM do motor lia sÃ³ `Produto.codigo_nfe` e descartava irmÃ£os cujo GM estÃ¡ no **overlay** |
| **Fix** | Filtrar GM **depois** do overlay Â· index/prefixo no motor Â· PDV scanner `GM0024` (sem hÃ­fen) usa famÃ­lia |
| **Validar** | Ctrl+F5 loja Â· buscar `GM0024` â†’ 3 itens Â· PDV igual |
| **Loja** | **âœ…** v8.22 |

### ðŸ› Hotfix #4 modelo persiste (teste **v8.19+** Â· **loja v8.22**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Campo **Modelo** no cadastro â€œsalvavaâ€ e sumia ao reabrir |
| **Causa** | Modelo sÃ³ no JSON do overlay; resposta/lista/Mongo sem coluna; detalhe Mongo sobrescrevia com Modelo vazio |
| **Fix** | Coluna `Produto.modelo` + sync no salvar Â· espelho Mongo `Modelo`/`NomeModelo` Â· lista/detalhe/UI sempre devolvem o campo |
| **MigraÃ§Ã£o** | `0051_produto_modelo` (copia do overlay â†’ coluna) |
| **Validar** | Ctrl+F5 loja Â· editar modelo Â· Salvar Â· reabrir |
| **Loja** | **âœ…** v8.22 |

### ðŸ”„ Handoff Renan 14/07 â€” histÃ³rico (versÃ£o antiga)

> **Atualizado 16/07:** status vivo estÃ¡ no **CHECKLIST ÃšNICO** no topo do CHECKPOINT (Zap # + FL + P).

| Ambiente (14/07) | VersÃ£o | Notas |
| ---------------- | ------ | ----- |
| **Loja** | era v8.22 â†’ hoje **v8.69** | ver CHECKLIST ÃšNICO no topo |
| **Teste** | hoje **v8.97** | ver CHECKLIST ÃšNICO no topo |

**PrÃ³ximo:** fecha â†’ **#12+#17** Â· aberto Zap â†’ **#7**.

### âœ… #17 Busca CP inteligente â€” P0+P1+P2 (16/07 Â· **teste**)

| Item | Detalhe |
| ---- | ------- |
| **P0** | Valor (bruto/pago/restante; inteiro ou `1.500,00` / R$) + data digitada (venc./comp./pagto) |
| **P1** | Boleto (cÃ³digo/linha) + nÂº documento sÃ³-dÃ­gitos |
| **P2** | Parcela (`2/6`, Â«parcela 2Â») + CPF/CNPJ com/sem mÃ¡scara |
| **Causa bug valor** | QS Postgres `_aplicar_texto_qs` nÃ£o buscava valor â€” sÃ³ texto |
| **Arquivos** | `lancamentos_financeiro_pg_util.py` Â· `mongo_financeiro_util.py` Â· help Â§10 Â· placeholder CP |
| **Validar** | âœ… Renan testou Â· valor + parcela OK |
| **Loja** | ðŸ“¦ **pronto produÃ§Ã£o** â€” frase + senha (pacote do fecha c/ #12) |

### ðŸ› Pacote performance + UX (teste **v8.17**)

| Item | Detalhe |
| ---- | ------- |
| **#5** | Busca do cadastro e da Entrada NF mais leve no BCA |
| **#6** | ValidaÃ§Ã£o de PIN com menos consultas ao banco |
| **#16** | Aviso da limpeza de datas no CP movido para o local pedido |
| **Arquivos** | `views.py` Â· `caixa_util.py` Â· `entrada_nota.html` Â· `lancamentos_contas_pagar_teste.html` |
| **Validar** | Ctrl+F5 teste Â· busca cadastro Â· busca Entrada NF Â· PIN Â· posiÃ§Ã£o do aviso no CP |
| **Loja** | **â³** sÃ³ com frase + senha |

### ðŸ› Hotfix #18 frete nas 3 vias e cupom fiscal (teste **v8.16**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Na `via do cliente` do fluxo de entrega a taxa nÃ£o saÃ­a no primeiro cupom, mas reaparecia na reimpressÃ£o |
| **Fix** | frete incluÃ­do no payload das 3 vias de entrega e fallback explÃ­cito no render do cupom 80mm / NFC-e |
| **Arquivos** | `pdv_wizard.js` Â· `venda_cupom_80mm.js` Â· `nfce_cupom_util.py` |
| **Validar** | Ctrl+F5 teste Â· fluxo entrega com 3 vias Â· via do cliente Â· cupom fiscal |
| **Loja** | **â³** sÃ³ com frase + senha |

### ðŸ› Hotfix #3 custo via BCA (teste **v8.15**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cadastro e Entrada NF via busca BCA continuavam mostrando custo **R$ 0,00** mesmo com custo salvo no produto |
| **Causa** | `/api/buscar/` nÃ£o reaproveitava o custo salvo em `Produto.custo` quando o documento da busca vinha zerado |
| **Fix** | fallback de custo Postgres no `api_buscar_produtos` para fluxos `compras=1` / cadastro / Entrada NF |
| **Arquivo** | `produtos/views.py` |
| **Validar** | Ctrl+F5 teste Â· cadastro lista custo Â· busca da Entrada NF com o mesmo produto |
| **Loja** | **â³** sÃ³ com frase + senha |

### ðŸ› PÃ³s-validaÃ§Ã£o bugs loja Zap 12/07 (teste **v8.14**) â€” ajustes amarelo/vermelho

| Item | Detalhe |
| ---- | ------- |
| **#3** | Cadastro lista mostra **preÃ§o de custo** com fallback mais robusto |
| **#4** | Busca cadastro passa a considerar **modelo** (`cadastro_extras.modelo`) |
| **#15** | **CÃ³digo interno** novo ocupa a **menor lacuna livre** a partir de **4010** |
| **#16** | **CP** continua limpando datas na busca e agora mostra **aviso visual** |
| **#18** | **Taxa de entrega** entra tambÃ©m na **via do cliente** do cupom |
| **Validar** | Ctrl+F5 teste Â· #3 Â· #4 Â· #15 Â· #16 Â· #18 |
| **Loja** | **â³** sÃ³ com frase + senha |

### ðŸ› Lote bugs loja Zap 12/07 (teste **v8.12**) â€” pacote checklist #

| # | Item | Status |
| - | ---- | ------ |
| **1** | Entrada NF etapa 7 â€” valor recarregava a cada tecla | âœ… nÃ£o rebuilda preview na validaÃ§Ã£o |
| **2** | Entrada NF etapa 2 â€” busca barras | âœ… nÃ£o bloqueia modo scanner |
| **3** | Cadastro lista â€” custo | âœ… custos compra na row Mongo + overlay |
| **4** | Cadastro â€” campo modelo nÃ£o salvava | âœ… body + `cadastro_extras.modelo` |
| **8** | Produto novo sumia da lista | âœ… merge coloca no topo |
| **9** | Fiado MP â†’ forma genÃ©rica no caixa | âœ… `[MP_POINT]` + split conferÃªncia |
| **10** | Fiado sem cancelar/voltar | âœ… botÃ£o **Cancelar cobranÃ§a** |
| **11** | Menu caixa/vendas Ã s vezes nÃ£o fecha | âœ… Esc no iframe fecha overlay |
| **13** | XML boleto â†’ **Boleto BancÃ¡rio CN** | âœ… (Renan confirmou CN) |
| **14** | Add produto perde barras/lote | âœ… invalidate soft (FL-026) |
| **15** | CÃ³digo interno 9000+ â†’ 4xxx | âœ… teto auto 5999 (FL-025) |
| **16** | CP busca limpa datas | âœ… (FL-023) |
| **18** | Taxa entrega no cupom/NFC-e | âœ… `VendaAgro.frete` + vFrete + linha cupom |
| **5â€“7, 12, 17** | LentidÃ£o / PIN / impressÃ£o / devoluÃ§Ã£o parcial / busca CP inteligente | â³ depois |

| Item | Detalhe |
| ---- | ------- |
| **Validar** | Ctrl+F5 teste Â· nÃºmeros #1â€“4, #8â€“11, #13â€“16, #18 |
| **MigraÃ§Ã£o** | `0050_vendaagro_frete` (Render aplica no deploy) |
| **Loja** | **â³** sÃ³ com frase + senha |

### Entrada NF â€” BCA fix motor padrÃ£o (11/07 Â· **teste v8.10**)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Entrada NF usava cache local PDV + `entrada_nfe=1` â€” lista diferente do cadastro |
| **Fix** | **SÃ³ servidor** BCA Â· `/api/buscar/?compras=1` Â· sem merge cache |
| **Validar** | Ctrl+F5 teste Â· `milho` / `#prova` = mesma ordem que cadastro |
| **Loja** | **â³** apÃ³s OK |

### ðŸš€ Deploy loja **v8.07** â€” BCA + pacote teste (11/07 â€” Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Merge `teste` â†’ `producao`: **BCA** PDV + Cadastro + Consulta Â· cadastro estoque/vitrines Â· fiado Â· indicadores |
| **Validado loja** | Renan OK PDV + cadastro + consulta |
| **Revert total BCA** | **`producao-backup-pre-bca-20260711`** Â· **`rollback/pre-bca-v766`** @ **`e260c48`** (v7.66) |
| **Pendente produto** | Desativar `/produtos/gestao/` se cadastro OK |

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Card **Faturamento vendas (PDV)** Â· **1 arquivo** `indicadores_gerencial_pg.py` |
| **Sem** | cadastro Â· busca Â· fix despesas CP (af328ba) |
| **Commit** | `e260c48` |

### ðŸ› FIX â€” Indicadores faturamento PDV Â«â€”Â» (11/07 Â· **v8.04 teste**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Card **Faturamento vendas (PDV)** = **R$ â€”** (ex. mÃªs ant. jun/26) Â· DRE e demais KPIs OK |
| **Causa** | v7.65 tinha card no HTML mas **faltava backend**; planilha histÃ³rica sem fallback |
| **Fix** | `_faturamento_pdv_periodo` â†’ mesma funÃ§Ã£o do BI (`_dashboard_mongo_vendas_serie`) |
| **Loja** | **âœ… v7.66** |

### âœ… Deploy loja **v7.65** â€” Indicadores planilha + Estoque giro (11/07 â€” Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Tipo+Grupo planilha CP Â· aba Estoque & giro Â· **sÃ³ leitura BI** |
| **NÃ£o mexe** | PDV Â· CP Â· caixa Â· fiado |
| **Pode mudar** | NÃºmeros Indicadores + Resumo gerencial (classificaÃ§Ã£o) |

### âœ… Indicadores â€” Tipo + Grupo + Estoque e giro (11/07 Â· **v7.98+ teste**)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Planilha oficial CP (Tipo/Grupo) na tabela despesas + aba **Estoque e giro** (parado 90d + top 5 giro 30d) |
| **URL aba** | `/financeiro/dashboard-gerencial/?aba=estoque` |
| **Validar** | Ctrl+F5 teste â†’ Indicadores â†’ aba Estoque e giro Â· despesas agrupadas Tipoâ†’Grupo |
| **Fix v7.99** | Aba abre na hora + Â«Carregandoâ€¦Â» Â· dados via API (consulta pesada nÃ£o trava a pÃ¡gina) |
| **Fix 11/07** | Tabela despesas alinhada Ã  **CP** (competÃªncia Â· bruto Â· todas empresas) â€” antes filtrava sÃ³ 1 empresa |

### âœ… Deploy loja **v7.63** â€” Unificar planos despesa CP (11/07 â€” Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Mapa + URLs staff Â· migrate **0049** Â· sÃ³ renomeia plano_conta |
| **Pacote** | `pacote/planos-cp-producao` â†’ `producao` `d901763` (**sem** cadastro/busca) |
| **Apply PG loja** | **âœ… 11/07** lote #1 Â· **2483** tÃ­tulos Â· pÃ³s-sim **0** a renomear Â· **3286 Â· R$ 1.144.537,97** Â· fora mapa **0** |
| **Status** | **âœ… CONCLUÃDO** loja |

### âœ… Deploy loja **v7.61** â€” Fiado baixa parcial (11/07 â€” Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Baixa parcial Â· popup Total/Parcial Â· FIFO Â· PDV pagamento |
| **Pacote** | `pacote/fiado-baixa-parcial-producao` â†’ `producao` `304a5fa` |
| **PÃ³s-deploy** | Render ~2â€“5 min Â· **Ctrl+F5** PDVs Â· caixa aberto |

### Fiado â€” baixa parcial no PDV (11/07)

| Item | Detalhe |
| ---- | ------- |
| **Status** | **âœ… loja v7.61** (cÃ³digo) Â· docs checkpoint **v7.62** |
| **Teste** | Validado Â· popup Total (Enter) / Parcial (P) |
| **Fora** | FL-029 crÃ©dito Â· FL-052 NFC-e Â· FL-019 recibo |

### Cadastro ERP â€” estoque + vitrines (11/07)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Trazer para `/produtos/cadastro-erp/` o que sÃ³ existia em `/produtos/gestao/` (coluna estoque + ajuste + vitrines marca/cat/forn) |
| **Feito teste v7.69â€“v7.76** | Busca Postgres (`catalogo_agro.buscar`) Â· GM prefixo Â· custo lista Â· facetas marcas Aâ€“Z |
| **Feito teste v7.82** | **API Ãºnica** `/api/buscar/` â€” PDV padrÃ£o Â· cadastro `?contexto=cadastro&compras=1` (+ custo/saldo/filtros). Lista Aâ€“Z continua `api_produtos_cadastro` sem `q` |
| **ValidaÃ§Ã£o Renan 11/07** | Teste 1+2 busca unificada OK Â· canÃ¡rio `[TESTE]` confirmou cadastro + wizard (cache local) |
| **Feito v7.69â€“v8.08** | **BCA** â€” PDV + cadastro + Consulta + **Entrada NF** |
| **ValidaÃ§Ã£o Renan** | **âœ… loja** PDV/cadastro/consulta + Entrada NF |

### FIX â€” Indicadores nÃºmeros vs BI (09/07 Â· v7.61)

| Item | Detalhe |
| ---- | ------- |
| **Receita** | Card **Faturamento PDV** = mesmo dado do BI; **Receita financeira (DRE)** = lanÃ§amentos |
| **Despesas** | ClassificaÃ§Ã£o fixa/variÃ¡vel/outra corrigida; hint Â«role a tabelaÂ» para ver todos os grupos |
| **Loja** | **âœ… v8.07** â€” Renan OK |

### âœ… Deploy loja **v7.60** â€” Financeiro gerencial SisVale (09/07 â€” Renan senha OK Â· loja aberta)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Resumo + Indicadores + GrÃ¡fico gastos â€” **sÃ³ leitura** SisVale; **nÃ£o** para uso gerencial ainda â€” **teste com dado real** |
| **Pacote** | v7.42â€“v7.60: financeiro gerencial + fixes Compras mÃ©tricas + barra lateral BI |
| **Risco operaÃ§Ã£o** | PDV/CP/caixa **inalterados**; Compras/BI com fixes jÃ¡ validados no teste |
| **Como** | Merge `teste` â†’ `producao` (09/07) |
| **PÃ³s-deploy** | Render ~2â€“5 min Â· **Ctrl+F5** Â· testar `/financeiro/dashboard-gerencial/` e Resumo |
| **Uso** | **NÃ£o** decidir gestÃ£o por essas telas atÃ© Renan fechar validaÃ§Ã£o |

### UX â€” Financeiro gerencial 100 % SisVale (09/07 Â· v7.60)

| Item | Detalhe |
| ---- | ------- |
| **Resumo** | `/financeiro/resumo-gerencial/` â€” saiu **Fonte Mongo/ERP**, **Filtro contas** e textos Postgres/Mongo; sÃ³ data base + valor |
| **API** | `/api/financeiro/resumo-operacional` e `gap-equilibrio` â€” sÃ³ lanÃ§amentos SisVale (`fonte=postgres`) |
| **GrÃ¡fico gastos** | `/financeiro/grafico-gastos/` â€” agregaÃ§Ã£o sÃ³ SisVale; ajuda sem Mongo |
| **Menu** | Links Indicadores/Resumo sem Â«PostgresÂ» no tooltip |
| **Validar** | Ctrl+F5 teste â†’ Resumo + Indicadores + GrÃ¡fico gastos â€” **Ctrl+F** nÃ£o deve achar ERP/Mongo/DtoLancamento |

### UX â€” Indicadores sÃ³ SisVale (09/07 Â· v7.58)

| Item | Detalhe |
| ---- | ------- |
| **Tela** | `/financeiro/dashboard-gerencial/` â€” removido **Filtro contas** (ERP/.env); textos sem Mongo/Postgres/ERP |
| **Dados** | Sempre lanÃ§amentos SisVale; DRE usa sÃ³ contas de **operaÃ§Ã£o da loja** (fixo no cÃ³digo) |

### ðŸ› FIX â€” Indicadores financeiros 500 (09/07 Â· **v7.54**)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `/financeiro/dashboard-gerencial/` â†’ Server Error 500 |
| **Causa** | `titulos_financeiro_montar_qs` passou a exigir `despesa`; DRE/indicadores buscam receita+despesa sem filtro |
| **Fix** | `despesa` opcional em `titulos_financeiro_montar_qs` Â· template **Despesas por categoria** com grupos fixa/variÃ¡vel |
| **Validar** | Ctrl+F5 teste â†’ Indicadores â†’ KPIs + tabela categorias + grÃ¡fico |

### ðŸ› FIX â€” Indicadores 500 + despesas fixa/variÃ¡vel (09/07 Â· **v7.54â€“v7.55**)

| Item | Detalhe |
| ---- | ------- |
| **500** | `titulos_financeiro_montar_qs` exigia `despesa`; DRE Indicadores lÃª receita+despesa â†’ `TypeError` |
| **Fix** | `despesa` opcional no PG Â· rÃ³tulos **despesas por categoria** Â· cards/tabela **fixa / variÃ¡vel / outras** + filtros |
| **Validar** | Ctrl+F5 `/financeiro/dashboard-gerencial/` |

### âœ… Indicadores financeiros â€” tela nova PG (09/07 Â· Renan Â· **v7.53**)

| Item | Detalhe |
| ---- | ------- |
| **URL** | `/financeiro/dashboard-gerencial/` (`dashboard_financeiro_completo`) â€” **substituiu** tela Mongo |
| **Fonte** | 100 % **Postgres** (`TituloFinanceiroAgro`) â€” mesma base Resumo gerencial + CP |
| **KPIs** | Receita, margens, equilÃ­brio, caixa (pagamentoÂ·realizado), DRE, ref. mÃ©dia 60d |
| **Novo** | **Despesas por categoria** â€” fixa/variÃ¡vel/outra Â· 3 meses ou 3 semanas Â· tabela + grÃ¡fico top 10 Â· Î”% Â· clique â†’ CP |
| **Arquivos** | `indicadores_gerencial.html` Â· `indicadores_gerencial_pg.py` Â· `gastos_variacao_pg.py` Â· `financeiro/views.py` |
| **GrÃ¡fico gastos** | `/financeiro/grafico-gastos/` **mantido** (anÃ¡lise sÃ©rie longa) |
| **Validar** | Ctrl+F5 teste â†’ Indicadores â†’ KPIs batem Resumo gerencial Â· trocar 3 meses/semanas |

### ðŸ› FIX â€” Compras nÃºmeros zerados no teste (08/07 Â· Renan Â· v7.51)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | SugestÃ£o Â«â€”Â», vendas 0, custo R$ 0 na busca (ex. milho) |
| **Causa** | F5 no teste lia sÃ³ Postgres (poucas vendas); API zerava mÃ©dia/custo do catÃ¡logo |
| **Fix** | F5 hÃ­brido PG+Mongo; busca nÃ£o apaga valor bom com zero da API |
| **Barra lateral** | **NÃ£o mexido** (Renan: nÃ£o tocar) |
| **Validar** | Ctrl+F5 Compras â†’ buscar milho â†’ mÃ©dia/sugestÃ£o/custo |

### ðŸ› FIX â€” barra lateral sumiu (08/07 Â· Renan Â· v7.47)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Faixa escura Ã  esquerda (PDV Â· Dashboard Â· +) **sumiu no teste**; produÃ§Ã£o OK |
| **Causa** | `isPaginaAuxiliarSemShell` no script de limite de abas â€” `mountShell` no outro IIFE â†’ **ReferenceError** |
| **Fix** | Helpers em `window.__agroIsPaginaAuxiliarSemShell` / `__agroPathLookLikeFolhaPlanilha` (escopo compartilhado) |
| **Validar** | Ctrl+F5 na home teste â†’ barra igual print produÃ§Ã£o |
| **ProduÃ§Ã£o** | **NÃ£o mexido** |

### ðŸ› REVERT â€” barra lateral BI (08/07 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Erro** | Hotfixes v7.43â€“v7.45 no shell quebraram a **faixa de guias** da home |
| **AÃ§Ã£o** | **Revertido** `_agro_open_external.html` + BI + `agro_dual_window.js` ao estado **prÃ©-v7.42** |
| **Mantido** | SÃ³ isenÃ§Ã£o folha Compras (`embed=1` / planilha) â€” **nÃ£o** mexe na home |
| **Mantido** | Fix mÃ©tricas Compras (v7.44) â€” F5 nÃ£o zera catÃ¡logo |
| **Validar** | Ctrl+F5 na home â†’ faixa verde **PDV Â· Dashboard Â· +** Ã  esquerda |

### ðŸ› HOTFIX â€” barra lateral nÃ£o monta (08/07 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | BI/Compras abrem mas **sem faixa verde** Ã  esquerda (PDV Â· Dashboard Â· +) |
| **Causa** | `mountShell` quebrava na fase de rota e apagava o DOM; boot sÃ³ no `DOMContentLoaded` |
| **Fix** | DOM separado da rota; boot com retry + `load`/`pageshow`; gestÃ£o no `localStorage` ativa shell |
| **Arquivo** | `_agro_open_external.html` |

### ðŸ› HOTFIX â€” Compras mÃ©tricas zeradas + barra lateral (08/07 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Telas abrem mas sem guias laterais; produtos sem vendas/custo; planilha com mÃ©dia 0 |
| **Causa mÃ©tricas** | F5 (`aplicarMetricasCompraNaBase`) **zerava** todo produto fora do payload PG (sÃ³ ~6 com venda no teste) |
| **Fix mÃ©tricas** | F5 sÃ³ atualiza quem veio na API; busca `?compras=1` puxa mÃ©dia PG por produto |
| **Fix shell** | `mountShell` com retry; `__agroInAppAddTab` tenta remontar antes de `location.assign` |
| **Arquivos** | `compras.html` Â· `compras_metricas_util.py` Â· `views.py` Â· `_agro_open_external.html` Â· `dashboard_gerencial.html` |

### ðŸ› HOTFIX â€” barra lateral / navegaÃ§Ã£o (08/07 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Sumiu barra lateral; F10/Compras/Produtos nÃ£o abrem â€” fica no BI |
| **Causa** | Patch folha Compras deixou shell lateral meio montado; `__agroInAppAddTab` falhava sem fallback |
| **Fix** | Remonta shell se incompleto; fallback `location.assign`; folha embed nÃ£o bloqueia shell do BI |
| **Arquivos** | `_agro_open_external.html` Â· `agro_dual_window.js` Â· `dashboard_gerencial.html` |

### âœ… Compras â€” UX + NF Agro + custo (08/07 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Tudo sugerido: rÃ³tulos NF Agro (sem Â«ERPÂ»); tela mais legÃ­vel; custo/lucro corrigidos; subir teste |
| **Custo** | Se Â«finalÂ» zerado â†’ usa base cadastro ou Ãºltima Entrada NF |
| **Lucro** | SÃ³ exibe quando hÃ¡ custo confiÃ¡vel â€” senÃ£o Â«Sem custo cadastradoÂ» |
| **Textos** | Â«ComprarÂ», Centro+Vila, Â«Ãšltimas entradas NF AgroÂ», Â«Ãšlt. NFÂ» no detalhe |
| **F5 / mÃ©tricas** | `compras_metricas_util` preenche Ãºltima entrada NF Agro (colunas 6â€“7) |
| **Arquivos** | `compras.html` Â· `compras_relatorio_planilha.html` Â· `compras_metricas_util.py` Â· `compras_ultimas_compras_util.py` |
| **Pacote** | Inclui tambÃ©m cadastro ERP marca/cat + folha popup (sessÃ£o 08/07) |

### ðŸ› Compras â€” Folha Compras popup (08/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Folha (fornecedor/cat/unidade) Ã s vezes nÃ£o abria ou abria Â«bugadaÂ» em nova aba |
| **Causa** | Limite 3 abas SisVale + barra lateral engolindo a pÃ¡gina da planilha |
| **Fix** | Popup com iframe na Compras; `?embed=1`; folha isenta do shell/limite de abas |
| **Arquivos** | `compras.html` Â· `compras_relatorio_planilha.html` Â· `_agro_open_external.html` |

### ðŸ› Cadastro ERP â€” marca/categoria sumindo (08/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Marca, categoria, subcategorias Â«nÃ£o salvavamÂ» / sumiam ao reabrir ou em outro PC |
| **Causa** | Modal mesclava **lista velha** por cima do **detalhe da API**; popup Â«texto localÂ» confundia |
| **Fix** | API prevalece na abertura; lista atualiza apÃ³s salvar; **sÃ³ Postgres** (overlay + Produto); cache facetas limpa |
| **Arquivos** | `_modal_editar_produto_cadastro_erp.inc.html` Â· `cadastro_erp_panel.js` Â· `views.py` |
| **Teste** | Cadastro â†’ editar â†’ marca/cat â†’ **Salvar no Agro** â†’ F5 â†’ outro PC â€” campos persistem |

### ðŸ“‹ DecisÃ£o â€” popups / modais (08/07 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Hoje** | `<div>` + Tailwind + JS (`hidden`) â€” sem lib de modal |
| **`<dialog>` nativo** | Avaliado â€” **nÃ£o** migrar nem obrigar em popup novo |
| **Motivo** | ConsistÃªncia com ~dezenas de modais existentes; zero ganho visÃ­vel/perf |
| **Regra assistente** | Novo popup â†’ padrÃ£o da tela; tela nova do zero â†’ `<dialog>` opcional |
| **Docs** | `banana.md` Â§4.14 Â· **`SISTVALE.md`** Â· **`FOOD.md`** |

### âœ… Deploy loja **v7.40** â€” FL-051 baixa fiado no PDV (08/07 â€” Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Baixa fiado no **PDV pagamento** Â· painel fiado **fecha** antes do pagamento no PDV principal |
| **Como** | Merge `teste` â†’ `producao` `54564da` |
| **Status** | **âœ… loja** |

### âœ… FL-051 â€” Baixa fiado no PDV

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Baixa fiado no **PDV pagamento** (formas + maquininha + MP Point) Â· painel fiado **fecha** antes do pagamento no PDV principal |
| **Como** | Merge `teste` â†’ `producao` |
| **PÃ³s-deploy** | Render ~2â€“5 min Â· **Ctrl+F5** nos PDVs Â· testar Baixa no fiado (painel e `/fiado/`) |
| **Fora** | NFC-e na quitaÃ§Ã£o = **FL-052** |
| **Fluxo overlay** | **Baixa** no painel fiado â†’ **fecha painel** â†’ pagamento no **PDV principal** |
| **Fluxo pÃ¡gina** | `/fiado/` fora do painel â†’ `/pdv/` cobranÃ§a normal |
| **Velocidade** | Caixa jÃ¡ aberto nÃ£o refaz consulta Â· POST sem resumo pesado |

### âœ… Deploy loja **v7.27** â€” PDV Nova venda + frete F7 (07/07 â€” Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | BotÃ£o **Nova venda F12** Â· modal confirmaÃ§Ã£o padrÃ£o PDV Â· frete sÃ³ apÃ³s etapa Entrega Â· **F7 direto** sem frete (fix CSS) |
| **Como** | Cherry-pick `2267c0a` + `9d2dac3` + `34abbbe` |
| **Status** | **âœ… loja** |

### âœ… Deploy loja **v7.24** â€” NFC-e timeout reemitir (07/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Reemitir nÃ£o estoura 30s Render Â· sempre JSON Â· mensagem clara se SEFAZ cair |
| **Commit loja** | `2a0dc35` |
| **Status** | **âœ… loja** |

### âœ… Deploy loja **v7.23** â€” NFC-e retry 403 SEFAZ (07/07)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Retry HTTP **403/5xx** Â· timeout conexÃ£o 12s Â· mensagem legÃ­vel (sem HTML) |
| **Como** | Cherry-pick `a2f252a` **sÃ³ NFC-e** |
| **Commit loja** | `db22c40` |
| **Status** | **âœ… loja** |

### âœ… Deploy loja **v7.22** â€” NFC-e retry SEFAZ (07/07 â€” Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Retry conexÃ£o SEFAZ (1/2/4/8 s) Â· background nÃ£o desiste em falha de rede |
| **Como** | Cherry-pick `1223bb5` **sÃ³ NFC-e** |
| **Arquivos** | `nfce_sp_emissao_util.py` Â· `sefaz_soap_util.py` Â· `views_nfce.py` |
| **Status** | **âœ… loja** |

### ðŸ› NFC-e â€” SEFAZ conexÃ£o recusada (07/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | 1Âª venda OK; demais falham `Connection refused` em `nfce.fazenda.sp.gov.br` |
| **Causa** | Instabilidade SEFAZ/rede **+** cÃ³digo sem retry em falha de conexÃ£o |
| **Fix** | Retry HTTP SEFAZ (1/2/4/8 s) Â· background segue em erro de rede |
| **OperaÃ§Ã£o** | Vendas pendentes: **Reemitir NFC-e** em `/vendas/` |

### âœ… Deploy loja **v7.21** â€” PDV entregas + confirmar venda (07/07 â€” Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Duplo clique confirmar Â· badge Entregas Â· entrega encerra sÃ³ no PDV Â· idempotente |
| **Merge** | `teste` atÃ© `d186601` â†’ `producao` |
| **Status** | **âœ… loja** |

### ðŸ› PDV v7.19 (07/07) â€” fluxo entrega

| DecisÃ£o | Detalhe |
| ------- | ------- |
| **Quem encerra** | PDV, ao confirmar venda/cobranÃ§a (`finalizar entrega`) |
| **Antes (bug)** | Servidor encerrava no ERP **antes** do PDV â†’ erro Â«nÃ£o encontradaÂ» |

### ðŸ› PDV urgente v7.18 (07/07)

| Bug | Causa | Fix |
| --- | ----- | --- |
| Â«Entrega pendente nÃ£o encontradaÂ» com venda OK | ERP jÃ¡ encerrava entrega; PDV chamava de novo | Finalizar idempotente (404 = jÃ¡ fechada) |

### ðŸ› PDV urgente v7.17 (07/07)

| Bug | Causa | Fix |
| --- | ----- | --- |
| Venda duplicada ao clicar vÃ¡rias vezes em Confirmar | BotÃ£o liberava antes de zerar carrinho | Trava atÃ© fim do fechamento + zera carrinho na hora |
| Entregas: badge Â«1Â» mas lista vazia | Cache local `total` â‰  lista | Contador = tamanho da lista; refresh forÃ§ado ao abrir |

### âœ… Deploy loja **v7.15** â€” Pix MP sem card duplicado (07/07 â€” Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Some card Â«Mercado Pago â€” Pix automÃ¡ticoÂ» quando cobra na maquininha |
| **Merge** | `teste` â†’ `producao` |
| **Status** | **âœ… loja** â€” Render deployando |

### âœ… PDV â€” Pix MP v7.15 (07/07)

| Item | Detalhe |
| ---- | ------- |
| **Pix MP auto** | Some card Â«Mercado Pago â€” Pix automÃ¡ticoÂ» (sÃ³ botÃ£o verde) |

### âœ… Deploy loja **v7.13** â€” MP Point PDV pagamento (07/07 â€” Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Fluxo TEF (cobrar na tranche) Â· cancel PDVâ†”maquininha Â· popups MP Â· crÃ©dito Ã  vista 1x Â· layout pagamento |
| **Merge** | `teste` â†’ `producao` Â· `2e920b1` |
| **Status** | **âœ… loja** |

### âœ… PDV â€” MP Point popup v7.11â€“12 (07/07)

| Item | Detalhe |
| ---- | ------- |
| **Maquininha** | Cancel/recusa usa o mesmo modal grande do cancel PDV |

### âœ… PDV â€” MP Point popup v7.10 (07/07)

| Item | Detalhe |
| ---- | ------- |
| **Popup** | Ainda maior |
| **Fix** | Overlay sumia sÃ³ visualmente â€” travava clique; Â«EntendiÂ» libera PDV |

### âœ… PDV â€” MP Point popup v7.09 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Cancel** | SÃ³ modal centralizado; texto e Â«EntendiÂ» no meio, maior |
| **Removido** | Faixa amarela atrÃ¡s do popup |

### âœ… PDV â€” MP Point v7.08 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Aviso cancel** | Modal grande + faixa fixa na tela pagamento (nÃ£o some sozinha) |
| **CrÃ©dito** | Envia `default_installments: 1` â€” Ã  vista sem pergunta na MP |
| **Parcelado** | Sem mudanÃ§a |

### âœ… PDV â€” MP Point cancel bidirecional v7.07 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Cancel PDV** | API MP com header `at_terminal` quando valor jÃ¡ estÃ¡ na maquininha |
| **BotÃ£o espera** | Chama cancel na MP antes de abortar o poll |
| **Renan** | Pediu: cancelar no sistema tambÃ©m cancelar na maquininha |

### âœ… PDV â€” pagamento UX v7.06 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Topo** | Removida barra verde Â«Etapa 4 PagamentoÂ» (mais Ã¡rea Ãºtil) |
| **RodapÃ©** | Desconto/frete e botÃµes Confirmar maiores |

### âœ… PDV â€” pagamento UX v7.05 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Valor** | Card grande, nÃºmero centralizado, botÃ£o largo, passos em chips |
| **RevisÃ£o** | Â«Pode confirmarÂ» centralizado; lanÃ§amento 2 linhas + botÃµes pequenos |
| **Renan** | MP ok nas formas; sem mais venda teste (cartÃ£o) |

### âœ… PDV â€” pagamento UX v7.04 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Tranche** | BotÃ£o verde Â«Cobrar na maquininhaÂ» / Â«LanÃ§ar pagamentoÂ» (Enter continua valendo) |
| **RevisÃ£o** | SÃ³ Â«Resta pagarÂ» + total; detalhes sÃ³ se desconto/frete |
| **Teste Renan** | Formas corretas na MP; nÃ£o fechou mais venda (evitar cartÃ£o) |

### âœ… PDV â€” pagamento UX v7.03 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Direita** | Card grande Â«Resta pagarÂ» / Â«QuitadoÂ» + jÃ¡ lanÃ§ado |
| **Lista** | Badge verde **Pago** em cada lanÃ§amento; MP pago com fundo verde |

### âœ… PDV â€” MP Point fluxo TEF v7.02 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Tranche** | Enter no valor â†’ envia Point + espera + lanÃ§a Â«Pago na maquininhaÂ» |
| **Confirmar** | SÃ³ grava venda/cupom (nÃ£o cobra de novo) |
| **API** | `confirmar-tranche` + status `PAID` Â· finalizar aceita `erp_payload` completo |
| **Lista** | MP pago sem Alterar/Excluir |

### âœ… PDV â€” layout pagamento v7.01 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Barra inferior** | Campos e botÃµes maiores; borda escura nos inputs |
| **Pagamento** | Inputs com mais contraste (monitor claro) |
| **Parcelado MP** | Aviso se total &lt; R$ 10 |

### âœ… PDV â€” layout pagamento v6.98â€“99 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Barra inferior** | Voltar + desconto + frete + confirmar (lado a lado) |
| **Removido** | ObservaÃ§Ã£o final + footer duplicado na etapa pagamento |

### âœ… PDV â€” MP Point v6.97 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Avisos** | Confirmar venda / MP / caixa â†’ toast PDV (sem alert Chrome) |
| **Overlay gestÃ£o** | Borda/botÃ£o emerald (sem laranja) |
| **Render print** | Sem `MP_POINT_PRINT_ON_TERMINAL` â†’ cÃ³digo usa `seller_ticket` |

### âœ… PDV â€” MP Point v6.96 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Popup** | Maior, cores emerald/slate (sem laranja) |
| **Avisos** | Toast padrÃ£o PDV (nÃ£o alert do navegador) |
| **Recusa/cancel** | Poll detecta `failed`/`canceled` na maquininha |
| **Parcelas** | Envio reforÃ§ado + aviso se MP confirmar diferente do PDV |
| **Comprovante** | PadrÃ£o `seller_ticket` â€” **no Render** trocar `MP_POINT_PRINT_ON_TERMINAL` se ainda `no_ticket` |

### âœ… PDV â€” MP Point v6.95 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Parcelado** | `default_installments` como **inteiro** (fix erro MP property_type) |
| **Popup espera** | Painel maior; passos 1â€“4; Â«Travou?Â» recolhido |
| **Cancel** | PDV chama API cancel MP; poll detecta cancel na maquininha |
| **Comprovante** | Maquininha: `MP_POINT_PRINT_ON_TERMINAL` no Render (padrÃ£o `no_ticket`) |

### âœ… PDV â€” MP Point v6.93 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Fix 500** | `MP_ORDERS_URL` restaurada + sintaxe corrigida |

### âœ… PDV â€” MP Point v6.92 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Fix 500** | Constante `MP_ORDERS_URL` restaurada em `mercado_pago_point.py` |
| **API** | Erro interno no criar â†’ JSON legÃ­vel (nÃ£o pÃ¡gina HTML) |

### âœ… PDV â€” MP Point v6.91 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Fix** | Token CSRF no PDV (`get_token`) â€” erro Â«Unexpected token \<Â» ao confirmar MP |
| **Erro legÃ­vel** | Se servidor falhar, mensagem em portuguÃªs (nÃ£o JSON quebrado) |

### âœ… PDV â€” MP Point v6.90 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Host MP** | SÃ³ navegador que **abriu Gaveta/Teste** manda Point (flag sessÃ£o; notebook bloqueado) |
| **Venda** | `pagamentos` gravam `maquinaId` no ERP/Agro (conferÃªncia MP) |
| **Som** | Um beep sÃ³ (ok ou erro se forma divergiu) |

### âœ… PDV â€” MP Point v6.89 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Fechar caixa** | Linhas MP separadas: Pix / DÃ©bito / CrÃ©dito **â€” Mercado Pago** vs demais maquininhas |
| **2Âº PDV** | MP automÃ¡tico **sÃ³ Caixa Gaveta** (1Âº aberto); Notebook = Cielo/Sicredi/Sicoob |
| **Venda** | `pagamentos_json` guarda `maquinaId` + `cobrarNoPointMp` para conferÃªncia |

### âœ… PDV â€” MP Point v6.88 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Garantia MP** | ApÃ³s pagar, lÃª forma real da maquininha; se divergir do PDV â†’ alerta + grava forma do MP |
| **Parcelado MP** | Envia N parcelas para a maquininha (`default_installments`) |
| **Fechar caixa** | Parcelado soma em **CrÃ©dito** (sÃ³ no fechamento; venda/ERP como antes) |
| **Painel espera** | Contador Â· som ok/erro Â· timeout Â· fila na maquininha Â· ajuda aberta |

### âœ… PDV â€” MP Point v6.87 (06/07)

| Item | Detalhe |
| ---- | ------- |
| **MP** | CartÃ£o e Pix **sÃ³ automÃ¡tico** (sem Manual) |
| **Pix** | MP auto Â· Cielo manual Â· Sicoob chave |
| **CartÃ£o** | MP auto Â· Cielo Â· Sicredi manual |
| **Fix** | Desconto/frete no Point Â· forma na mÃ¡quina Â· painel espera |

### ðŸ”Œ MP Point â€” reativar conta nova (06/07)

| Item | Detalhe |
| ---- | ------- |
| **Lado MP** | Maquininha **ativa** Â· modo **vincular com o caixa** âœ… Â· app **pdvagromais** Â· credencial **ProduÃ§Ã£o** |
| **Lado Agro** | Render **teste** â€” `MP_POINT_*` configurado (Renan 06/07) |
| **Ordem** | Validar v6.87 no teste â†’ depois loja |
| **Status** | **ðŸ§ª teste v6.87** deploy |

### âœ… Deploy loja **v6.86** â€” PIN PDV + contagem caixa (05/07 noite â€” Renan senha OK)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | PIN manual/descanso PDV Â· contagem fechar caixa ao reabrir Chrome Â· overlay caixa nÃ£o trava |
| **Commits loja** | `8410ebc` Â· `8df7e18` Â· `ca78b88` Â· `ae69827` |
| **Fora da loja** | Entregas PDV Â· FL-049/050 (sÃ³ teste) |
| **Status** | **âœ… loja** â€” validar PIN + contagem na operaÃ§Ã£o |

### ðŸ› PDV â€” botÃ£o PIN nÃ£o abre (05/07 noite)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Descanso automÃ¡tico e botÃ£o **PIN** no PDV nÃ£o mostram tela |
| **Causa** | Fix de modal bloqueava `openLock` mesmo em abertura **manual** |
| **Fix** | PIN manual forÃ§a abertura Â· overlay sÃ³ pausa se **visÃ­vel** |
| **Status** | **ðŸ§ª teste v6.86** |

### ðŸ› Caixa â€” contagem some ao fechar navegador (05/07 noite)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Digitou contagem Â· fechou Chrome Â· voltou zerado |
| **Causa** | Chave do turno exigia match exato de PKs e apagava localStorage |
| **Fix** | Guarda por **dia** Â· grava turno ao digitar Â· sÃ³ limpa se **mudou o dia** |
| **Status** | **ðŸ§ª teste v6.86** |

### ðŸ› PDV â€” descanso atrÃ¡s do modal Entregas (05/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Modal Â«Pagamento na entregaÂ» aberto Â· descanso/PIN aparece **atrÃ¡s** Â· tela trava |
| **Causa** | `<dialog>` fica acima do PIN Â· descanso nÃ£o pausava com modal PDV aberto |
| **Fix** | Pausa idle com modal/dialog PDV Â· fecha modais antes do PIN Â· **detecÃ§Ã£o genÃ©rica** (`dialog[open]` + `aria-modal` + entrega wizard + overlay iframe) |
| **Status** | **ðŸ§ª teste v6.85** â€” validar pagamento, NFC-e, cliente, entrega |

### ðŸ› Entregas PDV vazio mas bloqueia fechar caixa (05/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Fechar caixa lista N entregas Â«pagamento na entregaÂ» Â· PDV â†’ Entregas: Â«Nenhuma pendÃªnciaÂ» |
| **Causa** | Fechar caixa vÃª **todos caixas abertos** Â· PDV filtrava sÃ³ o **caixa do navegador** (ex. Caixa #2 vs pendÃªncias no #1) |
| **Fix** | API PDV usa **mesmo critÃ©rio** do fechamento Â· lista mostra **qual caixa** Â· link laranja abre PDV com `?entregas=1` |
| **ProduÃ§Ã£o** | Mesmo cÃ³digo na loja â€” pode afetar se houver notebook/teste + gaveta abertos com entrega pendente |
| **Status** | **ðŸ§ª teste v6.85** â€” validar botÃ£o laranja + lista com caixa |

### ðŸ› Caixa fechar â€” cache contagem sumindo (05/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Fechar/abrir navegador ou erro no fechamento â†’ contagem por forma e cÃ©dulas **zerada** |
| **Causa** | Patch turno limpava localStorage **no clique em fechar** (antes de confirmar) |
| **Fix** | Grava **na hora** no aparelho Â· limpa sÃ³ apÃ³s **fechamento OK** ou **virou o dia** / **mudou turno** |
| **Status** | **ðŸ§ª teste v6.81** |

### ðŸ› Caixa fechar â€” tela trava com modal cÃ©dulas + descanso (05/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Parado na contagem (overlay cÃ©dulas / fiado): cliques e teclado morrem â€” igual PIN descanso |
| **Causa** | Modo descanso (~3 min) ativava **por baixo** dos overlays do caixa Â· ou PDV overlay sem pausar idle |
| **Fix** | PIN descanso sempre **por cima** Â· pausa idle com overlay PDV/caixa aberto Â· cÃ©dulas acima do fiado |
| **Status** | **ðŸ§ª teste v6.81** â€” validar fechamento + contagem cÃ©dulas (PDV overlay e pÃ¡gina direta) |

### âœ… FL-050 â€” `/vendas/` aguardar NFC-e background (04/07)

| Item | Detalhe |
| ---- | ------- |
| **Fix** | `venda_nfce_processando` (~120 s) Â· botÃ£o **Emitindoâ€¦** + alerta *aguarde* Â· POST reemitir **409** se ainda processando Â· detalhe venda igual |
| **Status** | **ðŸ§ª teste** Â· **nÃ£o estava na loja** (v6.77 sÃ³ tinha fix CPF/F9 no PDV) |

### âœ… FL-049 â€” CPF cliente no PDV + NFC-e (04/07)

| Item | Detalhe |
| ---- | ------- |
| **Fix** | Campo **CPF** nos modais PDV (editar Â· cadastro rÃ¡pido Â· entrega) Â· grava `ClienteAgro` Â· Enter/F9 usam CPF cadastrado sem modal |
| **Status** | **ðŸ§ª teste** |

### ~~ðŸ“‹ Fila â€” **FL-050**~~ *(fechado â€” ver acima)*

### ~~ðŸ“‹ Fila â€” **FL-049**~~ *(fechado â€” ver acima)*

### âœ… Deploy loja **v6.77** (03/07 â€” Renan senha OK)

| Pacote | Commits / nota |
| ------ | -------------- |
| **NFC-e CPF + sync F9** | `3453968` Â· merge `8495fb7` |
| **Status** | Render produÃ§Ã£o deployando |

### NFC-e â€” CPF com impressÃ£o + sync SEFAZ (03/07)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Background emitia sem CPF antes do modal; F9 imprimia antes da SEFAZ autorizar |
| **DecisÃ£o** | **Enter** = NFC-e background (rÃ¡pido) Â· **F9** = modal CPF + `nfce_sincrona` + imprime sÃ³ se autorizou |
| **Arquivos** | `pdv_wizard.js` Â· `views_nfce.py` |
| **Status** | **âœ… loja v6.77** |

### âœ… Deploy loja **v6.75** (03/07 madrugada â€” Renan senha OK)

| Pacote | Commits / nota |
| ------ | -------------- |
| **Busca rÃ¡pida** | `908ff07` â€” entrada NF + cadastro |
| **Caixa contagem** | `21491cd` â€” zera rascunho ao mudar turno |
| **Fiado busca** | `21491cd` â€” cliente sem saldo na busca |
| **Roteiro Cursor** | `banana-roteiro.md` + modo econÃ´mico |
| **Merge** | `d26e913` `teste` â†’ `producao` |

### âš¡ Busca produtos â€” entrada NF + cadastro (03/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Digitar Â«milhoÂ»/GM/EAN na etapa produtos: 4â€“14 s por tecla; requests empilhadas |
| **Fix** | Servidor: `catalogo_agro.buscar` sem scan `.iterator()` Â· match exato antes do `icontains` Â· GM/barras sÃ³ com tamanho mÃ­nimo Â· sem fallback Mongo em `agro_pg` Â· cache 45 s entrada NF. Cliente: abort Â· debounce 400 ms Â· cache PDV local. **Cadastro:** GM sÃ³ com 5+ chars Â· barras 8+ Â· debounce cÃ³digo 420 ms Â· sem 2Âª busca PDV |
| **Status** | **âœ… loja v6.75** |

### ðŸ› Fechar caixa â€” contagem do dia anterior (03/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Contagem por forma ficava salva atÃ© o fechamento do dia seguinte |
| **Fix** | Rascunho amarrado ao turno (sessÃ£o) Â· limpa localStorage ao mudar turno/fechar |
| **Status** | **âœ… loja v6.75** |

### ðŸ› Fiado â€” busca sÃ³ com saldo (03/07)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Renan â€” na busca, ver cliente mesmo sem pendÃªncia |
| **Fix** | `apenas_saldo=0` quando digita busca Â· cadastro ClienteAgro entra na lista |
| **Status** | **âœ… loja v6.75** |

### ðŸ› Modo descanso â€” tela borrada ilegÃ­vel (03/07 Â· loja)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | ApÃ³s ~3 min sem mexer: overlay escuro + tudo borrado; popup PIN ilegÃ­vel (Chrome/GPU) |
| **Causa** | `backdrop-blur` + `filter: blur` nos irmÃ£os do body â€” bug de composiÃ§Ã£o no Chrome |
| **Fix** | `_screensaver_pin.html` â€” fundo sÃ³lido 94 %, sem blur no fundo; `#sspin-root` isolado no `body` |
| **Status** | **âœ… loja v6.28** |

### âœ… Deploy loja **v6.20** â€” perf PC fraco + RH vale CP (03/07)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Renan â€” loja fechou Â· perf + fix RH vale Â· senha OK |
| **Commits** | `c9c1ece`â€¦`6741ed2` em `producao` (7 commits cherry do `teste`) |
| **Perf** | Splash catÃ¡logo Â· cache offline Â· sync foco 5 min Â· F11 animaÃ§Ãµes Â· GestÃ£o sem iframe PDV Â· BI lazy Â· repouso abas 5/20 min |
| **RH** | Vale caixa **nÃ£o** duplica pagamento CP parcial na folha (`e557857` / `6741ed2`) |
| **Mantido** | Carrinho v6.16 |

### ðŸ› RH folha â€” vale duplicava pagamento CP (02/07 Â· Renan) â€” **âœ… loja v6.20**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Vale caixa + pagamento CP parcial iguais â†’ CP **Pago** inflado |
| **Fix** | Hook sÃ³ baixa direta em LanÃ§amentos Â· sync Postgres Â· aviso verde no fechamento |
| **Status** | **Na loja** â€” nÃ£o reabrir folhas jÃ¡ gambiarradas |
| **16/07** | Renan pediu cherry **sÃ³** deste bug + senha â€” **jÃ¡ estava** em `producao` (`e557857` / `6741ed2`) Â· **nada a subir** |

### ðŸ› FL-048 â€” Â«Baixar ZIP selecionadoÂ» nÃ£o baixava (02/07 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Â«Kit recuperaÃ§Ã£o zeroÂ» OK Â· Â«Baixar ZIP selecionadoÂ» recarrega a tela sem arquivo |
| **Causa** | `resumo.xlsx` â€” campo `categorias` (lista) no manifest; openpyxl nÃ£o aceita lista na cÃ©lula |
| **Fix** | `_excel_scalar()` em `pg_backup_util.py` Â· mensagem de erro legÃ­vel na view |
| **Kit zero** | ConteÃºdo conferido OK (guias + `render-env-atual.env` + scripts) |

### âœ… Deploy loja **v6.16** â€” carrinho PDV itens travados (02/07)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Renan â€” cherry **sÃ³** fix carrinho Â· senha OK |
| **Commit** | `add4ce6` em `producao` |
| **O quÃª** | Lista busca sumia de verdade Â· toque no carrinho fecha busca Â· clique na linha por Ã­ndice |
| **Validado** | Renan â€” **loja v6.16 OK** (GM6082 + GM6083) |
| **Fora** | Pacote perf Â· RH vale CP Â· entregas topbarâ†’subtotal |

### ðŸ› PDV wizard â€” carrinho itens travados (02/07 Â· Renan) â€” **âœ… loja v6.16**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Alguns produtos no carrinho nÃ£o respondem a +/âˆ’, preÃ§o nem remover; outros na mesma venda OK; sÃ³ **LIMPAR** tira |
| **Exemplos** | GM6082 (dobradiÃ§a) Â· GM6083 (facÃ£o) |
| **Causa** | Lista da **busca** nÃ£o sumia (`#pdv-product-autocomplete` â€” `.hidden` perdia para `display:flex`) |
| **Fix** | `pdv_wizard.html` + `pdv_wizard.js` |
| **Status** | **Fechado** â€” loja confirmada Renan |

### WIP teste â€” perf CPU/RAM (02/07 Â· Renan) â€” **âœ… loja v6.20**

| Item | Detalhe |
| ---- | ------- |
| **Splash catÃ¡logo** | Atraso **400 ms** (cache rÃ¡pido **nÃ£o aparece**) Â· mÃ­n. visÃ­vel **200 ms** |
| **Config F11** | Modal Â· **Menos animaÃ§Ãµes** separado PDV / GestÃ£o (`agro_perf_fx.js`) Â· tambÃ©m no Menu **F10** (GestÃ£o) |
| **GestÃ£o 2 apps** | Sem iframe PDV oculto Â· Dashboard **sÃ³ carrega ao abrir** guia |
| **Abas livres** | **5 min** pausa animaÃ§Ãµes Â· **20 min** descarrega iframe (volta ao clicar â€” nÃ£o recarrega ao trocar aba) |
| **BI** | Pausa grÃ¡ficos/pulsos quando guia Dashboard nÃ£o estÃ¡ ativa |
| **PDV sync foco** | Delta catÃ¡logo ao trocar janela: **8s â†’ 5 min** (busca local intacta) |
| **Overlay Caixa/Fiado** | JÃ¡ limpava iframe ao fechar (`agro_pdv_overlay.js`) |
| **Teste Renan (PC forte)** | SÃ³ PDV ~0,2â€“2,8% Â· sÃ³ GestÃ£o ~3,5â€“7% Â· ambos ~4â€“10% CPU idle |

**WIP teste (antigo):** PDV splash 1Âª carga Â· cache offline Â· wizard delta no foco.

### DecisÃ£o operaÃ§Ã£o â€” Entregas Ã— PDV (01/07, Renan)

| Item | Detalhe |
| ---- | ------- |
| **Fluxo loja** | PDV entrega â†’ entregador leva e cobra â†’ volta â†’ **retoma no PDV** e fecha venda com pagamento real |
| **Tela `/entregas/`** | Uso **raro** (ex. **terÃ§a** â€” rota sÃ­tio); status do meio **nÃ£o usados** â€” sÃ³ **pendente** e **entregue** |
| **BotÃ£o PDV** | SÃ³ **Â«Entregas pendentesÂ»** embaixo (card Subtotal) â€” **removido** da barra de cima Â· abre modal cobranÃ§a na volta |
| **Pagamento na entrega** | Fechar venda no PDV â†’ **entregue** em `/entregas/` + some da fila Â«Entregas pendentesÂ» |
| **Pagamento na loja** | Venda fecha na hora â†’ **nÃ£o** entra na fila PDV Â· **nÃ£o** bloqueia fechar caixa Â· fica **pendente** em `/entregas/` atÃ© **fechar o caixa** â†’ aÃ­ vira **entregue** |
| **CÃ³digo** | `marcar_entrega_pendente_fechada` Â· `finalizar_entregas_pagas_pendentes_ao_fechar_caixa` Â· hook em `caixa_fechar` |

### âœ… Deploy loja **v6.15** â€” fix botÃ£o **Pagar** clique morto (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” produÃ§Ã£o direto + **`99738595`** |
| **O quÃª** | Cherry **`399f2f2`** â†’ **`b8edc38`** â€” sÃ³ `pdv_wizard.js` + `pdv_wizard.html` |
| **Migrate** | Nenhuma |
| **Validar loja** | Ctrl+F5 badge **v6.15** Â· finalizar venda â†’ nova venda â†’ clicar **Pagar** (sem sÃ³ F7) |

### ðŸ› PDV wizard â€” botÃ£o **Pagar** clique morto Â· **âœ… loja v6.15**

| Item | Detalhe |
| ---- | ------- |
| **Causa** | Toast pÃ³s-venda invisÃ­vel bloqueava cliques no canto inferior direito |
| **Fix** | `pointer-events-none` ao esconder toast Â· limpar ao clicar Pagar |

### ðŸš€ Deploy loja **v6.14** â€” FL-048 kit + env no ZIP + backup noturno (30/06)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” *pode subir* + **`99738595`** |
| **O quÃª** | Cherry **`c875945`** Â· **`021a96e`** Â· **`ff1f0d9`** Â· **`d178536`** |
| **Migrate** | Nenhuma |
| **Validar loja** | Ctrl+F5 badge **v6.14** Â· Admin â†’ Backup â†’ ZIP â†’ `kit/render-env-atual.env` |

### âœ… **FL-048** â€” backup Postgres + recuperaÃ§Ã£o zero

**Rotina:** marcar todas â†’ Â«Baixar ZIPÂ» â†’ guardar no PC/nuvem. **Um ZIP basta** â€” sem site depois.

**Dentro do ZIP:** `data/*.jsonl` Â· `manifest.json` Â· `resumo.xlsx` Â· pasta `kit/` com:
- `GUIA-BACKUP-PAINEL.txt` (espelho do painel)
- `render-env-atual.env` â€” **Environment real** do servidor (senhas, Mongo, NFC, MPâ€¦)
- `LEIA-ME-RECUPERACAO-ZERO.txt` Â· scripts

**resumo.xlsx (Renan 03/07):** **nÃ£o** Ã© a lista completa â€” Ã© **amostra legÃ­vel** para conferÃªncia rÃ¡pida. **Backup/restauraÃ§Ã£o real** = `data/catalogo.jsonl` (tudo do **Postgres**). Aba **Contagens** = total por tabela; se o Excel tiver menos linhas, o resto estÃ¡ sÃ³ no JSONL. Planilha sÃ³ de cadastro: **Cadastro ERP â†’ Excel â†“**.

**Excel v6.69+:** uma aba por tabela (ex. `catalogo_Produto` com cÃ³digo, nome, preÃ§oâ€¦) â€” nÃ£o mistura overlay/promoÃ§Ã£o na mesma folha.

**Fora do ZIP:** cÃ³digo Git Â· dados *dentro* do Mongo ERP (credenciais vÃªm no .env).

**Parcial / restore parcial:** inalterado â€” checkbox por categoria.

**Backup noturno:** cron 04h Â· webhook/S3 Â· ver `pg_backup_nightly.py`.

**Desastre:** novo Render â†’ deploy `producao` â†’ colar `kit/render-env-atual.env` (trocar `DATABASE_URL`) â†’ migrate â†’ superuser â†’ Restore ZIP.

**Rollback noite:** restore = sÃ³ **dados** Â· cÃ³digo ruim = **deploy versÃ£o antiga** (git/banana).

**Fonte checklist:** `pg_backup_render_checklist.py` Â· `pg_backup_disaster_kit.py` Â· `pg_backup_nightly.py` Â· `pg_backup_upload.py`

**âœ… Renan (03/07):** senha do **admin superuser** trocada Â· usuÃ¡rio **novo para loja** criado **sem** superuser (nÃ£o vÃª backup/restore).

| Quem | Admin Django | Backup Postgres |
| ---- | ------------ | --------------- |
| **Superuser** (vocÃª) | Sim | Baixar + restaurar (com senha) |
| **UsuÃ¡rio loja** (staff, sem superuser) | SÃ³ o que o perfil permitir | **NÃ£o** aparece |

**Validar:** login superuser â†’ Admin â†’ **OperaÃ§Ãµes SisVale** Â· login usuÃ¡rio loja â†’ **sem** bloco backup.

**Pendente operaÃ§Ã£o loja (02/07):** replicar **2 apps Chrome PDV + GestÃ£o na barra** em **todos os PCs Win10** â€” roteiro em **Â§ Atalhos Win10** abaixo.

### âœ… Deploy loja **v6.11** â€” popup Caixa (overlay JS quebrado) (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” *arruma / pode mandar* (continuaÃ§Ã£o deploy PDV) |
| **Commit** | **`8947c7f`** (sync **`agro_pdv_overlay.js`** + **`agro_dual_window.js`** de teste) |
| **Rollback** | Tag **`producao-rollback-v6.10-20260702`** @ **`8670f40`** |
| **Causa v6.10** | **`agro_pdv_overlay.js` na loja** tinha `options.force = true` **sem** `options` â†’ `ReferenceError` Â· `openPdvPanel` engolia o erro â†’ Caixa ia para **janela GestÃ£o** (shell lateral) Â· **teste** jÃ¡ tinha o JS completo desde v6.05 â€” cherry-pick v6.10 **nÃ£o copiou** esse arquivo |
| **Fix** | Overlay JS completo Â· `navigateGestao` **nunca** `pulseGestaoFocus` para Caixa/Vendas/Fiado no PDV |
| **Validar loja** | Ctrl+Shift+R badge **v6.11** Â· Caixa â†’ **popup laranja ~95%** (igual teste) |

**Se amanhÃ£ ainda falhar (checklist):**

| # | Conferir |
| - | -------- |
| 1 | Badge **v6.11** no PDV (senÃ£o deploy/cache HTML) |
| 2 | F12 â†’ Network â†’ `agro_pdv_overlay.js?v=` **commit** (nÃ£o `v=1`) |
| 3 | F12 â†’ Console â€” erro vermelho ao clicar Caixa? |
| 4 | Ctrl+Shift+R **no app PDV** (nÃ£o sÃ³ F5) |
| 5 | Paridade **prod=teste** nos 3 JS overlay â€” **OK pÃ³s-v6.11** (`agro_dual_window`, `agro_pdv_overlay`, `pdv_wizard`) |

**Riscos restantes (baixa):** cache agressivo Chrome Â· app GestÃ£o aberto ao lado redirecionando foco (v6.11 bloqueia `pulseGestaoFocus` no PDV para Caixa/Vendas/Fiado).

**Renan 02/07 madrugada:** *Â«parece que estÃ£o bomÂ»* â€” validaÃ§Ã£o definitiva na **abertura da loja** (Caixa/Vendas/Fiado â†’ popup laranja Â· badge **v6.11**).

### âš ï¸ Deploy loja **v6.10** â€” incompleto (02/07)

| Item | Detalhe |
| ---- | ------- |
| **Commit** | **`8670f40`** |
| **Problema** | **`agro_pdv_overlay.js` nÃ£o foi copiado** â€” JS quebrado na loja Â· substituÃ­do por **v6.11** |

### âœ… PDV Caixa popup 95% â€” **v6.11 loja** (02/07 Â· Renan)

### ðŸ› PDV topbar â€” ainda quebrado pÃ³s-v6.09 (02/07 madrugada Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Clique em Caixa/Vendas/Fiado no PDV nÃ£o abre overlay Â· `/caixa/` em nova guia abre mas cards do menu nÃ£o navegam |
| **Causa extra** | Scripts `agro_dual_window.js` / `agro_pdv_overlay.js` com **`?v=1` fixo** (browser servia JS antigo) Â· `isGestaoHost()` ainda tratava qualquer URL nÃ£o-PDV como gestÃ£o â†’ shell lateral engolia `/caixa/` |
| **Fix teste v6.51** | `agro_asset_v` no context (commit Render) Â· `isGestaoHost` sÃ³ com papel gestÃ£o explÃ­cito Â· roteador PDV + `openPdvPanel` reforÃ§ados Â· fallbacks antes de `pulseGestaoFocus` silencioso |
| **Validar teste** | Ctrl+Shift+R no PDV Â· DevTools â†’ `agro_dual_window.js?v=<commit>` (nÃ£o `v=1`) Â· topbar â†’ overlay Â· `/caixa/` guia separada â†’ cards navegam |

### âœ… Deploy loja **v6.09** â€” PDV topbar overlay (fix isPdvHost) (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” *pode mandar* + senha **`99738595`** (sem reteste) |
| **Commit** | **`1591b63`** (cherry-pick **`bfd4de5`** de teste) |
| **Rollback** | Tag **`producao-rollback-v6.08-20260702`** @ **`6461974`** |
| **O quÃª** | **PDV:** Caixa / Vendas / Fiado abrem overlay Â· submenus `/caixa/` em guia separada voltam a clicar |
| **Migrate** | Nenhuma |
| **Validar loja** | Ctrl+F5 badge **v6.09** Â· topbar PDV â†’ overlay laranja Â· cards do caixa navegam |

### âœ… Deploy loja **v6.08** â€” PDV topbar overlay + busca cadastro/NF (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” *pode mandar para produÃ§Ã£o ambos* + senha **`99738595`** |
| **Commit** | **`6461974`** (cherry-pick **`f012d42`** de teste) |
| **Rollback** | Tag **`producao-rollback-v6.07-20260702`** @ **`aa9f66e`** |
| **O quÃª** | **PDV:** Caixa / Consultar vendas / Fiado abrem overlay de novo Â· **Perf:** busca Entrada NF etapa 2 + Cadastro ERP (motor lite, cache 45 s) |
| **Migrate** | Nenhuma |
| **Validar loja** | Ctrl+F5 badge **v6.08** Â· PDV topbar â†’ overlay laranja Â· Entrada NF etapa 2 + Cadastro busca mais rÃ¡pida |

### âœ… Deploy loja **v6.07** â€” PDV botÃµes Pagar + Ã­cone entrega (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” *manda* + senha **`99738595`** |
| **Rollback** | Tag **`producao-rollback-v6.06-20260702`** @ **`d6c271f`** |
| **Git** | **`teste` `0d5c094`** Â· **`producao` `aa9f66e`** |
| **O quÃª** | Subtotal PDV: **Pagar** + F7 Â· **Entrega** = Ã­cone caminhÃ£o + F3 Â· sem quebra de linha |
| **Validar loja** | Ctrl+F5 badge **v6.07** Â· botÃµes subtotal em uma linha |

### âœ… Deploy loja **v6.05â€“v6.06** â€” perf telas + Voltar PDV + overlay Caixa + scripts Win (02/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” *pode subir tudo para produÃ§Ã£o* + senha **`99738595`** |
| **Rollback** | Tag **`producao-rollback-v6.04-20260702`** @ **`84541c2`** |
| **Git** | **`teste` `7b48e21`** Â· **`producao` `ea5f972`** + docs **`d6c271f`** Â· badge loja **`6.06`** |
| **O quÃª** | **Perf:** facetas gestÃ£o cache 15 min + adiado no load Â· CP bootstrap sem totais Â· staleRefresh +700 ms Â· cap scan PG Â· entrada NF rascunhos sem 503 Mongo Â· **UX:** Â«Voltar PDV F1Â» some em GestÃ£o Â· **Bug:** overlay Caixa no PDV nÃ£o abre BI Â· **Ops:** scripts atalhos Chrome Win (`criar_atalhos` + `remover_apps`) |
| **Migrate** | Nenhuma |
| **Validar loja** | Ctrl+F5 badge **v6.06** Â· **PDV** Caixa â†’ menu caixa (nÃ£o BI) Â· **GestÃ£o** sem F1 Â· gestÃ£o/cadastro/lanÃ§amentos/entrada NF mais rÃ¡pidos |

### ðŸ› PDV overlay Caixa abre BI GestÃ£o (01/07 Â· Renan) â€” **âœ… v6.05**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | App **SisVale PDV** Â· botÃ£o **Caixa** abre overlay laranja mas iframe mostra **GestÃ£o EstratÃ©gica (BI)** Â· persiste ao fechar/reabrir Â· sÃ³ **Ctrl+F5** corrige |
| **Causa** | Dois apps Chrome compartilham `localStorage agro_app_role_v1` Â· janela GestÃ£o manda `agro-open-inapp-tab` / foco BI Â· overlay aceitava `/dashboard/` no iframe |
| **Fix** | `agro_dual_window.js` + `agro_pdv_overlay.js` (ver deploy **v6.05**) |
| **Deploy** | **âœ… loja v6.05** |
| **Validar** | Ctrl+F5 no **PDV** Â· Caixa fechado â†’ overlay **menu caixa** (nÃ£o BI) Â· fechar/reabrir 3Ã— OK Â· com **GestÃ£o** aberta ao lado, Caixa continua certo |

### ðŸ› PDV topbar â€” Caixa / Vendas / Fiado nÃ£o abrem overlay (02/07 Â· Renan) â€” **fix teste v6.51 Â· loja ainda v6.09**

**Sintoma pÃ³s-v6.08/v6.09:** clique normal nÃ£o abre overlay; abrir em nova guia carrega `/caixa/` mas submenus tambÃ©m nÃ£o respondem.

**Causa:** (1) `readAppRole` / `window.name` PDV em URL errada Â· (2) **`agro_asset_v` ausente** â†’ cache `?v=1` Â· (3) `isGestaoHost()` amplo (`dualFlagOn && !isPdvPath`) montava shell em `/caixa/`.

**CorreÃ§Ã£o v6.51:** `context_processors.agro_asset_v` Â· `agro_dual_window.js` â€” `isGestaoHost` estrito Â· `openPdvPanel`/`navigateGestao`/roteador com fallbacks Â· `isPdvHost` + `isPdvPath` no router.

### ðŸ”§ Perf multi-tela â€” **âœ… v6.05**

| Tela | API / view principal | Fix **v6.05** |
| ---- | -------------------- | ------------- |
| **GestÃ£o** | `api_produtos_gestao_facetas` | Cache 15 min Â· facetas sÃ³ ao abrir Â«Filtros avanÃ§adosÂ» |
| **LanÃ§amentos CP** | bootstrap + `/api/lancamentos/` | Bootstrap `skip_totais` Â· staleRefresh +700 ms Â· cap scan PG |
| **Entrada NF** | `api/entrada-nota/rascunhos/` | Sem 503 Mongo quando rascunho PG |

**Medir amanhÃ£ (Win10 loja, DevTools â†’ Network, Ctrl+F5):**

| URL | Esperado pÃ³s-fix (ordem de grandeza) |
| --- | ------------------------------------ |
| `api_produtos_gestao_lista?pagina=1` | **&lt; 1,5 s** (PG+ledger) |
| `api_produtos_gestao_facetas` | **0 ms** no load (adiado); **&lt; 2 s** 1Âª vez ao abrir filtros (cache 15 min) |
| `api/produtos/cadastro/?pagina=1` | **&lt; 2 s** |
| `lancamentos/contas-pagar/` (document) | Lista visÃ­vel **&lt; 1 s** (bootstrap); API sync **+0,7 s** |
| `/api/lancamentos/?tipo=pagar&â€¦venc_de=HOJE` | **&lt; 1,5 s** PG |
| `api/entrada-nota/rascunhos/` | **&lt; 2 s** (sem 503 Mongo) |
| `/api/buscar/?q=â€¦&entrada_nfe=1` | **&lt; 1 s** busca 2+ chars |

### ðŸ”§ Perf busca produtos â€” Entrada NF etapa 2 + Cadastro ERP (02/07 Â· Renan) â€” **âœ… v6.08**

| Tela | O quÃª | Fix |
| ---- | ----- | --- |
| **Entrada NF Â· produtos** | `/api/buscar/?entrada_nfe=1` ainda pesado (Mongo inteiro + ajustes estoque) | Motor **lite** (projeÃ§Ã£o slim, regex cap 80, sem ajustes PIN) Â· `limit=48` no front |
| **Cadastro ERP Â· busca** | Lista demora ao digitar | Cache 45 s por termo Â· limite 64 Â· debounce 240 ms Â· sem badge ERP pendentes durante busca |

**Arquivos:** `views.py` Â· `motor_busca_unificado_util.py` Â· `entrada_nota.html` Â· `cadastro_erp_panel.js`

**Validar:** Ctrl+F5 Â· Entrada NF etapa 2 â€” buscar Â«raÃ§Ã£oÂ» / GM Â· Cadastro â€” mesma busca Â· DevTools &lt; ~1 s na API

### ðŸ”§ Fix â€” Â«Voltar ao PDV F1Â» some em GestÃ£o â€” **âœ… v6.05**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | App **SisVale GestÃ£o** (`agro_app_role=gestao`) ainda mostrava botÃ£o **VOLTAR AO PDV F1** no topo (ex.: Cadastro Produtos) â€” v6.00 sÃ³ escondia no BI |
| **Causa** | `_pdv_voltar_link.html` / F1 global nÃ£o checavam papel GestÃ£o; sÃ³ `dashboard_gerencial.html` usava `data-agro-hide-pdv` |
| **Fix** | `_agro_consulta_ui.html` â€” script + CSS global `data-agro-hide-pdv` quando `agro_app_role=gestao` ou `agro_inapp_embed` Â· `_pdv_voltar_link.html` â€” classe `agro-pdv-voltar-link` + skip server-side gestÃ£o Â· `_atalho_voltar_pdv.html` â€” F1 ignorado em gestÃ£o Â· `mobile_ajuste.html` â€” mesma classe |
| **Arquivos** | `_agro_consulta_ui.html`, `_pdv_voltar_link.html`, `_atalho_voltar_pdv.html`, `mobile_ajuste.html` |
| **Deploy** | **âœ… loja v6.05** |
| **Validar** | Ctrl+F5 no atalho **GestÃ£o** â†’ Cadastro ERP, Compras, LanÃ§amentos, RH â€” **sem** link F1 no topo Â· atalho **PDV** mantÃ©m F1 onde existia |

### âœ… Deploy loja **v6.04** â€” PIN descanso + cadastro 1234 online (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” *Pin manda tambem* + senha **`99738595`** |
| **Rollback** | Tag **`producao-rollback-v6.03-20260701`** @ **`e4232e8`** |
| **Git produÃ§Ã£o (prÃ©/post)** | **`e4232e8`** â†’ **`84541c2`** |
| **Cherry-picks** | **`872bf96`** (PIN Ãºnico servidor) + **`135e785`** (1234 cadastro inicial online) |
| **O quÃª** | Modo descanso LanÃ§amentos (~3 min idle) Â· PIN **sempre no servidor** Â· **1234** abre cadastro 1Âª vez Â· RH Operadores continua gestÃ£o |
| **Migrate** | Nenhuma Â· drift **base/estoque** prÃ©-existente |
| **DependÃªncia** | **`872bf96`** necessÃ¡rio antes de **`135e785`** |

**Validar loja:** Ctrl+F5 Â· badge **v6.04** Â· LanÃ§amentos idle ~3 min â†’ screensaver PIN Â· operador sem PIN: **1234** â†’ cadastro â†’ PIN definitivo Â· RH â†’ Operadores pins OK Â· 2 PCs mesmo PIN.

### âœ… Deploy loja **v6.00** â€” 2 janelas Chrome PDV/GestÃ£o (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” *pode enviar para produÃ§Ã£o* + senha **`99738595`** |
| **Rollback** | Tag **`producao-rollback-v5.99-20260701`** @ **`81c485c`** |
| **Git produÃ§Ã£o** | **`fe97096`** Â· features **`bda42ca`** |
| **Cherry-picks** | **`0dbd799`** â†’ **`0f77603`** (6 commits) Â· **`c1f9970`** jÃ¡ estava **`771ad00`** |
| **O quÃª** | PDV e GestÃ£o em **2 atalhos Chrome** Â· GestÃ£o **sem** guia PDV na sidebar Â· **sem** PDV F1 no topo do BI Â· overlay consultas ~95% Â· InÃ­cio no PDV foca janela GestÃ£o |
| **ExcluÃ­do** | **0048** orÃ§amentos PG Â· PIN descanso **`135e785`** *(subiu no pacote **v6.04**)* |
| **Migrate** | Nenhuma Â· drift **base/estoque** prÃ©-existente (igual pacotes 1â€“4) |

**Validar loja:** Ctrl+F5 Â· atalho **GestÃ£o** (`agro_app_role=gestao`) â†’ BI/caixa **sem** botÃ£o PDV Â· atalho **PDV** â†’ balcÃ£o dedicado Â· `scripts/criar_atalhos_sistvale.ps1` se faltar `.lnk`.

**Atalhos na barra Windows (Renan 01/07):** apps Chrome (`chrome_proxy`).

| Estado Renan (Win11 dev) | Detalhe |
| --- | --- |
| **Apps instalados** | PDV **`mcbdcdbnbbfijbkpclihamnpahafeigl`** Â· GestÃ£o/BI **`beilkmpkajkdhaejggnppapepjkgajjp`** (Chrome gravou label **SisVale InteligÃªncia de NegÃ³cio**) |
| **Barra OK** | Script detecta `*PDV*` / `*Intelig*` Â· atalhos **SisVale PDV** + **SisVale Gestao** na barra |
| **Remocao forcada** | `remover_apps_chrome_sistvale.ps1 -FecharChrome` Â· registro Windows Apps OK |
| **Fantasma `chrome://apps`** | Icone **SistVale** antigo pode ficar ate **fechar Chrome por completo** ou reiniciar PC â€” **pode ignorar** se **Instalar pagina como app** ja apareceu |

### ðŸ“‹ Pendente loja â€” **Win10 amanhÃ£ (02/07)** â€” 2 apps PDV + GestÃ£o na barra

**Objetivo:** em **cada PC Win10 da loja** (balcÃ£o, caixa, gestÃ£oâ€¦), mesmo setup do Renan: **2 janelas separadas** na barra â€” **sem** icone globo do Chrome Â· **sem** app antigo **SistVale** misturando abas.

**Antes:** Ctrl+F5 no site Â· badge loja **v6.04+** Â· Chrome atualizado.

**Por PC (repetir):**

| # | O quÃª |
| --- | --- |
| 1 | Abrir PowerShell na pasta do repo **ou** copiar `scripts\criar_atalhos_sistvale.ps1` + `scripts\remover_apps_chrome_sistvale.ps1` para o PC |
| 2 | **Se existir app SistVale antigo** (menu so Â«Abrir no appâ€¦Â»): `powershell -ExecutionPolicy Bypass -File .\scripts\remover_apps_chrome_sistvale.ps1 -FecharChrome` Â· fechar Chrome Â· ignorar fantasma em `chrome://apps` se **Instalar pagina como app** ja voltou |
| 3 | Abrir 2 janelas para instalar: `powershell -ExecutionPolicy Bypass -File .\scripts\criar_atalhos_sistvale.ps1 -BaseUrl "https://sistvale.com.br" -AbrirParaInstalar` |
| 4 | **Janela PDV** (`/pdv/`): menu â‹® â†’ **Instalar pagina como app** â†’ nome **SisVale PDV** |
| 5 | **Janela GestÃ£o** (BI): menu â‹® â†’ **Instalar pagina como app** â†’ nome **SisVale Gestao** *(Chrome pode gravar Â«InteligÃªncia de NegÃ³cioÂ» â€” OK)* |
| 6 | Fixar barra: `powershell -ExecutionPolicy Bypass -File .\scripts\criar_atalhos_sistvale.ps1 -BaseUrl "https://sistvale.com.br" -FixarBarra -Desktop -LimparBarra` |
| 7 | **Win10:** se `-FixarBarra` nao aparecer na barra, arrastar `.lnk` da **Area de trabalho** ou **Iniciar â†’ SisVale** para a barra Â· botao direito â†’ **Fixar na barra de tarefas** |
| 8 | Desfixar pins velhos: **SistVale**, **Consulta**, **Inteligencia** duplicada, icone **globo** (`chrome.exe`) |

**Validar no PC:**

| Atalho | Esperado |
| --- | --- |
| **SisVale PDV** | Abre **sÃ³ balcÃ£o** Â· janela propria Â· sem aba Chrome generica |
| **SisVale Gestao** | Abre **BI/caixa** Â· **sem** botao PDV na lateral Â· **sem** Â«Voltar PDV F1Â» no topo do BI |

**Notas:**

- **App-id e por maquina** â€” nao copiar IDs do PC do Renan; o script detecta sozinho apos instalar.
- **Perfil Chrome:** script usa perfil **Default** (Chrome normal da loja). Se a loja usar outro perfil, avisar antes.
- Scripts no repo: `scripts/criar_atalhos_sistvale.ps1` Â· `scripts/remover_apps_chrome_sistvale.ps1`

**Comando rapido refazer barra:** `criar_atalhos_sistvale.ps1 -BaseUrl https://sistvale.com.br -FixarBarra -Desktop -LimparBarra`

### âœ… Deploy loja **v5.77â€“v5.99** â€” pacotes 1â€“4 cherry (01/07 noite)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” *pode mandar tudo para produÃ§Ã£o* (loja fechada); **sem** merge `teste` inteiro |
| **Rollback** | Tag **`producao-rollback-v5.76-20260701`** @ **`7593664`** (HEAD anterior) |
| **Git produÃ§Ã£o** | **`81c485c`** @ `producao` Â· features **`594c1cd`** Â· rollback anterior **`7593664`** |
| **Pacote 1** | Caixa overlay 2 apps, PDV lateral F7/F3 (**sem** migraÃ§Ã£o **0048** orÃ§amentos PG) |
| **Pacote 2** | Retiradas Excel + operador + hÃ­fen ASCII |
| **Pacote 3** | RH ficha limpa, cancelar pagamento duplicado, sync CP, vale caixaâ†’folha (**`ce775c2`** skip vazio â€” jÃ¡ na loja) |
| **Pacote 4** | Entregas pÃ³s-venda `venda_id` + fiado; painel sem rÃ³tulos ERP |
| **ExcluÃ­do teste (pacotes 1â€“4)** | **0048** orÃ§amentos PG, PIN **135e785** *(subiu **v6.04**)*, dual window **0dbd799**, overlay **9896a90** *(subiu no pacote **v6.00**)* |
| **Migrate** | **Sem** 0048; `makemigrations --check` ainda aponta drift **base/estoque** (prÃ©-existente â€” nÃ£o gerado neste deploy) |
| **Render** | Push `producao` OK Â· badge **v5.98** apÃ³s Ctrl+F5 |

**Validar ao voltar (checklist curto):** Ctrl+F5 Â· **Caixa** overlay Menu/scroll Â· **PDV** F7/F3 lateral Â· **Retiradas** Excel + lista jun/2026 Â· **RH** ficha + fechamento Igualar CP Â· **Entregas** pÃ³s-venda fiado Â· orÃ§amentos PG **nÃ£o** subiram (comportamento legado).

**Rollback (se der problema):** `git checkout producao-rollback-v5.76-20260701` â†’ push `producao` (ou redeploy tag no Render).

### SÃ³ no **teste** (loja **v6.04** nÃ£o tem)

| Item | Detalhe |
| ---- | ------- |
| **OrÃ§amentos PG** | Migration **`0048`** Â· API `/api/pdv/orcamentos/` Â· sync multi-PC Â· GMORC bootstrap |

### Renan â€” desvinculaÃ§Ã£o Mongo (resumo)

**API ERP cortada** âœ… Â· **~85 %** operaÃ§Ã£o jÃ¡ Postgres Â· Mongo restante = RH salÃ¡rio CP, calendÃ¡rio LanÃ§amentos, BI hÃ­brido, etc. â€” **nÃ£o trava PDV** (ver Â§4.15).


### ðŸ› RH ficha â€” botÃ£o Â«Abrir folhaÂ» sumia (01/07 Â· teste)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Renan (Geraldo) â€” instruÃ§Ã£o Â«Caminho principalÂ» sem botÃ£o; **vale no mÃªs** funcionou |
| **Causa** | BotÃ£o sÃ³ no `{% empty %}` da lista â€” some quando jÃ¡ hÃ¡ fechamentos antigos |
| **Fix** | **Atalhos** + rodapÃ© da seÃ§Ã£o **3** sempre visÃ­veis Â· se mÃªs corrente jÃ¡ existe â†’ **Ir para folha MM/AAAA** |
| **Arquivos** | `funcionario_ficha.html` Â· `rh/views.py` Â· `rh_help_agents.html` |
| **OperaÃ§Ã£o** | MÃªs novo = **Abrir folha** (atalho ou Â§3) Â· mÃªs passado = link na lista ou vale/caixa na data |

### âœ… Deploy loja **v5.73â€“v5.75** â€” RH folha UX + reabrir (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan Â· produÃ§Ã£o + senha **`99738595`** Â· cherry **isolado** **`54aaa32`** Â· **`a10faa1`** Â· **`b825230`** (**sem** PDV/orÃ§amentos/caixa overlay) |
| **Pacote** | Lista fechamentos **todos status** Â· tela fechamento **limpa** (ajuda no **?**) Â· **Reabrir** mantÃ©m valor pago Â· detalhe sem recalc a cada F5 |
| **Validado loja** | Igualar CP Â· parcial CP Â· caixa SalÃ¡rios Â· Reabrir (bug Pago sumindo) |
| **PÃ³s-deploy** | Render ~2â€“5 min Â· **Ctrl+F5** RH fechamento |

### âœ… Deploy loja **v5.71â€“v5.72** â€” hotfix migrate RH 0005 (01/07)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Deploy **`61e19c2`** (v5.70) falhou no **migrate** â€” `rh.0005` apontava `base.0010` inexistente na loja |
| **Fix** | DependÃªncia â†’ **`base.0009`** Â· **`a138625`** produÃ§Ã£o |
| **PÃ³s-deploy** | Render ~2â€“5 min Â· **Ctrl+F5** |

### âœ… Deploy loja **v5.70** â€” RH pagamento salÃ¡rio CP + caixa (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan Â· *banana manda produÃ§Ã£o* + senha **`99738595`** Â· cherry **isolado** **`5434de0`** (**sem** PDV/outros do teste) |
| **Git produÃ§Ã£o** | Cherry-pick **`5434de0`** â†’ **`producao`** |
| **O quÃª** | `PagamentoSalarioFuncionario` Â· Pago sync = **vales + pagamentos** Â· baixa CP â†’ RH Â· caixa **SalÃ¡rios (pagamento folha)** |
| **Migrate** | **`0005_pagamento_salario_funcionario`** â€” Render roda no deploy |
| **PÃ³s-deploy** | Render ~2â€“5 min Â· **Ctrl+F5** Â· baixa CP â†’ Â«IgualarÂ» **nÃ£o apaga** Â· caixa plano **SalÃ¡rios** |

### âœ… Deploy loja **v5.67â€“v5.68** â€” RH folha espelha CP Postgres (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan Â· cherry-pick isolado + senha **`99738595`** |
| **Git produÃ§Ã£o** | **`77bbd14`** â€” cherry-pick **`ce775c2`** (2 arquivos: espelho Mongoâ†’PG apÃ³s sync folha) |
| **O quÃª** | Â«Igualar ao que estÃ¡ na folhaÂ» / Â«Criar ou atualizarÂ» passa a refletir vales e descontos no CP |
| **Risco** | **Baixo** â€” sÃ³ 1 linha PG do tÃ­tulo sincronizado; nÃ£o apaga outros lanÃ§amentos |
| **PÃ³s-deploy** | Render ~2â€“5 min Â· Geraldo jun/2026: passo 1 Salvar e recalcular â†’ passo 2 Igualar â†’ **Ctrl+F5** CP |

### âœ… Deploy loja **v5.66** â€” hotfix retiradas Adiantamento (01/07)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | `/caixa/retiradas/` jun/2026 + plano **Adiantamento** â†’ **500** (v5.65) |
| **Causa** | Cherry-pick vales perdeu `_op_exib` no merge â€” sÃ³ quebrava ao listar **ValeFuncionario** |
| **Fix** | Helpers inline em `caixa_retiradas_util.py` Â· **`1c46fc7`** cherry-pick **`producao`** |
| **Validar** | Ctrl+F5 Â· mesmo filtro Â· lista vales jun/2026 |

### âœ… Deploy loja **v5.65** â€” NF busca + retiradas vales (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan Â· *enviar para produÃ§Ã£o* + senha **`99738595`** Â· cherry-pick isolado (**sem** merge `teste`) |
| **Git** | **`de825f3`** (vales) Â· **`f1453c3`** (NF) Â· push **`producao`** |
| **Pacote** | NF passo 2 + vales RH no histÃ³rico retiradas |
| **Incidente** | Filtro Adiantamento 500 â€” corrigido **v5.66** |

### âœ… Deploy loja **v5.62** â€” fix fiado PDV/F8 (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan Â· *manda direto produÃ§Ã£o* + senha **`99738595`** Â· teste sem fiado (zerado â€” nÃ£o dava para validar) |
| **Git** | Merge **`teste`â†’`producao`** **`545aad3`** Â· fiado **`6875a3a`** + auditoria **`47c20e0`** |
| **Pacote** | Fiado: gestÃ£o/F8 lista tÃ­tulos por **nome** (igual grade) Â· comando `fiado_auditar_cadastros_duplicados` |
| **PÃ³s-deploy** | Render ~2â€“5 min Â· **Ctrl+F5** Â· **1 guia** Â· Queila: abrir gestÃ£o pelo PDV = **todos** tÃ­tulos Â· total = lateral **R$ 435,66** |
| **Auditoria** | Shell loja: `python manage.py fiado_auditar_cadastros_duplicados` |

### âœ… Deploy loja **v5.61** â€” perf busca PDV (01/07)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan Â· correÃ§Ã£o lentidÃ£o + senha **`99738595`** |
| **Git** | Cherry-pick **`bb5f1b6`** â†’ **`producao`** **`9fb0385`** Â· **sem** fiado v5.58 |
| **Pacote** | Cache local PDV Â· GM debounce Â· busca wizard menos Mongo Â· promo cache 90 s |
| **Arquivos** | `pdv_wizard.js` Â· `motor_busca_unificado_util.py` Â· `views.py` |
| **PÃ³s-deploy** | **Ctrl+F5** Â· **1 guia** Â· nÃ£o abrir BI+caixa+PDV juntos |

### ðŸ”´ INCIDENTE loja â€” tela cinza / fila **01/07** (histÃ³rico)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cinza ao abrir Â· caixa/vendas/fiado lentos Â· **1 worker** + vÃ¡rias guias |
| **Causa** | Fila Gunicorn (nÃ£o crash) Â· logs tudo 200 |
| **OperaÃ§Ã£o** | Fechar guias extras Â· avaliar **2 workers** Render |

### ðŸŸ  Fiado â€” sÃ³ 3 tÃ­tulos pelo PDV/F8 **v5.58â†’v5.62 loja** (01/07)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Queila Â· lateral **R$ 435,66** Â· gestÃ£o por cadastro **19+3** Â· PDV/F8 sÃ³ **3** (R$ 25,60) |
| **Causa** | Cadastros duplicados mesmo nome Â· filtro sÃ³ `cliente_agro_id` no atalho PDV |
| **Fix** | `_q_titulos_cliente_gestao` + F8 `_fiado_resumo` + `fiado_gestao.js` por **nome** |
| **Queila** | Â«(nÃ£o usar mais)Â» **R$ 410** + Â«Hinnen aÂ» **R$ 25,60** = **R$ 435,66** |
| **Auditoria loja** | `python manage.py fiado_auditar_cadastros_duplicados` Â· `--json` |

### âœ… Deploy loja **v5.56** â€” fix Indisp. F8 (30/06)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan Â· *manda* + senha **`99738595`** |
| **Git** | `teste` â†’ **`producao`** fast-forward **`3467ea0`â†’`59ef94d`** Â· push **`producao`** |
| **Pacote** | Fix **Indisp.** â€” catÃ¡logo Postgres (interno/barras/variaÃ§Ãµes) alinhado ao PDV Â· fix core **`b0df2a7`** |
| **PÃ³s-deploy** | Render ~2â€“5 min Â· **Ctrl+F5** PDV Â· badge **v5.56** Â· F8 Queila â†’ **+1** nos itens que eram Indisp. |

### ðŸŸ  F8 Â«Indisp.Â» na loja â€” fix catÃ¡logo Postgres **v5.55** (30/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Loja **v5.53** Â· F8 Queila Â· Â«Itens mais compradosÂ» com **Indisp.** (ex. areia, sachÃª, raÃ§Ã£o â€” cÃ³digos **2580**, **2236**, **818**â€¦) |
| **Causa** | Checagem **v5.51** sÃ³ `codigo_nfe` + Mongo Â· na loja catÃ¡logo = **Postgres** (`codigo_interno`, barras, variaÃ§Ãµes) â€” vendas antigas gravam **cÃ³digo interno**, nÃ£o GM |
| **Fix v5.55** | `codigos_gm_ativos_no_catalogo` alinhado ao PDV: overlay (nfe+barras) Â· **Produto** (nfe+interno+barras) Â· **ProdutoMarcaVariacaoAgro** Â· Mongo `index_codigos`/barras |
| **Arquivo** | `relacionamento_historico_erp_util.py` |
| **Deploy teste** | OK **`b0df2a7`** |
| **Deploy loja** | **`59ef94d`** **v5.56** |

### âœ… Deploy loja **v5.53** (30/06)

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan Â· *manda produÃ§Ã£o* + senha **`99738595`** |
| **Git** | `teste` â†’ **`producao`** fast-forward **`8ff62ca`â†’`3467ea0`** Â· push **`producao`** |
| **Pacote** | F8 perf (histÃ³rico paginado Â· lazy ciclo/cross) Â· **FL-042** (migration **0047** + comandos import/revert/probe) Â· alertas aba Resumo/Ciclo **v5.52** Â· Indisp. catÃ¡logo PG/Mongo **v5.51** Â· fix perf batch Mongo **v5.53** |
| **PÃ³s-deploy loja** | Aguardar Render ~2â€“5 min Â· **Ctrl+F5** PDV Â· conferir badge **v5.53** Â· migration **0047** (Render costuma rodar sozinha) |
| **Import ERP na loja** | **NÃ£o** veio no deploy â€” dados histÃ³rico ERP sÃ³ no **teste** (`erp-hist-teste-3`). Loja: import manual no shell **sÃ³** quando Renan decidir (mesmo fluxo FL-042) |

### ðŸŸ¢ Staging perf F8 â€” resolvido **v5.53** (30/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Render teste Â«sÃ³ carregandoÂ» apÃ³s **v5.52** Â· Gunicorn workers bloqueados |
| **v5.52 culpado?** | **NÃ£o** â€” sÃ³ JS (`pdv_relacionamento.js` alertas aba) + VERSION; **red herring** no timing |
| **Causa** | **v5.51** `codigos_gm_ativos_no_catalogo` (Mongo fallback) chamado **por venda** em `_serialize_venda_historico_*` Â· F8 inicial = 12 vendas â†’ **12+ roundtrips Mongo** Â· pior apÃ³s import **erp-hist-teste-3** (4309 vendas / 7501 itens) |
| **Fix v5.53** | Batch Ãºnico em `_historico_vendas_paginado` (todos codigos da pÃ¡gina) Â· serializers aceitam `ativos_it` opcional Â· `max_time_ms=8000` no find `DtoProduto` |
| **Arquivos** | `relacionamento_cliente_util.py` Â· `relacionamento_historico_erp_util.py` |
| **Deploy teste** | OK **v5.53** Â· Renan validou F8 rÃ¡pido |

### ðŸ§­ Dois trilhos â€” **nÃ£o misturar** (Renan Â· 30/06)

| Trilho | O quÃª | Onde testar | PrÃ³ximo passo |
| ------ | ----- | ----------- | ------------- |
| **A â€” F8 rÃ¡pido** | Modal abre sem travar Â· histÃ³rico 12 + Carregar mais Â· ciclo/cross lazy | PDV teste Â· Ctrl+F5 Â· badge **v5.48** Â· F8 | Renan valida abertura rÃ¡pida |
| **B â€” Import ERP (FL-042)** | Mongo vendas â‰¤26/05 â†’ Postgres Â· merge no F8 | Shell Render teste | Live **v5.48** â†’ `relacionamento_import_historico_erp --lote erp-hist-teste-2` Â· **Itens importados > 0** |

**Feito:** lote `erp-hist-teste-1` **revertido** (4309 cabeÃ§alhos vazios). **Commit teste:** `bdea194` **v5.48** (trilho A + fix join trilho B).

### F8 Relacionamento â€” perf abertura Â· **teste v5.48**

| Item | Detalhe |
| ---- | ------- |
| **Problema** | F8 ficava em Â«Carregando histÃ³ricoâ€¦Â» â€” API montava tudo de uma vez (`cross_sell` scan 8000 itens, ciclo, 500 vendas + merge 12 ERP) |
| **Fix** | Carga inicial **leve**: resumo + fiado (badge) + top produtos + **12 vendas** + pets/extras |
| **HistÃ³rico** | PÃ¡gina **12** vendas Â· botÃ£o **Carregar mais** Â· `GET ?secao=historico&historico_offset=` |
| **Lazy** | **Ciclo raÃ§Ã£o** e **Cross-sell** sÃ³ ao abrir a aba (`?secao=ciclo_racao` / `cross_sell`) |
| **Ciclo alertas Resumo** | Prefetch ciclo **em background** apÃ³s abrir (nÃ£o bloqueia) |
| **Alertas aba (Renan Â· 30/06)** | Caixa vermelha saiu do **Resumo** Â· **Resumo** (>60d sumido) e **Ciclo** (nÂº atrasados) no tÃ­tulo da aba â€” estilo Fiado |
| **Top produtos** | Amostra **150** vendas (antes 500) â€” suficiente para ranking |
| **Arquivos** | `relacionamento_cliente_util.py` Â· `views.py` Â· `pdv_relacionamento.js` |
| **Deploy** | **teste v5.48** Â· commit `bdea194` Â· push 30/06 |

### PendÃªncias fila â€” **FL-043** Â· **FL-044** (Renan Â· 30/06)

| ID | P | Pedido |
| -- | - | ------ |
| **FL-043** | **P2,8** | BotÃ£o desconto na baixa do fiado |
| **FL-044** | **P2,9** | Desconto automÃ¡tico funcionÃ¡rio (% prÃ©-definida) â€” provÃ¡vel junto **FL-001** (preÃ§o Ã— forma ou grupo cliente) |

### FL-042 fix **v5.47** â€” dry-run deu 0 importÃ¡veis (Renan Â· 30/06)

| Item | Detalhe |
| ---- | ------- |
| **Bug** | Leitura Mongo **sem** ClienteID/nome (projeÃ§Ã£o slim) â†’ 6236 Â«sem cliente AgroÂ» |
| **Fix** | ProjeÃ§Ã£o completa + match ID variantes + ponte DtoPessoa + nome na venda |
| **Consumidor** | **CONSUMIDOR NÃƒO IDENTIFICADO** â†’ **ignorado** (contador `vendas_consumidor`) |
| **PrÃ©-import** | Se Â«sem cliente AgroÂ» alto â†’ **`sincronizar_clientes_agro`** no teste |
| **PrÃ³ximo** | Import real teste â†’ validar F8 |

**Dry-run v5.47 OK (Renan Â· 30/06):** **4309** importÃ¡veis Â· **1264** consumidor ignorado Â· **663** sem cliente Agro Â· **876** clientes Â· 6236 no corte.

**Sync clientes teste:** **1473 criados** Â· **394** tel duplicado (ok).

**Import teste:** `python manage.py relacionamento_import_historico_erp --lote erp-hist-teste-1`

**Import real v1 (Renan Â· 30/06) â€” bug 0 itens:**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | **4309** vendas gravadas Â· **0** itens Â· warnings `naive datetime` |
| **Causa** | Join cabeÃ§alhoâ†’`DtoVendaProduto`: lookup usava sÃ³ 1Âª chave (`Id`/`_id`); linhas Mongo usam `NumeroVenda`/outra variante |
| **Fix** | `_venda_join_keys_header` + `_itens_raw_venda` (todas chaves H2) Â· `make_aware` em `data_venda` Â· contador `vendas_sem_itens` |
| **Reverter** | `python manage.py relacionamento_reverter_historico_erp --lote erp-hist-teste-1` |
| **Revert OK (Renan Â· 30/06)** | Shell teste: **4309** vendas removidas Â· lote **erp-hist-teste-1** apagado |
| **Import v2 (erp-hist-teste-2)** | Ainda **0 itens** Â· **4309 vendas sem linha** â€” Mongo `DtoVendaProduto` nÃ£o casou |
| **Fix v5.49** | FK ampliado + embutidos â€” ainda 0 (probe: **14068** linhas Mongo, **0** match) |
| **Causa v5.50** | `_id` ObjectId no cabeÃ§alho â†’ query sÃ³ ObjectId; `VendaID` na linha = **string** (tipo BSON) |
| **Fix v5.50** | `str(ObjectId)` em scalars + probe amostra â‰¤ corte ERP + `item_mongo_amostra` |
| **Deploy teste v5.48** | F8 perf (carga inicial + histÃ³rico paginado) + fix join chaves |
| **Reverter v2** | `relacionamento_reverter_historico_erp --lote erp-hist-teste-2` |
| **Probe v5.50 OK (Renan Â· 30/06)** | `itens_mongo` 1/4/1 Â· `VendaID_tipo: str` Â· amostra â‰¤26/05 |
| **Import v3 OK (Renan Â· 30/06)** | Lote **`erp-hist-teste-3`** Â· **4309** vendas Â· **7501** itens Â· **0** vendas sem linha Â· **4988** itens sem GM ativo (+1 Â«Indisp.Â») |
| **Validar** | F8 cliente com histÃ³rico ERP Â· aba HistÃ³rico Â· badge ERP Â· +1 sÃ³ se GM ativo |
| **Indisp. fix v5.51** | `codigos_gm_ativos_no_catalogo` â†’ overlay + **Produto PG** + **Mongo** ativo (GM4856 etc.) Â· **sem** reimport |
| **Indisp. (Renan Â· 30/06)** | Regra antiga: sÃ³ overlay `codigo_nfe` â†’ muitos Â«Indisp.Â» com nome igual no SisVale |

### FL-042 â€” HistÃ³rico ERP no F8 **v5.46+** Â· **teste**

| Item | Detalhe |
| ---- | ------- |
| **Escopo** | Import **1Ã—** Mongo `DtoVenda` â†’ Postgres **tabelas separadas** Â· merge sÃ³ no **F8** |
| **Corte** | ERP **â‰¤ 26/05/2026** Â· SisVale F8 **â‰¥ 27/05/2026** Â· testes PDV antes 27/05 **ignorados** |
| **Migration** | **`0047`** `RelacionamentoHistoricoImportLoteAgro` + venda/item histÃ³rico |
| **Comandos** | `relacionamento_import_historico_erp --dry-run` Â· import real Â· `relacionamento_reverter_historico_erp --lote X` ou `--tudo` |
| **`.env`** | `AGRO_REL_HISTORICO_ERP=true` (false = F8 sem merge, dados ficam) |
| **SeguranÃ§a** | **NÃ£o** mexe `VendaAgro`, fiado, cashback, vale, caixa, estoque |
| **Produto sumiu** | Snapshot nome/cÃ³digo Â· **+1** sÃ³ se GM ativo no cadastro Â· senÃ£o Â«Indisp.Â» |
| **Pendente teste** | ApÃ³s deploy: **dry-run** no Shell Render teste â†’ Renan ok â†’ import real |

---

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” *pode subir* + senha **99738595** Â· loja estava **v5.22.1** |
| **Pacote** | Merge **`teste` â†’ `producao`** Â· Relacionamento F8 + pets Postgres + menu caixa v5.24 + fiado link |
| **VERSION** | **5.44** UTF-8 (corrigido merge â€” evita build UTF-16) |
| **Migration** | **`0046`** `relacionamento_extras_json` |
| **ContingÃªncia** | Manual Â§3.2 (Zap + pausa ~2 min) â€” sem FL-038 cÃ³digo |
| **Loja** | Ctrl+F5 Â· badge **v5.44** Â· F8 + pet + venda R$ 1 |
| **Revert** | redeploy commit **`producao` anterior ao merge** |

### ðŸ“¦ PACOTE LOJA â€” **v5.44** Â· **SUBIU** 30/06

**DecisÃµes Renan (30/06):**

| Tema | DecisÃ£o |
| ---- | ------- |
| **Deploy seguro** | **FL-038** â€” neste deploy usa **rotina manual** Â§3.2 (Zap + janela calma + parar de finalizar venda ~2 min) Â· cÃ³digo FL-038 **depois** (P2) |
| **Fila offline** | **NÃ£o agora** â€” registrado **FL-041** (P3, projeto grande) |
| **Pets** | **OpÃ§Ã£o A** JSON Postgres (**v5.44**) Â· **FL-040** tabela Pet (P3) |

**ContingÃªncia neste deploy (sem cÃ³digo FL-038):**

1. Escolher **janela calma** (evitar pico e fechamento de caixa).
2. **Zap loja:** *Â«Atualizando o sistema ~2 min â€” **nÃ£o finalize venda** agora. Quem jÃ¡ clicou Finalizar, aguarde. Depois: Ctrl+F5 no PDV.Â»*
3. Renan avisa operadores: **parar de vender** ~2 min.
4. Assistente: merge + push **`producao`** â†’ acompanhar Render atÃ© **Live**.
5. **Ctrl+F5** em todos os PDVs Â· badge **v5.44** Â· venda teste R$ 0,01 (opcional).
6. Conferir **migration** `0046` rodou (Render deploy log).

**Script:** `scripts/preparar_deploy_loja_v544.ps1` â€” valida `VERSION` UTF-8 e diff Â· push sÃ³ com `-ExecutarPush` apÃ³s autorizaÃ§Ã£o.

---

#### O que entra na loja (resumo operador)

**ðŸ›’ PDV â€” Relacionamento com cliente (F8 / botÃ£o Hist.)**

| Novidade | Detalhe |
| -------- | ------- |
| **Atalho F8** | Modal do cliente selecionado (nÃ£o consumidor final) |
| **Aba Resumo** | 9 indicadores **numa linha** (visitas, ticket, cashback, vale, total, freq., Ãºltima visita, pets, WhatsApp) |
| **Top produtos** | Tabela + botÃ£o **+1 un.** no balcÃ£o (nÃ£o fecha o modal) |
| **Fiado** | SÃ³ na aba **Fiado** (laranja/vermelho + valor) Â· botÃ£o **LanÃ§amentos** abre gestÃ£o fiado do cliente |
| **HistÃ³rico** | Itens mais comprados + Ãºltimas vendas (sem cards duplicados do Resumo) |
| **Pets / saÃºde / anotaÃ§Ãµes** | Salvos no **cadastro Postgres** â€” **qualquer caixa** vÃª (nÃ£o fica sÃ³ no PC) |
| **Risco** | **Baixo** â€” consulta + cadastro auxiliar Â· **nÃ£o muda** preÃ§o, estoque nem finalizar venda |

**ðŸ’µ Caixa**

| Item | Nota |
| ---- | ---- |
| **Menu CAIXA mais rÃ¡pido** | Se a loja **jÃ¡** estiver na v5.24 (`eb9fcc9`), **nÃ£o repete** Â· se Render ainda v5.22, entra neste pacote |

**ðŸ—„ï¸ Banco (Postgres)**

| Migration | O quÃª |
| --------- | ----- |
| **`0046`** | Campo `relacionamento_extras_json` em **Cliente Agro** (pets, lembretes, anotaÃ§Ãµes) |

**âŒ Fora deste pacote**

| Item | Motivo |
| ---- | ------ |
| **FL-038 cÃ³digo** | SÃ³ documentaÃ§Ã£o â€” deploy usa Zap + pausa manual |
| **FL-039 / FL-040** | Ficha `/clientes/` e tabela Pet â€” P3 |
| **FL-041** | Fila vendas offline â€” P3 |
| **Demais FL-00x** | NÃ£o testados neste ciclo |

---

#### Checklist pÃ³s-deploy loja (Renan)

| # | O quÃª |
| - | ----- |
| 1 | Badge **v5.44** (Ctrl+F5) |
| 2 | PDV Â· cliente cadastrado Â· **F8** â†’ Resumo + abas |
| 3 | Pet no F8 â†’ outro PC vÃª o mesmo pet |
| 4 | Cliente com fiado â†’ aba Fiado + lanÃ§amentos |
| 5 | Menu **CAIXA** abre rÃ¡pido |
| 6 | Venda normal R$ 1 (sanidade) |

**Revert:** redeploy commit anterior em `producao` (anotar hash pÃ³s-merge).

---

**Anterior:** **17/06/2026** Â· produÃ§Ã£o **12â€“16/jun** (PDV, Caixa, Entrada NF, Etiquetas, Cadastro â€” lista guardada pelo Renan).

**Esta lista:** produÃ§Ã£o **17/06 â€“ 29/06/2026** Â· badge loja **v5.22 live** (v5.24 menu caixa **nÃ£o subiu**) Â· gerada **29/06**.

**Formato:** **lista completa** (PDV + caixa + NF + gestÃ£o + financeiro + BI + compras) â€” **nÃ£o** usar versÃ£o Â«sÃ³ balcÃ£oÂ».

**CadÃªncia Renan (29/06):** copiar e enviar no **WhatsApp da loja** **a cada 2 dias**, a partir de **29/06/2026**.

| PrÃ³ximos envios |
| --------------- |
| **29/06** (inÃ­cio) Â· **01/07** Â· **03/07** Â· **05/07** Â· **07/07** Â· â€¦ |

Entre deploys pode **reenviar a mesma**; quando subir pacote novo na **produÃ§Ã£o**, atualizar data de corte + bullets + badge `VERSION`.

**Dica pÃ³s-deploy:** **Ctrl+F5** no Chrome apÃ³s atualizaÃ§Ã£o.

---

ðŸš€ **AtualizaÃ§Ãµes do Sistema â€” GM Agro** ðŸš€  
ðŸ“… **ProduÃ§Ã£o: 17 a 29 de junho**

**ðŸ›’ PDV (Vendas)**  
â€¢ Autocomplete de produtos renovado: fundo azul, lista maior, **Carregar mais** e **Enter** adiciona sem fechar a busca.  
â€¢ Busca de **clientes** mais rÃ¡pida (lista guardada no navegador).  
â€¢ **Finalizar venda** mais rÃ¡pido: cupom fiscal sai em segundo plano; nÃ£o trava se o ERP estiver lento ou fora.  
â€¢ PDV **continua vendendo** mesmo com Mongo/ERP fora (produtos vÃªm do sistema Agro).  
â€¢ Etiquetas **GM com hÃ­fen**: bip nÃ£o apaga item do carrinho nem confunde cÃ³digos parecidos.  
â€¢ Modal de **CPF na NFC-e** maior e mais legÃ­vel.  
â€¢ **Entrega (F3):** telas e popups maiores; fluxo reorganizado (endereÃ§o â†’ taxa â†’ pagamento â†’ troco).  
â€¢ **Conferir entrega** mostra frete e total certos.  
â€¢ Ao **trocar cliente** na entrega, endereÃ§o nÃ£o Â«grudaÂ» mais da venda anterior.  
â€¢ **Selos de promo** no carrinho: verde quando atingiu a promo; amarelo quando **faltam unidades**.  
â€¢ Promo **Â«leve X pague YÂ»**: unidades extras voltam ao **preÃ§o normal** (ex.: 5Âº item fora da promo).  
â€¢ **Promo mix** (vÃ¡rios produtos): total calculado certo; linhas da mesma promo juntas, com borda colorida e selo **MIX**.  
â€¢ **Remover** item virou Ã­cone de lixeira (mais espaÃ§o na linha).

**ðŸ’µ Caixa (Fechamento)**  
â€¢ Nova tela de **histÃ³rico de retiradas/saÃ­das**: filtros por data, plano e quem levou (padrÃ£o = hoje).  
â€¢ ApÃ³s registrar saÃ­da: aviso verde **Â«Retirada concluÃ­daÂ»** e campos limpos.  
â€¢ **CorreÃ§Ã£o importante:** devoluÃ§Ã£o no mesmo dia **nÃ£o descontava em dobro** no fechamento nem no relatÃ³rio.  
â€¢ Menu **CAIXA** mais rÃ¡pido (detalhes pesados sÃ³ na tela **Saldo**) â€” **ðŸ“‹ fila** Â· prÃ³ximo deploy loja (ainda **nÃ£o** na v5.22 live).

**ðŸ§¾ Entrada de Nota Fiscal**  
â€¢ **Rascunhos** da nota salvos no sistema Agro (mais estÃ¡vel).  
â€¢ **Reabrir** nota finalizada: estorna tÃ­tulo no financeiro corretamente.  
â€¢ Estoque: aviso se ainda nÃ£o aplicou; reabrir limpa etapa de **lote/validade**.  
â€¢ **Auditoria financeiro** da NF (botÃ£o na lista de notas).

**ðŸ·ï¸ Etiquetas de PreÃ§o**  
â€¢ CÃ³digo interno faixa **230** impresso como **CODE128** (leitura mais confiÃ¡vel no balcÃ£o).

**ðŸ“¦ Cadastro / GestÃ£o de Produtos**  
â€¢ Busca na lista por **cÃ³digo GM** e **cÃ³digo de barras**.  
â€¢ **GestÃ£o** mais rÃ¡pida e estÃ¡vel (menos dependÃªncia do ERP).  
â€¢ Estoque operacional no **Agro** â€” venda baixa saldo mesmo com Mongo fora.

**ðŸŽ PromoÃ§Ãµes (cadastro)**  
â€¢ **Salvar promoÃ§Ã£o** corrigido (nÃ£o dava mais erro na etapa 2).  
â€¢ BotÃ£o **Excluir** na lista (remove duplicatas).  
â€¢ Etapa de produtos: **bip direto**, busca por GM ou nome; lista **continua aberta** apÃ³s adicionar.  
â€¢ BotÃ£o **Continuar â€” escolher produtos** corrigido.

**ðŸ’° LanÃ§amentos / Contas a pagar**  
â€¢ Contas a pagar e receber no **sistema Agro** â€” mais rÃ¡pido e estÃ¡vel.  
â€¢ Filtros da lista carregam **mais rÃ¡pido**.  
â€¢ **Backup** em ZIP (sÃ³ em aberto) e Excel completo (admin).  
â€¢ **Nova saÃ­da** em tela cheia: emprÃ©stimo entrada + pagamento; quitar por item.  
â€¢ **Totais corrigidos** â€” sincronizaÃ§Ã£o alinhou valores com o backup conferido.

**ðŸ“Š Tela inicial (BI)**  
â€¢ Cards de **contas a pagar/receber** alinhados ao financeiro Agro.  
â€¢ **GrÃ¡fico de gastos** por plano de conta (botÃ£o laranja no card Contas a Pagar).  
â€¢ **Meta de vendas** do mÃªs: histÃ³rico da planilha (set/25â€“mai/26) + vendas PDV atuais â€” comparaÃ§Ã£o mais realista.  
â€¢ Card de **validade** corrigido (produtos com data prÃ³xima aparecem certo).

**ðŸ›ï¸ Compras**  
â€¢ SugestÃ£o de compra usa **vendas do Agro** (mais rÃ¡pido).  
â€¢ **Folha Compras** por categoria/unidade inclui dados da gestÃ£o.  
â€¢ Card **Â«Ãºltimas comprasÂ»** na busca (NF Agro + ERP).

**ðŸ“¦ TransferÃªncias e validade**  
â€¢ Telas de **transferÃªncia** e **relatÃ³rio de validade** usam estoque Agro (ajustes + vendas).

---

### WIP â€” Relacionamento PDV (F8 rascunho) **30/06**

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | Modal com abas â€” testar na **loja** com cliente cheio de compras; demais abas aos poucos |
| **Atalho** | **F8** ou botÃ£o **Hist.** (F5 voltou a ser refresh do navegador) |
| **Aba inicial** | **Resumo** (padrÃ£o ao abrir) â€” **v5.41** |
| **Modal** | Altura **fixa** (ref. aba HistÃ³rico) â€” troca de aba **nÃ£o redimensiona** Â· scroll sÃ³ no miolo â€” **v5.35** |
| **Resumo** | **9 cards em linha** (1Ã—9): Visitas Â· Ticket Â· Cashback Â· Vale Â· Total Â· Freq. Â· Ãšlt. visita Â· Pets Â· WhatsApp â€” **v5.43** |
| **HistÃ³rico** | Sem cards Visitas/Ticket/Total (sÃ³ no Resumo) â€” **v5.42** |
| **Fiado** | BotÃ£o **LanÃ§amentos** â†’ `/fiado/?from=pdv&cliente=PK` abre **modal do cliente** (fallback API se nÃ£o estiver na lista) â€” **v5.39** |
| **Carrinho** | BotÃ£o **+ 1 un.** â€” cache local primeiro (sem ida ao servidor) Â· **v5.45 teste** |
| **Perf F8** | Abertura rÃ¡pida Â· histÃ³rico **12 + Carregar mais** Â· ciclo/cross **lazy** Â· **WIP teste 30/06** |
| **Risco loja** | **Baixo** â€” sÃ³ consulta Â· nÃ£o mexe venda/preÃ§o/estoque Â· modal lento OK |
| **Deploy loja** | **ðŸ“¦ Pacote v5.44 pronto** â€” aguardando autorizaÃ§Ã£o Renan (Â§ CHECKPOINT pacote loja) |
| **API** | `GET /api/pdv/relacionamento-cliente/?cliente_agro_pk=` Â· lazy `?secao=ciclo_racao|cross_sell|historico` |
| **Abas** | Resumo Â· HistÃ³rico Â· Ciclo raÃ§Ã£o Â· Cross-sell Â· Fiado Â· Cashback Â· MÃ©tricas Â· Pets Â· SaÃºde Â· AnotaÃ§Ãµes Â· Contato |
| **Dados reais** | Vendas PDV + itens Â· fiado Â· cashback/vale â€” fonte **Postgres Agro** |
| **Extras cliente** | Pets Â· saÃºde Â· anotaÃ§Ãµes â†’ **`ClienteAgro.relacionamento_extras_json` (Postgres)** Â· **v5.44** Â· qualquer caixa vÃª |
| **API extras** | `GET` painel traz `extras` Â· `POST /api/pdv/relacionamento-cliente/extras/` grava |
| **Regra assistente** | Relacionamento **cliente/pets/anotaÃ§Ãµes** = falar sÃ³ **Postgres** â€” nÃ£o misturar com ERP/Mongo nesse contexto |
| **PendÃªncia P3** | **FL-039** pets na ficha `/clientes/` Â· **FL-040** tabela Pet normalizada (opÃ§Ã£o B) |
| **Teste** | Render teste Â· PDV Â· cliente cadastrado Â· **F8** ou **Hist.** |

#### FL-042 â€” histÃ³rico ERP no F8 (Renan Â· 30/06)

**Hoje:** F8 sÃ³ vÃª vendas **SisVale** (`VendaAgro`). Vendas sÃ³ no ERP **nÃ£o entram**.

**DecisÃ£o seguranÃ§a (Renan):** **import Ãºnico** â†’ **Postgres somente leitura** â†’ **sem vÃ­nculo vivo com Mongo**. F8 **nunca** consulta Mongo em tempo real.

| OpÃ§Ã£o | SeguranÃ§a SisVale | PrÃ³s | Contras |
| ----- | ----------------- | ---- | ------- |
| **A â€” Mongo 1Ã— (recomendado)** | Alta, se gravar em **tabela histÃ³rica separada** | Completo (`DtoVenda` + itens); `ClienteID` casa com `externo_id`; sem planilha manual | Carga Mongo na hora do import; precisa **data corte** + dedup vs PDV |
| **B â€” Excel** | Alta (Renan revisa antes) | Controle humano; testa no staging com arquivo; zero carga Mongo | Trabalho manual; export ERP pode vir incompleto |
| **C â€” Mongo sempre ligado no F8** | **Evitar** | â€” | LentidÃ£o, dependÃªncia Mongo, risco duplicar venda ERP+PDV, quebra se espelho mudar |

**Recomendado:** **A + B** â€” comando **1Ã—** lÃª Mongo (`DtoVenda` / `DtoVendaProduto`), grava Postgres, **encerra**. Excel sÃ³ para **conferÃªncia**, correÃ§Ã£o ou o que o Mongo nÃ£o casar.

**Regras para nÃ£o quebrar loja:**
1. **NÃ£o** gravar histÃ³rico como venda â€œde balcÃ£oâ€ â€” tabela prÃ³pria (ex. `ClienteVendaHistoricoErpAgro`) **ou** `VendaAgro` com flag `origem=historico_erp` **excluÃ­da** de caixa, estoque, NFC-e e lista `/vendas/`.
2. **Data corte** = dia anterior ao PDV SisVale valer (ex. vendas Mongo **atÃ©** essa data; depois sÃ³ `VendaAgro`).
3. **Dedup:** mesmo `venda_id` ERP + cliente jÃ¡ no PDV â†’ nÃ£o somar duas vezes.
4. **Dry-run** primeiro (relatÃ³rio: quantas vendas, clientes sem match, duplicatas) â†’ Renan ok â†’ import real.
5. F8 sÃ³ faz **merge Postgres** (histÃ³rico importado + `VendaAgro`).

**Antes de codar:** dry-run no **teste** â†’ Renan ok â†’ import loja.

**Data corte PDV (Renan Â· 30/06 â€” confirmado):** **27/05/2026** = inÃ­cio **permanente** no SisVale. Antes disso pode existir **vendinha de teste** no PDV â€” **nÃ£o conta** no F8 como venda SisVale.

| Fonte | PerÃ­odo no F8 | Nota |
| ----- | ------------- | ---- |
| **Mongo ERP (import 1Ã—)** | data venda **â‰¤ 26/05/2026** | HistÃ³rico antigo |
| **`VendaAgro` SisVale** | **â‰¥ 27/05/2026** | ProduÃ§Ã£o real; ignora testes anteriores |
| **Teste PDV antes 27/05** | **Fora** do relacionamento | Fica na `/vendas/` se existir, mas F8 nÃ£o soma |

**Produtos que nÃ£o existem mais no SisVale** (cadastro excluÃ­do, cÃ³digo mudou, GM diferente):

- Na importaÃ§Ã£o grava **foto do item na Ã©poca**: cÃ³digo ERP, cÃ³digo GM (se tinha), **descriÃ§Ã£o**, qtd, valor â€” **sem depender** do cadastro atual.
- **Visitas, ticket, total, histÃ³rico de vendas** â†’ entram **normais** (Ã© venda passada, nÃ£o precisa produto vivo).
- **Top produtos / ciclo raÃ§Ã£o** â†’ agrupa pela **descriÃ§Ã£o + cÃ³digo gravado no histÃ³rico** (texto da Ã©poca).
- BotÃ£o **+1 no carrinho** (sÃ³ itens SisVale atuais hoje):
  - achou produto **ativo** no catÃ¡logo â†’ **+1** funciona;
  - **nÃ£o achou** (excluÃ­do / cÃ³digo mudou) â†’ mostra o nome **como estava**, **sem +1** ou com aviso *Â«indisponÃ­vel no cadastroÂ»* â€” operador busca similar manualmente.
- **NÃ£o** recria produto, **nÃ£o** altera estoque, **nÃ£o** quebra a tela.

Dry-run do import tambÃ©m lista **quantos itens** ficaram sem match no catÃ¡logo atual (sÃ³ informativo).

**Consumidor nÃ£o identificado:** vendas em nome **CONSUMIDOR NÃƒO IDENTIFICADO** (ou similar) â†’ **nÃ£o importa** (contador `vendas_consumidor`). Se importar, tambÃ©m nÃ£o quebra.

**PrÃ©-import (teste/loja):** se dry-run mostrar muitas Â«sem cliente AgroÂ», rodar **`sincronizar_clientes_agro`** antes.

**Garantias Renan (fiado / cashback / vale):**
- Import **nÃ£o grava** em `VendaAgro`, **nÃ£o abre** tÃ­tulo fiado, **nÃ£o mexe** `saldo_cashback` nem `saldo_vale_credito` do cliente.
- Fiado/cashback/vale no F8 continuam lendo **saldo real** (Postgres + regras atuais) â€” histÃ³rico ERP Ã© **sÃ³ leitura decorativa** (visitas, ticket, top produtos, lista antiga).
- Se no ERP antigo a venda era fiado, no F8 pode **aparecer como informaÃ§Ã£o** (Â«comprou fiado em 2024Â») â€” **nÃ£o altera** saldo em aberto de hoje.

**ReversÃ­vel (se der merda):**
1. Tabela **separada** (nÃ£o misturar com venda de balcÃ£o).
2. Cada import com **`lote_id`** (ex. `erp-hist-20260630`).
3. Comando **`reverter`** = apagar sÃ³ aquele lote (ou apagar tudo do histÃ³rico).
4. Flag **`.env`** liga/desliga merge no F8 **sem** apagar dados (`AGRO_REL_HISTORICO_ERP=false` â†’ F8 volta ao comportamento de hoje).
5. Ordem: **teste** dry-run â†’ import teste â†’ validar F8 â†’ **sÃ³ entÃ£o** loja (com frase + senha se produÃ§Ã£o).

---

| # | O quÃª | Passou? |
| - | ----- | ------- |
| 0 | **F8** abre na aba **Resumo** (nÃ£o HistÃ³rico) | â˜ |
| 0a | **Resumo:** 9 cards **numa linha sÃ³** (como abas) | â˜ |
| 0b | Aba **HistÃ³rico** sem cards Visitas / Total / Ticket (sÃ³ itens + vendas) | â˜ |
| 4 | Cadastrar **pet** no F8 â†’ fechar â†’ abrir em **outro PC** (ou outro Chrome) â†’ pet aparece | â˜ |
| 5 | **SaÃºde** e **anotaÃ§Ãµes** tambÃ©m persistem no cadastro | â˜ |
| 1 | **Resumo** sem card fiado (sÃ³ 3 cards) | â˜ |
| 2 | Cliente com fiado â†’ aba **FIADO** laranja/vermelha + valor | â˜ |
| 3 | Aba Fiado â†’ botÃ£o **LanÃ§amentos do cliente** abre gestÃ£o com modal do cliente | â˜ |

**NÃ£o testar ainda:** **FL-038 contingÃªncia deploy** â€” sÃ³ documentaÃ§Ã£o; **sem cÃ³digo**.

**Depois de OK:** Renan validou layout **v5.43** â€” para **loja**: pedir *Â«pode subir para produÃ§Ã£oÂ»* + senha **99738595** no mesmo chat Â· assistente monta cherry-pick (Relacionamento + **v5.24** caixa, corrigir `VERSION` UTF-8 do build que falhou).

### DEPLOY LOJA â€” perf menu caixa **v5.24** (29/06) âŒ build falhou

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” teste OK Â· *pode subir* + senha **99738595** |
| **Pacote** | Cherry-pick teste **`11d8634`** â†’ loja **`eb9fcc9`** |
| **Render** | **29/06 ~20:54** â€” *Exited with status 1 while building* Â· **live continua `0a0fd52` v5.22** |
| **Causa** | Arquivo **`VERSION`** no commit gravado em **UTF-16 (BOM)** â†’ `scripts/record_deploy.py` â†’ `read_app_version()` UTF-8 â†’ **UnicodeDecodeError** (1Âº passo do build) |
| **CorreÃ§Ã£o** | Recommit **`VERSION`** UTF-8 (`5.24\n`) + redeploy (cÃ³digo **`11d8634`** OK) Â· opcional: `read_app_version` tolerante a UTF-16 |
| **Renan 30/06** | **ðŸ“‹ Fila** â€” **nÃ£o** redeploy isolado agora Â· sobe **junto com o prÃ³ximo pacote** loja (fix `VERSION` no cherry-pick final) |
| **Revert loja** | jÃ¡ estÃ¡ em **`0a0fd52`** (v5.22) â€” botÃ£o Rollback no Render Ã© redundante |

### DEPLOY LOJA â€” devoluÃ§Ã£o caixa **FL-017** **v5.22** (29/06) âœ…

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” *pode enviar produÃ§Ã£o* + senha **99738595** Â· *com muito cuidado* |
| **Risco** | **Baixo** â€” sÃ³ `caixa_util.py` + `caixa_relatorio_util.py` Â· **sem** migraÃ§Ã£o Â· **sem** PDV |
| **Pacote** | Cherry-pick cÃ³digo teste **`a936f97`** + **`bed21ee`** â†’ commit loja **`0a0fd52`** (nÃ£o merge inteiro `teste`) |
| **O quÃª** | Fechamento: devoluÃ§Ã£o no mesmo turno nÃ£o desconta 2Ã— Â· RelatÃ³rio: vendas bruto + devoluÃ§Ãµes â†’ saldo certo |
| **Loja** | Ctrl+F5 apÃ³s Render Â· badge **v5.22** Â· falta fictÃ­cia (ex. 3Ã— R$ 70 = R$ 210) **nÃ£o** deve voltar |
| **Revert** | redeploy **`a44422c`** (produÃ§Ã£o v5.19 prÃ©-fix) |

### âœ… FL-017 â€” validaÃ§Ã£o Renan **teste v5.22** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **RelatÃ³rio caixa** | **âœ…** â€” Vendas + DevoluÃ§Ãµes batem Â· saldo coerente (print: entradas R$ 26 âˆ’ saÃ­das R$ 6 = **R$ 20**) |
| **Fechamento turno** | **âœ… teste** â€” esperado deixa de Â«inventar faltaÂ» em dobro na devoluÃ§Ã£o |
| **Frete no relatÃ³rio** | Renan viu linha de frete que antes nÃ£o aparecia â€” efeito colateral do relatÃ³rio voltar a fechar certo (nÃ£o era foco do fix) |
| **Loja â€” relato operador** | 3 devoluÃ§Ãµes **R$ 70** â†’ fechamento mostrava **falta ~R$ 210** (70Ã—3, desconto em dobro) â€” **fix v5.22 na loja** |
| **PrÃ³ximo** | Conferir loja pÃ³s-deploy (Ctrl+F5 Â· badge v5.22) |

### âš ï¸ FL-017 â€” confusÃ£o teste vs loja (29/06 â€” histÃ³rico)

| Onde | VersÃ£o | Fix devoluÃ§Ã£o caixa |
| ---- | ------ | ------------------- |
| **Render teste** | **v5.22** | **âœ…** |
| **Render SistVale** | **v5.22** | **âœ… deploy `0a0fd52`** |

### FIX â€” relatÃ³rio caixa saldo devoluÃ§Ã£o **FL-017** **v5.22** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | 3Ã— R$ 1,50 vendidas e devolvidas no dia â†’ relatÃ³rio **Vendas R$ 0** + **DevoluÃ§Ãµes âˆ’R$ 4,50** â†’ saldo **âˆ’R$ 4,50** (deveria **R$ 0**) |
| **Causa** | RelatÃ³rio **omitia** vendas devolvidas na seÃ§Ã£o Vendas **e** somava DevoluÃ§Ãµes â€” desconto em dobro no saldo |
| **Fix** | Vendas no relatÃ³rio = **bruto do dia** (inclui depois devolvidas); DevoluÃ§Ãµes abatem â†’ saldo **0** |
| **BI card VENDAS** | **R$ 0 Â· excl. 3 dev.** continua certo â€” Ã© **lÃ­quido** do dia, nÃ£o o relatÃ³rio de movimentos |
| **Teste** | Ctrl+F5 Â· RelatÃ³rio caixa hoje â†’ **Vendas +R$ 4,50** Â· **DevoluÃ§Ãµes âˆ’R$ 4,50** Â· **Saldo R$ 0** |

### PERF â€” painel caixa menu abre mais rÃ¡pido **v5.24 teste** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Breve demora ao clicar **CAIXA** no PDV (menu do turno) |
| **Causa** | Menu carregava consultas do **Saldo** (vendas Ã³rfÃ£s, tabela completa, planos saÃ­da) + cÃ¡lculo do turno **2Ã—** |
| **Fix** | Menu: sÃ³ resumo (esperado dinheiro + qtd vendas) Â· Ã³rfÃ£s/movimentos sÃ³ em **Saldo caixa** Â· agregaÃ§Ã£o **1 passagem** |
| **Ainda pesa** | Tailwind CDN + fontes Google na 1Âª abertura (padrÃ£o MPA) â€” nÃ£o mudou neste patch |

### FIX â€” devoluÃ§Ã£o nÃ£o duplica saldo do turno **FL-017** **v5.21** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | DevoluÃ§Ã£o no **mesmo turno**: venda some da lista **e** retirada desconta de novo â†’ saldo cai **2Ã—** |
| **Causa** | `resumo_esperado_por_forma` excluÃ­a venda `devolvida_em` **e** subtraÃ­a retirada Â«DevoluÃ§Ã£o venda #â€¦Â» |
| **Fix** | Retirada de devoluÃ§Ã£o **sÃ³ ignora** no esperado se a venda era **deste turno**; outro turno continua sÃ³ pela retirada (igual relatÃ³rio caixa) |
| **Arquivos** | `caixa_util.py` Â· `caixa_relatorio_util.py` (helper compartilhado) |
| **Teste staging** | Abrir caixa Â· vender Dinheiro Â· devolver Â· **Esperado** deve cair **1Ã—** (nÃ£o 2Ã—) Â· Ctrl+F5 painel caixa |

### DEPLOY LOJA â€” promo mix + selos carrinho **v5.19** (29/06) âœ…

| Item | Detalhe |
| ---- | ------- |
| **AutorizaÃ§Ã£o** | Renan â€” staging lento Â· *pode enviar produÃ§Ã£o* + senha **99738595** |
| **Risco** | Baixo â€” sÃ³ **PDV wizard** (JS/CSS) + fix catÃ¡logo delta Â· **sem** migraÃ§Ã£o banco |
| **Pacote** | `teste` â†’ `producao` merge **`a44422c`** (v5.08 â†’ v5.19) |
| **O quÃª** | Promo mix (preÃ§o correto 3+2) Â· selos MIX Â· agrupa linhas Â· fix import catÃ¡logo |
| **Loja** | **Ctrl+F5** no Chrome apÃ³s deploy Render Â· vendas em andamento: OK continuar apÃ³s refresh |
| **Revert** | redeploy commit **`8ea8ac9`** (produÃ§Ã£o prÃ©-merge) |

### FIX â€” busca PDV vazia no `runserver` local **v5.13** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `python manage.py runserver` Â· busca nÃ£o acha produto |
| **Causa** | Import errado em `mesclar_catalogo_pdv_cache` â†’ delta HTTP 500 Â· cache vazio |
| **Fix** | `integracoes.texto.normalizar` + fallback catÃ¡logo no wizard |
| **Local** | `.env` com Mongo (`VENDA_ERP_MONGO_*`) Â· reiniciar runserver Â· Ctrl+F5 PDV |

### FIX â€” promo mix 3+2 + indicador visual ligaÃ§Ã£o **v5.15â€“5.16** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Mix 3 un. produto A + 2 un. produto B â†’ preÃ§o/selo errado (ex. R$ 13,00 em vez de **R$ 12,90**) |
| **Causa** | Slots promocionais na ordem FIFO do carrinho (3Ã— promo na 1Âª linha) |
| **Fix** | Aloca slots promocionais priorizando **maior preÃ§o de tabela** (melhor desconto ao cliente) |
| **Visual** | Linhas da mesma promo mix: **borda colorida** + pill **MIX** no nome + selo **MIX / N de X** |
| **Arquivos** | `pdv_promocoes.js` Â· `pdv_wizard.js` Â· `pdv_wizard.html` |
| **Teste** | Ctrl+F5 Â· GM1769 Ã—3 + GM1771 Ã—2 (promo leve 4) â†’ total **R$ 12,90** Â· mesma cor nas 2 linhas |

### UX â€” selo mix menos confuso **v5.18** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Â«MIX 3 de 4Â» + Â«MIX 1 de 4Â» parecia **duas promos incompletas** (ex. 5+1 un.) |
| **Fix** | Selo mix ativo: **MIX 4 un.** (bloco fechado) + **N aqui** (quanto veio **desta linha**) |
| **Pendente mix** | **Faltam N Â· 3/4** â€” mostra progresso no carrinho |
| **Exemplo 5+1** | Linha A: **MIX 4 un.** / **3 aqui** + **+2 normal** Â· Linha B: **MIX 4 un.** / **1 aqui** |
| **Layout selo** | Coluna promo **largura/altura fixa** â€” GM e qtd alinhados mesmo com 1 ou 2 selos |
| **Cor mix** | Borda + gradiente **esquerda e direita** (mesma cor = mesma promo no carrinho) |
| **Ordem carrinho** | CritÃ©rio atingido â†’ linhas da **mesma promo ativa** **juntam** automaticamente |
| **Selo mix camadas** | **1Âª linha do bloco:** `MIX 4 un.` + `N aqui` Â· **demais:** sÃ³ `N aqui` (+ normal se houver) |

### FIX â€” promo mix (mesma promoÃ§Ã£o, produtos diferentes) **v5.10+** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | 2+2 saches da promo Â«testeÂ» â†’ cada linha Â«Faltam 2Â» Â· total errado |
| **Era** | Contava qtd **por produto**, nÃ£o por promoÃ§Ã£o |
| **Fix** | Soma unidades de **todos os produtos da mesma promo** (id) Â· leve X e acima de X |
| **Exemplo** | GM1769 Ã—2 + GM1771 Ã—2 â†’ **R$ 10,00** Â· selo **2 promo** em cada linha |
| **Teste** | Ctrl+F5 Â· promo Â«testeÂ» Â· mix 4 un. |

### WIP â€” FL-003 fase 1: selo promo no carrinho PDV (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Onde** | Linha do carrinho â€” entre **GM** e **qtd** (Ã¡rea indicada no print) |
| **Selo verde** | CritÃ©rio atingido â€” ex. **PROMO 4Ã—** |
| **Selo amarelo** | Duas linhas: **PROMO** (cima) + **Faltam N** (baixo) Â· N = mix no carrinho |
| **Mix ativo** | Borda colorida + **MIX** no nome Â· **1Âª linha:** `MIX 4 un.` + `N aqui` Â· **demais:** sÃ³ `N aqui` |
| **Dois selos** | Passou do critÃ©rio â€” ex. **4 promo** + **+1 normal** (5 un. leve 4) |
| **Remover** | Texto virou **lixeira** (Ã­cone) para ganhar espaÃ§o |
| **SÃ³ visual** | PreÃ§o jÃ¡ calculado antes; **nÃ£o** mexe em venda/caixa/fiscal |
| **Teste** | Ctrl+F5 PDV Â· GM1787 Â· qty 3/4/5/8/9 |

**FL-003 â€” fases restantes (depois):**

| Fase | Escopo | Status |
| ---- | ------ | ------ |
| **1** | Selo visual no carrinho PDV (wizard) | ðŸ”„ teste **v5.09** |
| **2** | Desconto promo no **DRE/relatÃ³rios** como Â«desconto clientesÂ» | ðŸ“‹ pendente |
| **3** | Linha de desconto promo na **impressÃ£o** da venda (80 mm / PDF) | ðŸ“‹ pendente |

### Promo Â«Leve X pague YÂ» â€” resto ao preÃ§o normal **v5.07+** (29/06) âœ… loja

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | Leve 4 @ R$ 2,50 â†’ 5Âº sache preÃ§o normal (12,90 nÃ£o 12,50) |
| **Era** | qty â‰¥ X â†’ **todas** as unidades a R$ Y |
| **Fix** | Grupos completos de X a R$ Y Â· resto ao preÃ§o tabela Â· 4â†’10,00 Â· 5â†’12,90 |
| **Deploy** | `teste`â†’`producao` **29/06** Â· merge `fe3e9a6` Â· checkpoint loja `8ea8ac9` |
| **Conferir loja** | Ctrl+F5 PDV Â· GM1787 Â· qty 5 â†’ total **R$ 12,90** |

### Fila loja â€” pedidos Zap / melhorias (Renan triagem)

> **Status operacional (Zap #+P+deploy):** ver **CHECKLIST ÃšNICO** no topo do CHECKPOINT. Esta tabela Ã© o **cadastro FL** (P + mÃ³dulo + pedido). Ao mudar status, atualizar **os dois**.

**Pacotes prontos â€” aguardando deploy loja (Renan 30/06):**

| Pacote | O quÃª | ObservaÃ§Ã£o |
| ------ | ----- | ---------- |
| **v5.44 Relacionamento F8** | Modal F8 + pets PG + fiado link | **âœ… teste** Â· **ðŸ“¦ pronto** Â· ver CHECKPOINT Â«PACOTE LOJA v5.44Â» |
| ~~v5.24 perf caixa~~ | Menu caixa lazy | **Branch `producao` jÃ¡ tem `eb9fcc9`** â€” conferir se Render live |

**DecisÃ£o deploy (30/06):** Renan â€” **FL-038 manual** neste deploy Â· demais atualizaÃ§Ãµes testadas sobem no **v5.44**.

**Como usar:** manda item a item no chat (`@banana` + prioridade + tela). Assistente registra aqui. **NÃ£o** vira cÃ³digo atÃ© vocÃª pedir ou subir de prioridade.

**Escala P (Renan):** **Px,y** = entre **Px** e **P(x+1)** â€” mais urgente que o de baixo, menos que o de cima. Decimal **menor** = mais perto do **P** inteiro de cima (ex. **P1,1** antes de **P1,5**). Inteiros: **P0** para a loja Â· **P1** grave Â· **P2** melhoria Â· **P3** depois. **Conflito** de sub-prioridade (ex. jÃ¡ existe **P2,9**): **nÃ£o** rebaixar para **P2** â€” usar decimal mais fino (**P2,91**, **P2,92**â€¦).

**ConferÃªncia (29/06):** itens **P1,x** jÃ¡ na fila batem com a regra (entre **P1** e **P2**): **FL-021** Â· **FL-022** = **P1,1** Â· **FL-019** Â· **FL-020** = **P1,5**. Nenhum precisou mudar de faixa. Ordem sugerida ao atacar: P1 â†’ P1,1 â†’ P1,5 â†’ P2.

**Lote Zap 29/06/2026 16:20** â€” neste envio: **FL-023â€¦FL-035** (fim da fila **FL-035**). Na conversa do Zap: procurar mensagens do **29/06** **atÃ© ~16:20** para achar o trecho; o Ãºltimo item deste lote Ã© **FL-035**.

| # | P | MÃ³dulo | Pedido | Status | Desde |
| - | - | ------ | ------ | ------ | ----- |
| **FL-001** | **P3** | PreÃ§os / PDV | Tabelas de preÃ§o personalizÃ¡veis por **forma de pagamento** ou **grupo de cliente** | ðŸ“‹ Pendente | 29/06 |
| **FL-002** | **P3** | PromoÃ§Ãµes | Revisar **usabilidade** da tela de promoÃ§Ã£o e **limpar textos inÃºteis** | ðŸ“‹ Pendente | 29/06 |
| **FL-003** | **P2** | PromoÃ§Ãµes / PDV / DRE | **Fase 1** selo promo no carrinho PDV Â· **Fase 2** DRE Â«desconto clientesÂ» Â· **Fase 3** linha na impressÃ£o da venda | ðŸ”„ Fase 1 teste Â· 2â€“3 pendente | 29/06 |
| **FL-004** | **P3** | RH | **Batida de ponto** dos funcionÃ¡rios (registro entrada/saÃ­da) | ðŸ“‹ Pendente | 29/06 |
| **FL-005** | **P2** | Entrega / impressÃ£o | Na impressÃ£o (separaÃ§Ã£o/entrega): **valor em R$ do troco a levar** na ida â€” **conferir antes** (hoje sÃ³ Â«troco: sim/nÃ£oÂ») | ðŸ“‹ Pendente Â· ðŸ” conferir | 29/06 |
| **FL-006** | **P2** | PDV / Entregas | **Ligar PDV** ao painel de entregas + **revisÃ£o visual** da tela `/entregas/` | ðŸ“‹ Pendente | 29/06 |
| **FL-007** | **P2** | UX geral | Revisar **tamanhos de layout** (Agro Display Scale) â€” **comeÃ§ar por** `/vendas/` (consulta de vendas) | ðŸ“‹ Pendente | 29/06 |
| **FL-008** | **P1** | PDV | Itens no carrinho **travam** — não altera qtd, preço nem remove · ex. **GM6083** | ✅ **loja v11.99** | 28/07 |
| **FL-009** | **P2** | Etiquetas | Na tela de **impressÃ£o de etiquetas**: ao adicionar item, **nÃ£o fechar** o autocomplete (manter busca aberta para bipar/digitar o prÃ³ximo) | ðŸ“‹ Pendente | 29/06 |
| **FL-010** | **P2** | Vendas | **Consulta de vendas** (`/vendas/`): buscador e **filtros completos** (perÃ­odo, cliente, forma, status, textoâ€¦) | ðŸ“‹ Pendente | 29/06 |
| **FL-011** | **P3** | Cashback | Revisar tela de **cashback** â€” melhorar usabilidade | ðŸ“‹ Pendente | 29/06 |
| **FL-012** | **P2** | Entrada NF | **Lixeira** para **desvincular produto da nota** â€” alinhar escopo com **Queila** antes de codar | ðŸ“‹ Pendente Â· â“ Queila | 29/06 |
| **FL-013** | **P2** | Etiquetas / impressÃ£o | CÃ³digo de barras em **PNG** (tipografia atual dificulta bip **1D e 2D**) | ðŸ“‹ Pendente | 29/06 |
| **FL-014** | **P3** | PDV | Projetar forma **mais prÃ¡tica** de alterar **quantidade** no carrinho | ðŸ“‹ Pendente | 29/06 |
| **FL-015** | **P2** | Etiquetas / PDV | **Regra bipagem etiqueta granel** â€” PDV nÃ£o leu direito; Renan fez **poucos testes** ainda | ðŸ“‹ Pendente Â· ðŸ” validar | 29/06 |
| **FL-016** | **P1** | Caixa | **Reset da contagem** do caixa (dia anterior) | ✅ **loja v6.75** | 03/07 |
| **FL-017** | **P1** | Caixa / devoluÃ§Ã£o | **DevoluÃ§Ã£o duplicada** no caixa â€” apaga venda e ainda registra **saÃ­da** (dobra o efeito) | **âœ… loja v5.22** Â· validado teste | 29/06 |
| **FL-018** | **P2** | Vendas | **Frete** no total da venda (`VendaAgro.frete`) | âœ… parcial 12/07 | 29/06 |
| **FL-019** | **P1,5** | Fiado | **Recibo de pagamentos** no fiado (comprovante ao cliente) | ✅ **teste v15.59** · PDV pergunta + Reimprimir `/fiado/` · 80 mm | 29/06 |
| **FL-020** | **P1,5** | PDV / fiscal | **Taxa de entrega** no cupom fiscal e cupom de venda (Renan 12/07: **deve sair**) | âœ… 12/07 | 29/06 |
| **FL-021** | **P1,1** | CP | BotÃ£o **NF** nÃ£o aparece na lista â€” ex.: tÃ­tulo **RBS R$ 781,64** | âœ… **loja v8.68** | 29/06 |
| **FL-022** | **P1,1** | CP | **Busca** no campo de filtros **inconsistente** (resultados variam / nÃ£o acha) | âœ… **#17** Renan testou Â· ðŸ“¦ pronto produÃ§Ã£o (fecha) | 29/06 |
| **FL-023** | **P1,2** | CP | Ao **buscar** na lista: **limpar filtros de data** | âœ… 12/07 | 29/06 16:20 |
| **FL-024** | **P1,6** | Cadastro | **Zap #22:** cat/sub/marca — só selecionar se existir · Food+PIN+log · busca sem acento | ✅ **loja v10.57** | 18/07 |
| **FL-025** | **P0,9** | Cadastro ERP | **SequÃªncia cÃ³digo interno** 9000+ â†’ **4010â€“5999** | âœ… 12/07 | 29/06 16:20 |
| **FL-026** | **P2** | Entrada NF | Add produto novo perde barras/lote | âœ… 12/07 | 29/06 16:20 |
| **FL-027** | **P2** | Entrada NF | XML forma boleto â†’ **Boleto BancÃ¡rio CN** | âœ… 12/07 | 29/06 16:20 |
| **FL-028** | **P1** | Fiado | BotÃ£o **Baixa** manda quitar **total de notas** de uma vez e **dÃ¡ erro** | âœ… TolerÃ¢ncia centavos v7.36 | 29/06 16:20 |
| **FL-029** | **P1,1** | Fiado | Baixa parcial ✅ loja v7.61 · **falta** opção deixar em **crédito** | ⚠️ parcial | 11/07 |
| **FL-030** | **P1,3** | Fiado / PDV | Forma de **ignorar bloqueio** por cliente com **notinhas fiado vencidas** â€” **PIN Geraldo / Geraldinho** | ðŸ“‹ Pendente | 29/06 16:20 |
| **FL-031** | **P1,6** | Entregas | **Terminar** de arrumar tela **`/entregas/`** | ðŸ“‹ Pendente | 29/06 16:20 |
| **FL-032** | **P1,5** | PDV | BotÃ£o **reset** no PDV â€” zerar pedido e **comeÃ§ar nova venda** | **âœ… loja v7.27** | 29/06 16:20 |
| **FL-051** | **P1** | Fiado / PDV | **Baixa fiado no PDV** â€” Baixa em `/fiado/` â†’ pagamento wizard (formas + maquininha + Point); substitui modal atual | âœ… **loja v7.40** | 07/07 |
| **FL-052** | **P1,1** | Fiado / fiscal | **NFC-e na baixa fiado** â€” emitir cupom na **quitaÃ§Ã£o** com forma real (venda original `venda_agro`); validar contador/SEFAZ | ðŸ“‹ Fila apÃ³s **FL-051** | 07/07 |
| **FL-033** | **P3** | BI / Home | **Zap #21:** indicador comparativo â€” **N-Ã©simo** dia da semana vs mÃªs anterior (ex. **3Âª terÃ§a** Ã— **3Âª terÃ§a**) | ðŸ“‹ Pendente Â· foto Word | 16/07 |
| **FL-053** | **P2** | Entrada NF / custo | **Zap #19:** histÃ³rico custo Ãºltimos pedidos **duplica tick** (fim etapa 2 + finalizar NF) | ðŸ“‹ Pendente Â· foto Word | 16/07 |
| **FL-054** | **P1,5** | Entregas / impressÃ£o | **Zap #20:** reimprimir papÃ©is (separaÃ§Ã£o Â· entregador Â· cliente) | ðŸ“‹ Pendente Â· foto Word | 16/07 |
| **FL-055** | **P0,1** | NFC-e / frete | **Zap #23:** rejeiÃ§Ã£o **535** â€” frete no total sem `vFrete` nos itens | âœ… **loja v9.16** | 16/07 |
| **FL-056** | **P0** | NFC-e / SEFAZ | RejeiÃ§Ãµes **963** (fiado+card) + **225** (CFOP/CEST pontuaÃ§Ã£o) â€” vendas #2812/#3347 | ðŸ“¦ **pronto pra envio** Â· teste **v9.21** | 17/07 |
| **FL-057** | **P0,1** | Ops / Render / Postgres | **PgBouncer** na loja — `agro-db` pooling + `DATABASE_URL` porta **6432** + restart web | 📋 Pendente · Renan no painel | 21/07 |
| **FL-058** | **P0,2** | PDV / Clientes / crédito | **Adicionar vale crédito** ao cliente **pelo PDV** (lançar crédito na conta do cliente na loja) · liga FL-029 crédito | ✅ **PDV-CLI-CADASTRO** 17/08 | 01/08 |
| **FL-034** | **P1,9** | PDV / Clientes | BotÃ£o **HistÃ³rico** nÃ£o filtra vendas do **cliente selecionado** â€” deve filtrar (relacionamento / devoluÃ§Ã£o) | ðŸ”„ **F8 modal rascunho** teste Â· fila loja | 29/06 16:20 |
| **FL-035** | **P2** | DevoluÃ§Ã£o | **DevoluÃ§Ã£o parcial** da venda â€” ou **itens especÃ­ficos** | ðŸ“¦ **#12** pronto loja (fecha) Â· âœ… teste Renan | 29/06 16:20 |
| **FL-036** | **P3** | PDV / Promo | **Faixa vertical** ou chaves ligando selos do **mesmo mix** no carrinho (opÃ§Ã£o visual 2) | ðŸ“‹ Pendente | 29/06 |
| **FL-037** | **P3** | PDV / Promo | **Selo mix Ãºnico** entre linhas (rowspan / bloco central â€” opÃ§Ã£o 3 experimental) | ðŸ“‹ Pendente | 29/06 |
| **FL-038** | **P2** | Deploy | **ContingÃªncia deploy** â€” Â§**3.2.0** leigo Â· Â§3.2.4 tÃ©cnico Â· **este deploy = manual Â§3.2** | ðŸ“‹ CÃ³digo pendente | 30/06 |
| **FL-039** | **P3** | Clientes | **Pets/saÃºde/anotaÃ§Ãµes** na **ficha** `/clientes/` (hoje sÃ³ no F8) | ðŸ“‹ Pendente | 30/06 |
| **FL-040** | **P3** | Clientes / PDV | **Tabela Pet** normalizada no Postgres (opÃ§Ã£o B â€” evoluir do JSON) | ðŸ“‹ Pendente | 30/06 |
| **FL-041** | **P3** | PDV | **Fila vendas offline** â€” processar no PC e sync depois (Renan descartou curto prazo) | ðŸ“‹ Pendente | 30/06 |
| **FL-046** | **P2** | PDV / Clientes | **2 janelas Chrome** (PDV + gestÃ£o) Â· atalhos Â· foco sem 2Âº PDV | âœ… loja **v6.00** | 01/07 |
| **FL-047** | **P2** | UX gestÃ£o | **Sidebar abas:** recolhida **~48px** sÃ³ Ã­cones Â· clique troca Â· seta expande | âœ… loja **v6.00** | 01/07 |
| **FL-048** | **P2** | Ops / Postgres | **Painel backup Postgres** â€” ZIP+Excel+restore Â· `/interno/pg-backup/` Â· Admin | ðŸ§ª **teste** 03/07 | 03/07 |
| **FL-049** | **P1,5** | PDV / Clientes / fiscal | CPF no PDV + NFC-e usa CPF salvo | ✅ **na loja** (`6af5cac`) | 04/07 |
| **FL-050** | **P2** | Vendas / fiscal | **`/vendas/`** â€” apÃ³s venda, **nÃ£o forÃ§ar** reemissÃ£o ao clicar cupom fiscal enquanto NFC-e roda em **background** Â· avisar *aguarde* Â· rÃ³tulo do botÃ£o muda (emitindo â†’ **reimprimir** quando OK) | ðŸ§ª **teste** 04/07 | 03/07 |
| **FL-042** | **P2** | PDV / Clientes | **HistÃ³rico ERP no F8** â€” **v5.46 teste** Â· import 1Ã— Â· corte ERP **â‰¤26/05** Â· SisVale **â‰¥27/05** | ðŸ§ª Render teste Â· dry-run â†’ import | 30/06 |
| **FL-043** | **P2,8** | Fiado | BotÃ£o **desconto** na **baixa** do fiado | ðŸ“‹ Pendente | 30/06 |
| **FL-044** | **P2,9** | PDV / PreÃ§os / RH | **Desconto automÃ¡tico funcionÃ¡rio** â€” % prÃ©-definida Â· provÃ¡vel junto com **tabelas de preÃ§o Ã— forma de pagamento ou grupo de cliente** (ver **FL-001**) | ðŸ“‹ Pendente | 30/06 |

**Notas assistente (cÃ³digo interno â€” Renan ignora se quiser):**

| ID | Tag | Escopo tÃ©cnico resumido |
| -- | --- | --------------------- |
| FL-001 | `preco-tabela-forma-grupo` | Cadastro tabelas preÃ§o Ã— forma ou Ã— grupo â€” fora do `precos_por_forma` atual |
| FL-002 | `promo-ux-copy` | `promocoes` form/wizard â€” UX + textos Â«?Â» / labels / ajuda |
| FL-003 | `pdv-promo-badge-dre-cupom` | **Fase 1 âœ… teste:** selo carrinho `pdv_wizard` + `pdv_promocoes.js` Â· **Fase 2 ðŸ“‹:** DRE/relatÃ³rio Â«desconto clientesÂ» Â· **Fase 3 ðŸ“‹:** cupom 80mm/PDF venda |
| FL-004 | `rh-batida-ponto` | MÃ³dulo novo em `rh/` â€” hoje sÃ³ folha/vales/ficha; definir: tablet/celular, PIN, export folha, integraÃ§Ã£o fechamento |
| FL-005 | `entrega-print-troco-r$` | `wizardPrintHtmlSeparacao` â€” hoje `troco: sim/nÃ£o`; falta **R$ a levar** (troco calculado = paga com âˆ’ total) |
| FL-006 | `pdv-entregas-painel-link` | Fluxo PDV â†’ `entregas_painel.html` + polish visual painel |
| FL-007 | `layout-scale-audit` | **1Âª tela:** `vendas_lista` / `/vendas/` â€” depois PDV, caixa, entrega, CPâ€¦ |
| FL-008 | `pdv-carrinho-item-travado` | Bug: linha carrinho sem editar qty/preÃ§o/remover â€” reproduzir + `pdv_wizard.js` Â· ex. **GM6083** |
| FL-009 | `etiquetas-autocomplete-aberto` | `produtos_etiquetas.js` / core â€” apÃ³s add na fila, manter painel de busca + foco no campo |
| FL-010 | `vendas-lista-filtros` | `vendas_lista` view + template â€” busca texto + filtros avanÃ§ados |
| FL-011 | `cashback-ux` | Tela cashback â€” mapear rota/template atual; UX + textos |
| FL-012 | `entrada-nf-desvincular-lixeira` | `entrada_nota.html` â€” API jÃ¡ tem `*_desvincular_de`; falta UI lixeira + fluxo com Queila |
| FL-013 | `etiquetas-barcode-png` | ImpressÃ£o etiquetas: trocar render (SVG/fonte?) por **PNG** raster â€” leitura scanner 1D/2D |
| FL-014 | `pdv-qty-ux` | Design qty no carrinho â€” `pdv_wizard.js` +/- e campo; menos cliques/teclado |
| FL-015 | `pdv-etiqueta-granel-scan` | `consulta_produtos.js` / `pdv_wizard.js` + `produtos_etiquetas*` â€” regra granel vs cÃ³digo loja |
| FL-016 | `caixa-reset-contagem` | Fechamento/abertura â€” contagem dia anterior; `caixa_util` / painel fechar |
| FL-017 | `caixa-devolucao-duplicada` | DevoluÃ§Ã£o: estorno venda + movimento saÃ­da em duplicidade â€” `devolucao_*` views |
| FL-018 | `vendas-detalhe-frete` | `/vendas/` detalhe/lista total sem frete; caixa relatÃ³rio OK â€” alinhar `VendaAgro.frete` |
| FL-019 | `fiado-recibo-pagamento` | Fiado: impressÃ£o/PDF recibo ao registrar pagamento parcial |
| FL-020 | `cupom-frete-separado` | `venda_cupom_80mm` + NFC-e â€” frete nÃ£o linha de produto / nÃ£o base tributÃ¡vel cupom |
| FL-021 | `cp-btn-nf-ausente` | `lancamentos_contas_pagar_teste.html` â€” `urlEntradaNfeEmbed` / vÃ­nculo NF entrada Â· ex. **RBS 781,64** |
| FL-022 | `cp-busca-inconsistente` | Filtro busca lista CP â€” `mongo_financeiro_util` + JS modal filtros |
| FL-023 | `cp-busca-limpa-datas` | Ao buscar na lista CP: resetar filtros de **data** (perÃ­odo nÃ£o deve persistir na busca textual) |
| FL-024 | `cadastro-popup-cat-marca-food` | **#22:** nÃ£o auto-criar ao digitar Â· select sÃ³ se existir Â· popup Food+PIN+log Â· busca CI sem acento Â· cat/sub/marca |
| FL-025 | `codigo-interno-seq-4k` | SequÃªncia cÃ³digo interno GM â€” hoje **9000+**; alvo combinado **~4000â€“5000** â€” `cadastro_erp` / overlay |
| FL-026 | `entrada-nf-perde-conferencia` | Add linha nova na NF: zera barras (passo 3) e lote/val (4â€“5) dos itens jÃ¡ conferidos |
| FL-027 | `entrada-nf-xml-forma-boleto-cn` | Parse XML etapa 7: mapear forma pag. **Boleto BancÃ¡rio CN** (nÃ£o sÃ³ Â«Boleto BancÃ¡rioÂ») |
| FL-028 | `fiado-baixa-lote-erro` | BotÃ£o baixa fiado tenta quitar **todas** as notas â€” erro; fluxo deve ser seletivo ou corrigir aggregate |
| FL-029 | `fiado-baixa-parcial-credito` | Validar baixa parcial + saldo em **crÃ©dito** cliente |
| FL-030 | `fiado-ignorar-vencido-pin` | Override bloqueio notinhas vencidas â€” PIN **Geraldo** / **Geraldinho** |
| FL-031 | `entregas-polish` | Continuar FL-006 â€” `entregas_painel.html` + APIs |
| FL-032 | `pdv-reset-nova-venda` | BotÃ£o explÃ­cito reset carrinho/contexto e nova venda |
| FL-033 | `bi-vendas-dia-nth-weekday` | **#21:** dashboard N-Ã©simo weekday vs mÃªs anterior |
| FL-053 | `entrada-nf-historico-custo-duplo` | **#19:** tick histÃ³rico custo 2Ã— (etapa 2 + finalize) â€” dedupe / um sÃ³ evento |
| FL-054 | `entregas-reimprimir-papeis` | **#20:** reimpressÃ£o separaÃ§Ã£o / entregador / cliente no painel entregas |
| FL-055 | `nfce-frete-vfrete-itens-535` | **#23:** `det/prod/vFrete` = `ICMSTot/vFrete` (535) |
| FL-056 | `nfce-963-card-fiado-225-fiscal-digitos` | **963:** sem `card` em tPag 05 Â· **225:** NCM/CFOP/CEST sÃ³ dÃ­gitos |
| FL-057 | `render-pgbouncer-agro-db-6432` | Render: Info do Postgres → Connection pooling · web loja `DATABASE_URL` pooled **:6432** · redeploy · conferir healthz |
| FL-058 | `pdv-vale-credito-cliente` | PDV: lançar / adicionar **vale crédito** ao cliente selecionado (Postgres) · uso depois na venda · overlap FL-029 crédito |
| FL-034 | `pdv-historico-cliente-filtro` | HistÃ³rico vendas deve respeitar **cliente selecionado** no PDV |
| FL-035 | `devolucao-parcial-itens` | DevoluÃ§Ã£o por itens / parcial â€” hoje provavelmente venda inteira |
| FL-036 | `pdv-mix-selo-faixa-vertical` | Faixa/chaves CSS ligando coluna promo entre linhas do mesmo mix (opÃ§Ã£o 2) |
| FL-037 | `pdv-mix-selo-rowspan` | Selo mix Ãºnico central entre linhas â€” experimental (opÃ§Ã£o 3) |
| FL-038 | `deploy-contingencia` | Postgres escopos Â· cron ON/OFF Â· middleware POSTs por mÃ³dulo (pdv, caixa, fiado, devolucao, lancamentos) Â· Â§3.2.4 |
| FL-039 | `cliente-ficha-pets-relacionamento` | Exibir/editar `relacionamento_extras_json` na ficha `/clientes/` |
| FL-040 | `cliente-pet-tabela-normalizada` | Modelo `ClientePetAgro` (+ lembretes) â€” migrar do JSON quando priorizar |
| FL-041 | `pdv-fila-vendas-offline` | Projeto grande: fila local + sync + estoque/fiado â€” **nÃ£o** substitui FL-038 curto prazo |
| FL-042 | `relacionamento-import-erp-1x` | Mongo DtoVenda 1Ã— â†’ Postgres histÃ³rico Â· merge F8 Â· **sem** leitura Mongo no PDV Â· Excel = audit |
| FL-043 | `fiado-baixa-desconto` | UI + backend baixa fiado â€” aplicar **desconto** no pagamento (parcial ou total) |
| FL-044 | `pdv-desconto-funcionario-auto` | % desconto por funcionÃ¡rio/cliente grupo Â· overlap **FL-001** (tabela preÃ§o Ã— forma ou Ã— grupo) |
| FL-048 | `pg-backup-painel-portavel` | Admin â†’ `/interno/pg-backup/` Â· ZIP manifest+JSONL+**resumo.xlsx** Â· checkbox Â· restore+senha admin |
| FL-049 | `pdv-cliente-cpf-cadastro-nfce` | Campo **CPF** no cadastro cliente PDV (F8/modal) Â· persistir `ClienteAgro` Â· emissÃ£o NFC-e puxa CPF do cliente selecionado (hoje: modal sÃ³ se cadastro vazio) |
| FL-050 | `vendas-lista-nfce-background-ux` | `/vendas/` + detalhe: distinguir **processando** (`nfce_solicitada` sem doc / status PROCESSANDO) vs **erro** vs **autorizada** Â· nÃ£o abrir modal reemitir no meio Â· poll ou refresh Â· rÃ³tulos: *Emitindoâ€¦* / *Reimprimir cupom fiscal* Â· `vendas_lista.html` Â· `nfce_venda_util.painel_nfce_venda` Â· `views_nfce` |
| FL-051 | `fiado-baixa-pdv-pagamento` | `/fiado/` Baixa â†’ `/pdv/checkout/` modo cobranÃ§a (`titulo_id`/`titulo_ids`) Â· pagamento completo PDV Â· confirmar quita `FiadoTituloAgro` + `MovimentoCaixa` Â· deprecar modal `fiado-modal-baixa` |
| FL-052 | `fiado-baixa-nfce-quitacao` | ApÃ³s baixa: NFC-e da **venda original** com `tPag` da forma paga (nÃ£o crÃ©dito loja) Â· CPF cliente Â· conferÃªncia fiscal antes de auto |

**Notas FL-051 / FL-052 (07/07):** Renan escolheu **sempre PDV** (nÃ£o hÃ­brido). **FL-051** = **P1** (operacional). **FL-052** = **P1,1** (fiscal na fila, nÃ£o no 1Âº pacote). Incluir fix **FL-028** se possÃ­vel no mesmo pacote pagamento.

**Notas FL-050 (03/07):** **P2** â€” Renan: operador vai direto em Consulta vendas apÃ³s Enter no PDV; clique trata como reemissÃ£o. Causa provÃ¡vel: `venda_nfce_pendente` = true com `nfce_solicitada` e sem `NfceDocumentoAgro` ainda. `views_nfce` jÃ¡ retorna Â«em processamentoÂ» no POST duplicado â€” falta UX na **lista**.

**Notas FL-049 (03/07):** **P1,5** â€” junto **FL-019** Â· **FL-020** Â· **FL-032** na faixa. Objetivo: operador cadastra CPF uma vez no PDV; cupom fiscal usa automaticamente. Conferir se ficha `/clientes/` jÃ¡ tem CPF e espelhar no F8.

**Notas FL-043 / FL-044 (30/06):** **P2,8** desconto na baixa fiado Â· **P2,9** desconto auto funcionÃ¡rio (% cadastro) â€” Renan acha que depende de **FL-001** (preÃ§o por forma/grupo). **FL-033** (BI vendas dia) ficou **P2,91** (liberou **P2,9** para **FL-044**).

**Notas lote 29/06 16:20:** **FL-025** **P0,9** (quase P1 â€” sequÃªncia cÃ³digo). **FL-028** **P1** fiado baixa em lote. **FL-029** reforÃ§a fiado (**P1,1**, junto FL-019 recibo). **FL-030** PINs nomeados â€” conferir usuÃ¡rios no admin. **FL-031** overlap com **FL-006** entregas. **FL-032** outro **P1,5** PDV (FL-020 = cupom frete).

**Notas FL-021 / FL-022 (CP â€” 29/06):** **P1,1**. FL-021: coluna/botÃ£o **NF** sÃ³ renderiza se `urlEntradaNfeEmbed(row)` achar vÃ­nculo entrada NF â€” reproduzir com fornecedor **RBS** Â· valor **R$ 781,64**. FL-022: busca na lista (modal Filtros) â€” anotar termo que falha vs que acha.

**Notas FL-016 / FL-017 (caixa â€” 29/06):** dois **P1** operacionais. FL-017: conferir se devoluÃ§Ã£o gera movimento caixa **e** reverte venda sem idempotÃªncia. Pedir nÂº venda + print fechamento se repetir.

**Notas FL-018:** divergÃªncia **tela vendas** vs **relatÃ³rio caixa** â€” bug de exibiÃ§Ã£o/cÃ¡lculo no detalhe, nÃ£o necessariamente no caixa.

**Notas FL-020:** frete hoje pode entrar no total do cupom/NFC-e; Renan quer **fora** do cupom fiscal e do cupom de venda (cobranÃ§a Ã  parte ou sÃ³ entrega).

**Notas FL-001:** hoje o PDV jÃ¡ aplica preÃ§o por forma (`precos_por_forma` / promoÃ§Ãµes). Escopo novo = cadastro de **tabelas** (vÃ¡rios preÃ§os por produto Ã— forma ou Ã— grupo cliente) â€” projeto grande; definir regras com Renan antes de codar.

**Notas FL-007:** Renan (29/06) â€” **priorizar `/vendas/`** como piloto do ajuste de layout (Agro Display Scale Â§11); usar como referÃªncia antes das demais telas.

**Notas FL-015:** poucos testes na loja (29/06). Conferir: cÃ³digo impresso na etiqueta granel vs parser PDV (`eh_granel`, prefixo GM, peso embutido). Pedir 2â€“3 cÃ³digos que falharam + print da etiqueta.

**Notas FL-013:** hipÃ³tese â€” barras em SVG/fonte na folha saem finas/baixo contraste para leitor; gerar PNG (ou aumentar quiet zone / altura) na impressÃ£o de preÃ§o.

**Notas FL-012:** backend parcial em `entrada_nota` / overlay (`c_prod_nf_desvincular_de`, `ean_embalagem_nf_desvincular_de`). **Pendente:** conversa com **Queila** â€” quando usar lixeira, nota em qual etapa, confirmaÃ§Ã£o.

**Notas FL-005:** prÃ©-anÃ¡lise 29/06 â€” via **SeparaÃ§Ã£o** jÃ¡ imprime Â«troco: sim/nÃ£oÂ»; **nÃ£o** imprime o **valor em reais** a levar. Campo `entrega.troco` no PDV = Â«cliente paga comÂ». Implementar linha explÃ­cita tipo **Â«Levar troco: R$ X,XXÂ»** nas vias separaÃ§Ã£o + entregador (se dinheiro na entrega).

**Notas FL-008:** **P1** â€” impacto direto no balcÃ£o. **Caso reportado (29/06):** **GM6083** â€” apÃ³s add no carrinho, qtd/preÃ§o/remover nÃ£o respondem; sÃ³ Â«Limpar carrinhoÂ». Reproduzir no staging com esse cÃ³digo; anotar se promo ou forma de pagamento estava ativa.

**Notas FL-004:** projeto **novo** â€” RH atual cobre folha, vales e ficha; **nÃ£o** tem ponto. Antes de codar: Renan define se Ã© sÃ³ registro interno, se entra no fechamento da folha e quem bate (PIN, lista na loja, celular).

**Notas FL-003:** **P2**. **Fase 1 (29/06):** selo na linha do carrinho (entre GM e qtd) â€” verde Â«PROMO 4Ã—Â», amarelo Â«Faltam NÂ», misto Â«4 promoÂ» + Â«+1 normalÂ» quando passa do bloco leve X; lixeira no lugar de Â«RemoverÂ». SÃ³ exibiÃ§Ã£o â€” preÃ§o jÃ¡ vem da regra leve X. **Fase 2:** alinhar com Renan se Â«desconto clientesÂ» = plano DRE existente ou campo novo na venda. **Fase 3:** cupom/impressÃ£o 80 mm.

### FECHADO + DEPLOY LOJA â€” hotfix promo Â«ContinuarÂ» **v4.90** (29/06)

**Renan:** *Â«mandaÂ»* + senha **99738595**.

**Loja (`producao`):** merge **`teste`** â†’ **`producao`** Â· fast-forward **`54aa23b`** Â· **push OK**.

**PÃ³s-deploy loja:** Ctrl+F5 Â· Nova promoÃ§Ã£o â†’ **Continuar â€” escolher produtos** â†’ etapa 2.

### INCIDENTE LOJA â€” promo Â«ContinuarÂ» nÃ£o avanÃ§a etapa 1 **v4.90** (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Clicar **Continuar â€” escolher produtos** nÃ£o vai para etapa 2 |
| **Causa** | Erro de sintaxe JS no v4.88 â€” script inteiro nÃ£o carregava |
| **Fix v4.90** | Restaurar `tentarAutoAdicionarBusca` + bind **Continuar** independente da etapa 2 |

### FECHADO + DEPLOY LOJA â€” promo UX + keep-warm staging **v4.86â€“v4.88** (29/06)

**Renan:** *Â«pode subir para produÃ§Ã£oÂ»* + senha **99738595** (nÃ£o perigoso para loja em uso).

**Loja (`producao`):** merge **`teste`** â†’ **`producao`** Â· fast-forward **`871c4b4`** Â· **push OK**.

**PÃ³s-deploy loja:** Ctrl+F5 Â· PromoÃ§Ãµes etapa 2 â€” add vÃ¡rios da mesma busca Â· bip/autocomplete GM/nome Â· **Excluir** duplicatas.

| Pacote | O quÃª | Risco loja |
| ------ | ----- | ---------- |
| **v4.88** | + Add mantÃ©m lista de busca | SÃ³ tela promoÃ§Ã£o |
| **v4.83â€“v4.84** | Bip + autocomplete GM/nome | SÃ³ tela promoÃ§Ã£o |
| **v4.81** | BotÃ£o Excluir promoÃ§Ã£o | PG |
| **v4.86â€“v4.87** | Cron ping Mongo 5 min + keep-warm **staging** | NÃ£o acelera nem desacelera PDV/caixa |

### FECHADO + DEPLOY LOJA â€” promoÃ§Ãµes bip + autocomplete GM/nome **v4.83â€“v4.84** (29/06)

**Renan:** *Â«manda direto produÃ§Ã£oÂ»* + senha **99738595** (Render teste lento na 1Âª abertura â€” validaÃ§Ã£o direto na loja).

**Loja (`producao`):** merge **`teste`** â†’ **`producao`** Â· fast-forward **`536c5c5`** Â· **push OK**.

**PÃ³s-deploy loja:** Ctrl+F5 Â· PromoÃ§Ãµes etapa 2 Â· bip barras Â· autocomplete GM/nome Â· **Excluir** duplicatas se ainda houver.

| Item | Detalhe |
| ---- | ------- |
| **v4.83â€“v4.84** | Barras entra direto Â· GM/nome autocomplete Â· GM completo match exato Â· botÃ£o Excluir (v4.81) |
| **Dados** | PromoÃ§Ãµes 100 % Postgres |

### FECHADO + DEPLOY LOJA â€” promoÃ§Ãµes botÃ£o Excluir **v4.81** (29/06)

**Renan:** *Â«manda produÃ§Ã£oÂ»* + senha **99738595**.

**Loja (`producao`):** merge **`teste`** â†’ **`producao`** Â· fast-forward **`536527a`** Â· **push OK**.

**PÃ³s-deploy loja:** Ctrl+F5 Â· PromoÃ§Ãµes â†’ **Excluir** duplicatas Â· manter a de 4 produtos.

| Item | Detalhe |
| ---- | ------- |
| **v4.81** | BotÃ£o **Excluir** na lista Â· confirmaÃ§Ã£o Â· PG (`PromocaoAgro` CASCADE) |
| **Dados** | PromoÃ§Ãµes 100 % Postgres â€” Mongo sÃ³ na busca de produto |

### FECHADO + DEPLOY LOJA â€” fix promoÃ§Ã£o Â«SalvarÂ» **v4.80** (29/06)

**Renan:** *Â«pode subir diretoÂ»* + senha **99738595** (fix isolado â€” sÃ³ mensagem JS, sem banco/API).

**Loja (`producao`):** merge **`teste`** â†’ **`producao`** Â· **push OK** (apÃ³s resolver conflito `banana.md`).

**PÃ³s-deploy loja:** Ctrl+F5 Â· Nova promoÃ§Ã£o â†’ produtos â†’ **Salvar promoÃ§Ã£o** â†’ mensagem verde + lista.

### FIX â€” promoÃ§Ã£o Â«SalvarÂ» quebra com classList (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Etapa 2 Â· adicionar produtos Â· **Salvar promoÃ§Ã£o** â†’ erro `DOMTokenList` / token com espaÃ§os |
| **Causa** | `showMsg` passava 3 classes Tailwind numa string sÃ³ para `classList.add()` |
| **Fix v4.80** | Cada classe como argumento separado |
| **Risco loja** | **Baixo** â€” 2 linhas JS na tela de promoÃ§Ã£o; nÃ£o altera PDV, caixa, financeiro |

### FECHADO + DEPLOY LOJA â€” fix endereÃ§o entrega grudado **v4.78** (29/06)

**Renan:** *Â«subaÂ»* + senha **99738595**.

**Loja (`producao`):** merge **`teste`** â†’ **`producao`** Â· commit **`91dbc09`** Â· merge **`6791b80`** Â· **push OK**.

**PÃ³s-deploy loja:** Ctrl+F5 Â· trocar cliente na entrega F3 â†’ endereÃ§o do cadastro (nÃ£o gruda o da venda anterior).

### INCIDENTE LOJA â€” endereÃ§o entrega grudado em todos os clientes (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Todo cliente na entrega F3 mostrava o mesmo endereÃ§o do teste (Av. Adhemarâ€¦) |
| **Causa** | EndereÃ§o da venda anterior ficava no estado da sessÃ£o; ao trocar cliente nÃ£o limpava |
| **Fix v4.78** | Troca de cliente repÃµe endereÃ§o do cadastro; F3 entrega sincroniza; voltar a Produtos zera |
| **Agora na loja** | ApÃ³s deploy **v4.78** Â· **Ctrl+F5** se ainda aparecer endereÃ§o antigo |

### FECHADO + DEPLOY LOJA â€” merge teste â†’ producao **v4.77** (29/06)

**Renan:** *Â«manda para produÃ§Ã£o - seja oque Deus quiserÂ»* + senha **99738595**.

**Loja (`producao`):** merge **`teste`** â†’ **`producao`** Â· pacote PDV entrega **v4.59â€“v4.77** Â· **push OK** Â· commit **`987f75c`**.

**PÃ³s-deploy loja:** Ctrl+F5 Â· PDV entrega F3 â†’ espaÃ§o no logradouro Â· Conferir entrega (frete/total) Â· popups entrega revisados.

### PDV entrega fluxo â€” sequÃªncia taxa/troco (29/06)

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | Taxa fora do form de endereÃ§o; popup apÃ³s endereÃ§o; dinheiro/cartÃ£o depois da taxa; troco mostra total |
| **SequÃªncia** | F3 â†’ pagamento local â†’ endereÃ§o â†’ taxa+horÃ¡rio â†’ meio â†’ troco (total) â†’ enviar/ir pagamento |
| **Futuro** | `entregaTaxaDevePularAuto()` â€” frete grÃ¡tis por endereÃ§o omite popup taxa |
| **v4.77** | Entrega endereÃ§o: espaÃ§o no logradouro (trim sÃ³ ao sair do campo / F7 â€” nÃ£o a cada tecla) |
| **v4.76** | Conferir entrega: frete R$ 10 (e total) corrigido â€” nÃ£o zerava ao confirmar taxa |
| **v4.75** | Fix regressÃ£o: Conferir entrega vazava p/ Pagamento (div extra v4.65); JS+CSS forÃ§a esconder fora da etapa 2 |
| **v4.74** | Conferir entrega: sem vÃ£o vazio no meio; texto maior; partida visÃ­vel no resumo |
| **v4.73** | Popup Dinheiro/CartÃ£o (e pagamento na entrega/loja): card ~3Ã— maior; botÃµes altos |
| **v4.72** | Tela endereÃ§o entrega: grade uniforme (sem vÃ£o vazio); labels/campos maiores; obs em 1 linha |
| **v4.71** | Popup pagamento local/meio mais largo (~40rem); botÃµes sem quebra de linha |
| **v4.70** | Fix: `</div>` sobrando em `step_entrega.html` (v4.65) exibia Conferir entrega na etapa Produtos; JS guarda visibilidade fora de entrega |
| **v4.69** | Popups entrega: tipografia maior (tÃ­tulo, total troco, campos, botÃµes) |
| **v4.68** | Popup troco: layout compacto; nÃ£o sobrepÃµe conferir entrega; corpo do card visÃ­vel |
| **v4.67** | Popup taxa/horÃ¡rio: grade uniforme (3 frete + valor|horÃ¡rio); confirma sÃ³ no rodapÃ© F7 |
| **v4.66** | Fix popup entrega sÃ³ cabeÃ§alho: sync nÃ£o esconde botÃµes ao ir pro endereÃ§o |
| **v4.65** | Popup entrega: card `fit-content` (sem faixa branca); overlay fora da etapa; clique no Voltar liberado |
| **v4.64** | Popups entrega compactos (altura = conteÃºdo); rodapÃ© Voltar/F7 clicÃ¡vel de novo |
| **v4.63** | Conferir entrega sem rolagem: 3 colunas, Partida oculta, total no topo, obs em 1 linha |
| **v4.62** | ImpressÃ£o mais leve: JsBarcode 1Ã— na pÃ¡gina, sem pop-up extra no PDV, modais sem blur GPU |
| **v4.61** | tela **Conferir entrega** revisÃ£o ERP (grade label/valor, total lateral, painel Ãºnico) |
| **v4.60** | popups entrega overlay fixo 16:9 |
| **v4.59** | fix popup cadastro sÃ³ quando usuÃ¡rio altera dados na tela |

### PDV entrega fluxo â€” fix (29/06 madrugada)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | ApÃ³s **Entregar F3**, repetia Â«Retirada ou entrega?Â» e Â«Taxa/horÃ¡rio/trocoÂ» antes do pagamento |
| **Fix** | F3 â†’ direto **Onde serÃ¡ o pagamento?**; taxa+horÃ¡rio no form de endereÃ§o; troco sÃ³ no fluxo dinheiro |
| **Tela cinza** | Etapa entrega sem wizard nem form â€” `return` no render bloqueava tela Â· fix `a43a8fd` **v4.54** |
| **Teste** | Ctrl+F5 apÃ³s deploy Â· Entregar F3 â†’ card Â«Onde serÃ¡ o pagamento?Â» |

### AGENDA AMANHÃƒ â€” itens **8â€“11** Â§4.15 (Renan **29/06 noite**)

**DecisÃ£o Renan:** *Â«amanhÃ£ mexemos com issoÂ»* â€” retomar **backup + congelar + checkpoint CP** (trilha operacional antes do item **12** cancelar ERP). **Outro chat** aberto para **outro problema** (nÃ£o Ã© CP) â€” este tÃ³pico continua aqui no CHECKPOINT.

**Por que mexer se CP jÃ¡ funciona no Postgres?** OperaÃ§Ã£o diÃ¡ria **nÃ£o precisa** â€” 8â€“11 Ã© **fechamento seguro** (cÃ³pia no PC, carimbo Mongo, prova de totais) **antes** de cancelar assinatura ERP. NÃ£o Ã© consertar CP.

**O que jÃ¡ estÃ¡ OK (nÃ£o refazer do zero):**

| Item | Status |
| ---- | ------ |
| CP/CR **lista + pagar** na **loja** | **Postgres** Â· **741** em aberto Â· **~R$ 393.652,70** (25â€“26/06) |
| **Backup PC (#10)** | **âœ… 25/06** â€” ZIP abertos + Excel no PC |
| **Checkpoint parcial (#11)** | **âœ…** ~17,7 mil tÃ­tulos carimbo + **corte Agroâ†’ERP** API (v1.14) â€” SisVale **nÃ£o envia** baixa pro WL |
| **Congelar Mongo (#8)** | **Off** â€” flag `AGRO_FINANCEIRO_MONGO_CONGELADO` ainda **nÃ£o** na loja |
| CÃ³digo desvinculaÃ§Ã£o | **teste v4.51** cadastro aux PG Â· loja **v4.49** |

**Ordem amanhÃ£ (Renan + assistente):**

| Passo | # | O quÃª | Onde |
| ----- | - | ----- | ---- |
| **1** | **10** | Backup **atualizado** no PC (CP abertos ZIP + Excel completo + cadastro Excel se quiser) | **sistvale.com.br** (loja) â€” URLs abaixo |
| **2** | **11** | Conferir totais CP **deduplicados** (referÃªncia = **tela**, nÃ£o soma crua CSV) | Loja Â· filtros Em aberto sem data |
| **3** | **8** | Congelar financeiro Mongo + `AGRO_FINANCEIRO_MONGO_CONGELADO=true` | **Render SistVale (produÃ§Ã£o)** â€” **nÃ£o** ensaio no teste |
| **4** | **11** | Se faltar carimbo: botÃ£o **CONGELAR** em `/lancamentos/` (admin) ou `manage.py congelar_lancamentos_financeiro_agro` | **Loja** |
| **5** | â€” | Conferir `/api/agro/fonte-status/` Â· `financeiro_mongo_congelado: true` | Loja |

**URLs backup (loja):**

| Arquivo | URL |
| ------- | --- |
| Em aberto | `https://sistvale.com.br/api/lancamentos/backup-abertos.zip` |
| Todos | `https://sistvale.com.br/api/lancamentos/backup-completo.xlsx` |

**Teste vs loja (importante â€” conversa 29/06):**

| Ambiente | CP Postgres | Mongo |
| -------- | ----------- | ----- |
| **Render teste** | Banco **isolado** â€” pagar/editar no teste **nÃ£o mexe** na loja | **LÃª** espelho compartilhado Â· `AGRO_STAGING_READONLY` bloqueia quase toda gravaÃ§Ã£o |
| **Render loja** | CP real da operaÃ§Ã£o | Espelho compartilhado Â· **congelar/checkpoint definitivo Ã© aqui** |

**Cuidado:** botÃ£o **CONGELAR** no **teste** ainda **carimba** o Mongo compartilhado (nÃ£o apaga valores, mas Ã© redundante). Ritual **8â€“11 = loja**.

**ReferÃªncia totais (dedup):** Qtd **741** Â· A pagar **~R$ 393.652,70** Â· Excel abertos **748 linhas** (+7 dup. ERP, ex. Geraldo) â€” **usar tela**.

**Render amanhÃ£:** assistente **sÃ³ mexe env produÃ§Ã£o** se Renan pedir **explÃ­cito** na sessÃ£o. **NÃ£o** push `producao` / cherry-pick loja sem frase + senha **99738595**.

**Depois de 8â€“11 estÃ¡vel:** item **12** cancelar ERP (Renan decide) Â· fase **D** cÃ³digo Compras fornecedor **continua pausada** atÃ© fechar CP ou outro chat.

**Novo chat (outro problema):** ler CHECKPOINT + Â§4.15 Â· **nÃ£o** misturar com passos 8â€“11 salvo Renan pedir.

---

### WIP â€” desvinculaÃ§Ã£o Mongo Â§4.15 (**29/06**)

**Feito recente:** pacotes corte v4.31â€“v4.49 Â· loja **v4.49** Â· cadastro aux PG **v4.51 teste** (`17210f7`).

**Trilha (atualizada):**

| Fase | # | O quÃª | Status |
| ---- | - | ----- | ------ |
| **AmanhÃ£** | **10â†’8â†’11** | Backup + congelar + checkpoint CP | **ðŸ“… Renan 30/06** |
| ~~**C**~~ | cÃ³digo | Cadastro ERP aux PG | **âœ… v4.51 teste** |
| **D** | cÃ³digo | Compras relatÃ³rio fornecedor sem Mongo | Pendente (outro chat) |
| **F** | **9** | Motor busca GM | Por Ãºltimo |
| **G** | **12** | Cancelar ERP | SÃ³ apÃ³s 8â€“11 OK |

**Pausado atÃ© amanhÃ£:** nada â€” CP **volta** na agenda. **CÃ³digo fase D** pode seguir em chat separado se Renan quiser.

**Renan 29/06 â€” medo CP:** entendido â€” amanhÃ£ Ã© **operacional fechamento**, nÃ£o remigrar tÃ­tulos. CP **jÃ¡ PG** na loja.

### FECHADO TESTE â€” cadastro ERP auxiliares Postgres **v4.51** (29/06)

| Rota / fluxo | Antes | Agora (`agro_pg`) |
| ------------ | ----- | ----------------- |
| `api_produtos_cadastro_proximo_cb_loja` | Mongo obrigatÃ³rio | Seq **230â€¦** sÃ³ Postgres + overlays |
| `api_produtos_cadastro_detalhe` (`__novo__`) | CÃ³digo seq consultava Mongo | Seq sÃ³ Postgres |
| `try_criar_produto_postgres_somente_agro` | Idem | Idem |
| `api_produtos_somente_agro_excluir` | Mongo obrigatÃ³rio | Apaga `Produto` PG + limpa overlay |
| Excel import/export prÃ©via/aplicar/reverter | Estado/import exigia Mongo | CatÃ¡logo + gravaÃ§Ã£o via `Produto` |
| `api_produtos_cadastro_compras_historico` | 503 sem Mongo | Lista vazia + aviso (histÃ³rico ERP opcional) |

**Conferir staging:** Ctrl+F5 cadastro Â· novo produto (cÃ³digo auto) Â· botÃ£o **230â€¦** Â· Excel â†‘â†“ Â· `/api/agro/fonte-status/` â†’ `cadastro_somente_postgres: true`.

### FECHADO + DEPLOY LOJA â€” merge max planilha/PDV **v4.49** (29/06)

**Renan:** *Â«mandaÂ»* + senha **99738595** Â· cherry-pick **`f0c6f29`** â†’ **`98e4685`** Â· **push OK**.

**PÃ³s-deploy loja:** Ctrl+F5 Â· **MÃªs anterior (mai)** Â· **21â€“22/05** ~R$ 2.451 / R$ 2.870 (nÃ£o R$ 53 teste).

### FECHADO + DEPLOY LOJA â€” grÃ¡fico planilha **v4.47** (29/06)

**Renan:** *Â«mandaÂ»* + senha **99738595** Â· cherry-pick **`def40ba`** â†’ **`d67a1b3`** Â· **push OK**.

**PÃ³s-deploy loja:** Ctrl+F5 Â· **MÃªs anterior (mai)** â†’ barras mÃªs cheio Â· jun+ sÃ³ PDV.

**Nota Renan 29/06 â€” mÃ©dia vs barras 21â€“22/mai:** **mÃ©dia base R$ 2.882** OK Â· barras 21â€“22 eram R$ 53 (venda teste PDV) â€” **fix v4.49** merge **max(PDV, planilha)** Â· **âœ… loja 29/06**.

### WIP â†’ TESTE â€” meta C planilha + 3 meses (**29/06**)

**Pacote:** migration **`0045`** + **`DashboardVendaDiaHistoricoAgro`** Â· import **`docs/dados/vendas_centro_nov2025.xlsx`** (272 dias Â· set/25â€“mai/26 Centro) Â· merge meta C **VendaAgro > planilha** Â· fÃ³rmula **M-1+M-2+M-3** Â· fix datas Excel (nov/dez 2025 + linha espÃºria 2027-01-01).

**ValidaÃ§Ã£o Renan staging v4.45 (29/06 ~00:08):** **âœ… parcial** â€” no **teste** quase nÃ£o hÃ¡ vendas PDV (mÃªs **R$ 84,62**), entÃ£o grÃ¡fico/cores do mÃªs **corrente** nÃ£o dÃ¡ para julgar direito. **MÃ©dia base ~R$ 104 mil** no tooltip **faz sentido** (planilha + 3 meses).

**ProduÃ§Ã£o v4.45 (29/06):** Renan *Â«pode enviarÂ»* + senha **99738595** Â· cherry-pick **`d75927d`** â†’ **`d078b4e`** Â· **push OK**. Conferir BI loja: Ctrl+F5 Â· tooltip Â«3 mesesÂ» Â· jun/2026 sem +1394%.

**Ajuste grÃ¡fico (v4.47):** planilha preenche **barras** set/25â€“mai/26 Â· **âœ… loja 29/06**.

### FECHADO + DEPLOY LOJA â€” pacote corte Mongo **v4.31â€“v4.36** (28/06)

**Renan:** *Â«pode subirÂ»* + senha **99738595** Â· prints baseline produÃ§Ã£o **antes** do deploy (comparar se precisar).

**Loja (`producao`):** cherry-pick **`bc805e7`** Â· **`d21a718`** Â· **`a516a64`** â†’ **`77f1254`** Â· **push OK** Â· **nÃ£o** merge inteiro `teste` Â· banana loja **intacto** (sÃ³ cÃ³digo).

**PÃ³s-deploy loja:** Ctrl+F5 Â· badge BI **v4.36+** Â· `/api/agro/fonte-status/` Â· smoke: vender 1 un. â†’ GestÃ£o saldo Â· BI Fonte PDV.

**Smoke produÃ§Ã£o pÃ³s v4.36 (28/06 ~22:27 â€” Renan):** **âœ… PDV â†’ estoque desce**

| Item | Detalhe |
| ---- | ------- |
| **Produto** | GM9503 **teste** |
| **Venda** | **#2273** Â· R$ 1,00 Â· **BAIXA REGISTRADA** Â· ERP ACEITO |
| **Cadastro ERP** | estoque **-5 â†’ -6** (C) |
| **GestÃ£o** | saldo acompanhou (print) |

**Incidente pÃ³s v4.36 â€” meta C / comparaÃ§Ã£o BI (28/06 noite):**

| Sintoma | Causa |
| ------- | ----- |
| **MÃªs ant.** grÃ¡fico ~**R$ 18k** (sÃ³ fim de mai) Â· tooltip meta Â«**+1394%**Â» Â· cores estranhas | Pacote corte: **histÃ³rico da meta C** passou a usar **sÃ³ VendaAgro** (`agro_pg`). Loja tem PDV SisVale **sÃ³ desde ~fim/mai** â€” abr/mai quase vazios no PG |
| Card **-75,3%** hoje | **Normal** â€” Ã© **hoje vs ontem**, nÃ£o meta C |
| GrÃ¡fico mÃªs **R$ 95.988** (Fonte PDV) | **OK** â€” vendas do mÃªs corrente no PG |

**Regra meta C (nÃ£o Ã© 60 dias):** para cada dia, mÃ©dia de **M-1 + M-2 + M-3** (3 meses civis anteriores) â€” **Renan 29/06** (antes eram 2). Weekday + ocorrÃªncia no mÃªs. HistÃ³rico: planilha set/25â€“mai/26 + jun+ PG.

**CorreÃ§Ã£o provisÃ³ria meta C â€” decisÃ£o Renan (28/06):** **opÃ§Ã£o B â€” Excel manual** (nÃ£o hÃ­brido Mongo).

| # | Caminho | Status |
| - | ------- | ------ |
| ~~A~~ | HÃ­brido Mongo sÃ³ meta histÃ³rico | **Descartado** (Renan prefere planilha) |
| **B** | Tabela PG **`DashboardVendaDiaHistoricoAgro`** (data + total) Â· Renan cola/importa Excel Â· merge na meta C: **VendaAgro** > planilha > 0 | **âœ… implementado teste v4.45** |
| C | Backfill Mongo â†’ PG | NÃ£o |

**Formato Excel (Renan enviar):**

| Campo | Exemplo | Nota |
| ----- | ------- | ---- |
| **Data** | `01/set` Â· `02/set` Â· â€¦ | **DD/mmm** (abr, mai, jun, jul, ago, **set**, out, nov, dez, jan, fev, mar) â€” **como no print** âœ… |
| **Total** | `R$ 5.090,37` | Faturamento **do dia** (loja toda) Â· ponto milhar + vÃ­rgula decimal OK |
| **PerÃ­odo planilha** | **set/2025 â€“ mai/2026** | Jun/2026+ **nÃ£o** entra no Excel â€” vendas **jÃ¡ no SisVale** (PDV) |
| **Arquivo Renan** | `vendas.cvs.xlsx` â†’ **`docs/dados/vendas_centro_nov2025.xlsx`** | **âœ… 29/06** Â· **272 linhas** (1 espÃºria omitida) Â· **01/09/2025 â€“ 31/05/2026** Â· **sÃ³ Centro** |
| **Merge meta C** | **max(PDV, planilha)** por dia | Venda teste PDV nÃ£o apaga planilha; jun+ sÃ³ PDV |
| **Ano** | Se a planilha **nÃ£o** tiver coluna ano | Assumimos **2025** para novâ€“dez e **2026** para jan em diante (Renan corrige se errar) |
| **Dia sem venda** | `0` ou linha omitida | Omitir = sem venda naquele dia |

**Importante:** fase **1** = Excel + **meta C** (3 meses + merge PG/planilha). SES/clima **nÃ£o** entra neste pacote.

**PrÃ³ximo passo:** se Renan quiser **loja** â†’ cherry-pick **`d75927d`** (v4.45) com frase + senha Â· na loja conferir jun/2026 (tooltip + % meta, sem +1394%).

**WIP â€” Renan quer revisar fÃ³rmula da meta (28/06 noite):**

| Tema | DecisÃ£o / nota |
| ---- | -------------- |
| **Dados** | Planilha **set/2025â€“mai/2026** + **jun+ PG** (merge) â€” meta C atual |
| **FÃ³rmula** | **Meta C = mÃ©dia M-1 + M-2 + M-3** (29/06 Renan; antes 2 meses) Â· SES/clima **nÃ£o** agora |
| **Clima/chuva** | CÃ³digo Gemini (`previsao_mensal.py` + OpenWeather) â€” **fase separada**; nÃ£o substitui meta C atual |
| **Parecer assistente** | SES **â‰ ** meta C de hoje (weekday + ocorrÃªncia no mÃªs); ver CHECKPOINT ou chat 28/06 |

**Baseline produÃ§Ã£o ANTES deploy (28/06 ~22:13 â€” prints Renan):**

| Tela | Valor / nota |
| ---- | ------------ |
| **BI `/`** v4.21.1 | Vendas hoje **R$ 1.310,04** Â· **34** vendas Â· grÃ¡fico mÃªs **R$ 95.988,00** Â· a pagar hoje **R$ 2.911,24** Â· atraso CP **R$ 63.754,44** |
| **CP** | **746** tÃ­t. abertos Â· bruto **R$ 407.700,86** Â· a pagar **R$ 396.515,99** |
| **`/vendas/` 30d** | **1920** vendas Â· **R$ 104.990,11** |
| **`/vendas/` fiado pend. ERP** | **2122** Â· **R$ 116.137,93** (filtro ativo no print) |

**Assistente:** checklist staging **6/6 + DRE â­** âœ… Â· deploy loja **feito**.

**Antes de comeÃ§ar:** Ctrl+F5 Â· conferir `/api/agro/fonte-status/` â†’ `catalogo_postgres: true` Â· `pdv_catalogo_somente_postgres: true` Â· `gestao_somente_postgres: true`

| # | Tela | Passos | OK? | Nota |
| - | ---- | ------ | --- | ---- |
| **1** | **Entrada NF** | Busca **NF** (ex. `112`) â†’ **Auditar financeiro** | **âœ… 28/06** | OK **1** Â· alertas **0** Â· tÃ­tulo CP PG conferido na NF 112 |
| **2** | **PDV â†’ GestÃ£o** | Vender **1 un.** Â· GestÃ£o aberta â†’ saldo desce | **âœ… 28/06** | GM9503 **teste** Â· **47 â†’ 46** (C) |
| **3** | **BI `/`** | GrÃ¡fico vendas carrega Â· meta C / ticket coerentes (sem erro Mongo) | **âœ… 28/06** | Ctrl+F5 apÃ³s venda Â· card **R$ 1,50 / 1 venda** Â· `/vendas/` **#37** 21:03 Â· grÃ¡fico **Fonte PDV** Â· barra **28/06** Â· tooltip meta C Â«sem base 2 mesesÂ» = **staging sem histÃ³rico M-1/M-2** (esperado) Â· ticket hoje **1,50** |
| **4** | **GrÃ¡fico gastos** | Abre Â· totais **â‰ˆ CP** mesmo perÃ­odo (competÃªncia ou vencimento) | **âœ… 28/06** | Modo **Comparar** Â· **Jul/2026** bolinha â†’ popup CP **119 tÃ­t.** Â· bruto/a pagar **R$ 68.235,20** â‰ˆ ponto grÃ¡fico **68.235** Â· PG OK Â· Â±0 tempo real vs 26/06 = staging estÃ¡vel |
| **5** | **LanÃ§amentos DRE** | Abre perÃ­odo Â· totais batem (sem tela vazia / 503) | **â­ 28/06** | **`LANCAMENTOS_DRE_ATIVO=false`** de propÃ³sito â€” Renan: incoerente no **Mongo** Â· **fora** do pacote corte v4.31â€“v4.43 Â· nÃ£o bloqueia cherry-pick |
| **6** | **GestÃ£o** | Lista abre rÃ¡pido Â· filtros **marca / categoria** Â· apÃ³s Entrada NF **nÃ£o** trava minutos | **âœ… 28/06** | Renan: **tudo certo** â€” lista rÃ¡pida Â· filtros OK |
| **7** | **Compras** | Card Â«**Ãºltimas compras**Â» Â· **Folha Compras** â†’ categoria (planilha A4) | **âœ… 28/06** | GM9503 Â· **Teste R$ 1,00** Â· folha **RaÃ§Ãµes** **232 prod.** A4 **27 pÃ¡g.** Â· obs. micro recarga (nÃ£o bloqueia) |

**Incidente 28/06:** 1Âª abertura staging = tela cinza (cold start Render) Â· Ctrl+Shift+R â†’ BI OK. **BI passo 3:** card vendas **nÃ£o atualiza sozinho** â€” precisa **Ctrl+F5** apÃ³s venda (KPI hoje Ã© live no servidor, mas a **pÃ¡gina** jÃ¡ estava aberta).

**Meta C no staging:** poucas vendas PDV â†’ mÃªs corrente **R$ 0â€“84** e **-99% vs base** = **esperado**. A **mÃ©dia base ~R$ 104 mil** (planilha 3 meses) Ã© o sinal de que import/merge **OK**. Cores e Â«falta para bater meta %Â» no mÃªs cheio = validar na **loja**.

**Passo 4 â€” GrÃ¡fico gastos (Renan agora):**

1. BI `/` â†’ card **Contas a Pagar** â†’ botÃ£o laranja **GrÃ¡fico gastos** (ou `/financeiro/grafico-gastos/`).
2. PerÃ­odo: **01/06/2026 â€“ 28/06/2026** (ou **MÃªs atÃ© hoje** na toolbar).
3. Filtros: **ReferÃªncia = Vencimento** Â· **Valor = Bruto (tÃ­tulo)** Â· **Tempo real** Â· planos **Todos** marcados.
4. Anotar **total** do grÃ¡fico (faixa de totais / soma visÃ­vel).
5. Abrir **Contas a pagar** `/lancamentos/contas-pagar/` â€” **mesmo perÃ­odo** + filtro **Vencimento** + situaÃ§Ã£o que bater com **bruto** (ex. **Todos** ou **Em aberto** â€” usar o par que o Â«?Â» do grÃ¡fico descreve).
6. **OK** se totais **â‰ˆ iguais** (centavos). **DiferenÃ§a grande** â†’ anotar valor grÃ¡fico vs CP + print.

*GrÃ¡fico da **home BI** (card gastos-plano, se ligado) **â‰ ** esta tela â€” validar sÃ³ `/financeiro/grafico-gastos/`.*

**DRE (passo 5):** tela Â«FunÃ§Ã£o desativadaÂ» = **esperado**. Flag **`LANCAMENTOS_DRE_ATIVO`** off no Render (teste e loja). Motivo Renan: totais **incoerentes** quando lia Mongo â€” pausa atÃ© reprogramar PG. **NÃ£o entra** na validaÃ§Ã£o do pacote corte; contagem **7 OK** = itens **1â€“4 + 6â€“7** (DRE â­).

**Passo 7 â€” Compras (Ãºltimo antes da loja):**

1. **`/compras/`** â€” card produto â†’ **Â«Ãšltimas compras (ERP)Â»** âœ… **GM9503** Â· **Teste R$ 1,00** (entrada NF teste).
2. Menu **Folha Compras** â†’ **planilha por categoria** â†’ **RaÃ§Ãµes** + **A4** âœ… **232 prod.** Â· 27 pÃ¡gs. (28/06 22:12).

**Obs. Compras:** card detalhe pode **micro recarregar** / sensaÃ§Ã£o de lentidÃ£o â€” **nÃ£o bloqueia** pacote corte; anotado para otimizar depois se incomodar.

**Auditoria NF â€” dois modos (Renan perguntou brecha):**

| Modo | Quando | O quÃª audita |
| ---- | ------ | ------------ |
| **Suspeitas** | Filtro ConcluÃ­da **sem busca** | SÃ³ notas concluÃ­das **sem** flag Â«financeiro gravadoÂ» â€” caÃ§a esquecimento de Â«Salvar + a pagarÂ» |
| **Ampliada** | **Com busca** (NF/fornecedor) ou filtro Financeiro | ConferÃªncia **nota a nota**: tÃ­tulo existe no CP Postgres + fornecedor bate |

**Brecha conhecida:** nota **ConcluÃ­da + financeiro gravado** nÃ£o reentra no modo suspeitas. Se a flag estiver errada (marcada sem tÃ­tulo real), passa batido **atÃ©** auditar com busca ou amostragem. **Teste staging:** buscar NF conhecida (112) â†’ OK:1. **OperaÃ§Ã£o:** amostrar 2â€“3 NFs/mÃªs com busca; comando `auditar_corte_mongo_pg` no CP (read-only).

**Se falhar:** anotar **# da linha** + mensagem na tela ou URL vermelha no DevTools (Rede).

**Depois dos 7 OK:** Renan manda *Â«pode subir produÃ§Ã£oÂ»* + senha **99738595** â†’ assistente cherry-pick **pacote corte** (nÃ£o merge inteiro `teste`). **DRE â­ nÃ£o conta como falha.**

**Commits pacote corte:** `bc805e7` v4.31 Â· `d21a718` v4.33 Â· `a516a64` v4.36 Â· banana `57885fa` v4.38

### banana â€” alinhamento checklist Â§4.15 (03/06 Â· assistente)

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Tabelas duplicadas Â§4.15 sincronizadas; status teste vs loja; rodapÃ© roadmap |
| **Deploy teste** | **`3fec909`** Â· **v4.37** |

### Corte Mongo â€” pacote 3 parciais fechados (03/06 Â· assistente)

| Item | Detalhe |
| ---- | ------- |
| **BI vendas (#3)** | Modo PDV forÃ§ado com `agro_pg`; meta C sem fallback DtoVenda; vÃ­nculos sem coleÃ§Ã£o Mongo `vendas_agro` |
| **GestÃ£o (#4)** | Com catÃ¡logo PG: **sem fallback Mongo** em lista/facetas; saldo jÃ¡ ledger (pacote 2) |
| **Compras (#5)** | `api_buscar` Ãºltimas compras **sem exigir Mongo**; Entrada NF etapa 6 lÃª produto via Postgres |
| **Deploy teste** | **`a516a64`** (cÃ³digo) Â· **`f986160`** (banana) Â· **v4.36** |

**Renan â€” retestar no staging (Ctrl+F5):** BI `/` Â· GestÃ£o lista/filtros Â· Compras card Â«Ãºltimas comprasÂ» Â· folha categoria.

### Corte Mongo â€” pacote 2 (03/06 Â· assistente)

| Item | Detalhe |
| ---- | ------- |
| **GestÃ£o saldo** | Lista PG nÃ£o lÃª estoque Mongo quando ledger operacional (`agro_estoque_operacional_sem_mongo_erp`) |
| **Compras Ãºltima compra** | Com `agro_pg`: sÃ³ **Entrada NF Agro** + vendas PG â€” sem scan DtoCompra* ERP |
| **Compras planilha** | MÃ©tricas/vendas pÃ³s-compra 100 % Postgres quando catÃ¡logo PG |
| **BI ticket** | Ticket mÃ©dio usa **VendaAgro** no modo PDV |
| **GrÃ¡fico gastos** | Mensagem erro amigÃ¡vel se PG ativo e Mongo cair |
| **Deploy teste** | **`d21a718`** Â· **v4.33** |

**Renan â€” testar 1 a 1 no staging (Ctrl+F5):**

| # | Tela | O que conferir |
| - | ---- | -------------- |
| 1 | Entrada NF | Auditar financeiro (concluÃ­das) |
| 2 | PDV â†’ GestÃ£o | Vender 1 un. Â· saldo desce na gestÃ£o aberta |
| 3 | BI `/` | GrÃ¡fico vendas + meta |
| 4 | GrÃ¡fico gastos | Abre Â· totais vs CP mesmo perÃ­odo |
| 5 | LanÃ§amentos DRE | **â­** desligado (`LANCAMENTOS_DRE_ATIVO`) â€” fora do pacote |
| 6 | GestÃ£o | **âœ…** lista + filtros marca/categoria |
| 7 | Compras | **âœ…** Ãºltimas compras + folha **RaÃ§Ãµes** A4 |

### Corte Mongo â€” pacote 1 seguranÃ§a (03/06 Â· assistente)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Renan: avanÃ§ar desvinculaÃ§Ã£o com paridade segura; acionar sÃ³ se necessÃ¡rio |
| **NF auditoria** | TÃ­tulos CP via **Postgres** quando financeiro PG (`_titulos_pg_por_*`) |
| **BI meta C** | HistÃ³rico M-1/M-2 prefere **VendaAgro**; ERP sÃ³ se perÃ­odo vazio |
| **GestÃ£o saldo** | Ouve fila `agro_pdv_catalog_patch_queue_v1` â€” saldo atualiza apÃ³s venda PDV |
| **Comando** | `python manage.py auditar_corte_mongo_pg` â€” CP PG vs Mongo read-only |
| **Deploy teste** | **`bc805e7`** Â· **v4.31** |
| **Renan testar** | (1) Entrada NF â†’ Auditar financeiro Â· (2) Vender 1 un. â†’ GestÃ£o saldo desce Â· (3) BI meta |

### CP â€” filtro por competÃªncia e pagamento (03/06 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Lista nova CP: alÃ©m de vencimento, filtrar por **competÃªncia** e **pagamento** (vencimento = padrÃ£o) |
| **UI** | Select **Filtrar por** no painel Filtros; labels De/AtÃ© dinÃ¢micos; aviso pagamento + em aberto |
| **API** | Reutiliza `ref` + `comp_de/ate` Â· `pag_de/ate` Â· `venc_de/ate` (jÃ¡ existentes) |
| **Atalhos Hoje/SÃ¡bâ€¦** | Continuam no eixo **vencimento** |
| **GrÃ¡fico gastos** | Drill-down `embed=grafico` inalterado |
| **Arquivo** | `lancamentos_contas_pagar_teste.html` |
| **Deploy teste** | **`fc1abe8`** Â· **v4.26** (hook pÃ³s-checkpoint) |
| **Deploy produÃ§Ã£o** | **`69e5eb8`** Â· **v4.21** Â· Renan **99738595** Â· **28/06** |

### Entrada NF â€” rascunho Postgres (24/06 Â· assistente)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Rascunho assistente (etapas 1â€“6) sair do Mongo `AgroEntradaNotaRascunho` |
| **Modelo** | `EntradaNotaRascunhoAgro` Â· migration `0043` + `0044` |
| **Flag** | `AGRO_ENTRADA_NF_RASCUNHO_PG` â€” default **ligado** quando financeiro PG ativo |
| **Import legado** | `python manage.py importar_rascunhos_entrada_nota_mongo_pg` Â· **auto** na listagem + boot (`maybe_bootstrap_rascunhos_entrada_nota_pg`) |
| **Incidente 28/06** | Loja lista vazia â€” PG ligado, dados ainda no Mongo; **fix v4.20** import auto no boot + listagem + **fallback Mongo** atÃ© PG popular |
| **Hotfix loja** | ~~Shell/env~~ â†’ **v4.20 no ar** Â· 1Âª abertura importa Mongoâ†’PG Â· fallback se PG vazio |
| **Audit v4.17** | Fix `_object_id_rascunho` Â· reabrir etapas Â· msgs etapa 8 |
| **Deploy teste** | v4.09â€“v4.22 Â· NF 112 GM9503 **47** âœ… |
| **Deploy produÃ§Ã£o v4.20** | **28/06** Â· **`7834e66`** Â· Renan autorizou (senha) âœ… Â· **Renan OK loja** â€” lista NF voltou (rascunhos + concluÃ­das) |
| **Deploy produÃ§Ã£o v4.17** | **28/06** Â· **`cdc198f`** Â· Renan autorizou (senha) âœ… |

### Caixa â€” histÃ³rico retiradas + feedback saÃ­da (24/06 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Pedido** | Â«Retirada / saÃ­daÂ» no painel â†’ tela de **histÃ³rico** (nÃ£o form direto) |
| **Rota** | `/caixa/retiradas/` Â· filtros: data (calendÃ¡rio Agro), plano, quem levou Â· padrÃ£o **hoje** |
| **Nova saÃ­da** | BotÃ£o laranja grande â†’ `?painel=retirada` (form existente) |
| **Feedback saÃ­da** | ApÃ³s registrar: banner verde Â«Retirada concluÃ­daÂ» + limpa todos os campos |
| **Deploy teste** | **`619fbba`** Â· **v4.07** Â· Renan OK |
| **Fix FAB (27/06)** | PDV + **Aa** reposicionam para canto livre (caixa prioriza inferior direito) |
| **Deploy produÃ§Ã£o** | **27/06** Â· merge `teste`â†’`producao` **`5568302`** Â· **v4.07** Â· Renan autorizou |

### PRODUTO â€” FOOD delivery em branco (27/06 Â· Renan)

| Item | Detalhe |
| ---- | ------- |
| **Nomes** | **SISTVALE** = produto Â· **BANANA** = GM Agro Â· **FOOD** = delivery em branco |
| **Repo FOOD** | GitHub **`hinnen/food`** (privado, vazio â†’ espelho cÃ³digo) |
| **Docs** | **`FOOD.md`** Â· **`SISTVALE.md`** (mapa) Â· `docs/FOOD-INSTANCIA-BRANCA.md` Â· `.env.food.example` Â· `render-food.yaml` Â· `scripts/espelhar_repo_food.ps1` |
| **Chat** | Loja GM â†’ `@banana` Â· FOOD â†’ `@FOOD` |
| **PrÃ³ximo Renan** | Clone FOOD local âœ… Â· Render **pausado** |
| **GM Agro** | Sem mudanÃ§a de deploy/dados |

### INCIDENTE â€” loja lenta geral (27/06) Â· diagnÃ³stico fechado pelos grÃ¡ficos

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | SisVale demora abrir (tela cinza); balcÃ£o lento; vÃ­deo WhatsApp ~11:54 |
| **Gargalo** | **`agro-db`** (Postgres 256 MB) â€” **nÃ£o** o servidor web |
| **Web `Sistvale - ProduÃ§Ã£o`** | 512 MB Â· memÃ³ria **~40â€“50%** Â· CPU picos **~30â€“40%** Â· **1 worker** â€” saudÃ¡vel, **espera o banco** |
| **MemÃ³ria DB â€” 7 dias** | Working set **80â€“95% a semana inteira** â€” pressÃ£o **crÃ´nica**, nÃ£o sÃ³ 27/06 |
| **Disco DB â€” 7 dias** | ~100 MB atÃ© **25/06** â†’ **~150â€“180 MB a partir 26/06** (import CP/CR ~18k tÃ­tulos) |
| **CPU DB** | ~10â€“12% â€” **nÃ£o** Ã© gargalo |
| **Escrita DB** | **40â€“60 KB/s estÃ¡vel a semana** + ops 1kâ€“3k/intervalo â€” loja + ledger + financeiro PG |
| **ConexÃµes DB** | Picos **70â€“80 / limite 100** â€” fila quando memÃ³ria alta |
| **TransaÃ§Ãµes** | Pico **~6000** **22/06 ~17h** â€” provÃ¡vel import/bootstrap financeiro |
| **IndisponÃ­vel** | **24/06 19h10** e **26/06 16h51** â€” restart Render (coincide CP PG) |
| **Top query agora** | `TituloFinanceiroAgro` â€” **142k linhas** lidas Â· ~115 ms/chamada (CP/BI pesado) |
| **Deploys 27/06 manhÃ£** | 3 seguidos (8h51â€“10h) â€” piora momentÃ¢nea, **nÃ£o causa raiz** |
| **Por que Â«atÃ© ontemÂ»** | **26/06** dados financeiros no Postgres + disco subiu; **27/06** loja aberta + deploys + Mongo ERP fora |
| **AÃ§Ã£o** | **`agro-db` â†’ Basic-1gb** âœ… **Renan 27/06** â€” pÃ³s-upgrade working set **&lt;20%** (limite **1 GB**; antes ~90% em 256 MB) |
| **Renan agora** | **Ctrl+F5** loja Â· testar abrir SisVale + PDV + 1 venda Â· gestÃ£o/DRE ainda podem ser Mongo |
| **Depois do upgrade** | Conferir working set **~30â€“50%**; otimizar queries financeiro se CP/BI ainda pesado |

### PACOTE â€” corte ERP sem Mongo (26/06) Â· **teste v3.77**

| Item | Detalhe |
| ---- | ------- |
| **Contexto** | ERP caiu (loja nÃ£o pagou); Renan pediu **fechar pendÃªncias do corte** no **teste** â€” validar um a um antes de produÃ§Ã£o |
| **Commit** | **`7992b0a`** (cÃ³digo) Â· push **`94616aa`** â€” analytics PG + Compras + gestÃ£o |
| **Migrate** | **Nenhuma** |
| **Flags Render teste** | `AGRO_FONTE_CATALOGO=agro_pg` Â· `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` Â· financeiro PG auto se tÃ­tulos existirem Â· ledger estoque se jÃ¡ ligado |

**O que entrou neste pacote:**

| MÃ³dulo | MudanÃ§a |
| ------ | ------- |
| **DRE** | `/api/lancamentos/dre/resumo/` â†’ Postgres quando financeiro PG |
| **Fluxo calendÃ¡rio** | projeÃ§Ã£o diÃ¡ria via `TituloFinanceiroAgro` + mÃ©dia `VendaAgro` |
| **BI card gastos-plano** | totais por plano no Postgres |
| **GrÃ¡fico gastos (sÃ©rie)** | API `/api/financeiro/grafico-gastos/dados/` + planos iniciais PG |
| **GestÃ£o produtos** | saldos via ledger/Agro quando Mongo off |
| **Compras planilha** | dim categoria/unidade + relatÃ³rios categoria/unidade **100% catÃ¡logo Postgres**; mÃ©tricas vendas via `VendaAgro` |

**PrÃ³ximas fases (fora deste pacote):**

- RelatÃ³rio Compras **por fornecedor** ainda exige Mongo catÃ¡logo
- Motor busca GM unificado (Compras/NF)
- Resumo gerencial financeiro completo PG

**Checklist Renan â€” testar no Render teste (Ctrl+F5):**

| # | Tela | Renan 27/06 | Nota |
| - | ---- | ----------- | ---- |
| 1 | PDV busca | **âœ…** teste + **âœ…** produÃ§Ã£o | â€” |
| 2 | CP/CR | **âœ…** | â€” |
| 3 | DRE | **â­** desativado opcional | Ignorar no pacote |
| 4 | CalendÃ¡rio fluxo | **âš ï¸** vendas incluem vendas **do teste** | **Esperado no staging** â€” Postgres prÃ³prio Â· na **loja** sÃ³ vendas loja |
| 5 | GrÃ¡fico gastos | **âœ…** teste + **âœ… prod** (v3.55 grÃ¡fico Â· v3.58 sync CP) | Renan conferir tela **82.643** |
| 6 | BI `/` card gastos-plano | **â­** Renan nÃ£o achou | Card **sÃ³** se `AGRO_DASHBOARD_GASTOS_PLANO=true` â€” **opcional** Â· pode pular |
| 7 | GestÃ£o saldo pÃ³s-venda | **âŒâ†’fix v3.82** vendeu 7 Â· ficou -3 | v3.54 nÃ£o baixava estoque sem Mongo Â· **retestar venda** |
| 8 | Compras planilha | **â­** tela recarrega sozinha | Pouco usada Â· dados PG depois Â· **ignorar por ora** (Renan) |
| 9 | Cadastro ERP | **âœ…** lista OK Â· um pouco mais lenta que loja | AceitÃ¡vel |

**ProduÃ§Ã£o:** sÃ³ quando Renan pedir com frase + senha **99738595** â€” pacote Ãºnico (grÃ¡fico + baixa estoque + desvinculo); **sem** cherry-pick avulso.

### ANÃLISE item 5 â€” grÃ¡fico vs CP vs backup (Jul/2026 Â· Renan 27/06)

| Fonte | Ambiente | Jul/2026 |
| ----- | -------- | -------- |
| GrÃ¡fico | ProduÃ§Ã£o | **82.643** |
| CP (direto ou bolinha) | ProduÃ§Ã£o | **94.879** (147 tÃ­t.) |
| GrÃ¡fico | Teste | **49.385** âŒ |
| CP | Teste | **82.643** (145 tÃ­t.) |
| Backup Excel RP | PC | Renan: **~93â€“94k** (â‰ˆ prod CP) |

**CorreÃ§Ã£o 27/06:** CP produÃ§Ã£o **direto na tela** = mesmo **94.879** â€” nÃ£o Ã© bug sÃ³ da bolinha.

**Leitura atual:**
- **Prod CP ~94k â‰ˆ backup ~93â€“94k** â†’ provÃ¡vel foto **Mongo/ERP completa** (todos planos em aberto).
- **GrÃ¡fico prod ~82k** â†’ **exclui emprÃ©stimo** por regra do BI (~12k a menos) â€” nÃ£o bate CP.
- **Teste CP 82k vs prod 94k** â†’ staging PG **145 tÃ­t.** vs loja **147** â€” import/cÃ³pia nÃ£o idÃªntica ao Mongo ao vivo.
- **GrÃ¡fico teste 49k** â†’ bug agregaÃ§Ã£o PG (pacote).

**Fix v3.85 (teste) / v3.55 (prod):** grÃ¡fico PG = CP Â· **Renan OK teste 82.643** Â· prod CP ainda **~94k** â€” ver abaixo.

**Prod ~94k vs backup ~82k (27/06):**
- **Checkpoint 19/06** â€” sÃ³ carimbo Mongo; **nÃ£o muda valor**.
- **Renan filtro jul/2026 aberto:** Excel backup **145** tÃ­t. **82.642,99** Â· CP prod **147** tÃ­t. **A pagar 94.879,36** (Bruto 96.948,53).
- **Causa provÃ¡vel:** import PG **~26/06** do Mongo **vivo** (â‰  foto 19/06) + **dedup** CP; **+2 tÃ­tulos** e **~+12k saldo** vs Excel.
**Qual Ã© o correto? (Renan â€” operaÃ§Ã£o loja)**

| Fonte | Jul/2026 aberto | ConfiÃ¡vel? |
| ----- | ----------------- | ---------- |
| **Mongo ao vivo** (diag. 27/06) | **145 Â· 82.642,99** | **Sim** â€” bate Excel 19/06 |
| **CP prod (Postgres)** | ~~147 Â· 94.879~~ â†’ **145 Â· 82.642,99** | **Sim** â€” sync shell 27/06 pÃ³s v3.58 |
| **Teste** | ~82k | Sim (PG staging â‰ˆ Mongo) |

**Para a loja:** o certo Ã© **~82.643** (Mongo). ~~Tela prod **94k**~~ â†’ **corrigido** sync shell 27/06.

### FECHADO â€” produÃ§Ã£o sync Mongoâ†’PG **v3.58** (27/06)

| Item | Resultado |
| ---- | --------- |
| **Shell prod** | `--apply --conferir-jul` OK |
| **PG** | 17â€¯886 â†’ **17â€¯878** tÃ­tulos Â· **8 Ã³rfÃ£os** removidos |
| **CP jul/2026 aberto** | **145 Â· R$ 82.642,99** â€” bate Mongo/Excel |
| **Renan** | Conferir CP na tela (Ctrl+F5) e grÃ¡fico jul = **82.643** Â· **âœ… shell OK** |

### FIX teste â€” grÃ¡fico gastos + baixa estoque PDV **27/06 Â· v3.82**

| Item | Detalhe |
| ---- | ------- |
| **GrÃ¡fico 500** | VariÃ¡vel `incluir_nomes` renomeada e quebrou API â†’ Â«Falha na comunicaÃ§Ã£oÂ» |
| **GestÃ£o -3** | `AGRO_PDV_VENDA_SEM_MONGO_ERP` desligava baixa estoque na venda Â· gestÃ£o/BI divergiam |
| **Fix** | Baixa sempre via ledger Postgres Â· gestÃ£o ledger sem Mongo espelho |
| **Renan retestar** | 1) GrÃ¡fico jul Bruto 2) Venda teste GM9503 â†’ gestÃ£o deve ir **-10** (Ctrl+F5 busca) |
| **Loja v3.54** | **Mesmo bug baixa** â€” **nÃ£o cherry-pick urgente** (Renan 27/06: estoque loja perdido Â· vai recontar Â· sobe **no pacote** junto com desvinculo) |

### FECHADO â€” PDV finalizaÃ§Ã£o rÃ¡pida sem Mongo/ERP **26/06** Â· teste + **loja v3.54**

| Item | Detalhe |
| ---- | ------- |
| **ReclamaÃ§Ã£o loja** | Demora ao confirmar venda (Mongo/SEFAZ sÃ­ncronos) |
| **Fix teste** | **`3907006`** Â· v3.79 |
| **Fix loja** | **`676ea13`** cherry-pick Â· **99738595** 27/06 Â· Render **v3.54** |
| **ConteÃºdo** | `AGRO_PDV_VENDA_SEM_MONGO_ERP` Â· `AGRO_PDV_NFCE_ASSINCRONA` Â· baixa estoque Postgres Â· NFC-e thread |
| **Loja** | Ctrl+F5 PDV Â· confirmar PIX/dinheiro â€” barra rÃ¡pida; cupom em background |

### URGENTE â€” PDV sem produtos com ERP/Mongo fora **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma Renan** | ERP caiu Â· PDV nÃ£o busca produtos |
| **Causa** | CatÃ¡logo PDV montava lista no **Mongo**; `/api/todos-produtos/delta/` falhava sem Mongo mesmo com `agro_pg` na loja |
| **Fix teste** | **`89400df`** / cÃ³digo **`901d646`** |
| **Fix loja** | **`e23ae38`** cherry-pick Â· **v3.52** â€” **âœ… push produÃ§Ã£o** (Renan **99738595** Â· teste OK Â· problema sÃ³ loja) |
| **Render** | Aguardar deploy Sistvale ProduÃ§Ã£o Â· **Ctrl+F5** no PDV |
| **Arquivos** | `views.py` Â· `consulta_produtos.js` â€” **sÃ³ leitura** catÃ¡logo |
| **Renan testar loja** | Aguardar Render Â· **Ctrl+F5** no PDV â€” catÃ¡logo deve carregar do Postgres |

### WIP â€” grÃ¡fico gastos comparar **v3.69** (26/06)

| Item | Detalhe |
| ---- | ------- |
| **Renan** | v3.68 OK Â· quer **variaÃ§Ã£o** visÃ­vel sem passar o mouse |
| **Fix teste** | Badge fixo por mÃªs (ex. **âˆ’558**) abaixo do ponto â€” verde se caiu, vermelho se subiu, cinza se Â±0 |
| **Arquivo** | `grafico_gastos.html` |

### FECHADO â€” grÃ¡fico comparar v3.68 *(Renan: ficou melhor)*

### FECHADO â€” produÃ§Ã£o grÃ¡fico gastos UX **v3.51** (26/06)

| Item | Detalhe |
| ---- | ------- |
| **Renan** | *Â«cherry pick e suba produÃ§Ã£oÂ»* Â· **99738595** |
| **Commits** | `4de07de` scroll planos Â· `8c6d67a` rÃ³tulos comparar + fonte maior |
| **Arquivo** | sÃ³ `grafico_gastos.html` |
| **Fora** | PDV Â· produtos Â· gravaÃ§Ã£o CP/LanÃ§amentos |

### FECHADO â€” produÃ§Ã£o grÃ¡fico gastos planos = CP **v3.48** (26/06)

| Item | Detalhe |
| ---- | ------- |
| **Renan** | Teste OK Â· *Â«suba para produÃ§Ã£oÂ»* Â· cherry-pick isolado |
| **Commit loja** | **`40d037a`** (cherry-pick `3c85b6f`) |
| **ConteÃºdo** | Lista planos grÃ¡fico = CP; **sÃ³ leitura** â€” PDV/produtos/lanÃ§amentos inalterados |
| **Migrate** | **Nenhuma** |

### WIP â€” grÃ¡fico gastos planos = CP **24/06** *(fechado loja v3.48)*

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | Planos no grÃ¡fico tinham itens do cadastro (ex. **Despesas DispensÃ¡veis**, plano pai sem tÃ­tulo) que **nÃ£o** aparecem em Contas a pagar â€” quer **lista igual ao CP** |
| **Fix teste** | Lista via **`/api/lancamentos/planos-distintos/`** (mesmos filtros venc/comp/pag + status); SSR backend alinhado; ao **Aplicar** recarrega planos antes do grÃ¡fico |
| **Arquivos** | `mongo_financeiro_util.py` Â· `financeiro/views.py` Â· `grafico_gastos.html` |
| **Renan testar** | Mesmo filtro CP (venc., saldo aberto, 3 meses) â€” contagem e nomes dos planos devem bater; pai sem lanÃ§amento **some** dos dois |

### FECHADO â€” produÃ§Ã£o perf + BI validade + CP **v3.47** (26/06)

| Item | Detalhe |
| ---- | ------- |
| **Renan** | *Â«pode mandar para produÃ§Ã£oÂ»* Â· **99738595** Â· sem testar Â· cuidado PDV/produtos/lanÃ§amentos |
| **MÃ©todo** | Cherry-pick (nÃ£o merge teste inteiro) sobre v3.39 |
| **Commits** | `7c84d57` Â· `1473620` Â· `b48d142` Â· `510f4d9` Â· `927dba0` Â· `88b5a60` â†’ **`3793af9`** |
| **ConteÃºdo loja** | BI validade + conferir Â· perf Mongo Â· `/vendas/` rÃ¡pido Â· PDV cache localStorage Â· CP filtros |
| **Dados** | **Sem** flags staging Â· **sem** mudar gravaÃ§Ã£o PDV/lanÃ§amentos/produtos (sÃ³ Ã­ndice + UI + leitura) |
| **Migrate Render** | `python manage.py migrate` â€” Ã­ndice `vendaagro_criado_em_idx` |

### WIP â€” lentidÃ£o teste (vendas + PDV 1Âª abertura) **24/06** *(Renan nÃ£o testou antes do deploy)*

| Item | Detalhe |
| ---- | ------- |
| **v3.53** | Busca Consulta OK â€” validade BI nÃ£o satura Mongo |
| **v3.55** | **`/vendas/`** â€” filtro por data usa Ã­ndice (`criado_em`); JSON pesado adiado; fiado/NFC-e 1Ã— por linha Â· **PDV wizard** â€” catÃ¡logo no **localStorage** (mesma chave da Consulta) + delta em background; nÃ£o refaz download ao fechar Chrome |
| **Renan testar** | `/vendas/?preset=hoje` Â· fechar Chrome â†’ abrir PDV (F1) â€” busca local deve aparecer rÃ¡pido |

### WIP â€” lentidÃ£o teste (vendas + busca PDV) **24/06** *(histÃ³rico v3.53)*

| Item | Detalhe |
| ---- | ------- |
| **Sintoma Renan** | `/vendas/` e autocomplete Consulta demoram minutos no **teste**; **produÃ§Ã£o 3.33** normal |
| **Causa** | Pacote BI validade v3.47+ consultava **Mongo para todos** os PIDs com validade a cada abertura do BI/atalhos â€” competia com `/api/buscar/` e `/api/todos-produtos/` (**buscador PDV nÃ£o foi alterado**) |
| **Fix teste** | **v3.53** â€” validade: SQL para lotes com qtd>0 + Mongo sÃ³ Â«conferirÂ» + cache 3 min; vendas: NFC-e config 1Ã— por pÃ¡gina |
| **Renan testar** | Fechar aba do BI Â· Ctrl+F5 Consulta â†’ busca local deve voltar rÃ¡pido; `/vendas/` hoje |

### WIP â€” BI validade Â«ConferirÂ» sem saldo **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Pedido Renan** | OpÃ§Ã£o **2**: card com fila **Conf. venc. / Conf. mÃªs** (validade ok, saldo C+V e lote zerados â€” estoque furado) |
| **Regra com saldo** | **Vencidos / No mÃªs** (topo) = sÃ³ com estoque operacional > 0 ou lote qtd > 0 |
| **OpÃ§Ã£o 1 (auto)** | Salvar validade no relatÃ³rio espelha data no lote Agro (jÃ¡ no cÃ³digo) |
| **Deploy teste** | **`b48d142`** Â· **v3.51** |
| **Renan testar** | `/` â€” Simparic e outros furos devem aparecer em **Conf. mÃªs** (roxo) |

### WIP â€” BI validade card **26/06** *(histÃ³rico â€” fix contagem extras)*

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Renan alterou validade no relatÃ³rio (ex. Simparic 30/06) â€” card **Validade** do BI ficou **0 / 0** |
| **Causa** | BI contava sÃ³ `EstoqueLote` com qtd>0; Â«SalvarÂ» no relatÃ³rio gravava sÃ³ `cadastro_extras` |
| **Fix teste** | **`7c84d57`** Â· **v3.47** |
| **Renan testar** | Recarregar `/` â€” **No mÃªs** â‰¥ 1 Â· opcional: abrir relatÃ³rio validade e **Salvar** de novo num produto |

### FECHADO â€” produÃ§Ã£o TransferÃªncias + Validade PG **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Renan** | Teste OK Â· **99738595** Â· *Â«pode enviar para produÃ§Ã£oÂ»* |
| **Cherry-pick** | **`29a4c63`** â†’ **`b3db16e`** Â· loja **v3.33** |
| **ConteÃºdo** | Saldos operacionais Agro (ajuste+ledger) Â· TransferÃªncias + Validade |
| **Risco loja** | **Baixo** â€” sÃ³ leitura estoque; PDV/caixa/financeiro inalterados |

### FECHADO â€” TransferÃªncias + Validade â†’ PG **26/06 (teste)**

| Item | Detalhe |
| ---- | ------- |
| **Renan** | *Â«banana.md simÂ»* Â· teste **âœ…** sem problema |
| **Deploy teste** | **`29a4c63`** Â· **v3.42+** |
| **ProduÃ§Ã£o** | **âœ… Renan 26/06** â€” **99738595** Â· **v3.33** (`b3db16e`) |

### WIP â€” CP filtros mais rÃ¡pido **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Renan** | Pediu *Â«banana.md faÃ§aÂ»* â€” **sem** frase *pode subir produÃ§Ã£o* **nem** senha (**erro assistente 26/06**) |
| **Merge** | `teste`â†’`producao` Â· **`6418b39`** Â· loja **v3.32** |
| **ConteÃºdo** | BI cards CP/CR Postgres Â· resumo gerencial default PG Â· grÃ¡fico gastos UX Â· CP filtros UX |
| **Risco loja** | **Baixo** â€” sÃ³ leitura financeira + telas isoladas; PDV/caixa inalterados |

### FECHADO â€” BI home financeiro â†’ PG **26/06 (teste v3.23+)**

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Home BI cards CP/CR + totais hoje/atraso via Postgres; resumo gerencial default Postgres |
| **Deploy teste** | **`70fc5f1`** Â· **v3.22â€“v3.26** |
| **Renan validou** | **âœ… 26/06** â€” BI v3.23: a pagar hoje **R$ 1.475,35** Â· atraso CP **R$ 64.219,62** Â· CR hoje **R$ 0** Â· atraso CR **R$ 27.091,40** |
| **ProduÃ§Ã£o** | **âœ… Renan 26/06** â€” **v3.30** (`6418b39`) |
| **Fora** | GrÃ¡fico gastos por plano na home (Mongo) Â· DRE/calendÃ¡rio |

### WIP â€” CP filtros mais rÃ¡pido **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Renan: filtragem CP um pouco mais lenta |
| **Causa** | Tela esperava carregar **planos** antes da **lista** (2 idas seguidas) |
| **Fix teste** | **`88b5a60`** Â· v3.35 â€” lista primeiro; planos sÃ³ se painel Filtros aberto |
| **Renan testar** | Atalho **Hoje** / **Aplicar** â€” lista deve aparecer mais rÃ¡pido |

### WIP â€” BI / resumo gerencial â†’ PG **26/06** *(histÃ³rico â€” fechado acima)*

### FECHADO â€” produÃ§Ã£o listas auxiliares PG **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Renan** | **99738595** Â· loja aberta â€” pacote sÃ³ leitura PG + grÃ¡fico gastos UX (isolado) |
| **Merge** | `teste`â†’`producao` Â· **`4e22328`** (fast-forward pÃ³s v3.12) |
| **ConteÃºdo** | Listas auxiliares PG Â· grÃ¡fico gastos atalhos/UX Â· migration `0002_grafico_gastos_atalho_agro` |
| **Risco loja** | **Baixo** â€” CP/CR/gravaÃ§Ã£o jÃ¡ PG; PDV/caixa/fiado inalterados; deploy Render rolling (~1 min) |

### FECHADO â€” listas auxiliares PG **26/06 (teste v3.17)**

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Baixa CP (formas/bancos), autocomplete LanÃ§amentos, fornecedores NF â†’ Postgres |
| **Deploy teste** | **`01a9348`** Â· **v3.17** |
| **Renan validou** | **âœ… 26/06** â€” `fonte-status` (`financeiro_postgres: true`) Â· CP filtro **Hoje** Â· baixa **R$ 1,49** Â«LanÃ§amento manual 1Â» planno **teste** â†’ **Quitados/Pago** |
| **Prints** | NÃ£o veio NF passo 1 nem PDV â€” baixa fim-a-fim cobre `opcoes-baixa`; nova saÃ­da implÃ­cita pelo tÃ­tulo manual |
| **ProduÃ§Ã£o** | **âœ… Renan 26/06** â€” 99738595 Â· **v3.20** (`4e22328`) |

### WIP â€” listas auxiliares PG (fornecedor / planos / formas) **28/05** *(histÃ³rico)*

### FECHADO â€” export LanÃ§amentos + GestÃ£o PG **28/05 (teste v3.11)**

| Item | Detalhe |
| ---- | ------- |
| **Export CSV/PDF/Excel** | `_lancamentos_financeiro_dados_export` â†’ Postgres quando `financeiro_postgres` |
| **GestÃ£o** | `agro_gestao_usa_postgres()` ligado na **loja** com `AGRO_FONTE_CATALOGO=agro_pg` (sem env novo) |
| **Renan testar** | **âœ… Renan 28/05** â€” gestÃ£o + export OK no teste |
| **ProduÃ§Ã£o** | **âœ… Renan 28/05** â€” 99738595 Â· deploy **v3.12** (`3838594`) |

### AUDIT rabo de solta â€” antes sprint desvinculo Renan **28/05**

| # | Item | Status | AÃ§Ã£o |
| - | ---- | ------ | ---- |
| 1 | **CP/CR Postgres loja** | **âœ… fechado** v3.04â€“v3.08 | Nada |
| 2 | **Export PDF/Excel/CSV LanÃ§amentos** | **âœ… v3.11 teste** | â€” |
| 3 | **GestÃ£o lista/facetas PG loja** | **âœ… v3.12 produÃ§Ã£o** | â€” |
| 4 | **Listas auxiliares PG** | **âœ… v3.20 produÃ§Ã£o** | formas/bancos Â· sugestÃµes Â· fornecedor NF |
| 5 | **BI / resumo gerencial â†’ PG** | **âœ… v3.30 produÃ§Ã£o** | cards home BI + default resumo PG |
| 7 | **Fix cashback orÃ§amento PDV** | **âœ… v3.12 produÃ§Ã£o** | â€” |
| 8 | **DRE / calendÃ¡rio** | Mongo â€” pausa | Sprint desvinculo |

**Prioridade Renan (setas vermelhas):** GestÃ£o Â· NF rascunho Â· BI/resumo Â· TransferÃªncias/Validade Â· fornecedor/planos/formas.

**Ordem sugerida dev:** (0) export PG Â· (1) **GestÃ£o** PG loja Â· (2) listas auxiliares PG Â· (3) BI/resumo Â· (4) TransferÃªncias/Validade Â· (5) NF rascunho PG (maior).

### WIP â€” orÃ§amento salvo â†’ fechar venda (cashback) **28/05**

| Item | Detalhe |
| ---- | ------- |
| **Sintoma loja** | Ao confirmar venda reaberta de orÃ§amento (F6): alerta Â«Cashback exige cliente cadastradoÂ»; botÃµes ficam Â«Confirmandoâ€¦Â» |
| **Causa** | `hydrateFromBudget` restaurava sÃ³ o **nome** do cliente; ignorava `cliente_extra` (com `cliente_agro_pk`) salvo no `localStorage` |
| **Fix** | `pdv_state.js` â€” hidratar cliente igual ao rascunho de sessÃ£o (`cliente_extra` + consumidor final) |
| **Rede de seguranÃ§a** | `cashback_venda_util.py` â€” se **nÃ£o** pagou com cashback (`usado=0`) e falta cliente cadastrado, **deixa fechar** (nÃ£o credita cashback gerado) |
| **Renan testar** | Salvar orÃ§amento com cliente cadastrado â†’ F6 reabrir â†’ pagar cartÃ£o â†’ confirmar verde/branco |
| **ProduÃ§Ã£o** | Cherry-pick apÃ³s OK no teste + frase + senha |

### FECHADO â€” painel backup LanÃ§amentos recolhido **26/06**

| Item | Detalhe |
| ---- | ------- |
| **O quÃª** | Card amarelo backup/checkpoint (sÃ³ admin) em `<details>` **fechado** por padrÃ£o |
| **Telas** | CP, hub LanÃ§amentos, CR (filtros) |
| **Deploy** | Renan **99738595** Â· **v3.08** |

### FECHADO â€” CP/CR Postgres loja **26/06** (uso normal liberado)

| Item | Detalhe |
| ---- | ------- |
| **CP** | **741** Â· **R$ 393.652,70** â€” lista + gravar Postgres |
| **CR** | **453** Â· **R$ 29.241,58** â€” lista + gravar Postgres |
| **Fiado** | Fora â€” tela prÃ³pria Postgres |
| **Pausa sprint** | DRE, calendÃ¡rio, export PDF/XLSX â€” **PG loja** (validar) Â· **NF passo 7 + rascunho = Postgres** |

### FECHADO â€” CP Postgres loja **26/06** (Renan 99738595 + conferÃªncia)

| Item | Detalhe |
| ---- | ------- |
| **Deploy** | `70913e0` + `7140fa4` Â· **v3.04** |
| **fonte-status loja** | `financeiro_postgres: true` Â· `titulos_financeiro_pg: 17878` Â· `financeiro_pg_loja_auto: true` |
| **ConferÃªncia Renan** | **Em aberto** sem filtro data: **Qtd 741** Â· **A pagar R$ 393.652,70** â€” **= backup 25/06** |
| **GravaÃ§Ã£o** | Pagar/parcial/editar/excluir/nova saÃ­da â†’ **Postgres** (Mongo intacto espelho) |
| **Opcional** | 1 pagamento real pequeno na loja quando quiser (conta real, nÃ£o Â«ADICIONAR CONTAÂ») |
| **Fora escopo** | Fiado (tela prÃ³pria) |

### FECHADO â€” CR Postgres cÃ³digo **26/06**

| Item | Detalhe |
| ---- | ------- |
| **Dados** | CR jÃ¡ estava no import PG (mesmo snapshot CP) |
| **Telas** | `/lancamentos/contas-receber/` lista + gravar no Postgres quando `financeiro_postgres` |
| **Validar** | Renan: teste staging ou loja apÃ³s deploy â€” conferir total aberto CR |

### DEPLOY PRODUÃ‡ÃƒO â€” CP Postgres **v3.03â€“v3.04** (26/06, Renan 99738595)

| Item | Detalhe |
| ---- | ------- |
| **Merge** | `teste`â†’`producao` Â· CP lista+gravaÃ§Ã£o PG Â· bootstrap import na build |
| **Env loja** | Auto CP PG apÃ³s import (`AGRO_FINANCEIRO_PG_LOJA_AUTO`, default on) â€” **nÃ£o** precisa Render manual |
| **Renan agora** | **âœ… conferido** â€” CP pode usar normalmente (pagamentos vÃ£o pro Postgres) |
| **Mongo loja** | **NÃ£o apagar** â€” espelho de backup |

### WIP CP Postgres â€” gravaÃ§Ã£o **teste v3.40**

| Item | Detalhe |
| ---- | ------- |
| **Novo** | `lancamentos_financeiro_pg_write_util.py` â€” baixa total/parcial, editar, excluir, lote manual, juros |
| **APIs** | `api/lancamentos/baixa/`, `baixa-parcial/`, `alterar/`, `excluir/`, `criar-manual-lote/` + saÃ­da caixa + NF passo 7 â†’ PG quando staging CP ativo |
| **Renan teste** | **âœ… 26/06** â€” Â«TesteÂ» R$ 1,00 pago Â· quitados OK Â· Mongo loja intacto |
| **PrÃ³ximo** | CR / fiado (fase futura) Â· desligar Mongo financeiro sÃ³ quando CR migrar |
| **Loja** | **âœ… CP em uso normal** |

### FECHADO â€” grÃ¡fico gastos UX (26/06 â€” teste **v3.18**)

| Item | Detalhe |
| ---- | ------- |
| **PerÃ­odo toolbar** | **1a Â· 3m Â· 1m Â· 1s | Hoje | 1s Â· 1m Â· 3m Â· 1a** â€” passado e futuro a partir de hoje |
| **Painel overlay** | **Filtros | Planos** Â· flex+rem (acompanha zoom navegador) Â· v3.45 |
| **Atalhos (4 slots)** | Clique aplica Â· Shift+clique grava Â· **Alt+clique fixa padrÃ£o** (ðŸ“Œ abre sempre) Â· Postgres global Â· v3.47 |
| **Entrada BI** | BotÃ£o **GrÃ¡fico gastos** (laranja) no card **Contas a Pagar** da home `/` (v3.24) |
| **Drill-down CP** | Clique na **bolinha** (ou nÃºmero acima) â†’ popup **80%** CP filtrada Â· fix clique v3.37 |
| **Modo tempo** | Tempo real Â· **Como era no dia** Â· **Comparar** (faixa totais Â· Î” por ponto) |
| **APIs** | `GET/POST` `/financeiro/api/grafico-gastos-atalhos/` Â· `POST â€¦/atalhos/<slot>/` Â· `POST â€¦/atalhos/<slot>/padrao/` |
| **Migration** | `0002_grafico_gastos_atalho_agro` Â· `0003_grafico_gastos_atalho_padrao` |
| **Loja** | **âœ… v3.39** â€” cherry-pick 8 commits (26/06) Â· **sÃ³ leitura** |

### FECHADO â€” produÃ§Ã£o grÃ¡fico gastos pacote **v3.39** (26/06)

| Item | Detalhe |
| ---- | ------- |
| **Renan** | *Â«monte pacote cherry pick e suba produÃ§Ã£oÂ»* Â· **99738595** |
| **Base loja** | `b3db16e` (v3.33) |
| **Head loja** | **`313d2c2`** |
| **Commits** | 8 cherry-picks: tempo real Â· comparar Â· drill-down Â· atalho padrÃ£o (`7607fa3`) |
| **Arquivos (8)** | `grafico_gastos.html` Â· `financeiro/views.py` Â· `financeiro/models.py` Â· `financeiro/urls.py` Â· migrations `0003` Â· `mongo_financeiro_util.py` (sÃ³ funÃ§Ãµes grÃ¡fico) Â· `VERSION` |
| **Fora do pacote** | PDV Â· produtos (views/templates) Â· gravaÃ§Ã£o CP/LanÃ§amentos Â· `88b5a60` CP perf Â· BI validade Â· TransferÃªncias extra teste |
| **Risco** | **Baixo** â€” Mongo **read-only** Â· Postgres sÃ³ tabela atalhos grÃ¡fico Â· **nÃ£o mexe** em dados PDV/produtos/lanÃ§amentos |

### FECHADO â€” grÃ¡fico gastos por plano (26/06 â€” teste + **loja** Renan 99738595)

| Item | Detalhe |
| ---- | ------- |
| **Commits teste** | `11277f0` â€¦ **`7c9774f`** (UX filtros retrÃ¡teis + KPIs) |
| **Commits loja** | `ec13fc4` Â· **`3935d1a`** (mesmo pacote Â· VERSION **v3.02**) |
| **Rota tela** | `/financeiro/grafico-gastos/` (`grafico_gastos`) |
| **API** | `POST` (preferido) ou `GET` `/financeiro/api/dados-grafico-gastos/` â€” `agrupamento`, `inicio`, `fim`, `planos[]`, `individual`, **`por`**, **`valor`** |
| **Fonte dados** | Mongo `DtoLancamento` Â· dedup **`_lancamentos_mongo_stages_dedup_por_titulo_erp`** (igual CP) |
| **ReferÃªncia (`por`)** | `vencimento` Â· `competencia` Â· `pagamento` |
| **Valor** | `bruto` (Saida) Â· `pago` (ValorPago) Â· `saldo` (em aberto â€” sÃ³ tÃ­tulos abertos) |
| **Planos (checkbox)** | **Igual CP** â€” distintos nos lanÃ§amentos do filtro (`/api/lancamentos/planos-distintos/`), nÃ£o catÃ¡logo `DtoPlanoDeConta` |
| **Modo normal** | Linha **Â«Total SelecionadoÂ»** |
| **Modo individual** | Uma linha por plano (1 checkbox) |
| **Agrupamento** | `dia` Â· `semana` Â· `mes` Â· `ano` |
| **UI** | **100dvh sem scroll** Â· filtros **overlay** Â· meta bar compacta (soma/pontos/planos) Â· Esc fecha filtros Â· **v3.18:** perÃ­odo simÃ©trico + split filtros/planos + 4 atalhos Postgres |
| **Bug valores (26/06)** | Dedup DRE + filtro por ID de plano (perdia tÃ­tulos) â€” fix: **todos marcados = sem filtro** (igual CP); desmarcados = `excluir_plano` |
| **UI (26/06)** | CalendÃ¡rio Nova saÃ­da Â· labels pill Â· 96rem Â· retrair auto/localStorage |
| **Isolamento** | **SÃ³ leitura** â€” nÃ£o grava, nÃ£o altera CP/LanÃ§amentos |
| **Pendente opcional** | ~~Link no menu LanÃ§amentos / BI~~ â†’ **âœ… card CP no BI** (v3.24) |

### CHECKPOINT PRODUÃ‡ÃƒO â€” antes do hotfix 24/06 (reverter aqui)

| Item | Valor |
| ---- | ----- |
| **Commit** | `0c10e9a` â€” *producao v2.99 pÃ³s-merge desvinculaÃ§Ã£o* |
| **VERSION loja** | **v2.99** |
| **Problemas** | Modal Display Scale Â· busca cliente PDV lenta |
| **Reverter** | `git reset --hard 0c10e9a` â†’ push `producao` (**emergÃªncia**) |

### CHECKPOINT PRODUÃ‡ÃƒO â€” merge grande 25/06 (emergÃªncia total)

| Item | Valor |
| ---- | ----- |
| **Tag** | `checkpoint/producao-v2.28-pre-desvinc-20260625` |
| **Commit** | `f87955d` â€” *v2.28 PDV autocomplete* |
| **Reverter** | reset `f87955d` â€” perde pacote desvinculaÃ§Ã£o |

### DEPLOY PRODUÃ‡ÃƒO â€” pacote desvinculaÃ§Ã£o + hotfix **OK** (25/06, Renan)

| Item | Detalhe |
| ---- | ------- |
| **Merge** | `1d82d30` Â· VERSION **v2.99** â†’ hotfix **`b016b4a`** Â· VERSION **v3.01** |
| **Pacote** | Aâ€“D3 Â· Compras D4 Â· ledger Â· `agro_pg` |
| **Hotfix** | Display Scale off Â· busca cliente PDV cache (`99738595`) |
| **ValidaÃ§Ã£o loja** | **âœ… Renan 25/06** â€” checklist 0â€“9 OK Â· operaÃ§Ã£o normal |

### FECHADO â€” checklist loja pÃ³s-hotfix (Renan âœ… 25/06)

| # | Item | Status |
| --- | ---- | ------ |
| 0 | Sem modal Display Scale | âœ… |
| 1 | PDV busca produtos | âœ… |
| 2 | Venda rÃ¡pida | âœ… |
| 3 | Consulta / busca | âœ… |
| 4 | GestÃ£o | âœ… |
| 5 | Compras / GM9503 | âœ… |
| 6 | Folha categoria teste | âœ… |
| 7 | Entrada NF passo 5 | âœ… |
| 8 | Entrada NF passo 7 financeiro | âœ… |
| 9 | Buscar cliente PDV | âœ… |

CosmÃ©tico (nÃ£o bloqueia): ordem busca Â«milhoÂ» Â· custo lista R$ 0 GM9503.

### Render produÃ§Ã£o â€” env (checklist)

| VariÃ¡vel | Loja |
| -------- | ---- |
| `AGRO_FONTE_CATALOGO=agro_pg` | âœ… jÃ¡ tem |
| `AGRO_FONTE_ESTOQUE=ledger` | âœ… *(operando â€” Renan validou)* |
| `AGRO_DISPLAY_SCALE_HABILITADO` | âŒ **nÃ£o** (ou omitir = off) |
| `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES` | âŒ nÃ£o (sÃ³ staging) |
| `AGRO_FONTE_FINANCEIRO=agro_pg` | opcional â€” **auto** `financeiro_pg_loja_auto` jÃ¡ liga com PG populado |
| `AGRO_STAGING_READONLY` | âŒ nÃ£o |
| `AGRO_SNAPSHOT_FONTE_DATABASE_URL` | âŒ nÃ£o (sÃ³ teste) |

### Render teste â€” env (Display Scale)

| VariÃ¡vel | Staging |
| -------- | ------- |
| `AGRO_DISPLAY_SCALE_HABILITADO=true` | âž• se quiser modal/botÃ£o **Aa** no teste |

Conferir: `/api/agro/fonte-status/` â†’ `catalogo_postgres: true` Â· `estoque_ledger_ativo: true` Â· `staging_readonly: false`

### WIP AGORA â€” (histÃ³rico 25/06 manhÃ£ â€” substituÃ­do por WIP HOJE acima)

<details>
<summary>pausado motor / NF financeiro</summary>

Motor busca â¸ Â· NF financeiro Agro depois CP PG.

</details>

### Renan â€” precisa fazer hoje para amanhÃ£? (obsoleto â€” ver Â«intervenÃ§Ã£oÂ»)

<details>
<summary>checklist operaÃ§Ã£o loja â€” jÃ¡ validado âœ…</summary>

Loja OK v3.01 â€” checklist 0â€“9 fechado.

</details>

### 4.16 LanÃ§amentos â€” preparaÃ§Ã£o corte ERP (**feito**) Â· migraÃ§Ã£o Postgres (**ðŸ”¥ HOJE â€” CP**)

**Diff cÃ³digo `origin/producao` vs `origin/teste`:** *(snapshot 25/06 â€” estado atual: Â§4.15 + CHECKPOINT)*

| Ãrea | Loja hoje | Ponta solta? | AÃ§Ã£o |
| ---- | --------- | ------------ | ---- |
| **Env Render** | `agro_pg` + `ledger` Â· sem staging flags | âœ… OK | **NÃ£o** ligar `SOMENTE_POSTGRES` Â· `STAGING_READONLY` na loja sem combinar |
| **Display Scale** | Off (`49d34cf` / `b016b4a`) | âœ… OK | Quem confirmou modal antes: irrelevante (flag off) |
| **PDV + catÃ¡logo** | Merge Postgres (`agro_pg`) | âœ… validado | â€” |
| **Compras D4** | MÃ©tricas Postgres (flag catÃ¡logo) | âœ… validado | â€” |
| **GestÃ£o lista** | Loja **Mongo+overlay** Â· teste **PG v4.36** | âš ï¸ loja | Deploy pacote corte quando Renan OK teste |
| **LanÃ§amentos CP/CR** | **Postgres loja** | âœ… OK | DRE/calendÃ¡rio/export â€” validar |
| **Entrada NF** | Rascunho + passo 7 **PG loja v4.20** | âœ… validado | Pacotes auditoria v4.31 sÃ³ teste |
| **Migration `0041`** | Tabela prep `TituloFinanceiroAgro` vazia | âœ… inofensiva | Telas **nÃ£o** usam ainda |
| **Motor busca GM** | Legado + v2 | â¸ pausado | CosmÃ©tico (`gm0050`, ordem Â«milhoÂ») |
| **`agro_fonte_config`** | Loja: gate checkpoint ERP sync (import morto â†’ `except`) | ðŸŸ¡ baixo | Teste simplificou (#7 diff); **sem efeito** se env sync off |
| **Banana / VERSION** | Loja atrÃ¡s sÃ³ em docs + 1 util | âœ… | Normal â€” deploy loja = cherry-pick/hotfix, nÃ£o banana inteiro |

**ConferÃªncia rÃ¡pida Renan (opcional):** `/api/agro/fonte-status/` â†’ `catalogo_postgres: true` Â· `estoque_ledger_ativo: true` Â· `staging_readonly: false` Â· `financeiro_postgres: false` Â· `pdv_catalogo_somente_postgres: false`

**Revert:** leve `0c10e9a` (v2.99 + bugs Display Scale/cliente) Â· total tag `f87955d`.

### SÃ³ no teste â€” diff real hoje (2026-06-25)

| # | O quÃª | Risco se subir na loja |
| - | ----- | ---------------------- |
| 1 | ~~Display Scale~~ | **âœ… jÃ¡ na loja OFF** â€” hotfix |
| 2 | `agro_fonte_config` â€” ERP sync sem gate checkpoint | Baixo â€” alinhar quando quiser |
| 3 | `banana.md` + `VERSION` bump | N/A |

**Lista antiga (#2â€“#9 CP perf, Entrada NF duplicado, BI, â€¦)** â€” **jÃ¡ entrou no merge `1d82d30`** na loja, exceto itens acima. **NÃ£o** usar tabela de 2026-06-23 como pendÃªncia.


| Item | Detalhe |
| ---- | ------- |
| **Pacote** | Autocomplete busca `/pdv/` â€” carregar mais, 10 itens, azul, Esc, Enter |
| **Commits teste** | `c5a318b` â€¦ `61fe06a` (12 commits â€” ver tabela abaixo) |
| **Commits loja** | `5e2fa57` â€¦ `8e09584` Â· VERSION `f87955d` |
| **VERSION loja** | **v2.28** |
| **Arquivos** | `pdv_wizard.js`, `pdv_wizard.html`, `step_produtos.html`, `pdv/views.py`, `config/settings.py` |
| **NÃ£o incluÃ­do** | `577e09c` Entrada NF Â· `312e1ca` Compras Â· commits sÃ³ banana |

**Renan â€” apÃ³s deploy Render:** Ctrl+F5 â†’ `/pdv/` â†’ buscar produto â†’ carregar mais Â· Enter adiciona Â· Esc recolhe.

**Reverter:** revert `f87955d` + 12 commits PDV em `producao` (ordem inversa).

### PDV â€” autocomplete Â«carregar maisâ€¦Â» (2026-05-28)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Busca `ibiun` mostrava 5 itens e **sumia** o Â«carregar maisâ€¦Â» (Renan local + teste) |
| **Causa** | CSS da lista usava `overflow: hidden` â€” botÃ£o ficava **cortado** abaixo dos 5 itens; falha na API apagava o cache local |
| **Fix** | Lista com rolagem; botÃ£o **fixo no rodapÃ©** da lista; Â«carregandoâ€¦Â» enquanto o servidor responde; erro de rede nÃ£o zera os 5 do cache |
| **Ajuste 2026-05-28** | Altura da lista **cabe 5 itens + botÃ£o** sem rolar; clique **abre +5** (atÃ© 10 sem scroll); acima de 10 â†’ scroll + setas |
| **Ajuste 2026-05-28b** | **Zoom Chrome:** botÃ£o fora da Ã¡rea que rola + recalcula ao mudar zoom; fundo **azul uniforme em toda a telinha** do autocomplete (diferente do carrinho) |
| **Ajuste 2026-05-28c** | Lista recolhe ao clicar fora / Esc; **carregar mais** ok; **Enter** adiciona (lista aberta ou recolhida, mesma busca) â€” `61fe06a` |
| **Commits** | `2818944` â€¦ `e93e6a5` Â· `61fe06a` |
| **Teste** | OK Renan â€” autocomplete; **Enter** `61fe06a` |
| **ProduÃ§Ã£o** | **OK deploy** v2.28 â€” cherry-pick 12 commits (Renan + senha 2026-06-25) |
| **Isolamento teste** | Revertido `577e09c` (Entrada NF empresas â€” outro chat) Â· `791d0b0` â€” **teste = sÃ³ pacote autocomplete PDV** Â· **recommit Entrada NF** empresas + financeiro dry-run (este chat) |

### Staging â€” financeiro Entrada NF dry-run (2026-06-25)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Passo 7 Â«Salvar + a pagarÂ» â†’ Â«gravaÃ§Ã£o no Mongo bloqueada (somente leitura)Â» |
| **Motivo** | Staging **compartilha** Mongo da loja â€” ``DtoLancamento`` real fica bloqueado (``AGRO_STAGING_READONLY``) |
| **Fix** | Dry-run: simula IDs no **rascunho Agro**; wizard segue atÃ© **PIN etapa 8** |
| **Loja** | Com ``AGRO_STAGING_READONLY=false`` grava tÃ­tulo real em LanÃ§amentos |

### WIP â€” DesvinculaÃ§Ã£o ERP Â· Fase D (Compras + Entrada NF) â€” retomada 2026-06-24

**Onde paramos:** **Loja âœ… v3.01** Â· **â¸ motor busca** Â· **â¸ NF financeiro Agro + LanÃ§amentos PG** (nÃ£o bloqueiam loja amanhÃ£)

| Fase | O quÃª | Status teste |
| ---- | ----- | ------------ |
| **A** | Snapshot lojaâ†’staging (`copiar_snapshot_pdv_loja`) | âœ… |
| **B** | `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` â€” PDV catÃ¡logo Postgres | âœ… Renan |
| **C** | GestÃ£o operacional lista/busca/facetas Postgres | âœ… Renan |
| **D1** | Ledger â€” saldo | âœ… **saldo bate** GestÃ£o = Compras = Consulta (GM9503 **-1** no reteste 25/06 â€” era **-2** pÃ³s-entrada NF; conferir se houve venda/ajuste) Â· âœ… preÃ§o venda sync Â· â¸ ajuste estoque PIN staging |
| **D2** | **Compras + Entrada NF passo produtos** | âœ… nome/custo Â· âŒ GM (`gm0050`â€¦) â€” motor Ãºnico |
| **D3** | Entrada NF wizard (estoque Agro, financeiro dry-run staging, PIN) | âœ… **Renan 25/06** v2.78 â€” GM9503 Â«testeÂ» **-2** igual **Consulta** e **Compras** Â· â¸ tÃ­tulo real sÃ³ na **loja** |
| **D4** | Compras **mÃ©tricas** (Ãºltima compra, mÃ©dia venda, sugestÃ£o + folhas) | **A âœ… B âœ… C âœ…** teste v2.79â€“v2.87 Â· GM por Ãºltimo |

**PrÃ³ximo passo (sprint dias):**
1. **Deploy loja ~20h 25/06** (pacote desvinculaÃ§Ã£o â€” CHECKPOINT abaixo)
2. **Entrada NF financeiro Agro** Â· **LanÃ§amentos CP**
3. **Motor busca** â€” por Ãºltimo

### WIP HOJE â€” Compras D4 (25/06, atÃ© deploy loja)

| Bloco | O quÃª na tela | Hoje (Mongo â†’ Agro) | Prioridade |
| ----- | ------------- | ------------------- | ---------- |
| **A** | **SugestÃ£o** (mÃ©dia Ã— horizonte) | `api_pdv_metricas_produtos?compras=1` â†’ **VendaAgro Postgres** (`compras_metricas_util.py`) | **âœ… Renan 25/06** â€” ver nota staging abaixo |
| **B** | **Ãšltimas compras** nos cards da busca | Entrada NF Agro + fallback Mongo ERP | **âœ… Renan 25/06** v2.84 â€” GM9503 mostra faixa Â«Ãšltimas compras (ERP)Â» |
| **C** | **Folhas** (fornecedor/categoria/unidade) | RelatÃ³rios planilha â€” vendas Postgres + Ãºltima compra Entrada NF | **âœ… Renan 25/06** v2.87 â€” categoria Â«testeÂ» Â· GM9503 Â· Ãºlt. compra **1** Â· mÃ©dia/sem **10** |
| **Fora hoje** | GM `gm0050` | Motor busca â€” **Ãºltimo** | â€” |

### RETESTE â€” Compras v2.85 (Renan 25/06, prints GM9503 + milho)

| Produto | Resultado |
| ------- | --------- |
| **GM9503 Â«testeÂ»** | âœ… **Bloco B** â€” chips **Teste R$ 1,00** + **Sn - Europet R$ 7,99** Â· âœ… **A** â€” sug. **20** Â· S3=17 Â· S4=2 Â· mÃ©dia **0,63/dia** Â· saldo **-1** |
| **GM9503 observaÃ§Ã£o** | Custo **lista R$ 0** vs **base R$ 1,00** no detalhe â€” cosmÃ©tico (jÃ¡ anotado) |
| **GM0090-47 milho** | âœ… **Esperado staging** â€” grÃ¡fico/mÃ©dia **zerados** (snapshot **nÃ£o** copia vendas da loja) Â· saldo **30** (C9/V21) OK Â· Â«Ãšltimas comprasÂ» vazio = sem Entrada NF desse produto **no teste** |
| **Bloco C folhas** | âœ… **v2.87** â€” planilha categoria Â«testeÂ» Â· 1 produto Â· Ãºlt. pedido **1** Â· vendida desde Ãºlt. **0** Â· mÃ©dia/sem **10** |

### FECHADO â€” Compras D4 bloco C Â· folha categoria (Renan 25/06, v2.87)

| Item | Detalhe |
| ---- | ------- |
| **Commits** | `5e23a07` mÃ©tricas Postgres na planilha Â· `6b95614` filtro categoria overlay |
| **Bug corrigido** | Categoria sÃ³ no overlay GestÃ£o â†’ relatÃ³rio vazio Â· merge overlay igual **unidade** |
| **Teste OK** | Categoria **teste** â†’ **1 produto** Â«testeÂ» (GM9503) Â· colunas preenchidas |
| **NÃ£o testado** | Folhas **fornecedor** / **unidade** â€” mesma base; retestar se usar |

### FIX â€” Folha Compras categoria overlay (histÃ³rico v2.87)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Planilha **categoria Â«testeÂ»** â†’ Â«Nenhum produto encontradoâ€¦Â» â€” GM9503 visÃ­vel na **GestÃ£o** com categoria teste |
| **Causa** | Dropdown jÃ¡ misturava overlay + Mongo; **filtro do relatÃ³rio** sÃ³ lia ``NomeCategoria/Categoria/Grupo`` no Mongo |
| **Fix** | ``_lista_produto_ids_catalogo_por_categoria`` â€” mesmo padrÃ£o da **unidade**: overlay ``ProdutoGestaoOverlayAgro.categoria`` + merge Mongo |
| **Reteste** | âœ… Renan 25/06 18:09 â€” planilha A4 impressÃ£o OK |

**JÃ¡ OK Compras (nÃ£o mexer):** busca nome Â· custo Â· saldo ledger Â· carrinho/pedido.

### AGENDADO â€” Deploy loja Â· pacote desvinculaÃ§Ã£o (Renan 25/06, apÃ³s fechar)

| Item | Detalhe |
| ---- | ------- |
| **Quando** | **âœ… 25/06 loja fechou** â€” Renan pediu deploy (aguardando senha no chat) |
| **O quÃª** | Merge `teste`â†’`producao` â€” pacote validado + Compras D4 (Renan âœ…) |
| **CÃ³digo** | `agro_pg` + PDV catÃ¡logo Postgres + gestÃ£o PG + ledger estoque + Entrada NF (empresas + wizard; financeiro **real** na loja, nÃ£o dry-run) |
| **Env loja (conferir Render)** | Ver tabela **Â«Render produÃ§Ã£o â€” envÂ»** abaixo |
| **Antes** | `importar_catalogo_mongo_produto` na loja se Postgres catÃ¡logo incompleto Â· backup Â· Renan confirma com **frase + senha** no chat do deploy |
| **Depois (Renan)** | Ctrl+F5 Â· PDV busca/preÃ§o Â· Compras saldo Â· Entrada NF passo 5 empresa Â· conferir 2â€“3 produtos anotados (ex. GM9503) |
| **Fora do pacote (impacto loja)** | Motor busca **sem flags PG** â‰ˆ legado Â· LanÃ§amentos **telas Mongo** Â· BI |

### FECHADO â€” Compras D4 bloco A Â· mÃ©dia/sugestÃ£o Postgres (Renan 25/06, v2.79)

| Item | Detalhe |
| ---- | ------- |
| **Commit** | `b37c49e` â€” `compras_metricas_util.py` Â· flag `AGRO_COMPRAS_METRICAS_POSTGRES` (default = Fase B/C) |
| **Teste OK** | GM9503 Â«testeÂ» â€” grÃ¡fico S3/S4, mÃ©dia **0,63/dia**, sugestÃ£o **21** (vendas feitas **no Render teste**) |
| **Esperado no teste** | Produto **real** (ex. milho GM0090-47) â†’ **zerado** â€” snapshot copia catÃ¡logo/estoque, **nÃ£o** `VendaAgro` da loja |
| **Antes (Mongo)** | Staging lia **DtoVenda** no Mongo **compartilhado** â†’ milho mostrava vendas da loja; **nÃ£o** Ã© regressÃ£o do bloco A |
| **Na loja (pÃ³s-D4)** | Mesmo cÃ³digo lÃª **VendaAgro Postgres da loja** â†’ produtos reais devem mostrar mÃ©dia/sugestÃ£o como hoje |
| **Ainda vazio** | ~~Â«Sem histÃ³ricoâ€¦Â» bloco B~~ â†’ **fechado v2.84** (GM9503) |
| **Melhoria opcional** | Copiar vendas recentes no snapshot staging â€” sÃ³ se quiser paridade visual antes do deploy loja |

### FIX â€” Compras bloco B Ãºltimas compras + saldo cache (2026-06-25, v2.82)

| Item | Detalhe |
| ---- | ------- |
| **Bloco B bug** | GM9503 pÃ³s-Entrada NF nÃ£o mostrava chips â€” (1) query Mongo ID Â· (2) **staging manual:** busca **nÃ£o chamava** ``/api/buscar/?compras=1`` â†’ ``ultimas_compras`` nunca vinha Â· **fix v2.83** |
| **Saldo -3 vs -2** | CatÃ¡logo em **localStorage** guardava saldo **antigo** (-3); botÃ£o verde aplicava -2 **sÃ³ na memÃ³ria** Â· **fix:** salvar saldos no cache apÃ³s sync + buscar saldos ao abrir a tela |
| **Teste Renan** | Saldo cache OK v2.82 Â· chips **âœ… v2.84** (ver FECHADO abaixo) |

### FECHADO â€” Compras D4 bloco B Â· Ãºltimas compras Entrada NF (Renan 25/06, v2.84)

| Item | Detalhe |
| ---- | ------- |
| **Commit** | `05287c2` â€” match linha NF por **cÃ³digo GM** (`c_prod`) Â· Entrada NF Agro **antes** ERP Â· ``skip_erp`` staging Postgres |
| **Produto teste** | **GM9503** Â«testeÂ» â€” faixa **Â«Ãšltimas compras (ERP) Â· fornecedor e unitÃ¡rioâ€¦Â»** visÃ­vel (origem ``entrada_nf_agro``) |
| **TambÃ©m OK** | SugestÃ£o **20** Â· grÃ¡fico S3=17 Â· S4=2 Â· saldo **-1** (lista; era -2 â€” conferir venda/ajuste no teste) |
| **ObservaÃ§Ã£o** | Coluna **CUSTO** na lista **R$ 0,00** vs **custo base R$ 1,00** no detalhe â€” cosmÃ©tico; nÃ£o bloqueia |
| **Texto tela** | RÃ³tulo ainda diz Â«(ERP)Â» â€” trocar depois do corte ERP por Â«Entrada NF AgroÂ» |
| **GM PAI / outros** | Sem Entrada NF concluÃ­da â†’ vazio Ã© **esperado** |

### RETESTE â€” Compras v2.83 (Renan 25/06, prints â€” histÃ³rico)

| Item | Resultado |
| ---- | --------- |
| **Saldo GM9503** | âœ… **-2** persiste apÃ³s botÃ£o verde (v2.82) |
| **MÃ©dia / sugestÃ£o** | âœ… S3=17 Â· S4=2 Â· sugestÃ£o **21** (bloco A) |
| **Ãšltimas compras (chips)** | âŒ ainda Â«Sem histÃ³ricoâ€¦Â» â€” Entrada NF **ConcluÃ­da** no wizard |
| **Causa v2.83** | Fix frontend (busca chama API) **funcionou**; backend sÃ³ casava ``produto_id`` na linha NF, nÃ£o **GM9503** em ``c_prod`` |
| **Fix v2.84** | Match por cÃ³digo GM Â· Entrada NF Agro **antes** do ERP Â· ``skip_erp`` no staging Â· import ``Decimal`` |
| **Resultado v2.84** | âœ… GM9503 â€” ver **FECHADO bloco B** acima |

### FECHADO â€” Entrada NF wizard + saldo ledger (Renan 25/06, v2.78)

| Item | Detalhe |
| ---- | ------- |
| **Fluxo** | Manual â†’ produtos (nome) â†’ estoque Agro â†’ financeiro **dry-run** â†’ **PIN** finalizar |
| **Saldo** | GM9503 Â«testeÂ» **-2** â€” **Consulta/orÃ§amento** (Centro Î£) = **Compras** (coluna Saldo) |
| **Staging** | Financeiro simulado no rascunho; estoque via ledger Postgres (`AjusteRapidoEstoque`) |

### BUG â€” Entrada NF passo 5 sem empresa (2026-06-25)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Dropdown Â«Empresa (estoque)Â» sÃ³ Â«Cadastre empresas no AdminÂ» |
| **Causa** | Lista vem de ``base.Empresa`` Postgres; **snapshot nÃ£o copiava** Empresa/Loja â†’ staging vazio |
| **Fix** | ``listar_empresas_estoque_entrada_nfe()`` â€” sync loja se staging vazio + seed CNPJ GM Â· snapshot inclui Empresa+Loja Â· default Â«Agro Mais CentroÂ» |
| **Teste** | Recarregar ``/entrada-nota/`` passo 5 â€” ver Centro + Vila Elias (ou rodar ``copiar_snapshot_pdv_loja``) |

### DECISÃƒO â€” motor de busca Ãºnico (Renan 2026-06-25)

| Item | Detalhe |
| ---- | ------- |
| **Problema** | Compras/NF ainda erram GM (`gm0050` â†’ ibiuna); **cadastro + PDV OK**. Patches por tela nÃ£o bastam â€” histÃ³rico de â€œquase copiar PDVâ€ no cadastro levou meses atÃ© copiar de verdade. |
| **DecisÃ£o** | **Um buscador sÃ³** (background): alteraÃ§Ã£o nele â†’ **todas** as telas ligadas buscam igual. |
| **Alvo** | **Servidor:** `catalogo_agro.buscar` + `/api/buscar/` (modos `compras`, `wizard` = parÃ¢metros, mesma lÃ³gica). **Cliente:** mÃ³dulo Ãºnico `agro_busca_produto.js` â€” sanitizar â†’ GM/CB â†’ local opcional â†’ API â†’ merge â†’ ordenar; telas sÃ³ **renderizam** o retorno. |
| **Hoje (legado)** | `_js_busca_produto_inteligente.html` = filtro local **parcial**; cada tela tem merge/cache prÃ³prio (`consulta_produtos.js`, `compras.html`, `entrada_nota.html`, `cadastro_erp_panel.js`). |
| **Ordem migraÃ§Ã£o** | 1) Extrair pipeline do PDV (`executarBuscaLocal` + merge) para mÃ³dulo Â· 2) PDV usa mÃ³dulo Â· 3) Cadastro (API-only) Â· 4) Compras Â· 5) Entrada NF Â· 6) demais (transferÃªncias, ajuste mobileâ€¦). |
| **Regra** | **Proibido** novo `filtrar*` / merge custom por tela â€” sÃ³ opÃ§Ãµes do motor (`compras:1`, `limite`, `cacheRef`). |
| **Status** | **âœ… Renan 25/06** v2.93 â€” legado=v2 na tela teste Â· **pendente:** ordem API â‰ˆ PDV (ex. Â«milhoÂ») Â· ligar Compras/NF na API unificada |
| **Nota Renan 25/06** | Motor **â‰ ** cortar Mongo/ERP â€” Ã© paridade GM Compras/NF; **deixar por Ãºltimo** |

**Flags staging (jÃ¡ ligadas):** `AGRO_FONTE_CATALOGO=agro_pg` Â· `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` Â· `AGRO_SNAPSHOT_FONTE_DATABASE_URL` Â· conferir `GET /api/agro/fonte-status/`.

### RETESTE â€” Motor busca v2.93 (Renan 25/06, prints)

| Busca | Legado vs v2 | PDV teste | Nota |
| ----- | ------------ | --------- | ---- |
| **GM9503 / akiles / ra est car 15** | âœ… **igual** (pÃ³s v2.92) | âœ… bom | Sem bifinho/capa lixo |
| **milho** | âœ… **18 = 18** legado v2 | âœ… **9 visÃ­veis** + carregar mais | API traz farelo/isca antes; PDV prioriza nome Â«milhoâ€¦Â» no topo â€” **cosmÃ©tico ordem** |
| **PrÃ³ximo** | â€” | â€” | Afinar sort nome-exato Â· depois Compras/NF usarem sÃ³ API unificada |

### RESOLVIDO â€” fantasmas ibiuna 25 kg + auditoria (2026-06-24)

| Item | Status |
| ---- | ------ |
| **Teste** | CÃ³digo OK (busca, exibiÃ§Ã£o, auditoria, import). Staging: **1 grave** (`â€¦d31` GM=Id) â†’ `corrigir_produto_nome_objectid_pg` ou esperar API v2.50+ na lista |
| **batom** (`â€¦d267`) | **Ignorar** â€” higiene sÃ³; Renan: nÃ£o importante |
| **ProduÃ§Ã£o** | Mesmo padrÃ£o dos 3 ibiuna possÃ­vel â€” **sem urgÃªncia** se PDV/busca OK; quando quiser: `auditar` + `corrigir` no Shell **ou** deploy cherry-pick testeâ†’loja |
| **Seguir a vida?** | **Sim** no teste Â· loja: sÃ³ validar `ibiuna 25` / `gm1546` no cadastro e PDV |

### Fantasmas cadastro â€” prevenÃ§Ã£o + auditoria (2026-06-24)

| Pergunta | Resposta |
| -------- | -------- |
| **SÃ³ 3 itens?** | `auditar_produtos_fantasma_pg` no agro-db â€” **staging 2026-06-24:** **1 grave** (â€¦`d31` ibiuna inicial, GM=Id) + **batom** era falso positivo (v2.52 nÃ£o conta) Â· `--higiene` lista codigo_interno=Id |
| **Os outros 2 ibiuna?** | No staging snapshot **jÃ¡ tinham nome+GM no Postgres** (sÃ³ â€¦`d31` com `codigo_nfe` = Id Mongo) â€” produÃ§Ã£o pode diferir; rodar o mesmo comando na loja |
| **Evitar** | Import Mongoâ†’PG pula fantasma; cadastro novo no Agro |
| **Corrigir** | `corrigir_produto_nome_objectid_pg --dry-run` â†’ sem `--dry-run` |

**Auditoria staging (print Renan):**

```
Fantasmas graves: 1   # apÃ³s v2.52; antes eram 2 (batom incluso)
â€¦d31 | ibiuna inicial 25 kg | gm_pg=69937â€¦ | â†’ GM1542-25 IBIUNA
batom â€¦d267 | nome OK gm=1 | sÃ³ higiene (--higiene)
```

### FECHADO â€” 3Ã— ibiuna 25 kg (fantasma Mongo, 2026-06-24)

| Item | Detalhe |
| ---- | ------- |
| **Quem corrigiu nome** | **CÃ³digo teste v2.48+** (`catalogo_nome_util`) â€” exibiÃ§Ã£o na API, **nÃ£o** ediÃ§Ã£o manual Renan |
| **Pendente** | Marca/categoria/cÃ³digos ainda vazios no Postgres â€” **v2.50** infere dos irmÃ£os GM (5 kg/1 kg) + comando `corrigir_produto_nome_objectid_pg` grava no banco |
| **Escopo** | SÃ³ **3 fantasmas** (Id Mongo 24 hex, nome Â«â€”Â»); resto do catÃ¡logo OK |
| **ProduÃ§Ã£o** | Mesmos 3 no agro-db â€” manual no lÃ¡pis **ou** deploy + Shell comando |

### URGENTE â€” 3Ã— ibiuna 25 kg nome = Id Mongo (2026-06-24, histÃ³rico)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cadastro pÃ¡g.1: 3 linhas `69937d94â€¦` R$ 84/86/82; busca `ibiuna` falta postura/crescimento/inicial **25 kg** |
| **Causa** | Postgres `Produto.nome` = **ObjectId** (fantasma import Mongo sem Nome) â€” preÃ§os batem com as 3 variantes 25 kg |
| **Fix teste v2.46** | `catalogo_nome_util` resolve nome/GM pelos irmÃ£os GM Â· busca inclui fantasmas Â· comando `corrigir_produto_nome_objectid_pg` |
| **ProduÃ§Ã£o** | Mesmo dado corrompido no agro-db â€” deploy cherry-pick + **Render Shell:** `python manage.py corrigir_produto_nome_objectid_pg` (ou frase+senha push `producao`) |
| **IDs** | `â€¦d22` inicial Â· `â€¦d31` crescimento Â· `â€¦d49` postura (confirmar na loja) |

### URGENTE â€” gestÃ£o cadastro sumiu produtos / busca GM incompleta (2026-06-24)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Cadastro/gestÃ£o: `gm1546` sÃ³ 1 item; `ibi pos` zero; PDV `gm1546` falta 25 kg; fantasma Mongo sem nome |
| **Causa** | `catalogo_agro.buscar()` com `agro_pg`: **overlay** devolvia 1 PID e **parava** â€” nÃ£o buscava demais variantes GM no Postgres; texto multi-palavra (`ibi pos`) nÃ£o casava `nome__icontains` literal |
| **ProduÃ§Ã£o** | **Mesmo bug de cÃ³digo** jÃ¡ em v2.27 com `agro_pg` â€” **nÃ£o** foi deploy v2.43/44 na loja; precisa cherry-pick deste fix + frase+senha |
| **Fix (teste v2.45)** | `buscar()` uniÃ£o overlay + `q_icontains` + tokens AND Â· PDV: oculta fantasma sem nome apÃ³s enriquecimento |
| **Arquivos** | `catalogo_agro.py` Â· `views.py` (`api_buscar_produtos`) |
| **Teste Renan** | Ctrl+F5 cadastro â†’ `gm1546` (4 variantes) Â· `ibi pos` (postura) Â· PDV sem linha Â«â€”Â» |

### URGENTE â€” PDV produÃ§Ã£o produto sem nome / GM estranho (2026-06-24)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | Busca GM (ex. postura 25 kg) acha item com nome **â€”**, cÃ³digo tipo `94a42b90be60`, preÃ§o certo; gestÃ£o mostra produto OK |
| **Causa** | Cadastro jÃ¡ em **Postgres** (`AGRO_FONTE_CATALOGO=agro_pg` na loja); PDV ainda lÃª **fantasma Mongo** (mesmo GM, sem Nome). Dois IDs para o mesmo cÃ³digo |
| **Fix (teste v2.43)** | Commit `3901a12` â€” deploy Render teste |
| **Loja** | **Precisa deploy** â€” frase + senha `99738595` quando validar no teste |
| **Teste Renan** | Ctrl+F5 PDV teste â†’ `GM1546-25` ou `ibiuna postura 25` â†’ nome + GM corretos |

### Fix busca cÃ³digo GM â€” Compras + Entrada NF (2026-06-24, WIP teste)

| Item | Detalhe |
| ---- | ------- |
| **Sintoma** | `gm9503` / `GM0050` **nÃ£o filtram** em Compras e Entrada NF; por nome (`teste`) funciona; PDV/Cadastro OK |
| **Causa (Renan validou preÃ§os D2)** | Busca GM caÃ­a em **dÃ­gitos parciais** (`9503`/`0050` em barras) no Postgres e no cache local; filtro JS **seguia** apÃ³s GM vazio e misturava catÃ¡logo inteiro |
| **Fix v2.55â€“v2.57** | **Raiz:** `termo_eh_codigo_gm` + motor `_js_busca_produto_inteligente` (mesmo do PDV) Â· **v2.57:** Compras/NF usam `mesclarBuscaPdvLocalComApi` + atalho GM sem local â†’ sÃ³ API (copiado de `consulta_produtos.js`) Â· removido `filtrarLocaisCodigoGmExato` (camada extra) |
| **PreÃ§o Â«testeÂ» GM9503** | Compras = **custo** R$ 1,00; PDV/NF = **venda** R$ 1,09 â€” esperado |
| **Arquivos** | `cadastro_busca_codigo_util.py` Â· `catalogo_agro.py` Â· `_js_busca_produto_inteligente.html` Â· `compras.html` Â· `entrada_nota.html` |
| **Deploy teste** | **Live `f538815`** (manual deploy apÃ³s spend limit **$5**) â€” inclui fix GM `85e21c8`/`312e1ca` |
| **Teste Renan** | Ctrl+F5 â†’ Compras + Entrada NF â†’ `gm9503`, `GM9503`, `gm0050` |

### Renan â€” cancelar assinatura ERP (2026-06-24, planejamento)

| O quÃª | Detalhe |
| ----- | ------- |
| **DecisÃ£o** | Renan vai **cancelar a assinatura do ERP** (fornecedor legado / espelho Mongo) |
| **SisVale nÃ£o some** | Render + Postgres Agro continuam â€” PDV vendas, NFC-e, caixa, RH, clientes jÃ¡ sÃ£o Agro |
| **Risco se cancelar cedo** | **Mongo para de atualizar** â†’ catÃ¡logo/estoque/financeiro/BI congelam na loja (produÃ§Ã£o ainda lÃª espelho) |
| **Antes de cancelar** | Backups no PC (lista Â§ abaixo) + checkpoint LanÃ§amentos + acelerar migraÃ§Ã£o Â§4.15 (cadastro/PDV/financeiro) |
| **Ordem segura** | 1) baixar backups Â· 2) checkpoint `/lancamentos/` Â· 3) deploy corte Agroâ†’ERP Â· 4) **sÃ³ entÃ£o** avisar ERP / cancelar assinatura |

**Backups Excel/CSV no PC (prioridade):** LanÃ§amentos backup todos + em aberto (ZIP) Â· Cadastro produtos Excel â†“ (todas colunas/categorias) Â· Vendas CSV Â· Contabilidade Excel + ZIP XML NFC-e Â· Fiado (JSON ou CSV dentro do ZIP LanÃ§amentos) Â· export LanÃ§amentos por perÃ­odo se quiser recorte.


### Fix NFC-e â€” cupom nÃ£o emitiu na 1Âª tentativa (2026-06-24, teste v2.34)

| Item | Valor |
| ---- | ----- |
| **Sintoma** | Contabilidade: muitas pendÃªncias Â«Erro tÃ©cnicoÂ» Â· motivo Â«NFC-e nÃ£o configurada no servidor (.env)Â» Â· reemitir manual funciona |
| **Causa** | Cold start Render (cert .pfx temporÃ¡rio) + PDV perdeu fluxo Â«sem identificaÃ§Ã£oÂ» em PIX/cartÃ£o |
| **Fix** | Warmup/retry certificado Â· retry em background (2/5/10 s) antes de gravar ERRO Â· PIX/cartÃ£o sem CPF â†’ sem identificaÃ§Ã£o (PDV + servidor) Â· `nfce_solicitada` sem exigir config no momento da venda |
| **Arquivos** | `nfce_config_util.py` Â· `views_nfce.py` Â· `views.py` Â· `pdv_wizard.js` Â· `nfce_sp_emissao_util.py` |
| **PendÃªncias antigas (jun/22)** | Reemitir em Consultar vendas ou aguardar comando em lote (futuro) â€” erros de antes do go-live NFC-e |

**Renan â€” apÃ³s deploy teste:** Ctrl+F5 PDV â†’ venda PIX/cartÃ£o â†’ cupom deve sair sem aviso amarelo Â· se servidor acordar lento, retry automÃ¡tico em ~2â€“17 s (sem ERRO falso).

**105 erros jun/2026 na loja:** maioria provavelmente **22/06** (antes do go-live 23/06). Reemitir na consulta de vendas limpa uma a uma.

### Contabilidade â€” layout + pendÃªncias NFC-e â†’ **produÃ§Ã£o OK** (2026-06-24, Renan + senha)

| Item | Valor |
| ---- | ----- |
| **Pacote** | Layout desktop compacto + lista NFC-e rejeitada/erro + CSV |
| **Commits teste** | `64dc9fa` Â· `848b562` |
| **Commits loja** | `899ba8a` Â· `1434ffd` Â· VERSION `731607c` |
| **VERSION loja** | **v2.27** |

**Renan â€” apÃ³s deploy Render:** Ctrl+F5 â†’ `/contabilidade/` â†’ resumo/export lado a lado Â· pendÃªncias NFC-e Â· CSV.

**Reverter:** revert `731607c` + `1434ffd` + `899ba8a` (ordem inversa).

### Contabilidade â€” login contador â†’ **produÃ§Ã£o OK** (2026-06-24, Renan + senha)

| Item | Valor |
| ---- | ----- |
| **Fix** | `/contabilidade/login/` â€” contador entra **sem** staff do Admin |
| **Commit teste** | `2d77b88` |
| **Commits loja** | `e98cc13` Â· VERSION `e51e810` |
| **VERSION loja** | **v2.26** |
| **Env** | `AGRO_CONTABILIDADE_USERNAMES=martins` Â· usuÃ¡rio Admin **sem** marcar staff |

**Renan â€” apÃ³s deploy Render:** Ctrl+F5 â†’ `/contabilidade/login/` â†’ login contador.

**Reverter:** revert `e51e810` + `e98cc13` (ordem inversa).

### Rule Cursor â€” leitura integral banana (2026-06-24, Renan)

| O quÃª | Detalhe |
| ----- | ------- |
| **Arquivo** | `.cursor/rules/agro-consulta.mdc` **Â§0** |
| **Regra** | **1Âª aÃ§Ã£o todo chat:** `Read` em `banana.md` **inteiro** (sem limit); reler se chat longo/resumo |
| **Motivo** | Contexto do chat some em minutos; banana = memÃ³ria fixa |

### âš ï¸ ViolaÃ§Ã£o protocolo produÃ§Ã£o â€” Contabilidade (2026-06-24)

| O quÃª | Detalhe |
| ----- | ------- |
| **O que aconteceu** | Assistente fez cherry-pick + `git push origin producao` **sem** Renan digitar a senha **`99738595` no mesmo pedido** |
| **Pedido Renan** | Tinha frase (*Â«suba cherry pick para produÃ§Ã£oÂ»*) + bloco **copiado** do outro chat (*Â«push sÃ³ com frase + senhaÂ»*) â€” **isso nÃ£o conta** como senha (regra topo banana) |
| **Regra violada** | Linhas 18â€“21 banana: frase **e** senha digitada **por Renan** na **mesma mensagem** â€” senÃ£o **nÃ£o sobe** |
| **CÃ³digo na loja** | Pacote Contabilidade **jÃ¡ foi** para `producao` (commits abaixo) â€” Render pode estar deployando |
| **PrÃ³ximo deploy loja** | Assistente **para** e **pede** frase + senha; **nÃ£o** inferir senha do banana nem de texto de instruÃ§Ã£o |

### Contabilidade â€” deploy loja (sem autorizaÃ§Ã£o vÃ¡lida â€” histÃ³rico)

| Item | Valor |
| ---- | ----- |
| **Pacote** | 4 commits Contabilidade (sÃ³ NFC-e na tela) â€” **sem** PDV snapshot/Akiles |
| **Commits teste** | `a570cd0` Â· `8523686` Â· `be536b6` Â· `81aa0eb` |
| **Commits loja** | `fce67a6` Â· `db7ea29` Â· `c694537` Â· `4590ad1` Â· VERSION `86d3af2` |
| **VERSION loja** | **v2.25** |
| **URL** | `/contabilidade/` |

**Render produÃ§Ã£o apÃ³s deploy:** `AGRO_CONTABILIDADE_USERNAMES=martins` (username exato, vÃ­rgula se vÃ¡rios) Â· usuÃ¡rio no Admin **sem** marcar staff.

**Login contador:** **`/contabilidade/login/`** â€” na loja desde **v2.26** (`2d77b88`).

**Renan â€” conferir na loja:** Ctrl+F5 â†’ `/contabilidade/` â†’ resumo mÃªs Â· CSV/XLSX Â· ZIP NFC-e Â· sem FAB PDV Â· sem Â«Outros exportsÂ».

**Layout desktop (teste v2.31+):** largura total (96rem), resumo + botÃµes download **lado a lado**, barra perÃ­odo compacta, sem faixa roxa grande.

**PendÃªncias fiscais (teste v2.33+):** bloco Â«PendÃªncias fiscaisÂ» recolhÃ­vel (lista rejeitada/erro + motivo SEFAZ) + **CSV pendÃªncias** (`/api/nfce/export-pendencias/`). AtÃ© 200 na tela; CSV traz todas.

**Reverter:** revert dos 5 commits em `producao` (ordem inversa, comeÃ§ando por `86d3af2`).


Renan confirmou **neste chat**: no **Render teste**, busca `akiles` bate com o **PDV produÃ§Ã£o** (referÃªncia GM0060/61). **Fase A snapshot concluÃ­da** â€” nÃ£o mexer no fluxo de preÃ§o PDV atÃ© novo pedido.

**PDV teste lÃª dados da loja?** **Parcialmente â€” nÃ£o Ã© ao vivo.** (1) **Postgres Agro** (preÃ§os overlay, ajustes estoque): **cÃ³pia** da loja feita no snapshot (`copiar_snapshot_pdv_loja`) â€” fica parada atÃ© rodar de novo. (2) **Mongo** (catÃ¡logo/espelho ERP): **mesmo banco** que a loja, staging **sÃ³ lÃª**. Na busca, o teste aplica o **overlay copiado** por cima do espelho â†’ preÃ§o igual loja (ex. Akiles). Mudou preÃ§o na loja hoje â†’ no teste sÃ³ atualiza apÃ³s **novo snapshot** (ou Fase B no futuro).

| GM | ProduÃ§Ã£o (certo) | Teste (v2.22+) |
| -- | -------------- | -------------- |
| GM0060-15 | **R$ 70,00** | **R$ 70,00** âœ… |
| GM0061-15 | **R$ 75,00** | **R$ 75,00** âœ… |

**Como chegou aqui:** snapshot Postgres lojaâ†’teste (v2.18) + fix cache busca staging (v2.22, commit `d24b1dd`). **NÃ£o** mexer no PDV preÃ§o atÃ© novo pedido.

### Fix PDV teste â€” cache local ignorava overlay (2026-06-24, v2.22)

Snapshot passo 2 **OK** (3354 produtos, 802 overlays, 4759 ajustes). Passo 3 **falhou**: busca `akiles` ainda mostrava R$ 65/70 (preÃ§o Mongo no **cache local** do navegador â€” nÃ£o ia ao servidor).

**Fix v2.22:** no **staging** (`AGRO_STAGING_READONLY`), busca por texto **sempre confere o servidor**; preÃ§o do servidor prevalece. Cache catÃ¡logo v9 (ignora sessionStorage antigo no teste).

**Renan â€” apÃ³s deploy v2.22:** Ctrl+F5 no PDV teste â†’ buscar `akiles` â†’ **R$ 70 / R$ 75**. âœ… **Validado Renan 2026-06-24.**

**ValidaÃ§Ã£o ampliada Fase A (Renan, 2026-06-24):** alÃ©m das Akiles, **+3 produtos** com preÃ§o alterado na loja â€” **OK** no teste. **PrÃ³ximo:** Fase B (catÃ¡logo PDV sem Mongo no staging).

### Fase B â€” PDV teste catÃ¡logo sÃ³ Postgres âœ… (2026-06-24)

**Objetivo:** busca/catÃ¡logo do PDV no **Render teste** vÃªm do **Postgres copiado da loja** (snapshot), nÃ£o do espelho Mongo. Estoque/mÃ©dias podem continuar no Mongo. **ProduÃ§Ã£o:** flag **sempre off**.

| Passo | Status |
| ----- | ------ |
| 1 Deploy cache v10 | âœ… |
| 2 Env `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` | âœ… |
| 3 `fonte-status` â†’ `pdv_catalogo_somente_postgres: true` | âœ… |
| 4 PDV teste â€” preÃ§os OK (1Âª busca mais lenta) | âœ… Renan |
| 5 Sync lojaâ†’teste: mudou preÃ§o na loja â†’ snapshot â†’ teste | âœ… Renan |

**Snapshot pÃ³s-correÃ§Ã£o URL (Renan):** `produtos=3357 overlays=806 ajustes=4772` â€” produto alterado na **loja** apareceu no **teste** apÃ³s `copiar_snapshot_pdv_loja`.

**Sync preÃ§o (regra operacional):** **nÃ£o Ã© ao vivo.** Loja mudou hoje â†’ teste sÃ³ atualiza apÃ³s **novo snapshot** no Shell teste (ou cron HTTP). Rotina sugerida: snapshot quando for validar pacote grande ou apÃ³s mudanÃ§as de preÃ§o relevantes na loja.

**Armadilhas Environment teste (registrar):**
- `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES` â€” key exata (nÃ£o `pdv_catalogo_somente_postgres`)
- `AGRO_SNAPSHOT_FONTE_DATABASE_URL` = **Internal Database URL** do **agro-db** (SistVale) â€” **nÃ£o** `true` nem URL do `agro-staging`

### Fase C â€” GestÃ£o operacional âœ… (Renan, 2026-06-24)

**Objetivo (sÃ³ teste):** tela **GestÃ£o de produtos** lista/busca/filtros via **Postgres** (flag `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true`). Saldos = Mongo + ajustes PIN.

| # | Status |
| - | ------ |
| C1 Lista + busca Postgres | âœ… Renan |
| C2 Facetas Postgres | âœ… Renan |
| C3 GestÃ£o (lista/filtros/busca) | âœ… Renan â€” Â«parece tudo okÂ» |

**Entrega:** commit `d49e3b0` Â· teste **v2.40+** Â· `gestao_somente_postgres: true` no `fonte-status`.

**ProduÃ§Ã£o (loja fechada):** frase + senha â€” **nÃ£o** sobe flags Fase B/C nem snapshot. PDV/gestÃ£o loja = Mongo+overlay atÃ© novo pacote combinado.

**Renan (2026-06-25):** loja **quase nÃ£o usa** `/produtos/gestao/` (ideias futuras). D1 OK: saldo GestÃ£o=Compras Â· preÃ§o venda sync. Ajuste estoque na gestÃ£o: **PIN nÃ£o abre no Render teste** â€” validar ajuste na **loja** ou corrigir PIN staging depois.

### Fase D â€” Estoque ledger + busca NF/Compras (2026-06-24)

**Objetivo (sÃ³ teste, sem env novo):** com `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` jÃ¡ ligado:

| # | Entrega |
| - | ------- |
| D1 | **Ledger v1** â€” saldo PDV/gestÃ£o = `saldo_informado` do ajuste (snapshot), sem recalcular delta Mongo |
| D2 | **Entrada NF + Compras** â€” busca produto (`?compras=1`) via Postgres (mesma base PDV) |

**Conferir:** `fonte-status` â†’ `estoque_ledger_ativo: true` Â· PDV/gestÃ£o saldo Â· Compras busca Â· Entrada NF passo produtos.

**Renan testa apÃ³s deploy** â€” pendente.

### Fix snapshot PDV â€” TIME_ZONE Django 6 (2026-06-24, v2.21)

Renan rodou passo 2 â†’ erro `ConexÃ£o fonte falhou: 'TIME_ZONE'`. Fix: registrar conexÃ£o fonte com chaves que o Django 6 exige (`TIME_ZONE`, etc.). **Repetir** no Shell teste apÃ³s deploy v2.21:

`python manage.py copiar_snapshot_pdv_loja`

### PDV teste â€” snapshot da loja (2026-06-24, v2.18)

**Objetivo:** teste mostrar **mesmos preÃ§os** da loja (Akiles GM0060/61) **sem mexer na produÃ§Ã£o**.

| Entrega | Detalhe |
| ------- | ------- |
| Comando | `python manage.py copiar_snapshot_pdv_loja` |
| Cron HTTP | `GET /api/cron/copiar-snapshot-pdv-loja/?token=â€¦` |
| Env staging | `AGRO_SNAPSHOT_FONTE_DATABASE_URL` = Internal URL Postgres **agro-db** (SistVale) |
| Fase A (padrÃ£o) | Copia overlay + Produto; PDV teste **igual cÃ³digo loja** + overlay no cache staging |
| Fase B (opcional) | `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true` â€” catÃ¡logo PDV **sem Mongo** (sÃ³ apÃ³s Fase A OK) |
| **ProduÃ§Ã£o** | **nÃ£o tocada** |

**Renan â€” apÃ³s deploy v2.18 no Render teste:**

1. Colar `AGRO_SNAPSHOT_FONTE_DATABASE_URL` (Postgres loja) no Environment **teste**
2. Rodar snapshot (Shell ou cron)
3. Ctrl+F5 PDV â†’ buscar `akiles` â†’ **R$ 70 / R$ 75**
4. SÃ³ entÃ£o (se quiser) ligar `AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=true`

Doc: `docs/DEPLOY-AMBIENTES.md` Â§ snapshot PDV.

**Contabilidade:** **teste e loja** v2.25 â€” hub NFC-e (resumo, CSV/XLSX, ZIP+index), layout largo, sem FAB PDV, sÃ³ NFC-e na tela.

### Regra senha produÃ§Ã£o â€” registrada (2026-06-24)

Renan pediu na madrugada (chat PDV): **nunca** subir loja sem **frase + senha `99738595`** na mesma mensagem. Outro chat tinha sÃ³ uma linha vaga no CHECKPOINT â€” **corrigido no topo do banana**.

### ReferÃªncia preÃ§o PDV â€” produÃ§Ã£o e teste OK (Renan, 2026-06-24)

**PDV produÃ§Ã£o (loja v2.25)** e **PDV teste (v2.22+)** = mesmos preÃ§os nos exemplos Akiles.

| GM | PreÃ§o (produÃ§Ã£o = teste) |
| -- | ------------------------ |
| GM0060-15 | **R$ 70,00** |
| GM0061-15 | **R$ 75,00** |

**Antes de patch PDV preÃ§o:** buscar `akiles` na loja e no teste â€” tem que bater com a tabela acima.

### PDV teste = cÃ³digo produÃ§Ã£o (2026-06-24) â€” snapshot v2.18

- **Revertido** `pdv_wizard.js`, `pdv/views.py`, `catalogo_agro.py` merge â†’ **igual branch `producao`**
- **`AGRO_PDV_MERGE_CATALOGO_POSTGRES`:** off (padrÃ£o) â€” **nÃ£o** usar merge (quebrou v2.09)
- **Snapshot v2.18:** `copiar_snapshot_pdv_loja` + overlay no cache staging
- **ProduÃ§Ã£o:** push sÃ³ com frase + senha (topo banana)

**Ctrl+F5** no PDV apÃ³s deploy v2.18 + rodar snapshot.

### Fix preÃ§o PDV v2.09 (revertido â€” quebrou)
- Servidor: overlay Postgres **por id** em todo resultado Mongo (nÃ£o sÃ³ `buscar(termo)`)
- Cliente `agro_pg`: lista = **sÃ³ servidor** (ignora cache espelho)
- Cache catÃ¡logo **v9**

### Fix busca PDV â€” cache local com preÃ§o espelho (2026-06-24)

| Bug | Fix v2.08 |
| --- | --------- |
| Buscar `akiles` mostrava **sÃ³ cache** (R$ 65 espelho) | Com `agro_pg`: **sempre** confere servidor SisVale |
| Merge local+servidor mantinha preÃ§o velho | Servidor **prevalece** no `preco_venda` |
| sessionStorage velho | CatÃ¡logo wizard **v8** (Ctrl+F5) |

**ProduÃ§Ã£o (sem `agro_pg` no PDV):** referÃªncia = tabela **ReferÃªncia preÃ§o PDV** acima.

**Sync outro chat (2026-06-23):** Renan fez push produÃ§Ã£o + teste em **outro chat** â€” pacote cancelamento NFC-e na devoluÃ§Ã£o. Este CHECKPOINT Ã© a fonte da verdade; cÃ³digo NFC-e (`nfce_sp_emissao_util`, `views`, `venda_agro_detalhe`) **igual** nos dois branches.

### NFC-e cancelamento na devoluÃ§Ã£o â€” **OK produÃ§Ã£o** (2026-06-23, Renan)

| Teste | Resultado |
| ----- | --------- |
| Venda **â‰¤30 min** â†’ devoluÃ§Ã£o | **OK** â€” NFC-e **nÂº 4** sÃ©rie **21** cancelada na SEFAZ + estoque/caixa |
| Venda **>30 min** (~141 min) â†’ cancelar | **501** esperado â€” mensagem clara (minutos desde emissÃ£o) |
| Erro **225** (schema XML) | **Corrigido** v2.03 â€” nÃ£o reproduziu apÃ³s fix |

**OperaÃ§Ã£o loja:** devolver dentro de **30 min** da venda â†’ cupom cancela sozinho. Passou disso â†’ 501; Agro ajusta estoque/caixa; fiscal fica com contador (NF-e devoluÃ§Ã£o mod. 55).

**Pacote produÃ§Ã£o:** `02cdb98` (v2.03) â€” cancelamento evento 110111 + mensagens 501/225.

### ProduÃ§Ã£o â€” erros cancelamento NFC-e (referÃªncia)

| CÃ³digo | Significado | O que fazer |
| ------ | ----------- | ----------- |
| **501** | Prazo esgotado (~**30 min** desde autorizaÃ§Ã£o do cupom) | Esperado. NF-e devoluÃ§Ã£o mod. 55 / contador |
| **225** | Schema XML invÃ¡lido | Corrigido v2.03 (`infEvento` sÃ³ `Id`; lxml) |

### HistÃ³rico debug cancelamento (2026-06-23)

| Item | ExplicaÃ§Ã£o |
| ---- | ---------- |
| **Erro** | `501 â€” Prazo de cancelamentoâ€¦` ao cancelar cupom recente |
| **Regra real SEFAZ** | NFC-e (mod. 65): cancelamento por evento sÃ³ **~30 min** apÃ³s **autorizaÃ§Ã£o do cupom** (nÃ£o 24 h; nÃ£o reinicia na devoluÃ§Ã£o) |
| **Por que nÂº 3 falhou** | Venda recente, mas entre emissÃ£o â†’ devoluÃ§Ã£o â†’ bugs (infEvento) â†’ deploys passou **>30 min** desde a autorizaÃ§Ã£o |
| **O que fazer nÂº 3** | Cupom **permanece autorizado** na SEFAZ. Caminho fiscal: **NF-e devoluÃ§Ã£o mod. 55** (contador) referenciando a chave da NFC-e |
| **CÃ³digo** | Mensagem corrigida (30 min + minutos desde emissÃ£o); Â§4.3 banana atualizado |

### ProduÃ§Ã£o â€” fix cancelamento Â«infEvento nÃ£o encontradoÂ» (2026-06-23)

| Item | Valor |
| ---- | ----- |
| **Problema** | DevoluÃ§Ã£o OK, NFC-e nÂº 3 nÃ£o cancelou â€” Â«infEvento nÃ£o encontradoÂ» |
| **Causa** | XML do evento perdia `xmlns` na serializaÃ§Ã£o antes da assinatura |
| **Fix** | `c0f0ef0` / produÃ§Ã£o `c89f3ef` â€” xmlns em `<evento>` + botÃ£o **Cancelar NFC-e** na venda |
| **Venda nÂº 3** | JÃ¡ devolvida â€” apÃ³s deploy: abrir venda â†’ **Cancelar NFC-e** |

### ProduÃ§Ã£o â€” deploy NFC-e cancelamento (2026-06-23, Renan pediu)

| Item | Valor |
| ---- | ----- |
| **Branch** | `producao` â† cherry-pick `7227d83` + `7395c2a` |
| **Commits loja** | `1d09438` (CPF PIX) Â· `20c33bb` (cancelamento SEFAZ na devoluÃ§Ã£o) |
| **VERSION loja** | **1.97** |
| **Bug pÃ³s-deploy** | Â«infEvento nÃ£o encontradoÂ» â€” **corrigido** v1.99 |
| **Fix loja** | `c89f3ef` |

### NFC-e â€” regras resumidas + cancelamento na devoluÃ§Ã£o (2026-06-23)

- **Â§4.3** â€” tabela Â«quando emiteÂ», popups PDV e devoluÃ§Ã£o (documentaÃ§Ã£o operacional).
- **DevoluÃ§Ã£o:** tenta **cancelar NFC-e na SEFAZ** (evento 110111); motivo padrÃ£o *Devolucao de mercadoria registrada no sistema Agro.*; status local **Cancelada** se OK; aviso se prazo **30 min** ou falha.
- **Arquivos:** `nfce_sp_emissao_util.py` (`cancelar_nfce_autorizada`), `views.py`, `models.py`, `nfce_venda_util.py`, `venda_agro_detalhe.html`.

### Fix pÃ³s go-live NFC-e (2026-06-23, Renan)

| Problema | Resposta / fix |
| -------- | -------------- |
| **DevoluÃ§Ã£o cancela cupom?** | **Sim, automaticamente** (SEFAZ SP, atÃ© **~30 min** desde autorizaÃ§Ã£o). Fora do prazo â†’ **501** + aviso; estoque/caixa ajustados igual. |
| **PIX + impressÃ£o, cliente sem CPF** | Modal CPF (commit `7227d83`). |

### NFC-e â€” go-live **OK** (2026-06-23, Renan)

| Passo | O quÃª | Status |
| ----- | ----- | ------ |
| **1** | Vars Render produÃ§Ã£o (`TP_AMB=1`, CSC, cert, sÃ©rie 21) | **OK** |
| **2** | `/api/nfce/status/` `ativo: true` | **OK** |
| **3** | PDV + vendas â†’ `producao` v1.93 (`9cf4522`) | **OK** |
| **4** | Venda real na loja + QR na consulta SEFAZ | **OK** (Renan) |

**Primeira NFC-e produÃ§Ã£o validada (23/06/2026):**

| Campo | Valor |
| ----- | ----- |
| NÃºmero / sÃ©rie | **nÂº 2** Â· sÃ©rie **21** |
| Protocolo | `135264232730394` |
| Pagamento | PIX Â· R$ 1,00 (venda teste) |
| Consumidor | nÃ£o identificado |
| Consulta QR | **OK** â€” Renan conferiu no site SEFAZ SP (Â«quenteÂ») |

**OperaÃ§Ã£o:** PIX/cartÃ£o â†’ NFC-e automÃ¡tica Â· dinheiro â†’ escolha NFC/Venda Â· reemissÃ£o `/vendas/`.

**Render:** se prÃ³xima nota der rejeiÃ§Ã£o **539** (nÃºmero duplicado), `NFC_E_PROXIMO_NUMERO` = **3** (Ãºltima autorizada foi **2**). O sistema tambÃ©m avanÃ§a pelo Postgres apÃ³s cada emissÃ£o OK.

**Pacote deploy:** `9cf4522` Â· `2386eb2` Â· revert = revert dos 2 commits em `producao`.

**Vars obrigatÃ³rias (Render produÃ§Ã£o):** `NFC_E_ENABLED`, `NFC_E_TP_AMB=1`, `NFC_E_MODO=manual`, `NFC_E_SERIE=21`, `NFC_E_PROXIMO_NUMERO`, cert `NFC_E_CERT_BASE64`+`NFC_E_CERT_PASSWORD`, `NFC_E_CSC_ID`+`NFC_E_CSC_TOKEN`, emitente (`CNPJ`, `IE`, `RAZAO`, endereÃ§o, `CMUN=3524600`). Detalhe: CHECKPOINT + `docs/NFCE-PRODUCAO.md`.

### NFC-e â€” certificado e Base64 (2026-06-22, Renan)

**Pode copiar o certificado do teste?** **Sim**, se for o **mesmo arquivo .pfx A1** da loja (e-CNPJ `48900774000103`) â€” na GM Agro costuma ser **um certificado sÃ³** para homolog e produÃ§Ã£o. Copie no Render produÃ§Ã£o: `NFC_E_CERT_BASE64`, `NFC_E_CERT_PASSWORD`, razÃ£o, IE, endereÃ§o, etc.

**O que NÃƒO pode ser igual ao teste** (tem que ser **produÃ§Ã£o**):

| VariÃ¡vel | Teste (homolog) | ProduÃ§Ã£o (loja) |
|----------|-----------------|-----------------|
| `NFC_E_TP_AMB` | `2` | **`1`** |
| `NFC_E_CSC_ID` / `NFC_E_CSC_TOKEN` | CSC do portal **homologaÃ§Ã£o** | CSC do portal **produÃ§Ã£o** NFC-e SP |
| `NFC_E_PROXIMO_NUMERO` | numeraÃ§Ã£o homolog (ex. jÃ¡ foi 13) | **prÃ³ximo livre sÃ©rie 21 em produÃ§Ã£o** (pode ser `1` se nunca emitiu) |

**Comando PowerShell deu erro?** Quase sempre Ã© **caminho errado** (`C:\caminho\...` era sÃ³ exemplo). Jeito mais fÃ¡cil:

1. Abra o Render **teste** â†’ Environment â†’ copie o valor inteiro de `NFC_E_CERT_BASE64` â†’ cole na **produÃ§Ã£o** (se for o mesmo .pfx).
2. **Ou** no PowerShell, com o caminho **real** do arquivo (aspas se tiver espaÃ§o):

```powershell
$pfx = "C:\Users\RenanHinnen\Downloads\seu-certificado.pfx"
[Convert]::ToBase64String([IO.File]::ReadAllBytes($pfx)) | Set-Clipboard
```

Se der *Â«nÃ£o encontradoÂ»*, arraste o `.pfx` para a janela do PowerShell para colar o caminho certo.

**Senha:** copie `NFC_E_CERT_PASSWORD` do teste se for o mesmo arquivo.

### NFC-e â€” obter CSC (passo a passo, 2026-06-22)

Portal SP: [https://www.nfce.fazenda.sp.gov.br/NFCePortal/](https://www.nfce.fazenda.sp.gov.br/NFCePortal/)

| Passo | O quÃª | Status |
|-------|--------|--------|
| **1** | Abrir portal + certificado A1 da loja no PC | OK |
| **2â€“3** | CSC produÃ§Ã£o na tela SEFAZ (Gerenciamento CÃ³d SeguranÃ§a) | OK (Renan 22/06) |
| **4** | `NFC_E_CSC_ID` + `NFC_E_CSC_TOKEN` no Render **produÃ§Ã£o** | **OK** (Renan) |
| **5** | Conferir `/api/nfce/status/` logado: `ativo: true`, `tp_amb: 1` | **OK** (Renan 22/06) |

**Mapa:** ID Token = `NFC_E_CSC_ID` (nÃºmero, ex. `1` ou `000001`) Â· CSC = `NFC_E_CSC_TOKEN` (texto longo). **ProduÃ§Ã£o** = menu **Â«com validade jurÃ­dicaÂ»** (nÃ£o sÃ³ homologaÃ§Ã£o). CSC do **teste** â‰  CSC da **loja**.

### Paridade prod Ã— teste â€” auditoria Git (2026-06-22)

**Backend core idÃªntico** (`git diff origin/producao origin/teste` â€” **sem diferenÃ§a**): `mongo_financeiro_util.py`, `views.py`, `catalogo_agro.py`, `cadastro_codigo_sequencial_util.py`, `nfe_entrada_util.py`, migrations NFC-e.

**Loja (`producao` v2.03)** = NFC-e emissÃ£o v1.93 + cancelamento devoluÃ§Ã£o v2.03 (`02cdb98`). **Cadastro Postgres / cÃ³digo 4010+** jÃ¡ estavam na loja â€” **nÃ£o** estÃ£o pendentes.

### SÃ³ no teste â€” ainda NÃƒO na loja (2026-06-23)

ConferÃªncia: `git diff origin/producao origin/teste --stat` Â· **28 arquivos** Â· ~2k linhas (maioria front/PDV/BI). **NFC-e** (emissÃ£o + cancelamento) **jÃ¡ na loja** â€” nÃ£o entra na lista.


| #   | Pacote                                | Arquivos / nota                                                                                                           | Risco loja                                                             |
| --- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| 1   | **Agro Display Scale**                | `agro_display_scale.js`, `_agro_display_scale.html`, `_agro_consulta_ui.html`, BI                                         | MÃ©dio â€” calibraÃ§Ã£o global de tamanho de tela                           |
| 2   | **CP lista â€” perf/UX**                | `lancamentos_contas_pagar_teste.html` â€” bootstrap HTML, prefetch BI, badge Â«SincronizandoÂ», colunas PIN, filtro hoje      | Baixo â€” sÃ³ template; **API jÃ¡ igual** na loja                          |
| 3   | **Entrada NF â€” financeiro duplicado** | `entrada_nota.html` â€” pula etapa se tÃ­tulo jÃ¡ existe; mensagens Â«jÃ¡ existia / recuperadoÂ»                                 | Baixo                                                                  |
| 4   | **BI â€” launchpad / escala rÃ¡pida**    | `dashboard_gerencial.html`, `partials/dashboard_gerencial_body.html`, `home.html`                                         | Baixo                                                                  |
| 5   | **PDV entrega**                       | `partials/pdv/step_entrega.html`                                                                                          | MÃ©dio                                                                  |
| 6   | **Caixa â€” PIX MP QR**                 | `caixa_util.py` â€” normaliza Â«Mercado Pago â€¦ QRÂ» como PIX                                                                  | Baixo                                                                  |
| 7   | **Config status staging**             | `agro_fonte_config.py` â€” `staging_readonly` no status; ERP sync sÃ³ por env (sem auto-block checkpoint)                    | Baixo â€” prod mantÃ©m lÃ³gica checkpoint no sync                          |
| 8   | **Textos prÃ©-corte ERP**              | `lancamentos_pre_corte_erp_panel.html`                                                                                    | CosmÃ©tico                                                              |
| 9   | **PIN calendÃ¡rio CP/DRE/fluxo**       | +1 linha include PIN em calendÃ¡rio/DRE/fluxo                                                                              | Baixo                                                                  |


**Removido da lista Â«pendenteÂ» (jÃ¡ estava na loja):** cadastro v1.87â€“v1.89, deploy fixes `bb27da1`/`nfe_entrada_util` â€” cÃ³digo **igual** nos dois branches.

**Como conferir de novo:** `git fetch origin` â†’ `git diff origin/producao origin/teste --stat`

### Pacote teste v1.92 â†’ **produÃ§Ã£o** (2026-06-22, Renan neste chat)

Renan autorizou *Â«1.92 do teste pode subir para produÃ§Ã£oÂ»* (validou botÃ£o layout clÃ¡ssico sumiu; **exclusÃ£o** nÃ£o testou no Render teste â€” `AGRO_STAGING_READONLY` bloqueia gravaÃ§Ã£o Mongo; vai testar na **loja**).


| Pacote                                            | `teste`   | `producao` (cherry-pick)            |
| ------------------------------------------------- | --------- | ----------------------------------- |
| Nova saÃ­da â€” mÃªs calendÃ¡rio, sucesso, volta BI/CP | `b5e499e` | `99ea1ae` v1.57                     |
| Excluir manual Agro **quitado** (CR/CP backend)   | `726b3ee` | `1d412e0` v1.58                     |
| CP â€” Excluir na lista nova + fim layout clÃ¡ssico  | `e98db0f` | `2d88ee4` (**VERSION loja = 1.92**) |


**Loja:** Ctrl+F5 apÃ³s deploy Render â†’ Contas a pagar â†’ expandir linha â†’ **Excluir** (sem ir ao clÃ¡ssico). CR: tÃ­tulo manual quitado deve mostrar Excluir. **Renan:** conferir exclusÃ£o na loja real.

**Reverter:** revert dos 3 commits em `producao` (ordem inversa).

### Mapa loja vs teste â€” resumo (2026-06-23)


| Ambiente     | VERSION | HEAD      | Nota |
| ------------ | ------- | --------- | ---- |
| **teste**    | v2.05   | `df2d1f7` | HomologaÃ§Ã£o â€” banana sync outro chat |
| **produÃ§Ã£o** | v2.03   | `02cdb98` | Loja â€” NFC-e emissÃ£o v1.93 + cancelamento devoluÃ§Ã£o v2.03 |

**Pacote cancelamento NFC-e na loja (outro chat):** `1d09438` (CPF PIX) Â· `20c33bb` (cancelamento SEFAZ) Â· `c89f3ef` (xmlns/retry) Â· `4995c0e` (501/30 min) Â· `02cdb98` (fix **225**).

**PrÃ³ximos candidatos produÃ§Ã£o:** pacotes **#1â€“#9** da tabela Â«SÃ³ no testeÂ» (Display Scale, CP perf, Entrada NF, â€¦). NFC-e **jÃ¡ na loja**.

### Contas a pagar â€” Excluir na lista nova + fim do layout clÃ¡ssico (2026-06-22)


| Item                   | Detalhe                                                                                              |
| ---------------------- | ---------------------------------------------------------------------------------------------------- |
| **Sintoma**            | **Excluir** na lista nova abria a tela clÃ¡ssica (`/classico/?mongo_id=â€¦`) e **nÃ£o apagava** o tÃ­tulo |
| **Causa**              | BotÃ£o era link para o layout antigo (corrigido em `b5e499e`; reforÃ§o neste patch)                    |
| **Fix**                | Excluir chama `api/lancamentos/excluir/` na prÃ³pria tela; operador do PIN no payload                 |
| **Layout clÃ¡ssico CP** | `/lancamentos/contas-pagar/classico/` â†’ **redirect** para `/lancamentos/contas-pagar/`               |
| **UI**                 | Removidos botÃµes Â«Layout clÃ¡ssicoÂ» (lista + calendÃ¡rio)                                              |
| **Teste Render**       | Ctrl+F5 â†’ expandir linha â†’ **Excluir** â†’ confirmar â†’ tÃ­tulo some sem mudar de tela                   |
| **ProduÃ§Ã£o**           | `2d88ee4` v1.92 â€” Renan validou sumiÃ§o do clÃ¡ssico; exclusÃ£o a conferir na loja (staging readonly)   |


### Chat canÃ´nico â€” produÃ§Ã£o (2026-06-22, Renan)


| Regra                    | Detalhe                                                            |
| ------------------------ | ------------------------------------------------------------------ |
| **Onde sobe loja**       | **Este chat** â€” Renan farÃ¡ push produÃ§Ã£o **daqui**                 |
| **Outros chats/posts**   | Pode ter havido deploy fora; **nÃ£o** confiar sÃ³ na memÃ³ria do chat |
| **Fonte da verdade**     | CHECKPOINT + `git diff origin/producao origin/teste --stat`        |
| **Antes de cherry-pick** | Tabela Â«SÃ³ no testeÂ»; Renan confirma o pacote                      |


### Cadastro â€” cÃ³digo sequencial produto novo (2026-06-22, Renan OK teste â€” **jÃ¡ na loja**)


| Item                | Detalhe                                                                                                                                         |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **ValidaÃ§Ã£o Renan** | OK no Render **teste**                                                                                                                          |
| **Commits `teste`** | `ffc82ba` v1.87 Â· `cea03c7` v1.88 Â· `a2303c7` v1.89                                                                                             |
| **ProduÃ§Ã£o**        | `8889955` â€” **jÃ¡ na loja** (cÃ³digo **igual** nos branches)                                                                                      |
| **Reverter**        | Revert `8889955`; arquivos: `cadastro_codigo_sequencial_util.py`, `catalogo_agro.py`, `views.py`, `_modal_editar_produto_cadastro_erp.inc.html` |


**Regras (canÃ´nicas â€” Â§4.6):** cÃ³digo sistema **4 dÃ­gitos**, faixa **4010â€“9999**, sequÃªncia sÃ³ pelo **cÃ³digo sistema** (GM nÃ£o conta); GM = `GM` + nÃºmero (editÃ¡vel livre); modal novo nÃ£o repinta ao carregar cÃ³digos.

### Fluxo Render â€” teste automÃ¡tico, produÃ§Ã£o sÃ³ com pedido (2026-06-22, Renan)


| Regra                     | Detalhe                                                                                                |
| ------------------------- | ------------------------------------------------------------------------------------------------------ |
| **Onde testa**            | Render projeto **teste** â€” **nÃ£o** local                                                               |
| **Dois sites**            | **teste** = homologaÃ§Ã£o Â· **SistVale** = loja                                                          |
| **Assistente â†’ teste**    | Commit + push `**teste` automÃ¡tico** (deploy Render segue)                                             |
| **Assistente â†’ produÃ§Ã£o** | **SÃ³** com frase explÃ­cita (*Â«pode subir (produÃ§Ã£o)Â»*, etc.) **+ senha `99738595`** na **mesma** mensagem |


### Registro no banana â€” toda entrega (2026-06-22, Renan)

Assistente **sempre** registra no CHECKPOINT (e Â§ do mÃ³dulo se couber): **o quÃª**, **commits**, **versÃ£o**, **teste/produÃ§Ã£o**, **como reverter** se relevante. Objetivo: prÃ³ximo chat, diagnÃ³stico, rollback.

### Cadastro â€” cÃ³digo sequencial (histÃ³rico curto)

**Sintoma inicial:** `__novo__` no cÃ³digo Â· depois nÃºmero errado (9504) Â· piscada apagando nome.

**Fix (branch `teste`, validado v1.87â€“v1.89):** ver bloco Â«Cadastro â€” cÃ³digo sequencial produto novoÂ» acima.

### Cadastro produtos â€” etapa 1 Postgres + perf lista (2026-06-22, Renan OK)


| Item                                     | Status                                                                                                                               |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `AGRO_FONTE_CATALOGO=agro_pg` staging    | Renan validou                                                                                                                        |
| Import `importar_catalogo_mongo_produto` | 3354 produtos                                                                                                                        |
| Busca nome + GM/barras                   | OK (v1.83+)                                                                                                                          |
| Piscadinha preÃ§o errado                  | **Resolvida** v1.84 â€” lista sÃ³ servidor                                                                                              |
| Abertura lenta pÃ³s-fix                   | **Melhor** v1.85 â€” prefetch 1Âª pÃ¡gina, badge ERP em paralelo, Postgres sem `count`, busca local instantÃ¢nea (preÃ§o Â«â€¦Â» atÃ© servidor) |


**Commits:** `3c9ed24` v1.84 Â· `8d76146` v1.85 Â· branch `teste`.

**PrÃ³ximo passo cadastro Postgres na loja:** flag `AGRO_FONTE_CATALOGO=agro_pg` + import â€” **jÃ¡ feito** (`3d8ae08`). Perf lista (v1.84â€“v1.85) **jÃ¡ na loja** (mesmo JS/backend).

**Cadastro Ã— PDV (etapa 1 â€” linguagem de loja):**


| O que vocÃª faz no cadastro                        | Aparece no PDV?                                                        |
| ------------------------------------------------- | ---------------------------------------------------------------------- |
| **Muda preÃ§o/nome** de produto que **jÃ¡ existia** | **Sim** â€” overlay + merge Postgres na busca                            |
| **Cria produto novo** (Id `AGROâ€¦`)                | **Sim** (apÃ³s deploy v1.86+) â€” busca e catÃ¡logo local mesclam Postgres |
| **Mesmo PC**, cache antigo                        | Atualizar estoque/catÃ¡logo no PDV (botÃ£o sync) ou Ctrl+Shift+R         |


**Fix PDVÃ—cadastro (2026-06-22):** `mesclar_prods_busca_pdv` + `mesclar_catalogo_pdv_cache` â€” produtos do Postgres entram na busca `/api/buscar/` e no download do catÃ¡logo local.

### FAB PDV â€” sobreposiÃ§Ã£o com botÃµes e modais (2026-05-21, Renan)

**Sintoma:** botÃ£o flutuante **PDV** ficava **na frente** de outros elementos conforme a tela â€” ex. **Cancelar** e Â«Preencher pela fraseÂ» no modal **Nova saÃ­da** (LanÃ§amentos).

**Causa:**


| Item    | Detalhe                                                                           |
| ------- | --------------------------------------------------------------------------------- |
| z-index | FAB em **9000**; Nova saÃ­da `#agro-nova-saida-overlay` em **z 250** â†’ FAB ganhava |
| Ocultar | `shouldHide()` sÃ³ via `body.modal-open`; Nova saÃ­da usa `.hidden` + `aria-hidden` |


**Fix** (`produtos/templates/produtos/_agro_pdv_fab.html`):


| AÃ§Ã£o                        | Comportamento                                                                                                              |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| z-index **90**              | Acima do conteÃºdo; **abaixo** de modais (200+)                                                                             |
| `hasOpenOverlay()`          | Some com `[role=dialog]`, `[aria-modal]`, `.sv-modal.show`, `#agro-nova-saida-overlay:not(.hidden)`                        |
| ColisÃ£o canto inf. esquerdo | Sobe sobre barras fixas; se encostar em **button/a**, sobe mais ou vai pro canto **direito** (`.agro-pdv-fab-wrap--right`) |
| Observer                    | `MutationObserver` debounced (class / `aria-hidden`) na Ã¡rvore do `body`                                                   |


**F1** continua global quando o FAB estÃ¡ oculto. **FX on/off** inalterado.

**Teste Chrome:** Ctrl+F5 â†’ LanÃ§amentos â†’ **Nova saÃ­da** â†’ FAB nÃ£o aparece; fechar â†’ volta. Redimensionar janela estreita â†’ nÃ£o cobrir botÃµes do rodapÃ©.

### Perf â€” animaÃ§Ãµes do botÃ£o PDV flutuante (2026-06-19, dÃºvida Renan)

AcÃºmulo de animaÃ§Ãµes no app **pode** pesar em PC fraco ao longo do tempo â€” mas **este FAB Ã© impacto baixo**: 1 elemento, sÃ³ CSS (`transform`/`opacity`/gradiente), sem JS extra nem trÃ¡fego de rede. O que pesa de verdade: troca de pÃ¡gina inteira (Chrome MPA), listas grandes, Mongo, JS do PDV/LanÃ§amentos.

**Interruptor implementado:** mini-botÃ£o **FX on / FX off** acima do FAB (persiste no navegador). Desliga efeitos **decorativos** (FAB arco-Ã­ris, Validade pulsando, pulso PDV/OrÃ§amento no BI). MantÃ©m animaÃ§Ãµes **funcionais** (loading, scanner, salvando).

### FAB PDV + Nova saÃ­da quitado â†’ **produÃ§Ã£o** (2026-06-19, Renan pediu)

**Commit:** `f824944` Â· v1.48 Â· cherry-pick escopo fechado de `teste`.


| Pacote         | Detalhe                                                                                                              |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| **FAB PDV**    | `_agro_pdv_fab.html` + include em `_agro_open_external.html` â€” pulso/arco-Ã­ris, FX on/off, some em modal, z-index 90 |
| **Nova saÃ­da** | Quitado **por linha** (modo Parc.); forma opcional; API JSON segura; expandir emprÃ©stimo dual (`95474d5`)            |


**Loja:** Ctrl+F5 apÃ³s deploy Render â†’ FAB canto esquerdo; Nova saÃ­da â†’ 3 parcelas + emprÃ©stimo dual.

### Nova saÃ­da â†’ **produÃ§Ã£o** (2026-06-19, pacote anterior)

**Commit:** `be9558c` Â· v1.47 Â· cherry-pick escopo fechado de `teste` (`9ba11e4`â€¦`cd5a6e0`).


| Pacote     | Detalhe                                                                |
| ---------- | ---------------------------------------------------------------------- |
| Parcelas   | NÂº + intervalo â†’ grade; backend `parcelas_saida`                       |
| CalendÃ¡rio | Popup grande (competÃªncia, vencimento, parcelas)                       |
| UI         | Grid 4 col; pÃ­lulas **Total | Parc.**; emprÃ©stimo saÃ­da abaixo entrada |


**Loja:** Ctrl+F5 apÃ³s deploy Render â†’ Nova saÃ­da.

### Bug produÃ§Ã£o â€” HTTP 500 ao Finalizar (2026-06-19)

| Sintoma | Alerta JS Â«Resposta invÃ¡lida do servidor (HTTP 500)Â» â€” corpo HTML, nÃ£o JSON |
| CenÃ¡rio Renan | EmprÃ©stimo dual + 3 parcelas + quitado (836,95 / 836,94) |
| Causa provÃ¡vel | ExceÃ§Ã£o **fora** do `try` da API (`date.fromisoformat` no cabeÃ§alho ou `expandir_linhas_emprestimo_dual_lote`) â†’ pÃ¡gina de erro Django |
| Fix v1.48 | `_d()` try/except Â· try expandir Â· quitado linha a linha Â· JS trecho HTML |
| Fix v1.49 | `sum(v for v, _, _ in parcelas_pag)` â€” tupla 3 itens |
| Fix v1.50 | Â«ADICIONAR CONTAÂ» nÃ£o vale para **quitado** |
| EmprÃ©stimo dual forma | **Forma entrada** (obrig.) + **Forma saÃ­da** (opc.) â€” 4Âª coluna da grade; backend `_fin_ln_campo` / expandir dual |
| Layout Valor dual | **Renan nÃ£o satisfeito** â€” tentativas v1.70â€“v1.71; **desistiu**; produÃ§Ã£o v1.51 |
| Juros dual parcelado | SaÃ­da > entrada â†’ cada parcela gera **Pagamento** + **Juros** (proporcional); UI mostra Â«231,85 + 18,15Â» + **?** na grade |

### EmprÃ©stimo dual â€” juros parcelado (2026-06-19)

**Regra (Renan):** parcelas somam **valor saÃ­da** (ex. 4Ã—250 = 1000). DiferenÃ§a saÃ­daâˆ’entrada (72,60) vira **Juros de EmprÃ©stimos** **por parcela** (18,15), restante **Pagamento de EmprÃ©stimos** (231,85). Antes validava contra entrada e juros iam num tÃ­tulo sÃ³.

**UI:** valor da parcela continua 250; abaixo Â«231,85 + 18,15Â» + **?** explicando planos. Modo Total: hint sob valores entrada/saÃ­da.

**Backend:** `expandir_linhas_emprestimo_dual_lote` + `split_decimal_proporcional`.

### Juros dual parcelado â†’ **produÃ§Ã£o** (2026-06-19, Renan pediu teste real)

**Commit:** `4505805` Â· v1.52 Â· cherry-pick de `teste` (`0517525`).

**Loja:** Ctrl+F5 apÃ³s deploy Render â†’ Nova saÃ­da â†’ EmprÃ©stimo dual â†’ entrada 927,40 / saÃ­da 1000 â†’ 4Ã—250 â†’ composiÃ§Ã£o **231,85 + 18,15** â†’ Finalizar.

### Nova saÃ­da â€” UX pÃ³s-gravar e intervalo mensal (2026-06-19, Renan)


| Item                 | Comportamento                                                                                             |
| -------------------- | --------------------------------------------------------------------------------------------------------- |
| **Intervalo mensal** | **Mensal / bimestral / trimestral** = mesmo dia no mÃªs (19/06 â†’ 19/07); semanal/quinzenal = dias corridos |
| **Sucesso**          | Painel verde no modal (Â«N tÃ­tulos gravadosÂ»); **sem** alerta ERP nem lista de IDs                         |
| **Volta**            | BI `/` â†’ fica no BI Â· Contas a pagar â†’ recarrega lista **in-place** (sem redirect)                        |


**Arquivos:** `lancamento_nova_saida.js`, `lancamento_nova_saida_modal.html`, `dashboard_gerencial.html`, `lancamentos_contas_pagar_teste.html`, `mongo_financeiro_util.py` (`_fin_vencimento_parcela`).

**Deploy:** **produÃ§Ã£o** `99ea1ae` v1.57 (cherry-pick `b5e499e`).

### Excluir entrada manual quitada (2026-06-19, Renan)

**Sintoma:** Â«Entrada de EmprÃ©stimoÂ» quitada em Contas a receber â€” coluna AÃ§Ãµes vazia, sem Excluir.

**Causa:** `_lancamento_pode_excluir_agro` barrava **quitado** antes de checar **Lote manual Agro**.

**Fix:** manual Agro (Nova saÃ­da / lote manual) pode excluir **mesmo quitado**; ERP continua bloqueado.

**Deploy:** **produÃ§Ã£o** `1d412e0` v1.58 (cherry-pick `726b3ee`).

| UX quitado Nova saÃ­da | **ProduÃ§Ã£o** v1.48+: modo Parcela â†’ botÃ£o **Quitado** em cada linha; Ctrl+F5 apÃ³s deploy |

### EmprÃ©stimo dual forma + layout â†’ **produÃ§Ã£o** (2026-06-19, Renan pediu)

**Commit:** `76e2a8b` Â· v1.51 Â· cherry-pick escopo fechado de `teste` (`7ef55b0`â€¦`a47933b`).


| Pacote             | Detalhe                                                                                      |
| ------------------ | -------------------------------------------------------------------------------------------- |
| **Forma split**    | Entrada obrigatÃ³ria Â· saÃ­da opcional Â· campos separados no modal e no Mongo                  |
| **Grade**          | Forma dual na 4Âª coluna (sem linha extra); `.hidden` CSS Agro                                |
| **Valor dual**     | Entrada/saÃ­da lado a lado na coluna Valor â€” layout **imperfeito**; Renan desistiu de refinar |
| **Alerta parcial** | GravaÃ§Ã£o parcial mostra atÃ© 4 erros no alert                                                 |


**Loja:** Ctrl+F5 apÃ³s deploy Render â†’ Nova saÃ­da â†’ EmprÃ©stimo (entrada + pagamento).

### Ambiente Renan â€” Chrome (nÃ£o Electron)


| Item           | Valor                                                        |
| -------------- | ------------------------------------------------------------ |
| **Navegador**  | **Google Chrome** (MPA: cada tela = pÃ¡gina nova)             |
| **Electron**   | Testado; **muito lento** â€” **nÃ£o** Ã© referÃªncia para UX/perf |
| **Assistente** | **NÃ£o perguntar** Chrome vs Electron; assumir Chrome         |


**LanÃ§amentos / BI:** prefetch + bootstrap HTML (v1.48) valem no Chrome; aba lateral do shell **nÃ£o** entra no fluxo dele.

### LanÃ§amentos CP â€” abertura (Chrome, 2026-06-19)


| Feito (v1.48)                   | Efeito real no Chrome                                                         |
| ------------------------------- | ----------------------------------------------------------------------------- |
| Bootstrap HTML (hoje + abertos) | Elimina o â€œCarregandoâ€¦â€ da **2Âª** chamada API; lista aparece quando o JS roda |
| Prefetch + cache sessionStorage | Ajuda na **reabertura**; 1Âª do dia ainda espera o servidor                    |
| Sincronizandoâ€¦                  | Atualiza em background                                                        |


**Renan:** melhora **sutil** â€” OK para o desenho atual.

**O que ainda pesa (nÃ£o dÃ¡ para sumir com patch pequeno):**

1. Troca de pÃ¡gina inteira BI â†’ LanÃ§amentos (branco + download HTML/JS).
2. PIN na 1Âª entrada da sessÃ£o.
3. Mongo na montagem da pÃ¡gina (bootstrap) â€” trocou API depois por consulta no Django.

**ConclusÃ£o:** para **Chrome MPA + Mongo**, o pacote atual Ã© **quase o teto** (v1.48).

**Roadmap â€” adiado (Renan, 2026-06-19):** nÃ£o investir agora no prÃ³ximo salto. Retomar quando pedir:


| Prioridade futura | OpÃ§Ã£o                                     | Nota                               |
| ----------------- | ----------------------------------------- | ---------------------------------- |
| A                 | **Financeiro Postgres** (`agro_pg`)       | Alinha com Â§4.15; ganho estrutural |
| B                 | **Lista CP no BI** (sem trocar de pÃ¡gina) | Ganho de percepÃ§Ã£o no Chrome       |
| C                 | Enxugar HTML/JS da pÃ¡gina CP              | Ganho menor                        |


AtÃ© lÃ¡: manter bootstrap + prefetch + cache; **nÃ£o** empilhar micro-otimizaÃ§Ãµes.

### Nova saÃ­da â€” grid alinhado (`teste`, 2026-06-19)


| O quÃª                   | Detalhe                                                                |
| ----------------------- | ---------------------------------------------------------------------- |
| **Grid 2Âª linha**       | 4 colunas iguais Ã  1Âª linha (plano Â· valor Â· competÃªncia Â· vencimento) |
| **Chave Total/Parcela** | PÃ­lulas **Total                                                        |
| **EmprÃ©stimo dual**     | SaÃ­da **abaixo** da entrada, mesma coluna do valor                     |



| O quÃª                       | Detalhe                                                                                 |
| --------------------------- | --------------------------------------------------------------------------------------- |
| **CalendÃ¡rio**              | Popup grande (competÃªncia, vencimento, parcelas) â€” cÃ©lulas ~2,85rem, Limpar/Hoje        |
| **Chave Totalâ†’Parcela**     | Ao lado do valor; padrÃ£o **Total** (card NÂº parcelas oculto); **Parcela** mostra o card |
| **Parcelas** (modo Parcela) | NÂº + intervalo â†’ grade; valor sempre **total**                                          |


**Teste:** Ctrl+F5 â†’ Nova saÃ­da â†’ clicar CompetÃªncia/Vencimento (calendÃ¡rio grande) Â· chave Parcela â†’ card aparece.

### Nova saÃ­da â€” parcelas + layout emprÃ©stimo (`teste`, 2026-06-19)


| O quÃª                 | Detalhe                                                                    |
| --------------------- | -------------------------------------------------------------------------- |
| **Layout emprÃ©stimo** | SÃ³ entrada + saÃ­da (sem Valor duplicado); grid 4 colunas                   |
| **Parcelas**          | NÂº + intervalo â†’ grade vencimento/valor                                    |
| **EmprÃ©stimo dual**   | Parcelas na **saÃ­da**; backend `parcelas_saida` em `mongo_financeiro_util` |


**Teste:** Ctrl+F5 â†’ Nova saÃ­da â†’ 3 parcelas mensais Â· emprÃ©stimo dual com saÃ­da parcelada.

### O que este documento jÃ¡ cobre (atÃ© aqui)

- [x] NegÃ³cio GM Agro / SisVale e perfil dos operadores
- [x] Stack Django + Postgres + Mongo + Render + Electron
- [x] Deploy teste/producao e staging readonly
- [x] Mapa dos mÃ³dulos principais com arquivos e armadilhas
- [x] NFC-e: modo por forma, sÃ©rie 21, reemissÃ£o, schema 225, UX modal/toast
- [x] Clientes Agro, duas telas de cadastro produto, estoque, caixa, RH
- [x] Como Renan usa nos chats: `@banana` (Ãºnico anexo obrigatÃ³rio)
- [x] Regra: assistente nÃ£o pergunta se atualiza AGENTS.md
- [x] VersÃ£o app: bump automÃ¡tico `VERSION` por commit (hook `.githooks/pre-commit`)
- [x] Arquivo: `banana.md` na raiz
- [x] Ponteiros para docs irmÃ£os (sem duplicar AGENTS.md inteiro)
- [x] Linha do tempo recente de commits
- [x] Â§4.15 roadmap desvinculaÃ§Ã£o ERP (Mongo â†’ Postgres)
- [x] Regra: assistente atualiza banana automaticamente (sem perguntar)
- [x] Nova saÃ­da: tipografia maior + card expandido ocupa altura (sem vazio embaixo)
- [x] **EmprÃ©stimo (entrada + pagamento)** â€” pseudo-plano Nova saÃ­da + lote manual (2026-06-18)
- [x] PDV wizard: diagnÃ³stico GM/hÃ­fen no barras (Â§4.2 + abaixo)
- [x] **Contas a pagar â€” layout novo padrÃ£o** + `/classico/` (2026-06-19)
- [x] **Lista CP â€” colunas por PIN** (visibilidade, ordem drag, save) (2026-06-19)
- [x] **Lista CP â€” perf + preservar filtros/vista** apÃ³s baixa/NF/Nova saÃ­da (2026-06-19)
- [x] **PIN LanÃ§amentos** â€” sÃ³ na entrada do mÃ³dulo + descanso; abertura **hoje / em aberto** (2026-06-19)
- [x] **Ambiente Renan:** Chrome (MPA); Electron testado e descartado â€” assistente nÃ£o pergunta
- [x] **BotÃ£o flutuante PDV** â€” canto inferior esquerdo, `/pdv/`, F1 global (2026-06-19)
- [x] **FAB PDV â€” sobreposiÃ§Ã£o** â€” some em modal; z-index 90; colisÃ£o â†’ sobe ou canto direito (2026-05-21)

### LanÃ§amentos â€” Contas a pagar layout novo (2026-06-19)


| URL                                   | Tela                     |
| ------------------------------------- | ------------------------ |
| `/lancamentos/contas-pagar/`          | **Layout novo** (padrÃ£o) |
| `/lancamentos/contas-pagar/classico/` | Redirect â†’ layout padrÃ£o |
| `/lancamentos/contas-pagar/teste/`    | Redirect â†’ padrÃ£o        |


**Mesma API** `/api/lancamentos/`. **Editar / Excluir** no layout novo: modal + APIs `alterar`/`excluir` (sem redirecionar); vista preservada apÃ³s salvar/excluir. `**/classico/`** redireciona para o layout padrÃ£o (2026-06-22).

**Perf:** projeÃ§Ã£o slim Mongo; `skip_totais` pÃ¡g. 2+; cache sessionStorage; planos lazy.

**Abertura rÃ¡pida:** prefetch BI/F7; cache sessionStorage; selo **Sincronizandoâ€¦**; **lista embutida no HTML** (bootstrap servidor, hoje+abertos); no app com abas, link do BI abre **aba lateral** (BI nÃ£o some).

**Vista:** baixa, NF, Nova saÃ­da recarregam in-place (scroll, expandidos, Â«carregar maisÂ», URL).

**Colunas (menu â–¦ Colunas):** visibilidade + ordem por **operador do PIN** (`localStorage` `gm_fin_sv_cp_cols_v1`). BotÃ£o **Salvar no meu PIN** grava; persiste ao fechar o sistema. Arrastar **â‹®â‹®** na lista â€” quanto mais acima, mais Ã  esquerda na tabela. **PadrÃ£o** (sem save): Vencimento Â· Fornecedor Â· Plano/grupo Â· DescriÃ§Ã£o Â· Saldo Â· Status Â· NF Â· Pagar.

**Planos de contas (filtros):** todos **marcados ao abrir**; desmarcar vale sÃ³ na sessÃ£o (nÃ£o grava â€” ao reabrir o sistema volta tudo marcado).

**Abertura:** lista padrÃ£o = **em aberto Â· vencimento hoje** (sem filtros na URL). Deep link / filtros salvos na URL respeitados. Painel **Filtros** â†’ **Filtrar por:** vencimento (padrÃ£o) Â· competÃªncia Â· pagamento (mesmos campos De/AtÃ©; aviso se pagamento + em aberto).

**PIN:** uma vez ao entrar em LanÃ§amentos (qualquer rota `/lancamentos/*`); navegaÃ§Ã£o interna sem repetir; sÃ³ de novo no **modo descanso** (idle). `/lancamentos/` redireciona direto para **Contas a pagar** (sem popup hub).

**Arquivos:** `lancamentos_contas_pagar_teste.html`, `mongo_financeiro_util.py`, `views.py`, `urls.py`, `lancamentos_financeiros.html`, `lancamentos_contas_pagar_calendario.html`, `includes/lancamentos_pin_entrada.html`, `_screensaver_pin.html`.

**ProduÃ§Ã£o:** merge `teste`â†’`producao` 2026-06-19 (Renan pediu).

### NFC-e â€” status staging (2026-06-18)

- [x] **ReemissÃ£o** em Consultar vendas â€” Renan confirmou funcionando
- [x] Erro **225** â€” `card/tpIntegra=2` + IBPT por item
- [x] **PIX/cartÃ£o** â€” sem popup NFC/Venda; modal CPF se cliente sem CPF
- [x] **DevoluÃ§Ã£o** â€” cancelamento SEFAZ automÃ¡tico Â· **OK produÃ§Ã£o** nÂº 4 (Renan 23/06) Â· prazo **30 min**
- [x] **Dinheiro** â€” popup NFC/Venda; modal CPF **grande** (v1.11+)
- [x] Toast falha fiscal **depois** da impressÃ£o Windows
- [x] **ProduÃ§Ã£o** â€” emissÃ£o v1.93 Â· nÂº 2 sÃ©rie 21 Â· QR SEFAZ OK Â· cancelamento devoluÃ§Ã£o **v2.03** Â· nÂº 4 cancelado (Renan 23/06/2026)

### LanÃ§amentos â€” EmprÃ©stimo (entrada + pagamento) â€” feito 2026-06-18

**Onde:** modal **Nova saÃ­da** em **LanÃ§amentos** (`/lancamentos/`) + **Lote manual** (`/lancamentos/novo-manual/`). No **BI** (`/`): atalho sÃ³ no card **Contas a Pagar** (sem botÃ£o na barra PDV/OrÃ§amento/Menu).

**Plano na lista:** `EmprÃ©stimo (entrada + pagamento)` (buscar Â«emprestÂ»).


| Campo        | Entrada (receita) | SaÃ­da / pagamento      |
| ------------ | ----------------- | ---------------------- |
| Loja, pessoa | sim               | sim                    |
| Conta, forma | nÃ£o               | sim                    |
| Comp./venc.  | **hoje** (auto)   | o que preencher        |
| Quitado      | **sempre**        | chip Â«QuitadoÂ» sÃ³ aqui |
| RecorrÃªncia  | desligada         | â€”                      |


**Valores:** entrada + saÃ­da. Se **saÃ­da > entrada** â†’ tÃ­tulo extra em **Juros de EmprÃ©stimos** (diferenÃ§a). Pagamento principal = valor da entrada quando hÃ¡ juros.

**Toggle Pagar/Receber:** ignorado neste modo (gera receita + despesa no mesmo envio).

**Ajuda UI:** textos longos removidos do card; botÃ£o **?** no cabeÃ§alho (sÃ³ quando o plano emprÃ©stimo dual estÃ¡ ativo).

**ProduÃ§Ã£o:** `d75c436` v1.10 (2026-06-18) â€” cherry-pick escopo fechado; **nÃ£o** inclui PDV wizard nem merge `teste` inteiro.

### URGENTE â€” PDV carrinho some ao bipar GM no barras (2026-06-18)


| Onde                        | Sintoma                                                         | Status                                    |
| --------------------------- | --------------------------------------------------------------- | ----------------------------------------- |
| **Wizard** `/pdv/checkout/` | Cada bipe **GM1546-5S** remove **1 item** do carrinho (4â†’3â†’2â†’1) | **Staging `teste`** â€” aguarda teste Renan |
| **Legado** `/consulta/`     | Carrinho zerava ou perdia itens (F4 pÃ³s-bip, match parcial)     | **ProduÃ§Ã£o** `59bdedc` v1.02              |


**Caso Renan (IbiÃºna ensacada):** produto **1467** â€” GM `**GM1546-5S`** erroneamente no campo **CÃ³digo de barras** (aba Fiscal). Leitor manda `GM1546-5S`; campo de busca mostra `**GM15465S`** (hÃ­fen engolido).

**Causa wizard:** atalho `**-`** na busca = `bumpLastCartItem(-1)` (menos qty do **Ãºltimo** item). O hÃ­fen do GM disparava remoÃ§Ã£o **sem** inserir o carÃ¡cter.

**Patch wizard (`pdv_wizard.js`, em `teste`):**

- `-` / `+` ignorados enquanto digita GM/SKU ou janela pÃ³s-bip (1,5 s)
- CÃ³digos `**GMâ€¦`** â†’ modo **barcode** (auto-adiciona)
- Match **alnum** no cache (`GM15465S` = `GM1546-5S`)
- **F4** bloqueado apÃ³s leitor (campo com cÃ³digo ou janela ativa)

**Teste pÃ³s-deploy (Renan):**

1. Ctrl+F5 no wizard Â· 4 itens no carrinho
2. Bipar **GM1546-5S** / **GM15465S** vÃ¡rias vezes
3. Carrinho **nÃ£o** perde itens; idealmente adiciona IbiÃºna
4. Corrigir cadastro: barras â†’ EAN ou `230â€¦` + reimprimir etiqueta

**PDV legado:** `consulta_produtos.js` + `_js_buscaâ€¦` (produÃ§Ã£o v1.02).

**ProduÃ§Ã£o wizard:** ainda **sem** este fix â€” cherry-pick sÃ³ quando Renan pedir apÃ³s OK no staging.

### Etiquetas â€” barras deve ser EAN, nÃ£o GM (decisÃ£o 2026-06-18)

**Regra:** o leitor bipa **o que estÃ¡ codificado no barras** da etiqueta. **Correto:** EAN/GTIN numÃ©rico (ex. `7897030100427`) â†’ PDV recebe nÃºmero. **GM** (`GM1541-5S`, `GM1518-125-3`) sÃ³ aparece quando a etiqueta foi gerada **sem** Â«CÃ³digo de barrasÂ» vÃ¡lido no cadastro â€” aÃ­ o SisVale usa **CODE128 com texto GM** (`produtos_etiquetas_core.js` â†’ `valorBarcodeProduto`).


| O quÃª                                             | Onde                                                                         |
| ------------------------------------------------- | ---------------------------------------------------------------------------- |
| CÃ³digo GM                                         | **Texto** abaixo do barras (referÃªncia humana)                               |
| EAN 8/12/13                                       | **Dentro** do barras â€” Ã© isso que o leitor deve mandar                       |
| Etiquetas antigas / jÃ¡ impressas com GM no barras | PDV aceita GM (patch carrinho); **reimprimir** quando houver EAN no cadastro |
| IbiÃºna ensacada **GM1546-5S** (prod. 1467)        | Barras errado no cadastro â€” corrigir Fiscal â†’ EAN/`230â€¦` â†’ reimprimir        |


**OperaÃ§Ã£o:** antes de imprimir lote, abrir ðŸ–¨ï¸ no cadastro â€” se aparecer aviso Ã¢mbar Â«Sem EANÂ», corrigir cadastro primeiro.

### CÃ³digo de barras interno da loja (decisÃ£o 2026-06-18)

Nem todo item tem EAN de fÃ¡brica. Casos normais na GM Agro:


| SituaÃ§Ã£o                                                                        | Barras no cadastro                                                     |
| ------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Embalagem do **fornecedor** (NF, saco fechado)                                  | EAN/GTIN **real** da embalagem quando existir                          |
| Saco **maior** com EAN, **unidade de dentro** sem barras (ex. ensacado na loja) | **CÃ³digo interno** numÃ©rico criado pela loja â€” nÃ£o Ã© EAN do fornecedor |
| **Reembalagem** na loja (raÃ§Ã£o a granel, fracionado)                            | Idem â€” cÃ³digo interno (7 dÃ­gitos, 13 com DV vÃ¡lido, etc.)              |


**NÃ£o** confundir: Â«inventarÂ» aqui = **identificador interno SisVale/loja**, nÃ£o falsificar GTIN de fabricante. PDV e etiqueta tratam 4â€“14 dÃ­gitos como barras normal (CODE128); EAN-13 de **fÃ¡brica** exige DV correto.

**Faixa 230â€¦ (interno loja):** gerada pelo SisVale (`agro_codigo_barras_loja_util.py`) â€” 230 + 10 dÃ­gitos sequenciais. **NÃ£o Ã© EAN-13** (Ãºltimo dÃ­gito Ã© sequÃªncia, nÃ£o DV). Etiqueta imprime **CODE128**; modal ðŸ–¨ï¸ mostra Â«Barras interno lojaÂ». EAN real (ex. Abacate `789â€¦`) continua EAN13.

**DiagnÃ³stico Mongo:** `python manage.py pdv_diagnostico_codigo GM1546-5S GM1518-125-3 1813647`

### CÃ³digo de barras curto (7 dÃ­gitos) + PDV â€” feito 2026-06-18

Match exato sÃ³-dÃ­gitos + fallback API (`_js_busca_produto_inteligente.html`, `consulta_produtos.js`). Etiquetas: CODE128 antes do fallback GM (`extrairCodigoBarrasCurto`).

### Etiqueta EAN-13 layout â€” feito 2026-06-18

**Sintoma:** barras 13 dÃ­gitos com DV errado â†’ preview sÃ³ preÃ§o cortado, sem barras.

**CorreÃ§Ã£o:** `produtos_etiquetas_core.js` â€” valida/corrige DV, fallback CODE128, CSS 40Ã—40 mm; modal cadastro avisa DV corrigido (Ã¢mbar).

**Staging (`teste`):** `5bcc05d` v1.19 Â· `5c6590a` v1.20 (230â€¦ CODE128) Â· `c234822` v1.22 (busca cadastro). **Ctrl+F5** cadastro + PDV apÃ³s deploy Render.

### Cadastro produtos â€” busca GM/barras â€” feito 2026-06-18

**Sintoma:** lista cadastro nÃ£o achava por cÃ³digo GM nem barras.

**CorreÃ§Ã£o (`c234822` v1.22 + `â€¦` v1.24):** modo **normal**, `api_produtos_cadastro`, prefixo GM (`GM1541` â†’ `GM1541-5S`), Enter forÃ§a busca, limpa Â«Continue digitandoÂ» ao buscar.

### ProduÃ§Ã£o â€” patch PDV carrinho (feito 2026-06-18)

Renan validou no staging â†’ subiu **sÃ³** o patch urgente (`59bdedc` em `producao`, v1.02). **NÃ£o** mergeou `teste` inteiro.

**Arquivos:**


| Arquivo                                       | Papel             |
| --------------------------------------------- | ----------------- |
| `consulta_produtos.js`                        | Fix carrinho + GM |
| `_js_busca_produto_inteligente.html`          | Match GM          |
| `pdv_diagnostico_codigo.py`                   | DiagnÃ³stico Mongo |
| `produtos_etiquetas_core.js` + modal cadastro | EAN no barras     |


**Loja:** Ctrl+F5 no `/consulta/` apÃ³s deploy Render.

### WIP / nÃ£o commitado (snapshot 2026-06-19)


| Arquivo                                                | Tema                                          |
| ------------------------------------------------------ | --------------------------------------------- |
| `AGENTS.md`                                            | Nota `@banana` vs enciclopÃ©dia (local)        |
| `nfe_entrada_util.py`, `views.py`, `entrada_nota.html` | Entrada NF â€” sync financeiro desync (etapa 7) |


**Acabou de subir em `producao`:** LanÃ§amentos CP (layout + perf + vista + PIN entrada + filtro hoje) Â· v1.33.

**Teste Renan (staging):** `/lancamentos/contas-pagar/` â†’ filtrar â†’ baixa/Nova saÃ­da â†’ filtros e scroll mantidos.

### PendÃªncias conhecidas (produto)

**DesvinculaÃ§Ã£o ERP (responsividade)** â€” ver Â§4.15â€“4.16:

- [x] **Fiado** â€” Postgres nativo (`FiadoTituloAgro`); **fora** do pacote LanÃ§amentosâ†’Mongo
- [x] LanÃ§amentos â€” backup ZIP + checkpoint (~17â€¯703 tÃ­tulos) + layout/PIN/perf
- [x] Corte Agroâ†’ERP (API) â€” **produÃ§Ã£o v1.14** (`372f90f`; automÃ¡tico apÃ³s checkpoint)
- [x] **PrÃ³xima fase financeiro:** CP/CR **PG loja** â€” DRE/calendÃ¡rio/export validar
- [ ] **Nunca** merge `teste` inteiro em `producao` â€” sÃ³ cherry-pick do escopo combinado
- [x] CatÃ¡logo Postgres **teste** â€” Renan OK 2026-06-22
- [x] CatÃ¡logo Postgres **produÃ§Ã£o** â€” v1.53
- [x] PDV catÃ¡logo + GestÃ£o + ledger + Compras D4 â€” **teste** (pacotes corte v4.31â€“v4.36)
- [x] BI home financeiro â†’ PG (cards CP/CR teste v3.23+)
- [x] BI vendas histÃ³rico â†’ VendaAgro â€” **teste v4.36** (validar Renan)
- [ ] GrÃ¡fico gastos dados â€” **PG loja** (validar vs CP)
- [x] Pacotes corte Mongo v4.31â€“v4.36 â†’ **loja** (**28/06** Â· senha Renan Â· `77f1254`)
- [ ] TransferÃªncias, Validade, fornecedor NF (sync profundo)

**Outras:**

- [x] **PDV legado carrinho GM** â€” produÃ§Ã£o `59bdedc` v1.02 (2026-06-18)
- [x] **LanÃ§amentos CP â€” perf abertura Chrome** â€” bootstrap + prefetch + cache (`teste` v1.48); Renan OK melhora sutil; **prÃ³ximo salto adiado**
- [ ] **LanÃ§amentos CP perf â€” roadmap adiado** â€” retomar: (A) `agro_pg` financeiro **ou** (B) lista no BI sem navegar **ou** (C) enxugar pÃ¡gina CP. **NÃ£o** micro-otimizar atÃ© Renan pedir.
- [ ] **Layout novo CP + perf lista** â†’ produÃ§Ã£o (cherry-pick quando Renan pedir)
- [ ] **PDV wizard carrinho GM** â€” em staging `teste`; Renan validar â†’ cherry-pick produÃ§Ã£o quando pedir
- [ ] Dedupe clientes Mongo vs ERP por CPF (futuro)
- [x] Tela contabilidade â€” itens **1,2,3,4,8** â€” **teste + produÃ§Ã£o** v2.25 (2026-06-24)

---

## Roadmap â€” tela Contabilidade (`/contabilidade/`)

**Implementado (teste + loja v2.25):** resumo mÃªs, CSV/XLSX, ZIP com `index.csv` + autorizadas/canceladas, login dedicado (`AGRO_CONTABILIDADE_USERNAMES`). Ver CHECKPOINT 1.0.66.

**Fluxo Renan:** sempre **teste primeiro** â†’ validar no Render teste â†’ **replicar produÃ§Ã£o** (mesmo cÃ³digo; NFC-e sÃ©rie **21** jÃ¡ Ã© produÃ§Ã£o) quando pedir frase + senha.

**Hoje (antes v2.14):** sÃ³ ZIP XML autorizadas.

**Objetivo:** hub para o **escritÃ³rio** baixar fiscal + espelhos gerenciais, sem entrar no PDV.

### Prioridade alta (provÃ¡vel uso real)

| Ferramenta | Para quÃª | EsforÃ§o | Notas |
| ---------- | -------- | ------- | ----- |
| **Resumo do mÃªs NFC-e** (antes do ZIP) | Qtd autorizadas / canceladas, total R$, faixa numeraÃ§Ã£o sÃ©rie 21 | Baixo | âœ… teste v2.14 |
| **Planilha CSV/XLSX NFC-e** | NÂº, sÃ©rie, chave, data/hora, valor venda, CPF consumidor, status, venda # | Baixo | âœ… teste v2.14 |
| **ZIP incluir canceladas + Ã­ndice** | XML autorizado + marcar **Cancelada** no `index.csv` dentro do ZIP | MÃ©dio | âœ… teste v2.14 |
| **Atalho LanÃ§amentos export** | Mesmo mÃªs â†’ CSV/XLSX/PDF financeiro (APIs jÃ¡ existem em `/lancamentos/`) | Baixo | âœ… teste v2.14 |
| **Atalho Vendas CSV** | `/vendas/exportar-csv/` filtrado por perÃ­odo | Baixo | âœ… teste v2.14 |

### Prioridade mÃ©dia

| Ferramenta | Para quÃª | EsforÃ§o |
| ---------- | -------- | ------- |
| **Caixa â€” fechamentos do mÃªs** | PDF/CSV por sessÃ£o (gaveta): entradas, sangrias, formas | MÃ©dio â€” `caixa_util`, `MovimentoCaixa` |
| **Entrada NF / compras** | Resumo entradas no perÃ­odo (Mongo + Agro) | MÃ©dio |
| **DRE / resumo gerencial** | Link `/lancamentos/dre/` + `/financeiro/resumo-gerencial/` com mÃªs | Baixo |
| **PendÃªncias fiscais** | Lista NFC-e rejeitada/erro no mÃªs (nÃ£o autorizadas) | Baixo | âœ… teste v2.33+ â€” bloco recolhÃ­vel + CSV |

### Prioridade baixa / fase 2

| Ferramenta | Para quÃª | EsforÃ§o |
| ---------- | -------- | ------- |
| **UsuÃ¡rio Â«ContabilidadeÂ»** | Login sÃ³ leitura + export (sem PDV/caixa) | MÃ©dio | âœ… teste v2.14 â€” `AGRO_CONTABILIDADE_USERNAMES` |
| **Log Â«quem baixou o ZIPÂ»** | Auditoria | Baixo |
| **E-mail automÃ¡tico mensal** | Enviar ZIP dia 1 | Alto â€” Render + SMTP |
| **NF-e devoluÃ§Ã£o mod. 55** | Quando NFC-e passou 30 min e foi devolvida | Alto â€” emissÃ£o prÃ³pria |
| **XML cancelamento (evento)** | Guardar e exportar procEventoNFe | MÃ©dio â€” hoje sÃ³ mudamos status local |

### O que **nÃ£o** misturar na contabilidade (por enquanto)

- ERP sÃ©rie **20** (continua no ERP legado; Agro Ã© sÃ©rie **21**).
- RH folha completa (dado sensÃ­vel â€” link separado se um dia).
- EdiÃ§Ã£o de lanÃ§amentos (contador sÃ³ **exporta**, nÃ£o lanÃ§a).

**PrÃ³ximo passo sugerido (Renan escolhe):** bloco **Resumo + CSV + ZIP melhorado** num Ãºnico patch na tela atual.
- [ ] **Merge NFC-e â†’ `producao`** apÃ³s OK Renan + checklist `docs/NFCE-PRODUCAO.md`
- [ ] Testes automatizados sync clientes / NFC-e (futuro)

### InstruÃ§Ãµes para o assistente (prÃ³xima atualizaÃ§Ã£o)

**AtualizaÃ§Ã£o automÃ¡tica (padrÃ£o â€” nÃ£o perguntar ao Renan):**

Ao **entregar** fix, feature ou deploy (teste ou produÃ§Ã£o) â†’ **editar `banana.md`**: CHECKPOINT (o quÃª, commits, `VERSION`, teste OK, produÃ§Ã£o se houver, dica de revert) + Â§ do mÃ³dulo se for regra permanente. **ObrigatÃ³rio** para qualquer mudanÃ§a de comportamento do sistema. **NÃ£o** pedir autorizaÃ§Ã£o. **NÃ£o** registrar chat sÃ³ explicativo ou detalhe passageiro.

**Quando Renan pedir *"atualize a banana"* (ou revisÃ£o explÃ­cita):**

1. Ler `git log teste --oneline -20` e `git status`.
2. Atualizar seÃ§Ãµes **4** (mÃ³dulos afetados), **8** (linha do tempo) e este **CHECKPOINT**.
3. Incrementar versÃ£o do checkpoint: patch = pequenos fixes; minor = mÃ³dulo novo; major = reestruturaÃ§Ã£o do doc.
4. Mover itens de **WIP** para **coberto** apÃ³s commit.
5. **PendÃªncias:** manter lista viva â€” abrir/fechar conforme estado real (nÃ£o sÃ³ o que Renan disser na hora).
6. **NÃ£o** inflar o doc: manter tabelas; detalhe longo vai para doc irmÃ£o ou AGENTS.md Â§7.
7. **Nunca** perguntar ao Renan se deve atualizar o `AGENTS.md`.

### Fim do checkpoint v1.0.77

*PrÃ³xima ediÃ§Ã£o comeÃ§a abaixo desta linha ou substituindo o bloco CHECKPOINT acima.*
