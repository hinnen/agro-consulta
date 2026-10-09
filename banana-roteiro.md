# BANANA ROTEIRO — ler isto **antes** do `banana.md`

**Substitui** a leitura integral do banana na maioria dos chats. O `banana.md` completo (~4500 linhas) fica como arquivo de detalhe e histórico.

**Renan:** anexe `@banana-roteiro` (ou só descreva a tarefa — a rule Cursor já puxa este arquivo).

---

---

## 0. Regras duras (ler sempre) · atualizado 30/07/2026

### 0.1 Dados no Postgres (multi-PC)

- Loja = **vários PCs** (duas lojas). Nada operacional pode viver **só** num PC, no browser (`localStorage`) ou só no Mongo.
- **Postgres = fonte da verdade** (vínculo NF, rascunho, preço/overlay, estoque Agro, **presets de etiquetas**, filtros Folha saldo, financeiro quando já migrado). PC local = prova rápida; Mongo = espelho legado — **não** é seguro para vínculo NF.
- Assistente: gravar no **PG**, commit das correções importantes; **nunca** deixar vínculo / nota / preço / estoque / financeiro / **preset / filtro salvo da loja** só no PC, localStorage ou Mongo.
- Exemplos: `EntradaNfeVinculoAgro` · `EtiquetaPresetAgro` · `ComprasFolhaSaldoFiltroPreset`.
- **Erro já cometido:** presets de etiquetas em `localStorage` → sumiam nos outros PCs. **Corrigido** → Postgres (`ETQ-PRESET-PG`).

### 0.2 Git — push `teste` ao fechar entrega · produção só com senha (30/07/2026)

| Ação | Regra |
| ---- | ----- |
| **Fechar entrega** (fix/feature/pacote) | `commit` em `teste` + **`git push origin teste`** — **sempre**, **sem** pedir autorização |
| **Meio de tarefa / WIP quebrado** | **Não** push obrigatório (evita lixo no remoto) |
| **Validar** | PC local (`docs/TESTE-LOCAL.md`) — Render free **não** é gate |
| **Loja / `producao`** | **Só** frase explícita + senha `99738595` na **mesma** mensagem |
| **2 repositórios Git?** | **Não** — `teste` (backup+rascunho) e `producao` (loja) bastam; histórico reverte bug |

**Por quê:** se o PC estragar, o código já está no GitHub. Bug no `teste` **não** quebra a loja.

Detalhe no topo do `banana.md` (bloco **Teste / backup GitHub**).

---

## 1. Todo chat — ordem fixa

```
1. Ler ESTE arquivo inteiro (banana-roteiro.md)
2. Ler banana.md linhas 1–41 (regras duras: produção, teste, registro)
3. Ler banana.md §0 TL;DR (seção «## 0.»)
4. CHECKPOINT: grep no banana.md pela palavra-chave do módulo (tabela §3)
   → ler só os blocos ### que baterem + a linha de versão (teste/loja)
5. Seguir fluxograma §2 conforme a tarefa
6. Se §2 não cobrir → escada §4
```

**Não** ler o banana inteiro salvo §5.

---

## 2. Fluxograma por tarefa

Escolha o ramo que mais se aproxima. Leia **na ordem**; pare quando tiver contexto suficiente.

### 2.1 Qual módulo / tela?

| Se a tarefa é sobre… | Ler em `banana.md` | Extra |
| -------------------- | ------------------ | ----- |
| **PDV** — `/consulta/`, `/pdv/checkout/`, carrinho, F8, promo, overlay topbar | `### 4.2` | CHECKPOINT: `PDV`, `F8`, `wizard`, `overlay` |
| **Cadastro ERP** — `/produtos/cadastro-erp/`, planilha Excel | `### 4.6` (cadastro) | CHECKPOINT: `cadastro`, `ERP`, `planilha`, `busca cadastro` · Excel ↓ fornecedores: **§9** |
| **Gestão produtos** — `/produtos/gestao/`, overlay, lentidão pós-NF | `### 4.6` (gestão) | CHECKPOINT: `gestão`, `gestao` · `AGENTS.md` §7 gestão se perf |
| **NFC-e / cupom fiscal** | `### 4.3` | `docs/NFCE-PRODUCAO.md` se produção SEFAZ |
| **Vendas / devolução** | `### 4.3` (devolução) + `### 4.4` | CHECKPOINT: `devolução`, `FL-017` |
| **Clientes / fiado** | `### 4.5` + trecho fiado em `### 4.2` se PDV | CHECKPOINT: `fiado`, `cliente`, `F8` |
| **Entrada NF** | `### 4.7` | `AGENTS.md` §7 entrada NF se XML/modal |
| **Estoque / ajuste / sync** | `### 4.8` | `docs/ESTOQUE_AGRO_FONTE_DA_VERDADE.md` |
| **Compras** | `### 4.9` | `AGENTS.md` §7 compras se relatório/planilha |
| **Lançamentos / CP / CR / DRE** | `### 4.10` | Se corte Mongo/PG: `### WIP` lançamentos (~L771–1020) · CHECKPOINT: `Lançamentos`, `CP`, `Postgres` |
| **Caixa** — abrir, fechar, sangria, gaveta | `### 4.11` | CHECKPOINT: `caixa`, `Caixa` |
| **RH** — folha, vale, ficha | `### 4.12` | `AGENTS.md` §9 · CHECKPOINT: `RH`, `folha` |
| **Home / BI** — `/`, gráficos | `### 4.1` | CHECKPOINT: `BI`, `dashboard`, `gastos` |
| **Relatórios** — Central `/relatorios/`, filtros cat/sub 1–4, ABC, ranking | `### 4.1` (bullet Central) + CHECKPOINT `relatórios` | Filtros: período · **categoria** · **sub 1/2/3/4** (combinar) · agrupar · **500 cat/sub → ✅ Live v18.26.1** (Renan OK 28/08) |
| **Entregas** — `/entregas/`, rota terça, painel | CHECKPOINT: `entrega`, `entregas`, `FL-006`, `FL-031` | Fluxo loja: PDV → retorno entregador → baixa PDV |
| **WhatsApp lojas** — `/atendimento-whatsapp/`, QR, filas Centro/Vila | `### 4.16` | CHECKPOINT: `WhatsApp`, `WA-ATEND-QR` |
| **Tarefas / pendências** — hub `/vendas/lojas/` → Tarefas | CHECKPOINT: `Tarefas`, `pendências`, `Vendas lojas` | PIN + timeline em `tarefas/` |
| **Fotos produto** — hub `/vendas/lojas/` → Fotos | CHECKPOINT: `Fotos`, `FOTOS-PRODUTO` | Busca+bip+até 4 fotos · `views_fotos_produto` |

### 2.2 Tipo de mudança (somar ao ramo acima)

| Se for… | Ler também |
| ------- | ---------- |
| **Layout / visual / fonte / botão** | `### 4.14` · `AGENTS.md` §5 e **§11** (Display Scale) |
| **Só backend / API / bug dados** | § do módulo (§2.1) — **não** precisa §4.14 |
| **Deploy teste** (prova) | Topo L27–35 · CHECKPOINT «Prova = local» — Renan valida **no PC**; **não** Render staging free |
| **Deploy produção / cherry loja** | Topo L22–31 · `## 3` até `### 3.2` · CHECKPOINT deploy loja · **parar e confirmar** com Renan |
| **Desvinculação Mongo / corte ERP** | `### 4.15` + `### Checklist — corte total` · escada §5 (ler muito) |
| **Variável `.env`** | `## 5` |
| **Dúvida «como usar o Cursor»** | `## 6` |

### 2.3 Árvore rápida (texto)

```
Tarefa
 ├─ Deploy produção? → topo L22-31 + §3.2 + CHECKPOINT deploy → §5 se conflito
 ├─ Módulo conhecido? → tabela §2.1 → (+ §2.2 se visual/deploy)
 ├─ Só pergunta / explicar? → §0 + CHECKPOINT grep → fim
 └─ Não sei o módulo → §4 inteiro (### 4.1–4.14) + CHECKPOINT → ainda falta? → §5
```

---

## 3. Palavras-chave CHECKPOINT (grep)

Usar **Grep** em `banana.md`, seção `## CHECKPOINT`, com 1–3 termos:

`PDV` · `cadastro` · `gestão` · `gestao` · `caixa` · `fiado` · `F8` · `RH` · `folha` · `Lançamentos` · `CP` · `NF` · `entrada` · `compras` · `estoque` · `relatórios` · `relatorio` · `deploy` · `loja` · `teste` · `v6` · `Mongo` · `overlay` · `Chrome` · `WhatsApp` · `WA-ATEND` · `Tarefas` · `pendências` · `Vendas lojas` · `Fotos` · `FOTOS-PRODUTO`

Ler no máximo **5** subseções `###` que baterem + a linha **Versão app**.

---

## 4. Escada se faltou contexto

| Degrau | Quando | Ler |
| ------ | ------ | --- |
| **A** | Roteiro + §2 não bastou | `## 4` completo (mapa módulos, ~L390–611) |
| **B** | WIP financeiro / desvinculação ativa | Blocos `### WIP` / `### PRÓXIMO` entre L771–1030 |
| **C** | Histórico de incidente citado pelo Renan | Grep CHECKPOINT pelo número da versão ou FL-xxx |
| **D** | Ainda ambíguo | `banana.md` **inteiro** (§5) |

---

## 5. Quando ler `banana.md` INTEIRO

- Renan pediu *«lê o banana inteiro»* ou *«contexto completo»*
- Deploy **produção** com cherry-pick de vários pacotes
- Desvinculação Mongo / migração Postgres em andamento
- Retomar trabalho após **semanas** ou chat muito resumido pelo Cursor
- Degrau **D** da escada §4

---

## 6. Manutenção (assistente)

| Evento | Atualizar |
| ------ | --------- |
| Novo módulo grande no §4 | Linha na tabela §2.1 |
| Nova palavra CHECKPOINT recorrente | §3 |
| Mudança na regra de leitura | Este arquivo + `.cursor/rules/agro-consulta.mdc` §0 |
| WIP / deploy / decisão | `banana.md` CHECKPOINT (como hoje) |
| **Deploy loja concluído** | No `banana.md`: registrar Live **e limpar** badges «pronto para envio / aguarda senha» do que **já subiu** (vira ✅ enviado). Renan 23/07: *limpar sempre o que já foi enviado*. |

*Não* duplicar WIP aqui — só o **mapa de leitura**.

---

## 7. Checklist único — path CAIXA-DEVOL-DINHEIRO-MP (loja v17.84)

Verificação **23/08/2026**. Um só checklist. Tudo cruzado com código + **118** provas do path + 41 Fechar-loja + 68 repasse.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CAIXA-DEVOL-DINHEIRO-MP** | ✅ **enviado / Live v17.84** | **NÃO** |
| 2 | **PDV-BALANCA-UI-CAIXA** | ✅ Live v17.83 (permanece) | **NÃO** |
| 3 | **PDV-BALANCA-KG-VIVO** | ✅ Live v17.82 (permanece) | **NÃO** |
| 4 | **NFCE-DEST-CNPJ** | ✅ Live v17.81 (permanece) | **SIM** (`0099`) |

- [x] Venda devolvida do turno **continua** no esperado da maquininha
- [x] Caso loja: débito MP 49 + 5,90 → esperado MP **54,90** · gaveta **abre − 49**
- [x] Pix MP e crédito MP + devolução em dinheiro: pinpad fica; gaveta cai
- [x] Cielo não vaza para o Point
- [x] FL-017 dinheiro+dinheiro: esperado = abertura
- [x] Aviso amarelo só cartão/Pix → dinheiro (não no FL-017)
- [x] Parcial / outro turno / sangria cobertos
- [x] Auto (MP / fiado / vale / cashback) = esperado · sem rascunho · `readonly`
- [x] API `escopo=loja` não mistura Centro × Vila
- [x] Relatório de caixa não duplica movimento de devolução
- [x] Sem migrate

**Status: enviado / Live v17.84.** Rollback: tag `rollback/pre-caixa-devol-dinheiro-mp-v17.83` @ `8bb72875` · `docs/ROLLBACK-CAIXA-DEVOL-DINHEIRO-MP.md`. Prova: `scripts/verify_caixa_devolucao_dinheiro_mp_path.py` (118/118).

---

## 8. Checklist — path PDV-PRECO-MANUAL-FORMA (loja v18.50)

Preço digitado no carrinho **não** volta ao lista ao escolher forma.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-PRECO-MANUAL-FORMA** | ✅ **Live v18.50** | **NÃO** |

**Prova (histórico):** `scripts/verify_pdv_preco_manual_forma.py`. Lote atual pendente: **§10**.

---

## 9. Checklist — path CAD-XLSX-ULT-FORN (loja v18.02 · conferido 28/08/2026)

Excel ↓ do cadastro: colunas opcionais **Últ. / 2º / 3º fornecedor** (Entrada NF Agro). Cruzado com tip `origin/producao` @ **v18.27+**.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CAD-XLSX-ULT-FORN** | ✅ **Live v18.02+** · Renan OK **28/08** | **NÃO** |

- [x] Cherry só deste pacote (24/08) — commits `8502f2c` · `5af9b6c` · `5e7c284` ainda ancestrais do tip
- [x] `FORNECEDOR_EXPORT_KEYS` / `enriquecer_rows_ultimos_fornecedores` no tip
- [x] `ultimos_fornecedores_por_produto_ids` (lote, não N+1)
- [x] Checkboxes JS `fornecedor_compra_1..3` no Excel ↓
- [x] Excel ↑ ignora essas colunas
- [x] Sem migrate
- [x] Rollback: tag `rollback/pre-cad-xlsx-ult-forn-v17.84` @ `da7c1cb` · branch backup · `docs/ROLLBACK-CAD-XLSX-ULT-FORN.md`

**Status: fechado / Live.** Renan 28/08: *«já foi resolvido»* — sem ação nova. Detalhe CHECKPOINT em `banana.md`.

---

## 10. Checklist único — lote 28/08b (loja **v18.64**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CP-NE-BUSCA-EMPRESA** | ✅ **Live v18.64** | **NÃO** |
| 2 | **REPASSE-COFRINHO-ACUM** | ✅ **Live v18.64** | **SIM** (`0102` no-op) |
| 3 | **PDV-MODO-POR-FORMA** | ✅ **Live v18.64** | **NÃO** |
| 4 | **REPASSE-PDV-OVERLAY-LIMPO** | ✅ **Live v18.64** | **NÃO** |

**Status: enviado / Live v18.64.** `producao` @ `5e6e44a`. Rollback: tag `rollback/pre-lote-checklist-2808b-v18.50` @ `4836ec1` · `docs/ROLLBACK-LOTE-CHECKLIST-2808b.md`.

---

## 11. Checklist único — lote 28/08c (loja **v18.72**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **NS-ESCOLHA-EMP** | ✅ **Live v18.72** | **NÃO** |
| 2 | **REPASSE-PDV-OVERLAY-POPUP** | ✅ **Live v18.72** | **NÃO** |
| 3 | **CP-EMP-PG-FALLBACK** | ✅ **Live v18.72** | **NÃO** |

**Status: enviado / Live v18.72.** `producao` @ `ae126d9`.  
**Rollback:** tag `rollback/pre-lote-checklist-2808c-v18.64` @ `5e6e44a` · branch `producao-backup-pre-v1872-lote-checklist-20260828` · `docs/ROLLBACK-LOTE-CHECKLIST-2808c.md` · **só** frase+senha.  
**Smoke:** healthz ok · home **18.72** · PDV/consulta **200**.

---

## 12. Checklist único — lote 29/08b (loja **v19.01**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **REPASSE-HERO-LOTE** | ✅ **Live v19.01** | **NÃO** |
| 2 | **TABELA-PRECO-FORMA** | ✅ **Live v19.01** | **SIM** (`produtos.0104`) |
| 3 | **PDV-PEDIR-CUPOM-QTD** | ✅ **Live v19.01** | **SIM** (`estoque.0020`) |

**Status: enviado / Live v19.01.** `producao` @ `7c69fbc`.  
**Rollback:** tag `rollback/pre-lote-checklist-2908b-v18.83` @ `d836982` · branch `producao-backup-pre-v1901-lote-checklist-20260829` · `docs/ROLLBACK-LOTE-CHECKLIST-2908b.md` · **só** frase+senha.  
**Smoke:** healthz ok · home/consulta/PDV **200** · badge **v19.01** · Ctrl+F5 · tabelas % **inativas**.

---

## 13. Checklist único — lote 29/08g (loja **v19.60**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1–7 | **REPASSE-*** (formula·campos·totais·contraste·3OK·ghost·aviso) | ✅ **Live v19.60** | **NÃO** |
| 8 | **PIN-OPERADOR-QUEM** | ✅ **Live v19.60** | **NÃO** |
| 9 | **NFCE-DESC-ITENS** | ✅ **Live v19.60** | **NÃO** |
| 10 | **PDV-CUPOM-DINHEIRO** | ✅ **Live v19.60** | **NÃO** |
| 11 | **CAD-EXCLUIR-MSG-STAFF** | ✅ **Live v19.60** | **NÃO** |
| 12 | **CAD-VAL-ESPELHO** | ✅ **Live v19.60** | **NÃO** |
| 13 | **PDV-PEDIR-ESCRITO-UX** | ✅ **Live v19.60** | **NÃO** |
| 14 | **NF-ESTOQUE-BLOQUEIO-FALSO** | ✅ **Live v19.60** | **NÃO** |
| 15 | **PDV-CHAT-LOJA** | ✅ **Live v19.60** | **SIM** (`produtos.0105`) |

**Status: enviado / Live v19.60.** `producao` @ `460e1c7`.  
**Rollback:** tag `rollback/pre-lote-checklist-2908g-v19.02` @ `6b1eeed` · branch `producao-backup-pre-v1960-lote-checklist-20260829` · `docs/ROLLBACK-LOTE-CHECKLIST-2908g.md` · **só** frase+senha.  
**Smoke:** healthz ok · home/consulta/PDV **200** · badge **v19.60** · Ctrl+F5. **Pendente SOLO:** `BI-META-C-VILA-RAMP`.

---

## 14. Checklist único — lote 30/08 (`deploy/prep-checklist-3008` · alvo loja **v19.83**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-TRANSF-FORCADA** | ✅ **Live v19.83** · prova **88/88** | **NÃO** |
| 2 | **PDV-ENTER-SEM-IMP** | ✅ **Live v19.83** · prova **41/41** | **NÃO** |
| 3 | **CAIXA-DEVOL-MP-MESMA** | ✅ **Live v19.83** · prova **171/171** | **NÃO** |
| 4 | **NFCE-REEMIT-TIMEOUT** | ✅ **Live v19.83** · prova **38/38** | **NÃO** |

**Status: enviado / Live v19.83.** `producao` @ `09d5968`.  
**Rollback:** tag `rollback/pre-lote-checklist-3008-v19.63` @ `71eea32` · branch `producao-backup-pre-v1983-lote-checklist-20260830` · `docs/ROLLBACK-LOTE-CHECKLIST-3008.md` · **só** frase+senha.  
**Smoke:** healthz ok · badge **v19.83** · Ctrl+F5 · Enter=sem · Pedir/Forçada · Fechar caixa · Reemitir. **Pendente SOLO:** `BI-META-C-VILA-RAMP`.

---

## 15. Checklist único — lote 31/08 (loja **v20.45**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-TOPBAR-LAYOUT** | ✅ **Live v20.45** | **SIM** (`0110`) |
| 2 | **PDV-TOPBAR-MAIS** | ✅ **Live v20.45** | **SIM** (`0107`) |
| 3 | **PDV-WA-TOPBAR-BREVE** | ✅ **Live v20.45** | **NÃO** |
| 4 | **PDV-PIN-CHAT-TEMPEDIDO** | ✅ **Live v20.45** | **NÃO** |
| 5 | **REPASSE-FUNDO-TROCO** | ✅ **Live v20.45** | **SIM** (`0106`) |

**Status: enviado / Live v20.45.** `producao` @ `18fc7d1`.  
**Rollback:** tag `rollback/pre-lote-checklist-3108-v20.22` @ `75779df` · branch `producao-backup-pre-v2045-lote-checklist-20260831` · `docs/ROLLBACK-LOTE-CHECKLIST-3108.md` · **só** frase+senha.  
**Smoke:** healthz ok · badge **v20.45** · Ctrl+F5. **Fora ainda:** `WA-ATEND-QR` · `BI-META-C-VILA-RAMP`.

---

## 16. Checklist único — lote 31/08b (loja **v20.49**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-WA-COR** | ✅ **Live v20.49** | **NÃO** |
| 2 | **REPASSE-ARREDONDA-COFRE** | ✅ **Live v20.49** | **NÃO** |

**Status: enviado / Live v20.49.** `producao` @ `31941b8`.  
**Rollback:** tag `rollback/pre-lote-checklist-3108b-v20.45` @ `18fc7d1` · branch `producao-backup-pre-v2049-lote-checklist-20260831` · `docs/ROLLBACK-LOTE-CHECKLIST-3108b.md` · **só** frase+senha.  
**Smoke:** healthz ok · badge **v20.49** · Ctrl+F5. **Fora ainda:** `WA-ATEND-QR` · `BI-META-C-VILA-RAMP`.

---

## 17. Checklist único — lote 01/09 (`deploy/prep-checklist-0109` · alvo loja **v20.56**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-ENTREGA-F3** | ✅ **Live v20.56** · prova **68/68** | **NÃO** |
| 2 | **BI-DEVOL-DIA** | ✅ **Live v20.56** | **NÃO** |
| 3 | **ENT-VIA-DIN-SEM-MAQ** | ✅ **Live v20.56** · prova **49/49** | **NÃO** |

**Status: enviado / Live v20.56.** `producao` @ `d30c5ca`.  
**Rollback:** tag `rollback/pre-lote-checklist-0109-v20.49` @ `31941b8` · branch `producao-backup-pre-v2056-lote-checklist-20260901` · `docs/ROLLBACK-LOTE-CHECKLIST-0109.md` · **só** frase+senha.  
**Smoke:** healthz ok · badge **v20.56** · Ctrl+F5. **Fora:** `WA-ATEND-QR` · `WA-FIADO-MSG` · `BI-META-C-VILA-RAMP`.

---

## 18. Checklist único — lote 01/09c (`deploy/prep-checklist-0109c` · alvo loja **v20.58**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **BI-DEVOL-PLANILHA** | ✅ **Live v20.58** · prova **28/28** | **NÃO** |

**Status: enviado / Live v20.58.** `producao` @ `751c0d4`.  
**Rollback:** tag `rollback/pre-lote-checklist-0109c-v20.56` @ `d30c5ca` · branch `producao-backup-pre-v2058-lote-checklist-20260901` · `docs/ROLLBACK-LOTE-CHECKLIST-0109c.md` · **só** frase+senha.  
**Smoke:** healthz ok · badge **v20.58** · Ctrl+F5. **Fora:** `WA-ATEND-QR` · `WA-FIADO-MSG` · `BI-META-C-VILA-RAMP`.

---

## 19. Checklist único — lote 03/09 (`deploy/prep-checklist-0309` · loja **v21.84**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PIN-VENDA-10S** | ✅ **Live v21.84** | **NÃO** |
| 2 | **FIADO-VER-RECIBOS** | ✅ **Live v21.84** | **NÃO** |
| 3 | **PDV-OVERLAY-STACK** | ✅ **Live v21.84** | **NÃO** |
| 4 | **VENDAS-LISTA-UX** | ✅ **Live v21.84** | **NÃO** |
| 5 | **F8-HIST-VENDAS** | ✅ **Live v21.84** | **NÃO** |
| 6 | **CAIXA-FIADO-CONF** | ✅ **Live v21.84** | **SIM** `0123` |

**Status: enviado / Live v21.84.** `producao` @ `c165db2`.  
**Rollback:** tag `rollback/pre-lote-checklist-0309-v21.82` @ `527be62` · branch `producao-backup-pre-v2183-lote-checklist-20260903` · `docs/ROLLBACK-LOTE-CHECKLIST-0309.md` · **só** frase+senha.  
**Smoke:** healthz ok · consulta **200** · badge **v21.84** · Ctrl+F5. **Fora:** WhatsApp extra · `CLI-FORM-PDV-LAYOUT` · `CAD-FALLBACK-HIST`.

---

## 20. Checklist único — lote 04/09b (`deploy/prep-checklist-0409` · alvo loja **v21.89**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **LOGIN-BI-FECHADO** + **LOGIN-UI-AGRO** | ✅ **Live v21.89** | **NÃO** |
| 2 | **NF-LISTA-ANDAMENTO** | ✅ **Live v21.89** | **NÃO** |
| 3 | **ETQ-A6-BONUS** | ✅ **Live v21.89** | **NÃO** |
| 4 | **FIADO-LIMITE-LINHA** | ✅ **Live v21.89** | **NÃO** |
| 5 | **PDV-CHAT-POLL-10S** | ✅ **Live v21.89** | **NÃO** |
| 6 | **WA-XFER-PIX-ORC** | ✅ **Live v21.89** | **SIM** `0125` |

**Status: enviado / Live v21.89.** `producao` @ `4910c79`.  
**Rollback:** tag `rollback/pre-lote-checklist-0409-v21.88` @ `329f9b5` · branch `producao-backup-pre-v2189-lote-checklist-20260904` · `docs/ROLLBACK-LOTE-CHECKLIST-0409.md` · **só** frase+senha.  
**Smoke:** healthz ok · badge **v21.89** · Ctrl+F5. **Fora:** Excel cadastro · WhatsApp extra.

---

## 21. Checklist único — lote 05/09g (`deploy/prep-checklist-0509g` · alvo loja **v21.91**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-ENTREGA-TABELA-FORMA** | ✅ **Live v21.91** | **NÃO** |
| 2 | **REPASSE-ZERO-OK** | ✅ **Live v21.91** | **NÃO** |
| 3 | **PDV-VALE-SALDO-LIVE** | ✅ **Live v21.91** | **NÃO** |
| 4 | **MP-POINT-FINAL-PIN** | ✅ **Live v21.91** | **NÃO** |
| 5 | **PDV-VALE-USADO** | ✅ **Live v21.91** | **NÃO** |
| 6 | **PDV-ORC-LISTA-LIVE** | ✅ **Live v21.91** | **NÃO** |
| 7 | **WA-LISTA-SEM-PISCA** | ✅ **Live v21.91** | **NÃO** |
| 8 | **WA-FACHONA-PRETA** | ✅ **Live v21.91** | **NÃO** |
| 9 | **WA-PIN-COMPOSER** | ✅ **Live v21.91** | **NÃO** |
| 10 | **WA-SAUDACAO-RICH** + **WA-ARQUIVO** | ✅ **Live v21.91** | **SIM** `0126` |

**Status: enviado / Live v21.91.** `producao` @ `319404f`.  
**Rollback:** tag `rollback/pre-lote-checklist-0509g-v21.90` @ `aaff41d` · branch `producao-backup-pre-v2191-lote-checklist-20260905` · `docs/ROLLBACK-LOTE-CHECKLIST-0509g.md` · **só** frase+senha.  
**Smoke:** healthz ok · badge **v21.91** · Ctrl+F5. **Não** ligar ponte Zap.

---

## 22. Checklist único — lote 05/09h (`deploy/prep-checklist-0509h` · alvo loja **v21.93**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **LANC-PIN-TECLADO** | ✅ **Live v21.93** | **NÃO** |
| 2 | **WA-TROCAR-FEED** | ✅ **Live v21.93** | **NÃO** |
| 3 | **WA-TOPBAR-OVERLAY** | ✅ **Live v21.93** | **NÃO** |

**Status: enviado / Live v21.93.** `producao` @ `8884c9c`.  
**Rollback:** tag `rollback/pre-lote-checklist-0509h-v21.92` @ `041e1b5` · branch `producao-backup-pre-v2193-lote-checklist-20260905` · `docs/ROLLBACK-LOTE-CHECKLIST-0509h.md` · **só** frase+senha.  
**Smoke:** Ctrl+F5 · badge **v21.93** · PDV F7 · Lançamentos PIN velho → teclado · Zap balcão 1 barra.  
**Não subiu:** merge `teste` · Excel · resto ponte foto/agenda.

---

## 23. Checklist único — VL-HUB-TAREFAS · ✅ **Live v23.18+**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **VL-HUB-TAREFAS** | ✅ **Live v23.18+** | **SIM** `tarefas.0001` + `0002` |

**Status: enviado / Live.** Ver CHECKPOINT banana.

---

## 24. Checklist único — lote 08/09 (`deploy/prep-checklist-0809` · loja **v23.58**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PIN-ALERT-TECLADO** | ✅ **Live v23.58** · prova **117/117** | **NÃO** |
| 2 | **REL-QUEM-COMPROU** | ✅ **Live v23.58** · prova **67/67** | **NÃO** |
| 3 | **ETQ-COLAR-MV** | ✅ **Live v23.58** · prova **82/82** | **NÃO** |

**Status: enviado / Live v23.58.** `producao` @ `0e0c419`.  
**Rollback:** tag `rollback/pre-checklist-0809-v23.45` @ `1b942c4` · branch `producao-backup-pre-v2358-checklist-20260908` · `docs/ROLLBACK-CHECKLIST-0809.md` · **só** frase+senha.  
**Smoke:** healthz · badge **v23.58** · Ctrl+F5 · PIN teclado · Quem já comprou · Etiquetas colar/ranking.

---

## 25. Checklist único — lote 09/09 · ✅ **Live v23.70**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-ENTREGA-PAGAS-24H** | ✅ Live · **64/64** | **SIM** `0129` |
| 2 | **PDV-ENTREGA-LOJA-SAIDA** | ✅ Live · **12/12** | **NÃO** |
| 3 | **CAIXA-ENTREGA-ADIAR** | ✅ Live · **26/26** | **SIM** `0128` |
| 4 | **TAREFAS-DETALHE-LOTE** | ✅ Live · **90/90** | **NÃO** |
| 5 | **REPASSE-COFRE-PLANO** | ✅ Live · **62/62** | **NÃO** |
| 6 | **REPASSE-GESTAO-SIMPLES** | ✅ Live · **64/64** | **NÃO** |

**Status: ✅ Live v23.70** — `producao` @ `71a169a` · Render `dep-dagsqovlk1mc73a5t8cg`.  
**Antes:** Live **v23.58** @ `0e0c419`. Migrate **0128+0129** OK. **Não** merge `teste`.  
**Rollback:** tag `rollback/pre-checklist-0909-v23.58` · `docs/ROLLBACK-CHECKLIST-0909.md`.  
**Smoke:** Ctrl+F5 · badge **v23.70** · Entregas · Adiar · Tarefas · repasse.

---

## 26. Checklist único — MP-POINT-POLL-RETRY · ✅ **Live v23.73**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **MP-POINT-POLL-RETRY** | ✅ Live · poll **13/13** · PIN **41/41** · tests **16/16** | **NÃO** |

**Status: ✅ Live v23.73** — `producao` @ `c7e6fd2` · Render `dep-dah01b95efls739b0u30`.  
**Antes:** Live **v23.70** @ `71a169a`. Bug **#7423** (502 matava espera Point). **Não** merge `teste`.  
**Rollback:** tag `rollback/pre-mp-point-poll-retry-v23.70` · `docs/ROLLBACK-MP-POINT-POLL-RETRY.md`.  
**Smoke:** Ctrl+F5 · badge **v23.73** · Point: oscilação de rede continua aguardando.

---

## 27. Checklist único — lote 10/09 · ✅ **Live v23.91**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PIN-SSPIN-GLOBAL** | ✅ Live · **197/197** | **NÃO** |
| 2 | **FOTOS-PRODUTO-MOBILE** | ✅ Live · **59/59** | **NÃO** |
| 3 | **PIN-NS-BI** | ✅ Live · **130/130** | **NÃO** |
| 4 | **CLI-DUP-TEL-Z** | ✅ Live · **60/60** | **NÃO** |
| 5 | **NF-FIN-NAO-TEM** | ✅ Live · **15/15** | **NÃO** |
| 6 | **NF-AGUARDA-PRODUTO** | ✅ Live · **6/6** | **NÃO** |

**Status: ✅ Live v23.91** — `producao` @ `22186fb` · Render `dep-dahg41navr4c738vno40`.  
**Antes:** Live **v23.76** @ `056e9a7`. **Não** merge `teste`. Sem migrate.  
**Rollback:** tag `rollback/pre-checklist-1009-v23.76` · `docs/ROLLBACK-CHECKLIST-1009.md`.  
**Smoke:** healthz ok · Ctrl+F5 · badge **v23.91**.

---

## 28. Checklist único — lote 11/09 · ✅ **Live v23.92**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **REPASSE-ACUM-EXTRA-BUG** | ✅ Live · path **18/18** | **NÃO** |

**Status: ✅ Live v23.92** — `producao` @ `78098c3` · Render `dep-dai2a3uk1f9s73ep6kvg`.  
**Antes:** Live **v23.91** @ `22186fb`. **Não** merge `teste`. Sem migrate.  
**Rollback:** tag `rollback/pre-checklist-1109-v23.91` · `docs/ROLLBACK-CHECKLIST-1109.md`.  
**Smoke:** Ctrl+F5 · badge **v23.92** · Repasse acumulado ~**254**.

---

## 29. Checklist único — lote 11/09b · ✅ **Live v23.93**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **REPASSE-PCT-ZERO** | ✅ Live · path **20/20** · PIN **9973** | **NÃO** |

**Status: ✅ Live v23.93** — `producao` @ `fc34325` · Render `dep-dai36tks728c73c9snm0`.  
**Antes:** Live **v23.92** @ `78098c3`. **Não** merge `teste`. Sem migrate.  
**Rollback:** tag `rollback/pre-checklist-1109b-v23.92` · `docs/ROLLBACK-CHECKLIST-1109b.md`.  
**Smoke:** Ctrl+F5 · badge **v23.93** · Repasse PDV **0%**.

---

## 30. Checklist único — lote 11/09d · ✅ **Live v23.94**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **NF-LOTE-XML** | ✅ Live · path **48/48** · PIN **9973** | **NÃO** |
| 2 | **REPASSE-HIST-OVERLAY** | ✅ Live · hist **19/19** | **NÃO** |
| 3 | **REPASSE-STATUS-FLASH** | ✅ Live · **43/43** · PIN **9973** | **NÃO** |

**Status: ✅ Live v23.94** — `producao` @ `c3e0b1c` · Render `dep-daiji4qjnfac73e9r5dg`.  
**Antes:** Live **v23.93** @ `fc34325`. **Não** merge `teste`. Sem migrate.  
**Rollback:** tag `rollback/pre-checklist-1109d-v23.93` · `docs/ROLLBACK-CHECKLIST-1109d.md`.  
**Smoke:** Ctrl+F5 · badge **v23.94** · NF etapa 4 · Repasse Histórico · TRANSFERINDO.

---

## 31. Checklist único — lote 12/09c · ✅ **Live v23.96**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **WA-PONTE-ULTRA-LEVE** | ✅ Live · **47/47** | **NÃO** |
| 2 | **WA-APP-SEM-PDV** | ✅ Live · **53/53** | **NÃO** |
| 3 | **PDV-RACOES-MARCA-VAZIA** | ✅ Live · **57/57** | **NÃO** |
| 4 | **PDV-CHAT-ENTREGA-DOCK** | ✅ Live · **7/7** | **NÃO** |
| 5 | **PDV-ENT-HORARIO-OPCOES** | ✅ Live · **16/16** | **NÃO** |
| 6 | **PDV-ENT-TROCO-ENTER** | ✅ Live · **6/6** | **NÃO** |
| 7 | **PDV-ENT-OVERLAY-SPLIT** | ✅ Live · lote **61/61** | **SIM** `0130` |
| 8 | **ETQ-LOTE-FILA** | ✅ Live · **78/78** | **NÃO** |
| 9 | **PDV-IMP-SEP-OFF** | ✅ Live | **NÃO** |
| 10 | **PDV-IMP-PIN-ANTES** | ✅ Live | **NÃO** |
| 11 | **PDV-ENT-CARD-LATERAL** | ✅ **Live v23.96** · **51/51** | **NÃO** |
| 12 | **PDV-ENT-ALERTA-POR-ID** | ✅ **Live v23.96** · **51/51** | **NÃO** |
| 13 | **PDV-ENT-MUDAR-LOJA** | ✅ **Live v23.96** · **39/39** | **SIM** `0131` |

**Status: ✅ Live v23.96** — `#11`–`#13`. Antes: Live **v23.95** @ `8faa4c1`.  
**Rollback:** tag `rollback/pre-checklist-1209c-v23.95` · `docs/ROLLBACK-CHECKLIST-1209c.md`.

---

## 33. Checklist único — lote 14/09 (`deploy/prep-checklist-1409` · alvo loja **v25.20**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 18 | **NF-PIN-EXIGE-FIN** | 🟢 PREP · **9/9** | **NÃO** |
| 19 | **CP-BAIXA-DESC** | 🟢 PREP · **12/12** | **NÃO** |
| 20 | **PDV-TABELA-PRINT-FORMA** | 🟢 PREP · **17/17** | **NÃO** |
| 21 | **PDV-CONFIRM-QUITADO-TROCAR** | 🟢 PREP · **31/31** | **NÃO** |
| 22 | **PDV-FINAL-TIMEOUT-UI** | 🟢 PREP · **13/13** | **NÃO** |
| 23 | Zap PDV lento | ✅ já Live | — |
| 24 | **PDV-PRECO-FORMA-DIN** | 🟢 PREP · **15/15** | **NÃO** |
| 25 | **CAIXA-ABERTURA-MILHAR** | 🟢 PREP · **14/14** | **NÃO** |
| 26 | **PIN-VENDA-45S** | 🟢 PREP · **17/17** · pin **79** | **NÃO** |
| — | **PDV-ENTREGAS-MODAL-BODY** | 🟢 PREP | **NÃO** |
| — | **PDV-FECHAR-CTA-QUITADO** | 🟢 PREP · **60/60** | **NÃO** |
| — | **PDV-ORC-IMPRIMIR** | 🟢 PREP | **NÃO** |

**Status: 🟢 PREP pronto · aguarda senha.** Base loja **v23.99** @ `a43340a`.  
**Rollback:** tag `rollback/pre-checklist-1409-v23.99` · `docs/ROLLBACK-CHECKLIST-1409.md`.  
**Não** merge `teste`. Lojas abertas → deploy só com pausa + frase + senha.

---

## 34. Checklist único — lote 30/09 (`deploy/prep-checklist-3009` · alvo loja **v25.75**)

16 pacotes do checklist. **Não** inclui a unificação de fichas (dado já feito na loja). **Não** merge `teste`.

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1–2 | Limite no fiado · corrigir o nome | 🟢 PREP · **13/13** | **NÃO** |
| 3 | Cartão da entrega no dia anterior | 🟢 PREP · **68/68** | **SIM** `0134`+`0135` |
| 4 | Dia da entrega | 🟢 PREP · **40/40** | **SIM** `0133` |
| 5 | Excel de clientes + valor do mês | 🟢 PREP · **42/42** | **NÃO** |
| 6 | PIN no Point | 🟢 PREP · **14/14** | **NÃO** |
| 7 | Código na nota | 🟢 PREP · **24/24** | **NÃO** |
| 8 | Cofrinho no fechar | 🟢 PREP · **15/15** · cofrinho **39/39** | **NÃO** |
| 9 | Ver a outra loja | 🟢 PREP · no **40/40** | **NÃO** |
| 10 | PIN na Gestão | 🟢 PREP · **43/43** | **NÃO** |
| 12 | Limite no card do PDV | 🟢 PREP · **39/39** | **NÃO** |
| 13 | Busca do PDV | 🟢 PREP · **30/30** | **NÃO** |
| 14 | Nome no carrinho | 🟢 PREP · **34/34** | **NÃO** |
| 15 | Ordem e Zap no fiado | 🟢 PREP · **59/59** | **NÃO** |
| 16 | Logos do Dispenser | 🟢 PREP · **27/27** | **NÃO** |

**Status: 🟢 PREP · aguarda pausa + frase + senha.** Loja ainda **v25.36** @ `c8b78d80`.  
**Rollback:** tag `rollback/pre-checklist-3009-v25.36` · `docs/ROLLBACK-CHECKLIST-3009.md`.

---

## 36. Checklist único — Dispenser PIN do descanso (30/09)

A impressão da folha **já está na loja (v25.77)**. O §35 **já está na loja (v25.75)**. O que falta, e o **único** que entra no próximo envio:

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **DSP-PIN-DESCANSO** | **19/19** | **NÃO** |

**O quê:** no Dispenser, depois de um tempo parado, o cartão do PIN cobre a tela e o OK funciona.  
**Não mexe:** PDV, caixa, venda, fiado, nota, financeiro.  
**Status: ✅ Live v25.78** — `producao` @ `4e4a1244`. **Não** foi merge do `teste`.  
**Rollback:** tag `rollback/pre-dsp-pin-descanso-v25.77` · `docs/ROLLBACK-DSP-PIN-DESCANSO.md` · **só** frase+senha.

---

## 37. Checklist único — Dispenser fundo branco na impressão (30/09)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **DSP-PRINT-BRANCO** | **20/20** | **NÃO** |

**O quê:** logo, animal e foto de ingredientes saem com o fundo da prévia, não pretos.  
**Não mexe:** PDV, caixa, venda, fiado, nota, financeiro.  
**Status: ✅ Live v25.79** — `producao` @ `072ff56e` · Render `dep-daumndjm8hqs738qeci0`. **Não** foi merge do `teste`.  
**Rollback:** tag `rollback/pre-dsp-print-branco-v25.78` · `docs/ROLLBACK-DSP-PRINT-BRANCO.md` · **só** frase+senha.

---

## 38. Checklist único — etiqueta da nota só com o nome do cadastro (01/10)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **NF-ETQ-NOME-CADASTRO** | **14/14** · térmica **39/39** | **NÃO** |

**O quê:** na etapa 6 da entrada de nota, a etiqueta imprime o nome do cadastro. Não cola `(vinculo_c_prod)` nem `(ean_pg)`, e não repete a cada gravação. Parêntese de verdade (500 ml) fica.  
**Não mexe:** PDV, caixa, venda, fiado, financeiro.  
**Status: ✅ Live v25.80** — `producao` @ `780015dd` · Render `dep-dautkfrm8hqs7393o53g`. **Não** foi merge do `teste`.  
**Rollback:** tag `rollback/pre-nf-etq-nome-cadastro-v25.79` · `docs/ROLLBACK-NF-ETQ-NOME-CADASTRO.md` · **só** frase+senha.

---

## 39. Checklist único — etiqueta da nota com código GM (01/10)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **NF-ETQ-CODIGO-GM** | **21/21** · térmica **39/39** | **NÃO** |

**O quê:** na etapa 6 da entrada de nota, a etiqueta usava `cProd` do XML (código da nota). Agora usa **código GM** do vínculo (`codigo_nfe` / `codigo_gm`; fallback `produto_id` ERP). EAN prioriza cadastro quando existir.  
**Não mexe:** PDV, caixa, venda, fiado, financeiro.  
**Status: ✅ Live v25.81** — `producao` @ `6c64ddaa` · **não** foi merge do `teste`.  
**Rollback:** tag `rollback/pre-nf-etq-codigo-gm-v25.80` · `docs/ROLLBACK-NF-ETQ-CODIGO-GM.md` · **só** frase+senha.

---

## 40. Checklist único — Excel clientes: média fiado por mês (01/10)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CLIENTE-MEDIA-FIADO-MES** | `verify_cliente_planilha_path.py` (média mensal + contratos) | **NÃO** |

**O quê:** na planilha **Excel ↓** de `/clientes/`, a coluna passa a **Média fiado/mês (3 meses)** = soma do fiado no mês atual + 2 anteriores, **÷ 3** (mês sem compra = zero). Antes era média **por compra**. Janela de dados = 3 meses calendário (desde o dia 1 do mês mais antigo).  
**Não mexe:** PDV, caixa, limite fiado na loja (só o número exportado na coluna cinza).  
**Status: ✅ Live v25.82** — `producao` @ `3b33d4ac` · **não** foi merge do `teste`.  
**Rollback:** tag `rollback/pre-cliente-media-fiado-mes-v25.81` · `docs/ROLLBACK-CLIENTE-MEDIA-FIADO-MES.md` · **só** frase+senha.

---

## 42. Checklist único — Excel clientes grava todos os limites (01/10)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CLIENTE-XLSX-LIMITE-TODOS** | import **401/401** limites · fiado em aberto **não** grava | **NÃO** |

**O quê:** Excel ↑ em `/clientes/` só gravava as **primeiras 400** alterações. Quem ficava depois continuava com limite **0** e o PDV mostrava **R$ 5.000**. Agora grava **todas**. **0** no cadastro = padrão R$ 5.000 no PDV; **0,01** bloqueia. Coluna **Fiado em aberto** continua cinza (não altera dívida).  
**Não mexe:** saldo fiado, títulos, PDV, caixa.  
**Status: ✅ Live v25.84** — `producao` @ `a294eb3b` · **não** foi merge do `teste`.  
**Rollback:** tag `rollback/pre-cliente-xlsx-limite-todos-v25.82` · `docs/ROLLBACK-CLIENTE-XLSX-LIMITE-TODOS.md` · **só** frase+senha.

---

## 41. Checklist único — cron RH envio CP (Render exit 1) (01/10)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **RH-CRON-ENVIO-RENDER** | `verify_rh_envio_cp_automatico_path.py` **20/20** | **NÃO** |

**O quê:** cron **`agro-rh-envio-cp-automatico`**: salário R$ 0 → `pulados_salario_zero`, **exit 0**; erros reais no log.  
**Status: ✅ Live v25.85** — cherry com WhatsApp vazio · **não** merge do `teste`.  
**Rollback:** `docs/ROLLBACK-DEPLOY-V2585-RH-WHATSAPP.md` · tag `rollback/pre-deploy-v2584-live-20261001` · **só** frase+senha.

---

## 43. Checklist único — Excel clientes: WhatsApp vazio apaga (01/10)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CLIENTE-XLSX-WHATSAPP-VAZIO** | `verify_cliente_planilha_path.py` **57/57** | **NÃO** |

**O quê:** Excel ↑ `/clientes/`: **WhatsApp vazio** apaga o número (demais colunas: vazio = não altera).  
**Status: ✅ Live v25.85** — cherry · **não** merge do `teste`.  
**Rollback:** `docs/ROLLBACK-DEPLOY-V2585-RH-WHATSAPP.md` · tag `rollback/pre-deploy-v2584-live-20261001` · **só** frase+senha.

---

## 44. Checklist único — etiquetas barras laser 1D (01/10)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **ETQ-BARCODE-LASER** | `verify_etiquetas_termica_path.js` **44/44** | **NÃO** |

**O quê:** etiquetas térmicas (entrada NF, fila, cadastro): barras **mais grossas**, quiet zone GS1, SVG **sem encolher** no flex; core cache **v=25**.  
**Não mexe:** PDV, caixa, financeiro, cadastro além do JS de etiqueta.  
**Status: ✅ Live v25.86** — `producao` @ `c2a95d73` · cherry **`9453a951`** · **não** merge do `teste`.  
**Rollback:** tag `rollback/pre-etq-barcode-laser-v25.85` · `docs/ROLLBACK-ETQ-BARCODE-LASER.md` · **só** frase+senha.

---

## 45. Checklist único — lote 03/10 · ✅ **Live v25.99**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1–7 | ETQ-EAN · PDV-EDIT · BUG-28 · BUG-32 · ETQ-53X30 · PEDIR-ETQ53 · BIP-30 | ✅ **Live v25.99** | **NÃO** |

**Status:** ✅ enviado / Live **v25.99** — `producao` @ `55f8fe79`. **Não** merge `teste`.  
**Rollback:** tag `rollback/pre-checklist-0310-v25.86` · `docs/ROLLBACK-LOTE-CHECKLIST-0310.md`.

---

## 46. Checklist único — lote 03/10b (`deploy/prep-checklist-0310b` · alvo loja **v26.00**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **ETQ-53-UX** | 🟢 **PREP pronto** · **29/29** + smoke **16/16** | **NÃO** |
| 2 | **CLIENTE-NOVO-LIMITE-001** | 🟢 **PREP pronto** · **10/10** | **SIM** `0136` |

**O quê:** (1) botão **53×30 mm** + preview/presets na tela de etiquetas. (2) cadastro novo = limite fiado **0,01** (bloqueia até subir); cliente antigo com **0** continua R$ 5.000.  
**Não mexe:** finalizar venda, caixa, Point, NFC-e.  
**Branch PREP:** `deploy/prep-checklist-0310b` · tip `51a62353` · base Live **v25.99** @ `55f8fe79`.  
**Rollback:** tag `rollback/pre-checklist-0310b-v25.99` · `docs/ROLLBACK-CHECKLIST-0310b.md` · **só** frase+senha.  
**Status:** ✅ Live **v26.00**.  

---

## 51. CHECKLIST ÚNICO — ETQ-53-QUOTA · ✅ Live v26.02

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **ETQ-53-QUOTA** | ✅ **Live v26.02** · **34/34** + smoke **22/22** | **NÃO** |

**Rollback:** tag `rollback/pre-etq-53-quota-v26.00` · `docs/ROLLBACK-ETQ-53-QUOTA.md` · **só** frase+senha.

---

## 53. CHECKLIST ÚNICO — PDV-FIADO-LIMITE-REFRESH · ✅ Live v26.05

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-FIADO-LIMITE-REFRESH** | ✅ **Live v26.05** · `producao` @ `829e4475` · prova **39/39** | **NÃO** |

**Rollback:** tag `rollback/pre-pdv-fiado-limite-refresh-v26.02` · `docs/ROLLBACK-PDV-FIADO-LIMITE-REFRESH.md` · **só** frase+senha.

---

## 54. CHECKLIST ÚNICO — CREDITO-SCORE-SHADOW · ✅ **Live v26.07**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CREDITO-SCORE-SHADOW** | ✅ **Live v26.07** · `producao` @ `d6c19c84` · prova **93/93** | **SIM** `0137` |

**Rollback:** tag `rollback/pre-credito-score-shadow-v26.05` @ `41a6fdea` · `docs/ROLLBACK-CREDITO-SCORE-SHADOW.md` · **só** frase+senha.  
**Flag:** **ON** no Render (env).  

---

## 58. CHECKLIST ÚNICO — ETQ-PRINT-ELGIN-MAP · 🟢 PREP v26.40 · aguarda senha

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **ETQ-PRINT-ELGIN-MAP** | 🟢 **no PREP** · full **74/74** | **NÃO** |

**Branch PREP:** `deploy/prep-etq-print-elgin-map` · base Live **v26.35** @ `c517af0a`.  
**Prova:** full **74/74** · print **40→Elgin 40x40** · **50→Elgin 50x30** · **PREP_FAILS=0**.  
**Doc:** `docs/DEPLOY-PREP-ETQ-PRINT-ELGIN-MAP.md` · rollback `docs/ROLLBACK-ETQ-PRINT-ELGIN-MAP.md`.  
**Na senha:** `reset --hard origin/deploy/prep-etq-print-elgin-map` → push `producao`. **Não** merge `teste`.

---

## 57. Checklist único — lote 05/10c · ✅ **Live v26.35**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-PEDIR-PARCIAL-RESTO** | ✅ **Live v26.35** · **38/38** + smoke **36/36** | **NÃO** |
| 2 | **PDV-PEDIR-PRONTO-TRANSF** | ✅ **Live v26.35** · **42/42** | **NÃO** |
| 3 | **META-MODO-AGORA** | ✅ **Live v26.35** · **125/125** | **NÃO** |
| 4 | **CREDITO-SCORE-XLSX-COLS** | ✅ **Live v26.35** · **86/86** | **NÃO** |
| 5 | **ETQ-PRESET-ESPELHO** | ✅ **Live v26.35** · **40/40** + smoke **24/24** | **NÃO** |
| 6 | **ETQ-PRINT-DIRETO** | ✅ **Live v26.35** · **68/68** | **NÃO** |
| 7 | **PDV-PEDIR-PRINT-3** | ✅ **Live v26.35** · **37/37** + smoke **23/23** | **NÃO** |

**Status:** ✅ enviado / Live **v26.35** — `producao` @ `c517af0a` · Render `dep-db201js9v7es73fuo7sg`. **Não** merge `teste`.  
**Rollback:** tag `rollback/pre-checklist-0510c-v26.08` · `docs/ROLLBACK-CHECKLIST-0510c.md` · **só** frase+senha.  
**Smoke:** healthz ok · Ctrl+F5 · badge **v26.35**.

---

## 60. Checklist único — lote 07/10 · ✅ **Live v26.53** · 07/10

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **ETQ-PONTE-TOPBAR-BIP** | ✅ **Live v26.53** · **23/23** + smoke **13/13** | **NÃO** |
| 2 | **FIADO-LOJA-COMPRA** | ✅ **Live v26.53** · **39/39** | **SIM** 0139 |

**Live:** producao @ `1f127019` · Render `dep-db3350eq1p3s73f1mvig`.  
**Rollback:** tag `rollback/pre-checklist-0710-v26.46` @ `68ef04ce` · `docs/ROLLBACK-CHECKLIST-0710.md` · **só** frase+senha.

---

## 63. Checklist único — lote 07/10b · ✅ **Live v26.56** · 07/10

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **BUG34-DEVOL-FRETE** | ✅ **Live v26.56** · **42/42** | **NÃO** |
| 2 | **FIADO-CUPOM-SALDO-MISTO** | ✅ **Live v26.56** · **34/34** + **48/48** | **NÃO** |

**Live:** producao @ `471a11a9` · Render `dep-db35p995efls73cmb2fg`.  
**Rollback:** tag `rollback/pre-checklist-0710b-v26.53` @ `32198975` · `docs/ROLLBACK-CHECKLIST-0710b.md` · **só** frase+senha.

---

---

## 67. Checklist único — lote 07/10c · ✅ **Live v26.60** · 07/10

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **BUG31-CB-TABELA** | ✅ **Live v26.60** · **38/38** + JS **6/6** | **NÃO** |
| 2 | **ETQ-PRESET-NF-MEM** | ✅ **Live v26.60** · **51/51** + sync **26/26** | **NÃO** |
| 3 | **NF-SEM-NUMERO** | ✅ **Live v26.60** · **16/16** | **NÃO** |
| 4 | **CP-CONTAS-LOJA-PG** | ✅ **Live v26.60** · **49/49** | **SIM** 0140 |

**Live:** producao @ `10b40523` · Render `dep-db3b287f3r2c738k1eig`.  
**Rollback:** tag `rollback/pre-checklist-0710c-v26.56` @ `d08617b0` · `docs/ROLLBACK-CHECKLIST-0710c.md` · **só** frase+senha.

---

## 68. Checklist único — lote 08/10 (deploy/prep-checklist-0810 · alvo loja **v26.62**)

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CREDITO-SCORE-TRAVAS-V12** | 🟢 **PREP** · **101/101** + xlsx **90/90** + lab **34/34** | **NÃO** |
| 2 | **PDV-BALANCA-ETQ-PLU** | 🟢 **PREP** · **27/27** + unit **5/5** | **NÃO** |

**Branch PREP:** deploy/prep-checklist-0810 · tip 09b8ea61 · base Live **v26.60** @ e4206bbf.  
**Rollback:** tag rollback/pre-checklist-0810-v26.60 · docs/ROLLBACK-CHECKLIST-0810.md · **só** frase+senha.  
**Na senha:** pausar vendas · tip PREP → producao → Ctrl+F5 · badge **v26.62**. **Não** merge teste. **Não** usar deploy/prep-credito-v12.

---

## 69. Checklist único — lote 08/10b · ✅ **Live v26.66** · 08/10

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CREDITO-SCORE-TRAVAS-V12** | ✅ **Live v26.66** · **101/101** + xlsx **90/90** + lab **34/34** | **NÃO** |
| 2 | **PDV-BALANCA-ETQ-PLU** | ✅ **Live v26.66** · **27/27** | **NÃO** |
| 3 | **META-LOJAS** | ✅ **Live v26.66** · **154/154** | **NÃO** |
| 4 | **REL-HORA** | ✅ **Live v26.66** · **67/67** | **NÃO** |

**Live:** producao @ `1fbe1a8e` · Render `dep-db3otbk9v7es73aj86c0`.  
**Rollback:** tag `rollback/pre-checklist-0810b-v26.60` @ `e4206bbf` · `docs/ROLLBACK-CHECKLIST-0810b.md` · **só** frase+senha.

---

## 70. Checklist único — REL-HORA-ROTULO · 🟢 pronto para envio · 08/10

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **REL-HORA-ROTULO** | 🟢 **pronto para envio à produção** · path **85/85** · unit **7/7** | **NÃO** |

**O quê:** no hora a hora, o cartão grande diz **Média do dia** ou **Total do período**. Loja **v26.66** ainda mostra **TOTAL**.  
**Não entra:** resto do `teste`. **Só** frase + senha.

---

## 71. Checklist único — lote 08/10c · ✅ **Live v26.70** · 08/10

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **REL-HORA-ROTULO** | ✅ **Live v26.70** · **85/85** | **NÃO** |
| 2 | **PDV-BALANCA-ETQ-ENTER** | ✅ **Live v26.70** · **35/35** | **NÃO** |
| 3 | **CREDITO-LIMITE-REVISAO** | ✅ **Live v26.70** · **38/38** | **SIM** 0141 |

**Live:** producao @ `3d8b95c7` · Render `dep-db3ppemgekts73e01ed0`.  
**Rollback:** tag `rollback/pre-checklist-0810c-v26.66` @ `f29d3f8e` · `docs/ROLLBACK-CHECKLIST-0810c.md` · **só** frase+senha.

---

## 72. Checklist único — PDV-BALANCA-AGRO-PG · ✅ **Live v26.71** · 09/10

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-BALANCA-AGRO-PG** | ✅ **Live v26.71** · bip OK | **NÃO** |

**Live:** `producao` @ `2eab76c1`. Preço etiqueta: ver §73.

---

## 73. Checklist único — PDV-BALANCA-PRECO-ETQ · 🟢 PREP aguarda senha · 09/10

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **PDV-BALANCA-PRECO-ETQ** | 🟢 **PREP pronto — aguarda senha** · **52/52** | **NÃO** |

**Branch PREP:** `deploy/prep-pdv-balanca-agro-pg` · alvo **v26.72**.  
**Na senha:** `reset --hard origin/deploy/prep-pdv-balanca-agro-pg` → push `producao` · **Ctrl+F5**.  
**Smoke:** `2001000004812` → carrinho **R$ 4,81** (não 9,40).
