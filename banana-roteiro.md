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

`PDV` · `cadastro` · `gestão` · `gestao` · `caixa` · `fiado` · `F8` · `RH` · `folha` · `Lançamentos` · `CP` · `NF` · `entrada` · `compras` · `estoque` · `relatórios` · `relatorio` · `deploy` · `loja` · `teste` · `v6` · `Mongo` · `overlay` · `Chrome`

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

## 10. Checklist único — lote 28/08b (PREP · aguarda senha)

Sobre loja **v18.50**. Branch `deploy/prep-checklist-2808b` · badge alvo **v18.64**. **Produção não alterada.**

| # | Pacote | Status | Migrate |
| - | ------ | ------ | ------- |
| 1 | **CP-NE-BUSCA-EMPRESA** | 🟢 PREP · prova **61+18** | **NÃO** |
| 2 | **REPASSE-COFRINHO-ACUM** | 🟢 PREP · path **175** · cof **28** | **SIM** (`0102` no-op) |
| 3 | **PDV-MODO-POR-FORMA** | 🟢 PREP · path **25/25** | **NÃO** |
| 4 | **REPASSE-PDV-OVERLAY-LIMPO** | 🟢 PREP · path **187** | **NÃO** |

- [x] Cherry só destes 4 (+ `0102`) sobre `origin/producao` @ v18.50
- [x] Arquivos do lote idênticos ao `teste`
- [x] Paths: PDV modo **25** · overlay/repasse **187** · cof **28** · fechar **68+41** · CP **61+18**
- [x] `manage.py check` · Node JS · Django **6/6** modo por forma
- [ ] Senha produção — **pendente** (próximo chat, lojas pausadas)

Rollback: `docs/ROLLBACK-LOTE-CHECKLIST-2808b.md`. Detalhe CHECKPOINT em `banana.md`.
