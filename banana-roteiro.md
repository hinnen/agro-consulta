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
| **Cadastro ERP** — `/produtos/cadastro-erp/`, planilha Excel | `### 4.6` (cadastro) | CHECKPOINT: `cadastro`, `ERP`, `planilha`, `busca cadastro` |
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
| **Relatórios** — Central `/relatorios/`, filtros cat/sub 1–4, ABC, ranking | `### 4.1` + CHECKPOINT `relatórios` | Filtros: período · **categoria** · **sub 1/2/3/4** (combinar) · agrupar |
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

## 7. Checklist único — path FIADO-TITULO-FRETE (teste v17.83)

Verificação **23/08/2026**. Um só checklist. Path cruzado com código + `scripts/verify_fiado_titulo_frete_path.py` + Django `tests_fiado_titulos_frete`.

Caso loja: **Venda #3437** Joelma (Esposa Wagner) — cupom **R$ 409,50** (itens 399,50 + frete 10). Título **Pedido 3437 = R$ 399,50 quitado**. A 2ª linha Fiado (frete) não virava título.

- [x] PDV `pagamentosDetalheParaErp` manda **todas** as linhas Fiado (cada uma com `fiadoCronograma`)
- [x] Persistência **não** faz `break` na 1ª linha; `fiado_cronograma_json` junta e renumera
- [x] `pagamentos_json` guarda as duas fatias (fonte Postgres, não localStorage)
- [x] `valor_fiado_venda_local` soma as duas linhas → **409,50**
- [x] `_parcelas_fiado_para_titulos` gera 399,50 + 10,00 mesmo com cronograma só da 1ª
- [x] Parcelas numeradas **1 e 2** → chaves `pdv:{pk}:1` / `:2` sem colidir
- [x] Venda nova: `criar_titulos_de_venda` cria os dois títulos
- [x] Venda antiga com título curto: cria **Pedido {pk} · complemento** (não duplica se a soma já fecha)
- [x] 2ª execução do backfill é no-op
- [x] Uma linha Fiado já com 409,50 **não** inventa complemento
- [x] Baixa soma o saldo de **todos** os títulos em aberto da seleção
- [x] Cancelar venda cancela todos os títulos da venda
- [x] Conferência de caixa chama `criar_titulos` (repara turno ainda aberto)
- [x] Backfill filtra `forma_pagamento` fiado, limite **8000**, `--pk` para uma venda
- [x] Joelma: `python manage.py backfill_fiado_titulos --pk 3437` (não lote cego)
- [x] **Atenção:** se a loja já cobrou 409,50 em espécie contra o papel, **não** rode o `--pk` — seria cobrança em dobro. Cancele o complemento.
- [x] Sem migrate. NFC-e / caixa / balança USE-P2 **fora** deste recorte
- [x] `python scripts/verify_fiado_titulo_frete_path.py` → **VERIFY_OK**
- [x] `python manage.py test produtos.tests_fiado_titulos_frete` → **OK**

**Status: ✅ pronto para envio** (teste **v17.83**). Loja continua **v17.82** até frase + senha. USE-P2 / visor kg permanece Live v17.82.

**Você (local):** merge deste path em `teste` → Ctrl+F5 PDV → venda fiado + entrega R$ 10 → `/fiado/` deve listar **409,50** (ou 399,50 quitado + 10 complemento). Rollback: voltar o commit deste pacote (sem mexer v17.82 da loja).
