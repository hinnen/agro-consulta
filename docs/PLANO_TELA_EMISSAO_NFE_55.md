# Plano de tela — Emissão de NF-e (modelo 55, DANFE A4)

**Status:** implementado (UI + rascunho Mongo + emissão simulada em homologação). SOAP autorização produção: fase 2. Este arquivo continua como spec de referência.

**Público:** loja GM Agro / SisVale, telas 14–19", operadores idosos, teclado/scanner primeiro.

**`banana.md`:** o arquivo não está no repositório nem no ambiente do agente. Este plano segue o fluxo típico dessa skill (tokens + wireframes + princípios **antes** de código), cruzado com `AGENTS.md` §5 (não landing page “AI slop”: paleta emerald / orange / slate, contraste alto, botões grandes, ajuda longa só em «?»).

---

## 1. Qual documento emitir — veredito

| Papel | Modelo SEFAZ | Papel físico | Uso na loja |
| ----- | ------------ | ------------ | ----------- |
| **NF-e (nota grande)** | **`55`** | DANFE A4 | Venda B2B, produtor rural, outra UF, entrega com destinatário completo |
| NFC-e (cupom) | `65` | 80 mm | Balcão / consumidor presente — **fora desta tela** |

**Confiança: alta** de que a tela nova é **modelo 55**. O palpite do usuário está correto.

### 1.1 O que o certificado pode (e não pode) “dizer”

O certificado A1 **não grava o modelo 55 ou 65**. O modelo vai no XML (`ide.mod`). O PFX assina o CNPJ.

Neste projeto o A1 já é usado para **Distribuição DF-e de NF-e** (endpoint nacional `NFeDistribuicaoDFe` em `produtos/sefaz_dfe_client.py`, vars `NFE_DIST_DFE_*`). Isso prova:

- Tipo: **e-CNPJ A1** (`.pfx`), não e-CPF.
- Serve para **assinar XML de NF-e 55** (e, em tese, também NFC-e 65).
- Já opera no **ecossistema NF-e 55** (notas destinadas ao CNPJ da loja).

O que **não** dá para afirmar só pelo certificado (e o `.pfx` **não está** neste ambiente Cloud / `.env` local de dev):

- Credenciamento do CNPJ na SEFAZ-SP para **emitir** 55 e/ou 65 (precisa Portal SEFAZ / contador).
- CSC (token da NFC-e) — só NFC-e 65.
- Validade / CNPJ sujeito / vencimento do PFX — inspecionar em staging com `cryptography` **sem imprimir chave privada**.

**Para o Composer:** esta tela emite **somente `mod=55`**. NFC-e 65 fica para o PDV no futuro (há comentário “NFC-e futura” no overlay fiscal de cadastro). Não misturar os dois fluxos na mesma tela.

Padrão fiscal de produto já alinhado a Simples Nacional, SP, consumidor (`produtos/agro_produto_fiscal_defaults.py`): NCM paliativo `23099020`, CFOP `5102`, CSOSN `102`, origem `0`. Na NF-e 55 B2B o CFOP pode mudar (ex. `5102` não contribuinte interna; `6102` outra UF) — a tela precisa **mostrar e permitir ajuste**, com default inteligente.

---

## 2. O que já existe (reusar, não reinventar)

| Peça | Onde | Como usar |
| ---- | ---- | --------- |
| Shell densa Inter + `_agro_consulta_ui.html` | Entrada NF-e, Caixa, Promoções | Mesmo `<head>`, F1 Voltar ao PDV |
| Lista + editor full-screen | `entrada_nota.html` | **Espelhar o padrão** (lista → «Nova» / abrir rascunho) |
| Wizard numerado + «?» | Entrada NF 7 passos | Wizard de **saída** (passos abaixo) |
| Busca produto / cliente | PDV `consulta_produtos`, `clientes_lista` | Reusar APIs de busca, não nova grade Mongo |
| Defaults fiscais | `agro_produto_fiscal_defaults.py` + modal cadastro ERP | Prefill NCM/CFOP/CSOSN |
| Certificado A1 | `NFE_DIST_DFE_CERT_PATH` etc. | Mesmo PFX para assinar emissão (fase 2 backend) |
| Venda / orçamento | `VendaAgro`, checkout PDV | Origem opcional: “puxar venda” |
| Cliente Agro | `ClienteAgro` | Hoje tem **CPF**, endereço, **não** CNPJ/IE |

**Não existe** cliente SOAP de autorização (`NFeAutorizacao`), fila de XML de saída, DANFE PDF, nem tela de emissão.

---

## 3. Direção visual (passada “banana” / frontend-design)

### 3.1 Tokens (obrigatório ficar no SisVale, não no genérico roxo)

- **Base:** `slate-50` fundo, `slate-900` texto, branco cards, borda `slate-200` 2px.
- **Ação primária:** `emerald-600/700` (Emitir / Autorizar).
- **Ação de conferência / alerta:** `orange-500/700` (pendência fiscal, CFOP).
- **Informação / SEFAZ:** `sky-600`.
- **Erro SEFAZ:** `red-700` fundo `red-50` borda 2px — texto grande, sem toast sumindo sozinho.
- **Tipo:** Inter 400/700/900; inputs `min-h-[48px]`; botões `min-h-[44px]` (primário da emissão `min-h-[56px]`).
- **Não usar:** gradiente arco-íris, glassmorphism, ilustração IA, coluna `max-w-lg`.

### 3.2 Layout

Shell **16:9 larga** estilo Caixa (`.caixa-shell`, até ~96rem, `100dvh`, `overflow: hidden` no body). Lista e editor: **grade horizontal**, não formulário estreito.

Princípios:

1. Um olhar: **quem compra · o que leva · se pode emitir**.
2. Pendência fiscal **vermelha/laranja na linha**, não escondida no XML.
3. Teclado: F1 PDV, F2 busca produto, F3 busca cliente, F8 prévia DANFE, F10 emitir (com PIN).
4. Ajuda longa só no «?».

### 3.3 Wireframe — lista (`/nfe/emissao/`)

```
┌──────────────────────────────── caixa-shell 100dvh ────────────────────────────────┐
│ NF-e SAÍDA 55          [?]  [Voltar PDV F1]     [Nova F4]  [Atualizar]             │
│ busca  [chave, dest, valor…]              [Filtros]  chips: rascunho|enviada|rej. │
│────────────────────────────────────────────────────────────────────────────────────│
│ DESTINATÁRIO          Nº/SÉRIE    TOTAL      SITUAÇÃO           AÇÕES               │
│ Fazenda X · CNPJ      12 / 1      R$ 4.820   Rascunho           Abrir               │
│ Cooperativa Y         11 / 1      R$ 1.100   Autorizada 55      DANFE · XML         │
│ João CPF              —           R$   380   Rejeitada 326      Corrigir            │
└────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.4 Wireframe — editor (após Nova / Abrir)

Cabeçalho compacto + trilho de passos (igual entrada NF). Corpo em **duas colunas** no 16:9:

```
┌── Passos: 1 Destinatário  2 Itens  3 Fiscal  4 Pagamento  5 Conferir  6 PIN/SEFAZ ──┐
│ ESQUERDA (flex 1, scroll)              │ DIREITA (min 22rem, sticky)               │
│ campos do passo atual                  │ DESTINATÁRIO (nome, doc, UF)              │
│ tabela itens (passo 2)                 │ QTD ITENS · TOTAL                         │
│                                        │ Chip: modelo 55 · série · ambiente        │
│                                        │ Semáforo: NCM · CFOP · dest · cert        │
│                                        │ [Prévia DANFE F8]  [Salvar rascunho]      │
└────────────────────────────────────────┴───────────────────────────────────────────┘
│ [← Voltar]                                              [Continuar →] / [EMITIR]   │
```

Zoom alto: só o painel esquerdo rola; passos e rodapé ficam.

---

## 4. Fluxo de telas e passos

Espelhar Entrada NF-e: **lista** (rascunhos e autorizadas) + **editor** full-screen. Não wizard modal.

### Passo 1 — Destinatário (obrigatório na 55)

- Busca cliente Agro (nome, WhatsApp, CPF) + opcional Mongo `DtoPessoa`.
- Campos visíveis grandes: nome, CPF **ou** CNPJ, IE (ou “ISENTO”), CEP, UF, cidade, bairro, logradouro, número, complemento, e-mail (opcional XML).
- **Indicador de tipo:** pessoa física / jurídica (pelo documento).
- **Consumidor final** (`indFinal=1`) vs **não** (`0`) — default: PJ com IE = não consumidor; CPF = consumidor final.
- **Presença:** `indPres` default `9` (não presencial / internet) nesta tela; `1` presencial se vier do PDV.
- **UF destino vs emitente:** se diferente de SP (emitente no Dist-DFe é SP), CFOP padrão dos itens vira **6xxx** (ex. `6102`) e avisar em laranja: “operação interestadual — NFC-e não se aplica; esta é a nota certa”.
- **Buraco de cadastro:** `ClienteAgro` não tem CNPJ/IE. No passo 1, permitir preencher e **salvar de volta** no cliente (novos campos ou `cadastro_extras`). Sem isso a 55 B2B não fecha.
- Endereço: reusar os campos já existentes no form de cliente.

Bloqueio de avançar: nome + (CPF 11 ou CNPJ 14) + endereço mínimo (UF, cidade, logradouro). IE: se CNPJ e contribuinte ICMS, IE obrigatória ou “ISENTO”.

### Passo 2 — Itens

- Busca igual PDV (código, barras, nome). Scanner no campo sempre focado quando o passo está ativo.
- Colunas: qtd, un, descrição, NCM, CFOP, CSOSN, v. unit, total linha. NCM/CFOP/CSOSN **editáveis na linha** (fonte mono, alto).
- Default por produto: overlay fiscal / `fiscal_padrao_ui_cadastro`.
- Remover linha com confirmação. Qtd e preço teclado (Enter adiciona).
- Total da nota no rail direito, sempre visível.

### Passo 3 — Fiscal da nota (não por item)

Campos que o operador **precisa ver**, com default seguro:

- Natureza: `VENDA` (editável).
- Série: config `NFE_EMISSAO_SERIE` (default `1`).
- Número: “automático na autorização” — não pedir na loja.
- Tipo: `1` saída, `mod=55` **fixo na UI** (chip, não combo 55/65).
- Ambiente: chip Homologação/Produção a partir de `NFE_DIST_DFE_TP_AMB` (mesmo do Dist-DFe). Homologação = faixa laranja “TESTE SEFAZ — DANFE sem valor fiscal”.
- Finalidade: `1` normal (NFe complementar / ajuste **fora do v1**).
- Frete: `9` sem frete default; se informar transportadora, passo extra recolhido (`<details>`).
- Informações complementares: textarea opcional (ex. pedido, placa).

Impostos: **não** abrir apuração completa no v1. Calcular XML com CSOSN 102 (SN sem crédito) + CFOP da linha. Texto no «?»: “Simples Nacional: o DANFE mostra CSOSN; não substitui o contador.”

### Passo 4 — Pagamento (tPag)

Mapear formas do PDV/caixa para `tPag` SEFAZ (01 dinheiro, 03/04 cartão, 15 boleto, 17 PIX, 90 sem pagto, 99 outros). Se fiado: `90` ou `05` crédito loja + aviso. Soma das parcelas = total. Sem isso a SEFAZ rejeita.

### Passo 5 — Conferir

Checklist grande, uma linha por item vermelho/verde:

- Destinatário completo
- Todo item com NCM 8 dígitos
- CFOP coerente com UF (5xxx interna / 6xxx inter)
- Totais batem
- Certificado configurado (reusar `distribuicao_dfe_configurada`)
- Ambiente (homolog/prod)

Prévia DANFE A4 (HTML print, não precisa PDF lib no v1): layout “parece DANFE” (emitente, dest, itens, totais, placeholder chave). Impressão F8.

### Passo 6 — PIN e envio SEFAZ

Igual Entrada NF: PIN de confirmação **antes** de transmitir. Estados:

1. Rascunho  
2. Assinando / enviando (botão desabilitado, texto “Enviando à SEFAZ…”)  
3. Autorizada → chave 44, protocolo, botões DANFE + XML  
4. Rejeitada → `cStat` + `xMotivo` **em português visível**, voltar ao passo do erro  
5. Contigência: **não no v1** (só mensagem “SEFAZ indisponível, tente de novo”)

---

## 5. Origens da nota (obrigatório: manual **e** a partir do PDV)

A tela **tem que** servir os dois jeitos. Não é “só avulsa” nem “só se achar a ficha da venda”.

### 5.1 Manual (avulsa, sem venda)

- Launchpad Comercial: **«NF-e saída (A4)»** → `/nfe/saida/` (`nfe_saida`).
- Na lista, botão **Nova (F4)** abre o editor vazio.
- Operador busca cliente e produtos na mão (scanner/teclado), igual Entrada NF / PDV.
- `venda_agro_id` fica vazio.
- No editor, atalho **«Puxar venda/pedido»** (busca por nº, cliente, data) — se escolher uma `VendaAgro`, vira o fluxo 5.2 (prefill + vínculo). Assim o caixa não precisa sair da tela de NF-e.

### 5.2 A partir de pedido/venda do PDV (prefill + vínculo)

Fonte: `VendaAgro` + `ItemVendaAgro` (a venda **já registrada** no Agro). Orçamento só no carrinho **não** vira NF-e 55: primeiro registra a venda (estoque/caixa), depois emite a nota grande.

Pontos de entrada (todos obrigatórios no v1):

| Onde | O que aparece |
| ---- | ------------- |
| **PDV, depois de gravar a venda** | Botão grande **«NF-e A4 (55)»** no comprovante / passo final do wizard (ao lado do cupom 80 mm e da NFC-e 65, se existir). Abre `/nfe/saida/?venda=<pk>` com editor já preenchido. **Não** transmite sozinho (diferente da NFC-e automática). |
| **Lista de vendas** | Ação **«NF-e 55»** na grade (venda não devolvida). Se já houver NF-e autorizada para aquela venda: vira **«DANFE»** (abre a nota, não duplica). |
| **Ficha da venda** | Botão laranja **«Emitir NF-e 55»** no header (`venda_agro_detalhe.html`). Se autorizada: **«Ver DANFE»**. |

Prefill:

- Destinatário: nome, documento (`cliente_documento` / CPF/CNPJ do cliente Agro), endereço se existir.
- Itens: qtd, nome, unitário, NCM/CFOP/CSOSN do cadastro/overlay.
- Pagamento: `forma_pagamento` / `pagamentos_json` → `tPag`.
- Totais iguais aos da venda; se o operador mudar preço/qtd, o rail avisa **«diferente da venda #id»** mas não bloqueia (nota pode divergir; vínculo permanece).

Regras:

- Uma NF-e **autorizada** por venda. Segunda emissão: avisa e oferece a existente.
- Venda **devolvida total**: não emite 55; se já autorizada, só consulta/cancelamento (fase 2).
- NFC-e 65 da mesma venda **pode coexistir** no v1 (aviso no «?»: cupom ≠ DANFE A4). Não bloquear 55 só porque tem 65 — o Composer mostra chip «esta venda tem NFC-e» e segue.
- Query: `/nfe/saida/?venda=123` cria rascunho (ou reabre o rascunho aberto daquela venda) e já entra no editor.
- API: `POST api/nfe-saida/de-venda/` `{venda_id}` → `{ok, id}` do rascunho.

**Não** emitir 55 em background no `salvar venda` do PDV (NFC-e já faz isso no cupom). A 55 sempre passa pela tela de conferência + PIN.

---

## 6. Dados, APIs e arquivos (para o Composer)

### 6.1 Persistência (v1)

Mongo, no mesmo espírito de entrada NF (`nfe_entrada_util.py`): coleção nova, ex. `AgroNfeSaida`.

Campos mínimos do documento:

- `status`: `rascunho` | `enviando` | `autorizada` | `rejeitada` | `cancelada` | `descartada`
- `mod`: `"55"` (sempre)
- `serie`, `nNF`, `chave`, `protocolo`, `c_stat`, `x_motivo`
- `tp_amb`, `destinatario` {}, `itens` [], `pagamento` [], `fiscal` {}, `inf_complementar`
- `venda_agro_id`, `usuario`, `created_at`, `updated_at`
- `xml_autorizado` (string, só após autorização)
- `pin_ok_em` (opcional)

### 6.2 Rotas (nomes)

Páginas:

- `nfe_saida` → `/nfe/saida/`
- editor na mesma página (lista + overlay editor), como entrada

APIs:

- `GET api/nfe-saida/listar/`
- `POST api/nfe-saida/salvar/` (rascunho)
- `GET api/nfe-saida/<id>/`
- `POST api/nfe-saida/<id>/emitir/` (PIN + SEFAZ; no **primeiro PR de UI** pode responder stub `{ok:false, erro:"SEFAZ emissão ainda não ligada"}` **desde que a UI trate o estado**)
- `GET api/nfe-saida/<id>/xml/` e prévia HTML DANFE
- `GET api/nfe-saida/status-cert/` — reusa Dist-DFe: configurada, UF, CNPJ mascarado, tp_amb, **mod_ui=55**

Reuso: `api` de busca produto PDV; busca cliente.

### 6.3 Arquivos previstos (quando for implementar)

- `produtos/templates/produtos/nfe_saida.html` (+ CSS no próprio arquivo, padrão entrada)
- `produtos/nfe_saida_util.py` (rascunho Mongo, status)
- `produtos/views.py` (views finas) + `produtos/urls.py`
- `produtos/templates/produtos/includes/nfe_saida_help_agents.html` + espelho `AGENTS.md` §11
- Launchpad em `dashboard_gerencial.html` (coluna Comercial)
- Botão em `venda_agro_detalhe.html`
- **Não** alterar `entrada_nota.html` além de um link “Emitir saída 55” opcional no header

### 6.4 Emissão de verdade (fase 2 — fora do primeiro recorte de tela)

Montar XML NF-e 4.00, assinar com o mesmo PFX, `NFeAutorizacao` SEFAZ-SP, consulta recibo, cancelamento. Bibliotecas já citadas: `cryptography`, `lxml`, `signxml`. Preferir módulo novo `produtos/sefaz_nfe_emissao.py`, **não** inchando `sefaz_dfe_client.py`.

O primeiro PR de Composer deve entregar **tela completa usável em rascunho + conferência + prévia**, com emitir chamando API (stub ou real). Não deixar botão morto sem feedback.

---

## 7. Regras de negócio visíveis na UI

- Chip permanente **«NF-e 55 · DANFE A4»** — nunca combo 55/65.
- Homologação: faixa laranja persistente.
- Rejeição: não apagar rascunho; destacar campo.
- Autorizada: travar edição; só DANFE, XML, (fase 2) cancelar.
- Mesma venda: no máximo uma NF-e autorizada; segunda tentativa avisa.
- Estorno de estoque: **esta tela não baixa estoque** (PDV/venda já baixou). Só documento fiscal.
- Financeiro Mongo: **não** gerar título novo na autorização v1 (venda/caixa já cuidam). Documentar no «?».

---

## 8. Acessibilidade / loja

- Contraste WCAG-ish: texto `slate-900` em branco; não usar orange em texto fino.
- Foco visível emerald no input.
- Clique fora **não** fecha o editor (igual modal XML da entrada).
- Esc: pede “sair sem salvar?” se houver dirty.
- Autosave rascunho (debounce), hint “salvo hh:mm” como entrada NF.

---

## 9. Ajuda «?» (rascunho de texto)

Slug `nfe_saida_o_que_e`: esta tela gera a **nota fiscal eletrônica grande (modelo 55)**, impressa em A4 (DANFE), para empresa, produtor ou venda que precisa de destinatário completo. **Não** é o cupom do caixa (NFC-e 65).

Slug `nfe_saida_destinatario`: CNPJ + IE para empresa; CPF + endereço para pessoa. Sem endereço a SEFAZ recusa.

Slug `nfe_saida_sefaz`: o certificado da loja (A1) assina a nota. Homologação = teste. Produção = nota válida. PIN evita clique acidental.

---

## 10. Recorte para o Composer (ordem)

1. Spec já está neste arquivo — **não redesenhar o produto**.
2. Página lista + editor + wizard 6 passos + rail direito + «?» + F1.
3. Mongo rascunho + listar/salvar/abrir.
4. Prefill a partir de `VendaAgro` (`?venda=` + `POST de-venda` + botões no PDV, lista e ficha).
5. Emissão **manual** (Nova) na mesma tela.
6. Semáforo e prévia DANFE HTML.
7. API emitir: stub claro **ou** SOAP se o PFX estiver no `.env` de teste.
8. Launchpad + PDV pós-venda + lista + ficha.
9. Atualizar `AGENTS.md` §3 (rota) e §7 (decisão: saída 55).

**Fora:** NFC-e 65, cancelamento, carta de correção, contingência, cálculo de ST, múltiplas séries na UI.

---

## 11. Prompt curto para colar no Composer

```
Implemente a tela de emissão de NF-e modelo 55 conforme docs/PLANO_TELA_EMISSAO_NFE_55.md.
Dois caminhos obrigatórios: (1) manual — Nova na lista /nfe/saida/; (2) a partir
de VendaAgro do PDV — botão no fim da venda, na lista de vendas e na ficha,
com /nfe/saida/?venda=<pk> e POST api/nfe-saida/de-venda/. Não emitir 55
automaticamente no salvar venda. Não invente NFC-e 65 nesta tela. Reuse o
padrão lista+editor de entrada_nota.html e a shell larga de caixa (AGENTS.md §5).
Primeiro PR: UI completa + rascunho + prefill de venda + prévia DANFE + emitir
com feedback (stub SEFAZ ok se certificado não estiver no ambiente). Branch: teste.
```
