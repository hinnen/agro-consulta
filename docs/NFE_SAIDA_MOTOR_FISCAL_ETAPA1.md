# Etapa 1 — Diagnóstico do motor fiscal NF-e 55 + proposta de telas

**Branch:** `cursor/nfe-fiscal-config-1e79` (a partir de `producao` @ `91c8ca6a`)  
**Sem migration / sem alteração de banco nesta etapa.**

Compilado com: código atual · prompt ChatGPT · combinados da loja (não merge `teste`, não NFC-e, não inventar alíquota, fiscal separado do endereço de entrega, tela compacta).

---

## 1. O que o XML realmente faz hoje

O motor (`nfe_saida_tributacao_util.py`) **não calcula ICMS/PIS/COFINS destacados** na operação típica SN. A montagem (`nfe_saida_sp_emissao_util.py`) **preenche `ICMSTot` com zeros literais**, exceto `vProd`/`vNF` e, se houver IBPT, `vTotTrib`.

Isso **não é necessariamente erro**. No Simples Nacional, o ICMS da venda comum **não é destacado** na NF-e (recolhimento pelo DAS). PIS/COFINS da mesma forma, com CST de “outras operações” / NT.

O DANFE lê o XML autorizado (`nfe_saida_danfe_html.py`) — **não inventa** totais.

### Campos zerados no `ICMSTot`

| Tag XML | Hoje | Zero é correto quando | Calcular / parametrizar quando |
|---------|------|------------------------|--------------------------------|
| `vBC`, `vICMS` | sempre `0.00` | CSOSN **102/103/300/400** (ICMS não destacado) | CSOSN **900** (e similares) se o contador exigir destaque — hoje **bloqueado** |
| `vICMSDeson` | `0.00` | sem desoneração | regra específica (não temos) |
| `vFCPUFDest`, `vICMSUFDest`, `vICMSUFRemet` | `0.00` | SN **não assume DIFAL** só por ser interestadual | só se o contador validar obrigação de DIFAL/FCP para o CNPJ |
| `vFCP` | `0.00` | sem FCP destacado | ST/FCP parametrizado |
| `vBCST`, `vST`, `vFCPST`, `vFCPSTRet` | `0.00` | item **sem** ST nesta operação | CSOSN **201/202/203/500** — motor **bloqueia** até haver parâmetros |
| `vII`, `vIPI`, `vIPIDevol` | `0.00` | não importador / não IPI | não previsto |
| `vPIS`, `vCOFINS` | `0.00` | CST **49** (grupo `PISOutr`/`COFINSOutr` com BC/alíq 0) ou CST **04–09** (`PISNT`) | CST tributado 01/02 — **não mapeado**; bloqueia |
| `vFrete`, `vSeg`, `vDesc`, `vOutro` | `0.00` | operação sem esses valores | frete na NF-e ainda não entra no XML |
| `vTotTrib` | omitido ou IBPT | Lei **12.741/2012** (aproximado ao consumidor) — **não** é imposto destacado | só com tabela IBPT válida (`NFC_E_IBPT_TOKEN` ou fallback após validação) |

### Por item (`det/imposto`)

| Grupo | Comportamento |
|-------|----------------|
| `ICMSSN102` | orig + CSOSN 102/103/300/400 — **sem** `vICMS` |
| `ICMSSN101` | exige `NFE_SAIDA_PCRED_SN_ALIQ` (uma alíquota no `.env`, **não** por competência) — senão **bloqueia** |
| `ICMSSN201/202/203/500/900` | **bloqueio** (“parametrização adicional”) |
| `PISNT` / `COFINSNT` | CST 04–09 |
| `PISOutr` / `COFINSOutr` | CST 49 (e lista 50–99) com **vBC/p/v = 0** |

**Default perigoso (vamos mudar na etapa 3):** se o produto não tem CSOSN, o motor **preenche 102**. O prompt pede o contrário: **bloquear** se não houver regra. Manter 102 só quando o **contador validar** a regra geral “venda SN consumidor / sem crédito”.

CST PIS vazio → cai em **49** via padrão cadastro SP/SN (`agro_produto_fiscal_defaults.py`). Mesma lógica: na NF-e deve ser **regra validada**, não silêncio.

---

## 2. Três coisas que não são o mesmo imposto

| Conceito | Onde aparece | Hoje |
|----------|----------------|------|
| **Destacado na NF-e** | grupos ICMS/PIS/COFINS + `ICMSTot` | zeros na venda SN 102/49 — **legítimo** se o CSOSN/CST estiverem certos |
| **Recolhido no DAS** | fora da NF-e | o sistema **não** calcula DAS (correto) |
| **Crédito ao destinatário** | CSOSN 101 `pCredSN`/`vCredICMSSN` | só com alíquota no `.env`; **um** percentual para todos os meses |
| **Aproximado Lei 12.741** | `vTotTrib` + texto `infCpl` | só se IBPT ligado; senão alerta, **não** inventa |

---

## 3. Cadastros e lojas

**Produtos:** NCM, CEST, CFOP, CSOSN, origem, CST PIS/COFINS já existem (overlay/gestão + `fiscal_por_produto_id`). NFC-e ainda usa defaults genéricos (`23099020`/`5102`/`102`) no cadastro UI. **NF-e não deve copiar esse genérico** (já bloqueia NCM vazio no motor; o JS da busca de produto na NF-e ainda pode sugerir `23099020` — gap).

**Clientes:** bloco `fiscal_nfe` no `ClienteAgro` (IE, situação ICMS, finalidade, endereço fiscal) **sem** alterar entrega do PDV. Falta aba visual na ficha.

**Estabelecimentos:** Centro e Vila já existem na NFC-e (`nfce_cfg("centro"|"vila")`, CNPJs no `.env`). NF-e 55 hoje puxa **um** emitente (`NFE_DIST_DFE_CNPJ` + série `NFE_EMISSAO_SERIE`). **Isolar Centro/Vila na NF-e 55** é requisito da tela fiscal (mesmo padrão NFC-e, séries **próprias** da 55).

**Configuração do sistema hoje:** F11 abre modal **leve** (animações + link Planos de contas). Fiscal **não** cabe nesse modal. Padrão visual de página: `planos_conta_config.html` (shell 100dvh, fonte grande).

**Certificado:** `.env` / secret Render (`NFC_E_CERT_*`). Na UI: só **situação** (ok / vencendo / ausente), **nunca senha**.

---

## 4. Lacunas já conhecidas (prefeitura + legal)

- `cMun` do destinatário cai em Jacupiranga se a cidade for outra.
- Sem perfil “órgão público” / empenho (`xPed` / `infCpl`).
- Sem prévia “Conferência Fiscal” na tela de emitir.
- Sem regras por competência, por CNPJ, com vigência e “quem aprovou”.
- Interestadual **não** gera DIFAL (certo até o contador dizer o contrário).
- CNPJ **não** implica crédito (já: finalidade e situação ICMS separam revenda vs consumo).

---

## 5. Proposta de telas (não implementar nesta etapa)

### 5.1 Entrada

No F11, **um link** (como Planos de contas): **Fiscal / NF-e** → `/configuracao/fiscal-nfe/`  
Só `is_staff`. Operador de caixa **não** edita.

Layout: igual Planos de contas — 16:9, fonte grande, abas no topo, **seletor Centro | Vila** sempre visível.

### 5.2 Abas (compactas; textos longos no «?»)

**A. Empresa** — leitura dos dados já usados na emissão (CNPJ, IE, CRT=1 SN, UF, município IBGE, série NF-e 55, ambiente homolog/prod). Status do A1 (válido/ausente), sem senha. Pendência se série/CNPJ da Vila não estiver isolado.

**B. Simples / ICMS** — tabela **por competência** (mês/ano, % crédito, vigência, status Pendente / Preenchido / Validado). Exceções ST em lista, vazias até o contador. **Não** um único `%` no `.env`.

**C. PIS/COFINS** — CST padrão da operação (ex.: 49) só depois de Validado. Sem alíquota inventada.

**D. IBPT** — fonte, validade, ligado/desligado. Sem tabela = pendência, XML **sem** `vTotTrib`.

**E. Regras por operação** — linhas: interna/interestadual × finalidade × situacao ICMS × (opcional NCM/categoria) → CFOP + CSOSN. Status e “aprovado por”. Precedência: exceção produto > categoria > regra geral. Sem regra = **bloquear emissão** (não cair em 102).

**Rodapé:** botão **Questionário para o contador** (copiar/PDF) com os 9 blocos do prompt, **dois CNPJs**. Respostas entram nas abas; campos ficam Pendente até Validado.

### 5.3 Na NF-e (etapa 4 do prompt, depois)

Passo **Conferência Fiscal** (antes do PIN): por item NCM, CFOP, CSOSN, CST, valores do **mesmo** `apurar_nfe_saida`, texto do tipo *“ICMS não destacado nesta operação do Simples Nacional, CSOSN 102”*. Avisos de cMun/IBPT/pendência.

Não duplicar cadastro de produto. Aba Fiscal na ficha do cliente (só dados `fiscal_nfe`).

---

## 6. Dados necessários (rascunho — **não criar tabelas agora**)

Quando autorizar a etapa 2:

1. `NfeEstabelecimentoFiscal` — chave `centro`/`vila` (CNPJ), série 55, CRT, status questionário.
2. `NfeAliquotaCreditoSn` — competência, %, vigência, aprovador.
3. `NfeRegraOperacao` — filtros + CFOP/CSOSN + vigência + status.
4. `NfeParametroIbpt` — versão/validade (token continua no secret).
5. Audit log (quem mudou o quê).

Certificado e senhas **permanecem** no Render. Postgres só parâmetros fiscais.

---

## 7. O que **não** faremos (desvios conscientes do prompt)

- Merge de `teste` / PR #405.
- Alterar NFC-e, PDV, notas já autorizadas.
- Inventar % de crédito, DIFAL, IBS/CBS, NCM.
- Esconder Configuração Fiscal dentro do modal F11 de animações.
- Assumir que todo zero no XML é bug.

## 8. Próximo (etapa 2, só com seu OK)

Página Configurações → Fiscal / NF-e + questionário + modelos PG, **campos vazios Pendente**, motor ainda lendo `.env` até a etapa 3 ligar as regras.
