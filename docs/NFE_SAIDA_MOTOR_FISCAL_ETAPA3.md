# Etapa 3 — Motor lê regras Validadas (sem transmissão)

**Branch:** `cursor/nfe-fiscal-config-1e79`  
**Não** NFC-e 65 · **não** PDV/estoque/financeiro · **não** Etapa 4 (Conferência) · **não** produção.

O prompt do ChatGPT foi cruzado com o combinado da loja (Etapas 1–2): não inventar alíquota/NCM/DIFAL/IBS-CBS; zero no XML SN 102/49 continua legítimo **quando a regra Validada diz isso**; Centro/Vila sem misturar CNPJ/série.

## O que o motor faz agora

- Só aplica `NfeRegraOperacao` com status **Validado**, na data de emissão, do estabelecimento da nota.
- Precedência: produto/NCM → categoria → geral. Conflito no mesmo nível **bloqueia**.
- Sem regra Validada: **bloqueia** (não preenche CSOSN 102, CFOP 5102 nem CST 49).
- Cadastro explícito do item/produto incompatível com a regra: **bloqueia** (não substitui em silêncio).
- CSOSN 101: `pCredSN` da competência **AAAA-MM** Validada daquele estabelecimento (`Decimal`). Sem o mês certo: bloqueia. Não usa `.env` nem outro CNPJ.
- PIS/COFINS: CST da regra / parâmetro Validado / exceção; grupos XML já existentes; CST tributado não mapeado bloqueia.
- IBPT só com cadastro Validado **e** fonte no servidor. Sem isso, omite `vTotTrib`.
- ST/DIFAL: continua sem cálculo; flag ST no item bloqueia.
- Destinatário: não classifica só por CPF/CNPJ/IE. `cMun` de 7 dígitos; cidade diferente da do emitente **não** cai em Jacupiranga.
- Emitente: CNPJ/série do estabelecimento do documento. Vila sem série 55 própria não usa a do Centro. Série vinda do navegador divergente é recusada.
- API emitir: valida **antes** de marcar `ENVIANDO`; rascunho permanece se houver bloqueio.
- PIN / `is_staff` / XSD PL 010f / transmissão manual: mantidos. **Nenhuma nota foi transmitida neste trabalho.**

## Primeira NF-e simples (depois de validar com o contador)

1. Fiscal / NF-e → **Centro** (ou Vila, se for esse o emitente).
2. Empresa: CRT + série 55; Validar com nome/CRC/data do contador.
3. Uma regra geral Validada: interna × consumidor × CFOP 4 dígitos × CSOSN 3 × CST PIS/COFINS 2, vigência cobrindo a data.
4. Produto com NCM de 8 dígitos real (não genérico da NFC-e).
5. Destinatário com cidade + **cMun IBGE** se não for o município do emitente.
6. Emitir manualmente com PIN. Homologação vs produção continua no `tpAmb` do servidor.

Sem esses pontos o motor **bloqueia de propósito**.
