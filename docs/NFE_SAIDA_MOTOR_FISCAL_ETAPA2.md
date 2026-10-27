# Etapa 2 — Página Fiscal / NF-e + modelos PostgreSQL

**Branch:** `cursor/nfe-fiscal-config-1e79` (continua a Etapa 1, a partir de `producao`)  
**Motor / XML / DANFE / NFC-e:** intocados. Parâmetros novos **não** entram em `apurar_nfe_saida`.

## O que foi entregue

- Página `/configuracao/fiscal-nfe/` (staff), visual no padrão Planos de contas.
- Link **Fiscal / NF-e** no F11 (somente o link; formulários fora do modal).
- Seletor **Centro | Vila Elias** no topo; dados isolados por estabelecimento.
- Abas: Empresa, Simples/ICMS, PIS/COFINS, IBPT, Regras, Questionário (9 grupos).
- Questionário: copiar texto + impressão do navegador (PDF).
- Status: Pendente / Preenchido / Validado. Preencher **não** valida.
- Validar exige cadastro completo + nome/CRC/data do **contador** (distinto do usuário staff que clicou).
- Formato ao validar (e ao gravar se preenchido): CFOP 4 dígitos, CSOSN 3, CST PIS/COFINS 2, NCM 8 quando informado. Sem tabela de códigos e sem completar zeros.
- APIs de gravação recusam estabelecimento desconhecido (não assumem Centro).
- Exceções PIS/COFINS: operação, produto, NCM, CST PIS, CST COFINS.
- Página: DOM com `textContent` / `.value` (sem `innerHTML` de dados).
- Migração `0153` (campos do contador). `0152`/`0153` via `migrate` após histórico até `0151` em Postgres descartável.
- `0039` (SQLite `get_model`) e `0065` (seed vivo lê `exibir_pdv` da `0085`) diagnosticadas, **sem** reescrita e **sem** fake em produção/staging. Em staging a `0065` já está aplicada; o risco da `0065` é banco vazio, não o `migrate` de `0152`.

## Modelos (`produtos.0152_nfe_fiscal_config` + `0153_nfe_fiscal_contador`)

1. `NfeEstabelecimentoFiscal`
2. `NfeAliquotaCreditoSn` (unicidade estabelecimento+competência)
3. `NfeParametroPisCofins` (+ exceções JSON, sem alíquota)
4. `NfeParametroIbpt` (token permanece no secret)
5. `NfeRegraOperacao`
6. `NfeExcecaoIcmsSt` (estrutura ST/DIFAL, sem cálculo)
7. `NfeQuestionarioResposta`
8. `NfeAlteracaoFiscalLog`

Sem seed de regras. Tabela criada ≠ configuração ativa.

## Pendente para Etapa 3+

- Ligar regras validadas ao motor; **bloquear** CSOSN 102 silencioso.
- Série/CNPJ da Vila na **transmissão** (hoje a emissão 55 ainda usa o emitente único).
- `cMun`, NCM `23099020` no JS, Conferência Fiscal, IBS/CBS, DIFAL/ST calculados.
- Não aplicar esta migração em produção até o merge explícito.

## Staging sem merge integral

Cherry-pick dos commits desta branch em `teste`, ou deploy da própria branch no serviço de staging. **Não** merge `teste`↔`producao`.
