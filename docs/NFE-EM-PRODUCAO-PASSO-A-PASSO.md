# NF-e 55 na loja — passo a passo (simples)

## O que já funciona hoje

**Nova** · emitir com PIN · **DANFE** · **XML**.

- **Homologação** (`NFE_DIST_DFE_TP_AMB=2`): por padrão ainda **simula** a autorização (rápido para conferir tela). Para testar SOAP real em homolog, use `NFE_SAIDA_SIMULAR_EMIT=0`.
- **Produção** (`NFE_DIST_DFE_TP_AMB=1`): emite de verdade na SEFAZ-SP (**NFeAutorizacao4**, modelo **55**). Faixa «SEM VALOR FISCAL» some quando o ambiente é produção.

---

## 1. Deploy do código (Render)

- Branch do site da loja: **`producao`**
- Cada deploy já roda **`migrate`** (rascunhos **0146** · numeração NF-e **0147**)
- Se o DANFE novo já apareceu no **sistvale.com.br**, o código **já subiu**

---

## 2. Depois do deploy (uma vez no Shell do Render)

No serviço **Sistvale - Produção** → **Shell**:

```bash
cd /opt/render/project/src   # ou pasta do app, se diferente
./scripts/post_deploy_checklist_1810e_nfe_danfe_palavras_pdv3.sh
```

Isso faz: **migrate** · **backfill** dos apelidos de busca no PDV · provas automáticas.

Se o backfill já rodou antes, é seguro rodar de novo.

---

## 3. Teste rápido na loja (2 minutos)

1. Abrir **https://sistvale.com.br/nfe/saida/** (Ctrl+F5)
2. **Nova** → não pode aparecer «serviço legado indisponível»
3. Emitir em homolog (PIN **9973**) → **Imprimir DANFE** + **Baixar XML**
4. PDV: buscar **2 tempo** ou **oleo** (apelidos)

---

## 4. Ligar emissão **real** (produção SEFAZ)

No **Environment** do Render (Sistvale - Produção), conferir:

| Variável | Valor (GM Agro — produção) |
|----------|----------------------------|
| `NFE_DIST_DFE_TP_AMB` | **1** |
| `NFC_E_TP_AMB` | **1** |
| `NFE_SAIDA_SIMULAR_EMIT` | **0** (ou deixe vazio) |
| `NFE_EMISSAO_SERIE` | **2** |
| `NFE_EMISSAO_PROXIMO_NUMERO` | **1** |

Copiar no Render → **Environment** (uma linha por variável):

```
NFE_DIST_DFE_TP_AMB=1
NFC_E_TP_AMB=1
NFE_SAIDA_SIMULAR_EMIT=0
NFE_EMISSAO_SERIE=2
NFE_EMISSAO_PROXIMO_NUMERO=1
```

A **1ª NF-e autorizada** em produção será **nº 1 / série 2**; o sistema incrementa o contador no Postgres (`NfeNumeracaoAgro`) depois disso.

Mesmo **certificado A1** e dados do emitente (`NFC_E_*` / `NFE_DIST_DFE_*`) da NFC-e.

**Antes da 1ª nota real:** faça **uma** nota em homolog com `NFE_SAIDA_SIMULAR_EMIT=0` e `TP_AMB=2` para validar XML (opcional).

**PDV / NFC-e 65** não mudam por causa da NF-e 55.

---

## 5. Subir pacote novo (assistente)

Frase + senha **no mesmo chat** (regra `banana.md`):

- Ex.: *«pode subir para produção»* + senha **99738595**

Script (quem for operar o git):

```bash
./scripts/cutover_loja_checklist_1810e_nfe_danfe_palavras_pdv3.sh --exec
```

Variáveis: `AGRO_LOJA_SENHA=99738595` e `AGRO_LOJA_FRASE='...'`
