# NF-e 55 na loja — passo a passo (simples)

## O que já funciona hoje (homologação)

Com **faixa preta** «SEM VALOR FISCAL» = **teste SEFAZ**, igual ao print que você mandou.  
**Nova** · emitir com PIN · **DANFE** · **XML** — isso é o esperado até ligar SEFAZ **produção** (fase 2).

---

## 1. Deploy do código (Render)

- Branch do site da loja: **`producao`**
- Cada deploy já roda **`migrate`** (inclui tabela **`NfeSaidaRascunhoAgro`** — migration **0146**)
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

## 4. O que ainda NÃO é «nota fiscal de verdade» na SEFAZ

| Hoje | Depois (fase 2) |
|------|------------------|
| Ambiente **homologação** (`NFE_DIST_DFE_TP_AMB=2`) | `TP_AMB=1` produção |
| Autorização **simulada** | Webservice **NFeAutorizacao4** real |
| DANFE para conferência | DANFE com valor fiscal |

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
