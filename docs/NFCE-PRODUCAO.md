# NFC-e — checklist para produção (GM Agro / Jacupiranga-SP)

Deploy: branch **`producao`** → Render **Sistvale - Produção**.  
Staging validado em **`teste`** → **agro-consulta-staging**.

---

## 1. Modo de emissão (PDV)

| Variável | Produção recomendada |
|----------|----------------------|
| `NFC_E_MODO` | **`manual`** (padrão) — emissão **por forma de pagamento** |

Com `NFC_E_MODO=manual` (padrão):

| Forma de pagamento | NFC-e |
|--------------------|-------|
| PIX | **Automática** |
| Cartão de débito | **Automática** |
| Cartão de crédito | **Automática** |
| Cartão de crédito parcelado | **Automática** |
| Dinheiro, Fiado, Vale crédito, Cashback, Outro… | **Opcional** — checkbox no PDV |

- **`auto`**: emite NFC-e em **toda** venda (ignora regra acima).
- Venda mista (ex.: PIX + Dinheiro) → NFC-e **automática** (há forma automática).

---

## 2. XML mensal (contabilidade)

API pronta para o escritório (ZIP com XMLs autorizados do mês):

`GET /api/nfce/export-xml/?ano=2026&mes=6`

Tela de relatórios: pode ser ligada depois a essa rota (usuário vai indicar onde colocar).

---

## 3. Variáveis Render — produção

Copiar do staging e **conferir** estes valores:

| Variável | Valor GM Agro |
|----------|----------------|
| `NFC_E_ENABLED` | `true` |
| `NFC_E_TP_AMB` | **`1`** (SEFAZ produção) |
| `NFC_E_MODO` | **`manual`** |
| `NFC_E_SERIE` | **`21`** (não usar série 20 do ERP) |
| `NFC_E_PROXIMO_NUMERO` | Próximo livre após última NFC-e autorizada |
| `NFC_E_CMUN` | **`3524600`** (Jacupiranga) |
| `NFC_E_CIDADE` | `Jacupiranga` |
| `NFC_E_UF` | `SP` |
| `NFC_E_CNPJ` | `48900774000103` |
| `NFC_E_CERT_BASE64` | Certificado A1 **produção** (não o de homologação) |
| `NFC_E_CERT_PASSWORD` | Senha do .pfx |
| `NFC_E_CSC_ID` / `NFC_E_CSC_TOKEN` | CSC **produção** (portal NFC-e SP) |
| Demais emitente | IE, razão, endereço, CEP, fone — iguais ao cadastro SEFAZ |

Opcional (Lei 12.741 / IBPT no cupom): `NFC_E_IBPT_TOKEN`, `NFC_E_IBPT_CNPJ`.

---

## 4. Antes do merge `teste` → `producao`

- [ ] Staging: venda **PIX/cartão** → NFC-e automática (modal CPF).
- [ ] Staging: venda **Dinheiro** sem checkbox → **sem** NFC-e.
- [ ] Staging: venda **Dinheiro** com checkbox → NFC-e autorizada.
- [ ] Série **21** e numeração alinhada (sem conflito 539).
- [ ] Cupom impresso com QR Code e protocolo.
- [ ] ERP continua na série 20 (sem interferência).

---

## 5. Deploy produção

1. Merge `teste` → `producao` (quando autorizado).
2. Render aplica deploy automático.
3. Conferir `/api/nfce/status/` (logado): `ativo: true`, `modo: por_forma`, `tp_amb: 1`, `serie: 21`.
4. **Uma venda teste** PIX e uma Dinheiro com checkbox; conferir SEFAZ / cupom.
5. Ajustar `NFC_E_PROXIMO_NUMERO` se necessário após primeira emissão real.

---

## 6. Operacional na loja

1. **PIX ou cartão** → NFC-e sai **sozinha** (modal CPF na confirmação).
2. **Dinheiro, fiado, etc.** → marque **«Emitir cupom fiscal (NFC-e)»** se o cliente quiser nota.
3. **Confirmar com impressão (F9)** para cupom térmico fiscal.
4. Reimpressão: lista de vendas → **Imprimir** (2ª via se houver NFC-e autorizada).

---

## 7. APIs úteis

| Rota | Uso |
|------|-----|
| `GET /api/nfce/status/` | Config ativa / modo / série |
| `POST /venda/<pk>/nfce/emitir/` | Emitir NFC-e depois da venda |
| `GET /venda/<pk>/nfce/cupom/` | Cupom fiscal 80 mm |
| `GET /api/nfce/export-xml/?ano=&mes=` | ZIP XMLs do mês |

---

*Última revisão: emissão por forma de pagamento + série 21 Agro.*
