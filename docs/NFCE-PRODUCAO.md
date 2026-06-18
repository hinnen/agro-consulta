# NFC-e — checklist para produção (GM Agro / Jacupiranga-SP)

Deploy: branch **`producao`** → Render **Sistvale - Produção**.  
Staging validado em **`teste`** → **agro-consulta-staging**.

---

## 1. Modo de emissão (PDV)

| Variável | Produção recomendada |
|----------|----------------------|
| `NFC_E_MODO` | **`manual`** — operador marca **«Emitir cupom fiscal (NFC-e)»** no passo Pagamento |

- **`manual`** (padrão): venda grava normalmente; NFC-e só se o checkbox estiver marcado.
- **`auto`**: emite NFC-e em toda venda (comportamento anterior).

---

## 2. Variáveis Render — produção

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

## 3. Antes do merge `teste` → `producao`

- [ ] Staging: venda **sem** checkbox → **sem** NFC-e na venda.
- [ ] Staging: venda **com** checkbox → modal CPF → NFC-e autorizada + cupom 80 mm.
- [ ] Série **21** e numeração alinhada (sem conflito 539).
- [ ] Cupom impresso com QR Code e protocolo.
- [ ] ERP continua na série 20 (sem interferência).

---

## 4. Deploy produção

1. Merge `teste` → `producao` (quando autorizado).
2. Render aplica deploy automático.
3. Conferir `/api/nfce/status/` (logado): `ativo: true`, `modo: manual`, `tp_amb: 1`, `serie: 21`.
4. **Uma venda teste** com checkbox marcado; conferir autorização no portal SEFAZ / cupom.
5. Ajustar `NFC_E_PROXIMO_NUMERO` se necessário após primeira emissão real.

---

## 5. Operacional na loja

1. PDV → Pagamento → marcar **«Emitir cupom fiscal (NFC-e)»** quando o cliente quiser nota.
2. Informar CPF ou «sem identificação».
3. **Confirmar com impressão (F9)** para cupom térmico fiscal.
4. Venda sem checkbox = comprovante interno apenas (sem NFC-e).
5. Reimpressão: lista de vendas → **Imprimir** (2ª via do cupom fiscal se houver NFC-e autorizada).

---

## 6. APIs úteis

| Rota | Uso |
|------|-----|
| `GET /api/nfce/status/` | Config ativa / modo / série |
| `POST /venda/<pk>/nfce/emitir/` | Emitir NFC-e depois da venda |
| `GET /venda/<pk>/nfce/cupom/` | Cupom fiscal 80 mm |
| `GET /api/nfce/export-xml/?ano=&mes=` | ZIP XMLs do mês |

---

*Última revisão: emissão manual PDV + série 21 Agro.*
