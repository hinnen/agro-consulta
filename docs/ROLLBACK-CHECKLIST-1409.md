# Rollback — Checklist 14/09 (`deploy/prep-checklist-1409` · alvo loja **v25.20**)

Ponto **antes** deste lote = loja **Live v23.99** (`a43340a` / REPASSE-DIA-HERO).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `a43340a` (Live v23.99) |
| **Tag** | `rollback/pre-checklist-1409-v23.99` |
| **Branch backup** | `producao-backup-pre-v2520-checklist-20260914` |
| **Branch PREP** | `deploy/prep-checklist-1409` · tip **v25.20** |
| **O quê sobe** | bugs **#18–#22, #24–#26** · `PDV-ENTREGAS-MODAL-BODY` · `PDV-FECHAR-CTA-QUITADO` · `PDV-ORC-IMPRIMIR` |
| **O quê NÃO sobe** | merge do `teste` · bug **#23** (já Live) · DRE/Excel/WA WIP · resto do tip |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-1409-v23.99
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.99**.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `NF-PIN-EXIGE-FIN` (#18) | **Não** venda | Só Entrada NF · bloqueia PIN sem «a pagar» |
| `CP-BAIXA-DESC` (#19) | **Não** | Campo opcional na baixa CP |
| `PDV-TABELA-PRINT-FORMA` / `PDV-PRECO-FORMA-DIN` (#20/#24) | **Sim** (preço/notinha) | Dinheiro deve usar tabela A · corrige preço errado de crédito |
| `PDV-CONFIRM-QUITADO-TROCAR` (#21) | **Sim** (Confirmar) | Só libera Confirmar após Trocar com quitado |
| `PDV-FINAL-TIMEOUT-UI` (#22) | **Sim** (Confirmar) | Timeout 55s + layout PAGAR 1440 · não muda regra de venda |
| `CAIXA-ABERTURA-MILHAR` (#25) | **Não** venda | Só abrir caixa · milhar BR |
| `PIN-VENDA-45S` (#26) | **Sim** (PIN) | TTL 45s · **próxima venda pede PIN de novo** |
| `PDV-ENTREGAS-MODAL-BODY` | **Não** (só UI) | Modal Entregas no Pagamento |
| `PDV-FECHAR-CTA-QUITADO` | **Sim** (pós-pago) | Popup fechar · não muda estoque/caixa |
| `PDV-ORC-IMPRIMIR` | **Não** venda | Só orçamento Salvar\|Imprimir |

**Piora venda/caixa?** Não esperado se smoke local OK. Point / fiado / vale Live **v23.99** permanece.

## Provas (pré-envio · tip teste + PREP)

| Pacote | Prova |
| ------ | ----- |
| #18 | **9/9** |
| #19 | **12/12** |
| #20 | **17/17** |
| #21 | **31/31** |
| #22 | **13/13** |
| #24 | **15/15** |
| #25 | **14/14** |
| #26 | **17/17** · pin-na-ação **79/79** · PIN **9973** |
| Entregas modal | **37/37** (no tip `teste`) |
| Fechar CTA | **60/60** |
| Orçamento Imprimir | path OK |

## Depois do deploy (você)

1. **Ctrl+F5** em todos os PCs · badge **v25.20**
2. Smoke rápido: 1 venda Dinheiro · PIN na próxima · Entregas no Pagamento · Trocar c/ quitado
3. Se algo estranho → rollback (frase + senha) para **v23.99**
