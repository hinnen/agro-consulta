# Rollback — Checklist 30/09 (loja alvo **v25.75**)

Ponto **antes** deste lote = loja **Live v25.36** (`c8b78d80` / etiqueta térmica).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `c8b78d80` (Live v25.36) |
| **Tag** | `rollback/pre-checklist-3009-v25.36` |
| **Branch backup** | `producao-backup-pre-v2575-checklist-20260930` |
| **Branch PREP** | `deploy/prep-checklist-3009` · alvo **v25.75** |
| **O quê sobe** | os 16 do checklist (entrega, fiado, Excel, PIN, busca, carrinho, cofrinho, nota, Dispenser) |
| **O quê NÃO sobe** | merge do `teste` · unificação de fichas (já feita no dado da loja em 30/09) · etiqueta térmica (já Live) |
| **Migrate** | **SIM** `0133` + `0134` + `0135` (campos novos, venda de hoje não muda sozinha) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-3009-v25.36
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.36**.

## Provas no PC (30/09, antes do envio)

Não rodou no banco da loja. SQLite local.

| Prova | Resultado |
| ----- | --------- |
| PIN no Point | 14/14 |
| Dia da entrega + ver a outra loja | 40/40 |
| Cartão / Pix de outro dia | 68/68 |
| Excel clientes | 42/42 |
| Limite no fiado + nome | 13/13 |
| Limite no card do PDV | 39/39 |
| PIN na Gestão | 43/43 |
| Busca do PDV | 30/30 |
| Nome no carrinho | 34/34 |
| Ordem e Zap no fiado | 59/59 |
| Código na nota | 24/24 |
| Fechar sem separar cofre | 15/15 |
| Cofrinho (botão Separar continua) | 39/39 |
| Logos do Dispenser | 27/27 |

## Depois do deploy

1. **Ctrl+F5** · badge **v25.75**
2. Venda normal de hoje não muda o esperado do caixa
3. Fechar a Vila: sem faixa «Separe» automática
4. Entrega de amanhã: o número do botão só sobe no dia
