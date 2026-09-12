# Rollback — Checklist 12/09c (loja alvo **v23.96**)

Ponto **antes** deste lote = loja **Live v23.95** (`8faa4c1` / checklist 12/09b).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `8faa4c1` (Live v23.95) |
| **Tag** | `rollback/pre-checklist-1209c-v23.95` |
| **Branch backup** | `producao-backup-pre-v2396-checklist-20260912c` |
| **Branch PREP** | `deploy/prep-checklist-1209c` · tip **v23.96** |
| **O quê sobe** | `PDV-ENT-CARD-LATERAL` · `PDV-ENT-ALERTA-POR-ID` · `PDV-ENT-MUDAR-LOJA` |
| **O quê NÃO sobe** | merge do `teste` · DRE/Excel/dashboard WIP · outros |
| **Migrate** | **SIM** `produtos.0131` (`loja_pagamento`) |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-checklist-1209c-v23.95
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.95**.  
**Atenção migrate:** ao voltar, o código some o campo `loja_pagamento`; a coluna no Postgres pode permanecer (inofensiva) até reverse manual se quiser limpar.

## Risco operacional (lojas abertas)

| Pacote | Afeta finalizar venda / PDV? | Nota |
| ------ | ---------------------------- | ---- |
| `PDV-ENT-CARD-LATERAL` | Baixo | Card Entregas: tags, Adiar 1h, Maps, layout |
| `PDV-ENT-ALERTA-POR-ID` | Baixo | Adiar alerta por entrega (não global) |
| `PDV-ENT-MUDAR-LOJA` | Médio | Mudar Loja (entrega/pagamento/ambas) · precisa caixa destino aberto · migrate **0131** |

**Piora venda/caixa?** Não esperado — só overlay/fluxo Entregas. Venda normal / Point / caixa Live **v23.95** permanece.

## Provas (pré-envio · tip)

| Pacote | Prova |
| ------ | ----- |
| `PDV-ENT-CARD-LATERAL` | **51/51** · PIN **9973** |
| `PDV-ENT-ALERTA-POR-ID` | **51/51** · PIN **9973** |
| `PDV-ENT-MUDAR-LOJA` | **39/39** · PIN **9973** |
| Regressão | `manage.py check` OK · views só patch mudar-loja (sem WIP tip) |

## Depois do deploy (você)

1. **Ctrl+F5** · badge **v23.96**
2. Entregas: tags sem scroll · **Mudar Loja** · **Adiar 1 Dia** · Adiar 1h por card
3. Mudar só pagamento: caixa da outra loja precisa estar **aberto**
4. Venda normal / caixa: sem mudança esperada
