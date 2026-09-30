# Rollback — ETQ-TERMICA-VARIAS (loja **v25.36**)

Ponto **antes** deste pacote = loja **Live v25.35** (`a854a182`).

| Item | Valor |
| ---- | ----- |
| **Commit loja antes** | `a854a182` (Live v25.35) |
| **Tag** | `rollback/pre-etq-termica-varias-v25.35` |
| **Branch backup** | `producao-backup-pre-v2536-etq-termica-20260930` |
| **O quê subiu** | `ETQ-TERMICA-VARIAS` — térmica imprime a fila, nome sem reticências, centavos separados, barras com faixa |
| **O quê NÃO subiu** | merge do `teste` · entrega · caixa · Excel clientes · PIN · código da nota |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-etq-termica-varias-v25.35
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v25.35**.

## Risco operacional

| Pacote | Afeta finalizar venda / caixa? | Nota |
| ------ | ------------------------------ | ---- |
| `ETQ-TERMICA-VARIAS` | **Não** | Só impressão de etiqueta (tela, histórico, etapa 6 da nota, cadastro). A6/gôndola continua várias na mesma folha. |

**Piora venda/caixa?** Não esperado.
