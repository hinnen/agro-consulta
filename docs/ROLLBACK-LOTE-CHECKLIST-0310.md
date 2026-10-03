# Rollback — lote checklist 03/10 (`deploy/prep-checklist-0310`)

Volta a loja ao estado **antes** deste lote (Live **v25.86**).

| Item | Valor |
| ---- | ----- |
| **Antes (loja)** | `producao` @ `f540081c` · VERSION **25.86** |
| **Tag sugerida** | `rollback/pre-checklist-0310-v25.86` |
| **Branch backup** | `producao-backup-pre-v2595-checklist-20261003` |
| **Branch PREP** | `deploy/prep-checklist-0310` · alvo loja **v25.95** |
| **Migrate** | **NÃO** |
| **Merge `teste`?** | **NÃO** — só cherry deste PREP |

## Pacotes do lote

| # | Pacote | Risco venda aberta |
| - | ------ | ------------------ |
| 1 | **ETQ-EAN-LOJA** | Baixo — códigos `230…` novos + impressão |
| 2 | **PDV-EDIT-CB-ETQ** | Baixo — só lápis/etiqueta do produto |
| 3 | **BUG-28-ENT-LOJA-PIN** | Médio — PIN pós-venda/entrega (é **correção** de loop) |
| 4 | **BUG-32-SO-ENT-OUTRA** | Médio — fluxo entrega→pagamento (é **correção** de trava) |

## Como voltar (só frase + senha)

```bash
git fetch origin tag rollback/pre-checklist-0310-v25.86
git checkout producao
git reset --hard rollback/pre-checklist-0310-v25.86
git push origin producao --force-with-lease
```

## Provas (no PREP)

- `verify_etq_ean_loja_path.py` **74/74**
- `verify_pdv_edicao_barras_etq_path.py` **74/74**
- `verify_bug28_entrega_loja_pin_path.py` **10/10**
- `verify_bug32_so_entrega_outra_loja_path.py` **8/8**
- `verify_pdv_ent_loja_lanc_path.py` **33/33**
- térmica **56/56** · várias **39/39**
