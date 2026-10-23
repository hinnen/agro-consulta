# PREP — CHECKLIST 23/10 · PDV-RAPIDOS · v27.26.22

- **Base loja:** `producao` @ `b70a67b9` (v27.26.21)
- **Origem:** `cursor/pdv-rapidos-ac2d` @ `4274dec4` + `384fdd7a` + `501a2a53`
- **Migrate:** **SIM** `0150` e `0151` (slots e layout dos Rápidos no Postgres)
- **Caixa / checkout:** não entram no delta
- **PDV:** `pdv_wizard` ganha Alt+6 (Rápidos). F6 de orçamento permanece.

## Prova

```bash
.venv/bin/python scripts/verify_pdv_rapidos_path.py
```

## Checkpoint

```bash
./scripts/prep_checkpoint_checklist_2310_pdv_rapidos.sh
```

## Cutover

```bash
./scripts/cutover_loja_checklist_2310_pdv_rapidos.sh
AGRO_LOJA_FRASE='…' AGRO_LOJA_SENHA='…' ./scripts/cutover_loja_checklist_2310_pdv_rapidos.sh --exec
```

## Revert

```bash
git push origin rollback/pre-v272622-2310-pdv-rapidos:producao
```

Volta para **v27.26.21** @ `b70a67b9`. As tabelas `0150`/`0151` ficam no Postgres (não apagam dados).

## Smoke

**Ctrl+F5** no PDV → **Alt+6** → teclas **1–9** → **Esc** → Definir slot → arrastar proporção. F6 de orçamento continua igual.
