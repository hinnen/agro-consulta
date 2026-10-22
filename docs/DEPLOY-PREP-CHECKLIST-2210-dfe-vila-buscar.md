# PREP — CHECKLIST 22/10 · DFE-VILA-BUSCAR · v27.26.21

- **Base loja:** `producao` @ `25844530` (v27.26.20)
- **PREP:** `deploy/prep-checklist-2210-dfe-vila-buscar`
- **Migrate:** não
- **PDV / caixa:** não alterados (só Entrada NF · aba SEFAZ / Dist DF-e)

## O quê

- **Buscar notas novas** consulta Dist DF-e para **Centro** (`NFC_E_CNPJ`) e **Vila fiscal** (`NFC_E_VILA_CNPJ` / `48900774000286`).
- Cursor `ultNSU` **por CNPJ**; caixa PG unificada; etiqueta **Vila Elias** na lista; ciência/manifestação no CNPJ correto.
- Override opcional: `NFE_DIST_DFE_CNPJS`.

## Prova local

```bash
.venv/bin/python scripts/verify_dfe_multi_cnpj.py
.venv/bin/python scripts/verify_dfe_nsu_656.py
```

## Checkpoint

```bash
./scripts/prep_checkpoint_checklist_2210_dfe_vila_buscar.sh
```

## Cutover (dry-run / exec)

```bash
./scripts/cutover_loja_checklist_2210_dfe_vila_buscar.sh
AGRO_LOJA_FRASE='…' AGRO_LOJA_SENHA='…' ./scripts/cutover_loja_checklist_2210_dfe_vila_buscar.sh --exec
```

## Revert

```bash
git push origin rollback/pre-v272621-2210-dfe-vila-buscar:producao
```

Volta para **v27.26.20** @ `25844530`.

## Smoke loja

**Ctrl+F5** em `/entrada-nota/` → aba SEFAZ → **Atualizar status** (dois CNPJs) → **Buscar notas novas** → nota da Vila em Pendentes com etiqueta **Vila Elias**.
