# PREP deploy — CHECKLIST 09/10h · alvo **v26.78**

**Status:** ✅ **enviado / Live v26.78** — `producao` @ `5f460cb6` (10/10/2026).

## CHECKLIST ÚNICO (o que sobe)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **ETQ-BIP-230-VARIANTES** | bip **10/10** · etq **74/74** | **NÃO** |
| 2 | **CADASTRO-CB-230-MAX-NCM** | gerador **19/19** | **NÃO** |
| 3 | **PDV-VALOR-RS-KG** | path **46/46** · PIN **9973** | **NÃO** |

Extra (opcional pós-deploy): `python manage.py migrar_cb_loja_legado` — **não** entra no cutover automático.

**Pausa loja:** ~1–2 min (Render build; **sem** migrate Django).

**Tip PREP:** `deploy/prep-checklist-0910h` · base Live **v26.76** @ `52da69c2`

## Já pré-armado

| Item | Valor |
| ---- | ----- |
| Tip PREP | `origin/deploy/prep-checklist-0910h` |
| Rollback tag | `rollback/pre-checklist-0910h-v26.76` → Live **v26.76** |
| Backup branch | `producao-backup-pre-v2678-checklist-0910h` |
| Doc rollback | `docs/ROLLBACK-CHECKLIST-0910h.md` |
| Script | `scripts/cutover_loja_checklist_0910h.sh` |

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_0910h.sh --exec
```

## Smoke

- Ctrl+F5 · badge **v26.78**
- Bip GM4045: `2300000001479` **ou** `2300000001471`
- Cadastro: botão **230** (Fiscal)
- Wizard: produto **KG** + Enter → modal R$ · produto **UN** + Enter → **sem** modal

## Voltar

`docs/ROLLBACK-CHECKLIST-0910h.md`
