# PREP deploy — CHECKLIST 09/10f · alvo **v26.76**

**Status:** ✅ **enviado / Live v26.76** — `producao` @ `896327e9` (09/10/2026).

## CHECKLIST ÚNICO (o que sobe)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **ETQ-EAN-LOJA-DV** (+ botão **230** cadastro Postgres) | etq **74/74** · gerador **15/15** · termica **69/69** · unit **5/5** | **NÃO** |
| 2 | **PDV-VALOR-RS-MODAL** (wizard `/pdv/checkout/`) | path **39/39** · PIN **9973** · qty 1,064 | **NÃO** |

**Pausa loja:** ~1–2 min (Render build; **sem** migrate).

**Tip PREP:** `deploy/prep-checklist-0910f` (ancestral = Live **v26.74** @ `origin/producao`)  
Alias legado (mesmo tip): `deploy/prep-pdv-valor-rs-modal`.

## Já pré-armado (não gasta tempo na senha)

| Item | Valor |
| ---- | ----- |
| Tip PREP | `origin/deploy/prep-checklist-0910f` |
| Rollback tag | `rollback/pre-checklist-0910f-v26.74` → tip Live **v26.74** |
| Backup branch | `producao-backup-pre-v2676-checklist-0910f` |
| Rollback docs | `docs/ROLLBACK-ETQ-EAN-LOJA-DV.md` · `docs/ROLLBACK-PDV-VALOR-RS-MODAL.md` |
| Script dry-run | `scripts/cutover_loja_checklist_0910f.sh` |

## Na senha (caminho mais curto)

```bash
# Opção A — um comando (recomendado)
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_0910f.sh --exec

# Opção B — manual
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-checklist-0910f
git push origin producao
```

Acompanhar Render até Live → **Ctrl+F5**.

## Smoke (loja)

1. Cadastro ERP · botão **230** (Fiscal) gera código mesmo com Mongo off.
2. Reimprimir etiqueta **230… legado** · bip `2300000001488` → produto correto.
3. `/pdv/checkout/` · badge **v26.76** · buscar produto · Enter → modal **Valor em R$?** · `10` ou Enter vazio.
4. Bip balança `2001000004812` → **não** abre modal R$ (qty da etiqueta).

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-PDV-VALOR-RS-MODAL.md` (tag única do lote **v26.74**).
