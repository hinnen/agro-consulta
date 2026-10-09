# PREP deploy — PDV-BALANCA-QTY-VALOR · alvo **v26.74**

**Status:** 🟢 **pronto para envio à produção** · tip armado · cutover mínimo  
**Não sobe sozinho.** Só com frase explícita + senha `99738595` na mesma mensagem.

## CHECKLIST ÚNICO (o que sobe)

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **PDV-BALANCA-QTY-VALOR** (wizard `/pdv/checkout/`) | **63/63** · agro **56/56** · etq **41/41** · unit **11/11** · PIN 9973 · qty 0,512 | **NÃO** |

Nada mais no checklist está «pronto para envio» — lotes anteriores já são Live.

**O quê (tela da loja = wizard):**
1. **Etiqueta balança** — bip EAN → preço unitário + qty = total÷unitário.
2. **Sem balança** — `produto R$10` / `$10` / `=10` / `10$` → qty automática.

**Tip PREP:** `deploy/prep-pdv-balanca-agro-pg` (ancestral = Live **v26.71** @ `2eab76c1`)  
**Delta:** 7 commits · só `views.py` + `pdv_wizard.js` (+ legado `consulta_produtos.js`) + VERSION/docs/provas  
**Pausa loja:** ~1–2 min (Render build; **sem** migrate novo)

## Já pré-armado (não gasta tempo na senha)

| Item | Valor |
| ---- | ----- |
| Tip PREP = tip feature | `53638a5d` |
| Rollback tag | `rollback/pre-pdv-balanca-qty-valor-v26.71` → `2eab76c1` |
| Backup branch | `producao-backup-pre-v2674-qty-valor` |
| Doc rollback | `docs/ROLLBACK-PDV-BALANCA-QTY-VALOR.md` |
| Script | `scripts/cutover_loja_qty_valor.sh` (dry-run sem senha) |

## Na senha (caminho mais curto)

```bash
# Opção A — um comando (recomendado)
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_qty_valor.sh --exec

# Opção B — manual
git fetch origin
git checkout producao
git reset --hard origin/deploy/prep-pdv-balanca-agro-pg
git push origin producao
```

Acompanhar Render até Live → **Ctrl+F5** no PDV · badge **v26.74**.

Smoke: `/pdv/checkout/` · bip `2001000004812` → qty≈0,512 · `produto R$10` Enter.

## Voltar (só frase + senha)

Ver `docs/ROLLBACK-PDV-BALANCA-QTY-VALOR.md`.
