# PREP — CHECKLIST 10/10k · alvo **v26.89**

**Status:** 🟢 **armado após gates** · Live **v26.88** @ `3d19e97b` · PREP `deploy/prep-checklist-1010k`.

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **PDV-BUSCA-EAN230** | legado **15/15** · bip **10/10** · PDV **10/10** · Django **43/43** | **NÃO** |

**O quê:** bip EAN-13 `230…` válido no **PDV** usa o mesmo critério da **Gestão** (cadastro raiz / overlay), não `index_codigos` Mongo stale; cache BCA não guarda bip 230; JSON da API recalcula `index_codigos` após overlay.

**Migração 230:** já executada na v26.88 (`Corrigidos: 6`). **Não** rodar de novo salvo orientação.

**Pós-deploy (loja):** Ctrl+F5 no PDV · testar bip `2300000001556` → **GM0024-P**.

| Campo | Valor |
| ----- | ----- |
| **Tip PREP** | `deploy/prep-checklist-1010k` |
| **Rollback tag** | `rollback/pre-checklist-1010k-v26.88` → `3d19e97b` |
| **Backup branch** | `producao-backup-pre-v2689-checklist-1010k` |
| **Cutover** | `scripts/cutover_loja_checklist_1010k.sh` |

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1010k.sh --exec
```

## Reverter

```bash
git push origin rollback/pre-checklist-1010k-v26.88:producao
```
