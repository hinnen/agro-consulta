# PREP — CHECKLIST 10/10f · alvo **v26.84**

**Status:** 🟡 **armado** — aguarda cutover (Live hoje **v26.83** @ `0c39a6e0`).

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CB-230-LEGADO-LOTE** | legado **11/11** · bip **10/10** · gerador **26/26** · Django **26/26** | **NÃO** |

**O quê:** corrige cadastro 230 legado (DV errado) → EAN que a etiqueta bipa; **varredura em massa** + normalização ao salvar na gestão. **Não** muda regra GM4045 (1479 ≠ 1471 na busca).

**Pós-deploy (obrigatório, uma vez no servidor):**

```bash
python manage.py migrar_cb_loja_legado --dry-run
python manage.py migrar_cb_loja_legado
```

Só mexer manualmente nos GM que aparecerem como **COLISÃO** no log.

| Campo | Valor |
| ----- | ----- |
| **Tip PREP** | `deploy/prep-checklist-1010f` |
| **Base Live** | **v26.83** @ `0c39a6e0` |
| **Rollback tag** | `rollback/pre-checklist-1010f-v26.83` → `0c39a6e0` |
| **Backup branch** | `producao-backup-pre-v2684-checklist-1010f` |
| **Cutover** | `scripts/cutover_loja_checklist_1010f.sh` |

## Dry-run

```bash
./scripts/cutover_loja_checklist_1010f.sh
```

## Na senha

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1010f.sh --exec
```

## Smoke

1. Ctrl+F5 · badge **v26.84**.
2. Rodar `migrar_cb_loja_legado` (dry-run → apply).
3. Bip GM0024-P (ou outro corrigido) → produto certo.

## Voltar

`docs/ROLLBACK-CHECKLIST-1010f.md`
