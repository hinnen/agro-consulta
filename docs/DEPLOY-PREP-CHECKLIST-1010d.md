# PREP — CHECKLIST 10/10d · alvo **v26.82**

**Status:** ✅ **enviado / Live v26.82** — `producao` @ `b7bdde7b` · 10/10/2026.

| # | Pacote | Prova | Migrate |
| - | ------ | ----- | ------- |
| 1 | **CB-230-EXCLUSIVO** | gerador **24/24** · bip **10/10** · etq **74/74** · Django **27/27** | **NÃO** |
| 2 | **CADASTRO-BUSCA-MED** | path **37/37** · taxonomia unit **4/4** · API **200** | **NÃO** |

**O quê (resumo):**

- **CB-230:** gerador só EAN-13 válido · busca literal (sem equivalência legado↔canônico) · lock no save · cmd `reatribuir_cb_loja_exclusivo` (GM4045 opcional no deploy).
- **CADASTRO-BUSCA-MED:** palavras-chave + classes vet na aba **2. Busca** · sync Mongo `AgroBuscaTextoExtra` · PDV/catálogo indexam texto extra.

| Campo | Valor |
| ----- | ----- |
| **Tip PREP** | `deploy/prep-checklist-1010d` (ver `git log -1`) |
| **Base Live** | **v26.80** @ `2ac4789c` |
| **Rollback tag** | `rollback/pre-checklist-1010d-v26.80` → `2ac4789c` |
| **Backup branch** | `producao-backup-pre-v2682-checklist-1010d` |
| **Cutover** | `scripts/cutover_loja_checklist_1010d.sh` |

## Dry-run (agora)

```bash
./scripts/cutover_loja_checklist_1010d.sh
```

## Na senha (rápido)

```bash
AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
  ./scripts/cutover_loja_checklist_1010d.sh --exec
```

Equivale a: `git push origin origin/deploy/prep-checklist-1010d:producao` após gates.

## Smoke (lojas)

1. Ctrl+F5 · badge **v26.82**.
2. Cadastro → botão **230** → salvar → etiqueta → bip no PDV acha o produto certo (GM4045: `1479` ≠ `1471`).
3. Cadastro medicamento → aba **Busca** → palavra-chave → PDV busca sinônimo.
4. (Opcional deploy) `python manage.py reatribuir_cb_loja_exclusivo --codigo-gm GM4045 --esperado-atual 2300000001479` dry-run; `--aplicar --confirmar GM4045` só se Renan pedir.

## Voltar (só frase + senha)

`docs/ROLLBACK-CHECKLIST-1010d.md`
