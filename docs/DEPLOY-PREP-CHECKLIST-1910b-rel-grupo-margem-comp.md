# CHECKLIST 19/10b — REL-GRUPO-MARGEM-COMP · v27.26.13

**Loja hoje:** v27.26.12 @ `f33ee5cd` (produção). **Sobe:** só relatório `/relatorios/vendas-grupo/` (+ hub/ajuda). **Sem migrate.** **Não** merge `teste`.

| Item | Valor |
| ---- | ----- |
| Pacote | **REL-GRUPO-MARGEM-COMP** |
| Commit teste | `dfabcf9f` |
| PREP branch | `deploy/prep-checklist-1910b` |
| Revert | `git push origin rollback/pre-v272613-1910b-rel-grupo:producao` → **v27.26.12** @ **`f33ee5cd`** |

## Antes (assist.) — já feito no prep

1. `git fetch origin producao deploy/prep-checklist-1910b`
2. `./scripts/prep_checkpoint_checklist_1910b_rel_grupo_margem_comp.sh`

## No cutover (Renan: pausar vendas + senha na mesma mensagem)

```bash
export AGRO_LOJA_FRASE='pode subir para produção o pacote 19/10b'
export AGRO_LOJA_SENHA='99738595'
./scripts/cutover_loja_checklist_1910b_rel_grupo_margem_comp.sh --exec
```

## Pós-deploy (Render Shell)

```bash
./scripts/post_deploy_checklist_1910b_rel_grupo_margem_comp.sh
```

## Prova local (PIN teste 9973)

- `python scripts/verify_rel_grupo_margem_path.py` — 14 OK
- `python scripts/verify_rel_cat_sub_excel_path.py` — 30 OK (regressão cat/sub)

## Risco loja aberta

Somente GET em Central de Relatórios; não altera PDV, caixa nem gravação de vendas.
