# Rollback — REL-MV-LOJA (loja alvo **v25.22**)

Ponto **antes** = loja **Live v23.99** (`a43340a` / REPASSE-DIA-HERO).

| Item | Valor |
| ---- | ----- |
| **Commit loja hoje (antes do deploy)** | `a43340a` (Live v23.99) |
| **Tag** | `rollback/pre-rel-mv-loja-v23.99` |
| **Branch backup** | `producao-backup-pre-v2522-rel-mv-loja-20260914` |
| **O quê sobe** | `REL-MV-LOJA` (filtro Loja em Mais vendidos + Vendas por grupo) |
| **O quê NÃO sobe** | merge do `teste` · checklist 14/09 PREP (#18–#26 etc.) |
| **Migrate** | **NÃO** |

```bash
git fetch origin
git checkout producao
git reset --hard rollback/pre-rel-mv-loja-v23.99
git push origin producao --force-with-lease
```

**Só** com frase + senha do Renan. Volta para Live **v23.99**.

## Risco operacional (lojas abertas)

| Pacote | Afeta venda / caixa? | Nota |
| ------ | -------------------- | ---- |
| `REL-MV-LOJA` | **Não** | Só Central de Relatórios · PDV/caixa/NF intactos |

## Provas

| Pacote | Prova |
| ------ | ----- |
| `REL-MV-LOJA` | `verify_rel_mv_loja_path.py` **107/107** · unit **10/10** · PIN **9973** |

## Depois do deploy

1. **Ctrl+F5** · badge **v25.22**
2. Relatórios → Mais vendidos / Vendas por grupo → Loja Centro+Vila / Centro / Vila
