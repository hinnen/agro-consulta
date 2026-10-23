# PREP — CHECKLIST 23/10b · PDV-RAPIDOS Home · v27.26.23

- **Base loja:** `producao` @ `c8695c5d` (v27.26.22, Rápidos já com migrate 0150+0151)
- **Origem:** `cursor/pdv-rapidos-ac2d` @ `b83c9dde`
- **Migrate:** não (tabelas já existem)
- **Caixa / checkout:** não entram

## O quê

- Botão **Rápidos** na busca, antes de Rações, atalho **Home**
- **F5** volta a atualizar a página
- **Insert** fica livre

## Prova

```bash
.venv/bin/python scripts/verify_pdv_rapidos_path.py
```

## Revert

```bash
git push origin rollback/pre-v272623-2310b-pdv-rapidos-home:producao
```

Volta para **v27.26.22** @ `c8695c5d` (atalho antigo Alt+6 / F5 conforme o que já está live).
