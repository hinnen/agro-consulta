# Corte 25/11c — busca pelo código do fornecedor

**Ainda não enviado.** Loja no Git: **v27.26.99** @ `3143bbb8`.

| # | Pacote | O que muda | Tabela nova |
| - | ------ | ---------- | ----------- |
| 1 | **BUSCA-COD-FORN** | A busca do cadastro e da nota acha o produto pelo código do fornecedor, o da nota e o digitado. Frase com espaço não entra nessa busca | Não |

Não muda preço, venda, caixa nem estoque. Não é merge do `teste`.

**Na senha, com as vendas pausadas:** `./scripts/cutover_loja_checklist_2511c.sh --exec`

**Depois do build:** Ctrl+F5. Badge **v27.26.100**.

**Volta:** `git push origin 3143bbb8:producao`
