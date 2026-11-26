# Corte 26/11b — gaveta de Vendas

**Ainda não enviado.** Loja no ar: **v27.26.105** @ `c3526a16`.

| # | Pacote | O que muda | Tabela nova |
| - | ------ | ---------- | ----------- |
| 1 | **VENDAS-GAVETA-HORA** | Gaveta de Total, Centro e Vila mostra a diferença, Estipulado e Esperado. Total cabe no celular. Horário da última leitura boa; se falhar, fica o anterior. Atualizar renova os números sem apagar a tela | Não |

Não mexe em PDV, caixa nem venda. Não é merge do `teste`.

**Na senha, com as vendas pausadas:** `./scripts/cutover_loja_checklist_2611b.sh --exec`

**Logo depois:** se o painel não começar o build, disparar.

**Depois do build:** Ctrl+F5 no SistApp. Badge **v27.26.107**.

**Volta:** `git push origin c3526a16:producao`
