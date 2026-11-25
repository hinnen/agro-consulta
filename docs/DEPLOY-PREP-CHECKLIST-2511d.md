# Corte 25/11d — gestão busca código com 4 dígitos

**Enviado e no ar** `21fd9bb3` (**v27.26.102**). O painel construiu sozinho. Volta para a **v27.26.100** @ `fe7ff673`.

| # | Pacote | O que muda | Tabela nova |
| - | ------ | ---------- | ----------- |
| 1 | **BUSCA-CADASTRO** | Na gestão de produtos, código com 4 dígitos ou mais passa a buscar. Antes a tela esperava 8 e nem consultava. O arquivo da tela muda para o navegador pegar o novo | Não |

PDV e entrada de nota já acham. Não muda preço, venda, caixa nem estoque. Não é merge do `teste`.

**Depois do build:** Ctrl+F5. Badge **v27.26.102**.

**Volta:** `git push origin fe7ff673:producao`
