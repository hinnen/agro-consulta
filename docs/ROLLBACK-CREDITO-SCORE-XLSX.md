# Rollback — CREDITO-SCORE-XLSX (+ COLS)

Revert Excel ↓ do laboratório e/ou colunas extras + rótulo «Revisar dados».

**Não** rodar sem frase explícita + senha de produção do Renan.

## O que reverter

- Rota `fiado/analise-credito/export-xlsx/` e view de montagem xlsx
- Botão **Excel ↓** / colunas de indicadores / regra provisória `rotulo_candidato_revisao`
- Badges «Revisar dados» no lab e detalhe

## O que NÃO mexer

- Fórmula do score (`analisar_cliente` pontos)
- Limite, fiado, PDV, vendas
- Tabela shadow (só leitura financeira)
