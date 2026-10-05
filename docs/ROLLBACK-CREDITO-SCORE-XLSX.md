# Rollback — CREDITO-SCORE-XLSX

Revert só o Excel ↓ do laboratório de crédito (`/fiado/analise-credito/export-xlsx/`).

**Não** rodar sem frase explícita + senha de produção do Renan.

## O que reverter

- Rota `fiado/analise-credito/export-xlsx/`
- View `credito_score_laboratorio_export_xlsx` + helpers de montagem xlsx
- Botão **Excel ↓** no template do laboratório

## O que NÃO mexer

- Motor shadow / tabela `ClienteAnaliseCreditoAgro`
- Limite, fiado, PDV, vendas
