# Rollback — BI-META-C-VILA (Meta C Vila / soma Centro+Vila)

**Migrate:** NÃO  
**Env nova obrigatória:** NÃO (`AGRO_VILA_ABERTURA` opcional; default `2026-07-20`)

## O que reverter (só estes)

- `produtos/views.py` (bloco Meta C + `serie_compare` com `deposito_filtro`)
- `produtos/mongo_vendas_util.py`
- `produtos/mongo_financeiro_util.py`
- `produtos/lancamentos_financeiro_pg_analytics_util.py`
- `produtos/templates/produtos/partials/dashboard_gerencial_body.html` (texto «?»)
- `scripts/verify_meta_c_vila_abertura.py` / `scripts/verify_vendas_lojas_resumo_path.py` (prova)

## Como (loja)

1. **Antes do envio:** tag `rollback/pre-bi-meta-c-vila` no tip `producao` atual.
2. Se der problema: voltar esses arquivos ao estado da tag (cherry-pick reverso / checkout da tag nos paths) + push `producao` **só** com frase+senha.
3. Ctrl+F5 · BI filtro Centro deve voltar ao comportamento anterior (meta misturada em C+V).

## Por que é seguro isolar

Pacote **só Meta C / previsão**. Não mexe em venda, caixa, estoque, NFC-e nem migrate. Pode subir **sozinho**, fora do checklist grande.
