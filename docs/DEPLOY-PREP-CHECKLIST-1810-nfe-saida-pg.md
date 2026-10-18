# PREP — **NFE-SAIDA-55-PG** · **18/10** · **v27.26.3**

**Migrate:** **SIM** — `produtos.0146_nfe_saida_rascunho_agro`.

**Base loja:** v27.26 @ `f82f5be1` (tela NF-e já live; rascunhos falhavam sem Mongo).

**Prova:** `verify_nfe_saida_path.py` → **41/41** · `tests_nfe_saida_util` **6/6**.

**Smoke loja:** `/nfe/saida/` · **Nova (F4)** · lista sem «serviço legado indisponível» · `?venda=<pk>`.

**Revert:** `producao` → `f82f5be1` (v27.26) — tabela PG nova pode ficar órfã (sem uso).

**Nota:** emissão SEFAZ produção (`tp_amb=1`) fase 2 SOAP inalterada.
