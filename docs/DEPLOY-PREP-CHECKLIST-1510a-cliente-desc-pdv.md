# PREP — **CLIENTE-LISTA-DESC-PDV** · **15/10a** · **v27.20**

**Migrate:** `0145_clienteagro_desconto_pdv_percentual` (obrigatório pós-deploy).

**Ordem recomendada:** cutover **15/10** v27.19 primeiro · depois este pacote (ou na mesma pausa: dois `--exec` + migrate).

**Rollback tag:** `rollback/pre-v2719-cliente-desc-pdv` (base v27.19 após primeiro cutover).
