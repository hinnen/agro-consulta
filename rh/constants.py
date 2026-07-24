"""Constantes do RH — evitar import circular com produtos em modelos/migrations."""

# Deve coincidir com produtos.saida_caixa_planos (id adiant_vale / salario_folha)
ADIANTAMENTO_PLANO_ID = "adiant_vale"
SALARIO_PLANO_ID = "salario_folha"

# Texto exato do plano na saída de caixa / Mongo (DtoLancamento.PlanoDeConta)
PLANO_ADIANTAMENTO_CANONICO = "2.1.1.1.1 — Adiantamento de Salário ( Vale )"
PLANO_SALARIO_CANONICO = "Salários"

REF_TIPO_MONGO_DTO_LANCAMENTO = "MONGO_DtoLancamento"

# Baixa parcial sobre o título único de salário (sem novo DtoLancamento de vale)
REF_TIPO_RH_SALARIO_PARCIAL = "RH_SALARIO_PARCIAL"

# Pagamento de salário (não vale) — CP ou caixa
REF_TIPO_PG_BAIXA_CP = "PG_BAIXA_CP"
REF_TIPO_RH_PAGAMENTO_SALARIO_PARCIAL = "RH_PAGAMENTO_SALARIO_PARCIAL"
