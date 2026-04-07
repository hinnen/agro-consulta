"""Constantes do RH — evitar import circular com produtos em modelos/migrations."""

# Deve coincidir com produtos.saida_caixa_planos (id adiant_vale)
ADIANTAMENTO_PLANO_ID = "adiant_vale"

# Texto exato do plano na saída de caixa / Mongo (DtoLancamento.PlanoDeConta)
PLANO_ADIANTAMENTO_CANONICO = "2.1.1.1.1 — Adiantamento de Salário ( Vale )"

REF_TIPO_MONGO_DTO_LANCAMENTO = "MONGO_DtoLancamento"

# Baixa parcial sobre o título único de salário (sem novo DtoLancamento de vale)
REF_TIPO_RH_SALARIO_PARCIAL = "RH_SALARIO_PARCIAL"
