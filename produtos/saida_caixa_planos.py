# Planos permitidos na tela "Saída no caixa" (/caixa/saida/) — texto do plano como no ERP/Mongo.
# id estável para lógica (ex.: outros); label é o que aparece no select.

SAIDA_CAIXA_PLANOS = [
    {"id": "adiant_vale", "label": "Adiantamento de Salário (Vale)", "plano": "2.1.1.1.1 — Adiantamento de Salário ( Vale )"},
    {"id": "alimentacao", "label": "Alimentação", "plano": "2.1.1.5 — Alimentação"},
    {"id": "brindes", "label": "Brindes e ações festivas", "plano": "2.3.4 — Brindes e ações festivas"},
    {"id": "comb_strada", "label": "Combustível Strada", "plano": "2.1.7.1 — Combustivel Strada"},
    {"id": "comb_demais", "label": "Combustível demais carros", "plano": "2.1.7.2 — Combustivel Demais Carros"},
    {"id": "compra_sn", "label": "Compra mercadoria SN", "plano": "2.2.1.2 — Compra Mercadoria SN"},
    {"id": "embalagens", "label": "Embalagens", "plano": "2.2.3 — Embalagens"},
    {"id": "limpeza", "label": "Material de limpeza e conservação", "plano": "2.1.13 — Material de Limpeza e Conservação"},
    {"id": "escritorio", "label": "Materiais de escritório", "plano": "2.2.5 — Materias de Escritório"},
    {"id": "informatica", "label": "Materiais de informática", "plano": "2.3.1 — Materias de informatica"},
    {"id": "ret_geraldinho", "label": "Retiradas Geraldinho", "plano": "2.4.2 — Retiradas Geraldinho"},
    {"id": "ret_geraldo", "label": "Retiradas Geraldo", "plano": "2.4.1 — Retiradas Geraldo"},
    {"id": "outros", "label": "Outros", "plano": "10 — Outros", "outros": True},
]

PLANO_OUTROS_ID = "outros"
