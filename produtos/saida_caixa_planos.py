# Planos da tela "Saída / Retirada" do caixa.
# Lista viva = PlanoContaAgro com exibir_pdv=True (Configuração → Planos de contas).
# Depósito + IDs especiais (vale / salário / outros) ficam estáveis para a lógica do backend.

from __future__ import annotations

from typing import Any

PLANO_DEPOSITO_ID = "deposito"
PLANO_OUTROS_ID = "outros"
PLANO_ADIANT_VALE_ID = "adiant_vale"
PLANO_SALARIO_FOLHA_ID = "salario_folha"

# Nomes oficiais → id estável (lógica RH / motivo obrigatório em Outros).
_SPECIAL_ID_BY_NOME: dict[str, str] = {
    "Adiantamento de Salário (Vale)": PLANO_ADIANT_VALE_ID,
    "Salários": PLANO_SALARIO_FOLHA_ID,
    "Outros (verificar)": PLANO_OUTROS_ID,
}

# Rótulos do select (operador) quando diferem do nome oficial.
_LABEL_BY_NOME: dict[str, str] = {
    "Salários": "Salários (pagamento folha)",
    "Outros (verificar)": "Outros",
    "Combustível Demais Carros": "Combustível demais carros",
    "Compra Mercadoria SN": "Compra mercadoria SN",
    "Material de Limpeza e Conservação": "Material de limpeza e conservação",
    "Matérias de Escritório": "Materiais de escritório",
    "Matérias de Informática": "Materiais de informática",
}

# Fallback estático se o banco ainda não tiver planos / migrate pendente.
_FALLBACK_STATIC: list[dict[str, Any]] = [
    {
        "id": PLANO_DEPOSITO_ID,
        "label": "Depósito (caixa → banco)",
        "plano": "",
        "somente_caixa": True,
    },
    {
        "id": PLANO_ADIANT_VALE_ID,
        "label": "Adiantamento de Salário (Vale)",
        "plano": "Adiantamento de Salário (Vale)",
    },
    {
        "id": PLANO_SALARIO_FOLHA_ID,
        "label": "Salários (pagamento folha)",
        "plano": "Salários",
    },
    {"id": "alimentacao", "label": "Alimentação", "plano": "Alimentação"},
    {
        "id": "brindes",
        "label": "Brindes e ações festivas",
        "plano": "Brindes e ações festivas",
    },
    {"id": "comb_strada", "label": "Combustível Strada", "plano": "Combustível Strada"},
    {
        "id": "comb_demais",
        "label": "Combustível demais carros",
        "plano": "Combustível Demais Carros",
    },
    {
        "id": "compra_sn",
        "label": "Compra mercadoria SN",
        "plano": "Compra Mercadoria SN",
    },
    {"id": "embalagens", "label": "Embalagens", "plano": "Embalagens"},
    {
        "id": "limpeza",
        "label": "Material de limpeza e conservação",
        "plano": "Material de Limpeza e Conservação",
    },
    {
        "id": "escritorio",
        "label": "Materiais de escritório",
        "plano": "Matérias de Escritório",
    },
    {
        "id": "informatica",
        "label": "Materiais de informática",
        "plano": "Matérias de Informática",
    },
    {
        "id": "ret_geraldinho",
        "label": "Retiradas Geraldinho",
        "plano": "Retiradas Geraldinho",
    },
    {"id": "ret_geraldo", "label": "Retiradas Geraldo", "plano": "Retiradas Geraldo"},
    {
        "id": PLANO_OUTROS_ID,
        "label": "Outros",
        "plano": "Outros (verificar)",
        "outros": True,
    },
]

# Compat: código antigo importa SAIDA_CAIXA_PLANOS como lista fixa.
# Preferir listar_planos_saida_caixa() nas views.
SAIDA_CAIXA_PLANOS = _FALLBACK_STATIC


def _entry_deposito() -> dict[str, Any]:
    return {
        "id": PLANO_DEPOSITO_ID,
        "label": "Depósito (caixa → banco)",
        "plano": "",
        "somente_caixa": True,
    }


def listar_planos_saida_caixa() -> list[dict[str, Any]]:
    """Lista do select PDV: Depósito + planos oficiais com «Mostrar no PDV»."""
    try:
        from produtos.models import PlanoContaAgro
        from produtos.planos_conta_util import id_publico_plano

        qs = (
            PlanoContaAgro.objects.filter(ativo=True, exibir_pdv=True)
            .order_by("nome")
            .only("pk", "nome")
        )
        rows = list(qs)
    except Exception:
        return list(_FALLBACK_STATIC)

    if not rows:
        return list(_FALLBACK_STATIC)

    out: list[dict[str, Any]] = [_entry_deposito()]
    for p in rows:
        nome = (p.nome or "").strip()
        if not nome:
            continue
        sid = _SPECIAL_ID_BY_NOME.get(nome) or id_publico_plano(p.pk)
        entry: dict[str, Any] = {
            "id": sid,
            "label": _LABEL_BY_NOME.get(nome, nome),
            "plano": nome,
            "pk": p.pk,
        }
        if sid == PLANO_OUTROS_ID:
            entry["outros"] = True
        out.append(entry)
    return out
