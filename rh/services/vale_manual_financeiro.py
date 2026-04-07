"""
Vale na ficha RH: baixa parcial no título único de salário (Mongo), com formas/bancos como na saída de caixa.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from django.contrib.auth.models import AnonymousUser

from produtos.models import OpcaoBaixaFinanceiroExtra
from produtos.mongo_financeiro_util import listar_formas_e_bancos_distintos
from produtos.views import _mesclar_opcoes_baixa_com_extras, obter_conexao_mongo

from rh.models import Funcionario


def montar_choices_formas_bancos(user, *, modo: str = "erp") -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """
    Retorna listas de (value, label) com value no formato id|||nome (igual à tela de saída de caixa).
    """
    modo = (modo or "erp").strip().lower()
    if modo not in ("erp", "historico"):
        modo = "erp"
    _, db = obter_conexao_mongo()
    if db is None:
        return [], []
    formas, bancos = listar_formas_e_bancos_distintos(db, modo=modo)
    if user is not None and not isinstance(user, AnonymousUser):
        extras_q = OpcaoBaixaFinanceiroExtra.objects.filter(usuario=user)
        formas, _ = _mesclar_opcoes_baixa_com_extras(
            formas, extras_q.filter(tipo=OpcaoBaixaFinanceiroExtra.Tipo.FORMA)
        )
        bancos, _ = _mesclar_opcoes_baixa_com_extras(
            bancos, extras_q.filter(tipo=OpcaoBaixaFinanceiroExtra.Tipo.BANCO)
        )
    formas.sort(key=lambda x: (x.get("nome") or "").lower())
    bancos.sort(key=lambda x: (x.get("nome") or "").lower())

    def linha(item: dict[str, Any]) -> tuple[str, str]:
        i = str(item.get("id") or "").strip()
        n = str(item.get("nome") or "").strip().replace("|", "/")
        suf = " · lista pessoal" if item.get("origem") == "manual" else ""
        return (f"{i}|||{n}", f"{n}{suf}")

    return [linha(x) for x in formas], [linha(x) for x in bancos]


def _split_id_nome(val: str) -> tuple[str | None, str]:
    s = (val or "").strip()
    if not s:
        return None, ""
    idx = s.find("|||")
    if idx >= 0:
        left = s[:idx].strip() or None
        right = s[idx + 3 :].strip()
        return left, right
    return None, s


def executar_vale_com_lancamento_mongo(
    *,
    funcionario: Funcionario,
    usuario,
    data,
    valor: Decimal,
    observacao: str,
    forma_value: str,
    banco_value: str,
) -> dict[str, Any]:
    """
    Aplica o vale como baixa parcial no título único de salário (Mongo) da competência.
    Exige título gerado no fechamento RH — não cria mais despesa separada de adiantamento.
    """
    from rh.models import ValeFuncionario
    from rh.services.salario_financeiro_mongo import registrar_vale_como_baixa_parcial_salario

    return registrar_vale_como_baixa_parcial_salario(
        funcionario=funcionario,
        usuario=usuario,
        data=data,
        valor=valor,
        observacao=observacao or "",
        forma_value=forma_value,
        banco_value=banco_value,
        tipo_origem=ValeFuncionario.TipoOrigem.MANUAL,
    )
