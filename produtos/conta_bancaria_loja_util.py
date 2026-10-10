"""Cadastro de contas/bancos da loja (Postgres) — lista da baixa CP."""
from __future__ import annotations

import logging
from typing import Any

from django.db import IntegrityError, transaction
from django.db.models import Q

from produtos.models import ContaBancariaLojaAgro, OpcaoBaixaFinanceiroExtra, TituloFinanceiroAgro

logger = logging.getLogger(__name__)

_PLACEHOLDER_NOMES = frozenset(
    {
        "ADICIONAR CONTA",
        "ADICIONAR BANCO",
    }
)


def _nome_limpo(nome: str) -> str:
    return " ".join(str(nome or "").strip().split())[:300]


def _codigo_limpo(codigo: str) -> str:
    return str(codigo or "").strip()[:80]


def _nome_chave(nome: str) -> str:
    return _nome_limpo(nome).casefold()


def serializar_conta(obj: ContaBancariaLojaAgro) -> dict[str, Any]:
    return {
        "pk": obj.pk,
        "id": obj.codigo,
        "codigo": obj.codigo,
        "nome": obj.nome,
        "ativo": bool(obj.ativo),
        "ordem": int(obj.ordem or 0),
    }


def ensure_seed_contas_loja() -> dict[str, int]:
    """Se a tabela estiver vazia, copia contas já usadas nos títulos + extras pessoais."""
    if ContaBancariaLojaAgro.objects.exists():
        return {"criados": 0, "ja_havia": ContaBancariaLojaAgro.objects.count()}

    criados = 0
    seen_cod: set[str] = set()
    seen_nome: set[str] = set()

    def _try_add(nome: str, codigo: str) -> None:
        nonlocal criados
        n = _nome_limpo(nome)
        c = _codigo_limpo(codigo)
        if not n or n.upper() in _PLACEHOLDER_NOMES:
            return
        if not c:
            return
        nk = _nome_chave(n)
        if c in seen_cod or nk in seen_nome:
            return
        seen_cod.add(c)
        seen_nome.add(nk)
        try:
            ContaBancariaLojaAgro.objects.create(nome=n, codigo=c, ativo=True, ordem=criados)
            criados += 1
        except IntegrityError:
            pass

    for bn, bid in (
        TituloFinanceiroAgro.objects.exclude(banco="")
        .exclude(banco_id="")
        .values_list("banco", "banco_id")
        .distinct()
        .iterator(chunk_size=2000)
    ):
        _try_add(str(bn or ""), str(bid or ""))

    for nome, id_erp, pk in OpcaoBaixaFinanceiroExtra.objects.filter(
        tipo=OpcaoBaixaFinanceiroExtra.Tipo.BANCO
    ).values_list("nome", "id_erp", "pk"):
        cod = _codigo_limpo(id_erp) or f"extra-{pk}"
        _try_add(str(nome or ""), cod)

    return {"criados": criados, "ja_havia": 0}


def listar_contas_loja(*, incluir_inativos: bool = False, q: str = "") -> list[dict[str, Any]]:
    ensure_seed_contas_loja()
    qs = ContaBancariaLojaAgro.objects.all()
    if not incluir_inativos:
        qs = qs.filter(ativo=True)
    qq = (q or "").strip()
    if qq:
        qs = qs.filter(Q(nome__icontains=qq) | Q(codigo__icontains=qq))
    return [serializar_conta(o) for o in qs.order_by("ordem", "nome")]


def listar_bancos_para_baixa() -> list[dict[str, str]]:
    """Lista ativa + placeholder «ADICIONAR CONTA» no início (formato da API de baixa)."""
    from produtos.mongo_financeiro_util import _bancos_lista_com_placeholder_inicio

    ensure_seed_contas_loja()
    bancos = [
        {"id": o.codigo, "nome": o.nome}
        for o in ContaBancariaLojaAgro.objects.filter(ativo=True).order_by("ordem", "nome")
    ]
    return _bancos_lista_com_placeholder_inicio(bancos)


@transaction.atomic
def criar_conta_loja(nome: str) -> ContaBancariaLojaAgro:
    n = _nome_limpo(nome)
    if len(n) < 2:
        raise ValueError("Informe o nome da conta (mín. 2 letras).")
    if n.upper() in _PLACEHOLDER_NOMES:
        raise ValueError("Esse nome é reservado. Escolha outro.")
    if ContaBancariaLojaAgro.objects.filter(nome__iexact=n).exists():
        raise ValueError("Já existe uma conta com esse nome.")
    obj = ContaBancariaLojaAgro.objects.create(nome=n, ativo=True)
    return obj


@transaction.atomic
def renomear_conta_loja(pk: int, nome: str) -> ContaBancariaLojaAgro:
    n = _nome_limpo(nome)
    if len(n) < 2:
        raise ValueError("Informe o nome da conta (mín. 2 letras).")
    if n.upper() in _PLACEHOLDER_NOMES:
        raise ValueError("Esse nome é reservado. Escolha outro.")
    obj = ContaBancariaLojaAgro.objects.filter(pk=pk).first()
    if not obj:
        raise LookupError("Conta não encontrada.")
    if ContaBancariaLojaAgro.objects.filter(nome__iexact=n).exclude(pk=pk).exists():
        raise ValueError("Já existe uma conta com esse nome.")
    antigo = obj.nome
    obj.nome = n
    obj.save(update_fields=["nome", "atualizado_em"])
    if antigo != n and obj.codigo:
        TituloFinanceiroAgro.objects.filter(banco_id=obj.codigo).update(banco=n)
    return obj


@transaction.atomic
def set_ativo_conta_loja(pk: int, ativo: bool) -> ContaBancariaLojaAgro:
    obj = ContaBancariaLojaAgro.objects.filter(pk=pk).first()
    if not obj:
        raise LookupError("Conta não encontrada.")
    obj.ativo = bool(ativo)
    obj.save(update_fields=["ativo", "atualizado_em"])
    return obj
