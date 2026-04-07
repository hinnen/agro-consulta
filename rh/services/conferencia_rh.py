"""
Leituras somente leitura para conferência de vínculos RH ↔ ClienteAgro e vales.
Usado pela tela de conferência técnica (gestão RH).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from django.db.models import Count, F, Q
from django.db.models.functions import Lower

from produtos.models import ClienteAgro

from rh.models import Funcionario, InconsistenciaIntegracaoRh, ValeFuncionario


@dataclass
class ConferenciaRhSnapshot:
    """Resumo numérico + listas limitadas para exibição."""

    n_funcionario_total: int
    n_funcionario_sem_cliente_agro: int
    n_duplicata_ativa_empresa_cliente: int
    n_inconsistencias_vale_sem_funcionario: int
    n_vales_empresa_divergente: int
    n_grupos_nome_cliente_duplicado: int
    n_grupos_cpf_cliente_duplicado: int
    duplicatas_ativas: list[dict[str, Any]]
    inconsistencias_vale: list[InconsistenciaIntegracaoRh]
    vales_empresa_divergente: list[ValeFuncionario]
    grupos_nome_duplicado: list[dict[str, Any]]
    grupos_cpf_duplicado: list[dict[str, Any]]


def _duplicatas_perfil_ativo_mesma_empresa() -> tuple[int, list[dict[str, Any]]]:
    """Não deveria ocorrer se a UniqueConstraint estiver ativa no banco."""
    qs = (
        Funcionario.objects.filter(ativo=True)
        .values("empresa_id", "cliente_agro_id")
        .annotate(c=Count("id"))
        .filter(c__gt=1)
        .order_by("-c")[:50]
    )
    rows = list(qs)
    return len(rows), [
        {
            "empresa_id": r["empresa_id"],
            "cliente_agro_id": r["cliente_agro_id"],
            "quantidade": r["c"],
        }
        for r in rows
    ]


def _vales_empresa_divergente_limite(n: int = 200) -> tuple[int, list[ValeFuncionario]]:
    base = ValeFuncionario.objects.filter(cancelado=False).select_related(
        "funcionario", "funcionario__empresa", "empresa"
    )
    divergentes = list(base.exclude(empresa_id=F("funcionario__empresa_id")).order_by("-data", "-id")[:n])
    total = base.exclude(empresa_id=F("funcionario__empresa_id")).count()
    return total, divergentes


def _clienteagro_nomes_duplicados(limite_grupos: int = 30, limite_por_grupo: int = 8) -> tuple[int, list[dict[str, Any]]]:
    grupos = (
        ClienteAgro.objects.annotate(nl=Lower("nome"))
        .values("nl")
        .annotate(c=Count("id"))
        .filter(c__gt=1)
        .order_by("-c")[:limite_grupos]
    )
    out: list[dict[str, Any]] = []
    for g in grupos:
        nl = g["nl"]
        pks = list(
            ClienteAgro.objects.annotate(nl=Lower("nome"))
            .filter(nl=nl)
            .order_by("pk")
            .values_list("pk", flat=True)[:limite_por_grupo]
        )
        exemplos = list(
            ClienteAgro.objects.filter(pk__in=pks).values("id", "nome", "externo_id", "cpf", "origem_import", "ativo")
        )
        out.append({"chave": nl, "total": g["c"], "exemplos": exemplos})
    total_grupos = (
        ClienteAgro.objects.annotate(nl=Lower("nome"))
        .values("nl")
        .annotate(c=Count("id"))
        .filter(c__gt=1)
        .count()
    )
    return total_grupos, out


def _clienteagro_cpf_duplicados(limite_grupos: int = 30, limite_por_grupo: int = 8) -> tuple[int, list[dict[str, Any]]]:
    base = ClienteAgro.objects.exclude(Q(cpf="") | Q(cpf__isnull=True))
    grupos = (
        base.values("cpf")
        .annotate(c=Count("id"))
        .filter(c__gt=1)
        .order_by("-c")[:limite_grupos]
    )
    out: list[dict[str, Any]] = []
    for g in grupos:
        cpf = g["cpf"]
        pks = list(base.filter(cpf=cpf).order_by("pk").values_list("pk", flat=True)[:limite_por_grupo])
        exemplos = list(
            ClienteAgro.objects.filter(pk__in=pks).values("id", "nome", "externo_id", "cpf", "origem_import", "ativo")
        )
        out.append({"cpf": cpf, "total": g["c"], "exemplos": exemplos})
    total_grupos = base.values("cpf").annotate(c=Count("id")).filter(c__gt=1).count()
    return total_grupos, out


def montar_snapshot_conferencia_rh(
    *,
    limite_inconsistencias: int = 200,
    limite_vales_div: int = 200,
) -> ConferenciaRhSnapshot:
    n_fun = Funcionario.objects.count()
    n_sem = Funcionario.objects.filter(cliente_agro__isnull=True).count()
    n_dup, dup_rows = _duplicatas_perfil_ativo_mesma_empresa()
    inc_base = InconsistenciaIntegracaoRh.objects.filter(
        resolvida=False,
        tipo=InconsistenciaIntegracaoRh.Tipo.VALE_SEM_FUNCIONARIO,
    )
    n_inc = inc_base.count()
    inc_list = list(
        inc_base.select_related("empresa").order_by("-criado_em")[:limite_inconsistencias]
    )
    n_vdiv, vales_div = _vales_empresa_divergente_limite(limite_vales_div)
    n_gnome, grupos_nome = _clienteagro_nomes_duplicados()
    n_gcpf, grupos_cpf = _clienteagro_cpf_duplicados()

    return ConferenciaRhSnapshot(
        n_funcionario_total=n_fun,
        n_funcionario_sem_cliente_agro=n_sem,
        n_duplicata_ativa_empresa_cliente=n_dup,
        n_inconsistencias_vale_sem_funcionario=n_inc,
        n_vales_empresa_divergente=n_vdiv,
        n_grupos_nome_cliente_duplicado=n_gnome,
        n_grupos_cpf_cliente_duplicado=n_gcpf,
        duplicatas_ativas=dup_rows,
        inconsistencias_vale=inc_list,
        vales_empresa_divergente=vales_div,
        grupos_nome_duplicado=grupos_nome,
        grupos_cpf_duplicado=grupos_cpf,
    )
