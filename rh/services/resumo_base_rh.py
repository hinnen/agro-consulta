"""
Resumo administrativo para saneamento da base (RH + ClienteAgro).
"""

from __future__ import annotations

from dataclasses import dataclass

from django.db.models import Count, Q

from base.models import Empresa

from produtos.models import ClienteAgro

from rh.models import Funcionario


@dataclass
class ResumoAdministrativoRh:
    total_perfis: int
    perfis_ativos: int
    perfis_com_cliente_externo_id: int
    perfis_cliente_origem_rh_legacy: int
    total_clienteagro: int
    clienteagro_sem_externo_id: int
    clienteagro_origem_rh_legacy: int
    por_empresa: list[dict]


def montar_resumo_administrativo_rh() -> ResumoAdministrativoRh:
    total_perfis = Funcionario.objects.count()
    perfis_ativos = Funcionario.objects.filter(ativo=True).count()
    perfis_com_cliente_externo_id = Funcionario.objects.filter(
        cliente_agro__externo_id__gt="",
    ).count()
    perfis_cliente_origem_rh_legacy = Funcionario.objects.filter(
        cliente_agro__origem_import="rh_legacy",
    ).count()

    total_ca = ClienteAgro.objects.count()
    ca_sem_ext = ClienteAgro.objects.filter(Q(externo_id="") | Q(externo_id__isnull=True)).count()
    ca_rh_legacy = ClienteAgro.objects.filter(origem_import="rh_legacy").count()

    por_empresa = list(
        Empresa.objects.filter(ativo=True)
        .annotate(
            n_perfis=Count("funcionarios", distinct=True),
            n_perfis_ativos=Count("funcionarios", filter=Q(funcionarios__ativo=True), distinct=True),
        )
        .values("id", "nome_fantasia", "n_perfis", "n_perfis_ativos")
        .order_by("nome_fantasia")
    )

    return ResumoAdministrativoRh(
        total_perfis=total_perfis,
        perfis_ativos=perfis_ativos,
        perfis_com_cliente_externo_id=perfis_com_cliente_externo_id,
        perfis_cliente_origem_rh_legacy=perfis_cliente_origem_rh_legacy,
        total_clienteagro=total_ca,
        clienteagro_sem_externo_id=ca_sem_ext,
        clienteagro_origem_rh_legacy=ca_rh_legacy,
        por_empresa=por_empresa,
    )
