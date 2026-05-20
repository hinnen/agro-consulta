"""Auditoria em lote: notas Entrada NF-e vs. títulos em DtoLancamento (contas a pagar)."""

from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.nfe_entrada_util import auditar_entrada_nfe_financeiro_lote
from produtos.views import obter_conexao_mongo


class Command(BaseCommand):
    help = (
        "Lista notas Entrada NF-e sem título no financeiro ou com fornecedor divergente do Cliente do título. "
        "Concluída na tela = PIN etapa 6; não substitui esta auditoria."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--filtro",
            default="concluida",
            help="Mesmo filtro da lista (concluida, todas, financeiro, …). Padrão: concluida.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=300,
            help="Máximo de alertas retornados (padrão 300).",
        )

    def handle(self, *args, **options):
        client, db = obter_conexao_mongo()
        if db is None:
            self.stderr.write(self.style.ERROR("Mongo indisponível"))
            return
        col_pessoa = getattr(client, "col_c", None) or "DtoPessoa"
        filtro = str(options.get("filtro") or "concluida")
        lim = int(options.get("limit") or 300)
        out = auditar_entrada_nfe_financeiro_lote(
            db,
            col_pessoa=col_pessoa,
            filtro_lista=filtro,
            limit=lim,
        )
        if not out.get("ok"):
            self.stderr.write(self.style.ERROR(out.get("erro") or "Falha"))
            return
        self.stdout.write(out.get("nota") or "")
        self.stdout.write(
            f"Filtro={filtro} · OK/bonificação={out.get('total_ok_ou_bonificacao')} · "
            f"alertas={out.get('total_alertas')} · resumo={out.get('resumo')}"
        )
        for a in out.get("alertas") or []:
            self.stdout.write(
                f"  · {a.get('fornecedor')} NF {a.get('nf')} [{a.get('situacao')}] — {a.get('detalhe')}"
            )
