"""Lista produtos Postgres «fantasma» (import Mongo sem cadastro completo)."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.catalogo_nome_util import auditar_fantasmas_catalogo


class Command(BaseCommand):
    help = (
        "Audita Produto com Id Mongo 24 hex e nome vazio/—/ObjectId. "
        "Use após import Mongo→PG ou quando suspeitar de cadastro quebrado."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--ativos",
            action="store_true",
            help="Somente produtos ativos (cadastro_inativo=False, ativo=True).",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            help="Saída JSON (uma linha por item).",
        )

    def handle(self, *args, **options):
        import json

        rows = auditar_fantasmas_catalogo(ativos_apenas=bool(options.get("ativos")))
        n = len(rows)
        if options.get("json"):
            for row in rows:
                self.stdout.write(json.dumps(row, ensure_ascii=False))
            self.stdout.write(json.dumps({"total": n}, ensure_ascii=False))
            return

        if n == 0:
            self.stdout.write(self.style.SUCCESS("Nenhum fantasma encontrado."))
            return

        self.stdout.write(self.style.WARNING(f"Fantasmas encontrados: {n}"))
        for r in rows:
            self.stdout.write(
                "{id} | pg={nome_pg!r} gm_pg={gm!r} | R$ {pv:.2f} | → {nome!r} {gm2!r} {marca!r}".format(
                    id=r["produto_externo_id"],
                    nome_pg=r["nome_pg"] or "—",
                    gm=r["codigo_nfe_pg"] or "—",
                    pv=r["preco_venda"],
                    nome=r["nome_resolvido"] or "—",
                    gm2=r["codigo_nfe_resolvido"] or "—",
                    marca=r["marca_resolvida"] or "—",
                )
            )
        self.stdout.write(
            self.style.NOTICE(
                "Corrigir: python manage.py corrigir_produto_nome_objectid_pg --dry-run"
            )
        )
