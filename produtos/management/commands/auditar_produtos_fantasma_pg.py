"""Lista produtos Postgres «fantasma» (import Mongo sem cadastro completo)."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.catalogo_nome_util import auditar_fantasmas_catalogo, produto_codigo_interno_oid


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

        parser.add_argument(
            "--higiene",
            action="store_true",
            help="Inclui itens só com codigo_interno=Id (ex. batom) — não quebram busca.",
        )

    def handle(self, *args, **options):
        import json

        rows = auditar_fantasmas_catalogo(ativos_apenas=bool(options.get("ativos")))
        n = len(rows)
        higiene = []
        if options.get("higiene"):
            from produtos.models import Produto

            qs = Produto.objects.all().order_by("nome", "pk")
            if options.get("ativos"):
                qs = qs.filter(cadastro_inativo=False, ativo=True)
            for p in qs.iterator(chunk_size=200):
                if produto_codigo_interno_oid(p):
                    higiene.append(
                        {
                            "produto_externo_id": p.produto_externo_id,
                            "nome": p.nome,
                            "codigo_nfe": p.codigo_nfe,
                            "tipo": "higiene_codigo_interno",
                        }
                    )
        if options.get("json"):
            for row in rows:
                self.stdout.write(json.dumps(row, ensure_ascii=False))
            for row in higiene:
                self.stdout.write(json.dumps(row, ensure_ascii=False))
            self.stdout.write(json.dumps({"total_grave": n, "total_higiene": len(higiene)}, ensure_ascii=False))
            return

        if n == 0:
            self.stdout.write(self.style.SUCCESS("Nenhum fantasma grave encontrado."))
        else:
            self.stdout.write(self.style.WARNING(f"Fantasmas graves: {n}"))
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
        if higiene:
            self.stdout.write(self.style.NOTICE(f"Higiene codigo_interno=Id: {len(higiene)}"))
            for h in higiene:
                self.stdout.write(
                    "  {id} | {nome!r} gm={gm!r}".format(
                        id=h["produto_externo_id"],
                        nome=h["nome"],
                        gm=h["codigo_nfe"] or "—",
                    )
                )
        elif options.get("higiene"):
            self.stdout.write("Higiene codigo_interno=Id: 0")
