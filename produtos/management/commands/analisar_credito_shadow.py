"""
Dry-run / persistência do laboratório de crédito (shadow).

  python manage.py analisar_credito_shadow
  python manage.py analisar_credito_shadow --cliente-id 12
  python manage.py analisar_credito_shadow --persist

Sem --persist: NÃO grava nada.
Com --persist: só INSERT em ClienteAnaliseCreditoAgro.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from produtos.credito_score_shadow import analisar_clientes


class Command(BaseCommand):
    help = "Análise de crédito shadow (dry-run por padrão). Não altera fiado operacional."

    def add_arguments(self, parser):
        parser.add_argument(
            "--cliente-id",
            type=int,
            default=None,
            help="Analisa só este ClienteAgro.pk",
        )
        parser.add_argument(
            "--persist",
            action="store_true",
            help="Grava snapshots em ClienteAnaliseCreditoAgro (única escrita).",
        )

    def handle(self, *args, **options):
        cid = options.get("cliente_id")
        persist = bool(options.get("persist"))
        ids = [cid] if cid else None
        if cid is not None and cid <= 0:
            raise CommandError("cliente-id inválido.")

        self.stdout.write(
            self.style.WARNING(
                "Modo: "
                + ("PERSIST (só tabela shadow)" if persist else "DRY-RUN (não grava)")
            )
        )
        resultados = analisar_clientes(cliente_ids=ids, persist=persist)
        if not resultados:
            self.stdout.write("Nenhum cliente encontrado.")
            return

        self.stdout.write(
            f"{'Cliente':<32} {'Score':>5} {'Conf':<10} {'Atual':>10} {'Saldo':>10} {'Média':>10} {'Suger':>10}"
        )
        self.stdout.write("-" * 100)
        for r in resultados[:200]:
            nome = (r.cliente_nome or "")[:30]
            sc = "—" if r.score is None else str(r.score)
            self.stdout.write(
                f"{nome:<32} {sc:>5} {r.confianca:<10} "
                f"{float(r.limite_efetivo):>10.2f} {float(r.saldo_aberto):>10.2f} "
                f"{float(r.media_fiado_3m):>10.2f} {float(r.limite_sugerido):>10.2f}"
            )
        if len(resultados) > 200:
            self.stdout.write(f"… +{len(resultados) - 200} clientes")
        self.stdout.write(self.style.SUCCESS(f"Total: {len(resultados)}"))
