"""Consulta Dist DF-e 1×/dia e grava caixa de entrada (mesmo fluxo do cron HTTP)."""
from __future__ import annotations

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Dist DF-e: consulta SEFAZ no máximo 1×/dia e grava notas na caixa de entrada Postgres."

    def add_arguments(self, parser):
        parser.add_argument(
            "--force",
            action="store_true",
            help="Ignora o limite local de 1×/dia (cooldown SEFAZ continua valendo).",
        )

    def handle(self, *args, **options):
        from produtos.dfe_inbox_util import dfe_executar_consulta_e_gravar
        from produtos.sefaz_dfe_client import _cfg_dist_dfe

        if options.get("force"):
            import re
            from datetime import date

            from django.core.cache import cache

            cnpj = re.sub(r"\D", "", str(_cfg_dist_dfe().get("cnpj") or ""))[:14]
            if len(cnpj) == 14:
                cache.delete(f"agro_dfe_cron_day:{cnpj}:{date.today().isoformat()}")

        res = dfe_executar_consulta_e_gravar(origem="cron")
        if res.get("pulado"):
            self.stdout.write(self.style.WARNING(res.get("motivo") or "Pulado (já rodou hoje)."))
            return
        if res.get("ok"):
            inbox = res.get("inbox") or {}
            self.stdout.write(
                self.style.SUCCESS(
                    f"OK cStat={res.get('c_stat')} novas={inbox.get('novas')} "
                    f"atualizadas={inbox.get('atualizadas')} ultNSU={res.get('ult_nsu')}"
                )
            )
            return
        self.stderr.write(
            self.style.ERROR(
                f"Falha: {res.get('erro') or res.get('x_motivo') or res} "
                f"(aguardar={res.get('aguardar_segundos')})"
            )
        )
