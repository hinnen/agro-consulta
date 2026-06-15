"""Congela títulos financeiros no Mongo (AgroFonteVerdade) antes de desligar sync ERP."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.mongo_financeiro_util import congelar_lancamentos_financeiro_agro
from produtos.views import obter_conexao_mongo


class Command(BaseCommand):
    help = (
        "Marca todos os DtoLancamento com AgroFonteVerdade (não altera valores). "
        "Rode antes de AGRO_FINANCEIRO_ERP_SYNC_HABILITADO=false em produção."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Só conta quantos títulos seriam marcados.",
        )

    def handle(self, *args, **options):
        dry = bool(options.get("dry_run"))
        _, db = obter_conexao_mongo()
        if db is None:
            self.stderr.write(self.style.ERROR("Mongo indisponível."))
            return
        r = congelar_lancamentos_financeiro_agro(db, usuario="manage.py", dry_run=dry)
        if not r.get("ok"):
            self.stderr.write(self.style.ERROR(str(r.get("erro") or "Falha")))
            return
        if dry:
            self.stdout.write(
                self.style.WARNING(
                    f"[dry-run] ~{r.get('total_estimado')} títulos; "
                    f"{r.get('ja_marcados')} já com AgroFonteVerdade; "
                    f"{r.get('a_marcar')} a marcar."
                )
            )
            return
        self.stdout.write(
            self.style.SUCCESS(
                f"Congelados {r.get('marcados_agora')} título(s) "
                f"(total estimado na coleção: {r.get('total_estimado')})."
            )
        )
        self.stdout.write("")
        self.stdout.write("Próximos passos no ambiente (Render / .env):")
        self.stdout.write("  1. AGRO_FINANCEIRO_MONGO_CONGELADO=true")
        self.stdout.write("  2. AGRO_FINANCEIRO_ERP_SYNC_HABILITADO=false")
        self.stdout.write(
            "  3. No WL/SisVale: desligar sincronização Mongo da coleção DtoLancamento "
            "(senão o ERP pode sobrescrever títulos de fora do Agro)."
        )
        self.stdout.write(
            "  4. (Opcional) mongodump da coleção DtoLancamento para backup físico."
        )
