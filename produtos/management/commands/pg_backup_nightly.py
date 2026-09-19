"""Backup Postgres noturno — completo + por categoria + kit zero (FL-048)."""

from django.core.management.base import BaseCommand

from produtos.pg_backup_nightly import executar_pg_backup_nightly, nightly_backup_permitido


class Command(BaseCommand):
    help = "Gera backups Postgres (completo, kit, por categoria) e envia se AGRO_PG_BACKUP_UPLOAD_MODE estiver configurado."

    def add_arguments(self, parser):
        parser.add_argument(
            "--sem-upload",
            action="store_true",
            help="Só gera ZIPs em memória (teste); não envia para nuvem.",
        )
        parser.add_argument(
            "--sem-categorias",
            action="store_true",
            help="Não gera ZIP individual por categoria (só completo + kit).",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Ignora AGRO_PG_BACKUP_NIGHTLY_ENABLED (útil em staging com ALLOW_STAGING).",
        )

    def handle(self, *args, **options):
        if not options["force"]:
            ok, motivo = nightly_backup_permitido()
            if not ok:
                self.stderr.write(self.style.ERROR(motivo))
                raise SystemExit(1)

        result = executar_pg_backup_nightly(
            username="manage.py",
            upload=not options["sem_upload"],
            incluir_por_categoria=not options["sem_categorias"],
            force=options["force"],
        )
        for art in result.get("artefatos") or []:
            up = art.get("upload") or {}
            up_txt = "enviado" if up.get("ok") else ("pulado" if up.get("skipped") else "falhou")
            self.stdout.write(
                f"{art.get('tipo')}: {art.get('filename')} ({art.get('bytes')} bytes) — upload {up_txt}"
            )
        for err in result.get("erros") or []:
            self.stderr.write(self.style.ERROR(err))
        if not result.get("ok"):
            raise SystemExit(1)
        self.stdout.write(self.style.SUCCESS(f"OK — {result.get('total_artefatos')} artefato(s)."))
