"""Gera títulos de salário no CP no dia configurado por funcionário."""

from django.core.management.base import BaseCommand
from django.utils import timezone

from rh.services.envio_cp_automatico import rodar_envio_cp_automatico_diario


class Command(BaseCommand):
    help = (
        "RH: no dia de envio de cada funcionário, cria título CP da competência anterior "
        "(conta placeholder). Rode diariamente via cron."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Só lista candidatos do dia, sem gravar.",
        )
        parser.add_argument(
            "--data",
            type=str,
            default="",
            help="Data YYYY-MM-DD (default: hoje local).",
        )

    def handle(self, *args, **options):
        hoje = timezone.localdate()
        raw = (options.get("data") or "").strip()
        if raw:
            from datetime import date as date_cls

            hoje = date_cls.fromisoformat(raw[:10])
        out = rodar_envio_cp_automatico_diario(hoje=hoje, dry_run=bool(options.get("dry_run")))
        self.stdout.write(
            f"RH envio CP auto {out.get('hoje')}: candidatos={out.get('candidatos')} "
            f"criados={out.get('criados')} ja={out.get('ja_existiam')} erros={len(out.get('erros') or [])}"
        )
        for e in out.get("erros") or []:
            self.stderr.write(self.style.ERROR(str(e)[:400]))
        if not out.get("ok") and (out.get("erros") or []):
            raise SystemExit(1)
        self.stdout.write(self.style.SUCCESS("OK"))
