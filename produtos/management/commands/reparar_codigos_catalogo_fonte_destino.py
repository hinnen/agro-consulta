"""
Repara nome/GM/EAN na loja a partir de um Postgres fonte (staging).
NÃO mexe em preço nem estoque.

Uso:
  set AGRO_CATALOGO_FONTE_DATABASE_URL=<external URL agro-staging>
  set AGRO_CATALOGO_DEST_DATABASE_URL=<external URL agro-db loja>
  python manage.py reparar_codigos_catalogo_fonte_destino --aplicar

Sem --aplicar: só imprime quantos mudariam.
"""
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import connections
import dj_database_url


_CAMPOS = ("nome", "codigo_barras", "codigo_nfe", "codigo_interno")


def _cfg(url: str) -> dict:
    cfg = dj_database_url.parse(url.strip(), conn_max_age=0)
    cfg.setdefault("TIME_ZONE", None)
    cfg.setdefault("ATOMIC_REQUESTS", False)
    cfg.setdefault("AUTOCOMMIT", True)
    return cfg


class Command(BaseCommand):
    help = "Copia nome/GM/EAN/codigo_interno da fonte → destino (sem preço)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--aplicar",
            action="store_true",
            help="Grava no destino. Sem isto, só conta.",
        )
        parser.add_argument(
            "--limite",
            type=int,
            default=0,
            help="Máximo de produtos a alterar (0 = todos).",
        )

    def handle(self, *args, **options):
        from django.conf import settings
        from produtos.models import Produto

        fonte_url = (getattr(settings, "AGRO_CATALOGO_FONTE_DATABASE_URL", "") or "").strip()
        dest_url = (getattr(settings, "AGRO_CATALOGO_DEST_DATABASE_URL", "") or "").strip()
        if not fonte_url:
            from decouple import config

            fonte_url = (config("AGRO_CATALOGO_FONTE_DATABASE_URL", default="") or "").strip()
        if not fonte_url:
            # Local já no staging: usa DATABASE_URL atual como fonte.
            fonte_url = (config("DATABASE_URL", default="") or "").strip()
        if not dest_url:
            from decouple import config

            dest_url = (config("AGRO_CATALOGO_DEST_DATABASE_URL", default="") or "").strip()

        if not fonte_url or not dest_url:
            self.stderr.write(
                "Falta AGRO_CATALOGO_DEST_DATABASE_URL no .env (External URL do agro-db da loja).\n"
                "Fonte = DATABASE_URL atual (agro-staging) se AGRO_CATALOGO_FONTE não estiver setada."
            )
            return
        if fonte_url == dest_url:
            self.stderr.write("Fonte e destino iguais — abortado.")
            return

        settings.DATABASES["cat_fonte"] = _cfg(fonte_url)
        settings.DATABASES["cat_dest"] = _cfg(dest_url)
        connections.databases["cat_fonte"] = settings.DATABASES["cat_fonte"]
        connections.databases["cat_dest"] = settings.DATABASES["cat_dest"]
        for alias in ("cat_fonte", "cat_dest"):
            if alias in connections:
                connections[alias].close()

        aplicar = bool(options.get("aplicar"))
        limite = int(options.get("limite") or 0)

        fonte_qs = (
            Produto.objects.using("cat_fonte")
            .exclude(produto_externo_id__isnull=True)
            .exclude(produto_externo_id="")
            .only("produto_externo_id", *_CAMPOS)
        )

        n_igual = 0
        n_mudaria = 0
        n_sem_dest = 0
        n_aplicado = 0
        exemplos: list[str] = []

        for src in fonte_qs.iterator(chunk_size=400):
            ext = (src.produto_externo_id or "").strip()
            if not ext:
                continue
            dest = (
                Produto.objects.using("cat_dest")
                .filter(produto_externo_id=ext)
                .only("pk", "produto_externo_id", *_CAMPOS)
                .first()
            )
            if not dest:
                n_sem_dest += 1
                continue

            mudou = False
            before = {}
            for c in _CAMPOS:
                sv = (getattr(src, c, None) or "")
                dv = (getattr(dest, c, None) or "")
                if isinstance(sv, str):
                    sv = sv.strip()
                if isinstance(dv, str):
                    dv = dv.strip()
                # Fonte vazia não sobrescreve destino
                if not sv:
                    continue
                if str(sv) != str(dv):
                    mudou = True
                    before[c] = (dv, sv)

            if not mudou:
                n_igual += 1
                continue

            n_mudaria += 1
            if len(exemplos) < 12:
                exemplos.append(
                    f"{ext} | "
                    + "; ".join(f"{k}: {a!r} → {b!r}" for k, (a, b) in before.items())
                )

            if not aplicar:
                continue
            if limite and n_aplicado >= limite:
                continue

            for c, (_old, new) in before.items():
                setattr(dest, c, new)
            dest.save(using="cat_dest", update_fields=list(before.keys()))
            n_aplicado += 1

        self.stdout.write(
            f"iguais={n_igual} mudaria={n_mudaria} sem_destino={n_sem_dest} "
            f"aplicado={n_aplicado} modo={'APLICAR' if aplicar else 'DRY'}"
        )
        for line in exemplos:
            self.stdout.write(line)
