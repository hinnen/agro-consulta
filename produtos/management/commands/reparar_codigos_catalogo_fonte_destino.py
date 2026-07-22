"""
Repara nome/GM/EAN na loja a partir de um Postgres fonte (staging).
NÃO mexe em preço nem estoque.

.env:
  DATABASE_URL=...agro-staging...   (fonte, se AGRO_CATALOGO_FONTE não setada)
  AGRO_CATALOGO_DEST_DATABASE_URL=...agro-db loja...

  python manage.py reparar_codigos_catalogo_fonte_destino
  python manage.py reparar_codigos_catalogo_fonte_destino --aplicar
"""
from __future__ import annotations

import dj_database_url
from django.core.management.base import BaseCommand
from django.db import connections


_CAMPOS = ("nome", "codigo_barras", "codigo_nfe", "codigo_interno")


def _cfg(url: str) -> dict:
    cfg = dj_database_url.parse(url.strip(), conn_max_age=0)
    cfg.setdefault("TIME_ZONE", None)
    cfg.setdefault("ATOMIC_REQUESTS", False)
    cfg.setdefault("AUTOCOMMIT", True)
    cfg.setdefault("OPTIONS", {})
    cfg.setdefault("CONN_HEALTH_CHECKS", False)
    cfg.setdefault("CONN_MAX_AGE", 0)
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
        from decouple import config
        from django.conf import settings
        from produtos.models import Produto

        fonte_url = (getattr(settings, "AGRO_CATALOGO_FONTE_DATABASE_URL", "") or "").strip()
        dest_url = (getattr(settings, "AGRO_CATALOGO_DEST_DATABASE_URL", "") or "").strip()
        if not fonte_url:
            fonte_url = (config("AGRO_CATALOGO_FONTE_DATABASE_URL", default="") or "").strip()
        if not fonte_url:
            fonte_url = (config("DATABASE_URL", default="") or "").strip()
        if not dest_url:
            dest_url = (config("AGRO_CATALOGO_DEST_DATABASE_URL", default="") or "").strip()

        if not fonte_url or not dest_url:
            self.stderr.write(
                "Falta AGRO_CATALOGO_DEST_DATABASE_URL no .env (External URL do agro-db da loja).\n"
                "Fonte = DATABASE_URL atual (agro-staging) se AGRO_CATALOGO_FONTE não estiver setada."
            )
            return
        if fonte_url.strip() == dest_url.strip():
            self.stderr.write("Fonte e destino iguais — abortado.")
            return

        settings.DATABASES["cat_fonte"] = _cfg(fonte_url)
        settings.DATABASES["cat_dest"] = _cfg(dest_url)
        connections.databases["cat_fonte"] = settings.DATABASES["cat_fonte"]
        connections.databases["cat_dest"] = settings.DATABASES["cat_dest"]
        for alias in ("cat_fonte", "cat_dest"):
            if alias in connections:
                try:
                    connections[alias].close()
                except Exception:
                    pass

        aplicar = bool(options.get("aplicar"))
        limite = int(options.get("limite") or 0)

        self.stdout.write("Lendo destino…")
        dest_map: dict[str, Produto] = {}
        for d in (
            Produto.objects.using("cat_dest")
            .exclude(produto_externo_id__isnull=True)
            .exclude(produto_externo_id="")
            .only("pk", "produto_externo_id", *_CAMPOS)
            .iterator(chunk_size=800)
        ):
            dest_map[(d.produto_externo_id or "").strip()] = d
        self.stdout.write(f"destino={len(dest_map)}")

        self.stdout.write("Comparando fonte…")
        n_igual = 0
        n_mudaria = 0
        n_sem_dest = 0
        n_aplicado = 0
        exemplos: list[str] = []
        to_save: list[tuple[Produto, list[str]]] = []

        for src in (
            Produto.objects.using("cat_fonte")
            .exclude(produto_externo_id__isnull=True)
            .exclude(produto_externo_id="")
            .only("produto_externo_id", *_CAMPOS)
            .iterator(chunk_size=800)
        ):
            ext = (src.produto_externo_id or "").strip()
            if not ext:
                continue
            dest = dest_map.get(ext)
            if not dest:
                n_sem_dest += 1
                continue

            fields: list[str] = []
            before: dict[str, tuple[str, str]] = {}
            for c in _CAMPOS:
                sv = getattr(src, c, None)
                dv = getattr(dest, c, None)
                sv = (sv or "").strip() if isinstance(sv, str) else (str(sv) if sv is not None else "")
                dv = (dv or "").strip() if isinstance(dv, str) else (str(dv) if dv is not None else "")
                if not sv:
                    continue
                if sv != dv:
                    before[c] = (dv, sv)
                    setattr(dest, c, sv)
                    fields.append(c)

            if not fields:
                n_igual += 1
                continue

            n_mudaria += 1
            if len(exemplos) < 15:
                exemplos.append(
                    f"{ext} | " + "; ".join(f"{k}: {a!r}->{b!r}" for k, (a, b) in before.items())
                )
            to_save.append((dest, fields))

        if aplicar:
            for dest, fields in to_save:
                if limite and n_aplicado >= limite:
                    break
                dest.save(using="cat_dest", update_fields=fields)
                n_aplicado += 1

        self.stdout.write(
            f"iguais={n_igual} mudaria={n_mudaria} sem_destino={n_sem_dest} "
            f"aplicado={n_aplicado} modo={'APLICAR' if aplicar else 'DRY'}"
        )
        for line in exemplos:
            self.stdout.write(line)
