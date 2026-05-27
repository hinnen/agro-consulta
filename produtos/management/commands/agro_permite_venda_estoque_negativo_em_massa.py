"""
Marca «Permitir venda com estoque negativo» em todos os produtos (Mongo + overlay Agro).

  python manage.py agro_permite_venda_estoque_negativo_em_massa
  python manage.py agro_permite_venda_estoque_negativo_em_massa --dry-run
  python manage.py agro_permite_venda_estoque_negativo_em_massa --somente-overlay
"""

from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from produtos.cadastro_estoque_negativo_util import (
    CADASTRO_EXTRA_PERMITE_VENDA_NEGATIVO,
    mongo_set_permite_venda_negativo_payload,
)
from produtos.models import ProdutoGestaoOverlayAgro
from produtos.views import obter_conexao_mongo


class Command(BaseCommand):
    help = "Ativa venda com estoque negativo em massa (espelho Mongo e cadastro_extras do overlay)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Só conta quantos registros seriam alterados.",
        )
        parser.add_argument(
            "--somente-overlay",
            action="store_true",
            help="Não altera o Mongo (só SQLite overlay).",
        )
        parser.add_argument(
            "--somente-mongo",
            action="store_true",
            help="Não altera overlays SQLite.",
        )
        parser.add_argument(
            "--batch",
            type=int,
            default=400,
            help="Tamanho do lote ao salvar overlays (padrão 400).",
        )

    def handle(self, *args, **options):
        dry = bool(options["dry_run"])
        somente_ov = bool(options["somente_overlay"])
        somente_mongo = bool(options["somente_mongo"])
        batch = max(50, min(int(options["batch"] or 400), 2000))

        if dry:
            self.stdout.write(self.style.WARNING("Modo dry-run — nada será gravado."))

        mongo_n = 0
        if not somente_ov:
            client, db = obter_conexao_mongo()
            if db is None or client is None:
                self.stderr.write(self.style.ERROR("Mongo indisponível."))
            else:
                col = db[client.col_p]
                payload = mongo_set_permite_venda_negativo_payload(True)
                if dry:
                    mongo_n = col.count_documents({})
                    self.stdout.write(f"Mongo: {mongo_n} documento(s) receberiam $set.")
                else:
                    res = col.update_many({}, {"$set": payload})
                    mongo_n = int(res.modified_count or 0)
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Mongo: {mongo_n} documento(s) atualizado(s) "
                            f"(matched {int(res.matched_count or 0)})."
                        )
                    )

        ov_n = 0
        if not somente_mongo:
            qs = ProdutoGestaoOverlayAgro.objects.all().only(
                "id", "produto_externo_id", "cadastro_extras"
            )
            total_ov = qs.count()
            if dry:
                ov_n = total_ov
                self.stdout.write(f"Overlay: {ov_n} registro(s) receberiam cadastro_extras.")
            else:
                buf: list[ProdutoGestaoOverlayAgro] = []
                for ov in qs.iterator(chunk_size=batch):
                    ex = dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
                    if ex.get(CADASTRO_EXTRA_PERMITE_VENDA_NEGATIVO) is True:
                        continue
                    ex[CADASTRO_EXTRA_PERMITE_VENDA_NEGATIVO] = True
                    ov.cadastro_extras = ex
                    buf.append(ov)
                    if len(buf) >= batch:
                        with transaction.atomic():
                            ProdutoGestaoOverlayAgro.objects.bulk_update(
                                buf, ["cadastro_extras", "atualizado_em"]
                            )
                        ov_n += len(buf)
                        buf = []
                if buf:
                    with transaction.atomic():
                        ProdutoGestaoOverlayAgro.objects.bulk_update(
                            buf, ["cadastro_extras", "atualizado_em"]
                        )
                    ov_n += len(buf)
                self.stdout.write(self.style.SUCCESS(f"Overlay: {ov_n} registro(s) atualizado(s)."))

        if not dry:
            self.stdout.write(
                self.style.SUCCESS(
                    "Concluído. Novos produtos já nascem com a opção ligada por padrão na tela."
                )
            )
