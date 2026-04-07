"""
Preenche DataPagamento no Mongo a partir do texto ``Agro parc. DD/MM/AAAA`` nas observações,
quando o campo ficou na data mínima (0001-01-01) após sync do ERP.

Uso:
  python manage.py fix_agro_parcial_datapagamento --dry-run
  python manage.py fix_agro_parcial_datapagamento
"""

from __future__ import annotations

import re
from datetime import date, datetime, time as dtime

from bson import ObjectId
from django.core.management.base import BaseCommand
from django.utils import timezone

from produtos.mongo_financeiro_util import COL_DTO_LANCAMENTO, _SENTINEL
from produtos.views import obter_conexao_mongo

_AGRO_PARC = re.compile(r"Agro parc\.\s*(\d{2}/\d{2}/\d{4})", re.IGNORECASE)


def _ultima_data_observacao(obs: str):
    ms = _AGRO_PARC.findall(obs or "")
    if not ms:
        return None
    last = ms[-1]
    try:
        dd, mm, yyyy = last.split("/")
        d = date(int(yyyy), int(mm), int(dd))
        return timezone.make_aware(
            datetime.combine(d, dtime(12, 0, 0)),
            timezone.get_current_timezone(),
        )
    except (ValueError, TypeError):
        return None


class Command(BaseCommand):
    help = "Corrige DataPagamento em títulos com baixa parcial Agro e data só nas observações."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Só lista quantos documentos seriam alterados, sem gravar.",
        )

    def handle(self, *args, **options):
        dry = bool(options.get("dry_run"))
        _, db = obter_conexao_mongo()
        if db is None:
            self.stderr.write(self.style.ERROR("Mongo indisponível."))
            return

        col = db[COL_DTO_LANCAMENTO]
        q = {
            "Observacoes": _AGRO_PARC,
            "ValorPago": {"$gt": 0.02},
            "$or": [
                {"DataPagamento": {"$exists": False}},
                {"DataPagamento": None},
                {"DataPagamento": {"$lte": _SENTINEL}},
            ],
        }
        cursor = col.find(q, {"Observacoes": 1, "DataPagamento": 1})
        atualizar = 0
        for doc in cursor:
            oid = doc.get("_id")
            if not isinstance(oid, ObjectId):
                continue
            dt = _ultima_data_observacao(str(doc.get("Observacoes") or ""))
            if dt is None:
                continue
            atualizar += 1
            if dry:
                self.stdout.write(f"[dry-run] {oid} -> DataPagamento={dt.isoformat()}")
            else:
                col.update_one(
                    {"_id": oid},
                    {"$set": {"DataPagamento": dt, "LastUpdate": timezone.now()}},
                )
        msg = f"{'Simulado' if dry else 'Atualizado'}: {atualizar} documento(s)."
        self.stdout.write(self.style.SUCCESS(msg))
