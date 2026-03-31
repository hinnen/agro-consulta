from __future__ import annotations

"""
Converte campos de ID financeiro que ficaram como ObjectId para string,
para alinhar com o DTO do ERP (que espera string).

Afeta a coleção Mongo DtoLancamento:
  - BancoID
  - FormaPagamentoID
  - EmpresaID
  - ClienteID
  - LancamentoGrupoID

Use primeiro em modo dry-run:
  python manage.py corrigir_ids_financeiro --dry-run

Depois, para aplicar de fato:
  python manage.py corrigir_ids_financeiro
"""

from bson import ObjectId
from django.core.management.base import BaseCommand

from produtos.mongo_financeiro_util import COL_DTO_LANCAMENTO
from produtos.views import obter_conexao_mongo


class Command(BaseCommand):
  help = "Normaliza IDs financeiros (BancoID/FormaPagamentoID etc.) de ObjectId para string em DtoLancamento."

  def add_arguments(self, parser):
    parser.add_argument(
      "--dry-run",
      action="store_true",
      help="Só conta quantos registros seriam alterados, sem gravar.",
    )

  def handle(self, *args, **options):
    dry = bool(options.get("dry_run"))
    _, db = obter_conexao_mongo()
    if db is None:
      self.stderr.write(self.style.ERROR("Mongo indisponível"))
      return

    col = db[COL_DTO_LANCAMENTO]
    campos = ["BancoID", "FormaPagamentoID", "EmpresaID", "ClienteID", "LancamentoGrupoID"]
    total_alterados = 0

    for campo in campos:
      filtro = {campo: {"$type": "objectId"}}
      qtd = col.count_documents(filtro)
      if qtd == 0:
        self.stdout.write(f"{campo}: nenhum documento com ObjectId.")
        continue
      self.stdout.write(f"{campo}: encontrados {qtd} documentos com ObjectId.")
      if dry:
        continue

      cur = col.find(filtro, {"_id": 1, campo: 1})
      alterados_campo = 0
      for doc in cur:
        oid = doc.get(campo)
        if not isinstance(oid, ObjectId):
          continue
        col.update_one({"_id": doc["_id"]}, {"$set": {campo: str(oid)}})
        alterados_campo += 1
      total_alterados += alterados_campo
      self.stdout.write(self.style.SUCCESS(f"{campo}: convertidos {alterados_campo} documentos."))

    if dry:
      self.stdout.write(self.style.WARNING("Dry-run concluído — nenhuma alteração gravada."))
    else:
      self.stdout.write(self.style.SUCCESS(f"Correção concluída. Total de documentos alterados: {total_alterados}"))

