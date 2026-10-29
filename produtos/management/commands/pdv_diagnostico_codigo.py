"""
Diagnóstico rápido de produto por código GM / barras (bug PDV carrinho / bip).

  python manage.py pdv_diagnostico_codigo GM4579 GM15181253
"""

from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from produtos.models import ProdutoGestaoOverlayAgro
from produtos.mongo_index_codigos import INDEX_CODIGOS_CAMPO
from produtos.views import motor_busca_consulta_documentos, obter_conexao_mongo


class Command(BaseCommand):
    help = "Lista Id Mongo, códigos e index_codigos para termos de busca do PDV."

    def add_arguments(self, parser):
        parser.add_argument("codigos", nargs="+", help="Ex.: GM4579 7891234567890")

    def handle(self, *args, **options):
        client, db = obter_conexao_mongo()
        if db is None:
            self.stderr.write("Mongo indisponível.")
            return

        for raw in options["codigos"]:
            q = str(raw or "").strip()
            if not q:
                continue
            self.stdout.write(self.style.HTTP_INFO(f"\n=== {q} ==="))
            prods = motor_busca_consulta_documentos(q, db, client, limit=8, include_inactive=True)
            if not prods:
                self.stdout.write("  Nenhum documento no motor de busca.")
                continue
            for p in prods:
                pid = str(p.get("Id") or p.get("_id") or "").strip()
                ix = p.get(INDEX_CODIGOS_CAMPO) or []
                row = {
                    "id": pid or "(VAZIO)",
                    "nome": (p.get("Nome") or "")[:80],
                    "CodigoNFe": p.get("CodigoNFe") or p.get("Codigo"),
                    "CodigoBarras": p.get("CodigoBarras") or p.get("EAN_NFe"),
                    "inativo": bool(p.get("CadastroInativo")),
                    "index_codigos_n": len(ix) if isinstance(ix, list) else 0,
                    "index_codigos_amostra": (ix[:12] if isinstance(ix, list) else []),
                }
                self.stdout.write(json.dumps(row, ensure_ascii=False, indent=2))
                if not pid or pid.lower() == "none":
                    self.stdout.write(self.style.ERROR("  ^ PRODUTO SEM Id — PDV não deve adicionar ao carrinho."))
                ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid[:64]).first()
                if ov:
                    self.stdout.write(
                        f"  overlay: codigo_nfe={ov.codigo_nfe!r} barras={ov.codigo_barras!r}"
                    )
