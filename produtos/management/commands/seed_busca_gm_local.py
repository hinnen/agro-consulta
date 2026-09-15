"""Semeia 3 irmãos GM0024-* (código só no overlay) para testar busca no PC."""
from __future__ import annotations

from django.core.management.base import BaseCommand
from django.test.utils import override_settings

from produtos.models import Produto, ProdutoGestaoOverlayAgro


_SPECS = (
    ("seed-gm0024-1", "Junto Cubo Teste Local 1", "GM0024-1", "9101"),
    ("seed-gm0024-10", "Junto Cubo Teste Local 10", "GM0024-10", "9102"),
    ("seed-gm0024-15", "Junto Cubo Teste Local 15", "GM0024-15", "9103"),
)


class Command(BaseCommand):
    help = "Cria 3 produtos GM0024-1/10/15 (GM só no overlay) para validar busca local."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limpar",
            action="store_true",
            help="Remove os produtos seed-gm0024-*",
        )
        parser.add_argument(
            "--provar",
            action="store_true",
            help="Após semear, roda buscar('gm0024') e imprime quantos achou",
        )

    def handle(self, *args, **options):
        if options["limpar"]:
            n_p = Produto.objects.filter(produto_externo_id__startswith="seed-gm0024").delete()
            n_o = ProdutoGestaoOverlayAgro.objects.filter(
                produto_externo_id__startswith="seed-gm0024"
            ).delete()
            self.stdout.write(self.style.WARNING(f"Removido produtos={n_p} overlays={n_o}"))
            return

        for pid, nome, gm, cod in _SPECS:
            Produto.objects.update_or_create(
                produto_externo_id=pid,
                defaults={
                    "erp_produto_id": pid,
                    "codigo_interno": cod,
                    "codigo_nfe": "",  # GM só no overlay (cenário da loja)
                    "nome": nome,
                    "ativo": True,
                    "cadastro_inativo": False,
                    "preco_venda": 19.90,
                },
            )
            ProdutoGestaoOverlayAgro.objects.update_or_create(
                produto_externo_id=pid,
                defaults={"codigo_nfe": gm, "nome": ""},
            )
        self.stdout.write(self.style.SUCCESS("OK — 3 produtos seed-gm0024-* no overlay."))
        self.stdout.write(
            "No PowerShell, antes do runserver:\n"
            "  $env:AGRO_FONTE_CATALOGO='agro_pg'\n"
            "  python manage.py runserver\n"
            "Depois: Cadastro ou /api/buscar/?q=gm0024&contexto=cadastro = 3 itens."
        )

        if options["provar"]:
            from produtos import catalogo_agro as cat
            from produtos.motor_busca_unificado_util import buscar_documentos_unificado

            with override_settings(
                AGRO_FONTE_CATALOGO="agro_pg",
                AGRO_PDV_MERGE_CATALOGO_POSTGRES=True,
            ):
                rows = cat.buscar("gm0024", limit=20)
                docs = buscar_documentos_unificado("gm0024", None, None, limit=20)
            self.stdout.write(
                f"catalogo_agro.buscar = {len(rows)} | "
                f"unificado = {len(docs)} | "
                f"codigos={[d.get('CodigoNFe') for d in docs]}"
            )
            if len(docs) >= 3:
                self.stdout.write(self.style.SUCCESS("PROVA OK — 3+ resultados"))
            else:
                self.stdout.write(self.style.ERROR(f"PROVA FALHOU — só {len(docs)}"))
