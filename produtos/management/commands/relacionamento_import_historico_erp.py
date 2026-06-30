"""Import único vendas ERP → histórico F8 (FL-042)."""
from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand

from produtos.relacionamento_historico_erp_util import importar_historico_erp_mongo


class Command(BaseCommand):
    help = (
        "Importa vendas ERP (Mongo DtoVenda) até a data corte para histórico F8. "
        "Use --dry-run antes. Não mexe fiado, cashback, caixa nem VendaAgro."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Só relatório — não grava Postgres",
        )
        parser.add_argument(
            "--ate",
            default="2026-05-26",
            help="Último dia ERP inclusive (default: 2026-05-26)",
        )
        parser.add_argument(
            "--pdv-desde",
            default="2026-05-27",
            help="PDV SisVale permanente desde (default: 2026-05-27)",
        )
        parser.add_argument(
            "--desde",
            default="2015-01-01",
            help="Início varredura Mongo (default: 2015-01-01)",
        )
        parser.add_argument(
            "--lote",
            default="",
            help="ID do lote (default: erp-hist-AAAA-MM-DD)",
        )
        parser.add_argument(
            "--chunk-meses",
            type=int,
            default=1,
            help="Meses por fatia na leitura Mongo (default: 1)",
        )

    def handle(self, *args, **options):
        ate = date.fromisoformat(str(options["ate"])[:10])
        pdv_desde = date.fromisoformat(str(options["pdv_desde"])[:10])
        desde = date.fromisoformat(str(options["desde"])[:10])
        dry = bool(options.get("dry_run"))

        r = importar_historico_erp_mongo(
            ate=ate,
            pdv_desde=pdv_desde,
            desde=desde,
            lote_id=str(options.get("lote") or ""),
            dry_run=dry,
            chunk_meses=int(options.get("chunk_meses") or 1),
        )
        if not r.get("ok"):
            self.stderr.write(self.style.ERROR(r.get("erro") or "Falha"))
            return

        st = r.get("stats") or {}
        prefix = "DRY-RUN · " if dry else ""
        self.stdout.write(
            self.style.SUCCESS(
                f"{prefix}Lote {st.get('lote_id')} · ERP ≤ {st.get('erp_ate')} · "
                f"PDV ≥ {st.get('pdv_desde')}\n"
                f"  Clientes Agro ativos: {st.get('clientes_agro_ativos')} "
                f"(com externo_id: {st.get('clientes_agro_com_externo_id')})\n"
                f"  Pessoas Mongo indexadas: {st.get('pessoas_mongo_index')}\n"
                f"  Cabeçalhos lidos: {st.get('cabecalhos_lidos')}\n"
                f"  No corte: {st.get('vendas_no_corte')}\n"
                f"  Consumidor não identificado (ignoradas): {st.get('vendas_consumidor')}\n"
                f"  Importáveis: {st.get('vendas_importadas')}\n"
                f"  Sem cliente Agro: {st.get('vendas_sem_cliente')}\n"
                f"  Duplicadas (já importadas): {st.get('vendas_duplicadas')}\n"
                f"  Clientes com venda: {st.get('clientes_com_venda')}\n"
                f"  Itens importados: {st.get('itens_importados', '—')}\n"
                f"  Vendas sem linha Mongo: {st.get('vendas_sem_itens', '—')}\n"
                f"  Itens sem catálogo ativo (amostra): {st.get('itens_sem_codigo_catalogo', '—')}"
            )
        )
