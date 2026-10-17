"""Promove EAN bipável (230…) e move legado para opcionais — ver ``cb_loja_legado_migrate_util``."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.cb_loja_legado_migrate_util import (
    migrar_cb_loja_legado_em_produto,
    migrar_cb_loja_legado_lote,
)


class Command(BaseCommand):
    help = "Cadastro: legado 230… sem DV → principal EAN válido + legado em codigos_barras_opcionais."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Só listar alterações.")
        parser.add_argument("--limit", type=int, default=5000, help="Máximo de produtos.")
        parser.add_argument(
            "--pid",
            type=str,
            default="",
            help="Só um produto_externo_id (ex. id do GM4045).",
        )
        parser.add_argument(
            "--liberar-intruso",
            action="store_true",
            help="Colisão: gera 230 novo no produto que ocupa o EAN bipável e migra o legado.",
        )

    def handle(self, *args, **options):
        dry = bool(options["dry_run"])
        limit = max(1, int(options["limit"] or 5000))
        pid = str(options.get("pid") or "").strip()
        liberar = bool(options.get("liberar_intruso"))
        db, col = None, None
        try:
            from produtos.mongo_util import obter_conexao_mongo

            _c, db = obter_conexao_mongo()
            if _c is not None:
                col = _c.col_p
        except Exception:
            db, col = None, None

        done = 0
        if pid:
            from produtos.catalogo_agro import obter_produto_model

            p = obter_produto_model(pid)
            if p is None:
                self.stderr.write(f"Produto não encontrado: {pid}")
                return
            r = migrar_cb_loja_legado_em_produto(
                p,
                dry_run=dry,
                liberar_intruso=liberar,
                db=db,
                col=col,
            )
            if r:
                if r.get("erro"):
                    self.stderr.write(self.style.ERROR(str(r["erro"])))
                self.stdout.write(str(r))
                done = 0 if r.get("erro") else 1
            else:
                self.stdout.write("Nada a migrar (já EAN válido ou não é 230… legado).")
            if done and not dry:
                try:
                    from produtos.bca_busca_cache_util import bca_busca_cache_bump_invalidate

                    bca_busca_cache_bump_invalidate()
                except Exception:
                    pass
            return

        if dry and liberar:
            self.stdout.write(
                self.style.WARNING(
                    "Dry-run com --liberar-intruso: não grava reatribuições; "
                    "colisões listadas podem sumir ao rodar sem --dry-run."
                )
            )

        res = migrar_cb_loja_legado_lote(
            limit=limit,
            dry_run=dry,
            liberar_intruso=liberar,
            db=db,
            col=col,
        )
        for r in res.get("colisoes_detalhe") or []:
            self.stderr.write(
                self.style.ERROR(
                    f"COLISÃO {r.get('produto_externo_id')}: {r.get('legado')} → bip "
                    f"{r.get('principal_novo')} — {str(r.get('erro', ''))[:160]}"
                )
            )
        done = int(res.get("corrigidos") or 0)
        extra = ""
        if res.get("grupos") is not None:
            extra = f" | Grupos EAN: {res.get('grupos')} | Reatribuídos: {res.get('reatribuidos_grupo', 0)}"
        self.stdout.write(
            self.style.SUCCESS(
                f"Corrigidos: {done} | Colisões (mexer só nestes): {res.get('colisoes', 0)}"
                + extra
                + (" (dry-run)" if dry else "")
            )
        )
        if done and not dry:
            try:
                from produtos.bca_busca_cache_util import bca_busca_cache_bump_invalidate

                bca_busca_cache_bump_invalidate()
                self.stdout.write("Cache busca BCA invalidado (bip 230 no PDV).")
            except Exception:
                pass
