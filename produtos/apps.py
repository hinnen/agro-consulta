from django.apps import AppConfig


class ProdutosConfig(AppConfig):
    name = "produtos"

    def ready(self):
        import produtos.signals  # noqa: F401
        self._agendar_bootstrap_financeiro_pg_staging()

    @staticmethod
    def _agendar_bootstrap_financeiro_pg_staging() -> None:
        """Fallback se buildCommand do Render não rodar o script (1 worker, cache lock)."""
        import logging
        import threading

        from django.conf import settings

        if not (
            getattr(settings, "AGRO_ERP_PEDIDOS_DRY_RUN", False)
            and getattr(settings, "AGRO_STAGING_READONLY", False)
        ):
            return

        def _run() -> None:
            try:
                from django.core.cache import cache

                if not cache.add("agro_financeiro_pg_bootstrap_v1", 1, timeout=7200):
                    return
                from produtos.lancamentos_financeiro_agro_util import (
                    maybe_bootstrap_financeiro_pg_staging,
                )

                r = maybe_bootstrap_financeiro_pg_staging()
                if not r.get("skipped") and not r.get("ok"):
                    logging.getLogger(__name__).error(
                        "bootstrap financeiro PG staging: %s", r.get("erro")
                    )
            except Exception:
                logging.getLogger(__name__).exception("bootstrap financeiro PG staging")

        threading.Thread(target=_run, daemon=True, name="agro-fin-pg-bootstrap").start()
