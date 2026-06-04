"""Variáveis de template partilhadas entre apps."""

import logging

logger = logging.getLogger(__name__)


def home_launcher_nav(request):
    try:
        from produtos.views import home_launcher_nav_items

        return {"home_launcher_nav_items": home_launcher_nav_items()}
    except Exception:
        logger.exception("home_launcher_nav: falha ao montar menu do launcher")
        return {"home_launcher_nav_items": []}

