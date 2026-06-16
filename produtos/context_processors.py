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


def agro_app_build(request):
    try:
        from config.app_build_util import get_app_build_info

        return {"agro_build": get_app_build_info()}
    except Exception:
        logger.exception("agro_app_build: falha ao ler versão")
        return {
            "agro_build": {
                "version": "1.01",
                "version_label": "1.01",
                "build": 0,
                "commit": "",
                "commit_full": "",
                "branch": "",
                "built_at": "",
                "version_commits": [],
            }
        }

