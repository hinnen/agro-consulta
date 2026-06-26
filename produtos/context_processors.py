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


def agro_emprestimo_dual_ui(request):
    try:
        from produtos.mongo_financeiro_util import emprestimo_defaults_para_ui

        return {"agro_emprestimo_dual_cfg": emprestimo_defaults_para_ui()}
    except Exception:
        logger.exception("agro_emprestimo_dual_ui")
        return {"agro_emprestimo_dual_cfg": {}}


def agro_banco_placeholder_ui(request):
    try:
        from produtos.mongo_financeiro_util import _banco_placeholder_para_select

        ph = _banco_placeholder_para_select()
        return {"agro_banco_placeholder_id": str(ph.get("id") or "").strip()}
    except Exception:
        logger.exception("agro_banco_placeholder_ui")
        return {"agro_banco_placeholder_id": "6990cf726c4d856abaa670c6"}


def agro_display_scale_ui(request):
    from django.conf import settings

    return {
        'agro_display_scale_habilitado': bool(
            getattr(settings, 'AGRO_DISPLAY_SCALE_HABILITADO', False)
        ),
    }


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

