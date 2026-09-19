from django.apps import AppConfig


class ConfigConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "config"
    label = "agro_config"

    def ready(self) -> None:
        try:
            from config.app_build_util import sync_build_stamp

            sync_build_stamp()
        except Exception:
            import logging

            logging.getLogger(__name__).exception("sync_build_stamp na subida do app")
