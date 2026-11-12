"""Leitura (e gravação) de ficha da loja, sem migrar o banco da loja.

Conversas do Zap continuam no banco `default` deste PC.
PIN, cliente, fiado, vale e cashback vão para `loja` quando
`AGRO_LOJA_DATABASE_URL` existe.
"""

_APP_MODELOS = {
    ("produtos", "ClienteAgro"),
    ("produtos", "ClienteAgroEventoAgro"),
    ("produtos", "FiadoTituloAgro"),
    ("base", "PerfilUsuario"),
}


def _loja_ligada():
    from django.conf import settings

    return "loja" in getattr(settings, "DATABASES", {})


class LojaLeituraRouter:
    def db_for_read(self, model, **hints):
        if _loja_ligada() and (model._meta.app_label, model.__name__) in _APP_MODELOS:
            return "loja"
        return None

    def db_for_write(self, model, **hints):
        if _loja_ligada() and (model._meta.app_label, model.__name__) in _APP_MODELOS:
            return "loja"
        return None

    def allow_relation(self, obj1, obj2, **hints):
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if db == "loja":
            return False
        return None
