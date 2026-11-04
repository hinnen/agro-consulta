#!/usr/bin/env python3
"""Prova: entrega a pagar continua no PDV depois que o caixa fecha.

Copia o SQLite local e desfaz cada prova. Não usa o Postgres da loja.
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _banco_local() -> None:
    url = (os.environ.get("DATABASE_URL") or "").strip().lower()
    if url.startswith("postgres"):
        raise SystemExit("VERIFY_FAIL: este script não roda no Postgres da loja")
    destino = Path("/tmp/agro-entrega-caixa-fechado.sqlite")
    if destino.exists():
        destino.unlink()
    os.environ["DATABASE_URL"] = f"sqlite:///{destino}"


def main() -> int:
    util = (ROOT / "produtos/entrega_pdv_pendente_util.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    falta = []
    if "apenas_caixas_abertos: bool = False" not in util:
        falta.append("overlay")
    if "sessao_caixa__fechado_em__isnull=False" not in util:
        falta.append("caixa fechado")
    if "PONTO_CAIXA_TESTE" not in util:
        falta.append("caixa teste")
    trecho = views.split("def api_pdv_entregas_pendentes", 1)[-1][:600]
    if "apenas_caixas_abertos=False" not in trecho:
        falta.append("api")
    if falta:
        print("VERIFY_FAIL", ", ".join(falta))
        return 1
    _banco_local()
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from django.apps import apps
    from django.db import connection

    with connection.schema_editor() as editor:
        for model in apps.get_models():
            if model._meta.proxy or not model._meta.managed:
                continue
            editor.create_model(model)
    from produtos.tests_entrega_pdv_overlay import EntregaPdvOverlayCaixaFechadoTests

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(EntregaPdvOverlayCaixaFechadoTests)
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    if not result.wasSuccessful():
        print(f"VERIFY_FAIL {len(result.failures)} falhas {len(result.errors)} erros")
        return 1
    print("VERIFY_OK 9")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
