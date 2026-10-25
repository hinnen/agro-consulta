"""Validação opcional do XML NF-e 4.00 contra XSD oficial (arquivo local)."""
from __future__ import annotations

import os
from pathlib import Path

from django.conf import settings


def xsd_nfe_v4_path() -> Path | None:
    env = (os.environ.get("NFE_SAIDA_XSD_PATH") or "").strip()
    if env:
        p = Path(env)
        return p if p.is_file() else None
    for rel in (
        "produtos/schemas/nfe/leiauteNFe_v4.00.xsd",
        "docs/schemas/nfe/leiauteNFe_v4.00.xsd",
    ):
        p = Path(settings.BASE_DIR) / rel
        if p.is_file():
            return p
    return None


def validar_xml_nfe_xsd(xml_body: str) -> tuple[bool, str]:
    """
    Retorna (ok, detalhe). Se XSD não estiver no servidor, (True, 'skip:no_xsd').
    """
    xsd = xsd_nfe_v4_path()
    if not xsd:
        return True, "skip:no_xsd"
    try:
        from lxml import etree
    except ImportError:
        return True, "skip:no_lxml"
    try:
        schema_doc = etree.parse(str(xsd))
        schema = etree.XMLSchema(schema_doc)
        doc = etree.fromstring(xml_body.encode("utf-8"))
        schema.assertValid(doc)
        return True, "ok"
    except Exception as exc:
        return False, str(exc)[:800]
