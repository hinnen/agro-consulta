"""
CA bundle para HTTPS com webservices SEFAZ (NFe/NFC-e, distDFe).

Servidores da SEFAZ usam certificados ICP-Brasil (ex.: AC SOLUTI SSL EV G4),
que não entram no bundle Mozilla/certifi. Mesclamos certifi + cadeia v10 em disco.
"""

from __future__ import annotations

import os
import tempfile
from functools import lru_cache
from pathlib import Path

import certifi

_CERTS_DIR = Path(__file__).resolve().parent / "certs" / "icpbrasil"
_ICP_PEM_FILES = (
    "AC-SOLUTI-SSL-EV-G4.pem",
    "ICP-Brasilv10.pem",
)


@lru_cache(maxsize=1)
def sefaz_ca_bundle_path() -> str:
    icp_pem = ""
    for name in _ICP_PEM_FILES:
        path = _CERTS_DIR / name
        if not path.is_file():
            raise FileNotFoundError(f"Certificado ICP-Brasil ausente: {path}")
        icp_pem += path.read_text(encoding="ascii")
        if not icp_pem.endswith("\n"):
            icp_pem += "\n"

    fd, out_path = tempfile.mkstemp(prefix="agro_sefaz_ca_", suffix=".pem")
    try:
        os.write(fd, (certifi.contents() + icp_pem).encode("ascii"))
    finally:
        os.close(fd)
    return out_path


def sefaz_requests_verify() -> str | bool:
    """Valor para ``requests``/``verify=`` nas chamadas à SEFAZ."""
    return sefaz_ca_bundle_path()
