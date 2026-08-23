"""Prova NFC-e destinatário CPF e CNPJ (sem chamar SEFAZ).

Uso: python scripts/verify_nfce_dest_cnpj_path.py
"""
from __future__ import annotations

import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.models import NfceDocumentoAgro
from produtos.nfce_sp_emissao_util import (
    NS,
    _preencher_dest_nfce,
    cnpj_valido,
    cpf_valido,
    documento_dest_nfce,
    tipo_documento_dest_nfce,
)
from produtos.views_nfce import _nfce_opts_payload

CHECKS = 0
FAILS: list[str] = []


def ok(msg: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    FAILS.append(msg)
    print(f"  FAIL  {msg}")


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def main() -> int:
    print("=== NFCE-DEST-CNPJ path ===")
    check(cpf_valido("52998224725"), "CPF válido (fluxo antigo)")
    check(documento_dest_nfce("529.982.247-25") == "52998224725", "máscara CPF")
    check(tipo_documento_dest_nfce("52998224725") == "CPF", "tipo CPF")
    check(cnpj_valido("11222333000181"), "CNPJ válido")
    check(documento_dest_nfce("11.222.333/0001-81") == "11222333000181", "máscara CNPJ")
    check(tipo_documento_dest_nfce("11222333000181") == "CNPJ", "tipo CNPJ")
    check(documento_dest_nfce("11111111111") == "", "CPF repetido rejeitado")
    check(documento_dest_nfce("00000000000000") == "", "CNPJ zero rejeitado")

    inf = ET.Element(f"{{{NS}}}infNFe")
    _preencher_dest_nfce(inf, "52998224725")
    dest = inf.find(f"{{{NS}}}dest")
    check(dest is not None and dest.findtext(f"{{{NS}}}CPF") == "52998224725", "XML dest/CPF")
    check(dest is not None and dest.find(f"{{{NS}}}CNPJ") is None, "XML CPF sem tag CNPJ")
    check(dest is not None and dest.findtext(f"{{{NS}}}indIEDest") == "9", "indIEDest=9 no CPF")

    class V:
        cliente_nome = "PADARIA TESTE LTDA"

    inf2 = ET.Element(f"{{{NS}}}infNFe")
    _preencher_dest_nfce(inf2, "11222333000181", V())
    dest2 = inf2.find(f"{{{NS}}}dest")
    check(dest2 is not None and dest2.findtext(f"{{{NS}}}CNPJ") == "11222333000181", "XML dest/CNPJ")
    check(dest2 is not None and dest2.find(f"{{{NS}}}CPF") is None, "XML CNPJ sem tag CPF")
    check(dest2 is not None and dest2.findtext(f"{{{NS}}}xNome") == "PADARIA TESTE LTDA", "xNome no CNPJ")
    check(dest2 is not None and dest2.findtext(f"{{{NS}}}indIEDest") == "9", "indIEDest=9 no CNPJ")

    doc, sem = _nfce_opts_payload({"nfce_cpf": "11.222.333/0001-81"})
    check(doc == "11222333000181" and not sem, "payload nfce_cpf CNPJ")
    doc, sem = _nfce_opts_payload({"cliente_documento": "52998224725"})
    check(doc == "52998224725" and not sem, "payload cliente_documento CPF")
    doc, sem = _nfce_opts_payload({"nfce_sem_identificacao": True})
    check(doc == "" and sem, "payload sem identificação")

    field = NfceDocumentoAgro._meta.get_field("dest_cpf")
    check(int(field.max_length) >= 14, f"dest_cpf max_length={field.max_length} (>=14)")

    if FAILS:
        print(f"FALHOU {len(FAILS)}: " + " | ".join(FAILS))
        return 1
    print(f"OK {CHECKS} checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
