"""Verificação path NFC-e Centro × Vila (sem chamar SEFAZ).

Uso: python scripts/verify_nfce_vila_path.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.models import NfceDocumentoAgro, NfceNumeracaoAgro, VendaAgro
from produtos.nfce_config_util import (
    nfce_cfg,
    nfce_cnpj_da_chave,
    nfce_configurada,
    nfce_loja_de_cnpj,
    nfce_loja_de_venda,
)
from produtos.nfce_cupom_util import serializar_nfce_cupom_80mm

CHECKS = 0


def ok(msg: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"  OK  {msg}")


def main() -> int:
    print("=== NFCE-VILA-EMIT path ===")
    assert nfce_configurada(loja="centro"), "centro não configurada"
    assert nfce_configurada(loja="vila"), "vila não configurada"
    ok("configurada centro+vila")

    cc, cv = nfce_cfg("centro"), nfce_cfg("vila")
    assert cc["cnpj"] == "48900774000103"
    assert cv["cnpj"] == "48900774000286"
    assert cv["ie"] == "394051450113"
    assert "Joaquim" in cv["logradouro"]
    assert cc["cert_path"] == cv["cert_path"]
    assert cc["csc_token"] == cv["csc_token"]
    ok("CNPJ/IE/endereço + cert/CSC compartilhados")

    class Fake:
        deposito = "vila"
        sessao_caixa = None

    assert nfce_loja_de_venda(Fake()) == "vila"
    Fake.deposito = "centro"
    assert nfce_loja_de_venda(Fake()) == "centro"
    ok("loja_de_venda por depósito")

    assert NfceNumeracaoAgro.objects.filter(emitente_cnpj="48900774000286").exists()
    ok("numeração Vila no PG")

    doc = (
        NfceDocumentoAgro.objects.filter(
            status=NfceDocumentoAgro.Status.AUTORIZADA,
            emitente_cnpj="48900774000286",
        )
        .select_related("venda")
        .order_by("-pk")
        .first()
    )
    if not doc:
        print("  SKIP prova SEFAZ local (sem NFC-e Vila autorizada neste PG)")
    else:
        v = doc.venda
        assert nfce_loja_de_venda(v) == "vila"
        assert nfce_cnpj_da_chave(doc.chave) == "48900774000286"
        assert nfce_loja_de_cnpj(doc.emitente_cnpj) == "vila"
        cup = serializar_nfce_cupom_80mm(v, doc)
        assert "0002-86" in cup["emitente_cnpj"]
        ok(f"prova SEFAZ venda #{v.pk} chave/cupom Vila (nº {doc.numero})")

    print(f"VERIFY_OK ({CHECKS})")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"VERIFY_FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
