#!/usr/bin/env python
"""Path — Feito na lista de códigos pendentes grava opcional no cadastro."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def check_text(text: str, *needles: str, label: str = "") -> None:
    for n in needles:
        if n not in text:
            fail(f"{label} missing {n!r}")
        else:
            ok(f"{label} {n!r}")


api = (ROOT / "produtos" / "ajuste_codigo_pendente_views.py").read_text(encoding="utf-8")
tpl = (
    ROOT / "produtos" / "templates" / "produtos" / "ajuste_codigos_pendentes_lista.html"
).read_text(encoding="utf-8")

check_text(
    api,
    "def aplicar_codigo_pendente_no_cadastro",
    "mesclar_codigos_barras_opcionais_adicionar",
    "STATUS_FEITO",
    "cadastro_info = aplicar_codigo_pendente_no_cadastro",
    'payload["cadastro"]',
    "pelo menos 8 dígitos",
    label="api",
)

# Feito deve aplicar ANTES de salvar status
feito_block = api[
    api.find("if st == AjusteCodigoPendenteAgro.STATUS_FEITO") : api.find(
        "obj.status = st"
    )
]
if "aplicar_codigo_pendente_no_cadastro(obj)" not in feito_block:
    fail("STATUS_FEITO não chama aplicar antes de salvar")
else:
    ok("STATUS_FEITO aplica no cadastro antes de salvar")
if "status=400" not in feito_block:
    fail("falha ao gravar não retorna 400")
else:
    ok("falha ao gravar -> 400 (nao marca feito)")

check_text(
    tpl,
    "grava o bipado no cadastro como código opcional",
    'data-st="feito"',
    "Gravar no cadastro",
    "data-remove=\"0\"",
    "d.cadastro",
    label="tpl",
)

print()
print(f"{oks} OK · {len(fails)} FAIL")
raise SystemExit(1 if fails else 0)
