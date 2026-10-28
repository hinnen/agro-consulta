#!/usr/bin/env python3
"""Path — quantidade no bip (+/− e +10) e alterar o total já contado."""
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


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    print("VERIFY AJUSTE-QTD-BIP")
    util = read("produtos/contagem_ciclica_util.py")
    views = read("produtos/contagem_ciclica_views.py")
    ma = read("produtos/templates/produtos/mobile_ajuste.html")
    sheet = ma[ma.find('id="ma-qtd-sheet"') : ma.find('id="ma-qtd-sheet"') + 1800]

    if "substituir: bool = False" in util and "linha.qtd_pass1 = q" in util:
        ok("substituir grava o total")
    else:
        fail("substituir")
    if "Este item já está num bloco fechado" in util:
        ok("bloco fechado não altera")
    else:
        fail("bloco fechado")
    if "substituir=" in views and '"substituir"' in views:
        ok("api aceita substituir")
    else:
        fail("api")
    for n in ("ma-qtd-mais10", "ma-qtd-menos10", "ma-qtd-modo-alterar", "ma-qtd-modo-somar", "ma-qtd-ok"):
        if n in sheet:
            ok(f"folha {n}")
        else:
            fail(f"folha {n}")
    if "<input" in sheet:
        fail("folha tem campo de teclado")
    else:
        ok("folha sem teclado")
    if "payload.substituir" in ma:
        ok("cliente manda substituir")
    else:
        fail("cliente")
    ver = read("VERSION").strip()
    if ver == "27.26.41":
        ok(f"VERSION {ver}")
    else:
        fail(f"VERSION {ver}")

    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {oks}/{oks + len(fails)}")
    if not fails:
        print("VERIFY_OK")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
