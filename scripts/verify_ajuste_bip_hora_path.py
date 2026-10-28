#!/usr/bin/env python3
"""Path — primeiro horário do bip e correção de bloco já fechado."""
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
    print("VERIFY AJUSTE-BIP-HORA")
    util = read("produtos/contagem_ciclica_util.py")
    views = read("produtos/contagem_ciclica_views.py")
    ma = read("produtos/templates/produtos/mobile_ajuste.html")

    if "def _corrigir_saldo_bloco_fechado" in util and "inventário · correção" in util:
        ok("corrige bloco fechado")
    else:
        fail("correção")
    if "origem" in util and "CONTAGEM_CICLICA" in util[util.find("def delta_movimento_apos") : util.find("def delta_movimento_apos") + 1800]:
        ok("fechamento do bloco não conta como venda")
    else:
        fail("delta ignora bloco")
    if "linha._ja_tinha = True" in util and "linha.contado_pass1_em = now" not in util[util.find("linha._ja_tinha = True") - 200 : util.find("linha._ja_tinha = True") + 80]:
        ok("segundo bip não troca o horário")
    else:
        fail("horário")
    if '"ja_tinha"' in views and '"qtd_antes"' in views:
        ok("api avisa que já tinha")
    else:
        fail("api")
    if "maQtdAoBipar" in ma and "Já contado:" in ma and "Corrige o bloco já gravado" in ma:
        ok("tela avisa soma e correção")
    else:
        fail("tela")
    ver = read("VERSION").strip()
    if ver == "27.26.42":
        ok(f"VERSION {ver}")
    else:
        fail(f"VERSION {ver}")

    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {oks}/{oks + len(fails)}")
    if not fails:
        print("VERIFY_OK")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
