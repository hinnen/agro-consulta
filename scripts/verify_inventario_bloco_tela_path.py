#!/usr/bin/env python3
"""Path — amostra só no PC, bloco fechado no celular, achado de novo soma."""
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
    print("VERIFY INVENTARIO-BLOCO-TELA")
    util = read("produtos/contagem_ciclica_util.py")
    views = read("produtos/contagem_ciclica_views.py")
    admin = read("produtos/contagem_ciclica_admin_util.py")
    painel = read("produtos/templates/produtos/contagem_ciclica_painel_admin.html")
    ma = read("produtos/templates/produtos/mobile_ajuste.html")

    if "linha._reaberto_bloco = True" in util and "linha.ajuste_id = None" in util:
        ok("reabre bloco e zera o vínculo")
    else:
        fail("reabrir bloco")
    if "q_final = (anterior + q_contada)" in util:
        ok("achado de novo soma no saldo atual")
    else:
        fail("soma 5+2")
    if '"bloco_fechado": bool(ln.ajuste_id)' in util and "contados_bloco_atual" in util:
        ok("payload bloco fechado")
    else:
        fail("payload")
    if '"blocos": blocos' in admin and "ContagemCiclicaBloco" in admin:
        ok("painel lista blocos")
    else:
        fail("painel blocos")
    if "reaberto_bloco" in views and "detalhe=True" in views:
        ok("api conta reaberto e devolve detalhe")
    else:
        fail("api")
    if "Amostras para imprimir" in painel and "cc-blocos" in painel:
        ok("impressão no painel do PC")
    else:
        fail("painel html")
    if "Bloco já fechado" in ma and "p._blocoFechado" in ma:
        ok("celular marca fechado")
    else:
        fail("celular")
    if "/contagem-ciclica/bloco/" not in ma:
        ok("celular não abre a folha")
    else:
        fail("celular abre folha")
    ver = read("VERSION").strip()
    if ver == "27.26.37":
        ok(f"VERSION {ver}")
    else:
        fail(f"VERSION {ver}")

    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {oks}/{oks + len(fails)}")
    if not fails:
        print("VERIFY_OK")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
