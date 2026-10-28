#!/usr/bin/env python3
"""Path — cancelar sessão aberta no painel da gerência."""
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
    print("VERIFY CONTAGEM-CANCELAR")
    views = read("produtos/contagem_ciclica_admin_views.py")
    urls = read("produtos/urls.py")
    tpl = read("produtos/templates/produtos/contagem_ciclica_painel_admin.html")
    util = read("produtos/contagem_ciclica_util.py")

    if "def api_ciclica_admin_cancelar" in views and "cancelar_sessao(sessao)" in views:
        ok("view cancela")
    else:
        fail("view")
    if "_gate_admin(request)" in views[views.find("def api_ciclica_admin_cancelar") : views.find("def api_ciclica_admin_cancelar") + 400]:
        ok("gate gerência")
    else:
        fail("gate")
    if "api_ciclica_admin_cancelar" in urls and "ciclica/admin/cancelar/" in urls:
        ok("url")
    else:
        fail("url")
    if "Cancelar contagem" in tpl and "cancelarSessao" in tpl and "stopPropagation" in tpl:
        ok("botão no card")
    else:
        fail("botão")
    if "Contagem já gravada" in util and "CANCELADA" in util and "AjusteRapidoEstoque.objects" not in util[util.find("def cancelar_sessao") : util.find("def cancelar_sessao") + 500]:
        ok("não apaga ajuste do bloco")
    else:
        fail("cancelar mexe no estoque")
    ver = read("VERSION").strip()
    if ver == "27.26.40":
        ok(f"VERSION {ver}")
    else:
        fail(f"VERSION {ver}")

    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {oks}/{oks + len(fails)}")
    if not fails:
        print("VERIFY_OK")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
