#!/usr/bin/env python3
"""Path RH-FOLHA-IMPRESSAO-EXCEL — estático, sem banco."""
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
    print("VERIFY RH-FOLHA-IMPRESSAO-EXCEL")
    views = read("rh/views.py")
    urls = read("rh/urls.py")
    exp = read("rh/services/folha_export.py")
    lista = read("rh/templates/rh/fechamentos_lista.html")
    det = read("rh/templates/rh/fechamento_detalhe.html")
    imp = read("rh/templates/rh/fechamento_imprimir.html")

    bloco = views.split("def rh_fechamentos_excel", 1)[1].split("def rh_fechamento_detalhe", 1)[0]
    if "require_GET" in views and ".save(" not in bloco and "recalcular_fechamento" not in bloco:
        ok("excel e impressão não gravam nem recalculam")
    else:
        fail("endpoint grava ou recalcula")
    if "rh_fechamentos_excel" in urls and "rh_fechamento_imprimir" in urls:
        ok("rotas")
    else:
        fail("rotas")
    if (
        "size: A4 portrait" in imp
        and "VIA FUNCIONÁRIO" in views
        and "VIA EMPRESA" in views
        and "print-color-adjust: exact" in imp
    ):
        ok("duas vias na A4 em pé")
    else:
        fail("impressão")
    if "request.GET.urlencode" in lista and "rh_fechamentos_excel" in lista:
        ok("excel usa o filtro da tela")
    else:
        fail("filtro do excel")
    if "Imprimir folha" in det and "PAGAMENTO_SALARIO" not in exp.split("def xlsx_fechamentos", 1)[0]:
        ok("botão na folha e pagamento fora da soma da via")
    else:
        fail("detalhe ou soma")
    if fails:
        print(f"VERIFY_FAIL {len(fails)}")
        return 1
    print(f"VERIFY_OK {oks}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
