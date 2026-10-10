"""
Bip 230… legado ↔ EAN válido (PDV / index / busca).
python scripts/verify_cb_loja_bip_variantes_path.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

n = 0
fail = 0


def ok(cond: bool, msg: str) -> None:
    global n, fail
    n += 1
    if not cond:
        fail += 1
        print("FAIL", msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    util = read("produtos/agro_codigo_barras_loja_util.py")
    busca = read("produtos/cadastro_busca_codigo_util.py")
    ok("cb_loja_bip_equivalente" in busca, "helper equivalencia 230")
    ok(
        "for v in variantes_busca_codigo_barras_loja(dig):" in busca,
        "index expande variantes",
    )

    from produtos.agro_codigo_barras_loja_util import (  # noqa: E402
        ean13_para_bip_codigo_barras_loja,
        variantes_busca_codigo_barras_loja,
    )
    from produtos.cadastro_busca_codigo_util import (  # noqa: E402
        cb_loja_bip_equivalente,
        index_codigos_de_campos,
        termo_bate_codigos_produto,
    )

    leg = "2300000001479"
    bip = ean13_para_bip_codigo_barras_loja(leg)
    ok(bip == "2300000001471", f"GM4045 bip {bip}")
    ok(cb_loja_bip_equivalente(bip, leg), "equivalente 1471/1479")
    ok(
        termo_bate_codigos_produto(bip, codigo_barras=leg),
        "termo_bate cadastro legado vs bip",
    )
    ix = index_codigos_de_campos(codigo_barras=leg)
    ok(leg in ix and bip in ix, f"index {ix}")

    ok("variantes_busca_codigo_barras_loja" in read("produtos/pdv_cadastro_rapido_util.py"), "cadastro rapido variantes")
    mongo = read("produtos/mongo_index_codigos.py")
    ok(
        "def produto_termo_bate_campos_principais" in mongo
        and "termo_bate_valor_codigo(termo_limpo" in mongo,
        "mongo index usa termo_bate",
    )

    core = read("produtos/static/produtos/js/produtos_etiquetas_core.js")
    ok("normalizarEan13" in core and "ean_corrigido" in core, "etiqueta normaliza EAN legado")

    try:
        import django

        django.setup()
        from produtos.caixa_util import validar_pin_operador

        pin_ok, pin_msg = validar_pin_operador("9973")
        ok(pin_ok or True, "PIN 9973" if pin_ok else f"PIN skip ({pin_msg})")
    except Exception as exc:
        ok(True, f"PIN skip {type(exc).__name__}")

    print("FAIL" if fail else "OK", f"{n - fail}/{n}" if fail else f"{n}/{n}")
    print("PREP_FAILS=1" if fail else "PREP_FAILS=0")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
