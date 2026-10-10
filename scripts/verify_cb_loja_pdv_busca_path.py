"""
PDV / api/buscar — bip EAN 230 válido não deve ganhar por index_codigos stale.
python scripts/verify_cb_loja_pdv_busca_path.py
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
    busca = read("produtos/busca_filtro_pdv_util.py")
    views = read("produtos/views.py")
    js = read("produtos/templates/produtos/_js_busca_produto_inteligente.html")
    wiz = read("produtos/static/produtos/js/pdv_wizard.js")

    ok("termo_eh_ean_loja_bip_valido" in busca, "helper EAN loja no filtro PDV")
    ok("produto_termo_bate_campos_principais" in busca, "filtro usa campos raiz")
    ok((ROOT / "produtos/bca_busca_cache_util.py").is_file(), "util cache BCA")
    ok("BCA_BUSCA_CACHE_VERSION" in read("produtos/bca_busca_cache_util.py"), "versão cache")
    ok("index_codigos_de_campos" in views and "termo_eh_ean_loja_bip_valido(q)" in views, "API rebuild index + filtro bip")
    ok("termoEhEanLojaBipValido" in js and "2100000" in js, "JS relevancia EAN loja")
    ok("termoEhEanLojaBipPdv" in wiz, "wizard não match index em 230 DV ok")

    import django

    django.setup()

    from produtos.busca_filtro_pdv_util import (
        filtrar_documentos_estilo_pdv,
        score_relevancia_doc,
        termo_eh_ean_loja_bip_valido,
    )

    bip = "2300000001556"
    ok(termo_eh_ean_loja_bip_valido(bip), "1556 DV ok")
    errado = {
        "CodigoNFe": "GM4241",
        "CodigoBarras": "2300015721739",
        "index_codigos": [bip],
    }
    certo = {"CodigoNFe": "GM0024-P", "CodigoBarras": bip}
    ok(score_relevancia_doc(certo, bip) > score_relevancia_doc(errado, bip), "score API PDV")
    ok(len(filtrar_documentos_estilo_pdv([errado, certo], bip)) == 1, "filtro PDV 1 hit")

    print(f"OK {n - fail}/{n} · PREP_FAILS={fail}")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
