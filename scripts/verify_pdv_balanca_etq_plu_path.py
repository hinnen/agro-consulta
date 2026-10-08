# -*- coding: utf-8 -*-
"""
Prova — bip etiqueta balança EAN-13 PLU 4 dígitos (`PDV-BALANCA-ETQ-PLU`).

Caso loja: `2001000004812` → PLU `0010` · R$ 4,81 · overlay/código barras SisVale.

  python scripts/verify_pdv_balanca_etq_plu_path.py
"""
from __future__ import annotations

import os
import re
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_contratos_arquivo() -> None:
    print("== Contratos código ==")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/consulta_produtos.js").read_text(encoding="utf-8")

    check("parse_fn", "def _parse_etiqueta_balanca_ean13_br" in views)
    check("buscar_fn", "def _buscar_produto_por_codigo_interno_balanca" in views)
    check("prefer_plu4", "Prefere o PLU de 4 dígitos" in views or "plu4 = cod4.zfill(4)" in views)
    check("overlay_fallback", "_mongo_produtos_por_overlay_codigo_busca" in views)
    check("api_usa_balanca", "bal = _parse_etiqueta_balanca_ean13_br(q)" in views)
    check("exact_flag", '"exact_barcode_match"' in views or "'exact_barcode_match'" in views)
    check("js_parse", "function parseEtiquetaBalancaEan13" in js)
    check("js_local_plu", "encontrarProdutoPorCodigoInternoBalanca" in js)
    check("js_api_etiqueta", "function executarBuscaAPIEtiquetaBalanca" in js)
    check("js_preco_etiqueta", "preco_etiqueta_balanca: true" in js)
    check("js_auditoria", "auditoria_codigo_bip: digits" in js)
    check(
        "js_caminho_scanner",
        "localBal" in js and "executarBuscaAPIEtiquetaBalanca(digits, bal)" in js,
    )


def test_parse_e_busca() -> None:
    print("== Parse + busca (mock) ==")
    import django

    django.setup()
    from produtos.views import (
        _buscar_produto_por_codigo_interno_balanca,
        _ean13_digito_verificador,
        _parse_etiqueta_balanca_ean13_br,
    )

    ean = "2001000004812"
    parsed = _parse_etiqueta_balanca_ean13_br(ean)
    check("parse_ok", parsed is not None)
    if parsed:
        cod4, preco = parsed
        check("plu_0010", cod4 == "0010", repr(cod4))
        check("preco_4_81", preco == Decimal("4.81"), str(preco))
    check("dv", _ean13_digito_verificador("200100000481") == 2)
    check("dv_invalido", _parse_etiqueta_balanca_ean13_br("2001000004810") is None)
    check("nao_13", _parse_etiqueta_balanca_ean13_br("200100000481") is None)
    check("flag_errado", _parse_etiqueta_balanca_ean13_br("1001000004812") is None)

    # index com 0010 exato
    client = MagicMock()
    client.col_p = "DtoProduto"
    col = MagicMock()
    db = {client.col_p: col}

    # Preferência: consulta 0010 antes de variante curta 10
    calls: list = []

    def find_one(q, *a, **k):
        calls.append(q)
        idx = q.get("index_codigos")
        if idx == "0010":
            return {"Id": "pid-racao", "Nome": "Racao", "index_codigos": ["0010", "gm0010-1"]}
        if idx == "10":
            return {"Id": "outro", "Nome": "Errado", "index_codigos": ["10"]}
        return None

    col.find_one.side_effect = find_one
    got = _buscar_produto_por_codigo_interno_balanca(db, client, "0010")
    check("busca_index_0010", got and got.get("Id") == "pid-racao", str(got))
    check(
        "nao_usou_10_primeiro",
        bool(calls) and calls[0].get("index_codigos") == "0010",
        str(calls[:2]),
    )

    # Sem index: overlay único
    col.find_one.side_effect = lambda *a, **k: None
    col.find.return_value.limit.return_value = []
    overlay_doc = {"Id": "pid-ov", "Nome": "Racao overlay"}

    with patch(
        "produtos.views._mongo_produtos_por_overlay_codigo_busca",
        return_value=[overlay_doc],
    ) as m_ov:
        got2 = _buscar_produto_por_codigo_interno_balanca(db, client, "0010")
        check("overlay_unico", got2 and got2.get("Id") == "pid-ov", str(got2))
        check("overlay_chamado", m_ov.called)

    # Overlay ambíguo → None
    with patch(
        "produtos.views._mongo_produtos_por_overlay_codigo_busca",
        side_effect=lambda termo, *a, **k: (
            [{"Id": "a"}, {"Id": "b"}] if termo == "0010" else []
        ),
    ):
        # by_id will get both from first termo
        got3 = _buscar_produto_por_codigo_interno_balanca(db, client, "0010")
        # side_effect returns 2 docs once — function accumulates; len!=1 → None
        check("overlay_ambiguo_none", got3 is None, str(got3))


def test_js_plu_helpers() -> None:
    print("== JS (node) ==")
    js_path = ROOT / "produtos/static/produtos/js/consulta_produtos.js"
    src = js_path.read_text(encoding="utf-8")
    # Extrai funções mínimas para smoke node
    m_dv = re.search(
        r"function digitoVerificadorEan13Primeiros12\(d12\) \{[\s\S]*?\n\}",
        src,
    )
    m_parse = re.search(
        r"function parseEtiquetaBalancaEan13\(digits13\) \{[\s\S]*?\n\}",
        src,
    )
    check("js_extract_dv", bool(m_dv))
    check("js_extract_parse", bool(m_parse))
    if not (m_dv and m_parse):
        return
    snippet = m_dv.group(0) + "\n" + m_parse.group(0) + "\n"
    snippet += """
const bal = parseEtiquetaBalancaEan13('2001000004812');
if (!bal || bal.codigo4 !== '0010' || Math.abs(bal.valorReais - 4.81) > 0.001 || !bal.checkOk) {
  console.error('FAIL js_parse', bal);
  process.exit(1);
}
const bad = parseEtiquetaBalancaEan13('2001000004810');
if (!bad || bad.checkOk) {
  console.error('FAIL js_dv', bad);
  process.exit(1);
}
console.log('OK js_parse_node');
"""
    import subprocess
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(snippet)
        tmp = f.name
    try:
        r = subprocess.run(["node", tmp], capture_output=True, text=True, timeout=10)
        check("js_node_parse", r.returncode == 0, (r.stdout + r.stderr).strip())
    finally:
        Path(tmp).unlink(missing_ok=True)


def main() -> int:
    print("PDV-BALANCA-ETQ-PLU — prova de path\n")
    test_contratos_arquivo()
    test_parse_e_busca()
    test_js_plu_helpers()
    print()
    print(f"OK={len(oks)} FAIL={len(fails)}")
    if fails:
        print("Falhas:", ", ".join(fails))
        return 1
    print("PREP_FAILS=0")
    return 0


if __name__ == "__main__":
    # fix accidental walrus in earlier draft — ensure clean
    raise SystemExit(main())
