# -*- coding: utf-8 -*-
"""Prova: o script da entrada de NF fecha e a lista ainda é chamada."""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HTML = ROOT / "produtos/templates/produtos/entrada_nota.html"


def main() -> int:
    text = HTML.read_text(encoding="utf-8")
    fails = []
    oks = 0

    def check(ok: bool, msg: str) -> None:
        nonlocal oks
        if ok:
            oks += 1
            print("OK", msg)
        else:
            fails.append(msg)
            print("FAIL", msg)

    scripts = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", text, flags=re.S | re.I)
    check(len(scripts) >= 1, "tem script da página")
    big = max(scripts, key=len)
    js = ROOT / "scripts/.entrada_nfe_tela_check.js"
    js.write_text(big, encoding="utf-8")
    try:
        r = subprocess.run(["node", "--check", str(js)], capture_output=True, text=True)
        check(r.returncode == 0, "node --check " + (r.stderr.strip() or "ok"))
    finally:
        js.unlink(missing_ok=True)

    check(text.count("async function entradaNfeConfirmarUnificarForn") == 1, "unificar fornecedor uma vez")
    check(text.count("function entradaNfeGradeEmbColsAplicar") == 1, "embalagem uma vez")
    check(text.count("const ENTRADA_NFE_LS_EMB_COLS") == 1, "chave da embalagem uma vez")
    check("async function carregarRascunhos()" in text, "função da lista existe")
    check("carregarRascunhos();" in text, "lista é chamada ao abrir")
    aplicar = text.find("function entradaNfeGradeEmbColsAplicar")
    unif = text.find("async function entradaNfeConfirmarUnificarForn")
    check(unif > 0 and aplicar > unif, "unificar termina antes da função da embalagem")
    trecho = text[aplicar:aplicar + 900]
    check("if (!o.skipStore)" in trecho, "embalagem grava a preferência")

    print("---")
    print(f"VERIFY_OK {oks}" if not fails else f"VERIFY_FAIL {len(fails)}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
