#!/usr/bin/env python3
"""Prova do path CAD-PRECO-CENTAVOS.

Simula: digita 82,90 -> blur -> troca aba -> payload -> backend -> PDV.
Cruza fonte do modal + Node (JS real) + espelho Python + util Django.
"""
from __future__ import annotations

import math
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODAL = ROOT / "produtos" / "templates" / "produtos" / "_modal_editar_produto_cadastro_erp.inc.html"
JS_PDV = ROOT / "produtos" / "static" / "produtos" / "js" / "precos_forma_pagamento.js"
VIEWS = ROOT / "produtos" / "views.py"
NODE_JS = ROOT / "scripts" / "verify_cadastro_preco_centavos.js"

PASS = 0
FAIL = 0


def check(ok: bool, msg: str) -> None:
    global PASS, FAIL
    safe = msg.encode("ascii", "replace").decode("ascii")
    if ok:
        PASS += 1
        print(f"  OK  {safe}")
    else:
        FAIL += 1
        print(f" FAIL {safe}")


def parse_moeda_espelho(s):
    if s is None or s == "":
        return 0.0
    if isinstance(s, bool):
        return 0.0
    if isinstance(s, (int, float)):
        return float(s) if math.isfinite(s) else 0.0
    t = str(s).replace(" ", "").replace("\u00a0", "")
    if not t:
        return 0.0
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    try:
        n = float(t)
    except ValueError:
        return 0.0
    return n if math.isfinite(n) else 0.0


def parse_moeda_antigo(s):
    t = str(s).replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return 0.0


def fmt_moeda2(n: float) -> str:
    x = round(float(n) + 1e-12, 2)
    sinal = "-" if x < 0 else ""
    x = abs(x)
    inteiro = int(x)
    cents = int(round((x - inteiro) * 100))
    if cents == 100:
        inteiro += 1
        cents = 0
    return f"{sinal}{inteiro},{cents:02d}"


def blur_forma(mapa: dict, forma: str, raw: str) -> str:
    t = (raw or "").strip()
    if not t:
        mapa.pop(forma, None)
        return ""
    n = parse_moeda_espelho(t)
    if n > 0:
        mapa[forma] = n
        return fmt_moeda2(n)
    mapa.pop(forma, None)
    return ""


def payload_por_forma(mapa: dict) -> dict:
    out = {}
    for k, v in mapa.items():
        n = parse_moeda_espelho(v)
        if n > 0:
            out[k] = n
    return out


def main() -> int:
    html = MODAL.read_text(encoding="utf-8")
    pdv = JS_PDV.read_text(encoding="utf-8")
    views = VIEWS.read_text(encoding="utf-8")

    print("== Fonte ==")
    check(MODAL.is_file(), "modal de cadastro existe")
    check("function _parseMoedaTexto" in html, "helper _parseMoedaTexto")
    check("typeof s === 'number'" in html, "parseMoeda protege numero JS")
    check("t.indexOf(',') >= 0" in html, "ponto de milhar so com virgula")
    check(
        ".replace(/\\s/g, '').replace(/\\./g, '').replace(',', '.')" not in html,
        "parser antigo saiu do modal",
    )
    check("commitPrecoGrupoCampo" in html, "grupo A/B formata no blur")
    check("agroCadastroObterPrecosPorFormaPayload" in html, "payload por forma")
    check("agroCadastroObterPrecosGruposPayload" in html, "payload grupos")
    check("parseMoeda(map[k])" in html, "salvar reparseia mapa com parseMoeda")
    check("parseMoeda(g.preco_a)" in html, "salvar grupos reparseia preco_a")
    check("normalizar_precos_por_forma_payload" in views, "views grava via normalizar por forma")
    check("normalizar_precos_grupos_payload" in views, "views grava via normalizar grupos")
    check("function toNum(v, fb)" in pdv, "PDV toNum existe")
    check("precoBaseForma" in pdv, "PDV precoBaseForma existe")

    blob = re.search(
        r"function _parseMoedaTexto\(t\) \{.*?function parseMoedaStrict\(s\) \{.*?\n  \}",
        html,
        re.S,
    )
    check(blob is not None, "bloco parseMoeda extraivel")
    if blob:
        body = blob.group(0)
        check("typeof s === 'number'" in body, "guarda de numero no bloco")
        check("indexOf(',') >= 0" in body, "virgula decide milhar no bloco")

    print("== Parser (espelho) ==")
    check(abs(parse_moeda_antigo(82.9) - 829.0) < 1e-9, "bug antigo: 82.9 numero = 829")
    check(abs(parse_moeda_espelho(82.9) - 82.9) < 1e-9, "fix: 82.9 numero = 82.9")
    check(abs(parse_moeda_espelho("82,90") - 82.9) < 1e-9, "'82,90' = 82.9")
    check(abs(parse_moeda_espelho("82.90") - 82.9) < 1e-9, "'82.90' = 82.9")
    check(abs(parse_moeda_espelho("82,10") - 82.1) < 1e-9, "'82,10' = 82.1 (nao 821)")
    check(abs(parse_moeda_espelho(82.1) - 82.1) < 1e-9, "numero 82.1 = 82.1")
    check(abs(parse_moeda_espelho("1.234,56") - 1234.56) < 1e-9, "'1.234,56' = 1234.56")
    check(parse_moeda_espelho("") == 0.0, "vazio = 0")
    check(abs(parse_moeda_espelho(92) - 92.0) < 1e-9, "inteiro 92")
    check(fmt_moeda2(82.9) == "82,90", "fmt 82.9 -> 82,90")
    check(fmt_moeda2(829) == "829,00", "fmt 829 -> 829,00")
    check(fmt_moeda2(parse_moeda_antigo(82.9)) == "829,00", "bug antigo na tela: 829,00")

    print("== Path Por forma ==")
    mapa = {}
    tela = blur_forma(mapa, "PIX", "82,90")
    check(tela == "82,90", "1 digitou 82,90 / blur: tela 82,90")
    check(abs(mapa["PIX"] - 82.9) < 1e-9, "1 estado PIX = 82.9")
    check(fmt_moeda2(parse_moeda_espelho(mapa["PIX"])) == "82,90", "2 troca aba: continua 82,90")
    payload = payload_por_forma(mapa)
    check(abs(payload["PIX"] - 82.9) < 1e-9, "3 payload PIX = 82.9")
    check(abs(payload["PIX"] - 829) > 1, "3 payload nao e 829")
    check(fmt_moeda2(parse_moeda_espelho(payload["PIX"])) == "82,90", "4 reabrir: PIX 82,90")

    tela_deb = blur_forma(mapa, "Cartao de debito", "87,00")
    check(tela_deb == "87,00", "debito 87,00 intacto")
    check(fmt_moeda2(parse_moeda_espelho(mapa["PIX"])) == "82,90", "PIX nao muda ao editar debito")

    print("== Path 2 grupos ==")
    ga = parse_moeda_espelho("82,90")
    gb = parse_moeda_espelho("92,00")
    check(fmt_moeda2(ga) == "82,90", "grupo A blur tela 82,90")
    grupos = {"preco_a": ga, "preco_b": gb, "formas_a": ["PIX"], "formas_b": ["Fiado"]}
    pa = parse_moeda_espelho(grupos["preco_a"])
    pb = parse_moeda_espelho(grupos["preco_b"])
    check(abs(pa - 82.9) < 1e-9, "payload grupo A = 82.9")
    check(abs(pb - 92.0) < 1e-9, "payload grupo B = 92")
    check(fmt_moeda2(parse_moeda_espelho(pa)) == "82,90", "reabrir grupo A: 82,90")

    print("== Backend + PDV ==")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    sys.path.insert(0, str(ROOT))
    backend_ok = False
    try:
        import django

        django.setup()
        from produtos.precos_forma_pagamento_util import (
            normalizar_precos_grupos_payload,
            normalizar_precos_por_forma_payload,
            preco_venda_para_forma,
        )

        backend_ok = True
        out = normalizar_precos_por_forma_payload({"PIX": payload["PIX"]})
        check(abs(out.get("PIX", 0) - 82.9) < 1e-9, "backend PIX 82.9")
        pdv_pix = preco_venda_para_forma(99.0, out, "PIX")
        check(abs(pdv_pix - 82.9) < 1e-9, "PDV PIX usa 82.90 (nao 99 nem 829)")
        pdv_din = preco_venda_para_forma(99.0, out, "Dinheiro")
        check(abs(pdv_din - 99.0) < 1e-9, "PDV Dinheiro sem tabela usa venda 99")
        gnorm = normalizar_precos_grupos_payload(
            {"preco_a": pa, "preco_b": pb, "formas_a": ["PIX"], "formas_b": ["Fiado"]}
        )
        check(gnorm is not None, "backend grupos aceita 82.9")
        check(abs(gnorm["preco_a"] - 82.9) < 1e-9, "backend grupo A 82.9")
        pdv_g = preco_venda_para_forma(
            99.0, None, "PIX", precos_modo="grupos", precos_grupos=gnorm
        )
        check(abs(pdv_g - 82.9) < 1e-9, "PDV grupos PIX = preco A 82.90")
        pdv_fiado = preco_venda_para_forma(
            99.0, None, "Fiado", precos_modo="grupos", precos_grupos=gnorm
        )
        check(abs(pdv_fiado - 92.0) < 1e-9, "PDV grupos Fiado = preco B 92")
    except Exception as exc:
        check(False, "backend/PDV nao rodou: " + str(exc)[:80])
    check(backend_ok, "Django setup backend")

    print("== Node JS real ==")
    node = subprocess.run(
        ["node", str(NODE_JS)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    check(node.returncode == 0, "node verify_cadastro_preco_centavos.js exit 0")
    if node.returncode != 0:
        err = (node.stdout or "") + (node.stderr or "")
        print(err[-800:])
    else:
        n_ok = len(re.findall(r"^\s+OK  ", node.stdout or "", re.M))
        check(n_ok >= 20, f"node {n_ok} asserts (>=20)")

    print(f"\nRESULTADO {PASS} ok / {FAIL} falha")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
