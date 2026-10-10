# -*- coding: utf-8 -*-
"""
Prova — busca do PDV não mistura palavra solta (`PDV-BUSCA-COERENTE`).

Acerto = verde. Chute = cinza. Cursor = azul.
«milho grande» não traz bebedouro. Número «25» não casa dentro de «125».

  python scripts/verify_pdv_busca_coerente_path.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

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


def test_arquivos() -> None:
    print("== Contratos UI / motor ==")
    html = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    motor = (ROOT / "produtos/motor_busca_unificado_util.py").read_text(encoding="utf-8")
    cat = (ROOT / "produtos/catalogo_agro.py").read_text(encoding="utf-8")

    check("acerto_verde", "background: #dcfce7" in html)
    row_cor = html.split("background: #dcfce7;", 1)[0][-400:]
    check("acerto_nao_azul_lista", "#bfdbfe" not in row_cor and "#93c5fd" not in row_cor)
    sel = html.split(".pdv-ac-row.pdv-ac-row-selected,", 1)[-1].split("}", 1)[0]
    check("cursor_azul", "#93c5fd" in sel)
    check("cursor_chute_azul", "pdv-ac-row--chute.pdv-ac-row-selected" in html)
    chute = html.split(".pdv-ac-row.pdv-ac-row--chute {", 1)[-1].split("}", 1)[0]
    check("chute_cinza", "#e7edf4" in chute and "#93c5fd" not in chute)
    check("js_refina", "function refinarListaBuscaPdv" in js and "refinarListaBuscaPdv(" in js)
    check("js_grande_fraco", "grande: 1" in js)
    check("js_numero_inteiro", "(?:^|[^0-9])" in js)
    check("motor_fallback", "token_fallback_frase" in motor and "token_fallback_frase" in cat)
    check("sem_token_mais_longo", "max(partes, key=len)" not in motor and "max(partes_fb, key=len)" not in cat)


def test_fallback_py() -> None:
    print("== Fallback do servidor ==")
    import django

    django.setup()
    from produtos.busca_filtro_pdv_util import token_busca_fraco, token_fallback_frase

    check("grande_fraco", token_busca_fraco("grande") is True)
    check("milho_forte", token_busca_fraco("milho") is False)
    check("25_fraco", token_busca_fraco("25") is True)
    check("fb_milho", token_fallback_frase("milho grande") == "milho")
    check("fb_estima", token_fallback_frase("racao estima carne") == "estima")
    check("fb_bebedouro", token_fallback_frase("bebedouro grande") == "bebedouro")
    check("fb_ibiuna", token_fallback_frase("ibiuna 25") is None)
    check("fb_so_fraco", token_fallback_frase("grande azul") == "grande")


def test_js() -> None:
    print("== Lista local (casos da loja) ==")
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    start = js.index("var PDV_BUSCA_TOKEN_FRACO")
    end = js.index("function filterCatalogLocal")
    body = js[start:end]
    harness = r"""
function stripAccents(s) {
  return String(s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}
function onlyDigits(s) { return String(s || '').replace(/\D/g, ''); }
function looksLikeSkuCode(q) {
  var s = String(q || '').trim();
  if (!s || /\s/.test(s)) return false;
  if (/^\d{6,}$/.test(onlyDigits(s))) return true;
  return /^[a-zA-Z]{1,6}[\w.\-]*\d/i.test(s);
}
""" + body + r"""
const catalogo = [
  { nome: 'milho grande 47 kg', marca: 'PILAR' },
  { nome: 'milho 1kg', marca: '' },
  { nome: 'milho 10kg', marca: '' },
  { nome: 'milho 125g', marca: '' },
  { nome: 'bebedouro grande porte 10l', marca: 'MENPLAST' },
  { nome: 'ibiuna crescimento e engorda 25kg', marca: 'IBIUNA' },
  { nome: 'ibiuna inicial 25 kg', marca: 'IBIUNA' },
  { nome: 'ibiuna cres/eng.5kg', marca: 'IBIUNA' },
  { nome: 'ibiuna crescimento e engorda 1kg', marca: 'IBIUNA' },
  { nome: 'vassoura palha de milho com cabo', marca: '' },
  { nome: 'ração estima carne 15kg', marca: 'ESTIMA' },
];
function nomes(q) {
  return refinarListaBuscaPdv(catalogo, q).map(function (p) {
    return { nome: p.nome, chute: produtoBuscaEhChute(p, q) };
  });
}
const out = {
  milhoGrande: nomes('milho grande'),
  ibiuna25: nomes('ibiuna 25'),
  milho: nomes('milho'),
  gm: nomes('GM1541-25'),
  bebedouro: nomes('bebedouro azul'),
};
process.stdout.write(JSON.stringify(out));
"""
    r = subprocess.run(
        ["node", "-e", harness],
        capture_output=True,
        text=True,
        timeout=20,
        cwd=str(ROOT),
    )
    check("node_ok", r.returncode == 0, (r.stderr or "")[:180])
    if r.returncode != 0:
        return
    data = json.loads(r.stdout)
    mg = data["milhoGrande"]
    check("mg_sem_bebedouro", all("bebedouro" not in x["nome"] for x in mg), str([x["nome"] for x in mg]))
    check("mg_primeiro_exato", mg and mg[0]["nome"].startswith("milho grande") and mg[0]["chute"] is False)
    check("mg_outro_milho_chute", any(x["nome"] == "milho 1kg" and x["chute"] for x in mg))
    check("mg_125_chute", any(x["nome"] == "milho 125g" and x["chute"] for x in mg))
    ib = data["ibiuna25"]
    check("ib_25_exato", all(not x["chute"] for x in ib if "25" in x["nome"]))
    check("ib_5_chute", any("5kg" in x["nome"] and x["chute"] for x in ib))
    check("ib_1kg_chute", any(x["nome"].endswith("1kg") and x["chute"] for x in ib))
    check("ib_sem_milho", all("milho" not in x["nome"] for x in ib))
    check("uma_palavra_sem_chute", data["milho"] and all(not x["chute"] for x in data["milho"]))
    check("gm_nao_filtra", len(data["gm"]) == catalogo_len())
    bb = data["bebedouro"]
    check("bb_so_bebedouro", bb and all("bebedouro" in x["nome"] for x in bb), str(bb))


def catalogo_len() -> int:
    return 11


def main() -> int:
    print("verify_pdv_busca_coerente_path")
    test_arquivos()
    test_fallback_py()
    test_js()
    print()
    if fails:
        print(f"FALHOU: {len(fails)} falha(s), {len(oks)} ok")
        for name in fails:
            print(f"  - {name}")
        return 1
    print(f"OK verify_pdv_busca_coerente_path — {len(oks)}/{len(oks)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
