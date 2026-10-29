# -*- coding: utf-8 -*-
"""
Prova — nome do produto no carrinho do PDV em até 2 linhas (`PDV-CART-NOME-2L`).

Nome curto continua numa linha. Nome longo usa duas e, se ainda não couber,
o texto inteiro fica no title (passar o mouse). GM, quantidade e preço não mudam.

  python scripts/verify_pdv_cart_nome_2l_path.py
"""
from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HTML = ROOT / "produtos/templates/produtos/pdv_wizard.html"
JS = ROOT / "produtos/static/produtos/js/pdv_wizard.js"

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _block(text: str, start: str, end: str = "}") -> str:
    i = text.find(start)
    if i < 0:
        return ""
    j = text.find(end, i)
    if j < 0:
        return ""
    return text[i : j + 1]


def test_contrato() -> None:
    print("== Contrato CSS / JS ==")
    html = HTML.read_text(encoding="utf-8")
    js = JS.read_text(encoding="utf-8")
    nome = _block(html, ".pdv-cart-nome {")
    check("css_existe", bool(nome))
    check("duas_linhas", "-webkit-line-clamp: 2" in nome and "line-clamp: 2" in nome)
    check("quebra_normal", "white-space: normal" in nome and "white-space: nowrap" not in nome)
    check("corta_o_resto", "overflow: hidden" in nome)
    check("nao_estica_palavra", "overflow-wrap: break-word" in nome)
    compact = html.split(".pdv-cart-nome { font-size: 16.1px !important; }", 1)
    check("tela_baixa_so_fonte", len(compact) == 2 and "nowrap" not in compact[0][-80:])
    check(
        "js_title",
        'class="pdv-cart-nome" title="' in js and "escapeHtml(item.nome)" in js,
    )
    check("js_uma_vez", js.count('class="pdv-cart-nome"') == 1)
    check("gm_continua", 'class="pdv-cart-gm"' in js and "data-item-qty-input" in js)
    check("preco_continua", "data-item-price-input" in js and "data-remove-item" in js)
    mix = _block(html, ".pdv-cart-mix-tag {")
    check("selo_mix_no_nome", "pdv-cart-mix-tag" in mix and "renderCartMixNameTag(item)" in js)


def _chrome() -> str | None:
    candidatos = [
        shutil.which("chrome"),
        shutil.which("chrome.exe"),
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    ]
    for c in candidatos:
        if c and Path(c).is_file():
            return c
    return None


def _fixture(style: str) -> str:
    longo = "ração special cat gato castrado ultralife salmao premium 10kg extra grande"
    curto = "milho grande 47 kg"
    gigante = longo + " pacote adulto filhote mix vermelho azul amarelo verde ainda maior"
    aspas = 'ração "premium" <gato> & salmão'
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><style>
{style}
body {{ margin: 0; background: #ecfdf5; font-family: Inter, system-ui, sans-serif; }}
#palco {{ width: 1100px; padding: 8px; }}
</style></head><body>
<div id="palco">
  <div class="pdv-cart-row" id="row-longo">
    <span style="width:3rem;height:3rem;display:block"></span>
    <div class="pdv-cart-line overflow-hidden"><span class="pdv-cart-nome" id="longo" title="{longo}">{longo}</span></div>
    <div class="pdv-cart-row-tools">
      <span class="pdv-cart-gm">GM0004-1</span>
      <span class="pdv-cart-promo-wrap pdv-cart-promo--empty"></span>
      <div class="pdv-cart-qty-wrap"><button type="button">−</button><input class="pdv-cart-qty-input" value="1"><button type="button">+</button></div>
      <span class="pdv-cart-grupos-hint"></span>
      <div class="pdv-cart-price-wrap"><div class="pdv-cart-price-box"><span class="pdv-cart-price-prefix">R$</span><input class="pdv-cart-price-input" value="18,40"></div></div>
      <div class="pdv-cart-actions"><button type="button" class="pdv-cart-remove">x</button><button type="button" class="pdv-cart-edit">e</button></div>
    </div>
  </div>
  <div class="pdv-cart-row" id="row-curto">
    <span style="width:3rem;height:3rem;display:block"></span>
    <div class="pdv-cart-line overflow-hidden"><span class="pdv-cart-nome" id="curto" title="{curto}">{curto}</span></div>
    <div class="pdv-cart-row-tools"><span class="pdv-cart-gm">GM0090-47</span></div>
  </div>
  <div class="pdv-cart-row" id="row-gigante">
    <span style="width:3rem;height:3rem;display:block"></span>
    <div class="pdv-cart-line overflow-hidden"><span class="pdv-cart-nome" id="gigante" title="{gigante}">{gigante}</span></div>
    <div class="pdv-cart-row-tools"><span class="pdv-cart-gm">GM0004-1</span></div>
  </div>
  <div class="pdv-cart-row pdv-cart-row--mix pdv-cart-row--mix-0" id="row-mix">
    <span style="width:3rem;height:3rem;display:block"></span>
    <div class="pdv-cart-line overflow-hidden"><span class="pdv-cart-nome" id="mix"><span class="pdv-cart-mix-tag">MIX</span>{longo}</span></div>
    <div class="pdv-cart-row-tools"><span class="pdv-cart-gm">GM0004-1</span></div>
  </div>
</div>
<pre id="out"></pre>
<script>
function linhas(el) {{
  var cs = getComputedStyle(el);
  var lh = parseFloat(cs.lineHeight);
  if (!lh || lh < 8) lh = parseFloat(cs.fontSize) * 1.2;
  return Math.round(el.getBoundingClientRect().height / lh);
}}
function pacote(id) {{
  var el = document.getElementById(id);
  var cs = getComputedStyle(el);
  return {{
    id: id,
    linhas: linhas(el),
    clamp: cs.webkitLineClamp || cs.lineClamp || "",
    nowrap: cs.whiteSpace === "nowrap",
    overflow: cs.overflow,
    cortou: el.scrollHeight > el.clientHeight + 2,
    title: el.getAttribute("title") || "",
    texto: (el.textContent || "").trim()
  }};
}}
var row = document.getElementById("row-longo");
var gm = document.querySelector("#row-longo .pdv-cart-gm");
var qty = document.querySelector("#row-longo .pdv-cart-qty-input");
var preco = document.querySelector("#row-longo .pdv-cart-price-input");
var lixo = document.querySelector("#row-longo .pdv-cart-remove");
document.getElementById("out").textContent = JSON.stringify({{
  longo: pacote("longo"),
  curto: pacote("curto"),
  gigante: pacote("gigante"),
  mix: pacote("mix"),
  mixVisivel: (document.querySelector("#mix .pdv-cart-mix-tag") || {{}}).textContent || "",
  gm: getComputedStyle(gm).whiteSpace,
  qty: qty.value,
  preco: preco.value,
  lixo: !!lixo,
  colunas: getComputedStyle(row).gridTemplateColumns.split(" ").length
}});
</script>
</body></html>
"""


def test_tela(chrome: str) -> None:
    print("== Tela (Chrome) ==")
    html = HTML.read_text(encoding="utf-8")
    style_i = html.find("<style>")
    style_j = html.find("</style>", style_i)
    style = html[style_i + len("<style>") : style_j]
    # marca aspas só no contrato JS; a ficha da tela usa nomes comuns da loja
    page = _fixture(style)
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "cart.html"
        path.write_text(page, encoding="utf-8")
        url = path.as_uri()
        for rotulo, w, h in (("larga", 1600, 1000), ("baixa", 1440, 800)):
            proc = subprocess.run(
                [
                    chrome,
                    "--headless=new",
                    "--disable-gpu",
                    "--no-first-run",
                    f"--window-size={w},{h}",
                    "--virtual-time-budget=3000",
                    "--dump-dom",
                    url,
                ],
                capture_output=True,
                text=True,
                timeout=40,
                encoding="utf-8",
                errors="replace",
            )
            raw = proc.stdout or ""
            i = raw.find('id="out"')
            blob = ""
            if i >= 0:
                a = raw.find(">", i)
                b = raw.find("</pre>", a)
                blob = raw[a + 1 : b].strip()
            data = {}
            try:
                data = json.loads(blob)
            except json.JSONDecodeError:
                data = {}
            check(f"{rotulo}_abriu", bool(data), blob[:180])
            if not data:
                continue
            check(f"{rotulo}_ate_2", data["longo"]["linhas"] <= 2 and data["gigante"]["linhas"] <= 2)
            check(f"{rotulo}_curto_1", data["curto"]["linhas"] == 1, str(data["curto"]["linhas"]))
            check(f"{rotulo}_sem_nowrap", data["longo"]["nowrap"] is False and data["curto"]["nowrap"] is False)
            check(f"{rotulo}_title", data["longo"]["title"].startswith("ração special"))
            check(f"{rotulo}_mix_ate_2", data["mix"]["linhas"] <= 2, str(data["mix"]["linhas"]))
            check(f"{rotulo}_mix_selo", data["mixVisivel"].strip() == "MIX")
            if rotulo == "larga":
                check("larga_nome_apertado_2", data["longo"]["linhas"] == 2, str(data["longo"]["linhas"]))
                check("larga_gigante_corta", data["gigante"]["linhas"] == 2 and data["gigante"]["cortou"] is True)
                check("larga_colunas_lado", data["colunas"] == 3, str(data["colunas"]))
                check("gm_uma_linha", data["gm"] == "nowrap")
                check("qtd_continua", data["qty"] == "1")
                check("preco_continua", data["preco"] == "18,40")
                check("lixeira_continua", data["lixo"] is True)
            else:
                check("baixa_colunas_nome_largo", data["colunas"] == 2, str(data["colunas"]))
                check(
                    "baixa_gigante_quebra",
                    data["gigante"]["linhas"] == 2 and data["gigante"]["cortou"] is False,
                    str(data["gigante"]["linhas"]),
                )


def main() -> int:
    test_contrato()
    chrome = _chrome()
    if not chrome:
        check("chrome", False, "Chrome não encontrado")
    else:
        test_tela(chrome)
    print(f"\n{len(oks)} ok / {len(fails)} falhou")
    if fails:
        print("FALHOU: " + ", ".join(fails))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
