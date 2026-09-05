"""VERIFY DSP-PET-BG — pet upload nao usa knock de fundo branco/preto.

Contrato:
- Upload pet: so resizeImageToDataUrl (PNG) — pelo branco (Bernese) nao some.
- Upload ingredientes: continua knockEdgeMatteToDataUrl.
- Simula regressao: peito branco ligado ao contorno + fundo transparente
  → knock come o peito; resize (pipeline pet) preserva.

Run: python scripts/verify_dsp_pet_bg.py
"""
from __future__ import annotations

import re
from pathlib import Path

from PIL import Image, ImageDraw

ROOT = Path(__file__).resolve().parents[1]
STUDIO = ROOT / "produtos/templates/produtos/dispenser_a6_studio.html"
PETS = ROOT / "produtos/static/produtos/dispenser-a6/lib/pets"
CSS = ROOT / "produtos/static/produtos/dispenser-a6/dispenser.css"

PASS = 0
FAIL = 0


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print(f"  OK  {msg}")


def bad(msg: str) -> None:
    global FAIL
    FAIL += 1
    print(f"  FAIL {msg}")


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        bad(msg)


def extract_handler(src: str, start_marker: str, end_marker: str | None = None) -> str:
    i = src.find(start_marker)
    if i < 0:
        return ""
    chunk = src[i : i + 2200]
    if end_marker:
        j = chunk.find(end_marker, len(start_marker))
        if j > 0:
            return chunk[:j]
    return chunk


def is_bg(r: int, g: int, b: int, a: int) -> bool:
    if a < 40:
        return True
    mx = max(r, g, b)
    mn = min(r, g, b)
    if mn >= 228:
        return True
    if mx <= 36:
        return True
    return False


def knock_edge(im: Image.Image, max_side: int = 720) -> Image.Image:
    """Espelho do knockEdgeMatteToDataUrl do studio (contrato DSP-PNG-BG)."""
    im = im.convert("RGBA")
    w, h = im.size
    scale = min(1, max_side / max(w, h))
    cw = max(1, round(w * scale))
    ch = max(1, round(h * scale))
    im = im.resize((cw, ch), Image.Resampling.BILINEAR)
    px = im.load()
    seen = [[False] * cw for _ in range(ch)]
    q: list[tuple[int, int]] = []

    def push(x: int, y: int) -> None:
        if x < 0 or y < 0 or x >= cw or y >= ch:
            return
        if seen[y][x]:
            return
        r, g, b, a = px[x, y]
        if not is_bg(r, g, b, a):
            return
        seen[y][x] = True
        q.append((x, y))

    for x in range(cw):
        push(x, 0)
        push(x, ch - 1)
    for y in range(ch):
        push(0, y)
        push(cw - 1, y)
    while q:
        x, y = q.pop()
        r, g, b, a = px[x, y]
        if a < 40:
            px[x, y] = (r, g, b, 0)
        else:
            mx = max(r, g, b)
            mn = min(r, g, b)
            if mn >= 245 or mx <= 18:
                px[x, y] = (r, g, b, 0)
            elif mn >= 228:
                na = max(0, min(255, (245 - mn) * 12))
                px[x, y] = (r, g, b, na)
            elif mx <= 36:
                na = max(0, min(255, mx * 6))
                px[x, y] = (r, g, b, na)
            else:
                px[x, y] = (r, g, b, 0)
        push(x + 1, y)
        push(x - 1, y)
        push(x, y + 1)
        push(x, y - 1)
    return im


def resize_png(im: Image.Image, max_side: int = 720) -> Image.Image:
    """Espelho do resizeImageToDataUrl PNG (pipeline pet)."""
    im = im.convert("RGBA")
    w, h = im.size
    scale = min(1, max_side / max(w, h))
    cw = max(1, round(w * scale))
    ch = max(1, round(h * scale))
    return im.resize((cw, ch), Image.Resampling.BILINEAR)


def make_bernese_like() -> Image.Image:
    """Silhueta com peito branco ligado ao contorno + fundo transparente.

    Modelo do bug: flood do knock entra pelo peito branco e «come» o interior.
    """
    im = Image.new("RGBA", (400, 400), (0, 0, 0, 0))
    d = ImageDraw.Draw(im)
    # corpo escuro
    d.ellipse((80, 60, 320, 340), fill=(30, 25, 22, 255))
    # peito branco que TOCA a borda inferior da silhueta (liga ao fundo)
    d.ellipse((140, 180, 260, 360), fill=(245, 245, 248, 255))
    # mancha branca lateral (como na foto)
    d.ellipse((250, 140, 310, 220), fill=(240, 238, 235, 255))
    return im


def chest_opaque_count(im: Image.Image) -> int:
    """Pixels quase-brancos opacos na faixa do peito (centro-baixo)."""
    w, h = im.size
    px = im.load()
    n = 0
    y0, y1 = int(h * 0.45), int(h * 0.92)
    x0, x1 = int(w * 0.30), int(w * 0.70)
    for y in range(y0, y1):
        for x in range(x0, x1):
            r, g, b, a = px[x, y]
            if a >= 200 and min(r, g, b) >= 220:
                n += 1
    return n


def test_studio_source() -> None:
    print("\n== Studio HTML (contrato pet vs ing) ==")
    src = STUDIO.read_text(encoding="utf-8")
    check("id=\"dspPetFile\"" in src, "input dspPetFile existe")
    check("+ Adicionar pet" in src, "botao + Adicionar pet")

    pet_block = extract_handler(
        src,
        'document.getElementById("dspPetFile").addEventListener("change"',
        'document.getElementById("dspReset")',
    )
    check(bool(pet_block), "handler dspPetFile localizado")
    check(
        "resizeImageToDataUrl(img, 720" in pet_block,
        "pet upload chama resizeImageToDataUrl(720)",
    )
    check(
        '{ type: "image/png" }' in pet_block or 'type: "image/png"' in pet_block,
        "pet upload salva PNG (transparencia)",
    )
    check(
        "knockWhiteToDataUrl" not in pet_block,
        "pet upload NAO chama knockWhiteToDataUrl",
    )
    check(
        "knockEdgeMatteToDataUrl" not in pet_block,
        "pet upload NAO chama knockEdgeMatteToDataUrl",
    )
    check(
        "cloudPushMidia(\"pet\"" in pet_block,
        "pet upload grava nuvem cloudPushMidia(pet)",
    )
    check("selectPet(item)" in pet_block, "pet upload seleciona o item")
    check(
        "pelo branco" in pet_block or "Bernese" in pet_block,
        "comentario explica porque pet nao knoca",
    )

    # ingredientes ainda knoca
    check(
        "knockEdgeMatteToDataUrl(img, 900" in src,
        "ingredientes ainda usam knockEdgeMatte(900)",
    )
    # knockWhite so existe como alias — nao deve ser chamado no pet
    knock_calls = [
        m.start()
        for m in re.finditer(r"knockWhiteToDataUrl\s*\(", src)
    ]
    # definicao + talvez zero calls; se houver call, nao pode estar no pet block
    call_sites = []
    for pos in knock_calls:
        # skip function definition line
        line = src[max(0, pos - 40) : pos + 20]
        if "function knockWhiteToDataUrl" in line:
            continue
        call_sites.append(pos)
    check(
        len(call_sites) == 0,
        f"knockWhiteToDataUrl sem chamadas ativas (calls={len(call_sites)})",
    )


def test_css_pet_circle() -> None:
    print("\n== CSS moldura pet ==")
    css = CSS.read_text(encoding="utf-8")
    check(".dsp-pet" in css, "classe .dsp-pet")
    # moldura redonda esconde fundo — contrato do fix
    pet_css = ""
    for m in re.finditer(r"\.dsp-pet[^{]*\{[^}]+\}", css):
        pet_css += m.group(0)
    check(
        "border-radius" in pet_css or "border-radius: 50%" in css,
        "pet tem border-radius (moldura redonda)",
    )


def test_regression_bernese() -> None:
    print("\n== Regressao Bernese (peito branco) ==")
    src_im = make_bernese_like()
    before = chest_opaque_count(src_im)
    check(before > 500, f"sintese tem peito branco opaco ({before} px)")

    knocked = knock_edge(src_im, 720)
    after_knock = chest_opaque_count(knocked)
    check(
        after_knock < before * 0.35,
        f"knock ANTIGO comeria peito: {before} -> {after_knock} (prova do bug)",
    )

    resized = resize_png(src_im, 720)
    after_resize = chest_opaque_count(resized)
    # resize 400->720 scale=1, conta deve ser igual; se maior imagem, proporcional
    check(
        after_resize >= before * 0.90,
        f"pipeline PET (resize) preserva peito: {before} -> {after_resize}",
    )

    # centro do peito continua opaco apos resize
    w, h = resized.size
    cx, cy = w // 2, int(h * 0.70)
    pix = resized.getpixel((cx, cy))
    check(pix[3] >= 200, f"pixel peito central opaco a={pix[3]} rgb={pix[:3]}")
    check(min(pix[0], pix[1], pix[2]) >= 220, f"pixel peito ainda branco {pix[:3]}")


def test_stock_pets() -> None:
    print("\n== Pets de biblioteca (stock) ==")
    files = sorted(PETS.glob("*.png"))
    check(len(files) >= 3, f"biblioteca pets tem {len(files)} PNG")
    wiped = 0
    for p in files:
        im = Image.open(p).convert("RGBA")
        out = resize_png(im, 720)
        opq = sum(1 for pix in out.getdata() if pix[3] >= 200)
        if opq < 80:
            wiped += 1
            bad(f"stock wipe {p.name} opaque={opq}")
        else:
            ok(f"stock {p.name} opaque={opq}")
    check(wiped == 0, "nenhum stock pet sumiu no resize")


def test_ing_still_knocks() -> None:
    print("\n== Ingredientes: knock ainda limpa matte branco ==")
    base = Image.new("RGBA", (200, 300), (255, 255, 255, 255))
    d = ImageDraw.Draw(base)
    d.ellipse((50, 80, 150, 220), fill=(40, 40, 200, 255))
    kn = knock_edge(base, 900)
    corners = [kn.getpixel(p)[3] for p in [(0, 0), (kn.size[0] - 1, 0), (0, kn.size[1] - 1), (kn.size[0] - 1, kn.size[1] - 1)]]
    check(all(a < 40 for a in corners), f"matte branco ingredientes limpo corners={corners}")
    cx = kn.getpixel((kn.size[0] // 2, kn.size[1] // 2))
    check(cx[3] >= 200, f"sujeito ingredientes preservado {cx}")


def main() -> None:
    print("VERIFY DSP-PET-BG")
    check(STUDIO.is_file(), "studio HTML existe")
    test_studio_source()
    test_css_pet_circle()
    test_regression_bernese()
    test_stock_pets()
    test_ing_still_knocks()
    print("---")
    print(f"PASS {PASS}  FAIL {FAIL}")
    if FAIL:
        raise SystemExit(1)
    print("VERIFY_OK DSP-PET-BG")


if __name__ == "__main__":
    main()
