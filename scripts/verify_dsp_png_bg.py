"""VERIFY DSP-PNG-BG — mirrors knockEdgeMatte + letterbox PNG contracts.

Run: python scripts/verify_dsp_png_bg.py
"""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw

ROOT = Path(__file__).resolve().parents[1]
INGS = ROOT / "produtos/static/produtos/dispenser-a6/lib/ings"
STUDIO = ROOT / "produtos/templates/produtos/dispenser_a6_studio.html"
CSS = ROOT / "produtos/static/produtos/dispenser-a6/dispenser.css"
fails = 0


def is_bg(r, g, b, a):
    if a < 40:
        return True
    mx = max(r, g, b)
    mn = min(r, g, b)
    if mn >= 228:
        return True
    if mx <= 36:
        return True
    return False


def knock_edge(im, max_side=900):
    im = im.convert("RGBA")
    w, h = im.size
    scale = min(1, max_side / max(w, h))
    cw = max(1, round(w * scale))
    ch = max(1, round(h * scale))
    im = im.resize((cw, ch), Image.Resampling.BILINEAR)
    px = im.load()
    seen = [[False] * cw for _ in range(ch)]
    q = []

    def push(x, y):
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
    cleared = 0
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
                cleared += 1
            elif mn >= 228:
                na = max(0, min(255, (245 - mn) * 12))
                px[x, y] = (r, g, b, na)
                cleared += 1
            elif mx <= 36:
                na = max(0, min(255, mx * 6))
                px[x, y] = (r, g, b, na)
                cleared += 1
            else:
                px[x, y] = (r, g, b, 0)
                cleared += 1
        push(x + 1, y)
        push(x - 1, y)
        push(x, y + 1)
        push(x, y - 1)
    return im, cleared


def letterbox_png(im, ratio=40 / 58):
    im = im.convert("RGBA")
    cw, ch = im.size
    aspect = cw / ch
    if aspect > ratio:
        out_w, out_h = cw, max(1, round(cw / ratio))
        ox, oy = 0, (out_h - ch) // 2
    elif aspect < ratio:
        out_w, out_h = max(1, round(ch * ratio)), ch
        ox, oy = (out_w - cw) // 2, 0
    else:
        return im
    out = Image.new("RGBA", (out_w, out_h), (0, 0, 0, 0))
    out.paste(im, (ox, oy), im)
    return out


def corner_alphas(im):
    w, h = im.size
    return [im.getpixel(p)[3] for p in [(0, 0), (w - 1, 0), (0, h - 1), (w - 1, h - 1)]]


def check(cond, msg):
    global fails
    if not cond:
        fails += 1
        print("FAIL", msg)
    else:
        print("OK  ", msg)


def main():
    global fails
    src = STUDIO.read_text(encoding="utf-8")
    css = CSS.read_text(encoding="utf-8")
    check('ingViewport.style.backgroundColor = "#ffffff"' in src, "syncIngFrameBg always white")
    check("nunca amostra canto" in src, "resize comment: no corner sample")
    check('ctx.fillStyle = "#ffffff"' in src, "JPEG fill fixed white")
    check('type: "image/png", portraitRatio' in src, "ingredient upload saves PNG")
    check("knockEdgeMatteToDataUrl(img, 900" in src, "upload runs knock before resize")
    check('bg = "rgb(" + d[0]' not in src, "old corner RGB sample removed")
    check("opaque += 1" not in src, "old multi-corner sampler removed")
    check("background: #ffffff; /* PNG transparente" in css, "CSS viewport white")

    stock_fail = 0
    files = sorted(INGS.glob("*.png"))
    for p in files:
        im = Image.open(p)
        kn, _ = knock_edge(im, 900)
        boxed = letterbox_png(kn)
        if any(a >= 40 for a in corner_alphas(boxed)):
            stock_fail += 1
            print("FAIL stock corners", p.name, corner_alphas(boxed))
        opq = sum(1 for pix in kn.getdata() if pix[3] >= 200)
        if opq < 50:
            stock_fail += 1
            print("FAIL stock wiped", p.name, opq)
    check(stock_fail == 0, f"stock {len(files)} PNGs survive knock+letterbox")

    syn = Image.new("RGBA", (200, 300), (0, 0, 0, 0))
    d = ImageDraw.Draw(syn)
    d.ellipse((40, 60, 160, 220), fill=(200, 40, 40, 255))
    c = syn.getpixel((0, 0))
    check(c[:3] == (0, 0, 0) and c[3] == 0, "synth corner RGB black under alpha=0")
    boxed = letterbox_png(syn)
    check(all(a == 0 for a in corner_alphas(boxed)), "letterbox PNG keeps transparent corners")
    check(boxed.getpixel((100, 150))[3] == 255, "letterbox PNG keeps subject")

    for name, bg, fill in [
        ("black", (0, 0, 0, 255), (220, 80, 40, 255)),
        ("white", (255, 255, 255, 255), (40, 40, 200, 255)),
    ]:
        base = Image.new("RGBA", (200, 300), bg)
        d = ImageDraw.Draw(base)
        d.ellipse((50, 80, 150, 220), fill=fill)
        kn, cl = knock_edge(base, 900)
        check(all(a < 40 for a in corner_alphas(kn)), f"{name} matte corners clear (cleared={cl})")
        cx = kn.getpixel((kn.size[0] // 2, kn.size[1] // 2))
        check(cx[3] >= 200, f"{name} matte subject kept {cx}")

    grey = Image.new("RGBA", (200, 300), (140, 140, 140, 255))
    d = ImageDraw.Draw(grey)
    d.ellipse((50, 80, 150, 220), fill=(30, 160, 60, 255))
    kn, cl = knock_edge(grey, 900)
    check(
        kn.getpixel((2, 2))[3] >= 200,
        f"grey matte preserved at edge alpha={kn.getpixel((2, 2))[3]} cleared={cl}",
    )

    for name in [
        "solo-arroz.png",
        "solo-quinoa.png",
        "solo-sardinha.png",
        "combo-carne-vegetais.png",
        "carne-legumes.png",
    ]:
        p = INGS / name
        if not p.exists():
            continue
        im = Image.open(p).convert("RGBA")
        w, h = im.size
        scale = min(1, 900 / max(w, h))
        before = sum(1 for pix in im.getdata() if pix[3] >= 200)
        kn, cl = knock_edge(im, 900)
        after = sum(1 for pix in kn.getdata() if pix[3] >= 200)
        expect = before * (scale * scale)
        retain = after / max(1, expect)
        check(retain >= 0.55, f"{name} retain={retain:.2f} cleared={cl}")

    for name in ["combo-carne-vegetais.png", "combo-frango-carne.png"]:
        im = Image.open(INGS / name)
        kn, _ = knock_edge(im, 900)
        out = letterbox_png(kn)
        check(all(a == 0 for a in corner_alphas(out)), f"pipeline {name} corners transparent")
        opq = sum(1 for pix in out.getdata() if pix[3] >= 200)
        check(opq > 1000, f"pipeline {name} subject pixels={opq}")

    print("---")
    print("FAILS", fails)
    if fails:
        raise SystemExit(1)
    print("VERIFY_OK DSP-PNG-BG")


if __name__ == "__main__":
    main()
