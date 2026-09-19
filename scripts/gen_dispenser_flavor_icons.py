# -*- coding: utf-8 -*-
"""Gera PNGs de ícones de sabores (contorno colorido, 256x256, fundo transparente)."""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw

OUT = Path(__file__).resolve().parents[1] / "produtos" / "static" / "produtos" / "dispenser-a6" / "lib" / "icons"
SIZE = 256
PAD = 28
SW = 14  # stroke ~1.7 in 24px viewBox → ~18; use 14 for soft look


def new_canvas():
    return Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))


def xy(x: float, y: float):
    """Map 0–24 viewBox → canvas with padding."""
    usable = SIZE - 2 * PAD
    return int(PAD + x / 24 * usable), int(PAD + y / 24 * usable)


def poly(draw, pts, color, width=SW):
    mapped = [xy(*p) for p in pts]
    draw.line(mapped + [mapped[0]], fill=color, width=width, joint="curve")


def stroke_line(draw, pts, color, width=SW):
    mapped = [xy(*p) for p in pts]
    draw.line(mapped, fill=color, width=width, joint="curve")


def ellipse_outline(draw, x0, y0, x1, y1, color, width=SW):
    a = xy(x0, y0) + xy(x1, y1)
    # draw thicker by offsetting
    draw.ellipse(a, outline=color, width=width)


def circle_outline(draw, cx, cy, r, color, width=SW):
    ellipse_outline(draw, cx - r, cy - r, cx + r, cy + r, color, width)


def circle_fill(draw, cx, cy, r, color):
    a = xy(cx - r, cy - r) + xy(cx + r, cy + r)
    draw.ellipse(a, fill=color)


def save(name: str, im: Image.Image):
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{name}.png"
    im.save(path, "PNG", optimize=True)
    print("ok", path.name)


def icon_leite():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (52, 152, 219, 255)  # #3498db
    # carton body
    stroke_line(d, [(8, 7), (8, 20), (16, 20), (16, 7)], c)
    # gable top
    stroke_line(d, [(8, 7), (12, 4), (16, 7)], c)
    # spout
    stroke_line(d, [(14.5, 5.2), (17.5, 4.2), (17.8, 6.2), (15.2, 6.8)], c)
    # drop
    stroke_line(d, [(11.5, 11), (12, 14), (12.5, 11)], c)
    ellipse_outline(d, 11.2, 10.2, 12.8, 11.8, c, SW - 2)
    save("leite", im)


def icon_ovo():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (241, 196, 15, 255)  # #f1c40f
    # egg outline
    ellipse_outline(d, 7.5, 4.5, 16.5, 20.5, c)
    # highlight curve
    stroke_line(d, [(10, 8), (9.5, 12), (10.2, 15)], c, SW - 3)
    save("ovo", im)


def icon_bacalhau():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (36, 113, 163, 255)  # #2471a3
    # fish body
    stroke_line(
        d,
        [(4, 12), (7, 8.5), (12, 7.5), (17, 8.5), (20, 12), (17, 15.5), (12, 16.5), (7, 15.5), (4, 12)],
        c,
    )
    # tail
    stroke_line(d, [(4, 12), (2.2, 9.2), (2.2, 14.8), (4, 12)], c)
    circle_fill(d, 16.5, 11.2, 0.7, c)
    stroke_line(d, [(11, 9), (11, 15)], c, SW - 3)
    save("bacalhau", im)


def icon_veado():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (110, 44, 0, 255)  # #6e2c00
    # head
    ellipse_outline(d, 8, 10, 16, 19, c)
    # ears
    stroke_line(d, [(9.2, 11), (7.5, 7.5), (10, 10)], c)
    stroke_line(d, [(14.8, 11), (16.5, 7.5), (14, 10)], c)
    # antlers
    stroke_line(d, [(9.5, 10.5), (8, 5.5), (6.2, 4), (7.2, 6.2)], c, SW - 2)
    stroke_line(d, [(14.5, 10.5), (16, 5.5), (17.8, 4), (16.8, 6.2)], c, SW - 2)
    # nose
    ellipse_outline(d, 10.8, 15.5, 13.2, 17.8, c, SW - 3)
    save("veado", im)


def icon_camarao():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (231, 76, 60, 255)  # #e74c3c
    # curved body
    stroke_line(d, [(6, 15), (8, 10), (12, 7.5), (16, 8), (18.5, 11)], c)
    stroke_line(d, [(8, 10), (9.5, 13.5), (12, 15.5), (15, 15), (17, 12.5)], c)
    # head/tail curl
    stroke_line(d, [(18.5, 11), (20, 9), (19, 7.2)], c)
    # legs
    for x0, y0, x1, y1 in [
        (9, 13.5, 7, 16.5),
        (11.5, 15, 10, 18),
        (14, 15.2, 13.5, 18.2),
    ]:
        stroke_line(d, [(x0, y0), (x1, y1)], c, SW - 3)
    # eye
    circle_fill(d, 17.2, 9.5, 0.55, c)
    save("camarao", im)


def icon_batata():
    """Batata branca (diferente da batata-doce roxa)."""
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (183, 149, 11, 255)  # #b7950b
    ellipse_outline(d, 6.5, 7, 17.5, 18, c)
    stroke_line(d, [(9.5, 10.5), (10.2, 11.2)], c, SW - 4)
    stroke_line(d, [(13.5, 12), (14.2, 12.8)], c, SW - 4)
    stroke_line(d, [(11, 14.5), (11.8, 15.2)], c, SW - 4)
    save("batata-branca", im)


def icon_milho():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (241, 196, 15, 255)
    g = (39, 174, 96, 255)
    # cob
    ellipse_outline(d, 9, 5.5, 15, 19.5, c)
    # kernels grid
    for y in (8, 11, 14, 17):
        stroke_line(d, [(10.2, y), (13.8, y)], c, SW - 5)
    for x in (11, 13):
        stroke_line(d, [(x, 7), (x, 18)], c, SW - 5)
    # husk leaves
    stroke_line(d, [(9.5, 6.5), (6.5, 4), (8.5, 8)], g, SW - 2)
    stroke_line(d, [(14.5, 6.5), (17.5, 4), (15.5, 8)], g, SW - 2)
    save("milho", im)


def icon_linhaca():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (147, 81, 22, 255)  # #935116
    # seed cluster
    for cx, cy, r in [
        (9, 10, 2.2),
        (14.5, 9.5, 2.2),
        (11.5, 14.5, 2.3),
        (15.5, 14, 1.9),
        (8.5, 14.8, 1.8),
    ]:
        ellipse_outline(d, cx - r, cy - r, cx + r, cy + r, c, SW - 2)
    save("linhaca", im)


def icon_cranberry():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (192, 57, 43, 255)
    g = (39, 174, 96, 255)
    circle_outline(d, 9.5, 13, 3.4, c)
    circle_outline(d, 14.8, 12.2, 3.4, c)
    circle_outline(d, 12, 8.2, 2.8, c)
    stroke_line(d, [(12, 5.8), (12, 4.5)], g, SW - 3)
    stroke_line(d, [(12, 6), (13.5, 5)], g, SW - 3)
    save("cranberry", im)


def icon_banana():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (241, 196, 15, 255)
    stroke_line(d, [(7, 8), (8.5, 14), (11, 18), (15, 19.5), (18, 17)], c)
    stroke_line(d, [(8.2, 7.2), (10, 13), (13, 17), (16.5, 18.2)], c)
    stroke_line(d, [(7, 8), (8.2, 7.2), (9.2, 5.5)], c)
    save("banana", im)


def icon_couve():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (30, 132, 73, 255)  # #1e8449
    # leafy shape
    stroke_line(
        d,
        [(12, 5), (16.5, 8), (18, 13), (15, 18), (12, 19.5), (9, 18), (6, 13), (7.5, 8), (12, 5)],
        c,
    )
    stroke_line(d, [(12, 6.5), (12, 18.5)], c, SW - 2)
    stroke_line(d, [(12, 10), (15.5, 8.5)], c, SW - 3)
    stroke_line(d, [(12, 13), (16, 13.5)], c, SW - 3)
    stroke_line(d, [(12, 10), (8.5, 8.5)], c, SW - 3)
    stroke_line(d, [(12, 13), (8, 13.5)], c, SW - 3)
    save("couve", im)


def icon_inhame():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (125, 60, 152, 255)  # #7d3c98
    stroke_line(
        d,
        [(10, 5.5), (14.5, 7), (17, 12), (15.5, 18), (11, 19.5), (7.5, 16), (7, 10), (10, 5.5)],
        c,
    )
    stroke_line(d, [(11, 9), (13, 10.5)], c, SW - 4)
    stroke_line(d, [(10, 13), (12.5, 14)], c, SW - 4)
    save("inhame", im)


def icon_alecrim():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    c = (22, 160, 133, 255)  # #16a085
    stroke_line(d, [(12, 20), (12, 5)], c)
    for y, dx in [(7, 3.2), (10, 3.5), (13, 3.2), (16, 2.8)]:
        stroke_line(d, [(12, y), (12 - dx, y - 1.2)], c, SW - 2)
        stroke_line(d, [(12, y), (12 + dx, y - 1.2)], c, SW - 2)
    save("alecrim", im)


def icon_mix_sabores():
    im = new_canvas()
    d = ImageDraw.Draw(im)
    circle_outline(d, 9.2, 10.2, 4.1, (230, 126, 34, 255))  # #e67e22
    circle_outline(d, 14.8, 10.2, 4.1, (192, 57, 43, 255))  # #c0392b
    circle_outline(d, 12, 15.2, 4.1, (39, 174, 96, 255))  # #27ae60
    save("mix-sabores", im)


def main():
    icon_leite()
    icon_ovo()
    icon_bacalhau()
    icon_veado()
    icon_camarao()
    icon_batata()
    icon_milho()
    icon_linhaca()
    icon_cranberry()
    icon_banana()
    icon_couve()
    icon_inhame()
    icon_alecrim()
    icon_mix_sabores()
    print("done →", OUT)


if __name__ == "__main__":
    main()
