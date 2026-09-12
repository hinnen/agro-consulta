# -*- coding: utf-8 -*-
"""WA-PC-ICON — prova: ícone bolha S nos PNGs do PWA Zap."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def main() -> int:
    from PIL import Image

    p192 = ROOT / "produtos/static/produtos/pwa/zap-loja-192.png"
    p512 = ROOT / "produtos/static/produtos/pwa/zap-loja-512.png"
    views = (ROOT / "produtos/views_atendimento_whatsapp.py").read_text(encoding="utf-8")
    html = (ROOT / "produtos/templates/produtos/atendimento_whatsapp.html").read_text(encoding="utf-8")
    cel = (ROOT / "produtos/templates/produtos/atendimento_whatsapp_celular.html").read_text(
        encoding="utf-8"
    )

    check(p192.is_file() and p512.is_file(), "arquivos icone")
    im192 = Image.open(p192).convert("RGBA")
    im512 = Image.open(p512).convert("RGBA")
    check(im192.size == (192, 192), "192 size")
    check(im512.size == (512, 512), "512 size")
    check(p192.stat().st_size > 5000 and p512.stat().st_size > 20000, "tamanhos razoaveis")
    check("/static/produtos/pwa/zap-loja-192.png" in views, "manifest usa 192")
    check("/static/produtos/pwa/zap-loja-512.png" in views, "manifest usa 512")
    check("zap-loja-192.png" in html, "html PC apple-touch")
    check("zap-loja-192.png" in cel, "html cel apple-touch")
    check(p512.stat().st_size > p192.stat().st_size, "512 maior que 192")

    # Achatar sobre fundo escuro e achar pixel bem verde (bolha)
    def tem_verde(im: Image.Image) -> bool:
        flat = Image.new("RGBA", im.size, (15, 23, 42, 255))
        flat.alpha_composite(im)
        rgb = flat.convert("RGB")
        w, h = rgb.size
        for y in range(h // 5, (4 * h) // 5, 4):
            for x in range(w // 5, (4 * w) // 5, 4):
                r, g, b = rgb.getpixel((x, y))
                if g > 120 and g > r + 20 and g > b + 20:
                    return True
        return False

    check(tem_verde(im192), "192 tem verde da bolha")
    check(tem_verde(im512), "512 tem verde da bolha")

    print("---")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
