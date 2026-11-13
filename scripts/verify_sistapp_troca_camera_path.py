#!/usr/bin/env python3
"""Path estático — câmera de trás no Ajuste, Fotos e Etiquetas. Sem troca de lente."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    js = read("produtos/static/produtos/js/sistapp_camera_escolha.js")
    for n in (
        'facingMode: { ideal: "environment" }',
        "width: { ideal: 1920 }",
        "height: { ideal: 1080 }",
        "focusMode: \"continuous\"",
        "function aplicar",
        "function afiar",
        "sistapp_camera_id_v1",
        "sistapp_camera_id_v2",
    ):
        if n in js:
            ok(f"js {n[:40]}")
        else:
            fail(f"js falta {n}")
    if "getCameras" in js or "function proxima" in js:
        fail("js ainda troca de câmera")
    else:
        ok("js sem troca")

    telas = {
        "ajuste": "produtos/templates/produtos/mobile_ajuste.html",
        "fotos": "produtos/templates/produtos/fotos_produto/app.html",
        "etiquetas": "produtos/templates/produtos/etiquetas_solicitacao/app.html",
    }
    for nome, rel in telas.items():
        html = read(rel)
        if "sistapp_camera_escolha.js" not in html:
            fail(f"{nome} sem o script")
        else:
            ok(f"{nome} script")
        if "SistAppCamera.aplicar" not in html:
            fail(f"{nome} não aplica o pedido da câmera de trás")
        else:
            ok(f"{nome} aplica")
        if "Trocar câmera" in html or "btnTrocaCam" in html or "maTrocarCamera" in html:
            fail(f"{nome} ainda tem troca de câmera")
        else:
            ok(f"{nome} sem troca")
        if "videoConstraints" in html:
            fail(f"{nome} monta videoConstraints fora do helper")
        else:
            ok(f"{nome} sem videoConstraints solto")

    print(f"\nVERIFY_OK {oks}" if not fails else f"\nVERIFY FAIL {oks}/{oks + len(fails)}")
    for f in fails:
        print(" -", f)
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
