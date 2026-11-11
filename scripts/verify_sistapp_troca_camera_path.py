#!/usr/bin/env python3
"""Path estático — troca de câmera no Ajuste, Fotos e Etiquetas. Sem banco."""
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
        "sistapp_camera_id_v1",
        "function ordenar",
        "Html5Qrcode.getCameras",
        "window.SistAppCamera",
    ):
        if n in js:
            ok(f"js {n}")
        else:
            fail(f"js falta {n}")

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
        if "Trocar câmera" not in html:
            fail(f"{nome} sem o botão")
        else:
            ok(f"{nome} botão")

    print(f"\nVERIFY_OK {oks}" if not fails else f"\nVERIFY FAIL {oks}/{oks + len(fails)}")
    for f in fails:
        print(" -", f)
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
