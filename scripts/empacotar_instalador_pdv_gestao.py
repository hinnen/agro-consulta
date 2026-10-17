#!/usr/bin/env python3
"""Gera scripts/Clique-aqui-instalar-PDV-e-Gestao.zip para loja (1 link)."""
from __future__ import annotations

import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PKG = ROOT / "pacote-instalador-pdv-gestao"
OUT = ROOT / "Clique-aqui-instalar-PDV-e-Gestao.zip"
ZIP_ROOT = "SisVale-PDV-e-Gestao"

FILES = [
    (PKG / "CLIQUE-AQUI-INSTALAR-PDV-E-GESTAO.bat", f"{ZIP_ROOT}/CLIQUE-AQUI-INSTALAR-PDV-E-GESTAO.bat"),
    (PKG / "LEIA-ME.txt", f"{ZIP_ROOT}/LEIA-ME.txt"),
    (ROOT / "instalar_sistvale_pdv_gestao.ps1", f"{ZIP_ROOT}/instalar_sistvale_pdv_gestao.ps1"),
    (ROOT / "remover_apps_chrome_sistvale.ps1", f"{ZIP_ROOT}/remover_apps_chrome_sistvale.ps1"),
]


def _crlf_bytes(path: Path) -> bytes:
    raw = path.read_bytes()
    text = raw.decode("utf-8")
    return text.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8").replace(b"\n", b"\r\n")


def main() -> None:
    missing = [str(p) for p, _ in FILES if not p.is_file()]
    if missing:
        raise SystemExit("Arquivos ausentes:\n" + "\n".join(missing))

    if OUT.exists():
        OUT.unlink()

    with zipfile.ZipFile(OUT, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for src, arc in FILES:
            if src.suffix.lower() == ".bat":
                data = _crlf_bytes(src)
                zf.writestr(arc, data)
            else:
                zf.write(src, arc)

    print(f"OK: {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
