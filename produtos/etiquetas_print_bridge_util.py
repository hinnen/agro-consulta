"""Empacota a pasta agro-print-bridge em ZIP para download pelo SisVale."""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

from django.conf import settings

_SKIP_DIR_NAMES = {
    "node_modules",
    "dist",
    ".git",
    "__pycache__",
}
_SKIP_FILE_SUFFIXES = {".pyc", ".log"}

_LEIA_ME = """Agro Etiqueta Print — SisVale
================================

1) Extraia esta pasta (botão direito → Extrair tudo).
2) Abra a pasta e dê dois cliques em:

   1-INSTALAR.bat

3) Espere “Pronto”. Se faltar Node/Electron, o instalador BAIXA SOZINHO
   (precisa de internet na 1ª vez).
4) A ponte fica em segundo plano (bandeja).
5) Volte no SisVale → Etiquetas → o card deve ficar VERDE.
6) Ao ligar o PC, a ponte sobe sozinha.

Se o card continuar amarelo: rode de novo 1-INSTALAR.bat
"""

_INSTALAR_BAT = r"""@echo off
chcp 65001 >nul
cd /d "%~dp0"
title Agro Etiqueta Print — instalar
call "%~dp0Instalar-inicio-Windows.bat"
"""


def bridge_source_dir() -> Path:
    """Raiz do pacote no disco (repo / deploy Render)."""
    bases = []
    try:
        bases.append(Path(settings.BASE_DIR).resolve())
    except Exception:
        pass
    bases.append(Path(__file__).resolve().parents[1])
    for base in bases:
        candidates = [
            base / "agro-print-bridge",
            base.parent / "agro-print-bridge",
        ]
        for c in candidates:
            if (c / "main.js").is_file() and (c / "package.json").is_file():
                return c
    raise FileNotFoundError("Pasta agro-print-bridge nao encontrada no servidor.")


def build_print_bridge_zip() -> bytes:
    root = bridge_source_dir()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("Agro-Etiqueta-Print/LEIA-ME.txt", _LEIA_ME)
        zf.writestr("Agro-Etiqueta-Print/1-INSTALAR.bat", _INSTALAR_BAT)
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            rel_parts = path.relative_to(root).parts
            if any(p in _SKIP_DIR_NAMES for p in rel_parts):
                continue
            if path.suffix.lower() in _SKIP_FILE_SUFFIXES:
                continue
            arc = "Agro-Etiqueta-Print/" + "/".join(rel_parts)
            zf.write(path, arcname=arc)
    return buf.getvalue()
