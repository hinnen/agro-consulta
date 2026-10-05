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

_LEIA_ME = """Agro Etiqueta Print - SisVale
================================

1) Extraia esta pasta (botao direito -> Extrair tudo).
2) Abra a pasta e de dois cliques em:

   1-INSTALAR.bat

3) Espere "Pronto". Se faltar Node/Electron, o instalador BAIXA SOZINHO
   (precisa de internet na 1a vez).
4) A ponte fica em segundo plano (bandeja).
5) Volte no SisVale -> Etiquetas -> o card deve ficar VERDE.
6) Ao ligar o PC, a ponte sobe sozinha.

Se o card continuar amarelo: rode de novo 1-INSTALAR.bat
"""

# ASCII only + CRLF: cmd.exe em Windows antigo quebra com tracinho UTF-8 (—).
_INSTALAR_BAT = (
    "@echo off\r\n"
    "cd /d \"%~dp0\"\r\n"
    "title Agro Etiqueta Print - instalar\r\n"
    "call \"%~dp0Instalar-inicio-Windows.bat\"\r\n"
)


def _ascii_crlf_bytes(text: str) -> bytes:
    """Garante CRLF e só ASCII (bat seguro no cmd)."""
    t = text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\r\n")
    return t.encode("ascii", errors="strict")


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
        zf.writestr(
            "Agro-Etiqueta-Print/LEIA-ME.txt",
            _LEIA_ME.encode("utf-8"),
        )
        zf.writestr(
            "Agro-Etiqueta-Print/1-INSTALAR.bat",
            _ascii_crlf_bytes(_INSTALAR_BAT.replace("\r\n", "\n")),
        )
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            rel_parts = path.relative_to(root).parts
            if any(p in _SKIP_DIR_NAMES for p in rel_parts):
                continue
            if path.suffix.lower() in _SKIP_FILE_SUFFIXES:
                continue
            arc = "Agro-Etiqueta-Print/" + "/".join(rel_parts)
            # .bat do disco: regrava ASCII+CRLF se for texto bat
            if path.suffix.lower() == ".bat":
                raw = path.read_text(encoding="utf-8", errors="replace")
                # remove chars nao-ASCII (—, acentos em echo) p/ cmd
                safe = "".join(ch if ord(ch) < 128 else "-" for ch in raw)
                zf.writestr(arc, _ascii_crlf_bytes(safe))
            else:
                zf.write(path, arcname=arc)
    return buf.getvalue()
