"""Empacota a pasta agro-print-bridge em ZIP para download pelo SisVale.

Layout amigavel (loja):
  Agro-Etiqueta-Print/
    CLIQUE-AQUI-INSTALAR.bat   <- unico arquivo para o usuario clicar
    LEIA-ME.txt
    app/                      <- codigo da ponte + Node portatil (vendor)
"""
from __future__ import annotations

import io
import logging
import urllib.request
import zipfile
from pathlib import Path

from django.conf import settings

logger = logging.getLogger(__name__)

_SKIP_DIR_NAMES = {
    "node_modules",
    "dist",
    ".git",
    "__pycache__",
    "vendor",  # vendor entra so pelo cache controlado
}
_SKIP_FILE_SUFFIXES = {".pyc", ".log"}

# Node embutido no ZIP (x64 = PCs da loja). Evita 404 na hora de instalar.
_NODE_VER = "v22.14.0"
_NODE_ARCH = "win-x64"
# Nome oficial no dist: node-v22.14.0-win-x64.zip (mantem o "v" no nome da pasta).
_NODE_FOLDER = f"node-{_NODE_VER}-{_NODE_ARCH}"
_NODE_ZIP_NAME = f"{_NODE_FOLDER}.zip"
_NODE_URLS = (
    f"https://nodejs.org/dist/{_NODE_VER}/{_NODE_ZIP_NAME}",
    f"https://npmmirror.com/mirrors/node/{_NODE_VER}/{_NODE_ZIP_NAME}",
)

_LEIA_ME = """Agro Etiqueta Print - SisVale
================================

1) Extraia esta pasta (botao direito -> Extrair tudo).
2) De DOIS CLIQUES so neste arquivo:

   CLIQUE-AQUI-INSTALAR.bat

   (e o unico que precisa clicar - o resto fica na pasta app)

3) Espere "Pronto". A ponte sobe sozinha e tambem ao ligar o PC.
4) Volte no SisVale -> Etiquetas -> o card deve ficar VERDE.

Se o card continuar amarelo: rode de novo CLIQUE-AQUI-INSTALAR.bat
"""

_CLIQUE_AQUI_BAT = """@echo off
cd /d "%~dp0"
title Agro Etiqueta Print - instalar
if not exist "%~dp0app\\Instalar-inicio-Windows.bat" (
  echo ERRO: pasta app nao encontrada. Extraia o ZIP inteiro.
  pause
  exit /b 1
)
call "%~dp0app\\Instalar-inicio-Windows.bat"
"""


def _ascii_crlf_bytes(text: str) -> bytes:
    t = text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\r\n")
    return t.encode("ascii", errors="strict")


def bridge_source_dir() -> Path:
    bases = []
    try:
        bases.append(Path(settings.BASE_DIR).resolve())
    except Exception:
        pass
    bases.append(Path(__file__).resolve().parents[1])
    for base in bases:
        for c in (base / "agro-print-bridge", base.parent / "agro-print-bridge"):
            if (c / "main.js").is_file() and (c / "package.json").is_file():
                return c
    raise FileNotFoundError("Pasta agro-print-bridge nao encontrada no servidor.")


def _vendor_cache_dir() -> Path:
    try:
        base = Path(settings.BASE_DIR).resolve()
    except Exception:
        base = Path(__file__).resolve().parents[1]
    d = base / ".cache" / "agro-print-bridge-vendor"
    d.mkdir(parents=True, exist_ok=True)
    return d


def ensure_node_vendor_zip() -> Path:
    """Baixa Node portatil uma vez (cache) para embutir no ZIP da loja."""
    dest = _vendor_cache_dir() / _NODE_ZIP_NAME
    if dest.is_file() and dest.stat().st_size > 1_000_000:
        return dest
    last_err: Exception | None = None
    for url in _NODE_URLS:
        try:
            logger.info("Baixando Node vendor para ZIP da ponte: %s", url)
            req = urllib.request.Request(url, headers={"User-Agent": "AgroEtiquetaPrint/1.0"})
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = resp.read()
            if len(data) < 1_000_000:
                raise RuntimeError(f"zip Node muito pequeno ({len(data)} bytes)")
            dest.write_bytes(data)
            return dest
        except Exception as e:
            last_err = e
            logger.warning("Falha ao baixar Node vendor %s: %s", url, e)
    raise RuntimeError(f"Nao foi possivel baixar Node para o ZIP da ponte: {last_err}")


def build_print_bridge_zip() -> bytes:
    root = bridge_source_dir()
    node_zip = ensure_node_vendor_zip()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Raiz: so o que o usuario precisa ver/clicar
        zf.writestr("Agro-Etiqueta-Print/LEIA-ME.txt", _LEIA_ME.encode("utf-8"))
        zf.writestr(
            "Agro-Etiqueta-Print/CLIQUE-AQUI-INSTALAR.bat",
            _ascii_crlf_bytes(_CLIQUE_AQUI_BAT),
        )
        # App (codigo)
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            rel_parts = path.relative_to(root).parts
            if any(p in _SKIP_DIR_NAMES for p in rel_parts):
                continue
            if path.suffix.lower() in _SKIP_FILE_SUFFIXES:
                continue
            arc = "Agro-Etiqueta-Print/app/" + "/".join(rel_parts)
            if path.suffix.lower() == ".bat":
                raw = path.read_text(encoding="utf-8", errors="replace")
                safe = "".join(ch if ord(ch) < 128 else "-" for ch in raw)
                zf.writestr(arc, _ascii_crlf_bytes(safe))
            else:
                zf.write(path, arcname=arc)
        # Node portatil embutido (instalador usa sem baixar da internet)
        zf.write(
            node_zip,
            arcname=f"Agro-Etiqueta-Print/app/vendor/{_NODE_ZIP_NAME}",
        )
    return buf.getvalue()
