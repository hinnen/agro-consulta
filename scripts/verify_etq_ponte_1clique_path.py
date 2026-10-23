# -*- coding: utf-8 -*-
"""
Prova detalhada — ZIP ponte 1 clique + Node embutido (`ETQ-PONTE-1CLIQUE`).

  set AGRO_PIN_TESTE=9973
  set PYTHONIOENCODING=utf-8
  python scripts/verify_etq_ponte_1clique_path.py
"""
from __future__ import annotations

import io
import os
import re
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client, override_settings
from django.urls import reverse

from produtos.caixa_util import validar_pin_operador
from produtos.etiquetas_print_bridge_util import (
    _NODE_ZIP_NAME,
    build_print_bridge_zip,
    ensure_node_vendor_zip,
)

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
oks: list[str] = []
fails: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_contratos() -> None:
    print("== 1) Contratos arquivo / UI ==")
    util = (ROOT / "produtos/etiquetas_print_bridge_util.py").read_text(encoding="utf-8")
    ps1 = (ROOT / "agro-print-bridge/ensure-node.ps1").read_text(encoding="utf-8")
    html = (ROOT / "produtos/templates/produtos/produtos_etiquetas.html").read_text(
        encoding="utf-8"
    )
    js = (ROOT / "produtos/static/produtos/js/produtos_etiquetas.js").read_text(
        encoding="utf-8"
    )
    instalar = (ROOT / "agro-print-bridge/Instalar-inicio-Windows.bat").read_text(
        encoding="utf-8", errors="replace"
    )

    check("util_clique_aqui", "CLIQUE-AQUI-INSTALAR.bat" in util)
    check("util_app_prefix", 'Agro-Etiqueta-Print/app/' in util or "app/" in util)
    check("util_vendor_fn", "ensure_node_vendor_zip" in util)
    check("util_node_v_in_name", "node-v" in util or "node-{_NODE_VER}" in util)
    check("ps1_vendor", "vendor" in ps1 and "Usando Node que veio no pacote" in ps1)
    check("ps1_nodejs_fallback", "nodejs.org/dist" in ps1)
    check("html_clique", "CLIQUE-AQUI-INSTALAR.bat" in html)
    check("js_clique", "CLIQUE-AQUI-INSTALAR.bat" in js)
    check("instalar_ascii", all(ord(c) < 128 for c in instalar), f"len={len(instalar)}")
    check("instalar_sem_emdash", "\u2014" not in instalar)
    check("sem_1_instalar_util", "1-INSTALAR.bat" not in util)


def test_pin() -> None:
    print("== 2) PIN operacional ==")
    ok, msg = validar_pin_operador(PIN)
    check("pin_9973", ok, (msg or "")[:60])


def test_zip_layout_e_node() -> None:
    print("== 3) ZIP layout + Node embutido ==")
    vendor = ensure_node_vendor_zip()
    check("vendor_cache_existe", vendor.is_file(), str(vendor))
    check("vendor_cache_tamanho", vendor.stat().st_size > 1_000_000, str(vendor.stat().st_size))
    check("vendor_nome", vendor.name == _NODE_ZIP_NAME, vendor.name)

    raw = build_print_bridge_zip()
    check("zip_magic", raw[:2] == b"PK", str(len(raw)))
    check("zip_tamanho_com_node", len(raw) > 10_000_000, str(len(raw)))

    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        names = zf.namelist()
    root_files = sorted(
        n for n in names if n.count("/") == 1 and not n.endswith("/")
    )
    # Agro-Etiqueta-Print/CLIQUE... e LEIA-ME
    check(
        "raiz_so_clique_e_leia",
        set(Path(n).name for n in root_files)
        == {"CLIQUE-AQUI-INSTALAR.bat", "LEIA-ME.txt"},
        str([Path(n).name for n in root_files]),
    )
    check(
        "tem_app_main",
        "Agro-Etiqueta-Print/app/main.js" in names,
    )
    check(
        "tem_app_instalar",
        "Agro-Etiqueta-Print/app/Instalar-inicio-Windows.bat" in names,
    )
    check(
        "tem_app_ensure_node",
        "Agro-Etiqueta-Print/app/ensure-node.ps1" in names,
    )
    vendor_names = [
        n for n in names if n.startswith("Agro-Etiqueta-Print/app/vendor/") and n.endswith(".zip")
    ]
    check("tem_vendor_node_zip", len(vendor_names) == 1, str(vendor_names))
    check("sem_node_modules", not any("node_modules" in n for n in names))
    check(
        "sem_arquivos_soltos_raiz_app",
        "Agro-Etiqueta-Print/main.js" not in names,
    )

    # Conteudo do CLIQUE-AQUI
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        clique = zf.read("Agro-Etiqueta-Print/CLIQUE-AQUI-INSTALAR.bat")
        check("clique_ascii", all(b < 128 for b in clique))
        check("clique_crlf", b"\r\n" in clique)
        check(
            "clique_chama_app",
            b"app\\Instalar-inicio-Windows.bat" in clique
            or b"app/Instalar-inicio-Windows.bat" in clique,
        )
        # Extrai Node vendor e confere node.exe
        vname = vendor_names[0]
        vbytes = zf.read(vname)
    check("vendor_zip_magic", vbytes[:2] == b"PK")
    with tempfile.TemporaryDirectory() as td:
        tdp = Path(td)
        vz = tdp / "node.zip"
        vz.write_bytes(vbytes)
        with zipfile.ZipFile(vz) as nz:
            nz.extractall(tdp / "out")
        exes = list((tdp / "out").rglob("node.exe"))
        check("vendor_tem_node_exe", len(exes) >= 1, str(exes[:2]))


def test_http_download() -> None:
    print("== 4) HTTP download (gate staff) ==")
    User = get_user_model()
    superu = User.objects.filter(is_superuser=True).order_by("pk").first()
    check("tem_superuser", superu is not None, getattr(superu, "username", ""))
    if not superu:
        return
    hosts = list(
        getattr(
            __import__("django.conf", fromlist=["settings"]).settings,
            "ALLOWED_HOSTS",
            [],
        )
        or []
    )
    for h in ("testserver", "localhost", "127.0.0.1"):
        if h not in hosts:
            hosts.append(h)

    url = reverse("api_etiquetas_print_bridge_download")
    c = Client()
    with override_settings(ALLOWED_HOSTS=hosts):
        c.force_login(superu)
        r = c.get(url)
        check("download_200", r.status_code == 200, str(r.status_code))
        check("download_zip_magic", r.content[:2] == b"PK", str(len(r.content)))
        disp = r.get("Content-Disposition", "")
        check("download_filename", "Agro-Etiqueta-Print" in disp and ".zip" in disp, disp[:80])
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            names = zf.namelist()
        check(
            "download_tem_clique",
            "Agro-Etiqueta-Print/CLIQUE-AQUI-INSTALAR.bat" in names,
        )
        check(
            "download_tem_vendor",
            any(n.startswith("Agro-Etiqueta-Print/app/vendor/node-") for n in names),
        )


def test_regressao_scripts() -> None:
    print("== 5) Regressão verify bridge/full ==")
    env = os.environ.copy()
    env["AGRO_PIN_TESTE"] = PIN
    env["PYTHONIOENCODING"] = "utf-8"
    for label, script, needle in (
        ("bridge_dl", "scripts/verify_etq_print_bridge_dl_path.js", "VERIFY_OK"),
        ("full_path", "scripts/verify_etq_print_direto_full_path.js", "VERIFY_OK"),
    ):
        r = subprocess.run(
            ["node", str(ROOT / script)],
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        out = (r.stdout or "") + (r.stderr or "")
        m = re.search(r"VERIFY_OK\s+(\d+)/(\d+)", out)
        if m:
            check(
                f"reg_{label}",
                r.returncode == 0 and needle in out,
                f"{m.group(1)}/{m.group(2)}",
            )
        else:
            check(f"reg_{label}", r.returncode == 0 and needle in out, out[-160:].replace("\n", " "))


def main() -> int:
    print(f"=== verify ETQ-PONTE-1CLIQUE · PIN={PIN} ===")
    test_contratos()
    test_pin()
    test_zip_layout_e_node()
    test_http_download()
    test_regressao_scripts()
    print(f"=== {len(oks)} OK · {len(fails)} FAIL ===")
    if fails:
        print("FAILS:", ", ".join(fails))
        print("PREP_FAILS=" + str(len(fails)))
        return 1
    print("PREP_FAILS=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
