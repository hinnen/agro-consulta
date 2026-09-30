# -*- coding: utf-8 -*-
"""Prova — Excel clientes (`CLIENTE-XLSX-FIADO`). python scripts/verify_cliente_planilha_path.py"""
from __future__ import annotations

import os
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client

from produtos.cliente_planilha_util import (
    COL_ID,
    COL_LIMITE_FIADO,
    COL_MEDIA_FIADO_3M,
    COL_MES_MAIS_FIADO,
    COL_NOME,
    EXPORT_ONLY,
    IMPORT_EDIT_KEYS,
    aplicar_importacao_clientes,
    coletar_linhas_export_clientes,
    montar_xlsx_clientes,
    preview_importacao_clientes,
)
from produtos.models import ClienteAgro

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_arquivos() -> None:
    print("== Contratos UI / URLs ==")
    html = (ROOT / "produtos/templates/produtos/clientes_lista.html").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    util = (ROOT / "produtos/cliente_planilha_util.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views_cliente_planilha.py").read_text(encoding="utf-8")

    check("btn_export", 'id="clientes-btn-export-xlsx"' in html and "Excel ↓" in html)
    check("btn_import", 'id="clientes-btn-import-xlsx"' in html and "Excel ↑" in html)
    check("modal_import", 'id="clientes-import-modal"' in html)
    check("previa_btn", 'id="clientes-import-previa-btn"' in html)
    check("confirmar_btn", 'id="clientes-import-confirmar"' in html)
    check("url_export", "api/clientes/export-xlsx/" in urls)
    check("url_preview", "api/clientes/import-preview/" in urls)
    check("url_aplicar", "api/clientes/import-aplicar/" in urls)
    check("view_export", "def api_clientes_export_xlsx" in views)
    check("col_media_3m", COL_MEDIA_FIADO_3M in util)
    check("col_mes_mais", COL_MES_MAIS_FIADO in util)
    check("export_only_fiado", COL_MEDIA_FIADO_3M in EXPORT_ONLY and COL_MES_MAIS_FIADO in EXPORT_ONLY)
    check("limite_editavel", COL_LIMITE_FIADO in IMPORT_EDIT_KEYS)
    check("aba_como_usar", '"Como usar"' in util or "'Como usar'" in util)


def test_export_xlsx_bytes() -> None:
    print("== Export XLSX ==")
    rows = coletar_linhas_export_clientes()[:5]
    if not rows:
        cli = ClienteAgro.objects.create(nome="Cliente Prova Planilha")
        rows = coletar_linhas_export_clientes()[:5]
        cli.delete()
    data = montar_xlsx_clientes(rows)
    check("xlsx_bytes", len(data) > 2000, f"{len(data)} bytes")
    tmp = Path(tempfile.mkdtemp()) / "t.xlsx"
    tmp.write_bytes(data)
    from openpyxl import load_workbook

    wb = load_workbook(tmp)
    ws = wb["Clientes"]
    check("sheet_clientes", ws.title == "Clientes")
    check("header_nome", ws.cell(1, 3).value in ("Nome",) or "Nome" in str(ws.cell(1, 1).value))
    hdrs = [ws.cell(1, c).value for c in range(1, 30)]
    check("hdr_media", "Média fiado (3 meses)" in hdrs)
    check("hdr_mes", "Mês que mais comprou fiado" in hdrs)
    check("hdr_valor_mes", "Valor do mês que mais comprou" in hdrs)
    check("hdr_valor_ant", "Valor mês anterior" in hdrs)
    check("hdr_limite", "Limite fiado" in hdrs)
    check("aba_instr", "Como usar" in wb.sheetnames)
    wb.close()
    tmp.unlink(missing_ok=True)


def test_import_roundtrip() -> None:
    print("== Import prévia + aplicar ==")
    cli = ClienteAgro.objects.create(
        nome="ZZ Prova Excel Cliente",
        whatsapp="11999990001",
        limite_fiado_local=Decimal("0"),
    )
    try:
        rows = coletar_linhas_export_clientes()
        row = next(r for r in rows if r[COL_ID] == cli.pk)
        row[COL_LIMITE_FIADO] = 150.0
        row[COL_NOME] = "ZZ Prova Excel Cliente Editado"
        data = montar_xlsx_clientes([row])
        tmp = Path(tempfile.mkdtemp()) / "imp.xlsx"
        tmp.write_bytes(data)

        prev = preview_importacao_clientes(tmp)
        check("prev_ok", prev.get("n_alteracoes", 0) >= 1, f"alt={prev.get('n_alteracoes')}")
        check("prev_limite", any(
            c.get("campo") == COL_LIMITE_FIADO for a in prev.get("alteracoes", []) for c in a.get("campos", [])
        ))

        User = get_user_model()
        user = User.objects.filter(is_superuser=True).first() or User.objects.first()
        r = aplicar_importacao_clientes(tmp, user, nome_arquivo="prova.xlsx")
        check("apply_ok", r.get("clientes_alterados", 0) >= 1)

        cli.refresh_from_db()
        check("nome_gravado", cli.nome == "ZZ Prova Excel Cliente Editado")
        check("limite_gravado", Decimal(str(cli.limite_fiado_local)) == Decimal("150.00"))
        check("editado_local", cli.editado_local is True)
        tmp.unlink(missing_ok=True)
    finally:
        cli.delete()


def test_http_endpoints() -> None:
    print("== HTTP (login) ==")
    from django.conf import settings

    if "testserver" not in settings.ALLOWED_HOSTS:
        settings.ALLOWED_HOSTS = [*settings.ALLOWED_HOSTS, "testserver"]
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first()
    if not user:
        check("http_skip", True, "sem user")
        return
    c = Client()
    c.force_login(user)
    r = c.get("/api/clientes/export-xlsx/")
    check("http_export_200", r.status_code == 200, str(r.status_code))
    check(
        "http_export_xlsx",
        "spreadsheetml" in (r.get("Content-Type") or ""),
        r.get("Content-Type", ""),
    )
    r2 = c.post("/api/clientes/import-preview/")
    check("http_preview_400_sem_arquivo", r2.status_code == 400)


def main() -> int:
    test_arquivos()
    test_export_xlsx_bytes()
    test_import_roundtrip()
    test_http_endpoints()
    print(f"\n== {len(oks)} OK · {len(fails)} FAIL ==")
    if fails:
        for f in fails:
            print("  -", f)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
