#!/usr/bin/env python
"""Path Contabilidade NFC-e Centro × Vila + reforço numeração PK.

Uso: python scripts/verify_contabilidade_nfce_loja_path.py
"""
from __future__ import annotations

import ast
import inspect
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def ok(msg: str = "") -> None:
    global oks
    oks += 1
    if msg:
        print(f"  OK  {msg}")


def fail(msg: str) -> None:
    fails.append(msg)
    print(f"  FAIL  {msg}")


def check_file(path: str, *needles: str, label: str = "") -> None:
    p = ROOT / path
    if not p.exists():
        fail(f"MISSING {path}")
        return
    text = p.read_text(encoding="utf-8", errors="replace")
    for n in needles:
        if n not in text:
            fail(f"{label or path} missing {n!r}")
        else:
            ok()


def main() -> int:
    print("=== CTB-NFCE-LOJA + NFCE-SEQ path ===")

    # --- UI Contabilidade ---
    check_file(
        "produtos/templates/produtos/contabilidade_painel.html",
        "ctb-loja-btn",
        'data-loja="centro"',
        'data-loja="vila"',
        'data-loja="todas"',
        "ctb-loja",
        "Qual loja baixar?",
        "qsPeriodo",
        "loja=",
        "nomeArquivo",
        "setLoja",
        label="painel",
    )
    check_file(
        "produtos/templates/produtos/contabilidade_painel.html",
        "is-on",
        "CNPJ /0001",
        "CNPJ /0002",
        label="painel-ux",
    )

    # --- Util filtro ---
    check_file(
        "produtos/nfce_contabilidade_util.py",
        "normalizar_loja_filtro",
        "rotulo_loja_filtro",
        "emitente_cnpj",
        'loja_n == "centro"',
        'Q(emitente_cnpj="")',
        '"loja"',
        "montar_zip_nfce_mes",
        "NFC-e{pasta_loja}",
        label="util",
    )

    # --- Views API ---
    check_file(
        "produtos/views_nfce.py",
        "_parse_loja_request",
        "normalizar_loja_filtro",
        "rotulo_loja_filtro",
        "resumo_nfce_mes(ano, mes, loja)",
        "montar_zip_nfce_mes(ano, mes, loja)",
        "pendencias_nfce_csv_bytes(ano, mes, loja)",
        "planilha_nfce_csv_bytes(ano, mes, loja)",
        'filename="nfce-xml{suf}',
        label="views",
    )

    # --- Rotas ---
    check_file(
        "produtos/urls.py",
        "api_nfce_contabilidade_resumo",
        "api_nfce_export_xml_zip",
        "contabilidade_painel",
        label="urls",
    )

    # --- Hotfix sequência PK Vila ---
    check_file(
        "produtos/nfce_sp_emissao_util.py",
        "_sync_nfcenumeracao_pk_sequence",
        "_get_or_create_numeracao",
        "IntegrityError",
        "pg_get_serial_sequence",
        "produtos_nfcenumeracaoagro",
        label="nfce-seq",
    )

    # --- AST: assinaturas com loja ---
    util_src = (ROOT / "produtos/nfce_contabilidade_util.py").read_text(encoding="utf-8")
    tree = ast.parse(util_src)
    want = {
        "resumo_nfce_mes": True,
        "linhas_planilha_nfce_mes": True,
        "montar_zip_nfce_mes": True,
        "planilha_nfce_csv_bytes": True,
        "planilha_nfce_xlsx_bytes": True,
        "pendencias_nfce_csv_bytes": True,
        "pendencias_nfce_resumo_json": True,
    }
    found = {k: False for k in want}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in want:
            args = [a.arg for a in node.args.args]
            if "loja" in args or any(
                kw.arg == "loja" for kw in (node.args.kwonlyargs or [])
            ):
                found[node.name] = True
                ok(f"assinatura {node.name}(loja)")
            else:
                # kwonly
                kwonly = [a.arg for a in node.args.kwonlyargs]
                if "loja" in kwonly or "loja" in args:
                    found[node.name] = True
                    ok(f"assinatura {node.name}(loja)")
                else:
                    fail(f"{node.name} sem parâmetro loja")
    for name, ok_flag in found.items():
        if not ok_flag:
            fail(f"função {name} não encontrada/sem loja")

    # --- Runtime leve (sem DB se falhar) ---
    try:
        import os
        import sys as _sys

        if str(ROOT) not in _sys.path:
            _sys.path.insert(0, str(ROOT))
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
        import django

        django.setup()
        from produtos.nfce_contabilidade_util import (
            normalizar_loja_filtro,
            rotulo_loja_filtro,
            PLANILHA_CABECALHO,
            PENDENCIAS_CABECALHO,
            resumo_nfce_mes,
            linhas_planilha_nfce_mes,
        )
        from produtos.nfce_sp_emissao_util import (
            _get_or_create_numeracao,
            _sync_nfcenumeracao_pk_sequence,
        )

        assert normalizar_loja_filtro("VILA") == "vila"
        assert normalizar_loja_filtro("ambas") == "todas"
        assert rotulo_loja_filtro("centro") == "Centro"
        assert "loja" in PLANILHA_CABECALHO
        assert "emitente_cnpj" in PLANILHA_CABECALHO
        assert "loja" in PENDENCIAS_CABECALHO
        ok("normalizar/rótulo/cabeçalhos")
        assert callable(_get_or_create_numeracao)
        assert callable(_sync_nfcenumeracao_pk_sequence)
        ok("helpers numeração PK")

        # resumo mês atual — não deve explodir
        from datetime import date

        hoje = date.today()
        for loja in ("centro", "vila", "todas"):
            r = resumo_nfce_mes(hoje.year, hoje.month, loja)
            assert r["loja"] == loja or (loja == "todas" and r["loja"] == "todas")
            assert "autorizadas" in r
            assert r["loja_rotulo"]
            ok(f"resumo_nfce_mes({loja})")
            rows = linhas_planilha_nfce_mes(hoje.year, hoje.month, loja)
            if loja in ("centro", "vila") and rows:
                bad = [x for x in rows if x.get("loja") not in (loja,)]
                # legado centro pode vir como centro; vila só vila
                if loja == "vila":
                    bad = [x for x in rows if x.get("loja") != "vila"]
                if bad:
                    fail(f"planilha {loja}: linhas de outra loja ({len(bad)})")
                else:
                    ok(f"planilha {loja} só da loja ({len(rows)} linhas)")
            else:
                ok(f"planilha {loja} ({len(rows)} linhas)")
    except Exception as e:
        fail(f"runtime Django: {e}")

    print(f"checks_ok={oks} fails={len(fails)}")
    for f in fails:
        print("FAIL", f)
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
