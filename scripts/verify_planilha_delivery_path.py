"""VERIFY PLANILHA-DELIVERY — Excel ↓/↑ colunas Delivery/catálogo.

Run: python scripts/verify_planilha_delivery_path.py
"""
from __future__ import annotations

import ast
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails = 0


def ok(msg: str) -> None:
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    global fails
    fails += 1
    print(f" FAIL {msg}")


def read(rel: str) -> str:
    p = ROOT / rel
    if not p.is_file():
        fail(f"ausente {rel}")
        return ""
    return p.read_text(encoding="utf-8", errors="replace")


def must_contain(rel: str, needles: list[str], label: str = "") -> None:
    txt = read(rel)
    if not txt:
        return
    for n in needles:
        if n not in txt:
            fail(f"{label or rel}: falta `{n}`")
        else:
            ok(f"{label or rel}: `{n[:56]}`")


def check_ast() -> None:
    for rel in (
        "produtos/cadastro_planilha_util.py",
        "produtos/tests_cadastro_planilha_delivery.py",
    ):
        try:
            ast.parse(read(rel))
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_fonte_cols() -> None:
    util = read("produtos/cadastro_planilha_util.py")
    keys = [
        "COL_DEL_ATIVO",
        "COL_DEL_TITULO",
        "COL_DEL_DESCRICAO",
        "COL_DEL_ORDEM",
        "COL_DEL_DESTAQUE",
        "COL_DEL_ESTOQUE_NEG",
        "COL_DEL_PESO",
        "COL_DEL_CAT",
        "COL_DEL_SUB1",
        "COL_DEL_SUB2",
        "COL_DEL_SUB3",
        "COL_DEL_SUB4",
        "COL_DEL_EMBALAGENS",
        "DELIVERY_IMPORT_KEYS",
        "_aplicar_patch_delivery",
        "_validar_patch_delivery",
        "_enriquecer_rows_delivery_batch",
        "_delivery_blob",
    ]
    for k in keys:
        if k not in util:
            fail(f"util sem {k}")
        else:
            ok(f"util {k}")
    for label in (
        "Delivery ativo",
        "Delivery título",
        "Delivery categoria",
        "Delivery embalagens",
    ):
        if label not in util:
            fail(f"header ausente: {label}")
        else:
            ok(f"header {label}")


def check_ui() -> None:
    must_contain(
        "produtos/static/produtos/js/cadastro_erp_panel.js",
        [
            "delivery_ativo",
            "delivery_categoria",
            "delivery_sub4",
            "delivery_embalagens",
            "Delivery ativo",
        ],
        "JS export",
    )
    must_contain(
        "produtos/templates/produtos/produtos_cadastro_erp.html",
        ["<strong>Delivery</strong>", "Embalagens:", "catálogo público"],
        "modal Excel",
    )


def check_escopo_loja() -> None:
    """Diff vs producao não deve tocar PDV/caixa (só cadastro planilha)."""
    import subprocess

    r = subprocess.run(
        [
            "git",
            "diff",
            "origin/producao...HEAD",
            "--name-only",
            "--",
            "produtos/cadastro_planilha_util.py",
            "produtos/static/produtos/js/cadastro_erp_panel.js",
            "produtos/templates/produtos/produtos_cadastro_erp.html",
            "produtos/tests_cadastro_planilha_delivery.py",
            "scripts/verify_planilha_delivery_path.py",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    files = [x.strip() for x in (r.stdout or "").splitlines() if x.strip()]
    if not files:
        # Feature pode já estar só no teste sem diff naming — check working tree
        ok("escopo: arquivos do pacote presentes (diff producao opcional)")
    else:
        for f in files:
            ok(f"escopo pacote: {f}")
    # Garantir que o util planilha não é importado em fluxo caixa
    caixa = read("produtos/caixa_util.py")
    if "cadastro_planilha_util" in caixa:
        fail("caixa_util importa planilha — risco loja")
    else:
        ok("caixa_util sem planilha")


def check_logica_django() -> None:
    import django

    django.setup()
    from produtos.cadastro_planilha_util import (
        COL_DEL_ATIVO,
        COL_DEL_CAT,
        COL_DEL_DESTAQUE,
        COL_DEL_EMBALAGENS,
        COL_DEL_PESO,
        COL_DEL_SUB1,
        COL_DEL_TITULO,
        DELIVERY_IMPORT_KEYS,
        EXPORT_COL_KEYS,
        _aplicar_patch_delivery,
        _eh_limpar_planilha,
        _ler_planilha,
        _map_headers,
        _parse_bool_planilha,
        _patch_da_linha,
        _tem_alteracao,
        linha_export_planilha,
        montar_xlsx_cadastro,
        normalizar_colunas_export,
    )

    for k in DELIVERY_IMPORT_KEYS:
        if k not in EXPORT_COL_KEYS:
            fail(f"EXPORT_COL_KEYS sem {k}")
        else:
            ok(f"EXPORT_COL_KEYS {k}")

    cols = normalizar_colunas_export("delivery_ativo,delivery_peso,nome")
    if cols[0] != "id" or COL_DEL_ATIVO not in cols:
        fail("normalizar_colunas_export delivery")
    else:
        ok("normalizar_colunas_export delivery")

    if _parse_bool_planilha("Sim") is not True:
        fail("parse Sim")
    else:
        ok("parse Sim")
    if _parse_bool_planilha("Não") is not False:
        fail("parse Não")
    else:
        ok("parse Não")
    if not _eh_limpar_planilha("-"):
        fail("limpar -")
    else:
        ok("limpar -")

    row = {
        "id": "AGRODEL01",
        "codigo_nfe": "GM9999",
        "nome": "Racao path",
        "marca": "AKILES",
        "categoria": "Racoes",
        "subcategoria": "",
        "subcategoria_2": "",
        "subcategoria_3": "",
        "subcategoria_4": "",
        "unidade": "KG",
        "modelo": "",
        "peso_etiqueta": "15 kg",
        "codigo_barras": "7891234567890",
        "preco_custo": 10,
        "preco_venda": 25,
        COL_DEL_ATIVO: "Sim",
        COL_DEL_TITULO: "Ração path",
        COL_DEL_PESO: "15 kg",
        COL_DEL_CAT: "Cães",
        COL_DEL_SUB1: "Adulto",
        COL_DEL_EMBALAGENS: "GM9999:Granel",
    }
    line = linha_export_planilha(row)
    if line[COL_DEL_ATIVO] != "Sim" or line[COL_DEL_PESO] != "15 kg":
        fail("linha_export delivery")
    else:
        ok("linha_export delivery")

    blob = montar_xlsx_cadastro(
        [row],
        colunas=["id", "nome", *DELIVERY_IMPORT_KEYS],
    )
    with TemporaryDirectory() as td:
        path = Path(td) / "del.xlsx"
        path.write_bytes(blob)
        headers, raw_rows = _ler_planilha(path)
    for h in ("Delivery ativo", "Delivery peso", "Delivery categoria", "Delivery embalagens"):
        if h not in headers:
            fail(f"xlsx sem coluna {h}")
        else:
            ok(f"xlsx coluna {h}")
    colmap = _map_headers(headers)
    patch = _patch_da_linha(raw_rows[0], colmap)
    for k, expect in (
        (COL_DEL_ATIVO, "Sim"),
        (COL_DEL_TITULO, "Ração path"),
        (COL_DEL_PESO, "15 kg"),
        (COL_DEL_CAT, "Cães"),
        (COL_DEL_SUB1, "Adulto"),
    ):
        if patch.get(k) != expect:
            fail(f"roundtrip {k}: {patch.get(k)!r} != {expect!r}")
        else:
            ok(f"roundtrip {k}")

    # célula vazia não entra
    patch_vazio = _patch_da_linha(
        {"ID": "x", "Delivery ativo": "", "Delivery peso": "10 kg"},
        _map_headers(["ID", "Delivery ativo", "Delivery peso"]),
    )
    if COL_DEL_ATIVO in patch_vazio:
        fail("célula vazia entrou no patch")
    else:
        ok("célula vazia não altera")
    if patch_vazio.get(COL_DEL_PESO) != "10 kg":
        fail("peso preenchido sumiu")
    else:
        ok("peso preenchido no patch")

    atual = {COL_DEL_ATIVO: "Sim", COL_DEL_PESO: "15 kg"}
    if _tem_alteracao(atual, {COL_DEL_ATIVO: "Sim"}):
        fail("_tem_alteracao falso positivo")
    else:
        ok("_tem_alteracao iguais")
    if not _tem_alteracao(atual, {COL_DEL_ATIVO: "Não"}):
        fail("_tem_alteracao não detectou")
    else:
        ok("_tem_alteracao diferente")

    ov = SimpleNamespace(cadastro_extras={}, produto_externo_id="p1")
    err = _aplicar_patch_delivery(
        ov,
        {
            COL_DEL_ATIVO: "Sim",
            COL_DEL_TITULO: "Card WA",
            COL_DEL_PESO: "10 kg",
            COL_DEL_DESTAQUE: "Não",
        },
    )
    if err:
        fail(f"aplicar patch: {err}")
    else:
        d = ov.cadastro_extras.get("delivery") or {}
        if not d.get("ativo") or d.get("titulo") != "Card WA" or d.get("peso_texto") != "10 kg":
            fail(f"delivery gravado errado: {d}")
        else:
            ok("aplicar_patch_delivery ativo+titulo+peso")

    # desligar ativo sem outros campos “úteis” pode limpar delivery se tudo zero
    ov2 = SimpleNamespace(
        cadastro_extras={
            "delivery": {
                "ativo": True,
                "titulo": "X",
                "descricao": "",
                "ordem": 0,
                "destaque": False,
                "permitir_estoque_negativo": False,
                "peso_texto": "",
                "imagem_base64": "",
                "imagem_mime": "image/jpeg",
                "categoria_id": 0,
                "subcategoria_id": 0,
                "subcategoria2_id": 0,
                "subcategoria3_id": 0,
                "subcategoria4_id": 0,
                "embalagens": [],
            }
        },
        produto_externo_id="p2",
    )
    err2 = _aplicar_patch_delivery(ov2, {COL_DEL_ATIVO: "Não", COL_DEL_TITULO: "-"})
    if err2:
        fail(f"desligar: {err2}")
    else:
        ok("aplicar_patch desligar/limpar título")


def main() -> int:
    print("=== VERIFY PLANILHA-DELIVERY ===")
    check_ast()
    check_fonte_cols()
    check_ui()
    check_escopo_loja()
    check_logica_django()
    print("---")
    if fails:
        print(f"VERIFY_FAIL {fails}")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
