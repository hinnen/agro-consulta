"""VERIFY PLANILHA-MED — Excel ↓/↑ colunas busca medicamentos (Agro).

Run: AGRO_PIN_TESTE=9973 .venv/bin/python scripts/verify_planilha_med_path.py
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
PIN_ENV = (os.environ.get("AGRO_PIN_TESTE") or "").strip()


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


def check_ast() -> None:
    for rel in (
        "produtos/cadastro_planilha_util.py",
        "produtos/tests_cadastro_planilha_med.py",
        "scripts/verify_planilha_med_path.py",
    ):
        try:
            ast.parse(read(rel))
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_fonte_cols() -> None:
    util = read("produtos/cadastro_planilha_util.py")
    keys = [
        "COL_MED_PALAVRAS",
        "COL_MED_CLASSES",
        "COL_MED_ESPECIES",
        "MED_IMPORT_KEYS",
        "_aplicar_patch_med",
        "_validar_patch_med",
        "_enriquecer_rows_med_batch",
        "_med_busca_blob",
        "sincronizar_busca_agro_no_mongo",
    ]
    for k in keys:
        if k not in util:
            fail(f"util sem {k}")
        else:
            ok(f"util {k}")
    for label in (
        "Med palavras-chave",
        "Med classes (slugs)",
        "Med espécies (slugs)",
    ):
        if label not in util:
            fail(f"header ausente: {label}")
        else:
            ok(f"header {label}")
    js = read("produtos/static/produtos/js/cadastro_erp_panel.js")
    if "palavras_chave', label: 'Med palavras-chave'" not in js:
        fail("cadastro_erp_panel.js sem colunas MED export")
    else:
        ok("panel export MED")


def check_isolamento_loja() -> None:
    caixa = read("produtos/caixa_util.py")
    if "cadastro_planilha_util" in caixa:
        fail("caixa_util importa planilha")
    else:
        ok("caixa_util sem planilha")


def check_logica_django() -> None:
    import django

    django.setup()
    from produtos.cadastro_planilha_util import (
        COL_MED_CLASSES,
        COL_MED_ESPECIES,
        COL_MED_PALAVRAS,
        EXPORT_COL_KEYS,
        IMPORT_KEYS,
        MED_IMPORT_KEYS,
        _aplicar_patch_med,
        _enriquecer_row_med_planilha,
        _ler_planilha,
        _map_headers,
        _patch_da_linha,
        _tem_alteracao,
        linha_export_planilha,
        montar_xlsx_cadastro,
        normalizar_colunas_export,
    )
    from produtos.models import ProdutoGestaoOverlayAgro

    for k in MED_IMPORT_KEYS:
        if k not in EXPORT_COL_KEYS or k not in IMPORT_KEYS:
            fail(f"keys export/import {k}")
        else:
            ok(f"keys {k}")

    cols = normalizar_colunas_export("palavras_chave,classes_vet,nome")
    if cols[0] != "id" or COL_MED_PALAVRAS not in cols:
        fail("normalizar_colunas_export med")
    else:
        ok("normalizar_colunas_export med")

    row = {
        "id": "AGROMED01",
        "nome": "Prod med path",
        COL_MED_PALAVRAS: "Dexon Biodex",
        COL_MED_CLASSES: ["antibiotico", "vermifugo"],
        COL_MED_ESPECIES: ["caes"],
    }
    line = linha_export_planilha(row)
    if line[COL_MED_CLASSES] != "antibiotico, vermifugo":
        fail(f"linha_export classes: {line[COL_MED_CLASSES]!r}")
    else:
        ok("linha_export classes slugs")

    blob = montar_xlsx_cadastro(
        [row],
        colunas=["id", "nome", *MED_IMPORT_KEYS],
    )
    with TemporaryDirectory() as td:
        path = Path(td) / "med.xlsx"
        path.write_bytes(blob)
        headers, raw_rows = _ler_planilha(path)
    for h in ("Med palavras-chave", "Med classes (slugs)", "Med espécies (slugs)"):
        if h not in headers:
            fail(f"xlsx sem coluna {h}")
        else:
            ok(f"xlsx coluna {h}")

    colmap = _map_headers(headers)
    patch = _patch_da_linha(raw_rows[0], colmap)
    if patch.get(COL_MED_PALAVRAS) != "Dexon Biodex":
        fail("roundtrip palavras")
    else:
        ok("roundtrip palavras")
    if patch.get(COL_MED_CLASSES) != ["antibiotico", "vermifugo"]:
        fail(f"roundtrip classes {patch.get(COL_MED_CLASSES)!r}")
    else:
        ok("roundtrip classes")

    patch_vazio = _patch_da_linha(
        {"ID": "x", "Med palavras-chave": "", "Med classes (slugs)": "antibiotico"},
        _map_headers(["ID", "Med palavras-chave", "Med classes (slugs)"]),
    )
    if COL_MED_PALAVRAS in patch_vazio:
        fail("célula vazia palavras entrou no patch")
    else:
        ok("célula vazia não apaga palavras")
    if patch_vazio.get(COL_MED_CLASSES) != ["antibiotico"]:
        fail("patch classes após vazio palavras")
    else:
        ok("patch classes ok com palavras vazia")

    patch_bad = _patch_da_linha(
        {"ID": "x", "Med classes (slugs)": "antibiotico, slug-falso-xyz"},
        _map_headers(["ID", "Med classes (slugs)"]),
    )
    if "__erro_classes_vet" not in patch_bad:
        fail("slug inválido deveria gerar erro")
    else:
        ok("slug inválido rejeitado")

    ov = ProdutoGestaoOverlayAgro(produto_externo_id="tmp-verify-med")
    ov.cadastro_extras = {
        "palavras_chave": "Antigo",
        "classes_vet": ["vermifugo"],
        "especies_vet": ["bovinos"],
    }
    err = _aplicar_patch_med(ov, {COL_MED_CLASSES: ["antibiotico"]})
    if err:
        fail(f"aplicar_patch_med: {err}")
    else:
        ex = ov.cadastro_extras
        if ex.get("palavras_chave") != "Antigo" or ex.get("especies_vet") != ["bovinos"]:
            fail("patch parcial apagou outros campos MED")
        elif ex.get("classes_vet") != ["antibiotico"]:
            fail("patch classes não aplicou")
        else:
            ok("patch parcial preserva extras")

    atual = {"palavras_chave": "A", "classes_vet": ["antibiotico"], "especies_vet": ["caes"]}
    if _tem_alteracao(atual, {COL_MED_PALAVRAS: "A"}):
        fail("tem_alteracao falso positivo palavras")
    else:
        ok("tem_alteracao palavras igual")

    enrich_row: dict = {"id": "e1"}
    _enriquecer_row_med_planilha(enrich_row, ov)
    cv_en = enrich_row.get("classes_vet")
    if cv_en != ["antibiotico"]:
        fail(f"enriquecer row med: {cv_en!r}")
    else:
        ok("enriquecer row med")


def check_pin() -> None:
    if not PIN_ENV:
        print("  SKIP pin (AGRO_PIN_TESTE)")
        return
    import django

    django.setup()
    from base.models import PerfilUsuario
    from django.contrib.auth import get_user_model
    from produtos.caixa_util import validar_pin_operador

    User = get_user_model()
    user, _ = User.objects.get_or_create(
        username="pin_prova_planilha_med",
        defaults={"is_staff": True, "is_superuser": True, "is_active": True},
    )
    user.set_password("prova-pin-planilha-med")
    user.is_active = True
    user.save()
    perfil = PerfilUsuario.objects.filter(senha_rapida=PIN_ENV).first()
    if not perfil:
        cod = PIN_ENV[:4] if len(PIN_ENV) >= 4 else "9901"
        while PerfilUsuario.objects.filter(codigo_vendedor=cod).exists():
            cod = f"9{PerfilUsuario.objects.count():03d}"[:4]
        perfil = PerfilUsuario.objects.create(
            usuario=user,
            codigo_vendedor=cod,
            senha_rapida=PIN_ENV,
            ativo=True,
        )
    else:
        perfil.senha_rapida = PIN_ENV
        perfil.ativo = True
        perfil.save()
    ok_pin, msg = validar_pin_operador(PIN_ENV)
    if ok_pin:
        ok(f"PIN {PIN_ENV} validado")
    else:
        fail(f"PIN: {msg}")


def check_django_tests() -> None:
    import subprocess

    r = subprocess.run(
        [
            sys.executable,
            "-m",
            "django",
            "test",
            "produtos.tests_cadastro_planilha_med",
            "--settings=config.settings",
            "-v0",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        fail(f"django test med: {r.stderr[-400:]}")
    elif "OK" not in r.stdout and r.returncode == 0:
        ok("django test med (10)")
    else:
        ok("django test med")


def main() -> int:
    print("VERIFY PLANILHA-MED\n")
    check_ast()
    check_fonte_cols()
    check_isolamento_loja()
    check_logica_django()
    check_pin()
    check_django_tests()
    print(f"\nVERIFY_MED_FAILS={fails}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
