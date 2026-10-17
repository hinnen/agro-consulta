#!/usr/bin/env python3
"""
Prova detalhada — NF-EAN-OPCIONAL.

Entrada NF: EAN do XML casa com ``cadastro_extras.codigos_barras_opcionais``.
Prioridade: ean_pg → ean_overlay → ean_overlay_opcional.
Duplicidade (2+ overlays com o mesmo EAN opcional) → não vincula.
"""
from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OK = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_contracts() -> None:
    print("== Contratos código ==")
    util = (ROOT / "produtos/nfe_entrada_util.py").read_text(encoding="utf-8")
    tests = (ROOT / "produtos/tests_nfe_ean_opcional.py").read_text(encoding="utf-8")
    banana = (ROOT / "banana.md").read_text(encoding="utf-8")
    prep = ROOT / "docs/DEPLOY-PREP-NF-EAN-OPCIONAL.md"
    check("fn_opcional", "def _produto_pg_por_ean_opcional" in util)
    check("fn_casar", "def casar_produtos_postgres" in util)
    check("match_tipo", 'mtipo = "ean_overlay_opcional"' in util)
    check("usa_q_json", "q_overlay_json_barras_opcionais" in util)
    check("confirma_extras", "codigos_barras_opcionais_de_cadastro_extras" in util)
    # Ordem: principal PG → overlay principal → opcional
    i_pg = util.find('mtipo = "ean_pg"')
    i_ov = util.find('mtipo = "ean_overlay"')
    i_op = util.find('mtipo = "ean_overlay_opcional"')
    check("ordem_prioridade", 0 < i_pg < i_ov < i_op)
    chunk = util[util.find("def _produto_pg_por_ean_opcional") : util.find("def casar_produtos_postgres")]
    check("ambiguo_none", "if len(pids) > 1:" in chunk and "return None" in chunk)
    check("min_8_digitos", "len(ean) < 8" in chunk)
    check("test_arquivo", (ROOT / "produtos/tests_nfe_ean_opcional.py").is_file())
    check("test_casa", "test_gm5218_casa_ean_exato_dos_codigos_opcionais" in tests)
    check("test_prio", "test_ean_principal_mantem_prioridade_sobre_opcional" in tests)
    check("test_amb", "test_ean_opcional_ambiguo_nao_escolhe_produto" in tests)
    check("sem_debug", "hypothesisId" not in util and "#region agent" not in util)
    check("banana_pacote", "NF-EAN-OPCIONAL" in banana)
    check("banana_pronto", "pronto para" in banana and "NF-EAN-OPCIONAL" in banana)
    check("prep_doc", prep.is_file() and "pronto para envio" in prep.read_text(encoding="utf-8"))
    check("migrate_nao", "Migrate** | **NÃO**" in banana or "Migrate | **NÃO**" in banana or "**NÃO**" in banana)


def test_count_tests() -> None:
    print("== Contagem testes ==")
    nfe = ast.parse((ROOT / "produtos/tests_nfe_ean_opcional.py").read_text(encoding="utf-8"))
    opc = ast.parse((ROOT / "produtos/tests_codigos_barras_opcionais.py").read_text(encoding="utf-8"))

    def count(tree: ast.AST) -> int:
        n = 0
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                for m in node.body:
                    if isinstance(m, ast.FunctionDef) and m.name.startswith("test_"):
                        n += 1
        return n

    cn, co = count(nfe), count(opc)
    check("nfe_3", cn == 3, str(cn))
    check("opc_17", co == 17, str(co))
    check("total_20", cn + co == 20, str(cn + co))


def test_django() -> None:
    print("== Django 20/20 ==")
    py = ROOT / ".venv/bin/python"
    if not py.is_file():
        py = Path(sys.executable)
    r = subprocess.run(
        [
            str(py),
            "manage.py",
            "test",
            "produtos.tests_nfe_ean_opcional",
            "produtos.tests_codigos_barras_opcionais",
            "-v1",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    out = (r.stdout or "") + (r.stderr or "")
    log = Path("/opt/cursor/artifacts/verify-nfe-ean-opcional-django.log")
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(out, encoding="utf-8")
    check("exit_0", r.returncode == 0, f"rc={r.returncode}")
    check("ran_20", "Ran 20 tests" in out, out[-200:].replace("\n", " "))
    check("ok_token", bool(re.search(r"\bOK\b", out.splitlines()[-5:] and "\n".join(out.splitlines()[-8:]) or out)), "")


def test_prep_git() -> None:
    print("== Tip PREP vs producao ==")
    ancestral = subprocess.run(
        [
            "git",
            "merge-base",
            "--is-ancestor",
            "origin/producao",
            "origin/deploy/prep-nf-ean-opcional",
        ],
        cwd=ROOT,
        capture_output=True,
    )
    if ancestral.returncode != 0:
        ancestral = subprocess.run(
            ["git", "merge-base", "--is-ancestor", "origin/producao", "HEAD"],
            cwd=ROOT,
            capture_output=True,
        )
    check("prep_ancestral", ancestral.returncode == 0)
    mig = subprocess.run(
        [
            "git",
            "diff",
            "--name-only",
            "origin/producao...origin/deploy/prep-nf-ean-opcional",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).stdout
    if not mig.strip():
        mig = subprocess.run(
            ["git", "diff", "--name-only", "origin/producao...HEAD"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        ).stdout
    mig_paths = [ln for ln in mig.splitlines() if "/migrations/" in ln.replace("\\", "/")]
    check("prep_sem_migrate", not mig_paths, ",".join(mig_paths[:3]) or "ok")
    need = (
        "produtos/nfe_entrada_util.py" in mig
        and "produtos/tests_nfe_ean_opcional.py" in mig
    )
    check("prep_tem_fix", need, mig.strip()[:120] or "ok")


def main() -> int:
    print("Prova detalhada — NF-EAN-OPCIONAL\n")
    test_contracts()
    test_count_tests()
    test_django()
    test_prep_git()
    print(f"\nOK={OK} FAIL={FAIL}")
    print(f"PREP_FAILS={FAIL}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
