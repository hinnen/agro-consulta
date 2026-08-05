#!/usr/bin/env python
"""Smoke: hotfix MODAL-UTF8 seguro para loja aberta. VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import re
import subprocess
import sys


PROD = "origin/producao"
HOT = "origin/deploy/hotfix-modal-utf8-v13.83"
MODAL = "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html"


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def show(ref: str, path: str) -> str:
    return subprocess.check_output(["git", "show", f"{ref}:{path}"], stderr=subprocess.STDOUT).decode(
        "utf-8"
    )


def main() -> None:
    # 1) só 2 arquivos mudam vs loja
    names = (
        subprocess.check_output(["git", "diff", "--name-only", f"{PROD}...{HOT}"])
        .decode()
        .strip()
        .splitlines()
    )
    if set(names) != {"VERSION", MODAL}:
        fail(f"diff inesperado vs loja: {names}")

    ver = subprocess.check_output(["git", "show", f"{HOT}:VERSION"]).decode().strip()
    if ver != "13.83":
        fail(f"VERSION hotfix={ver!r} esperado 13.83")

    live = show(PROD, MODAL)
    fix = show(HOT, MODAL)

    if "\u251c" not in live:
        fail("loja atual sem mojibake? esperado ├ no modal live")
    if "\u251c" in fix:
        fail("hotfix ainda tem caractere ├ (mojibake)")

    # 2) acentos legíveis
    for need in (
        "Preços/Margem",
        "Composição",
        "Marcas/Cód.",
        "são obrigatórios",
        "Custo + Acrésc.",
        "Preço de Venda Final",
        "Comissão (R$)",
        "Excluir (só SisVale)",
        "Alterações",
    ):
        if need not in fix:
            fail(f"hotfix sem texto limpo: {need}")
        if need in live:
            fail(f"loja live já tem texto limpo inesperado: {need}")

    # 3) path CUSTO-FAMILIA intacto (ids / hooks) — igual na loja e no hotfix
    for need in (
        "edit-cf-ativo",
        "edit-cf-baixa-estoque",
        "edit-kit-ativo",
        "edit-kit-campos",
        "coletarCustoFamiliaPayload",
        "baixa_estoque_saco",
        "custo_familia",
        "comp-btn-buscar",
        "atualizarKitCamposVisivel",
        "origem === 'custo_familia'",
        "api/produtos/custo-familia/propagar",
    ):
        if need not in fix:
            fail(f"hotfix perdeu hook CF: {need}")
        if need not in live:
            fail(f"loja live sem hook CF (deploy CF ausente?): {need}")

    # 4) sem regressão estrutural: mesmos ids de input principais
    id_re = re.compile(r'\bid="([^"]+)"')
    ids_live = set(id_re.findall(live))
    ids_fix = set(id_re.findall(fix))
    only_live = sorted(ids_live - ids_fix)
    only_fix = sorted(ids_fix - ids_live)
    if only_live or only_fix:
        fail(f"ids divergem live-only={only_live[:10]} fix-only={only_fix[:10]}")

    # 5) diff "sem acento" quase idêntico: remove ├* e compara comprimento relativo
    # Normaliza mojibake live via cp850 → utf8 e compara ao hotfix
    try:
        live_norm = live.encode("cp850").decode("utf-8")
    except UnicodeEncodeError as e:
        fail(f"não deu para normalizar live via cp850: {e}")
    if live_norm != fix:
        # permitir só diferença de newline
        a = live_norm.replace("\r\n", "\n")
        b = fix.replace("\r\n", "\n")
        if a != b:
            # achar 1º ponto
            for i, (ca, cb) in enumerate(zip(a, b)):
                if ca != cb:
                    fail(
                        f"hotfix != reverse(cp850 live) @ {i}: "
                        f"{a[max(0,i-20):i+20]!r} vs {b[max(0,i-20):i+20]!r}"
                    )
            if len(a) != len(b):
                fail(f"hotfix len {len(b)} != live_norm {len(a)}")

    # 6) util CF ainda no tip do hotfix (mesmo tree producao + modal)
    for path in (
        "produtos/custo_familia_util.py",
        "produtos/composicao_kit_util.py",
        "scripts/verify_custo_familia.py",
    ):
        subprocess.check_output(["git", "cat-file", "-e", f"{HOT}:{path}"])

    print("VERIFY_OK")


if __name__ == "__main__":
    main()
