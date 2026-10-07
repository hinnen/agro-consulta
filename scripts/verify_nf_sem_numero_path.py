"""Prova NF-SEM-NUMERO: Nº NF vazio → SEM-DDMM-XXXX (UI + backend + extrator CP).

VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

CHECKS = 0


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"OK {msg}")


def _read(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def prova_fonte() -> None:
    util = _read("produtos/nfe_entrada_util.py")
    views = _read("produtos/views.py")
    html = _read("produtos/templates/produtos/entrada_nota.html")

    if "def gerar_numero_nf_sem_nota" not in util:
        fail("util sem gerar_numero_nf_sem_nota")
    if "def garantir_numero_nf_cabecalho" not in util:
        fail("util sem garantir_numero_nf_cabecalho")
    if 'return f"SEM-{ddmm}-{suf}"' not in util:
        fail("formato SEM-DDMM-XXXX ausente")
    ok("util: gera SEM-DDMM-XXXX")

    if "cab_norm = garantir_numero_nf_cabecalho" not in util:
        fail("salvar_rascunho sem garantir")
    if "existente=str(cab_prev.get(\"numero\")" not in util and "existente=str(cab_prev.get('numero')" not in util:
        if "existente=" not in util or "cab_prev" not in util:
            fail("atualizar_rascunho sem reusar numero existente")
    ok("rascunho: garante/reusa numero")

    if "_NF_SEM_RE" not in util:
        fail("extrator sem _NF_SEM_RE")
    ok("extrator CP reconhece SEM-")

    if "garantir_numero_nf_cabecalho" not in views:
        fail("views sem import/uso garantir")
    if "garantir numero SEM" not in views and "garantir_numero_nf_cabecalho(cab_pin" not in views:
        fail("PIN sem garantir SEM")
    ok("PIN + financeiro: rede de segurança SEM")

    if "entradaNfeGerarNumeroSemNota" not in html:
        fail("UI sem gerador SEM")
    if "entradaNfeGarantirNumeroNfCampo" not in html:
        fail("UI sem garantir no campo")
    if "entradaNfeConfirmarFornecedorEtapa" not in html:
        fail("UI sem confirmar fornecedor")
    if "entradaNfeGarantirNumeroNfCampo()" not in html:
        fail("confirmar não chama garantir")
    if "vazio = SEM" not in html:
        fail("placeholder sem hint SEM")
    ok("UI: confirma fornecedor gera SEM se vazio")


def prova_unit() -> None:
    r = subprocess.run(
        [
            sys.executable,
            "manage.py",
            "test",
            "produtos.tests_entrada_nf_sem_numero",
            "-v",
            "1",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out = (r.stdout or "") + (r.stderr or "")
    if r.returncode != 0:
        fail(f"unit tests falhou:\n{out[-2000:]}")
    ok("unit tests_entrada_nf_sem_numero")


def main() -> None:
    prova_fonte()
    prova_unit()
    print(f"VERIFY_OK: NF-SEM-NUMERO ({CHECKS} checks)")


if __name__ == "__main__":
    main()
