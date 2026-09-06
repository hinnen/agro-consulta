"""Prova NF-FIN-MANUAL-RELIGA: CP já existe, etapa 7 laranja, nota manual sem chave.

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
    views = _read("produtos/views.py")
    util = _read("produtos/nfe_entrada_util.py")
    html = _read("produtos/templates/produtos/entrada_nota.html")
    if "rascunho {rid_obs}" not in views:
        fail("views.py não grava rascunho na observação do CP")
    ok("CP novo grava rascunho na observação")
    if "or (ev[\"nf_ok\"] and ev[\"fornecedor_forte\"])" not in util:
        fail("validador não religa por NF exata + fornecedor")
    ok("validador aceita NF exata + fornecedor")
    if "entradaNfeFinanceiroTituloJaGerado" not in html:
        fail("JS sem entradaNfeFinanceiroTituloJaGerado")
    if "Conta a pagar já gerada" not in html:
        fail("JS sem rótulo Conta a pagar já gerada")
    if "nfe-wiz-fin-alerta" not in html:
        fail("JS sem alerta laranja da etapa financeiro")
    ok("UI: alerta laranja + botão vira Conta a pagar já gerada")


def prova_unitaria() -> None:
    env = os.environ.copy()
    env["DJANGO_SETTINGS_MODULE"] = "config.settings"
    r = subprocess.run(
        [
            sys.executable,
            "manage.py",
            "test",
            "produtos.tests_entrada_nf_financeiro_vinculo",
            "produtos.tests_entrada_nf_reabertura_estoque.EntradaNfReaberturaEstoqueTests.test_etapa_financeiro_reconhece_ids_preservados_sem_criar_duplicata",
            "-v",
            "1",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        fail(f"django test falhou:\n{r.stdout}\n{r.stderr}")
    m = re.search(r"Ran (\d+) test", r.stdout + r.stderr)
    n = int(m.group(1)) if m else 0
    if n < 12:
        fail(f"esperava >=12 testes, veio {n}")
    ok(f"django vínculo + anti-duplicata flag {n}/{n}")


def main() -> None:
    prova_fonte()
    prova_unitaria()
    print(f"VERIFY_OK {CHECKS}/{CHECKS}")


if __name__ == "__main__":
    main()
