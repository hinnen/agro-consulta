#!/usr/bin/env python3
"""Path RH-FAIXA-SALARIAL — estático, sem banco."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def main() -> int:
    print("VERIFY RH-FAIXA-SALARIAL")
    views = (ROOT / "rh/views.py").read_text(encoding="utf-8")
    urls = (ROOT / "rh/urls.py").read_text(encoding="utf-8")
    ficha = (ROOT / "rh/templates/rh/funcionario_ficha.html").read_text(encoding="utf-8")
    bloco = views.split("def rh_funcionario_salario_corrigir", 1)[1].split("def rh_funcionario_salario_adicionar", 1)[0]
    if "recalcular" not in bloco and "FechamentoFolha" not in bloco:
        ok("corrigir faixa não recalcula a folha")
    else:
        fail("folha")
    if "rh_funcionario_salario_corrigir" in urls:
        ok("rota")
    else:
        fail("rota")
    if "Salvar faixa" in ficha and "data_inicio_vigencia" in ficha:
        ok("histórico editável")
    else:
        fail("ficha")
    if "set_rollback(True)" in views.split("def rh_funcionario_decimo_terceiro", 1)[1].split("def rh_funcionario_salario_corrigir", 1)[0]:
        ok("as duas parcelas continuam juntas na mesma transação")
    else:
        fail("parcelas")
    if fails:
        print(f"VERIFY_FAIL {len(fails)}")
        return 1
    print(f"VERIFY_OK {oks}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
