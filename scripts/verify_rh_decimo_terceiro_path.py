#!/usr/bin/env python3
"""Path RH-DECIMO-TERCEIRO — estático, sem banco."""
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


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    print("VERIFY RH-DECIMO-TERCEIRO")
    svc = read("rh/services/decimo_terceiro.py")
    views = read("rh/views.py")
    urls = read("rh/urls.py")
    ficha = read("rh/templates/rh/funcionario_ficha.html")
    mig = read("rh/migrations/0008_decimoterceirofuncionario.py")
    bloco = views.split("def rh_funcionario_decimo_terceiro", 1)[1].split("def rh_funcionario_salario_adicionar", 1)[0]
    if "recalcular_fechamento" not in svc and "FechamentoFolha" not in svc and "recalcular_fechamento" not in bloco:
        ok("13º não recalcula a folha")
    else:
        fail("13º mexe na folha")
    if "rh_funcionario_decimo_terceiro" in urls and "Décimo terceiro" in svc:
        ok("rota e plano")
    else:
        fail("rota ou plano")
    if "Lançar 13º" in ficha and "Não muda o líquido" in ficha:
        ok("botão na ficha")
    else:
        fail("ficha")
    if "DecimoTerceiroFuncionario" in mig and "rh_decimo_unico_parcela_ano" in mig:
        ok("migração 0008")
    else:
        fail("migração")
    if "set_rollback" in svc and "mongo_lancamento_id" in svc:
        ok("não duplica título e desfaz se o financeiro falhar")
    else:
        fail("gravação")
    if fails:
        print(f"VERIFY_FAIL {len(fails)}")
        return 1
    print(f"VERIFY_OK {oks}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
