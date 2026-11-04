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
    if not _prova_banco():
        print("VERIFY_FAIL banco")
        return 1
    ok("faixa de abril corrigida e a folha intacta")
    print(f"VERIFY_OK {oks}")
    return 0


def _prova_banco() -> bool:
    import os
    import sys
    from datetime import date
    from decimal import Decimal
    from pathlib import Path

    url = (os.environ.get("DATABASE_URL") or "").strip().lower()
    if url.startswith("postgres"):
        print("FAIL prova no Postgres da loja")
        return False
    raiz = Path(__file__).resolve().parents[1]
    if str(raiz) not in sys.path:
        sys.path.insert(0, str(raiz))
    destino = Path("/tmp/agro-rh-faixa.sqlite")
    if destino.exists():
        destino.unlink()
    os.environ["DATABASE_URL"] = f"sqlite:///{destino}"
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from django.apps import apps
    from django.db import connection

    with connection.schema_editor() as editor:
        for model in apps.get_models():
            if model._meta.proxy or not model._meta.managed:
                continue
            editor.create_model(model)

    from base.models import Empresa
    from produtos.models import ClienteAgro
    from rh.models import FechamentoFolhaSimplificado, Funcionario, HistoricoSalarial
    from rh.views import corrigir_historico_salarial

    empresa = Empresa.objects.create(nome_fantasia="GM Agro")
    pessoa = ClienteAgro.objects.create(nome="Renan Prova")
    func = Funcionario.objects.create(cliente_agro=pessoa, empresa=empresa, nome_cache="Renan Prova")
    antiga = HistoricoSalarial.objects.create(
        funcionario=func,
        salario_base=Decimal("1853.00"),
        data_inicio_vigencia=date(2026, 4, 22),
        data_fim_vigencia=date(2026, 9, 30),
    )
    vigente = HistoricoSalarial.objects.create(
        funcionario=func,
        salario_base=Decimal("2000.00"),
        data_inicio_vigencia=date(2026, 10, 1),
    )
    folhas = FechamentoFolhaSimplificado.objects.count()
    erro = corrigir_historico_salarial(
        antiga,
        salario=Decimal("1853.00"),
        inicio=date(2026, 1, 7),
        fim=date(2026, 9, 30),
        motivo="correção da admissão",
    )
    if erro:
        print("FAIL correção", erro)
        return False
    cruza = corrigir_historico_salarial(
        antiga,
        salario=Decimal("1853.00"),
        inicio=date(2026, 1, 7),
        fim=date(2026, 10, 1),
        motivo="cruza",
    )
    if not cruza:
        print("FAIL aceitou faixa cruzada")
        return False
    antiga.refresh_from_db()
    vigente.refresh_from_db()
    if antiga.data_inicio_vigencia != date(2026, 1, 7) or antiga.salario_base != Decimal("1853.00"):
        print("FAIL antiga", antiga.data_inicio_vigencia, antiga.salario_base)
        return False
    if vigente.data_inicio_vigencia != date(2026, 10, 1) or vigente.salario_base != Decimal("2000.00"):
        print("FAIL vigente mudou")
        return False
    if FechamentoFolhaSimplificado.objects.count() != folhas:
        print("FAIL folha criada")
        return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
