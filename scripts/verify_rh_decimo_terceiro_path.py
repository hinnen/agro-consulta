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
    if not _prova_banco():
        print("VERIFY_FAIL banco")
        return 1
    ok("folha intacta e o mesmo título é atualizado")
    print(f"VERIFY_OK {oks}")
    return 0


def _prova_banco() -> bool:
    """SQLite novo. O 13º não cria folha e a segunda gravação reusa o título."""
    import os
    from datetime import date
    from decimal import Decimal
    from pathlib import Path
    from unittest.mock import patch

    url = (os.environ.get("DATABASE_URL") or "").strip().lower()
    if url.startswith("postgres"):
        print("FAIL prova no Postgres da loja")
        return False
    import sys

    raiz = Path(__file__).resolve().parents[1]
    if str(raiz) not in sys.path:
        sys.path.insert(0, str(raiz))
    destino = Path("/tmp/agro-rh-decimo.sqlite")
    if destino.exists():
        destino.unlink()
    os.environ["DATABASE_URL"] = f"sqlite:///{destino}"
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from django.apps import apps
    from django.contrib.auth import get_user_model
    from django.db import connection

    with connection.schema_editor() as editor:
        for model in apps.get_models():
            if model._meta.proxy or not model._meta.managed:
                continue
            editor.create_model(model)

    from base.models import Empresa
    from produtos.models import ClienteAgro, TituloFinanceiroAgro
    from rh.models import FechamentoFolhaSimplificado, Funcionario
    from rh.services.decimo_terceiro import lancar_decimo_terceiro

    empresa = Empresa.objects.create(nome_fantasia="GM Agro")
    pessoa = ClienteAgro.objects.create(nome="Renan Prova")
    func = Funcionario.objects.create(
        cliente_agro=pessoa,
        empresa=empresa,
        nome_cache="Renan Prova",
    )
    user = get_user_model().objects.create_user(username="renan_decimo", password="x", first_name="Renan")
    folhas_antes = FechamentoFolhaSimplificado.objects.count()
    mid = "507f1f77bcf86cd799439011"

    def fake_inserir(*_a, **_k):
        return {"ids": [mid]}

    with patch("rh.services.decimo_terceiro.obter_conexao_mongo", return_value=(None, None)), patch(
        "produtos.lancamentos_financeiro_pg_write_util.inserir_lancamentos_manual_lote_dispatch",
        side_effect=fake_inserir,
    ):
        primeiro = lancar_decimo_terceiro(
            func,
            ano=2026,
            parcela="UNICA",
            valor=Decimal("2000.00"),
            data_vencimento=date(2026, 12, 20),
            usuario=user,
        )
        if not primeiro.get("ok") or not primeiro.get("criado") or primeiro.get("id") != mid:
            print("FAIL primeiro lançamento", primeiro)
            return False
        TituloFinanceiroAgro.objects.create(
            mongo_id=mid,
            despesa=True,
            valor_bruto=Decimal("2000.00"),
            valor_pago=Decimal("0"),
            valor_restante=Decimal("2000.00"),
            descricao="13º prova",
        )
        segundo = lancar_decimo_terceiro(
            func,
            ano=2026,
            parcela="UNICA",
            valor=Decimal("1800.00"),
            data_vencimento=date(2026, 12, 20),
            usuario=user,
        )
        if not segundo.get("ok") or segundo.get("criado") or segundo.get("id") != mid:
            print("FAIL atualização", segundo)
            return False
        titulo = TituloFinanceiroAgro.objects.get(mongo_id=mid)
        titulo.valor_pago = Decimal("1900.00")
        titulo.save(update_fields=["valor_pago"])
        bloqueado = lancar_decimo_terceiro(
            func,
            ano=2026,
            parcela="UNICA",
            valor=Decimal("1800.00"),
            data_vencimento=date(2026, 12, 20),
            usuario=user,
        )
    if bloqueado.get("ok"):
        print("FAIL aceitou valor menor que o pago", bloqueado)
        return False
    from rh.models import DecimoTerceiroFuncionario

    reg = DecimoTerceiroFuncionario.objects.get(funcionario=func, ano=2026, parcela="UNICA")
    if reg.valor != Decimal("1800.00") or reg.mongo_lancamento_id != mid:
        print("FAIL registro", reg.valor, reg.mongo_lancamento_id)
        return False
    if FechamentoFolhaSimplificado.objects.count() != folhas_antes:
        print("FAIL folha foi criada")
        return False
    if DecimoTerceiroFuncionario.objects.count() != 1:
        print("FAIL duplicou o 13º")
        return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
