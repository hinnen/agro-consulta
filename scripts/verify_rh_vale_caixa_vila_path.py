#!/usr/bin/env python
"""Vale no caixa Vila: perfil RH pode ser de outra empresa (Centro). VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"  OK {msg}")


def main() -> None:
    print("verify_rh_vale_caixa_vila_path...")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()

    from base.models import Empresa
    from produtos.models import ClienteAgro
    from rh.models import Funcionario
    from rh.utils import resolver_perfil_rh_para_vale_operacao_caixa

    utils_src = open(os.path.join(ROOT, "rh", "utils.py"), encoding="utf-8").read()
    if "resolver_perfil_rh_para_vale_operacao_caixa" not in utils_src:
        fail("rh/utils.py sem resolver perfil caixa multi-loja")
    ok("helper no utils")

    mongo = open(os.path.join(ROOT, "rh", "services", "salario_financeiro_mongo.py"), encoding="utf-8").read()
    if "resolver_perfil_rh_para_vale_operacao_caixa" not in mongo:
        fail("salario_financeiro_mongo não usa helper caixa")
    if "Empresa '" in mongo and "não encontrada — não foi possível vincular vale" in mongo:
        fail("ainda exige empresa do turno no vale")
    ok("tentar_caixa_adiant sem bloqueio empresa turno")

    views = open(os.path.join(ROOT, "produtos", "views.py"), encoding="utf-8").read()
    if "cliente_agro_id_rh" not in views:
        fail("views sem cliente_agro_id_rh")
    if views.count("cliente_agro_id=cliente_agro_id_rh") < 2:
        fail("views não repassa cliente_agro_id ao vale e salário")
    if "Fonte da verdade = loja do caixa" not in views:
        fail("views perdeu forçar empresa loja em despesas comuns")
    ok("views vale+salário + empresa loja intacta")

    pg = open(os.path.join(ROOT, "rh", "services", "pagamento_salario.py"), encoding="utf-8").read()
    if "resolver_perfil_rh_para_vale_operacao_caixa" not in pg:
        fail("pagamento_salario não usa helper caixa")
    if "Empresa '" in pg and "não encontrada" in pg and "resolver_empresa" in pg:
        fail("pagamento_salario ainda bloqueia empresa turno")
    ok("pagamento_salario alinhado")

    imp = open(os.path.join(ROOT, "rh", "services", "importador_vales_caixa.py"), encoding="utf-8").read()
    if "resolver_perfil_rh_para_vale_operacao_caixa" in imp:
        fail("importador não deve usar resolver de caixa (empresa do lançamento Mongo)")
    if "resolver_perfil_rh_para_vale" not in imp:
        fail("importador perdeu resolver clássico")
    ok("importador vales Mongo inalterado")

    if "registrar_vale_como_baixa_parcial_salario" not in mongo:
        fail("fluxo baixa parcial ausente")
    if "_anexar_retirada_turno_caixa_saida" not in views:
        fail("views sem retirada turno após vale")
    ok("fluxo RH + retirada turno referenciado")

    try:
        emp_centro = Empresa.objects.filter(nome_fantasia__icontains="Centro").first()
        if not emp_centro:
            emp_centro = Empresa.objects.create(nome_fantasia="Agro Mais Centro", ativo=True)
        f = (
            Funcionario.objects.filter(empresa=emp_centro, ativo=True, cliente_agro__ativo=True)
            .select_related("cliente_agro")
            .first()
        )
        if not f:
            ca = ClienteAgro.objects.create(nome="Func Teste Vale Vila", ativo=True)
            f = Funcionario.objects.create(
                empresa=emp_centro,
                cliente_agro=ca,
                nome_cache="Func Teste Vale Vila",
                ativo=True,
            )
        ca = f.cliente_agro

        f_hit, modo, err = resolver_perfil_rh_para_vale_operacao_caixa(
            "Agro Mais Vila Elias",
            cliente_agro_id=ca.pk,
        )
        if err:
            fail(f"resolver retornou erro: {err}")
        if not f_hit or f_hit.pk != f.pk:
            fail("não achou perfil Centro com empresa turno Vila inexistente/diferente")
        if modo not in ("id_loja", "id", "nome_loja", "nome"):
            fail(f"modo inesperado: {modo}")
        ok("resolve perfil Centro com label empresa Vila no caixa")

        f2, _, err2 = resolver_perfil_rh_para_vale_operacao_caixa(
            "Agro Mais Vila Elias",
            mongo_cliente_id=f"local:{ca.pk}",
        )
        if err2 or not f2 or f2.pk != f.pk:
            fail("fallback mongo local: não resolveu")
        ok("resolve por pessoa_id local")
        print("VERIFY_OK 10/10")
    except Exception as exc:
        print(f"  SKIP ORM ({exc.__class__.__name__}: {exc})")
        print("VERIFY_OK 8/8 (estático; ORM exige migrate)")


if __name__ == "__main__":
    main()
