#!/usr/bin/env python3
"""Prova detalhada RH-CRON-ENVIO — envio automático salário -> CP.

Cenário loja (Queila): dia_envio=28 · dia_venc=1 · em 28/08 -> competência 08 · venc 01/09.
Cron Render: manage.py rh_envio_cp_automatico · agenda 15 6 * * * UTC (~03:15 BR).

  python scripts/verify_rh_envio_cp_automatico_path.py

VERIFY_OK N/N · VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from unittest.mock import MagicMock, patch

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
    print("=== estático ===")
    svc = _read("rh/services/envio_cp_automatico.py")
    cmd = _read("rh/management/commands/rh_envio_cp_automatico.py")
    views = _read("produtos/views.py")
    urls = _read("produtos/urls.py")
    ry = _read("render.yaml")

    if "def competencia_e_vencimento_para_envio" not in svc:
        fail("falta competencia_e_vencimento_para_envio")
    if "def processar_envio_cp_automatico_funcionario" not in svc:
        fail("falta processar_envio_cp_automatico_funcionario")
    if "def rodar_envio_cp_automatico_diario" not in svc:
        fail("falta rodar_envio_cp_automatico_diario")
    if "garantir_fechamento_aberto" not in svc:
        fail("não abre fechamento")
    if "garantir_titulo_salario_fechamento" not in svc:
        fail("não gera título CP")
    if "nao_e_dia_envio" not in svc:
        fail("falta skip fora do dia")
    if "auto_desligado" not in svc:
        fail("falta skip dia_envio=0")
    ok("serviço envio_cp_automatico completo")

    if "rodar_envio_cp_automatico_diario" not in cmd:
        fail("comando não chama rodar_diario")
    if "--data" not in cmd:
        fail("comando sem --data (replay dia)")
    if "--dry-run" not in cmd:
        fail("comando sem --dry-run")
    ok("management command rh_envio_cp_automatico")

    if "api_cron_rh_envio_cp_automatico" not in views:
        fail("falta API cron")
    if "ALERTA_VENDAS_CRON_TOKEN" not in views:
        fail("API sem token")
    if "api/cron/rh-envio-cp-automatico/" not in urls:
        fail("rota cron ausente")
    ok("API /api/cron/rh-envio-cp-automatico/ + token")

    if "agro-rh-envio-cp-automatico" not in ry:
        fail("render.yaml sem cron RH")
    if "rh_envio_cp_automatico" not in ry:
        fail("render.yaml startCommand errado")
    if 'schedule: "15 6 * * *"' not in ry and "15 6 * * *" not in ry:
        fail("agenda cron esperada 15 6 * * * UTC")
    if "branch: producao" not in ry:
        fail("cron RH deve apontar producao")
    ok("render.yaml cron RH (15 6 UTC · producao)")


def prova_competencia_casos() -> None:
    print("=== competência / vencimento ===")
    import django

    django.setup()
    from rh.services.envio_cp_automatico import (
        competencia_e_vencimento_para_envio,
        data_vencimento_salario_competencia,
    )

    # Loja Queila/Renan/Geraldo/Zuleide: envio 28 · venc 1
    c, v = competencia_e_vencimento_para_envio(
        date(2026, 8, 28), dia_envio=28, dia_vencimento=1
    )
    if c != date(2026, 8, 1) or v != date(2026, 9, 1):
        fail(f"caso 28/1 em 28/08 -> esperado 08/01 + 09/01 · got {c} {v}")
    ok("envio 28 / venc 1 em 28/08 -> comp 08 · venc 01/09")

    c, v = competencia_e_vencimento_para_envio(
        date(2026, 7, 28), dia_envio=28, dia_vencimento=1
    )
    if c != date(2026, 7, 1) or v != date(2026, 8, 1):
        fail(f"caso 28/1 em 28/07 -> esperado 07/01 + 08/01 · got {c} {v}")
    ok("envio 28 / venc 1 em 28/07 -> comp 07 · venc 01/08")

    # Isabela: envio 28 · venc 14
    c, v = competencia_e_vencimento_para_envio(
        date(2026, 8, 28), dia_envio=28, dia_vencimento=14
    )
    if c != date(2026, 8, 1) or v != date(2026, 9, 14):
        fail(f"caso 28/14 -> esperado 08/01 + 09/14 · got {c} {v}")
    ok("envio 28 / venc 14 -> comp 08 · venc 14/09")

    # Vitor: envio 28 · venc 7
    c, v = competencia_e_vencimento_para_envio(
        date(2026, 8, 28), dia_envio=28, dia_vencimento=7
    )
    if c != date(2026, 8, 1) or v != date(2026, 9, 7):
        fail(f"caso 28/7 -> esperado 08/01 + 09/07 · got {c} {v}")
    ok("envio 28 / venc 7 -> comp 08 · venc 07/09")

    # Envio cedo (1) · venc depois (5) -> mês anterior
    c, v = competencia_e_vencimento_para_envio(
        date(2026, 9, 1), dia_envio=1, dia_vencimento=5
    )
    if c != date(2026, 8, 1) or v != date(2026, 9, 5):
        fail(f"caso 1/5 em 01/09 -> esperado 08/01 + 09/05 · got {c} {v}")
    ok("envio 1 / venc 5 em 01/09 -> comp 08 · venc 05/09")

    # Virada de ano
    c, v = competencia_e_vencimento_para_envio(
        date(2027, 1, 1), dia_envio=1, dia_vencimento=5
    )
    if c != date(2026, 12, 1) or v != date(2027, 1, 5):
        fail(f"virada ano 1/5 -> esperado 12/2026 + 05/01/2027 · got {c} {v}")
    ok("virada de ano envio 1 / venc 5")

    dv = data_vencimento_salario_competencia(date(2026, 8, 1), 1)
    if dv != date(2026, 9, 1):
        fail(f"data_vencimento_salario_competencia 08->01/09 · got {dv}")
    ok("helper vencimento = dia V do mês seguinte")

    # Fevereiro curto: venc 31 -> clamp 28/29
    dv = data_vencimento_salario_competencia(date(2026, 1, 1), 31)
    if dv != date(2026, 2, 28):
        fail(f"clamp fev/2026 venc 31 -> 28 · got {dv}")
    ok("clamp dia em fevereiro curto")


def prova_skip_e_filtro() -> None:
    print("=== skip / filtro diário ===")
    import django

    django.setup()
    from rh.services.envio_cp_automatico import (
        processar_envio_cp_automatico_funcionario,
        rodar_envio_cp_automatico_diario,
    )

    fn = MagicMock()
    fn.pk = 99
    fn.ativo = True
    fn.dia_envio_cp_auto = 0
    fn.dia_vencimento_salario = 1
    fn.nome_exibicao = "Off"

    r = processar_envio_cp_automatico_funcionario(fn, hoje=date(2026, 8, 28), forcar=False)
    if not r.get("skipped") or r.get("motivo") != "auto_desligado":
        fail(f"dia_envio=0 deveria skip auto_desligado · {r}")
    ok("skip auto_desligado (dia 0)")

    fn.dia_envio_cp_auto = 28
    r = processar_envio_cp_automatico_funcionario(fn, hoje=date(2026, 8, 27), forcar=False)
    if not r.get("skipped") or r.get("motivo") != "nao_e_dia_envio":
        fail(f"fora do dia deveria skip · {r}")
    ok("skip nao_e_dia_envio (27!=28)")

    fn.ativo = False
    r = processar_envio_cp_automatico_funcionario(fn, hoje=date(2026, 8, 28), forcar=False)
    if not r.get("skipped") or r.get("motivo") != "inativo":
        fail(f"inativo deveria skip · {r}")
    ok("skip inativo")

    # dry_run filtra pelo dia
    with patch("rh.services.envio_cp_automatico.Funcionario") as FM:
        qs = MagicMock()
        qs.count.return_value = 2
        qs.values_list.return_value = [(3, "Queila Hinnen", 1), (4, "Renan Hinnen", 1)]
        FM.objects.filter.return_value.select_related.return_value.order_by.return_value = qs
        out = rodar_envio_cp_automatico_diario(hoje=date(2026, 8, 28), dry_run=True)
        if out.get("candidatos") != 2 or not out.get("dry_run"):
            fail(f"dry_run dia 28 · {out}")
        FM.objects.filter.assert_called()
        kwargs = FM.objects.filter.call_args.kwargs
        if kwargs.get("dia_envio_cp_auto") != 28 or kwargs.get("ativo") is not True:
            fail(f"filtro dry_run errado · {kwargs}")
    ok("dry_run filtra ativo + dia_envio == 28")


def prova_processar_abre_folha_e_titulo() -> None:
    print("=== processar (mock PG/título) ===")
    import django

    django.setup()
    from rh.services import envio_cp_automatico as mod

    fn = MagicMock()
    fn.pk = 3
    fn.ativo = True
    fn.dia_envio_cp_auto = 28
    fn.dia_vencimento_salario = 1
    fn.nome_exibicao = "Queila Hinnen"

    fech = MagicMock()
    fech.mongo_lancamento_salario_id = "abc123"
    fech.refresh_from_db = MagicMock()

    with (
        patch.object(mod, "garantir_fechamento_aberto", return_value=fech) as gfech,
        patch.object(mod, "recalcular_fechamento") as recalc,
        patch.object(
            mod,
            "garantir_titulo_salario_fechamento",
            return_value={"ok": True, "id": "abc123", "criado": True},
        ) as gtit,
        patch(
            "rh.services.salario_financeiro_mongo.sincronizar_valores_titulo_salario_mongo"
        ) as sync,
        patch.object(mod.transaction, "atomic") as atomic,
    ):
        atomic.return_value.__enter__ = MagicMock(return_value=None)
        atomic.return_value.__exit__ = MagicMock(return_value=False)
        r = mod.processar_envio_cp_automatico_funcionario(
            fn, hoje=date(2026, 8, 28), forcar=False
        )

    if not r.get("ok") or r.get("competencia") != "2026-08-01":
        fail(f"processar Queila 28/08 · {r}")
    if r.get("vencimento") != "2026-09-01":
        fail(f"vencimento esperado 2026-09-01 · {r}")
    if not r.get("criado"):
        fail(f"deveria marcar criado · {r}")
    gfech.assert_called_once()
    assert gfech.call_args[0][1] == date(2026, 8, 1)
    gtit.assert_called_once()
    recalc.assert_called()
    sync.assert_called()
    ok("processar: folha 08 + título + sync (mock)")

    # salário 0 -> erro sem inventar título
    with (
        patch.object(mod, "garantir_fechamento_aberto", return_value=fech),
        patch.object(mod, "recalcular_fechamento"),
        patch.object(
            mod,
            "garantir_titulo_salario_fechamento",
            return_value={"ok": False, "erro": "Salário R$ 0"},
        ),
        patch.object(mod.transaction, "atomic") as atomic,
    ):
        atomic.return_value.__enter__ = MagicMock(return_value=None)
        atomic.return_value.__exit__ = MagicMock(return_value=False)
        r2 = mod.processar_envio_cp_automatico_funcionario(
            fn, hoje=date(2026, 8, 28), forcar=True
        )
    if r2.get("ok") is not False or "0" not in (r2.get("erro") or ""):
        fail(f"salário 0 deveria falhar · {r2}")
    ok("salário R$ 0 -> ok=False (não inventa título)")


def prova_idempotente_ja_existe() -> None:
    print("=== idempotência ===")
    import django

    django.setup()
    from rh.services import envio_cp_automatico as mod

    fn = MagicMock()
    fn.pk = 3
    fn.ativo = True
    fn.dia_envio_cp_auto = 28
    fn.dia_vencimento_salario = 1
    fn.nome_exibicao = "Queila Hinnen"
    fech = MagicMock()
    fech.mongo_lancamento_salario_id = "jaexiste"
    fech.refresh_from_db = MagicMock()

    with (
        patch.object(mod, "garantir_fechamento_aberto", return_value=fech),
        patch.object(mod, "recalcular_fechamento"),
        patch.object(
            mod,
            "garantir_titulo_salario_fechamento",
            return_value={"ok": True, "id": "jaexiste", "criado": False},
        ),
        patch("rh.services.salario_financeiro_mongo.sincronizar_valores_titulo_salario_mongo"),
        patch.object(mod.transaction, "atomic") as atomic,
    ):
        atomic.return_value.__enter__ = MagicMock(return_value=None)
        atomic.return_value.__exit__ = MagicMock(return_value=False)
        r = mod.processar_envio_cp_automatico_funcionario(
            fn, hoje=date(2026, 8, 28), forcar=True
        )
    if r.get("criado") is not False or not r.get("ok"):
        fail(f"reprocessar deveria criado=False · {r}")
    ok("reprocessar mesmo dia -> criado=False (idempotente)")


def prova_live_opcional() -> None:
    """Se AGRO_VERIFY_RH_CRON_LIVE=1 + DATABASE_URL loja: confere Queila 08 e dry_run 28."""
    print("=== live opcional ===")
    if os.environ.get("AGRO_VERIFY_RH_CRON_LIVE", "").strip() not in ("1", "true", "yes"):
        ok("live pulado (sem AGRO_VERIFY_RH_CRON_LIVE=1)")
        return

    import django

    django.setup()
    from rh.models import FechamentoFolhaSimplificado, Funcionario
    from rh.services.envio_cp_automatico import rodar_envio_cp_automatico_diario

    q = Funcionario.objects.filter(nome_cache__icontains="queila", ativo=True).first()
    if not q:
        fail("live: Queila não encontrada")
    if int(q.dia_envio_cp_auto or 0) != 28 or int(q.dia_vencimento_salario or 0) != 1:
        fail(f"live: Queila config {q.dia_envio_cp_auto}/{q.dia_vencimento_salario}")
    ok("live: Queila envio=28 venc=1")

    fe = FechamentoFolhaSimplificado.objects.filter(
        funcionario=q, competencia=date(2026, 8, 1)
    ).first()
    if not fe:
        fail("live: falta folha Queila 2026-08")
    if fe.data_vencimento_pagamento != date(2026, 9, 1):
        fail(f"live: venc Queila 08 · {fe.data_vencimento_pagamento}")
    if not (fe.mongo_lancamento_salario_id or "").strip():
        fail("live: folha 08 sem título CP")
    ok("live: Queila folha 08 + título + venc 01/09")

    dry = rodar_envio_cp_automatico_diario(hoje=date(2026, 8, 28), dry_run=True)
    if dry.get("candidatos", 0) < 1:
        fail(f"live dry_run 28/08 sem candidatos · {dry}")
    nomes = [n for _, n, _ in (dry.get("funcionarios") or [])]
    if not any("Queila" in (n or "") for n in nomes):
        fail(f"live dry_run 28 sem Queila · {nomes}")
    ok(f"live dry_run 28/08: {dry.get('candidatos')} candidatos incl. Queila")


def main() -> None:
    prova_fonte()
    prova_competencia_casos()
    prova_skip_e_filtro()
    prova_processar_abre_folha_e_titulo()
    prova_idempotente_ja_existe()
    prova_live_opcional()
    print(f"VERIFY_OK {CHECKS}/{CHECKS}")


if __name__ == "__main__":
    main()
