# -*- coding: utf-8 -*-
"""
Prova detalhada — Pedir loja Pronto opcional + Transferir sel + bip 30 min
(`PDV-PEDIR-PRONTO-TRANSF`).

Contrato:
  - Transferir no Aceito (Pronto NÃO obrigatório)
  - Pronto continua disponível
  - Transferir selecionados (Aceito e Pronto)
  - Bip 30 min após Aceitar vale em Aceito e em Pronto (Postgres aceito_em)
  - Alerta visual segue recebidos_bip (multi-PC via poll)

  set AGRO_PIN_TESTE=9973
  set PYTHONIOENCODING=utf-8
  python scripts/verify_pdv_pedir_pronto_transf_path.py
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_contratos() -> None:
    print("== 1) Contratos arquivo ==")
    js = _read("produtos/static/produtos/js/pdv_pedir_loja.js")
    html = _read("produtos/templates/produtos/partials/pdv/pedir_loja_overlay.html")
    util = _read("produtos/pdv_transf_loja_util.py")
    tests = _read("produtos/tests_pdv_transf_loja.py")

    check("util_transf_aceito_e_pronto", "if sol.status not in (STATUS_ACEITO, STATUS_PRONTO):" in util)
    check("util_sem_exige_pronto", "Marque Pronto antes" not in util)
    check("util_msg_pronto_opcional", "se quiser, marque Pronto" in util)
    check("util_bip_grace_30", "PEDIR_LOJA_BIP_GRACE = timedelta(minutes=30)" in util)
    check("util_bip_aceito_e_pronto", "status__in=(STATUS_ACEITO, STATUS_PRONTO)" in util)
    check("util_pronto_preenche_aceito_em", "if sol.aceito_em is None:" in util and "STATUS_PRONTO" in util)
    check("util_solicitacao_deve_bipar", "def solicitacao_deve_bipar" in util)
    check("util_contar_recebidos_bip", "def contar_recebidos_bip" in util)
    check("util_resumo_recebidos_bip", '"recebidos_bip"' in util or "'recebidos_bip'" in util)

    check(
        "js_transf_botao_aceito_ou_pronto",
        "(st === 'aceito' || st === 'pronto')" in js and 'data-pl-acao="transferir"' in js,
    )
    check("js_pronto_ainda_existe", 'data-pl-acao="pronto"' in js and "Pronto" in js)
    check("js_transferir_sel_fn", "transferirSelecionadosTodos" in js and "coletarLotesTransferirSel" in js)
    check(
        "js_transferir_sel_aceito_pronto",
        'data-pl-st="aceito"], .pl-card[data-pl-st="pronto"]' in js
        or 'data-pl-st="aceito"],.pl-card[data-pl-st="pronto"]' in js.replace(" ", ""),
    )
    check("js_badge_por_bip", "applyBadge(n, bip)" in js and "function applyBadge(n, bip)" in js)
    check("js_bip_texto_pausa", "bip pausado 30 min" in js)
    check("js_poll_12s", "12000" in js)
    check("js_visibility_sync", "visibilitychange" in js)
    check("js_apply_resumo_bip", "recebidos_bip" in js and "syncBeepPendentes" in js)

    check("html_btn_transferir_sel", 'id="pdv-pedir-loja-transferir-sel"' in html)
    check(
        "html_ajuda_transf_direto",
        "direto no Aceito" in html or "opcional" in html.lower(),
    )
    check("html_ajuda_bip_30", "30 min" in html and ("Pronto" in html or "Aceitar" in html))

    check("tests_transf_aceito_ou_pronto", "test_transferir_aceito_ou_pronto" in tests)
    check("tests_bip_aceito", "test_bip_aceito_folga_30min" in tests)
    check("tests_bip_pronto", "test_bip_pronto_mesma_folga" in tests or "STATUS_PRONTO" in tests)


def test_runtime_pode_agir_e_bip() -> None:
    print("== 2) Runtime pode_agir + bip ==")
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from django.utils import timezone

    from produtos.pdv_transf_loja_util import (
        PEDIR_LOJA_BIP_GRACE,
        STATUS_ACEITO,
        STATUS_PENDENTE,
        STATUS_PRONTO,
        contar_recebidos_bip,
        pode_agir,
        solicitacao_deve_bipar,
    )

    def sol(st, origem="vila"):
        return SimpleNamespace(status=st, loja_origem=origem, loja_destino="centro")

    ok, err = pode_agir(sol(STATUS_PENDENTE), "vila", "transferir")
    check("rt_transf_pendente_bloqueia", ok is False, err or "ok")

    ok, err = pode_agir(sol(STATUS_ACEITO), "vila", "transferir")
    check("rt_transf_aceito_libera", ok is True and not err, err or "ok")

    ok, err = pode_agir(sol(STATUS_PRONTO), "vila", "transferir")
    check("rt_transf_pronto_libera", ok is True and not err, err or "ok")

    ok, err = pode_agir(sol(STATUS_ACEITO), "vila", "pronto")
    check("rt_pronto_apos_aceito", ok is True, err or "ok")

    agora = timezone.now()
    check("rt_bip_pendente", solicitacao_deve_bipar(STATUS_PENDENTE, agora, agora) is True)
    check("rt_bip_aceito_agora_silencio", solicitacao_deve_bipar(STATUS_ACEITO, agora, agora) is False)
    check("rt_bip_pronto_agora_silencio", solicitacao_deve_bipar(STATUS_PRONTO, agora, agora) is False)
    check(
        "rt_bip_aceito_29min_silencio",
        solicitacao_deve_bipar(STATUS_ACEITO, agora - timedelta(minutes=29), agora) is False,
    )
    check(
        "rt_bip_pronto_29min_silencio",
        solicitacao_deve_bipar(STATUS_PRONTO, agora - timedelta(minutes=29), agora) is False,
    )
    check(
        "rt_bip_aceito_31min_volta",
        solicitacao_deve_bipar(STATUS_ACEITO, agora - timedelta(minutes=31), agora) is True,
    )
    check(
        "rt_bip_pronto_31min_volta",
        solicitacao_deve_bipar(STATUS_PRONTO, agora - timedelta(minutes=31), agora) is True,
    )
    check("rt_bip_grace_30", PEDIR_LOJA_BIP_GRACE == timedelta(minutes=30))

    qs = MagicMock()
    qs.filter.return_value = qs
    qs.count.return_value = 0
    n = contar_recebidos_bip(qs, agora)
    check("rt_contar_bip_zero", n == 0, str(n))


def test_runtime_aplicar_pronto_preenche_aceito_em() -> None:
    print("== 3) Runtime Pronto preenche aceito_em ==")
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from django.utils import timezone

    from produtos.pdv_transf_loja_util import STATUS_ACEITO, STATUS_PRONTO, aplicar_status

    agora = timezone.now()
    sol = SimpleNamespace(
        status=STATUS_ACEITO,
        loja_origem="vila",
        loja_destino="centro",
        aceito_em=None,
        aceito_por_label="",
        aceito_por=None,
        pronto_em=None,
        pronto_por_label="",
        pronto_por=None,
        cancelado_em=None,
        cancelado_por_label="",
        cancelado_por=None,
        cancelado_motivo="",
        save=MagicMock(),
    )
    with patch("produtos.pdv_transf_loja_util._registrar_evento"), patch(
        "produtos.pdv_transf_loja_util.timezone.now", return_value=agora
    ):
        ok, err = aplicar_status(
            sol,
            "pronto",
            loja_atual="vila",
            operador_label="Teste",
            usuario=None,
        )
    check("rt_aplicar_pronto_ok", ok is True and not err, err or "ok")
    check("rt_aplicar_pronto_status", sol.status == STATUS_PRONTO, str(sol.status))
    check("rt_aplicar_pronto_aceito_em", sol.aceito_em == agora, str(sol.aceito_em))
    check("rt_aplicar_pronto_pronto_em", sol.pronto_em == agora, str(sol.pronto_em))


def test_django_unit() -> None:
    print("== 4) Django unit (PodeAgir + bip) ==")
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from django.conf import settings
    from django.test.utils import get_runner

    TestRunner = get_runner(settings)
    runner = TestRunner(verbosity=0, interactive=False, keepdb=True)
    failures = runner.run_tests(
        [
            "produtos.tests_pdv_transf_loja.PodeAgirTests",
            "produtos.tests_pdv_transf_loja.UtilBasicoTests",
        ]
    )
    check("django_unit_pode_agir_bip", failures == 0, f"failures={failures}")


def main() -> int:
    pin = (os.environ.get("AGRO_PIN_TESTE") or "").strip()
    print("VERIFY PDV-PEDIR-PRONTO-TRANSF PATH")
    print(f"PIN={'set' if pin else 'não set'} ({pin or '—'})")
    test_contratos()
    try:
        test_runtime_pode_agir_e_bip()
        test_runtime_aplicar_pronto_preenche_aceito_em()
        test_django_unit()
    except Exception as exc:
        check("runtime_setup", False, str(exc))
        print(f"ERRO runtime: {exc}")

    print()
    print(f"RESULTADO: {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("FALHAS:")
        for f in fails:
            print(f"  - {f}")
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    print("PREP_FAILS=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
