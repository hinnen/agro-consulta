#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prova detalhada — PDV-ENT-MUDAR-LOJA (entrega / pagamento / ambos).

  set AGRO_PIN_TESTE=9973
  python scripts/verify_pdv_entrega_mudar_loja_path.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print("  OK  " + name + ((" — " + detail) if detail else ""))
    else:
        fails.append(name)
        print("  FAIL " + name + ((" — " + detail) if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_estatico() -> None:
    print("== Estático ==")
    models = _read("produtos/models.py")
    util = _read("produtos/entrega_pdv_pendente_util.py")
    views = _read("produtos/views.py")
    urls = _read("produtos/urls.py")
    js = _read("produtos/static/produtos/js/pdv_wizard.js")
    state = _read("produtos/static/produtos/js/pdv_state.js")
    ov = _read("produtos/templates/produtos/partials/pdv/entrega_wizard_overlay.html")
    step = _read("produtos/templates/produtos/partials/pdv/step_produtos.html")
    pdv = _read("pdv/views.py")
    mig = ROOT / "produtos/migrations/0131_pedido_entrega_loja_pagamento.py"

    check("migrate_0131", mig.is_file())
    check("model_loja_pagamento", "loja_pagamento" in models)
    check("fn_mudar", "def mudar_loja_entrega_pdv" in util)
    check("fn_pag_efetiva", "def loja_pagamento_efetiva" in util)
    check("api_view", "def api_pdv_entrega_pendente_mudar_loja" in views)
    check("url_mudar", "mudar-loja/" in urls and "api_pdv_entrega_pendente_mudar_loja" in urls)
    check("bootstrap_url", "apiPdvEntregaPendenteMudarLoja" in pdv)
    check("state_campos", "lojaPagamento:" in state and "lojaEscopo:" in state)
    check("overlay_escopo", "pdv-ed-loja-escopo-entrega" in ov and "pdv-ed-loja-escopo-ambos" in ov)
    check("modal_card", 'id="pdv-entrega-mudar-loja-modal"' in step)
    check("js_confirmar_escopo", "function confirmarLojaSaidaComEscopo" in js)
    check("js_payload_pag", "loja_pagamento: lojaPagamentoEntregaAtual" in js)
    check("js_btn_loja_card", "pdv-entrega-mudar-loja" in js)
    check("js_api_mudar", "apiPdvEntregaPendenteMudarLoja" in js)
    check("registrar_loja_pag", 'campos["loja_pagamento"]' in views)
    check("resolver_loja_pag", "loja_pagamento" in util and "obter_caixa_pai_aberto" in util)


def test_runtime() -> None:
    print("== Runtime Django + PIN ==")
    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.caixa_util import (
        PONTO_CAIXA_GAVETA,
        PONTO_CAIXA_VILA,
        rotulo_operador_pin,
        validar_pin_operador,
    )
    from produtos.entrega_pdv_pendente_util import (
        loja_pagamento_efetiva,
        mudar_loja_entrega_pdv,
        serializar_entrega_pendente_pdv,
    )
    from produtos.models import PedidoEntrega, SessaoCaixa

    ok_pin, err = validar_pin_operador(PIN)
    check("pin_9973", ok_pin, (err or "")[:80])
    quem = rotulo_operador_pin(PIN) if ok_pin else "Verify"
    check("pin_rotulo", bool(quem), str(quem)[:40])

    user = get_user_model().objects.filter(is_active=True).order_by("id").first()
    check("user_ativo", user is not None)
    if not user:
        return

    gav = SessaoCaixa.objects.create(usuario=user, ponto_caixa=PONTO_CAIXA_GAVETA, valor_abertura=0)
    vil = SessaoCaixa.objects.create(usuario=user, ponto_caixa=PONTO_CAIXA_VILA, valor_abertura=0)
    ent = PedidoEntrega.objects.create(
        cliente_nome="Verify Mudar Loja",
        telefone="13999990000",
        endereco_linha="Rua Teste",
        itens_json=[{"nome": "Item", "qtd": 1, "preco": 10}],
        total_texto="R$ 10,00",
        forma_pagamento="Dinheiro",
        aguarda_pagamento_pdv=True,
        loja_entrega="centro",
        loja_pagamento="centro",
        sessao_caixa=gav,
        origem="pdv",
        operador=quem,
    )
    try:
        check("criada_centro", ent.loja_entrega == "centro" and loja_pagamento_efetiva(ent) == "centro")

        ent2, err2 = mudar_loja_entrega_pdv(
            ent.pk, loja="vila", escopo="pagamento", pin=PIN, quem=quem
        )
        check("pag_ok", err2 is None and ent2 is not None, str(err2 or "")[:120])
        if ent2:
            ent2.refresh_from_db()
            check("pag_loja_ent_igual", ent2.loja_entrega == "centro")
            check("pag_loja_pag_vila", ent2.loja_pagamento == "vila")
            ponto2 = getattr(ent2.sessao_caixa, "ponto_caixa", "") if ent2.sessao_caixa_id else ""
            check("pag_sessao_vila", ponto2 == PONTO_CAIXA_VILA, str(ponto2))

        ent3, err3 = mudar_loja_entrega_pdv(
            ent.pk, loja="vila", escopo="entrega", pin=PIN, quem=quem
        )
        check("ent_ok", err3 is None and ent3 is not None, str(err3 or "")[:120])
        if ent3:
            ent3.refresh_from_db()
            check("ent_loja_vila", ent3.loja_entrega == "vila")
            check("ent_pag_ainda_vila", ent3.loja_pagamento == "vila")

        ent4, err4 = mudar_loja_entrega_pdv(
            ent.pk, loja="centro", escopo="ambos", pin=PIN, quem=quem
        )
        check("ambos_ok", err4 is None and ent4 is not None, str(err4 or "")[:120])
        if ent4:
            ent4.refresh_from_db()
            check("ambos_ent_centro", ent4.loja_entrega == "centro")
            check("ambos_pag_centro", ent4.loja_pagamento == "centro")
            ponto4 = getattr(ent4.sessao_caixa, "ponto_caixa", "") if ent4.sessao_caixa_id else ""
            check(
                "ambos_sessao_gav",
                ponto4 == PONTO_CAIXA_GAVETA,
                str(ponto4),
            )

        row = serializar_entrega_pendente_pdv(ent4 or ent)
        check("serial_loja_pag", "loja_pagamento" in row and "pode_mudar_loja" in row)

        # HTTP API
        client = Client(HTTP_HOST="127.0.0.1")
        client.force_login(user)
        url = reverse("api_pdv_entrega_pendente_mudar_loja", args=[ent.pk])
        r = client.post(
            url,
            data=json.dumps({"loja": "vila", "escopo": "pagamento", "pin": PIN}),
            content_type="application/json",
        )
        try:
            dj = r.json()
        except Exception:
            dj = {}
        check(
            "http_mudar_ok",
            r.status_code == 200 and bool(dj.get("ok")),
            f"status={r.status_code} {str(dj)[:140]}",
        )

        # paga na loja: só entrega
        paga = PedidoEntrega.objects.create(
            cliente_nome="Verify Paga Loja",
            aguarda_pagamento_pdv=False,
            paga_na_loja=True,
            loja_entrega="centro",
            loja_pagamento="centro",
            origem="pdv",
            operador=quem,
        )
        try:
            _, err_p = mudar_loja_entrega_pdv(
                paga.pk, loja="vila", escopo="pagamento", pin=PIN, quem=quem
            )
            check("paga_bloqueia_pag", err_p is not None and "paga" in (err_p or "").lower())
            ep, err_e = mudar_loja_entrega_pdv(
                paga.pk, loja="vila", escopo="entrega", pin=PIN, quem=quem
            )
            check("paga_permite_ent", err_e is None and ep is not None and ep.loja_entrega == "vila")
        finally:
            paga.delete()
    finally:
        PedidoEntrega.objects.filter(pk=ent.pk).delete()
        SessaoCaixa.objects.filter(pk__in=[gav.pk, vil.pk]).delete()


def main() -> int:
    print(f"PIN teste = {PIN!r}")
    test_estatico()
    try:
        test_runtime()
    except Exception as e:
        check("runtime_crash", False, str(e)[:220])
    print()
    print(f"{len(oks)} ok · {len(fails)} fail")
    if fails:
        print("FAILED:")
        for f in fails:
            print("  - " + f)
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
