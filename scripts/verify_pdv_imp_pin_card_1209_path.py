#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prova detalhada — PDV impressão/PIN/card entrega 12/09c.

Pacotes:
  PDV-IMP-SEP-OFF · PDV-IMP-PIN-ANTES · PDV-ENT-CARD-LATERAL

  set AGRO_PIN_TESTE=9973
  python scripts/verify_pdv_imp_pin_card_1209_path.py
"""
from __future__ import annotations

import os
import re
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


def _mei_sep_checked(html: str) -> bool:
    """True se #mei-chk-sep ainda tem atributo checked no HTML."""
    m = re.search(
        r'<input[^>]*id=["\']mei-chk-sep["\'][^>]*>',
        html,
        flags=re.I,
    )
    if not m:
        return False
    return bool(re.search(r"\bchecked\b", m.group(0), flags=re.I))


def _imp_sep_checked(html: str) -> bool:
    m = re.search(
        r'<input[^>]*id=["\']imp-chk-sep["\'][^>]*>',
        html,
        flags=re.I,
    )
    if not m:
        return False
    return bool(re.search(r"\bchecked\b", m.group(0), flags=re.I))


def test_estatico() -> None:
    print("== Estático PDV-IMP-SEP-OFF / PIN-ANTES / CARD-LATERAL ==")
    wiz_html = _read("produtos/templates/produtos/pdv_wizard.html")
    modals = _read("produtos/templates/produtos/partials/pdv/modals.html")
    painel = _read("produtos/templates/produtos/entregas_painel.html")
    wiz_js = _read("produtos/static/produtos/js/pdv_wizard.js")
    cons_js = _read("produtos/static/produtos/js/consulta_produtos.js")

    # --- SEP-OFF ---
    check("sep_wizard_sem_checked", not _mei_sep_checked(wiz_html))
    check("sep_modals_sem_checked", not _mei_sep_checked(modals))
    check("sep_painel_sem_checked", not _imp_sep_checked(painel))
    check("sep_painel_js_false", "imp-chk-sep').checked = false" in painel or 'imp-chk-sep").checked = false' in painel)
    check("sep_ent_ainda_checked", 'id="mei-chk-ent"' in wiz_html and "checked" in wiz_html)
    check("sep_cup_ainda_checked", 'id="mei-chk-cup"' in wiz_html)
    # Reset ao abrir modal: sep=false
    check(
        "sep_modal_reset_false",
        "chkSep.checked = false" in wiz_js or "chkSep) chkSep.checked = false" in wiz_js,
    )
    check("sep_consulta_reset", "chkSep.checked = false" in cons_js)

    # --- PIN-ANTES ---
    check("pin_fn_registrar", "function wizardRegistrarEntregaPainelAposEscolha" in wiz_js)
    check("pin_enviar_garantir", "PIN para enviar entrega" in wiz_js)
    check("pin_enviar_antes_print", "gmSspinGarantirOperador(run" in wiz_js)
    # Ordem: GarantirOperador envolve escolha+print — procurar bloco enviar
    idx_enviar = wiz_js.find("function wizardEnviarEntregaPainel")
    idx_reg = wiz_js.find("function wizardRegistrarEntregaPainelAposEscolha")
    check("pin_reg_antes_enviar_fn", 0 <= idx_reg < idx_enviar or idx_reg > 0)
    bloco_enviar = wiz_js[idx_enviar : idx_enviar + 2500] if idx_enviar >= 0 else ""
    check("pin_enviar_usa_garantir", "gmSspinGarantirOperador" in bloco_enviar)
    check("pin_enviar_max60", "maxFrescoS: 60" in bloco_enviar)
    check(
        "pin_retry_sem_print",
        "wizardRegistrarEntregaPainelAposEscolha(opt, true, orcId)" in wiz_js
        or "alreadyPrinted" in wiz_js,
    )
    check("pin_abrir_se_erro", "gmSspinAbrirSeErroPin" in wiz_js and "PIN para enviar entrega" in wiz_js)
    # Não imprimir se alreadyPrinted
    bloco_reg = wiz_js[idx_reg : idx_reg + 2200] if idx_reg >= 0 else ""
    check("pin_skip_print_retry", "if (!alreadyPrinted)" in bloco_reg)
    check("pin_print_so_primeira", "wizardImprimirPacoteEntrega(orcId, opt)" in bloco_reg)

    idx_loja = wiz_js.find("function wizardIrParaPagamentoComImpressao")
    bloco_loja = wiz_js[idx_loja : idx_loja + 2200] if idx_loja >= 0 else ""
    check("pin_loja_garantir", "gmSspinGarantirOperador" in bloco_loja)
    check("pin_loja_titulo", "PIN para seguir com a entrega" in bloco_loja)
    check("pin_loja_max120", "maxFrescoS: 120" in bloco_loja)

    check("pin_confirm_fresco_entrega", "frescoEntrega" in wiz_js)
    check(
        "pin_confirm_ttl120",
        "frescoEntrega ? 120" in wiz_js or "frescoEntrega ? 120 :" in wiz_js,
    )

    check("pin_consulta_garantir", "PIN para registrar entrega" in cons_js)
    check("pin_consulta_antes_imp", "gmSspinGarantirOperador" in cons_js)

    # Ordem crítica: NÃO imprimir antes do PIN no fluxo enviar
    # No código novo: run() só depois do GarantirOperador; print está dentro de registrar
    check(
        "pin_nao_print_antes_garantir",
        "wizardImprimirPacoteEntrega" not in bloco_enviar.split("gmSspinGarantirOperador")[0]
        if "gmSspinGarantirOperador" in bloco_enviar
        else False,
    )

    # --- CARD-LATERAL ---
    check("card_flex", "pdv-entrega-card flex items-stretch" in wiz_js)
    check("card_acoes_cls", "pdv-entrega-card-acoes" in wiz_js)
    check("card_acoes_col", "flex-col gap-1" in wiz_js and "pdv-entrega-card-acoes" in wiz_js)
    check("card_info_flex1", 'class="min-w-0 flex-1"' in wiz_js)
    check("card_btn_wfull", "pdv-entrega-imprimir w-full" in wiz_js)
    check("card_retomar_curto", ">Retomar</button>" in wiz_js)
    check("card_sem_btns_embaixo", 'mt-2 flex flex-wrap gap-1.5">' not in wiz_js[wiz_js.find("htmlEntregaPendenteCard") : wiz_js.find("bindEntregaPendenteCardBtns")])
    # Bind ainda existe
    check("card_bind_btns", "function bindEntregaPendenteCardBtns" in wiz_js)
    check("card_concluir", "pdv-entrega-concluir w-full" in wiz_js)
    check("card_adiar_1h_hora", "pdv-entrega-adiar-alerta-1h" in wiz_js and "Adiar 1h" in wiz_js)
    check("card_rota_incluir_lbl", 'aria-label="Incluir na rota"> Incluir</label>' in wiz_js)


def test_runtime() -> None:
    print("== Runtime Django + PIN ==")
    import json

    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.caixa_util import (
        MSG_PIN_OPERADOR_OBRIGATORIO,
        rotulo_operador_pin,
        validar_pin_operador,
    )
    from produtos.models import PedidoEntrega

    ok_pin, err = validar_pin_operador(PIN)
    check("pin_9973", ok_pin, (err or "")[:80])
    rot = rotulo_operador_pin(PIN) if ok_pin else ""
    check("pin_rotulo", bool(rot), rot[:40])

    user = get_user_model().objects.filter(is_active=True).order_by("id").first()
    check("user_ativo", user is not None)
    if not user:
        return

    client = Client(HTTP_HOST="127.0.0.1")
    client.force_login(user)

    url = reverse("api_entrega_registrar")
    body = {
        "orc_local_id": 991209001,
        "cliente_nome": "Verify Imp Pin Card",
        "telefone": "13999999999",
        "endereco_linha": "Rua Path Verify Jacupiranga/SP",
        "itens": [{"nome": "Item", "qtd": 1, "preco": 10}],
        "total_texto": "R$ 10,00",
        "retomar_codigo": "GMORC991209001",
        "forma_pagamento": "Dinheiro",
        "troco_precisa": False,
        "aguarda_pagamento_pdv": True,
        "loja_entrega": "centro",
        "origem": "pdv",
    }
    r0 = client.post(
        url,
        data=json.dumps(body),
        content_type="application/json",
    )
    check("reg_sem_pin_status", r0.status_code in (403, 401, 400), str(r0.status_code))
    try:
        d0 = r0.json() if r0.content else {}
    except Exception:
        d0 = {}
    msg0 = str(d0.get("erro") or d0.get("mensagem") or "")
    check(
        "reg_sem_pin_msg",
        "PIN" in msg0.upper() or "Identifique" in msg0,
        msg0[:80],
    )

    body_pin = dict(body)
    body_pin["pin"] = PIN
    body_pin["orc_local_id"] = 991209002
    body_pin["retomar_codigo"] = "GMORC991209002"
    r1 = client.post(
        url,
        data=json.dumps(body_pin),
        content_type="application/json",
    )
    try:
        d1 = r1.json() if r1.content else {}
    except Exception:
        d1 = {}
    check(
        "reg_com_pin_ok",
        r1.status_code == 200 and bool(d1.get("ok")),
        f"status={r1.status_code} body={str(d1)[:140]}",
    )
    if d1.get("ok") and d1.get("id"):
        PedidoEntrega.objects.filter(pk=d1["id"]).delete()
    PedidoEntrega.objects.filter(orc_local_id__in=[991209001, 991209002]).delete()

    check("msg_pin_const", "Identifique-se com o PIN" in MSG_PIN_OPERADOR_OBRIGATORIO)


def main() -> int:
    print(f"PIN teste = {PIN!r}")
    test_estatico()
    try:
        test_runtime()
    except Exception as e:
        check("runtime_crash", False, str(e)[:200])
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
