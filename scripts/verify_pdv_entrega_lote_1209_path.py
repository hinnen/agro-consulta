#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prova detalhada — lote PDV entrega 12/09.

Pacotes:
  PDV-CHAT-ENTREGA-DOCK · PDV-ENT-HORARIO-OPCOES · PDV-ENT-TROCO-ENTER · PDV-ENT-OVERLAY-SPLIT

  set AGRO_PIN_TESTE=9973
  python scripts/verify_pdv_entrega_lote_1209_path.py
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
    print("== Estático (4 pacotes) ==")
    chat_html = _read("produtos/templates/produtos/partials/pdv/chat_loja_overlay.html")
    chat_js = _read("produtos/static/produtos/js/pdv_chat_loja.js")
    wiz_js = _read("produtos/static/produtos/js/pdv_wizard.js")
    wiz_html = _read("produtos/templates/produtos/pdv_wizard.html")
    ent_ov = _read("produtos/templates/produtos/partials/pdv/entrega_wizard_overlay.html")
    step = _read("produtos/templates/produtos/partials/pdv/step_produtos.html")
    util = _read("produtos/entrega_pdv_pendente_util.py")
    models = _read("produtos/models.py")
    urls = _read("produtos/urls.py")
    views = _read("produtos/views.py")
    boot = _read("pdv/views.py")
    mig = ROOT / "produtos/migrations/0130_pedido_entrega_pdv_lista_concluida.py"

    # CHAT-ENTREGA-DOCK
    check("chat_css_var", "--pdv-chat-dock-left" in chat_html)
    check("chat_entrega_rule", 'data-pdv-step="entrega"] #pdv-chat-loja-dock' in chat_html)
    check("chat_fallback_27", "27.5rem" in chat_html)
    check("chat_js_repo", "function reposicionarDock" in chat_js)
    check("chat_js_btn_next", "pdv-btn-next" in chat_js and "getBoundingClientRect" in chat_js)
    check("chat_js_reset", "step !== 'entrega'" in chat_js)

    # HORARIO-OPCOES
    check("hor_sem_time", 'type="time"' not in ent_ov)
    check("hor_grade", 'id="pdv-ed-horario-opcoes"' in ent_ov)
    check("hor_hidden", 'id="pdv-entrega-horario"' in ent_ov and 'type="hidden"' in ent_ov)
    for h in range(9, 18):
        check(f"hor_opcao_{h:02d}", f'value="{h:02d}:00"' in ent_ov)
    check("hor_overlay_maior", 'data-pdv-ed-painel="detalhes"]' in wiz_html and "52rem" in wiz_html)
    check("hor_js_exige", "Escolha o horário da entrega" in wiz_js)
    check("hor_js_fixos", "entregaHorariosFixos" in wiz_js and "commitEntregaHorarioOpcao" in wiz_js)
    check("hor_js_bloqueia", "if (!hor)" in wiz_js)

    # TROCO-ENTER
    check("troco_preenche", "function preencherEntregaTrocoSemTroco" in wiz_js)
    check("troco_total", "moneyFieldDisplay(total)" in wiz_js)
    check("troco_vazio_auto", "preencherEntregaTrocoSemTroco" in wiz_js)
    check("troco_alerta_velho_fora", "use 0 ou 0,00 se não precisar" not in wiz_js)
    check("troco_placeholder", "Enter = sem troco" in ent_ov)

    # OVERLAY-SPLIT
    check("ov_largo", ("88rem" in step or "76rem" in step))
    check("ov_split", "pdv-entregas-split" in step)
    check("ov_listas", 'id="pdv-entregas-list-pagar"' in step and 'id="pdv-entregas-list-pagas"' in step)
    check("ov_scroll", "overflow-y: auto" in step)
    check("ov_card", "pdv-entrega-card" in step)
    check("ov_js_card", "function htmlEntregaPendenteCard" in wiz_js)
    check("ov_js_concluir", "function concluirEntregaPagaOverlay" in wiz_js)
    check("ov_btn_concluir", "pdv-entrega-concluir" in wiz_js)
    check("ov_model", "pdv_lista_concluida" in models)
    check("ov_mig", mig.is_file())
    check("ov_qs_filtro", "pdv_lista_concluida=False" in util)
    check("ov_24h", "HORAS_PAGAS_LOJA_PDV = 24" in util)
    check("ov_url", "concluir-overlay/" in urls)
    check("ov_api", "def api_pdv_entrega_pendente_concluir_overlay" in views)
    check("ov_boot", "apiPdvEntregaPendenteConcluirOverlay" in boot)
    check("ov_util_fn", "def concluir_entrega_paga_overlay" in util)


def test_runtime() -> None:
    print("== Runtime Django + PIN ==")
    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse
    from django.utils import timezone
    from datetime import timedelta

    from produtos.caixa_util import rotulo_operador_pin, validar_pin_operador
    from produtos.entrega_pdv_pendente_util import (
        HORAS_PAGAS_LOJA_PDV,
        concluir_entrega_paga_overlay,
        listar_entregas_pagas_loja_pdv,
        queryset_entregas_pagas_loja_pdv,
    )
    from produtos.models import PedidoEntrega, VendaAgro

    ok_pin, err = validar_pin_operador(PIN)
    check("pin_9973", ok_pin, (err or "")[:80])
    rot = rotulo_operador_pin(PIN) if ok_pin else ""
    check("pin_rotulo", bool(rot), rot[:40])
    check("horas_24_const", HORAS_PAGAS_LOJA_PDV == 24)

    check(
        "reverse_concluir",
        reverse("api_pdv_entrega_pendente_concluir_overlay", args=[1]).endswith(
            "/concluir-overlay/"
        ),
    )

    user = get_user_model().objects.filter(is_active=True).order_by("id").first()
    check("user_ativo", user is not None)
    if not user:
        return

    # Migrate aplicada
    try:
        PedidoEntrega.objects.filter(pdv_lista_concluida=False).count()
        check("db_campo_concluida", True)
    except Exception as e:
        check("db_campo_concluida", False, str(e)[:120])
        return

    venda = VendaAgro.objects.create(cliente_nome="Verify Lote1209", total=12, deposito="centro")
    ent = PedidoEntrega.objects.create(
        cliente_nome="Verify Lote1209",
        endereco_linha="Rua Path 12 Jacupiranga/SP",
        itens_json=[{"nome": "Item", "qtd": 1, "preco": 12}],
        total_texto="R$ 12,00",
        forma_pagamento="Pago na loja",
        origem="pdv",
        loja_entrega="centro",
        paga_na_loja=True,
        aguarda_pagamento_pdv=False,
        venda_agro_id=venda.pk,
        status=PedidoEntrega.Status.PENDENTE,
        pdv_lista_concluida=False,
    )
    try:
        ids_antes = {r["id"] for r in listar_entregas_pagas_loja_pdv(loja="centro")}
        check("lista_tem_paga", ent.pk in ids_antes)

        # Concluir util
        e2, err2 = concluir_entrega_paga_overlay(ent.pk, loja="centro")
        check("concluir_util_ok", e2 is not None and not err2, err2 or "")
        check("concluir_flag", bool(e2 and e2.pdv_lista_concluida))
        ids_depois = {r["id"] for r in listar_entregas_pagas_loja_pdv(loja="centro")}
        check("lista_sumiu", ent.pk not in ids_depois)

        # Idempotente
        e3, err3 = concluir_entrega_paga_overlay(ent.pk, loja="centro")
        check("concluir_idempotente", e3 is not None and not err3)

        # 24h: entrega antiga fora da QS
        antiga = PedidoEntrega.objects.create(
            cliente_nome="Verify Antiga 24h",
            endereco_linha="Rua Velha",
            itens_json=[{"nome": "X", "qtd": 1}],
            total_texto="R$ 1,00",
            forma_pagamento="Pago na loja",
            origem="pdv",
            loja_entrega="centro",
            paga_na_loja=True,
            aguarda_pagamento_pdv=False,
            status=PedidoEntrega.Status.PENDENTE,
            pdv_lista_concluida=False,
        )
        PedidoEntrega.objects.filter(pk=antiga.pk).update(
            criado_em=timezone.now() - timedelta(hours=25)
        )
        check(
            "24h_corta",
            antiga.pk not in {x.pk for x in queryset_entregas_pagas_loja_pdv()},
        )
        antiga.delete()

        # HTTP
        c = Client(HTTP_HOST="127.0.0.1")
        c.force_login(user)

        venda2 = VendaAgro.objects.create(
            cliente_nome="Verify HTTP Concluir", total=5, deposito="centro"
        )
        ent_http = PedidoEntrega.objects.create(
            cliente_nome="Verify HTTP Concluir",
            endereco_linha="Av HTTP",
            itens_json=[{"nome": "Y", "qtd": 1}],
            total_texto="R$ 5,00",
            forma_pagamento="Pago na loja",
            origem="pdv",
            loja_entrega="centro",
            paga_na_loja=True,
            aguarda_pagamento_pdv=False,
            venda_agro_id=venda2.pk,
            status=PedidoEntrega.Status.PENDENTE,
            pdv_lista_concluida=False,
        )
        r_list = c.get(reverse("api_pdv_entregas_pendentes") + "?loja=centro")
        d_list = r_list.json() if r_list.status_code == 200 else {}
        ids_pagas = {x.get("id") for x in (d_list.get("itens_pagas") or [])}
        check("http_lista_200", r_list.status_code == 200)
        check("http_aparece_antes", ent_http.pk in ids_pagas)

        r_conc = c.post(
            reverse("api_pdv_entrega_pendente_concluir_overlay", args=[ent_http.pk]),
            data=json.dumps({"loja": "centro"}),
            content_type="application/json",
        )
        d_conc = r_conc.json() if r_conc.status_code == 200 else {}
        check("http_concluir_200", r_conc.status_code == 200, str(r_conc.status_code))
        check("http_concluir_ok", d_conc.get("ok") is True, str(d_conc.get("erro") or "")[:80])

        r_list2 = c.get(reverse("api_pdv_entregas_pendentes") + "?loja=centro")
        d_list2 = r_list2.json() if r_list2.status_code == 200 else {}
        ids_pagas2 = {x.get("id") for x in (d_list2.get("itens_pagas") or [])}
        check("http_sumiu_depois", ent_http.pk not in ids_pagas2)

        # PIN rota: registrar entrega paga (se API exigir pin)
        body_paga = {
            "cliente_nome": "Verify PIN Registrar",
            "endereco_linha": "Rua PIN 1",
            "itens": [{"nome": "Z", "qtd": 1, "preco": 3}],
            "total_texto": "R$ 3,00",
            "forma_pagamento": "Pago na loja",
            "origem": "pdv",
            "loja_entrega": "centro",
            "venda_id": venda2.pk,
            "pin": PIN,
        }
        r_reg = c.post(
            reverse("api_entrega_registrar"),
            data=json.dumps(body_paga),
            content_type="application/json",
        )
        d_reg = r_reg.json() if r_reg.content else {}
        check(
            "http_registrar_pin",
            r_reg.status_code == 200 and d_reg.get("ok") is True,
            str(d_reg.get("erro") or r_reg.status_code)[:100],
        )
        eid_reg = d_reg.get("id")
        if eid_reg:
            PedidoEntrega.objects.filter(pk=eid_reg).delete()

        ent_http.delete()
        venda2.delete()
    finally:
        PedidoEntrega.objects.filter(pk=ent.pk).delete()
        venda.delete()


def main() -> int:
    print("VERIFY PDV-ENTREGA-LOTE-1209")
    print(f"PIN={PIN}")
    test_estatico()
    print()
    try:
        test_runtime()
    except Exception as e:
        check("runtime_crash", False, str(e)[:200])
        print(f"  FAIL runtime_crash — {e}")

    print()
    print(f"RESULT {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("FAILED:", ", ".join(fails))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
