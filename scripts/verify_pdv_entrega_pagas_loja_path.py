# -*- coding: utf-8 -*-
"""Prova detalhada PDV-ENTREGA-PAGAS-24H.

  python scripts/verify_pdv_entrega_pagas_loja_path.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client
from django.urls import reverse
from django.utils import timezone

from produtos.caixa_util import rotulo_operador_pin, validar_pin_operador
from produtos.entrega_pdv_pendente_util import (
    HORAS_PAGAS_LOJA_PDV,
    finalizar_entregas_pagas_pendentes_ao_fechar_caixa,
    listar_entregas_bloqueando_fechamento_caixa,
    listar_entregas_pagas_loja_pdv,
    listar_entregas_pendentes_pdv,
    maps_url_entrega,
    marcar_entrega_pendente_fechada,
    queryset_entregas_pagas_loja_pdv,
)
from produtos.models import PedidoEntrega, SessaoCaixa, VendaAgro

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


def test_arquivos() -> None:
    print("== Arquivos / contratos ==")
    models = _read("produtos/models.py")
    util = _read("produtos/entrega_pdv_pendente_util.py")
    views = _read("produtos/views.py")
    urls = _read("produtos/urls.py")
    js = _read("produtos/static/produtos/js/pdv_wizard.js")
    html = _read("produtos/templates/produtos/partials/pdv/step_produtos.html")
    wiz = _read("produtos/templates/produtos/pdv_wizard.html")
    mig = ROOT / "produtos/migrations/0129_pedido_entrega_paga_na_loja.py"

    check("mig_0129", mig.is_file())
    check("model_field", "paga_na_loja = models.BooleanField" in models)
    check("horas_24", "HORAS_PAGAS_LOJA_PDV = 24" in util)
    check("qs_pagas", "def queryset_entregas_pagas_loja_pdv" in util)
    check("listar_pagas", "def listar_entregas_pagas_loja_pdv" in util)
    check("maps_fn", "def maps_url_entrega" in util)
    check("url_api", "api/pdv/entregas-pendentes/" in urls)
    check("api_itens_pagas", '"itens_pagas": itens_pagas' in views)
    check("registrar_flag", '"paga_na_loja": True' in views)
    check("tab_pagar", 'id="pdv-entregas-tab-pagar"' in html)
    check("tab_pagas", 'id="pdv-entregas-tab-pagas"' in html)
    check("btn_rota", 'id="pdv-entregas-rota-pagas"' in html)
    check("help_24h", "24 h" in html or "24h" in html.lower())
    check("js_aba", "entregasPendentesAba" in js)
    check("js_itens_pagas", "itensPagas" in js)
    check("js_rota", "function abrirRotaEntregasPagas" in js)
    check("js_maps", "function abrirMapsEntregaPendente" in js)
    check("js_sem_cancel_pagas", "pode_cancelar" in js)
    check("css_pagas_btn", "entregas-pagas" in wiz)
    check("bloqueio_nao_usa_paga", "paga_na_loja" not in util.split("def queryset_entregas_bloqueando")[1][:500])


def test_pin() -> None:
    print("== PIN 9973 ==")
    ok_pin, err = validar_pin_operador(PIN)
    check("pin_valido", ok_pin, (err or "")[:80])
    rot = rotulo_operador_pin(PIN) if ok_pin else ""
    check("pin_rotulo", bool(rot), rot[:40])


def test_db() -> None:
    print("== Dados / caixa ==")
    agora = timezone.now()
    user = get_user_model().objects.filter(is_superuser=True).first() or get_user_model().objects.first()
    check("tem_user", user is not None)
    if not user:
        return
    sess = SessaoCaixa.objects.create(usuario=user, ponto_caixa="gaveta", valor_abertura=0)
    venda = VendaAgro.objects.create(
        cliente_nome="Verify Pagas Loja",
        total=10,
        sessao_caixa=sess,
        deposito="centro",
    )
    nova = PedidoEntrega.objects.create(
        cliente_nome="Verify Paga Loja",
        status=PedidoEntrega.Status.PENDENTE,
        aguarda_pagamento_pdv=False,
        paga_na_loja=True,
        forma_pagamento="Pago na loja",
        loja_entrega="centro",
        plus_code="8X5R+7M9 Jacupiranga",
        endereco_linha="Rua Teste 1",
        itens_json=[{"nome": "X", "qtd": 1}],
        total_texto="R$ 1,00",
        venda_agro=venda,
        sessao_caixa=sess,
    )
    velha = PedidoEntrega.objects.create(
        cliente_nome="Verify Paga Velha",
        status=PedidoEntrega.Status.PENDENTE,
        aguarda_pagamento_pdv=False,
        paga_na_loja=True,
        forma_pagamento="Pago na loja",
        loja_entrega="centro",
        itens_json=[{"nome": "Y", "qtd": 1}],
        total_texto="R$ 2,00",
    )
    cobranca = PedidoEntrega.objects.create(
        cliente_nome="Verify A Pagar",
        status=PedidoEntrega.Status.PENDENTE,
        aguarda_pagamento_pdv=True,
        paga_na_loja=False,
        loja_entrega="centro",
        pdv_wizard_state={"x": 1},
        itens_json=[{"nome": "Z", "qtd": 1}],
        total_texto="R$ 3,00",
        sessao_caixa=sess,
    )
    vila_paga = PedidoEntrega.objects.create(
        cliente_nome="Verify Vila Paga",
        status=PedidoEntrega.Status.PENDENTE,
        aguarda_pagamento_pdv=False,
        paga_na_loja=True,
        forma_pagamento="Pago na loja",
        loja_entrega="vila",
        itens_json=[{"nome": "V", "qtd": 1}],
        total_texto="R$ 4,00",
    )
    cancelada = PedidoEntrega.objects.create(
        cliente_nome="Verify Paga Cancel",
        status=PedidoEntrega.Status.CANCELADO,
        aguarda_pagamento_pdv=False,
        paga_na_loja=True,
        forma_pagamento="Pago na loja",
        loja_entrega="centro",
        itens_json=[{"nome": "C", "qtd": 1}],
        total_texto="R$ 5,00",
    )
    try:
        PedidoEntrega.objects.filter(pk=velha.pk).update(
            criado_em=agora - timedelta(hours=HORAS_PAGAS_LOJA_PDV + 2)
        )
        ids_pagas_c = {r["id"] for r in listar_entregas_pagas_loja_pdv(loja="centro")}
        ids_pagas_v = {r["id"] for r in listar_entregas_pagas_loja_pdv(loja="vila")}
        ids_bloq = {
            r["id"]
            for r in listar_entregas_bloqueando_fechamento_caixa(
                sessao_ids=[sess.pk], loja="centro"
            )
        }
        ids_pend = {r["id"] for r in listar_entregas_pendentes_pdv(loja="centro")}
        check("paga_aparece_24h", nova.pk in ids_pagas_c)
        check("velha_some", velha.pk not in ids_pagas_c)
        check("cancel_some", cancelada.pk not in ids_pagas_c)
        check("vila_nao_no_centro", vila_paga.pk not in ids_pagas_c)
        check("vila_na_vila", vila_paga.pk in ids_pagas_v)
        check("cobranca_nao_paga", cobranca.pk not in ids_pagas_c)
        check("paga_nao_trava_caixa", nova.pk not in ids_bloq)
        check("cobranca_trava", cobranca.pk in ids_bloq)
        check("paga_nao_lista_a_pagar", nova.pk not in ids_pend)
        check("qs_paga_flag", queryset_entregas_pagas_loja_pdv().filter(pk=nova.pk).exists())
        row = next((r for r in listar_entregas_pagas_loja_pdv(loja="centro") if r["id"] == nova.pk), None)
        check("sem_retomar", bool(row) and row.get("pode_retomar") is False)
        check("sem_adiar", bool(row) and row.get("pode_adiar") is False)
        check("sem_cancelar", bool(row) and row.get("pode_cancelar") is False)
        check("sem_assumir", bool(row) and row.get("pode_assumir") is False)
        check("pode_imprimir", bool(row) and row.get("pode_imprimir") is True)
        check("maps_plus", "8X5R" in (maps_url_entrega(nova) or ""))
        check("row_maps", bool(row) and "maps.google" in (row.get("maps_url") or "") or "search" in (row.get("maps_url") or ""))

        n_fin = finalizar_entregas_pagas_pendentes_ao_fechar_caixa([sess.pk])
        nova.refresh_from_db()
        check("fechar_caixa_marca_entregue", nova.status == PedidoEntrega.Status.ENTREGUE, nova.status)
        check("fechar_caixa_ainda_paga_flag", nova.paga_na_loja is True)
        ids_pagas_depois = {r["id"] for r in listar_entregas_pagas_loja_pdv(loja="centro")}
        check("ainda_no_overlay_24h", nova.pk in ids_pagas_depois, f"fin={n_fin}")
        ids_bloq2 = {
            r["id"]
            for r in listar_entregas_bloqueando_fechamento_caixa(
                sessao_ids=[sess.pk], loja="centro"
            )
        }
        check("entregue_nao_trava", nova.pk not in ids_bloq2)

        marcar_entrega_pendente_fechada(cobranca.pk, venda_agro_id=venda.pk)
        cobranca.refresh_from_db()
        check("cobrada_vira_entregue", cobranca.status == PedidoEntrega.Status.ENTREGUE)
        check("cobrada_nao_vira_paga_loja", cobranca.paga_na_loja is False)
        ids_pagas_3 = {r["id"] for r in listar_entregas_pagas_loja_pdv(loja="centro")}
        check("cobrada_nao_aba_pagas", cobranca.pk not in ids_pagas_3)
    finally:
        PedidoEntrega.objects.filter(
            pk__in=[nova.pk, velha.pk, cobranca.pk, vila_paga.pk, cancelada.pk]
        ).delete()
        VendaAgro.objects.filter(pk=venda.pk).delete()
        SessaoCaixa.objects.filter(pk=sess.pk).delete()


def test_http() -> None:
    print("== HTTP Django Client ==")
    user = get_user_model().objects.filter(is_active=True).first()
    check("http_user", user is not None)
    if not user:
        return
    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(user)
    r_anon = Client(HTTP_HOST="127.0.0.1").get(reverse("api_pdv_entregas_pendentes") + "?loja=centro")
    check("anon_redirect_ou_401", r_anon.status_code in (302, 401, 403), str(r_anon.status_code))

    r = c.get(reverse("api_pdv_entregas_pendentes") + "?loja=centro")
    check("api_200", r.status_code == 200, str(r.status_code))
    if r.status_code != 200:
        return
    data = r.json()
    check("api_ok", data.get("ok") is True)
    check("api_tem_itens", isinstance(data.get("itens"), list))
    check("api_tem_pagas", isinstance(data.get("itens_pagas"), list))
    check("api_total_pagas", "total_pagas" in data)

    venda = VendaAgro.objects.create(cliente_nome="HTTP Paga Loja", total=8, deposito="centro")
    ent_ids = []
    try:
        body_paga = {
            "cliente_nome": "HTTP Paga Loja",
            "endereco_linha": "Av Teste 100 Centro",
            "plus_code": "8X5R+7M9 Jacupiranga",
            "itens": [{"nome": "Racao", "qtd": 1, "preco": 8}],
            "total_texto": "R$ 8,00",
            "forma_pagamento": "Pago na loja",
            "origem": "pdv",
            "loja_entrega": "centro",
            "venda_id": venda.pk,
            "pin": PIN,
        }
        r2 = c.post(
            reverse("api_entrega_registrar"),
            data=json.dumps(body_paga),
            content_type="application/json",
        )
        check("registrar_paga_http", r2.status_code == 200, str(r2.status_code)[:80])
        d2 = r2.json() if r2.status_code == 200 else {}
        check("registrar_paga_ok", d2.get("ok") is True, str(d2.get("erro") or "")[:80])
        eid = d2.get("id")
        if eid:
            ent_ids.append(eid)
            obj = PedidoEntrega.objects.filter(pk=eid).first()
            check("http_flag_paga", bool(obj) and obj.paga_na_loja is True)
            check("http_nao_aguarda", bool(obj) and obj.aguarda_pagamento_pdv is False)

        r3 = c.get(reverse("api_pdv_entregas_pendentes") + "?loja=centro")
        d3 = r3.json() if r3.status_code == 200 else {}
        ids_pagas = {x.get("id") for x in (d3.get("itens_pagas") or [])}
        ids_pagar = {x.get("id") for x in (d3.get("itens") or [])}
        check("http_aparece_pagas", eid in ids_pagas if eid else False)
        check("http_nao_aparece_pagar", eid not in ids_pagar if eid else False)

        body_cobrar = {
            "cliente_nome": "HTTP A Pagar",
            "endereco_linha": "Rua Cobrar 2",
            "itens": [{"nome": "Item", "qtd": 1}],
            "total_texto": "R$ 9,00",
            "forma_pagamento": "Dinheiro",
            "troco_precisa": False,
            "aguarda_pagamento_pdv": True,
            "pdv_wizard_state": {"entrega": {"localPagamento": "entrega"}},
            "origem": "pdv",
            "loja_entrega": "centro",
            "pin": PIN,
        }
        r4 = c.post(
            reverse("api_entrega_registrar"),
            data=json.dumps(body_cobrar),
            content_type="application/json",
        )
        d4 = r4.json() if r4.status_code == 200 else {}
        check("registrar_cobrar_ok", d4.get("ok") is True, str(d4.get("erro") or r4.status_code)[:80])
        eid2 = d4.get("id")
        if eid2:
            ent_ids.append(eid2)
            obj2 = PedidoEntrega.objects.filter(pk=eid2).first()
            check("http_cobrar_aguarda", bool(obj2) and obj2.aguarda_pagamento_pdv is True)
            check("http_cobrar_nao_paga", bool(obj2) and obj2.paga_na_loja is False)

        c_sem = Client(HTTP_HOST="127.0.0.1")
        c_sem.force_login(user)
        r5 = c_sem.post(
            reverse("api_entrega_registrar"),
            data=json.dumps({k: v for k, v in body_paga.items() if k != "pin"}),
            content_type="application/json",
        )
        d5 = r5.json() if r5.content else {}
        check(
            "registrar_sem_pin_recusa",
            r5.status_code in (403, 400) or d5.get("ok") is False,
            str(r5.status_code),
        )
    finally:
        if ent_ids:
            PedidoEntrega.objects.filter(pk__in=ent_ids).delete()
        VendaAgro.objects.filter(pk=venda.pk).delete()


def main() -> None:
    print("=== PDV-ENTREGA-PAGAS-24H detalhada ===")
    test_arquivos()
    test_pin()
    try:
        test_db()
    except Exception as ex:
        check("db_exc", False, str(ex)[:240])
    try:
        test_http()
    except Exception as ex:
        check("http_exc", False, str(ex)[:240])
    print("")
    print(f"VERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhas: " + ", ".join(fails))
        sys.exit(1)


if __name__ == "__main__":
    main()
