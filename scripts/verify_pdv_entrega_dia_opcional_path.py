#!/usr/bin/env python3
"""PDV-ENT-DIA-OPCIONAL — dia da entrega opcional no fechamento."""
from __future__ import annotations

import os
import sys
from datetime import date, time as dtime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ.setdefault("SECRET_KEY", "verify-pdv-ent-dia")

import django

django.setup()

import json

from django.test import Client
from django.utils import timezone

from produtos.caixa_util import validar_pin_operador
from produtos.entrega_pdv_pendente_util import (
    data_hoje_loja,
    listar_entregas_bloqueando_fechamento_caixa,
    listar_entregas_pagas_loja_pdv,
    listar_entregas_pendentes_pdv,
    parse_data_prevista_entrega,
    qs_excluindo_adiadas_futuras,
    queryset_entregas_pagas_loja_pdv,
)
from produtos.models import PedidoEntrega, SessaoCaixa

PASS = 0
FAIL = 0


def check(ok: bool, msg: str) -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  OK  {msg}")
    else:
        FAIL += 1
        print(f" FAIL {msg}")


def testar_banco_e_pin() -> None:
    print("=== banco + PIN ===")
    from django.db import connection

    sql_bloq = str(
        qs_excluindo_adiadas_futuras(PedidoEntrega.objects.all()).query
    )
    check("data_prevista" in sql_bloq and "caixa_adiada_para" in sql_bloq, "SQL do caixa ignora dia futuro")
    sql_pagas = str(queryset_entregas_pagas_loja_pdv().query)
    check("data_prevista" in sql_pagas and "paga_na_loja" in sql_pagas, "SQL das pagas segura o dia combinado")

    try:
        with connection.cursor() as cur:
            cur.execute("SELECT data_prevista FROM produtos_pedidoentrega LIMIT 0")
    except Exception as ex:
        print("  --  banco local sem a coluna (falta migrate 0133). PIN não rodou aqui.")
        return

    ok_pin, err_pin = validar_pin_operador("9973")
    check(ok_pin, "PIN 9973 aceito" + ("" if ok_pin else f" ({err_pin})"))
    ruim_pin, _ = validar_pin_operador("0000")
    check(not ruim_pin, "PIN errado recusado")

    user = SessaoCaixa._meta.get_field("usuario").remote_field.model.objects.filter(
        is_superuser=True
    ).first()
    if user is None:
        user = SessaoCaixa._meta.get_field("usuario").remote_field.model.objects.first()
    check(user is not None, "tem usuário no banco")
    if user is None:
        return

    hoje = data_hoje_loja()
    amanha = hoje + timedelta(days=1)
    sess = SessaoCaixa.objects.create(usuario=user, ponto_caixa="gaveta", valor_abertura=0)
    ent = PedidoEntrega.objects.create(
        cliente_nome="Prova dia entrega",
        aguarda_pagamento_pdv=True,
        status=PedidoEntrega.Status.PENDENTE,
        sessao_caixa=sess,
        loja_entrega="centro",
        loja_pagamento="centro",
        hora_prevista=dtime(10, 0),
        data_prevista=amanha,
        origem="pdv",
    )
    paga = PedidoEntrega.objects.create(
        cliente_nome="Prova dia paga",
        paga_na_loja=True,
        pdv_lista_concluida=False,
        status=PedidoEntrega.Status.PENDENTE,
        loja_entrega="centro",
        data_prevista=amanha,
        origem="pdv",
    )
    criado_ids = [ent.pk, paga.pk]
    try:
        PedidoEntrega.objects.filter(pk=paga.pk).update(
            criado_em=timezone.now() - timedelta(days=3)
        )
        bloq = listar_entregas_bloqueando_fechamento_caixa(sessao_ids=[sess.pk], loja="centro")
        check(not any(r["id"] == ent.pk for r in bloq), "amanhã não trava o caixa de hoje")
        vis = listar_entregas_pendentes_pdv(loja="centro")
        row = next((r for r in vis if r["id"] == ent.pk), None)
        check(row is not None and row.get("data_prevista") == amanha.isoformat(), "lista mostra o dia")
        pagas = listar_entregas_pagas_loja_pdv(loja="centro")
        check(any(r["id"] == paga.pk for r in pagas), "paga na loja fica até o dia combinado")

        ent.data_prevista = hoje
        ent.save(update_fields=["data_prevista", "atualizado_em"])
        bloq_hoje = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[sess.pk], loja="centro"
        )
        check(any(r["id"] == ent.pk for r in bloq_hoje), "no dia combinado trava o caixa")

        paga.data_prevista = hoje - timedelta(days=1)
        paga.save(update_fields=["data_prevista", "atualizado_em"])
        pagas2 = listar_entregas_pagas_loja_pdv(loja="centro")
        check(not any(r["id"] == paga.pk for r in pagas2), "dia passado e fora das 24h some das pagas")

        client = Client()
        body = {
            "cliente_nome": "Prova API dia",
            "hora_prevista": "11:00",
            "data_prevista": amanha.isoformat(),
            "forma_pagamento": "PIX",
            "pin": "9973",
            "origem": "pdv",
            "loja_entrega": "centro",
            "loja_pagamento": "centro",
            "aguarda_pagamento_pdv": True,
            "itens": [],
        }
        if ok_pin:
            resp = client.post(
                "/entregas/api/registrar/",
                data=json.dumps(body),
                content_type="application/json",
                HTTP_HOST="127.0.0.1",
            )
            data = {}
            try:
                data = resp.json()
            except Exception:
                data = {}
            check(resp.status_code == 200 and data.get("ok"), f"API grava amanhã ({resp.status_code})")
            if data.get("id"):
                criado_ids.append(int(data["id"]))
                salvo = PedidoEntrega.objects.filter(pk=data["id"]).first()
                check(
                    salvo is not None and salvo.data_prevista == amanha,
                    "registro ficou com o dia futuro",
                )
            body["data_prevista"] = (hoje - timedelta(days=1)).isoformat()
            resp_p = client.post(
                "/entregas/api/registrar/",
                data=json.dumps(body),
                content_type="application/json",
                HTTP_HOST="127.0.0.1",
            )
            try:
                data_p = resp_p.json()
            except Exception:
                data_p = {}
            check(resp_p.status_code == 400 and "passado" in str(data_p.get("erro") or ""), "API recusa dia passado")
            if data_p.get("id"):
                criado_ids.append(int(data_p["id"]))
            body["pin"] = "0000"
            body["data_prevista"] = amanha.isoformat()
            resp_z = client.post(
                "/entregas/api/registrar/",
                data=json.dumps(body),
                content_type="application/json",
                HTTP_HOST="127.0.0.1",
            )
            check(resp_z.status_code == 403, "API recusa PIN errado")
    finally:
        PedidoEntrega.objects.filter(pk__in=criado_ids).delete()
        SessaoCaixa.objects.filter(pk=sess.pk).delete()


def main() -> int:
    html = (ROOT / "produtos/templates/produtos/partials/pdv/entrega_wizard_overlay.html").read_text(
        encoding="utf-8"
    )
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    util = (ROOT / "produtos/entrega_pdv_pendente_util.py").read_text(encoding="utf-8")
    model = (ROOT / "produtos/models.py").read_text(encoding="utf-8")
    mig = (ROOT / "produtos/migrations/0133_pedido_entrega_data_prevista.py").read_text(encoding="utf-8")

    print("=== entrega dia opcional ===")
    check('id="pdv-ed-dia-opcoes"' in html, "botões Hoje / Amanhã / Outro dia")
    check('value="hoje"' in html and 'value="amanha"' in html and 'value="outro"' in html, "3 opções")
    check('id="pdv-entrega-dia-outro"' in html and 'type="date"' in html, "data só em Outro dia")
    check("commitEntregaDiaOpcao" in js and "data_prevista:" in js, "JS grava data_prevista")
    check("outro.value = modo === 'outro' ? iso : ''" in js, "Outro dia não herda data da venda anterior")
    check("dia && dia > hoje) return 0" in js, "alerta não dispara em dia futuro")
    check("if (!dia.ok)" in js, "F7 recusa Outro dia vazio")
    check("entregaEhDoDiaUi" in js, "contagem do botão ignora entrega de outro dia")
    check("data_prevista" in model and "data_prevista" in mig, "campo no model + migrate 0133")
    check("Q(data_prevista__isnull=True)" in util, "caixa de hoje não trava dia futuro")
    check("data_prevista__gte=hoje" in util, "pagas na loja ficam até o dia combinado")

    hoje = date(2026, 9, 29)
    vazio, err = parse_data_prevista_entrega("", hoje)
    check(vazio is None and err == "", "vazio = hoje (não grava)")
    mesmo, err2 = parse_data_prevista_entrega("2026-09-29", hoje)
    check(mesmo is None and err2 == "", "hoje explícito = não grava")
    amanha, err3 = parse_data_prevista_entrega("2026-09-30", hoje)
    check(amanha == hoje + timedelta(days=1) and err3 == "", "amanhã grava")
    br, err4 = parse_data_prevista_entrega("02/10/2026", hoje)
    check(br == date(2026, 10, 2) and err4 == "", "data BR grava")
    passado, err5 = parse_data_prevista_entrega("2026-09-28", hoje)
    check(passado is None and "passado" in err5, "passado recusa")
    ruim, err6 = parse_data_prevista_entrega("semana que vem", hoje)
    check(ruim is None and err6 != "", "texto inválido recusa")

    try:
        testar_banco_e_pin()
    except Exception as ex:
        check(False, "banco explodiu: " + str(ex)[:180])

    print(f"\n{PASS} ok · {FAIL} fail")
    if FAIL:
        print("FAILED")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
