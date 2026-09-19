#!/usr/bin/env python3
"""Prova PDV-VALE-USADO — pagar com vale baixa o saldo (bug loja #16).

  python scripts/verify_vale_credito_venda_path.py

Fonte · payload · ORM · API Django Client (PIN 9973 + caixa) · HTTP se runserver.
VERIFY_OK N/N · VERIFY_FAIL.
"""
from __future__ import annotations

import json
import os
import sys
import uuid
from decimal import Decimal
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

PIN = "9973"
BASE = os.environ.get("AGRO_VERIFY_BASE", "http://127.0.0.1:8000").rstrip("/")
TAG = f"ZZ-VALE16-{uuid.uuid4().hex[:8]}"
USER_BOT = "vale16_verify_bot"
CHECKS = 0
FAILS: list[str] = []


def fail(msg: str) -> None:
    FAILS.append(msg)
    print(f"FAIL {msg}")


def ok(msg: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"OK {msg}")


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def prova_fonte() -> None:
    print("=== estático ===")
    util = _read("produtos/vale_credito_venda_util.py")
    views = _read("produtos/views.py")
    models = _read("produtos/models.py")
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")
    caixa = _read("produtos/caixa_util.py")
    check("def valor_vale_credito_usado_no_payload" in util, "util usado")
    check("def aplicar_movimento_vale_credito_venda" in util, "util aplicar")
    check("payload_e_compra_vale_credito" in util, "util ignora compra de vale")
    check("externo_id" in util, "devolução acha cliente por agro/ERP")
    check('VALE_USADO = "vale_usado"' in models, "evento VALE_USADO")
    check('VALE_DEVOLUCAO = "vale_devolucao"' in models, "evento VALE_DEVOLUCAO")
    check('"Vale crédito"' in caixa, "forma Vale crédito no caixa")
    check("validar_vale_credito_payload" in views, "API valida vale antes de gravar")
    check("aplicar_movimento_vale_credito_venda" in views, "persist baixa vale")
    check("cliente_saldos" in views, "resposta devolve saldo")
    check("creditar_vale_devolucao" in views, "devolução credita vale")
    check("except ValueError" in views and "vale_credito" in views, "ValueError vira 400")
    check("aplicarSaldoClienteNoPdv" in wizard, "JS aplica saldo na tela")
    check("cliente_saldos" in wizard, "JS lê cliente_saldos")
    check("loadWizardClientesCache(true)" in wizard, "JS recarrega lista após vale")
    check("forma === 'Vale crédito'" in wizard, "JS forma Vale crédito")
    check("Math.min(saldoValeAtual(st), rest)" in wizard, "JS não lança vale acima do saldo")


def prova_payload() -> None:
    print("=== payload ===")
    import django

    django.setup()
    from produtos.vale_credito_venda_util import valor_vale_credito_usado_no_payload

    misto = {
        "pagamentos": [
            {"formaPagamento": "Dinheiro", "valorPagamento": 10},
            {"formaPagamento": "Vale crédito", "valorPagamento": "15,50"},
        ]
    }
    check(
        valor_vale_credito_usado_no_payload(misto) == Decimal("15.50"),
        "soma só o vale (15,50) no misto",
    )
    compra = {
        "compra_vale_credito": True,
        "pagamentos": [{"formaPagamento": "Vale crédito", "valorPagamento": 20}],
        "itens": [{"id": "vale-credito", "qtd": 1, "preco": 20}],
    }
    check(valor_vale_credito_usado_no_payload(compra) == Decimal("0"), "compra de vale não baixa")
    legado = {"forma_pagamento": "Vale crédito", "total": "8.00"}
    check(valor_vale_credito_usado_no_payload(legado) == Decimal("8.00"), "forma única legado")
    check(valor_vale_credito_usado_no_payload({}) == Decimal("0"), "sem pagamento = 0")
    check(valor_vale_credito_usado_no_payload(None) == Decimal("0"), "payload vazio = 0")


def _limpar(user=None) -> None:
    from produtos.models import (
        ClienteAgro,
        ClienteAgroEventoAgro,
        ItemVendaAgro,
        SessaoCaixa,
        VendaAgro,
    )

    qs = ClienteAgro.objects.filter(nome__startswith=TAG)
    pks = list(qs.values_list("pk", flat=True))
    vids = list(VendaAgro.objects.filter(cliente_nome__startswith=TAG).values_list("pk", flat=True))
    ItemVendaAgro.objects.filter(venda_id__in=vids).delete()
    VendaAgro.objects.filter(pk__in=vids).delete()
    ClienteAgroEventoAgro.objects.filter(cliente_pk_snap__in=pks).delete()
    qs.delete()
    if user is not None:
        SessaoCaixa.objects.filter(usuario=user, fechado_em__isnull=True).delete()


def prova_django() -> None:
    print("=== Django ORM + API ===")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from django.urls import reverse

    from base.models import PerfilUsuario
    from produtos.caixa_util import operador_label_de_pin
    from produtos.models import (
        ClienteAgro,
        ClienteAgroEventoAgro,
        SessaoCaixa,
        VendaAgro,
    )
    from produtos.vale_credito_venda_util import (
        aplicar_movimento_vale_credito_venda,
        creditar_vale_devolucao,
        validar_vale_credito_payload,
    )

    pin_ok, label, err = operador_label_de_pin(PIN)
    check(pin_ok and bool(label) and not err, f"PIN 9973 operador={label!r}")

    User = get_user_model()
    user, _ = User.objects.get_or_create(username=USER_BOT, defaults={"is_staff": True, "is_superuser": True})
    user.set_password("verify-bot-x")
    user.is_staff = True
    user.is_superuser = True
    user.save()
    perfil, _ = PerfilUsuario.objects.get_or_create(user=user, defaults={"senha_rapida": PIN, "codigo_vendedor": "Z16"})
    if perfil.senha_rapida != PIN:
        perfil.senha_rapida = PIN
        perfil.save(update_fields=["senha_rapida"])

    _limpar(user)
    try:
        cli = ClienteAgro.objects.create(
            nome=f"{TAG} CLI",
            ativo=True,
            editado_local=True,
            saldo_vale_credito=Decimal("40.00"),
            saldo_cashback=Decimal("0"),
        )
        data_ok = {
            "cliente_agro_pk": cli.pk,
            "pagamentos": [{"forma": "Vale crédito", "valor": 12.5}],
        }
        vok, vmsg, _ = validar_vale_credito_payload(data_ok, cliente_agro=cli)
        check(vok, "valida 12,50 em 40,00")
        out = aplicar_movimento_vale_credito_venda(data_ok, cliente_agro=cli, venda_pk=1, usuario="verify")
        cli.refresh_from_db()
        check(out.get("aplicado") is True and cli.saldo_vale_credito == Decimal("27.50"), "baixa 40 para 27,50")
        check(
            ClienteAgroEventoAgro.objects.filter(
                cliente_agro=cli, tipo=ClienteAgroEventoAgro.Tipo.VALE_USADO
            ).exists(),
            "grava evento vale_usado",
        )

        data_alto = {
            "cliente_agro_pk": cli.pk,
            "pagamentos": [{"forma": "Vale crédito", "valor": 99}],
        }
        vok2, msg2, _ = validar_vale_credito_payload(data_alto, cliente_agro=cli)
        check(not vok2 and "acima" in (msg2 or "").lower(), "bloqueia acima do saldo")

        data_cf = {"pagamentos": [{"forma": "Vale crédito", "valor": 1}]}
        vok3, msg3, _ = validar_vale_credito_payload(data_cf, cliente_agro=None)
        check(not vok3 and "cadastrado" in (msg3 or "").lower(), "sem cliente cadastrado recusa")

        venda_fake = VendaAgro.objects.create(
            cliente_nome=cli.nome,
            cliente_id_erp=f"agro:{cli.pk}",
            total=Decimal("12.50"),
            forma_pagamento="Vale crédito",
            deposito="centro",
        )
        cred = creditar_vale_devolucao(venda=venda_fake, valor=Decimal("12.50"), usuario="verify")
        cli.refresh_from_db()
        check(cred.get("ok") and cli.saldo_vale_credito == Decimal("40.00"), "devolução devolve 12,50")

        sess = SessaoCaixa.objects.create(
            usuario=user,
            ponto_caixa=SessaoCaixa.PontoCaixa.TESTE,
            valor_abertura=Decimal("10"),
        )
        client = Client(HTTP_HOST="127.0.0.1")
        client.force_login(user)
        s = client.session
        s["pdv_sessao_caixa_id"] = sess.pk
        s["pdv_ponto_operacao"] = "teste"
        s.save()

        payload = {
            "pin": PIN,
            "cliente": cli.nome,
            "cliente_agro_pk": cli.pk,
            "itens": [{"id": "verify-vale-16", "nome": "Item prova vale", "qtd": 1, "preco": 10, "unidade": "UN"}],
            "forma_pagamento": "Vale crédito",
            "pagamentos": [{"formaPagamento": "Vale crédito", "valorPagamento": 10}],
            "deposito": "centro",
        }
        with override_settings(
            ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"],
            PDV_VENDA_ERP_ENVIO=False,
            PDV_BAIXA_ESTOQUE_AGRO_NA_VENDA=False,
            NFC_E_ENABLED=False,
        ):
            r = client.post(
                reverse("api_enviar_pedido_erp"),
                data=json.dumps(payload),
                content_type="application/json",
                HTTP_HOST="127.0.0.1",
            )
        body = r.json() if "json" in (r.get("Content-Type") or "") else {}
        check(r.status_code == 200 and body.get("ok") is True, f"API venda vale HTTP {r.status_code} ok={body.get('ok')}")
        saldos = body.get("cliente_saldos") or {}
        check(
            abs(float(saldos.get("saldo_vale_credito") or -1) - 30.0) < 0.009,
            f"API devolve saldo 30,00 ({saldos})",
        )
        cli.refresh_from_db()
        check(cli.saldo_vale_credito == Decimal("30.00"), "Postgres 40-10=30 apos API")
        vid = body.get("venda_id")
        vdb = VendaAgro.objects.filter(pk=vid).first()
        check(vdb is not None and str(vdb.forma_pagamento).lower().find("vale") >= 0, "venda gravada com forma vale")

        payload_alto = dict(payload)
        payload_alto["pagamentos"] = [{"formaPagamento": "Vale crédito", "valorPagamento": 999}]
        payload_alto["itens"] = [{"id": "verify-vale-16b", "nome": "Item alto", "qtd": 1, "preco": 999, "unidade": "UN"}]
        with override_settings(
            ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"],
            PDV_VENDA_ERP_ENVIO=False,
            PDV_BAIXA_ESTOQUE_AGRO_NA_VENDA=False,
            NFC_E_ENABLED=False,
        ):
            r2 = client.post(
                reverse("api_enviar_pedido_erp"),
                data=json.dumps(payload_alto),
                content_type="application/json",
                HTTP_HOST="127.0.0.1",
            )
        b2 = r2.json() if "json" in (r2.get("Content-Type") or "") else {}
        check(r2.status_code == 400 and b2.get("ok") is False, "API recusa vale acima do saldo")
        cli.refresh_from_db()
        check(cli.saldo_vale_credito == Decimal("30.00"), "saldo intacto após recusa")

        payload_compra = {
            "pin": PIN,
            "cliente": cli.nome,
            "cliente_agro_pk": cli.pk,
            "compra_vale_credito": True,
            "itens": [{"id": "vale-credito", "nome": "Vale crédito", "qtd": 1, "preco": 5, "unidade": "UN"}],
            "forma_pagamento": "Dinheiro",
            "pagamentos": [{"formaPagamento": "Dinheiro", "valorPagamento": 5}],
            "deposito": "centro",
        }
        with override_settings(
            ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"],
            PDV_VENDA_ERP_ENVIO=False,
            PDV_BAIXA_ESTOQUE_AGRO_NA_VENDA=False,
            NFC_E_ENABLED=False,
        ):
            r3 = client.post(
                reverse("api_enviar_pedido_erp"),
                data=json.dumps(payload_compra),
                content_type="application/json",
                HTTP_HOST="127.0.0.1",
            )
        b3 = r3.json() if "json" in (r3.get("Content-Type") or "") else {}
        check(r3.status_code == 200 and b3.get("ok") is True, "API compra de vale (crédito) ok")
        cli.refresh_from_db()
        check(cli.saldo_vale_credito == Decimal("35.00"), "compra de vale soma 5 (30 para 35)")
    finally:
        _limpar(user)


def prova_http() -> None:
    print("=== HTTP local ===")
    try:
        req = Request(BASE + "/healthz", method="GET")
        with urlopen(req, timeout=3) as resp:
            check(resp.status == 200, f"healthz {BASE} 200")
    except (URLError, OSError, TimeoutError) as exc:
        print(f"(HTTP pulado — runserver não está no ar: {exc})")


def main() -> int:
    prova_fonte()
    prova_payload()
    prova_django()
    prova_http()
    if FAILS:
        print(f"\nVERIFY_FAIL {len(FAILS)}")
        for m in FAILS:
            print(" -", m)
        return 1
    print(f"\nVERIFY_OK {CHECKS}/{CHECKS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
