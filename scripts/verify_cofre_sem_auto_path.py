#!/usr/bin/env python
"""Fechar caixa não credita cofrinho e não baixa o esperado da gaveta."""
from __future__ import annotations

import os
import re
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ["DEBUG"] = "False"

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client
from django.utils import timezone

from produtos.caixa_util import (
    filtrar_sessoes_por_deposito,
    serializar_estado_conferencia_fechar,
    validar_pin_operador,
)
from produtos.models import MovimentoCaixa, RepasseVilaReservaMovimentoAgro, SessaoCaixa
from produtos.repasse_vila_util import COFRE_SALARIO, COFRE_VILA_ELIAS, saldo_cofrinho_vila

User = get_user_model()
fails: list[str] = []
oks = 0
PREFIX = "verify-cofre-sem-auto"


def check(cond, msg: str) -> None:
    global oks
    if cond:
        oks += 1
        print("OK", msg)
    else:
        fails.append(msg)
        print("FAIL", msg)


def _dinheiro(estado: dict) -> str:
    for row in estado.get("linhas") or []:
        if row.get("forma") == "Dinheiro":
            return str(row.get("esperado"))
    return ""


def main() -> int:
    user, _ = User.objects.get_or_create(
        username="verify_cofre_sem_auto",
        defaults={"is_staff": True, "is_active": True},
    )
    saldo_sal = saldo_cofrinho_vila(cofre=COFRE_SALARIO)
    saldo_ve = saldo_cofrinho_vila(cofre=COFRE_VILA_ELIAS)
    n_fech = RepasseVilaReservaMovimentoAgro.objects.filter(origem="fechamento_caixa").count()
    abertas_antes = set(
        SessaoCaixa.objects.filter(fechado_em__isnull=True).values_list("pk", flat=True)
    )
    sessao = SessaoCaixa.objects.create(
        ponto_caixa="vila",
        valor_abertura=Decimal("500.00"),
        usuario=user,
        observacao_abertura=PREFIX,
    )
    try:
        pin_ok, pin_err = validar_pin_operador("9973")
        check(pin_ok, f"PIN aceito{'' if pin_ok else ' · ' + pin_err}")

        client = Client(HTTP_HOST="127.0.0.1")
        client.force_login(user)
        sess = client.session
        sess["pdv_deposito"] = "vila"
        sess.save()
        client.cookies["agro_pdv_deposito"] = "vila"

        abertas = list(
            SessaoCaixa.objects.filter(fechado_em__isnull=True)
            .select_related("usuario")
            .prefetch_related("vendas", "movimentos")
            .order_by("aberto_em")
        )
        alvo = filtrar_sessoes_por_deposito(abertas, "vila")
        bruto = serializar_estado_conferencia_fechar(alvo, deposito="vila")
        resp = client.get("/api/caixa/conferencia-estado/", {"escopo": "loja"})
        if "json" not in (resp.get("Content-Type") or ""):
            trecho = resp.content.decode("utf-8", errors="replace")[:400].replace("\n", " ")
            check(False, f"API não devolveu JSON · {resp.status_code} · {trecho}")
            return 1
        data = resp.json()
        check(resp.status_code == 200 and data.get("ok"), "API do Fechar caixa responde")
        check(
            str(data.get("tot_esperado_dinheiro")) == str(bruto.get("tot_esperado_dinheiro")),
            "API não baixa o dinheiro esperado",
        )
        check(_dinheiro(data) == _dinheiro(bruto), "linha Dinheiro igual à gaveta")
        aviso = data.get("aviso_reserva_vila") or {}
        check(not aviso.get("tem") and not str(aviso.get("texto") or "").strip(), "API sem faixa Separe")

        pagina = client.get("/caixa/fechar/")
        body = pagina.content.decode("utf-8", errors="replace")
        check(pagina.status_code == 200, "tela Fechar caixa abre")
        check("Separe R$" not in body, "tela sem texto Separe R$")
        faixa = re.search(r'id="cf-aviso-reserva-vila" class="([^"]*)"', body)
        check(bool(faixa and "hidden" in faixa.group(1)), "faixa do cofrinho escondida")

        sess = client.session
        sess["pdv_deposito"] = "centro"
        sess.save()
        client.cookies["agro_pdv_deposito"] = "centro"
        centro = client.get("/api/caixa/conferencia-estado/", {"escopo": "loja"}).json()
        check(not (centro.get("aviso_reserva_vila") or {}).get("tem"), "Centro sem faixa de cofrinho")

        sess = client.session
        sess["pdv_deposito"] = "vila"
        sess.pop("pdv_sessao_caixa_id", None)
        sess.save()
        client.cookies["agro_pdv_deposito"] = "vila"
        fecha = client.post(
            "/caixa/fechar/",
            {
                "acao": "fechar_um",
                "sessao_id": str(sessao.pk),
                "pin": "9973",
                "observacao_fechamento": PREFIX,
            },
        )
        sessao.refresh_from_db()
        check(fecha.status_code in (302, 200) and sessao.fechado_em is not None, "caixa de prova fechou com PIN")
        check(
            RepasseVilaReservaMovimentoAgro.objects.filter(origem="fechamento_caixa").count() == n_fech,
            "fechar não criou separação automática",
        )
        check(
            not RepasseVilaReservaMovimentoAgro.objects.filter(sessao_caixa=sessao).exists(),
            "caixa de prova sem movimento de cofre",
        )
        check(saldo_cofrinho_vila(cofre=COFRE_SALARIO) == saldo_sal, "saldo Salário igual")
        check(saldo_cofrinho_vila(cofre=COFRE_VILA_ELIAS) == saldo_ve, "saldo Vila Elias igual")
        ainda = set(
            SessaoCaixa.objects.filter(fechado_em__isnull=True).values_list("pk", flat=True)
        )
        check(abertas_antes <= ainda, "caixas que já estavam abertos continuam abertos")
    finally:
        MovimentoCaixa.objects.filter(sessao_caixa_id=sessao.pk).delete()
        SessaoCaixa.objects.filter(pk=sessao.pk).delete()
        User.objects.filter(username="verify_cofre_sem_auto").delete()

    print("---")
    print(f"oks={oks} fails={len(fails)}")
    for item in fails:
        print("FAIL", item)
    if fails:
        return 1
    print("VERIFY_COFRE_SEM_AUTO_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
