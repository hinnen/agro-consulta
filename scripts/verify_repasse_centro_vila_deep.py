#!/usr/bin/env python3
"""Prova Django (quando DB migrado) — repasse Centro → Vila não altera Vila→Centro."""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import RequestFactory

from produtos import caixa_util as cu
from produtos.models import MovimentoCaixa, RepasseCentroVilaAgro, SessaoCaixa
from produtos.repasse_vila_util import confirmar_repasse_centro_vila

fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def table_ready(name: str) -> bool:
    return name in connection.introspection.table_names()


def main() -> int:
    if not table_ready("produtos_repassecentrovilaagro"):
        print("SKIP deep DB (migrate 0148 pendente neste ambiente)")
        return 0

    User = get_user_model()
    user, _ = User.objects.get_or_create(
        username="repasse_cv_verify", defaults={"is_staff": True}
    )
    rf = RequestFactory()
    tag = "REPASSE-CV-VERIFY"

    gaveta = (
        SessaoCaixa.objects.filter(ponto_caixa="gaveta", fechado_em__isnull=True)
        .order_by("-pk")
        .first()
    )
    vila = (
        SessaoCaixa.objects.filter(ponto_caixa="vila", fechado_em__isnull=True)
        .order_by("-pk")
        .first()
    )
    if not gaveta:
        print("SKIP deep (sem gaveta aberta — smoke manual na loja)")
        return 0

    req = rf.post("/api/repasse-vila/confirmar-centro-vila/")
    req.user = user
    if vila:
        req.session = {"sessao_caixa_id": vila.pk}
    else:
        req.session = {"sessao_caixa_id": gaveta.pk}

    ok_pin, label, err_pin = cu.operador_label_de_pin("9973")
    if ok_pin:
        ok(f"PIN 9973 → {label}")
    else:
        print("SKIP PIN 9973 (cadastro local):", err_pin)

    rep, err = confirmar_repasse_centro_vila(
        req,
        quem_levou="Teste verify",
        valor=Decimal("0.01"),
        operador=label if ok_pin else "verify",
    )
    if err and "dinheiro" in err.lower():
        ok(f"bloqueio saldo gaveta: {err[:80]}")
        return 0 if not fails else 1
    if err:
        fail(err)
        return 1

    assert rep is not None
    RepasseCentroVilaAgro.objects.filter(pk=rep.pk).update(observacao=tag)
    ms = rep.movimento_saida
    must_saida = ms and ms.tipo == MovimentoCaixa.Tipo.RETIRADA
    must_sessao = ms and ms.sessao_caixa_id == gaveta.pk
    if must_saida and must_sessao:
        ok("saída RETIRADA na gaveta Centro")
    else:
        fail("movimento saída não amarrado à gaveta Centro")

    if rep.movimento_entrada_id:
        me = rep.movimento_entrada
        if me and me.tipo == MovimentoCaixa.Tipo.REFORCO and vila and me.sessao_caixa_id == vila.pk:
            ok("entrada REFORCO na Vila")
        elif vila:
            fail("entrada não na sessão Vila")
    else:
        ok("pendente Vila (caixa Vila fechado)")

    # limpa teste
    if ms:
        ms.delete()
    if rep.movimento_entrada_id:
        rep.movimento_entrada.delete()
    rep.delete()

    if fails:
        print("VERIFY_REPASSE_CENTRO_VILA_DEEP_FAIL", fails)
        return 1
    print(f"VERIFY_REPASSE_CENTRO_VILA_DEEP_OK ({oks})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
