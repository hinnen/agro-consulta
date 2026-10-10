#!/usr/bin/env python
"""Prova RH operadores PIN — vínculo / 1234 / desativar."""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth.models import User

from base.models import PerfilUsuario
from base.operador_util import (
    PIN_BOOTSTRAP,
    criar_operador,
    desativar_operador,
    listar_operadores,
    reativar_operador,
    resetar_pin_bootstrap,
)
from produtos.caixa_util import cadastrar_pin_operador_primeira_vez, validar_pin_operador

ok_n = 0
fail_n = 0


def check(cond: bool, msg: str) -> None:
    global ok_n, fail_n
    if cond:
        ok_n += 1
        print(f"  OK  {msg}")
    else:
        fail_n += 1
        print(f"  FAIL {msg}")


def main() -> int:
    print("--- RH operadores PIN ---")
    nome = "Teste Pin Rh Auto"
    # limpa restos
    for u in User.objects.filter(first_name="Teste", last_name="Pin Rh Auto"):
        PerfilUsuario.objects.filter(user=u).delete()
        u.delete()

    ok, data, err = criar_operador(nome=nome)
    check(ok and data is not None, f"criar operador ({err or 'ok'})")
    pid = int(data["id"]) if data else 0
    check(data and data.get("pin_personalizado") is False, "PIN inicial não personalizado")
    check(
        PerfilUsuario.objects.filter(pk=pid, senha_rapida=PIN_BOOTSTRAP, ativo=True).exists(),
        "senha_rapida=1234 no PG",
    )

    check(validar_pin_operador(PIN_BOOTSTRAP)[0] is False, "1234 bloqueado na validação")
    ok_cad, rot, err_cad = cadastrar_pin_operador_primeira_vez(pid, "5821", bootstrap="1234")
    check(ok_cad, f"bootstrap troca PIN ({err_cad or rot})")
    check(validar_pin_operador("5821")[0] is True, "PIN novo valida")

    ok_rst, err_rst = resetar_pin_bootstrap(pid)
    check(ok_rst, f"reset 1234 ({err_rst})")
    check(
        PerfilUsuario.objects.filter(pk=pid, senha_rapida=PIN_BOOTSTRAP).exists(),
        "após reset voltou 1234",
    )

    ok_off, err_off = desativar_operador(pid)
    check(ok_off, f"desativar ({err_off})")
    check(not any(x["id"] == pid for x in listar_operadores(incluir_inativos=False)), "sumiu da lista ativa")
    check(validar_pin_operador("5821")[0] is False, "PIN antigo não valida inativo")

    ok_on, err_on = reativar_operador(pid)
    check(ok_on, f"reativar ({err_on})")
    check(
        PerfilUsuario.objects.filter(pk=pid, ativo=True, senha_rapida=PIN_BOOTSTRAP).exists(),
        "reativar volta 1234",
    )

    # limpa
    p = PerfilUsuario.objects.filter(pk=pid).select_related("user").first()
    if p:
        uid = p.user_id
        p.delete()
        User.objects.filter(pk=uid).delete()

    print(f"\nVERIFY {'OK' if fail_n == 0 else 'FAIL'} {ok_n}/{ok_n + fail_n}")
    return 0 if fail_n == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
