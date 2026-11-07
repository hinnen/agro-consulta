# -*- coding: utf-8 -*-
"""Prova path PIN-SAIDA-OPERADOR — Nathan digita PIN ≠ grava Renan (Chrome)."""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import RequestFactory

from base.models import PerfilUsuario
from produtos.caixa_util import (
    MSG_PIN_AMBIGUO,
    operador_label_de_pin,
    registrar_retirada_turno_caixa,
    rotulo_operador_pin,
    usuario_django_de_pin,
    usuario_movimento_de_pin_ou_sessao,
    validar_pin_operador,
)

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


class Sess(dict):
    modified = False


def check_static() -> None:
    caixa = (ROOT / "produtos/caixa_util.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    emb = (
        ROOT / "produtos/templates/produtos/includes/caixa_saida_embed.html"
    ).read_text(encoding="utf-8")

    check("MSG_PIN_AMBIGUO" in caixa, "constante PIN ambíguo")
    check("def usuario_movimento_de_pin_ou_sessao" in caixa, "helper user movimento")
    check("_pin_tem_conflito" in caixa, "detecção conflito PIN")
    check("nome_exibicao_perfil" in caixa, "rótulo usa RH nome_exibicao")
    check("len(matches) != 1" in caixa, "PIN ambíguo não escolhe .first()")

    m = re.search(
        r"def registrar_retirada_turno_caixa\(.*?(?=\ndef )",
        caixa,
        re.S,
    )
    check(bool(m), "bloco registrar_retirada")
    if m:
        body = m.group(0)
        check("usuario_movimento_de_pin_ou_sessao" in body, "retirada usa PIN/sessão")
        check("request.user if getattr" not in body, "retirada sem Chrome fallback")

    src_api = ""
    m2 = re.search(
        r"def api_lancamentos_saida_caixa\(request\):.*?(?=\n@login_required|\ndef api_)",
        views,
        re.S,
    )
    check(bool(m2), "bloco api saida caixa")
    if m2:
        src_api = m2.group(0)
        check("gravar_operador_sessao_pdv(request, pin)" in src_api, "saída grava sessão PIN")
        check("user_pin = usuario_django_de_pin(pin)" in src_api, "saída resolve user do PIN")
        check("usuario=user_pin" in src_api, "vale/salário usam user_pin")
        check("usuario=request.user" not in src_api, "saída sem request.user no quem")
        # depósito também exige PIN (antes era sem PIN → Chrome)
        i_pin = src_api.find("operador_label_de_pin(pin)")
        i_dep = src_api.find("if is_deposito:")
        check(i_pin >= 0 and i_dep >= 0 and i_pin < i_dep, "PIN antes do depósito")

    check("pin: pin" in emb or "pin:pin" in emb.replace(" ", ""), "embed manda pin")
    check("agroCaixaPedirPin" in emb, "embed pede PIN na UI")

    est = (ROOT / "estoque/views.py").read_text(encoding="utf-8")
    hub = (ROOT / "produtos/vendas_lojas_hub_acesso_util.py").read_text(encoding="utf-8")
    check("filter(senha_rapida=pin, ativo=True).first()" not in est, "estoque sem .first() no PIN")
    check("def _erro_pin_operador" in est, "estoque recusa PIN repetido")
    check("_perfil_usuario_por_pin" in hub, "hub usa o mesmo resolvedor")
    check(".first()" not in hub.split("def _perfil_por_pin")[1].split("def ")[0], "hub sem .first()")


def check_runtime() -> None:
    User = get_user_model()
    # limpa restos de runs anteriores (SQL — evita cascade em tabelas RH ausentes)
    from django.db import connection

    with connection.cursor() as c:
        c.execute(
            "DELETE FROM base_perfilusuario WHERE user_id IN "
            "(SELECT id FROM auth_user WHERE username LIKE 'pin_saida_%')"
        )
        c.execute("DELETE FROM auth_user WHERE username LIKE 'pin_saida_%'")

    u_renan = User.objects.create_user(
        username="pin_saida_renan",
        password="x",
        first_name="Renan",
        last_name="Hinnen",
    )
    u_nathan = User.objects.create_user(
        username="pin_saida_nathan",
        password="x",
        first_name="Nathan",
        last_name="Hinnen",
    )
    p_renan = PerfilUsuario.objects.create(
        user=u_renan,
        codigo_vendedor="9901",
        senha_rapida="8811",
        ativo=True,
        primeiro_acesso=False,
    )
    p_nathan = PerfilUsuario.objects.create(
        user=u_nathan,
        codigo_vendedor="9902",
        senha_rapida="8822",
        ativo=True,
        primeiro_acesso=False,
    )

    ok_n, lab_n, err_n = operador_label_de_pin("8822")
    check(ok_n and "Nathan" in lab_n and err_n == "", "PIN Nathan → Nathan")
    ok_r, lab_r, err_r = operador_label_de_pin("8811")
    check(ok_r and "Renan" in lab_r and err_r == "", "PIN Renan → Renan")
    check(usuario_django_de_pin("8822") == u_nathan, "user Django Nathan")

    # Chrome logado como Renan + PIN Nathan → movimento = Nathan
    rf = RequestFactory()
    req = rf.post("/")
    req.session = Sess()
    req.user = u_renan

    u_mov = usuario_movimento_de_pin_ou_sessao(req, "8822")
    check(u_mov == u_nathan, "movimento PIN Nathan (Chrome=Renan)")

    # Sem PIN e sem sessão → None (não Chrome)
    check(usuario_movimento_de_pin_ou_sessao(req, "") is None, "sem PIN não usa Chrome")

    # PIN ambíguo
    p_nathan.senha_rapida = "8811"
    p_nathan.save(update_fields=["senha_rapida"])
    ok_a, err_a = validar_pin_operador("8811")
    check(not ok_a and MSG_PIN_AMBIGUO[:20] in err_a, "PIN duplicado recusado")
    ok_b, _lab_b, err_b = operador_label_de_pin("8811")
    check(not ok_b and MSG_PIN_AMBIGUO[:20] in err_b, "label PIN duplicado recusado")
    check(rotulo_operador_pin("8811") == "", "rótulo vazio se ambíguo")
    check(usuario_django_de_pin("8811") is None, "user None se ambíguo")

    # cleanup via SQL (sqlite local pode faltar FKs RH)
    from django.db import connection

    with connection.cursor() as c:
        c.execute(
            "DELETE FROM base_perfilusuario WHERE id IN (%s, %s)",
            [p_renan.pk, p_nathan.pk],
        )
        c.execute(
            "DELETE FROM auth_user WHERE id IN (%s, %s)",
            [u_renan.pk, u_nathan.pk],
        )
    ok("cleanup SQL")


def main() -> int:
    print("verify_pin_operador_saida_nathan_path...")
    check_static()
    check_runtime()
    print(f"total OK={OKS} FAIL={len(FAILS)}")
    if FAILS:
        print("VERIFY_FAIL")
        for f in FAILS:
            print(" -", f)
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
