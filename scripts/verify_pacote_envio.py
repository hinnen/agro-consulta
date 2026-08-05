#!/usr/bin/env python
"""Smoke geral do lote pronto para envio (CHECKLIST UNICO).

Nao repete o verify de cada pacote: aqui so garante que as telas que a loja usa
todo dia continuam de pe (200) com o codigo do `teste`. VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import django  # noqa: E402

django.setup()

from django.contrib.auth import get_user_model  # noqa: E402
from django.test import Client  # noqa: E402
from django.urls import NoReverseMatch, reverse  # noqa: E402

# (nome da rota, rotulo) — telas tocadas pelo lote + telas criticas de loja
ROTAS = [
    ("home", "BI / (BI-TOPBAR-TOTAL)"),
    ("entrada_nota", "Entrada NF (SEFAZ-UI · DFE-CIENCIA · CP-DUP-BACKUP)"),
    ("grafico_gastos", "Grafico gastos (GG-GASTOS)"),
    ("planos_conta_config", "Planos de contas (PLANOS-CONTA)"),
    ("lancamentos_contas_pagar", "Contas a pagar (CP-DUP-BACKUP)"),
    ("lancamentos_financeiros", "Lancamentos"),
    ("produtos_cadastro_erp", "Cadastro ERP (COMP-UX)"),
    ("consulta_produtos", "PDV consulta"),
    ("pdv_checkout", "PDV checkout"),
    ("caixa_painel", "Caixa painel"),
    ("vendas_lista", "Vendas"),
    ("clientes_lista", "Clientes"),
    ("rh_painel", "RH"),
    ("home_atalhos", "Atalhos"),
]

falhas: list[str] = []
okc = 0


def main() -> None:
    global okc
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    if user is None:
        print("VERIFY_FAIL: sem usuario no banco local")
        sys.exit(1)
    c = Client(headers={"host": "127.0.0.1"})
    c.force_login(user)

    for nome, rotulo in ROTAS:
        try:
            url = reverse(nome)
        except NoReverseMatch:
            falhas.append(f"rota {nome} nao existe ({rotulo})")
            continue
        try:
            r = c.get(url)
        except Exception as exc:  # noqa: BLE001
            falhas.append(f"{rotulo} {url} -> EXC {type(exc).__name__}: {exc}")
            continue
        if r.status_code in (200, 302):
            okc += 1
            print(f"  OK {r.status_code} {url}  {rotulo}")
        else:
            falhas.append(f"{rotulo} {url} -> {r.status_code}")

    # healthz sem login
    anon = Client(headers={"host": "127.0.0.1"})
    r = anon.get("/healthz")
    if r.status_code != 200:
        falhas.append(f"/healthz -> {r.status_code}")
    else:
        okc += 1
        print("  OK 200 /healthz")

    if falhas:
        for f in falhas:
            print(f"  FAIL {f}")
        print(f"VERIFY_FAIL {len(falhas)} tela(s)")
        sys.exit(1)
    print(f"VERIFY_OK {okc} telas de pe")


if __name__ == "__main__":
    main()
