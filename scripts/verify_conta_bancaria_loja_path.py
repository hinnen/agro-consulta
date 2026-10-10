#!/usr/bin/env python
"""Prova detalhada — CP-CONTAS-LOJA-PG (contas/banco Postgres da loja).

Cobertura:
  1) contratos UI/template + rotas + migrate + backup
  2) util: criar / renomear / desativar / seed / placeholder / erros
  3) rename propaga nome em TituloFinanceiroAgro (mesmo banco_id)
  4) lista da baixa = loja PG (fonte_bancos)
  5) HTTP Client: login · CRUD APIs · opcoes-baixa · página CP
  6) PIN 9973 (operador fresco) — gestão de conta não exige PIN; baixa fake passa gate
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import Client, override_settings
from django.urls import NoReverseMatch, reverse

from produtos.conta_bancaria_loja_util import (
    criar_conta_loja,
    ensure_seed_contas_loja,
    listar_bancos_para_baixa,
    listar_contas_loja,
    renomear_conta_loja,
    set_ativo_conta_loja,
)
from produtos.models import ContaBancariaLojaAgro, TituloFinanceiroAgro

PIN_TESTE = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
FAILS = 0
N = 0


def ok(cond: bool, msg: str) -> None:
    global FAILS, N
    N += 1
    if cond:
        print(f"  OK  {msg}")
    else:
        FAILS += 1
        print(f" FAIL {msg}")


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def section(title: str) -> None:
    print(f"\n== {title} ==")


def main() -> int:
    tag = f"__TESTE_CONTA_LOJA_{os.getpid()}__"
    ContaBancariaLojaAgro.objects.filter(nome__startswith="__TESTE_CONTA_LOJA_").delete()
    TituloFinanceiroAgro.objects.filter(mongo_id__startswith="tcl-").delete()

    # ----- 1) contratos estáticos -----
    section("contratos estaticos")
    tpl = read("produtos/templates/produtos/lancamentos_contas_pagar_teste.html")
    ok("id=\"sv-bx-banco-gestao\"" in tpl, "UI: botao lapis sv-bx-banco-gestao")
    ok("id=\"sv-modal-contas-loja\"" in tpl, "UI: modal gestao contas")
    ok("Desativar" in tpl and "Ativar" in tpl, "UI: ativar/desativar (sem excluir)")
    ok("sv-c-excluir" not in tpl and "API_CONTAS_EXCLUIR" not in tpl, "UI: sem botao excluir conta")
    ok("API_CONTAS_LOJA" in tpl and "API_CONTAS_CRIAR" in tpl, "UI: URLs gestão")
    ok("api_contas_bancarias_loja_lista" in tpl, "UI: reverse lista")
    ok("abrirGestaoContasLoja" in tpl and "carregarGestaoContasLoja" in tpl, "UI: JS gestão")
    ok("fonte_bancos" in read("produtos/views.py"), "views: fonte_bancos na API baixa")
    ok("listar_bancos_para_baixa" in read("produtos/views.py"), "views: baixa usa lista loja")
    ok("ContaBancariaLojaAgro" in read("produtos/pg_backup_registry.py"), "backup financeiro inclui modelo")
    mig = read("produtos/migrations/0140_contabancarialojaagro.py")
    ok("ContaBancariaLojaAgro" in mig and "0139_fiadotituloagro_deposito" in mig, "migrate 0140 ok")
    for name in (
        "api_contas_bancarias_loja_lista",
        "api_contas_bancarias_loja_criar",
        "api_contas_bancarias_loja_renomear",
        "api_contas_bancarias_loja_toggle",
        "api_lancamentos_opcoes_baixa",
    ):
        try:
            if "renomear" in name or "toggle" in name:
                u = reverse(name, kwargs={"pk": 1})
            else:
                u = reverse(name)
            ok(bool(u), f"reverse {name} = {u}")
        except NoReverseMatch:
            ok(False, f"reverse {name}")

    # ----- 2) util -----
    section("util CRUD / regras")
    seed = ensure_seed_contas_loja()
    ok(isinstance(seed, dict), f"seed retorna dict {seed}")
    obj = criar_conta_loja(tag)
    ok(obj.pk > 0 and obj.ativo and obj.codigo.startswith("loja-"), f"criar ok codigo={obj.codigo}")
    try:
        criar_conta_loja(tag)
        ok(False, "duplicata deve falhar")
    except ValueError:
        ok(True, "duplicata nome → ValueError")
    try:
        criar_conta_loja("ADICIONAR CONTA")
        ok(False, "placeholder reservado deve falhar")
    except ValueError:
        ok(True, "nome reservado ADICIONAR CONTA → ValueError")
    try:
        criar_conta_loja("x")
        ok(False, "nome curto deve falhar")
    except ValueError:
        ok(True, "nome < 2 → ValueError")

    bancos = listar_bancos_para_baixa()
    ok(bancos and str(bancos[0].get("nome") or "").upper().find("ADICIONAR") >= 0, "placeholder primeiro na baixa")
    ok(obj.codigo in {str(b.get("id")) for b in bancos}, "conta ativa na baixa")
    ok(tag in {str(b.get("nome")) for b in bancos}, "nome na baixa")

    # título fake com banco_id = codigo → rename propaga
    tid = f"tcl-{os.getpid()}"[:32]
    TituloFinanceiroAgro.objects.create(
        mongo_id=tid,
        despesa=True,
        descricao="path contas loja",
        banco=tag[:120],
        banco_id=obj.codigo[:32],
        valor_bruto=1,
        valor_pago=0,
        valor_restante=1,
        quitado=False,
    )
    novo = tag + "_REN"
    obj2 = renomear_conta_loja(obj.pk, novo)
    ok(obj2.nome == novo, "renomear util")
    t = TituloFinanceiroAgro.objects.get(mongo_id=tid)
    ok(t.banco == novo and t.banco_id == obj.codigo, "rename propaga TituloFinanceiroAgro.banco")

    set_ativo_conta_loja(obj.pk, False)
    ok(obj.codigo not in {str(b.get("id")) for b in listar_bancos_para_baixa()}, "desativada some da baixa")
    gest = listar_contas_loja(incluir_inativos=True)
    hit = next((x for x in gest if x["pk"] == obj.pk), None)
    ok(hit is not None and hit["ativo"] is False, "inativa na gestão")
    set_ativo_conta_loja(obj.pk, True)
    ok(obj.codigo in {str(b.get("id")) for b in listar_bancos_para_baixa()}, "reativar volta")

    # loja inteira (não por usuário): 2ª “sessão” vê mesma conta
    ok(
        ContaBancariaLojaAgro.objects.filter(pk=obj.pk).exists(),
        "conta na tabela loja (compartilhada)",
    )

    # ----- 3) HTTP -----
    section("HTTP Client + PIN")
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    ok(user is not None, "usuário Django")
    if not user:
        print(f"\nTOTAL={N} FAILS={FAILS}")
        return 1

    hosts = list(getattr(settings, "ALLOWED_HOSTS", []) or [])
    if "testserver" not in hosts:
        hosts = hosts + ["testserver", "localhost", "127.0.0.1"]

    with override_settings(ALLOWED_HOSTS=hosts):
        c = Client()
        # sem login
        r0 = c.get(reverse("api_contas_bancarias_loja_lista"))
        ok(r0.status_code in (302, 401, 403), f"lista sem login → {r0.status_code}")

        c.force_login(user)
        r_list = c.get(reverse("api_contas_bancarias_loja_lista") + "?inativos=1")
        j_list = r_list.json() if r_list.status_code == 200 else {}
        ok(r_list.status_code == 200 and j_list.get("ok"), f"GET lista inativos ({r_list.status_code})")
        ok(any(x.get("pk") == obj.pk for x in (j_list.get("itens") or [])), "lista HTTP contém conta teste")

        tag_http = tag + "_HTTP"
        ContaBancariaLojaAgro.objects.filter(nome=tag_http).delete()
        r_cr = c.post(
            reverse("api_contas_bancarias_loja_criar"),
            data=json.dumps({"nome": tag_http}),
            content_type="application/json",
        )
        j_cr = r_cr.json() if r_cr.status_code == 200 else {}
        ok(r_cr.status_code == 200 and j_cr.get("ok"), f"POST criar ({r_cr.status_code} {j_cr.get('erro')})")
        pk_http = int((j_cr.get("item") or {}).get("pk") or 0)
        cod_http = str((j_cr.get("item") or {}).get("id") or "")
        ok(pk_http > 0 and cod_http.startswith("loja-"), f"criar HTTP pk={pk_http}")

        r_ren = c.post(
            reverse("api_contas_bancarias_loja_renomear", kwargs={"pk": pk_http}),
            data=json.dumps({"nome": tag_http + "_X"}),
            content_type="application/json",
        )
        j_ren = r_ren.json() if r_ren.status_code == 200 else {}
        ok(r_ren.status_code == 200 and (j_ren.get("item") or {}).get("nome") == tag_http + "_X", "POST renomear")

        r_off = c.post(
            reverse("api_contas_bancarias_loja_toggle", kwargs={"pk": pk_http}),
            data=json.dumps({"ativo": False}),
            content_type="application/json",
        )
        j_off = r_off.json() if r_off.status_code == 200 else {}
        ok(r_off.status_code == 200 and (j_off.get("item") or {}).get("ativo") is False, "POST desativar")

        r_on = c.post(
            reverse("api_contas_bancarias_loja_toggle", kwargs={"pk": pk_http}),
            data=json.dumps({"ativo": True}),
            content_type="application/json",
        )
        ok(r_on.status_code == 200 and (r_on.json().get("item") or {}).get("ativo") is True, "POST ativar")

        r_op = c.get(
            reverse("api_lancamentos_opcoes_baixa")
            + "?modo=erp&apenas_cadastro_erp=1&somente_dinheiro_banco=1"
        )
        j_op = r_op.json() if r_op.status_code == 200 else {}
        ok(r_op.status_code == 200, f"GET opcoes-baixa ({r_op.status_code})")
        ok(j_op.get("fonte_bancos") == "loja_pg", f"fonte_bancos=loja_pg (got {j_op.get('fonte_bancos')!r})")
        banc_ids = {str(b.get("id") or "") for b in (j_op.get("bancos") or [])}
        banc_nomes = {str(b.get("nome") or "") for b in (j_op.get("bancos") or [])}
        ok(cod_http in banc_ids, "conta HTTP na opcoes-baixa")
        ok(any("ADICIONAR CONTA" in n.upper() for n in banc_nomes), "placeholder nas opções")
        # desativada não deve aparecer
        ContaBancariaLojaAgro.objects.filter(pk=pk_http).update(ativo=False)
        r_op2 = c.get(
            reverse("api_lancamentos_opcoes_baixa")
            + "?modo=erp&apenas_cadastro_erp=1&somente_dinheiro_banco=1"
        )
        ids2 = {str(b.get("id") or "") for b in (r_op2.json().get("bancos") or [])}
        ok(cod_http not in ids2, "desativada fora de opcoes-baixa")

        # página CP
        try:
            r_page = c.get("/lancamentos/contas-pagar/")
            ok(r_page.status_code == 200, f"GET /lancamentos/contas-pagar/ → {r_page.status_code}")
            body = r_page.content.decode("utf-8", errors="replace")
            ok("sv-bx-banco-gestao" in body and "sv-modal-contas-loja" in body, "HTML CP traz lápis+modal")
            ok("/api/lancamentos/contas-loja/" in body, "HTML CP embute URL contas-loja")
        except Exception as e:
            ok(False, f"página CP: {e}")

        # PIN 9973 — operador fresco (gestão não exige; baixa sim)
        try:
            url_op = reverse("api_pdv_registrar_operador")
            r_pin = c.post(
                url_op,
                data=json.dumps({"pin": PIN_TESTE}),
                content_type="application/json",
            )
            j_pin = r_pin.json() if r_pin.status_code == 200 else {}
            ok(
                r_pin.status_code == 200 and j_pin.get("ok") and (j_pin.get("operador") or "").strip(),
                f"PIN {PIN_TESTE} → operador {j_pin.get('operador')!r}",
            )
            # baixa com id fake: passa gate PIN (não 403 MSG)
            r_bx = c.post(
                reverse("api_lancamentos_baixa"),
                data=json.dumps(
                    {
                        "ids": ["000000000000000000000000"],
                        "tipo": "pagar",
                        "data_movimento": "2026-10-07",
                        "forma_pagamento": "Dinheiro",
                        "banco": tag_http + "_X",
                        "banco_id": cod_http,
                    }
                ),
                content_type="application/json",
            )
            j_bx = {}
            try:
                j_bx = r_bx.json()
            except Exception:
                pass
            erro = str(j_bx.get("erro") or "")
            from produtos.caixa_util import MSG_PIN_OPERADOR_OBRIGATORIO

            ok(
                MSG_PIN_OPERADOR_OBRIGATORIO not in erro,
                f"baixa com PIN fresco não trava no gate PIN (status={r_bx.status_code})",
            )
        except Exception as e:
            ok(False, f"PIN/baixa gate: {e}")

        ContaBancariaLojaAgro.objects.filter(pk=pk_http).delete()

    # cleanup
    ContaBancariaLojaAgro.objects.filter(nome__startswith="__TESTE_CONTA_LOJA_").delete()
    TituloFinanceiroAgro.objects.filter(mongo_id__startswith="tcl-").delete()

    print(f"\nTOTAL={N} FAILS={FAILS}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
