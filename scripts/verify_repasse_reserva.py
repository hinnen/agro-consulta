#!/usr/bin/env python
"""Prova detalhada — REPASSE-RESERVA (troco que fica na Vila + layout)."""
from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from django.contrib.auth import get_user_model
from django.contrib.sessions.middleware import SessionMiddleware
from django.test import Client, RequestFactory
from django.utils import timezone

from produtos import caixa_util as cu
from produtos.models import (
    MovimentoCaixa,
    RepasseVilaCentroAgro,
    RepasseVilaConfigAgro,
    SessaoCaixa,
)
from produtos.repasse_vila_util import (
    ZERO,
    _dec,
    calcular_disponivel,
    confirmar_repasse,
    listar_acumulado_detalhe,
    obter_config,
    reserva_vila_config,
    salvar_reserva_vila,
)

User = get_user_model()
fails: list[str] = []
oks = 0
TAG = "verify-reserva-bot"


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def check_file(rel: str, *needles: str) -> None:
    p = ROOT / rel
    if not p.exists():
        fail(f"MISSING {rel}")
        return
    text = p.read_text(encoding="utf-8", errors="replace")
    for n in needles:
        if n not in text:
            fail(f"{rel} missing {n!r}")
        else:
            ok(f"path {rel} · {n}")


def forbid(rel: str, *needles: str) -> None:
    p = ROOT / rel
    if not p.exists():
        fail(f"MISSING {rel}")
        return
    text = p.read_text(encoding="utf-8", errors="replace")
    for n in needles:
        if n in text:
            fail(f"{rel} still has {n!r}")
        else:
            ok(f"forbid {rel} · {n}")


def _cleanup_tag() -> None:
    for rep in RepasseVilaCentroAgro.objects.filter(observacao=TAG):
        if rep.movimento_saida_id:
            MovimentoCaixa.objects.filter(pk=rep.movimento_saida_id).delete()
        if rep.movimento_entrada_id:
            MovimentoCaixa.objects.filter(pk=rep.movimento_entrada_id).delete()
        rep.delete()


def main() -> int:
    hoje = timezone.localdate()

    # --- path / contrato ---
    check_file(
        "produtos/models.py",
        "reserva_vila",
        "Valor fixo que fica na Vila",
    )
    check_file(
        "produtos/migrations/0095_repasse_vila_reserva.py",
        "reserva_vila",
        "0094_repasse_vila_delta_cache",
        "repassevilaconfigagro",
    )
    check_file(
        "produtos/repasse_vila_util.py",
        "salvar_reserva_vila",
        "reserva_vila_config",
        "total_sugerido_bruto",
        'reserva = reserva_vila_config(cfg)',
        "o valor que fica na Vila cobre o envio",
        "if valor_manual is None:",
    )
    check_file(
        "produtos/views_repasse_vila.py",
        "salvar_reserva_vila",
        "reserva_padrao",
        '"reserva_vila"',
    )
    check_file(
        "produtos/templates/produtos/repasse_vila.html",
        "rv-reserva",
        "rv-salvar-reserva",
        "rv-fold",
        "Dias 1 a 15",
        "Dias 16 a 31",
        "Fica na Vila",
        "tot - reserva",
        "class=\"rv-fold\"",
        "reservaAtual",
        "fetch('/api/repasse-vila/config/'",
    )
    check_file(
        "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html",
        "pdv-rp-reserva",
        "pdv-rp-salvar-reserva",
        "Fica na Vila",
        "<details class=\"rp-card\">",
        "% lucro e opções",
    )
    check_file(
        "produtos/static/produtos/js/pdv_repasse_vila.js",
        "reservaAtual",
        "pdv-rp-reserva",
        "reserva_vila: reservaAtual()",
        "tot - reservaAtual()",
    )
    check_file(
        "produtos/templates/produtos/includes/repasse_help_agents.html",
        "Fica na Vila",
        "desconta",
        "todos os PCs",
    )
    forbid("produtos/templates/produtos/repasse_vila.html", "localStorage")
    forbid("produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html", "localStorage")
    forbid("produtos/static/produtos/js/pdv_repasse_vila.js", "localStorage")

    tela = (ROOT / "produtos/templates/produtos/repasse_vila.html").read_text(
        encoding="utf-8", errors="replace"
    )
    if "<details class=\"rv-fold\" open" in tela or "<details class='rv-fold' open" in tela:
        fail("rv-fold não deve abrir sozinho")
    else:
        ok("rv-fold recolhido por padrão")
    if "id=\"rv-reserva\"" in tela and tela.find("id=\"rv-reserva\"") < tela.find("class=\"rv-fold\""):
        ok("campo reserva fica fora do bloco recolhido")
    else:
        fail("reserva deveria ficar visível (fora do fold)")

    field = RepasseVilaConfigAgro._meta.get_field("reserva_vila")
    ok("campo PG Decimal") if field.get_internal_type() == "DecimalField" else fail(
        f"tipo {field.get_internal_type()}"
    )

    # --- runtime config ---
    cfg = obter_config()
    res_antes = _dec(cfg.reserva_vila)
    salvar_reserva_vila(-3, operador="reserva-verify")
    ok("clamp neg→0") if reserva_vila_config() == ZERO else fail("clamp neg")
    salvar_reserva_vila(Decimal("100000"), operador="reserva-verify")
    ok("clamp teto") if reserva_vila_config() == Decimal("99999.99") else fail("clamp teto")
    salvar_reserva_vila(Decimal("123.45"), operador="reserva-verify")
    recarregado = _dec(RepasseVilaConfigAgro.objects.order_by("pk").first().reserva_vila)
    ok("PG persiste 123.45") if recarregado == Decimal("123.45") else fail(
        f"PG={recarregado}"
    )

    salvar_reserva_vila(Decimal("200.00"), operador="reserva-verify")
    calc = calcular_disponivel(hoje)
    bruto = _dec(calc.get("total_sugerido_bruto"))
    sug = _dec(calc.get("total_sugerido"))
    falta = _dec(calc.get("falta_dinheiro"))
    acum = _dec(calc.get("acumulado_anterior"))
    reserva = _dec(calc.get("reserva_vila"))
    esp = max(ZERO, (bruto - Decimal("200.00")).quantize(Decimal("0.01")))
    if reserva == Decimal("200.00"):
        ok("calc.reserva_vila=200")
    else:
        fail(f"calc reserva={reserva}")
    if sug == esp:
        ok("sugerido = bruto − 200")
    else:
        fail(f"sug={sug} esp={esp} bruto={bruto}")
    esp_bruto = (falta + acum).quantize(Decimal("0.01"))
    if bruto == esp_bruto:
        ok("bruto = falta + acum líquido")
    else:
        fail(f"bruto {bruto} != falta+acum {esp_bruto}")

    det = listar_acumulado_detalhe(hoje)
    if abs(_dec(det.get("total_sugerido")) - sug) < Decimal("0.02"):
        ok("detalhe total_sugerido = calc")
    else:
        fail(f"detalhe sug {det.get('total_sugerido')} != {sug}")
    if abs(_dec(det.get("reserva_vila")) - Decimal("200.00")) < Decimal("0.02"):
        ok("detalhe expõe reserva")
    else:
        fail("detalhe sem reserva 200")

    salvar_reserva_vila(ZERO, operador="reserva-verify")
    calc0 = calcular_disponivel(hoje)
    if _dec(calc0.get("total_sugerido")) == _dec(calc0.get("total_sugerido_bruto")):
        ok("reserva 0 não altera sugerido")
    else:
        fail("reserva 0 ainda desconta")

    # --- HTTP API ---
    user, _ = User.objects.get_or_create(
        username="repasse_verify_bot", defaults={"is_staff": True}
    )
    client = Client(HTTP_HOST="127.0.0.1")
    client.force_login(user)
    r = client.get("/api/repasse-vila/config/")
    if r.status_code == 200 and "reserva_vila" in r.json():
        ok("GET config reserva_vila")
    else:
        fail(f"GET config {r.status_code}")

    r = client.post(
        "/api/repasse-vila/config/",
        data=json.dumps({"reserva_vila": "80,50"}),
        content_type="application/json",
    )
    j = r.json() if r.status_code == 200 else {}
    if r.status_code == 200 and j.get("ok") and abs(float(j.get("reserva_vila") or 0) - 80.5) < 0.02:
        ok("POST config vírgula 80,50")
    else:
        fail(f"POST config {r.status_code} {j}")
    if reserva_vila_config() == Decimal("80.50"):
        ok("POST gravou no PG")
    else:
        fail(f"PG após POST {reserva_vila_config()}")

    r = client.post(
        "/api/repasse-vila/config/",
        data=json.dumps({"reserva_vila": 0}),
        content_type="application/json",
    )
    if r.status_code == 200 and reserva_vila_config() == ZERO:
        ok("POST zera reserva")
    else:
        fail("não zerou reserva")

    r = client.get("/api/repasse-vila/calc/")
    if r.status_code == 200:
        cj = r.json()
        if "reserva_vila" in cj and "total_sugerido_bruto" in cj:
            ok("GET calc reserva+bruto")
        else:
            fail("calc sem campos reserva")
    else:
        fail(f"GET calc {r.status_code}")

    r = client.get("/repasse-vila/")
    body = r.content if r.status_code == 200 else b""
    ok("GET tela 200") if r.status_code == 200 else fail(f"tela {r.status_code}")
    for needle in (b"rv-reserva", b"rv-fold", b"Dias 1 a 15", b"Dias 16 a 31", b"Fica na Vila"):
        if needle in body:
            ok(f"tela tem {needle.decode()}")
        else:
            fail(f"tela sem {needle.decode()}")

    r = client.get("/caixa/retiradas/?repasse=1")
    ob = r.content if r.status_code == 200 else b""
    if b"pdv-rp-reserva" in ob and b"pdv-rp-salvar-reserva" in ob:
        ok("overlay na Retiradas tem reserva")
    else:
        fail("overlay sem campo reserva")

    # --- confirmar desconta reserva (e valor manual não) ---
    _cleanup_tag()
    s_vila = SessaoCaixa.objects.filter(ponto_caixa="vila", fechado_em__isnull=True).first()
    created_vila = False
    if not s_vila:
        s_vila = SessaoCaixa.objects.create(
            ponto_caixa="vila", valor_abertura=Decimal("100"), usuario=user
        )
        created_vila = True
    rf = RequestFactory()
    req = rf.post("/api/repasse-vila/confirmar/")
    req.user = user
    SessionMiddleware(lambda r: None).process_request(req)
    req.session.save()
    cu.definir_ponto_operacao_browser(req, "vila", s_vila.pk)
    from produtos.pdv_deposito_util import gravar_deposito_request

    gravar_deposito_request(req, "vila")
    req.session.save()

    salvar_reserva_vila(ZERO, operador="reserva-verify")
    calc_base = calcular_disponivel(hoje)
    bruto_envio = _dec(calc_base.get("total_sugerido_bruto"))
    print(f"HOJE {hoje} bruto_envio={bruto_envio} falta={calc_base.get('falta_dinheiro')} acum={calc_base.get('acumulado_anterior')}")

    if bruto_envio > Decimal("1.00"):
        corte = min(Decimal("10.00"), (bruto_envio / 2).quantize(Decimal("0.01")))
        salvar_reserva_vila(corte, operador="reserva-verify")
        calc_cut = calcular_disponivel(hoje)
        esperado = _dec(calc_cut.get("total_sugerido"))
        rep, err = confirmar_repasse(
            request=req,
            quem_levou="Bot Reserva",
            percentual_lucro=50,
            incluir_cmv=True,
            incluir_lucro=True,
            incluir_fiado=True,
            modo_dia_cheio=False,
            incluir_acumulado=True,
            operador="bot-reserva",
        )
        if err:
            fail(f"confirmar com reserva: {err}")
        else:
            assert rep is not None
            rep.observacao = TAG
            rep.save(update_fields=["observacao"])
            if _dec(rep.valor_total) == esperado:
                ok(f"confirmar desconta reserva total={rep.valor_total}")
            else:
                fail(f"confirmar {rep.valor_total} != esperado {esperado}")
            if rep.movimento_saida_id and _dec(rep.movimento_saida.valor) == esperado:
                ok("saída caixa = total com reserva")
            else:
                fail("saída caixa diverge")
    else:
        ok("confirmar reserva skipped (nada a levar hoje)")

    salvar_reserva_vila(Decimal("99999.99"), operador="reserva-verify")
    calc_full = calcular_disponivel(hoje)
    if _dec(calc_full.get("total_sugerido_bruto")) > 0:
        _rep, err_cov = confirmar_repasse(
            request=req,
            quem_levou="Bot Cobre",
            incluir_cmv=True,
            incluir_lucro=True,
            incluir_fiado=True,
            incluir_acumulado=True,
            operador="bot-reserva",
        )
        if err_cov and "fica na Vila" in err_cov:
            ok("reserva cobre o envio → recusa")
        elif err_cov and "Nada a levar" in err_cov:
            ok("nada a levar (reserva ou já enviado)")
        else:
            fail(f"esperava recusa reserva, veio {err_cov!r}")
        if _dec(calc_full.get("total_sugerido")) == ZERO:
            ok("sugerido zera quando reserva cobre")
        else:
            fail(f"sugerido deveria ser 0, {calc_full.get('total_sugerido')}")
    else:
        ok("recusa reserva skipped (bruto 0)")

    salvar_reserva_vila(Decimal("50.00"), operador="reserva-verify")
    disp = _dec((calcular_disponivel(hoje).get("disponivel") or {}).get("total"))
    if disp > 0:
        vm = min(Decimal("12.34"), disp)
        rep_m, err_m = confirmar_repasse(
            request=req,
            quem_levou="Bot Manual",
            incluir_cmv=True,
            incluir_lucro=True,
            incluir_fiado=True,
            valor_manual=vm,
            operador="bot-reserva",
        )
        if err_m:
            fail(f"manual com reserva: {err_m}")
        else:
            assert rep_m is not None
            rep_m.observacao = TAG
            rep_m.save(update_fields=["observacao"])
            if _dec(rep_m.valor_total) == vm:
                ok("valor manual ignora reserva")
            else:
                fail(f"manual {rep_m.valor_total} != {vm}")
    else:
        ok("manual skipped (sem disponível nas linhas)")

    salvar_reserva_vila(res_antes, operador="reserva-verify")
    if reserva_vila_config() == res_antes:
        ok("restaura reserva original")
    else:
        fail("não restaurou reserva")

    _cleanup_tag()
    if created_vila and s_vila and not MovimentoCaixa.objects.filter(sessao_caixa=s_vila).exists():
        s_vila.delete()
    ok("cleanup")

    print("---")
    print(f"oks={oks} fails={len(fails)}")
    for f in fails:
        print(f)
    if fails:
        return 1
    print("VERIFY_RESERVA_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
