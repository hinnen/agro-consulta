#!/usr/bin/env python
"""Prova — REPASSE-RESERVA: valor manual no lucro antes do % + diário + log."""
from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
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
    RepasseVilaReservaLogAgro,
    SessaoCaixa,
)
from produtos.repasse_vila_util import (
    RESERVA_VILA_DESDE_DEFAULT,
    ZERO,
    _dec,
    calcular_disponivel,
    confirmar_repasse,
    listar_acumulado_detalhe,
    listar_log_reserva,
    obter_config,
    reserva_aplicada_no_dia,
    reserva_vila_config,
    reserva_vila_desde_config,
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
        RepasseVilaReservaLogAgro.objects.filter(repasse_id=rep.pk).delete()
        rep.delete()


def main() -> int:
    hoje = timezone.localdate()

    check_file(
        "produtos/models.py",
        "reserva_vila",
        "reserva_vila_desde",
        "RepasseVilaReservaLogAgro",
        "lucro_penultimo_dia",
        "antes de aplicar o %",
    )
    check_file(
        "produtos/migrations/0097_repasse_reserva_lucro_log.py",
        "reserva_vila_desde",
        "RepasseVilaReservaLogAgro",
        "2026, 8, 18",
    )
    check_file(
        "produtos/repasse_vila_util.py",
        "salvar_reserva_vila",
        "reserva_aplicada_no_dia",
        "lucro_penultimo",
        "listar_log_reserva",
        "RESERVA_VILA_DESDE_DEFAULT",
        "antes do %",
    )
    check_file(
        "produtos/views_repasse_vila.py",
        "api_repasse_vila_reserva_log",
        "listar_log_reserva",
        "reserva_vila_desde",
    )
    check_file(
        "produtos/urls.py",
        "api/repasse-vila/reserva-log/",
    )
    check_file(
        "produtos/templates/produtos/repasse_vila.html",
        "rv-reserva",
        "rv-log-lista",
        "Fica na Vila",
        "penúltimo",
        "fetchLogReserva",
    )
    check_file(
        "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html",
        "pdv-rp-reserva",
        "Fica na Vila",
    )
    check_file(
        "produtos/static/produtos/js/pdv_repasse_vila.js",
        "reservaAtual",
        "reserva_vila: reservaAtual()",
    )
    forbid(
        "produtos/static/produtos/js/pdv_repasse_vila.js",
        "tot - reservaAtual()",
    )
    forbid(
        "produtos/templates/produtos/repasse_vila.html",
        "tot - reserva",
    )
    check_file(
        "produtos/templates/produtos/includes/repasse_help_agents.html",
        "antes",
        "18/08/2026",
        "todos os PCs",
    )

    field = RepasseVilaConfigAgro._meta.get_field("reserva_vila")
    ok("campo PG Decimal") if field.get_internal_type() == "DecimalField" else fail(
        f"tipo {field.get_internal_type()}"
    )
    ok("campo desde") if RepasseVilaConfigAgro._meta.get_field(
        "reserva_vila_desde"
    ).get_internal_type() == "DateField" else fail("sem desde")

    cfg = obter_config()
    res_antes = _dec(cfg.reserva_vila)
    desde_antes = getattr(cfg, "reserva_vila_desde", None)

    salvar_reserva_vila(-3, operador="reserva-verify")
    ok("clamp neg→0") if reserva_vila_config() == ZERO else fail("clamp neg")
    salvar_reserva_vila(Decimal("100000"), operador="reserva-verify")
    ok("clamp teto") if reserva_vila_config() == Decimal("99999.99") else fail("clamp teto")

    salvar_reserva_vila(Decimal("200.00"), operador="reserva-verify")
    if reserva_vila_desde_config() == RESERVA_VILA_DESDE_DEFAULT:
        ok("desde = 18/08/2026")
    else:
        fail(f"desde={reserva_vila_desde_config()}")

    logs = listar_log_reserva(limit=10)
    if any(x.get("tipo") == "config" and abs(x.get("valor_depois", 0) - 200) < 0.02 for x in logs):
        ok("log config 200")
    else:
        fail("sem log config")

    # Dia anterior ao desde: reserva NÃO aplica
    dia_antes = RESERVA_VILA_DESDE_DEFAULT - timedelta(days=1)
    apl0 = reserva_aplicada_no_dia(dia_antes, lucro_bruto=Decimal("999"))
    ok("antes do desde = 0") if apl0 == ZERO else fail(f"apl0={apl0}")

    calc = calcular_disponivel(hoje)
    lucro_b = _dec(calc.get("lucro_bruto_dia"))
    reserva_apl = _dec(calc.get("reserva_aplicada"))
    pen = _dec(calc.get("lucro_penultimo_dia"))
    pct = _dec(calc.get("percentual_lucro"))
    alvo_lucro = _dec((calc.get("alvos") or {}).get("lucro"))
    desp_c = _dec(calc.get("despesas_centro_dia"))

    if hoje >= RESERVA_VILA_DESDE_DEFAULT:
        esp_apl = min(Decimal("200.00"), max(ZERO, lucro_b))
        if reserva_apl == esp_apl:
            ok(f"reserva_aplicada={reserva_apl}")
        else:
            fail(f"reserva_aplicada={reserva_apl} esp={esp_apl}")
        esp_pen = max(ZERO, (lucro_b - reserva_apl).quantize(Decimal("0.01")))
        if pen == esp_pen:
            ok(f"penúltimo={pen}")
        else:
            fail(f"pen={pen} esp={esp_pen}")
        esp_alvo = max(ZERO, ((esp_pen * pct / Decimal("100")) - desp_c).quantize(Decimal("0.01")))
        if alvo_lucro == esp_alvo:
            ok("alvo lucro = % do penúltimo − planos")
        else:
            fail(f"alvo_lucro={alvo_lucro} esp={esp_alvo}")
    else:
        ok("hoje antes do desde (skip fórmula)")

    # NÃO corta de novo o total
    sug = _dec(calc.get("total_sugerido"))
    bruto = _dec(calc.get("total_sugerido_bruto"))
    if sug == bruto or (bruto < 0 and sug == ZERO):
        ok("sugerido não corta reserva de novo")
    else:
        fail(f"sug={sug} bruto={bruto} (não deveria subtrair reserva no total)")

    det = listar_acumulado_detalhe(hoje)
    if abs(_dec(det.get("total_sugerido")) - sug) < Decimal("0.02"):
        ok("detalhe total_sugerido = calc")
    else:
        fail(f"detalhe sug {det.get('total_sugerido')} != {sug}")

    salvar_reserva_vila(ZERO, operador="reserva-verify")
    calc0 = calcular_disponivel(hoje)
    if _dec(calc0.get("reserva_aplicada")) == ZERO:
        ok("reserva 0 → aplicada 0")
    else:
        fail("reserva 0 ainda aplica")

    # HTTP
    user, _ = User.objects.get_or_create(
        username="repasse_verify_bot", defaults={"is_staff": True}
    )
    client = Client(HTTP_HOST="127.0.0.1")
    client.force_login(user)
    r = client.get("/api/repasse-vila/config/")
    j = r.json() if r.status_code == 200 else {}
    if r.status_code == 200 and "reserva_vila" in j and "reserva_vila_desde" in j:
        ok("GET config reserva+desde")
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

    r = client.get("/api/repasse-vila/reserva-log/")
    if r.status_code == 200 and isinstance(r.json().get("logs"), list):
        ok("GET reserva-log")
    else:
        fail(f"reserva-log {r.status_code}")

    r = client.get("/api/repasse-vila/calc/")
    if r.status_code == 200:
        cj = r.json()
        if "reserva_aplicada" in cj and "lucro_penultimo_dia" in cj:
            ok("GET calc penúltimo")
        else:
            fail("calc sem penúltimo")
    else:
        fail(f"GET calc {r.status_code}")

    r = client.get("/repasse-vila/")
    body = r.content if r.status_code == 200 else b""
    ok("GET tela 200") if r.status_code == 200 else fail(f"tela {r.status_code}")
    for needle in (b"rv-reserva", b"rv-log-lista", b"Fica na Vila"):
        if needle in body:
            ok(f"tela tem {needle.decode()}")
        else:
            fail(f"tela sem {needle.decode()}")

    # confirmar grava snapshot + log
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

    salvar_reserva_vila(Decimal("50.00"), operador="reserva-verify")
    calc_base = calcular_disponivel(hoje)
    sug_envio = _dec(calc_base.get("total_sugerido"))
    print(
        f"HOJE {hoje} sug={sug_envio} pen={calc_base.get('lucro_penultimo_dia')} "
        f"res={calc_base.get('reserva_aplicada')}"
    )

    if sug_envio > Decimal("1.00"):
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
            fail(f"confirmar: {err}")
        else:
            assert rep is not None
            rep.observacao = TAG
            rep.save(update_fields=["observacao"])
            if _dec(rep.valor_total) == sug_envio:
                ok(f"confirmar total={rep.valor_total}")
            else:
                fail(f"confirmar {rep.valor_total} != {sug_envio}")
            if _dec(rep.reserva_aplicada) == _dec(calc_base.get("reserva_aplicada")):
                ok("snapshot reserva_aplicada")
            else:
                fail("sem snapshot reserva")
            if _dec(rep.lucro_penultimo_dia) == _dec(calc_base.get("lucro_penultimo_dia")):
                ok("snapshot penúltimo")
            else:
                fail("sem snapshot penúltimo")
            if RepasseVilaReservaLogAgro.objects.filter(
                tipo="aplicado", repasse_id=rep.pk
            ).exists():
                ok("log aplicado no envio")
            else:
                fail("sem log aplicado")
    else:
        ok("confirmar skipped (nada a levar)")

    # valor manual digitado ainda manda o total
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
            fail(f"manual: {err_m}")
        else:
            assert rep_m is not None
            rep_m.observacao = TAG
            rep_m.save(update_fields=["observacao"])
            if _dec(rep_m.valor_total) == vm:
                ok("valor manual manda o total")
            else:
                fail(f"manual {rep_m.valor_total} != {vm}")
    else:
        ok("manual skipped")

    # restaura
    salvar_reserva_vila(res_antes, operador="reserva-verify")
    if desde_antes is not None:
        cfg2 = obter_config()
        cfg2.reserva_vila_desde = desde_antes
        cfg2.save(update_fields=["reserva_vila_desde"])
    if reserva_vila_config() == res_antes:
        ok("restaura reserva")
    else:
        fail("não restaurou")

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
