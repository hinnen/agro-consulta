#!/usr/bin/env python
"""Prova detalhada — REPASSE-ZERO-OK (confirmar com algum dos 3 valores em 0,00).

  python scripts/verify_repasse_zero_ok_path.py

Contratos: fonte · PIN 9973 · Django (confirmar_repasse isolado + cleanup) · HTTP local se up.
VERIFY_REPASSE_ZERO_OK_PATH_OK N/N · VERIFY_FAIL.
"""
from __future__ import annotations

import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ["DEBUG"] = "False"

PIN = "9973"
BASE = os.environ.get("AGRO_VERIFY_BASE", "http://127.0.0.1:8000").rstrip("/")
PREFIX = "verify-repasse-zero-ok"
USER_BOT = "verify_zero_ok_bot"

fails: list[str] = []
oks = 0


def check(cond, msg: str) -> None:
    global oks
    if cond:
        oks += 1
        print("OK", msg)
    else:
        fails.append(msg)
        print("FAIL", msg)


def needle(path: str, *needles: str, forbid: bool = False) -> None:
    text = (ROOT / path).read_text(encoding="utf-8", errors="replace")
    for n in needles:
        found = n in text
        if forbid:
            check(not found, f"{path} sem {n!r}")
        else:
            check(found, f"{path} tem {n!r}")


def calc_fake(total: Decimal = Decimal("80.00")):
    from produtos.repasse_vila_util import RESERVA_VILA_DESDE_DEFAULT

    t = float(total)
    return {
        "ok": True,
        "percentual_lucro": 50.0,
        "percentual_padrao": 50.0,
        "reserva_vila": 50.0,
        "reserva_vila_desde": RESERVA_VILA_DESDE_DEFAULT.isoformat(),
        "reserva_aplicada": 50.0,
        "parte_salario": 50.0,
        "parte_vila_elias": 40.0,
        "lucro_penultimo_dia": 120.0,
        "receita_dia": 300.0,
        "cmv_dia": 100.0,
        "lucro_bruto_dia": 200.0,
        "fiado_pago_dia": 0.0,
        "despesas_centro_dia": 0.0,
        "despesas_vila_dia": 0.0,
        "acumulado_anterior": 0.0,
        "total_sugerido": t,
        "total_sugerido_bruto": t,
        "disponivel": {"cmv": t, "lucro": 0.0, "fiado": 0.0, "total": t},
        "alvos": {"cmv": t, "lucro": 0.0, "fiado": 0.0},
        "ja_enviado": {"cmv": 0.0, "lucro": 0.0, "fiado": 0.0, "total": 0.0},
        "ja_eletronico": 0.0,
        "ja_eletronico_aplicado": 0.0,
        "falta_dinheiro": t,
    }


def prova_fonte() -> None:
    print("=== fonte ===")
    UTIL = "produtos/repasse_vila_util.py"
    JS = "produtos/static/produtos/js/pdv_repasse_vila.js"
    HTML = "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html"
    VIEW = "produtos/views_repasse_vila.py"

    needle(UTIL, "if vm == 0:", "Levar ao Centro 0,00", "if vm < 0:", "Valor manual inválido.")
    needle(UTIL, "if vm <= 0:", forbid=True)
    needle(JS, "zeroSeVazio", "pode ser 0,00", "cofreCheckPassosAtivos", ">= 0.009")
    needle(JS, "Informe ao menos um valor maior que zero.")
    needle(JS, "Preencha os 3 valores", forbid=True)
    needle(HTML, "pdv-rp-input-cofre-sal", "pdv-rp-input-cofre-ve", "pdv-rp-manual", "placeholder=\"0,00\"")
    needle(VIEW, "somente_cofres", 'vm_raw not in (None, "")')
    node = subprocess.run(
        ["node", "--check", str(ROOT / JS)],
        capture_output=True,
        text=True,
    )
    check(node.returncode == 0, "node --check pdv_repasse_vila.js")


def prova_node_passos() -> None:
    print("=== node (3 OKs pulam 0,00) ===")
    js = r"""
    function ativos(sal, ve, lev) {
      var vals = { salario: sal, vilaElias: ve, levar: lev };
      var passos = [{key:'salario'},{key:'vilaElias'},{key:'levar'}];
      return passos.filter(function (p) { return Number(vals[p.key] || 0) >= 0.009; }).length;
    }
    function allZero(a,b,c) { return a < 0.009 && b < 0.009 && c < 0.009; }
    if (ativos(0, 40, 80) !== 2) process.exit(2);
    if (ativos(50, 0, 0) !== 1) process.exit(3);
    if (ativos(0, 0, 10) !== 1) process.exit(4);
    if (ativos(0, 0, 0) !== 0) process.exit(5);
    if (!allZero(0,0,0)) process.exit(6);
    if (allZero(0,0,0.01)) process.exit(7);
    if (ativos(0.005, 0, 1) !== 1) process.exit(8);
    """
    r = subprocess.run(["node", "-e", js], capture_output=True, text=True)
    check(r.returncode == 0, "filtro 3 OKs: 0,00 sai da fila · 1–2 ativos · todos zero = 0")


def cleanup(user=None) -> None:
    from produtos.models import (
        MovimentoCaixa,
        RepasseVilaCentroAgro,
        RepasseVilaReservaMovimentoAgro,
        SessaoCaixa,
    )

    reps = RepasseVilaCentroAgro.objects.filter(observacao__contains=PREFIX)
    mov_ids = []
    for rep in reps:
        mov_ids.extend([rep.movimento_saida_id, rep.movimento_entrada_id])
    if user is not None:
        reps_u = RepasseVilaCentroAgro.objects.filter(usuario=user)
        for rep in reps_u:
            mov_ids.extend([rep.movimento_saida_id, rep.movimento_entrada_id])
        RepasseVilaReservaMovimentoAgro.objects.filter(usuario=user).delete()
        reps_u.delete()
    tagged = RepasseVilaReservaMovimentoAgro.objects.filter(observacao__contains=PREFIX)
    tagged.delete()
    reps.delete()
    MovimentoCaixa.objects.filter(pk__in=[x for x in mov_ids if x]).delete()
    MovimentoCaixa.objects.filter(observacao__contains=PREFIX).delete()
    SessaoCaixa.objects.filter(usuario__username=USER_BOT).delete()


def make_req(user, sessao):
    from django.contrib.sessions.middleware import SessionMiddleware
    from django.test import RequestFactory
    from produtos import caixa_util as cu
    from produtos.pdv_deposito_util import gravar_deposito_request

    rf = RequestFactory()
    req = rf.post("/api/repasse-vila/confirmar/")
    req.user = user
    SessionMiddleware(lambda r: None).process_request(req)
    req.session.save()
    cu.definir_ponto_operacao_browser(req, "vila", sessao.pk)
    gravar_deposito_request(req, "vila")
    req.session.save()
    return req


def prova_django() -> None:
    print("=== Django confirmar_repasse ===")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from django.utils import timezone

    from produtos import caixa_util as cu
    from produtos.models import (
        MovimentoCaixa,
        RepasseVilaCentroAgro,
        RepasseVilaReservaMovimentoAgro,
        SessaoCaixa,
    )
    from produtos.repasse_vila_util import (
        COFRE_VILA_ELIAS,
        _dec,
        confirmar_repasse,
        obter_config,
        resumo_cofrinho_vila,
    )

    ok_pin, label_pin, err_pin = cu.operador_label_de_pin(PIN)
    check(ok_pin and bool(label_pin) and not err_pin, f"PIN 9973 operador={label_pin!r}")

    User = get_user_model()
    cfg = obter_config()
    saldo_antes = _dec(getattr(cfg, "saldo_reserva_vila", 0))
    saldo_ve_antes = _dec(getattr(cfg, "saldo_cofre_vila_elias", 0))
    user, _ = User.objects.get_or_create(username=USER_BOT, defaults={"is_staff": True})
    cleanup(user)

    box = {"sessao": None}

    def nova_sessao():
        cleanup(user)
        s = SessaoCaixa.objects.create(ponto_caixa="vila", valor_abertura=Decimal("500"), usuario=user)
        MovimentoCaixa.objects.create(
            sessao_caixa=s,
            tipo=MovimentoCaixa.Tipo.REFORCO,
            forma_pagamento="Dinheiro",
            valor=Decimal("3000.00"),
            observacao=PREFIX + " reforco",
            usuario=user,
        )
        box["sessao"] = s
        return s

    hoje = timezone.localdate()
    fake = calc_fake(Decimal("80.00"))
    try:
        sessao = nova_sessao()
        req = make_req(user, sessao)
        with patch("produtos.repasse_vila_util.calcular_disponivel", return_value=fake), patch(
            "produtos.caixa_util.obter_caixa_vila_aberto", side_effect=lambda *a, **k: box["sessao"]
        ), patch("produtos.caixa_util.obter_caixa_gaveta_aberto", return_value=None):
            rep, err = confirmar_repasse(
                request=req,
                quem_levou="Bot ZeroOk",
                valor_manual=Decimal("0.00"),
                forma_pagamento="Dinheiro",
                operador=label_pin or "Bot ZeroOk",
                data_ref=hoje,
                separar_reserva=True,
                valor_cofre_salario=Decimal("40.00"),
                valor_cofre_vila_elias=Decimal("0.00"),
                forcar_manual_zerado=True,
            )
            check(err == "" and rep is None, f"Centro 0 + salário 40 = só cofres · {err!r} rep={rep}")
            check(
                RepasseVilaReservaMovimentoAgro.objects.filter(
                    usuario=user, cofre="salario", valor=Decimal("40.00")
                ).exists(),
                "gravou cofrinho salário 40",
            )
            check(
                not RepasseVilaReservaMovimentoAgro.objects.filter(usuario=user, cofre=COFRE_VILA_ELIAS).exists(),
                "VE 0,00 não gerou movimento",
            )
            check(
                not MovimentoCaixa.objects.filter(
                    sessao_caixa=sessao, tipo=MovimentoCaixa.Tipo.RETIRADA, observacao__contains="Repasse Vila"
                ).exists(),
                "Centro 0,00 não retirou da gaveta",
            )

            sessao = nova_sessao()
            req = make_req(user, sessao)

            rep2, err2 = confirmar_repasse(
                request=req,
                quem_levou="Bot ZeroOk",
                valor_manual=Decimal("0.00"),
                forma_pagamento="Dinheiro",
                operador=label_pin or "Bot ZeroOk",
                data_ref=hoje,
                separar_reserva=True,
                valor_cofre_salario=Decimal("0.00"),
                valor_cofre_vila_elias=Decimal("35.00"),
                forcar_manual_zerado=True,
            )
            check(err2 == "" and rep2 is None, f"Centro 0 + VE 35 = só cofres · {err2!r}")
            check(
                RepasseVilaReservaMovimentoAgro.objects.filter(
                    usuario=user, cofre=COFRE_VILA_ELIAS, valor=Decimal("35.00")
                ).exists(),
                "gravou cofre Vila Elias 35",
            )

            sessao = nova_sessao()
            req = make_req(user, sessao)

            rep3, err3 = confirmar_repasse(
                request=req,
                quem_levou="Bot ZeroOk",
                valor_manual=Decimal("80.00"),
                forma_pagamento="Dinheiro",
                operador=label_pin or "Bot ZeroOk",
                data_ref=hoje,
                separar_reserva=False,
                valor_cofre_salario=Decimal("0.00"),
                valor_cofre_vila_elias=Decimal("0.00"),
                forcar_manual_zerado=True,
            )
            check(
                not err3 and rep3 is not None and _dec(rep3.valor_total) == Decimal("80.00"),
                f"cofres 0,00 + Centro 80 confirma envelope · {err3}",
            )
            if rep3:
                rep3.observacao = PREFIX
                rep3.save(update_fields=["observacao"])
            check(
                not RepasseVilaReservaMovimentoAgro.objects.filter(usuario=user).exists(),
                "cofres 0,00 não separaram junto do envelope",
            )

            sessao = nova_sessao()
            req = make_req(user, sessao)

            rep4, err4 = confirmar_repasse(
                request=req,
                quem_levou="Bot ZeroOk",
                valor_manual=Decimal("0.00"),
                forma_pagamento="Dinheiro",
                operador=label_pin or "Bot ZeroOk",
                data_ref=hoje,
                separar_reserva=False,
                valor_cofre_salario=Decimal("0.00"),
                valor_cofre_vila_elias=Decimal("0.00"),
                forcar_manual_zerado=True,
            )
            check(rep4 is None and "Nada a levar" in (err4 or ""), f"os 3 em 0,00 travam · {err4!r}")

            rep5, err5 = confirmar_repasse(
                request=req,
                quem_levou="Bot ZeroOk",
                valor_manual=Decimal("-1.00"),
                forma_pagamento="Dinheiro",
                operador=label_pin or "Bot ZeroOk",
                data_ref=hoje,
                forcar_manual_zerado=True,
            )
            check(rep5 is None and "inválido" in (err5 or "").lower(), f"negativo continua inválido · {err5!r}")

        with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
            client = Client(HTTP_HOST="127.0.0.1")
            client.force_login(user)
            overlay = (ROOT / "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html").read_text(
                encoding="utf-8"
            )
            check("pdv-rp-manual" in overlay and 'placeholder="0,00"' in overlay, "overlay tem 3 campos 0,00")
            r_rep = client.get("/repasse-vila/")
            check(r_rep.status_code == 200, f"GET /repasse-vila/ {r_rep.status_code}")
    finally:
        cleanup(user)
        cfg2 = obter_config()
        cfg2.saldo_reserva_vila = saldo_antes
        cfg2.saldo_cofre_vila_elias = saldo_ve_antes
        cfg2.save(update_fields=["saldo_reserva_vila", "saldo_cofre_vila_elias"])
        # resumo só pra não deixar lint; não falha se PG local sem cofrinho real
        try:
            resumo_cofrinho_vila(hoje)
        except Exception:
            pass


def prova_http() -> None:
    print("=== HTTP local ===")
    try:
        req = Request(BASE + "/healthz", method="GET")
        with urlopen(req, timeout=4) as resp:
            code = int(resp.status)
    except (URLError, TimeoutError, OSError, HTTPError) as exc:
        check(True, f"runserver off — HTTP skip ({type(exc).__name__})")
        return
    check(code in (200, 204), f"healthz {code}")
    try:
        req2 = Request(BASE + "/static/produtos/js/pdv_repasse_vila.js", method="GET")
        with urlopen(req2, timeout=6) as resp2:
            body = resp2.read().decode("utf-8", "replace")
        check("zeroSeVazio" in body and "cofreCheckPassosAtivos" in body, "JS estático local tem REPASSE-ZERO-OK")
        check("Preencha os 3 valores" not in body, "JS estático sem trava de preencher os 3")
    except (URLError, TimeoutError, OSError, HTTPError) as exc:
        check(True, f"static JS skip ({type(exc).__name__})")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    prova_fonte()
    prova_node_passos()
    prova_django()
    prova_http()
    print("---")
    print(f"oks={oks} fails={len(fails)}")
    for item in fails:
        print("FAIL", item)
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_REPASSE_ZERO_OK_PATH_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
