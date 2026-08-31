#!/usr/bin/env python
"""Prova detalhada — arredondar cofres do repasse (REPASSE-ARREDONDA-COFRE).

Contrato loja:
- Salário, Vila Elias e Levar ao Centro aceitam o valor digitado (pra mais ou pra menos).
- Falta soma amanhã. Excedente vira crédito (acumulado negativo) e desconta o próximo dia.
- Sem trava «maior que o pendente». Gaveta insuficiente ainda barra.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ["DEBUG"] = "False"

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
    RepasseVilaReservaMovimentoAgro,
    SessaoCaixa,
)
from produtos.repasse_vila_util import (
    COFRE_VILA_ELIAS,
    COFRE_VILA_ELIAS_DESDE,
    RESERVA_VILA_DESDE_DEFAULT,
    _dec,
    _extra_do_calc,
    abater_extras_do_acumulado,
    confirmar_repasse,
    obter_config,
    pendente_reserva_cofrinho_ate,
    resumo_cofrinho_vila,
    separar_reserva_diaria,
)

User = get_user_model()
fails: list[str] = []
oks = 0
PREFIX = "verify-arredonda-cofre"


def check(cond, msg):
    global oks
    if cond:
        oks += 1
        print("OK", msg)
    else:
        fails.append(msg)
        print("FAIL", msg)


def needle(path: str, *needles: str, forbid: bool = False):
    text = (ROOT / path).read_text(encoding="utf-8", errors="replace")
    for n in needles:
        found = n in text
        if forbid:
            check(not found, f"{path} sem {n!r}")
        else:
            check(found, f"{path} tem {n!r}")


def calc_fake(reserva=Decimal("52.80"), total=Decimal("80.00"), parte_vila_elias=Decimal("66.43")):
    return {
        "ok": True,
        "percentual_lucro": 50.0,
        "percentual_padrao": 50.0,
        "reserva_vila": float(reserva),
        "reserva_vila_desde": RESERVA_VILA_DESDE_DEFAULT.isoformat(),
        "reserva_aplicada": float(reserva),
        "parte_salario": float(reserva),
        "parte_vila_elias": float(parte_vila_elias),
        "lucro_penultimo_dia": 120.0,
        "receita_dia": 300.0,
        "cmv_dia": 100.0,
        "lucro_bruto_dia": 200.0,
        "fiado_pago_dia": 0.0,
        "despesas_centro_dia": 0.0,
        "despesas_vila_dia": 0.0,
        "acumulado_anterior": 0.0,
        "total_sugerido": float(total),
        "total_sugerido_bruto": float(total),
        "disponivel": {"cmv": float(total), "lucro": 0.0, "fiado": 0.0, "total": float(total)},
        "alvos": {"cmv": float(total), "lucro": 0.0, "fiado": 0.0},
        "ja_enviado": {"cmv": 0.0, "lucro": 0.0, "fiado": 0.0, "total": 0.0},
        "ja_eletronico": 0.0,
        "ja_eletronico_aplicado": 0.0,
        "falta_dinheiro": float(total),
    }


def cleanup():
    reps = RepasseVilaCentroAgro.objects.filter(observacao=PREFIX)
    mov_ids = []
    for rep in reps:
        mov_ids.extend([rep.movimento_saida_id, rep.movimento_entrada_id])
    tagged = RepasseVilaReservaMovimentoAgro.objects.filter(
        idempotencia_chave__startswith=PREFIX
    )
    RepasseVilaReservaMovimentoAgro.objects.filter(tipo="estorno", estornado_de__in=tagged).delete()
    tagged.delete()
    for dia in (
        timezone.localdate() - timedelta(days=1),
        timezone.localdate(),
        timezone.localdate() + timedelta(days=1),
        date(2026, 8, 29),
        date(2026, 8, 30),
        date(2026, 8, 31),
    ):
        por_dia = RepasseVilaReservaMovimentoAgro.objects.filter(
            idempotencia_chave__contains=f":separacao:{dia.isoformat()}:"
        )
        RepasseVilaReservaMovimentoAgro.objects.filter(
            tipo="estorno", estornado_de__in=por_dia
        ).delete()
        por_dia.delete()
    reps.delete()
    MovimentoCaixa.objects.filter(pk__in=[x for x in mov_ids if x]).delete()
    MovimentoCaixa.objects.filter(observacao__contains=PREFIX).delete()
    SessaoCaixa.objects.filter(usuario__username="verify_arredonda_bot").delete()


def main() -> int:
    UTIL = "produtos/repasse_vila_util.py"
    JS = "produtos/static/produtos/js/pdv_repasse_vila.js"
    HTML = "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html"
    AJUDA = "produtos/templates/produtos/includes/repasse_help_agents.html"
    GESTAO = "produtos/templates/produtos/repasse_vila.html"

    needle(UTIL, "pendente_liquido", "Pode ser maior que o pendente")
    needle(UTIL, "maior que o pendente R$", "min(v_cof_sal, pend_sal)", "min(pedido, falta, dinheiro)", forbid=True)
    needle(JS, "pdv-rp-hero-cofre-acum", "pdv-rp-hero-cofre-ve-acum", "excedente", "desconta amanhã")
    needle(JS, "renderCalc();")
    needle(HTML, "pdv-rp-hero-cofre-acum", "pdv-rp-hero-cofre-ve-acum", "pode arredondar pra mais ou pra menos")
    needle(AJUDA, "negativo no acumulado", "pode arredondar pra mais ou pra menos")
    needle(GESTAO, "Excedente", "acumulado negativo")

    node = subprocess.run(
        ["node", "--check", str(ROOT / JS)],
        capture_output=True,
        text=True,
    )
    check(node.returncode == 0, "node --check pdv_repasse_vila.js")

    pin = "9973"
    ok_pin, label_pin, err_pin = cu.operador_label_de_pin(pin)
    check(ok_pin and bool(label_pin) and not err_pin, "PIN da loja reconhece operador")

    cfg = obter_config()
    reserva_antes = _dec(cfg.reserva_vila)
    saldo_antes = _dec(getattr(cfg, "saldo_reserva_vila", 0))
    saldo_ve_antes = _dec(getattr(cfg, "saldo_cofre_vila_elias", 0))
    desde_antes = cfg.reserva_vila_desde
    cleanup()

    user, _ = User.objects.get_or_create(username="verify_arredonda_bot", defaults={"is_staff": True})
    sessao = SessaoCaixa.objects.create(ponto_caixa="vila", valor_abertura=Decimal("500"), usuario=user)
    MovimentoCaixa.objects.create(
        sessao_caixa=sessao,
        tipo=MovimentoCaixa.Tipo.REFORCO,
        forma_pagamento="Dinheiro",
        valor=Decimal("3000.00"),
        observacao=PREFIX + " reforco",
        usuario=user,
    )

    # Janela de 1 dia (29/08) — senão o VE soma 29+30+31 e o crédito some.
    hoje = COFRE_VILA_ELIAS_DESDE
    amanha = hoje + timedelta(days=1)
    cfg.reserva_vila_desde = hoje
    cfg.saldo_reserva_vila = Decimal("0.00")
    cfg.saldo_cofre_vila_elias = Decimal("0.00")
    cfg.save(update_fields=["reserva_vila_desde", "saldo_reserva_vila", "saldo_cofre_vila_elias"])

    try:
        fake = calc_fake()
        with patch("produtos.repasse_vila_util.calcular_disponivel", return_value=fake):
            mov_menos, cri_menos, err_menos = separar_reserva_diaria(
                hoje,
                origem="lancamento_separado",
                operador="Bot Arredonda",
                usuario=user,
                sessao_caixa=sessao,
                valor=Decimal("50.00"),
            )
            check(
                not err_menos and cri_menos and _dec(mov_menos.valor) == Decimal("50.00"),
                f"salário 50 < 52,80 aceita · {err_menos}",
            )
            pend_hoje_sal = pendente_reserva_cofrinho_ate(hoje)
            check(pend_hoje_sal["pendente"] == Decimal("2.80"), "salário arredonda pra menos: falta 2,80 hoje")
            pend_am_sal = pendente_reserva_cofrinho_ate(amanha)
            check(pend_am_sal["pendente"] == Decimal("55.60"), "falta 2,80 soma na obrigação de amanhã (52,80+2,80)")

            mov_ve, cri_ve, err_ve = separar_reserva_diaria(
                hoje,
                origem="lancamento_separado",
                operador="Bot Arredonda",
                usuario=user,
                sessao_caixa=sessao,
                cofre=COFRE_VILA_ELIAS,
                valor=Decimal("100.00"),
            )
            check(
                not err_ve and cri_ve and _dec(mov_ve.valor) == Decimal("100.00"),
                f"VE 100 > 66,43 aceita · {err_ve}",
            )
            pend_ve = pendente_reserva_cofrinho_ate(hoje, cofre=COFRE_VILA_ELIAS)
            check(
                pend_ve["pendente"] == Decimal("0.00") and pend_ve["adiantado"] == Decimal("33.57"),
                "VE excedente 33,57 vira crédito",
            )
            rsum_ve = resumo_cofrinho_vila(hoje, cofre=COFRE_VILA_ELIAS)
            check(float(rsum_ve.get("pendente_liquido") or 0) == -33.57, "VE pendente_liquido = −33,57")
            pend_ve_am = pendente_reserva_cofrinho_ate(amanha, cofre=COFRE_VILA_ELIAS)
            check(pend_ve_am["pendente"] == Decimal("32.86"), "crédito VE desconta amanhã (66,43−33,57)")

            mov_extra, cri_ex, err_ex = separar_reserva_diaria(
                hoje,
                origem="lancamento_separado",
                operador="Bot Arredonda",
                usuario=user,
                sessao_caixa=sessao,
                valor=Decimal("20.00"),
            )
            check(
                not err_ex and cri_ex and _dec(mov_extra.valor) == Decimal("20.00"),
                f"ainda permite separar depois de já ter crédito/parcial · {err_ex}",
            )
            pend_sal_apos = pendente_reserva_cofrinho_ate(hoje)
            check(pend_sal_apos["adiantado"] == Decimal("17.20"), "50+20−52,80 = crédito 17,20 no salário")

            mov_sem, _cri_sem, err_sem = separar_reserva_diaria(
                hoje,
                origem="lancamento_separado",
                operador="Bot Arredonda",
                usuario=user,
                sessao_caixa=sessao,
                valor=Decimal("9999.00"),
            )
            check(mov_sem is None and "gaveta" in (err_sem or "").lower(), "valor alto demais barra por gaveta, não por pendente")
            check("maior que o pendente" not in (err_sem or ""), "mensagem não cita teto do pendente")

        RepasseVilaReservaMovimentoAgro.objects.filter(
            data_ref__gte=hoje, data_ref__lte=amanha
        ).delete()
        cfg.saldo_reserva_vila = Decimal("0.00")
        cfg.saldo_cofre_vila_elias = Decimal("0.00")
        cfg.save(update_fields=["saldo_reserva_vila", "saldo_cofre_vila_elias"])
        MovimentoCaixa.objects.create(
            sessao_caixa=sessao,
            tipo=MovimentoCaixa.Tipo.REFORCO,
            forma_pagamento="Dinheiro",
            valor=Decimal("3000.00"),
            observacao=PREFIX + " reforco-2",
            usuario=user,
        )

        rf = RequestFactory()
        req = rf.post("/api/repasse-vila/confirmar/")
        req.user = user
        SessionMiddleware(lambda r: None).process_request(req)
        req.session.save()
        cu.definir_ponto_operacao_browser(req, "vila", sessao.pk)
        from produtos.pdv_deposito_util import gravar_deposito_request

        gravar_deposito_request(req, "vila")
        req.session.save()

        fake_rep = calc_fake(Decimal("52.80"), Decimal("80.00"), Decimal("66.43"))
        with patch("produtos.repasse_vila_util.calcular_disponivel", return_value=fake_rep), patch(
            "produtos.caixa_util.obter_caixa_vila_aberto", return_value=sessao
        ), patch("produtos.caixa_util.obter_caixa_gaveta_aberto", return_value=None):
            rep, err = confirmar_repasse(
                request=req,
                quem_levou="Bot Arredonda",
                valor_manual=Decimal("120.00"),
                forma_pagamento="Dinheiro",
                operador="Bot Arredonda",
                data_ref=hoje,
                separar_reserva=True,
                valor_cofre_salario=Decimal("50.00"),
                valor_cofre_vila_elias=Decimal("100.00"),
                forcar_manual_zerado=True,
            )
            rsum_sal = resumo_cofrinho_vila(hoje)
            rsum_ve2 = resumo_cofrinho_vila(hoje, cofre=COFRE_VILA_ELIAS)
        check(not err and rep is not None and _dec(rep.valor_total) == Decimal("120.00"), f"repasse confirma 50+100+120 · {err}")
        if rep:
            rep.observacao = PREFIX
            rep.save(update_fields=["observacao"])
            check(
                RepasseVilaReservaMovimentoAgro.objects.filter(
                    repasse=rep, cofre="salario", valor=Decimal("50.00")
                ).exists(),
                "repasse gravou salário 50",
            )
            check(
                RepasseVilaReservaMovimentoAgro.objects.filter(
                    repasse=rep, cofre="vila_elias", valor=Decimal("100.00")
                ).exists(),
                "repasse gravou VE 100",
            )
            extra = _extra_do_calc(
                {
                    "alvos": {"cmv": 80.0, "lucro": 0.0, "fiado": 0.0},
                    "ja_eletronico_aplicado": 0,
                    "ja_enviado": {"total": 120.0},
                }
            )
            check(extra == Decimal("40.00"), "levar 120 vs alvo 80 = extra 40")
            acum_liq = abater_extras_do_acumulado(hoje, Decimal("10.00"), {
                "alvos": {"cmv": 80.0, "lucro": 0.0, "fiado": 0.0},
                "ja_eletronico_aplicado": 0,
                "ja_enviado": {"total": 120.0},
            })
            check(acum_liq == Decimal("-30.00"), "extra do envelope abate acumulado (10−40=−30)")
            check(float(rsum_sal.get("pendente_liquido") or 0) == 2.80, "após repasse salário ainda deve 2,80")
            check(float(rsum_ve2.get("pendente_liquido") or 0) == -33.57, "após repasse VE crédito −33,57")

        client = Client(HTTP_HOST="127.0.0.1")
        client.force_login(user)
        tela = client.get("/repasse-vila/")
        body = tela.content.decode("utf-8", errors="replace")
        check(tela.status_code == 200 and "rv-cofrinho-card" in body, "GET /repasse-vila/ 200")
        api_sal = client.get("/api/repasse-vila/cofrinho/")
        api_ve = client.get("/api/repasse-vila/cofrinho/?cofre=vila_elias")
        check(api_sal.status_code == 200 and "pendente_liquido" in api_sal.json(), "API cofrinho expõe pendente_liquido")
        check(api_ve.status_code == 200 and api_ve.json().get("cofre") == "vila_elias", "API cofre Vila Elias")

        overlay = (ROOT / HTML).read_text(encoding="utf-8")
        check("pdv-rp-input-cofre-sal" in overlay and "pdv-rp-input-cofre-ve" in overlay and "pdv-rp-manual" in overlay, "3 campos no overlay")
    finally:
        cleanup()
        cfg = RepasseVilaConfigAgro.objects.get(pk=cfg.pk)
        cfg.reserva_vila = reserva_antes
        cfg.saldo_reserva_vila = saldo_antes
        cfg.saldo_cofre_vila_elias = saldo_ve_antes
        cfg.reserva_vila_desde = desde_antes or RESERVA_VILA_DESDE_DEFAULT
        cfg.save(
            update_fields=[
                "reserva_vila",
                "saldo_reserva_vila",
                "saldo_cofre_vila_elias",
                "reserva_vila_desde",
            ]
        )

    print("---")
    print(f"oks={oks} fails={len(fails)}")
    for item in fails:
        print(item)
    if fails:
        return 1
    print("VERIFY_REPASSE_ARREDONDA_COFRE_PATH_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
