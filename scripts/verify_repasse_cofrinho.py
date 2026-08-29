#!/usr/bin/env python
"""Prova do cofrinho Vila: saldo, caixa, idempotência, repasse e rastreabilidade."""
from __future__ import annotations

import os
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
    RESERVA_VILA_DESDE_DEFAULT,
    _dec,
    aplicar_reserva_virtual_estado_caixa,
    confirmar_repasse,
    estornar_movimento_cofrinho,
    obter_config,
    pendente_reserva_cofrinho_ate,
    registrar_saldo_inicial_cofrinho,
    registrar_uso_ou_ajuste_cofrinho,
    resumo_cofrinho_vila,
    saldo_cofrinho_vila,
    separar_reserva_diaria,
    separar_reservas_ao_fechar_vila,
)

User = get_user_model()
fails: list[str] = []
oks = 0
PREFIX = "verify-cofre"


def check(cond, msg):
    global oks
    if cond:
        oks += 1
        print("OK", msg)
    else:
        fails.append(msg)
        print("FAIL", msg)


def calc_fake(reserva=Decimal("80.00"), total=Decimal("100.00")):
    return {
        "ok": True,
        "percentual_lucro": 50.0,
        "percentual_padrao": 50.0,
        "reserva_vila": float(reserva),
        "reserva_vila_desde": RESERVA_VILA_DESDE_DEFAULT.isoformat(),
        "reserva_aplicada": float(reserva),
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
    tagged.filter(tipo="estorno").delete()
    tagged.delete()
    for dia in (date(2026, 8, 26), date(2026, 8, 27), timezone.localdate()):
        por_dia = RepasseVilaReservaMovimentoAgro.objects.filter(
            idempotencia_chave__startswith=f"reserva-vila:separacao:{dia.isoformat()}:"
        )
        RepasseVilaReservaMovimentoAgro.objects.filter(
            tipo="estorno", estornado_de__in=por_dia
        ).delete()
        por_dia.delete()
    reps.delete()
    MovimentoCaixa.objects.filter(pk__in=[x for x in mov_ids if x]).delete()
    MovimentoCaixa.objects.filter(observacao__contains="Reserva cofrinho Vila").delete()
    MovimentoCaixa.objects.filter(observacao__contains="Estorno reserva cofrinho").delete()


def main():
    cfg = obter_config()
    reserva_antes = _dec(cfg.reserva_vila)
    saldo_antes = _dec(getattr(cfg, "saldo_reserva_vila", 0))
    cleanup()
    cfg.reserva_vila = Decimal("80.00")
    cfg.saldo_reserva_vila = Decimal("0.00")
    cfg.reserva_vila_desde = RESERVA_VILA_DESDE_DEFAULT
    cfg.save(update_fields=["reserva_vila", "saldo_reserva_vila", "reserva_vila_desde"])

    user, _ = User.objects.get_or_create(username="verify_cofre_bot", defaults={"is_staff": True})
    sessao = SessaoCaixa.objects.create(ponto_caixa="vila", valor_abertura=Decimal("500"), usuario=user)
    dia = date(2026, 8, 26)
    cfg.reserva_vila_desde = dia
    cfg.save(update_fields=["reserva_vila_desde"])
    try:
        with patch("produtos.repasse_vila_util.calcular_disponivel", return_value=calc_fake()):
            mov, criado, err = separar_reserva_diaria(
                dia, origem="lancamento_separado", operador="Bot Cofre", usuario=user, sessao_caixa=sessao
            )
            check(not err and criado and _dec(mov.valor) == Decimal("80.00"), "separação isolada R$ 80")
            check(saldo_cofrinho_vila() == Decimal("80.00"), "saldo acumulado após separação")
            check(cu.resumo_esperado_por_forma(sessao)["Dinheiro"] == Decimal("420.00"), "separação sai do esperado normal")
            qtd_mov_caixa = MovimentoCaixa.objects.filter(sessao_caixa=sessao).count()
            mov2, criado2, err2 = separar_reserva_diaria(
                dia, origem="lancamento_separado", operador="Bot Cofre", usuario=user, sessao_caixa=sessao
            )
            check(not err2 and not criado2 and mov2.pk == mov.pk, "separação diária idempotente")
            check(MovimentoCaixa.objects.filter(sessao_caixa=sessao).count() == qtd_mov_caixa, "sem retirada duplicada no caixa")

        aj, criado, err = registrar_uso_ou_ajuste_cofrinho(
            tipo="ajuste", valor="20", observacao="Acerto contado", operador="Bot Cofre",
            usuario=user, data_ref=dia, idempotencia_chave=PREFIX + ":ajuste"
        )
        check(not err and criado and _dec(aj.saldo_posterior) == Decimal("100.00"), "ajuste positivo rastreado")
        _sem_obs, _criado_sem_obs, err_sem_obs = registrar_uso_ou_ajuste_cofrinho(
            tipo="retirada", valor="1", observacao="", operador="Bot Cofre",
            usuario=user, data_ref=dia, idempotencia_chave=PREFIX + ":sem-obs"
        )
        check("observação" in err_sem_obs, "retirada bloqueada sem motivo")
        _sem_op, _criado_sem_op, err_sem_op = registrar_uso_ou_ajuste_cofrinho(
            tipo="ajuste", valor="1", observacao="Acerto", operador="",
            usuario=user, data_ref=dia, idempotencia_chave=PREFIX + ":sem-op"
        )
        check("operador" in err_sem_op.lower(), "ajuste bloqueado sem operador")
        uso, criado, err = registrar_uso_ou_ajuste_cofrinho(
            tipo="retirada", valor="30", observacao="Compra urgente", operador="Bot Cofre",
            usuario=user, data_ref=dia, idempotencia_chave=PREFIX + ":uso"
        )
        check(not err and criado and _dec(uso.valor) == Decimal("-30.00"), "retirada/uso exige motivo e reduz saldo")
        uso2, criado2, err2 = registrar_uso_ou_ajuste_cofrinho(
            tipo="retirada", valor="30", observacao="Compra urgente", operador="Bot Cofre",
            usuario=user, data_ref=dia, idempotencia_chave=PREFIX + ":uso"
        )
        check(not err2 and not criado2 and uso2.pk == uso.pk, "retry de retirada idempotente")
        est, criado, err = estornar_movimento_cofrinho(
            uso.pk, observacao="Uso cancelado", operador="Bot Cofre", usuario=user
        )
        check(not err and criado and _dec(est.valor) == Decimal("30.00"), "estorno inverso rastreado")
        est2, criado2, err2 = estornar_movimento_cofrinho(
            uso.pk, observacao="Uso cancelado", operador="Bot Cofre", usuario=user
        )
        check(not err2 and not criado2 and est2.pk == est.pk, "estorno idempotente")
        check(all([uso.operador, uso.observacao, uso.data_ref, uso.saldo_anterior is not None, uso.saldo_posterior is not None]), "rastreabilidade mínima completa")

        # Fechamento: aviso/esperado virtual e persistência usam o mesmo valor, sem duplicar.
        hoje = timezone.localdate()
        MovimentoCaixa.objects.create(
            sessao_caixa=sessao,
            tipo=MovimentoCaixa.Tipo.REFORCO,
            forma_pagamento="Dinheiro",
            valor=Decimal("2000.00"),
            observacao=PREFIX + " reforco teste",
            usuario=user,
        )
        cfg.reserva_vila_desde = hoje
        cfg.save(update_fields=["reserva_vila_desde"])
        with patch("produtos.repasse_vila_util.calcular_disponivel", return_value=calc_fake(Decimal("60"), Decimal("0"))):
            estado = {"tot_esperado_dinheiro": "420.00", "linhas": [{"forma": "Dinheiro", "esperado": "420.00", "retiradas": "80.00"}], "cards": []}
            reserva = {"tem": True, "valor": "60.00", "saldo": str(saldo_cofrinho_vila()), "dias": [], "texto": ""}
            aplicar_reserva_virtual_estado_caixa(estado, reserva)
            check(estado["tot_esperado_dinheiro"] == "360.00", "fechamento antecipa desconto no esperado")
            feitos, err = separar_reservas_ao_fechar_vila([sessao], operador="Bot Cofre", usuario=user)
            check(not err and sum((_dec(x.valor) for x in feitos), Decimal("0")) == Decimal("60.00"), "fechamento separa valor exato")
            feitos2, err2 = separar_reservas_ao_fechar_vila([sessao], operador="Bot Cofre", usuario=user)
            check(not err2 and not feitos2, "fechamento repetido não separa duas vezes")

        # Acumulado: não separar ontem → hoje deve ontem+hoje; separar a mais / saldo inicial abate amanhã.
        ontem = hoje - timedelta(days=1)
        amanha = hoje + timedelta(days=1)
        RepasseVilaReservaMovimentoAgro.objects.filter(
            data_ref__gte=ontem, data_ref__lte=amanha
        ).delete()
        cfg.reserva_vila_desde = ontem
        cfg.saldo_reserva_vila = Decimal("0.00")
        cfg.save(update_fields=["reserva_vila_desde", "saldo_reserva_vila"])
        with patch("produtos.repasse_vila_util.calcular_disponivel", return_value=calc_fake(Decimal("100"), Decimal("0"))):
            pend_ontem = pendente_reserva_cofrinho_ate(ontem)
            check(pend_ontem["pendente"] == Decimal("100.00"), "ontem sem separação deve 100")
            pend_hoje = pendente_reserva_cofrinho_ate(hoje)
            check(pend_hoje["pendente"] == Decimal("200.00"), "hoje acumula ontem+hoje = 200")
            mov_extra, criado_extra, err_extra = separar_reserva_diaria(
                hoje, origem="lancamento_separado", operador="Bot Cofre", usuario=user, sessao_caixa=sessao
            )
            check(not err_extra and criado_extra and _dec(mov_extra.valor) == Decimal("200.00"), "separação leva acumulado 200")
            si, cri_si, err_si = registrar_saldo_inicial_cofrinho(
                valor=Decimal("50"), observacao="Saldo inicial teste", operador="Bot Cofre",
                usuario=user, idempotencia_chave=PREFIX + ":saldo-ini"
            )
            check(not err_si and cri_si and _dec(si.valor) == Decimal("50.00"), "saldo inicial sobe saldo físico")
            pend_am = pendente_reserva_cofrinho_ate(amanha)
            check(pend_am["pendente"] == Decimal("50.00"), "adiantar/separar a mais abate próximo dia")
            rsum = resumo_cofrinho_vila(amanha)
            check(rsum["pendente_dia"] == 50.0 and float(rsum.get("adiantado") or 0) == 0.0, "resumo expõe pendente acumulado")

        # Repasse limpo: sem pendente acumulado e gaveta reforçada.
        RepasseVilaReservaMovimentoAgro.objects.filter(
            data_ref__gte=ontem, data_ref__lte=amanha
        ).delete()
        cfg.reserva_vila_desde = date(2026, 8, 27)
        cfg.saldo_reserva_vila = Decimal("0.00")
        cfg.save(update_fields=["reserva_vila_desde", "saldo_reserva_vila"])
        MovimentoCaixa.objects.create(
            sessao_caixa=sessao,
            tipo=MovimentoCaixa.Tipo.REFORCO,
            forma_pagamento="Dinheiro",
            valor=Decimal("2000.00"),
            observacao=PREFIX + " reforco repasse",
            usuario=user,
        )

        # Repasse + separação na mesma transação; fórmula não subtrai a reserva do total outra vez.
        dia_rep = date(2026, 8, 27)
        rf = RequestFactory()
        req = rf.post("/api/repasse-vila/confirmar/")
        req.user = user
        SessionMiddleware(lambda r: None).process_request(req)
        req.session.save()
        cu.definir_ponto_operacao_browser(req, "vila", sessao.pk)
        from produtos.pdv_deposito_util import gravar_deposito_request
        gravar_deposito_request(req, "vila")
        req.session.save()
        with patch("produtos.repasse_vila_util.calcular_disponivel", return_value=calc_fake(Decimal("50"), Decimal("100"))), patch(
            "produtos.caixa_util.obter_caixa_vila_aberto", return_value=sessao
        ), patch("produtos.caixa_util.obter_caixa_gaveta_aberto", return_value=None):
            rep, err = confirmar_repasse(
                request=req, quem_levou="Bot Cofre", valor_manual=Decimal("100"),
                forma_pagamento="Dinheiro", operador="Bot Cofre", data_ref=dia_rep,
                separar_reserva=True,
            )
        check(not err and rep is not None and _dec(rep.valor_total) == Decimal("100.00"), f"repasse mantém total sem descontar reserva duas vezes · {err}")
        if rep:
            rep.observacao = PREFIX
            rep.save(update_fields=["observacao"])
            check(RepasseVilaReservaMovimentoAgro.objects.filter(repasse=rep, origem="repasse", valor=Decimal("50")).exists(), "separação junto ao repasse vinculada")

        # Backend impede transferência que usaria a reserva pendente.
        dia_bloq = timezone.localdate() - timedelta(days=1)
        with patch("produtos.repasse_vila_util.calcular_disponivel", return_value=calc_fake(Decimal("50"), Decimal("9999"))), patch(
            "produtos.caixa_util.obter_caixa_vila_aberto", return_value=sessao
        ), patch("produtos.caixa_util.obter_caixa_gaveta_aberto", return_value=None):
            rep_b, err_b = confirmar_repasse(
                request=req, quem_levou="Bot Cofre", valor_manual=Decimal("9999"),
                forma_pagamento="Dinheiro", operador="Bot Cofre", data_ref=dia_bloq,
                separar_reserva=True,
            )
        check(rep_b is None and "permanecer na Vila" in err_b, "backend bloqueia consumo do reservado")

        client = Client(HTTP_HOST="127.0.0.1")
        client.force_login(user)
        resp = client.get("/api/repasse-vila/cofrinho/")
        check(resp.status_code == 200 and "saldo" in resp.json(), "API compartilhada do cofrinho")
        tela = client.get("/repasse-vila/")
        body = tela.content.decode("utf-8", errors="replace")
        check("rv-cofrinho-card" in body and "rv-lucro-ficou" in body, "cofrinho e Lucro ficou na Vila são cards separados")
        check("18/08/2026" in (ROOT / "produtos/templates/produtos/includes/repasse_help_agents.html").read_text(encoding="utf-8"), "vigência documentada desde criação do campo")
    finally:
        cleanup()
        cfg = RepasseVilaConfigAgro.objects.get(pk=cfg.pk)
        cfg.reserva_vila = reserva_antes
        cfg.saldo_reserva_vila = saldo_antes
        # Sempre volta o desde do produto — o teste muda a data e não pode deixar o PG local sujo.
        cfg.reserva_vila_desde = RESERVA_VILA_DESDE_DEFAULT
        cfg.save(update_fields=["reserva_vila", "saldo_reserva_vila", "reserva_vila_desde"])
        SessaoCaixa.objects.filter(pk=sessao.pk).delete()

    print("---")
    print(f"oks={oks} fails={len(fails)}")
    for item in fails:
        print(item)
    if fails:
        return 1
    print("VERIFY_REPASSE_COFRINHO_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
