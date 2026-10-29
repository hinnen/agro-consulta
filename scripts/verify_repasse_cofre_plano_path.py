#!/usr/bin/env python
"""Prova detalhada — REPASSE-COFRE-PLANO (retirada com plano → DRE empresa Vila).

  python scripts/verify_repasse_cofre_plano_path.py

Contratos: fonte · PIN 9973 · Django (2 cofres + empresa Vila + estorno título) · UI.
VERIFY_REPASSE_COFRE_PLANO_PATH_OK N/N · VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ["DEBUG"] = "False"

PIN = "9973"
PREFIX = "verify-cofre-plano"
USER_BOT = "verify_cofre_plano_bot"

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


def prova_fonte() -> None:
    print("=== fonte ===")
    needle(
        "produtos/saida_caixa_planos.py",
        "listar_planos_cofre_vila",
        "resolver_plano_cofre_vila",
    )
    needle(
        "produtos/repasse_vila_util.py",
        "_gravar_despesa_retirada_cofre",
        "empresa_nome_saida_caixa",
        "plano_id",
        "titulo_ids",
        "_excluir_titulos_retirada_cofre",
        'empresa_nome_saida_caixa("vila")',
    )
    needle(
        "produtos/templates/produtos/repasse_vila.html",
        "rv-cofre-plano",
        "rv-cofre-ve-plano",
        "Plano de conta",
        "rv-planos-cofre-boot",
        "body.plano_id",
        "Escolha o plano de conta.",
        "rv-estorno-modal",
        "pedirMotivoEstorno",
        "Motivo do estorno",
    )
    needle(
        "produtos/views_repasse_vila.py",
        "listar_planos_cofre_vila",
        "planos_cofre",
        "plano_id",
    )
    # Nunca força Centro na retirada do cofre
    needle(
        "produtos/repasse_vila_util.py",
        'empresa_nome_saida_caixa("centro")',
        forbid=True,
    )


def _pick_plano(planos: list) -> dict:
    for p in planos:
        nome = str(p.get("plano") or "").casefold()
        if "aliment" in nome or "despesa" in nome or "compra" in nome:
            return p
    return planos[0]


def prova_django() -> None:
    print("=== Django ===")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.utils import timezone

    from produtos import caixa_util as cu
    from produtos.caixa_util import empresa_nome_saida_caixa
    from produtos.models import (
        MovimentoCaixa,
        RepasseVilaConfigAgro,
        RepasseVilaReservaMovimentoAgro,
        TituloFinanceiroAgro,
    )
    from produtos.repasse_vila_util import (
        COFRE_SALARIO,
        COFRE_VILA_ELIAS,
        estornar_movimento_cofrinho,
        obter_config,
        registrar_uso_ou_ajuste_cofrinho,
        saldo_cofrinho_vila,
    )
    from produtos.saida_caixa_planos import listar_planos_cofre_vila

    ok_pin, label, err = cu.operador_label_de_pin(PIN)
    check(ok_pin and bool(label) and not err, f"PIN {PIN} = {label}")

    emp = empresa_nome_saida_caixa("vila")
    emp_centro = empresa_nome_saida_caixa("centro")
    check("vila" in emp.lower() or "elias" in emp.lower(), f"empresa Vila = {emp}")
    check(emp != emp_centro, f"empresa Vila ≠ Centro ({emp} vs {emp_centro})")

    planos = listar_planos_cofre_vila()
    check(bool(planos) and all(p.get("plano") for p in planos), "planos cofre sem deposito vazio")
    check(all(not p.get("somente_caixa") for p in planos), "planos cofre sem somente_caixa")
    banidos_ids = {"deposito", "adiant_vale", "salario_folha"}
    check(
        all(str(p.get("id") or "") not in banidos_ids for p in planos),
        "planos cofre sem ids deposito/vale/folha",
    )
    plano = _pick_plano(planos)
    plano_id = str(plano.get("id") or "")
    plano_nome = str(plano.get("plano") or "")

    cfg = obter_config()
    sal_antes = saldo_cofrinho_vila(cfg, cofre=COFRE_SALARIO)
    ve_antes = saldo_cofrinho_vila(cfg, cofre=COFRE_VILA_ELIAS)

    # limpa restos
    qs = RepasseVilaReservaMovimentoAgro.objects.filter(
        observacao__contains=PREFIX
    ) | RepasseVilaReservaMovimentoAgro.objects.filter(
        idempotencia_chave__startswith=PREFIX
    )
    ids = list(qs.values_list("id", flat=True))
    if ids:
        RepasseVilaReservaMovimentoAgro.objects.filter(estornado_de_id__in=ids).delete()
        RepasseVilaReservaMovimentoAgro.objects.filter(id__in=ids).delete()

    User = get_user_model()
    user, _ = User.objects.get_or_create(
        username=USER_BOT, defaults={"is_staff": True, "is_superuser": True}
    )
    dia = timezone.localdate()
    op = f"Bot {PREFIX}"
    valor = Decimal("7.77")
    mov_caixa_antes = MovimentoCaixa.objects.count()

    # sem plano
    m0, c0, e0 = registrar_uso_ou_ajuste_cofrinho(
        tipo="retirada",
        valor="1",
        observacao="x",
        operador=op,
        data_ref=dia,
        idempotencia_chave=f"{PREFIX}-nopl",
        cofre=COFRE_SALARIO,
    )
    check(m0 is None and "plano" in (e0 or "").lower(), "API regra: sem plano = erro")

    # ajuste sem plano continua ok
    aj, ok_aj, err_aj = registrar_uso_ou_ajuste_cofrinho(
        tipo="ajuste",
        valor=valor,
        observacao=f"{PREFIX} in sal",
        operador=op,
        data_ref=dia,
        idempotencia_chave=f"{PREFIX}-in-sal",
        cofre=COFRE_SALARIO,
    )
    check(ok_aj and aj and not err_aj, "ajuste salário sem plano ok")
    det_aj = aj.detalhe if isinstance(aj.detalhe, dict) else {}
    check(not det_aj.get("titulo_ids"), "ajuste não cria título financeiro")

    mov_s, ok_s, err_s = registrar_uso_ou_ajuste_cofrinho(
        tipo="retirada",
        valor=valor,
        observacao=f"{PREFIX} out sal",
        operador=op,
        data_ref=dia,
        idempotencia_chave=f"{PREFIX}-out-sal",
        cofre=COFRE_SALARIO,
        plano_id=plano_id,
    )
    check(ok_s and mov_s and not err_s, "retirada salário com plano")
    det_s = mov_s.detalhe if isinstance(mov_s.detalhe, dict) else {}
    check(det_s.get("plano_nome") == plano_nome, "plano_nome no detalhe salário")
    check(det_s.get("empresa_nome") == emp, "empresa_nome Vila no detalhe salário")
    check(det_s.get("empresa") == "vila", "flag empresa=vila no detalhe")
    tids = list(det_s.get("titulo_ids") or [])
    check(bool(tids), f"titulo_ids gravados salário ({tids})")

    # título real no PG (DRE/Lançamentos)
    tit_s = TituloFinanceiroAgro.objects.filter(mongo_id=tids[0], despesa=True).first()
    check(tit_s is not None, "título PG existe após retirada salário")
    if tit_s:
        check(tit_s.empresa == emp, f"título empresa Vila ({tit_s.empresa})")
        check(tit_s.empresa != emp_centro, "título não é empresa Centro")
        check(tit_s.plano_conta == plano_nome, f"título plano = {tit_s.plano_conta}")
        check(Decimal(str(tit_s.valor_bruto)) == valor, f"título valor = {tit_s.valor_bruto}")
        check(bool(tit_s.quitado), "título quitado (despesa paga)")
        check("Cofre Salário" in (tit_s.descricao or ""), f"desc título salário ({tit_s.descricao})")

    # entrada + retirada VE
    registrar_uso_ou_ajuste_cofrinho(
        tipo="ajuste",
        valor=valor,
        observacao=f"{PREFIX} in ve",
        operador=op,
        data_ref=dia,
        idempotencia_chave=f"{PREFIX}-in-ve",
        cofre=COFRE_VILA_ELIAS,
    )
    mov_v, ok_v, err_v = registrar_uso_ou_ajuste_cofrinho(
        tipo="retirada",
        valor=valor,
        observacao=f"{PREFIX} out ve",
        operador=op,
        data_ref=dia,
        idempotencia_chave=f"{PREFIX}-out-ve",
        cofre=COFRE_VILA_ELIAS,
        plano_id=plano_id,
    )
    check(ok_v and mov_v and not err_v, "retirada VE com plano")
    det_v = mov_v.detalhe if isinstance(mov_v.detalhe, dict) else {}
    check(det_v.get("empresa_nome") == emp, "empresa Vila no detalhe VE")
    tids_v = list(det_v.get("titulo_ids") or [])
    check(bool(tids_v), "titulo_ids VE")
    tit_v = TituloFinanceiroAgro.objects.filter(mongo_id=tids_v[0], despesa=True).first()
    check(tit_v is not None and tit_v.empresa == emp, "título PG VE empresa Vila")
    if tit_v:
        check("Cofre Vila Elias" in (tit_v.descricao or ""), "desc título VE")

    check(
        MovimentoCaixa.objects.count() == mov_caixa_antes,
        "retirada cofre não cria MovimentoCaixa na gaveta",
    )

    # retry idempotente não duplica título
    mov_s2, ok_s2, err_s2 = registrar_uso_ou_ajuste_cofrinho(
        tipo="retirada",
        valor=valor,
        observacao=f"{PREFIX} out sal",
        operador=op,
        data_ref=dia,
        idempotencia_chave=f"{PREFIX}-out-sal",
        cofre=COFRE_SALARIO,
        plano_id=plano_id,
    )
    check(mov_s2 and mov_s2.pk == mov_s.pk and not err_s2, "retry retirada idempotente")
    check(ok_s2 is False, "retry não cria 2º movimento (criado=False)")
    det_s2 = mov_s2.detalhe if isinstance(mov_s2.detalhe, dict) else {}
    check(list(det_s2.get("titulo_ids") or []) == tids, "retry não duplica titulo_ids")

    # estorno remove título (best-effort) + devolve saldo
    sal_pre_est = saldo_cofrinho_vila(cofre=COFRE_SALARIO)
    est, ok_e, err_e = estornar_movimento_cofrinho(
        mov_s.pk, observacao=f"{PREFIX} estorno sal", operador=op, usuario=user
    )
    check(ok_e and est and not err_e, "estorno salário ok")
    check(
        saldo_cofrinho_vila(cofre=COFRE_SALARIO) == sal_pre_est + valor,
        "estorno devolve saldo salário",
    )
    check(
        not TituloFinanceiroAgro.objects.filter(mongo_id=tids[0]).exists(),
        "estorno apagou título salário no PG",
    )
    est_det = est.detalhe if isinstance(est.detalhe, dict) else {}
    check(
        est_det.get("fin_excluiu_ok") is True and not est_det.get("fin_excluiu_erro"),
        f"estorno fin ok ({est_det.get('fin_excluiu_erro')})",
    )

    est_v, ok_ev, err_ev = estornar_movimento_cofrinho(
        mov_v.pk, observacao=f"{PREFIX} estorno ve", operador=op, usuario=user
    )
    check(ok_ev and est_v and not err_ev, "estorno VE ok")
    check(
        not TituloFinanceiroAgro.objects.filter(mongo_id=tids_v[0]).exists(),
        "estorno apagou título VE no PG",
    )

    # API HTTP com PIN 9973
    client = Client(HTTP_HOST="127.0.0.1")
    client.force_login(user)
    page = client.get("/repasse-vila/")
    body = page.content.decode("utf-8", errors="replace")
    check(page.status_code == 200, "GET gestão 200")
    check("rv-cofre-plano" in body and "rv-cofre-ve-plano" in body, "selects plano na UI")
    check("rv-planos-cofre-boot" in body, "boot JSON planos")
    check("Plano de conta" in body, "rótulo Plano de conta na UI")

    import json

    # API sem plano
    r_no = client.post(
        "/api/repasse-vila/cofrinho/movimento/",
        data=json.dumps(
            {
                "tipo": "retirada",
                "valor": "1.00",
                "observacao": f"{PREFIX} api nopl",
                "cofre": "salario",
                "pin": PIN,
                "idempotencia_chave": f"{PREFIX}-api-nopl",
            }
        ),
        content_type="application/json",
    )
    j_no = r_no.json()
    check(
        r_no.status_code == 400 and not j_no.get("ok") and "plano" in str(j_no.get("erro") or "").lower(),
        "API HTTP sem plano = 400",
    )

    # API com PIN + plano (salário)
    registrar_uso_ou_ajuste_cofrinho(
        tipo="ajuste",
        valor=Decimal("3.33"),
        observacao=f"{PREFIX} api in",
        operador=op,
        data_ref=dia,
        idempotencia_chave=f"{PREFIX}-api-in",
        cofre=COFRE_SALARIO,
    )
    r_ok = client.post(
        "/api/repasse-vila/cofrinho/movimento/",
        data=json.dumps(
            {
                "tipo": "retirada",
                "valor": "3.33",
                "observacao": f"{PREFIX} api out",
                "cofre": "salario",
                "plano_id": plano_id,
                "pin": PIN,
                "idempotencia_chave": f"{PREFIX}-api-out",
            }
        ),
        content_type="application/json",
    )
    j_ok = r_ok.json()
    check(r_ok.status_code == 200 and j_ok.get("ok"), f"API HTTP retirada+PIN ({j_ok.get('erro')})")
    mid_api = j_ok.get("movimento_id")
    m_api = (
        RepasseVilaReservaMovimentoAgro.objects.filter(pk=mid_api).first()
        if mid_api
        else RepasseVilaReservaMovimentoAgro.objects.filter(
            idempotencia_chave=f"{PREFIX}-api-out"
        )
        .order_by("-id")
        .first()
    )
    if m_api and not mid_api:
        mid_api = m_api.pk
    det_api = m_api.detalhe if m_api and isinstance(m_api.detalhe, dict) else {}
    tids_api = list(det_api.get("titulo_ids") or [])
    check(bool(tids_api), "API gravou titulo_ids")
    if mid_api:
        estornar_movimento_cofrinho(
            int(mid_api), observacao=f"{PREFIX} estorno api", operador=op, usuario=user
        )

    # restore saldos
    cfg2 = RepasseVilaConfigAgro.objects.get(pk=cfg.pk)
    cfg2.saldo_reserva_vila = sal_antes
    cfg2.saldo_cofre_vila_elias = ve_antes
    cfg2.save(update_fields=["saldo_reserva_vila", "saldo_cofre_vila_elias"])
    qs2 = RepasseVilaReservaMovimentoAgro.objects.filter(
        observacao__contains=PREFIX
    ) | RepasseVilaReservaMovimentoAgro.objects.filter(
        idempotencia_chave__startswith=PREFIX
    )
    ids2 = list(qs2.values_list("id", flat=True))
    if ids2:
        RepasseVilaReservaMovimentoAgro.objects.filter(estornado_de_id__in=ids2).delete()
        RepasseVilaReservaMovimentoAgro.objects.filter(id__in=ids2).delete()
    # limpa títulos órfãos do prefixo (se algum restou)
    TituloFinanceiroAgro.objects.filter(descricao__icontains=PREFIX).delete()
    check(
        saldo_cofrinho_vila(cofre=COFRE_SALARIO) == sal_antes
        and saldo_cofrinho_vila(cofre=COFRE_VILA_ELIAS) == ve_antes,
        "saldos restaurados",
    )


def main() -> int:
    prova_fonte()
    prova_django()
    print("---")
    total = oks + len(fails)
    print(f"oks={oks} fails={len(fails)} total={total}")
    for item in fails:
        print("FAIL_ITEM", item)
    if fails:
        print("VERIFY_FAIL")
        return 1
    print(f"VERIFY_REPASSE_COFRE_PLANO_PATH_OK {oks}/{oks}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
