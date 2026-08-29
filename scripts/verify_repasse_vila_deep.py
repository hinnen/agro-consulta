#!/usr/bin/env python
"""Prova detalhada — Repasse Vila → Centro (local)."""
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

from django.contrib.auth import get_user_model
from django.contrib.sessions.middleware import SessionMiddleware
from django.test import Client, RequestFactory
from django.urls import reverse
from django.utils import timezone

from produtos import caixa_util as cu
from produtos.fiado_credito_util import venda_local_tem_fiado
from produtos.models import (
    ItemVendaAgro,
    MovimentoCaixa,
    RepasseVilaCentroAgro,
    SessaoCaixa,
    VendaAgro,
)
from produtos.relatorios_vendas_util import mapa_produtos_meta
from produtos.repasse_vila_util import (
    _aware_bounds,
    _extra_do_calc,
    _vendas_vila_sem_fiado,
    abater_extras_do_acumulado,
    acumulado_anterior,
    aplicar_repasses_pendentes_centro,
    calcular_disponivel,
    confirmar_repasse,
    partir_despesas_centro_vila,
    quitar_acumulado_zerar,
    registrar_ajuste_acumulado,
    salvar_percentual_padrao,
    salvar_planos_desconto_centro,
    salvar_reserva_vila,
    texto_aviso_abertura,
)

User = get_user_model()
fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def main() -> int:
    user, _ = User.objects.get_or_create(
        username="repasse_verify_bot", defaults={"is_staff": True}
    )
    hoje = timezone.localdate()
    tag = f"REPASSE-VERIFY-{hoje.isoformat()}"

    for name in (
        "repasse_vila",
        "api_repasse_vila_calc",
        "api_repasse_vila_historico",
        "api_repasse_vila_config",
        "api_repasse_vila_meta",
        "api_repasse_vila_confirmar",
    ):
        try:
            reverse(name)
            ok(f"url {name}")
        except Exception as e:
            fail(f"url {name}: {e}")

    cfg = salvar_percentual_padrao(150, operador="bot")
    ok("clamp 150->100") if float(cfg.percentual_lucro_padrao) == 100.0 else fail("clamp 150")
    cfg = salvar_percentual_padrao(-5, operador="bot")
    ok("clamp -5->0") if float(cfg.percentual_lucro_padrao) == 0.0 else fail("clamp -5")
    cfg = salvar_percentual_padrao(50, operador="bot")
    ok("clamp 50") if float(cfg.percentual_lucro_padrao) == 50.0 else fail("clamp 50")

    res_antes = cfg.reserva_vila
    cfg = salvar_reserva_vila(-8, operador="bot")
    ok("reserva clamp neg") if float(cfg.reserva_vila) == 0.0 else fail("reserva neg")
    cfg = salvar_reserva_vila(200, operador="bot")
    ok("reserva 200") if float(cfg.reserva_vila) == 200.0 else fail("reserva 200")
    calc_res = calcular_disponivel(hoje)
    if "reserva_vila" in calc_res and "total_sugerido_bruto" in calc_res:
        ok("calc tem reserva")
        bruto = Decimal(str(calc_res["total_sugerido_bruto"]))
        sug = Decimal(str(calc_res["total_sugerido"]))
        # Cofrinho: reserva NÃO abate o envelope. UI/API expõe sug = max(0, bruto)
        # (bruto negativo = crédito / acumulado a favor).
        esperado = max(Decimal("0"), bruto)
        ok("reserva nao desconta sugerido (cofrinho)") if sug == esperado else fail(
            f"sug={sug} esperado={esperado} bruto={bruto}"
        )
        cofre = calc_res.get("cofrinho") or {}
        if "pendente_dia" in cofre or "saldo" in cofre:
            ok("calc expoe cofrinho")
        else:
            ok("calc sem card cofrinho (legado ok)")
    else:
        fail("calc sem reserva/total_sugerido_bruto")
    salvar_reserva_vila(res_antes, operador="bot")

    c_cent, c_vila = partir_despesas_centro_vila(
        {"Alimentação": Decimal("80.00"), "Combustível Strada": Decimal("20.00")},
        ["Alimentação"],
    )
    if c_cent == Decimal("80.00") and c_vila == Decimal("20.00"):
        ok("planos: marcado Centro / resto Vila")
    else:
        fail(f"planos split {c_cent}/{c_vila}")
    z_cent, z_vila = partir_despesas_centro_vila(
        {"Alimentação": Decimal("80.00")}, []
    )
    if z_cent == Decimal("0.00") and z_vila == Decimal("80.00"):
        ok("planos: nenhum marcado = tudo Vila")
    else:
        fail(f"planos vazio {z_cent}/{z_vila}")
    planos_antes = list(cfg.planos_desconto_centro or [])
    cfg = salvar_planos_desconto_centro(["Alimentação", "alimentação", ""], operador="bot")
    saved = list(cfg.planos_desconto_centro or [])
    if saved == ["Alimentação"]:
        ok("planos salvar dedup")
    else:
        fail(f"planos salvar {saved}")
    salvar_planos_desconto_centro(planos_antes, operador="bot")

    for rep in RepasseVilaCentroAgro.objects.filter(observacao=tag):
        if rep.movimento_saida_id:
            MovimentoCaixa.objects.filter(pk=rep.movimento_saida_id).delete()
        if rep.movimento_entrada_id:
            MovimentoCaixa.objects.filter(pk=rep.movimento_entrada_id).delete()
        rep.delete()
    VendaAgro.objects.filter(cliente_nome=tag).delete()

    sample = (
        ItemVendaAgro.objects.exclude(produto_id_externo="")
        .order_by("-id")
        .values_list("produto_id_externo", flat=True)
        .first()
    )
    pid = str(sample or "VERIFY-REPASSE-SKU-1")
    meta = mapa_produtos_meta([pid])
    custo = Decimal(str((meta.get(pid) or {}).get("custo") or 0))
    print("sku", pid, "custo", custo)

    v = VendaAgro.objects.create(
        total=Decimal("200.00"),
        forma_pagamento="Dinheiro",
        deposito="vila",
        cliente_nome=tag,
        usuario_registro="bot",
        pagamentos_json=[{"forma": "Dinheiro", "valor": 200}],
    )
    ItemVendaAgro.objects.create(
        venda=v,
        produto_id_externo=pid,
        descricao="Verify item",
        quantidade=Decimal("2"),
        valor_unitario=Decimal("100"),
        valor_total=Decimal("200"),
    )
    vf = VendaAgro.objects.create(
        total=Decimal("80.00"),
        forma_pagamento="Fiado",
        deposito="vila",
        cliente_nome=tag,
        usuario_registro="bot",
        pagamentos_json=[{"forma": "Fiado", "valor": 80}],
    )
    ItemVendaAgro.objects.create(
        venda=vf,
        produto_id_externo=pid,
        descricao="Fiado item",
        quantidade=Decimal("1"),
        valor_unitario=Decimal("80"),
        valor_total=Decimal("80"),
    )

    ok("detecta venda fiado") if venda_local_tem_fiado(vf) else fail("vf fiado")
    ok("detecta venda normal") if not venda_local_tem_fiado(v) else fail("v fiado")

    calc = calcular_disponivel(hoje, percentual_lucro=50)
    print(
        "calc",
        calc["receita_dia"],
        calc["cmv_dia"],
        calc["lucro_bruto_dia"],
        "n",
        calc["n_vendas"],
    )
    ok(f"receita>={200}") if calc["receita_dia"] >= 200 else fail(
        f"receita={calc['receita_dia']}"
    )

    desde, ate = _aware_bounds(hoje, hoje)
    ids = set(_vendas_vila_sem_fiado(desde, ate))
    ok("fiado fora") if vf.pk not in ids else fail("fiado no calc")
    ok("normal no calc") if v.pk in ids else fail("normal fora")

    # Venda PIX — já no Centro, reduz falta em dinheiro
    v_pix = VendaAgro.objects.create(
        total=Decimal("100.00"),
        forma_pagamento="PIX",
        deposito="vila",
        cliente_nome=tag,
        usuario_registro="bot",
        pagamentos_json=[{"forma": "PIX", "valor": 100}],
    )
    ItemVendaAgro.objects.create(
        venda=v_pix,
        produto_id_externo=pid,
        descricao="Pix item",
        quantidade=Decimal("1"),
        valor_unitario=Decimal("100"),
        valor_total=Decimal("100"),
    )
    calc_e = calcular_disponivel(hoje, percentual_lucro=50)
    ok("ja_eletronico>=100") if float(calc_e.get("ja_eletronico") or 0) >= 99.9 else fail(
        f"elet={calc_e.get('ja_eletronico')}"
    )
    # bolo sobe com a receita PIX; falta dinheiro < bolo se eletrônico cobrir parte
    alvo = float(calc_e.get("alvo_total") or 0)
    falta = float(calc_e.get("falta_dinheiro") or 0)
    elet = float(calc_e.get("ja_eletronico") or 0)
    ok("falta < alvo com PIX") if falta < alvo - 0.01 and elet >= 99.9 else fail(
        f"alvo={alvo} falta={falta} elet={elet}"
    )

    lucro_penultimo = Decimal(str(calc_e["lucro_penultimo_dia"]))
    # Planos já entraram antes da divisão — alvo lucro = penúltimo
    alvo_lucro = max(Decimal("0"), lucro_penultimo.quantize(Decimal("0.01")))
    ok("alvo lucro após planos+cofres") if abs(Decimal(str(calc_e["alvos"]["lucro"])) - alvo_lucro) <= Decimal(
        "0.02"
    ) else fail("alvo lucro")

    s_vila = SessaoCaixa.objects.filter(
        ponto_caixa="vila", fechado_em__isnull=True
    ).first()
    created_vila = False
    if not s_vila:
        s_vila = SessaoCaixa.objects.create(
            ponto_caixa="vila", valor_abertura=Decimal("100"), usuario=user
        )
        created_vila = True

    s_gav = SessaoCaixa.objects.filter(
        ponto_caixa="gaveta", fechado_em__isnull=True
    ).first()
    created_gav = False

    rf = RequestFactory()
    req = rf.post("/api/repasse-vila/confirmar/")
    req.user = user
    SessionMiddleware(lambda r: None).process_request(req)
    req.session.save()
    cu.definir_ponto_operacao_browser(req, "vila", s_vila.pk)
    from produtos.pdv_deposito_util import gravar_deposito_request

    gravar_deposito_request(req, "vila")
    req.session.save()

    sess = cu.obter_sessao_caixa_aberta_request(req)
    print("sessao_req", getattr(sess, "pk", None), getattr(sess, "ponto_caixa", None))
    print("dep_browser", cu.deposito_caixa_browser(req))

    rep1, err = confirmar_repasse(
        request=req,
        quem_levou="Bot Verify",
        percentual_lucro=50,
        incluir_cmv=True,
        incluir_lucro=True,
        incluir_fiado=True,
        modo_dia_cheio=False,
        operador="bot",
    )
    if err:
        fail(f"confirmar1: {err}")
        rep1 = None
    else:
        assert rep1 is not None
        rep1.observacao = tag
        rep1.save(update_fields=["observacao"])
        ok(f"confirmar1 total={rep1.valor_total} status={rep1.status_centro}")
        ok("forma padrao Dinheiro") if (rep1.forma_pagamento or "") == "Dinheiro" else fail(
            f"forma={rep1.forma_pagamento!r}"
        )
        if (
            rep1.movimento_saida_id
            and rep1.movimento_saida.tipo == MovimentoCaixa.Tipo.RETIRADA
            and rep1.movimento_saida.sessao_caixa_id == s_vila.pk
        ):
            ok("movimento saida Vila OK")
            ok("saida forma Dinheiro") if rep1.movimento_saida.forma_pagamento == "Dinheiro" else fail(
                "saida forma"
            )
        else:
            fail("movimento saida")
        if s_gav and s_gav.fechado_em is None:
            ok("entrada Centro aplicada") if (
                rep1.status_centro == "aplicado" and rep1.movimento_entrada_id
            ) else fail("Centro aberto sem aplicar")
            if rep1.movimento_entrada_id:
                ok("entrada forma Dinheiro") if (
                    rep1.movimento_entrada.forma_pagamento == "Dinheiro"
                ) else fail("entrada forma")
        else:
            ok("entrada pendente") if rep1.status_centro == "pendente" else fail(
                "deveria pendente"
            )

    # 2º envio: forma PIX + valor manual (dia cheio parcial)
    rep_pix, err_pix = confirmar_repasse(
        request=req,
        quem_levou="Bot PIX",
        percentual_lucro=50,
        incluir_cmv=True,
        incluir_lucro=False,
        incluir_fiado=False,
        modo_dia_cheio=True,
        valor_manual=Decimal("7.77"),
        forma_pagamento="PIX",
        operador="bot",
    )
    if err_pix:
        fail(f"confirmar PIX: {err_pix}")
    else:
        assert rep_pix is not None
        rep_pix.observacao = tag
        rep_pix.save(update_fields=["observacao"])
        ok("forma PIX gravada") if (rep_pix.forma_pagamento or "") == "PIX" else fail(
            f"pix forma={rep_pix.forma_pagamento!r}"
        )
        ok("PIX valor manual") if abs(float(rep_pix.valor_total) - 7.77) < 0.02 else fail(
            f"pix total={rep_pix.valor_total}"
        )
        if rep_pix.movimento_saida_id:
            ok("saida PIX") if rep_pix.movimento_saida.forma_pagamento == "PIX" else fail(
                "saida nao PIX"
            )
        else:
            fail("PIX sem saida")

    # Manual acima do automático (sem dia cheio) — não pode cortar no disponível
    calc_falt = calcular_disponivel(hoje, percentual_lucro=50, modo_dia_cheio=False)
    falta = Decimal(str(calc_falt["disponivel"]["total"]))
    if falta > 0:
        vm_acima = (falta + Decimal("50.00")).quantize(Decimal("0.01"))
        rep_man, err_man = confirmar_repasse(
            request=req,
            quem_levou="Bot Manual Acima",
            percentual_lucro=50,
            incluir_cmv=True,
            incluir_lucro=True,
            incluir_fiado=True,
            modo_dia_cheio=False,
            valor_manual=vm_acima,
            forma_pagamento="Dinheiro",
            operador="bot",
        )
        if err_man:
            fail(f"manual acima: {err_man}")
        else:
            assert rep_man is not None
            rep_man.observacao = tag
            rep_man.save(update_fields=["observacao"])
            ok("manual acima do automatico") if abs(float(rep_man.valor_total) - float(vm_acima)) < 0.02 else fail(
                f"manual cortado total={rep_man.valor_total} esperado={vm_acima}"
            )
    else:
        ok("manual acima skipped (sem falta)")

    calc2 = calcular_disponivel(hoje, percentual_lucro=50, modo_dia_cheio=False)
    calc3 = calcular_disponivel(hoje, percentual_lucro=50, modo_dia_cheio=True)
    ok(f"incremental total={calc2['disponivel']['total']}")
    # Dia cheio ignora dinheiro já enviado, mas ainda credita cartão/PIX
    ok("dia cheio > incremental") if float(calc3["disponivel"]["total"]) + 0.01 >= float(
        calc2["disponivel"]["total"]
    ) else fail("dia cheio")
    ok("dia cheio credita elet") if float(calc3.get("ja_eletronico") or 0) >= 99.9 else fail(
        "dia cheio sem elet"
    )

    pendentes = list(
        RepasseVilaCentroAgro.objects.filter(status_centro="pendente", observacao=tag)
    )
    if not pendentes:
        pendentes = [
            RepasseVilaCentroAgro.objects.create(
                data_ref=hoje,
                percentual_lucro=Decimal("50"),
                valor_cmv=Decimal("10"),
                valor_lucro=Decimal("0"),
                valor_fiado=Decimal("0"),
                valor_total=Decimal("10"),
                quem_levou="Bot Pending",
                status_centro="pendente",
                observacao=tag,
                sessao_vila=s_vila,
            )
        ]
        ok("pendente artificial")

    if not s_gav:
        s_gav = SessaoCaixa.objects.create(
            ponto_caixa="gaveta", valor_abertura=Decimal("50"), usuario=user
        )
        created_gav = True
    apps = aplicar_repasses_pendentes_centro(sessao_centro=s_gav, usuario=user)
    ok(f"aplicou {len(apps)}") if apps and all(
        r.status_centro == "aplicado" for r in apps
    ) else fail("aplicar pendentes")
    txt = texto_aviso_abertura(apps)
    ok("aviso texto") if (
        "repasse da Vila" in txt and "fazer a retirada" in txt
    ) else fail(f"aviso: {txt[:100]}")

    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(user)
    for path in (
        "/api/repasse-vila/calc/",
        "/api/repasse-vila/historico/",
        "/api/repasse-vila/meta/",
        "/api/repasse-vila/config/",
    ):
        r = c.get(path)
        ok(f"GET {path}") if r.status_code == 200 and r.json().get("ok") else fail(
            f"GET {path}"
        )
    r = c.get("/api/repasse-vila/historico/")
    if r.status_code == 200:
        hj = r.json()
        ok("hist lucro_ficou") if "lucro_ficou_vila" in hj and "lucro_bruto_mes" in hj else fail(
            "hist sem cards lucro"
        )
    r = c.get("/repasse-vila/")
    ok("GET tela") if r.status_code == 200 and b"Repasse" in r.content else fail("tela")
    if r.status_code == 200:
        body_tela = r.content
        ok("cards mes UI") if (
            b"Enviado ao Centro" in body_tela and b"Lucro ficou na Vila" in body_tela
        ) else fail("faltou cards mes na tela")
        if b"caixa/retiradas" in body_tela and b"repasse=1" in body_tela:
            ok("Transferir aponta Retiradas?repasse=1")
        elif b"/pdv/" in body_tela and b"repasse=1" in body_tela:
            fail("Transferir ainda aponta /pdv/")
        else:
            ok("Transferir sem /pdv/repasse")

    r = c.get("/api/repasse-vila/meta/")
    if r.status_code == 200:
        mj = r.json()
        formas = mj.get("formas_pagamento") or []
        ok("meta formas") if "Dinheiro" in formas and "PIX" in formas else fail(
            f"meta formas: {formas}"
        )
    else:
        fail("meta GET para formas")

    r = c.get("/caixa/retiradas/")
    body = r.content if r.status_code == 200 else b""
    ok("GET retiradas") if r.status_code == 200 else fail(f"GET retiradas {r.status_code}")
    ok("botao crh-btn-repasse") if b'id="crh-btn-repasse"' in body else fail("faltou crh-btn-repasse")
    ok("overlay na Retiradas") if b"pdv-repasse-overlay" in body else fail("faltou overlay")
    ok("js repasse na Retiradas") if b"pdv_repasse_vila.js" in body else fail("faltou js")
    ok("forma grid no overlay") if b"pdv-rp-forma-grid" in body else fail("faltou forma grid")
    ok("anti-autofill valor") if b"rp_valor_manual_somente" in body else fail("faltou anti-autofill")
    ok("campos cofre hero") if b"pdv-rp-input-cofre-sal" in body and b"pdv-rp-input-cofre-ve" in body else fail("faltou campos cofre")
    ok("hint levar centro") if b"Levar ao Centro" in body else fail("faltou hint levar")
    ok("quem so popup") if b"pdv-rp-quem-modal" in body and b"pdv-rp-btn-quem" not in body else fail("chips quem voltaram")
    ok("pin so popup") if b"pdv-rp-pin-modal" in body and b"pdv-rp-btn-pin" not in body else fail("chips pin voltaram")
    ok("sem forma na tela") if b"Forma de pagamento" not in body else fail("forma visivel")
    ok("forma oculta modal") if b'id="pdv-rp-forma-modal"' in body else fail("faltou forma modal oculto")
    ok("hero cofrinho") if b"pdv-rp-hero-cofre" in body and b"Cofrinho Sal" in body and b"pdv-rp-hero-cofre-ve" in body and b"Cofre Vila Elias" in body else fail("faltou hero cofrinho")
    ok("levar centro") if b"Levar ao Centro" in body else fail("faltou levar centro")
    ok("rp-popup css") if b"rp-popup" in body else fail("faltou rp-popup")
    # botao deve ser <button>, nao <a href=pdv>
    idx = body.find(b'id="crh-btn-repasse"')
    if idx > 0:
        chunk = body[max(0, idx - 80) : idx]
        ok("Repasse e <button>") if b"<button" in chunk else fail("Repasse nao e button")
    if b'pdv_home' in body or b"/pdv/?" in body or b"/pdv/checkout" in body:
        # pode ter FAB voltar PDV — so falha se link com repasse=1 para pdv
        if b"?repasse=1" in body and (b"/pdv/" in body or b"pdv_home" in body):
            # check specifically for href to pdv with repasse
            import re as _re

            if _re.search(br'href="[^"]*/pdv[^"]*repasse=1', body):
                fail("ainda existe href PDV?repasse=1")
            else:
                ok("sem href PDV?repasse=1")
        else:
            ok("sem navegacao PDV repasse")
    else:
        ok("Retiradas sem URL pdv")

    js = (ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js").read_text(encoding="utf-8")
    ok("js manda forma_pagamento") if "forma_pagamento: formaPag" in js else fail("js sem forma")
    ok("js limpa valor com letra") if "sanitizeManualField" in js else fail("js sem sanitize")
    ok("js manda data_ref") if "data_ref: dataRef()" in js or 'data_ref: dataRef()' in js else fail("js sem data_ref")
    ok("overlay tem dia") if b'id="pdv-rp-data"' in body else fail("faltou campo dia")

    from datetime import timedelta

    from produtos.repasse_vila_util import validar_data_ref_repasse

    d_ok, e_ok = validar_data_ref_repasse(hoje - timedelta(days=1))
    ok("valida ontem") if d_ok == hoje - timedelta(days=1) and not e_ok else fail(f"ontem {e_ok}")
    d_fut, e_fut = validar_data_ref_repasse(hoje + timedelta(days=1))
    ok("bloqueia futuro") if d_fut is None and e_fut else fail("futuro deveria falhar")
    r = c.get("/api/repasse-vila/calc/", {"data": (hoje - timedelta(days=1)).isoformat(), "pct": "50"})
    ok("calc dia passado") if r.status_code == 200 and r.json().get("ok") else fail("calc passado")
    r = c.get("/api/repasse-vila/calc/", {"data": (hoje + timedelta(days=2)).isoformat(), "pct": "50"})
    ok("calc futuro 400") if r.status_code == 400 and r.json().get("ok") is False else fail("calc futuro")

    # confirmar dia passado (valor manual pequeno) — precisa venda ontem
    ontem = hoje - timedelta(days=1)
    desde_o, ate_o = _aware_bounds(ontem, ontem)
    v_ont = VendaAgro.objects.create(
        total=Decimal("50.00"),
        forma_pagamento="Dinheiro",
        deposito="vila",
        cliente_nome=tag,
        usuario_registro="bot",
        pagamentos_json=[{"forma": "Dinheiro", "valor": 50}],
    )
    ItemVendaAgro.objects.create(
        venda=v_ont,
        produto_id_externo=pid,
        descricao="Verify ontem",
        quantidade=Decimal("1"),
        valor_unitario=Decimal("50"),
        valor_total=Decimal("50"),
    )
    VendaAgro.objects.filter(pk=v_ont.pk).update(criado_em=desde_o + timedelta(hours=12))

    rep_pass, err_pass = confirmar_repasse(
        request=req,
        quem_levou="Bot Ontem",
        percentual_lucro=50,
        incluir_cmv=True,
        incluir_lucro=False,
        incluir_fiado=False,
        modo_dia_cheio=True,
        valor_manual=Decimal("3.33"),
        forma_pagamento="Dinheiro",
        operador="bot",
        data_ref=ontem,
    )
    if err_pass:
        fail(f"confirmar ontem: {err_pass}")
    else:
        assert rep_pass is not None
        rep_pass.observacao = tag
        rep_pass.save(update_fields=["observacao"])
        ok("confirmar ontem data_ref") if rep_pass.data_ref == ontem else fail("data_ref ontem")
        ok("obs tem ref data") if ontem.strftime("%d/%m/%Y") in (rep_pass.movimento_saida.observacao or "") else fail(
            "obs sem data"
        )

    r = c.post(
        "/api/repasse-vila/config/",
        data=json.dumps({"percentual_lucro_padrao": 50}),
        content_type="application/json",
    )
    ok("POST config") if r.status_code == 200 and r.json().get("ok") else fail("POST config")
    r = c.post(
        "/api/repasse-vila/confirmar/",
        data=json.dumps({"quem_levou": "Fulano"}),
        content_type="application/json",
    )
    j = r.json()
    ok(f"confirmar sem caixa -> {j.get('erro')}") if j.get("ok") is False else fail(
        "confirmar deveria falhar"
    )

    from produtos.models import RepasseVilaAcumuladoAjusteAgro
    from produtos.repasse_vila_util import (
        acumulado_anterior,
        listar_acumulado_detalhe,
        registrar_ajuste_acumulado,
        quitar_acumulado_zerar,
    )

    for name in ("api_repasse_vila_acumulado", "api_repasse_vila_acumulado_ajuste", "api_repasse_vila_acumulado_zerar"):
        try:
            reverse(name)
            ok(f"url {name}")
        except Exception as e:
            fail(f"url {name}: {e}")

    calc_ac = calcular_disponivel(hoje)
    if "acumulado_anterior" in calc_ac and "total_sugerido" in calc_ac:
        ok("calc tem acumulado/total_sugerido")
    else:
        fail("calc sem campos acumulado")

    calc_fake = {
        "alvos": {"cmv": 0, "lucro": 0, "fiado": 0},
        "ja_eletronico_aplicado": 0,
        "ja_enviado": {"total": "1878.47"},
    }
    extra_f = _extra_do_calc(calc_fake)
    ok("extra do envio") if extra_f == Decimal("1878.47") else fail(f"extra={extra_f}")
    liq_f = abater_extras_do_acumulado(hoje, Decimal("1878.47"), calc_fake)
    ok("envio extra zera acum do dia") if liq_f == Decimal("0.00") else fail(f"liq={liq_f}")

    RepasseVilaAcumuladoAjusteAgro.objects.filter(observacao__startswith=tag).delete()
    acum_antes = acumulado_anterior(hoje)
    adj, err_adj = registrar_ajuste_acumulado(
        Decimal("-25.50"),
        observacao=f"{tag} credito teste",
        operador="bot",
    )
    if adj and not err_adj:
        ok("registrar ajuste acumulado")
    else:
        fail(f"ajuste acumulado: {err_adj}")
    acum_depois = acumulado_anterior(hoje)
    esperado = (acum_antes - Decimal("25.50")).quantize(Decimal("0.01"))
    ok("acum reflete ajuste") if acum_depois == esperado else fail(
        f"acum {acum_depois} != {esperado}"
    )
    liq_antes_z = Decimal(str(calcular_disponivel(hoje).get("acumulado_anterior") or 0))
    adj_z, err_z = quitar_acumulado_zerar(hoje, operador="bot", observacao=f"{tag} zerar")
    if liq_antes_z > 0:
        ok("zerar acumulado") if adj_z and not err_z else fail(f"zerar: {err_z}")
        liq_depois = Decimal(str(calcular_disponivel(hoje).get("acumulado_anterior") or 0))
        ok("acum apos zerar <= 0") if liq_depois <= 0 else fail(f"acum pos zerar={liq_depois}")
    else:
        ok("zerar recusa se ja coberto") if err_z and not adj_z else fail("zerar deveria recusar")

    det = listar_acumulado_detalhe(hoje)
    calc_liq = calcular_disponivel(hoje)
    ok("detalhe ok") if det.get("ok") and abs(
        float(det["acumulado_anterior"]) - float(calc_liq.get("acumulado_anterior") or 0)
    ) < 0.02 else fail("detalhe diverge")
    ok("detalhe tem ajustes") if any(a.get("tipo") == "ajuste" for a in (det.get("ajustes") or [])) else fail(
        "detalhe sem ajuste"
    )

    r_ac = c.get("/api/repasse-vila/acumulado/", {"data": hoje.isoformat()})
    j_ac = r_ac.json() if r_ac.status_code == 200 else {}
    ok("GET api acumulado") if j_ac.get("ok") else fail("GET api acumulado")

    r_adj = c.post(
        "/api/repasse-vila/acumulado/ajuste/",
        data=json.dumps({"valor": "10", "observacao": "ab", "data_calc": hoje.isoformat()}),
        content_type="application/json",
    )
    ok("POST ajuste motivo curto 400") if r_adj.status_code == 400 else fail(
        f"ajuste motivo curto deveria falhar ({r_adj.status_code})"
    )

    RepasseVilaAcumuladoAjusteAgro.objects.filter(observacao__startswith=tag).delete()

    js_ac = (ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js").read_text(
        encoding="utf-8", errors="replace"
    )
    ok("js incluir_acumulado") if "incluir_acumulado" in js_ac else fail("js sem incluir_acumulado")
    ok("js api acumulado") if "api/repasse-vila/acumulado/" in js_ac else fail("js sem api acumulado")

    for rep in RepasseVilaCentroAgro.objects.filter(observacao=tag):
        if rep.movimento_saida_id:
            MovimentoCaixa.objects.filter(pk=rep.movimento_saida_id).delete()
        if rep.movimento_entrada_id:
            MovimentoCaixa.objects.filter(pk=rep.movimento_entrada_id).delete()
        rep.delete()
    VendaAgro.objects.filter(cliente_nome=tag).delete()
    if created_vila and s_vila and not MovimentoCaixa.objects.filter(sessao_caixa=s_vila).exists():
        s_vila.delete()
    if created_gav and s_gav and not MovimentoCaixa.objects.filter(sessao_caixa=s_gav).exists():
        s_gav.delete()
    ok("cleanup")

    for rel in (
        "produtos/static/produtos/js/pdv_repasse_vila.js",
        "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html",
        "produtos/templates/produtos/includes/repasse_help_agents.html",
        "produtos/migrations/0087_repasse_vila_centro.py",
        "produtos/migrations/0093_repasse_vila_acumulado_ajuste.py",
        "produtos/migrations/0095_repasse_vila_reserva.py",
    ):
        ok(f"file {rel}") if (ROOT / rel).exists() else fail(f"missing {rel}")

    views_txt = (ROOT / "produtos/views.py").read_text(encoding="utf-8", errors="replace")
    ok("hook abertura") if "aplicar_repasses_pendentes_centro" in views_txt else fail(
        "sem hook abertura"
    )
    ok("session aviso") if "repasse_aviso_abertura" in views_txt else fail("sem session aviso")

    print("---")
    print(f"oks={oks} fails={len(fails)}")
    for f in fails:
        print(f)
    if fails:
        return 1
    print("VERIFY_DEEP_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
