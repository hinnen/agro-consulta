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
    _vendas_vila_sem_fiado,
    aplicar_repasses_pendentes_centro,
    calcular_disponivel,
    confirmar_repasse,
    salvar_percentual_padrao,
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

    lucro = Decimal(str(calc["lucro_bruto_dia"]))
    alvo = (max(Decimal("0"), lucro) * Decimal("50") / Decimal("100")).quantize(
        Decimal("0.01")
    )
    ok("alvo lucro 50%") if abs(Decimal(str(calc["alvos"]["lucro"])) - alvo) <= Decimal(
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

    calc2 = calcular_disponivel(hoje, percentual_lucro=50, modo_dia_cheio=False)
    calc3 = calcular_disponivel(hoje, percentual_lucro=50, modo_dia_cheio=True)
    ok(f"incremental total={calc2['disponivel']['total']}")
    ok("dia cheio CMV=alvo") if abs(
        calc3["disponivel"]["cmv"] - calc3["alvos"]["cmv"]
    ) <= 0.02 else fail("dia cheio")

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
    r = c.get("/repasse-vila/")
    ok("GET tela") if r.status_code == 200 and b"Repasse" in r.content else fail("tela")
    if r.status_code == 200:
        if b"caixa/retiradas" in r.content and b"repasse=1" in r.content:
            ok("Transferir aponta Retiradas?repasse=1")
        elif b"/pdv/" in r.content and b"repasse=1" in r.content:
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
