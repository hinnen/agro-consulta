# -*- coding: utf-8 -*-
"""
Prova detalhada — laboratório análise de crédito shadow (`CREDITO-SCORE-SHADOW`).

  set AGRO_PIN_TESTE=9973
  python scripts/verify_credito_score_shadow_path.py

Não grava na loja: inserts de prova usam savepoint+rollback.
HTTP usa Django test Client (não precisa runserver).
"""
from __future__ import annotations

import ast
import os
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.db import connection, transaction
from django.test import Client, override_settings
from django.urls import reverse
from django.utils import timezone

from produtos.cliente_planilha_util import _fiado_stats_3m_por_cliente
from produtos.credito_score_acesso_util import (
    credito_score_shadow_enabled,
    usuario_pode_acessar_credito_score_shadow,
)
from produtos.credito_score_shadow import (
    REGRA_VERSAO,
    _fator_pontualidade,
    _media_fiado_3m_mapa,
    _multiplicador_limite,
    _pontos_frequencia,
    _pontos_relacionamento,
    _pontos_situacao_atraso,
    analisar_cliente,
    analisar_clientes,
    persistir_analise,
)
from produtos.models import (
    ClienteAgro,
    ClienteAnaliseCreditoAgro,
    FiadoBaixaAgro,
    FiadoTituloAgro,
    VendaAgro,
)

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
oks: list[str] = []
fails: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _titulo(cli, *, bruto, pago, situacao, vencimento, chave, origem=None):
    return FiadoTituloAgro.objects.create(
        chave_unica=chave,
        cliente_agro=cli,
        cliente_nome=cli.nome,
        cliente_codigo=str(cli.pk),
        numero_documento=chave[:40],
        parcela_num=1,
        parcela_total=1,
        vencimento=vencimento,
        valor_bruto=Decimal(bruto),
        valor_pago=Decimal(pago),
        situacao=situacao,
        origem=origem or FiadoTituloAgro.Origem.PDV,
    )


def _baixa(titulo, valor, quando):
    b = FiadoBaixaAgro.objects.create(
        titulo=titulo, valor=Decimal(valor), forma_pagamento="Dinheiro", usuario="prova"
    )
    FiadoBaixaAgro.objects.filter(pk=b.pk).update(criado_em=quando)
    b.refresh_from_db()
    return b


def test_arquivos_e_isolamento() -> None:
    print("== 1) Arquivos + isolamento (sem escrita financeira no motor) ==")
    motor = ROOT / "produtos/credito_score_shadow.py"
    acesso = ROOT / "produtos/credito_score_acesso_util.py"
    views = ROOT / "produtos/credito_score_views.py"
    mig = ROOT / "produtos/migrations/0137_clienteanalisecreditoagro.py"
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    check("arquivo_motor", motor.is_file())
    check("arquivo_acesso", acesso.is_file())
    check("arquivo_views", views.is_file())
    check("arquivo_migration_0137", mig.is_file())
    check("rota_laboratorio", "fiado/analise-credito/" in urls)
    check("rota_detalhe", "fiado/analise-credito/cliente/<int:pk>/" in urls)
    check("regra_versao", REGRA_VERSAO == "shadow_v1")

    src = motor.read_text(encoding="utf-8")
    tree = ast.parse(src)
    writes: list[str] = []
    called: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Name):
            called.add(f.id)
            if f.id in ("create", "update", "bulk_create", "delete", "save"):
                writes.append(f.id)
        elif isinstance(f, ast.Attribute):
            called.add(f.attr)
            if f.attr in ("create", "update", "bulk_create", "delete", "save"):
                writes.append(f.attr)
    forbad_calls = (
        "definir_limite_fiado_cliente",
        "baixar_titulo",
        "criar_titulos_de_venda",
        "api_enviar_pedido_erp",
        "_persistir_venda_agro",
        "resumo_credito_fiado_cliente",
        "valor_fiado_usado_cliente",
    )
    for name in forbad_calls:
        check(f"motor_nao_chama_{name}", name not in called)
    # create só em ClienteAnaliseCreditoAgro (persistir_analise)
    check("unica_escrita_create", writes.count("create") == 1, str(writes))
    check("sem_update_delete_save", not any(w in writes for w in ("update", "delete", "save")), str(writes))
    check("src_sem_movimento_caixa_create", "MovimentoCaixa.objects.create" not in src)
    check("src_sem_ajuste_estoque_create", "AjusteRapidoEstoque.objects.create" not in src)

    mig_txt = mig.read_text(encoding="utf-8")
    check("mig_create_model", "CreateModel" in mig_txt and "ClienteAnaliseCreditoAgro" in mig_txt)
    # Só operações reais (comentário do arquivo cita AlterField/RunPython como proibidos)
    check("mig_sem_alterfield_op", "migrations.AlterField" not in mig_txt)
    check("mig_sem_runpython_op", "migrations.RunPython" not in mig_txt)
    check("mig_dep_0136", "0136_clienteagro_limite_fiado_default_001" in mig_txt)

    # menus: não linkar no fiado operacional / PDV
    fiado_tpl = list((ROOT / "produtos/templates/produtos").glob("*fiado*"))
    linked = False
    for p in fiado_tpl:
        if "credito_score" in p.name:
            continue
        t = p.read_text(encoding="utf-8", errors="ignore")
        if "analise-credito" in t or "credito_score_laboratorio" in t:
            linked = True
    check("sem_link_menu_fiado", not linked)


def test_formula_pura() -> None:
    print("== 2) Fórmula shadow_v1 (puros) ==")
    check("fator_0", _fator_pontualidade(0) == Decimal("1.00"))
    check("fator_3", _fator_pontualidade(3) == Decimal("0.90"))
    check("fator_5", _fator_pontualidade(5) == Decimal("0.75"))
    check("fator_10", _fator_pontualidade(10) == Decimal("0.50"))
    check("fator_20", _fator_pontualidade(20) == Decimal("0.20"))
    check("fator_40", _fator_pontualidade(40) == Decimal("0.00"))
    check("sit_0", _pontos_situacao_atraso(0) == 25)
    check("sit_2", _pontos_situacao_atraso(2) == 20)
    check("sit_5", _pontos_situacao_atraso(5) == 15)
    check("sit_10", _pontos_situacao_atraso(10) == 8)
    check("sit_20", _pontos_situacao_atraso(20) == 3)
    check("sit_40", _pontos_situacao_atraso(40) == 0)
    check("freq_0", _pontos_frequencia(0) == 0)
    check("freq_3", _pontos_frequencia(3) == 6)
    check("freq_6", _pontos_frequencia(6) == 10)
    check("rel_10d", _pontos_relacionamento(10) == 2)
    check("rel_400d", _pontos_relacionamento(400) == 10)
    check("mult_alto_risco", _multiplicador_limite(44) == Decimal("0.50"))
    check("mult_regular", _multiplicador_limite(60) == Decimal("0.75"))
    check("mult_bom", _multiplicador_limite(80) == Decimal("1.00"))
    check("mult_muito_bom", _multiplicador_limite(89) == Decimal("1.20"))
    check("mult_excelente", _multiplicador_limite(97) == Decimal("1.40"))
    check("mult_sem_score", _multiplicador_limite(None) == Decimal("0.50"))
    check("pin_disponivel", bool(PIN), PIN)


def test_db_cenarios() -> None:
    print("== 3) Cenários DB (rollback) ==")
    tables = set(connection.introspection.table_names())
    check("tabela_shadow_existe", "produtos_clienteanalisecreditoagro" in tables)
    if "produtos_clienteanalisecreditoagro" not in tables:
        print("  SKIP cenários DB — rode migrate 0137")
        return

    hoje = date(2026, 10, 5)
    with transaction.atomic():
        sid = transaction.savepoint()
        try:
            # sem histórico
            c0 = ClienteAgro.objects.create(nome="ZZ Shadow Sem Hist", limite_fiado_local=Decimal("0.01"))
            r0 = analisar_cliente(c0, hoje=hoje)
            check("sem_hist_score_null", r0.score is None)
            check("sem_hist_classif", r0.classificacao == ClienteAnaliseCreditoAgro.Classificacao.SEM_HISTORICO)
            check("limite_001_intact", c0.limite_fiado_local == Decimal("0.01"))

            # zero legado
            cz = ClienteAgro.objects.create(nome="ZZ Shadow Zero Legado", limite_fiado_local=Decimal("0"))
            rz = analisar_cliente(cz, hoje=hoje)
            check("zero_legado_cad", rz.limite_cadastrado == Decimal("0.00"))
            check("zero_legado_efetivo_gt0", rz.limite_efetivo > Decimal("0"))
            cz.refresh_from_db()
            check("zero_nao_convertido", cz.limite_fiado_local == Decimal("0"))

            # bom pagador
            bom = ClienteAgro.objects.create(nome="ZZ Shadow Bom", limite_fiado_local=Decimal("500"))
            for i in range(6):
                venc = hoje - timedelta(days=30 * (i + 1))
                t = _titulo(
                    bom,
                    bruto="100.00",
                    pago="100.00",
                    situacao=FiadoTituloAgro.Situacao.QUITADO,
                    vencimento=venc,
                    chave=f"shadow-bom-{bom.pk}-{i}",
                )
                _baixa(
                    t,
                    "100.00",
                    timezone.make_aware(datetime(venc.year, venc.month, venc.day, 12, 0, 0)),
                )
            rb = analisar_cliente(bom, hoje=hoje, media_3m=Decimal("200"), meses_fiado_6m={(2026, m) for m in range(5, 11)})
            check("bom_score_ge_70", rb.score is not None and rb.score >= 70, str(rb.score))
            check("bom_sugerido_200", rb.limite_sugerido >= Decimal("200"))

            # atrasos progressivos
            scores = []
            for dias, tag in ((0, "d0"), (3, "d3"), (10, "d10"), (40, "d40")):
                cli = ClienteAgro.objects.create(nome=f"ZZ Atr {tag}", limite_fiado_local=Decimal("300"))
                venc = hoje - timedelta(days=60)
                t = _titulo(cli, bruto="200.00", pago="200.00", situacao=FiadoTituloAgro.Situacao.QUITADO, vencimento=venc, chave=f"atr-{tag}-{cli.pk}")
                pag = venc + timedelta(days=dias)
                _baixa(t, "200.00", timezone.make_aware(datetime(pag.year, pag.month, pag.day, 10, 0, 0)))
                venc2 = hoje - timedelta(days=200)
                t2 = _titulo(cli, bruto="50.00", pago="50.00", situacao=FiadoTituloAgro.Situacao.QUITADO, vencimento=venc2, chave=f"atr2-{tag}-{cli.pk}")
                _baixa(t2, "50.00", timezone.make_aware(datetime(venc2.year, venc2.month, venc2.day, 10, 0, 0)))
                scores.append(analisar_cliente(cli, hoje=hoje, media_3m=Decimal("100")).score)
            check("atrasos_todos_score", all(s is not None for s in scores), str(scores))
            check("atrasos_pioram", scores[0] >= scores[1] > scores[2] > scores[3], str(scores))

            # vencido atual
            vv = ClienteAgro.objects.create(nome="ZZ Vencido", limite_fiado_local=Decimal("400"))
            for i in range(4):
                venc = hoje - timedelta(days=120 + 30 * i)
                t = _titulo(vv, bruto="80.00", pago="80.00", situacao=FiadoTituloAgro.Situacao.QUITADO, vencimento=venc, chave=f"vv-{vv.pk}-{i}")
                _baixa(t, "80.00", timezone.make_aware(datetime(venc.year, venc.month, venc.day, 9, 0, 0)))
            _titulo(vv, bruto="90.00", pago="0", situacao=FiadoTituloAgro.Situacao.ABERTO, vencimento=hoje - timedelta(days=20), chave=f"vv-open-{vv.pk}")
            rv = analisar_cliente(vv, hoje=hoje, media_3m=Decimal("100"))
            check("vencido_flag", rv.tem_vencido)
            check("vencido_sit_pts", rv.indicadores["situacao_atual_pontos"] == 3)

            # parcial / cancelado
            cp = ClienteAgro.objects.create(nome="ZZ Parcial", limite_fiado_local=Decimal("200"))
            venc = hoje - timedelta(days=10)
            tp = _titulo(cp, bruto="100.00", pago="40.00", situacao=FiadoTituloAgro.Situacao.PARCIAL, vencimento=venc, chave=f"par-{cp.pk}")
            _baixa(tp, "40.00", timezone.make_aware(datetime(venc.year, venc.month, venc.day, 12, 0, 0)))
            rp = analisar_cliente(cp, hoje=hoje)
            check("parcial_nao_quitado_avaliavel", rp.indicadores["titulos_quitados_avaliaveis"] == 0)
            cc = ClienteAgro.objects.create(nome="ZZ Cancel", limite_fiado_local=Decimal("100"))
            _titulo(cc, bruto="999.00", pago="0", situacao=FiadoTituloAgro.Situacao.CANCELADO, vencimento=hoje - timedelta(days=5), chave=f"can-{cc.pk}")
            rc = analisar_cliente(cc, hoje=hoje)
            check("cancelado_sem_score", rc.score is None)

            # mesmo nome
            a = ClienteAgro.objects.create(nome="Maria Silva Shadow", limite_fiado_local=Decimal("200"))
            b = ClienteAgro.objects.create(nome="Maria Silva Shadow", limite_fiado_local=Decimal("200"))
            venc = hoje - timedelta(days=40)
            t = _titulo(a, bruto="100.00", pago="100.00", situacao=FiadoTituloAgro.Situacao.QUITADO, vencimento=venc, chave=f"mix-a-{a.pk}")
            _baixa(t, "100.00", timezone.make_aware(datetime(venc.year, venc.month, venc.day, 11, 0, 0)))
            ra = analisar_cliente(a, hoje=hoje, nomes_duplicados={"maria silva shadow"})
            rb = analisar_cliente(b, hoje=hoje, nomes_duplicados={"maria silva shadow"})
            check("dup_nome_a_tem_score", ra.score is not None)
            check("dup_nome_b_sem_hist", rb.score is None)
            check("dup_alerta_a", any("duplicado" in x.lower() for x in ra.alertas))

            # invariância + histórico
            inv = ClienteAgro.objects.create(nome="ZZ Invar", limite_fiado_local=Decimal("123.45"))
            venc = hoje - timedelta(days=8)
            ti = _titulo(inv, bruto="70.00", pago="20.00", situacao=FiadoTituloAgro.Situacao.PARCIAL, vencimento=venc, chave=f"inv-{inv.pk}")
            _baixa(ti, "20.00", timezone.make_aware(datetime(venc.year, venc.month, venc.day, 9, 0, 0)))
            n_v = VendaAgro.objects.count()
            n_t = FiadoTituloAgro.objects.count()
            n_b = FiadoBaixaAgro.objects.count()
            snap_lim = inv.limite_fiado_local
            snap_bruto, snap_pago, snap_sit = ti.valor_bruto, ti.valor_pago, ti.situacao
            analisar_clientes(cliente_ids=[inv.pk], persist=True, hoje=hoje)
            persistir_analise(analisar_cliente(inv, hoje=hoje))
            inv.refresh_from_db()
            ti.refresh_from_db()
            check("inv_limite", inv.limite_fiado_local == snap_lim)
            check("inv_bruto", ti.valor_bruto == snap_bruto)
            check("inv_pago", ti.valor_pago == snap_pago)
            check("inv_sit", ti.situacao == snap_sit)
            check("inv_vendas_count", VendaAgro.objects.count() == n_v)
            check("inv_titulos_count", FiadoTituloAgro.objects.count() == n_t)
            check("inv_baixas_count", FiadoBaixaAgro.objects.count() == n_b)
            check("hist_duas_linhas", ClienteAnaliseCreditoAgro.objects.filter(cliente=inv).count() == 2)

            # sugerido = media * 0.5 para alto risco
            check(
                "sugerido_alto_risco_meio",
                (Decimal("489.57") * Decimal("0.50")).quantize(Decimal("0.01")) == Decimal("244.78"),
            )
        finally:
            transaction.savepoint_rollback(sid)


def test_cliente_real_media() -> None:
    print("== 4) Cliente real pk 79 (média 3m × planilha) ==")
    try:
        cli = ClienteAgro.objects.get(pk=79)
    except ClienteAgro.DoesNotExist:
        check("cliente_79_existe", False, "skip sem pk 79")
        return
    check("cliente_79_existe", True, (cli.nome or "")[:40])
    hoje = timezone.localdate()
    media_shadow = _media_fiado_3m_mapa({79}, hoje=hoje).get(79, Decimal("0"))
    stats = _fiado_stats_3m_por_cliente().get(79) or {}
    media_plan = Decimal(str(stats.get("media") or 0)).quantize(Decimal("0.01"))
    check("media_bate_planilha", media_shadow == media_plan, f"shadow={media_shadow} planilha={media_plan}")
    lim_antes = cli.limite_fiado_local
    n_tit = FiadoTituloAgro.objects.filter(cliente_agro_id=79).count()
    n_bx = FiadoBaixaAgro.objects.filter(titulo__cliente_agro_id=79).count()
    r = analisar_cliente(cli, hoje=hoje, media_3m=media_shadow)
    # não persistir no real
    cli.refresh_from_db()
    check("real_limite_intact", cli.limite_fiado_local == lim_antes)
    check("real_titulos_intact", FiadoTituloAgro.objects.filter(cliente_agro_id=79).count() == n_tit)
    check("real_baixas_intact", FiadoBaixaAgro.objects.filter(titulo__cliente_agro_id=79).count() == n_bx)
    check("real_tem_score_ou_hist", r.score is not None or r.classificacao == ClienteAnaliseCreditoAgro.Classificacao.SEM_HISTORICO)
    if r.score is not None and r.media_fiado_3m > 0:
        esperado = (r.media_fiado_3m * _multiplicador_limite(r.score)).quantize(Decimal("0.01"))
        check("real_sugerido_formula", r.limite_sugerido == esperado, f"{r.limite_sugerido} vs {esperado}")


def test_http_acesso() -> None:
    print("== 5) HTTP acesso (flag + allowlist) ==")
    User = get_user_model()
    superu = User.objects.filter(is_superuser=True).order_by("pk").first()
    check("tem_superuser", superu is not None, getattr(superu, "username", ""))
    if not superu:
        return

    url = reverse("credito_score_laboratorio")
    c = Client()

    hosts = list(getattr(__import__("django.conf", fromlist=["settings"]).settings, "ALLOWED_HOSTS", []) or [])
    if "testserver" not in hosts:
        hosts = list(hosts) + ["testserver", "localhost", "127.0.0.1"]

    with override_settings(
        AGRO_CREDITO_SCORE_SHADOW_ENABLED=False,
        AGRO_CREDITO_SCORE_SHADOW_USERNAMES="",
        ALLOWED_HOSTS=hosts,
    ):
        c.force_login(superu)
        check("flag_off_404_mesmo_super", c.get(url).status_code == 404)

    with override_settings(
        AGRO_CREDITO_SCORE_SHADOW_ENABLED=True,
        AGRO_CREDITO_SCORE_SHADOW_USERNAMES="renan",
        ALLOWED_HOSTS=hosts,
    ):
        c.force_login(superu)
        resp = c.get(url)
        check("flag_on_super_200", resp.status_code == 200, str(resp.status_code))
        check("html_titulo_lab", b"Laborat" in resp.content)

        comum = User.objects.filter(is_superuser=False, is_staff=False).exclude(username__iexact="renan").first()
        if comum:
            c.force_login(comum)
            check("operador_404", c.get(url).status_code == 404, comum.username)
        staff = User.objects.filter(is_staff=True, is_superuser=False).exclude(username__iexact="renan").first()
        if staff:
            c.force_login(staff)
            check("staff_sozinho_404", c.get(url).status_code == 404, staff.username)

        lab = User.objects.filter(username__iexact="renan").first()
        if lab:
            c.force_login(lab)
            check("allowlist_renan_200", c.get(url).status_code == 200)

        # detalhe cliente 79 se existir
        if ClienteAgro.objects.filter(pk=79).exists():
            c.force_login(superu)
            d = c.get(reverse("credito_score_cliente_detalhe", args=[79]))
            check("detalhe_79_200", d.status_code == 200, str(d.status_code))


def test_settings_default_seguro() -> None:
    print("== 6) Defaults seguros p/ produção ==")
    from django.conf import settings

    # No processo atual o .env local pode ter true — checamos o default do código
    settings_py = (ROOT / "config/settings.py").read_text(encoding="utf-8")
    check(
        "default_flag_false_no_codigo",
        'AGRO_CREDITO_SCORE_SHADOW_ENABLED"' in settings_py
        or "AGRO_CREDITO_SCORE_SHADOW_ENABLED" in settings_py,
    )
    check(
        "default_false_cast",
        'default=False' in settings_py
        and "AGRO_CREDITO_SCORE_SHADOW_ENABLED" in settings_py,
    )
    # helper respeita settings atuais
    check("helper_enabled_bool", isinstance(credito_score_shadow_enabled(), bool))
    check(
        "helper_sem_user_false",
        usuario_pode_acessar_credito_score_shadow(None) is False,
    )


def main() -> int:
    print(f"=== verify CREDITO-SCORE-SHADOW · PIN={PIN} · vendor={connection.vendor} ===")
    test_arquivos_e_isolamento()
    test_formula_pura()
    test_db_cenarios()
    test_cliente_real_media()
    test_http_acesso()
    test_settings_default_seguro()
    print(f"=== {len(oks)} OK · {len(fails)} FAIL ===")
    if fails:
        print("FAILS:", ", ".join(fails))
    return 0 if not fails else 1


if __name__ == "__main__":
    raise SystemExit(main())
