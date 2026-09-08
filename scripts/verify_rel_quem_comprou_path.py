# -*- coding: utf-8 -*-
"""
REL-QUEM-COMPROU — prova detalhada do path «Quem já comprou».

Cobre: arquivos · agregação (produto/categoria/Zap/sem cliente/devolvida) · HTTP · Excel.
"""
from __future__ import annotations

import os
import sys
import uuid
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.test import Client, override_settings
from django.urls import reverse
from django.utils import timezone

from produtos.models import ClienteAgro, ItemVendaAgro, Produto, VendaAgro
from produtos import relatorios_vendas_util as ru

FAILS: list[str] = []
OKS = 0
TAG = f"qcverify_{uuid.uuid4().hex[:10]}"
PID_A = f"{TAG}_prod_a"
PID_B = f"{TAG}_prod_b"
DOC_WA = f"999{uuid.uuid4().hex[:8]}"[:14]
DOC_SEM = f"888{uuid.uuid4().hex[:8]}"[:14]


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def check_static() -> None:
    print("--- static ---")
    urls = read("produtos/urls.py")
    util = read("produtos/relatorios_vendas_util.py")
    views = read("produtos/relatorios_central_views.py")
    hub = read("produtos/templates/produtos/relatorios_hub.html")
    tpl = read("produtos/templates/produtos/relatorios_quem_comprou.html")
    help_a = read("produtos/templates/produtos/includes/relatorios_help_agents.html")

    check("relatorios/quem-comprou/" in urls, "url path quem-comprou")
    check("name='relatorios_quem_comprou'" in urls, "url name relatorios_quem_comprou")
    check("def clientes_quem_comprou" in util, "util clientes_quem_comprou")
    check("def _enrich_clientes_whatsapp" in util, "util enrich whatsapp")
    check("def _wa_url_digits" in util, "util wa url")
    check("def relatorios_quem_comprou" in views, "view relatorios_quem_comprou")
    check("relatorios_quem_comprou.html" in views, "view render template proprio")
    check("export" in views and "xlsx" in views, "view export xlsx")
    check("relatorios_quem_comprou" in hub, "hub card link")
    check("Quem já comprou" in hub or "Quem ja comprou" in hub, "hub titulo")
    check("qc-wa" in tpl and "agroAbrirUrlExterna" in tpl, "tpl botao Zap")
    check("{nome}" in tpl and "{produto}" in tpl, "tpl placeholders msg")
    check("so_whatsapp" in tpl, "tpl filtro so Zap")
    check("qc-expand" in tpl and "qc-hist" in tpl, "tpl expandir historico")
    check("Excel" in tpl or "export_qs" in tpl, "tpl Excel")
    check("agro_busca_catalogo.js" in tpl, "tpl busca catalogo")
    check('rel_help == "quem_comprou"' in help_a, "ajuda quem_comprou")
    check("Sem disparo em massa" in help_a or "um a um" in help_a.lower() or "Zap" in help_a, "ajuda menciona Zap")
    check("cpf__in" in util, "util join ClienteAgro via cpf")
    check("(cli.cpf" in util or "cli.cpf " in util, "enrich le cli.cpf")
    check(reverse("relatorios_quem_comprou") == "/relatorios/quem-comprou/", "reverse url")
    check(reverse("relatorios_hub") == "/relatorios/", "reverse hub")


def _cleanup() -> None:
    ItemVendaAgro.objects.filter(produto_id_externo__startswith=TAG).delete()
    VendaAgro.objects.filter(cliente_documento__in=[DOC_WA, DOC_SEM]).delete()
    VendaAgro.objects.filter(cliente_nome__startswith=f"QC Verify {TAG}").delete()
    ClienteAgro.objects.filter(cpf__in=[DOC_WA, DOC_SEM]).delete()
    ClienteAgro.objects.filter(nome__startswith=f"QC Verify").filter(
        externo_id__startswith=f"erp_{TAG}"
    ).delete()
    Produto.objects.filter(produto_externo_id__in=[PID_A, PID_B]).delete()


def _mk_venda(*, nome: str, doc: str, cid: str, total: str) -> VendaAgro:
    v = VendaAgro.objects.create(
        cliente_nome=nome,
        cliente_documento=doc,
        cliente_id_erp=cid,
        total=Decimal(total),
        forma_pagamento="Dinheiro",
        deposito="centro",
    )
    # auto_now_add: força janela recente
    VendaAgro.objects.filter(pk=v.pk).update(criado_em=timezone.now() - timedelta(days=1))
    v.refresh_from_db()
    return v


def _mk_item(venda: VendaAgro, pid: str, qtd: str, valor: str, desc: str) -> ItemVendaAgro:
    return ItemVendaAgro.objects.create(
        venda=venda,
        produto_id_externo=pid,
        codigo="GM-QC",
        descricao=desc,
        quantidade=Decimal(qtd),
        valor_unitario=Decimal(valor) / Decimal(qtd),
        valor_total=Decimal(valor),
    )


def check_agg() -> None:
    print("--- agregacao ---")
    _cleanup()
    agora = timezone.now()
    desde = agora - timedelta(days=7)
    ate = agora + timedelta(hours=1)

    r0 = ru.clientes_quem_comprou(desde, ate)
    check(r0["filtro_ok"] is False, "sem filtro -> filtro_ok False")
    check(r0["rows"] == [], "sem filtro -> rows vazias")

    digits, url = ru._wa_url_digits("13999887766")
    check(digits == "13999887766", "wa digits normal")
    check(url == "https://wa.me/5513999887766", "wa url com 55")
    d55, u55 = ru._wa_url_digits("5513999887766")
    check(u55 == "https://wa.me/5513999887766", "wa url ja com 55")
    d_bad, u_bad = ru._wa_url_digits("123")
    check(d_bad == "" and u_bad == "", "wa curto invalido")

    Produto.objects.create(
        produto_externo_id=PID_A,
        nome=f"QC Racao {TAG}",
        categoria="Racoes QC",
        subcategoria="Cao",
        codigo_interno=f"GM-{TAG}-A",
    )
    Produto.objects.create(
        produto_externo_id=PID_B,
        nome=f"QC Jardim {TAG}",
        categoria="Jardim QC",
        subcategoria="Vaso",
        codigo_interno=f"GM-{TAG}-B",
    )
    cli_wa = ClienteAgro.objects.create(
        nome=f"QC Verify WA {TAG}",
        cpf=DOC_WA,
        whatsapp="13988776655",
        externo_id=f"erp_{TAG}_wa",
    )
    ClienteAgro.objects.create(
        nome=f"QC Verify SEM {TAG}",
        cpf=DOC_SEM,
        whatsapp="",
        externo_id=f"erp_{TAG}_sem",
    )

    v1 = _mk_venda(
        nome=f"QC Verify WA {TAG}",
        doc=DOC_WA,
        cid=cli_wa.externo_id,
        total="100.00",
    )
    _mk_item(v1, PID_A, "2", "100.00", f"QC Racao {TAG}")

    v2 = _mk_venda(
        nome=f"QC Verify SEM {TAG}",
        doc=DOC_SEM,
        cid=f"erp_{TAG}_sem",
        total="50.00",
    )
    _mk_item(v2, PID_A, "1", "50.00", f"QC Racao {TAG}")

    v3 = _mk_venda(
        nome="",
        doc="",
        cid="",
        total="30.00",
    )
    _mk_item(v3, PID_A, "1", "30.00", f"QC Racao {TAG} avulso")

    v4 = _mk_venda(
        nome=f"QC Verify WA {TAG}",
        doc=DOC_WA,
        cid=cli_wa.externo_id,
        total="40.00",
    )
    _mk_item(v4, PID_B, "1", "40.00", f"QC Jardim {TAG}")

    v5 = _mk_venda(
        nome=f"QC Verify WA {TAG}",
        doc=DOC_WA,
        cid=cli_wa.externo_id,
        total="20.00",
    )
    _mk_item(v5, PID_A, "1", "20.00", f"QC Racao devolvida {TAG}")
    VendaAgro.objects.filter(pk=v5.pk).update(devolvida_em=timezone.now())

    r_prod = ru.clientes_quem_comprou(desde, ate, produto_id=PID_A, ordenar="valor")
    check(r_prod["filtro_ok"] is True, "produto -> filtro_ok")
    nomes = {x["cliente"] for x in r_prod["rows"]}
    check(f"QC Verify WA {TAG}" in nomes, "produto inclui cliente com Zap")
    check(f"QC Verify SEM {TAG}" in nomes, "produto inclui cliente sem Zap")
    check(not any("(sem nome)" == n or n == "" for n in nomes if "avulso" in n.lower()), "avulso sem cliente fora")
    # venda sem cliente: chave nome:(sem nome) — garantir que total 30 nao entrou
    totais = {x["cliente"]: float(x["total"]) for x in r_prod["rows"]}
    check(abs(totais.get(f"QC Verify WA {TAG}", 0) - 100.0) < 0.01, "WA total so venda nao devolvida (=100)")
    check(abs(totais.get(f"QC Verify SEM {TAG}", 0) - 50.0) < 0.01, "SEM total 50")
    check(r_prod["resumo"]["clientes"] == 2, "resumo 2 clientes no produto A")

    row_wa = next(x for x in r_prod["rows"] if x["cliente"] == f"QC Verify WA {TAG}")
    check(row_wa["tem_whatsapp"] is True, "WA tem_whatsapp")
    check("wa.me" in (row_wa.get("whatsapp_url") or ""), "WA url wa.me")
    check(row_wa.get("compras") and len(row_wa["compras"]) >= 1, "historico compras")
    check(row_wa.get("ultima_fmt"), "ultima_fmt preenchida")
    check(row_wa.get("dias_desde") is not None, "dias_desde preenchido")

    r_so = ru.clientes_quem_comprou(desde, ate, produto_id=PID_A, so_whatsapp=True)
    check(r_so["resumo"]["clientes"] == 1, "so_whatsapp = 1 cliente")
    check(all(x["tem_whatsapp"] for x in r_so["rows"]), "so_whatsapp so com Zap")

    r_cat = ru.clientes_quem_comprou(desde, ate, categoria=["Racoes QC"])
    nomes_cat = {x["cliente"] for x in r_cat["rows"]}
    check(f"QC Verify WA {TAG}" in nomes_cat, "categoria Racoes inclui WA")
    check(f"QC Verify SEM {TAG}" in nomes_cat, "categoria Racoes inclui SEM")
    # produto B (Jardim) nao deve puxar so pela categoria Racoes
    tot_wa_cat = next(
        (float(x["total"]) for x in r_cat["rows"] if x["cliente"] == f"QC Verify WA {TAG}"),
        0,
    )
    check(abs(tot_wa_cat - 100.0) < 0.01, "categoria nao mistura Jardim no total Racoes")

    r_ord_q = ru.clientes_quem_comprou(desde, ate, produto_id=PID_A, ordenar="qtd")
    check(len(r_ord_q["rows"]) >= 2, "ordenar qtd retorna linhas")
    check(
        float(r_ord_q["rows"][0]["qtd"]) >= float(r_ord_q["rows"][1]["qtd"]),
        "ordenar qtd decrescente",
    )

    r_ord_v = ru.clientes_quem_comprou(desde, ate, produto_id=PID_A, ordenar="valor")
    check(
        float(r_ord_v["rows"][0]["total"]) >= float(r_ord_v["rows"][1]["total"]),
        "ordenar valor decrescente",
    )

    chave = ru._chave_cliente_venda("erp_1", "123", "Fulano")
    check(chave == "id:erp_1", "chave prioriza id erp")
    chave2 = ru._chave_cliente_venda("", "123", "Fulano")
    check(chave2 == "doc:123", "chave fallback documento")


def check_http() -> None:
    print("--- http ---")
    with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
        c = Client()
        r_hub = c.get(reverse("relatorios_hub"))
        check(r_hub.status_code == 200, f"hub HTTP 200 ({r_hub.status_code})")
        body_hub = r_hub.content.decode("utf-8", errors="replace")
        check("quem-comprou" in body_hub or "relatorios_quem_comprou" in body_hub, "hub HTML tem link")

        r_empty = c.get(reverse("relatorios_quem_comprou"))
        check(r_empty.status_code == 200, f"quem-comprou vazio HTTP 200 ({r_empty.status_code})")
        body = r_empty.content.decode("utf-8", errors="replace")
        check("Escolha um produto" in body or "produto ou" in body.lower(), "tela pede filtro")
        check("qc-msg" in body or "Mensagem do Zap" in body, "campo mensagem Zap")
        check("rel-help" in body or "?" in body, "ajuda presente")

        r_prod = c.get(
            reverse("relatorios_quem_comprou"),
            {"produto_id": PID_A, "periodo": "30d", "ordenar": "valor"},
        )
        check(r_prod.status_code == 200, f"filtro produto HTTP 200 ({r_prod.status_code})")
        bp = r_prod.content.decode("utf-8", errors="replace")
        check(f"QC Verify WA {TAG}" in bp, "HTML lista cliente WA")
        check("qc-wa" in bp, "HTML botao Zap")
        check("qc-expand" in bp, "HTML expandir")

        r_so = c.get(
            reverse("relatorios_quem_comprou"),
            {"produto_id": PID_A, "periodo": "30d", "so_whatsapp": "1"},
        )
        bs = r_so.content.decode("utf-8", errors="replace")
        check(f"QC Verify WA {TAG}" in bs, "so_whatsapp mantem WA")
        check(f"QC Verify SEM {TAG}" not in bs, "so_whatsapp esconde SEM")

        r_cat = c.get(
            reverse("relatorios_quem_comprou"),
            {"categoria": "Racoes QC", "periodo": "30d"},
        )
        check(r_cat.status_code == 200, f"filtro categoria HTTP 200 ({r_cat.status_code})")

        r_xlsx = c.get(
            reverse("relatorios_quem_comprou"),
            {"produto_id": PID_A, "periodo": "30d", "export": "xlsx"},
        )
        check(r_xlsx.status_code == 200, f"xlsx HTTP 200 ({r_xlsx.status_code})")
        ctype = r_xlsx.get("Content-Type", "")
        check("spreadsheet" in ctype or "xlsx" in ctype, f"xlsx content-type ({ctype})")
        check(len(r_xlsx.content) > 100, "xlsx tem bytes")
        check(
            "attachment" in (r_xlsx.get("Content-Disposition") or ""),
            "xlsx Content-Disposition",
        )


def main() -> int:
    print("VERIFY REL-QUEM-COMPROU")
    try:
        check_static()
        check_agg()
        check_http()
    finally:
        _cleanup()
        print("--- cleanup ok ---")

    print(f"\nRESULT {OKS}/{OKS + len(FAILS)}")
    if FAILS:
        print("FAILURES:")
        for f in FAILS:
            print(" -", f.encode("ascii", "replace").decode("ascii"))
        return 1
    print("ALL OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
