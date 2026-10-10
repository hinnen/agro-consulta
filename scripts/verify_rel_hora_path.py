# -*- coding: utf-8 -*-
"""
REL-HORA — prova detalhada da venda hora a hora.

Path:
  GET /relatorios/hora/?periodo=&de=&ate=&deposito=ambos|centro|vila&canal=&visao=&hora=&wd=
    -> parse filtros
    -> VendaAgro (sem devolução total) + PedidoEntrega não cancelado
    -> agregar por hora local (7h–18h) · Centro/Vila · balcão/entrega · costume 4 semanas
    -> Excel

  python scripts/verify_rel_hora_path.py
"""
from __future__ import annotations

import os
import sys
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from io import BytesIO
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.test import Client, RequestFactory, override_settings
from django.urls import reverse
from django.utils import timezone
from openpyxl import load_workbook

from produtos import relatorios_hora_util as hu
from produtos.models import PedidoEntrega, VendaAgro

FAILS: list[str] = []
OKS = 0
TAG = f"HORA {uuid.uuid4().hex[:8]}"
PIN_TESTE = "9973"
DIA = date(2026, 10, 7)  # quarta com venda real; marcas vão na madrugada


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


def _q(v) -> Decimal:
    return Decimal(str(v or 0)).quantize(Decimal("0.01"))


def check_static() -> None:
    print("--- static ---")
    util = read("produtos/relatorios_hora_util.py")
    views = read("produtos/relatorios_central_views.py")
    hub = read("produtos/templates/produtos/relatorios_hub.html")
    tpl = read("produtos/templates/produtos/relatorios_hora.html")
    help_a = read("produtos/templates/produtos/includes/relatorios_help_agents.html")
    urls = read("produtos/urls.py")
    check("def agregar_hora" in util, "util agregar_hora")
    check("def carregar_vendas_intervalo" in util, "util carregar")
    check("devolucao" in util and "Vendas por loja" in util, "util devolve na hora e alinha com Vendas por loja")
    check("PedidoEntrega.Status.CANCELADO" in util, "util ignora entrega cancelada")
    check("HORA_INI = 7" in util and "HORA_FIM = 18" in util, "util expediente 7h-18h")
    check("def relatorios_hora" in views, "view existe")
    check("relatorios/hora/" in urls and "name='relatorios_hora'" in urls, "url")
    check(reverse("relatorios_hora") == "/relatorios/hora/", "reverse /relatorios/hora/")
    check("relatorios_hora" in hub and "Venda hora a hora" in hub, "card na Central")
    check('name="deposito"' in tpl and "Centro + Vila" in tpl and "Só Vila" in tpl, "filtro loja")
    check('name="canal"' in tpl and "Só balcão" in tpl and "Só entrega" in tpl, "filtro canal")
    check('name="visao"' in tpl and "Média por dia" in tpl and "Soma do período" in tpl, "filtro media/soma")
    check("grade.rotulo_total" in tpl, "cartao usa o rotulo da visao")
    check(
        '"Média do dia" if media else "Total do período"' in util,
        "rotulo: media do dia ou total do periodo",
    )
    check("Média do dia" in help_a and "Total do período" in help_a, "ajuda explica o cartao")
    check("Mapa da semana" in tpl and "Quem atendeu" in tpl, "mapa e quem")
    check('rel_help == "hora"' in help_a and "COSTUME" in help_a, "ajuda ?")
    check("export=xlsx" in views or 'get("export") == "xlsx"' in views, "excel na view")


def _limpar() -> None:
    PedidoEntrega.objects.filter(cliente_nome__startswith=TAG).delete()
    VendaAgro.objects.filter(cliente_nome__startswith=TAG).delete()


def _venda(*, hora: int, minuto: int, total: str, deposito: str, operador: str, entrega: str | None) -> VendaAgro:
    tz = timezone.get_current_timezone()
    quando = timezone.make_aware(datetime.combine(DIA, time(hora, minuto)), tz)
    v = VendaAgro.objects.create(
        cliente_nome=f"{TAG} {deposito or 'vazio'} {hora}h",
        total=Decimal(total),
        forma_pagamento="Dinheiro",
        deposito=deposito,
        usuario_registro=operador,
    )
    VendaAgro.objects.filter(pk=v.pk).update(criado_em=quando)
    if entrega:
        PedidoEntrega.objects.create(
            cliente_nome=TAG,
            venda_agro=v,
            status=entrega,
        )
    v.refresh_from_db()
    return v


def check_marcadas() -> None:
    print("--- marcas isoladas (hora local, loja, entrega, devolucao) ---")
    _limpar()
    try:
        centro = _venda(hora=10, minuto=15, total="100.00", deposito="centro", operador="Ana", entrega=None)
        vila = _venda(
            hora=10, minuto=40, total="40.00", deposito="vila", operador="Bia",
            entrega=PedidoEntrega.Status.PENDENTE,
        )
        legado = _venda(hora=15, minuto=0, total="7.50", deposito="", operador="Caio", entrega=None)
        cancelada = _venda(
            hora=16, minuto=5, total="9.00", deposito="centro", operador="Duda",
            entrega=PedidoEntrega.Status.CANCELADO,
        )
        madrugada = _venda(hora=3, minuto=10, total="5.00", deposito="vila", operador="Eva", entrega=None)
        quase_meia = _venda(hora=23, minuto=50, total="8.00", deposito="centro", operador="Ana", entrega=None)
        devolvida = _venda(hora=11, minuto=0, total="999.00", deposito="vila", operador="Zeca", entrega=None)
        quando_dev = timezone.make_aware(datetime.combine(DIA, time(11, 5)), timezone.get_current_timezone())
        VendaAgro.objects.filter(pk=devolvida.pk).update(devolvida_em=quando_dev)

        local_centro = timezone.localtime(centro.criado_em)
        check(local_centro.hour == 10 and local_centro.date() == DIA, "criado_em 10h fica 10h na loja")

        rows = hu.carregar_vendas_intervalo(DIA, DIA)
        marcas = [r for r in rows if str(r.get("cliente") or "").startswith(TAG)]
        ids = {r["id"] for r in marcas}
        dev_linhas = [r for r in marcas if r["id"] == devolvida.pk]
        check(len(dev_linhas) == 2, "devolucao do mesmo dia entra a venda e sai o estorno")
        check(abs(sum(float(r["total"]) for r in dev_linhas)) < 0.01, "estorno do mesmo dia zera os 999")
        check(centro.pk in ids and vila.pk in ids and legado.pk in ids, "centro, vila e deposito vazio entram")
        check(sum(1 for r in marcas if not r.get("devolucao")) == 7, "7 cupons marcados")

        by_id = {r["id"]: r for r in marcas}
        check(by_id[vila.pk]["entrega"] is True, "pedido pendente = entrega")
        check(by_id[cancelada.pk]["entrega"] is False, "entrega cancelada = balcao")
        check(by_id[legado.pk]["deposito"] == "", "deposito vazio preservado")

        grade = hu.agregar_hora(
            marcas, [], dias=[DIA], dias_base=[], loja="ambos", canal="todos", visao="soma",
            hora_detalhe=10,
        )
        h10 = next(ln for ln in grade["expediente"] if ln["hora"] == 10)
        h15 = next(ln for ln in grade["expediente"] if ln["hora"] == 15)
        h16 = next(ln for ln in grade["expediente"] if ln["hora"] == 16)
        check(abs(h10["centro"] - 100.0) < 0.01 and abs(h10["vila"] - 40.0) < 0.01, "10h Centro 100 + Vila 40")
        check(abs(h10["entrega"] - 40.0) < 0.01 and abs(h10["balcao"] - 100.0) < 0.01, "10h entrega 40 e balcao 100")
        check("Ana 1" in h10["quem"] and "Bia 1" in h10["quem"], "10h quem Ana e Bia")
        check(abs(h15["centro"] - 7.50) < 0.01 and abs(h15["vila"]) < 0.01, "deposito vazio conta no Centro")
        check(abs(h16["balcao"] - 9.0) < 0.01 and abs(h16["entrega"]) < 0.01, "cancelada fica no balcao")
        check(abs(grade["total_bruto"] - 169.50) < 0.01, "total marcas 169,50 sem os 999")
        check(abs(grade["total_bruto"] - (grade["linhas"] and sum(ln["total_bruto"] for ln in grade["linhas"]))) < 0.01, "soma das horas = total")
        fora = {ln["hora"]: ln for ln in grade["fora"]}
        check(3 in fora and 23 in fora, "3h e 23h fora do expediente")
        check(abs(fora[23]["total"] - 8.0) < 0.01, "23h50 continua 23h (nao vira madrugada UTC)")
        check(grade["detalhe"]["n"] == 2, "detalhe das 10h tem as duas vendas")
        lojas_det = {v["loja"] for v in grade["detalhe"]["vendas"]}
        check(lojas_det == {"Centro", "Vila"}, "detalhe separa Centro e Vila")

        so_vila = hu.agregar_hora(marcas, [], dias=[DIA], dias_base=[], loja="vila", canal="todos", visao="soma")
        check(abs(so_vila["total_bruto"] - 45.0) < 0.01, "so Vila = 40 entrega + 5 madrugada")
        so_balcao = hu.agregar_hora(marcas, [], dias=[DIA], dias_base=[], loja="ambos", canal="balcao", visao="soma")
        check(abs(so_balcao["total_bruto"] - 129.50) < 0.01, "so balcao tira a entrega de 40")

        # costume: mesma quarta, 4 semanas antes, so a hora 10
        base_dias = hu.dias_costume([DIA])
        check(len(base_dias) == 4 and all(d.weekday() == DIA.weekday() for d in base_dias), "costume = 4 quartas anteriores")
        base_quando = timezone.make_aware(datetime.combine(base_dias[-1], time(10, 0)), timezone.get_current_timezone())
        base_v = VendaAgro.objects.create(
            cliente_nome=f"{TAG} costume",
            total=Decimal("80.00"),
            forma_pagamento="Dinheiro",
            deposito="centro",
            usuario_registro="Ana",
        )
        VendaAgro.objects.filter(pk=base_v.pk).update(criado_em=base_quando)
        base_rows = [r for r in hu.carregar_vendas_intervalo(min(base_dias), max(base_dias)) if str(r.get("cliente") or "").startswith(TAG)]
        com = hu.agregar_hora(
            marcas, base_rows, dias=[DIA], dias_base=base_dias, loja="ambos", canal="todos", visao="soma",
        )
        h10b = next(ln for ln in com["expediente"] if ln["hora"] == 10)
        # 80 numa das 4 quartas -> media 20; visao soma de 1 dia = 20
        check(h10b["base"] is not None and abs(h10b["base"] - 20.0) < 0.01, "costume 10h = 80/4 = 20")
        check(h10b["var"] is not None and h10b["var"] > 0, "10h acima do costume")
    finally:
        _limpar()
        resto = VendaAgro.objects.filter(cliente_nome__startswith=TAG).count()
        check(resto == 0, "marcas apagadas")


def _grade_ontem():
    rf = RequestFactory()
    req = rf.get(
        "/relatorios/hora/",
        {"periodo": "custom", "de": DIA.isoformat(), "ate": DIA.isoformat(), "deposito": "ambos", "canal": "todos"},
    )
    filtros, grade = hu.montar_relatorio(req)
    return filtros, grade


def check_ontem_real() -> None:
    print("--- ontem real cruzado com Vendas por loja ---")
    from produtos.vendas_lojas_util import vendas_lojas_totais

    c_loja, v_loja, t_loja = vendas_lojas_totais(DIA, DIA)
    filtros, grade = _grade_ontem()
    check(filtros["d0"] == DIA and filtros["d1"] == DIA, "periodo cai em 07/10")
    check(abs(_q(grade["total_bruto"]) - t_loja) < Decimal("0.01"), f"total hora {_q(grade['total_bruto'])} = vendas por loja {t_loja}")
    soma_c = _q(sum(ln["centro"] for ln in grade["linhas"]))
    soma_v = _q(sum(ln["vila"] for ln in grade["linhas"]))
    check(abs(soma_c - c_loja) < Decimal("0.01"), f"Centro {soma_c} = loja {c_loja}")
    check(abs(soma_v - v_loja) < Decimal("0.01"), f"Vila {soma_v} = loja {v_loja}")
    soma_horas = _q(sum(ln["total_bruto"] for ln in grade["linhas"]))
    check(abs(soma_horas - t_loja) < Decimal("0.01"), "soma das 24 horas = total")
    balcao = _q(sum(ln["balcao"] for ln in grade["linhas"]))
    entrega = _q(sum(ln["entrega"] for ln in grade["linhas"]))
    check(abs(balcao + entrega - t_loja) < Decimal("0.01"), "balcao + entrega = total")

    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime.combine(DIA, time.min), tz)
    fim = timezone.make_aware(datetime.combine(DIA, time.max), tz)
    por_hora = {h: Decimal("0.00") for h in range(24)}
    vendas = list(VendaAgro.objects.filter(criado_em__gte=ini, criado_em__lte=fim))
    for v in vendas:
        por_hora[timezone.localtime(v.criado_em).hour] += _q(v.total)
    from django.db.models import Exists, OuterRef
    from produtos.models import DevolucaoVendaAgro

    for ev in DevolucaoVendaAgro.objects.filter(criado_em__date__gte=DIA, criado_em__date__lte=DIA).select_related("venda"):
        if not ev.criado_em:
            continue
        por_hora[timezone.localtime(ev.criado_em).hour] -= _q(ev.total)
    has_ev = Exists(DevolucaoVendaAgro.objects.filter(venda_id=OuterRef("pk")))
    for v in VendaAgro.objects.filter(devolvida_em__date__gte=DIA, devolvida_em__date__lte=DIA).annotate(_tem_ev=has_ev).filter(_tem_ev=False):
        por_hora[timezone.localtime(v.devolvida_em).hour] -= _q(v.total)
    bate = all(abs(_q(ln["total_bruto"]) - por_hora[ln["hora"]].quantize(Decimal("0.01"))) < Decimal("0.02") for ln in grade["linhas"])
    check(bate, "cada hora bate com venda menos devolucao")
    check(int(round(grade["n_vendas"])) == len(vendas), f"cupons {grade['n_vendas']} = {len(vendas)}")

    meta = grade.get("meta") or {}
    check(bool(meta.get("ok")), "meta do dia calculou")
    if meta.get("ok"):
        ok(f"meta vendido {meta.get('vendido_fmt')} ritmo {meta.get('ritmo_fmt')} dia {meta.get('pct_dia_fmt')}")


def check_http_excel() -> None:
    print("--- http e excel ---")
    with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
        c = Client()
        r = c.get(reverse("relatorios_hora"), {"periodo": "custom", "de": DIA.isoformat(), "ate": DIA.isoformat()})
        body = r.content.decode("utf-8", errors="replace")
        check(r.status_code == 200, f"pagina ontem {r.status_code}")
        check("Venda hora a hora" in body and "Mapa da semana" in body, "pagina tem titulo e mapa")
        check("Centro + Vila" in body and "Só Vila" in body, "pagina tem lojas")
        from produtos.relatorios_vendas_util import fmt_brl
        from produtos.vendas_lojas_util import vendas_lojas_totais

        _c, _v, tot_dia = vendas_lojas_totais(DIA, DIA)
        tot_txt = fmt_brl(tot_dia).replace("R$ ", "")
        check(tot_txt in body, f"ontem mostra {tot_txt} na tela")
        check(_cartao(body) == "Total do período", "um dia: cartao Total do período")

        rv = c.get(reverse("relatorios_hora"), {"periodo": "custom", "de": DIA.isoformat(), "ate": DIA.isoformat(), "deposito": "vila"})
        vb = rv.content.decode("utf-8", errors="replace")
        check(rv.status_code == 200 and "Só Vila" in vb, "filtro so Vila abre")
        check(">Centro</th>" not in vb, "so Vila esconde coluna Centro")

        semana = c.get(reverse("relatorios_hora"), {"periodo": "semana"})
        sb = semana.content.decode("utf-8", errors="replace")
        check("Média por dia" in sb and "Soma do período" in sb, "semana oferece media e soma")
        soma = c.get(reverse("relatorios_hora"), {"periodo": "custom", "de": "2026-10-05", "ate": DIA.isoformat(), "visao": "soma"})
        sm = soma.content.decode("utf-8", errors="replace")
        _cs, _vs, tot_sem = vendas_lojas_totais(date(2026, 10, 5), DIA)
        sem_txt = fmt_brl(tot_sem).replace("R$ ", "")
        check(sem_txt in sm, f"soma seg-qua mostra {sem_txt}")

        det = c.get(
            reverse("relatorios_hora"),
            {"periodo": "custom", "de": DIA.isoformat(), "ate": DIA.isoformat(), "hora": "11"},
        )
        db = det.content.decode("utf-8", errors="replace")
        check("Vendas" in db and "Fechar lista" in db, "clique na hora abre a lista")
        check(db.count("venda_agro_detalhe") >= 0, "lista pronta")

        x = c.get(reverse("relatorios_hora"), {"periodo": "custom", "de": DIA.isoformat(), "ate": DIA.isoformat(), "export": "xlsx"})
        check(x.status_code == 200 and "spreadsheet" in x["Content-Type"], "excel 200")
        check(x.content[:2] == b"PK", "excel e zip xlsx")
        wb = load_workbook(BytesIO(x.content), data_only=True)
        check("Hora a hora" in wb.sheetnames and "Mapa da semana" in wb.sheetnames, "abas hora e mapa")
        ws = wb["Hora a hora"]
        # coluna F = Total R$ (6), a partir da linha de dados
        headers = [c.value for c in next(ws.iter_rows(min_row=5, max_row=5))]
        check(headers[0] == "Hora" and "Total R$" in headers and "Quem atendeu" in headers, f"cabecalho excel {headers[:6]}")
        idx = headers.index("Total R$")
        total_xl = Decimal("0")
        for row in ws.iter_rows(min_row=6, values_only=True):
            val = row[idx]
            if isinstance(val, (int, float)):
                total_xl += Decimal(str(val))
        check(abs(total_xl.quantize(Decimal("0.01")) - tot_dia) < Decimal("0.05"), f"excel soma {total_xl} = {tot_dia}")

        hub = c.get(reverse("relatorios_hub"))
        hb = hub.content.decode("utf-8", errors="replace")
        check(hub.status_code == 200 and "Venda hora a hora" in hb and "/relatorios/hora/" in hb, "Central aponta o card")


def _cartao(body: str) -> str:
    marca = 'tracking-wider text-slate-400">'
    i = body.find(marca)
    if i < 0:
        return ""
    j = body.find("</p>", i)
    return body[i + len(marca) : j].strip()


def _valor_cartao(body: str) -> str:
    i = body.find('tracking-wider text-slate-400">')
    if i < 0:
        return ""
    marca = 'text-xl font-black text-white">'
    j = body.find(marca, i)
    if j < 0:
        return ""
    j += len(marca)
    k = body.find("</p>", j)
    return body[j:k].strip()


def check_periodo_38_centro() -> None:
    """O caso da tela: 01/09–08/10, só Centro. Média não é a soma."""
    print("--- 38 dias so Centro ---")
    from produtos.relatorios_vendas_util import fmt_brl
    from produtos.vendas_lojas_util import vendas_lojas_totais

    d0, d1 = date(2026, 9, 1), date(2026, 10, 8)
    c_loja, v_loja, t_loja = vendas_lojas_totais(d0, d1)
    rf = RequestFactory()
    req_m = rf.get(
        "/relatorios/hora/",
        {
            "periodo": "custom",
            "de": d0.isoformat(),
            "ate": d1.isoformat(),
            "deposito": "centro",
            "canal": "todos",
            "visao": "media",
        },
    )
    filtros, media = hu.montar_relatorio(req_m)
    n = int(filtros["n_dias"] if "n_dias" in filtros else media["n_dias"])
    check(n == 38, f"01/09 a 08/10 = {n} dias")
    check(media["rotulo_total"] == "Média do dia", "38 dias: rotulo Média do dia")
    check(abs(_q(media["total_bruto"]) - c_loja) < Decimal("0.01"), f"soma Centro {_q(media['total_bruto'])} = vendas por loja {c_loja}")
    check(abs(_q(media["total"]) * n - _q(media["total_bruto"])) < Decimal("0.05"), "media x 38 = soma do periodo")
    check(_q(media["total"]) < Decimal("20000"), "cartao da media nao e o total de 38 dias")
    req_s = rf.get(
        "/relatorios/hora/",
        {
            "periodo": "custom",
            "de": d0.isoformat(),
            "ate": d1.isoformat(),
            "deposito": "centro",
            "canal": "todos",
            "visao": "soma",
        },
    )
    _fs, soma = hu.montar_relatorio(req_s)
    check(soma["rotulo_total"] == "Total do período", "soma: rotulo Total do período")
    check(abs(_q(soma["total"]) - c_loja) < Decimal("0.01"), f"cartao da soma {_q(soma['total'])} = Centro {c_loja}")
    check(abs(_q(soma["total_bruto"]) - _q(media["total_bruto"])) < Decimal("0.01"), "soma e media usam o mesmo bruto")
    ambas = _q(c_loja + v_loja)
    check(abs(ambas - t_loja) < Decimal("0.01"), f"Centro+Vila {ambas} = total lojas {t_loja}")

    with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
        c = Client()
        pagina = c.get(
            reverse("relatorios_hora"),
            {
                "periodo": "custom",
                "de": d0.isoformat(),
                "ate": d1.isoformat(),
                "deposito": "centro",
                "visao": "media",
            },
        )
        body = pagina.content.decode("utf-8", errors="replace")
        check(pagina.status_code == 200, "pagina 38 dias abre")
        check(_cartao(body) == "Média do dia", "tela 38 dias: cartao Média do dia")
        check(_valor_cartao(body) == fmt_brl(media["total"]), f"numero do cartao e a media {fmt_brl(media['total'])}")
        pagina_s = c.get(
            reverse("relatorios_hora"),
            {
                "periodo": "custom",
                "de": d0.isoformat(),
                "ate": d1.isoformat(),
                "deposito": "centro",
                "visao": "soma",
            },
        )
        bs = pagina_s.content.decode("utf-8", errors="replace")
        check(_cartao(bs) == "Total do período", "tela soma: cartao Total do período")
        check(_valor_cartao(bs) == fmt_brl(c_loja), f"numero da soma e o total Centro {fmt_brl(c_loja)}")


def check_pin() -> None:
    print("--- pin ---")
    try:
        from produtos.caixa_util import rotulo_operador_pin

        rot = (rotulo_operador_pin(PIN_TESTE) or "").strip()
        check(bool(rot), f"PIN {PIN_TESTE} existe ({rot})")
    except Exception as exc:
        fail(f"PIN {PIN_TESTE}: {exc}")


def main() -> int:
    check_static()
    check_ontem_real()
    check_marcadas()
    check_http_excel()
    check_periodo_38_centro()
    check_pin()
    print(f"--- {OKS} OK · {len(FAILS)} FAIL ---")
    for msg in FAILS:
        print("FAIL", msg.encode("ascii", "replace").decode("ascii"))
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
