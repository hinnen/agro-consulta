"""
Prova detalhada — Fiado UX cliente (`FIADO-VER-RECIBOS`).

Path:
  /fiado/ lista compacta · KPIs mês/mês ant · Limite na coluna
  → modal cliente tela cheia · hideChrome overlay PDV
  → Pedido clicável + Ver → popup /venda/<id>/
  → sem venda = Sistema antigo
  → Recibos em modal (não lista fixa)
  → tabela lançamentos com linha vertical + grade Baixa/Ver/Editar

  Sem migrate.

  python scripts/verify_fiado_ver_recibos_path.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.utils import timezone

from produtos.fiado_gestao_util import _kpis_mensais_fiado, resumo_gestao_fiado
from produtos.models import FiadoBaixaAgro, FiadoTituloAgro

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Arquivos / contratos ==")
    html = _read("produtos/templates/produtos/fiado_gestao.html")
    js = _read("produtos/static/produtos/js/fiado_gestao.js")
    util = _read("produtos/fiado_gestao_util.py")
    overlay = _read("produtos/static/produtos/js/agro_pdv_overlay.js")

    check("kpi_html_vendido_mes", 'id="fiado-kpi-vendido-mes"' in html and "vendido_mes" in html)
    check("kpi_html_vendido_ant", 'id="fiado-kpi-vendido-ant"' in html)
    check("kpi_html_pago_mes", 'id="fiado-kpi-pago-mes"' in html and "pago_mes" in html)
    check("kpi_html_pago_ant", 'id="fiado-kpi-pago-ant"' in html)
    check("kpi_util_fn", "def _kpis_mensais_fiado" in util)
    check("kpi_resumo_gestao", "resumo_gestao_fiado" in util and "**_kpis_mensais_fiado()" in util)
    check("kpi_js_atualizar", "kpiVendidoMes" in js and "vendido_mes_anterior" in js)

    check("limite_coluna_th", ">Limite<" in html or ">Limite</th>" in html)
    check("limite_input_js", "fiado-limite-input" in js and "gravarLimiteNaLinha" in js)
    check("limite_css", ".fiado-limite-input" in html)

    check("pedido_link_js", "fiado-link-pedido" in js and "abrirVendaPopup" in js)
    check("btn_ver_js", "fiado-btn-ver-tit" in js)
    check("sistema_antigo", "Sistema antigo" in js)
    check("venda_detalhe_url", "vendaDetalheBase" in html and "vendaDetalheUrl" in js)

    check("recibos_botao", 'id="fiado-btn-recibos"' in html)
    check("recibos_modal", 'id="fiado-modal-recibos"' in html)
    check("recibos_sem_box_fixa", 'id="fiado-recibos-box"' not in html)
    check("recibos_js_open", "btnRecibos" in js and "modalRecibos" in js)

    check("modal_fullscreen", "position: fixed" in html and "fiado-modal-panel" in html)
    check("modal_sem_borda_arred", "border-radius: 0 !important" in html)
    check("esconde_header_main", "body.fiado-modal-aberto > header" in html)

    check("tit_vertical", ".fiado-tit-table th:not(:last-child)" in html and "border-right" in html)
    check("tit_acoes_grade", ".fiado-tit-acoes" in html and "fiado-tit-acoes" in js)
    check("moeda_html", "fmtMoedaHtml" in js and "fiado-moeda" in html)

    check("overlay_hide_chrome", "hideChrome" in overlay and "is-chrome-hidden" in overlay)
    check("overlay_styles_v6", "agro-pdv-overlay-styles-v6" in overlay)
    check("fiado_post_hide", "hideChrome: !!on" in js)
    check("overlay_in_iframe", "agro-fiado-in-overlay" in html)


def test_kpis_runtime() -> None:
    print("== KPIs runtime ==")
    k = _kpis_mensais_fiado()
    for key in ("vendido_mes", "vendido_mes_anterior", "pago_mes", "pago_mes_anterior"):
        check(f"kpi_key_{key}", key in k and isinstance(k[key], float) and k[key] >= 0)

    r = resumo_gestao_fiado()
    check("resumo_tem_kpis", all(k in r for k in ("vendido_mes", "pago_mes", "total_saldo_aberto")))
    check("resumo_clientes", "clientes_com_saldo" in r)

    hoje = timezone.localdate()
    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime(hoje.year, hoje.month, 1, 0, 0, 0), tz)
    if hoje.month == 12:
        fim = timezone.make_aware(datetime(hoje.year + 1, 1, 1, 0, 0, 0), tz)
    else:
        fim = timezone.make_aware(datetime(hoje.year, hoje.month + 1, 1, 0, 0, 0), tz)

    from django.db.models import Sum
    from django.db.models.functions import Coalesce

    vendido_db = FiadoTituloAgro.objects.exclude(
        situacao=FiadoTituloAgro.Situacao.CANCELADO
    ).filter(criado_em__gte=ini, criado_em__lt=fim).aggregate(
        t=Coalesce(Sum("valor_bruto"), Decimal("0"))
    )["t"]
    pago_db = FiadoBaixaAgro.objects.filter(criado_em__gte=ini, criado_em__lt=fim).aggregate(
        t=Coalesce(Sum("valor"), Decimal("0"))
    )["t"]
    check(
        "kpi_vendido_bate_db",
        abs(float(vendido_db) - float(k["vendido_mes"])) < 0.02,
        f"util={k['vendido_mes']} db={float(vendido_db)}",
    )
    check(
        "kpi_pago_bate_db",
        abs(float(pago_db) - float(k["pago_mes"])) < 0.02,
        f"util={k['pago_mes']} db={float(pago_db)}",
    )


def test_titulo_venda_id_api_shape() -> None:
    print("== Titulo venda_agro_id ==")
    from produtos.fiado_gestao_util import titulo_para_dict

    t = (
        FiadoTituloAgro.objects.exclude(situacao=FiadoTituloAgro.Situacao.CANCELADO)
        .order_by("-pk")
        .first()
    )
    if not t:
        check("titulo_amostra", True, "sem títulos — skip shape")
        return
    d = titulo_para_dict(t)
    check("dict_tem_venda_agro_id", "venda_agro_id" in d)
    check("dict_tem_saldo", "saldo_aberto" in d and "numero_documento" in d)


def main() -> int:
    print("verify_fiado_ver_recibos_path")
    test_arquivos()
    test_kpis_runtime()
    test_titulo_venda_id_api_shape()
    print()
    if fails:
        print(f"FALHOU: {len(fails)} falha(s), {len(oks)} ok")
        for f in fails:
            print(f"  - {f}")
        return 1
    print(f"OK verify_fiado_ver_recibos_path — {len(oks)}/{len(oks)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
