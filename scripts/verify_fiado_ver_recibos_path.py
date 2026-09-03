"""
Prova detalhada — Fiado UX (`FIADO-VER-RECIBOS` + hotfixes overlay).

Path:
  /fiado/ lista · KPIs · Limite na coluna
  → modal cliente tela cheia · hideChrome / setNested
  → Esc/F1 com cliente aberto NÃO fecha overlay PDV (_atalho_voltar_pdv)
  → Pedido/Ver → overlay iframe venda (não location.href)
  → sem venda = Sistema antigo
  → Recibos em modal
  → tabela lançamentos compacta (table-layout fixed)

  python scripts/verify_fiado_ver_recibos_path.py
"""
from __future__ import annotations

import json
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

from django.contrib.auth import get_user_model
from django.test import Client
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
    atalho = _read("produtos/templates/produtos/_atalho_voltar_pdv.html")
    venda = _read("produtos/templates/produtos/venda_agro_detalhe.html")
    stack = _read("produtos/static/produtos/js/agro_overlay_stack.js")

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

    check("pedido_link_js", "fiado-link-pedido" in js and "abrirVendaOverlay" in js)
    check("btn_ver_js", "fiado-btn-ver-tit" in js)
    check("sistema_antigo", "Sistema antigo" in js)
    check("venda_detalhe_url", "vendaDetalheBase" in html and "vendaDetalheUrl" in js)
    check("venda_overlay_modal", 'id="fiado-modal-venda"' in html and 'id="fiado-venda-frame"' in html)
    check("venda_overlay_js", "abrirVendaOverlay" in js and "fecharVendaOverlay" in js)
    check("venda_sem_location_href", "window.location.href = url" not in js)
    check("venda_embed_param", "agro_fiado_embed" in js)
    check("venda_embed_html", "agro-venda-fiado-embed" in venda and "fiado-venda-overlay-close" in venda)

    check("recibos_botao", 'id="fiado-btn-recibos"' in html)
    check("recibos_modal", 'id="fiado-modal-recibos"' in html)
    check("recibos_sem_box_fixa", 'id="fiado-recibos-box"' not in html)
    check("recibos_js_open", "btnRecibos" in js and "modalRecibos" in js)

    check("modal_fullscreen", "position: fixed" in html and "fiado-modal-panel" in html)
    check("modal_sem_borda_arred", "border-radius: 0 !important" in html)
    check("esconde_header_main", "body.fiado-modal-aberto > header" in html)
    check("btn_volta_lista", "Lista fiado" in html or "fiado-cli-modal-fechar" in html)

    check("tit_vertical", ".fiado-tit-table th:not(:last-child)" in html and "border-right" in html)
    check("tit_fixed_layout", "table-layout: fixed" in html and "fiado-tit-c-acoes" in html)
    check("tit_acoes_grade", ".fiado-tit-acoes" in html and "fiado-tit-acoes" in js)
    check("moeda_html", "fmtMoedaHtml" in js and "fiado-moeda" in html)

    check("overlay_hide_chrome", "hideChrome" in overlay and "is-chrome-hidden" in overlay)
    check("overlay_styles_v7", "agro-pdv-overlay-styles-v7" in overlay)
    check("overlay_chrome_locked", "chromeLocked" in overlay)
    check("fiado_post_hide", "setNested" in js or "hideChrome: !!on" in js)
    check("overlay_in_iframe", "agro-fiado-in-overlay" in html)
    check("stack_engine", "AgroOverlayStack" in stack and "setNested" in stack)
    check("atalho_nested_esc", "hasNestedLayer" in atalho and "fiado-modal-aberto" in atalho)


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
        check("titulo_amostra", True, "sem titulos — skip shape")
        return
    d = titulo_para_dict(t)
    check("dict_tem_venda_agro_id", "venda_agro_id" in d)
    check("dict_tem_saldo", "saldo_aberto" in d and "numero_documento" in d)


def test_http_apis() -> None:
    print("== HTTP APIs ==")
    c = Client(HTTP_HOST="127.0.0.1")
    User = get_user_model()
    u = User.objects.filter(username="Renan").first() or User.objects.first()
    if not u:
        check("http_user", False, "sem usuario")
        return
    c.force_login(u)

    r = c.get("/fiado/")
    body = r.content.decode("utf-8", "replace")
    check("http_fiado_200", r.status_code == 200)
    check("http_fiado_kpi", "fiado-kpi-vendido-mes" in body)
    check("http_fiado_venda_modal", "fiado-modal-venda" in body)
    check("http_fiado_lista_btn", "Lista fiado" in body or "fiado-cli-modal-fechar" in body)

    r2 = c.get("/api/fiado/resumo/")
    check("http_resumo_200", r2.status_code == 200)
    if r2.status_code == 200:
        j = r2.json()
        check("http_resumo_ok", j.get("ok") is True and "vendido_mes" in j)

    r3 = c.get("/api/fiado/clientes/")
    check("http_clientes_200", r3.status_code == 200)
    if r3.status_code == 200:
        j3 = r3.json()
        rows = j3.get("clientes") or []
        check("http_clientes_lista", isinstance(rows, list))
        if rows:
            check("http_cliente_limite_key", "limite_fiado_local" in rows[0])

    tit = (
        FiadoTituloAgro.objects.exclude(situacao=FiadoTituloAgro.Situacao.CANCELADO)
        .filter(venda_agro_id__isnull=False)
        .order_by("-pk")
        .first()
    )
    if tit and tit.cliente_agro_id:
        r4 = c.get("/api/fiado/titulos/", {"cliente_agro_pk": tit.cliente_agro_id})
        check("http_titulos_200", r4.status_code == 200)
        if r4.status_code == 200:
            ts = r4.json().get("titulos") or []
            mine = [t for t in ts if t.get("id") == tit.pk]
            check("http_tit_venda_id", bool(mine and mine[0].get("venda_agro_id")))
            vid = mine[0].get("venda_agro_id") if mine else None
            if vid:
                r5 = c.get(f"/venda/{vid}/", {"agro_fiado_embed": "1"})
                check("http_venda_embed_200", r5.status_code == 200)
                vb = r5.content.decode("utf-8", "replace")
                check(
                    "http_venda_embed_js",
                    "agro_fiado_embed" in vb or "agro-venda-fiado-embed" in vb,
                )

        r6 = c.get("/api/fiado/recibos/", {"cliente_agro_pk": tit.cliente_agro_id})
        check("http_recibos_200", r6.status_code == 200)

        payload = {
            "cliente_agro_pk": tit.cliente_agro_id,
            "limite": float(tit.cliente_agro.limite_fiado_local or 0),
            "pin": "9973",
        }
        r7 = c.post(
            "/api/fiado/limite/",
            data=json.dumps(payload),
            content_type="application/json",
        )
        check(
            "http_limite_200",
            r7.status_code == 200,
            (r7.content[:120] or b"").decode("utf-8", "replace"),
        )
    else:
        check("http_tit_com_venda", True, "skip — sem titulo com venda")


def main() -> int:
    print("verify_fiado_ver_recibos_path")
    test_arquivos()
    test_kpis_runtime()
    test_titulo_venda_id_api_shape()
    test_http_apis()
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
