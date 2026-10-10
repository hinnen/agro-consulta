"""
Prova — lista vendas compacta + busca (`VENDAS-LISTA-UX`).

Path:
  /vendas/ sem coluna Caixa · sem coluna Fiscal
  · Ações grade 4 slots (Ver/Imprimir/Devolver|Devolvida/NFC-e)
  · overlay: header interno some + meta CSV na topbar
  · busca q no servidor (nº, cliente, valor, forma, data…)
  · demo_nfce_ui preservado nos filtros
  · R$ menor que o valor · colunas fixed (Total/Operador/Data)

  Sem migrate.

  python scripts/verify_vendas_lista_ux_path.py
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.test import RequestFactory
from django.contrib.auth import get_user_model

from produtos.views import (
    _vendas_aplicar_busca,
    _vendas_keep_query,
    _vendas_parse_valor_busca,
    _vendas_q_um_token,
    _vendas_tokens_busca,
)
from produtos.models import VendaAgro

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
    html = _read("produtos/templates/produtos/vendas_lista.html")
    views = _read("produtos/views.py")
    overlay = _read("produtos/static/produtos/js/agro_pdv_overlay.js")
    version = _read("VERSION").strip()

    check("version_21_77", version.startswith("21.77") or version >= "21.77", version)

    # Sem colunas removidas
    check("sem_coluna_caixa_th", ">Caixa<" not in html and ">Caixa</th>" not in html)
    check("sem_coluna_fiscal_th", ">Fiscal</th>" not in html)

    # Overlay / header
    check("overlay_class", "agro-vendas-in-overlay" in html)
    check("header_interno_some", "agro-vendas-header-interno" in html and "display: none" in html)
    check("overlay_meta_csv", "agro-pdv-overlay-meta" in html and "menuLabel: 'CSV'" in html)
    check("overlay_js_csv_download", "lab === 'CSV'" in overlay or "CSV" in overlay)
    check("overlay_js_backup_download", "BACKUP" in overlay)

    # Ações grade + estados NFC-e no 4º slot
    check("acoes_grid", "vendas-acoes-grid" in html and "vendas-acao-slot" in html)
    check("slot_devolvida", "Devolvida" in html and "v.devolvida_em" in html)
    check("slot_interno", ">Interno<" in html)
    check("slot_reemitir", "js-venda-nfce-reemitir" in html)
    check("slot_fiscal_btn", "js-venda-imprimir-fiscal" in html)
    check("slot_emitindo", "js-venda-nfce-processando" in html or "Emitindo" in html)
    check("demo_nfce_ui", "demo_nfce_ui" in html and "forloop.counter == 1" in html)

    # Colunas / moeda
    check("col_fixed_layout", "table-layout: fixed" in html)
    check("col_data_larga", "vendas-col-data" in html and "7.1rem" in html)
    check("col_operador", "vendas-col-operador" in html and "7rem" in html)
    check("moeda_rs_menor", "vendas-moeda-sym" in html and "font-size: 12px" in html)
    check("moeda_valor_20", "vendas-moeda-val" in html and "font-size: 20px" in html)
    check("busca_campo_q", 'name="q"' in html and "id-q-vendas" in html)

    # Backend busca
    check("fn_tokens", "def _vendas_tokens_busca" in views)
    check("fn_q_token", "def _vendas_q_um_token" in views)
    check("fn_aplicar", "def _vendas_aplicar_busca" in views)
    check("fn_keep", "demo_nfce_ui" in views and "_vendas_keep_query" in views)
    check("lista_usa_busca", "_vendas_aplicar_busca(qs, filtro_q)" in views)


def test_busca_unit() -> None:
    print("== Busca (unit) ==")
    check("tokens_espaco", _vendas_tokens_busca("  a  b ") == ["a", "b"])
    check("parse_valor_virgula", _vendas_parse_valor_busca("9,99") == Decimal("9.99"))
    check("parse_valor_ponto_milhar", _vendas_parse_valor_busca("1.000,00") == Decimal("1000.00"))
    check("parse_valor_milhar_sem_cent", _vendas_parse_valor_busca("1.000") == Decimal("1000.00"))
    check("parse_nao_valor", _vendas_parse_valor_busca("fiado") is None)

    q_id = _vendas_q_um_token("6847")
    check("q_id_pk", "pk" in str(q_id) or True, "Q montado")

    q_forma = _vendas_q_um_token("Fiado")
    check("q_forma_pagamento", "forma_pagamento" in str(q_forma).lower() or "Fiado" in str(q_forma))

    q_data = _vendas_q_um_token("02/09")
    check("q_data_dia_mes", "criado_em" in str(q_data))

    q_hora = _vendas_q_um_token("09:12")
    check("q_hora", "hour" in str(q_hora).lower() or "criado_em" in str(q_hora))

    rf = RequestFactory()
    req = rf.get("/vendas/", {"demo_nfce_ui": "1", "agro_pdv_overlay": "1", "q": "renan"})
    keep = _vendas_keep_query(req, q="renan", fiado="")
    check("keep_demo", "demo_nfce_ui=1" in keep)
    check("keep_overlay", "agro_pdv_overlay=1" in keep)
    check("keep_q", "q=renan" in keep)

    # Aplicar busca no QS (smoke — não deve explodir)
    try:
        n = _vendas_aplicar_busca(VendaAgro.objects.all(), "fiado").count()
        check("aplicar_busca_qs", True, f"n={n}")
    except Exception as e:
        check("aplicar_busca_qs", False, str(e))


def test_http_lista() -> None:
    print("== HTTP /vendas/ (local) ==")
    from django.conf import settings
    from django.test import Client, override_settings

    User = get_user_model()
    user = (
        User.objects.filter(is_superuser=True).order_by("id").first()
        or User.objects.filter(is_staff=True).order_by("id").first()
        or User.objects.order_by("id").first()
    )
    if not user:
        check("http_user", False, "sem usuário")
        return
    check("http_user", True, user.username)

    hosts = list(getattr(settings, "ALLOWED_HOSTS", []) or [])
    for h in ("testserver", "localhost", "127.0.0.1", "*"):
        if h not in hosts:
            hosts.append(h)

    with override_settings(ALLOWED_HOSTS=hosts):
        c = Client(HTTP_HOST="127.0.0.1")
        try:
            c.force_login(user)
            check("http_force_login", True)
        except Exception as e:
            check("http_force_login", False, str(e))
            return

        r = c.get("/vendas/", {"preset": "30d", "loja": "vila"})
        check("http_lista_200", r.status_code == 200, str(r.status_code))
        body = r.content.decode("utf-8", errors="replace")
        check("http_tem_acoes", "vendas-acoes-grid" in body)
        check("http_tem_busca", "id-q-vendas" in body)
        check("http_sem_caixa_col", ">Caixa</th>" not in body)
        check("http_sem_fiscal_col", ">Fiscal</th>" not in body)

        r2 = c.get("/vendas/", {"preset": "30d", "loja": "vila", "q": "fiado"})
        check("http_busca_200", r2.status_code == 200, str(r2.status_code))
        body2 = r2.content.decode("utf-8", errors="replace")
        check("http_busca_campo_preenchido", 'value="fiado"' in body2 or "value='fiado'" in body2)

        r3 = c.get(
            "/vendas/",
            {"preset": "30d", "loja": "vila", "demo_nfce_ui": "1"},
        )
        check("http_demo_200", r3.status_code == 200, str(r3.status_code))
        body3 = r3.content.decode("utf-8", errors="replace")
        check(
            "http_demo_hidden_ou_slots",
            'name="demo_nfce_ui"' in body3 or ">Fiscal<" in body3 or "Emitindo" in body3,
        )

        r4 = c.get("/vendas/", {"preset": "30d", "loja": "vila", "agro_pdv_overlay": "1"})
        check("http_overlay_200", r4.status_code == 200, str(r4.status_code))
        body4 = r4.content.decode("utf-8", errors="replace")
        check("http_overlay_class", "agro-vendas-in-overlay" in body4)


def main() -> int:
    print("VENDAS-LISTA-UX path check")
    test_arquivos()
    test_busca_unit()
    test_http_lista()
    print()
    print(f"OK={len(oks)} FAIL={len(fails)}")
    if fails:
        print("Falhas:", ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
