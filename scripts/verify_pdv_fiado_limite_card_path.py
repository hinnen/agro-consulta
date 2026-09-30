# -*- coding: utf-8 -*-
"""
Prova — limite do fiado no card de Saldos do PDV (`PDV-FIADO-LIMITE-CARD`).

  python scripts/verify_pdv_fiado_limite_card_path.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_arquivos() -> None:
    print("== Contratos UI / JS ==")
    html_side = (ROOT / "produtos/templates/produtos/partials/pdv/step_produtos.html").read_text(
        encoding="utf-8"
    )
    html = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    views = (ROOT / "produtos/fiado_gestao_views.py").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    boot = (ROOT / "pdv/views.py").read_text(encoding="utf-8")

    check("card_fiado", 'id="pdv-fiado-gestao-open"' in html_side and ">Fiado<" in html_side)
    check("card_limite", 'id="pdv-fiado-limite-open"' in html_side)
    check("card_dois_valores", "pdv-product-fiado-balance" in html_side and "pdv-product-fiado-limite" in html_side)
    check("modal", 'id="pdv-fiado-limite-modal"' in html)
    check("botoes", 'id="pdv-fiado-limite-menos"' in html and 'id="pdv-fiado-limite-mais"' in html)
    check("campo", 'id="pdv-fiado-limite-input"' in html)
    check("css_atraso", "pdv-fiado-usado--atraso" in html)
    check("js_passo", "FIADO_LIMITE_PASSO = 100" in js)
    check("js_abre", "function openFiadoLimiteModal" in js)
    check("js_grava", "function gravarLimiteFiadoPdv" in js)
    check("js_vermelho", "pdv-fiado-usado--atraso" in js)
    check("js_pin", "PIN para mudar o limite" in js and "apiPdvFiadoLimite" in js)
    check("js_sem_cliente_abre_fiado", "openFiadoGestao()" in js)
    check("api", "def api_pdv_fiado_limite" in views and "validar_pin_gerencial" in views)
    check("url", "api/pdv/fiado-limite/" in urls and "api_pdv_fiado_limite" in urls)
    check("boot", "apiPdvFiadoLimite" in boot)
    fiado_html = (ROOT / "produtos/templates/produtos/fiado_gestao.html").read_text(encoding="utf-8")
    fiado_js = (ROOT / "produtos/static/produtos/js/fiado_gestao.js").read_text(encoding="utf-8")
    check("cli_par", "fiado-cli-par" in fiado_html and "fiado-cli-pill-limite" in fiado_html)
    check("cli_popup", "fiado-modal-limite-cli" in fiado_html and "limitePdv" in fiado_html)
    check("cli_js", "function gravarLimiteCliente" in fiado_js and "function pintarParCliente" in fiado_js)


def test_node() -> None:
    print("== Sintaxe JS ==")
    try:
        r = subprocess.run(
            ["node", "--check", str(ROOT / "produtos/static/produtos/js/pdv_wizard.js")],
            capture_output=True,
            text=True,
            timeout=30,
        )
        check("node_check", r.returncode == 0, (r.stderr or "")[:120])
    except FileNotFoundError:
        check("node_check_skip", True, "node off")


def test_runtime() -> None:
    print("== Runtime Django ==")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.fiado_gestao_util import definir_limite_fiado_cliente
    from produtos.models import ClienteAgro

    cli = ClienteAgro.objects.order_by("pk").first()
    if not cli:
        check("cliente", False, "sem ClienteAgro")
        return
    check("cliente", True, f"pk={cli.pk}")
    anterior = Decimal(str(cli.limite_fiado_local or 0))
    User = get_user_model()
    u = User.objects.filter(is_active=True).order_by("pk").first()
    if not u:
        check("user", False, "sem usuario")
        return
    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(u)
    url = reverse("api_pdv_fiado_limite")
    try:
        r0 = c.post(
            url,
            data=json.dumps({"cliente_agro_pk": cli.pk, "limite": "100,00", "pin": "1234"}),
            content_type="application/json",
        )
        check("pin_padrao_recusa", r0.status_code == 403, str(r0.status_code))
        cli.refresh_from_db()
        check(
            "pin_padrao_nao_grava",
            Decimal(str(cli.limite_fiado_local or 0)) == anterior,
            str(cli.limite_fiado_local),
        )

        rneg = c.post(
            url,
            data=json.dumps({"cliente_agro_pk": cli.pk, "limite": "-10", "pin": "000000"}),
            content_type="application/json",
        )
        check("sem_pin_gerencial_nao_grava_negativo", rneg.status_code == 403, str(rneg.status_code))

        alvo = (anterior + Decimal("1.00")).quantize(Decimal("0.01"))
        with patch(
            "produtos.pin_gerencial_util.validar_pin_gerencial",
            return_value=(True, "Renan Hinnen", ""),
        ):
            rok = c.post(
                url,
                data=json.dumps(
                    {
                        "cliente_agro_pk": cli.pk,
                        "limite": f"{alvo:.2f}".replace(".", ","),
                        "pin": "1111",
                    }
                ),
                content_type="application/json",
            )
            rbad = c.post(
                url,
                data=json.dumps({"cliente_agro_pk": cli.pk, "limite": "-5", "pin": "1111"}),
                content_type="application/json",
            )
        check("pin_gerente_200", rok.status_code == 200, str(rok.status_code))
        body = rok.json()
        check("pin_gerente_ok", body.get("ok") is True and body.get("alterado_por") == "Renan Hinnen")
        check(
            "pin_gerente_valor",
            abs(float(body.get("limite_fiado_local") or 0) - float(alvo)) < 0.001,
            str(body.get("limite_fiado_local")),
        )
        check("credito_volta", isinstance(body.get("credito"), dict) and "usado" in body.get("credito", {}))
        check("negativo_400", rbad.status_code == 400, str(rbad.status_code))
    finally:
        definir_limite_fiado_cliente(cli.pk, anterior, usuario="verify-pdv-fiado-limite-restore")
        cli.refresh_from_db()
        check(
            "restore",
            Decimal(str(cli.limite_fiado_local or 0)) == anterior,
            str(anterior),
        )


def main() -> int:
    print("verify_pdv_fiado_limite_card_path")
    test_arquivos()
    test_node()
    try:
        test_runtime()
    except Exception as exc:
        check("runtime_crash", False, str(exc).split("\n")[0][:140])
    print()
    if fails:
        print(f"FALHOU: {len(fails)} falha(s), {len(oks)} ok")
        for name in fails:
            print(f"  - {name}")
        return 1
    print(f"OK verify_pdv_fiado_limite_card_path — {len(oks)}/{len(oks)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
