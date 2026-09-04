# -*- coding: utf-8 -*-
"""
Prova detalhada — Limite fiado na linha (`FIADO-LIMITE-LINHA`).

  python scripts/verify_fiado_limite_linha_path.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

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
    html = (ROOT / "produtos/templates/produtos/fiado_gestao.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/fiado_gestao.js").read_text(encoding="utf-8")
    util = (ROOT / "produtos/fiado_gestao_util.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/fiado_gestao_views.py").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")

    check("sem_btn_limite_cliente", 'id="fiado-btn-limite-avulso"' not in html and "Limite cliente" not in html)
    check("sem_modal_limite", 'id="fiado-modal-limite"' not in html)
    check("sem_form_avulso", 'id="fiado-form-limite-avulso"' not in html)
    check("th_hint", "Clique no valor da linha para editar o limite" in html)
    check("css_valor", ".fiado-limite-valor" in html and ".fiado-limite-cell" in html)
    check("css_input", ".fiado-limite-input" in html)
    check("css_lapis", 'content: "✎"' in html or "content:'✎'" in html.replace(" ", ""))
    check("js_render_botao", "fiado-limite-valor" in js and "data-valor" in js)
    check("js_iniciar", "function iniciarEdicaoLimite" in js)
    check("js_gravar", "function gravarLimiteNaLinha" in js and "salvarLimite" in js)
    check("js_finalizar", "function finalizarEdicaoLimite" in js)
    check("js_html_botao", "function htmlBotaoLimite" in js)
    check("js_enter_esc", "'Enter'" in js and "'Escape'" in js)
    check("js_focusout", "focusout" in js and "gravarLimiteNaLinha" in js)
    check("js_stop_row", "closest('.fiado-limite-valor')" in js and "stopPropagation" in js)
    check("js_sem_modal_avulso", "btnLimiteAvulso" not in js and "buscarClientesLimite" not in js)
    check("js_negativo", "Limite não pode ser negativo" in js)
    check("js_post_json", "cliente_agro_pk" in js and "urls.limite" in js)
    check("atualiza_cache", "row.limite_fiado_local = valorNum" in js)
    check("util_definir", "def definir_limite_fiado_cliente" in util)
    check("util_negativo", 'Limite não pode ser negativo.' in util)
    check("util_evento", "FiadoEventoAgro.Tipo.LIMITE" in util)
    check("api_view", "def api_fiado_limite" in views and "@require_POST" in views)
    check("api_url", "api/fiado/limite/" in urls)
    check("api_pg_campo", "limite_fiado_local" in views)


def test_node_syntax() -> None:
    print("== Sintaxe JS ==")
    try:
        r = subprocess.run(
            ["node", "--check", str(ROOT / "produtos/static/produtos/js/fiado_gestao.js")],
            capture_output=True,
            text=True,
            timeout=20,
        )
        check("node_check_js", r.returncode == 0, (r.stderr or "")[:80])
    except FileNotFoundError:
        check("node_check_skip", True, "node off")


def test_runtime() -> None:
    print("== Runtime Django (util + API) ==")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.caixa_util import validar_pin_operador
    from produtos.fiado_gestao_util import definir_limite_fiado_cliente
    from produtos.models import ClienteAgro

    pin_ok, pin_err = validar_pin_operador("9973")
    check("pin_9973", pin_ok, pin_err or "PIN ok (limite não exige PIN)")

    cli = ClienteAgro.objects.order_by("pk").first()
    if not cli:
        check("cliente_amostra", False, "sem ClienteAgro")
        return
    check("cliente_amostra", True, f"pk={cli.pk}")

    anterior = Decimal(str(cli.limite_fiado_local or 0))
    try:
        # negativo deve falhar
        try:
            definir_limite_fiado_cliente(cli.pk, Decimal("-1"), usuario="verify")
            check("util_rejeita_negativo", False)
        except ValueError:
            check("util_rejeita_negativo", True)

        alvo = (anterior + Decimal("0.01")).quantize(Decimal("0.01"))
        definir_limite_fiado_cliente(cli.pk, alvo, usuario="verify-limite-linha")
        cli.refresh_from_db()
        check("util_grava_pg", Decimal(str(cli.limite_fiado_local or 0)) == alvo, str(cli.limite_fiado_local))

        User = get_user_model()
        u = User.objects.filter(username="Renan").first() or User.objects.first()
        if not u:
            check("http_user", False, "sem usuario")
            return
        c = Client(HTTP_HOST="127.0.0.1")
        c.force_login(u)

        url_fiado = reverse("fiado_gestao")
        r_page = c.get(url_fiado)
        check("http_fiado_200", r_page.status_code == 200, str(r_page.status_code))
        body = r_page.content.decode("utf-8", "replace")
        check("http_fiado_sem_btn", "fiado-btn-limite-avulso" not in body)
        check("http_fiado_js_hook", "iniciarEdicaoLimite" in body or "fiado_gestao.js" in body)
        check("http_fiado_css_valor", "fiado-limite-valor" in body)

        # API POST — grava e restaura
        url_lim = reverse("api_fiado_limite")
        novo = float(alvo) + 0.02
        r_post = c.post(
            url_lim,
            data=json.dumps({"cliente_agro_pk": cli.pk, "limite": f"{novo:.2f}".replace(".", ",")}),
            content_type="application/json",
        )
        check("http_api_post_200", r_post.status_code == 200, str(r_post.status_code))
        try:
            j = r_post.json()
            check("http_api_ok", j.get("ok") is True, str(j)[:80])
            check(
                "http_api_valor",
                abs(float(j.get("limite_fiado_local") or 0) - novo) < 0.001,
                str(j.get("limite_fiado_local")),
            )
        except Exception as e:
            check("http_api_json", False, str(e)[:60])

        r_neg = c.post(
            url_lim,
            data=json.dumps({"cliente_agro_pk": cli.pk, "limite": "-5"}),
            content_type="application/json",
        )
        check("http_api_negativo", r_neg.status_code == 400)

        r_bad = c.post(
            url_lim,
            data=json.dumps({"cliente_agro_pk": 99999999, "limite": "1"}),
            content_type="application/json",
        )
        check("http_api_cliente_404", r_bad.status_code == 404)
    finally:
        # restaura valor original
        try:
            definir_limite_fiado_cliente(cli.pk, anterior, usuario="verify-restore")
            check("restore_limite", True, str(anterior))
        except Exception as e:
            check("restore_limite", False, str(e)[:80])


def main() -> int:
    print("verify_fiado_limite_linha_path")
    test_arquivos()
    test_node_syntax()
    try:
        test_runtime()
    except Exception as e:
        check("runtime_crash", False, str(e).split("\n")[0][:100])
    print()
    if fails:
        print(f"FALHOU: {len(fails)} falha(s), {len(oks)} ok")
        for f in fails:
            print(f"  - {f}")
        return 1
    print(f"OK verify_fiado_limite_linha_path — {len(oks)}/{len(oks)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
