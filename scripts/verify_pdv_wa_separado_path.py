#!/usr/bin/env python3
"""VERIFY PDV-WA-SEPARADO — ícone e orçamento saem do worker do PDV.

Venda, caixa, checkout e NF-e não entram neste pacote.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

failures: list[str] = []


def ok(msg: str) -> None:
    print(f"  OK {msg}")


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        failures.append(msg)
        print(f"FAIL {msg}")


def _names_vs_producao() -> set[str]:
    names: set[str] = set()
    for args in (
        ["git", "diff", "--name-only", "origin/producao"],
        ["git", "diff", "--cached", "--name-only", "origin/producao"],
        ["git", "diff", "--name-only", "origin/producao...HEAD"],
        ["git", "ls-files", "--others", "--exclude-standard"],
    ):
        out = subprocess.check_output(args, cwd=ROOT, text=True)
        names.update(ln.strip() for ln in out.splitlines() if ln.strip())
    return names


def _diff(path: str) -> str:
    return subprocess.check_output(
        ["git", "diff", "origin/producao", "--", path],
        cwd=ROOT,
        text=True,
    )


def main() -> int:
    import django

    django.setup()
    from django.conf import settings
    from django.http import HttpResponse
    from django.test import RequestFactory
    from django.urls import reverse

    from base.wa_split_middleware import AgroWhatsAppSplitMiddleware
    from produtos.agro_wa_origin_util import agro_wa_path_is_split_target

    settings.AGRO_WA_PUBLIC_ORIGIN = "https://agro-consulta-wa.onrender.com"
    loja = "sistvale.onrender.com"

    check(not agro_wa_path_is_split_target("/pdv/checkout/"), "checkout não sai do PDV")
    check(not agro_wa_path_is_split_target("/caixa/"), "caixa não sai do PDV")
    check(not agro_wa_path_is_split_target("/api/pdv/venda/"), "api de venda não sai do PDV")
    check(
        not agro_wa_path_is_split_target("/api/pdv/zap-loja/orcamento/"),
        "botão Loja fica no PDV",
    )
    check(not agro_wa_path_is_split_target("/healthz"), "healthz não é alvo do Zap")
    check(agro_wa_path_is_split_target("/atendimento-whatsapp/"), "tela do Zap sai")
    check(
        agro_wa_path_is_split_target("/api/atendimento-whatsapp/estado/"),
        "fila do Zap sai",
    )
    check(
        agro_wa_path_is_split_target("/api/atendimento-whatsapp/bridge/orcamento/"),
        "ponte antiga do orçamento sai",
    )

    def _app(_request):
        return HttpResponse("pass", status=200)

    mw = AgroWhatsAppSplitMiddleware(_app)
    rf = RequestFactory()

    def _hit(path: str, host: str = loja):
        req = rf.get(path)
        req.META["HTTP_HOST"] = host
        return mw(req)

    check(_hit("/pdv/checkout/").content == b"pass", "middleware deixa o checkout passar")
    check(
        _hit("/api/pdv/zap-loja/orcamento/").content == b"pass",
        "middleware deixa o botão Loja passar",
    )
    check(_hit("/healthz").content == b"pass", "middleware deixa o healthz passar")
    redir = _hit("/atendimento-whatsapp/?loja=1")
    check(redir.status_code in (301, 302), "tela antiga do Zap redireciona")
    loc = redir.get("Location") or ""
    check(
        loc.startswith("https://agro-consulta-wa.onrender.com/atendimento-whatsapp/"),
        "redirect vai para o serviço do Zap",
    )
    bridge = _hit("/api/atendimento-whatsapp/bridge/orcamento/")
    check(bridge.status_code == 503, "ponte no host da loja responde 503")
    check(b"servi" in bridge.content, "503 explica o serviço dedicado")
    same = _hit("/atendimento-whatsapp/", host="agro-consulta-wa.onrender.com")
    check(same.content == b"pass", "no próprio Zap não redireciona em loop")

    settings.AGRO_WA_PUBLIC_ORIGIN = ""
    check(
        _hit("/atendimento-whatsapp/").content == b"pass",
        "origem vazia desliga o redirect",
    )
    settings.AGRO_WA_PUBLIC_ORIGIN = "https://agro-consulta-wa.onrender.com"

    check(
        reverse("api_pdv_zap_loja_orcamento") == "/api/pdv/zap-loja/orcamento/",
        "rota do botão Loja",
    )
    check(
        reverse("api_atendimento_whatsapp_bridge_orcamento")
        == "/api/atendimento-whatsapp/bridge/orcamento/",
        "rota da ponte do orçamento",
    )
    check(
        reverse("api_nfe_saida_cancelar", kwargs={"doc_id": "x"}).endswith("/cancelar/"),
        "cancelar NF-e continua na loja",
    )
    check(reverse("pdv_checkout") == "/pdv/checkout/", "rota do checkout continua")

    urls_diff = _diff("produtos/urls.py")
    removed_urls = [
        ln
        for ln in urls_diff.splitlines()
        if ln.startswith("-") and not ln.startswith("---")
    ]
    check(not removed_urls, "urls.py só ganha rota, não apaga")
    check("api/pdv/zap-loja/orcamento/" in urls_diff, "diff traz a rota Loja")
    check("fiscal-nfe" in (ROOT / "produtos/urls.py").read_text(encoding="utf-8"), "fiscal-nfe permanece")

    wiz = _diff("produtos/static/produtos/js/pdv_wizard.js")
    wiz_rm = [ln[1:] for ln in wiz.splitlines() if ln.startswith("-") and not ln.startswith("---")]
    wiz_add = [ln[1:] for ln in wiz.splitlines() if ln.startswith("+") and not ln.startswith("+++")]
    check(
        wiz_rm == ["            return fetch('/api/atendimento-whatsapp/recurso-acao/', {"],
        "wizard só troca a URL do orçamento",
    )
    check(
        wiz_add == ["            return fetch('/api/pdv/zap-loja/orcamento/', {"],
        "wizard aponta para a rota nova",
    )

    html = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    check("agro-consulta-wa.onrender.com/atendimento-whatsapp/" in html, "ícone aponta o Zap separado")
    check(html.count("-wa2") >= 2, "cache do JS do PDV atualiza")

    top = (ROOT / "produtos/static/produtos/js/pdv_topbar_whatsapp.js").read_text(encoding="utf-8")
    check("window.open(href, '_blank', 'noopener')" in top, "ícone abre outra aba")
    check("if (zapEhSeparado()) return;" in top, "ícone não consulta a fila no PDV")

    views = (ROOT / "produtos/views_atendimento_whatsapp.py").read_text(encoding="utf-8")
    check("if exc.code not in (404, 502, 503):" in views, "404 do Zap cai no gravar local")
    check('"local": True' in views, "resposta local marcada")
    check("def api_pdv_zap_loja_orcamento" in views, "view do botão Loja existe")

    st = (ROOT / "config/settings.py").read_text(encoding="utf-8")
    check("base.wa_split_middleware.AgroWhatsAppSplitMiddleware" in st, "middleware ligado")
    check('default="https://agro-consulta-wa.onrender.com"' in st, "origem padrão do Zap")
    check('if "AGRO_WA_PUBLIC_ORIGIN" in os.environ:' in st, "env vazio desliga a origem")

    ver = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    check(ver == "27.26.45", f"VERSION 27.26.45 (lido {ver})")

    for js in (
        "produtos/static/produtos/js/pdv_wizard.js",
        "produtos/static/produtos/js/pdv_topbar_whatsapp.js",
    ):
        proc = subprocess.run(
            ["node", "--check", str(ROOT / js)],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        check(proc.returncode == 0, f"node --check {js}")

    names = _names_vs_producao()
    bad = [
        n
        for n in sorted(names)
        if any(tok in n for tok in ("caixa_fechar", "caixa_util", "checkout", "views_nfe", "/migrations/"))
    ]
    check(not bad, f"delta sem caixa/checkout/nfe/migrate ({bad or 'limpo'})")

    if failures:
        print(f"\nVERIFY_FAILS={len(failures)}")
        return 1
    print("\nVERIFY_OK 40")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
