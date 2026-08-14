#!/usr/bin/env python
"""Prova estática — Ajuste Mobile UX celular (overlays / teclado / scroll)."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def check(path: Path, *needles: str, label: str = "") -> None:
    text = path.read_text(encoding="utf-8")
    for n in needles:
        if n not in text:
            fail(f"{label or path.name}: falta «{n[:60]}»")
            return
    ok(f"{label or path.name}: {len(needles)} marcadores")


ma = ROOT / "produtos" / "templates" / "produtos" / "mobile_ajuste.html"
conf = ROOT / "produtos" / "templates" / "produtos" / "includes" / "agro_loja_confirm.html"
util = ROOT / "produtos" / "contagem_ciclica_util.py"

check(
    ma,
    "interactive-widget=resizes-content",
    "--ma-kb-inset",
    "window.maLockScroll",
    "ma-scroll-lock",
    "body.ma-page.ma-modal-open",
    "ma-head-actions",
    "padding-bottom: calc(0.75rem + var(--ma-safe-bottom) + var(--ma-kb-inset))",
    "maBip1On = false",
    "linhas_truncadas",
    "z-[155]",
    "z-[165]",
    "z-index: 170",
    "ma-ciclica-dias-custom",
    "Bip +1 off",
    label="mobile_ajuste.html",
)

# confirm usa maLockScroll + z 160 + kb inset
check(
    conf,
    "z-[160]",
    "maLockScroll",
    "--ma-kb-inset",
    "items-end",
    label="agro_loja_confirm.html",
)

check(
    util,
    "linhas_truncadas",
    "linhas_enviadas",
    "qs[:800]",
    label="contagem_ciclica_util.py",
)

# offer overlay alinhado ao teclado (bottom sheet no mobile)
text = ma.read_text(encoding="utf-8")
if ".ma-offer-overlay" in text and "align-items: flex-end" in text:
    ok("offer overlay bottom-sheet")
else:
    fail("offer overlay não é bottom-sheet")

if "Bip +1 off" in text:
    ok("rótulo Bip+1 off na cíclica")
else:
    fail("falta rótulo Bip+1 off")

if "flex-wrap" in text and "ma-head-actions" in text:
    ok("header actions wrap")
else:
    fail("header sem wrap")

# --- HTTP (Django client): login · página com gate · API cíclica cega ---
try:
    import os
    import sys
    import uuid
    from decimal import Decimal

    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from django.test import Client, override_settings
    from django.urls import reverse

    from estoque.models import ContagemCiclicaSessao
    from produtos.models import Produto

    needles_page = (
        "window.maLockScroll",
        "ma-ciclica-dias-custom",
        "ma-btn-ciclica",
        "agro-loja-confirm-modal",
        "ma-kb-inset",
        "Somar (cíclica)",
        "Bip +1 off",
        "linhas_truncadas",
        "ma-scroll-lock",
    )
    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        c = Client(HTTP_HOST="127.0.0.1")
        r = c.get("/ajuste-mobile/")
        t = r.content.decode("utf-8", "replace")
        if r.status_code == 200 and "interactive-widget=resizes-content" in t:
            ok("HTTP login viewport")
        else:
            fail(f"HTTP login {r.status_code}")

        session = c.session
        session["ajuste_mobile_gate"] = True
        session["ajuste_mobile_operador"] = "VerifyUxPage"
        session.save()
        r2 = c.get("/ajuste-mobile/")
        t2 = r2.content.decode("utf-8", "replace")
        miss = [n for n in needles_page if n not in t2]
        if r2.status_code == 200 and not miss:
            ok(f"HTTP pagina PIN + {len(needles_page)} UX")
        else:
            fail(f"HTTP pagina miss={miss} status={r2.status_code}")

        r3 = c.get("/ajuste-mobile/")
        t3 = r3.content.decode("utf-8", "replace")
        if "window.maLockScroll" not in t3:
            ok("HTTP gate one-shot")
        else:
            fail("HTTP gate one-shot quebrado")

        session = c.session
        session["ajuste_mobile_operador"] = "VerifyUxPage"
        session.save()

        tag = f"uxhttp-{uuid.uuid4().hex[:8]}"
        p = Produto.objects.create(
            produto_externo_id=tag,
            codigo_interno=f"GM-{tag[-6:]}",
            codigo_nfe=f"GM-{tag[-6:]}",
            nome=f"UX HTTP {tag}",
            categoria="UXTEST",
            custo=Decimal("1"),
            preco_venda=Decimal("2"),
            ativo=True,
            cadastro_inativo=False,
        )
        sid = None
        try:
            r4 = c.post(
                reverse("api_ciclica_abrir"),
                {
                    "deposito": "centro",
                    "escopo_tipo": "categoria",
                    "escopo_valor": "UXTEST",
                    "dias_movimentacao": "0",
                },
            )
            d4 = r4.json() if r4.status_code == 200 else {}
            sid = (d4.get("sessao") or {}).get("id")
            if d4.get("ok") and sid:
                ok(f"HTTP abrir #{sid}")
            else:
                fail(f"HTTP abrir {r4.status_code}")
            if sid:
                r5 = c.post(
                    reverse("api_ciclica_contar", kwargs={"pk": sid}),
                    {"produto_id": tag, "quantidade": "3.5"},
                )
                if (r5.json() or {}).get("ok"):
                    ok("HTTP contar")
                else:
                    fail("HTTP contar")
                r6 = c.get(reverse("api_ciclica_detalhe", kwargs={"pk": sid}))
                s = ((r6.json() or {}).get("sessao") or {})
                linhas = s.get("linhas") or []
                if s.get("cego") is True and "linhas_truncadas" in s:
                    bad = [
                        x
                        for x in linhas
                        if "saldo" in x or "qtd" in x or "quantidade" in x
                    ]
                    if not bad:
                        ok("HTTP detalhe cego + trunc flag")
                    else:
                        fail("HTTP vazou saldo/qtd")
                else:
                    fail("HTTP detalhe sem cego/trunc")
                c.post(reverse("api_ciclica_cancelar", kwargs={"pk": sid}))
                ok("HTTP cancelar")
        finally:
            if sid:
                ContagemCiclicaSessao.objects.filter(pk=sid).delete()
            Produto.objects.filter(pk=p.pk).delete()
except Exception as exc:
    fail(f"HTTP smoke: {exc}")

print()
if fails:
    print(f"VERIFY_FAIL {len(fails)}")
    for f in fails:
        print(" -", f)
    raise SystemExit(1)
print(f"VERIFY_OK {oks}")
