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
    "maCiclicaBipMaisUm",
    "maBipPiscarTela",
    "Incluir fora",
    "ma-ciclica-btn-incluir-fora",
    "formatsToSupport",
    "fps: 12",
    "keepBusy",
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
    "forcar: bool",
    "precisa_recontagem=(sessao.status == ContagemCiclicaStatus.PASS2)",
    label="contagem_ciclica_util.py",
)

# offer overlay alinhado ao teclado (bottom sheet no mobile)
text = ma.read_text(encoding="utf-8")
if ".ma-offer-overlay" in text and "align-items: flex-end" in text:
    ok("offer overlay bottom-sheet")
else:
    fail("offer overlay não é bottom-sheet")

if "maCiclicaBipMaisUm" in text and "maBipPiscarTela" in text:
    ok("Bip+1 na cíclica + pisca tela")
else:
    fail("falta Bip+1 cíclica / pisca tela")

if "grid-template-columns: repeat(5, minmax(0, 1fr))" in text and "ma-head-actions" in text:
    ok("header actions grade 5 no celular")
else:
    fail("header sem grade de 5 botões")

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
        "maCiclicaBipMaisUm",
        "linhas_truncadas",
        "ma-scroll-lock",
        "Incluir fora",
        "ma-ciclica-btn-incluir-fora",
        "formatsToSupport",
        "ma-dep-modal",
        "ma-pick",
        "ma-offer",
        "ma-ciclica-modal",
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
        tag2 = f"{tag}-b"
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
        p2 = Produto.objects.create(
            produto_externo_id=tag2,
            codigo_interno=f"GM-{tag2[-6:]}",
            codigo_nfe=f"GM-{tag2[-6:]}",
            nome=f"UX FORA {tag2}",
            categoria="OUTRA-CAT",
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
                    {
                        "produto_id": tag,
                        "qtd": "3.5",
                        "nome_produto": p.nome,
                        "codigo_interno": p.codigo_nfe,
                    },
                )
                d5 = r5.json() if r5.status_code == 200 else {}
                if d5.get("ok") and abs(float(d5.get("qtd_acumulada") or 0) - 3.5) < 0.01:
                    ok("HTTP contar qtd=3.5")
                else:
                    fail(f"HTTP contar {r5.status_code} {r5.content[:200]}")

                # Fora do escopo sem forcar → 400
                r_nf = c.post(
                    reverse("api_ciclica_contar", kwargs={"pk": sid}),
                    {"produto_id": tag2, "qtd": "1", "nome_produto": p2.nome},
                )
                d_nf = r_nf.json() if r_nf.status_code in (200, 400) else {}
                if r_nf.status_code == 400 and not d_nf.get("ok"):
                    ok("HTTP bloqueia fora do escopo")
                else:
                    fail(f"HTTP deveria bloquear fora: {r_nf.status_code}")

                r_f = c.post(
                    reverse("api_ciclica_contar", kwargs={"pk": sid}),
                    {
                        "produto_id": tag2,
                        "qtd": "2",
                        "forcar": "1",
                        "nome_produto": p2.nome,
                        "codigo_interno": p2.codigo_nfe,
                    },
                )
                d_f = r_f.json() if r_f.status_code == 200 else {}
                if d_f.get("ok") and abs(float(d_f.get("qtd_acumulada") or 0) - 2) < 0.01:
                    ok("HTTP forcar incluir fora")
                else:
                    fail(f"HTTP forcar {r_f.status_code} {r_f.content[:200]}")

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
            Produto.objects.filter(pk__in=[p.pk, p2.pk]).delete()
except Exception as exc:
    fail(f"HTTP smoke: {exc}")

# login template
login = ROOT / "produtos" / "templates" / "produtos" / "ajuste_mobile_login.html"
check(
    login,
    "interactive-widget=resizes-content",
    "safe-area-inset",
    "100dvh",
    "ajuste_mobile_manifest",
    "ajuste_mobile_sw",
    label="ajuste_mobile_login.html",
)

check(
    ma,
    "ajuste_mobile_manifest",
    "ajuste_mobile_sw",
    'scope: "/ajuste-mobile/"',
    label="mobile_ajuste.html PWA",
)

print()
if fails:
    print(f"VERIFY_FAIL {len(fails)}")
    for f in fails:
        print(" -", f)
    raise SystemExit(1)
print(f"VERIFY_OK {oks}")
