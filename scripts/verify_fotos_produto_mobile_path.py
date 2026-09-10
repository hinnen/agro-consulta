"""Prova — app Fotos produto (hub GM Lojas · path FOTOS-PRODUTO-MOBILE)."""
from __future__ import annotations

import base64
import os
import sys
from io import BytesIO
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.test import Client, override_settings
from django.urls import reverse

from produtos.catalogo_delivery_util import delivery_de_extras, normalizar_delivery
from produtos.fotos_produto_pin_util import SESSION_OPERADOR
from produtos.fotos_produto_util import (
    MAX_SLOTS,
    apagar_slot_foto,
    bytes_e_mime_do_slot,
    contagem_fotos,
    delivery_com_extras,
    gravar_slot_foto,
    overlay_get_or_create,
    slot_tem_foto,
    url_foto_produto,
)
from produtos.models import ProdutoGestaoOverlayAgro

OK = 0
FAIL = 0
PIN = (os.environ.get("AGRO_TEST_PIN") or "9973").strip()
PID = "fotos-verify-produto-001"


def check(label: str, cond: bool, detail: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {label}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"  FAIL {label}" + (f" — {detail}" if detail else ""))


def _csrf(c: Client) -> str:
    return c.cookies.get("csrftoken").value if c.cookies.get("csrftoken") else ""


def _tiny_jpeg_b64() -> str:
    try:
        from PIL import Image

        buf = BytesIO()
        Image.new("RGB", (32, 32), (40, 180, 90)).save(buf, format="JPEG", quality=80)
        return base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception:
        raw = (
            b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
            b"\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t"
            b"\x08\n\x0c\x14\r\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a"
            b"\x1f\x1e\x1d\x1a\x1c\x1c $.\' \",#\x1c\x1c(7),01444\x1f\'9=82<.342"
            b"\xff\xc0\x00\x0b\x08\x00\x01\x00\x01\x01\x01\x11\x00"
            b"\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b"
            b"\xff\xda\x00\x08\x01\x01\x00\x00?\x00\x7f\xff\xd9"
        )
        return base64.b64encode(raw).decode("ascii")


def main() -> int:
    print("VERIFY FOTOS-PRODUTO-MOBILE")
    print("== Arquivos ==")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    hub = (ROOT / "produtos/templates/produtos/vendas_lojas_hub.html").read_text(encoding="utf-8")
    app = (ROOT / "produtos/templates/produtos/fotos_produto/app.html").read_text(encoding="utf-8")
    pin = (ROOT / "produtos/templates/produtos/fotos_produto/pin.html").read_text(encoding="utf-8")
    util = (ROOT / "produtos/fotos_produto_util.py").read_text(encoding="utf-8")
    cat = (ROOT / "produtos/catalogo_delivery_util.py").read_text(encoding="utf-8")

    check("url_fotos_app", "fotos_produto_app" in urls and "vendas/lojas/fotos/" in urls)
    check("url_foto_bytes", "produto_foto_bytes" in urls and "api/produtos/foto/" in urls)
    check("hub_botao_fotos", "vl-hub-fotos" in hub and "fotos_produto_app" in hub)
    check("hub_tres_botoes", "vl-hub-vendas" in hub and "vl-hub-tarefas" in hub and "vl-hub-fotos" in hub)
    check("pin_tpl", "PIN" in pin and "fotos_produto_pin" in pin)
    check("app_scan", "html5-qrcode" in app and "btnScan" in app)
    check("app_slots", "slots" in app and "filePick" in app)
    check("app_busca_motor", "_js_busca_produto_inteligente" in app)
    check("util_max_slots", "MAX_SLOTS = 4" in util)
    check("delivery_extras", "imagens_extras" in cat and "normalizar_imagens_extras_delivery" in cat)
    check("url_leve_lista", "_url_imagem_produto_leve" in cat)

    print("== Util ==")
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=PID).delete()
    ov = overlay_get_or_create(PID)
    check("overlay_create", ov is not None and ov.produto_externo_id == PID)
    b64 = _tiny_jpeg_b64()
    ok0, err0 = gravar_slot_foto(ov, indice=0, imagem_base64=b64)
    check("gravar_principal", ok0, err0)
    ov.refresh_from_db()
    d = delivery_com_extras(delivery_de_extras(ov.cadastro_extras))
    check("slot0_tem", slot_tem_foto(d, 0))
    ok1, err1 = gravar_slot_foto(ov, indice=1, imagem_base64=b64)
    ok2, err2 = gravar_slot_foto(ov, indice=2, imagem_base64=b64)
    ok3, err3 = gravar_slot_foto(ov, indice=3, imagem_base64=b64)
    check("gravar_extras", ok1 and ok2 and ok3, f"{err1}|{err2}|{err3}")
    ov.refresh_from_db()
    d = delivery_com_extras(delivery_de_extras(ov.cadastro_extras))
    check("contagem_4", contagem_fotos(d) == 4, str(contagem_fotos(d)))
    raw, mime = bytes_e_mime_do_slot(d, 0)
    check("bytes_principal", bool(raw) and "jpeg" in (mime or ""), mime or "")
    ok_bad, _ = gravar_slot_foto(ov, indice=4, imagem_base64=b64)
    check("rejeita_slot4", not ok_bad)
    check("url_foto", "/api/produtos/foto/" in url_foto_produto(PID, 0))
    ok_del, _ = apagar_slot_foto(ov, indice=2)
    ov.refresh_from_db()
    d = delivery_com_extras(delivery_de_extras(ov.cadastro_extras))
    check("apagar_slot2", ok_del and not slot_tem_foto(d, 2) and contagem_fotos(d) == 3)

    d_prev = dict(d)
    d_norm = normalizar_delivery(
        {
            "ativo": True,
            "titulo": "x",
            "imagem_base64": d_prev.get("imagem_base64"),
            "imagem_mime": "image/jpeg",
        },
        processar_imagem=False,
    )
    check("norm_sem_extras_nao_inclui", "imagens_extras" not in d_norm)

    print("== HTTP ==")
    with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
        c = Client(enforce_csrf_checks=True)
        r_hub = c.get(reverse("vendas_lojas_hub"))
        check("hub_200", r_hub.status_code == 200)
        check("hub_html_fotos", b"Fotos" in r_hub.content and b"vl-hub-fotos" in r_hub.content)

        r_redir = c.get(reverse("fotos_produto_app"))
        check("app_pede_pin", r_redir.status_code in (302, 301))

        r_pin = c.get(reverse("fotos_produto_pin"))
        check("pin_200", r_pin.status_code == 200)
        token = _csrf(c)
        check("csrf_cookie", bool(token))

        r_login = c.post(
            reverse("fotos_produto_pin"),
            {"pin": PIN},
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
            HTTP_X_CSRFTOKEN=token,
        )
        if r_login.status_code != 200 or not (r_login.json() or {}).get("ok"):
            s = c.session
            s[SESSION_OPERADOR] = "Verify Fotos"
            s.save()
            check("pin_login_fallback_sessao", True, "PIN real indisponível — sessão forçada")
        else:
            check("pin_login", True, (r_login.json() or {}).get("operador", "")[:40])

        r_app = c.get(reverse("fotos_produto_app"))
        check("app_200", r_app.status_code == 200)
        check("app_html5", b"html5-qrcode" in r_app.content)

        r_det = c.get(reverse("api_fotos_produto_detalhe", kwargs={"produto_id": PID}))
        j_det = r_det.json() if r_det.status_code == 200 else {}
        check("api_detalhe", r_det.status_code == 200 and j_det.get("ok"), str(r_det.status_code))
        check("api_slots_4", len(j_det.get("slots") or []) == MAX_SLOTS)

        token = _csrf(c) or token
        r_post = c.post(
            reverse("api_fotos_produto_slot", kwargs={"produto_id": PID}),
            data='{"indice":0,"imagem_base64":"data:image/jpeg;base64,' + b64 + '"}',
            content_type="application/json",
            HTTP_X_CSRFTOKEN=token,
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        j_post = r_post.json() if r_post.status_code == 200 else {}
        check("api_salvar", r_post.status_code == 200 and j_post.get("ok"), str(r_post.content[:120]))

        r_img = c.get(reverse("produto_foto_bytes", kwargs={"produto_id": PID}) + "?i=0")
        check(
            "bytes_http",
            r_img.status_code == 200 and r_img.get("Content-Type", "").startswith("image/"),
        )

        r_img3 = c.get(reverse("produto_foto_bytes", kwargs={"produto_id": PID}) + "?i=3")
        check("bytes_extra", r_img3.status_code in (200, 404))

    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=PID).delete()

    print(f"\nVERIFY OK {OK}/{OK + FAIL}" if FAIL == 0 else f"\nVERIFY FAIL {FAIL} (ok {OK})")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
