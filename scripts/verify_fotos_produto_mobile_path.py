"""Prova detalhada — app Fotos produto (hub GM Lojas · path FOTOS-PRODUTO-MOBILE)."""
from __future__ import annotations

import base64
import json
import os
import sys
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.test import Client, override_settings
from django.urls import reverse

from produtos.catalogo_delivery_util import (
    aplicar_imagem_delivery_no_row,
    delivery_de_extras,
    normalizar_delivery,
)
from produtos.fotos_produto_pin_util import SESSION_OPERADOR, limpar_operador_sessao
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


def _tiny_jpeg_b64(*, color=(40, 180, 90)) -> str:
    try:
        from PIL import Image

        buf = BytesIO()
        Image.new("RGB", (48, 48), color).save(buf, format="JPEG", quality=80)
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
    print("VERIFY FOTOS-PRODUTO-MOBILE (detalhado)")
    print("== Arquivos ==")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    hub = (ROOT / "produtos/templates/produtos/vendas_lojas_hub.html").read_text(encoding="utf-8")
    app = (ROOT / "produtos/templates/produtos/fotos_produto/app.html").read_text(encoding="utf-8")
    pin = (ROOT / "produtos/templates/produtos/fotos_produto/pin.html").read_text(encoding="utf-8")
    util = (ROOT / "produtos/fotos_produto_util.py").read_text(encoding="utf-8")
    cat = (ROOT / "produtos/catalogo_delivery_util.py").read_text(encoding="utf-8")
    views_f = (ROOT / "produtos/views_fotos_produto.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")

    check("url_fotos_app", "fotos_produto_app" in urls and "vendas/lojas/fotos/" in urls)
    check("url_foto_bytes", "produto_foto_bytes" in urls and "api/produtos/foto/" in urls)
    check("url_api_slot", "api_fotos_produto_slot" in urls)
    check("hub_botao_fotos", "vl-hub-fotos" in hub and "fotos_produto_app" in hub)
    check("hub_tres_botoes", "vl-hub-vendas" in hub and "vl-hub-tarefas" in hub and "vl-hub-fotos" in hub)
    check("pin_tpl", "PIN" in pin and "fotos_produto_pin" in pin)
    check("app_scan", "html5-qrcode" in app and "btnScan" in app and "facingMode" in app)
    check("app_slots", "slots" in app and "filePick" in app and "comprimirJpeg" in app)
    check("app_busca_motor", "_js_busca_produto_inteligente" in app and "filtrarProdutosBuscaInteligente" in app)
    check("util_max_slots", "MAX_SLOTS = 4" in util)
    check("delivery_extras", "imagens_extras" in cat and "normalizar_imagens_extras_delivery" in cat)
    check("url_leve_lista", "_url_imagem_produto_leve" in cat)
    check(
        "views_preserva_extras",
        'if "imagens_extras" not in raw_del' in views and "imagens_extras" in views,
    )
    check("sw_scope_hub", "vendas_lojas_manifest" in app and "serviceWorker" in hub)

    print("== Util galeria ==")
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=PID).delete()
    ov = overlay_get_or_create(PID)
    check("overlay_create", ov is not None and ov.produto_externo_id == PID)
    b64 = _tiny_jpeg_b64()
    b64b = _tiny_jpeg_b64(color=(200, 40, 40))
    ok0, err0 = gravar_slot_foto(ov, indice=0, imagem_base64=b64)
    check("gravar_principal", ok0, err0)
    ov.refresh_from_db()
    d = delivery_com_extras(delivery_de_extras(ov.cadastro_extras))
    check("slot0_tem", slot_tem_foto(d, 0))
    ok1, err1 = gravar_slot_foto(ov, indice=1, imagem_base64=b64)
    ok2, err2 = gravar_slot_foto(ov, indice=2, imagem_base64=b64b)
    ok3, err3 = gravar_slot_foto(ov, indice=3, imagem_base64=b64)
    check("gravar_extras", ok1 and ok2 and ok3, f"{err1}|{err2}|{err3}")
    ov.refresh_from_db()
    d = delivery_com_extras(delivery_de_extras(ov.cadastro_extras))
    check("contagem_4", contagem_fotos(d) == 4, str(contagem_fotos(d)))
    raw, mime = bytes_e_mime_do_slot(d, 0)
    check("bytes_principal", bool(raw) and "jpeg" in (mime or ""), mime or "")
    raw3, mime3 = bytes_e_mime_do_slot(d, 3)
    check("bytes_extra3", bool(raw3) and "jpeg" in (mime3 or ""), mime3 or "")
    ok_bad, _ = gravar_slot_foto(ov, indice=4, imagem_base64=b64)
    check("rejeita_slot4", not ok_bad)
    check("url_foto", "/api/produtos/foto/" in url_foto_produto(PID, 0))
    ok_del, _ = apagar_slot_foto(ov, indice=2)
    ov.refresh_from_db()
    d = delivery_com_extras(delivery_de_extras(ov.cadastro_extras))
    check("apagar_slot2", ok_del and not slot_tem_foto(d, 2) and contagem_fotos(d) == 3)
    check("extras_ainda_1_e_3", slot_tem_foto(d, 1) and slot_tem_foto(d, 3))

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

    # Lista leve: row.imagem = URL HTTP, não data: base64
    row = {"id": PID, "imagem": ""}
    ov_mock = MagicMock()
    ov_mock.produto_externo_id = PID
    ov_mock.cadastro_extras = ov.cadastro_extras
    ov_mock.atualizado_em = ov.atualizado_em
    aplicar_imagem_delivery_no_row(row, ov)
    img = str(row.get("imagem") or "")
    check("row_imagem_http", img.startswith("/api/produtos/foto/") and "data:" not in img, img[:80])
    check("row_fotos_n", int(row.get("fotos_n") or 0) == 3, str(row.get("fotos_n")))

    # Overlay cadastro sem imagens_extras deve preservar (mesma lógica da view)
    ex = dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
    raw_del = {"ativo": True, "titulo": "cadastro", "imagem_base64": d.get("imagem_base64")}
    d_del = normalizar_delivery(raw_del, processar_imagem=False)
    if "imagens_extras" not in raw_del:
        prev_del = ex.get("delivery") if isinstance(ex.get("delivery"), dict) else {}
        if isinstance(prev_del.get("imagens_extras"), list) and prev_del.get("imagens_extras"):
            d_del["imagens_extras"] = prev_del["imagens_extras"]
    check(
        "preserva_extras_no_merge",
        isinstance(d_del.get("imagens_extras"), list)
        and any(
            isinstance(x, dict) and str(x.get("imagem_base64") or "").strip()
            for x in d_del["imagens_extras"]
        ),
    )

    print("== HTTP PIN + APIs ==")
    with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
        c = Client(enforce_csrf_checks=True)
        r_hub = c.get(reverse("vendas_lojas_hub"))
        check("hub_200", r_hub.status_code == 200)
        check("hub_html_fotos", b"Fotos" in r_hub.content and b"vl-hub-fotos" in r_hub.content)

        r_redir = c.get(reverse("fotos_produto_app"))
        check("app_pede_pin", r_redir.status_code in (302, 301))

        r_api_sem = c.get(reverse("api_fotos_produto_detalhe", kwargs={"produto_id": PID}))
        j_sem = r_api_sem.json() if r_api_sem.status_code in (401, 403) else {}
        check(
            "api_sem_pin_401",
            r_api_sem.status_code == 401 and j_sem.get("precisa_pin") is True,
            str(r_api_sem.status_code),
        )

        r_pin = c.get(reverse("fotos_produto_pin"))
        check("pin_200", r_pin.status_code == 200)
        token = _csrf(c)
        check("csrf_cookie", bool(token))

        r_pin_ruim = c.post(
            reverse("fotos_produto_pin"),
            {"pin": "0000"},
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
            HTTP_X_CSRFTOKEN=token,
        )
        check(
            "pin_errado_rejeita",
            r_pin_ruim.status_code in (403, 400) and not (r_pin_ruim.json() or {}).get("ok"),
            str(r_pin_ruim.status_code),
        )

        r_login = c.post(
            reverse("fotos_produto_pin"),
            {"pin": PIN},
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
            HTTP_X_CSRFTOKEN=token,
        )
        j_login = r_login.json() if r_login.status_code == 200 else {}
        check(
            "pin_9973_login",
            r_login.status_code == 200 and j_login.get("ok") is True,
            str(j_login.get("operador") or r_login.content[:80]),
        )
        check("pin_operador_renan", "renan" in str(j_login.get("operador") or "").lower(), str(j_login.get("operador")))

        r_app = c.get(reverse("fotos_produto_app"))
        check("app_200", r_app.status_code == 200)
        check("app_html5", b"html5-qrcode" in r_app.content)
        check("app_catalogo_slim_url", b"catalogo-slim" in r_app.content or b"catalogo_slim" in r_app.content or b"/api/pdv/catalogo-slim/" in r_app.content)

        r_det = c.get(reverse("api_fotos_produto_detalhe", kwargs={"produto_id": PID}))
        j_det = r_det.json() if r_det.status_code == 200 else {}
        check("api_detalhe", r_det.status_code == 200 and j_det.get("ok"), str(r_det.status_code))
        check("api_slots_4", len(j_det.get("slots") or []) == MAX_SLOTS)
        check("api_n_fotos_3", int(j_det.get("n_fotos") or 0) == 3, str(j_det.get("n_fotos")))

        token = _csrf(c) or token
        # Salvar slot 0 de novo
        r_post = c.post(
            reverse("api_fotos_produto_slot", kwargs={"produto_id": PID}),
            data=json.dumps(
                {
                    "indice": 0,
                    "imagem_base64": "data:image/jpeg;base64," + b64,
                    "imagem_mime": "image/jpeg",
                }
            ),
            content_type="application/json",
            HTTP_X_CSRFTOKEN=token,
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        j_post = r_post.json() if r_post.status_code == 200 else {}
        check("api_salvar_0", r_post.status_code == 200 and j_post.get("ok"), str(r_post.content[:120]))

        # Slot 2 (estava apagado) — regravo
        r_post2 = c.post(
            reverse("api_fotos_produto_slot", kwargs={"produto_id": PID}),
            data=json.dumps({"indice": 2, "imagem_base64": b64b, "imagem_mime": "image/jpeg"}),
            content_type="application/json",
            HTTP_X_CSRFTOKEN=token,
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        j2 = r_post2.json() if r_post2.status_code == 200 else {}
        check("api_salvar_2", r_post2.status_code == 200 and j2.get("ok"), str(r_post2.content[:80]))
        check("api_n_apos_2", int(j2.get("n_fotos") or 0) == 4, str(j2.get("n_fotos")))

        r_img = c.get(reverse("produto_foto_bytes", kwargs={"produto_id": PID}) + "?i=0")
        check(
            "bytes_http_0",
            r_img.status_code == 200 and r_img.get("Content-Type", "").startswith("image/"),
            r_img.get("Content-Type", ""),
        )
        r_img2 = c.get(reverse("produto_foto_bytes", kwargs={"produto_id": PID}) + "?i=2")
        check("bytes_http_2", r_img2.status_code == 200 and len(r_img2.content) > 20)
        r_img3 = c.get(reverse("produto_foto_bytes", kwargs={"produto_id": PID}) + "?i=3")
        check("bytes_http_3", r_img3.status_code == 200)
        r_bad_i = c.get(reverse("produto_foto_bytes", kwargs={"produto_id": PID}) + "?i=9")
        check("bytes_i9_404", r_bad_i.status_code == 404)

        # DELETE slot 1 via HTTP
        r_del = c.post(
            reverse("api_fotos_produto_slot", kwargs={"produto_id": PID}),
            data=json.dumps({"indice": 1, "acao": "apagar"}),
            content_type="application/json",
            HTTP_X_CSRFTOKEN=token,
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        j_del = r_del.json() if r_del.status_code == 200 else {}
        check("api_apagar_1", r_del.status_code == 200 and j_del.get("ok"), str(r_del.content[:80]))
        slots = {int(s["indice"]): s for s in (j_del.get("slots") or []) if isinstance(s, dict)}
        check("slot1_sem_foto", slots.get(1, {}).get("tem_foto") is False)
        check("slot0_ainda", slots.get(0, {}).get("tem_foto") is True)

        # Logout
        r_out = c.get(reverse("fotos_produto_logout"))
        check("logout_redir", r_out.status_code in (302, 301))
        r_after = c.get(reverse("api_fotos_produto_detalhe", kwargs={"produto_id": PID}))
        check("apos_logout_401", r_after.status_code == 401)

        # Slim = busca leve: sem campo imagem (galeria não entra na lista do PDV)
        r_slim = c.get(reverse("api_pdv_catalogo_slim"))
        slim_ok = r_slim.status_code == 200
        check("slim_200", slim_ok, str(r_slim.status_code))
        if slim_ok:
            try:
                jslim = r_slim.json()
                prods = jslim.get("produtos") or []
                sample = prods[0] if prods else {}
                check("slim_tem_produtos", len(prods) > 0, str(len(prods)))
                check(
                    "slim_sem_campo_imagem",
                    "imagem" not in sample,
                    "keys=" + ",".join(sorted(sample.keys())[:12]),
                )
            except Exception as exc:
                check("slim_json", False, str(exc))

    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=PID).delete()

    print(f"\nVERIFY OK {OK}/{OK + FAIL}" if FAIL == 0 else f"\nVERIFY FAIL {FAIL} (ok {OK})")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
