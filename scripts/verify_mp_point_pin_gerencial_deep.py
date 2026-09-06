# -*- coding: utf-8 -*-
"""Prova unificada MP-POINT-CANCEL-SAFE + PIN-GERENCIAL (deep path)."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import django

django.setup()

from django.test import RequestFactory

from produtos.mercado_pago_point import (
    mp_point_order_indica_cancelado,
    mp_point_order_indica_pago,
)
from produtos.pin_gerencial_util import (
    PIN_GERENCIAL_NOMES_UI,
    SESSION_MP_POINT_FORCAR_KEY,
    gravar_mp_point_forcar_bypass,
    is_usuario_gerencial,
    limpar_mp_point_forcar_bypass,
    mp_point_forcar_bypass_ativo,
    rotulo_gerencial_do_user,
    validar_pin_gerencial,
)
from produtos.views_mp_point import mp_point_bloqueio_venda_sessao

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def main() -> int:
    views_mp = (ROOT / "produtos/views_mp_point.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    pdv = (ROOT / "pdv/views.py").read_text(encoding="utf-8")
    wizard = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")

    # --- A) Cancel seguro (código) ---
    check("def _mp_point_promover_pago_local" in views_mp, "A promove PAID")
    check("pagamento_efetivado" in views_mp, "A abandon sinaliza pago")
    check("pedido_ainda_ativo" in views_mp, "A abandon não mente")
    check("recuperado_de_abandon" in views_mp, "A status recupera abandon")
    check("mp_point_bloqueio_venda_sessao" in views, "A gate ERP")
    check("MP_POINT_POLL_MAX = 150" in wizard, "A poll 5min")
    check("abandonOnTimeoutThenResolveOrReject" in wizard, "A timeout chama abandon")
    check("forcePaid" in wizard, "A forcePaid")

    # --- B) PIN gerencial ---
    check("api_pdv_mp_point_forcar_liberar" in views_mp, "B API forcar")
    check("api_pdv_mp_point_forcar_liberar" in urls, "B URL")
    check("apiPdvMpPointForcarLiberar" in pdv, "B bootstrap")
    check("showPdvPinGerencial" in wizard, "B overlay")
    check("Geraldo, Geraldinho ou Renan Hinnen" in wizard, "B nomes no overlay")
    check("mpPointBloqueio" in wizard, "B trata 409")
    check("limpar_mp_point_forcar_bypass" in views, "B limpa pós-venda")
    check("def _mp_point_marcar_forcado_liberar" in views_mp, "B marca órfão permanente")
    check("mp_point_forcado_liberar" in views_mp, "B flag payload")
    check("_mp_point_row_foi_forcado_liberar" in views_mp, "B bloqueio ignora forçado")
    check(
        "_mp_point_marcar_forcado_liberar(row, rotulo)" in views_mp,
        "B API chama marcar em PENDING e PAID",
    )
    check(
        "não promover a PAID" in views_mp or "nao promover a PAID" in views_mp,
        "B forçar não promove PAID",
    )

    # --- C) Match gerencial ---
    check(rotulo_gerencial_do_user(SimpleNamespace(username="Geraldo", first_name="", last_name="")) == "Geraldo", "C Geraldo")
    check(rotulo_gerencial_do_user(SimpleNamespace(username="Geraldinho", first_name="", last_name="")) == "Geraldinho", "C Geraldinho")
    check(rotulo_gerencial_do_user(SimpleNamespace(username="admin", first_name="Renan", last_name="Hinnen")) == "Renan Hinnen", "C Renan")
    check(rotulo_gerencial_do_user(SimpleNamespace(username="gmagromais", first_name="geraldo", last_name="hinnen")) == "Geraldo", "C geraldo hinnen")
    check(rotulo_gerencial_do_user(SimpleNamespace(username="caixa", first_name="Maria", last_name="")) is None, "C não-gerente")
    check(
        rotulo_gerencial_do_user(SimpleNamespace(username="Geraldinho", first_name="", last_name="")) != "Geraldo",
        "C dinho≠geraldo",
    )

    # --- D) indica pago/cancel ---
    check(mp_point_order_indica_pago({"status": "processed"}) is True, "D processed=pago")
    check(mp_point_order_indica_pago({"status": "created"}) is False, "D created≠pago")
    check(mp_point_order_indica_cancelado({"status": "canceled"}) is True, "D canceled")

    # --- E) bypass sessão ---
    rf = RequestFactory()
    req = rf.get("/")
    req.session = {}
    check(mp_point_forcar_bypass_ativo(req) is False, "E bypass off")
    gravar_mp_point_forcar_bypass(req, por="Geraldo", order_ids=["ORD_TEST"])
    check(mp_point_forcar_bypass_ativo(req) is True, "E bypass on")
    check(isinstance(req.session.get(SESSION_MP_POINT_FORCAR_KEY), dict), "E session key")
    # bloqueio com bypass ativo e sem session_key de Point → None
    check(mp_point_bloqueio_venda_sessao(req) is None, "E bloqueio respeita bypass")
    limpar_mp_point_forcar_bypass(req)
    check(mp_point_forcar_bypass_ativo(req) is False, "E bypass limpo")

    # bypass expirado
    gravar_mp_point_forcar_bypass(req, por="Renan Hinnen")
    req.session[SESSION_MP_POINT_FORCAR_KEY]["exp"] = time.time() - 10
    check(mp_point_forcar_bypass_ativo(req) is False, "E bypass expirado")

    # --- F) PIN validação (sem PIN real: vazio / 1234) ---
    ok_pin, rot, err = validar_pin_gerencial("")
    check(ok_pin is False and "PIN" in err, "F pin vazio")
    ok_pin2, _r2, err2 = validar_pin_gerencial("1234")
    check(ok_pin2 is False and "1234" in err2, "F pin 1234 bloqueado")
    ok_pin3, _r3, err3 = validar_pin_gerencial("99999999__nao_existe__")
    check(ok_pin3 is False and ("incorreto" in err3.lower() or "PIN" in err3), "F pin inventado")

    # --- G) reverse URL ---
    from django.urls import reverse

    check(reverse("api_pdv_mp_point_forcar_liberar").endswith("forcar-liberar/"), "G reverse forcar")
    check(reverse("api_pdv_mp_point_abandon").endswith("abandonar/"), "G reverse abandon")

    # --- H) nomes UI ---
    check("Geraldo" in PIN_GERENCIAL_NOMES_UI and "Geraldinho" in PIN_GERENCIAL_NOMES_UI and "Renan" in PIN_GERENCIAL_NOMES_UI, "H nomes")

    # --- I) users reais no Postgres loja (fonte da verdade) ---
    try:
        import re
        from urllib.parse import urlparse, unquote
        import psycopg2

        env = (ROOT / ".env").read_text(encoding="utf-8", errors="ignore")
        m = re.search(r"^AGRO_CATALOGO_DEST_DATABASE_URL=(.+)$", env, re.M)
        if not m:
            fail("I sem AGRO_CATALOGO_DEST_DATABASE_URL")
        else:
            url = m.group(1).strip().strip('"').strip("'")
            p = urlparse(url.replace("postgresql://", "postgres://", 1))
            conn = psycopg2.connect(
                dbname=p.path.lstrip("/"),
                user=unquote(p.username or ""),
                password=unquote(p.password or ""),
                host=p.hostname,
                port=p.port or 5432,
                sslmode="require",
                connect_timeout=20,
            )
            conn.set_session(readonly=True, autocommit=True)
            cur = conn.cursor()
            cur.execute(
                "SELECT id, username, first_name, last_name FROM auth_user ORDER BY id LIMIT 300"
            )
            found = set()
            for _id, un, fn, ln in cur.fetchall():
                u = SimpleNamespace(username=un or "", first_name=fn or "", last_name=ln or "")
                lab = rotulo_gerencial_do_user(u)
                if lab:
                    found.add(lab)
            conn.close()
            check("Geraldo" in found, "I PG tem Geraldo")
            check("Geraldinho" in found, "I PG tem Geraldinho")
            check("Renan Hinnen" in found, "I PG tem Renan Hinnen")
            check(len(found) >= 3, f"I gerenciais={sorted(found)}")
    except Exception as e:
        fail(f"I query loja PG: {e}")

    print("")
    print(f"OKS={OKS} FAILS={len(FAILS)}")
    if FAILS:
        for f in FAILS:
            print(" -", f)
        return 1
    print("VERIFY_MP_POINT_PIN_GERENCIAL_DEEP_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
