# -*- coding: utf-8 -*-
"""Prova path SAVE-ORC-CAIXA — orçamentos recentes + contagem caixa PG."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client, override_settings
from django.urls import reverse

from produtos.models import CaixaConferenciaRascunhoAgro, OrcamentoPdvAgro
from produtos.views import (
    _caixa_contagem_pg_carregar,
    _caixa_contagem_pg_limpar,
    _caixa_contagem_pg_salvar,
    _caixa_contagem_turno_key_loja,
)

FAILS: list[str] = []
OKS: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        OKS.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAILS.append(name)
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


def main() -> int:
    print("=== PATH SAVE-ORC-CAIXA ===\n")

    # --- arquivos / JS ---
    print("[1] Arquivos e marcadores")
    mig = ROOT / "produtos/migrations/0090_caixa_conferencia_rascunho_agro.py"
    check("migrate_0090", mig.is_file())
    mig_txt = mig.read_text(encoding="utf-8") if mig.is_file() else ""
    check("migrate_so_modelo", "CaixaConferenciaRascunhoAgro" in mig_txt and "RenameIndex" not in mig_txt)

    js_c = (ROOT / "produtos/static/produtos/js/consulta_produtos.js").read_text(encoding="utf-8")
    js_h = (ROOT / "static/js/home_modals_pdv.js").read_text(encoding="utf-8")
    html_cx = (ROOT / "produtos/templates/produtos/caixa_fechar.html").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")

    check("consulta_recentes", "recentes=1" in js_c)
    check("consulta_fetch_servidor", "fetchOrcamentoPdvServidor" in js_c and "aplicar(remote)" in js_c)
    check("home_recentes", "recentes=1" in js_h)
    check("home_api_url", "apiPdvOrcamentos" in js_h)
    check("caixa_html_turno_key", "turno_key" in html_cx and "contagem_turno_key" in html_cx)
    check("views_recentes_api", '"escopo": "recentes"' in views)
    check("views_pg_helpers", "_caixa_contagem_pg_salvar" in views and "_caixa_contagem_turno_key_loja" in views)
    check("home_bootstrap_orc", '"apiPdvOrcamentos": reverse("api_pdv_orcamentos")' in views)

    # --- helpers PG puros ---
    print("\n[2] Helpers Postgres contagem")
    key = _caixa_contagem_turno_key_loja("centro")
    check("turno_key_formato", key.endswith("::centro") and len(key) > 12, key)
    _caixa_contagem_pg_limpar(key)
    clean, ced = _caixa_contagem_pg_salvar(
        key, {"Dinheiro": "123,45", "PIX": "10"}, {"100": "1", "50": "2"}, usuario="prova"
    )
    check("pg_salvar", clean.get("Dinheiro") == "123,45" and ced.get("100") == "1")
    r2, c2 = _caixa_contagem_pg_carregar(key)
    check("pg_carregar", r2.get("Dinheiro") == "123,45" and c2.get("50") == "2")
    row = CaixaConferenciaRascunhoAgro.objects.filter(turno_key=key).first()
    check("pg_row", row is not None and row.atualizado_por == "prova")
    key_vila = _caixa_contagem_turno_key_loja("vila")
    check("turno_keys_distintos", key != key_vila, f"{key} vs {key_vila}")
    _caixa_contagem_pg_salvar(key_vila, {"Dinheiro": "1"}, {}, usuario="vila")
    r_c, _ = _caixa_contagem_pg_carregar(key)
    r_v, _ = _caixa_contagem_pg_carregar(key_vila)
    check("isolamento_lojas", r_c.get("Dinheiro") == "123,45" and r_v.get("Dinheiro") == "1")
    _caixa_contagem_pg_limpar(key)
    _caixa_contagem_pg_limpar(key_vila)
    check("pg_limpar", CaixaConferenciaRascunhoAgro.objects.filter(turno_key__in=[key, key_vila]).count() == 0)

    # --- HTTP ---
    print("\n[3] HTTP APIs")
    U = get_user_model()
    u = U.objects.filter(is_superuser=True).first()
    check("superuser", u is not None)
    if not u:
        print("\nABORT: sem superuser")
        return 1

    with override_settings(ALLOWED_HOSTS=["*"]):
        c = Client()
        c.force_login(u)

        # orçamentos recentes
        r = c.get("/api/pdv/orcamentos/?recentes=1&limite=10")
        body = r.json() if r.status_code == 200 else {}
        check("GET_recentes", r.status_code == 200 and body.get("ok") is True, f"status={r.status_code}")
        check("GET_recentes_items", isinstance(body.get("items"), list))

        # GET sem key deve falhar
        r_bad = c.get("/api/pdv/orcamentos/")
        check("GET_sem_key_400", r_bad.status_code == 400)

        # POST orçamento
        oid = 1786999999001
        OrcamentoPdvAgro.objects.filter(orc_local_id=oid).delete()
        entry = {
            "id": oid,
            "cliente": "PROVA SAVE-ORC",
            "cliente_key": "tmp:prova-save-orc:",
            "cliente_mode": "cliente",
            "total": "R$ 9,99",
            "itens": [{"id": "x", "nome": "Item prova", "qtd": 1, "preco": 9.99}],
            "entrega": False,
            "forma_pagamento": "",
            "origem": "manual",
            "usuario": "prova",
        }
        r_post = c.post(
            "/api/pdv/orcamentos/",
            data=json.dumps({"entry": entry}),
            content_type="application/json",
        )
        pb = r_post.json() if r_post.status_code == 200 else {}
        check("POST_orcamento", r_post.status_code == 200 and pb.get("ok") is True, str(pb.get("erro") or ""))
        check("POST_persistiu", OrcamentoPdvAgro.objects.filter(orc_local_id=oid).exists())

        r_det = c.get(f"/api/pdv/orcamentos/{oid}/")
        db = r_det.json() if r_det.status_code == 200 else {}
        check("GET_detalhe", r_det.status_code == 200 and db.get("item", {}).get("id") == oid)

        r_cli = c.get("/api/pdv/orcamentos/?cliente_key=tmp:prova-save-orc:")
        cb = r_cli.json() if r_cli.status_code == 200 else {}
        ids = [i.get("id") for i in (cb.get("items") or [])]
        check("GET_por_cliente", oid in ids)

        r_rec2 = c.get("/api/pdv/orcamentos/?recentes=1&limite=20")
        ids2 = [i.get("id") for i in (r_rec2.json().get("items") or [])]
        check("GET_recentes_inclui_novo", oid in ids2)

        # caixa save/load HTTP
        r_cx = c.post(
            reverse("api_caixa_conferencia_rascunho_salvar"),
            data=json.dumps(
                {
                    "rascunho": {"Dinheiro": "50,00"},
                    "cedulas": {"20": "2"},
                    "deposito": "centro",
                }
            ),
            content_type="application/json",
        )
        cxb = r_cx.json() if r_cx.status_code == 200 else {}
        check("POST_caixa_contagem", r_cx.status_code == 200 and cxb.get("ok") is True, str(cxb))
        tk = cxb.get("turno_key") or ""
        check("POST_caixa_turno_key", "::centro" in tk, tk)

        r_get = c.get("/api/caixa/conferencia-rascunho/?deposito=centro")
        gb = r_get.json() if r_get.status_code == 200 else {}
        check(
            "GET_caixa_contagem",
            r_get.status_code == 200
            and (gb.get("rascunho") or {}).get("Dinheiro") == "50,00"
            and (gb.get("cedulas") or {}).get("20") == "2",
            str(gb),
        )

        # limpeza
        OrcamentoPdvAgro.objects.filter(orc_local_id=oid).delete()
        if tk:
            _caixa_contagem_pg_limpar(tk)

    # --- reverse names ---
    print("\n[4] URLs nomeadas")
    check("url_orcamentos", reverse("api_pdv_orcamentos") == "/api/pdv/orcamentos/")
    check(
        "url_caixa_salvar",
        "conferencia-rascunho" in reverse("api_caixa_conferencia_rascunho_salvar"),
    )

    print("\n=== RESUMO ===")
    print(f"OK: {len(OKS)}  FAIL: {len(FAILS)}")
    if FAILS:
        print("Falhas:", ", ".join(FAILS))
        return 1
    print("PATH OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
