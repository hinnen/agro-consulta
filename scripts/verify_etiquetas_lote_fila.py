#!/usr/bin/env python
"""Prova rápida ETQ-LOTE-FILA — helpers + API fila/loja (Django)."""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client

from produtos.models import EtiquetaLoteAgro
from produtos.views import (
    _etiquetas_lote_flat,
    _etiquetas_lote_normalizar_itens_entrada,
    _etiquetas_lote_parse_config,
    _etiquetas_lote_totais,
)


def main() -> int:
    ok = 0
    fail = 0

    def check(name: str, cond: bool, detail: str = "") -> None:
        nonlocal ok, fail
        if cond:
            ok += 1
            print(f"  OK  {name}")
        else:
            fail += 1
            print(f"  FAIL {name} {detail}")

    itens = _etiquetas_lote_normalizar_itens_entrada(
        [
            {"id": "1", "nome": "Veneno", "qtd": 3, "preco_venda": "2,00"},
            {"id": "2", "nome": "Anticion", "qtd": 1, "preco_venda": 10},
        ]
    )
    flat = _etiquetas_lote_flat(itens)
    check("normaliza 2 produtos", len(itens) == 2)
    check("flat QTD 3+1=4", len(flat) == 4)
    check("preco 2.0", itens[0]["preco_venda"] == 2.0)

    cfg = _etiquetas_lote_parse_config(
        {"folhas_por_vez": 2, "etiquetas_por_folha": 18, "modo": "auto", "intervalo_seg": 5}
    )
    check("config folhas 2", cfg["folhas_por_vez"] == 2)
    check("config modo auto", cfg["modo"] == "auto")
    check("config clamp etq", _etiquetas_lote_parse_config({"etiquetas_por_folha": 99})["etiquetas_por_folha"] == 54)

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.filter(is_staff=True).first()
    if not user:
        user = User.objects.create_user("etq_lote_test", password="x")
    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(user)

    # limpa lotes de teste anteriores
    EtiquetaLoteAgro.objects.filter(nome__startswith="TEST-LOTE-FILA").delete()

    r = c.post(
        "/api/produtos/etiquetas/lote/",
        data={
            "origem": "fila",
            "nome": "TEST-LOTE-FILA",
            "preset_id": "gondola",
            "etiquetas_por_folha": 18,
            "folhas_por_vez": 2,
            "intervalo_seg": 3,
            "modo": "pausa",
            "itens": [
                {"id": "p1", "nome": "Item A", "qtd": 2, "preco_venda": 1.5},
                {"id": "p2", "nome": "Item B", "qtd": 1, "preco_venda": 2},
            ],
        },
        content_type="application/json",
    )
    j = r.json()
    check("POST fila 200", r.status_code == 200 and j.get("ok"), str(j)[:120])
    lote = j.get("lote") or {}
    pk = lote.get("id")
    check("total etiquetas 3", lote.get("total") == 3, str(lote.get("total")))
    check("proxima_qtd 3 (2 folhas x 18 cap)", lote.get("proxima_qtd") == 3, str(lote.get("proxima_qtd")))
    check("modo pausa", (lote.get("config") or {}).get("modo") == "pausa")

    if pk:
        r2 = c.post(
            f"/api/produtos/etiquetas/lote/{pk}/atualizar/",
            data={"qtd_massa": 2, "folhas_por_vez": 1, "etiquetas_por_folha": 2},
            content_type="application/json",
        )
        j2 = r2.json()
        lote2 = j2.get("lote") or {}
        check("atualizar qtd_massa", r2.status_code == 200 and j2.get("ok"), str(j2)[:120])
        check("total apos massa 4", lote2.get("total") == 4, str(lote2.get("total")))
        check("folha_size 2", lote2.get("folha_size") == 2, str(lote2.get("folha_size")))
        check("proxima_qtd 2", lote2.get("proxima_qtd") == 2, str(lote2.get("proxima_qtd")))

        r3 = c.post(f"/api/produtos/etiquetas/lote/{pk}/proxima-folha/", data={}, content_type="application/json")
        j3 = r3.json()
        check("proxima-folha", r3.status_code == 200 and j3.get("ok") and len(j3.get("itens") or []) == 2)

        r4 = c.post(
            f"/api/produtos/etiquetas/lote/{pk}/confirmar-folha/",
            data={"qtd": 2},
            content_type="application/json",
        )
        j4 = r4.json()
        lote4 = j4.get("lote") or {}
        check("confirmar cursor 2", lote4.get("cursor") == 2, str(lote4.get("cursor")))
        check("ainda aberto", lote4.get("status") == "aberto")

        r5 = c.post(f"/api/produtos/etiquetas/lote/{pk}/desfazer-folha/", data={}, content_type="application/json")
        j5 = r5.json()
        lote5 = j5.get("lote") or {}
        check("desfazer cursor 0", lote5.get("cursor") == 0, str(lote5.get("cursor")))

        # totais diretos
        obj = EtiquetaLoteAgro.objects.get(pk=pk)
        t = _etiquetas_lote_totais(obj)
        check("totais flat 4", t["total"] == 4)

        c.post(f"/api/produtos/etiquetas/lote/{pk}/cancelar/", data={}, content_type="application/json")

    print(f"\nResultado: {ok} ok · {fail} fail")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
