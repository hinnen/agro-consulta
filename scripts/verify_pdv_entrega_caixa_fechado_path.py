# -*- coding: utf-8 -*-
"""Prova: entrega a pagar não some do PDV quando o caixa original fecha.

  python scripts/verify_pdv_entrega_caixa_fechado_path.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client
from django.urls import reverse
from django.utils import timezone

from produtos.entrega_pdv_pendente_util import (
    adiar_entrega_caixa_um_dia,
    data_hoje_loja,
    listar_entregas_bloqueando_fechamento_caixa,
    listar_entregas_pendentes_pdv,
)
from produtos.models import PedidoEntrega, SessaoCaixa

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print("  OK  " + name + ((" — " + detail) if detail else ""))
    else:
        fails.append(name)
        print("  FAIL " + name + ((" — " + detail) if detail else ""))


def test_arquivos() -> None:
    print("== Arquivos ==")
    util = (ROOT / "produtos/entrega_pdv_pendente_util.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check(
        "overlay_default_aberto_false",
        "apenas_caixas_abertos: bool = False" in util,
    )
    check(
        "api_overlay_sem_filtro_caixa_aberto",
        "apenas_caixas_abertos=False" in views.split("def api_pdv_entregas_pendentes")[1][:600],
    )
    check(
        "bloqueio_inclui_caixa_fechado",
        "sessao_caixa__fechado_em__isnull=False" in util,
    )


def test_banco() -> None:
    print("== Banco / overlay ==")
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    if user is None:
        user = User.objects.create_user(username="prova_overlay_pdv", password="x")
    check("tem_user", user is not None)
    sess = SessaoCaixa.objects.create(usuario=user, ponto_caixa="gaveta", valor_abertura=0)
    ent = PedidoEntrega.objects.create(
        cliente_nome="Robert Bichao prova overlay",
        aguarda_pagamento_pdv=True,
        status=PedidoEntrega.Status.PENDENTE,
        sessao_caixa=sess,
        loja_entrega="centro",
        loja_pagamento="centro",
        forma_pagamento="Dinheiro",
        origem="pdv",
        pdv_wizard_state={"itens": []},
    )
    criado = [ent.pk]
    sessoes = [sess.pk]
    try:
        ids = {r["id"] for r in listar_entregas_pendentes_pdv(loja="centro")}
        check("overlay_com_caixa_aberto", ent.pk in ids)

        sess.fechado_em = timezone.now()
        sess.save(update_fields=["fechado_em"])
        ids_old = {
            r["id"]
            for r in listar_entregas_pendentes_pdv(
                loja="centro", apenas_caixas_abertos=True
            )
        }
        check("regressao_filtro_caixa_aberto_escondia", ent.pk not in ids_old)
        ids2 = {r["id"] for r in listar_entregas_pendentes_pdv(loja="centro")}
        check("overlay_com_caixa_fechado", ent.pk in ids2)
        ids_vila = {r["id"] for r in listar_entregas_pendentes_pdv(loja="vila")}
        check("vila_nao_lista_centro", ent.pk not in ids_vila)

        novo = SessaoCaixa.objects.create(usuario=user, ponto_caixa="gaveta", valor_abertura=0)
        sessoes.append(novo.pk)
        bloq = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[novo.pk], loja="centro"
        )
        check("caixa_novo_trava", any(r["id"] == ent.pk for r in bloq))
        bloq_v = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[novo.pk], loja="vila"
        )
        check("vila_nao_trava_centro", not any(r["id"] == ent.pk for r in bloq_v))

        out, err = adiar_entrega_caixa_um_dia(
            ent.pk, loja="centro", pin="", quem="Prova overlay"
        )
        check("adiar_ok", out is not None and not err, err or "")
        ids3 = {r["id"] for r in listar_entregas_pendentes_pdv(loja="centro")}
        check("overlay_depois_adiar", ent.pk in ids3)
        bloq_hoje = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[novo.pk], loja="centro"
        )
        check("adiar_solta_hoje", not any(r["id"] == ent.pk for r in bloq_hoje))

        c = Client()
        c.force_login(user)
        resp = c.get(
            reverse("api_pdv_entregas_pendentes") + "?loja=centro",
            HTTP_HOST="127.0.0.1",
        )
        data = resp.json() if resp.status_code == 200 else {}
        ids_api = {r.get("id") for r in (data.get("itens") or [])}
        check(
            "api_lista_caixa_fechado",
            resp.status_code == 200 and data.get("ok") and ent.pk in ids_api,
        )
    finally:
        PedidoEntrega.objects.filter(pk__in=criado).delete()
        SessaoCaixa.objects.filter(pk__in=sessoes).delete()
        if user.username == "prova_overlay_pdv":
            user.delete()


def main() -> int:
    test_arquivos()
    test_banco()
    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks)+len(fails)}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
