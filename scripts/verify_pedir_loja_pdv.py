"""Verificacao: Pedir loja (pedido de transferencia PDV Centro <-> Vila).
Roda: python scripts/verify_pedir_loja_pdv.py
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.urls import reverse
from django.test import Client

from estoque.models import HistoricoTransferencia, SolicitacaoTransferenciaPdv
from estoque.solicitacao_pdv_util import (
    aceitar,
    cancelar,
    criar_solicitacoes,
    listar,
    recusar,
    resumo,
)

PASS = 0
FAIL = 0


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print("OK  ", msg)


def fail(msg: str) -> None:
    global FAIL
    FAIL += 1
    print("FAIL", msg)


def check_file(rel: str, *needles: str) -> None:
    p = ROOT / rel
    if not p.is_file():
        fail(f"arquivo ausente: {rel}")
        return
    body = p.read_text(encoding="utf-8")
    missing = [n for n in needles if n not in body]
    if missing:
        fail(f"{rel} faltou {missing}")
    else:
        ok(rel)


def main() -> int:
    print("=== verify_pedir_loja_pdv ===")
    check_file(
        "produtos/templates/produtos/pdv_wizard.html",
        "pdv-topbar-pedir-loja-btn",
        "pdv_pedir_loja.js",
        "pedir_loja_overlay",
    )
    check_file(
        "produtos/templates/produtos/partials/pdv/pedir_loja_overlay.html",
        "pdv-pedir-loja-overlay",
        "Recebidos",
        "Enviar pedido",
    )
    check_file(
        "produtos/static/produtos/js/pdv_pedir_loja.js",
        "api/pdv/pedir-loja/",
        "pdv-wiz-topbar-btn--pedir-pendente",
        "Transferir agora",
    )
    check_file(
        "produtos/templates/produtos/transferencias.html",
        "abrirModalPedidosPdv",
        "Pedidos PDV",
        "api/pdv/pedir-loja/",
    )
    check_file(
        "produtos/urls.py",
        "api/pdv/pedir-loja/resumo/",
        "api_pdv_pedir_loja_acao",
    )
    check_file(
        "estoque/models.py",
        "class SolicitacaoTransferenciaPdv",
        "TIPO_PEDIDO_PDV",
    )

    for name in (
        "api_pdv_pedir_loja_resumo",
        "api_pdv_pedir_loja_lista",
        "api_pdv_pedir_loja_criar",
        "api_pdv_pedir_loja_acao",
    ):
        try:
            path = reverse(name, kwargs={"pk": 1} if name.endswith("acao") else None)
            if "/pedir-loja/" not in path:
                fail(f"reverse {name} path inesperado: {path}")
            else:
                ok(f"url {name} -> {path}")
        except Exception as exc:
            fail(f"reverse {name}: {exc}")

    marca = "VERIFY-PEDIR-LOJA"
    SolicitacaoTransferenciaPdv.objects.filter(nome_produto__startswith=marca).delete()

    criados, err = criar_solicitacoes(
        [
            {
                "produto_id": "pid-pedir-loja-1",
                "nome": f"{marca} racao",
                "codigo": "GM1",
                "quantidade": 2,
            },
            {
                "produto_id": "pid-pedir-loja-1",
                "nome": f"{marca} racao",
                "codigo": "GM1",
                "quantidade": 3,
            },
        ],
        "centro",
        usuario_label="teste-pdv",
    )
    if err or not criados or len(criados) != 1:
        fail(f"criar agrupou mal: err={err} n={len(criados or [])}")
    elif Decimal(str(criados[0]["quantidade"])) != Decimal("5"):
        fail(f"soma qtd esperada 5, veio {criados[0]['quantidade']}")
    elif criados[0]["loja_origem"] != "vila" or criados[0]["loja_destino"] != "centro":
        fail(f"direcao errada: {criados[0]}")
    else:
        ok("criar pedido Centro pede Vila (qtd somada 5)")

    r = resumo("vila")
    if r["pendentes_recebidos"] < 1 or r["badge"] < 1:
        fail(f"resumo vila sem badge: {r}")
    else:
        ok("badge na loja de origem (Vila)")

    rec = listar("vila", "recebidos")
    env = listar("centro", "enviados")
    if not rec or rec[0]["produto_id"] != "pid-pedir-loja-1":
        fail(f"lista recebidos vila: {rec[:1]}")
    elif not env:
        fail("lista enviados centro vazia")
    else:
        ok("listas recebidos/enviados")

    row = SolicitacaoTransferenciaPdv.objects.get(pk=criados[0]["id"])
    out, err = aceitar(row, "centro")
    if not err:
        fail("centro nao deveria aceitar (nao e origem)")
    else:
        ok("aceitar bloqueado na loja destino")

    row.refresh_from_db()
    out, err = aceitar(row, "vila", usuario_label="vila-op")
    if err or not out or out["status"] != "ACEITO":
        fail(f"aceitar vila: {err} {out}")
    else:
        ok("aceitar na origem")

    row.refresh_from_db()
    out, err = recusar(row, "vila")
    if err or out["status"] != "RECUSADO":
        fail(f"recusar apos aceito: {err} {out}")
    else:
        ok("recusar pedido aceito")

    criados2, err = criar_solicitacoes(
        [{"produto_id": "pid-pedir-loja-2", "nome": f"{marca} outro", "quantidade": 1}],
        "vila",
    )
    if err:
        fail(f"segundo criar: {err}")
    else:
        row2 = SolicitacaoTransferenciaPdv.objects.get(pk=criados2[0]["id"])
        out, err = cancelar(row2, "centro")
        if not err:
            fail("cancelar deveria ser so quem pediu")
        out, err = cancelar(row2, "vila")
        if err or out["status"] != "CANCELADO":
            fail(f"cancelar: {err} {out}")
        else:
            ok("cancelar pelo solicitante")

    if not HistoricoTransferencia.objects.filter(tipo=HistoricoTransferencia.TIPO_PEDIDO_PDV).exists():
        fail("historico PEDIDO_PDV nao gravou")
    else:
        ok("historico PEDIDO_PDV")

    c = Client()
    resp = c.get("/api/pdv/pedir-loja/resumo/", HTTP_HOST="localhost")
    if resp.status_code != 200:
        fail(f"GET resumo HTTP {resp.status_code}")
    else:
        body = resp.json()
        if not body.get("ok"):
            fail(f"GET resumo body {body}")
        else:
            ok("GET /api/pdv/pedir-loja/resumo/")

    SolicitacaoTransferenciaPdv.objects.filter(nome_produto__startswith=marca).delete()

    print(f"=== resultado: {PASS} ok / {FAIL} fail ===")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
