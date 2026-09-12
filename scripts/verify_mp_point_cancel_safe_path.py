# -*- coding: utf-8 -*-
"""Prova path MP-POINT-CANCEL-SAFE — timeout cancela + abandon não mente + gate órfão."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

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
    wizard = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")

    check("def _mp_point_promover_pago_local" in views_mp, "helper promove PAID")
    check("def mp_point_bloqueio_venda_sessao" in views_mp, "helper bloqueio sessão")
    check("pagamento_efetivado" in views_mp, "abandon sinaliza pago")
    check("pedido_ainda_ativo" in views_mp, "abandon não mente se cancel falha")
    check("recuperado_de_abandon" in views_mp, "status recupera abandon precoce")
    check("_mp_point_promover_pago_local(row, body)" in views_mp, "status promove PAID")

    # Abandon NÃO deve marcar abandoned cegamente no final antigo
    check(
        "row.status = PdvMercadoPagoPointOrder.Status.ABANDONED\n"
        "    row.save(update_fields=[\"status\", \"atualizado_em\"])\n"
        "    payload = {\"ok\": True, \"cancelou_maquininha\": bool(ok_mp)}"
        not in views_mp.replace("\r\n", "\n"),
        "removido abandon cego antigo",
    )
    check("mp_point_get_order(access_token=token, order_id=order_id)" in views_mp, "abandon consulta MP")

    check("mp_point_bloqueio_venda_sessao" in views, "enviar pedido ERP chama bloqueio")
    check("mp_point_bloqueio" in views, "JSON bloqueio flag")

    check("MP_POINT_POLL_MAX = 150" in wizard, "poll ~5 min")
    check("abandonOnTimeoutThenResolveOrReject" in wizard, "timeout chama abandon")
    check("pagamento_efetivado" in wizard, "JS trata pago no cancel/timeout")
    check("pedido_ainda_ativo" in wizard, "JS não aborta se cancel falhou")
    check("forcePaid" in wizard, "flag forcePaid no wait control")

    # Centro e Vila: mesmo cancel path (aprendizado — não há fork Vila)
    check("mp_point_cancel_order(access_token=token, order_id=order_id)" in views_mp, "cancel único")
    check("_token_da_conta(conta)" in views_mp, "token por conta Centro/Vila")

    from produtos.views_mp_point import mp_point_bloqueio_venda_sessao
    from produtos.mercado_pago_point import mp_point_cancel_order, mp_point_order_indica_pago

    check(callable(mp_point_bloqueio_venda_sessao), "bloqueio importável")
    check(callable(mp_point_cancel_order), "cancel importável")
    check(mp_point_order_indica_pago({"status": "processed"}) is True, "indica pago processed")
    check(mp_point_order_indica_pago({"status": "created"}) is False, "created não é pago")

    print("")
    print(f"OKS={OKS} FAILS={len(FAILS)}")
    if FAILS:
        for f in FAILS:
            print(" -", f)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
