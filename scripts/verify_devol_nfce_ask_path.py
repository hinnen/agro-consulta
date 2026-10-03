# -*- coding: utf-8 -*-
"""Prova path DEVOL-NFCE-ASK — pergunta cancelar cupom na devolução total.

Uso: python scripts/verify_devol_nfce_ask_path.py
"""
from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client, override_settings
from django.urls import reverse

from produtos.models import (
    ItemVendaAgro,
    NfceDocumentoAgro,
    SessaoCaixa,
    VendaAgro,
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


def _selecao_totaliza_py(
    itens: list[dict],
    *,
    frete_restante: float,
    frete_marcado: bool,
) -> bool:
    """Espelho da lógica JS selecaoTotaliza (prova de acerto)."""
    tem_linha = False
    for it in itens:
        q_max = float(it.get("qtd_restante") or 0)
        if q_max <= 0.0001:
            continue
        tem_linha = True
        if not it.get("checked"):
            return False
        q = float(it.get("qtd") or 0)
        if q + 0.0001 < q_max:
            return False
    if frete_restante > 0.009:
        tem_linha = True
        if not frete_marcado:
            return False
    return tem_linha


def main() -> int:
    print("=== PATH DEVOL-NFCE-ASK ===\n")

    html = (ROOT / "produtos/templates/produtos/venda_agro_detalhe.html").read_text(
        encoding="utf-8"
    )
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    banana = (ROOT / "banana.md").read_text(encoding="utf-8")

    print("[1] Marcadores UI / API")
    check("ui_modal_nfce_box", 'id="devolucao-ask-nfce"' in html)
    check("ui_btn_manter", 'id="btn-devolucao-ask-manter-cupom"' in html)
    check("ui_btn_cancelar_cupom", 'id="btn-devolucao-ask-cancelar-cupom"' in html)
    check("ui_selecao_totaliza", "function selecaoTotaliza()" in html)
    check("ui_tem_nfce_flag", "temNfceAutorizada" in html)
    check("ui_perguntar_nfce", "perguntarNfce = temNfceAutorizada && selecaoTotaliza()" in html)
    check("ui_payload_cancelar", "payload.cancelar_nfce = cancelarNfce" in html)
    check("ui_ajuda_pergunta", "pergunta" in html and "manter" in html.lower())
    check("api_flag_parse", '"cancelar_nfce" not in payload' in views)
    check("api_manter_aviso", "Cupom fiscal NFC-e" in views and "mantido" in views)
    check("api_so_totalizou", "if totalizou:" in views and "cancelar_nfce_pedido" in views)
    check("banana_4_3", "Devolver e cancelar cupom" in banana and "manter cupom" in banana)

    print("\n[2] Lógica selecaoTotaliza (espelho)")
    check(
        "total_sem_frete",
        _selecao_totaliza_py(
            [{"checked": True, "qtd": 2, "qtd_restante": 2}],
            frete_restante=0,
            frete_marcado=False,
        )
        is True,
    )
    check(
        "parcial_qtd",
        _selecao_totaliza_py(
            [{"checked": True, "qtd": 1, "qtd_restante": 2}],
            frete_restante=0,
            frete_marcado=False,
        )
        is False,
    )
    check(
        "item_desmarcado",
        _selecao_totaliza_py(
            [{"checked": False, "qtd": 2, "qtd_restante": 2}],
            frete_restante=0,
            frete_marcado=False,
        )
        is False,
    )
    check(
        "frete_obrigatorio",
        _selecao_totaliza_py(
            [{"checked": True, "qtd": 1, "qtd_restante": 1}],
            frete_restante=5.0,
            frete_marcado=False,
        )
        is False,
    )
    check(
        "frete_marcado_total",
        _selecao_totaliza_py(
            [{"checked": True, "qtd": 1, "qtd_restante": 1}],
            frete_restante=5.0,
            frete_marcado=True,
        )
        is True,
    )
    check(
        "so_frete",
        _selecao_totaliza_py([], frete_restante=8.5, frete_marcado=True) is True,
    )

    print("\n[3] HTTP devolução + mock SEFAZ")
    U = get_user_model()
    u = U.objects.filter(is_superuser=True).first()
    check("superuser", u is not None)
    if not u:
        print("\nABORT: sem superuser local")
        return 1

    def _mk_venda(*, tag: str, frete: Decimal = Decimal("0")) -> tuple[VendaAgro, ItemVendaAgro, NfceDocumentoAgro, SessaoCaixa]:
        sess = SessaoCaixa.objects.create(
            usuario=u,
            valor_abertura=Decimal("50"),
            ponto_caixa=SessaoCaixa.PontoCaixa.GAVETA,
        )
        total = Decimal("10.00") + frete
        v = VendaAgro.objects.create(
            cliente_nome=f"PROVA DEVOL-NFCE {tag}",
            total=total,
            frete=frete,
            forma_pagamento="Dinheiro",
            pagamentos_json=[{"forma": "Dinheiro", "valor": float(total)}],
            sessao_caixa=sess,
            deposito="centro",
            estoque_baixa_agro_aplicada=False,
            usuario_registro="prova",
            nfce_solicitada=True,
        )
        it = ItemVendaAgro.objects.create(
            venda=v,
            descricao=f"Item prova {tag}",
            quantidade=Decimal("1"),
            valor_unitario=Decimal("10"),
            valor_total=Decimal("10.00"),
            codigo="PROVA-DEVOL",
        )
        nf = NfceDocumentoAgro.objects.create(
            venda=v,
            status=NfceDocumentoAgro.Status.AUTORIZADA,
            chave="35260748900774000103550010000000011999999999"[:44],
            numero=999001,
            serie=21,
            protocolo="135260000000001",
            emitente_cnpj="48900774000103",
            tp_amb=2,
        )
        return v, it, nf, sess

    cancel_calls: list[int] = []

    def _fake_cancel(doc, **kwargs):
        cancel_calls.append(doc.pk)
        doc.status = NfceDocumentoAgro.Status.CANCELADA
        doc.mensagem_sefaz = "cancel mock"
        doc.save(update_fields=["status", "mensagem_sefaz"])
        return {"ok": True, "documento_id": doc.pk}

    with override_settings(ALLOWED_HOSTS=["*"]):
        c = Client()
        c.force_login(u)

        # --- A: manter cupom (cancelar_nfce=false) ---
        v1, it1, nf1, s1 = _mk_venda(tag="manter")
        cancel_calls.clear()
        c.session["pdv_sessao_caixa_id"] = s1.pk
        c.session["pdv_deposito"] = "centro"
        c.session.save()
        with patch(
            "produtos.nfce_sp_emissao_util.cancelar_nfce_autorizada",
            side_effect=_fake_cancel,
        ):
            r = c.post(
                reverse("api_venda_agro_devolver", args=[v1.pk]),
                data=json.dumps(
                    {
                        "motivo": "prova manter",
                        "itens": [{"item_id": it1.pk, "quantidade": 1}],
                        "devolver_frete": False,
                        "pagamentos": [{"forma": "Dinheiro", "valor": 10}],
                        "cancelar_nfce": False,
                    }
                ),
                content_type="application/json",
            )
        body = r.json() if r.status_code == 200 else {}
        check("http_manter_200", r.status_code == 200 and body.get("ok") is True, str(body.get("erro") or r.status_code))
        check("http_manter_nao_chamou_sefaz", cancel_calls == [], str(cancel_calls))
        check("http_manter_nfce_flag", body.get("nfce_cancelada") is False)
        nf1.refresh_from_db()
        v1.refresh_from_db()
        check("http_manter_status_autorizada", nf1.status == NfceDocumentoAgro.Status.AUTORIZADA)
        check("http_manter_venda_devolvida", bool(v1.devolvida_em))
        avisos = " ".join(body.get("avisos") or [])
        check("http_manter_aviso", "mantido" in avisos.lower())

        # --- B: cancelar cupom (cancelar_nfce=true) ---
        v2, it2, nf2, s2 = _mk_venda(tag="cancelar")
        cancel_calls.clear()
        c.session["pdv_sessao_caixa_id"] = s2.pk
        c.session["pdv_deposito"] = "centro"
        c.session.save()
        with patch(
            "produtos.nfce_sp_emissao_util.cancelar_nfce_autorizada",
            side_effect=_fake_cancel,
        ):
            r2 = c.post(
                reverse("api_venda_agro_devolver", args=[v2.pk]),
                data=json.dumps(
                    {
                        "motivo": "prova cancelar",
                        "itens": [{"item_id": it2.pk, "quantidade": 1}],
                        "devolver_frete": False,
                        "pagamentos": [{"forma": "Dinheiro", "valor": 10}],
                        "cancelar_nfce": True,
                    }
                ),
                content_type="application/json",
            )
        body2 = r2.json() if r2.status_code == 200 else {}
        check("http_cancel_200", r2.status_code == 200 and body2.get("ok") is True, str(body2.get("erro") or r2.status_code))
        check("http_cancel_chamou_sefaz", cancel_calls == [nf2.pk], str(cancel_calls))
        check("http_cancel_flag", body2.get("nfce_cancelada") is True)
        nf2.refresh_from_db()
        check("http_cancel_status", nf2.status == NfceDocumentoAgro.Status.CANCELADA)

        # --- C: parcial nunca cancela mesmo com cancelar_nfce=true ---
        v3, it3, nf3, s3 = _mk_venda(tag="parcial")
        ItemVendaAgro.objects.create(
            venda=v3,
            descricao="Item 2 prova",
            quantidade=Decimal("1"),
            valor_unitario=Decimal("5"),
            valor_total=Decimal("5.00"),
            codigo="PROVA-DEVOL-2",
        )
        v3.total = Decimal("15.00")
        v3.pagamentos_json = [{"forma": "Dinheiro", "valor": 15}]
        v3.save(update_fields=["total", "pagamentos_json"])
        cancel_calls.clear()
        c.session["pdv_sessao_caixa_id"] = s3.pk
        c.session["pdv_deposito"] = "centro"
        c.session.save()
        with patch(
            "produtos.nfce_sp_emissao_util.cancelar_nfce_autorizada",
            side_effect=_fake_cancel,
        ):
            r3 = c.post(
                reverse("api_venda_agro_devolver", args=[v3.pk]),
                data=json.dumps(
                    {
                        "motivo": "prova parcial",
                        "itens": [{"item_id": it3.pk, "quantidade": 1}],
                        "devolver_frete": False,
                        "pagamentos": [{"forma": "Dinheiro", "valor": 10}],
                        "cancelar_nfce": True,
                    }
                ),
                content_type="application/json",
            )
        body3 = r3.json() if r3.status_code == 200 else {}
        check("http_parcial_200", r3.status_code == 200 and body3.get("ok") is True, str(body3.get("erro") or r3.status_code))
        check("http_parcial_nao_cancela", cancel_calls == [] and body3.get("nfce_cancelada") is False)
        check("http_parcial_flag", body3.get("parcial") is True)
        nf3.refresh_from_db()
        v3.refresh_from_db()
        check("http_parcial_nfce_autorizada", nf3.status == NfceDocumentoAgro.Status.AUTORIZADA)
        check("http_parcial_venda_aberta", not bool(v3.devolvida_em))

        # --- D: sem chave cancelar_nfce → compat True (tenta cancelar) ---
        v4, it4, nf4, s4 = _mk_venda(tag="compat")
        cancel_calls.clear()
        c.session["pdv_sessao_caixa_id"] = s4.pk
        c.session["pdv_deposito"] = "centro"
        c.session.save()
        with patch(
            "produtos.nfce_sp_emissao_util.cancelar_nfce_autorizada",
            side_effect=_fake_cancel,
        ):
            r4 = c.post(
                reverse("api_venda_agro_devolver", args=[v4.pk]),
                data=json.dumps(
                    {
                        "motivo": "prova compat",
                        "itens": [{"item_id": it4.pk, "quantidade": 1}],
                        "devolver_frete": False,
                        "pagamentos": [{"forma": "Dinheiro", "valor": 10}],
                    }
                ),
                content_type="application/json",
            )
        body4 = r4.json() if r4.status_code == 200 else {}
        check("http_compat_200", r4.status_code == 200 and body4.get("ok") is True, str(body4.get("erro") or r4.status_code))
        check("http_compat_cancela", cancel_calls == [nf4.pk] and body4.get("nfce_cancelada") is True)

        # --- E: página detalhe renderiza botões ---
        v5, _it5, _nf5, _s5 = _mk_venda(tag="page")
        rp = c.get(reverse("venda_agro_detalhe", args=[v5.pk]))
        page = rp.content.decode("utf-8", errors="replace") if rp.status_code == 200 else ""
        check("page_200", rp.status_code == 200, str(rp.status_code))
        check("page_btn_manter", "btn-devolucao-ask-manter-cupom" in page)
        check("page_btn_cancelar", "btn-devolucao-ask-cancelar-cupom" in page)
        check("page_tem_nfce_true", "temNfceAutorizada = true" in page)

    print(f"\n{'VERIFY_OK' if not FAILS else 'VERIFY_FAIL'}  {len(OKS)} ok · {len(FAILS)} fail")
    if FAILS:
        for f in FAILS:
            print(f"  - {f}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
