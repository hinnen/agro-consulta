# -*- coding: utf-8 -*-
"""Prova path BUG-34 — devolução de entrega inclui frete ao zerar itens.

Casos:
  - milho + frete, checkbox frete OFF → frete entra (Gabriel)
  - parcial de item → frete NÃO auto
  - só frete restante → auto
  - HTTP real (caixa + PIN 9973): devolve e grava frete_devolvido
  - página detalhe com frete marcado

Uso: python scripts/verify_bug34_devol_frete_path.py
"""
from __future__ import annotations

import json
import os
import sys
import time
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model  # noqa: E402
from django.test import Client, override_settings  # noqa: E402
from django.urls import reverse  # noqa: E402

from produtos.caixa_util import rotulo_operador_pin, validar_pin_operador  # noqa: E402
from produtos.devolucao_venda_util import (  # noqa: E402
    frete_restante,
    montar_selecao_devolucao,
    valor_restante_venda,
)
from produtos.models import ItemVendaAgro, SessaoCaixa, VendaAgro  # noqa: E402
from produtos.pdv_transf_loja_util import PDV_OPERADOR_FRESCO_KEY  # noqa: E402

PIN = "9973"
FAILS: list[str] = []
OKS: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        OKS.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAILS.append(name)
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


class _FakeItem:
    def __init__(self, pk: int, qtd: Decimal, devolvida: Decimal, valor: Decimal, desc: str):
        self.pk = pk
        self.quantidade = qtd
        self.quantidade_devolvida = devolvida
        self.valor_total = valor
        self.valor_unitario = (valor / qtd) if qtd else Decimal("0")
        self.descricao = desc

    @property
    def quantidade_restante(self):
        return max(
            Decimal("0"),
            Decimal(str(self.quantidade)) - Decimal(str(self.quantidade_devolvida)),
        )


class _FakeQS:
    def __init__(self, items):
        self._items = list(items)

    def all(self):
        return self._items


class _FakeVenda:
    def __init__(self, frete: Decimal, frete_dev: Decimal, items):
        self.frete = frete
        self.frete_devolvido = frete_dev
        self.itens = _FakeQS(items)


def _sess_pin(c: Client, *, sessao_pk: int, label: str) -> None:
    s = c.session
    s["pdv_sessao_caixa_id"] = sessao_pk
    s["pdv_deposito"] = "centro"
    s["pdv_operador_nome"] = label
    s[PDV_OPERADOR_FRESCO_KEY] = float(time.time())
    s.save()


def main() -> int:
    print("=== PATH BUG-34 DEVOL FRETE ===\n")

    html = (ROOT / "produtos/templates/produtos/venda_agro_detalhe.html").read_text(
        encoding="utf-8"
    )
    util = (ROOT / "produtos/devolucao_venda_util.py").read_text(encoding="utf-8")

    print("[1] Marcadores UI / util")
    check("ui_frete_checked_html", 'id="devolucao-frete-chk"' in html and "checked" in html)
    check("ui_frete_destaque", "border-rose-400" in html and "Taxa de entrega" in html)
    check("ui_garantir_frete", "garantirFreteSeEsgotaItens" in html)
    check("ui_selecao_esgota", "selecaoEsgotaItens" in html)
    check("ui_default_frete_true", "freteChk.checked = freteRestante > 0.009" in html)
    check("ui_ajuda_frete", "todos" in html.lower() and "frete" in html.lower())
    check("util_esgota_fn", "_selecao_esgota_itens_restantes" in util)
    check("util_incluir_auto", "esgota_itens" in util and "incluir_frete" in util)
    check("util_frete_ja", "def frete_ja_devolvido" in util)
    check("util_save_frete", 'save(update_fields=["frete_devolvido"])' in util)
    views_txt = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check("view_cura_frete", "frete_ja_devolvido(v)" in views_txt)
    check("view_cura_total", "sistema (cura devolução)" in views_txt)

    print("\n[2] PIN 9973")
    pin_ok, pin_msg = validar_pin_operador(PIN)
    check("pin_9973_valido", pin_ok, pin_msg)
    pin_label = (rotulo_operador_pin(PIN) or "").strip()
    check("pin_9973_rotulo", bool(pin_label), pin_label or "(vazio)")

    print("\n[3] montar_selecao — milho + frete (caso Gabriel)")
    milho = _FakeItem(1, Decimal("1"), Decimal("0"), Decimal("80.00"), "MILHO")
    v = _FakeVenda(Decimal("15.00"), Decimal("0"), [milho])
    linhas, frete_v, err = montar_selecao_devolucao(
        v,
        itens_raw=[{"item_id": 1, "quantidade": 1}],
        devolver_frete=False,
        devolver_tudo=False,
    )
    check("sem_erro_milho", err is None, str(err))
    check("frete_auto_com_item_total", frete_v == Decimal("15.00"), str(frete_v))
    check("linha_milho", linhas is not None and len(linhas) == 1)

    print("\n[4] parcial de item — frete NÃO automático")
    milho2 = _FakeItem(2, Decimal("2"), Decimal("0"), Decimal("100.00"), "MILHO 2")
    v2 = _FakeVenda(Decimal("15.00"), Decimal("0"), [milho2])
    _, frete2, err2 = montar_selecao_devolucao(
        v2,
        itens_raw=[{"item_id": 2, "quantidade": 1}],
        devolver_frete=False,
        devolver_tudo=False,
    )
    check("parcial_sem_erro", err2 is None)
    check("parcial_sem_frete", frete2 == Decimal("0.00"), str(frete2))
    _, frete2b, _ = montar_selecao_devolucao(
        v2,
        itens_raw=[{"item_id": 2, "quantidade": 1}],
        devolver_frete=True,
        devolver_tudo=False,
    )
    check("parcial_frete_se_marcado", frete2b == Decimal("15.00"), str(frete2b))

    print("\n[5] só frete restante (itens já devolvidos)")
    milho3 = _FakeItem(3, Decimal("1"), Decimal("1"), Decimal("80.00"), "MILHO")
    v3 = _FakeVenda(Decimal("15.00"), Decimal("0"), [milho3])
    check("frete_restante_15", frete_restante(v3) == Decimal("15.00"))
    linhas3, frete3, err3 = montar_selecao_devolucao(
        v3, itens_raw=[], devolver_frete=False, devolver_tudo=False
    )
    check("so_frete_auto", err3 is None and frete3 == Decimal("15.00"), f"{err3}/{frete3}")
    check("so_frete_sem_linhas", linhas3 is not None and len(linhas3) == 0)

    print("\n[6] devolver_tudo clássico")
    milho4 = _FakeItem(4, Decimal("1"), Decimal("0"), Decimal("50.00"), "X")
    v4 = _FakeVenda(Decimal("10.00"), Decimal("0"), [milho4])
    linhas4, frete4, err4 = montar_selecao_devolucao(
        v4, itens_raw=None, devolver_frete=False, devolver_tudo=True
    )
    check("tudo_com_frete", err4 is None and frete4 == Decimal("10.00") and len(linhas4) == 1)

    print("\n[7] HTTP — venda milho+frete (Gabriel) + só frete preso")
    U = get_user_model()
    u = U.objects.filter(is_superuser=True).first() or U.objects.first()
    check("user_local", u is not None)
    if not u or not pin_ok:
        print("\nABORT: sem user/PIN para HTTP")
        print(f"\n{'VERIFY_FAIL' if FAILS else 'VERIFY_OK'}  {len(OKS)} ok · {len(FAILS)} fail")
        return 1 if FAILS else 0

    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        c = Client(HTTP_HOST="127.0.0.1")
        c.force_login(u)

        sess = SessaoCaixa.objects.create(
            usuario=u,
            valor_abertura=Decimal("100"),
            ponto_caixa=SessaoCaixa.PontoCaixa.GAVETA,
        )
        total = Decimal("95.00")  # 80 milho + 15 frete
        venda = VendaAgro.objects.create(
            cliente_nome="PROVA BUG34 Gabriel",
            total=total,
            frete=Decimal("15.00"),
            forma_pagamento="Dinheiro",
            pagamentos_json=[{"forma": "Dinheiro", "valor": float(total)}],
            sessao_caixa=sess,
            deposito="centro",
            estoque_baixa_agro_aplicada=False,
            usuario_registro="prova-bug34",
        )
        it = ItemVendaAgro.objects.create(
            venda=venda,
            descricao="MILHO PROVA BUG34",
            quantidade=Decimal("1"),
            valor_unitario=Decimal("80"),
            valor_total=Decimal("80.00"),
            codigo="BUG34-MILHO",
        )
        _sess_pin(c, sessao_pk=sess.pk, label=pin_label or "Renan")

        # A) checkbox frete OFF + pagar só milho → deve FALHAR (frete auto sobe o total)
        r_bad = c.post(
            reverse("api_venda_agro_devolver", args=[venda.pk]),
            data=json.dumps(
                {
                    "motivo": "bug34 pagar curto",
                    "itens": [{"item_id": it.pk, "quantidade": 1}],
                    "devolver_frete": False,
                    "pagamentos": [{"forma": "Dinheiro", "valor": 80}],
                    "cancelar_nfce": False,
                }
            ),
            content_type="application/json",
        )
        j_bad = r_bad.json() if r_bad.content else {}
        check(
            "http_pagar_sem_frete_rejeita",
            r_bad.status_code >= 400 or j_bad.get("ok") is not True,
            f"status={r_bad.status_code} body={j_bad.get('erro') or j_bad}",
        )

        # B) mesmo caso Gabriel: frete OFF no payload, mas pagar 95 → frete entra e zera venda
        r_ok = c.post(
            reverse("api_venda_agro_devolver", args=[venda.pk]),
            data=json.dumps(
                {
                    "motivo": "bug34 gabriel milho+frete",
                    "itens": [{"item_id": it.pk, "quantidade": 1}],
                    "devolver_frete": False,
                    "pagamentos": [{"forma": "Dinheiro", "valor": 95}],
                    "cancelar_nfce": False,
                    "pin": PIN,
                }
            ),
            content_type="application/json",
        )
        j_ok = r_ok.json() if r_ok.content else {}
        check(
            "http_gabriel_200",
            r_ok.status_code == 200 and j_ok.get("ok") is True,
            str(j_ok.get("erro") or r_ok.status_code),
        )
        venda.refresh_from_db()
        it.refresh_from_db()
        check("http_gabriel_frete_devolvido", Decimal(str(venda.frete_devolvido or 0)) == Decimal("15.00"))
        check("http_gabriel_item_zerado", Decimal(str(it.quantidade_restante)) <= Decimal("0.0001"))
        check("http_gabriel_venda_total", bool(venda.devolvida_em), str(venda.devolvida_em))
        check("http_gabriel_restante_zero", valor_restante_venda(venda) == Decimal("0.00"))

        # C) venda presa: item já devolvido, só frete
        sess2 = SessaoCaixa.objects.create(
            usuario=u,
            valor_abertura=Decimal("50"),
            ponto_caixa=SessaoCaixa.PontoCaixa.GAVETA,
        )
        v_preso = VendaAgro.objects.create(
            cliente_nome="PROVA BUG34 preso frete",
            total=Decimal("95.00"),
            frete=Decimal("15.00"),
            frete_devolvido=Decimal("0"),
            forma_pagamento="Dinheiro",
            pagamentos_json=[{"forma": "Dinheiro", "valor": 95}],
            sessao_caixa=sess2,
            deposito="centro",
            estoque_baixa_agro_aplicada=False,
            usuario_registro="prova-bug34",
        )
        it_p = ItemVendaAgro.objects.create(
            venda=v_preso,
            descricao="MILHO JA DEVOLVIDO",
            quantidade=Decimal("1"),
            quantidade_devolvida=Decimal("1"),
            valor_unitario=Decimal("80"),
            valor_total=Decimal("80.00"),
            codigo="BUG34-PRESO",
        )
        _sess_pin(c, sessao_pk=sess2.pk, label=pin_label or "Renan")
        r_f = c.post(
            reverse("api_venda_agro_devolver", args=[v_preso.pk]),
            data=json.dumps(
                {
                    "motivo": "bug34 so frete",
                    "itens": [],
                    "devolver_frete": False,
                    "pagamentos": [{"forma": "Dinheiro", "valor": 15}],
                    "cancelar_nfce": False,
                }
            ),
            content_type="application/json",
        )
        j_f = r_f.json() if r_f.content else {}
        check(
            "http_so_frete_200",
            r_f.status_code == 200 and j_f.get("ok") is True,
            str(j_f.get("erro") or r_f.status_code),
        )
        v_preso.refresh_from_db()
        check("http_so_frete_gravado", Decimal(str(v_preso.frete_devolvido or 0)) == Decimal("15.00"))
        check("http_so_frete_totalizou", bool(v_preso.devolvida_em))

        # D) parcial: 1 de 2 — frete NÃO some
        sess3 = SessaoCaixa.objects.create(
            usuario=u,
            valor_abertura=Decimal("50"),
            ponto_caixa=SessaoCaixa.PontoCaixa.GAVETA,
        )
        v_par = VendaAgro.objects.create(
            cliente_nome="PROVA BUG34 parcial",
            total=Decimal("115.00"),
            frete=Decimal("15.00"),
            forma_pagamento="Dinheiro",
            pagamentos_json=[{"forma": "Dinheiro", "valor": 115}],
            sessao_caixa=sess3,
            deposito="centro",
            estoque_baixa_agro_aplicada=False,
            usuario_registro="prova-bug34",
        )
        it_par = ItemVendaAgro.objects.create(
            venda=v_par,
            descricao="MILHO PARCIAL",
            quantidade=Decimal("2"),
            valor_unitario=Decimal("50"),
            valor_total=Decimal("100.00"),
            codigo="BUG34-PAR",
        )
        _sess_pin(c, sessao_pk=sess3.pk, label=pin_label or "Renan")
        r_p = c.post(
            reverse("api_venda_agro_devolver", args=[v_par.pk]),
            data=json.dumps(
                {
                    "motivo": "bug34 parcial",
                    "itens": [{"item_id": it_par.pk, "quantidade": 1}],
                    "devolver_frete": False,
                    "pagamentos": [{"forma": "Dinheiro", "valor": 50}],
                    "cancelar_nfce": False,
                }
            ),
            content_type="application/json",
        )
        j_p = r_p.json() if r_p.content else {}
        check(
            "http_parcial_200",
            r_p.status_code == 200 and j_p.get("ok") is True,
            str(j_p.get("erro") or r_p.status_code),
        )
        v_par.refresh_from_db()
        check("http_parcial_frete_fica", Decimal(str(v_par.frete_devolvido or 0)) == Decimal("0"))
        check("http_parcial_nao_total", not bool(v_par.devolvida_em))
        check("http_parcial_frete_restante", frete_restante(v_par) == Decimal("15.00"))

        # E) página detalhe — frete checked no HTML
        rp = c.get(reverse("venda_agro_detalhe", args=[v_par.pk]))
        page = rp.content.decode("utf-8", errors="replace") if rp.status_code == 200 else ""
        check("page_200", rp.status_code == 200, str(rp.status_code))
        check("page_frete_chk", 'id="devolucao-frete-chk"' in page)
        check("page_frete_checked_attr", 'id="devolucao-frete-chk"' in page and "checked" in page)
        check("page_garantir_js", "garantirFreteSeEsgotaItens" in page)
        # cleanup hint: it_p used
        _ = it_p.pk

    print(f"\n---\nOK={len(OKS)} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    if FAILS:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
