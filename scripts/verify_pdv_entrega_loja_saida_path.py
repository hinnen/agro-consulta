# -*- coding: utf-8 -*-
"""Prova path PDV-ENTREGA-LOJA-SAIDA.

  python scripts/verify_pdv_entrega_loja_saida_path.py
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

from produtos.caixa_util import PONTO_CAIXA_GAVETA, PONTO_CAIXA_VILA
from produtos.entrega_pdv_pendente_util import resolver_sessao_caixa_entrega_pdv
from produtos.models import SessaoCaixa

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print("  OK  " + name + ((" — " + detail) if detail else ""))
    else:
        fails.append(name)
        print("  FAIL " + name + ((" — " + detail) if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Arquivos ==")
    js = _read("produtos/static/produtos/js/pdv_wizard.js")
    state = _read("produtos/static/produtos/js/pdv_state.js")
    ov = _read("produtos/templates/produtos/partials/pdv/entrega_wizard_overlay.html")
    html = _read("produtos/templates/produtos/partials/pdv/step_entrega.html")
    views = _read("produtos/views.py")
    util = _read("produtos/entrega_pdv_pendente_util.py")

    check("state_loja_saida", "lojaSaida:" in state and "lojaSaidaConfirmada" in state)
    check("overlay_btns", 'id="pdv-ed-loja-centro"' in ov and 'id="pdv-ed-loja-vila"' in ov)
    check("js_fase_loja", "if (!e.lojaSaidaConfirmada) return 'loja'" in js)
    check("js_confirmar", "function confirmarLojaSaidaEntrega" in js)
    check("js_payload", "loja_entrega: lojaSaidaEntregaAtual" in js)
    check("js_outra_loja_painel", "entregaVaiParaOutraLoja(state)" in js)
    check("resumo_loja", 'id="pdv-resumo-loja-saida"' in html)
    check("api_campos_loja", 'campos["loja_entrega"] = loja_dest' in views)
    check("sessao_dest", "obter_caixa_pai_aberto(loja_dest)" in util)


def test_sessao_dest() -> None:
    print("== Sessao destino ==")
    User = SessaoCaixa._meta.get_field("usuario").remote_field.model
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    check("tem_user", user is not None)
    if not user:
        return
    gav = SessaoCaixa.objects.create(usuario=user, ponto_caixa=PONTO_CAIXA_GAVETA, valor_abertura=0)
    vil = SessaoCaixa.objects.create(usuario=user, ponto_caixa=PONTO_CAIXA_VILA, valor_abertura=0)
    try:
        class Req:
            session = {"pdv_deposito": "centro"}

        s = resolver_sessao_caixa_entrega_pdv(Req(), {"loja_entrega": "vila"})
        check(
            "centro_manda_vila",
            s is not None and getattr(s, "ponto_caixa", "") == PONTO_CAIXA_VILA,
        )
        s2 = resolver_sessao_caixa_entrega_pdv(Req(), {"loja_entrega": "centro"})
        check(
            "mesmo_centro_usa_browser",
            s2 is None or getattr(s2, "ponto_caixa", "") == PONTO_CAIXA_GAVETA,
        )
    finally:
        SessaoCaixa.objects.filter(pk__in=[gav.pk, vil.pk]).delete()


def main() -> None:
    print("=== PDV-ENTREGA-LOJA-SAIDA ===")
    test_arquivos()
    try:
        test_sessao_dest()
    except Exception as ex:
        check("sessao_dest_exc", False, str(ex)[:200])
    print("")
    print(f"VERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhas: " + ", ".join(fails))
        sys.exit(1)


if __name__ == "__main__":
    main()
