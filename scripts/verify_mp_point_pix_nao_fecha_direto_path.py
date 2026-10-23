#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Prova path: PIX Point automático não fecha venda sem acionar a máquina.

  python scripts/verify_mp_point_pix_nao_fecha_direto_path.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ok = 0
fail = 0


def check(cond: bool, msg: str) -> None:
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {msg}")
    else:
        fail += 1
        print(f"  FAIL {msg}")


def main() -> int:
    print("== MP-POINT-PIX-NAO-FECHA-DIRETO ==")
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    vp = (ROOT / "produtos/views_mp_point.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")

    check("function mpPointApiPronta" in js, "JS: mpPointApiPronta")
    check("function maquinaIncompativelComForma" in js, "JS: maquina incompatível forma")
    check("mid === 'pix_mp_qr' && f === 'PIX'" in js, "JS: PIX auto = pix_mp_qr")
    check("mid === 'mp_balcao' && f !== 'PIX'" in js, "JS: cartão auto ≠ PIX")
    check("erroLancamentoPointAutoSemCobranca" in js, "JS: bloqueia fechar sem cobrança")
    check("Nunca «lançar» PIX/cartão Point auto" in js or "precisa cobrar na maquininha" in js, "JS: commit não lança Point sem terminal")
    check("mpPointApiPronta()" in js and "deveCobrarMpPointNaTranche" in js, "JS: cobrar usa API pronta")
    check("mpPointApiPronta()" in js and "deveUsarMpPointNoFechar" in js, "JS: fechar usa API pronta")
    check("maquinaIncompativelComForma" in js and "hasMaquina = false" in js, "JS: limpa máquina incompatível no render")

    check("def mp_point_rejeitar_venda_erp_sem_maquina" in vp, "API: rejeita ERP sem máquina")
    check("mp_point_exige_maquina" in vp, "API: flag mp_point_exige_maquina")
    check("pix_mp_qr" in vp or "MAQUININHAS_MP_POINT_AUTO" in vp, "API: ids auto")
    check("mp_point_rejeitar_venda_erp_sem_maquina" in views, "views: chama rejeição no enviar pedido")

    # Unit: rejeição
    sys.path.insert(0, str(ROOT))
    import os

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    try:
        import django

        django.setup()
        from produtos.views_mp_point import mp_point_rejeitar_venda_erp_sem_maquina

        r1 = mp_point_rejeitar_venda_erp_sem_maquina(
            {"pagamentos": [{"formaPagamento": "PIX", "maquinaId": "pix_mp_qr", "valorPagamento": 10}]}
        )
        check(isinstance(r1, dict) and r1.get("mp_point_exige_maquina"), "unit: PIX pix_mp_qr bloqueia")
        r2 = mp_point_rejeitar_venda_erp_sem_maquina(
            {"pagamentos": [{"formaPagamento": "PIX", "maquinaId": "pix_cielo", "valorPagamento": 10}]}
        )
        check(r2 is None, "unit: PIX Cielo libera")
        r3 = mp_point_rejeitar_venda_erp_sem_maquina(
            {"pagamentos": [{"formaPagamento": "PIX", "maquinaId": "pix_mp_renan", "valorPagamento": 10}]}
        )
        check(r3 is None, "unit: PIX Renan (manual) libera")
        r4 = mp_point_rejeitar_venda_erp_sem_maquina(
            {"pagamentos": [{"formaPagamento": "PIX", "maquinaId": "pix_mp_vila", "mpBalcaoModo": "point"}]}
        )
        check(isinstance(r4, dict), "unit: PIX Vila auto bloqueia")
    except Exception as e:
        check(False, f"unit django: {e}")

    total = ok + fail
    print(f"\nVERIFY_OK {ok}/{total}" if fail == 0 else f"\nVERIFY_FAIL {ok}/{total}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
