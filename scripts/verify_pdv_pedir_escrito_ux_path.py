"""
Path PDV-PEDIR-ESCRITO-UX — escrito embaixo + envio só com texto.

  python scripts/verify_pdv_pedir_escrito_ux_path.py
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    print("VERIFY PDV-PEDIR-ESCRITO-UX PATH")
    js = _read("produtos/static/produtos/js/pdv_pedir_loja.js")
    html = _read("produtos/templates/produtos/partials/pdv/pedir_loja_overlay.html")
    util = _read("produtos/pdv_transf_loja_util.py")
    tests = _read("produtos/tests_pdv_transf_loja.py")

    # Layout: faixa de baixo na coluna busca (não no canto direito)
    view = html.split('class="pl-view-pedir', 1)[-1] if 'class="pl-view-pedir' in html else html
    check("layout_escrito_bar", "pl-escrito-bar" in html)
    check(
        "layout_escrito_na_busca",
        view.find("pl-col--busca") < view.find("pl-escrito-bar") < view.find("pl-col--pedido"),
    )
    check("layout_obs_na_barra", "pdv-pedir-loja-obs" in html and view.find("pl-escrito-bar") < view.find("pdv-pedir-loja-obs"))
    check("layout_direita_so_lista", "pl-col--pedido" in view and "Lista do pedido" in view)
    ped_side = view.split("pl-col--pedido", 1)[-1][:1200]
    check("layout_sem_escrito_na_direita", "pl-escrito-bar" not in ped_side and "pdv-pedir-loja-livre" not in ped_side)

    # UX enviar sem produto
    check("js_garantir_antes", "garantirItensAntesDeEnviar" in js)
    check("js_auto_livre", "if (texto) addCartLivre(true)" in js)
    check("js_fallback_obs", "dom.obs" in js and "addCartLivre(true)" in js)
    check("js_msg_sem_produto", "Escreva o pedido embaixo" in js)
    check("js_sem_obrigar_produto", "Inclua ao menos um produto." not in js)

    # Backend livre-only
    check("util_prefixo", "PREFIXO_ITEM_LIVRE" in util and 'livre:' in util)
    check("util_eh_livre", "def eh_item_livre" in util)
    check("util_msg_vazia", "Escreva um pedido ou inclua um produto" in util)
    check("util_pula_estoque", "if eh_item_livre(it.produto_externo_id):" in util)
    check("tests_escrito", "test_normalizar_pedido_escrito" in tests)

    # Runtime
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    sys.path.insert(0, str(ROOT))
    import django

    django.setup()

    from produtos.pdv_transf_loja_util import (
        PREFIXO_ITEM_LIVRE,
        _normalizar_itens,
        criar_solicitacao,
        eh_item_livre,
        serializar_item,
    )
    from estoque.models import SolicitacaoTransferenciaPdv

    itens, err = _normalizar_itens([{"livre": True, "nome": "sacola", "quantidade": 1}])
    check("rt_normalizar_ok", not err and len(itens) == 1, err)
    check("rt_id_livre", itens and eh_item_livre(itens[0]["produto_externo_id"]))
    check("rt_prefixo", itens and itens[0]["produto_externo_id"].startswith(PREFIXO_ITEM_LIVRE))

    itens2, err2 = _normalizar_itens([])
    check("rt_vazio_msg", "Escreva" in err2 and not itens2)

    sol = None
    try:
        sol, err_c = criar_solicitacao(
            loja_destino="centro",
            itens_raw=[{"livre": True, "nome": "café da manhã path", "quantidade": 1}],
            observacao="verify escrito ux",
            operador_label="VerifyEscrito",
            usuario=None,
        )
        check("rt_criar_so_escrito", sol is not None and not err_c, err_c or "")
        if sol:
            it = sol.itens.first()
            check(
                "rt_db_livre",
                it is not None and eh_item_livre(it.produto_externo_id) and it.quantidade_pedida == Decimal("1.000"),
            )
            ser = serializar_item(it)
            check("rt_ser_livre", ser.get("livre") is True and ser.get("nome") == "café da manhã path")
    except Exception as e:
        check("rt_criar_so_escrito", False, str(e)[:120])
    finally:
        if sol is not None:
            try:
                SolicitacaoTransferenciaPdv.objects.filter(pk=sol.pk).delete()
            except Exception:
                pass

    print()
    print(f"VERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhou: " + ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
