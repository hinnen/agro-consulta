"""
Path PDV-PEDIR-PARCIAL-PRINT ÔÇö imprimir todos + checkbox parcial + qtd 0 no hist├│rico.

  python scripts/verify_pdv_pedir_parcial_print_path.py
"""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" ÔÇö {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" ÔÇö {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    print("VERIFY PDV-PEDIR-PARCIAL-PRINT PATH")
    js = _read("produtos/static/produtos/js/pdv_pedir_loja.js")
    html = _read("produtos/templates/produtos/partials/pdv/pedir_loja_overlay.html")
    util = _read("produtos/pdv_transf_loja_util.py")
    views = _read("produtos/views_pdv_transf_loja.py")

    check("ui_imprimir_todos_btn", "pdv-pedir-loja-imprimir-todos" in html and "Imprimir todos" in html)
    check(
        "ui_imprimir_todos_fn",
        "imprimirTodosPedidos" in js and "montarHtmlCupomPedidos" in js and "abrirEscolhaImpressao" in js,
    )
    check("ui_sem_resumo_duplicado", "row.resumo" not in js or "escapeHtml(row.resumo" not in js)
    check("ui_checkbox_item", "pl-item-check" in js and "adiar_itens" in js)
    check("ui_historico_zero", "NÃO ENVIADO" in js)
    check("ui_toolbar", "pl-lista-toolbar" in html and "syncListaToolbar" in js)
    check("ui_ajuda_parcial", "fica na fila" in html.lower() or "fica pra depois" in html.lower())
    check("ui_aceitar_todos", "pdv-pedir-loja-aceitar-todos" in html and "Aceitar todos" in js)
    check("ui_transferir_sel", "pdv-pedir-loja-transferir-sel" in html and "transferirSelecionadosTodos" in js)
    check("ui_transf_aceito_ou_pronto", "STATUS_ACEITO, STATUS_PRONTO" in util and "(st === 'aceito' || st === 'pronto')" in js)
    check("ui_transf_sel_aceito", 'data-pl-st="aceito"], .pl-card[data-pl-st="pronto"]' in js)
    check("ui_ajuda_transf_direto", "transferir direto" in html.lower() or "Pronto opcional" in html or "opcional" in html.lower())
    check("ui_ajuda_pronto", "Pronto" in html and "30 min" in html)
    check("js_bip_alerta_por_bip", "applyBadge(n, bip)" in js and "bip pausado 30 min" in js)
    check("js_poll_12s", "12000" in js and "visibilitychange" in js)
    check("ui_marcar_todos", 'data-pl-sel="todos"' in js and "marcarChecksDoCard" in js)
    check("ui_secoes_status", "pl-sec" in js and "eh_resto" in js)
    check(
        "ui_confirm_lista",
        "Vai agora:" in js
        and ("Ficam na fila" in js or "fica na fila" in js.lower() or "Se deixar resto" in js),
    )
    check("ui_qtd_diff", "data-pl-pedida" in js and "is-diff" in js)
    check("css_status_card", 'data-pl-st="pronto"' in html or "pl-card[data-pl-st" in html)
    check("ser_eh_resto", '"eh_resto"' in util and "parcialmente_enviado" in util)
    check("view_ordena_status", "_st_ord" in views and 'When(status="pendente"' in views)

    check("util_adiar_ids", "def _parse_adiar_ids" in util and "adiar_item_ids" in util)
    check("util_resto_sol", 'acao="resto"' in util or "acao='resto'" in util)
    check("util_resto_create", "Restante do pedido" in util)
    check("view_passa_adiar", "adiar_itens" in views and "adiar_item_ids=" in views)
    check("view_msg_fila", "ficaram na fila" in views)

    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()

    from produtos.pdv_transf_loja_util import (
        STATUS_ACEITO,
        STATUS_PRONTO,
        _parse_adiar_ids,
        concluir_transferencia,
    )

    check("runtime_parse_adiar", _parse_adiar_ids(["10", 11, "x"]) == {10, 11})

    it1 = SimpleNamespace(
        pk=21,
        produto_externo_id="P1",
        quantidade=Decimal("8"),
        quantidade_pedida=Decimal("8"),
        nome_produto="Racao cao",
        codigo_interno="GM1",
        solicitacao=None,
        save=MagicMock(),
    )
    it2 = SimpleNamespace(
        pk=22,
        produto_externo_id="P2",
        quantidade=Decimal("5"),
        quantidade_pedida=Decimal("5"),
        nome_produto="Racao gato",
        codigo_interno="GM2",
        solicitacao=None,
        save=MagicMock(),
    )

    sol = SimpleNamespace(
        pk=99,
        status=STATUS_PRONTO,
        loja_origem="vila",
        loja_destino="centro",
        observacao="cliente",
        criado_por_label="Gabriel",
        criado_por=None,
        aceito_em=None,
        aceito_por_label="Op",
        aceito_por=None,
        pronto_em=None,
        pronto_por_label="",
        pronto_por=None,
        itens=MagicMock(),
        save=MagicMock(),
    )
    sol.itens.all.return_value = [it1, it2]

    created = {}

    def fake_create(**kwargs):
        ns = SimpleNamespace(pk=777, **kwargs)
        created["sol"] = ns
        return ns

    calls = []

    def fake_transf(*args, **kwargs):
        calls.append((args, kwargs))
        return {"ok": True, "quantidade": float(args[3])}

    @contextmanager
    def _atomic():
        yield

    with patch("estoque.views._transferir_entre_depositos_exec", side_effect=fake_transf), patch(
        "produtos.pdv_transf_loja_util.transaction.atomic", _atomic
    ), patch("produtos.pdv_transf_loja_util._registrar_evento"), patch(
        "produtos.views._invalidar_caches_apos_ajuste_pin"
    ), patch(
        "produtos.pdv_transf_loja_util.SolicitacaoTransferenciaPdv.objects.create",
        side_effect=fake_create,
    ):
        ok, err, res = concluir_transferencia(
            SimpleNamespace(),
            sol,
            loja_atual="vila",
            operador_label="Teste",
            usuario=None,
            quantidades_envio=[{"id": 21, "quantidade": "8"}],
            adiar_item_ids=[22],
        )

    check("runtime_parcial_ok", ok and not err, err or "ok")
    check("runtime_parcial_1_transf", len(calls) == 1, f"calls={len(calls)}")
    check("runtime_parcial_resto", bool(created.get("sol")), "resto criado")
    if created.get("sol"):
        check(
            "runtime_resto_status",
            created["sol"].status in (STATUS_ACEITO, STATUS_PRONTO),
            str(created["sol"].status),
        )
    check("runtime_item2_moveu", it2.solicitacao is created.get("sol"), str(it2.solicitacao))
    check(
        "runtime_res_resto_id",
        any(isinstance(r, dict) and r.get("resto_solicitacao_id") == 777 for r in res),
        str(res),
    )

    # qtd 0 no item marcado (sem adiar) ainda grava enviada 0
    it3 = SimpleNamespace(
        pk=31,
        produto_externo_id="P3",
        quantidade=Decimal("4"),
        quantidade_pedida=Decimal("4"),
        nome_produto="Milho",
        codigo_interno="GM3",
        save=MagicMock(),
    )
    it4 = SimpleNamespace(
        pk=32,
        produto_externo_id="P4",
        quantidade=Decimal("2"),
        quantidade_pedida=Decimal("2"),
        nome_produto="Quirera",
        codigo_interno="GM4",
        save=MagicMock(),
    )
    sol2 = SimpleNamespace(
        pk=100,
        status=STATUS_PRONTO,
        loja_origem="vila",
        loja_destino="centro",
        observacao="",
        criado_por_label="",
        criado_por=None,
        aceito_em=None,
        aceito_por_label="",
        aceito_por=None,
        pronto_em=None,
        pronto_por_label="",
        pronto_por=None,
        itens=MagicMock(),
        save=MagicMock(),
    )
    sol2.itens.all.return_value = [it3, it4]
    calls2 = []

    def fake_transf2(*args, **kwargs):
        calls2.append(args[3])
        return {"ok": True, "quantidade": float(args[3])}

    with patch("estoque.views._transferir_entre_depositos_exec", side_effect=fake_transf2), patch(
        "produtos.pdv_transf_loja_util.transaction.atomic", _atomic
    ), patch("produtos.pdv_transf_loja_util._registrar_evento"), patch(
        "produtos.views._invalidar_caches_apos_ajuste_pin"
    ):
        ok2, err2, _ = concluir_transferencia(
            SimpleNamespace(),
            sol2,
            loja_atual="vila",
            operador_label="Teste",
            usuario=None,
            quantidades_envio=[
                {"id": 31, "quantidade": "4"},
                {"id": 32, "quantidade": "0"},
            ],
        )
    check("runtime_zero_ok", ok2 and not err2, err2 or "ok")
    check("runtime_zero_1_transf", len(calls2) == 1 and Decimal(str(calls2[0])) == Decimal("4"))
    check("runtime_zero_pedida", it4.quantidade_pedida == Decimal("2") and it4.quantidade == Decimal("0.000"))

    print()
    print(f"RESULTADO: {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("FALHAS:")
        for f in fails:
            print(f"  - {f}")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
