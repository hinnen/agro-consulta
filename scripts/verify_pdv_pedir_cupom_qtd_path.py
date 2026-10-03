"""
Path PDV-PEDIR-CUPOM-QTD — cupom 80mm + qtd editável ao enviar.

  python scripts/verify_pdv_pedir_cupom_qtd_path.py
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
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    print("VERIFY PDV-PEDIR-CUPOM-QTD PATH")
    js = _read("produtos/static/produtos/js/pdv_pedir_loja.js")
    html = _read("produtos/templates/produtos/partials/pdv/pedir_loja_overlay.html")
    util = _read("produtos/pdv_transf_loja_util.py")
    views = _read("produtos/views_pdv_transf_loja.py")
    models = _read("estoque/models.py")
    mig = ROOT / "estoque/migrations/0020_solicitacao_item_quantidade_pedida.py"
    tests = _read("produtos/tests_pdv_transf_loja.py")

    # --- Contrato UI ---
    check("ui_btn_imprimir", "Imprimir cupom" in js and "data-pl-acao=\"imprimir\"" in js)
    check("ui_cupom_80mm", "size:80mm" in js and "SEPARAÇÃO" in js and "PEDIR LOJA #" in js)
    check("ui_cupom_usa_pedida", "quantidade_pedida" in js and "imprimirCupomSeparacao" in js)
    check("ui_qtd_edit_origem", "podeEditarQtd" in js and "aba === 'recebidos'" in js)
    check("ui_qtd_input", 'class="pl-item-qtd"' in js and "data-pl-item-id" in js)
    check("ui_qtd_prefill_pedida", "qtdAtual = edit ? pedida" in js or "edit ? pedida" in js)
    check("ui_transfer_manda_itens", "extra.itens = qtds" in js and "lerQtdsDoCard" in js)
    check("ui_msg_migrate", "rode migrate" in js)
    check("ui_ajuda_cupom", "Imprimir" in html and "quantidade" in html.lower())
    check("ui_css_print_btn", "pl-btn--print" in html)

    # --- Backend ---
    check("model_pedida", "quantidade_pedida" in models)
    check("migrate_0020", mig.is_file() and "quantidade_pedida" in mig.read_text(encoding="utf-8"))
    check("criar_grava_pedida", "quantidade_pedida=it[\"quantidade\"]" in util or "quantidade_pedida=it['quantidade']" in util)
    check("serializar_pedida", '"quantidade_pedida"' in util)
    check("resolver_qtds", "def _resolver_qtds_envio" in util)
    check("concluir_aceita_qtds", "quantidades_envio" in util and "mapa_envio" in util)
    check("concluir_pula_zero", "if q_env <= 0:" in util and "continue" in util)
    check("view_passa_qtds", "quantidades_envio=qtds_envio" in views)
    check("view_itens_ou_quantidades", 'payload.get("itens")' in views)
    check("tests_resolver", "ResolverQtdsEnvioTests" in tests)

    # --- Runtime util (sem DB) ---
    sys.path.insert(0, str(ROOT))
    import django
    import os

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    django.setup()

    from produtos.pdv_transf_loja_util import (
        STATUS_ACEITO,
        _resolver_qtds_envio,
        concluir_transferencia,
        serializar_item,
    )

    it1 = SimpleNamespace(
        pk=11,
        produto_externo_id="P1",
        quantidade=Decimal("5"),
        quantidade_pedida=Decimal("5"),
        nome_produto="Alcool",
        codigo_interno="GM1",
    )
    it2 = SimpleNamespace(
        pk=12,
        produto_externo_id="P2",
        quantidade=Decimal("2"),
        quantidade_pedida=Decimal("2"),
        nome_produto="Sal",
        codigo_interno="GM2",
    )

    mapa, err = _resolver_qtds_envio([it1, it2], [{"id": 11, "quantidade": "3"}, {"id": 12, "quantidade": "0"}])
    check("runtime_parcial", err == "" and mapa[11] == Decimal("3.000") and mapa[12] == Decimal("0.000"))

    mapa0, err0 = _resolver_qtds_envio([it1], [{"id": 11, "quantidade": "0"}])
    check("runtime_tudo_zero", mapa0 == {} and "zero" in err0.lower())

    ser = serializar_item(
        SimpleNamespace(
            pk=1,
            produto_externo_id="X",
            nome_produto="Milho",
            codigo_interno="99",
            quantidade=Decimal("1.5"),
            quantidade_pedida=Decimal("3"),
        )
    )
    check(
        "runtime_serializar",
        ser.get("quantidade_pedida") == 3.0 and ser.get("quantidade") == 1.5,
        str(ser),
    )

    # concluir: 1 item parcial, 1 zerado → só 1 transfer
    sol = SimpleNamespace(
        pk=42,
        status=STATUS_ACEITO,
        loja_origem="vila",
        loja_destino="centro",
        observacao="",
        itens=MagicMock(),
    )
    sol.itens.all.return_value = [it1, it2]
    calls = []

    def fake_transf(*args, **kwargs):
        calls.append((args, kwargs))
        return {"ok": True, "quantidade": float(args[3])}

    req = SimpleNamespace()

    @contextmanager
    def _atomic():
        yield

    with patch("estoque.views._transferir_entre_depositos_exec", side_effect=fake_transf), patch(
        "produtos.pdv_transf_loja_util.transaction.atomic", _atomic
    ), patch("produtos.pdv_transf_loja_util._registrar_evento"), patch(
        "produtos.views._invalidar_caches_apos_ajuste_pin"
    ):
        it1.save = MagicMock()
        it2.save = MagicMock()
        sol.save = MagicMock()
        ok, err_c, res = concluir_transferencia(
            req,
            sol,
            loja_atual="vila",
            operador_label="Teste",
            usuario=None,
            quantidades_envio=[{"id": 11, "quantidade": "3"}, {"id": 12, "quantidade": "0"}],
        )
    check("runtime_concluir_ok", ok and not err_c, err_c or "ok")
    check("runtime_concluir_1_transf", len(calls) == 1, f"calls={len(calls)}")
    if calls:
        check("runtime_concluir_qtd3", Decimal(str(calls[0][0][3])) == Decimal("3"), str(calls[0][0][3]))
    check("runtime_item_pedida_preservada", it1.quantidade_pedida == Decimal("5"))
    check("runtime_item_enviada", it1.quantidade == Decimal("3.000"))

    # DB / ORM: campo + criar pedido grava quantidade_pedida
    from estoque.models import SolicitacaoTransferenciaPdv, SolicitacaoTransferenciaPdvItem
    from produtos.pdv_transf_loja_util import criar_solicitacao

    try:
        SolicitacaoTransferenciaPdvItem._meta.get_field("quantidade_pedida")
        check("orm_campo_pedida", True)
    except Exception as e:
        check("orm_campo_pedida", False, str(e))

    sol_db = None
    try:
        sol_db, err_cr = criar_solicitacao(
            loja_destino="centro",
            itens_raw=[
                {
                    "produto_id": "VERIFY-PEDIR-CUPOM-QTD",
                    "nome": "Alcool teste path",
                    "codigo_interno": "PATH",
                    "quantidade": "4",
                }
            ],
            observacao="verify path auto",
            operador_label="VerifyPath",
            usuario=None,
        )
        check("db_criar_ok", sol_db is not None and not err_cr, err_cr or f"pk={getattr(sol_db, 'pk', None)}")
        if sol_db:
            item = sol_db.itens.first()
            check(
                "db_pedida_igual",
                item is not None
                and item.quantidade == Decimal("4.000")
                and item.quantidade_pedida == Decimal("4.000"),
                f"q={getattr(item, 'quantidade', None)} ped={getattr(item, 'quantidade_pedida', None)}",
            )
            ser2 = serializar_item(item)
            check("db_serializar_lista", ser2.get("quantidade_pedida") == 4.0)
    except Exception as e:
        check("db_criar_ok", False, str(e)[:120])
    finally:
        if sol_db is not None:
            try:
                SolicitacaoTransferenciaPdv.objects.filter(pk=sol_db.pk).delete()
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
