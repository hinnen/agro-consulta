"""
Path PDV-PEDIR-PARCIAL-RESTO — Transferir parcial: encerrar ou deixar resto.

  python scripts/verify_pdv_pedir_parcial_resto_path.py
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


def _mk_item(pk: int, pid: str, qtd: str, nome: str = "Prod") -> SimpleNamespace:
    return SimpleNamespace(
        pk=pk,
        produto_externo_id=pid,
        quantidade=Decimal(qtd),
        quantidade_pedida=Decimal(qtd),
        nome_produto=nome,
        codigo_interno=f"GM{pk}",
        solicitacao=None,
        save=MagicMock(),
    )


def _mk_sol(pk: int, itens: list) -> SimpleNamespace:
    sol = SimpleNamespace(
        pk=pk,
        status="pronto",
        loja_origem="vila",
        loja_destino="centro",
        observacao="cliente",
        criado_por_label="Op",
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
    sol.itens.all.return_value = itens
    return sol


@contextmanager
def _atomic():
    yield


def test_contratos() -> None:
    js = _read("produtos/static/produtos/js/pdv_pedir_loja.js")
    html = _read("produtos/templates/produtos/partials/pdv/pedir_loja_overlay.html")
    util = _read("produtos/pdv_transf_loja_util.py")
    views = _read("produtos/views_pdv_transf_loja.py")

    check("ui_btn_resto", "pdv-pedir-loja-confirm-resto" in html)
    check("ui_label_encerrar", "Transferir e encerrar" in js)
    check("ui_label_resto", "Transferir e deixar resto" in js or "deixar resto" in html.lower())
    check("ui_duas_opcoes", "duasOpcoesResto" in js and "selecaoTemResto" in js)
    check("ui_payload_flag", "deixar_resto:" in js or "deixar_resto =" in js)
    check("ui_transf_sel_flag", "deixarResto" in js and "transferirSelecionadosTodos" in js)
    check("ui_texto_corpo", "Se deixar resto" in js or "Encerrar =" in js)
    check("ui_fechar_choice", "fecharConfirm(true, 'resto')" in js and "fecharConfirm(true, 'encerrar')" in js)
    check("ui_abrir_choice", "choice === 'resto'" in js)
    check("css_resto", "pl-confirm-btns--resto" in html)
    check("util_flag", "deixar_resto" in util and "splits_resto" in util)
    check("util_encerrar_obs", "Encerrado sem resto" in util)
    check("util_split_create", "Parcial resto" in util)
    check("util_compat_adiar", "deixar_resto = bool(adiar_ids)" in util or "deixar_resto=None" in util)
    check("view_passa_flag", "deixar_resto=" in views and 'payload.get("deixar_resto")' in views)
    check("view_msg_resto", "ficaram na fila" in views and "resto_solicitacao_id" in views)
    check("smoke_script", (ROOT / "scripts/smoke_pdv_pedir_parcial_resto_local.py").is_file())


def test_runtime() -> None:
    from produtos.pdv_transf_loja_util import STATUS_ACEITO, STATUS_PRONTO, concluir_transferencia

    it_a = _mk_item(41, "PA", "8", "Racao")
    sol_a = _mk_sol(201, [it_a])
    created_a: dict = {}
    items_created: list = []

    def fake_create_sol(**kwargs):
        ns = SimpleNamespace(pk=801, **kwargs)
        created_a["sol"] = ns
        return ns

    def fake_create_item(**kwargs):
        items_created.append(kwargs)
        return SimpleNamespace(pk=9001, **kwargs)

    calls_a = []

    def fake_transf_a(*args, **kwargs):
        calls_a.append(args[3])
        return {"ok": True, "quantidade": float(args[3])}

    with patch("estoque.views._transferir_entre_depositos_exec", side_effect=fake_transf_a), patch(
        "produtos.pdv_transf_loja_util.transaction.atomic", _atomic
    ), patch("produtos.pdv_transf_loja_util._registrar_evento"), patch(
        "produtos.views._invalidar_caches_apos_ajuste_pin"
    ), patch(
        "produtos.pdv_transf_loja_util.SolicitacaoTransferenciaPdv.objects.create",
        side_effect=fake_create_sol,
    ), patch(
        "produtos.pdv_transf_loja_util.SolicitacaoTransferenciaPdvItem.objects.create",
        side_effect=fake_create_item,
    ):
        ok_a, err_a, res_a = concluir_transferencia(
            SimpleNamespace(),
            sol_a,
            loja_atual="vila",
            operador_label="Teste",
            usuario=None,
            quantidades_envio=[{"id": 41, "quantidade": "4"}],
            deixar_resto=True,
        )

    check("runtime_resto_ok", ok_a and not err_a, err_a or "ok")
    check(
        "runtime_resto_enviou_4",
        len(calls_a) == 1 and Decimal(str(calls_a[0])) == Decimal("4"),
        str(calls_a),
    )
    check("runtime_resto_criou_sol", bool(created_a.get("sol")), "sol resto")
    if created_a.get("sol"):
        check(
            "runtime_resto_status",
            created_a["sol"].status in (STATUS_ACEITO, STATUS_PRONTO),
            str(created_a["sol"].status),
        )
    check(
        "runtime_resto_item_qtd4",
        any(
            Decimal(str(x.get("quantidade", 0))) == Decimal("4")
            and x.get("produto_externo_id") == "PA"
            for x in items_created
        ),
        str(items_created),
    )
    check(
        "runtime_resto_id_res",
        any(isinstance(r, dict) and r.get("resto_solicitacao_id") == 801 for r in res_a),
        str(res_a),
    )
    check(
        "runtime_resto_origem_enviada",
        it_a.quantidade == Decimal("4.000") or it_a.quantidade == Decimal("4"),
        str(it_a.quantidade),
    )

    it_b = _mk_item(42, "PB", "8", "Milho")
    sol_b = _mk_sol(202, [it_b])
    created_b: dict = {}
    items_b: list = []
    calls_b = []

    def fake_create_sol_b(**kwargs):
        created_b["sol"] = SimpleNamespace(pk=802, **kwargs)
        return created_b["sol"]

    def fake_create_item_b(**kwargs):
        items_b.append(kwargs)
        return SimpleNamespace(pk=9002, **kwargs)

    def fake_transf_b(*args, **kwargs):
        calls_b.append(args[3])
        return {"ok": True, "quantidade": float(args[3])}

    with patch("estoque.views._transferir_entre_depositos_exec", side_effect=fake_transf_b), patch(
        "produtos.pdv_transf_loja_util.transaction.atomic", _atomic
    ), patch("produtos.pdv_transf_loja_util._registrar_evento"), patch(
        "produtos.views._invalidar_caches_apos_ajuste_pin"
    ), patch(
        "produtos.pdv_transf_loja_util.SolicitacaoTransferenciaPdv.objects.create",
        side_effect=fake_create_sol_b,
    ), patch(
        "produtos.pdv_transf_loja_util.SolicitacaoTransferenciaPdvItem.objects.create",
        side_effect=fake_create_item_b,
    ):
        ok_b, err_b, res_b = concluir_transferencia(
            SimpleNamespace(),
            sol_b,
            loja_atual="vila",
            operador_label="Teste",
            usuario=None,
            quantidades_envio=[{"id": 42, "quantidade": "4"}],
            deixar_resto=False,
        )

    check("runtime_encerrar_ok", ok_b and not err_b, err_b or "ok")
    check(
        "runtime_encerrar_enviou_4",
        len(calls_b) == 1 and Decimal(str(calls_b[0])) == Decimal("4"),
        str(calls_b),
    )
    check("runtime_encerrar_sem_sol", "sol" not in created_b, str(created_b))
    check("runtime_encerrar_sem_item", len(items_b) == 0, str(items_b))
    check(
        "runtime_encerrar_sem_resto_id",
        not any(isinstance(r, dict) and r.get("resto_solicitacao_id") for r in res_b),
        str(res_b),
    )

    it_c1 = _mk_item(51, "PC1", "3", "A")
    it_c2 = _mk_item(52, "PC2", "5", "B")
    sol_c = _mk_sol(203, [it_c1, it_c2])
    created_c: dict = {}

    with patch(
        "estoque.views._transferir_entre_depositos_exec",
        return_value={"ok": True, "quantidade": 3},
    ), patch("produtos.pdv_transf_loja_util.transaction.atomic", _atomic), patch(
        "produtos.pdv_transf_loja_util._registrar_evento"
    ), patch("produtos.views._invalidar_caches_apos_ajuste_pin"), patch(
        "produtos.pdv_transf_loja_util.SolicitacaoTransferenciaPdv.objects.create",
        side_effect=lambda **kw: created_c.setdefault("sol", SimpleNamespace(pk=803, **kw)),
    ):
        ok_c, err_c, _ = concluir_transferencia(
            SimpleNamespace(),
            sol_c,
            loja_atual="vila",
            operador_label="Teste",
            usuario=None,
            quantidades_envio=[{"id": 51, "quantidade": "3"}],
            adiar_item_ids=[52],
            deixar_resto=False,
        )

    check("runtime_adiar_encerrar_ok", ok_c and not err_c, err_c or "ok")
    check("runtime_adiar_encerrar_sem_resto", "sol" not in created_c)
    check(
        "runtime_adiar_encerrar_zero",
        it_c2.quantidade == Decimal("0") or it_c2.quantidade == Decimal("0.000"),
        str(it_c2.quantidade),
    )

    it_d1 = _mk_item(61, "PD1", "2", "C")
    it_d2 = _mk_item(62, "PD2", "7", "D")
    sol_d = _mk_sol(204, [it_d1, it_d2])
    created_d: dict = {}

    def fake_create_sol_d(**kwargs):
        created_d["sol"] = SimpleNamespace(pk=804, **kwargs)
        return created_d["sol"]

    with patch(
        "estoque.views._transferir_entre_depositos_exec",
        return_value={"ok": True, "quantidade": 2},
    ), patch("produtos.pdv_transf_loja_util.transaction.atomic", _atomic), patch(
        "produtos.pdv_transf_loja_util._registrar_evento"
    ), patch("produtos.views._invalidar_caches_apos_ajuste_pin"), patch(
        "produtos.pdv_transf_loja_util.SolicitacaoTransferenciaPdv.objects.create",
        side_effect=fake_create_sol_d,
    ):
        ok_d, err_d, _ = concluir_transferencia(
            SimpleNamespace(),
            sol_d,
            loja_atual="vila",
            operador_label="Teste",
            usuario=None,
            quantidades_envio=[{"id": 61, "quantidade": "2"}],
            adiar_item_ids=[62],
            deixar_resto=True,
        )

    check("runtime_adiar_resto_ok", ok_d and not err_d, err_d or "ok")
    check("runtime_adiar_resto_criou", bool(created_d.get("sol")))
    check(
        "runtime_adiar_resto_moveu",
        it_d2.solicitacao is created_d.get("sol"),
        str(getattr(it_d2, "solicitacao", None)),
    )

    it_e = _mk_item(71, "PE", "5", "Igual")
    sol_e = _mk_sol(205, [it_e])
    created_e: dict = {}
    with patch(
        "estoque.views._transferir_entre_depositos_exec",
        return_value={"ok": True, "quantidade": 5},
    ), patch("produtos.pdv_transf_loja_util.transaction.atomic", _atomic), patch(
        "produtos.pdv_transf_loja_util._registrar_evento"
    ), patch("produtos.views._invalidar_caches_apos_ajuste_pin"), patch(
        "produtos.pdv_transf_loja_util.SolicitacaoTransferenciaPdv.objects.create",
        side_effect=lambda **kw: created_e.setdefault("sol", SimpleNamespace(pk=805, **kw)),
    ):
        ok_e, err_e, res_e = concluir_transferencia(
            SimpleNamespace(),
            sol_e,
            loja_atual="vila",
            operador_label="Teste",
            usuario=None,
            quantidades_envio=[{"id": 71, "quantidade": "5"}],
            deixar_resto=True,
        )
    check("runtime_igual_ok", ok_e and not err_e, err_e or "ok")
    check("runtime_igual_sem_resto", "sol" not in created_e)
    check(
        "runtime_igual_sem_id",
        not any(isinstance(r, dict) and r.get("resto_solicitacao_id") for r in res_e),
        str(res_e),
    )


def main() -> int:
    print("VERIFY PDV-PEDIR-PARCIAL-RESTO PATH")
    print(f"PIN={'set' if (os.environ.get('AGRO_PIN_TESTE') or '').strip() else '— (smoke separado)'}")
    test_contratos()
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    try:
        test_runtime()
    except Exception as exc:
        check("runtime_setup", False, str(exc))
        print(f"ERRO runtime: {exc}")

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
