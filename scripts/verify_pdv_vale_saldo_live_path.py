"""
Prova PDV-VALE-SALDO-LIVE — vale creditado atualiza o contador na hora (bug loja #15).

  python scripts/verify_pdv_vale_saldo_live_path.py
  python scripts/verify_pdv_vale_saldo_live_path.py --live-pin 9973
"""
from __future__ import annotations

import argparse
import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" - {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" - {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Path arquivos ==")
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")
    util = _read("produtos/cliente_operacoes_util.py")
    acoes = _read("produtos/static/produtos/js/cliente_cadastro_acoes.js")
    side = _read("produtos/templates/produtos/partials/pdv/step_produtos.html")

    check("fn_aplicar", "function aplicarSaldoClienteNoPdv" in wizard)
    check("fn_force_bust", "opts.force" in wizard and "&_t=" in wizard)
    check(
        "on_vale_aplica",
        "ev.data.cliente" in wizard
        and "aplicarSaldoClienteNoPdv(ev.data.cliente)" in wizard,
    )
    check(
        "on_vale_force",
        "refreshCreditoFiadoCliente(0, { force: true })" in wizard,
    )
    check(
        "sem_refresh_frouxo",
        "refreshCreditoFiadoCliente(0, {});" not in wizard
        or wizard.count("refreshCreditoFiadoCliente(0, {});") == 0,
    )
    check("patch_usa_aplicar", "aplicarSaldoClienteNoPdv(updated)" in wizard)
    check("linha_min_pk", '"cliente_agro_pk": c.pk' in util)
    check("util_manual", "def creditar_vale_manual" in util)
    check("acoes_vale_manual", "vale_manual" in acoes and "onAposMudanca" in acoes)
    check("side_contador", "pdv-product-credit-balance" in side)
    check("side_btn_vale", "pdv-vale-credito-open" in side)
    check(
        "reset_limpa_credito",
        "creditoFiadoCliente = null" in wizard and "eraCompraVale" in wizard,
    )
    check("saldoVale_prioriza_credito", "creditoFiadoCliente.saldo_vale_credito" in wizard)


def test_django_util(*, live_pin: str | None) -> None:
    print("== Django util ==")
    import django

    django.setup()
    from produtos.cliente_operacoes_util import _linha_min, creditar_vale_manual
    from produtos.models import ClienteAgro

    check("_linha_min_tem_agro_pk", '"cliente_agro_pk": c.pk' in _read("produtos/cliente_operacoes_util.py"))
    cli = ClienteAgro.objects.filter(ativo=True).order_by("pk").first()
    if not cli:
        check("tem_cliente", False, "nenhum ClienteAgro ativo")
        return
    check("tem_cliente", True, f"pk={cli.pk}")

    linha = _linha_min(cli)
    check("linha_tem_pk", linha.get("cliente_agro_pk") == cli.pk)
    check("linha_tem_vale", "saldo_vale_credito" in linha)

    if not live_pin:
        print("  (pulei --live-pin: sem crédito real)")
        return

    print("== Live PIN credit ==")
    antes = Decimal(str(cli.saldo_vale_credito or 0))
    delta = Decimal("0.01")
    res = creditar_vale_manual(
        pk=cli.pk,
        valor=delta,
        motivo="prova PDV-VALE-SALDO-LIVE",
        pin=live_pin,
        origem_tela="pdv",
    )
    check("manual_ok", bool(res.get("ok")), str(res.get("erro") or ""))
    if not res.get("ok"):
        return
    cli.refresh_from_db()
    depois = Decimal(str(cli.saldo_vale_credito or 0))
    check("saldo_subiu", depois == (antes + delta).quantize(Decimal("0.01")), f"{antes} -> {depois}")
    cli_resp = res.get("cliente") or {}
    check(
        "resp_pk",
        int(cli_resp.get("cliente_agro_pk") or cli_resp.get("pk") or 0) == cli.pk,
    )
    check(
        "resp_saldo",
        abs(float(cli_resp.get("saldo_vale_credito") or 0) - float(depois)) < 0.001,
        str(cli_resp.get("saldo_vale_credito")),
    )
    # devolve o centavo (API so credita positivo)
    cli.saldo_vale_credito = antes
    cli.save(update_fields=["saldo_vale_credito", "atualizado_em"])
    cli.refresh_from_db()
    check("estorno_pg", Decimal(str(cli.saldo_vale_credito or 0)) == antes)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live-pin", default="", help="PIN operador para crédito real + estorno")
    args = ap.parse_args()
    test_arquivos()
    test_django_util(live_pin=(args.live_pin or "").strip() or None)
    print(f"\n{len(oks)} OK · {len(fails)} FAIL")
    if fails:
        print("Falhou:", ", ".join(fails))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
