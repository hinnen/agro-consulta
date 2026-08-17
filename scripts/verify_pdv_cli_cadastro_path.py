"""
Prova PDV-CLI-CADASTRO — modal sem scroll, duplicata, exclusão, vale crédito.

  python scripts/verify_pdv_cli_cadastro_path.py
"""
from __future__ import annotations

import os
import sys
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
    urls = _read("produtos/urls.py")
    util = _read("produtos/cliente_operacoes_util.py")
    views_c = _read("produtos/views_cliente_cadastro.py")
    views = _read("produtos/views.py")
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")
    state = _read("produtos/static/produtos/js/pdv_state.js")
    html = _read("produtos/templates/produtos/pdv_wizard.html")
    form = _read("produtos/templates/produtos/cliente_form.html")
    js = _read("produtos/static/produtos/js/cliente_cadastro_acoes.js")
    pdv_views = _read("pdv/views.py")
    models = _read("produtos/models.py")
    cash = _read("produtos/cashback_venda_util.py")

    check("url_limpar", "api_cliente_limpar_whatsapp" in urls)
    check("url_excluir", "api_cliente_excluir" in urls)
    check("url_vale", "api_cliente_vale_credito_manual" in urls)
    check("url_eventos", "api_cliente_eventos" in urls)
    check("util_limpar", "def limpar_whatsapp_duplicado" in util)
    check("util_excluir", "def excluir_cliente" in util)
    check("util_vale_manual", "def creditar_vale_manual" in util)
    check("util_vale_pago", "def aplicar_vale_pago_apos_venda" in util)
    check("util_pin", "validar_pin_operador" in util)
    check("view_limpar", "def api_cliente_limpar_whatsapp" in views_c)
    check("view_excluir", "def api_cliente_excluir" in views_c)
    check("model_evento", "class ClienteAgroEventoAgro" in models)
    check("mig_0092", (ROOT / "produtos/migrations/0092_clienteagro_evento.py").exists())
    check("html_sem_scroll", "overflow: hidden" in html and "pdv-client-edit-body" in html)
    check("html_nao_16x9_fixo", "calc(90vh * 16 / 9)" not in html)
    check("html_excluir_btn", "pdv-quick-client-edit-excluir" in html)
    check("html_vale_btn", "pdv-quick-client-edit-vale" in html)
    check("html_overlay", "cliente_cadastro_acoes.html" in html)
    check("js_acoes", "AgroClienteCadastroAcoes" in js and "showDuplicado" in js)
    check("js_wizard_dup", "whatsapp_duplicado" in wizard or "duplicado" in wizard)
    check("js_wizard_vale", "hydrateFromCompraValeCredito" in wizard)
    check("state_vale", "compraValeCredito" in state and "hydrateFromCompraValeCredito" in state)
    check("boot_urls", "apiClienteExcluirPattern" in pdv_views)
    check("persist_vale", "aplicar_vale_pago_apos_venda" in views)
    check("estoque_skip", "item_id_e_servico_pdv" in views)
    check("cashback_skip", "item_id_e_servico_pdv" in cash)
    check("form_clientes", "cli-form-excluir" in form and "cliente_cadastro_acoes.js" in form)
    check("side_vale", "pdv-vale-credito-open" in _read("produtos/templates/produtos/partials/pdv/step_produtos.html"))


def test_util_django() -> None:
    print("== Django util ==")
    import django

    django.setup()
    from decimal import Decimal

    from produtos.cliente_operacoes_util import (
        PID_VALE_CREDITO,
        item_id_e_servico_pdv,
        payload_e_compra_vale_credito,
        valor_compra_vale_credito,
    )

    check("pid_vale", PID_VALE_CREDITO == "vale-credito")
    check("servico_vale", item_id_e_servico_pdv("vale-credito"))
    check("servico_fiado", item_id_e_servico_pdv("fiado-cobranca"))
    check("servico_sku", not item_id_e_servico_pdv("abc123"))
    check(
        "payload_flag",
        payload_e_compra_vale_credito({"compra_vale_credito": True}, []),
    )
    check(
        "payload_item",
        payload_e_compra_vale_credito({}, [{"id": "vale-credito", "qtd": 1, "preco": 10}]),
    )
    check(
        "valor_vale",
        valor_compra_vale_credito([{"id": "vale-credito", "qtd": 1, "preco": "25,50"}]) == Decimal("25.50"),
    )


def main() -> int:
    test_arquivos()
    test_util_django()
    print(f"\n{len(oks)} OK · {len(fails)} FAIL")
    if fails:
        print("Falhou:", ", ".join(fails))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
