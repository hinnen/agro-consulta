#!/usr/bin/env python3
"""Cartão do cliente — regras sem banco. A loja não pode usar a migration 0153 do teste."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
oks = 0
fails: list[str] = []


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def main() -> int:
    util = (ROOT / "produtos/cliente_cartao_util.py").read_text(encoding="utf-8")
    trecho = util.split("def digito_ean13")[1].split("def _proximo_seq")[0]
    ns: dict = {"re": re, "PREFIXO": "981"}
    exec(compile("def digito_ean13" + trecho, "cliente_cartao_util.py", "exec"), ns)
    codigo = ns["montar_ean13"](1)
    if len(codigo) == 13 and codigo.startswith("981") and ns["eh_codigo_cartao"](codigo):
        ok(f"código {codigo}")
    else:
        fail("código 981")
    if "def numero_curto" in util and "def _proximo_seq" in util and "return ultimo + 1" in util:
        ok("número curto e a folha seguinte continua do último impresso")
    else:
        fail("número curto ou sequência")
    if not ns["eh_codigo_cartao"]("2300000000017"):
        ok("código de balança não é cartão")
    else:
        fail("balança entrou como cartão")

    pdv = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    if "pdvEhCartaoCliente" in pdv and "slice(0, 3) === '981'" in pdv:
        ok("PDV separa o 981 do produto")
    else:
        fail("PDV não separa o 981")
    tela = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    if "pdv-cartao-bip-overlay" in tela and "Código" in pdv and "saveQuickClientEditOverlay" in pdv:
        ok("busca mostra o código e a troca abre a janela do bip")
    else:
        fail("busca ou janela do bip")

    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    if "caixa/cartao-cliente/" in urls and "cartao/<str:token>/" in urls and "api/pdv/cartao-cliente/" in urls:
        ok("rotas da folha, do celular e do bip")
    else:
        fail("rota faltando")

    mig = (ROOT / "produtos/migrations/0153_cliente_cartao_agro.py").read_text(encoding="utf-8")
    if "0152_whatsapp_pedido_arquivar" in mig and "ClienteCartaoAgro" in mig:
        ok("migration do teste é a 0153 e depende do WhatsApp")
    else:
        fail("migration 0153 inesperada")

    loja = (ROOT / "scripts/loja_0156_cliente_cartao_agro.py").read_text(encoding="utf-8")
    if "0155_pdv_operacao_log_agro" in loja and "ClienteCartaoAgro" in loja:
        ok("na loja a tabela entra como 0156, depois da 0155")
    else:
        fail("falta a migration 0156 da loja")

    arte = ROOT / "produtos/static/produtos/img/cartao_cliente_arte.png"
    if arte.is_file() and arte.stat().st_size > 1000:
        ok("arte da folha")
    else:
        fail("arte da folha")

    if fails:
        print(f"\nVERIFY FAIL {oks}/{oks + len(fails)}")
        for f in fails:
            print(" -", f)
        return 1
    print(f"\nVERIFY_OK {oks}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
