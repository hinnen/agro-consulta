#!/usr/bin/env python3
"""Path: chat/venda renovam PIN sem abrir popup «tem pedido» do Pedir loja.

Bug loja v20.22: gm-sspin-operador → refreshResumo({ aposPin: true }) → abrirTemPedido.
Fix: evento global só badge; aposPin só em abrirPin().
"""
from __future__ import annotations

import re
import sys
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


def _chunk_after(hay: str, needle: str, n: int = 400) -> str:
    i = hay.find(needle)
    if i < 0:
        return ""
    return hay[i : i + n]


def main() -> int:
    print("VERIFY PDV-PIN-CHAT-TEMPEDIDO")
    js = _read("produtos/static/produtos/js/pdv_pedir_loja.js")
    chat = _read("produtos/static/produtos/js/pdv_chat_loja.js")
    wiz = _read("produtos/static/produtos/js/pdv_wizard.js")
    sspin = _read("produtos/templates/produtos/_screensaver_pin.html")
    pin_acao = _read("scripts/verify_pdv_pin_na_acao.py")
    pedir_v = _read("scripts/verify_pdv_pedir_loja.py")

    check("js_abrir_tem_pedido", "function abrirTemPedido" in js)
    check("js_apos_pin_gate", "opts.aposPin && !d.precisa_pin && n > 0" in js)
    check("js_abrir_pin_apos", "function abrirPin" in js and "refreshResumo({ aposPin: true })" in js)

    sspin_chunk = _chunk_after(js, "gm-sspin-operador", 320)
    check("js_listener_sspin", "gm-sspin-operador" in js)
    check(
        "js_listener_sem_apos_pin",
        "aposPin" not in sspin_chunk,
        "listener global sem aposPin",
    )
    check(
        "js_listener_so_refresh",
        "refreshResumo()" in sspin_chunk and "refreshResumo({ aposPin" not in sspin_chunk,
    )
    check(
        "js_comentario_chat_venda",
        "Chat" in sspin_chunk or "chat" in _chunk_after(js, "Só badge", 200).lower(),
    )

    # Contagem: aposPin só nos lugares certos (gate + abrirPin), não no listener.
    apos_hits = [m.start() for m in re.finditer(r"aposPin", js)]
    check("js_apos_pin_count_ok", len(apos_hits) == 2, f"hits={len(apos_hits)}")

    check("chat_garantir_operador", "gmSspinGarantirOperador" in chat and "PIN para o chat" in chat)
    check("wiz_confirm_garantir", "gmSspinGarantirOperador" in wiz and "PIN para confirmar a venda" in wiz)
    check("sspin_dispatch_operador", "gm-sspin-operador" in sspin and "function setOperador" in sspin)
    check(
        "sspin_renovar_set_operador",
        "renovar: true" in sspin and "setOperador" in _chunk_after(sspin, "renovar: true", 500),
    )

    check("prova_pedir_loja_gate", "js_sspin_sem_aviso_tem_pedido" in pedir_v)
    check("prova_pin_na_acao_existe", "verify_pdv_pin_na_acao" in pin_acao or pin_acao.startswith('"""'))
    check("path_script_self", Path(__file__).is_file())

    ver = _read("VERSION").strip()
    check("version_bump", ver >= "20.28", ver)

    print()
    print(f"VERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhou: " + ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
