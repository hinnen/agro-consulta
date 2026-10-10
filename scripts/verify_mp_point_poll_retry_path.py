#!/usr/bin/env python3
"""Prova path MP-POINT-POLL-RETRY — bug loja #7423 (Point cobrou, PDV parou no 502).

  python scripts/verify_mp_point_poll_retry_path.py

Contratos no JS do poll: 502/rede continua aguardando; só erro fatal / cancel aborta.
VERIFY_OK N/N · VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)

CHECKS = 0


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"OK {msg}")


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def _read(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def main() -> None:
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")
    fn_start = wizard.find("function pollMpPointUntilPaid")
    check(fn_start >= 0, "pollMpPointUntilPaid existe")
    fn_end = wizard.find("\n    function ", fn_start + 10)
    if fn_end < 0:
        fn_end = fn_start + 12000
    fn = wizard[fn_start:fn_end]
    check("function mpPointStatusTransientFailure" in fn, "helper falha transitória")
    check("function fatalMpPollError" in fn, "helper erro fatal")
    check("function schedulePollStep" in fn, "helper agenda próximo poll")
    check("code >= 500" in fn or "code >= 500" in fn.replace(" ", ""), "reconhece HTTP 5xx")
    check("code === 429" in fn, "reconhece 429")
    check("Conexão instável com Mercado Pago" in fn, "aviso UI conexão instável")
    check("return schedulePollStep(n)" in fn, "reagenda poll em falha")
    check("err.mpPointFatal" in fn, "catch não engole erro fatal")
    check("err.mpPointUserAbort" in fn, "catch não engole abort")
    check("err.mpPointUi" in fn, "catch não engole cancel/recusa")
    # Não pode mais matar a espera no primeiro !stRes.ok sem checar transitório
    check("mpPointStatusTransientFailure(stRes)" in fn, "checa transitório antes de abortar")
    check("throw new Error((stRes.data && (stRes.data.erro" not in fn, "sem throw cru no !stRes.ok")
    print(f"VERIFY_OK {CHECKS}/{CHECKS}")


if __name__ == "__main__":
    main()
