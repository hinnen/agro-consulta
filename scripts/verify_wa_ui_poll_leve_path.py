# -*- coding: utf-8 -*-
"""WA-UI-POLL-LEVE — prova: poll do Zap não engasga o PDV."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def main() -> int:
    js = (ROOT / "produtos/static/produtos/js/atendimento_whatsapp.js").read_text(encoding="utf-8")

    check("function waJanelaAtiva" in js, "waJanelaAtiva")
    check("document.hidden" in js, "respeita document.hidden")
    check("hasFocus" in js, "hasFocus (janela Zap vs PDV)")
    check("visibilitychange" in js, "visibilitychange refresh")
    check("addEventListener('focus'" in js or 'addEventListener("focus"' in js, "focus refresh")
    check("msgsEvery" in js and "estadoEvery" in js and "listaEvery" in js, "intervalos nomeados")
    check("ativa ? 2 : 6" in js, "msgs foco 5s / fundo 15s")
    check("ativa ? 4 : 12" in js, "estado foco 10s / fundo 30s")
    check("ativa ? 4 : 24" in js, "lista foco 10s / fundo 60s")
    check("ativa ? 24 : 48" in js, "status foco 60s / fundo 120s")
    # nao volta o poll agressivo antigo
    check("Mensagens do chat aberto: a cada 2,5s" not in js, "sem poll msgs 2.5s fixo")
    check("Lista/estado: a cada 5s" not in js, "sem lista 5s fixo")

    print("---")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
