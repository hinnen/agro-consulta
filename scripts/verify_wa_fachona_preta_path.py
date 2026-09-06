#!/usr/bin/env python
"""Prova path — some a faixa preta do Chrome (fora do escopo) no Zap web."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ok = 0
fail = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fail += 1
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


def main() -> int:
    topbar = (ROOT / "produtos/static/produtos/js/pdv_topbar_whatsapp.js").read_text(encoding="utf-8")
    dual = (ROOT / "produtos/static/produtos/js/agro_dual_window.js").read_text(encoding="utf-8")
    shell = (ROOT / "produtos/templates/produtos/_agro_open_external.html").read_text(encoding="utf-8")

    print("WA-FACHONA-PRETA path checks")
    check("topbar_sem_location_href_direto", "window.location.href = '/atendimento-whatsapp/'" not in topbar)
    check("topbar_abrirZapSemSairDoApp", "abrirZapSemSairDoApp" in topbar)
    check("topbar_navigateGestao", "navigateGestao" in topbar)
    check("dual_healPdvAppOutOfScope", "healPdvAppOutOfScope" in dual)
    check("dual_pulseGestaoFocus_heal", "pulseGestaoFocus(here)" in dual)
    check("dual_replace_pdv", "agro_app_role=pdv" in dual and "healPdvAppOutOfScope" in dual)
    check("shell_resume_key", "agro_resume_inapp_tab_v1" in shell)
    check("shell_resume_fn", "resumeInAppTabAfterHome" in shell)
    check("shell_replace_home", "RESUME_INAPP_TAB_KEY" in shell and "location.replace(homeInScope)" in shell)

    print(f"\nVERIFY_{'OK' if fail == 0 else 'FAIL'} {ok}/{ok + fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
