# -*- coding: utf-8 -*-
"""Prova — Limite fiado na linha (`FIADO-LIMITE-LINHA`)."""
from __future__ import annotations

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


def main() -> int:
    print("verify_fiado_limite_linha_path")
    html = (ROOT / "produtos/templates/produtos/fiado_gestao.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/fiado_gestao.js").read_text(encoding="utf-8")

    check("sem_btn_limite_cliente", 'id="fiado-btn-limite-avulso"' not in html and "Limite cliente" not in html)
    check("sem_modal_limite", 'id="fiado-modal-limite"' not in html)
    check("sem_form_avulso", 'id="fiado-form-limite-avulso"' not in html)
    check("th_hint", "Clique no valor da linha para editar o limite" in html)
    check("css_valor", ".fiado-limite-valor" in html and ".fiado-limite-cell" in html)
    check("css_input", ".fiado-limite-input" in html)
    check("js_render_botao", "fiado-limite-valor" in js and "data-valor" in js)
    check("js_iniciar", "function iniciarEdicaoLimite" in js)
    check("js_gravar", "function gravarLimiteNaLinha" in js and "salvarLimite" in js)
    check("js_finalizar", "function finalizarEdicaoLimite" in js)
    check("js_enter_esc", "Enter" in js and "Escape" in js and "finalizarEdicaoLimite" in js)
    check("js_stop_row", "fiado-limite-valor" in js and "stopPropagation" in js)
    check("js_sem_modal_avulso", "btnLimiteAvulso" not in js and "buscarClientesLimite" not in js)
    check("api_limite_url", "urls.limite" in js or "api_fiado_limite" in html)
    check("atualiza_cache", "row.limite_fiado_local = valorNum" in js)

    print()
    if fails:
        print(f"FALHOU: {len(fails)} · ok {len(oks)}")
        for f in fails:
            print(" -", f)
        return 1
    print(f"OK verify_fiado_limite_linha_path — {len(oks)}/{len(oks)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
