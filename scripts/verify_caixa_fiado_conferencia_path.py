# -*- coding: utf-8 -*-
"""Contrato: conferência fiado no Fechar caixa grava no Postgres."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails = 0


def check(name: str, ok: bool) -> None:
    global fails
    print(("  OK  " if ok else "  FAIL ") + name)
    if not ok:
        fails += 1


def main() -> int:
    print("verify_caixa_fiado_conferencia_path")
    models = (ROOT / "produtos/models.py").read_text(encoding="utf-8")
    util = (ROOT / "produtos/caixa_util.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    html = (ROOT / "produtos/templates/produtos/caixa_fechar.html").read_text(encoding="utf-8")
    modal = (ROOT / "produtos/templates/produtos/includes/caixa_fiado_wizard_modal.html").read_text(
        encoding="utf-8"
    )
    mig = ROOT / "produtos/migrations/0123_fiado_nota_caixa_conferida.py"
    check("mig_0123", mig.is_file())
    check("venda_field", "fiado_nota_caixa_conferida_em" in models)
    check("listar_skip_conferida", "fiado_nota_caixa_conferida_em__isnull=True" in util)
    check("marcar_fn", "def marcar_fiado_conferencia_caixa" in util)
    check("api_view", "def api_caixa_fiado_conferencia_salvar" in views)
    check("api_url", "api/caixa/fiado-conferencia/" in urls)
    check("js_grava", "gravarConferencia" in html and "idsMarcados" in html)
    check("data_id", "data-fiado-id" in modal)
    total = 8
    print(("OK" if fails == 0 else "FAIL") + f" verify_caixa_fiado_conferencia_path — {total - fails}/{total}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
