"""Contratos estaticos do pacote F8-HIST-VENDAS."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails = 0


def check(name: str, ok: bool) -> None:
    global fails
    print(("  OK  " if ok else "  FAIL ") + name)
    if not ok:
        fails += 1


def _slice_fn(src: str, name: str) -> str:
    needle = f"function {name}("
    start = src.find(needle)
    if start < 0:
        return ""
    nxt = src.find("\n    function ", start + len(needle))
    if nxt < 0:
        return src[start:]
    return src[start:nxt]


def main() -> int:
    print("verify_f8_hist_vendas_path")
    rel = (ROOT / "produtos/static/produtos/js/pdv_relacionamento.js").read_text(encoding="utf-8")
    version = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    hist = _slice_fn(rel, "renderHistorico")
    resumo = _slice_fn(rel, "renderResumo")

    check("version_readable", bool(version) and version.replace(".", "").isdigit())
    check("renderHistorico_exists", "function renderHistorico(" in rel and bool(hist))
    check("historico_ultimas_vendas", "Últimas vendas" in hist)
    check("historico_no_itens_mais_comprados", "Itens mais comprados" not in rel)
    check("historico_no_itens_mais_comprados_in_fn", "Itens mais comprados" not in hist)
    check("rel_historico_vendas_id", "rel-historico-vendas" in hist)
    check("rel_historico_mais_id", "rel-historico-mais" in hist)
    check("no_histCompraMeta", "histCompraMeta" not in rel)
    check("no_btnCartCol", "btnCartCol" not in rel)
    check("renderResumo_topProdutoListHtml", "topProdutoListHtml" in resumo)
    check("topProdutoListHtml_defined", "function topProdutoListHtml" in rel)

    total = 11
    print(("OK" if fails == 0 else "FAIL") + f" verify_f8_hist_vendas_path — {total - fails}/{total}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())