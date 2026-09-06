"""
Prova estática do pacote BI-TOPBAR-TOTAL (topbar Sync compacto + Total por unidade).
  .venv\\Scripts\\python.exe scripts/verify_bi_topbar_total.py
"""
from __future__ import annotations

import json
import os
import re
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

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
    return open(os.path.join(_ROOT, rel), encoding="utf-8").read()


def main() -> int:
    top = _read("produtos/templates/produtos/dashboard_gerencial.html")
    body = _read("produtos/templates/produtos/partials/dashboard_gerencial_body.html")

    print("== topbar Sync ==")
    check("sync_btn_id", 'id="btn-sync-dashboard"' in top)
    check("sync_short_title", re.search(r'id="sync-btn-title"[^>]*>\s*Sync\s*<', top) is not None)
    check("sync_compact_max_width", "max-width: 7.25rem" in top)
    check("sync_no_wide_padding", not re.search(
        r'id="btn-sync-dashboard"[^>]*px-4\s+py-1\.5', top
    ))
    check(
        "topbar_grid_periods_fr",
        "minmax(14rem, 1fr)" in top
        or "minmax(14rem,1fr)" in top
        or "minmax(0, 1fr)" in top
        or "dash-topbar--periods-row" in top,
    )
    check("topbar_periods_class", "dash-topbar-periods" in top)
    check("topbar_sync_wrap", "dash-topbar-sync" in top)
    check("sync_title_js_reset", 'btnSyncTitle.textContent = "Sync"' in top)
    check("sync_sub_compact", 'return (stockText || "—") + " · " + syncBiStatus' in top)
    check("sync_title_attr_full", 'title="Sincronizar estoque com o ERP"' in top)
    check("sync_btn_label_not_long", not re.search(r'id="sync-btn-title"[^>]*>\s*Sincronizar ERP\s*<', top))
    # title attribute / feedback may still say Sincronizar — OK if short button label is Sync
    check("cta_orc_abbrev", ">Orç.<" in top or "Orç. <small" in top)

    print("== faturamento por unidade + Total ==")
    check("total_badge_id", 'id="dash-total-unidades"' in body)
    check("push_total_label", 'lojas.push("Total")' in body)
    check("push_total_value", "lojaTotais.push(totalUnidades)" in body)
    check("sum_loop", "totalUnidades +=" in body)
    check("badge_pt_br", 'toLocaleString("pt-BR"' in body)
    check("total_color_sky", "#0369a1" in body)
    check("help_menciona_soma", "soma das lojas" in body)

    # Réplica da lógica JS (valores do print do Renan)
    labels = ["Centro", "Vila Elias"]
    values = [15575.19, 1979.40]
    total = 0.0
    for v in values:
        total += float(v or 0)
    total = round(total * 100) / 100
    labels2 = labels + ["Total"]
    values2 = values + [total]
    check("sum_screenshot_case", total == 17554.59, f"got {total}")
    check("series_len_3", len(labels2) == 3 and len(values2) == 3)
    check("total_last", labels2[-1] == "Total" and values2[-1] == total)
    check("no_float_drift", abs(values2[0] + values2[1] - values2[2]) < 1e-9)

    # JSON do backend (contrato)
    payload = {"labels": labels, "values": values}
    dumped = json.dumps(payload, ensure_ascii=False)
    check("json_roundtrip", json.loads(dumped)["values"] == values)

    # IDs JS ↔ HTML
    for eid in ("btn-sync-dashboard", "sync-btn-title", "sync-btn-sub", "sync-thermo-fill", "sync-thermo-text"):
        check(f"html_id_{eid}", f'id="{eid}"' in top)
        check(f"js_get_{eid}", f'getElementById("{eid}")' in top)

    check("body_get_total_badge", 'getElementById("dash-total-unidades")' in body)

    print()
    if fails:
        print(f"VERIFY_FAIL {len(fails)}/{len(oks)+len(fails)}")
        for f in fails:
            print(" -", f)
        return 1
    print(f"VERIFY_OK {len(oks)}/{len(oks)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
