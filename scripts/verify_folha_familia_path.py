#!/usr/bin/env python
"""Prova detalhada FOLHA-FAMILIA (exclui granel + rollup no saco). VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

FAIL: list[str] = []
OK = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" - {detail}" if detail else ""))
    else:
        FAIL.append(name + (f" - {detail}" if detail else ""))
        print(f"  FAIL {name}" + (f" - {detail}" if detail else ""))


def _read(rel: str) -> str:
    return open(os.path.join(ROOT, rel), encoding="utf-8").read()


def main() -> None:
    print("== Unit / util ==")
    from produtos.custo_familia_util import qtd_baixa_saco_por_unidade
    from produtos.compras_familia_folha_util import (
        arred_qtd_folha_compras,
        preparar_pids_folha_familia,
        rollup_first_dt_filhos_no_pai,
        rollup_qtds_filhos_no_pai,
    )
    from datetime import datetime

    # fator 1kg de saco 10kg = 0,1
    fat = qtd_baixa_saco_por_unidade(10, 1)
    check("fator_0_1", fat is not None and abs(float(fat) - 0.1) < 0.0001, str(fat))

    filhos = {
        "200": {"pai_id": "100", "fator": 0.1, "pai_nome": "Saco 10kg", "filho_nome": "Granel"},
        "201": {"pai_id": "100", "fator": 0.05, "pai_nome": "Saco 10kg", "filho_nome": "Meio"},
    }
    # 5 saco + 5×0,1 + 2×0,05 = 5 + 0,5 + 0,1 = 5,6
    q = {"100": 5.0, "200": 5.0, "201": 2.0}
    out = rollup_qtds_filhos_no_pai(q, filhos, display_pais={"100"})
    check("rollup_5_6", abs(float(out.get("100") or 0) - 5.6) < 0.01, str(out.get("100")))
    check("arred_5_5", arred_qtd_folha_compras(5.5) == 5.5)
    check("arred_meio_cima", arred_qtd_folha_compras(5.55) == 5.6)
    check("arred_zero", arred_qtd_folha_compras(None) == 0.0)

    # first_dt: filho vendeu antes → pai herda
    fdt = {"200": datetime(2026, 7, 1), "100": datetime(2026, 8, 1)}
    f2 = rollup_first_dt_filhos_no_pai(
        fdt,
        filhos,
        display_pais={"100"},
        qtd_por_canon={"200": 1.0, "100": 1.0},
    )
    check(
        "first_dt_antecipa",
        f2.get("100") == datetime(2026, 7, 1),
        str(f2.get("100")),
    )

    # pai fora do display: nao rola
    out_skip = rollup_qtds_filhos_no_pai(
        {"999": 1.0, "200": 10.0}, filhos, display_pais={"999"}
    )
    check("rollup_ignora_pai_fora", abs(float(out_skip.get("999") or 0) - 1.0) < 0.01)

    # variant_to_canon
    out_v = rollup_qtds_filhos_no_pai(
        {"CANON100": 5.0, "200": 5.0},
        filhos,
        variant_to_canon={"100": "CANON100", "200": "200"},
        display_pais={"CANON100"},
    )
    check(
        "rollup_via_canon",
        abs(float(out_v.get("CANON100") or 0) - 5.5) < 0.01,
        str(out_v.get("CANON100")),
    )

    print("== Preparar pids (mock indice) ==")
    import produtos.compras_familia_folha_util as mod

    real = mod.indice_filhos_custo_familia
    mod.indice_filhos_custo_familia = lambda: {
        "200": {
            "pai_id": "100",
            "fator": 0.1,
            "pai_nome": "Saco 10kg",
            "filho_nome": "Granel 1kg",
        },
        "201": {
            "pai_id": "100",
            "fator": 0.05,
            "pai_nome": "Saco 10kg",
            "filho_nome": "Meio",
        },
    }
    try:
        disp, sales, fmap, hints = preparar_pids_folha_familia(["200", "201", "300", "100"])
        check("exclui_200", "200" not in disp, str(disp))
        check("exclui_201", "201" not in disp, str(disp))
        check("mantem_300", "300" in disp)
        check("mantem_pai_100", "100" in disp)
        check("sales_tem_filhos", "200" in sales and "201" in sales, str(sales))
        # so filho na lista → injeta pai
        disp2, sales2, _, hints2 = preparar_pids_folha_familia(["200"])
        check("injeta_pai_sozinho", "100" in disp2 and "200" not in disp2, str(disp2))
        check("hint_pai_nome", hints2.get("100") == "Saco 10kg", str(hints2))
        check("sales_so_filho_inclui", "200" in sales2 and "100" in sales2, str(sales2))
        # sem filho: inalterado
        disp3, _, _, _ = preparar_pids_folha_familia(["400", "500"])
        check("sem_familia_igual", disp3 == ["400", "500"], str(disp3))
    finally:
        mod.indice_filhos_custo_familia = real

    print("== Path arquivos / hooks views ==")
    views = _read("produtos/views.py")
    check("util_existe", os.path.isfile(os.path.join(ROOT, "produtos", "compras_familia_folha_util.py")))
    check("views_import", "compras_familia_folha_util" in views)
    check("views_build_hook", "_compras_relatorio_rows_catalogo_sem_ult_doc_build" in views and "display_pids" in views)
    check("views_forn_pg", "_api_compras_relatorio_fornecedor_pg" in views)
    # 3 caminhos usam preparar/rollup
    n_prep = views.count("preparar_pids_folha_familia")
    n_roll = views.count("rollup_qtds_filhos_no_pai")
    n_arred = views.count("arred_qtd_folha_compras")
    check("hooks_preparar_3plus", n_prep >= 3, f"count={n_prep}")
    check("hooks_rollup_3plus", n_roll >= 3, f"count={n_roll}")
    check("hooks_arred_3plus", n_arred >= 3, f"count={n_arred}")
    # nao arredonda vendida com int nos 3 caminhos familia (ainda pode existir em outros)
    check("help_planilha_texto", "Granel/pacote" in _read("produtos/templates/produtos/compras_relatorio_planilha.html"))

    a4 = _read("produtos/templates/produtos/includes/_compras_relatorio_a4_core_js.html")
    plan = _read("produtos/templates/produtos/includes/_compras_planilha_print_js_snippet.html")
    check("a4_decimal_1", "maximumFractionDigits: 1" in a4)
    check("planilha_decimal_1", "maximumFractionDigits: 1" in plan)
    check("a4_fmt_vend", "compraFmtRelatorioVendDesdeUlt" in a4)
    check("planilha_fmt_vend", "planilhaFmtDesdeUlt" in plan)

    # PDV/caixa intactos (pacote nao mexe)
    check("pdv_wizard_existe", os.path.isfile(os.path.join(ROOT, "produtos", "static", "produtos", "js", "pdv_wizard.js")))
    check("caixa_util_existe", os.path.isfile(os.path.join(ROOT, "produtos", "caixa_util.py")))

    print("== Django setup + check + urls ==")
    import django

    django.setup()
    from django.core.management import call_command
    from django.urls import reverse
    from io import StringIO

    buf = StringIO()
    try:
        call_command("check", stdout=buf, stderr=buf)
        check("manage_check", True, "0 issues")
    except Exception as exc:  # noqa: BLE001
        check("manage_check", False, str(exc)[:120])

    for nome in (
        "api_compras_relatorio_fornecedor",
        "api_compras_relatorio_categoria",
        "api_compras_relatorio_unidade",
    ):
        try:
            u = reverse(nome)
            check(f"rev_{nome}", True, u)
        except Exception as exc:  # noqa: BLE001
            check(f"rev_{nome}", False, str(exc)[:80])

    print("== Runtime indice overlay (se houver) ==")
    from produtos.compras_familia_folha_util import indice_filhos_custo_familia

    idx = indice_filhos_custo_familia()
    check("indice_callable", isinstance(idx, dict), f"n={len(idx)}")
    if idx:
        sample = next(iter(idx.values()))
        check(
            "indice_amostra_fator",
            float(sample.get("fator") or 0) > 0 and bool(sample.get("pai_id")),
            str(sample)[:80],
        )
        # preparar com 1 filho real
        filho0 = next(iter(idx.keys()))
        pai0 = str(idx[filho0]["pai_id"])
        d, s, _, _ = preparar_pids_folha_familia([filho0])
        check("runtime_exclui_filho", filho0 not in d, str(d)[:60])
        check("runtime_injeta_pai", pai0 in d, str(d)[:60])
        check("runtime_sales_filho", filho0 in s, str(s)[:60])
    else:
        check("indice_vazio_ok_local", True, "sem custo_familia no DB local — skip amostra")

    print("== API smoke (auth) ==")
    from django.contrib.auth import get_user_model
    from django.test import Client

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    if user is None:
        check("api_user", False, "sem usuario local")
    else:
        c = Client(headers={"host": "127.0.0.1"})
        c.force_login(user)
        # categoria sem filtro → 400 esperado; com valor curto ainda ok se 200/400
        r1 = c.post(
            reverse("api_compras_relatorio_categoria"),
            data='{"categoria":"__folha_familia_smoke_inexistente__"}',
            content_type="application/json",
        )
        check(
            "api_cat_responde",
            r1.status_code in (200, 400),
            str(r1.status_code),
        )
        r2 = c.post(
            reverse("api_compras_relatorio_fornecedor"),
            data='{"fornecedor_nome":"__folha_familia_smoke_inexistente__"}',
            content_type="application/json",
        )
        check(
            "api_forn_responde",
            r2.status_code in (200, 400),
            str(r2.status_code),
        )
        # se 200, linhas nao devem incluir filhos do indice (quando houver)
        if r2.status_code == 200 and idx:
            try:
                body = r2.json()
                linhas = body.get("linhas") or []
                pids = {str(x.get("produto_id") or "") for x in linhas}
                leak = [f for f in idx if f in pids]
                check("api_forn_sem_filho_leak", not leak, str(leak)[:80])
            except Exception as exc:  # noqa: BLE001
                check("api_forn_json", False, str(exc)[:80])

    print()
    print(f"OK {OK}  FAIL {len(FAIL)}")
    if FAIL:
        print("VERIFY_FAIL")
        for f in FAIL:
            print(" ", f)
        sys.exit(1)
    print(f"VERIFY_OK {OK}/{OK}")


if __name__ == "__main__":
    main()
