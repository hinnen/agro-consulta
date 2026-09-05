"""
Verificacao detalhada: Uso loja donos (PDV-USO-DONOS).
Motivos Uso Geraldinho / Uso Geraldo · cards historico · titulos Uso Centro/Vila · botao verde.
Roda: python scripts/verify_uso_loja_donos_path.py
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client

from produtos.models import UsoLojaRetiradaAgro, UsoLojaRetiradaItemAgro
from produtos.uso_loja_util import (
    MOTIVO_LABEL,
    MOTIVOS_CARD_TOTAL,
    MOTIVOS_VALIDOS,
    confirmar_retirada_uso_loja,
    estornar_retirada_uso_loja,
    totais_uso_loja_por_deposito,
)

PASS = 0
FAIL = 0
PREFIX = "VERIFYUSODONOS"


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print(f"  OK  {msg}")


def bad(msg: str) -> None:
    global FAIL
    FAIL += 1
    print(f"  FAIL {msg}")


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        bad(msg)


def wipe() -> None:
    UsoLojaRetiradaAgro.objects.filter(
        observacao__startswith=PREFIX
    ).delete()
    UsoLojaRetiradaAgro.objects.filter(quem_levou__startswith=PREFIX).delete()


def test_modelo_motivos() -> None:
    print("\n== Modelo Motivo ==")
    vals = {c.value for c in UsoLojaRetiradaAgro.Motivo}
    check("uso_geraldinho" in vals, "Motivo.USO_GERALDINHO existe")
    check("uso_geraldo" in vals, "Motivo.USO_GERALDO existe")
    check(
        UsoLojaRetiradaAgro.Motivo.USO_GERALDINHO.label == "Uso Geraldinho",
        "label Uso Geraldinho",
    )
    check(
        UsoLojaRetiradaAgro.Motivo.USO_GERALDO.label == "Uso Geraldo",
        "label Uso Geraldo",
    )
    check("uso_geraldinho" in MOTIVOS_VALIDOS, "MOTIVOS_VALIDOS inclui geraldinho")
    check("uso_geraldo" in MOTIVOS_VALIDOS, "MOTIVOS_VALIDOS inclui geraldo")
    check(
        MOTIVO_LABEL.get("uso_geraldinho") == "Uso Geraldinho",
        "MOTIVO_LABEL geraldinho",
    )
    check(MOTIVO_LABEL.get("uso_geraldo") == "Uso Geraldo", "MOTIVO_LABEL geraldo")
    check(
        MOTIVOS_CARD_TOTAL
        == ("uso_geraldinho", "uso_geraldo"),
        "MOTIVOS_CARD_TOTAL = geraldinho+geraldo",
    )


def test_template_overlay() -> None:
    print("\n== Template overlay ==")
    html = (
        ROOT / "produtos/templates/produtos/partials/pdv/uso_loja_overlay.html"
    ).read_text(encoding="utf-8")
    check('data-motivo="uso_geraldinho"' in html, "botao motivo Uso Geraldinho")
    check('data-motivo="uso_geraldo"' in html, "botao motivo Uso Geraldo")
    check(">Uso Geraldinho<" in html, "texto Uso Geraldinho no HTML")
    check(">Uso Geraldo<" in html, "texto Uso Geraldo no HTML")
    check(">Uso Centro<" in html, "titulo card Uso Centro")
    check(">Uso Vila Elias<" in html, "titulo card Uso Vila Elias")
    check('id="pdv-uso-loja-tot-geraldinho-custo"' in html, "id tot geraldinho custo")
    check('id="pdv-uso-loja-tot-geraldinho-venda"' in html, "id tot geraldinho venda")
    check('id="pdv-uso-loja-tot-geraldo-custo"' in html, "id tot geraldo custo")
    check('id="pdv-uso-loja-tot-geraldo-venda"' in html, "id tot geraldo venda")
    check('data-motivo="uso_geraldinho"' in html and "ul-hist-total-tag" in html, "card hist geraldinho")
    # Titulos antigos sem "Uso " nao devem sobrar nos cards de deposito
    check(
        '<div class="ul-hist-total-loja">Centro</div>' not in html,
        "nao resta titulo sozinho Centro",
    )
    check(
        '<div class="ul-hist-total-loja">Vila Elias</div>' not in html,
        "nao resta titulo sozinho Vila Elias",
    )


def test_js_totais() -> None:
    print("\n== JS totais ==")
    js = (ROOT / "produtos/static/produtos/js/pdv_uso_loja.js").read_text(
        encoding="utf-8"
    )
    check("totGeraldinhoCusto" in js, "dom totGeraldinhoCusto")
    check("totGeraldoCusto" in js, "dom totGeraldoCusto")
    check("t.uso_geraldinho" in js, "renderHistTotais le uso_geraldinho")
    check("t.uso_geraldo" in js, "renderHistTotais le uso_geraldo")
    check("pdv-uso-loja-tot-geraldinho-custo" in js, "getElementById geraldinho")
    check("pdv-uso-loja-tot-geraldo-venda" in js, "getElementById geraldo venda")


def test_topbar_cor() -> None:
    print("\n== Topbar botao ==")
    wiz = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(
        encoding="utf-8"
    )
    check('id="pdv-topbar-uso-loja-btn"' in wiz, "botao topbar presente")
    check(
        'id="pdv-topbar-uso-loja-btn"' in wiz
        and "pdv-wiz-topbar-btn--emerald" in wiz[
            wiz.find('id="pdv-topbar-uso-loja-btn"') : wiz.find(
                'id="pdv-topbar-uso-loja-btn"'
            )
            + 180
        ],
        "botao Uso loja usa emerald (verde)",
    )
    check(
        "pdv-wiz-topbar-btn--slate" not in wiz[
            wiz.find('id="pdv-topbar-uso-loja-btn"') : wiz.find(
                'id="pdv-topbar-uso-loja-btn"'
            )
            + 180
        ],
        "botao Uso loja nao e mais slate",
    )


def _mk_retirada(motivo: str, dep: str, venda: str = "10.00", custo: str = "4.00"):
    itens = [
        {
            "produto_id": f"{PREFIX}-{motivo}-{dep}",
            "quantidade": 1,
            "nome": f"{PREFIX} item {motivo}",
            "codigo": "GMTEST",
            "preco_custo": custo,
            "preco_venda": venda,
        }
    ]
    ret, err = confirmar_retirada_uso_loja(
        deposito=dep,
        itens=itens,
        quem_levou=f"{PREFIX} quem",
        motivo=motivo,
        operador_label=f"{PREFIX} op",
        observacao=f"{PREFIX} obs {motivo}",
    )
    return ret, err


def test_totais_e_fluxo() -> None:
    print("\n== Totais + gravacao PG ==")
    wipe()
    t0 = totais_uso_loja_por_deposito()
    for k in ("centro", "vila", "uso_geraldinho", "uso_geraldo"):
        check(k in t0, f"totais chave inicial {k}")
        check(
            isinstance(t0[k].get("custo"), float) and isinstance(t0[k].get("venda"), float),
            f"totais {k} custo/venda float",
        )

    gh_before = float(t0["uso_geraldinho"]["venda"])
    gd_before = float(t0["uso_geraldo"]["venda"])
    centro_before = float(t0["centro"]["venda"])

    r1, err1 = _mk_retirada("uso_geraldinho", "centro", venda="12.50", custo="5.00")
    check(err1 == "" and r1 is not None, f"confirma uso_geraldinho ({err1 or 'ok'})")
    if r1:
        check(r1.motivo == "uso_geraldinho", "PG motivo=uso_geraldinho")
        check(r1.deposito == "centro", "PG deposito centro")
        check(r1.itens.count() == 1, "1 item na retirada geraldinho")

    r2, err2 = _mk_retirada("uso_geraldo", "vila", venda="8.00", custo="3.00")
    check(err2 == "" and r2 is not None, f"confirma uso_geraldo ({err2 or 'ok'})")
    if r2:
        check(r2.motivo == "uso_geraldo", "PG motivo=uso_geraldo")

    t1 = totais_uso_loja_por_deposito()
    check(
        abs(t1["uso_geraldinho"]["venda"] - (gh_before + 12.50)) < 0.001,
        "card Uso Geraldinho soma venda +12,50",
    )
    check(
        abs(t1["uso_geraldo"]["venda"] - (gd_before + 8.00)) < 0.001,
        "card Uso Geraldo soma venda +8,00",
    )
    check(
        abs(t1["centro"]["venda"] - (centro_before + 12.50)) < 0.001,
        "card Uso Centro inclui saida geraldinho",
    )
    check(
        abs(t1["uso_geraldinho"]["custo"] - (float(t0["uso_geraldinho"]["custo"]) + 5.00))
        < 0.001,
        "card Uso Geraldinho soma custo +5,00",
    )

    # Motivo padrao nao deve entrar nos cards de dono
    r3, err3 = _mk_retirada("limpeza", "centro", venda="99.00", custo="1.00")
    check(err3 == "" and r3 is not None, f"confirma limpeza ({err3 or 'ok'})")
    t2 = totais_uso_loja_por_deposito()
    check(
        abs(t2["uso_geraldinho"]["venda"] - t1["uso_geraldinho"]["venda"]) < 0.001,
        "limpeza nao soma no card Geraldinho",
    )
    check(
        abs(t2["uso_geraldo"]["venda"] - t1["uso_geraldo"]["venda"]) < 0.001,
        "limpeza nao soma no card Geraldo",
    )

    if r1:
        ok_est, err_est = estornar_retirada_uso_loja(
            retirada=r1, operador_label=f"{PREFIX} estorno"
        )
        check(ok_est and not err_est, f"estorno geraldinho ({err_est or 'ok'})")
        t3 = totais_uso_loja_por_deposito()
        check(
            abs(t3["uso_geraldinho"]["venda"] - gh_before) < 0.001,
            "apos estorno card Geraldinho volta",
        )

    wipe()


def test_api_historico_keys() -> None:
    print("\n== API historico (auth) ==")
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first()
    if user is None:
        user = User.objects.create_superuser(
            username=f"{PREFIX.lower()}_admin",
            email=f"{PREFIX.lower()}@test.local",
            password="test-verify-uso-donos",
        )
    c = Client()
    c.force_login(user)
    wipe()
    r, err = _mk_retirada("uso_geraldinho", "centro", venda="3.30", custo="1.10")
    check(err == "" and r is not None, f"seed API ({err or 'ok'})")

    host = {"HTTP_HOST": "127.0.0.1"}
    resp = c.get("/api/pdv/uso-loja/historico/?limit=20", **host)
    check(resp.status_code == 200, f"historico HTTP {resp.status_code}")
    data = resp.json() if resp.status_code == 200 else {}
    check(data.get("ok") is True, "historico ok=true")
    totais = data.get("totais") or {}
    check("uso_geraldinho" in totais, "API totais.uso_geraldinho")
    check("uso_geraldo" in totais, "API totais.uso_geraldo")
    check("centro" in totais and "vila" in totais, "API totais centro+vila")
    itens = data.get("itens") or []
    hit = next((x for x in itens if x.get("motivo") == "uso_geraldinho"), None)
    check(hit is not None, "historico lista saida uso_geraldinho")
    if hit:
        check(
            hit.get("motivo_label") == "Uso Geraldinho",
            "motivo_label Uso Geraldinho no historico",
        )

    meta = c.get("/api/pdv/uso-loja/meta/", **host)
    check(meta.status_code == 200, f"meta HTTP {meta.status_code}")
    mdata = meta.json() if meta.status_code == 200 else {}
    motivos = {m.get("value"): m.get("label") for m in (mdata.get("motivos") or [])}
    check(motivos.get("uso_geraldinho") == "Uso Geraldinho", "meta motivos geraldinho")
    check(motivos.get("uso_geraldo") == "Uso Geraldo", "meta motivos geraldo")
    wipe()


def main() -> int:
    print("=== verify_uso_loja_donos_path (PDV-USO-DONOS) ===")
    test_modelo_motivos()
    test_template_overlay()
    test_js_totais()
    test_topbar_cor()
    test_totais_e_fluxo()
    test_api_historico_keys()
    print(f"\n=== RESULTADO: {PASS} OK · {FAIL} FAIL ===")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
