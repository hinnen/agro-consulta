"""
PROMO-REGRA-TABELA-SAVE — path promo × tabela % (save, load, PDV util).
Roda: python scripts/verify_promo_regra_tabela_path.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

PASS = 0
FAIL = 0


def check(ok: bool, msg: str) -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  OK  {msg}")
    else:
        FAIL += 1
        print(f" FAIL {msg}")


def main() -> int:
    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.test import RequestFactory

    from produtos.models import PromocaoAgro, PromocaoProdutoAgro
    from produtos.promocoes_views import api_promocoes_salvar
    from produtos.tabela_preco_forma_util import regra_promo_vs_tabela

    script = (
        ROOT / "produtos/templates/produtos/includes/promocoes_form_script.html"
    ).read_text(encoding="utf-8")
    views = (ROOT / "produtos/promocoes_views.py").read_text(encoding="utf-8")
    form = (ROOT / "produtos/templates/produtos/promocoes_form.html").read_text(
        encoding="utf-8"
    )

    print("=== UI / script inline ===")
    check('id="promo-regra-vs-tabela"' in form, "HTML campo promo-regra-vs-tabela")
    check("regraVsTabela:" in script, "dom.regraVsTabela")
    check("regra_vs_tabela: dom.regraVsTabela" in script, "coletarPayload manda regra_vs_tabela")
    check("state.regra_vs_tabela" in script, "hydrateForm lê regra_vs_tabela")
    check("resolucoes_vs_tabela" in script, "payload resolucoes_vs_tabela")
    check("preco_y_t1:" in script, "payload preco_y_t1")
    check("preco_y_t2:" in script, "payload preco_y_t2")
    check(
        script.count("regra_vs_tabela") >= 2,
        "regra_vs_tabela referenciada 2+ no script",
    )

    print("=== Backend views ===")
    check("regra_vs_tabela" in views, "views grava regra_vs_tabela")
    check("resolucoes_vs_tabela" in views, "views grava resolucoes_vs_tabela")
    check('"regra_vs_tabela":' in views, "context inicial inclui regra_vs_tabela")

    print("=== Util promo x tabela ===")
    check(regra_promo_vs_tabela(9, 10, "promo") == 9, "regra promo → 9")
    check(regra_promo_vs_tabela(9, 10, "tabela") == 10, "regra tabela → 10")
    check(regra_promo_vs_tabela(9, 10, "maior") == 10, "regra maior → 10")

    print("=== API salvar + reload context ===")
    User = get_user_model()
    user, _ = User.objects.get_or_create(
        username="verify_promo_regra_tabela",
        defaults={"is_staff": True, "is_active": True},
    )
    user.set_password("x")
    user.save()
    factory = RequestFactory()

    def post_salvar(payload):
        req = factory.post(
            "/api/promocoes/salvar/",
            data=json.dumps(payload),
            content_type="application/json",
        )
        req.user = user
        with patch("produtos.promocoes_views.reverse", return_value="/promocoes/editar/1/"):
            return api_promocoes_salvar(req)

    payload = {
        "nome": "VERIFY REGRA PROMO",
        "tipo": "leve_pague",
        "qtd_x": "4",
        "preco_y": "2,50",
        "preco_y_t1": "2,40",
        "regra_vs_tabela": "promo",
        "resolucoes_vs_tabela": {"999001": "tabela"},
        "data_inicio": "2026-06-01",
        "permanente": True,
        "empresas": ["centro"],
        "ativo": True,
        "produtos": [
            {
                "produto_externo_id": "999001",
                "codigo": "GM999001",
                "nome_produto": "Teste verify",
                "preco_padrao": 10,
            }
        ],
    }
    resp = post_salvar(payload)
    check(resp.status_code == 200, f"POST salvar HTTP {resp.status_code}")
    data = json.loads(resp.content.decode("utf-8")) if resp.status_code == 200 else {}
    check(data.get("ok") is True, "POST salvar ok=true")
    pk = data.get("id")
    check(bool(pk), "retornou id da promo")

    if pk:
        promo = PromocaoAgro.objects.get(pk=pk)
        check(promo.regra_vs_tabela == "promo", f"PG regra={promo.regra_vs_tabela}")
        check(
            promo.resolucoes_vs_tabela.get("999001") == "tabela",
            "PG resolucao item tabela",
        )
        check(float(promo.preco_y_t1 or 0) == 2.4, "PG preco_y_t1")
        check((promo.regra_vs_tabela or "maior") == "promo", "reload regra promo")

        # update para tabela
        payload["id"] = pk
        payload["regra_vs_tabela"] = "tabela"
        resp2 = post_salvar(payload)
        promo.refresh_from_db()
        check(resp2.status_code == 200 and promo.regra_vs_tabela == "tabela", "update -> tabela")

        PromocaoProdutoAgro.objects.filter(promocao_id=pk).delete()
        promo.delete()

    print("=== valor invalido cai no default ===")
    payload.pop("id", None)
    payload["regra_vs_tabela"] = "invalido"
    payload["nome"] = "VERIFY REGRA DEFAULT"
    resp3 = post_salvar(payload)
    if resp3.status_code == 200:
        data3 = json.loads(resp3.content.decode("utf-8"))
        if data3.get("ok"):
            pk3 = data3.get("id")
            p3 = PromocaoAgro.objects.get(pk=pk3)
            check(p3.regra_vs_tabela == "maior", "regra invalida -> maior")
            PromocaoProdutoAgro.objects.filter(promocao_id=pk3).delete()
            p3.delete()
        else:
            check(False, "salvar com regra invalida para teste default")
    else:
        check(False, "salvar com regra invalida para teste default")

    print(f"\n{PASS} ok · {FAIL} fail")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
