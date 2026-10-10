#!/usr/bin/env python
"""Smoke verify: custo família + kit (saco custo/estoque). Saída VERIFY_OK ou VERIFY_FAIL."""
from __future__ import annotations

import os
import sys

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def main() -> None:
    from decimal import Decimal

    import django

    django.setup()

    from produtos.composicao_kit_util import (
        mesclar_composicao_no_extras,
        normalizar_composicao_lista,
    )
    from produtos.custo_familia_util import (
        aplicar_vinculo_saco_na_composicao,
        calcular_custo_filho,
        mesclar_custo_familia_no_extras,
        propagar_custo_familia_de_pai,
        qtd_baixa_saco_por_unidade,
    )
    from produtos.models import ProdutoGestaoOverlayAgro

    # --- calc puro ---
    if calcular_custo_filho(Decimal("94"), Decimal("47"), Decimal("5")) != Decimal("10.00"):
        fail("custo 47kg→5kg")
    if calcular_custo_filho(Decimal("94"), Decimal("47"), Decimal("1")) != Decimal("2.00"):
        fail("custo granel 1kg")
    if qtd_baixa_saco_por_unidade(47, 5) != Decimal("0.1064"):
        fail("qtd baixa 5/47")

    # --- vinculo saco injeta composicao ---
    ex: dict = {}
    mesclar_custo_familia_no_extras(
        ex,
        {
            "ativo": True,
            "pai_produto_id": "saco_test",
            "pai_nome": "Milho 47",
            "kg_pai": 47,
            "kg_filho": 5,
            "auto_sync": True,
            "baixa_estoque_saco": True,
        },
        filho_id="pct5_test",
    )
    mesclar_composicao_no_extras(
        ex,
        [{"produto_id": "outro", "nome": "Outro", "quantidade": 1, "deposito": "centro"}],
    )
    aplicar_vinculo_saco_na_composicao(ex)
    if not ex.get("kit", {}).get("baixa_componentes"):
        fail("kit.baixa deve ligar com saco+estoque")
    comp = ex.get("composicao") or []
    if len(comp) != 2:
        fail(f"esperava 2 linhas (manual+saco), got {len(comp)}: {comp}")
    saco = next((c for c in comp if c.get("origem") == "custo_familia"), None)
    if not saco or saco.get("produto_id") != "saco_test":
        fail("linha saco ausente")
    if float(saco["quantidade"]) != 0.1064:
        fail(f"qtd saco {saco['quantidade']}")

    # desliga baixa estoque saco → remove só linha gerenciada
    mesclar_custo_familia_no_extras(
        ex,
        {
            "ativo": True,
            "pai_produto_id": "saco_test",
            "pai_nome": "Milho 47",
            "kg_pai": 47,
            "kg_filho": 5,
            "baixa_estoque_saco": False,
        },
        filho_id="pct5_test",
    )
    aplicar_vinculo_saco_na_composicao(ex)
    if any(c.get("origem") == "custo_familia" for c in (ex.get("composicao") or [])):
        fail("linha saco deveria sumir com baixa off")
    if not any(c.get("produto_id") == "outro" for c in (ex.get("composicao") or [])):
        fail("linha manual não pode sumir")

    # --- propaga no PG ---
    pids = ["__vf_saco__", "__vf_pct5__", "__vf_granel__", "__vf_off__"]
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__in=pids).delete()
    ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id="__vf_saco__",
        nome="Saco VF",
        cadastro_extras={"preco_custo_overlay": 94.0},
    )
    ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id="__vf_pct5__",
        nome="5kg VF",
        cadastro_extras={
            "custo_familia": {
                "ativo": True,
                "pai_produto_id": "__vf_saco__",
                "kg_pai": 47,
                "kg_filho": 5,
                "auto_sync": True,
                "baixa_estoque_saco": True,
            }
        },
    )
    ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id="__vf_granel__",
        nome="Granel VF",
        cadastro_extras={
            "custo_familia": {
                "ativo": True,
                "pai_produto_id": "__vf_saco__",
                "kg_pai": 47,
                "kg_filho": 1,
                "auto_sync": True,
            }
        },
    )
    ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id="__vf_off__",
        nome="Off VF",
        cadastro_extras={
            "custo_familia": {
                "ativo": True,
                "pai_produto_id": "__vf_saco__",
                "kg_pai": 47,
                "kg_filho": 2,
                "auto_sync": False,
            },
            "preco_custo_overlay": 9.99,
        },
    )
    out = propagar_custo_familia_de_pai("__vf_saco__", Decimal("94"))
    if out.get("atualizados") != 2:
        fail(f"propagar atualizados={out}")
    c5 = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="__vf_pct5__")
    if float(c5.cadastro_extras.get("preco_custo_overlay")) != 10.0:
        fail("filho 5kg custo")
    cg = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="__vf_granel__")
    if float(cg.cadastro_extras.get("preco_custo_overlay")) != 2.0:
        fail("granel custo")
    coff = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="__vf_off__")
    if float(coff.cadastro_extras.get("preco_custo_overlay")) != 9.99:
        fail("auto_sync off não deveria mudar")

    # aplicar vinculo no filho 5kg
    ex5 = dict(c5.cadastro_extras)
    aplicar_vinculo_saco_na_composicao(ex5)
    c5.cadastro_extras = ex5
    c5.save(update_fields=["cadastro_extras", "atualizado_em"])
    from produtos.views import _composicao_efetiva_produto_agro

    comp_ef = _composicao_efetiva_produto_agro(c5, None)
    if not any(x.get("origem") == "custo_familia" for x in comp_ef):
        fail("composicao efetiva sem saco")

    # detalhe agro_pg devolve CF (não apaga ao reabrir)
    from produtos.catalogo_agro import produto_model_para_detalhe
    from produtos.models import Produto

    Produto.objects.filter(produto_externo_id="__vf_pct5__").delete()
    p_mod = Produto.objects.create(
        produto_externo_id="__vf_pct5__",
        codigo_interno="VF5",
        nome="5kg VF",
        custo=Decimal("10.00"),
        preco_venda=Decimal("15.00"),
        cadastro_somente_agro=True,
    )
    det = produto_model_para_detalhe(p_mod)
    if not det.get("custo_familia") or det["custo_familia"].get("pai_produto_id") != "__vf_saco__":
        fail(f"detalhe agro_pg sem custo_familia: {det.get('custo_familia')}")
    if not any(
        x.get("origem") == "custo_familia" for x in (det.get("composicao") or [])
    ):
        fail("detalhe agro_pg sem composicao saco")
    if det.get("eh_kit"):
        fail("só saco não deve marcar eh_kit")
    Produto.objects.filter(produto_externo_id="__vf_pct5__").delete()

    # UI ids / rotas
    from django.urls import reverse

    reverse("api_custo_familia_propagar")
    reverse("api_produtos_gestao_overlay_salvar")

    modal = os.path.join(
        ROOT,
        "produtos",
        "templates",
        "produtos",
        "_modal_editar_produto_cadastro_erp.inc.html",
    )
    html = open(modal, encoding="utf-8").read()
    for need in (
        "edit-cf-ativo",
        "edit-cf-baixa-estoque",
        "edit-cf-toggle-corpo",
        "edit-cf-corpo",
        "edit-kit-ativo",
        "edit-kit-campos",
        "edit-kit-toggle-corpo",
        "edit-kit-corpo",
        "coletarCustoFamiliaPayload",
        "baixa_estoque_saco",
        "comp-btn-buscar",
        "atualizarKitCamposVisivel",
        "atualizarCfCorpoVisivel",
    ):
        if need not in html:
            fail(f"modal sem {need}")
    if "outra ferramenta" in html.lower():
        fail("modal ainda tem divisor 'outra ferramenta'")
    if "Bloco 1 (saco):" in html:
        fail("texto longo do intro ainda fora do ?")
    if "comp-baixa-auto" in html:
        fail("modal ainda tem comp-baixa-auto legado")
    if "id=\"edit-cf-campos\" class=\"hidden" not in html and 'id="edit-cf-campos" class="hidden' not in html:
        # tolerar order of classes
        if 'id="edit-cf-campos"' not in html or "hidden" not in html.split('id="edit-cf-campos"')[1][:80]:
            fail("edit-cf-campos deve iniciar hidden")

    # cleanup
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__in=pids).delete()

    # normalizar lista preserva origem
    n = normalizar_composicao_lista(
        [{"produto_id": "X", "quantidade": 1, "origem": "custo_familia"}]
    )
    if not n or n[0].get("origem") != "custo_familia":
        fail("origem não preservada")

    print("VERIFY_OK")


if __name__ == "__main__":
    main()
