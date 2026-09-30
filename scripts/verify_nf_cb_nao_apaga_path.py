#!/usr/bin/env python
"""Prova NF-CB-NAO-APAGA: nota não apaga o código. VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FAIL: list[str] = []
OK = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL.append(name + (f" — {detail}" if detail else ""))
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def main() -> None:
    import django

    django.setup()
    from django.db import transaction

    from produtos.catalogo_agro import sincronizar_modelo_produto_de_overlay
    from produtos.mongo_index_codigos import aplicar_bip_entrada_nf_troca_inteligente
    from produtos.models import Produto, ProdutoGestaoOverlayAgro

    views = (ROOT / "produtos" / "views.py").read_text(encoding="utf-8")
    trecho = views.split("codigo_barras_bip_entrada_nf", 1)[-1][:1800]
    check("view_le_codigo_do_produto", "Produto.objects.filter" in trecho and "atual_cb" in trecho)

    r = aplicar_bip_entrada_nf_troca_inteligente(
        codigo_barras_atual="7899751100632", cadastro_extras={}, bip="7899751100632"
    )
    check("igual_noop", r["acao"] == "noop" and r["codigo_barras"] is None)

    r = aplicar_bip_entrada_nf_troca_inteligente(
        codigo_barras_atual="2300000001543", cadastro_extras={}, bip="7908405901478"
    )
    check("diferente_so_extra", r["acao"] == "opcional" and r["codigo_barras"] is None)
    check("diferente_nao_troca_230", "2300000001543" not in (r["codigos_barras_opcionais"] or []))
    check("diferente_entra_extra", "7908405901478" in (r["codigos_barras_opcionais"] or []))

    r = aplicar_bip_entrada_nf_troca_inteligente(
        codigo_barras_atual="", cadastro_extras={}, bip="7899751100632"
    )
    check("vazio_vira_principal", r["acao"] == "definir" and r["codigo_barras"] == "7899751100632")

    r = aplicar_bip_entrada_nf_troca_inteligente(
        codigo_barras_atual="7891111111111",
        cadastro_extras={"codigos_barras_opcionais": ["7892222222222"]},
        bip="7898752405197",
    )
    check(
        "extra_nao_apaga_o_que_ja_tinha",
        set(r["codigos_barras_opcionais"] or []) >= {"7892222222222", "7898752405197"},
    )

    r = aplicar_bip_entrada_nf_troca_inteligente(
        codigo_barras_atual="7899751100632", cadastro_extras={}, bip="123"
    )
    check("bip_curto_noop", r["acao"] == "noop")

    pid = "tmp-nf-cb-nao-apaga"
    pid2 = "tmp-nf-cb-vazio"
    try:
        with transaction.atomic():
            p = Produto.objects.create(
                produto_externo_id=pid,
                codigo_interno="0934",
                codigo_nfe="GM0934",
                codigo_barras="7899751100632",
                nome="lanterna teste",
                marca="MARCA",
                categoria="Ferragens",
                unidade="CX",
                custo=Decimal("10.00"),
                preco_venda=Decimal("20.00"),
            )
            ov = ProdutoGestaoOverlayAgro.objects.create(produto_externo_id=pid)
            sincronizar_modelo_produto_de_overlay(pid, ov, custo_payload=Decimal("12.50"))
            p.refresh_from_db()
            check("custo_nao_apaga_codigo", p.codigo_barras == "7899751100632")
            check("custo_grava", p.custo == Decimal("12.50"))
            check("custo_nao_apaga_marca", p.marca == "MARCA")
            check("custo_nao_apaga_unidade", p.unidade == "CX")
            check("custo_nao_apaga_preco", p.preco_venda == Decimal("20.00"))

            sincronizar_modelo_produto_de_overlay(pid, ov, payload={"preco_venda": "99.00"})
            p.refresh_from_db()
            check("preco_payload_nao_apaga_codigo", p.codigo_barras == "7899751100632")
            check("preco_payload_nao_troca_sem_overlay", p.preco_venda == Decimal("20.00"))

            atual = (ov.codigo_barras or "").strip() or (p.codigo_barras or "")
            res = aplicar_bip_entrada_nf_troca_inteligente(
                codigo_barras_atual=atual,
                cadastro_extras={},
                bip="7908405901478",
            )
            check("ficha_vazia_bip_diferente_extra", res["acao"] == "opcional")
            if res["acao"] == "opcional":
                ex = dict(ov.cadastro_extras or {})
                ex["codigos_barras_opcionais"] = res["codigos_barras_opcionais"]
                ov.cadastro_extras = ex
                ov.save(update_fields=["cadastro_extras", "atualizado_em"])
            sincronizar_modelo_produto_de_overlay(
                pid,
                ov,
                payload={
                    "codigo_barras_bip_entrada_nf": "7908405901478",
                    "origem_entrada_nf": True,
                },
            )
            p.refresh_from_db()
            ov.refresh_from_db()
            check("depois_do_bip_principal_fica", p.codigo_barras == "7899751100632")
            check("overlay_principal_nao_trocou", (ov.codigo_barras or "") == "")
            opc = (ov.cadastro_extras or {}).get("codigos_barras_opcionais") or []
            check("extra_gravado", "7908405901478" in opc)

            res_eq = aplicar_bip_entrada_nf_troca_inteligente(
                codigo_barras_atual=p.codigo_barras or "",
                cadastro_extras=ov.cadastro_extras,
                bip="7899751100632",
            )
            check("bip_igual_nao_mexe", res_eq["acao"] == "noop")

            ov.marca = "OUTRA"
            ov.codigo_barras = "7890000000001"
            ov.save(update_fields=["marca", "codigo_barras", "atualizado_em"])
            sincronizar_modelo_produto_de_overlay(pid, ov, custo_payload=Decimal("13.00"))
            p.refresh_from_db()
            check("overlay_preenchido_atualiza", p.codigo_barras == "7890000000001" and p.marca == "OUTRA")

            ov.codigo_barras = ""
            ov.save(update_fields=["codigo_barras", "atualizado_em"])
            sincronizar_modelo_produto_de_overlay(pid, ov, payload={"codigo_barras": ""})
            p.refresh_from_db()
            check("modal_pode_limpar", not (p.codigo_barras or "").strip())

            p2 = Produto.objects.create(
                produto_externo_id=pid2,
                codigo_interno="0001",
                codigo_nfe="GM0001",
                codigo_barras="",
                nome="sem codigo",
                unidade="UN",
                custo=Decimal("1"),
                preco_venda=Decimal("2"),
            )
            ov2 = ProdutoGestaoOverlayAgro.objects.create(produto_externo_id=pid2)
            res2 = aplicar_bip_entrada_nf_troca_inteligente(
                codigo_barras_atual=(ov2.codigo_barras or "") or (p2.codigo_barras or ""),
                cadastro_extras={},
                bip="7899751100649",
            )
            check("sem_codigo_bip_define", res2["acao"] == "definir" and res2["codigo_barras"] == "7899751100649")
            raise RuntimeError("rollback")
    except RuntimeError as exc:
        if str(exc) != "rollback":
            raise
        check("rollback_sem_sujar_banco", not Produto.objects.filter(produto_externo_id=pid).exists())

    total = OK + len(FAIL)
    print("")
    if FAIL:
        print(f"VERIFY_FAIL {OK}/{total}")
        for f in FAIL:
            print(" -", f)
        raise SystemExit(1)
    print(f"VERIFY_OK {OK}/{total}")


if __name__ == "__main__":
    main()
