# -*- coding: utf-8 -*-
"""Prova da revisão de limite (lab). Não grava na loja: savepoint + rollback."""
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

from django.db import transaction

from produtos.credito_limite_revisao import (
    aprovar_limite_cliente,
    ignorar_limite_cliente,
    listar_revisoes_limite,
    novo_limite_aplicavel,
)
from produtos.models import (
    ClienteAgro,
    ClienteAnaliseCreditoAgro,
    CreditoLimiteRevisaoDecisaoAgro,
    FiadoEventoAgro,
)

oks: list[str] = []
fails: list[str] = []


def check(name: str, cond: bool, extra: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {extra}" if extra else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {extra}" if extra else ""))


def _snap(cli, *, sugerido, confianca="MEDIA", score=90, vencido=False, alertas=None, cad=None):
    cadastro = cli.limite_fiado_local if cad is None else cad
    return ClienteAnaliseCreditoAgro.objects.create(
        cliente=cli,
        regra_versao="shadow_v1_2",
        score=score,
        classificacao=ClienteAnaliseCreditoAgro.Classificacao.BOM,
        confianca=confianca,
        limite_cadastrado_snapshot=cadastro,
        limite_efetivo_snapshot=cadastro if cadastro and cadastro > 0 else Decimal("5000"),
        limite_sugerido=sugerido,
        tem_vencido_snapshot=vencido,
        alertas_json=alertas or [],
        indicadores_json={},
    )


def main() -> int:
    print("== 1) Teto +20% ==")
    check("cap_200", novo_limite_aplicavel("100", "200") == Decimal("120.00"))
    check("abaixo_do_teto", novo_limite_aplicavel("100", "110") == Decimal("110.00"))
    check("reducao", novo_limite_aplicavel("100", "50") == Decimal("50.00"))
    src = (ROOT / "produtos/credito_limite_revisao.py").read_text(encoding="utf-8")
    check("usa_definir", "definir_limite_fiado_cliente(" in src)
    check("nao_grava_campo_direto", "limite_fiado_local =" not in src)

    print("== 2) Quem entra na lista ==")
    with transaction.atomic():
        sid = transaction.savepoint()
        bom = ClienteAgro.objects.create(nome="ZZ Rev Bom", limite_fiado_local=Decimal("100"))
        _snap(bom, sugerido=Decimal("200"))
        venc = ClienteAgro.objects.create(nome="ZZ Rev Venc", limite_fiado_local=Decimal("100"))
        _snap(venc, sugerido=Decimal("200"), vencido=True)
        baixa = ClienteAgro.objects.create(nome="ZZ Rev Baixa", limite_fiado_local=Decimal("100"))
        _snap(baixa, sugerido=Decimal("200"), confianca="BAIXA")
        bloq = ClienteAgro.objects.create(nome="ZZ Rev Bloq", limite_fiado_local=Decimal("0.01"))
        _snap(bloq, sugerido=Decimal("200"), cad=Decimal("0.01"))
        dados = ClienteAgro.objects.create(nome="ZZ Rev Dados", limite_fiado_local=Decimal("100"))
        _snap(dados, sugerido=Decimal("200"), alertas=["Quitado sem baixas suficientes"])
        igual = ClienteAgro.objects.create(nome="ZZ Rev Igual", limite_fiado_local=Decimal("100"))
        _snap(igual, sugerido=Decimal("100"))
        nomes = {r["cliente_nome"] for r in listar_revisoes_limite()}
        check("bom_entra", "ZZ Rev Bom" in nomes)
        check("vencido_fora", "ZZ Rev Venc" not in nomes)
        check("baixa_fora", "ZZ Rev Baixa" not in nomes)
        check("bloqueio_fora", "ZZ Rev Bloq" not in nomes)
        check("dados_fora", "ZZ Rev Dados" not in nomes)
        check("igual_fora", "ZZ Rev Igual" not in nomes)
        linha = next(r for r in listar_revisoes_limite() if r["cliente_pk"] == bom.pk)
        check("novo_120", linha["novo_limite"] == Decimal("120.00"), str(linha["novo_limite"]))
        check("diff_20", linha["diferenca"] == Decimal("20.00"))

        print("== 3) Aprovar e ignorar ==")
        n_evt = FiadoEventoAgro.objects.filter(cliente_agro=bom, tipo=FiadoEventoAgro.Tipo.LIMITE).count()
        ok = aprovar_limite_cliente(bom.pk, usuario="verify")
        bom.refresh_from_db()
        check("aprovou", ok is True)
        check("limite_virou_120", bom.limite_fiado_local == Decimal("120.00"), str(bom.limite_fiado_local))
        check(
            "evento_limite",
            FiadoEventoAgro.objects.filter(cliente_agro=bom, tipo=FiadoEventoAgro.Tipo.LIMITE).count() == n_evt + 1,
        )
        nomes2 = {r["cliente_nome"] for r in listar_revisoes_limite()}
        check("saiu_depois_aprovar", "ZZ Rev Bom" not in nomes2)

        ign = ClienteAgro.objects.create(nome="ZZ Rev Ign", limite_fiado_local=Decimal("80"))
        _snap(ign, sugerido=Decimal("200"))
        antes = ign.limite_fiado_local
        check("ignorou", ignorar_limite_cliente(ign.pk, usuario="verify") is True)
        ign.refresh_from_db()
        check("ignorar_nao_muda", ign.limite_fiado_local == antes)
        check(
            "decisao_ignorar",
            CreditoLimiteRevisaoDecisaoAgro.objects.filter(
                cliente=ign, acao=CreditoLimiteRevisaoDecisaoAgro.Acao.IGNORADO
            ).exists(),
        )
        transaction.savepoint_rollback(sid)

    print(f"=== {len(oks)} OK · {len(fails)} FAIL ===")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
