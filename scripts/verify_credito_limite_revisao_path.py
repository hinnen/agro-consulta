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

from django.contrib.auth import get_user_model
from django.db import transaction
from django.test import Client, override_settings
from django.urls import reverse

from produtos.caixa_util import validar_pin_operador
from produtos.credito_limite_revisao import (
    aprovar_limite_cliente,
    aprovar_limites_selecionados,
    ignorar_limite_cliente,
    listar_revisoes_limite,
    novo_limite_aplicavel,
)
from produtos.models import (
    ClienteAgro,
    ClienteAnaliseCreditoAgro,
    CreditoLimiteRevisaoDecisaoAgro,
    FiadoEventoAgro,
    FiadoTituloAgro,
    VendaAgro,
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
        check("segunda_aprovacao_nao", aprovar_limite_cliente(bom.pk, usuario="verify") is False)

        sem = ClienteAgro.objects.create(nome="ZZ Rev Sem", limite_fiado_local=Decimal("100"))
        _snap(sem, sugerido=Decimal("200"), confianca="SEM_DADOS", score=None)
        check("sem_dados_fora", "ZZ Rev Sem" not in {r["cliente_nome"] for r in listar_revisoes_limite()})

        a = ClienteAgro.objects.create(nome="ZZ Rev Lote A", limite_fiado_local=Decimal("100"))
        b = ClienteAgro.objects.create(nome="ZZ Rev Lote B", limite_fiado_local=Decimal("50"))
        c = ClienteAgro.objects.create(nome="ZZ Rev Lote C", limite_fiado_local=Decimal("40"))
        _snap(a, sugerido=Decimal("200"))
        _snap(b, sugerido=Decimal("200"))
        _snap(c, sugerido=Decimal("200"))
        n_tit = FiadoTituloAgro.objects.count()
        n_venda = VendaAgro.objects.count()
        ok_n, pulados = aprovar_limites_selecionados([a.pk, b.pk], usuario="verify")
        a.refresh_from_db()
        b.refresh_from_db()
        c.refresh_from_db()
        check("lote_dois", ok_n == 2 and pulados == 0, f"{ok_n}/{pulados}")
        check("lote_a_120", a.limite_fiado_local == Decimal("120.00"))
        check("lote_b_teto", b.limite_fiado_local == Decimal("60.00"), str(b.limite_fiado_local))
        check("lote_c_intacto", c.limite_fiado_local == Decimal("40.00"))
        check("titulos_intactos", FiadoTituloAgro.objects.count() == n_tit)
        check("vendas_intactas", VendaAgro.objects.count() == n_venda)
        transaction.savepoint_rollback(sid)

    print("== 4) Tela e acesso ==")
    html = (ROOT / "produtos/templates/produtos/credito_limite_revisao.html").read_text(encoding="utf-8")
    lab = (ROOT / "produtos/templates/produtos/credito_score_laboratorio.html").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    check("botao_lab", "REVISAR ALTERAÇÕES DE LIMITE" in lab)
    check("sem_aprovar_todos", "Aprovar todos" not in html and "aprovar_todos" not in html)
    check("tem_aprovar_selecionados", "Aprovar selecionados" in html)
    check("rota", "fiado/analise-credito/revisar-limites/" in urls)
    pin = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
    pin_ok, _msg = validar_pin_operador(pin)
    check("pin_9973", pin_ok is True, pin)

    User = get_user_model()
    superuser = User.objects.filter(is_superuser=True).order_by("pk").first()
    check("tem_superuser", superuser is not None, getattr(superuser, "username", ""))
    from django.conf import settings as dj_settings

    hosts = list(getattr(dj_settings, "ALLOWED_HOSTS", []) or [])
    if "testserver" not in hosts:
        hosts = hosts + ["testserver", "localhost", "127.0.0.1"]
    if superuser is not None:
        with override_settings(
            AGRO_CREDITO_SCORE_SHADOW_ENABLED=True,
            AGRO_CREDITO_SCORE_SHADOW_USERNAMES="renan",
            ALLOWED_HOSTS=hosts,
        ):
            c = Client()
            c.force_login(superuser)
            page = c.get(reverse("credito_limite_revisao"))
            check("super_200", page.status_code == 200, str(page.status_code))
            body = page.content.decode("utf-8", errors="replace")
            check("html_titulo", "Revisar alterações de limite" in body)
            check("html_sem_aprovar_todos", "Aprovar todos" not in body)
        operador = User.objects.filter(username__iexact="geraldinho").first()
        if operador is None:
            check("operador_404", True, "skip sem geraldinho")
        else:
            with override_settings(
                AGRO_CREDITO_SCORE_SHADOW_ENABLED=True,
                AGRO_CREDITO_SCORE_SHADOW_USERNAMES="renan",
                ALLOWED_HOSTS=hosts,
            ):
                c2 = Client()
                c2.force_login(operador)
                d = c2.get(reverse("credito_limite_revisao"))
                check("operador_404", d.status_code == 404, f"{operador.username} {d.status_code}")

    print(f"=== {len(oks)} OK · {len(fails)} FAIL ===")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
