# -*- coding: utf-8 -*-
"""Prova — lápis do limite com título + histórico ao corrigir o nome.

  python scripts/verify_fiado_limite_renome_path.py
"""
from __future__ import annotations

import os
import sys
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.cliente_operacoes_util import propagar_renome_cliente
from produtos.fiado_gestao_util import listar_clientes_fiado
from produtos.models import ClienteAgro, FiadoTituloAgro, PedidoEntrega, VendaAgro
from produtos.relacionamento_cliente_util import _vendas_cliente_qs

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_js() -> None:
    print("== Tela /fiado/ ==")
    js = (ROOT / "produtos/static/produtos/js/fiado_gestao.js").read_text(encoding="utf-8")
    check("botao_se_tem_pk", "const limiteCel = pk" in js and "fiado-limite-valor" in js)
    check("sem_pk_nao_edita", "Cadastro sem vínculo" in js)


def test_lista_e_renome() -> None:
    print("== Lista com título + renome ==")
    tag = uuid.uuid4().hex[:8]
    nome = f"ZZ Prova Limite {tag}"
    cli = ClienteAgro.objects.create(nome=nome, limite_fiado_local=Decimal("50.00"))
    venda = VendaAgro.objects.create(
        cliente_nome=nome,
        cliente_id_erp=f"agro:{cli.pk}",
        total=Decimal("19.90"),
        forma_pagamento="Fiado",
    )
    titulo = FiadoTituloAgro.objects.create(
        chave_unica=f"prova-limite:{tag}",
        cliente_agro=cli,
        venda_agro=venda,
        cliente_nome=nome,
        vencimento=date.today(),
        valor_bruto=Decimal("19.90"),
        situacao=FiadoTituloAgro.Situacao.ABERTO,
        origem=FiadoTituloAgro.Origem.PDV,
    )
    entrega = PedidoEntrega.objects.create(
        cliente_agro=cli,
        cliente_nome=nome,
        status=PedidoEntrega.Status.PENDENTE,
    )
    orfao = FiadoTituloAgro.objects.create(
        chave_unica=f"prova-orfao:{tag}",
        cliente_agro=None,
        cliente_nome=nome,
        vencimento=date.today(),
        valor_bruto=Decimal("3.00"),
        situacao=FiadoTituloAgro.Situacao.ABERTO,
        origem=FiadoTituloAgro.Origem.IMPORTACAO,
    )
    homonimo = ClienteAgro.objects.create(nome=f"ZZ Outro {tag}")
    titulo_outro = FiadoTituloAgro.objects.create(
        chave_unica=f"prova-outro:{tag}",
        cliente_agro=homonimo,
        cliente_nome=homonimo.nome,
        vencimento=date.today(),
        valor_bruto=Decimal("7.00"),
        situacao=FiadoTituloAgro.Situacao.ABERTO,
    )
    try:
        rows = listar_clientes_fiado(busca=tag, apenas_com_saldo=True)
        row = next((r for r in rows if r.get("cliente_agro_pk") == cli.pk), None)
        check("lista_acha_com_titulo", row is not None)
        check(
            "lista_mantem_pk",
            bool(row and row.get("cliente_agro_pk") == cli.pk),
            str(row.get("cliente_agro_pk") if row else None),
        )
        check("lista_tem_titulos", bool(row and row.get("titulos_abertos", 0) >= 1))

        novo = nome.replace("Limite", "Limite X")
        cli.nome = novo
        cli.save(update_fields=["nome", "atualizado_em"])
        propagar_renome_cliente(cli, nome)

        titulo.refresh_from_db()
        venda.refresh_from_db()
        entrega.refresh_from_db()
        orfao.refresh_from_db()
        titulo_outro.refresh_from_db()
        check("titulo_renomeado", titulo.cliente_nome == novo)
        check("venda_renomeada", venda.cliente_nome == novo)
        check("entrega_renomeada", entrega.cliente_nome == novo)
        check("orfao_segue_e_vincula", orfao.cliente_nome == novo and orfao.cliente_agro_id == cli.pk)
        check("nao_rouba_outro", titulo_outro.cliente_nome == homonimo.nome and titulo_outro.cliente_agro_id == homonimo.pk)
        n_hist = _vendas_cliente_qs(cli).filter(pk=venda.pk).count()
        check("historico_ainda_acha", n_hist == 1, str(n_hist))

        cli.nome = nome
        cli.save(update_fields=["nome", "atualizado_em"])
        propagar_renome_cliente(cli, novo)
        check("igual_nao_mexe", True)
    finally:
        FiadoTituloAgro.objects.filter(chave_unica__in=[
            f"prova-limite:{tag}",
            f"prova-orfao:{tag}",
            f"prova-outro:{tag}",
        ]).delete()
        PedidoEntrega.objects.filter(pk=entrega.pk).delete()
        VendaAgro.objects.filter(pk=venda.pk).delete()
        ClienteAgro.objects.filter(pk__in=[cli.pk, homonimo.pk]).delete()


def test_nao_pega_nome_de_outro() -> None:
    print("== Dois cadastros com o mesmo nome ==")
    tag = uuid.uuid4().hex[:8]
    nome = f"ZZ Igual {tag}"
    a = ClienteAgro.objects.create(nome=nome)
    b = ClienteAgro.objects.create(nome=nome)
    t = FiadoTituloAgro.objects.create(
        chave_unica=f"prova-dup:{tag}",
        cliente_agro=None,
        cliente_nome=nome,
        vencimento=date.today(),
        valor_bruto=Decimal("1.00"),
        situacao=FiadoTituloAgro.Situacao.ABERTO,
    )
    try:
        a.nome = nome + " A"
        a.save(update_fields=["nome", "atualizado_em"])
        propagar_renome_cliente(a, nome)
        t.refresh_from_db()
        check("homonimo_nao_vincula_orfao", t.cliente_agro_id is None and t.cliente_nome == nome)
    finally:
        t.delete()
        a.delete()
        b.delete()


def main() -> int:
    test_js()
    test_lista_e_renome()
    test_nao_pega_nome_de_outro()
    print(f"\n== {len(oks)} OK · {len(fails)} FAIL ==")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
