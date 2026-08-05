#!/usr/bin/env python
"""Smoke anti-duplicata PG + backup CP. VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def main() -> None:
    import django

    django.setup()
    from datetime import date
    from decimal import Decimal

    from django.urls import reverse

    from produtos.lancamentos_backup_util import montar_zip_backup_completo_pg
    from produtos.lancamentos_financeiro_pg_write_util import (
        buscar_titulo_pg_duplicado,
        inserir_lancamentos_manual_lote_pg,
        listar_titulos_pg_por_chave_nfe,
    )
    from produtos.models import TituloFinanceiroAgro

    for name in (
        "api_lancamentos_backup_completo_xlsx",
        "api_lancamentos_backup_abertos_xlsx",
        "api_lancamentos_backup_ultimo",
    ):
        reverse(name)

    tpl = open(
        os.path.join(
            ROOT, "produtos", "templates", "produtos", "lancamentos_contas_pagar_teste.html"
        ),
        encoding="utf-8",
    ).read()
    if 'id="sv-btn-backup"' not in tpl:
        fail("botão Backup ausente na tela CP")
    if "api_lancamentos_backup_ultimo" not in tpl:
        fail("tela CP sem status último backup")

    ent = open(
        os.path.join(ROOT, "produtos", "templates", "produtos", "entrada_nota.html"),
        encoding="utf-8",
    ).read()
    if "__entradaNfeFinSalvando" not in ent:
        fail("trava duplo clique financeiro NF ausente")

    marker = f"__VERIFY_DUP_{os.getpid()}__"
    TituloFinanceiroAgro.objects.filter(descricao__startswith="__VERIFY_DUP_").delete()
    chave = "35260749315542000142550000000149881816643599"
    r1 = inserir_lancamentos_manual_lote_pg(
        despesa=True,
        empresa_nome="LOJA TESTE",
        empresa_id=None,
        pessoa_nome="FORN VERIFY DUP",
        pessoa_id=None,
        data_competencia=date(2026, 7, 21),
        data_vencimento=date(2026, 8, 5),
        banco_nome="CAIXA",
        banco_id="x",
        forma_nome="BOLETO",
        forma_id=None,
        grupo_nome=None,
        grupo_id=None,
        usuario_label="verify",
        linhas=[
            {
                "valor": 123.45,
                "descricao": f"{marker} NF X (parcela 1/1)",
                "plano_conta": "COMPRA MERCADORIA CN",
                "observacao": f"Entrada NF-e Agro · chave {chave} · 2026-07-23",
            }
        ],
    )
    if not r1.get("ok") or not r1.get("ids"):
        fail(f"1º insert falhou: {r1}")
    r2 = inserir_lancamentos_manual_lote_pg(
        despesa=True,
        empresa_nome="LOJA TESTE",
        empresa_id=None,
        pessoa_nome="FORN VERIFY DUP",
        pessoa_id=None,
        data_competencia=date(2026, 7, 21),
        data_vencimento=date(2026, 8, 5),
        banco_nome="CAIXA",
        banco_id="x",
        forma_nome="BOLETO",
        forma_id=None,
        grupo_nome=None,
        grupo_id=None,
        usuario_label="verify",
        linhas=[
            {
                "valor": 123.45,
                "descricao": f"{marker} NF X (parcela 1/1)",
                "plano_conta": "COMPRA MERCADORIA CN",
                "observacao": f"Entrada NF-e Agro · chave {chave} · 2026-07-23",
            }
        ],
    )
    if r2.get("ok") or r2.get("ids"):
        fail(f"2º insert deveria bloquear, veio: {r2}")
    errs = " ".join(str(e.get("erro") or "") for e in (r2.get("erros") or []) if isinstance(e, dict))
    if "duplicidade bloqueada" not in errs.lower():
        fail(f"erro sem 'duplicidade bloqueada': {r2}")
    if not listar_titulos_pg_por_chave_nfe(chave):
        fail("listar por chave vazio")
    hit = buscar_titulo_pg_duplicado(
        despesa=True,
        pessoa_nome="FORN VERIFY DUP",
        valor=Decimal("123.45"),
        data_vencimento=date(2026, 8, 5),
        descricao=f"{marker} NF X (parcela 1/1)",
        observacoes=f"chave {chave}",
    )
    if not hit:
        fail("buscar_titulo_pg_duplicado não achou")

    blob, st = montar_zip_backup_completo_pg(somente_abertos=False, gerado_por="verify")
    if not st.get("ok") or len(blob) < 100:
        fail(f"backup pg falhou: {st}")

    TituloFinanceiroAgro.objects.filter(descricao__startswith="__VERIFY_DUP_").delete()
    print("VERIFY_OK anti-dup + backup CP")


if __name__ == "__main__":
    main()
