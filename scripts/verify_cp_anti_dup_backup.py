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


def ok(msg: str) -> None:
    print(f"OK {msg}")


def main() -> None:
    import django

    django.setup()
    from datetime import date
    from decimal import Decimal
    from pathlib import Path

    from django.contrib.auth import get_user_model
    from django.core.cache import cache
    from django.test import Client
    from django.urls import reverse

    from produtos.lancamentos_backup_util import (
        montar_zip_backup_completo_pg,
        montar_zip_backup_lancamentos_dispatch,
    )
    from produtos.lancamentos_financeiro_pg_write_util import (
        buscar_titulo_pg_duplicado,
        inserir_lancamentos_manual_lote_pg,
        listar_titulos_pg_por_chave_nfe,
    )
    from produtos.models import TituloFinanceiroAgro

    checks = 0

    for name in (
        "api_lancamentos_backup_completo_xlsx",
        "api_lancamentos_backup_abertos_xlsx",
        "api_lancamentos_backup_ultimo",
    ):
        reverse(name)
    checks += 1
    ok("rotas backup")

    tpl = Path(ROOT, "produtos/templates/produtos/lancamentos_contas_pagar_teste.html").read_text(
        encoding="utf-8"
    )
    for needle in (
        'id="sv-btn-backup"',
        "api_lancamentos_backup_ultimo",
        "Só em aberto",
        "initBackupCp",
        ".sv-backup-menu[hidden] { display: none; }",
        "function fecharMenu()",
        "ev.key === 'Escape'",
        'id="sv-backup-menu" hidden',
    ):
        if needle not in tpl:
            fail(f"tela CP sem '{needle}'")
    if tpl.count("fecharMenu();") < 3:
        fail("fecharMenu() deve fechar após baixar + Esc/fora (marcadores < 3)")
    checks += 1
    ok("UI Backup CP (menu fecha de verdade)")

    ent = Path(ROOT, "produtos/templates/produtos/entrada_nota.html").read_text(encoding="utf-8")
    if "__entradaNfeFinSalvando" not in ent:
        fail("trava duplo clique financeiro NF ausente")
    if "fin.recuperado" not in ent:
        fail("UI Entrada NF sem tratamento recuperado")
    checks += 1
    ok("trava Entrada NF")

    views_src = Path(ROOT, "produtos/views.py").read_text(encoding="utf-8")
    if "listar_titulos_pg_por_chave_nfe" not in views_src:
        fail("views sem listar_titulos_pg_por_chave_nfe")
    if '"recuperado": True' not in views_src:
        fail("views sem retorno recuperado")
    write_src = Path(ROOT, "produtos/lancamentos_financeiro_pg_write_util.py").read_text(
        encoding="utf-8"
    )
    if "buscar_titulo_pg_duplicado" not in write_src or "batch_sigs" not in write_src:
        fail("write util sem anti-dup / batch_sigs")
    bak_src = Path(ROOT, "produtos/lancamentos_backup_util.py").read_text(encoding="utf-8")
    if "montar_zip_backup_completo_pg" not in bak_src:
        fail("backup util sem ramo PG")
    checks += 1
    ok("código fonte presente")

    marker = f"__VERIFY_DUP_{os.getpid()}__"
    TituloFinanceiroAgro.objects.filter(descricao__startswith="__VERIFY_DUP_").delete()
    chave = "35260749315542000142550000000149881816643599"
    linha_base = {
        "valor": 123.45,
        "descricao": f"{marker} NF X (parcela 1/1)",
        "plano_conta": "COMPRA MERCADORIA CN",
        "observacao": f"Entrada NF-e Agro · chave {chave} · 2026-07-23",
    }
    kwargs = dict(
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
    )
    r1 = inserir_lancamentos_manual_lote_pg(**kwargs, linhas=[dict(linha_base)])
    if not r1.get("ok") or not r1.get("ids"):
        fail(f"1º insert falhou: {r1}")
    checks += 1
    ok("1º insert")

    r2 = inserir_lancamentos_manual_lote_pg(**kwargs, linhas=[dict(linha_base)])
    if r2.get("ok") or r2.get("ids"):
        fail(f"2º insert deveria bloquear, veio: {r2}")
    errs = " ".join(str(e.get("erro") or "") for e in (r2.get("erros") or []) if isinstance(e, dict))
    if "duplicidade bloqueada" not in errs.lower():
        fail(f"erro sem 'duplicidade bloqueada': {r2}")
    checks += 1
    ok("2º insert bloqueado por chave")

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
    checks += 1
    ok("buscar + listar por chave")

    # Assinatura sem chave (pessoa+valor+venc+desc).
    marker2 = f"{marker}_SIG"
    r_sig1 = inserir_lancamentos_manual_lote_pg(
        **{**kwargs, "pessoa_nome": "FORN VERIFY SIG"},
        linhas=[
            {
                "valor": 99.01,
                "descricao": f"{marker2} manual",
                "plano_conta": "COMPRA MERCADORIA CN",
                "observacao": "sem chave nf",
            }
        ],
    )
    if not r_sig1.get("ok"):
        fail(f"insert assinatura falhou: {r_sig1}")
    r_sig2 = inserir_lancamentos_manual_lote_pg(
        **{**kwargs, "pessoa_nome": "FORN VERIFY SIG"},
        linhas=[
            {
                "valor": 99.01,
                "descricao": f"{marker2} manual",
                "plano_conta": "COMPRA MERCADORIA CN",
                "observacao": "outra obs",
            }
        ],
    )
    errs_sig = " ".join(
        str(e.get("erro") or "") for e in (r_sig2.get("erros") or []) if isinstance(e, dict)
    )
    if r_sig2.get("ok") or "duplicidade bloqueada" not in errs_sig.lower():
        fail(f"assinatura sem chave deveria bloquear: {r_sig2}")
    checks += 1
    ok("bloqueio por assinatura sem chave")

    # Linha repetida no mesmo lote (batch_sigs).
    r_batch = inserir_lancamentos_manual_lote_pg(
        **kwargs,
        linhas=[
            {
                "valor": 55.55,
                "descricao": f"{marker} BATCH",
                "plano_conta": "COMPRA MERCADORIA CN",
                "observacao": "batch a",
                "data_vencimento": "2026-09-01",
            },
            {
                "valor": 55.55,
                "descricao": f"{marker} BATCH",
                "plano_conta": "COMPRA MERCADORIA CN",
                "observacao": "batch b",
                "data_vencimento": "2026-09-01",
            },
        ],
    )
    if len(r_batch.get("ids") or []) != 1:
        fail(f"batch_sigs deveria criar 1 id: {r_batch}")
    errs_b = " ".join(
        str(e.get("erro") or "") for e in (r_batch.get("erros") or []) if isinstance(e, dict)
    )
    if "mesmo lote" not in errs_b.lower() and "duplicidade bloqueada" not in errs_b.lower():
        fail(f"batch_sigs sem erro de linha repetida: {r_batch}")
    checks += 1
    ok("batch_sigs no mesmo lote")

    blob, st = montar_zip_backup_completo_pg(somente_abertos=False, gerado_por="verify")
    if not st.get("ok") or len(blob) < 100:
        fail(f"backup pg falhou: {st}")
    blob2, st2 = montar_zip_backup_completo_pg(somente_abertos=True, gerado_por="verify")
    if not st2.get("ok") or len(blob2) < 50:
        fail(f"backup abertos falhou: {st2}")
    blob3, st3 = montar_zip_backup_lancamentos_dispatch(
        None, somente_abertos=False, gerado_por="verify"
    )
    if not st3.get("ok") or len(blob3) < 50:
        fail(f"dispatch backup falhou: {st3}")
    checks += 1
    ok("backup ZIP completo/abertos/dispatch")

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).order_by("id").first()
    if user is None:
        user = User.objects.filter(is_staff=True).order_by("id").first()
    if user is None:
        fail("sem usuário staff/superuser para Client")
    client = Client()
    client.force_login(user)
    cache.set(
        "agro_lancamentos_backup_ultimo",
        {"quando": "2026-08-05T12:00:00", "por": "verify", "somente_abertos": False},
        timeout=60,
    )
    resp = client.get(reverse("api_lancamentos_backup_ultimo"), HTTP_HOST="127.0.0.1")
    if resp.status_code != 200:
        fail(f"backup_ultimo HTTP {resp.status_code}")
    body = resp.json()
    if not body.get("ok") or not body.get("ultimo"):
        fail(f"backup_ultimo body: {body}")
    checks += 1
    ok("API backup_ultimo")

    TituloFinanceiroAgro.objects.filter(descricao__startswith="__VERIFY_DUP_").delete()
    print(f"VERIFY_OK anti-dup + backup CP ({checks}/11)")


if __name__ == "__main__":
    main()
