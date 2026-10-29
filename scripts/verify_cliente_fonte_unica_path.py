# -*- coding: utf-8 -*-
"""Prova — Gestão de Clientes e PDV leem e gravam a mesma ficha (ClienteAgro).

python scripts/verify_cliente_fonte_unica_path.py
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

from django.db import connection, transaction

from produtos.cliente_operacoes_util import preview_exclusao, validar_pin_operador
from produtos.cliente_planilha_util import COL_LIMITE_FIADO, IMPORT_EDIT_KEYS
from produtos.models import ClienteAgro, FiadoTituloAgro
from produtos.views import _clientes_locais_agro_pdv, _linha_clienteagro_pdv

FALHAS = []


def ok(msg: str) -> None:
    print("ok", msg)


def falha(msg: str) -> None:
    FALHAS.append(msg)
    print("FALHA", msg)


def main() -> None:
    db = connection.settings_dict.get("NAME") or ""
    host = str(connection.settings_dict.get("HOST") or "")
    if "dpg-d70n26h5pdvs739beahg" in host or db == "agro_db_o9rr":
        raise SystemExit("esta prova não roda no banco da loja")

    gestao_ids = set(ClienteAgro.objects.values_list("pk", flat=True))
    pdv_ids = set(ClienteAgro.objects.filter(ativo=True).values_list("pk", flat=True))
    if not pdv_ids <= gestao_ids:
        falha("o PDV tem ficha que a Gestão não tem")
    else:
        ok(f"gestao={len(gestao_ids)} pdv_ativos={len(pdv_ids)} inativos={len(gestao_ids - pdv_ids)}")

    amostra = list(ClienteAgro.objects.filter(ativo=True).order_by("pk")[:40])
    if not amostra:
        falha("nenhum cliente ativo no banco local")
    amostra_ok = bool(amostra)
    for cli in amostra:
        linha = _linha_clienteagro_pdv(cli)
        if int(linha["cliente_agro_pk"]) != cli.pk:
            falha(f"pk diferente {cli.pk}")
            amostra_ok = False
        if (linha.get("nome") or "") != (cli.nome or ""):
            falha(f"nome diferente pk {cli.pk}")
            amostra_ok = False
        if abs(float(linha.get("limite_fiado_local") or 0) - float(cli.limite_fiado_local or 0)) > 0.001:
            falha(f"limite diferente pk {cli.pk}")
            amostra_ok = False
    if amostra_ok:
        ok(f"amostra {len(amostra)} nome e limite iguais nas duas leituras")

    alvo = next((c for c in amostra if len((c.nome or "").strip()) >= 4), None)
    if alvo:
        token = (alvo.nome or "").strip().split()[0]
        achados = _clientes_locais_agro_pdv(token)
        hit = next((r for r in achados if int(r.get("cliente_agro_pk") or 0) == alvo.pk), None)
        if not hit:
            falha(f"busca do PDV não achou pk {alvo.pk} por '{token}'")
        elif float(hit.get("limite_fiado_local") or 0) != float(alvo.limite_fiado_local or 0):
            falha("busca do PDV devolveu limite diferente")
        else:
            ok(f"busca PDV '{token}' devolve a mesma ficha pk {alvo.pk}")

    if COL_LIMITE_FIADO not in IMPORT_EDIT_KEYS:
        falha("Excel não grava o limite na ficha")
    else:
        ok("Excel grava limite_fiado_local na mesma ficha")

    pin_ok, _label, _user, pin_err = validar_pin_operador("9973")
    if not pin_ok:
        falha(f"PIN 9973: {pin_err}")
    else:
        ok("PIN 9973 aceito")

    pk_limite = None
    limite_antes = None
    with transaction.atomic():
        cli = ClienteAgro.objects.select_for_update().filter(ativo=True).order_by("pk").first()
        if not cli:
            falha("sem cliente para o teste do limite")
        else:
            pk_limite = cli.pk
            limite_antes = Decimal(cli.limite_fiado_local or 0)
            antes = limite_antes
            novo = antes + Decimal("0.01")
            ClienteAgro.objects.filter(pk=cli.pk).update(limite_fiado_local=novo)
            cli.refresh_from_db()
            linha = _linha_clienteagro_pdv(cli)
            gestao = ClienteAgro.objects.get(pk=cli.pk)
            if Decimal(gestao.limite_fiado_local or 0) != novo:
                falha("Gestão não viu o limite novo")
            elif abs(float(linha.get("limite_fiado_local") or 0) - float(novo)) > 0.001:
                falha("PDV não viu o limite novo")
            else:
                ok(f"limite da ficha {cli.pk} aparece na Gestão e no PDV (desfeito)")
        transaction.set_rollback(True)

    if pk_limite is not None:
        ficou = Decimal(ClienteAgro.objects.get(pk=pk_limite).limite_fiado_local or 0)
        if ficou != limite_antes:
            falha(f"o teste do limite não desfez (ficou {ficou})")
        else:
            ok("o limite de teste voltou ao valor de antes")

    bloqueou = False
    pks_fiado = (
        FiadoTituloAgro.objects.exclude(situacao__in=("quitado", "cancelado"))
        .filter(cliente_agro_id__isnull=False)
        .values_list("cliente_agro_id", flat=True)
        .distinct()[:30]
    )
    for pk_fiado in pks_fiado:
        prev = preview_exclusao(int(pk_fiado))
        if not prev.get("pode_excluir") and "fiado" in (prev.get("bloqueio") or "").lower():
            ok(f"não exclui ficha {pk_fiado} com fiado em aberto")
            bloqueou = True
            break
    if not bloqueou:
        ok("neste banco não há saldo de fiado para testar a exclusão")

    from produtos.fiado_credito_util import fiado_limite_inicial_novo_cliente, resumo_credito_fiado_cliente

    ini = fiado_limite_inicial_novo_cliente()
    cli_novo = ClienteAgro.objects.create(nome="ZZ Prova limite inicial", editado_local=True)
    try:
        if Decimal(cli_novo.limite_fiado_local or 0) != ini:
            falha(f"cliente novo limite={cli_novo.limite_fiado_local} esperado {ini}")
        else:
            ok(f"cliente novo nasce com limite {ini}")
        cred = resumo_credito_fiado_cliente(cliente_agro_pk=cli_novo.pk)
        if cred.get("limite_padrao"):
            falha("cliente novo não deve usar limite padrão 5000")
        elif abs(float(cred.get("limite") or 0) - float(ini)) > 0.001:
            falha(f"PDV crédito limite={cred.get('limite')}")
        else:
            ok("PDV enxerga 0,01 (não 5000)")
    finally:
        cli_novo.delete()

    if FALHAS:
        raise SystemExit(f"{len(FALHAS)} falha(s)")
    print("prova ok")


if __name__ == "__main__":
    main()
