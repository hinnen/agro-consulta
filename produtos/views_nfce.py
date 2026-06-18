"""Views NFC-e — emissão PDV, cupom e exportação mensal de XML."""
from __future__ import annotations

import io
import json
import re
import zipfile
from datetime import date

from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404
from django.views.decorators.http import require_GET, require_POST

from produtos.models import NfceDocumentoAgro, VendaAgro
def _mongo_conn():
    from produtos.views import obter_conexao_mongo

    return obter_conexao_mongo()
from produtos.nfce_config_util import nfce_config_resumo, nfce_configurada, nfce_emissao_solicitada
from produtos.nfce_cupom_util import serializar_nfce_cupom_80mm
from produtos.nfce_sp_emissao_util import cpf_valido, emitir_nfce_para_venda


def _nfce_opts_payload(data: dict) -> tuple[str, bool]:
    cpf = re.sub(r"\D", "", str(data.get("nfce_cpf") or data.get("cliente_documento") or ""))[:11]
    sem_id = bool(data.get("nfce_sem_identificacao"))
    if cpf and cpf_valido(cpf):
        return cpf, False
    if sem_id:
        return "", True
    return "", False


def tentar_emitir_nfce_pos_venda(venda: VendaAgro | None, data: dict) -> dict | None:
    """Emite NFC-e após gravar venda, se módulo ativo e PDV solicitou (manual ou auto)."""
    if not venda or not nfce_configurada():
        return None
    if not nfce_emissao_solicitada(data):
        return None
    cpf, sem_id = _nfce_opts_payload(data)
    if not cpf and not sem_id:
        return {
            "ok": False,
            "erro": "NFC-e: informe CPF do consumidor ou confirme venda sem identificação.",
        }
    client, db = _mongo_conn()
    col_p = getattr(client, "col_p", None) if client else None
    return emitir_nfce_para_venda(
        venda,
        cpf_dest=cpf,
        sem_identificacao=sem_id,
        db=db,
        col_p=col_p,
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_nfce_status(request):
    return JsonResponse({"ok": True, "nfce": nfce_config_resumo()})


@login_required(login_url="/admin/login/")
@require_GET
def api_venda_agro_nfce_cupom(request, pk):
    v = get_object_or_404(VendaAgro.objects.prefetch_related("itens"), pk=pk)
    nfce = getattr(v, "nfce", None)
    if not nfce or nfce.status != NfceDocumentoAgro.Status.AUTORIZADA:
        return JsonResponse(
            {"ok": False, "erro": "Esta venda não possui NFC-e autorizada para imprimir."},
            status=400,
        )
    raw_sv = (request.GET.get("segunda_via") or "1").strip().lower()
    segunda_via = raw_sv not in ("0", "false", "no", "off")
    client, db = _mongo_conn()
    col_p = getattr(client, "col_p", None) if client else None
    return JsonResponse(
        {
            "ok": True,
            "cupom": serializar_nfce_cupom_80mm(
                v, nfce, segunda_via=segunda_via, db=db, col_p=col_p
            ),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_venda_agro_nfce_emitir(request, pk):
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        body = {}
    v = get_object_or_404(VendaAgro.objects.prefetch_related("itens"), pk=pk)
    cpf, sem_id = _nfce_opts_payload(body)
    if not cpf and not sem_id:
        return JsonResponse(
            {"ok": False, "erro": "Informe CPF válido ou marque venda sem identificação."},
            status=400,
        )
    client, db = _mongo_conn()
    col_p = getattr(client, "col_p", None) if client else None
    out = emitir_nfce_para_venda(
        v,
        cpf_dest=cpf,
        sem_identificacao=sem_id,
        db=db,
        col_p=col_p,
    )
    st = 200 if out.get("ok") else 502
    return JsonResponse({"ok": bool(out.get("ok")), "nfce": out}, status=st)


@login_required(login_url="/admin/login/")
@require_GET
def api_nfce_export_xml_zip(request):
    """ZIP com XMLs autorizados do mês (pasta para contabilidade)."""
    hoje = date.today()
    try:
        ano = int(request.GET.get("ano") or hoje.year)
        mes = int(request.GET.get("mes") or hoje.month)
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "erro": "Parâmetros ano/mês inválidos."}, status=400)
    if mes < 1 or mes > 12:
        return JsonResponse({"ok": False, "erro": "Mês deve ser 1–12."}, status=400)

    qs = (
        NfceDocumentoAgro.objects.filter(
            status=NfceDocumentoAgro.Status.AUTORIZADA,
            criado_em__year=ano,
            criado_em__month=mes,
        )
        .exclude(xml_autorizado="")
        .order_by("numero")
    )
    buf = io.BytesIO()
    count = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for doc in qs:
            chave = (doc.chave or f"venda{doc.venda_id}").strip()
            nome = f"NFC-e/{ano}-{mes:02d}/{chave}.xml"
            zf.writestr(nome, doc.xml_autorizado.encode("utf-8"))
            count += 1
    if count == 0:
        return JsonResponse(
            {"ok": False, "erro": f"Nenhum XML autorizado em {mes:02d}/{ano}."},
            status=404,
        )
    buf.seek(0)
    resp = HttpResponse(buf.getvalue(), content_type="application/zip")
    resp["Content-Disposition"] = f'attachment; filename="nfce-xml-{ano}-{mes:02d}.zip"'
    return resp
