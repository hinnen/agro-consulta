"""Views NFC-e — emissão PDV, cupom e exportação mensal de XML."""
from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from datetime import date

from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.views.decorators.http import require_GET, require_POST

from produtos.models import NfceDocumentoAgro, VendaAgro
from produtos.nfce_config_util import nfce_config_resumo, nfce_configurada, nfce_emissao_solicitada
from produtos.nfce_cupom_util import serializar_nfce_cupom_80mm
from produtos.nfce_sp_emissao_util import cpf_valido, emitir_nfce_para_venda
from produtos.nfce_venda_util import painel_nfce_venda, registrar_nfce_erro_venda

logger = logging.getLogger(__name__)


def _mongo_conn():
    from produtos.views import obter_conexao_mongo

    return obter_conexao_mongo()

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
    if not venda:
        return None
    if not nfce_emissao_solicitada(data):
        return None
    cfg = nfce_config_resumo()
    tp_amb = int(cfg.get("tp_amb") or 2)
    if not nfce_configurada():
        doc = registrar_nfce_erro_venda(
            venda,
            "NFC-e não configurada no servidor (.env).",
            tp_amb=tp_amb,
        )
        return {
            "ok": False,
            "erro": "NFC-e não configurada no servidor (.env).",
            "documento_id": doc.pk,
        }
    cpf, sem_id = _nfce_opts_payload(data)
    if not cpf and not sem_id:
        doc = registrar_nfce_erro_venda(
            venda,
            "NFC-e: informe CPF do consumidor ou confirme venda sem identificação.",
            tp_amb=tp_amb,
        )
        return {
            "ok": False,
            "erro": "NFC-e: informe CPF do consumidor ou confirme venda sem identificação.",
            "documento_id": doc.pk,
        }
    if cpf and not cpf_valido(cpf):
        doc = registrar_nfce_erro_venda(
            venda,
            "CPF informado é inválido.",
            cpf_dest=cpf,
            tp_amb=tp_amb,
        )
        return {"ok": False, "erro": "CPF informado é inválido.", "documento_id": doc.pk}
    client, db = _mongo_conn()
    col_p = getattr(client, "col_p", None) if client else None
    return emitir_nfce_para_venda(
        venda,
        cpf_dest=cpf,
        sem_identificacao=sem_id,
        db=db,
        col_p=col_p,
    )


def anexar_nfce_resposta_venda(venda: VendaAgro | None, data: dict, payload: dict) -> dict:
    """Tenta emitir NFC-e após venda; nunca interrompe o fluxo da venda."""
    if not venda:
        return payload
    try:
        nfce = tentar_emitir_nfce_pos_venda(venda, data)
        if nfce is not None:
            payload["nfce"] = nfce
    except Exception:
        logger.exception("NFC-e pós-venda falhou (venda %s)", venda.pk)
        if nfce_emissao_solicitada(data):
            try:
                cfg = nfce_config_resumo()
                doc = registrar_nfce_erro_venda(
                    venda,
                    "Erro interno ao emitir NFC-e. Tente reemitir em Consultar vendas.",
                    tp_amb=int(cfg.get("tp_amb") or 2),
                )
                payload["nfce"] = {
                    "ok": False,
                    "erro": "Erro interno ao emitir NFC-e.",
                    "documento_id": doc.pk,
                }
            except Exception:
                logger.exception("NFC-e: falha ao registrar erro interno (venda %s)", venda.pk)
    return payload


@login_required(login_url="/admin/login/")
@require_GET
def contabilidade_painel(request):
    """Painel contabilidade — exportação mensal de XML NFC-e."""
    return render(
        request,
        "produtos/contabilidade_painel.html",
        {
            "nfce": nfce_config_resumo(),
            "export_xml_url": reverse("api_nfce_export_xml_zip"),
        },
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_venda_agro_nfce_info(request, pk):
    v = get_object_or_404(VendaAgro.objects.select_related("nfce"), pk=pk)
    return JsonResponse({"ok": True, "nfce_painel": painel_nfce_venda(v)})


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
    if v.devolvida_em:
        return JsonResponse({"ok": False, "erro": "Venda devolvida — não é possível emitir NFC-e."}, status=400)
    if not nfce_configurada():
        return JsonResponse(
            {"ok": False, "erro": "NFC-e não configurada no servidor (.env)."},
            status=503,
        )
    cpf, sem_id = _nfce_opts_payload(body)
    if not cpf and not sem_id:
        return JsonResponse(
            {"ok": False, "erro": "Informe CPF válido ou marque venda sem identificação."},
            status=400,
        )
    v.nfce_solicitada = True
    v.save(update_fields=["nfce_solicitada"])
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
    return JsonResponse(
        {
            "ok": bool(out.get("ok")),
            "nfce": out,
            "nfce_painel": painel_nfce_venda(
                VendaAgro.objects.select_related("nfce").get(pk=v.pk)
            ),
        },
        status=st,
    )


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
