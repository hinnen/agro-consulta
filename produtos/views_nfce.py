"""Views NFC-e — emissão PDV, cupom e exportação mensal de XML."""
from __future__ import annotations

import json
import logging
import re
from datetime import date

from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.contabilidade_acesso_util import (
    CONTABILIDADE_LOGIN_URL,
    contabilidade_login_required,
    usuario_pode_acessar_contabilidade,
    usuario_somente_contabilidade,
)
from produtos.models import NfceDocumentoAgro, VendaAgro
from produtos.nfce_config_util import nfce_config_resumo, nfce_configurada, nfce_emissao_solicitada
from produtos.nfce_contabilidade_util import (
    linhas_planilha_nfce_mes,
    montar_zip_nfce_mes,
    planilha_nfce_csv_bytes,
    planilha_nfce_xlsx_bytes,
    resumo_nfce_mes,
    urls_exportacao_mes,
)
from produtos.nfce_cupom_util import serializar_nfce_cupom_80mm
from produtos.nfce_sp_emissao_util import cancelar_nfce_autorizada, cpf_valido, emitir_nfce_para_venda
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


def _contabilidade_next_url(raw: str | None) -> str:
    n = (raw or "").strip()
    if not n.startswith("/") or n.startswith("//"):
        return reverse("contabilidade_painel")
    return n


@require_http_methods(["GET", "POST"])
def contabilidade_login(request):
    """Login escritório — não exige staff (Admin Django bloqueia contador)."""
    if request.user.is_authenticated and usuario_pode_acessar_contabilidade(request.user):
        return redirect(_contabilidade_next_url(request.GET.get("next")))

    erro = ""
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        password = request.POST.get("password") or ""
        user = authenticate(request, username=username, password=password)
        if user is None:
            erro = "Usuário ou senha incorretos."
        elif not usuario_pode_acessar_contabilidade(user):
            erro = "Este login não tem permissão para Contabilidade."
        else:
            login(request, user)
            return redirect(_contabilidade_next_url(request.POST.get("next") or request.GET.get("next")))

    return render(
        request,
        "produtos/contabilidade_login.html",
        {"erro": erro, "next": request.GET.get("next") or ""},
    )


@require_GET
def contabilidade_logout(request):
    logout(request)
    return redirect("contabilidade_login")


@contabilidade_login_required
@require_GET
def contabilidade_painel(request):
    """Painel contabilidade — NFC-e e atalhos de exportação ao escritório."""
    hoje = date.today()
    return render(
        request,
        "produtos/contabilidade_painel.html",
        {
            "nfce": nfce_config_resumo(),
            "export_xml_url": reverse("api_nfce_export_xml_zip"),
            "export_planilha_url": reverse("api_nfce_export_planilha"),
            "resumo_url": reverse("api_nfce_contabilidade_resumo"),
            "ano_default": hoje.year,
            "mes_default": hoje.month,
            "somente_contabilidade": usuario_somente_contabilidade(request.user),
            "mostrar_outros_exports": False,  # oculto por pedido Renan (só NFC-e na tela)
        },
    )


def _parse_ano_mes_request(request) -> tuple[int, int] | tuple[None, None]:
    hoje = date.today()
    try:
        ano = int(request.GET.get("ano") or hoje.year)
        mes = int(request.GET.get("mes") or hoje.month)
    except (TypeError, ValueError):
        return None, None
    if mes < 1 or mes > 12:
        return None, None
    return ano, mes


@contabilidade_login_required
@require_GET
def api_nfce_contabilidade_resumo(request):
    ano, mes = _parse_ano_mes_request(request)
    if ano is None:
        return JsonResponse({"ok": False, "erro": "Parâmetros ano/mês inválidos."}, status=400)
    data = resumo_nfce_mes(ano, mes)
    data["links"] = urls_exportacao_mes(ano, mes)
    return JsonResponse({"ok": True, "resumo": data})


@contabilidade_login_required
@require_GET
def api_nfce_export_planilha(request):
    ano, mes = _parse_ano_mes_request(request)
    if ano is None:
        return JsonResponse({"ok": False, "erro": "Parâmetros ano/mês inválidos."}, status=400)
    fmt = (request.GET.get("formato") or "csv").strip().lower()
    linhas = linhas_planilha_nfce_mes(ano, mes)
    if not linhas:
        return JsonResponse(
            {"ok": False, "erro": f"Nenhuma NFC-e em {mes:02d}/{ano}."},
            status=404,
        )
    if fmt == "xlsx":
        blob = planilha_nfce_xlsx_bytes(ano, mes)
        resp = HttpResponse(
            blob,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        resp["Content-Disposition"] = f'attachment; filename="nfce-planilha-{ano}-{mes:02d}.xlsx"'
        return resp
    blob = planilha_nfce_csv_bytes(ano, mes)
    resp = HttpResponse(blob, content_type="text/csv; charset=utf-8")
    resp["Content-Disposition"] = f'attachment; filename="nfce-planilha-{ano}-{mes:02d}.csv"'
    return resp


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
@require_POST
def api_venda_agro_nfce_cancelar(request, pk):
    v = get_object_or_404(VendaAgro.objects.select_related("nfce"), pk=pk)
    nfce = getattr(v, "nfce", None)
    if not nfce or nfce.status != NfceDocumentoAgro.Status.AUTORIZADA:
        return JsonResponse(
            {"ok": False, "erro": "Esta venda não tem NFC-e autorizada para cancelar."},
            status=400,
        )
    if not nfce_configurada():
        return JsonResponse(
            {"ok": False, "erro": "NFC-e não configurada no servidor (.env)."},
            status=503,
        )
    out = cancelar_nfce_autorizada(nfce)
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


@contabilidade_login_required
@require_GET
def api_nfce_export_xml_zip(request):
    """ZIP mensal: index.csv + XMLs autorizadas/canceladas."""
    ano, mes = _parse_ano_mes_request(request)
    if ano is None:
        return JsonResponse({"ok": False, "erro": "Parâmetros ano/mês inválidos."}, status=400)

    blob, count_xml = montar_zip_nfce_mes(ano, mes)
    if not blob:
        return JsonResponse(
            {"ok": False, "erro": f"Nenhuma NFC-e em {mes:02d}/{ano}."},
            status=404,
        )
    resp = HttpResponse(blob, content_type="application/zip")
    resp["Content-Disposition"] = f'attachment; filename="nfce-xml-{ano}-{mes:02d}.zip"'
    resp["X-Nfce-Xml-Count"] = str(count_xml)
    return resp
