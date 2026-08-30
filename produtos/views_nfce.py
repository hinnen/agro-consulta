"""Views NFC-e — emissão PDV, cupom e exportação mensal de XML."""
from __future__ import annotations

import copy
import json
import logging
import re
import threading
import time
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
from produtos.nfce_config_util import (
    nfce_config_resumo,
    nfce_configurada,
    nfce_emissao_automatica,
    nfce_emissao_solicitada,
    nfce_loja_de_venda,
    nfce_venda_tem_forma_pagamento_auto,
)
from produtos.nfce_contabilidade_util import (
    linhas_planilha_nfce_mes,
    montar_zip_nfce_mes,
    normalizar_loja_filtro,
    pendencias_nfce_csv_bytes,
    pendencias_nfce_resumo_json,
    planilha_nfce_csv_bytes,
    planilha_nfce_xlsx_bytes,
    resumo_nfce_mes,
    rotulo_loja_filtro,
    urls_exportacao_mes,
)
from produtos.nfce_cupom_util import serializar_nfce_cupom_80mm
from produtos.nfce_sp_emissao_util import (
    cancelar_nfce_autorizada,
    documento_dest_nfce,
    emitir_nfce_para_venda,
    mensagem_doc_dest_invalido,
)
from produtos.nfce_venda_util import painel_nfce_venda, registrar_nfce_erro_venda
from produtos.sefaz_soap_util import sefaz_erro_transiente

logger = logging.getLogger(__name__)

_ERRO_NFCE_CFG = "NFC-e não configurada no servidor (.env)."
_NFCE_RETRY_DELAYS_S = (2.0, 5.0, 10.0)


def _mongo_conn():
    from produtos.views import obter_conexao_mongo_pdv

    return obter_conexao_mongo_pdv()

def _nfce_opts_payload(data: dict) -> tuple[str, bool]:
    raw = str(data.get("nfce_cpf") or data.get("cliente_documento") or "")
    doc = documento_dest_nfce(raw)
    sem_id = bool(data.get("nfce_sem_identificacao"))
    if doc:
        return doc, False
    if sem_id:
        return "", True
    if data.get("nfce_sincrona") or data.get("nfce_escolha_explicita"):
        return "", False
    if nfce_emissao_automatica() or nfce_venda_tem_forma_pagamento_auto(data):
        return "", True
    return "", False


def _marcar_nfce_solicitada(venda: VendaAgro) -> None:
    if not getattr(venda, "nfce_solicitada", False):
        venda.nfce_solicitada = True
        venda.save(update_fields=["nfce_solicitada"])


def _nfce_ja_autorizada(venda: VendaAgro) -> bool:
    return NfceDocumentoAgro.objects.filter(
        venda=venda,
        status=NfceDocumentoAgro.Status.AUTORIZADA,
    ).exists()


def _emitir_nfce_pos_venda_sync(
    venda: VendaAgro,
    data: dict,
    *,
    sefaz_perfil: str = "sync",
) -> dict | None:
    """Tenta emitir NFC-e na thread da requisição."""
    loja = nfce_loja_de_venda(venda)
    cfg = nfce_config_resumo(loja)
    tp_amb = int(cfg.get("tp_amb") or 2)
    if not nfce_configurada(warmup=True, tentativas=3, loja=loja):
        return None
    cpf, sem_id = _nfce_opts_payload(data)
    digits_in = re.sub(r"\D", "", str(data.get("nfce_cpf") or data.get("cliente_documento") or ""))
    if digits_in and not cpf and not sem_id:
        err_doc = mensagem_doc_dest_invalido(digits_in)
        doc = registrar_nfce_erro_venda(
            venda,
            err_doc,
            cpf_dest=digits_in[:14],
            tp_amb=tp_amb,
        )
        return {"ok": False, "erro": err_doc, "documento_id": doc.pk}
    if not cpf and not sem_id:
        doc = registrar_nfce_erro_venda(
            venda,
            "NFC-e: informe CPF ou CNPJ do consumidor ou confirme venda sem identificação.",
            tp_amb=tp_amb,
        )
        return {
            "ok": False,
            "erro": "NFC-e: informe CPF ou CNPJ do consumidor ou confirme venda sem identificação.",
            "documento_id": doc.pk,
        }
    client, db = _mongo_conn()
    col_p = getattr(client, "col_p", None) if client else None
    return emitir_nfce_para_venda(
        venda,
        cpf_dest=cpf,
        sem_identificacao=sem_id,
        db=db,
        col_p=col_p,
        sefaz_perfil=sefaz_perfil,
    )


def _nfce_pos_venda_background_worker(venda_id: int, data: dict) -> None:
    """Retry NFC-e após cold start / certificado ainda não pronto no Render."""
    from django.db import connections

    payload = copy.deepcopy(data)
    connections.close_all()
    try:
        for wait_s in _NFCE_RETRY_DELAYS_S:
            time.sleep(wait_s)
            connections.close_all()
            try:
                venda = VendaAgro.objects.get(pk=venda_id)
            except VendaAgro.DoesNotExist:
                logger.error("NFC-e retry: venda %s não encontrada.", venda_id)
                return
            if _nfce_ja_autorizada(venda):
                logger.info("NFC-e retry: venda %s já autorizada.", venda_id)
                return
            out = _emitir_nfce_pos_venda_sync(venda, payload, sefaz_perfil="completo")
            if out and out.get("ok"):
                logger.info("NFC-e retry OK venda %s (após %.0fs)", venda_id, wait_s)
                return
            if out is None:
                continue
            erro = str(out.get("erro") or "")
            if sefaz_erro_transiente(erro):
                logger.warning(
                    "NFC-e retry rede venda %s (após %.0fs): %s",
                    venda_id,
                    wait_s,
                    erro[:160],
                )
                continue
            if erro != _ERRO_NFCE_CFG and "não configurada" not in erro.lower():
                return
        connections.close_all()
        try:
            venda = VendaAgro.objects.get(pk=venda_id)
        except VendaAgro.DoesNotExist:
            return
        if _nfce_ja_autorizada(venda):
            return
        cfg = nfce_config_resumo(nfce_loja_de_venda(venda))
        tp_amb = int(cfg.get("tp_amb") or 2)
        if not nfce_configurada(warmup=True, tentativas=3, loja=nfce_loja_de_venda(venda)):
            registrar_nfce_erro_venda(venda, _ERRO_NFCE_CFG, tp_amb=tp_amb)
            logger.warning(
                "NFC-e retry esgotado — config indisponível (venda %s).",
                venda_id,
            )
            return
        out = _emitir_nfce_pos_venda_sync(venda, payload, sefaz_perfil="completo")
        if out and not out.get("ok"):
            logger.warning(
                "NFC-e retry esgotado venda %s — %s",
                venda_id,
                (out.get("erro") or "")[:300],
            )
    except Exception:
        logger.exception("NFC-e retry background falhou (venda %s)", venda_id)
    finally:
        connections.close_all()


def _disparar_nfce_pos_venda_background(venda_id: int, data: dict) -> None:
    threading.Thread(
        target=_nfce_pos_venda_background_worker,
        args=(venda_id, data),
        daemon=True,
        name=f"nfce-venda-{venda_id}",
    ).start()


def tentar_emitir_nfce_pos_venda(venda: VendaAgro | None, data: dict) -> dict | None:
    """Emite NFC-e após gravar venda, se módulo ativo e PDV solicitou (manual ou auto)."""
    if not venda:
        return None
    if not nfce_emissao_solicitada(data):
        return None
    _marcar_nfce_solicitada(venda)
    from django.conf import settings

    sincrona = bool(data.get("nfce_sincrona"))
    assincrona = getattr(settings, "AGRO_PDV_NFCE_ASSINCRONA", True)

    if not sincrona and assincrona:
        cfg = nfce_config_resumo()
        tp_amb = int(cfg.get("tp_amb") or 2)
        _disparar_nfce_pos_venda_background(venda.pk, data)
        return {
            "ok": False,
            "erro": "Cupom fiscal em processamento. Se não sair em instantes, reemita em Consultar vendas.",
            "pendente_retry": True,
            "tp_amb": tp_amb,
        }
    out = _emitir_nfce_pos_venda_sync(venda, data)
    if out is not None:
        return out
    cfg = nfce_config_resumo()
    tp_amb = int(cfg.get("tp_amb") or 2)
    _disparar_nfce_pos_venda_background(venda.pk, data)
    return {
        "ok": False,
        "erro": "Cupom fiscal em processamento. Se não sair em instantes, reemita em Consultar vendas.",
        "pendente_retry": True,
        "tp_amb": tp_amb,
    }


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
            "export_pendencias_url": reverse("api_nfce_export_pendencias"),
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


def _parse_loja_request(request) -> str:
    return normalizar_loja_filtro(request.GET.get("loja"))


@contabilidade_login_required
@require_GET
def api_nfce_contabilidade_resumo(request):
    ano, mes = _parse_ano_mes_request(request)
    if ano is None:
        return JsonResponse({"ok": False, "erro": "Parâmetros ano/mês inválidos."}, status=400)
    loja = _parse_loja_request(request)
    data = resumo_nfce_mes(ano, mes, loja)
    data["links"] = urls_exportacao_mes(ano, mes)
    data["pendencias"] = pendencias_nfce_resumo_json(ano, mes, loja=loja)
    return JsonResponse({"ok": True, "resumo": data})


@contabilidade_login_required
@require_GET
def api_nfce_export_pendencias(request):
    ano, mes = _parse_ano_mes_request(request)
    if ano is None:
        return JsonResponse({"ok": False, "erro": "Parâmetros ano/mês inválidos."}, status=400)
    loja = _parse_loja_request(request)
    if not pendencias_nfce_resumo_json(ano, mes, loja=loja)["total"]:
        return JsonResponse(
            {
                "ok": False,
                "erro": f"Nenhuma pendência em {mes:02d}/{ano} ({rotulo_loja_filtro(loja)}).",
            },
            status=404,
        )
    blob = pendencias_nfce_csv_bytes(ano, mes, loja)
    suf = "" if loja == "todas" else f"-{loja}"
    resp = HttpResponse(blob, content_type="text/csv; charset=utf-8")
    resp["Content-Disposition"] = (
        f'attachment; filename="nfce-pendencias{suf}-{ano}-{mes:02d}.csv"'
    )
    return resp


@contabilidade_login_required
@require_GET
def api_nfce_export_planilha(request):
    ano, mes = _parse_ano_mes_request(request)
    if ano is None:
        return JsonResponse({"ok": False, "erro": "Parâmetros ano/mês inválidos."}, status=400)
    loja = _parse_loja_request(request)
    fmt = (request.GET.get("formato") or "csv").strip().lower()
    linhas = linhas_planilha_nfce_mes(ano, mes, loja)
    if not linhas:
        return JsonResponse(
            {
                "ok": False,
                "erro": f"Nenhuma NFC-e em {mes:02d}/{ano} ({rotulo_loja_filtro(loja)}).",
            },
            status=404,
        )
    suf = "" if loja == "todas" else f"-{loja}"
    if fmt == "xlsx":
        blob = planilha_nfce_xlsx_bytes(ano, mes, loja)
        resp = HttpResponse(
            blob,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        resp["Content-Disposition"] = (
            f'attachment; filename="nfce-planilha{suf}-{ano}-{mes:02d}.xlsx"'
        )
        return resp
    blob = planilha_nfce_csv_bytes(ano, mes, loja)
    resp = HttpResponse(blob, content_type="text/csv; charset=utf-8")
    resp["Content-Disposition"] = f'attachment; filename="nfce-planilha{suf}-{ano}-{mes:02d}.csv"'
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
    """Reemitir NFC-e — carimba na hora; trabalho pesado com teto; lock anti-duplo."""
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        body = {}
    v = get_object_or_404(VendaAgro.objects.prefetch_related("itens"), pk=pk)
    if v.devolvida_em:
        return JsonResponse({"ok": False, "erro": "Venda devolvida — não é possível emitir NFC-e."}, status=400)
    from django.core.cache import cache
    from django.utils import timezone

    from produtos.nfce_venda_util import painel_nfce_venda

    cpf, sem_id = _nfce_opts_payload(body)
    digits_in = re.sub(r"\D", "", str(body.get("nfce_cpf") or body.get("cliente_documento") or ""))
    if digits_in and not cpf and not sem_id:
        return JsonResponse(
            {"ok": False, "erro": mensagem_doc_dest_invalido(digits_in)},
            status=400,
        )
    if not cpf and not sem_id:
        return JsonResponse(
            {"ok": False, "erro": "Informe CPF ou CNPJ válido ou marque venda sem identificação."},
            status=400,
        )

    # Carimbo ANTES do lock Redis — se Redis travar, no F5 ainda aparece «Tentativa».
    agora = timezone.localtime(timezone.now()).strftime("%H:%M:%S")
    v.nfce_solicitada = True
    v.save(update_fields=["nfce_solicitada"])
    NfceDocumentoAgro.objects.filter(venda=v).exclude(
        status=NfceDocumentoAgro.Status.AUTORIZADA
    ).update(
        mensagem_sefaz=f"Tentativa {agora} — enviando à SEFAZ…",
        status=NfceDocumentoAgro.Status.ERRO,
    )

    lock_key = f"nfce_emit_lock_{int(pk)}"
    try:
        got_lock = cache.add(lock_key, "1", timeout=120)
    except Exception:
        logger.exception("NFC-e lock cache falhou venda %s — segue sem lock", pk)
        got_lock = True
    if not got_lock:
        return JsonResponse(
            {
                "ok": False,
                "processando": True,
                "erro": "Cupom fiscal já está sendo emitido nesta venda — aguarde até 1 minuto e atualize (F5).",
                "nfce_painel": painel_nfce_venda(v),
            },
            status=409,
        )
    try:
        return _api_venda_agro_nfce_emitir_locked(request, v, body, agora=agora, cpf=cpf, sem_id=sem_id)
    finally:
        try:
            cache.delete(lock_key)
        except Exception:
            logger.exception("NFC-e lock cache delete falhou venda %s", pk)


def _api_venda_agro_nfce_emitir_locked(
    request,
    v: VendaAgro,
    body: dict,
    *,
    agora: str,
    cpf: str,
    sem_id: bool,
):
    """Trabalho pesado com teto 18s. Carimbo já foi gravado na view pública."""
    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as FuturesTimeout

    from django.db import connections

    from produtos.nfce_venda_util import registrar_nfce_erro_venda

    def _trabalho() -> dict:
        connections.close_all()
        try:
            venda = VendaAgro.objects.prefetch_related("itens").get(pk=v.pk)
            if not nfce_configurada(warmup=True, tentativas=2, loja=nfce_loja_de_venda(venda)):
                return {"ok": False, "erro": _ERRO_NFCE_CFG}
            client, db = _mongo_conn()
            col_p = getattr(client, "col_p", None) if client else None
            return emitir_nfce_para_venda(
                venda,
                cpf_dest=cpf,
                sem_identificacao=sem_id,
                db=db,
                col_p=col_p,
                sefaz_perfil="sync",
            )
        finally:
            connections.close_all()

    out: dict
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        fut = pool.submit(_trabalho)
        try:
            out = fut.result(timeout=18)
            if not isinstance(out, dict):
                out = {"ok": False, "erro": "Resposta inválida na emissão."}
            if out.get("erro") == _ERRO_NFCE_CFG and not out.get("ok"):
                cfg = nfce_config_resumo(nfce_loja_de_venda(v))
                doc = registrar_nfce_erro_venda(
                    v,
                    _ERRO_NFCE_CFG,
                    cpf_dest=cpf,
                    sem_identificacao=sem_id,
                    tp_amb=int(cfg.get("tp_amb") or 2),
                )
                out = {"ok": False, "erro": _ERRO_NFCE_CFG, "documento_id": doc.pk}
        except FuturesTimeout:
            logger.warning("NFC-e reemitir timeout 18s venda %s", v.pk)
            cfg = nfce_config_resumo(nfce_loja_de_venda(v))
            doc = registrar_nfce_erro_venda(
                v,
                f"Timeout {agora}: SEFAZ/servidor não respondeu em 18s. Tente de novo em 1 minuto.",
                cpf_dest=cpf,
                sem_identificacao=sem_id,
                tp_amb=int(cfg.get("tp_amb") or 2),
            )
            out = {
                "ok": False,
                "erro": "SEFAZ/servidor não respondeu em 18s. Aguarde 1 minuto e reemitir.",
                "documento_id": doc.pk,
            }
    except Exception:
        logger.exception("NFC-e reemitir falhou (venda %s)", v.pk)
        cfg = nfce_config_resumo(nfce_loja_de_venda(v))
        doc = registrar_nfce_erro_venda(
            v,
            "Erro interno ao emitir NFC-e. Tente reemitir em instantes.",
            cpf_dest=cpf,
            sem_identificacao=sem_id,
            tp_amb=int(cfg.get("tp_amb") or 2),
        )
        out = {
            "ok": False,
            "erro": "Erro interno ao emitir NFC-e. Tente reemitir em instantes.",
            "documento_id": doc.pk,
        }
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    st = 200 if out.get("ok") else 502
    return _nfce_emitir_json_response(v, out, st)


def _nfce_emitir_json_response(v: VendaAgro, out: dict, st: int):
    from produtos.nfce_venda_util import painel_nfce_venda

    v_fresh = VendaAgro.objects.select_related("nfce").get(pk=v.pk)
    payload = {
        "ok": bool(out.get("ok")),
        "nfce": out,
        "nfce_painel": painel_nfce_venda(v_fresh),
    }
    if out.get("erro") and not out.get("ok"):
        payload["erro"] = out.get("erro")
    return JsonResponse(payload, status=st)


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
    if not nfce_configurada(warmup=True, tentativas=3, loja=nfce_loja_de_venda(v)):
        return JsonResponse(
            {"ok": False, "erro": _ERRO_NFCE_CFG},
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
    """ZIP mensal: index.csv + XMLs autorizadas/canceladas (por loja)."""
    ano, mes = _parse_ano_mes_request(request)
    if ano is None:
        return JsonResponse({"ok": False, "erro": "Parâmetros ano/mês inválidos."}, status=400)

    loja = _parse_loja_request(request)
    blob, count_xml = montar_zip_nfce_mes(ano, mes, loja)
    if not blob:
        return JsonResponse(
            {
                "ok": False,
                "erro": f"Nenhuma NFC-e em {mes:02d}/{ano} ({rotulo_loja_filtro(loja)}).",
            },
            status=404,
        )
    suf = "" if loja == "todas" else f"-{loja}"
    resp = HttpResponse(blob, content_type="application/zip")
    resp["Content-Disposition"] = f'attachment; filename="nfce-xml{suf}-{ano}-{mes:02d}.zip"'
    resp["X-Nfce-Xml-Count"] = str(count_xml)
    return resp
