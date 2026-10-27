"""Views e APIs — NF-e saída modelo 55."""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from functools import wraps

from decouple import config
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from produtos.caixa_util import validar_pin_operador
from produtos.nfe_saida_emissao_util import emitir_nfe_saida_documento, montar_html_danfe_previa
from produtos.nfe_saida_util import (
    NFE_SAIDA_STATUS_AUTORIZADA,
    NFE_SAIDA_STATUS_CANCELADA,
    NFE_SAIDA_STATUS_DESCARTADA,
    NFE_SAIDA_STATUS_ENVIANDO,
    NFE_SAIDA_STATUS_REJEITADA,
    autorizada_por_venda,
    checklist_conferencia,
    criar_nfe_saida_vazia,
    criar_ou_reabrir_de_venda,
    listar_nfe_saida,
    nfe_saida_atualizar_campos,
    nfe_saida_persistencia_ok,
    nfe_saida_tp_amb_default,
    obter_documento_nfe_saida,
    obter_nfe_saida_bruto,
    painel_nfe_saida_venda,
    salvar_documento_nfe_saida,
    cliente_agro_para_destinatario,
    persistir_fiscal_destinatario_no_cliente,
    salvar_fiscal_cliente_agro,
)
from produtos.sefaz_dfe_client import distribuicao_dfe_configurada

logger = logging.getLogger(__name__)


def _nfe_saida_staff(user) -> bool:
    return bool(user and user.is_authenticated and user.is_staff)


def nfe_saida_staff_required(view_func):
    """NF-e 55 — fase inicial restrita a administradores (is_staff)."""

    @login_required(login_url="/entrar/")
    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        if not _nfe_saida_staff(request.user):
            if request.path.startswith("/api/"):
                return JsonResponse(
                    {"ok": False, "erro": "NF-e 55 restrita a administradores."},
                    status=403,
                )
            return HttpResponse(
                "NF-e saída (55) restrita a administradores.",
                status=403,
                content_type="text/plain; charset=utf-8",
            )
        return view_func(request, *args, **kwargs)

    return _wrapped


def _mongo_db():
    from produtos.views import obter_conexao_mongo

    _client, db = obter_conexao_mongo()
    return db


def _usuario(request) -> str:
    if not request.user.is_authenticated:
        return ""
    return (
        getattr(request.user, "email", None) or request.user.get_username() or str(request.user.pk)
    )[:200]


def _mascarar_cnpj(cnpj: str) -> str:
    d = re.sub(r"\D", "", cnpj or "")[:14]
    if len(d) != 14:
        return ""
    return f"**.***.***/****-{d[-2:]}"


@nfe_saida_staff_required
@require_GET
def nfe_saida_view(request):
    venda_q = (request.GET.get("venda") or "").strip()
    venda_id = int(venda_q) if venda_q.isdigit() else None
    doc_id = (request.GET.get("doc") or "").strip()
    tp_amb = nfe_saida_tp_amb_default()
    return render(
        request,
        "produtos/nfe_saida.html",
        {
            "nfe_saida_venda_id": venda_id,
            "nfe_saida_doc_id": doc_id,
            "nfe_homolog": tp_amb == 2,
        },
    )


@nfe_saida_staff_required
@require_GET
def api_nfe_saida_status_cert(request):
    from produtos.nfe_saida_util import snapshot_emitente_loja

    cnpj = re.sub(r"\D", "", config("NFE_DIST_DFE_CNPJ", default="") or "")[:14]
    emitentes = [snapshot_emitente_loja("centro"), snapshot_emitente_loja("vila")]
    return JsonResponse(
        {
            "configurada": distribuicao_dfe_configurada(),
            "uf": (config("NFE_DIST_DFE_UF", default="SP") or "SP").strip().upper()[:2],
            "cnpj_mascarado": _mascarar_cnpj(cnpj),
            "tp_amb": nfe_saida_tp_amb_default(),
            "mod_ui": "55",
            "emitentes": [e for e in emitentes if e],
        }
    )


@nfe_saida_staff_required
@require_GET
def api_nfe_saida_listar(request):
    db = _mongo_db()
    if not nfe_saida_persistencia_ok(db):
        return JsonResponse(
            {"ok": False, "erro": "Persistência NF-e saída indisponível.", "itens": []}, status=503
        )
    try:
        lim = min(int(request.GET.get("limit") or 40), 100)
    except ValueError:
        lim = 40
    filtro = (request.GET.get("filtro") or "em_aberto").strip()[:24]
    busca = (request.GET.get("q") or "").strip()
    return JsonResponse({"ok": True, "itens": listar_nfe_saida(db, limit=lim, filtro=filtro, busca=busca)})


@nfe_saida_staff_required
@require_GET
def api_nfe_saida_obter(request, doc_id: str):
    db = _mongo_db()
    doc = obter_documento_nfe_saida(db, doc_id)
    if not doc:
        return JsonResponse({"ok": False, "erro": "Documento não encontrado."}, status=404)
    cert_ok = distribuicao_dfe_configurada()
    doc["checklist"] = checklist_conferencia(doc, cert_ok=cert_ok)
    if doc.get("venda_agro_id"):
        from produtos.nfce_venda_util import painel_nfce_venda
        from produtos.models import VendaAgro

        try:
            v = VendaAgro.objects.select_related("nfce").get(pk=int(doc["venda_agro_id"]))
            doc["nfce_venda"] = painel_nfce_venda(v)
        except VendaAgro.DoesNotExist:
            doc["nfce_venda"] = None
    return JsonResponse({"ok": True, "doc": doc})


@nfe_saida_staff_required
@require_POST
def api_nfe_saida_salvar(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    db = _mongo_db()
    doc_id = str(payload.get("id") or "").strip() or None
    body = payload.get("doc") if isinstance(payload.get("doc"), dict) else payload
    r = salvar_documento_nfe_saida(db, doc_id, body, usuario=_usuario(request))
    if r.get("ok"):
        dest = body.get("destinatario") if isinstance(body.get("destinatario"), dict) else {}
        try:
            persistir_fiscal_destinatario_no_cliente(dest)
        except Exception:
            logger.exception("fiscal_nfe salvar rascunho")
    st = 200 if r.get("ok") else 400
    return JsonResponse(r, status=st)


@nfe_saida_staff_required
@require_POST
def api_nfe_saida_nova(request):
    db = _mongo_db()
    r = criar_nfe_saida_vazia(db, usuario=_usuario(request))
    st = 200 if r.get("ok") else 503
    return JsonResponse(r, status=st)


@nfe_saida_staff_required
@require_POST
def api_nfe_saida_de_venda(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    vid = payload.get("venda_id")
    if vid is None:
        return JsonResponse({"ok": False, "erro": "Informe venda_id."}, status=400)
    db = _mongo_db()
    r = criar_ou_reabrir_de_venda(db, int(vid), usuario=_usuario(request))
    st = 200 if r.get("ok") else 400
    return JsonResponse(r, status=st)


@nfe_saida_staff_required
@require_GET
def api_nfe_saida_painel_venda(request, venda_id: int):
    db = _mongo_db()
    return JsonResponse({"ok": True, "painel": painel_nfe_saida_venda(venda_id, db=db)})


@nfe_saida_staff_required
@require_GET
def api_nfe_saida_buscar_vendas(request):
    from decimal import Decimal

    from django.db.models import Q

    from produtos.models import VendaAgro

    q = (request.GET.get("q") or "").strip()
    lim = min(int(request.GET.get("limit") or 15), 30)
    qs = VendaAgro.objects.filter(devolvida_em__isnull=True).order_by("-pk")
    if q:
        if q.isdigit():
            qs = qs.filter(pk=int(q))
        else:
            qs = qs.filter(Q(cliente_nome__icontains=q) | Q(cliente_documento__icontains=q))
    out = []
    db = _mongo_db()
    for v in qs[:lim]:
        painel = painel_nfe_saida_venda(v.pk, db=db)
        out.append(
            {
                "id": v.pk,
                "cliente": v.cliente_nome,
                "documento": v.cliente_documento,
                "total": float((v.total or Decimal("0")).quantize(Decimal("0.01"))),
                "criado_em": v.criado_em.isoformat() if v.criado_em else "",
                "painel": painel,
            }
        )
    return JsonResponse({"ok": True, "itens": out})


@nfe_saida_staff_required
def api_nfe_saida_cliente_fiscal(request):
    from produtos.models import ClienteAgro

    if request.method == "GET":
        q = (request.GET.get("cliente_agro_id") or "").strip()
        if not q.isdigit():
            return JsonResponse({"ok": False, "erro": "cliente_agro_id obrigatório."}, status=400)
        try:
            c = ClienteAgro.objects.get(pk=int(q))
        except ClienteAgro.DoesNotExist:
            return JsonResponse({"ok": False, "erro": "Cliente não encontrado."}, status=404)
        return JsonResponse({"ok": True, "destinatario": cliente_agro_para_destinatario(c)})

    if request.method != "POST":
        return JsonResponse({"ok": False, "erro": "Método não permitido."}, status=405)
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    cid = payload.get("cliente_agro_id")
    if not cid:
        return JsonResponse({"ok": False, "erro": "cliente_agro_id obrigatório."}, status=400)
    fiscal = payload.get("destinatario") if isinstance(payload.get("destinatario"), dict) else payload
    r = salvar_fiscal_cliente_agro(int(cid), fiscal)
    st = 200 if r.get("ok") else 400
    return JsonResponse(r, status=st)


@nfe_saida_staff_required
@require_POST
def api_nfe_saida_emitir(request, doc_id: str):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        payload = {}
    ok_pin, err_pin = validar_pin_operador(str(payload.get("pin") or ""))
    if not ok_pin:
        return JsonResponse({"ok": False, "erro": err_pin}, status=403)

    db = _mongo_db()
    if not nfe_saida_persistencia_ok(db):
        return JsonResponse({"ok": False, "erro": "Persistência NF-e saída indisponível."}, status=503)

    doc_id_norm = str(doc_id).strip()
    doc = obter_nfe_saida_bruto(db, doc_id_norm)
    if not doc:
        return JsonResponse({"ok": False, "erro": "Documento não encontrado."}, status=404)

    patch_dest = payload.get("destinatario")
    if isinstance(patch_dest, dict):
        from produtos.nfe_saida_email_util import mesclar_destinatario_emit

        doc = mesclar_destinatario_emit(doc, patch_dest)
        nfe_saida_atualizar_campos(db, doc_id_norm, {"destinatario": doc.get("destinatario")})
        try:
            persistir_fiscal_destinatario_no_cliente(doc.get("destinatario") or {})
        except Exception:
            logger.exception("fiscal_nfe salvar emitir")
    if str(doc.get("status") or "") == NFE_SAIDA_STATUS_AUTORIZADA:
        return JsonResponse({"ok": True, "ja_autorizada": True, "chave": doc.get("chave")})

    vid = doc.get("venda_agro_id")
    if vid:
        aut = autorizada_por_venda(db, int(vid))
        if aut and aut.get("id") != doc_id_norm:
            return JsonResponse(
                {
                    "ok": False,
                    "erro": "Já existe NF-e autorizada para esta venda.",
                    "doc_id": aut.get("id"),
                },
                status=409,
            )

    from produtos.nfe_saida_regras_validas_util import resolver_cnpj_serie_emitente
    from produtos.nfe_saida_tributacao_util import erros_fiscal_emissao
    from produtos.nfe_saida_util import (
        erros_cfop_coerente_uf,
        total_itens,
        uf_emitente_do_doc,
        validar_destinatario,
        validar_itens,
        validar_pagamentos,
    )

    cnpj_env = re.sub(r"\D", "", config("NFE_DIST_DFE_CNPJ", default="") or "")[:14]
    cnpj, _serie, err_em = resolver_cnpj_serie_emitente(doc, cnpj_pedido=cnpj_env)
    erros_pre = (
        validar_destinatario(doc.get("destinatario") or {})
        + validar_itens(doc.get("itens") or [])
        + validar_pagamentos(doc.get("pagamento") or [], total_itens(doc.get("itens") or []))
        + ([err_em] if err_em else [])
        + erros_cfop_coerente_uf(doc)
        + erros_fiscal_emissao(doc, uf_emit=uf_emitente_do_doc(doc), cnpj_emit=cnpj or cnpj_env)
    )
    if erros_pre:
        return JsonResponse(
            {
                "ok": False,
                "status": str(doc.get("status") or "rascunho"),
                "erro": erros_pre[0],
                "erros": erros_pre[:12],
            },
            status=400,
        )

    nfe_saida_atualizar_campos(
        db,
        doc_id_norm,
        {"status": NFE_SAIDA_STATUS_ENVIANDO, "updated_at": datetime.now(timezone.utc)},
    )

    out = emitir_nfe_saida_documento(doc, cnpj_emit=cnpj or cnpj_env)
    upd = {
        "status": out.get("status") or NFE_SAIDA_STATUS_REJEITADA,
        "c_stat": out.get("c_stat"),
        "x_motivo": out.get("x_motivo") or out.get("erro") or "",
        "updated_at": datetime.now(timezone.utc),
        "pin_ok_em": datetime.now(timezone.utc),
    }
    if out.get("ok"):
        upd["chave"] = out.get("chave") or ""
        upd["protocolo"] = out.get("protocolo") or ""
        upd["nNF"] = out.get("nNF")
        upd["serie"] = out.get("serie")
        upd["xml_autorizado"] = out.get("xml_autorizado") or ""
        upd["simulado"] = bool(out.get("simulado"))
    nfe_saida_atualizar_campos(db, doc_id_norm, upd)

    email_envio: dict | None = None
    if out.get("ok"):
        from produtos.nfe_saida_email_util import enviar_nfe_saida_por_email

        doc_email = dict(doc)
        doc_email.update({k: v for k, v in upd.items() if k != "updated_at"})
        site_base = request.build_absolute_uri("/").rstrip("/")
        email_envio = enviar_nfe_saida_por_email(doc_email, doc_id_norm, site_base=site_base)
        meta_email: dict = {}
        if email_envio.get("enviado"):
            meta_email = {
                "email_enviado_em": email_envio.get("enviado_em"),
                "email_enviado_para": email_envio.get("email"),
                "email_envio_erro": "",
            }
        elif email_envio.get("erro"):
            meta_email = {"email_envio_erro": str(email_envio.get("erro") or "")[:500]}
        if meta_email:
            nfe_saida_atualizar_campos(db, doc_id_norm, meta_email)

    refreshed = obter_documento_nfe_saida(db, doc_id_norm)
    body = {
        "ok": bool(out.get("ok")),
        "doc": refreshed,
        **{k: out[k] for k in ("erro", "simulado", "chave") if k in out},
    }
    if email_envio is not None:
        body["email_envio"] = email_envio
    st = 200 if out.get("ok") else 400
    return JsonResponse(body, status=st)


@nfe_saida_staff_required
@require_GET
def api_nfe_saida_danfe(request, doc_id: str):
    db = _mongo_db()
    doc = obter_documento_nfe_saida(db, doc_id)
    if not doc:
        return HttpResponse("Não encontrado", status=404)
    from produtos.nfe_saida_danfe_html import carregar_emitente_danfe, montar_html_danfe

    em = carregar_emitente_danfe(doc=doc)
    html = montar_html_danfe(doc, emitente=em)
    return HttpResponse(html, content_type="text/html; charset=utf-8")


@nfe_saida_staff_required
@require_GET
def api_nfe_saida_xml(request, doc_id: str):
    db = _mongo_db()
    if not nfe_saida_persistencia_ok(db):
        return HttpResponse("Indisponível", status=503)
    doc = obter_nfe_saida_bruto(db, str(doc_id).strip())
    if not doc:
        return HttpResponse("Não encontrado", status=404)
    xml = str(doc.get("xml_autorizado") or "").strip()
    if not xml:
        return HttpResponse("XML ainda não disponível (nota não autorizada).", status=404)
    return HttpResponse(xml, content_type="application/xml; charset=utf-8")


@nfe_saida_staff_required
@require_POST
def api_nfe_saida_descartar(request, doc_id: str):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        payload = {}
    ok_pin, err_pin = validar_pin_operador(str(payload.get("pin") or ""))
    if not ok_pin:
        return JsonResponse({"ok": False, "erro": err_pin}, status=403)
    db = _mongo_db()
    if not nfe_saida_persistencia_ok(db):
        return JsonResponse({"ok": False, "erro": "Persistência NF-e saída indisponível."}, status=503)
    doc_id_norm = str(doc_id).strip()
    doc = obter_nfe_saida_bruto(db, doc_id_norm)
    if not doc:
        return JsonResponse({"ok": False, "erro": "Não encontrado."}, status=404)
    if str(doc.get("status") or "") == NFE_SAIDA_STATUS_AUTORIZADA:
        return JsonResponse({"ok": False, "erro": "Nota autorizada não pode ser descartada."}, status=400)
    nfe_saida_atualizar_campos(
        db,
        doc_id_norm,
        {"status": NFE_SAIDA_STATUS_DESCARTADA, "updated_at": datetime.now(timezone.utc)},
    )
    return JsonResponse({"ok": True})


def _pin_ou_erro(payload: dict):
    ok_pin, err_pin = validar_pin_operador(str((payload or {}).get("pin") or ""))
    if not ok_pin:
        return JsonResponse({"ok": False, "erro": err_pin}, status=403)
    return None


@nfe_saida_staff_required
@require_POST
def api_nfe_saida_cancelar(request, doc_id: str):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        payload = {}
    err = _pin_ou_erro(payload)
    if err:
        return err
    db = _mongo_db()
    if not nfe_saida_persistencia_ok(db):
        return JsonResponse({"ok": False, "erro": "Persistência NF-e saída indisponível."}, status=503)
    doc_id_norm = str(doc_id).strip()
    doc = obter_nfe_saida_bruto(db, doc_id_norm)
    if not doc:
        return JsonResponse({"ok": False, "erro": "Não encontrado."}, status=404)
    from produtos.nfe_saida_evento_util import (
        aplicar_evento_no_doc,
        preparar_cancelamento,
        transmitir_evento,
        TP_CANCEL,
    )
    from produtos.nfe_saida_regras_validas_util import resolver_cnpj_serie_emitente

    prep = preparar_cancelamento(
        doc,
        motivo=str(payload.get("motivo") or payload.get("x_just") or ""),
        confirmar_fora_prazo=bool(payload.get("confirmar_fora_prazo")),
    )
    if not prep.get("ok"):
        return JsonResponse(prep, status=400)
    if doc.get("evento_em_curso"):
        return JsonResponse({"ok": False, "erro": "Evento já em transmissão."}, status=409)
    nfe_saida_atualizar_campos(db, doc_id_norm, {"evento_em_curso": True})
    cnpj_env = re.sub(r"\D", "", config("NFE_DIST_DFE_CNPJ", default="") or "")[:14]
    cnpj, _s, err_em = resolver_cnpj_serie_emitente(doc, cnpj_pedido=cnpj_env)
    if err_em:
        nfe_saida_atualizar_campos(db, doc_id_norm, {"evento_em_curso": False})
        return JsonResponse({"ok": False, "erro": err_em}, status=400)
    try:
        out = transmitir_evento(
            doc,
            cnpj_emit=cnpj or cnpj_env,
            tp_evento=TP_CANCEL,
            n_seq=1,
            x_just=prep["x_just"],
            usuario=_usuario(request),
        )
    except Exception:
        logger.exception("nfe_saida cancelar")
        nfe_saida_atualizar_campos(db, doc_id_norm, {"evento_em_curso": False})
        return JsonResponse({"ok": False, "erro": "Falha interna no cancelamento."}, status=500)
    if not out.get("ok"):
        nfe_saida_atualizar_campos(db, doc_id_norm, {"evento_em_curso": False})
        return JsonResponse(out, status=400)
    novo = aplicar_evento_no_doc(doc, out["evento"])
    nfe_saida_atualizar_campos(
        db,
        doc_id_norm,
        {
            "status": novo.get("status"),
            "eventos": novo.get("eventos"),
            "evento_em_curso": False,
            "protocolo_cancelamento": novo.get("protocolo_cancelamento") or "",
            "cancelada_em": novo.get("cancelada_em"),
            "cancelada_por": novo.get("cancelada_por") or "",
            "cancelada_motivo": novo.get("cancelada_motivo") or "",
            "updated_at": datetime.now(timezone.utc),
        },
    )
    refreshed = obter_documento_nfe_saida(db, doc_id_norm)
    return JsonResponse({"ok": True, "doc": refreshed, "c_stat": out.get("c_stat")})


@nfe_saida_staff_required
@require_POST
def api_nfe_saida_cce(request, doc_id: str):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        payload = {}
    err = _pin_ou_erro(payload)
    if err:
        return err
    db = _mongo_db()
    if not nfe_saida_persistencia_ok(db):
        return JsonResponse({"ok": False, "erro": "Persistência NF-e saída indisponível."}, status=503)
    doc_id_norm = str(doc_id).strip()
    doc = obter_nfe_saida_bruto(db, doc_id_norm)
    if not doc:
        return JsonResponse({"ok": False, "erro": "Não encontrado."}, status=404)
    from produtos.nfe_saida_evento_util import (
        aplicar_evento_no_doc,
        preparar_cce,
        transmitir_evento,
        TP_CCE,
    )
    from produtos.nfe_saida_regras_validas_util import resolver_cnpj_serie_emitente

    prep = preparar_cce(doc, texto=str(payload.get("texto") or payload.get("x_correcao") or ""))
    if not prep.get("ok"):
        return JsonResponse(prep, status=400)
    nfe_saida_atualizar_campos(db, doc_id_norm, {"evento_em_curso": True})
    cnpj_env = re.sub(r"\D", "", config("NFE_DIST_DFE_CNPJ", default="") or "")[:14]
    cnpj, _s, err_em = resolver_cnpj_serie_emitente(doc, cnpj_pedido=cnpj_env)
    if err_em:
        nfe_saida_atualizar_campos(db, doc_id_norm, {"evento_em_curso": False})
        return JsonResponse({"ok": False, "erro": err_em}, status=400)
    try:
        out = transmitir_evento(
            doc,
            cnpj_emit=cnpj or cnpj_env,
            tp_evento=TP_CCE,
            n_seq=int(prep["n_seq"]),
            x_correcao=prep["x_correcao"],
            usuario=_usuario(request),
        )
    except Exception:
        logger.exception("nfe_saida cce")
        nfe_saida_atualizar_campos(db, doc_id_norm, {"evento_em_curso": False})
        return JsonResponse({"ok": False, "erro": "Falha interna na CC-e."}, status=500)
    if not out.get("ok"):
        nfe_saida_atualizar_campos(db, doc_id_norm, {"evento_em_curso": False})
        return JsonResponse(out, status=400)
    novo = aplicar_evento_no_doc(doc, out["evento"])
    nfe_saida_atualizar_campos(
        db,
        doc_id_norm,
        {
            "eventos": novo.get("eventos"),
            "evento_em_curso": False,
            "updated_at": datetime.now(timezone.utc),
        },
    )
    refreshed = obter_documento_nfe_saida(db, doc_id_norm)
    return JsonResponse({"ok": True, "doc": refreshed, "c_stat": out.get("c_stat"), "aviso": prep.get("aviso")})


@nfe_saida_staff_required
@require_GET
def api_nfe_saida_evento_xml(request, doc_id: str):
    from produtos.nfe_saida_evento_util import eventos_doc

    db = _mongo_db()
    doc = obter_nfe_saida_bruto(db, str(doc_id).strip())
    if not doc:
        return HttpResponse("Não encontrado", status=404)
    n_seq = (request.GET.get("n_seq") or "1").strip()
    tipo = (request.GET.get("tipo") or "").strip().lower()
    tp = "110111" if tipo in ("cancel", "cancelamento", "110111") else "110110"
    for e in eventos_doc(doc):
        if str(e.get("n_seq")) == n_seq and str(e.get("tp_evento")) == tp and e.get("xml"):
            return HttpResponse(e["xml"], content_type="application/xml; charset=utf-8")
    return HttpResponse("Evento não encontrado.", status=404)


@nfe_saida_staff_required
@require_GET
def api_nfe_saida_evento_comprovante(request, doc_id: str):
    from produtos.nfe_saida_evento_util import eventos_doc, montar_html_comprovante_evento

    db = _mongo_db()
    doc = obter_nfe_saida_bruto(db, str(doc_id).strip())
    if not doc:
        return HttpResponse("Não encontrado", status=404)
    n_seq = (request.GET.get("n_seq") or "1").strip()
    tipo = (request.GET.get("tipo") or "cancel").strip().lower()
    tp = "110111" if tipo in ("cancel", "cancelamento", "110111") else "110110"
    for e in eventos_doc(doc):
        if str(e.get("n_seq")) == n_seq and str(e.get("tp_evento")) == tp:
            return HttpResponse(montar_html_comprovante_evento(doc, e), content_type="text/html; charset=utf-8")
    return HttpResponse("Evento não encontrado.", status=404)
