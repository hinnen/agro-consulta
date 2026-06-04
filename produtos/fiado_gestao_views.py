"""Views e APIs — gestão crédito loja / fiado."""

from __future__ import annotations

import json
import tempfile
from decimal import Decimal
from pathlib import Path

from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST

from produtos.caixa_util import obter_sessao_caixa_aberta_request, parse_valor_moeda_br
from produtos.fiado_credito_util import resumo_credito_fiado_cliente
from produtos.fiado_gestao_util import (
    _usuario_de_request,
    baixar_cliente_fiado,
    baixar_titulo,
    baixar_titulos_selecionados,
    definir_limite_fiado_cliente,
    editar_titulo_fiado,
    export_backup_fiado,
    listar_clientes_fiado,
    listar_titulos,
    resumo_from_clientes_fiado,
    resumo_gestao_fiado,
    titulo_para_dict,
)
from produtos.models import ClienteAgro, FiadoTituloAgro
from produtos.views import obter_conexao_mongo


@login_required(login_url="/admin/login/")
def fiado_gestao(request):
    sessao = obter_sessao_caixa_aberta_request(request)
    resumo = resumo_gestao_fiado()
    pk_pre = (request.GET.get("cliente") or request.GET.get("cliente_agro_pk") or "").strip()
    cliente_pre_pk = None
    if pk_pre.isdigit():
        cliente_pre_pk = int(pk_pre)
    return render(
        request,
        "produtos/fiado_gestao.html",
        {
            "resumo": resumo,
            "caixa_aberto": bool(sessao),
            "sessao_caixa_id": sessao.pk if sessao else None,
            "from_pdv": (request.GET.get("from") or "").strip() == "pdv",
            "cliente_pre_pk": cliente_pre_pk,
            "ledger_vazio": (resumo.get("titulos_abertos") or 0) == 0,
            "pode_importar": bool(getattr(request.user, "is_staff", False)),
        },
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_fiado_resumo(request):
    return JsonResponse({"ok": True, **resumo_gestao_fiado()})


@login_required(login_url="/admin/login/")
@require_GET
def api_fiado_clientes(request):
    busca = (request.GET.get("q") or request.GET.get("busca") or "").strip()
    apenas = (request.GET.get("apenas_saldo") or "1").strip() != "0"
    clientes = listar_clientes_fiado(busca=busca, apenas_com_saldo=apenas)
    return JsonResponse(
        {
            "ok": True,
            "clientes": clientes,
            "resumo": resumo_from_clientes_fiado(clientes) if apenas else resumo_gestao_fiado(),
        }
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_fiado_titulos(request):
    pk_raw = request.GET.get("cliente_agro_pk")
    cliente_pk = None
    if pk_raw:
        try:
            cliente_pk = int(pk_raw)
        except (TypeError, ValueError):
            cliente_pk = None
    cliente_nome = (request.GET.get("cliente_nome") or "").strip()
    cliente_codigo = (request.GET.get("cliente_codigo") or "").strip()
    situacao = (request.GET.get("situacao") or "abertos").strip()
    busca = (request.GET.get("q") or "").strip()
    try:
        limit = int(request.GET.get("limit") or 200)
    except (TypeError, ValueError):
        limit = 200
    titulos = listar_titulos(
        cliente_agro_pk=cliente_pk,
        cliente_nome=cliente_nome,
        cliente_codigo=cliente_codigo,
        situacao=situacao,
        busca=busca,
        limit=limit,
    )
    return JsonResponse({"ok": True, "titulos": titulos})


@login_required(login_url="/admin/login/")
@require_GET
def api_fiado_cliente_credito(request):
    pk_raw = request.GET.get("cliente_agro_pk")
    if not pk_raw:
        return JsonResponse({"ok": False, "erro": "Informe cliente_agro_pk."}, status=400)
    try:
        pk = int(pk_raw)
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "erro": "cliente_agro_pk inválido."}, status=400)
    cli = ClienteAgro.objects.filter(pk=pk).first()
    if not cli:
        return JsonResponse({"ok": False, "erro": "Cliente não encontrado."}, status=404)
    cid = (cli.externo_id or "").strip() or f"agro:{cli.pk}"
    client_m, db = obter_conexao_mongo()
    cred = resumo_credito_fiado_cliente(cid, cliente_agro_pk=cli.pk, db=db, client_m=client_m)
    cred["cliente_nome"] = cli.nome
    cred["limite_fiado_local"] = float(cli.limite_fiado_local or 0)
    return JsonResponse({"ok": True, **cred})


@login_required(login_url="/admin/login/")
@require_GET
def api_fiado_buscar_cliente(request):
    q = (request.GET.get("q") or "").strip()
    if len(q) < 2:
        return JsonResponse({"ok": True, "clientes": []})
    qs = ClienteAgro.objects.filter(ativo=True, nome__icontains=q).order_by("nome")[:15]
    out = []
    for c in qs:
        out.append(
            {
                "pk": c.pk,
                "nome": c.nome,
                "externo_id": c.externo_id,
                "limite_fiado_local": float(c.limite_fiado_local or 0),
            }
        )
    return JsonResponse({"ok": True, "clientes": out})


@login_required(login_url="/admin/login/")
@require_POST
def api_fiado_baixa(request):
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    try:
        titulo_id = int(data.get("titulo_id"))
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "erro": "titulo_id inválido."}, status=400)
    valor = parse_valor_moeda_br(data.get("valor"))
    if valor is None or valor <= 0:
        return JsonResponse({"ok": False, "erro": "Informe um valor maior que zero."}, status=400)
    forma = str(data.get("forma_pagamento") or data.get("forma") or "Dinheiro").strip()
    obs = str(data.get("observacao") or "").strip()
    registrar_caixa = data.get("registrar_caixa", True) is not False
    sessao = obter_sessao_caixa_aberta_request(request)
    if registrar_caixa and not sessao:
        registrar_caixa = False
    try:
        baixa = baixar_titulo(
            titulo_id,
            valor,
            forma,
            request=request,
            observacao=obs,
            registrar_caixa=registrar_caixa,
            usuario=_usuario_de_request(request),
        )
        titulo = FiadoTituloAgro.objects.get(pk=titulo_id)
        return JsonResponse(
            {
                "ok": True,
                "baixa_id": baixa.pk,
                "titulo": titulo_para_dict(titulo),
                "resumo": resumo_gestao_fiado(),
            }
        )
    except FiadoTituloAgro.DoesNotExist:
        return JsonResponse({"ok": False, "erro": "Título não encontrado."}, status=404)
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)


@login_required(login_url="/admin/login/")
@require_POST
def api_fiado_limite(request):
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    try:
        pk = int(data.get("cliente_agro_pk"))
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "erro": "cliente_agro_pk inválido."}, status=400)
    limite = parse_valor_moeda_br(data.get("limite") or data.get("limite_fiado_local"))
    if limite is None:
        try:
            limite = Decimal(str(data.get("limite") or "0").replace(",", "."))
        except Exception:
            return JsonResponse({"ok": False, "erro": "Limite inválido."}, status=400)
    try:
        cli = definir_limite_fiado_cliente(
            pk,
            limite,
            usuario=_usuario_de_request(request),
        )
        client_m, db = obter_conexao_mongo()
        cid = (cli.externo_id or "").strip() or f"agro:{cli.pk}"
        cred = resumo_credito_fiado_cliente(cid, cliente_agro_pk=cli.pk, db=db, client_m=client_m)
        return JsonResponse(
            {
                "ok": True,
                "cliente_agro_pk": cli.pk,
                "limite_fiado_local": float(cli.limite_fiado_local or 0),
                "credito": cred,
            }
        )
    except ClienteAgro.DoesNotExist:
        return JsonResponse({"ok": False, "erro": "Cliente não encontrado."}, status=404)
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)


@login_required(login_url="/admin/login/")
@require_POST
def api_fiado_baixa_cliente(request):
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    valor = parse_valor_moeda_br(data.get("valor"))
    if valor is None or valor <= 0:
        return JsonResponse({"ok": False, "erro": "Informe um valor maior que zero."}, status=400)
    forma = str(data.get("forma_pagamento") or data.get("forma") or "Dinheiro").strip()
    obs = str(data.get("observacao") or "").strip()
    pk_raw = data.get("cliente_agro_pk")
    cliente_pk = None
    if pk_raw is not None and str(pk_raw).strip() != "":
        try:
            cliente_pk = int(pk_raw)
        except (TypeError, ValueError):
            cliente_pk = None
    cliente_nome = str(data.get("cliente_nome") or "").strip()
    cliente_codigo = str(data.get("cliente_codigo") or "").strip()
    registrar_caixa = data.get("registrar_caixa", True) is not False
    sessao = obter_sessao_caixa_aberta_request(request)
    if registrar_caixa and not sessao:
        registrar_caixa = False
    try:
        r = baixar_cliente_fiado(
            valor,
            forma,
            cliente_agro_pk=cliente_pk,
            cliente_nome=cliente_nome,
            cliente_codigo=cliente_codigo,
            request=request,
            observacao=obs,
            registrar_caixa=registrar_caixa,
            usuario=_usuario_de_request(request),
        )
        return JsonResponse({"ok": True, **r, "resumo": resumo_gestao_fiado()})
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)


@login_required(login_url="/admin/login/")
@require_POST
def api_fiado_titulo_editar(request):
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    try:
        titulo_id = int(data.get("titulo_id"))
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "erro": "titulo_id inválido."}, status=400)
    try:
        titulo = editar_titulo_fiado(
            titulo_id,
            vencimento=data.get("vencimento"),
            valor_bruto=parse_valor_moeda_br(data.get("valor_bruto"))
            if data.get("valor_bruto") is not None
            else None,
            numero_documento=data.get("numero_documento"),
            descricao=data.get("descricao"),
            usuario=_usuario_de_request(request),
        )
        return JsonResponse(
            {
                "ok": True,
                "titulo": titulo_para_dict(titulo),
                "resumo": resumo_gestao_fiado(),
            }
        )
    except FiadoTituloAgro.DoesNotExist:
        return JsonResponse({"ok": False, "erro": "Título não encontrado."}, status=404)
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)


@login_required(login_url="/admin/login/")
@require_POST
def api_fiado_baixa_selecionados(request):
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    raw_ids = data.get("titulo_ids") or data.get("titulos") or []
    if not isinstance(raw_ids, list):
        return JsonResponse({"ok": False, "erro": "Informe titulo_ids como lista."}, status=400)
    valor_raw = data.get("valor")
    valor = parse_valor_moeda_br(valor_raw) if valor_raw not in (None, "") else None
    forma = str(data.get("forma_pagamento") or data.get("forma") or "Dinheiro").strip()
    obs = str(data.get("observacao") or "").strip()
    registrar_caixa = data.get("registrar_caixa", True) is not False
    sessao = obter_sessao_caixa_aberta_request(request)
    if registrar_caixa and not sessao:
        registrar_caixa = False
    try:
        r = baixar_titulos_selecionados(
            raw_ids,
            forma,
            valor=valor,
            request=request,
            observacao=obs,
            registrar_caixa=registrar_caixa,
            usuario=_usuario_de_request(request),
        )
        return JsonResponse({"ok": True, **r, "resumo": resumo_gestao_fiado()})
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)


@login_required(login_url="/admin/login/")
@require_POST
def api_fiado_importar_planilha(request):
    if not getattr(request.user, "is_staff", False):
        return JsonResponse({"ok": False, "erro": "Sem permissão para importar."}, status=403)
    upload = request.FILES.get("arquivo")
    if not upload:
        return JsonResponse({"ok": False, "erro": "Envie um arquivo CSV ou XLSX."}, status=400)
    nome = (upload.name or "").lower()
    if not nome.endswith((".csv", ".xlsx", ".xls")):
        return JsonResponse({"ok": False, "erro": "Use arquivo .csv ou .xlsx."}, status=400)
    suf = Path(nome).suffix or ".xlsx"
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suf) as tmp:
            for chunk in upload.chunks():
                tmp.write(chunk)
            tmp_path = Path(tmp.name)
        from produtos.fiado_import_util import aplicar_importacao_fiados

        r = aplicar_importacao_fiados(
            tmp_path,
            usuario=_usuario_de_request(request),
        )
        return JsonResponse({"ok": True, **r, "resumo": resumo_gestao_fiado()})
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    except Exception as exc:
        return JsonResponse({"ok": False, "erro": str(exc) or "Erro ao importar."}, status=500)
    finally:
        try:
            if "tmp_path" in locals() and tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


@login_required(login_url="/admin/login/")
@require_GET
def api_fiado_backup_export(request):
    payload = export_backup_fiado()
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    resp = HttpResponse(body, content_type="application/json; charset=utf-8")
    resp["Content-Disposition"] = 'attachment; filename="fiado_backup_agro.json"'
    return resp
