"""Repasse Vila → Centro — tela + APIs."""
from __future__ import annotations

import json
import logging
from datetime import date
from decimal import Decimal

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.caixa_util import (
    obter_caixa_vila_aberto,
    obter_sessao_caixa_aberta_request,
    operador_label_de_pin,
    usuario_django_de_pin,
)
from produtos.repasse_vila_util import (
    calcular_disponivel,
    confirmar_repasse,
    historico_mes,
    listar_acumulado_detalhe,
    listar_log_reserva,
    listar_planos_repasse_config,
    nomes_planos_desconto_centro,
    obter_config,
    quitar_acumulado_zerar,
    registrar_ajuste_acumulado,
    reserva_vila_desde_config,
    salvar_percentual_padrao,
    salvar_planos_desconto_centro,
    salvar_reserva_vila,
    serializar_repasse,
    validar_data_ref_repasse,
)

logger = logging.getLogger(__name__)


def _payload(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _parse_bool(v, default=False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "sim", "yes", "on"):
        return True
    if s in ("0", "false", "nao", "não", "no", "off"):
        return False
    return default


def _parse_date(raw) -> date | None:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        return None


@login_required(login_url="/admin/login/")
def repasse_vila_view(request):
    cfg = obter_config()
    hoje = timezone.localdate()
    calc = calcular_disponivel(hoje, _skip_acumulado=True)
    hist = historico_mes(hoje.year, hoje.month)
    url_pdv = reverse("pdv_home") + "?repasse=1"
    return render(
        request,
        "produtos/repasse_vila.html",
        {
            "percentual_padrao": cfg.percentual_lucro_padrao,
            "reserva_padrao": cfg.reserva_vila,
            "reserva_desde": reserva_vila_desde_config(cfg),
            "planos_repasse": listar_planos_repasse_config(cfg),
            "calc": calc,
            "hist": hist,
            "url_pdv_repasse": url_pdv,
            "caixa_vila_aberto": bool(obter_caixa_vila_aberto()),
        },
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_repasse_vila_calc(request):
    dia_raw = _parse_date(request.GET.get("data"))
    dia, err = validar_data_ref_repasse(dia_raw)
    if err or dia is None:
        return JsonResponse({"ok": False, "erro": err or "Data inválida"}, status=400)
    pct = request.GET.get("pct")
    modo = _parse_bool(request.GET.get("dia_cheio"), False)
    try:
        pct_v = Decimal(str(pct).replace(",", ".")) if pct not in (None, "") else None
    except Exception:
        pct_v = None
    out = calcular_disponivel(dia, percentual_lucro=pct_v, modo_dia_cheio=modo)
    out["ok"] = True
    return JsonResponse(out)


@login_required(login_url="/admin/login/")
@require_GET
def api_repasse_vila_historico(request):
    hoje = timezone.localdate()
    try:
        ano = int(request.GET.get("ano") or hoje.year)
        mes = int(request.GET.get("mes") or hoje.month)
    except Exception:
        ano, mes = hoje.year, hoje.month
    return JsonResponse(historico_mes(ano, mes))


@login_required(login_url="/admin/login/")
@require_GET
def api_repasse_vila_acumulado(request):
    dia_raw = _parse_date(request.GET.get("data"))
    dia, err = validar_data_ref_repasse(dia_raw or timezone.localdate())
    if err or dia is None:
        return JsonResponse({"ok": False, "erro": err or "Data inválida"}, status=400)
    return JsonResponse(listar_acumulado_detalhe(dia))


@login_required(login_url="/admin/login/")
@require_POST
def api_repasse_vila_acumulado_ajuste(request):
    payload = _payload(request)
    if payload is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    pin = str(payload.get("pin") or "").strip()
    operador = str(payload.get("operador") or "").strip()
    if pin:
        ok_pin, label, err_pin = operador_label_de_pin(pin)
        if not ok_pin:
            return JsonResponse({"ok": False, "erro": err_pin or label or "PIN inválido"}, status=400)
        operador = label or operador

    try:
        valor = Decimal(str(payload.get("valor") or "").replace(",", "."))
    except Exception:
        return JsonResponse({"ok": False, "erro": "Valor inválido"}, status=400)

    dia = _parse_date(payload.get("data_ref"))
    adj, err = registrar_ajuste_acumulado(
        valor,
        observacao=str(payload.get("observacao") or ""),
        operador=operador,
        data_ref=dia,
    )
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    dia_calc = _parse_date(payload.get("data_calc")) or timezone.localdate()
    return JsonResponse(
        {
            "ok": True,
            "ajuste": {
                "id": adj.pk,
                "valor": float(adj.valor),
                "observacao": adj.observacao,
            },
            "acumulado": listar_acumulado_detalhe(dia_calc),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_repasse_vila_acumulado_zerar(request):
    """Zera acumulado — dinheiro já transferido antes da ferramenta."""
    payload = _payload(request) or {}
    pin = str(payload.get("pin") or "").strip()
    operador = str(payload.get("operador") or "").strip()
    if pin:
        ok_pin, label, err_pin = operador_label_de_pin(pin)
        if not ok_pin:
            return JsonResponse({"ok": False, "erro": err_pin or label or "PIN inválido"}, status=400)
        operador = label or operador

    dia = _parse_date(payload.get("data_calc")) or timezone.localdate()
    obs = str(payload.get("observacao") or "").strip()
    adj, err = quitar_acumulado_zerar(
        dia,
        observacao=obs or "Transferido antes da ferramenta / zerado manualmente",
        operador=operador,
    )
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    return JsonResponse(
        {
            "ok": True,
            "ajuste": {"id": adj.pk, "valor": float(adj.valor)},
            "acumulado": listar_acumulado_detalhe(dia),
        }
    )


@login_required(login_url="/admin/login/")
@require_http_methods(["GET", "POST"])
def api_repasse_vila_config(request):
    if request.method == "GET":
        cfg = obter_config()
        return JsonResponse(
            {
                "ok": True,
                "percentual_lucro_padrao": float(cfg.percentual_lucro_padrao),
                "reserva_vila": float(cfg.reserva_vila),
                "reserva_vila_desde": reserva_vila_desde_config(cfg).isoformat(),
                "planos_desconto_centro": nomes_planos_desconto_centro(cfg),
                "planos": listar_planos_repasse_config(cfg),
                "atualizado_em": cfg.atualizado_em.isoformat() if cfg.atualizado_em else "",
                "atualizado_por": cfg.atualizado_por or "",
            }
        )
    payload = _payload(request) or {}
    if not payload and request.POST:
        payload = {
            "percentual_lucro_padrao": request.POST.get("percentual_lucro_padrao"),
            "reserva_vila": request.POST.get("reserva_vila"),
            "operador": request.POST.get("operador"),
        }
    op = str(payload.get("operador") or "").strip()
    if getattr(request, "user", None) and request.user.is_authenticated and not op:
        op = (request.user.get_username() or "")[:120]
    cfg = obter_config()
    if "percentual_lucro_padrao" in payload and payload.get("percentual_lucro_padrao") not in (None, ""):
        try:
            pct = Decimal(str(payload.get("percentual_lucro_padrao") or "50").replace(",", "."))
        except Exception:
            return JsonResponse({"ok": False, "erro": "Porcentagem inválida"}, status=400)
        cfg = salvar_percentual_padrao(pct, operador=op)
    if "reserva_vila" in payload:
        raw_res = payload.get("reserva_vila")
        try:
            reserva = Decimal(str(raw_res or "0").replace(",", "."))
        except Exception:
            return JsonResponse({"ok": False, "erro": "Valor que fica na Vila inválido"}, status=400)
        cfg = salvar_reserva_vila(reserva, operador=op)
    if "planos_desconto_centro" in payload:
        raw = payload.get("planos_desconto_centro")
        if raw is None:
            nomes = []
        elif isinstance(raw, list):
            nomes = raw
        elif isinstance(raw, str):
            nomes = [x.strip() for x in raw.split(",") if x.strip()]
        else:
            return JsonResponse({"ok": False, "erro": "Lista de planos inválida"}, status=400)
        cfg = salvar_planos_desconto_centro(nomes, operador=op)
    return JsonResponse(
        {
            "ok": True,
            "percentual_lucro_padrao": float(cfg.percentual_lucro_padrao),
            "reserva_vila": float(cfg.reserva_vila),
            "reserva_vila_desde": reserva_vila_desde_config(cfg).isoformat(),
            "planos_desconto_centro": nomes_planos_desconto_centro(cfg),
            "planos": listar_planos_repasse_config(cfg),
        }
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_repasse_vila_reserva_log(request):
    try:
        lim = int(request.GET.get("limit") or 80)
    except Exception:
        lim = 80
    return JsonResponse({"ok": True, "logs": listar_log_reserva(limit=lim)})


@login_required(login_url="/admin/login/")
@require_GET
def api_repasse_vila_meta(request):
    from rh.models import Funcionario

    cfg = obter_config()
    caixa = obter_sessao_caixa_aberta_request(request)
    vila = obter_caixa_vila_aberto()
    funcionarios = []
    qs = Funcionario.objects.filter(ativo=True).order_by("nome_cache", "id")[:200]
    for f in qs:
        nome = (getattr(f, "nome_exibicao", None) or getattr(f, "nome_cache", None) or "").strip()
        if not nome:
            continue
        ap = (getattr(f, "apelido_interno", None) or "").strip()
        label = f"{nome} ({ap})" if ap else nome
        funcionarios.append({"id": f.pk, "nome": label})
    calc = calcular_disponivel(timezone.localdate(), _skip_acumulado=True)
    from produtos.caixa_util import FORMAS_PAGAMENTO_CAIXA

    formas = [f for f in FORMAS_PAGAMENTO_CAIXA if f not in ("Fiado", "Vale crédito", "Cashback")]
    return JsonResponse(
        {
            "ok": True,
            "caixa_aberto": bool(caixa),
            "caixa_vila_aberto": bool(vila),
            "percentual_padrao": float(cfg.percentual_lucro_padrao),
            "reserva_vila": float(cfg.reserva_vila),
            "reserva_vila_desde": reserva_vila_desde_config(cfg).isoformat(),
            "funcionarios": funcionarios,
            "formas_pagamento": formas,
            "calc": calc,
            "url_tela": reverse("repasse_vila"),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_repasse_vila_confirmar(request):
    payload = _payload(request)
    if payload is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    pin = str(payload.get("pin") or "").strip()
    operador = str(payload.get("operador") or "").strip()
    if pin:
        ok_pin, label, err_pin = operador_label_de_pin(pin)
        if not ok_pin:
            return JsonResponse({"ok": False, "erro": err_pin or label or "PIN inválido"}, status=400)
        operador = label or operador
        user_dj = usuario_django_de_pin(pin)
        if user_dj is not None:
            request.user = user_dj  # type: ignore[attr-defined]

    quem = str(payload.get("quem_levou") or "").strip()
    try:
        pct = payload.get("percentual_lucro")
        pct_v = Decimal(str(pct).replace(",", ".")) if pct not in (None, "") else None
    except Exception:
        pct_v = None
    try:
        vm_raw = payload.get("valor_manual")
        vm = Decimal(str(vm_raw).replace(",", ".")) if vm_raw not in (None, "") else None
    except Exception:
        return JsonResponse({"ok": False, "erro": "Valor manual inválido"}, status=400)

    dia = _parse_date(payload.get("data_ref"))
    rep, err = confirmar_repasse(
        request=request,
        quem_levou=quem,
        percentual_lucro=pct_v,
        incluir_cmv=_parse_bool(payload.get("incluir_cmv"), True),
        incluir_lucro=_parse_bool(payload.get("incluir_lucro"), True),
        incluir_fiado=_parse_bool(payload.get("incluir_fiado"), True),
        modo_dia_cheio=_parse_bool(payload.get("modo_dia_cheio"), False),
        valor_manual=vm,
        forma_pagamento=str(payload.get("forma_pagamento") or "Dinheiro"),
        operador=operador,
        data_ref=dia,
        incluir_acumulado=_parse_bool(payload.get("incluir_acumulado"), False),
    )
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    return JsonResponse({"ok": True, "repasse": serializar_repasse(rep)})
