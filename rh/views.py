from __future__ import annotations

import json
import logging
from datetime import date
from decimal import Decimal
from functools import wraps

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db.models import Q, Sum
from django.http import HttpResponseForbidden, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect, ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from base.models import Empresa, Loja

from rh.access import usuario_rh_acesso_restrito
from rh.forms import (
    FechamentoEditForm,
    FechamentoTituloFinanceiroForm,
    FuncionarioForm,
    HistoricoSalarialForm,
    ReconciliarClienteAgroForm,
    ValeManualForm,
    ValeManualRHForm,
)
from rh.models import (
    FechamentoFolhaSimplificado,
    Funcionario,
    HistoricoSalarial,
    InconsistenciaIntegracaoRh,
    ValeFuncionario,
)

from produtos.views import obter_conexao_mongo
from rh.services.fechamento import (
    garantir_fechamento_aberto,
    money_two_decimals,
    motivo_bloqueio_exclusao_fechamento,
    primeiro_dia_mes,
    reabrir_fechamento,
    recalcular_fechamento,
    recalcular_todos_abertos_funcionario,
    salario_vigente_em,
    total_vales_mes,
    ultimo_dia_mes,
)
from rh.services.conferencia_rh import montar_snapshot_conferencia_rh
from rh.services.resumo_base_rh import montar_resumo_administrativo_rh
from rh.services.salario_financeiro_mongo import (
    criar_ou_atualizar_titulo_salario_mongo,
    sincronizar_valores_titulo_salario_mongo,
)
from rh.services.vale_manual_financeiro import (
    executar_vale_com_lancamento_mongo,
    montar_choices_formas_bancos,
)

logger = logging.getLogger(__name__)


def gestao_rh_required(view):
    @wraps(view)
    @login_required(login_url="/admin/login/")
    def _wrapped(request, *args, **kwargs):
        if not usuario_rh_acesso_restrito(request.user):
            return HttpResponseForbidden("Acesso negado à gestão de RH.")
        return view(request, *args, **kwargs)

    return _wrapped


@login_required(login_url="/admin/login/")
@ensure_csrf_cookie
def rh_painel(request):
    return render(
        request,
        "rh/rh_hub.html",
        {"rh_acesso_restrito": usuario_rh_acesso_restrito(request.user)},
    )


@login_required(login_url="/admin/login/")
@ensure_csrf_cookie
def rh_operadores_pins(request):
    funcionarios_outra = ["Matheus", "Estanislau"]
    return render(
        request,
        "rh/rh_operadores_pins.html",
        {"funcionarios_outra": funcionarios_outra},
    )


@gestao_rh_required
@never_cache
def rh_gestao_funcionarios(request):
    """Painel principal da gestão (substitui página única antiga)."""
    return render(
        request,
        "rh/gestao_dashboard.html",
        {
            "n_funcionarios": Funcionario.objects.filter(ativo=True).count(),
            "n_inconsistencias": InconsistenciaIntegracaoRh.objects.filter(resolvida=False).count(),
            "n_fech_abertos": FechamentoFolhaSimplificado.objects.filter(
                status=FechamentoFolhaSimplificado.Status.ABERTO
            ).count(),
        },
    )


def _filtros_funcionarios(request):
    q = Funcionario.objects.select_related("empresa", "loja", "cliente_agro").all()
    eid = request.GET.get("empresa")
    if eid:
        q = q.filter(empresa_id=eid)
    lid = request.GET.get("loja")
    if lid:
        q = q.filter(loja_id=lid)
    ativo = request.GET.get("ativo")
    if ativo == "1":
        q = q.filter(ativo=True)
    elif ativo == "0":
        q = q.filter(ativo=False)
    busca = (request.GET.get("q") or "").strip()
    if busca:
        q = q.filter(
            Q(nome_cache__icontains=busca)
            | Q(apelido_interno__icontains=busca)
            | Q(cliente_agro__nome__icontains=busca)
        )
    cargo = (request.GET.get("cargo") or "").strip()
    if cargo:
        q = q.filter(cargo__icontains=cargo)
    return q.order_by("empresa__nome_fantasia", "nome_cache")


@gestao_rh_required
def rh_funcionarios_lista(request):
    hoje = timezone.localdate()
    y, m = hoje.year, hoje.month
    ref_salario = ultimo_dia_mes(hoje)
    funcionarios = []
    for f in _filtros_funcionarios(request)[:500]:
        sal = salario_vigente_em(f, ref_salario)
        tv = total_vales_mes(f, y, m)
        liq = money_two_decimals(sal - tv)
        funcionarios.append(
            {
                "obj": f,
                "salario": sal,
                "vales_mes": tv,
                "liquido_previsto": liq,
            }
        )
    eid_filtro = request.GET.get("empresa")
    if eid_filtro:
        lojas_ctx = (
            Loja.objects.filter(ativa=True, empresa_id=eid_filtro)
            .select_related("empresa")
            .order_by("nome")
        )
    else:
        lojas_ctx = Loja.objects.none()
    return render(
        request,
        "rh/funcionarios_lista.html",
        {
            "funcionarios": funcionarios,
            "empresas": Empresa.objects.filter(ativo=True).order_by("nome_fantasia"),
            "lojas": lojas_ctx,
        },
    )


@gestao_rh_required
@require_http_methods(["GET", "POST"])
def rh_funcionario_novo(request):
    if request.method == "POST":
        form = FuncionarioForm(request.POST)
        if form.is_valid():
            f = form.save()
            sal = form.cleaned_data.get("salario_inicial")
            if sal is not None and sal > 0:
                HistoricoSalarial.objects.create(
                    funcionario=f,
                    salario_base=sal,
                    data_inicio_vigencia=timezone.localdate(),
                    motivo_alteracao="Cadastro inicial",
                )
            messages.success(request, "Funcionário cadastrado.")
            return redirect("rh_funcionario_ficha", pk=f.pk)
    else:
        form = FuncionarioForm()
    return render(request, "rh/funcionario_form.html", {"form": form, "titulo": "Novo funcionário"})


@gestao_rh_required
@require_http_methods(["GET", "POST"])
def rh_funcionario_editar(request, pk: int):
    f = get_object_or_404(Funcionario, pk=pk)
    if request.method == "POST":
        form = FuncionarioForm(request.POST, instance=f)
        if form.is_valid():
            form.save()
            messages.success(request, "Cadastro atualizado.")
            return redirect("rh_funcionario_ficha", pk=f.pk)
    else:
        form = FuncionarioForm(instance=f)
    return render(
        request,
        "rh/funcionario_form.html",
        {"form": form, "titulo": f"Editar — {f.nome_exibicao}", "funcionario": f},
    )


@gestao_rh_required
def rh_funcionario_ficha(request, pk: int):
    f = get_object_or_404(
        Funcionario.objects.select_related("empresa", "loja", "cliente_agro"),
        pk=pk,
    )
    hoje = timezone.localdate()
    y, m = hoje.year, hoje.month
    sal = salario_vigente_em(f, hoje)
    tv = total_vales_mes(f, y, m)
    historico = f.historicos_salario.all()[:50]
    vales = f.vales.all()[:200]
    fechamentos = f.fechamentos.all()[:24]
    sal_form = HistoricoSalarialForm()
    formas_c, bancos_c = montar_choices_formas_bancos(request.user, modo="erp")
    _, db_m = obter_conexao_mongo()
    vale_form = ValeManualRHForm(
        initial={"data": hoje, "registrar_no_financeiro": True},
        formas_choices=formas_c,
        bancos_choices=bancos_c,
    )
    return render(
        request,
        "rh/funcionario_ficha.html",
        {
            "funcionario": f,
            "salario_atual": sal,
            "total_vales_mes": tv,
            "liquido_previsto_mes": sal - tv,
            "historico_salario": historico,
            "vales": vales,
            "fechamentos": fechamentos,
            "sal_form": sal_form,
            "vale_form": vale_form,
            "vale_mongo_disponivel": db_m is not None,
            "vale_listas_vazias": db_m is not None and not formas_c and not bancos_c,
            "hoje": hoje,
        },
    )


@gestao_rh_required
@require_POST
@csrf_protect
def rh_funcionario_garantir_fechamento_mes_atual(request, pk: int):
    """Cria ou reabre folha do mês corrente sem depender de lançar vale antes."""
    f = get_object_or_404(Funcionario, pk=pk)
    hoje = timezone.localdate()
    fech = garantir_fechamento_aberto(f, hoje)
    messages.success(
        request,
        f"Folha {fech.competencia:%m/%Y} pronta. Lá você define o vencimento no financeiro (passo 2), se quiser.",
    )
    return redirect("rh_fechamento_detalhe", pk=fech.pk)


@gestao_rh_required
@require_POST
@csrf_protect
def rh_funcionario_salario_adicionar(request, pk: int):
    f = get_object_or_404(Funcionario, pk=pk)
    form = HistoricoSalarialForm(request.POST)
    if not form.is_valid():
        messages.error(request, "Dados de salário inválidos.")
        return redirect("rh_funcionario_ficha", pk=f.pk)
    hoje = form.cleaned_data["data_inicio_vigencia"]
    HistoricoSalarial.objects.filter(
        funcionario=f,
        data_fim_vigencia__isnull=True,
    ).update(data_fim_vigencia=hoje)
    hs = form.save(commit=False)
    hs.funcionario = f
    hs.save()
    recalcular_todos_abertos_funcionario(f)
    messages.success(request, "Nova faixa salarial registrada. Fechamentos abertos foram recalculados.")
    return redirect("rh_funcionario_ficha", pk=f.pk)


@gestao_rh_required
@require_POST
@csrf_protect
def rh_funcionario_vale_manual(request, pk: int):
    f = get_object_or_404(
        Funcionario.objects.select_related("empresa", "loja", "cliente_agro"),
        pk=pk,
    )
    formas_c, bancos_c = montar_choices_formas_bancos(request.user, modo="erp")
    form = ValeManualRHForm(
        request.POST,
        formas_choices=formas_c,
        bancos_choices=bancos_c,
    )
    if not form.is_valid():
        for _field, errs in form.errors.items():
            for err in errs:
                messages.error(request, err)
        return redirect("rh_funcionario_ficha", pk=f.pk)
    cd = form.cleaned_data

    if cd.get("registrar_no_financeiro"):
        r = executar_vale_com_lancamento_mongo(
            funcionario=f,
            usuario=request.user,
            data=cd["data"],
            valor=cd["valor"],
            observacao=cd.get("observacao") or "",
            forma_value=cd["forma_baixa"],
            banco_value=cd["banco_baixa"],
        )
        if not r.get("ok"):
            messages.error(request, r.get("erro") or "Falha ao gravar no financeiro.")
            return redirect("rh_funcionario_ficha", pk=f.pk)
        garantir_fechamento_aberto(f, cd["data"])
        msg = "Vale registrado como pagamento parcial do título de salário (Mongo)."
        if r.get("aviso"):
            msg += " " + str(r["aviso"])
        messages.success(request, msg)
        return redirect("rh_funcionario_ficha", pk=f.pk)

    ValeFuncionario.objects.create(
        funcionario=f,
        empresa=f.empresa,
        loja=f.loja,
        data=cd["data"],
        valor=cd["valor"],
        observacao=(cd.get("observacao") or "")[:500],
        tipo_origem=ValeFuncionario.TipoOrigem.MANUAL,
        criado_por=request.user,
    )
    garantir_fechamento_aberto(f, cd["data"])
    messages.success(request, "Vale lançado só no RH (sem título no financeiro).")
    return redirect("rh_funcionario_ficha", pk=f.pk)


@gestao_rh_required
@require_POST
@csrf_protect
def rh_funcionario_vale_cancelar(request, pk: int, vale_id: int):
    """Marca vale como cancelado (teste ou erro); realinha folha aberta e ValorPago no Mongo."""
    f = get_object_or_404(Funcionario, pk=pk)
    v = get_object_or_404(ValeFuncionario, pk=vale_id, funcionario=f)
    if v.cancelado:
        messages.info(request, "Este vale já estava cancelado.")
        return redirect("rh_funcionario_ficha", pk=f.pk)
    motivo = (request.POST.get("motivo") or "").strip()
    if len(motivo) < 3:
        messages.error(request, "Informe o motivo do cancelamento (pelo menos 3 caracteres).")
        return redirect("rh_funcionario_ficha", pk=f.pk)
    v.cancelado = True
    v.cancelado_em = timezone.now()
    v.motivo_cancelamento = motivo[:400]
    v.save(update_fields=["cancelado", "cancelado_em", "motivo_cancelamento", "atualizado_em"])
    recalcular_todos_abertos_funcionario(f)
    messages.success(request, "Vale cancelado no RH.")
    fe = (
        FechamentoFolhaSimplificado.objects.filter(
            funcionario=f,
            competencia=primeiro_dia_mes(v.data),
        )
        .first()
    )
    if fe and (fe.mongo_lancamento_salario_id or "").strip():
        sr = sincronizar_valores_titulo_salario_mongo(fe)
        if not sr.get("ok"):
            messages.warning(
                request,
                sr.get("erro") or "Não foi possível alinhar o ValorPago do título no Mongo — conferir no financeiro.",
            )
    return redirect("rh_funcionario_ficha", pk=f.pk)


@gestao_rh_required
def rh_fechamentos_lista(request):
    qs = FechamentoFolhaSimplificado.objects.select_related(
        "funcionario",
        "funcionario__cliente_agro",
        "empresa",
    ).all()
    comp = request.GET.get("competencia")
    if comp:
        try:
            d = date.fromisoformat(comp[:10])
            qs = qs.filter(competencia=primeiro_dia_mes(d))
        except ValueError:
            pass
    eid = request.GET.get("empresa")
    if eid:
        qs = qs.filter(empresa_id=eid)
    lid = request.GET.get("loja")
    if lid:
        qs = qs.filter(funcionario__loja_id=lid)
    fid = request.GET.get("funcionario")
    if fid:
        qs = qs.filter(funcionario_id=fid)
    st = request.GET.get("status")
    if st:
        qs = qs.filter(status=st)
    return render(
        request,
        "rh/fechamentos_lista.html",
        {
            "fechamentos": qs.order_by("-competencia", "funcionario__nome_cache")[:500],
            "empresas": Empresa.objects.filter(ativo=True).order_by("nome_fantasia"),
        },
    )


@gestao_rh_required
def rh_fechamento_detalhe(request, pk: int):
    f = get_object_or_404(
        FechamentoFolhaSimplificado.objects.select_related("funcionario", "funcionario__cliente_agro"),
        pk=pk,
    )
    if f.status == FechamentoFolhaSimplificado.Status.ABERTO:
        recalcular_fechamento(f)
        f.refresh_from_db()
    f = (
        FechamentoFolhaSimplificado.objects.select_related(
            "funcionario",
            "funcionario__cliente_agro",
        )
        .prefetch_related("itens")
        .get(pk=f.pk)
    )
    form = FechamentoEditForm(instance=f)
    tem_historico_salario = HistoricoSalarial.objects.filter(funcionario_id=f.funcionario_id).exists()
    salario_base_zero = f.salario_base_na_competencia == Decimal("0")
    ultimo_dia_competencia = ultimo_dia_mes(f.competencia)
    formas_c, bancos_c = montar_choices_formas_bancos(request.user, modo="erp")
    titulo_form = FechamentoTituloFinanceiroForm(formas_choices=formas_c, bancos_choices=bancos_c)
    if f.data_vencimento_pagamento:
        titulo_form.fields["data_vencimento"].initial = f.data_vencimento_pagamento
    else:
        titulo_form.fields["data_vencimento"].initial = ultimo_dia_competencia

    return render(
        request,
        "rh/fechamento_detalhe.html",
        {
            "fechamento": f,
            "form": form,
            "titulo_form": titulo_form,
            "tem_historico_salario": tem_historico_salario,
            "salario_base_zero": salario_base_zero,
            "ultimo_dia_competencia": ultimo_dia_competencia,
            "plano_salario_folha_hint": getattr(settings, "AGRO_RH_PLANO_SALARIO_FOLHA", ""),
            "fechamento_exclusao_bloqueio": motivo_bloqueio_exclusao_fechamento(f),
        },
    )


@gestao_rh_required
@require_POST
@csrf_protect
def rh_fechamento_salvar(request, pk: int):
    f = get_object_or_404(FechamentoFolhaSimplificado, pk=pk)
    if f.status != FechamentoFolhaSimplificado.Status.ABERTO:
        messages.error(request, "Só é possível editar fechamento com status Aberto.")
        return redirect("rh_fechamento_detalhe", pk=f.pk)
    form = FechamentoEditForm(request.POST, instance=f)
    if form.is_valid():
        form.save()
        recalcular_fechamento(f)
        f.refresh_from_db()
        if (f.mongo_lancamento_salario_id or "").strip():
            sr = sincronizar_valores_titulo_salario_mongo(f)
            if not sr.get("ok"):
                messages.warning(
                    request,
                    sr.get("erro") or "Fechamento salvo; não foi possível sincronizar o título no financeiro.",
                )
            else:
                messages.success(request, "Fechamento atualizado, recalculado e título no financeiro alinhado.")
        else:
            messages.success(request, "Fechamento atualizado e recalculado.")
    else:
        messages.error(request, "Revise os valores.")
    return redirect("rh_fechamento_detalhe", pk=f.pk)


@gestao_rh_required
@require_POST
@csrf_protect
def rh_fechamento_titulo_financeiro(request, pk: int):
    f = get_object_or_404(FechamentoFolhaSimplificado, pk=pk)
    formas_c, bancos_c = montar_choices_formas_bancos(request.user, modo="erp")
    form = FechamentoTituloFinanceiroForm(
        request.POST,
        formas_choices=formas_c,
        bancos_choices=bancos_c,
    )
    acao = (request.POST.get("titulo_acao") or "").strip()
    if not form.is_valid():
        for _field, errs in form.errors.items():
            for err in errs:
                messages.error(request, err)
        return redirect("rh_fechamento_detalhe", pk=f.pk)

    cd = form.cleaned_data
    dv = cd["data_vencimento"]
    mid = (f.mongo_lancamento_salario_id or "").strip()

    if acao == "sync_valores":
        if not mid:
            messages.error(request, "Ainda não há título no financeiro — use «Gerar / atualizar título».")
            return redirect("rh_fechamento_detalhe", pk=f.pk)
        f.data_vencimento_pagamento = dv
        f.save(update_fields=["data_vencimento_pagamento", "atualizado_em"])
        sr = sincronizar_valores_titulo_salario_mongo(f)
        if sr.get("ok"):
            messages.success(request, "Valores do título sincronizados com a folha.")
        else:
            messages.error(request, sr.get("erro") or "Falha ao sincronizar.")
        return redirect("rh_fechamento_detalhe", pk=f.pk)

    if acao != "publicar":
        messages.error(request, "Ação inválida.")
        return redirect("rh_fechamento_detalhe", pk=f.pk)

    if f.status != FechamentoFolhaSimplificado.Status.ABERTO and mid:
        messages.error(
            request,
            "Para alterar vencimento ou cabeçalho no financeiro, reabra a competência (Aberto). "
            "Ou use apenas «Igualar ao que está na folha» depois de lançar vales em atraso.",
        )
        return redirect("rh_fechamento_detalhe", pk=f.pk)

    r = criar_ou_atualizar_titulo_salario_mongo(
        f,
        usuario=request.user,
        data_vencimento=dv,
        forma_value=cd["forma_financeiro"],
        banco_value=cd["banco_financeiro"],
    )
    if r.get("ok"):
        msg = "Título de salário criado no financeiro." if r.get("criado") else "Título de salário atualizado no financeiro."
        messages.success(request, msg)
    else:
        messages.error(request, r.get("erro") or "Falha ao gravar no financeiro.")
    return redirect("rh_fechamento_detalhe", pk=f.pk)


@gestao_rh_required
@require_POST
@csrf_protect
def rh_fechamento_fechar(request, pk: int):
    f = get_object_or_404(FechamentoFolhaSimplificado, pk=pk)
    if f.status != FechamentoFolhaSimplificado.Status.ABERTO:
        messages.error(request, "Só é possível fechar competências ainda abertas.")
        return redirect("rh_fechamento_detalhe", pk=f.pk)
    recalcular_fechamento(f)
    f.status = FechamentoFolhaSimplificado.Status.FECHADO
    f.fechado_em = timezone.now()
    f.save(update_fields=["status", "fechado_em", "atualizado_em"])
    messages.success(request, "Competência fechada.")
    return redirect("rh_fechamento_detalhe", pk=f.pk)


@gestao_rh_required
@require_POST
@csrf_protect
def rh_fechamento_marcar_pago(request, pk: int):
    f = get_object_or_404(FechamentoFolhaSimplificado, pk=pk)
    if f.status == FechamentoFolhaSimplificado.Status.PAGO:
        messages.info(request, "Este fechamento já está como Pago.")
        return redirect("rh_fechamento_detalhe", pk=f.pk)
    recalcular_fechamento(f)
    prev = f.valor_liquido_previsto
    f.valor_pago = prev
    f.status = FechamentoFolhaSimplificado.Status.PAGO
    f.save(update_fields=["valor_pago", "status", "atualizado_em"])
    messages.success(request, "Marcado como pago (valor = líquido previsto).")
    return redirect("rh_fechamento_detalhe", pk=f.pk)


@gestao_rh_required
@require_POST
@csrf_protect
def rh_fechamento_reabrir(request, pk: int):
    f = get_object_or_404(FechamentoFolhaSimplificado, pk=pk)
    if f.status == FechamentoFolhaSimplificado.Status.ABERTO:
        messages.info(request, "Esta competência já está aberta.")
        return redirect("rh_fechamento_detalhe", pk=f.pk)
    reabrir_fechamento(f)
    messages.success(
        request,
        "Folha reaberta. Você pode editar a folha e o título no financeiro outra vez.",
    )
    return redirect("rh_fechamento_detalhe", pk=f.pk)


@gestao_rh_required
@require_POST
@csrf_protect
def rh_fechamento_excluir(request, pk: int):
    f = get_object_or_404(FechamentoFolhaSimplificado, pk=pk)
    motivo = motivo_bloqueio_exclusao_fechamento(f)
    if motivo:
        messages.error(request, motivo)
        return redirect("rh_fechamento_detalhe", pk=f.pk)
    fid = f.funcionario_id
    comp = f.competencia
    f.delete()
    messages.success(
        request,
        f"Competência {comp:%m/%Y} removida do RH. Se precisar de novo, ela volta ao lançar vale ou importar.",
    )
    return redirect("rh_funcionario_ficha", pk=fid)


@gestao_rh_required
def rh_inconsistencias_lista(request):
    qs = InconsistenciaIntegracaoRh.objects.select_related("empresa").filter(resolvida=False).order_by("-criado_em")
    return render(request, "rh/inconsistencias_lista.html", {"inconsistencias": qs[:500]})


@gestao_rh_required
@require_POST
@csrf_protect
def rh_inconsistencia_resolver(request, pk: int):
    inc = get_object_or_404(InconsistenciaIntegracaoRh, pk=pk)
    inc.resolvida = True
    inc.resolvida_em = timezone.now()
    inc.save(update_fields=["resolvida", "resolvida_em"])
    messages.success(request, "Marcada como resolvida.")
    return redirect("rh_inconsistencias_lista")


@gestao_rh_required
def rh_relatorios(request):
    hoje = timezone.localdate()
    y, m = hoje.year, hoje.month
    ref_salario = ultimo_dia_mes(hoje)
    resumo = []
    for f in Funcionario.objects.filter(ativo=True).select_related("empresa", "cliente_agro"):
        sal = salario_vigente_em(f, ref_salario)
        tv = total_vales_mes(f, y, m)
        resumo.append({"f": f, "sal": sal, "vales": tv, "liq": money_two_decimals(sal - tv)})
    resumo.sort(key=lambda x: -x["vales"])
    vales_periodo = [
        {
            "funcionario__nome_cache": row["funcionario__nome_cache"],
            "total": money_two_decimals(row["total"]),
        }
        for row in ValeFuncionario.objects.filter(cancelado=False, data__year=y, data__month=m)
        .values("funcionario__nome_cache")
        .annotate(total=Sum("valor"))
        .order_by("-total")[:20]
    ]
    abertos = FechamentoFolhaSimplificado.objects.filter(
        status=FechamentoFolhaSimplificado.Status.ABERTO
    ).count()
    divs = InconsistenciaIntegracaoRh.objects.filter(
        resolvida=False,
        tipo=InconsistenciaIntegracaoRh.Tipo.DIVERGENCIA,
    ).count()
    return render(
        request,
        "rh/relatorios.html",
        {
            "resumo_mes": resumo[:50],
            "vales_agregados": vales_periodo,
            "competencias_abertas": abertos,
            "divergencias": divs,
        },
    )


@gestao_rh_required
@never_cache
def rh_conferencia_tecnica(request):
    snap = montar_snapshot_conferencia_rh()
    resumo = montar_resumo_administrativo_rh()
    return render(
        request,
        "rh/conferencia_tecnica.html",
        {"snap": snap, "resumo": resumo},
    )


@gestao_rh_required
@require_http_methods(["GET", "POST"])
@csrf_protect
def rh_funcionario_reconciliar_cliente(request, pk: int):
    f = get_object_or_404(
        Funcionario.objects.select_related("empresa", "loja", "cliente_agro"),
        pk=pk,
    )
    if request.method == "POST":
        form = ReconciliarClienteAgroForm(request.POST, funcionario=f)
        if form.is_valid():
            ca = form.cleaned_data["cliente_agro"]
            f.cliente_agro = ca
            if form.cleaned_data.get("atualizar_nome_cache"):
                f.nome_cache = (ca.nome or "")[:200]
            f.save()
            messages.success(
                request,
                f"Vínculo atualizado para ClienteAgro #{ca.pk} ({ca.nome}).",
            )
            return redirect("rh_funcionario_ficha", pk=f.pk)
    else:
        form = ReconciliarClienteAgroForm(funcionario=f)
    return render(
        request,
        "rh/funcionario_reconciliar_cliente.html",
        {
            "funcionario": f,
            "form": form,
        },
    )


@gestao_rh_required
def rh_importar_vales(request):
    """Descontinuado: vales entram pela saída de caixa (plano Vale) ou lançamento manual na ficha."""
    messages.info(
        request,
        "A importação em lote de vales a partir do ERP/Mongo foi desligada. "
        "Use a saída de caixa com o plano «Adiantamento de Salário (Vale)» ou o vale manual na ficha do funcionário. "
        "Se um lançamento não gerar vale, consulte RH → Conferência.",
    )
    return redirect("rh_conferencia_tecnica")


@login_required(login_url="/admin/login/")
@require_GET
def api_rh_caixa_quem_leva(request):
    """
    Lista perfis RH ativos (com ClienteAgro) para o campo «Quem está levando» no caixa.
    Não exige permissão de gestão RH — apenas login (mesmo público da API de saída).
    """
    from django.conf import settings

    from rh.utils import resolver_empresa_por_nome_fantasia

    emp = None
    eid = request.GET.get("empresa_id")
    if eid:
        try:
            emp = Empresa.objects.filter(pk=int(eid), ativo=True).first()
        except (TypeError, ValueError):
            pass
    if emp is None:
        nome_emp = (request.GET.get("empresa_nome") or "").strip() or (
            getattr(settings, "AGRO_SAIDA_CAIXA_EMPRESA_PADRAO", "") or ""
        ).strip()
        emp = resolver_empresa_por_nome_fantasia(nome_emp)
    if emp is None:
        return JsonResponse(
            {
                "ok": False,
                "erro": "Empresa não informada ou não encontrada no cadastro base.",
                "pessoas": [],
            },
            status=400,
        )
    qs = (
        Funcionario.objects.filter(empresa=emp, ativo=True)
        .select_related("cliente_agro")
        .order_by("nome_cache", "apelido_interno", "id")
    )
    pessoas = []
    for f in qs[:500]:
        label = f.nome_exibicao
        ap = (f.apelido_interno or "").strip()
        if ap:
            label = f"{label} ({ap})"
        pessoas.append(
            {
                "cliente_agro_id": f.cliente_agro_id,
                "perfil_rh_id": f.pk,
                "label": label,
                "cargo": (f.cargo or "").strip(),
            }
        )
    return JsonResponse(
        {
            "ok": True,
            "empresa_id": emp.pk,
            "empresa_nome": emp.nome_fantasia,
            "pessoas": pessoas,
        }
    )


# --- APIs JSON (mesmo critério de acesso da gestão) ---


def _json_error(msg: str, status: int = 400):
    return JsonResponse({"ok": False, "erro": msg}, status=status)


@gestao_rh_required
@require_GET
def api_rh_funcionarios(request):
    qs = _filtros_funcionarios(request)
    out = []
    hoje = timezone.localdate()
    for f in qs[:200]:
        out.append(
            {
                "id": f.pk,
                "nome": f.nome_exibicao,
                "nome_cache": f.nome_cache,
                "apelido_interno": f.apelido_interno,
                "cliente_agro_id": f.cliente_agro_id,
                "cargo": f.cargo,
                "empresa_id": f.empresa_id,
                "loja_id": f.loja_id,
                "ativo": f.ativo,
                "salario_atual": str(salario_vigente_em(f, hoje)),
            }
        )
    return JsonResponse({"ok": True, "funcionarios": out})


@gestao_rh_required
@require_GET
def api_rh_funcionario_detail(request, pk: int):
    f = get_object_or_404(
        Funcionario.objects.select_related("cliente_agro"),
        pk=pk,
    )
    hoje = timezone.localdate()
    y, m = hoje.year, hoje.month
    return JsonResponse(
        {
            "ok": True,
            "funcionario": {
                "id": f.pk,
                "nome": f.nome_exibicao,
                "nome_cache": f.nome_cache,
                "cliente_agro_id": f.cliente_agro_id,
                "empresa_id": f.empresa_id,
                "loja_id": f.loja_id,
                "cargo": f.cargo,
                "ativo": f.ativo,
                "salario_atual": str(salario_vigente_em(f, hoje)),
                "vales_mes": str(total_vales_mes(f, y, m)),
            },
        }
    )


@gestao_rh_required
@require_POST
@csrf_protect
def api_rh_funcionario_create(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return _json_error("JSON inválido")
    form = FuncionarioForm(payload)
    if not form.is_valid():
        return JsonResponse({"ok": False, "erros": form.errors}, status=400)
    f = form.save()
    return JsonResponse({"ok": True, "id": f.pk}, status=201)


@gestao_rh_required
@require_POST
@csrf_protect
def api_rh_funcionario_vale(request, pk: int):
    f = get_object_or_404(
        Funcionario.objects.select_related("empresa", "loja", "cliente_agro"),
        pk=pk,
    )
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return _json_error("JSON inválido")

    if payload.get("registrar_no_financeiro") or payload.get("registrar_financeiro"):
        try:
            d_raw = payload.get("data")
            if hasattr(d_raw, "isoformat"):
                d_comp = d_raw
            else:
                d_comp = date.fromisoformat(str(d_raw)[:10])
            valor = Decimal(str(payload.get("valor") or "0").replace(",", "."))
        except (TypeError, ValueError):
            return _json_error("Informe data (AAAA-MM-DD) e valor numérico.")
        if valor <= 0:
            return _json_error("Valor deve ser maior que zero.")
        r = executar_vale_com_lancamento_mongo(
            funcionario=f,
            usuario=request.user,
            data=d_comp,
            valor=valor,
            observacao=str(payload.get("observacao") or ""),
            forma_value=str(payload.get("forma_baixa") or ""),
            banco_value=str(payload.get("banco_baixa") or ""),
        )
        if not r.get("ok"):
            return JsonResponse({"ok": False, "erro": r.get("erro")}, status=400)
        from rh.services.fechamento import garantir_fechamento_aberto

        garantir_fechamento_aberto(f, d_comp)
        mid = r.get("mongo_titulo_id")
        out = {"ok": True, "mongo_titulo_id": mid, "mongo_ids": [mid] if mid else []}
        if r.get("aviso"):
            out["aviso"] = r["aviso"]
        return JsonResponse(out)

    form = ValeManualForm(payload)
    if not form.is_valid():
        return JsonResponse({"ok": False, "erros": form.errors}, status=400)
    v = form.save(commit=False)
    v.funcionario = f
    v.empresa = f.empresa
    v.loja = f.loja
    v.tipo_origem = ValeFuncionario.TipoOrigem.MANUAL
    v.criado_por = request.user
    v.save()
    from rh.services.fechamento import garantir_fechamento_aberto

    garantir_fechamento_aberto(f, v.data)
    return JsonResponse({"ok": True, "vale_id": v.pk})


@gestao_rh_required
@require_GET
def api_rh_fechamentos(request):
    qs = FechamentoFolhaSimplificado.objects.select_related(
        "funcionario",
        "funcionario__cliente_agro",
    ).all()[:300]
    out = []
    for x in qs:
        out.append(
            {
                "id": x.pk,
                "funcionario_id": x.funcionario_id,
                "funcionario": x.funcionario.nome_exibicao,
                "competencia": x.competencia.isoformat(),
                "salario_base": str(x.salario_base_na_competencia),
                "total_vales": str(x.total_vales),
                "liquido_previsto": str(x.valor_liquido_previsto),
                "valor_pago": str(x.valor_pago),
                "status": x.status,
                "data_vencimento_pagamento": x.data_vencimento_pagamento.isoformat()
                if x.data_vencimento_pagamento
                else None,
                "mongo_lancamento_salario_id": (x.mongo_lancamento_salario_id or "").strip() or None,
            }
        )
    return JsonResponse({"ok": True, "fechamentos": out})


@gestao_rh_required
@require_POST
@csrf_protect
def api_rh_fechamentos_recalcular(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        payload = {}
    ids = payload.get("ids")
    if ids:
        for i in ids:
            f = get_object_or_404(FechamentoFolhaSimplificado, pk=int(i))
            recalcular_fechamento(f)
            f.refresh_from_db()
            if (f.mongo_lancamento_salario_id or "").strip():
                sincronizar_valores_titulo_salario_mongo(f)
    else:
        for f in FechamentoFolhaSimplificado.objects.filter(
            status=FechamentoFolhaSimplificado.Status.ABERTO
        ):
            recalcular_fechamento(f)
            f.refresh_from_db()
            if (f.mongo_lancamento_salario_id or "").strip():
                sincronizar_valores_titulo_salario_mongo(f)
    return JsonResponse({"ok": True})


@gestao_rh_required
@require_POST
@csrf_protect
def api_rh_fechamento_fechar(request, pk: int):
    f = get_object_or_404(FechamentoFolhaSimplificado, pk=pk)
    recalcular_fechamento(f)
    f.status = FechamentoFolhaSimplificado.Status.FECHADO
    f.fechado_em = timezone.now()
    f.save(update_fields=["status", "fechado_em", "atualizado_em"])
    return JsonResponse({"ok": True})


@gestao_rh_required
@require_POST
@csrf_protect
def api_rh_fechamento_marcar_pago(request, pk: int):
    f = get_object_or_404(FechamentoFolhaSimplificado, pk=pk)
    recalcular_fechamento(f)
    f.valor_pago = f.valor_liquido_previsto
    f.status = FechamentoFolhaSimplificado.Status.PAGO
    f.save(update_fields=["valor_pago", "status", "atualizado_em"])
    return JsonResponse({"ok": True})


@gestao_rh_required
@require_POST
@csrf_protect
def api_rh_importar_vales_caixa(request):
    return JsonResponse(
        {
            "ok": False,
            "erro": (
                "Endpoint descontinuado. Use saída de caixa (plano Adiantamento/Vale) "
                "ou vale manual na ficha. Em caso de dúvida, RH → Conferência."
            ),
        },
        status=410,
    )
