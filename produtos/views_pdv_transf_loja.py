"""APIs PDV — pedido de transferência entre lojas."""
from __future__ import annotations

import json
import logging

from django.contrib.auth.decorators import login_required
from django.db.models import Q
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from estoque.models import SolicitacaoTransferenciaPdv
from produtos.pdv_deposito_util import bootstrap_deposito, normalizar_deposito, rotulo_deposito
from produtos.pdv_transf_loja_util import (
    STATUS_CANCELADO,
    STATUS_CONCLUIDO,
    aplicar_status,
    concluir_transferencia,
    criar_solicitacao,
    resolver_operador_pdv,
    resumo_loja,
    serializar_solicitacao,
)

logger = logging.getLogger(__name__)


def _payload(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _loja_atual(request, payload=None) -> str:
    if payload and payload.get("loja"):
        return normalizar_deposito(payload.get("loja"))
    boot = bootstrap_deposito(request)
    return normalizar_deposito(boot.get("deposito") or "centro")


def _operador(request, payload=None):
    pin = ""
    if payload:
        pin = str(payload.get("pin") or "").strip()
    return resolver_operador_pdv(request, pin)


@login_required(login_url="/admin/login/")
@require_GET
def api_pdv_transf_loja_saldos(request):
    """Saldo Agro (ledger/ajuste) para os produtos da busca — wizard=1 zera isso."""
    raw = str(request.GET.get("ids") or "")
    ids = [x.strip()[:64] for x in raw.split(",") if x.strip()][:40]
    if not ids:
        return JsonResponse({"ok": True, "saldos": {}})
    db = client = None
    try:
        from produtos.views import obter_conexao_mongo

        client, db = obter_conexao_mongo()
    except Exception:
        db = client = None
    from produtos.estoque_saldo_agro_util import mapa_saldos_operacionais_agro

    m = mapa_saldos_operacionais_agro(ids, db=db, client=client) or {}
    out = {}
    for pid in ids:
        info = m.get(pid) or {}
        out[pid] = {
            "saldo_centro": float(info.get("saldo_centro") or 0),
            "saldo_vila": float(info.get("saldo_vila") or 0),
        }
    return JsonResponse({"ok": True, "saldos": out})


@login_required(login_url="/admin/login/")
@require_GET
def api_pdv_transf_loja_resumo(request):
    loja = _loja_atual(request)
    ok, label, _user, _err = resolver_operador_pdv(request, "")
    return JsonResponse(
        {
            "ok": True,
            "operador": label if ok else "",
            "precisa_pin": not ok,
            **resumo_loja(loja),
        }
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_pdv_transf_loja_lista(request):
    loja = _loja_atual(request)
    aba = str(request.GET.get("aba") or "recebidos").strip().lower()
    qs = SolicitacaoTransferenciaPdv.objects.prefetch_related("itens")
    if aba == "enviados":
        qs = qs.filter(
            loja_destino=loja,
            status__in=SolicitacaoTransferenciaPdv.STATUS_ABERTOS,
        )
    elif aba == "historico":
        qs = qs.filter(
            Q(loja_origem=loja) | Q(loja_destino=loja),
            status__in=(STATUS_CONCLUIDO, STATUS_CANCELADO),
        ).order_by("-atualizado_em", "-id")
    else:
        qs = qs.filter(
            loja_origem=loja,
            status__in=SolicitacaoTransferenciaPdv.STATUS_ABERTOS,
        )
    qs = qs[:80]
    return JsonResponse(
        {
            "ok": True,
            "loja": loja,
            "loja_label": rotulo_deposito(loja),
            "aba": aba,
            "itens": [serializar_solicitacao(s) for s in qs],
            **resumo_loja(loja),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_pdv_transf_loja_criar(request):
    payload = _payload(request)
    if payload is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    ok, label, user, err = _operador(request, payload)
    if not ok:
        return JsonResponse({"ok": False, "erro": err, "precisa_pin": True}, status=403)
    loja = _loja_atual(request, payload)
    sol, err_c = criar_solicitacao(
        loja_destino=loja,
        itens_raw=payload.get("itens") or [],
        observacao=str(payload.get("observacao") or ""),
        operador_label=label,
        usuario=user,
    )
    if err_c or sol is None:
        return JsonResponse({"ok": False, "erro": err_c or "Não foi possível criar"}, status=400)
    sol = (
        SolicitacaoTransferenciaPdv.objects.prefetch_related("itens", "eventos")
        .filter(pk=sol.pk)
        .first()
    )
    return JsonResponse(
        {
            "ok": True,
            "mensagem": "Pedido enviado para " + rotulo_deposito(sol.loja_origem) + ".",
            "solicitacao": serializar_solicitacao(sol, com_eventos=True),
            **resumo_loja(loja),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_pdv_transf_loja_acao(request, pk: int):
    payload = _payload(request) or {}
    ok, label, user, err = _operador(request, payload)
    if not ok:
        return JsonResponse({"ok": False, "erro": err, "precisa_pin": True}, status=403)
    loja = _loja_atual(request, payload)
    sol = (
        SolicitacaoTransferenciaPdv.objects.prefetch_related("itens")
        .filter(pk=pk)
        .first()
    )
    if sol is None:
        return JsonResponse({"ok": False, "erro": "Pedido não encontrado."}, status=404)
    acao = str(payload.get("acao") or "").strip().lower()
    if acao == "transferir":
        furado = bool(payload.get("estoque_furado") or payload.get("furado"))
        ajustar = bool(payload.get("ajustar_estoque") or payload.get("ajustar"))
        ajustes = payload.get("ajustes_por_produto") or payload.get("ajustes")
        if not isinstance(ajustes, dict):
            ajustes = None
        qtds_envio = payload.get("itens")
        if qtds_envio is None:
            qtds_envio = payload.get("quantidades_envio")
        if qtds_envio is None:
            qtds_envio = payload.get("quantidades")
        ok_t, err_t, _res = concluir_transferencia(
            request,
            sol,
            loja_atual=loja,
            operador_label=label,
            usuario=user,
            estoque_furado=furado,
            ajustar_estoque=ajustar,
            ajuste_quantidade=payload.get("ajuste_quantidade")
            if payload.get("ajuste_quantidade") is not None
            else payload.get("ajuste_qtd"),
            ajustes_por_produto=ajustes,
            quantidades_envio=qtds_envio,
        )
        if not ok_t:
            return JsonResponse({"ok": False, "erro": err_t}, status=400)
        sol.refresh_from_db()
        sol = (
            SolicitacaoTransferenciaPdv.objects.prefetch_related("itens", "eventos")
            .filter(pk=sol.pk)
            .first()
        )
        msg = "Estoque transferido."
        if furado:
            msg = "Estoque transferido · marcado furado."
            if ajustar:
                msg = "Estoque transferido · furado · saldo da origem ajustado."
        return JsonResponse(
            {
                "ok": True,
                "mensagem": msg,
                "solicitacao": serializar_solicitacao(sol, com_eventos=True),
                **resumo_loja(loja),
            }
        )
    # Cancelar com estoque furado + ajuste opcional (sem transferir)
    if acao == "cancelar":
        furado = bool(payload.get("estoque_furado") or payload.get("furado"))
        ajustar = bool(payload.get("ajustar_estoque") or payload.get("ajustar"))
        if furado and ajustar:
            from produtos.pdv_transf_loja_util import (
                _aplicar_ajuste_absoluto_origem,
                qtd_decimal_ou_zero,
            )

            ajustes = payload.get("ajustes_por_produto") or payload.get("ajustes")
            if not isinstance(ajustes, dict):
                ajustes = {}
            q_padrao = qtd_decimal_ou_zero(
                payload.get("ajuste_quantidade")
                if payload.get("ajuste_quantidade") is not None
                else payload.get("ajuste_qtd")
            )
            if q_padrao is None:
                return JsonResponse({"ok": False, "erro": "Quantidade de ajuste inválida."}, status=400)
            for it in sol.itens.all():
                from produtos.pdv_transf_loja_util import eh_item_livre

                if eh_item_livre(it.produto_externo_id):
                    continue
                q_aj = qtd_decimal_ou_zero(ajustes.get(it.produto_externo_id)) if ajustes else q_padrao
                if q_aj is None:
                    q_aj = q_padrao
                ok_a, err_a = _aplicar_ajuste_absoluto_origem(
                    request,
                    produto_id=it.produto_externo_id,
                    deposito=sol.loja_origem,
                    saldo_informado=q_aj,
                    nome_produto=it.nome_produto,
                    codigo_interno=it.codigo_interno,
                    observacao=f"Estoque furado · cancelar Pedir loja #{sol.pk} · {label}"[:500],
                    usuario=user,
                )
                if not ok_a:
                    return JsonResponse({"ok": False, "erro": err_a}, status=400)
            motivo = str(payload.get("motivo") or payload.get("observacao") or "Estoque furado")
            if "furado" not in motivo.lower():
                motivo = f"Estoque furado · {motivo}".strip(" ·")
            payload = {**payload, "motivo": motivo[:300]}

    ok_s, err_s = aplicar_status(
        sol,
        acao,
        loja_atual=loja,
        operador_label=label,
        usuario=user,
        motivo=str(payload.get("motivo") or payload.get("observacao") or ""),
    )
    if not ok_s:
        return JsonResponse({"ok": False, "erro": err_s}, status=400)
    sol = (
        SolicitacaoTransferenciaPdv.objects.prefetch_related("itens", "eventos")
        .filter(pk=sol.pk)
        .first()
    )
    return JsonResponse(
        {
            "ok": True,
            "mensagem": sol.get_status_display(),
            "solicitacao": serializar_solicitacao(sol, com_eventos=True),
            **resumo_loja(loja),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_pdv_transf_loja_ajustar(request):
    """Ajuste rápido de saldo Agro (Centro e/ou Vila) a partir do Pedir loja."""
    payload = _payload(request) or {}
    ok, label, user, err = _operador(request, payload)
    if not ok:
        return JsonResponse({"ok": False, "erro": err, "precisa_pin": True}, status=403)

    from produtos.pdv_transf_loja_util import (
        _aplicar_ajuste_absoluto_origem,
        qtd_decimal_ou_zero,
    )

    pid = str(payload.get("produto_id") or payload.get("id") or "").strip()[:100]
    if not pid:
        return JsonResponse({"ok": False, "erro": "Produto inválido."}, status=400)
    nome = str(payload.get("nome") or payload.get("nome_produto") or "Produto").strip()[:255] or "Produto"
    codigo = str(payload.get("codigo_interno") or payload.get("codigo") or "").strip()[:100]
    tem_c = "saldo_centro" in payload or "novo_centro" in payload
    tem_v = "saldo_vila" in payload or "novo_vila" in payload
    if not tem_c and not tem_v:
        return JsonResponse({"ok": False, "erro": "Informe o saldo de Centro e/ou Vila."}, status=400)

    feitos = []
    out_c = None
    out_v = None
    if tem_c:
        q_c = qtd_decimal_ou_zero(
            payload.get("saldo_centro")
            if payload.get("saldo_centro") is not None
            else payload.get("novo_centro")
        )
        if q_c is None:
            return JsonResponse({"ok": False, "erro": "Saldo Centro inválido."}, status=400)
        ok_a, err_a = _aplicar_ajuste_absoluto_origem(
            request,
            produto_id=pid,
            deposito="centro",
            saldo_informado=q_c,
            nome_produto=nome,
            codigo_interno=codigo,
            observacao=f"Ajuste Pedir loja · {label}"[:500],
            usuario=user,
        )
        if not ok_a:
            return JsonResponse({"ok": False, "erro": err_a}, status=400)
        feitos.append("Centro")
        out_c = float(q_c)
    if tem_v:
        q_v = qtd_decimal_ou_zero(
            payload.get("saldo_vila")
            if payload.get("saldo_vila") is not None
            else payload.get("novo_vila")
        )
        if q_v is None:
            return JsonResponse({"ok": False, "erro": "Saldo Vila inválido."}, status=400)
        ok_a, err_a = _aplicar_ajuste_absoluto_origem(
            request,
            produto_id=pid,
            deposito="vila",
            saldo_informado=q_v,
            nome_produto=nome,
            codigo_interno=codigo,
            observacao=f"Ajuste Pedir loja · {label}"[:500],
            usuario=user,
        )
        if not ok_a:
            return JsonResponse({"ok": False, "erro": err_a}, status=400)
        feitos.append("Vila")
        out_v = float(q_v)

    try:
        from produtos.views import _invalidar_caches_apos_ajuste_pin

        _invalidar_caches_apos_ajuste_pin()
    except Exception:
        pass

    return JsonResponse(
        {
            "ok": True,
            "mensagem": "Saldo ajustado (" + " + ".join(feitos) + ").",
            "produto_id": pid,
            "saldo_centro": out_c,
            "saldo_vila": out_v,
        }
    )
