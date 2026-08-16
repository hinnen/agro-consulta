"""Solicitação de transferência entre lojas no PDV (Centro ↔ Vila)."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.contrib.auth import get_user_model
from django.db import transaction
from django.utils import timezone

from estoque.models import (
    SolicitacaoTransferenciaPdv,
    SolicitacaoTransferenciaPdvEvento,
    SolicitacaoTransferenciaPdvItem,
)
from produtos.caixa_util import operador_label_de_pin, usuario_django_de_pin
from produtos.pdv_deposito_util import DEPOSITOS_VALIDOS, normalizar_deposito, rotulo_deposito

STATUS_PENDENTE = SolicitacaoTransferenciaPdv.STATUS_PENDENTE
STATUS_ACEITO = SolicitacaoTransferenciaPdv.STATUS_ACEITO
STATUS_PRONTO = SolicitacaoTransferenciaPdv.STATUS_PRONTO
STATUS_CONCLUIDO = SolicitacaoTransferenciaPdv.STATUS_CONCLUIDO
STATUS_CANCELADO = SolicitacaoTransferenciaPdv.STATUS_CANCELADO

ACOES_STATUS = {
    "aceitar": STATUS_ACEITO,
    "pronto": STATUS_PRONTO,
    "cancelar": STATUS_CANCELADO,
}

TRANSICOES = {
    (STATUS_PENDENTE, "aceitar"): STATUS_ACEITO,
    (STATUS_PENDENTE, "cancelar"): STATUS_CANCELADO,
    (STATUS_ACEITO, "pronto"): STATUS_PRONTO,
    (STATUS_ACEITO, "cancelar"): STATUS_CANCELADO,
    (STATUS_PRONTO, "cancelar"): STATUS_CANCELADO,
}

ACOES_ORIGEM = frozenset({"aceitar", "pronto"})
ACOES_QUALQUER_LOJA = frozenset({"cancelar", "transferir"})


def loja_oposta(deposito: str) -> str:
    dep = normalizar_deposito(deposito)
    return "vila" if dep == "centro" else "centro"


def qtd_decimal(valor) -> Decimal | None:
    if isinstance(valor, Decimal):
        q = valor
    else:
        raw = str(valor or "").strip().replace(" ", "")
        if not raw:
            return None
        if "," in raw and "." in raw:
            raw = raw.replace(".", "").replace(",", ".")
        elif "," in raw:
            raw = raw.replace(",", ".")
        try:
            q = Decimal(raw)
        except (InvalidOperation, ValueError):
            return None
    if q <= 0:
        return None
    return q.quantize(Decimal("0.001"))


def gravar_operador_sessao_pdv(request, pin: str) -> tuple[bool, str, object | None, str]:
    ok, label, err = operador_label_de_pin(pin)
    if not ok:
        return False, "", None, err
    user = usuario_django_de_pin(pin)
    request.session["pdv_operador_nome"] = (label or "")[:120]
    request.session["mobile_auth"] = True
    if user is not None and getattr(user, "pk", None):
        request.session["pdv_operador_user_id"] = int(user.pk)
    else:
        request.session.pop("pdv_operador_user_id", None)
    request.session.modified = True
    return True, label[:150], user, ""


def resolver_operador_pdv(request, pin: str = "") -> tuple[bool, str, object | None, str]:
    """Usa PIN informado ou o operador já logado no PDV (sem redigitar)."""
    pin = (pin or "").strip()
    if pin:
        return gravar_operador_sessao_pdv(request, pin)
    label = str(request.session.get("pdv_operador_nome") or "").strip()
    if not label:
        return False, "", None, "Entre com o PIN no PDV (botão PIN) para registrar a ação."
    uid = request.session.get("pdv_operador_user_id")
    user = None
    if uid:
        user = get_user_model().objects.filter(pk=uid).first()
    return True, label[:150], user, ""


def _normalizar_itens(itens_raw) -> tuple[list[dict], str]:
    if not isinstance(itens_raw, list) or not itens_raw:
        return [], "Inclua ao menos um produto."
    saida = []
    vistos = set()
    for raw in itens_raw[:40]:
        if not isinstance(raw, dict):
            continue
        pid = str(raw.get("produto_id") or raw.get("id") or "").strip()[:100]
        if not pid or pid in vistos:
            continue
        qtd = qtd_decimal(raw.get("quantidade") or raw.get("qtd") or raw.get("qty"))
        if qtd is None:
            return [], "Quantidade inválida em um dos itens."
        nome = str(raw.get("nome") or raw.get("nome_produto") or "Produto").strip()[:255] or "Produto"
        codigo = str(raw.get("codigo_interno") or raw.get("codigo") or "").strip()[:100]
        vistos.add(pid)
        saida.append(
            {
                "produto_externo_id": pid,
                "nome_produto": nome,
                "codigo_interno": codigo,
                "quantidade": qtd,
            }
        )
    if not saida:
        return [], "Inclua ao menos um produto."
    return saida, ""


def criar_solicitacao(*, loja_destino: str, itens_raw, observacao: str, operador_label: str, usuario):
    dest = normalizar_deposito(loja_destino)
    if dest not in DEPOSITOS_VALIDOS:
        return None, "Loja inválida."
    origem = loja_oposta(dest)
    itens, err = _normalizar_itens(itens_raw)
    if err:
        return None, err
    obs = str(observacao or "").strip()[:400]
    with transaction.atomic():
        sol = SolicitacaoTransferenciaPdv.objects.create(
            loja_origem=origem,
            loja_destino=dest,
            status=STATUS_PENDENTE,
            observacao=obs,
            criado_por_label=(operador_label or "")[:150],
            criado_por=usuario,
        )
        SolicitacaoTransferenciaPdvItem.objects.bulk_create(
            [
                SolicitacaoTransferenciaPdvItem(
                    solicitacao=sol,
                    produto_externo_id=it["produto_externo_id"],
                    nome_produto=it["nome_produto"],
                    codigo_interno=it["codigo_interno"],
                    quantidade=it["quantidade"],
                )
                for it in itens
            ]
        )
        _registrar_evento(
            sol,
            acao="pedir",
            status_de="",
            status_para=STATUS_PENDENTE,
            operador_label=operador_label,
            usuario=usuario,
            observacao=obs,
        )
    return sol, ""


def pode_agir(sol: SolicitacaoTransferenciaPdv, loja_atual: str, acao: str) -> tuple[bool, str]:
    loja = normalizar_deposito(loja_atual)
    acao = (acao or "").strip().lower()
    if acao in ACOES_ORIGEM and loja != sol.loja_origem:
        return False, f"Só a loja {rotulo_deposito(sol.loja_origem)} pode {acao} este pedido."
    if acao in ACOES_QUALQUER_LOJA and loja not in (sol.loja_origem, sol.loja_destino):
        return False, "Esta loja não participa deste pedido."
    if acao == "transferir":
        if sol.status not in (STATUS_ACEITO, STATUS_PRONTO):
            return False, "Aceite o pedido (e, se quiser, marque Pronto) antes de transferir o estoque."
        return True, ""
    destino = TRANSICOES.get((sol.status, acao))
    if not destino:
        return False, "Esta ação não vale para o status atual."
    return True, ""


def aplicar_status(
    sol: SolicitacaoTransferenciaPdv,
    acao: str,
    *,
    loja_atual: str,
    operador_label: str,
    usuario,
    motivo: str = "",
) -> tuple[bool, str]:
    acao = (acao or "").strip().lower()
    ok, err = pode_agir(sol, loja_atual, acao)
    if not ok:
        return False, err
    novo = TRANSICOES.get((sol.status, acao))
    if not novo:
        return False, "Esta ação não vale para o status atual."
    agora = timezone.now()
    label = (operador_label or "")[:150]
    de = sol.status
    sol.status = novo
    if novo == STATUS_ACEITO:
        sol.aceito_em = agora
        sol.aceito_por_label = label
        sol.aceito_por = usuario
    elif novo == STATUS_PRONTO:
        sol.pronto_em = agora
        sol.pronto_por_label = label
        sol.pronto_por = usuario
    elif novo == STATUS_CANCELADO:
        sol.cancelado_em = agora
        sol.cancelado_por_label = label
        sol.cancelado_por = usuario
        sol.cancelado_motivo = str(motivo or "").strip()[:300]
    sol.save()
    _registrar_evento(
        sol,
        acao=acao,
        status_de=de,
        status_para=novo,
        operador_label=label,
        usuario=usuario,
        observacao=sol.cancelado_motivo if novo == STATUS_CANCELADO else "",
    )
    return True, ""


def concluir_transferencia(
    request,
    sol: SolicitacaoTransferenciaPdv,
    *,
    loja_atual: str,
    operador_label: str,
    usuario,
) -> tuple[bool, str, list]:
    ok, err = pode_agir(sol, loja_atual, "transferir")
    if not ok:
        return False, err, []
    from estoque.views import _transferir_entre_depositos_exec

    itens = list(sol.itens.all())
    if not itens:
        return False, "Pedido sem itens.", []
    resultados = []
    with transaction.atomic():
        for it in itens:
            res = _transferir_entre_depositos_exec(
                request,
                "",
                it.produto_externo_id,
                it.quantidade,
                it.nome_produto,
                it.codigo_interno,
                f"PDV #{sol.pk} · {operador_label}"[:500],
                origem=sol.loja_origem,
                destino=sol.loja_destino,
                registrar_historico=True,
                invalidar_cache=False,
                pular_validacao_pin=True,
                usuario_label_override=operador_label,
            )
            if not res.get("ok"):
                return False, res.get("erro") or "Falha ao transferir estoque.", resultados
            resultados.append(res)
        de = sol.status
        agora = timezone.now()
        sol.status = STATUS_CONCLUIDO
        sol.concluido_em = agora
        sol.concluido_por_label = (operador_label or "")[:150]
        sol.concluido_por = usuario
        sol.save()
        _registrar_evento(
            sol,
            acao="transferir",
            status_de=de,
            status_para=STATUS_CONCLUIDO,
            operador_label=operador_label,
            usuario=usuario,
            observacao="",
        )
    from produtos.views import _invalidar_caches_apos_ajuste_pin

    try:
        _invalidar_caches_apos_ajuste_pin()
    except Exception:
        pass
    return True, "", resultados


def _registrar_evento(sol, *, acao, status_de, status_para, operador_label, usuario, observacao=""):
    SolicitacaoTransferenciaPdvEvento.objects.create(
        solicitacao=sol,
        acao=(acao or "")[:30],
        status_de=(status_de or "")[:20],
        status_para=(status_para or "")[:20],
        operador_label=(operador_label or "")[:150],
        operador=usuario,
        observacao=(observacao or "")[:400],
    )


def serializar_item(it: SolicitacaoTransferenciaPdvItem) -> dict:
    return {
        "id": it.pk,
        "produto_id": it.produto_externo_id,
        "nome": it.nome_produto,
        "codigo_interno": it.codigo_interno,
        "quantidade": float(it.quantidade),
        "quantidade_texto": _fmt_qtd(it.quantidade),
    }


def serializar_solicitacao(sol: SolicitacaoTransferenciaPdv, *, com_eventos: bool = False) -> dict:
    itens = [serializar_item(it) for it in sol.itens.all()]
    data = {
        "id": sol.pk,
        "loja_origem": sol.loja_origem,
        "loja_destino": sol.loja_destino,
        "loja_origem_label": rotulo_deposito(sol.loja_origem),
        "loja_destino_label": rotulo_deposito(sol.loja_destino),
        "status": sol.status,
        "status_label": sol.get_status_display(),
        "observacao": sol.observacao,
        "criado_em": sol.criado_em.isoformat() if sol.criado_em else "",
        "atualizado_em": sol.atualizado_em.isoformat() if sol.atualizado_em else "",
        "criado_por": sol.criado_por_label,
        "aceito_por": sol.aceito_por_label,
        "pronto_por": sol.pronto_por_label,
        "concluido_por": sol.concluido_por_label,
        "cancelado_por": sol.cancelado_por_label,
        "cancelado_motivo": sol.cancelado_motivo,
        "itens": itens,
        "qtd_itens": len(itens),
        "resumo": ", ".join(f"{i['nome']} × {i['quantidade_texto']}" for i in itens[:4]),
    }
    if com_eventos:
        data["eventos"] = [
            {
                "acao": ev.acao,
                "status_de": ev.status_de,
                "status_para": ev.status_para,
                "operador": ev.operador_label,
                "observacao": ev.observacao,
                "criado_em": ev.criado_em.isoformat() if ev.criado_em else "",
            }
            for ev in sol.eventos.all()[:30]
        ]
    return data


def _fmt_qtd(q: Decimal) -> str:
    s = f"{q.normalize():f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def resumo_loja(loja: str) -> dict:
    loja = normalizar_deposito(loja)
    abertos = SolicitacaoTransferenciaPdv.objects.filter(
        loja_origem=loja,
        status__in=SolicitacaoTransferenciaPdv.STATUS_ABERTOS,
    )
    pendentes = abertos.filter(status=STATUS_PENDENTE).count()
    return {
        "loja": loja,
        "loja_label": rotulo_deposito(loja),
        "recebidos_abertos": abertos.count(),
        "recebidos_pendentes": pendentes,
        "enviados_abertos": SolicitacaoTransferenciaPdv.objects.filter(
            loja_destino=loja,
            status__in=SolicitacaoTransferenciaPdv.STATUS_ABERTOS,
        ).count(),
    }
