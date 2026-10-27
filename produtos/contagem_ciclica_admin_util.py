"""Painel administrativo — contagem cíclica em aberto (visão gerência, não cega)."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

from django.db.models import Count, Max, Q
from django.utils import timezone

from estoque.models import (
    ContagemCiclicaEscopo,
    ContagemCiclicaLinha,
    ContagemCiclicaParticipante,
    ContagemCiclicaSessao,
    ContagemCiclicaStatus,
)
from produtos.contagem_ciclica_util import listar_sessoes_abertas, sessao_payload
from produtos.pin_gerencial_util import is_usuario_gerencial, validar_pin_gerencial
from produtos.vendas_lojas_hub_sessao_util import (
    SESSION_USER_ID,
    operador_da_sessao,
    pin_da_sessao,
)


def _pin_da_request(request) -> str:
    pin = (pin_da_sessao(request) or "").strip()
    if pin:
        return pin
    return str(request.session.get("agro_operador_pin") or "").strip()


def request_pode_ver_painel_ciclica_admin(request) -> bool:
    """PIN gerencial (Geraldo / Geraldinho / Renan) ou gerência hub (códigos/nomes configurados)."""
    try:
        user = getattr(request, "user", None)
        if user is not None and getattr(user, "is_authenticated", False):
            if getattr(user, "is_staff", False) or getattr(user, "is_superuser", False):
                return True
            if is_usuario_gerencial(user):
                return True
    except Exception:
        pass
    uid = request.session.get(SESSION_USER_ID)
    if uid:
        from django.contrib.auth import get_user_model

        u = get_user_model().objects.filter(pk=uid).first()
        if u and is_usuario_gerencial(u):
            return True
    pin = _pin_da_request(request)
    if pin:
        ok, _, _ = validar_pin_gerencial(pin)
        if ok:
            return True
        from produtos.vendas_lojas_hub_acesso_util import operador_liberado_vendas_tarefas

        if operador_liberado_vendas_tarefas(pin):
            return True
    return False


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v if v is not None else 0).replace(",", "."))
    except Exception:
        return Decimal("0")


def _escopo_label(sessao: ContagemCiclicaSessao) -> str:
    tipo = str(sessao.escopo_tipo or "")
    val = str(sessao.escopo_valor or "").strip()
    if tipo == ContagemCiclicaEscopo.CATEGORIA and val:
        return f"Categoria · {val}"
    if tipo == ContagemCiclicaEscopo.CORREDOR and val:
        return f"Corredor · {val}"
    if tipo == ContagemCiclicaEscopo.LOJA:
        return "Loja inteira"
    return val or tipo or "—"


def _progresso_sessao(sessao: ContagemCiclicaSessao) -> dict[str, Any]:
    total = int(sessao.total_itens or 0)
    if sessao.status == ContagemCiclicaStatus.PASS2:
        fila = ContagemCiclicaLinha.objects.filter(sessao=sessao, precisa_recontagem=True).count()
        ok2 = ContagemCiclicaLinha.objects.filter(
            sessao=sessao, precisa_recontagem=True, contado_pass2=True
        ).count()
        denom = max(fila, 1)
        pct = round(100.0 * ok2 / denom, 1)
        return {
            "fase": "recontagem",
            "pct": pct,
            "contados": ok2,
            "total": fila,
            "faltam": max(0, fila - ok2),
        }
    if sessao.escopo_tipo == ContagemCiclicaEscopo.CORREDOR:
        contados = ContagemCiclicaLinha.objects.filter(sessao=sessao, contado_pass1=True).count()
        total_lin = ContagemCiclicaLinha.objects.filter(sessao=sessao).count()
        return {
            "fase": "passagem_1",
            "pct": 0.0 if total_lin <= 0 else round(100.0 * contados / total_lin, 1),
            "contados": contados,
            "total": total_lin,
            "faltam": 0,
            "corredor": True,
        }
    c1 = ContagemCiclicaLinha.objects.filter(sessao=sessao, contado_pass1=True).count()
    total = total or ContagemCiclicaLinha.objects.filter(sessao=sessao).count()
    denom = max(total, 1)
    return {
        "fase": "passagem_1",
        "pct": round(100.0 * c1 / denom, 1),
        "contados": c1,
        "total": total,
        "faltam": max(0, total - c1),
    }


def _variacao_linha(ln: ContagemCiclicaLinha, sessao: ContagemCiclicaSessao) -> tuple[float, float]:
    saldo = float(ln.saldo_referencia or 0)
    if sessao.status == ContagemCiclicaStatus.PASS2 and ln.precisa_recontagem:
        q = ln.qtd_pass2 if ln.contado_pass2 and ln.qtd_pass2 is not None else ln.qtd_pass1
    else:
        q = ln.qtd_pass1 if ln.contado_pass1 and ln.qtd_pass1 is not None else None
    if q is None:
        return 0.0, 0.0
    qf = float(q)
    diff = qf - saldo
    impacto = diff * float(ln.custo_ref or 0)
    return diff, impacto


def _impacto_contado_sessao(sessao: ContagemCiclicaSessao) -> float:
    total = Decimal("0")
    qs = ContagemCiclicaLinha.objects.filter(sessao=sessao)
    if sessao.status == ContagemCiclicaStatus.PASS2:
        qs = qs.filter(precisa_recontagem=True, contado_pass2=True)
    else:
        qs = qs.filter(contado_pass1=True)
    for ln in qs.only("saldo_referencia", "qtd_pass1", "qtd_pass2", "custo_ref", "contado_pass1", "contado_pass2", "precisa_recontagem"):
        _, imp = _variacao_linha(ln, sessao)
        total += _dec(imp)
    return round(float(total), 2)


def sessao_card_resumo(sessao: ContagemCiclicaSessao) -> dict[str, Any]:
    base = sessao_payload(sessao, detalhe=False)
    prog = _progresso_sessao(sessao)
    agora = timezone.now()
    idade_min = int((agora - sessao.aberta_em).total_seconds() // 60) if sessao.aberta_em else 0
    parts_qs = ContagemCiclicaParticipante.objects.filter(sessao=sessao).order_by("-ultimo_ping_em")
    participantes = []
    for p in parts_qs:
        ping = p.ultimo_ping_em
        stale = bool(ping and (agora - ping) > timedelta(minutes=12))
        participantes.append(
            {
                "nome": p.operador_rotulo,
                "ultimo_ping": ping.isoformat() if ping else "",
                "stale": stale,
            }
        )
    agg = ContagemCiclicaLinha.objects.filter(sessao=sessao).aggregate(
        divergentes=Count(
            "id",
            filter=Q(contado_pass1=True) & Q(precisa_recontagem=True),
        ),
        ultima_contagem=Max("contado_pass1_em"),
    )
    return {
        "id": base["id"],
        "deposito": base["deposito"],
        "deposito_label": "Vila Elias" if sessao.deposito == "vila" else "Centro",
        "escopo_label": _escopo_label(sessao),
        "escopo_valor": base.get("escopo_valor"),
        "status": base["status"],
        "progresso": prog,
        "aberta_por": base.get("aberta_por"),
        "aberta_em": base.get("aberta_em"),
        "idade_minutos": idade_min,
        "participantes": participantes,
        "divergentes_pass1": int(agg.get("divergentes") or 0),
        "ultima_contagem_em": (
            agg["ultima_contagem"].isoformat() if agg.get("ultima_contagem") else ""
        ),
        "impacto_variacao_rs": _impacto_contado_sessao(sessao),
    }


def sessao_payload_admin(
    sessao: ContagemCiclicaSessao,
    *,
    q: str = "",
    limite_linhas: int = 250,
) -> dict[str, Any]:
    base = sessao_payload(sessao, detalhe=False)
    prog = _progresso_sessao(sessao)
    agora = timezone.now()
    aberta_em = sessao.aberta_em
    idade_min = int((agora - aberta_em).total_seconds() // 60) if aberta_em else 0

    parts_qs = ContagemCiclicaParticipante.objects.filter(sessao=sessao).order_by("-ultimo_ping_em")
    participantes = []
    for p in parts_qs:
        ping = p.ultimo_ping_em
        stale = False
        if ping:
            stale = (agora - ping) > timedelta(minutes=12)
        participantes.append(
            {
                "nome": p.operador_rotulo,
                "ultimo_ping": ping.isoformat() if ping else "",
                "entrou_em": p.entrou_em.isoformat() if p.entrou_em else "",
                "stale": stale,
            }
        )

    agg = ContagemCiclicaLinha.objects.filter(sessao=sessao).aggregate(
        auto_zero=Count("id", filter=Q(auto_zero_pass1=True)),
        divergentes=Count(
            "id",
            filter=Q(contado_pass1=True) & Q(precisa_recontagem=True),
        ),
        ultima_contagem=Max("contado_pass1_em"),
    )

    linhas_qs = ContagemCiclicaLinha.objects.filter(sessao=sessao)
    termo = (q or "").strip().lower()
    if termo:
        linhas_qs = linhas_qs.filter(
            Q(nome_produto__icontains=termo)
            | Q(codigo_interno__icontains=termo)
            | Q(produto_externo_id__icontains=termo)
            | Q(operador_pass1__icontains=termo)
            | Q(operador_pass2__icontains=termo)
        )
    if sessao.status == ContagemCiclicaStatus.PASS2:
        linhas_qs = linhas_qs.filter(precisa_recontagem=True).order_by(
            "contado_pass2", "-saldo_referencia", "nome_produto"
        )
    else:
        linhas_qs = linhas_qs.order_by("-contado_pass1", "nome_produto")

    total_q = linhas_qs.count()
    linhas_raw = list(linhas_qs[: max(1, min(limite_linhas, 500))])
    impacto_total = Decimal("0")
    linhas_out = []
    for ln in linhas_raw:
        diff, impacto = _variacao_linha(ln, sessao)
        impacto_total += _dec(impacto)
        contado = (
            ln.contado_pass2
            if sessao.status == ContagemCiclicaStatus.PASS2
            else ln.contado_pass1
        )
        qtd = (
            ln.qtd_pass2
            if sessao.status == ContagemCiclicaStatus.PASS2
            else ln.qtd_pass1
        )
        linhas_out.append(
            {
                "id": ln.pk,
                "produto_id": ln.produto_externo_id,
                "codigo": ln.codigo_interno,
                "nome": ln.nome_produto or ln.codigo_interno or ln.produto_externo_id,
                "categoria": ln.categoria,
                "contado": contado,
                "saldo_ref": float(ln.saldo_referencia or 0),
                "qtd": float(qtd) if qtd is not None else None,
                "variacao": diff,
                "impacto_rs": round(impacto, 2),
                "operador": ln.operador_pass2
                if sessao.status == ContagemCiclicaStatus.PASS2
                else ln.operador_pass1,
                "auto_zero": ln.auto_zero_pass1,
                "precisa_recontagem": ln.precisa_recontagem,
            }
        )

    return {
        **base,
        "escopo_label": _escopo_label(sessao),
        "deposito_label": "Vila Elias" if sessao.deposito == "vila" else "Centro",
        "progresso": prog,
        "idade_minutos": idade_min,
        "participantes_detalhe": participantes,
        "auto_zero": int(agg.get("auto_zero") or 0),
        "divergentes_pass1": int(agg.get("divergentes") or 0),
        "ultima_contagem_em": (
            agg["ultima_contagem"].isoformat() if agg.get("ultima_contagem") else ""
        ),
        "impacto_variacao_rs": round(float(impacto_total), 2),
        "linhas_admin": linhas_out,
        "linhas_admin_total": total_q,
        "linhas_admin_truncadas": total_q > len(linhas_out),
        "cego": False,
    }


def painel_dashboard_payload(
    *,
    deposito: str = "",
    busca: str = "",
) -> dict[str, Any]:
    sessoes = listar_sessoes_abertas(deposito or None)
    termo = (busca or "").strip().lower()
    cards = []
    pct_sum = 0.0
    operadores = set()
    impacto_global = Decimal("0")
    for s in sessoes:
        card = sessao_card_resumo(s)
        if termo:
            blob = " ".join(
                [
                    card.get("escopo_label") or "",
                    str(card.get("escopo_valor") or ""),
                    card.get("aberta_por") or "",
                    " ".join(p.get("nome") or "" for p in card.get("participantes") or []),
                ]
            ).lower()
            if termo not in blob:
                continue
        prog = card["progresso"]
        pct_sum += float(prog.get("pct") or 0)
        for p in card.get("participantes") or []:
            if p.get("nome"):
                operadores.add(p["nome"])
        impacto_global += _dec(card.get("impacto_variacao_rs") or 0)
        cards.append(card)

    n = len(cards)
    from estoque.models import ContagemCiclicaBloco

    blocos = []
    for b in ContagemCiclicaBloco.objects.select_related("sessao").order_by("-fechado_em")[:30]:
        blocos.append(
            {
                "id": b.pk,
                "sessao_id": b.sessao_id,
                "deposito": b.sessao.deposito,
                "escopo": (b.sessao.escopo_valor or b.sessao.escopo_tipo or "")[:80],
                "itens": b.itens_gravados,
                "operador": b.operador_rotulo,
                "quando": b.fechado_em.isoformat() if b.fechado_em else "",
                "url": f"/vendas/lojas/contagem-ciclica/bloco/{b.pk}/",
            }
        )
    return {
        "ok": True,
        "atualizado_em": timezone.now().isoformat(),
        "resumo": {
            "sessoes_abertas": n,
            "progresso_medio_pct": round(pct_sum / n, 1) if n else 0.0,
            "operadores_ativos": len(operadores),
            "impacto_variacao_rs": round(float(impacto_global), 2),
        },
        "sessoes": cards,
        "blocos": blocos,
    }


def atividade_recente_payload(limite: int = 40) -> list[dict[str, Any]]:
    """Últimas linhas contadas em sessões abertas (feed estilo WMS)."""
    sess_ids = list(
        ContagemCiclicaSessao.objects.filter(
            status__in=(ContagemCiclicaStatus.PASS1, ContagemCiclicaStatus.PASS2)
        ).values_list("pk", flat=True)[:40]
    )
    if not sess_ids:
        return []
    out: list[dict[str, Any]] = []
    linhas = (
        ContagemCiclicaLinha.objects.filter(sessao_id__in=sess_ids)
        .exclude(contado_pass1_em__isnull=True)
        .select_related("sessao")
        .order_by("-contado_pass1_em")[:limite]
    )
    for ln in linhas:
        out.append(
            {
                "quando": ln.contado_pass1_em.isoformat() if ln.contado_pass1_em else "",
                "sessao_id": ln.sessao_id,
                "deposito": ln.sessao.deposito,
                "escopo": _escopo_label(ln.sessao),
                "operador": ln.operador_pass1,
                "nome": (ln.nome_produto or ln.codigo_interno or "")[:80],
                "qtd": float(ln.qtd_pass1 or 0),
            }
        )
    return out
