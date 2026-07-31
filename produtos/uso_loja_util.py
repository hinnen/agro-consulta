"""Uso loja — saída de estoque para consumo interno (Postgres)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from django.db import transaction
from django.utils import timezone

from estoque.models import AjusteRapidoEstoque, OrigemAjusteEstoque
from produtos.models import UsoLojaRetiradaAgro, UsoLojaRetiradaItemAgro
from produtos.pdv_deposito_util import (
    DEPOSITO_CENTRO,
    DEPOSITO_VILA,
    normalizar_deposito,
    rotulo_deposito,
)

MOTIVOS_VALIDOS = frozenset(c.value for c in UsoLojaRetiradaAgro.Motivo)
MOTIVO_LABEL = {c.value: c.label for c in UsoLojaRetiradaAgro.Motivo}


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v).replace(",", ".").strip()).quantize(Decimal("0.001"))
    except Exception:
        return Decimal("0.000")


def _erp_ref_congelado(pid: str, dep: str) -> Decimal:
    aj = (
        AjusteRapidoEstoque.objects.filter(produto_externo_id=pid[:100], deposito=dep)
        .order_by("-criado_em", "-id")
        .only("saldo_erp_referencia")
        .first()
    )
    if aj is None:
        return Decimal("0.000")
    return _dec(aj.saldo_erp_referencia)


def _saldo_operacional(pid: str, dep: str) -> Decimal:
    from produtos.estoque_saldo_agro_util import mapa_saldos_operacionais_agro

    info = (mapa_saldos_operacionais_agro([pid], db=None, client=None) or {}).get(pid) or {}
    if dep == DEPOSITO_VILA:
        return _dec(info.get("saldo_vila", 0))
    return _dec(info.get("saldo_centro", 0))


def _empresa_loja(dep: str):
    from base.models import Empresa, Loja

    d = normalizar_deposito(dep)
    empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
    loja = None
    if empresa:
        if d == DEPOSITO_VILA:
            loja = Loja.objects.filter(empresa=empresa, nome__icontains="vila").first()
        else:
            loja = Loja.objects.filter(empresa=empresa, nome__icontains="centro").first()
    return empresa, loja


def resolver_deposito_uso_loja(request, deposito_payload: str | None) -> tuple[str | None, str]:
    """
    Com caixa aberto → depósito do turno.
    Sem caixa → usa o que o operador escolheu no overlay (obrigatório).
    """
    from produtos.pdv_deposito_util import trava_loja_por_caixa

    trava = trava_loja_por_caixa(request)
    if trava and trava.get("deposito") in (DEPOSITO_CENTRO, DEPOSITO_VILA):
        return trava["deposito"], ""
    raw = normalizar_deposito(deposito_payload or "")
    if raw not in (DEPOSITO_CENTRO, DEPOSITO_VILA):
        return None, "Escolha Centro ou Vila Elias (caixa fechado)."
    return raw, ""


@transaction.atomic
def confirmar_retirada_uso_loja(
    *,
    deposito: str,
    itens: list[dict[str, Any]],
    quem_levou: str,
    motivo: str,
    operador_label: str,
    usuario_django=None,
    sessao_caixa=None,
    observacao: str = "",
) -> tuple[UsoLojaRetiradaAgro | None, str]:
    dep = normalizar_deposito(deposito)
    if dep not in (DEPOSITO_CENTRO, DEPOSITO_VILA):
        return None, "Depósito inválido."
    if not itens:
        return None, "Adicione ao menos um produto."
    mot = (motivo or "").strip().lower()
    if mot and mot not in MOTIVOS_VALIDOS:
        return None, "Motivo inválido."
    quem = (quem_levou or "").strip() or (operador_label or "").strip()
    if not quem:
        return None, "Informe quem levou ou use o PIN."
    op = (operador_label or quem)[:120]
    empresa, loja = _empresa_loja(dep)

    linhas_ok: list[dict[str, Any]] = []
    for raw in itens:
        pid = str(raw.get("produto_id") or raw.get("id") or "").strip()
        if not pid:
            continue
        qtd = _dec(raw.get("quantidade") or raw.get("qtd") or 0)
        if qtd <= 0:
            continue
        linhas_ok.append(
            {
                "produto_id": pid[:100],
                "quantidade": qtd,
                "nome": str(raw.get("nome") or raw.get("nome_produto") or "")[:255],
                "codigo": str(raw.get("codigo") or raw.get("codigo_interno") or "")[:100],
            }
        )
    if not linhas_ok:
        return None, "Nenhum item com quantidade válida."

    retirada = UsoLojaRetiradaAgro.objects.create(
        deposito=dep,
        quem_levou=quem[:120],
        motivo=mot,
        operador_pin=op,
        usuario=usuario_django if usuario_django is not None else None,
        sessao_caixa=sessao_caixa,
        observacao=(observacao or "")[:2000],
    )

    for ln in linhas_ok:
        pid = ln["produto_id"]
        qtd = ln["quantidade"]
        saldo_antes = _saldo_operacional(pid, dep)
        saldo_depois = (saldo_antes - qtd).quantize(Decimal("0.001"))
        erp_ref = _erp_ref_congelado(pid, dep)
        nome = ln["nome"] or pid
        obs = (
            f"Uso loja #{retirada.pk} · {quem[:60]} · {rotulo_deposito(dep)}"
            + (f" · {MOTIVO_LABEL.get(mot, mot)}" if mot else "")
        )[:2000]
        adj = AjusteRapidoEstoque.objects.create(
            empresa=empresa,
            loja=loja,
            produto_externo_id=pid,
            codigo_interno=ln["codigo"],
            nome_produto=(f"{nome[:120]} · Uso loja #{retirada.pk} ({op})")[:255],
            deposito=dep,
            saldo_erp_referencia=erp_ref,
            saldo_informado=saldo_depois,
            origem=OrigemAjusteEstoque.USO_LOJA,
            usuario=usuario_django if usuario_django is not None else None,
            observacao=obs,
        )
        UsoLojaRetiradaItemAgro.objects.create(
            retirada=retirada,
            produto_externo_id=pid,
            codigo_interno=ln["codigo"],
            nome_produto=nome[:255],
            quantidade=qtd,
            ajuste=adj,
        )
    return retirada, ""


@transaction.atomic
def estornar_retirada_uso_loja(
    *,
    retirada: UsoLojaRetiradaAgro,
    operador_label: str,
    usuario_django=None,
) -> tuple[bool, str]:
    if retirada.estornado:
        return False, "Esta saída já foi estornada."
    op = (operador_label or "").strip()[:120] or "Operador"
    dep = normalizar_deposito(retirada.deposito)
    empresa, loja = _empresa_loja(dep)
    itens = list(retirada.itens.select_related("ajuste").all())
    if not itens:
        return False, "Retirada sem itens."

    for it in itens:
        qtd = _dec(it.quantidade)
        if qtd <= 0:
            continue
        pid = str(it.produto_externo_id or "").strip()[:100]
        saldo_antes = _saldo_operacional(pid, dep)
        saldo_depois = (saldo_antes + qtd).quantize(Decimal("0.001"))
        erp_ref = _erp_ref_congelado(pid, dep)
        nome = (it.nome_produto or pid)[:120]
        adj = AjusteRapidoEstoque.objects.create(
            empresa=empresa,
            loja=loja,
            produto_externo_id=pid,
            codigo_interno=str(it.codigo_interno or "")[:100],
            nome_produto=(f"{nome} · Estorno uso loja #{retirada.pk} ({op})")[:255],
            deposito=dep,
            saldo_erp_referencia=erp_ref,
            saldo_informado=saldo_depois,
            origem=OrigemAjusteEstoque.ESTORNO_USO_LOJA,
            usuario=usuario_django if usuario_django is not None else None,
            observacao=f"Estorno uso loja #{retirada.pk} · {op}"[:2000],
        )
        it.ajuste_estorno = adj
        it.save(update_fields=["ajuste_estorno"])

    retirada.estornado = True
    retirada.estornado_em = timezone.now()
    retirada.estornado_por = op
    retirada.save(update_fields=["estornado", "estornado_em", "estornado_por"])
    return True, ""


def serializar_retirada(r: UsoLojaRetiradaAgro) -> dict:
    itens = []
    for it in r.itens.all():
        itens.append(
            {
                "id": it.pk,
                "produto_id": it.produto_externo_id,
                "codigo": it.codigo_interno,
                "nome": it.nome_produto,
                "quantidade": float(it.quantidade),
            }
        )
    return {
        "id": r.pk,
        "deposito": r.deposito,
        "deposito_label": rotulo_deposito(r.deposito),
        "quem_levou": r.quem_levou,
        "motivo": r.motivo,
        "motivo_label": MOTIVO_LABEL.get(r.motivo, "") if r.motivo else "",
        "operador_pin": r.operador_pin,
        "criado_em": r.criado_em.isoformat() if r.criado_em else "",
        "estornado": bool(r.estornado),
        "estornado_em": r.estornado_em.isoformat() if r.estornado_em else "",
        "estornado_por": r.estornado_por or "",
        "itens": itens,
        "itens_count": len(itens),
    }
