"""Recibo 80mm de pagamento fiado (FL-019)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from django.db.models import Q
from django.utils import timezone

from produtos.caixa_util import format_moeda_br
from produtos.models import FiadoBaixaAgro, FiadoEventoAgro, FiadoTituloAgro


def _dec(val) -> Decimal:
    try:
        if val is None:
            return Decimal("0")
        return Decimal(str(val).replace(",", ".")).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


def _fmt_dt(dt) -> str:
    if not dt:
        return ""
    if isinstance(dt, str):
        return str(dt)[:16].replace("T", " ")
    if timezone.is_aware(dt):
        dt = timezone.localtime(dt)
    if isinstance(dt, datetime):
        return dt.strftime("%d/%m/%Y %H:%M")
    return str(dt)


def _moeda_rs(val) -> str:
    return f"R$ {format_moeda_br(val)}"


def saldo_aberto_cliente_fiado(
    *,
    cliente_agro_pk: int | None = None,
    cliente_nome: str = "",
    cliente_codigo: str = "",
) -> Decimal:
    filtros = Q()
    if cliente_agro_pk:
        filtros = Q(cliente_agro_id=int(cliente_agro_pk))
    elif (cliente_nome or "").strip():
        filtros = Q(cliente_nome__iexact=(cliente_nome or "").strip())
        if (cliente_codigo or "").strip():
            filtros &= Q(cliente_codigo=str(cliente_codigo).strip())
    if not filtros:
        return Decimal("0.00")
    qs = FiadoTituloAgro.objects.filter(filtros).exclude(
        situacao__in=(
            FiadoTituloAgro.Situacao.QUITADO,
            FiadoTituloAgro.Situacao.CANCELADO,
        )
    )
    total = Decimal("0.00")
    for t in qs.only("valor_bruto", "valor_pago", "situacao"):
        total += t.saldo_aberto
    return total.quantize(Decimal("0.01"))


def _baixas_por_ids(ids: list[int]) -> list[FiadoBaixaAgro]:
    if not ids:
        return []
    found = {
        b.pk: b
        for b in FiadoBaixaAgro.objects.select_related("titulo", "titulo__cliente_agro").filter(pk__in=ids)
    }
    return [found[i] for i in ids if i in found]


def montar_recibo_pagamento_fiado(
    *,
    recibo_id: int | None = None,
    baixas_ids: list[int] | None = None,
    segunda_via: bool = False,
) -> dict[str, Any]:
    evento = None
    snap: dict[str, Any] = {}
    ids: list[int] = []
    if recibo_id:
        evento = FiadoEventoAgro.objects.filter(pk=int(recibo_id), tipo=FiadoEventoAgro.Tipo.BAIXA).first()
        if not evento:
            raise ValueError("Recibo não encontrado.")
        snap = evento.payload_json if isinstance(evento.payload_json, dict) else {}
        if str(snap.get("origem") or "") == "pdv_idempotencia":
            res = snap.get("resultado") if isinstance(snap.get("resultado"), dict) else {}
            rid = res.get("recibo_id")
            if rid:
                return montar_recibo_pagamento_fiado(recibo_id=int(rid), segunda_via=segunda_via)
            ids = [int(x) for x in (res.get("baixas_ids") or []) if x]
        else:
            ids = [int(x) for x in (snap.get("baixas_ids") or []) if x]
            if not ids and evento.baixa_id:
                ids = [int(evento.baixa_id)]
    if baixas_ids:
        ids = [int(x) for x in baixas_ids if x]
    baixas = _baixas_por_ids(ids)
    if not baixas:
        raise ValueError("Nenhum pagamento encontrado para este recibo.")

    t0 = baixas[0].titulo
    cliente_nome = (snap.get("cliente_nome") or t0.cliente_nome or "").strip()
    cliente_pk = snap.get("cliente_agro_pk") or t0.cliente_agro_id
    valor_pago = sum((b.valor for b in baixas), Decimal("0")).quantize(Decimal("0.01"))
    if snap.get("valor_aplicado") is not None:
        try:
            valor_pago = _dec(snap.get("valor_aplicado"))
        except Exception:
            pass

    formas_snap = snap.get("formas") if isinstance(snap.get("formas"), list) else None
    if formas_snap:
        formas = []
        for row in formas_snap:
            if not isinstance(row, dict):
                continue
            formas.append(
                {
                    "forma": str(row.get("forma") or "").strip() or "—",
                    "valor": float(_dec(row.get("valor"))),
                    "valor_texto": _moeda_rs(row.get("valor")),
                }
            )
    else:
        agrup: dict[str, Decimal] = {}
        for b in baixas:
            k = (b.forma_pagamento or "—").strip() or "—"
            agrup[k] = (agrup.get(k) or Decimal("0")) + b.valor
        formas = [
            {"forma": k, "valor": float(v), "valor_texto": _moeda_rs(v)}
            for k, v in agrup.items()
        ]

    if snap.get("saldo_restante") is not None:
        saldo_rest = _dec(snap.get("saldo_restante"))
    else:
        saldo_rest = saldo_aberto_cliente_fiado(
            cliente_agro_pk=int(cliente_pk) if cliente_pk else None,
            cliente_nome=cliente_nome,
            cliente_codigo=t0.cliente_codigo or "",
        )

    titulos_linhas = []
    for b in baixas:
        tit = b.titulo
        parc = ""
        if tit.parcela_total and tit.parcela_total > 1:
            parc = f"{tit.parcela_num}/{tit.parcela_total}"
        titulos_linhas.append(
            {
                "titulo_id": tit.pk,
                "documento": tit.numero_documento or f"#{tit.pk}",
                "parcela": parc,
                "vencimento": tit.vencimento.strftime("%d/%m/%Y") if tit.vencimento else "",
                "valor_pago": float(b.valor),
                "valor_pago_texto": _moeda_rs(b.valor),
                "saldo_titulo": float(tit.saldo_aberto),
                "saldo_titulo_texto": _moeda_rs(tit.saldo_aberto),
                "nome": (tit.numero_documento or f"Título #{tit.pk}") + (f" · {parc}" if parc else ""),
                "subtotal": float(b.valor),
            }
        )

    criado = evento.criado_em if evento else baixas[0].criado_em
    usuario = (evento.usuario if evento else baixas[0].usuario) or ""
    quitou = saldo_rest <= Decimal("0.02")

    return {
        "ok": True,
        "tipo": "recibo_fiado",
        "subtitulo": "RECIBO DE PAGAMENTO FIADO",
        "recibo_id": int(evento.pk) if evento else None,
        "baixas_ids": [b.pk for b in baixas],
        "segunda_via": bool(segunda_via),
        "criado_em": _fmt_dt(criado),
        "cliente_nome": cliente_nome or "—",
        "cliente_agro_pk": int(cliente_pk) if cliente_pk else None,
        "valor_pago": float(valor_pago),
        "valor_pago_texto": _moeda_rs(valor_pago),
        "total": float(valor_pago),
        "total_texto": _moeda_rs(valor_pago),
        "saldo_restante": float(saldo_rest),
        "saldo_restante_texto": _moeda_rs(saldo_rest),
        "quitou": quitou,
        "formas": formas,
        "forma_pagamento": " + ".join(f"{r['forma']} {r['valor_texto']}" for r in formas),
        "operador": usuario,
        "titulos": titulos_linhas,
        "itens": titulos_linhas,
        "com_assinatura": True,
        "eh_fiado": True,
    }


def listar_recibos_pagamento_fiado(
    *,
    cliente_agro_pk: int | None = None,
    cliente_nome: str = "",
    cliente_codigo: str = "",
    limit: int = 40,
) -> list[dict[str, Any]]:
    if not cliente_agro_pk and not (cliente_nome or "").strip():
        return []
    if cliente_agro_pk:
        tit_filtros = Q(cliente_agro_id=int(cliente_agro_pk))
    else:
        tit_filtros = Q(cliente_nome__iexact=(cliente_nome or "").strip())
        if (cliente_codigo or "").strip():
            tit_filtros &= Q(cliente_codigo=str(cliente_codigo).strip())
    titulo_ids = list(FiadoTituloAgro.objects.filter(tit_filtros).values_list("pk", flat=True)[:2000])

    ev_q = Q(tipo=FiadoEventoAgro.Tipo.BAIXA)
    if cliente_agro_pk:
        ev_q &= Q(cliente_agro_id=int(cliente_agro_pk)) | Q(titulo_id__in=titulo_ids[:800])
    elif titulo_ids:
        ev_q &= Q(titulo_id__in=titulo_ids[:800])
    else:
        return []

    usados: set[int] = set()
    out: list[dict[str, Any]] = []
    for ev in FiadoEventoAgro.objects.filter(ev_q).order_by("-pk")[:120]:
        snap = ev.payload_json if isinstance(ev.payload_json, dict) else {}
        if str(snap.get("origem") or "") == "pdv_idempotencia":
            continue
        ids = [int(x) for x in (snap.get("baixas_ids") or []) if x]
        if not ids and ev.baixa_id:
            ids = [int(ev.baixa_id)]
        if not ids:
            continue
        if all(i in usados for i in ids) and len(ids) == 1:
            continue
        usados.update(ids)
        valor = snap.get("valor_aplicado")
        if valor is None:
            baixas = _baixas_por_ids(ids)
            valor = float(sum((b.valor for b in baixas), Decimal("0")))
        formas = snap.get("formas") if isinstance(snap.get("formas"), list) else []
        forma_txt = ""
        if formas:
            forma_txt = ", ".join(str(r.get("forma") or "") for r in formas if isinstance(r, dict))
        elif ids:
            baixas = _baixas_por_ids(ids)
            forma_txt = ", ".join(sorted({(b.forma_pagamento or "").strip() for b in baixas if b.forma_pagamento}))
        out.append(
            {
                "recibo_id": ev.pk,
                "baixas_ids": ids,
                "criado_em": _fmt_dt(ev.criado_em),
                "valor": float(_dec(valor)),
                "valor_texto": _moeda_rs(valor),
                "forma": forma_txt or "—",
                "operador": ev.usuario or "",
                "parcial": bool(snap.get("parcial")),
            }
        )
        if len(out) >= limit:
            return out

    if len(out) >= limit:
        return out

    rest = (
        FiadoBaixaAgro.objects.filter(titulo_id__in=titulo_ids[:800])
        .exclude(pk__in=usados)
        .select_related("titulo")
        .order_by("-pk")[:80]
    )
    grupos: dict[str, list[FiadoBaixaAgro]] = {}
    ordem: list[str] = []
    for b in rest:
        if b.movimento_caixa_id:
            key = f"m:{b.movimento_caixa_id}"
        else:
            dt = b.criado_em
            if dt and timezone.is_aware(dt):
                dt = timezone.localtime(dt)
            minuto = dt.strftime("%Y%m%d%H%M") if dt else "x"
            key = f"t:{minuto}:{(b.usuario or '').strip().lower()}"
        if key not in grupos:
            grupos[key] = []
            ordem.append(key)
        grupos[key].append(b)

    for key in ordem:
        if len(out) >= limit:
            break
        baixas = grupos[key]
        ids = [b.pk for b in baixas]
        valor = sum((b.valor for b in baixas), Decimal("0"))
        formas = sorted({(b.forma_pagamento or "").strip() for b in baixas if b.forma_pagamento})
        b0 = baixas[0]
        out.append(
            {
                "recibo_id": None,
                "baixas_ids": ids,
                "criado_em": _fmt_dt(b0.criado_em),
                "valor": float(valor),
                "valor_texto": _moeda_rs(valor),
                "forma": ", ".join(formas) if formas else "—",
                "operador": b0.usuario or "",
                "parcial": False,
            }
        )
    return out
