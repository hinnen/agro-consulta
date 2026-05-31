"""
Montagem do Relatório de Caixa (movimentos por categoria e período).
"""
from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from django.utils import timezone

from produtos.caixa_util import (
    FORMAS_PAGAMENTO_CAIXA,
    extrair_linhas_conferencia_sessao,
    normalizar_forma_pagamento_caixa,
    pagamentos_por_forma_venda,
    usuario_label_sessao_caixa,
)
from produtos.models import MovimentoCaixa, SessaoCaixa, VendaAgro


def _dec(val) -> Decimal:
    if isinstance(val, Decimal):
        return val.quantize(Decimal("0.01"))
    try:
        return Decimal(str(val or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


def _dt_range(di: date, df: date) -> tuple[datetime, datetime]:
    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime.combine(di, time.min), tz)
    fim = timezone.make_aware(datetime.combine(df, time.max), tz)
    return ini, fim


def _eh_devolucao_mov(obs: str) -> bool:
    o = (obs or "").strip().lower()
    return o.startswith("devolução venda") or o.startswith("devolucao venda")


def _eh_frete_mov(obs: str) -> bool:
    return "frete" in (obs or "").lower()


def _eh_deposito_mov(obs: str) -> bool:
    o = (obs or "").strip().lower()
    return o.startswith("depósito") or o.startswith("deposito")


def montar_relatorio_caixa(
    di: date,
    df: date,
    *,
    sessao_id: int | None = None,
    forma_pagamento: str | None = None,
) -> dict[str, Any]:
    ini, fim = _dt_range(di, df)
    filtro_forma = ""
    if forma_pagamento and str(forma_pagamento).strip():
        filtro_forma = normalizar_forma_pagamento_caixa(str(forma_pagamento).strip())

    def _sessao_ok(sid) -> bool:
        if sessao_id is None:
            return True
        try:
            return int(sid) == int(sessao_id)
        except (TypeError, ValueError):
            return False

    def _forma_ok(forma_linha: str) -> bool:
        if not filtro_forma:
            return True
        fn = normalizar_forma_pagamento_caixa(str(forma_linha or "").strip())
        return fn == filtro_forma

    linha_id = 0

    def _row(
        categoria: str,
        *,
        quando: datetime,
        descricao: str,
        forma: str = "",
        valor: Decimal,
        sinal: str,
        sessao_pk: int | None = None,
        ref: str = "",
    ) -> dict[str, Any]:
        nonlocal linha_id
        linha_id += 1
        v = _dec(valor)
        return {
            "id": linha_id,
            "categoria": categoria,
            "quando": quando,
            "descricao": descricao[:300],
            "forma": forma[:80] if forma else "",
            "valor": str(v),
            "valor_num": float(v),
            "sinal": sinal,
            "sessao_id": sessao_pk,
            "ref": ref,
        }

    buckets: dict[str, list[dict[str, Any]]] = {
        "aberturas": [],
        "vendas": [],
        "devolucoes": [],
        "reforcos": [],
        "retiradas": [],
        "depositos": [],
        "fretes": [],
    }

    # Aberturas de caixa no período
    for s in SessaoCaixa.objects.filter(aberto_em__gte=ini, aberto_em__lte=fim).order_by("aberto_em"):
        if not _sessao_ok(s.pk):
            continue
        fundo = _dec(s.valor_abertura)
        if fundo > 0 and _forma_ok("Dinheiro"):
            buckets["aberturas"].append(
                _row(
                    "aberturas",
                    quando=s.aberto_em,
                    descricao=f"Abertura caixa #{s.pk}",
                    forma="Dinheiro",
                    valor=fundo,
                    sinal="+",
                    sessao_pk=s.pk,
                    ref=f"sessao:{s.pk}",
                )
            )

    # Vendas (não devolvidas)
    vendas_qs = (
        VendaAgro.objects.filter(criado_em__gte=ini, criado_em__lte=fim, devolvida_em__isnull=True)
        .select_related("sessao_caixa")
        .order_by("criado_em")
    )
    for v in vendas_qs:
        sid = v.sessao_caixa_id
        if sid and not _sessao_ok(sid):
            continue
        if not sid and sessao_id is not None:
            continue
        pag = pagamentos_por_forma_venda(v)
        if not pag:
            pag = {"Outro": _dec(v.total)}
        for fn, val in pag.items():
            if val <= 0 or not _forma_ok(fn):
                continue
            buckets["vendas"].append(
                _row(
                    "vendas",
                    quando=v.criado_em,
                    descricao=f"Venda #{v.pk} · {(v.cliente_nome or '')[:40]}",
                    forma=fn,
                    valor=val,
                    sinal="+",
                    sessao_pk=sid,
                    ref=f"venda:{v.pk}",
                )
            )

    # Devoluções (data da devolução)
    dev_qs = VendaAgro.objects.filter(
        devolvida_em__gte=ini, devolvida_em__lte=fim
    ).order_by("devolvida_em")
    for v in dev_qs:
        sid = v.sessao_caixa_id
        if sid and not _sessao_ok(sid):
            continue
        if not sid and sessao_id is not None:
            continue
        pag = v.devolucao_pagamentos_json
        if isinstance(pag, list) and pag:
            for row in pag:
                if not isinstance(row, dict):
                    continue
                val = _dec(row.get("valor"))
                fn_dev = str(row.get("forma") or "")
                if val <= 0 or not _forma_ok(fn_dev):
                    continue
                buckets["devolucoes"].append(
                    _row(
                        "devolucoes",
                        quando=v.devolvida_em,
                        descricao=f"Devolução venda #{v.pk} · {(v.cliente_nome or '')[:35]}",
                        forma=fn_dev,
                        valor=val,
                        sinal="-",
                        sessao_pk=sid,
                        ref=f"dev:{v.pk}",
                    )
                )
        elif not filtro_forma:
            buckets["devolucoes"].append(
                _row(
                    "devolucoes",
                    quando=v.devolvida_em,
                    descricao=f"Devolução venda #{v.pk}",
                    forma="",
                    valor=_dec(v.total),
                    sinal="-",
                    sessao_pk=sid,
                    ref=f"dev:{v.pk}",
                )
            )

    # Movimentos manuais
    mov_qs = MovimentoCaixa.objects.filter(
        criado_em__gte=ini, criado_em__lte=fim
    ).select_related("sessao_caixa").order_by("criado_em")
    for m in mov_qs:
        if not _sessao_ok(m.sessao_caixa_id):
            continue
        obs = m.observacao or ""
        val = _dec(m.valor)
        if val <= 0:
            continue
        if m.tipo == MovimentoCaixa.Tipo.REFORCO and _forma_ok(m.forma_pagamento):
            buckets["reforcos"].append(
                _row(
                    "reforcos",
                    quando=m.criado_em,
                    descricao=obs[:120] or "Reforço manual",
                    forma=m.forma_pagamento,
                    valor=val,
                    sinal="+",
                    sessao_pk=m.sessao_caixa_id,
                    ref=f"mov:{m.pk}",
                )
            )
        elif m.tipo == MovimentoCaixa.Tipo.RETIRADA:
            if _eh_devolucao_mov(obs):
                continue
            if _eh_deposito_mov(obs) and _forma_ok(m.forma_pagamento):
                buckets["depositos"].append(
                    _row(
                        "depositos",
                        quando=m.criado_em,
                        descricao=obs[:120] or "Depósito caixa → banco",
                        forma=m.forma_pagamento,
                        valor=val,
                        sinal="-",
                        sessao_pk=m.sessao_caixa_id,
                        ref=f"mov:{m.pk}",
                    )
                )
            elif _eh_frete_mov(obs) and _forma_ok(m.forma_pagamento):
                buckets["fretes"].append(
                    _row(
                        "fretes",
                        quando=m.criado_em,
                        descricao=obs[:120] or "Frete / entrega",
                        forma=m.forma_pagamento,
                        valor=val,
                        sinal="-",
                        sessao_pk=m.sessao_caixa_id,
                        ref=f"mov:{m.pk}",
                    )
                )
            elif _forma_ok(m.forma_pagamento):
                buckets["retiradas"].append(
                    _row(
                        "retiradas",
                        quando=m.criado_em,
                        descricao=obs[:120] or "Retirada / saída",
                        forma=m.forma_pagamento,
                        valor=val,
                        sinal="-",
                        sessao_pk=m.sessao_caixa_id,
                        ref=f"mov:{m.pk}",
                    )
                )

    meta_categorias = [
        ("aberturas", "Aberturas de caixa", "+", "emerald"),
        ("vendas", "Vendas", "+", "emerald"),
        ("devolucoes", "Devoluções", "-", "rose"),
        ("reforcos", "Reforços", "+", "sky"),
        ("depositos", "Depósitos (caixa → banco)", "-", "sky"),
        ("retiradas", "Retiradas e despesas", "-", "orange"),
        ("fretes", "Fretes e entregas", "-", "amber"),
    ]

    secoes = []
    tot_entrada = Decimal("0")
    tot_saida = Decimal("0")
    for key, titulo, sinal_padrao, cor in meta_categorias:
        linhas = buckets.get(key) or []
        sub = Decimal("0")
        for ln in linhas:
            v = _dec(ln["valor"])
            if ln["sinal"] == "+":
                sub += v
                tot_entrada += v
            else:
                sub += v
                tot_saida += v
        secoes.append(
            {
                "key": key,
                "titulo": titulo,
                "sinal": sinal_padrao,
                "cor": cor,
                "linhas": linhas,
                "qtd": len(linhas),
                "subtotal": str(sub.quantize(Decimal("0.01"))),
                "subtotal_num": float(sub),
            }
        )

    saldo = (tot_entrada - tot_saida).quantize(Decimal("0.01"))

    sessoes_opts = list(
        SessaoCaixa.objects.filter(aberto_em__date__lte=df)
        .order_by("-aberto_em")[:80]
        .values("pk", "aberto_em", "fechado_em")
    )

    return {
        "secoes": secoes,
        "tot_entrada": str(tot_entrada),
        "tot_saida": str(tot_saida),
        "saldo": str(saldo),
        "tot_entrada_num": float(tot_entrada),
        "tot_saida_num": float(tot_saida),
        "saldo_num": float(saldo),
        "sessoes_opts": sessoes_opts,
    }


def montar_relatorio_conferencias_caixa(
    di: date,
    df: date,
    *,
    sessao_id: int | None = None,
    somente_com_diferenca: bool = False,
) -> dict[str, Any]:
    """Lista fechamentos com esperado × contado × diferença por forma (e totais)."""
    ini, fim = _dt_range(di, df)
    qs = SessaoCaixa.objects.filter(
        fechado_em__isnull=False, fechado_em__gte=ini, fechado_em__lte=fim
    ).select_related("usuario")
    if sessao_id is not None:
        qs = qs.filter(pk=sessao_id)
    qs = qs.order_by("-fechado_em")

    sessoes_rows: list[dict[str, Any]] = []
    agg: dict[str, dict[str, Decimal]] = {}

    def _agg_add(forma: str, esp: Decimal, cont: Decimal | None, dif: Decimal | None) -> None:
        if forma not in agg:
            agg[forma] = {
                "esperado": Decimal("0"),
                "contado": Decimal("0"),
                "diferenca": Decimal("0"),
                "qtd_contado": 0,
            }
        row = agg[forma]
        row["esperado"] += esp
        if cont is not None:
            row["contado"] += cont
            row["qtd_contado"] += 1
        if dif is not None:
            row["diferenca"] += dif

    qtd_com_diff = 0
    for s in qs:
        linhas_raw = extrair_linhas_conferencia_sessao(s)
        linhas_fmt: list[dict[str, Any]] = []
        sess_diff = False
        for L in linhas_raw:
            dif = L.get("diferenca")
            if dif is not None and abs(_dec(dif)) >= Decimal("0.01"):
                sess_diff = True
            cont = L.get("contado")
            _agg_add(
                L["forma"],
                _dec(L.get("esperado")),
                cont if cont is not None else None,
                dif if dif is not None else None,
            )
            if somente_com_diferenca and (dif is None or abs(_dec(dif)) < Decimal("0.01")):
                continue
            linhas_fmt.append(
                {
                    "forma": L["forma"],
                    "esperado": L["esperado_str"],
                    "contado": L["contado_str"] or "—",
                    "diferenca": L["diferenca_str"] or "—",
                    "tem_diff": dif is not None and abs(_dec(dif)) >= Decimal("0.01"),
                    "diff_positivo": dif is not None and _dec(dif) > 0,
                    "diff_negativo": dif is not None and _dec(dif) < 0,
                }
            )
        if somente_com_diferenca and not linhas_fmt:
            continue
        if sess_diff:
            qtd_com_diff += 1
        obs = (s.observacao_fechamento or "").strip()
        sessoes_rows.append(
            {
                "pk": s.pk,
                "fechado_em": s.fechado_em,
                "aberto_em": s.aberto_em,
                "usuario": usuario_label_sessao_caixa(s),
                "valor_fechamento": str(_dec(s.valor_fechamento))
                if s.valor_fechamento is not None
                else "",
                "observacao": obs[:200],
                "linhas": linhas_fmt,
                "tem_diferenca": sess_diff,
            }
        )

    totais_forma: list[dict[str, Any]] = []
    tot_esp = tot_cont = tot_dif = Decimal("0")
    ordem = [f for f in FORMAS_PAGAMENTO_CAIXA if f in agg]
    ordem.extend(sorted(set(agg.keys()) - set(ordem)))
    for fn in ordem:
        row = agg[fn]
        esp = _dec(row["esperado"]).quantize(Decimal("0.01"))
        cont = _dec(row["contado"]).quantize(Decimal("0.01"))
        dif = _dec(row["diferenca"]).quantize(Decimal("0.01"))
        if esp == 0 and cont == 0 and dif == 0:
            continue
        tot_esp += esp
        tot_cont += cont
        tot_dif += dif
        totais_forma.append(
            {
                "forma": fn,
                "esperado": str(esp),
                "contado": str(cont),
                "diferenca": str(dif),
                "tem_diff": abs(dif) >= Decimal("0.01"),
                "diff_positivo": dif > 0,
                "diff_negativo": dif < 0,
            }
        )

    sessoes_opts = list(
        SessaoCaixa.objects.filter(fechado_em__date__lte=df)
        .order_by("-fechado_em")[:80]
        .values("pk", "aberto_em", "fechado_em")
    )

    return {
        "sessoes": sessoes_rows,
        "totais_forma": totais_forma,
        "qtd_fechamentos": len(sessoes_rows),
        "qtd_com_diferenca": qtd_com_diff,
        "tot_esperado": str(tot_esp.quantize(Decimal("0.01"))),
        "tot_contado": str(tot_cont.quantize(Decimal("0.01"))),
        "tot_diferenca": str(tot_dif.quantize(Decimal("0.01"))),
        "sessoes_opts": sessoes_opts,
    }
