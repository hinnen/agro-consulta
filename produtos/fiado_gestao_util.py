"""Gestão de crédito loja (fiado): títulos, baixas, limite e auditoria."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from django.db import transaction
from django.db.models import Count, Q, Sum
from django.db.models.functions import Coalesce
from django.utils import timezone

from produtos.caixa_util import normalizar_forma_pagamento_caixa, obter_sessao_caixa_aberta_request
from produtos.fiado_credito_util import (
    fiado_limite_padrao,
    montar_cronograma_fiado,
    resolver_cliente_fiado,
    valor_fiado_usado_cliente_vendas,
    valor_fiado_venda_local,
    venda_local_tem_fiado,
)
from produtos.models import (
    ClienteAgro,
    FiadoBaixaAgro,
    FiadoEventoAgro,
    FiadoTituloAgro,
    MovimentoCaixa,
    SessaoCaixa,
    VendaAgro,
)


def _dec(val) -> Decimal:
    try:
        if val is None:
            return Decimal("0")
        return Decimal(str(val).replace(",", ".")).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


def _usuario_de_request(request) -> str:
    if not request or not getattr(request, "user", None):
        return ""
    u = request.user
    if not getattr(u, "is_authenticated", False):
        return ""
    return (
        (u.get_full_name() or "").strip()
        or u.get_username()
        or str(u.pk)
    )


def titulo_para_dict(t: FiadoTituloAgro) -> dict[str, Any]:
    saldo = t.saldo_aberto
    return {
        "id": t.pk,
        "cliente_agro_pk": t.cliente_agro_id,
        "cliente_nome": t.cliente_nome,
        "cliente_codigo": t.cliente_codigo,
        "numero_documento": t.numero_documento,
        "parcela_num": t.parcela_num,
        "parcela_total": t.parcela_total,
        "vencimento": t.vencimento.isoformat() if t.vencimento else "",
        "vencimento_texto": t.vencimento.strftime("%d/%m/%Y") if t.vencimento else "",
        "valor_bruto": float(t.valor_bruto),
        "valor_pago": float(t.valor_pago),
        "saldo_aberto": float(saldo),
        "situacao": t.situacao,
        "situacao_label": t.get_situacao_display(),
        "origem": t.origem,
        "descricao": t.descricao,
        "venda_agro_id": t.venda_agro_id,
        "atualizado_em": t.atualizado_em.isoformat() if t.atualizado_em else "",
    }


def titulo_snapshot(t: FiadoTituloAgro) -> dict[str, Any]:
    d = titulo_para_dict(t)
    d["chave_unica"] = t.chave_unica
    return d


def registrar_evento_fiado(
    tipo: str,
    *,
    cliente_agro: ClienteAgro | None = None,
    titulo: FiadoTituloAgro | None = None,
    baixa: FiadoBaixaAgro | None = None,
    payload: dict | None = None,
    usuario: str = "",
) -> FiadoEventoAgro:
    return FiadoEventoAgro.objects.create(
        tipo=tipo,
        cliente_agro=cliente_agro,
        titulo=titulo,
        baixa=baixa,
        payload_json=payload or {},
        usuario=(usuario or "")[:150],
    )


def _atualizar_situacao_titulo(titulo: FiadoTituloAgro) -> None:
    if titulo.situacao == FiadoTituloAgro.Situacao.CANCELADO:
        return
    saldo = titulo.saldo_aberto
    if saldo <= Decimal("0"):
        titulo.situacao = FiadoTituloAgro.Situacao.QUITADO
    elif titulo.valor_pago > Decimal("0"):
        titulo.situacao = FiadoTituloAgro.Situacao.PARCIAL
    else:
        titulo.situacao = FiadoTituloAgro.Situacao.ABERTO


def _filtro_titulos_cliente(
    cliente_id_erp: str = "",
    *,
    cliente_agro_pk: int | None = None,
) -> Q:
    erp_id, agro_pk, _cli = resolver_cliente_fiado(cliente_id_erp, cliente_agro_pk=cliente_agro_pk)
    filtros = Q()
    if agro_pk:
        filtros |= Q(cliente_agro_id=agro_pk)
    if erp_id:
        filtros |= Q(cliente_codigo=erp_id)
    cod = str(cliente_id_erp or "").strip()
    if cod.isdigit():
        filtros |= Q(cliente_codigo=cod)
    return filtros


def cliente_tem_ledger_fiado(
    cliente_id_erp: str = "",
    *,
    cliente_agro_pk: int | None = None,
) -> bool:
    filtros = _filtro_titulos_cliente(cliente_id_erp, cliente_agro_pk=cliente_agro_pk)
    if not filtros:
        return FiadoTituloAgro.objects.exists()
    return FiadoTituloAgro.objects.filter(filtros).exists()


def valor_fiado_usado_cliente(
    cliente_id_erp: str = "",
    *,
    cliente_agro_pk: int | None = None,
    excluir_venda_id: int | None = None,
) -> Decimal:
    """
    Saldo de fiado em aberto: soma títulos quando existir ledger; senão vendas legadas.
    """
    filtros = _filtro_titulos_cliente(cliente_id_erp, cliente_agro_pk=cliente_agro_pk)
    if filtros and FiadoTituloAgro.objects.filter(filtros).exists():
        qs = FiadoTituloAgro.objects.filter(filtros).exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        )
        total = Decimal("0")
        for t in qs.only("valor_bruto", "valor_pago", "situacao"):
            total += t.saldo_aberto
        return total.quantize(Decimal("0.01"))
    return valor_fiado_usado_cliente_vendas(
        cliente_id_erp,
        cliente_agro_pk=cliente_agro_pk,
        excluir_venda_id=excluir_venda_id,
    )


def _parse_vencimento(row: dict, fallback: date) -> date:
    raw = row.get("vencimento") or row.get("Vencimento")
    if isinstance(raw, date):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    s = str(raw or "").strip()
    if not s:
        return fallback
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return fallback


def criar_titulos_de_venda(
    venda: VendaAgro,
    *,
    usuario: str = "",
) -> list[FiadoTituloAgro]:
    """Gera títulos a partir de venda fiado PDV (idempotente)."""
    if not venda_local_tem_fiado(venda) or venda.devolvida:
        return []
    existentes = list(FiadoTituloAgro.objects.filter(venda_agro=venda))
    if existentes:
        return existentes

    _erp_id, agro_pk, cli = resolver_cliente_fiado(venda.cliente_id_erp)
    if agro_pk and not cli:
        cli = ClienteAgro.objects.filter(pk=agro_pk).first()
    valor_fiado = valor_fiado_venda_local(venda)
    if valor_fiado <= 0:
        return []

    cron = venda.fiado_cronograma_json
    if not isinstance(cron, list) or not cron:
        np, nd = 1, 30
        pj = venda.pagamentos_json
        if isinstance(pj, list):
            for row in pj:
                if not isinstance(row, dict):
                    continue
                if normalizar_forma_pagamento_caixa(str(row.get("forma") or "")) != "Fiado":
                    continue
                np = int(row.get("fiado_parcelas") or row.get("fiadoParcelas") or 1)
                nd = int(row.get("fiado_dias_primeiro") or row.get("fiadoDiasVencimento") or 30)
                fc = row.get("fiado_cronograma") or row.get("fiadoCronograma")
                if isinstance(fc, list) and fc:
                    cron = fc
                    break
        if not cron:
            base = venda.criado_em.date() if venda.criado_em else date.today()
            cron = montar_cronograma_fiado(valor_fiado, np, nd, base_date=base)

    cliente_nome = (venda.cliente_nome or "").strip() or (cli.nome if cli else "Cliente")
    cliente_codigo = str(venda.cliente_id_erp or "").strip()
    if cli and (cli.externo_id or "").strip():
        cliente_codigo = str(cli.externo_id).strip()
    elif cliente_codigo.lower().startswith(("agro:", "local:")):
        cliente_codigo = str(agro_pk or "")

    n_total = len(cron) if cron else 1
    titulos: list[FiadoTituloAgro] = []
    with transaction.atomic():
        for row in cron:
            if not isinstance(row, dict):
                continue
            parcela = int(row.get("parcela") or len(titulos) + 1)
            venc = _parse_vencimento(row, venda.criado_em.date() if venda.criado_em else date.today())
            v_parcela = _dec(row.get("valor"))
            if v_parcela <= 0:
                continue
            chave = f"pdv:{venda.pk}:{parcela}"
            doc = f"Pedido {venda.pk}"
            desc = (
                f"Valor de R$ {v_parcela:.2f} (Parcela {parcela} de {n_total}). "
                f"Referente ao PEDIDO DE VENDA #{venda.pk} "
                f"em {(venda.criado_em.strftime('%d/%m/%Y') if venda.criado_em else '')}."
            )
            titulo = FiadoTituloAgro.objects.create(
                chave_unica=chave,
                cliente_agro=cli,
                venda_agro=venda,
                cliente_nome=cliente_nome,
                cliente_codigo=cliente_codigo,
                numero_documento=doc,
                parcela_num=parcela,
                parcela_total=n_total,
                vencimento=venc,
                valor_bruto=v_parcela,
                valor_pago=Decimal("0"),
                situacao=FiadoTituloAgro.Situacao.ABERTO,
                origem=FiadoTituloAgro.Origem.PDV,
                descricao=desc[:500],
                dados_snapshot_json={"venda_id": venda.pk, "cronograma": row},
            )
            registrar_evento_fiado(
                FiadoEventoAgro.Tipo.TITULO_CRIADO,
                cliente_agro=cli,
                titulo=titulo,
                payload={"titulo": titulo_snapshot(titulo), "origem": "pdv"},
                usuario=usuario,
            )
            titulos.append(titulo)
    return titulos


def cancelar_titulos_venda(
    venda: VendaAgro,
    *,
    usuario: str = "",
    motivo: str = "",
) -> int:
    n = 0
    for t in FiadoTituloAgro.objects.filter(venda_agro=venda).exclude(
        situacao=FiadoTituloAgro.Situacao.CANCELADO
    ):
        snap = titulo_snapshot(t)
        t.situacao = FiadoTituloAgro.Situacao.CANCELADO
        t.save(update_fields=["situacao", "atualizado_em"])
        registrar_evento_fiado(
            FiadoEventoAgro.Tipo.CANCELAMENTO,
            cliente_agro=t.cliente_agro,
            titulo=t,
            payload={"titulo": snap, "motivo": motivo, "venda_id": venda.pk},
            usuario=usuario,
        )
        n += 1
    return n


def baixar_titulo(
    titulo_id: int,
    valor: Decimal | float,
    forma_pagamento: str,
    *,
    request=None,
    observacao: str = "",
    registrar_caixa: bool = True,
    sessao_caixa: SessaoCaixa | None = None,
    usuario: str = "",
) -> FiadoBaixaAgro:
    v = _dec(valor)
    if v <= 0:
        raise ValueError("Informe um valor maior que zero.")
    forma = normalizar_forma_pagamento_caixa(forma_pagamento or "Dinheiro")
    user_label = usuario or _usuario_de_request(request)

    with transaction.atomic():
        titulo = FiadoTituloAgro.objects.select_for_update().get(pk=titulo_id)
        if titulo.situacao in (
            FiadoTituloAgro.Situacao.QUITADO,
            FiadoTituloAgro.Situacao.CANCELADO,
        ):
            raise ValueError("Este título já está quitado ou cancelado.")
        saldo = titulo.saldo_aberto
        if v > saldo + Decimal("0.009"):
            raise ValueError(f"Valor maior que o saldo em aberto (R$ {saldo:.2f}).")

        sessao = sessao_caixa
        mov = None
        if registrar_caixa:
            if sessao is None and request is not None:
                sessao = obter_sessao_caixa_aberta_request(request)
            if sessao is None:
                raise ValueError("Abra o caixa neste navegador para registrar o recebimento.")
            obs_caixa = f"Baixa fiado título #{titulo.pk} — {titulo.cliente_nome[:40]}"
            if observacao:
                obs_caixa = f"{obs_caixa} — {observacao}"[:500]
            user_mov = None
            if request and getattr(request, "user", None) and request.user.is_authenticated:
                user_mov = request.user
            mov = MovimentoCaixa.objects.create(
                sessao_caixa=sessao,
                tipo=MovimentoCaixa.Tipo.REFORCO,
                forma_pagamento=forma,
                valor=v,
                observacao=obs_caixa,
                usuario=user_mov,
            )

        baixa = FiadoBaixaAgro.objects.create(
            titulo=titulo,
            valor=v,
            forma_pagamento=forma,
            sessao_caixa=sessao,
            movimento_caixa=mov,
            usuario=user_label,
            observacao=(observacao or "")[:500],
        )
        titulo.valor_pago = (titulo.valor_pago + v).quantize(Decimal("0.01"))
        _atualizar_situacao_titulo(titulo)
        titulo.save(update_fields=["valor_pago", "situacao", "atualizado_em"])

        registrar_evento_fiado(
            FiadoEventoAgro.Tipo.BAIXA,
            cliente_agro=titulo.cliente_agro,
            titulo=titulo,
            baixa=baixa,
            payload={
                "titulo": titulo_snapshot(titulo),
                "baixa": {
                    "id": baixa.pk,
                    "valor": float(baixa.valor),
                    "forma": baixa.forma_pagamento,
                    "movimento_caixa_id": mov.pk if mov else None,
                },
            },
            usuario=user_label,
        )
        return baixa


def baixar_cliente_fiado(
    valor: Decimal | float,
    forma_pagamento: str,
    *,
    cliente_agro_pk: int | None = None,
    cliente_nome: str = "",
    cliente_codigo: str = "",
    request=None,
    observacao: str = "",
    registrar_caixa: bool = True,
    usuario: str = "",
) -> dict[str, Any]:
    """Aplica pagamento nos títulos do cliente (vencimento mais antigo primeiro)."""
    v_total = _dec(valor)
    if v_total <= 0:
        raise ValueError("Informe um valor maior que zero.")

    filtros = Q()
    if cliente_agro_pk:
        filtros = Q(cliente_agro_id=cliente_agro_pk)
    elif (cliente_nome or "").strip():
        filtros = Q(cliente_nome__iexact=(cliente_nome or "").strip())
        if (cliente_codigo or "").strip():
            filtros &= Q(cliente_codigo=str(cliente_codigo).strip())
    else:
        raise ValueError("Informe o cliente.")

    titulos = list(
        FiadoTituloAgro.objects.filter(filtros)
        .exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        )
        .order_by("vencimento", "pk")
    )
    if not titulos:
        raise ValueError("Cliente sem títulos em aberto.")

    saldo_cli = sum(t.saldo_aberto for t in titulos)
    if v_total > saldo_cli + Decimal("0.009"):
        raise ValueError(f"Valor maior que o saldo em aberto (R$ {saldo_cli:.2f}).")

    nome_cli = titulos[0].cliente_nome
    baixas_ids: list[int] = []
    mov = None
    with transaction.atomic():
        sessao = None
        if registrar_caixa and request is not None:
            sessao = obter_sessao_caixa_aberta_request(request)
            if sessao is None:
                raise ValueError("Abra o caixa neste navegador para registrar o recebimento.")
            forma = normalizar_forma_pagamento_caixa(forma_pagamento or "Dinheiro")
            obs_caixa = f"Baixa fiado — {nome_cli[:40]}"
            if observacao:
                obs_caixa = f"{obs_caixa} — {observacao}"[:500]
            user_mov = request.user if getattr(request, "user", None) and request.user.is_authenticated else None
            mov = MovimentoCaixa.objects.create(
                sessao_caixa=sessao,
                tipo=MovimentoCaixa.Tipo.REFORCO,
                forma_pagamento=forma,
                valor=v_total,
                observacao=obs_caixa,
                usuario=user_mov,
            )

        restante = v_total
        for titulo in titulos:
            if restante <= Decimal("0"):
                break
            parcela = min(restante, titulo.saldo_aberto)
            if parcela <= Decimal("0"):
                continue
            baixa = baixar_titulo(
                titulo.pk,
                parcela,
                forma_pagamento,
                request=request,
                observacao=observacao,
                registrar_caixa=False,
                sessao_caixa=sessao,
                usuario=usuario,
            )
            if mov:
                baixa.movimento_caixa = mov
                baixa.sessao_caixa = sessao
                baixa.save(update_fields=["movimento_caixa", "sessao_caixa"])
            baixas_ids.append(baixa.pk)
            restante = (restante - parcela).quantize(Decimal("0.01"))

    return {
        "valor_aplicado": float(v_total.quantize(Decimal("0.01"))),
        "baixas_ids": baixas_ids,
        "titulos_afetados": len(baixas_ids),
        "movimento_caixa_id": mov.pk if mov else None,
    }


def definir_limite_fiado_cliente(
    cliente_agro_pk: int,
    limite: Decimal | float,
    *,
    usuario: str = "",
) -> ClienteAgro:
    cli = ClienteAgro.objects.get(pk=cliente_agro_pk)
    novo = _dec(limite)
    if novo < 0:
        raise ValueError("Limite não pode ser negativo.")
    anterior = _dec(cli.limite_fiado_local)
    cli.limite_fiado_local = novo
    cli.save(update_fields=["limite_fiado_local", "atualizado_em"])
    registrar_evento_fiado(
        FiadoEventoAgro.Tipo.LIMITE,
        cliente_agro=cli,
        payload={
            "cliente_agro_pk": cli.pk,
            "nome": cli.nome,
            "limite_anterior": float(anterior),
            "limite_novo": float(novo),
        },
        usuario=usuario,
    )
    return cli


def _qs_titulos_abertos():
    return FiadoTituloAgro.objects.exclude(
        situacao__in=(
            FiadoTituloAgro.Situacao.QUITADO,
            FiadoTituloAgro.Situacao.CANCELADO,
        )
    )


def _chave_grupo_fiado_cliente(
    *,
    cliente_agro_id: int | None,
    cliente_nome: str,
    cliente_codigo: str = "",
) -> str:
    """Uma linha na grade = um cliente (Agro ou nome normalizado), não um código ERP por título."""
    if cliente_agro_id:
        return f"agro:{cliente_agro_id}"
    from produtos.fiado_import_util import _norm_nome_fiado_match

    nm = _norm_nome_fiado_match(cliente_nome or "")
    if nm:
        return f"nome:{nm}"
    cod = str(cliente_codigo or "").strip()
    return f"cod:{cod}" if cod else "sem-cliente"


def _q_titulos_cliente_gestao(
    *,
    cliente_agro_pk: int | None = None,
    cliente_nome: str = "",
    cliente_codigo: str = "",
) -> Q:
    """Filtro para títulos do cliente na gestão (popup / baixa), sem repetir por código ERP."""
    if cliente_agro_pk:
        return Q(cliente_agro_id=cliente_agro_pk)
    nome = (cliente_nome or "").strip()
    cod = (cliente_codigo or "").strip()
    if cod:
        q = Q(cliente_codigo=cod)
        if nome:
            q &= Q(cliente_nome__iexact=nome)
        return q
    if not nome:
        return Q()
    from produtos.fiado_import_util import _norm_nome_fiado_match

    key = _norm_nome_fiado_match(nome)
    nomes = (
        FiadoTituloAgro.objects.exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        )
        .values_list("cliente_nome", flat=True)
        .distinct()
    )
    matching = [n for n in nomes if n and _norm_nome_fiado_match(n) == key]
    if matching:
        return Q(cliente_nome__in=matching)
    return Q(cliente_nome__iexact=nome)


def resumo_from_clientes_fiado(clientes: list[dict[str, Any]]) -> dict[str, Any]:
    total = Decimal("0")
    titulos = 0
    for c in clientes:
        total += Decimal(str(c.get("saldo_aberto") or 0))
        titulos += int(c.get("titulos_abertos") or 0)
    return {
        "titulos_abertos": titulos,
        "clientes_com_saldo": len(clientes),
        "total_saldo_aberto": float(total.quantize(Decimal("0.01"))),
    }


def resumo_gestao_fiado() -> dict[str, Any]:
    qs = _qs_titulos_abertos()
    agg = qs.aggregate(
        titulos_abertos=Count("pk"),
        total_bruto=Coalesce(Sum("valor_bruto"), Decimal("0")),
        total_pago=Coalesce(Sum("valor_pago"), Decimal("0")),
    )
    total_saldo = (agg["total_bruto"] - agg["total_pago"]).quantize(Decimal("0.01"))
    if total_saldo < 0:
        total_saldo = Decimal("0")
    chaves: set[str] = set()
    for agro_id, nome, cod in qs.values_list("cliente_agro_id", "cliente_nome", "cliente_codigo"):
        chaves.add(_chave_grupo_fiado_cliente(cliente_agro_id=agro_id, cliente_nome=nome or "", cliente_codigo=cod or ""))
    return {
        "titulos_abertos": int(agg["titulos_abertos"] or 0),
        "clientes_com_saldo": len(chaves),
        "total_saldo_aberto": float(total_saldo),
    }


def listar_clientes_fiado(
    *,
    busca: str = "",
    apenas_com_saldo: bool = True,
) -> list[dict[str, Any]]:
    qs = FiadoTituloAgro.objects.all()
    if apenas_com_saldo:
        qs = qs.exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        )
    qtxt = (busca or "").strip()
    if qtxt:
        qs = qs.filter(
            Q(cliente_nome__icontains=qtxt)
            | Q(cliente_codigo__icontains=qtxt)
            | Q(numero_documento__icontains=qtxt)
        )

    grupos: dict[str, dict] = {}
    hoje = date.today()
    qs = qs.select_related("cliente_agro").only(
        "pk",
        "cliente_agro_id",
        "cliente_nome",
        "cliente_codigo",
        "valor_bruto",
        "valor_pago",
        "vencimento",
        "situacao",
        "cliente_agro__externo_id",
        "cliente_agro__limite_fiado_local",
    )
    lim_padrao = fiado_limite_padrao()
    for t in qs.order_by("cliente_nome", "vencimento"):
        key = _chave_grupo_fiado_cliente(
            cliente_agro_id=t.cliente_agro_id,
            cliente_nome=t.cliente_nome or "",
            cliente_codigo=t.cliente_codigo or "",
        )
        if key not in grupos:
            cli = t.cliente_agro
            grupos[key] = {
                "cliente_agro_pk": t.cliente_agro_id,
                "cliente_nome": t.cliente_nome,
                "cliente_codigo": (cli.externo_id if cli and (cli.externo_id or "").strip() else ""),
                "saldo_aberto": Decimal("0"),
                "valor_bruto": Decimal("0"),
                "valor_pago": Decimal("0"),
                "titulos_abertos": 0,
                "tem_parcial": False,
                "tem_vencido": False,
                "vencimento_mais_antigo": None,
                "limite_fiado_local": float(cli.limite_fiado_local or 0) if cli else 0.0,
            }
        else:
            g0 = grupos[key]
            if t.cliente_agro_id and not g0["cliente_agro_pk"]:
                cli = t.cliente_agro
                g0["cliente_agro_pk"] = t.cliente_agro_id
                if cli and (cli.externo_id or "").strip():
                    g0["cliente_codigo"] = str(cli.externo_id).strip()
                g0["limite_fiado_local"] = float(cli.limite_fiado_local or 0) if cli else g0["limite_fiado_local"]
            elif len((t.cliente_nome or "")) > len(g0.get("cliente_nome") or ""):
                g0["cliente_nome"] = t.cliente_nome
        if t.situacao not in (
            FiadoTituloAgro.Situacao.QUITADO,
            FiadoTituloAgro.Situacao.CANCELADO,
        ):
            g = grupos[key]
            g["saldo_aberto"] += t.saldo_aberto
            g["valor_bruto"] += t.valor_bruto
            g["valor_pago"] += t.valor_pago
            g["titulos_abertos"] += 1
            if t.situacao == FiadoTituloAgro.Situacao.PARCIAL:
                g["tem_parcial"] = True
            if t.vencimento and t.vencimento < hoje:
                g["tem_vencido"] = True
            if t.vencimento and (
                g["vencimento_mais_antigo"] is None or t.vencimento < g["vencimento_mais_antigo"]
            ):
                g["vencimento_mais_antigo"] = t.vencimento

    out: list[dict] = []
    for g in grupos.values():
        saldo_dec = g["saldo_aberto"].quantize(Decimal("0.01"))
        lim_local = Decimal(str(g.get("limite_fiado_local") or 0))
        limite = lim_local if lim_local > 0 else lim_padrao
        disponivel = max(limite - saldo_dec, Decimal("0")).quantize(Decimal("0.01"))
        g["saldo_aberto"] = float(saldo_dec)
        g["valor_bruto"] = float(g["valor_bruto"].quantize(Decimal("0.01")))
        g["valor_pago"] = float(g["valor_pago"].quantize(Decimal("0.01")))
        g["limite"] = float(limite)
        g["disponivel"] = float(disponivel)
        venc = g.pop("vencimento_mais_antigo", None)
        g["vencimento_mais_antigo"] = venc.isoformat() if venc else ""
        g["vencimento_mais_antigo_texto"] = venc.strftime("%d/%m/%Y") if venc else "—"
        if g.get("tem_vencido"):
            g["situacao_resumo"] = "vencido"
            g["situacao_label"] = "Com vencido"
        elif g.get("tem_parcial"):
            g["situacao_resumo"] = "parcial"
            g["situacao_label"] = "Parcial"
        else:
            g["situacao_resumo"] = "aberto"
            g["situacao_label"] = "Em aberto"
        g.pop("tem_parcial", None)
        g.pop("tem_vencido", None)
        out.append(g)
    out.sort(key=lambda x: (-x["saldo_aberto"], x["cliente_nome"]))
    return out


def listar_titulos(
    *,
    cliente_agro_pk: int | None = None,
    cliente_nome: str = "",
    cliente_codigo: str = "",
    situacao: str = "abertos",
    busca: str = "",
    limit: int = 200,
) -> list[dict[str, Any]]:
    qs = FiadoTituloAgro.objects.only(
        "pk",
        "cliente_agro_id",
        "cliente_nome",
        "cliente_codigo",
        "numero_documento",
        "parcela_num",
        "parcela_total",
        "vencimento",
        "valor_bruto",
        "valor_pago",
        "situacao",
        "origem",
        "descricao",
        "venda_agro_id",
        "atualizado_em",
    ).all()
    sit = (situacao or "abertos").strip().lower()
    if sit == "abertos":
        qs = qs.exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        )
    elif sit == "vencidos":
        hoje = date.today()
        qs = qs.exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        ).filter(vencimento__lt=hoje)
    elif sit and sit != "todos":
        qs = qs.filter(situacao=sit)
    filtros_cli = _q_titulos_cliente_gestao(
        cliente_agro_pk=cliente_agro_pk,
        cliente_nome=cliente_nome,
        cliente_codigo=cliente_codigo,
    )
    if filtros_cli:
        qs = qs.filter(filtros_cli)
    qtxt = (busca or "").strip()
    if qtxt:
        qs = qs.filter(
            Q(cliente_nome__icontains=qtxt)
            | Q(cliente_codigo__icontains=qtxt)
            | Q(numero_documento__icontains=qtxt)
            | Q(descricao__icontains=qtxt)
        )
    qs = qs.order_by("vencimento", "cliente_nome")[: max(1, min(limit, 500))]
    return [titulo_para_dict(t) for t in qs]


def editar_titulo_fiado(
    titulo_id: int,
    *,
    vencimento: date | str | None = None,
    valor_bruto: Decimal | float | None = None,
    numero_documento: str | None = None,
    descricao: str | None = None,
    usuario: str = "",
) -> FiadoTituloAgro:
    with transaction.atomic():
        titulo = FiadoTituloAgro.objects.select_for_update().get(pk=titulo_id)
        if titulo.situacao in (
            FiadoTituloAgro.Situacao.QUITADO,
            FiadoTituloAgro.Situacao.CANCELADO,
        ):
            raise ValueError("Não é possível editar título quitado ou cancelado.")
        snap_antes = titulo_snapshot(titulo)
        alterou = False

        if vencimento is not None:
            if isinstance(vencimento, str):
                vencimento = _parse_vencimento({"vencimento": vencimento}, titulo.vencimento)
            if vencimento != titulo.vencimento:
                titulo.vencimento = vencimento
                alterou = True

        if valor_bruto is not None:
            novo = _dec(valor_bruto)
            if novo < titulo.valor_pago:
                raise ValueError(
                    f"Valor não pode ser menor que o já pago (R$ {titulo.valor_pago:.2f})."
                )
            if novo != titulo.valor_bruto:
                titulo.valor_bruto = novo
                alterou = True

        if numero_documento is not None:
            doc = str(numero_documento or "").strip()[:80]
            if doc != titulo.numero_documento:
                titulo.numero_documento = doc
                alterou = True

        if descricao is not None:
            desc = str(descricao or "").strip()[:500]
            if desc != titulo.descricao:
                titulo.descricao = desc
                alterou = True

        if not alterou:
            return titulo

        _atualizar_situacao_titulo(titulo)
        titulo.save(
            update_fields=[
                "vencimento",
                "valor_bruto",
                "numero_documento",
                "descricao",
                "situacao",
                "atualizado_em",
            ]
        )
        registrar_evento_fiado(
            "titulo_editado",
            cliente_agro=titulo.cliente_agro,
            titulo=titulo,
            payload={"antes": snap_antes, "depois": titulo_snapshot(titulo)},
            usuario=usuario,
        )
        return titulo


def baixar_titulos_selecionados(
    titulo_ids: list[int],
    forma_pagamento: str,
    *,
    valor: Decimal | float | None = None,
    request=None,
    observacao: str = "",
    registrar_caixa: bool = True,
    usuario: str = "",
) -> dict[str, Any]:
    ids = [int(x) for x in titulo_ids if x]
    if not ids:
        raise ValueError("Selecione ao menos um título.")
    titulos = list(
        FiadoTituloAgro.objects.filter(pk__in=ids)
        .exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        )
        .order_by("vencimento", "pk")
    )
    if not titulos:
        raise ValueError("Nenhum título em aberto na seleção.")
    if len(titulos) != len(set(ids)):
        raise ValueError("Um ou mais títulos não estão disponíveis para baixa.")

    saldo_sel = sum(t.saldo_aberto for t in titulos)
    v_total = _dec(valor) if valor is not None else saldo_sel
    if v_total <= 0:
        raise ValueError("Informe um valor maior que zero.")
    if v_total > saldo_sel + Decimal("0.009"):
        raise ValueError(f"Valor maior que o saldo selecionado (R$ {saldo_sel:.2f}).")

    nome_cli = titulos[0].cliente_nome
    baixas_ids: list[int] = []
    mov = None
    with transaction.atomic():
        sessao = None
        if registrar_caixa and request is not None:
            sessao = obter_sessao_caixa_aberta_request(request)
            if sessao is None:
                raise ValueError("Abra o caixa neste navegador para registrar o recebimento.")
            forma = normalizar_forma_pagamento_caixa(forma_pagamento or "Dinheiro")
            obs_caixa = f"Baixa fiado ({len(titulos)} tít.) — {nome_cli[:30]}"
            if observacao:
                obs_caixa = f"{obs_caixa} — {observacao}"[:500]
            user_mov = request.user if getattr(request, "user", None) and request.user.is_authenticated else None
            mov = MovimentoCaixa.objects.create(
                sessao_caixa=sessao,
                tipo=MovimentoCaixa.Tipo.REFORCO,
                forma_pagamento=forma,
                valor=v_total,
                observacao=obs_caixa,
                usuario=user_mov,
            )

        restante = v_total
        for titulo in titulos:
            if restante <= Decimal("0"):
                break
            parcela = min(restante, titulo.saldo_aberto)
            if parcela <= Decimal("0"):
                continue
            baixa = baixar_titulo(
                titulo.pk,
                parcela,
                forma_pagamento,
                request=request,
                observacao=observacao,
                registrar_caixa=False,
                sessao_caixa=sessao,
                usuario=usuario,
            )
            if mov:
                baixa.movimento_caixa = mov
                baixa.sessao_caixa = sessao
                baixa.save(update_fields=["movimento_caixa", "sessao_caixa"])
            baixas_ids.append(baixa.pk)
            restante = (restante - parcela).quantize(Decimal("0.01"))

    return {
        "valor_aplicado": float(v_total.quantize(Decimal("0.01"))),
        "baixas_ids": baixas_ids,
        "titulos_afetados": len(baixas_ids),
        "movimento_caixa_id": mov.pk if mov else None,
    }


def export_backup_fiado() -> dict[str, Any]:
    """Snapshot completo para download (redundância)."""
    titulos = [
        titulo_snapshot(t)
        for t in FiadoTituloAgro.objects.select_related("cliente_agro", "venda_agro").order_by("pk")
    ]
    baixas = []
    for b in FiadoBaixaAgro.objects.select_related("titulo").order_by("pk"):
        baixas.append(
            {
                "id": b.pk,
                "titulo_id": b.titulo_id,
                "valor": float(b.valor),
                "forma_pagamento": b.forma_pagamento,
                "usuario": b.usuario,
                "observacao": b.observacao,
                "movimento_caixa_id": b.movimento_caixa_id,
                "criado_em": b.criado_em.isoformat() if b.criado_em else "",
            }
        )
    eventos = []
    for e in FiadoEventoAgro.objects.order_by("pk"):
        eventos.append(
            {
                "id": e.pk,
                "tipo": e.tipo,
                "cliente_agro_id": e.cliente_agro_id,
                "titulo_id": e.titulo_id,
                "baixa_id": e.baixa_id,
                "payload_json": e.payload_json,
                "usuario": e.usuario,
                "criado_em": e.criado_em.isoformat() if e.criado_em else "",
            }
        )
    return {
        "gerado_em": timezone.now().isoformat(),
        "titulos": titulos,
        "baixas": baixas,
        "eventos": eventos,
        "totais": resumo_gestao_fiado(),
    }


def backfill_titulos_vendas_fiado(*, limite: int = 500) -> dict[str, int]:
    """Cria títulos para vendas fiado antigas sem ledger."""
    qs = (
        VendaAgro.objects.filter(devolvida_em__isnull=True)
        .order_by("-pk")
    )
    criados = 0
    pulados = 0
    for v in qs[:limite]:
        if not venda_local_tem_fiado(v):
            continue
        if FiadoTituloAgro.objects.filter(venda_agro=v).exists():
            pulados += 1
            continue
        titulos = criar_titulos_de_venda(v)
        if titulos:
            criados += len(titulos)
    return {"criados": criados, "pulados": pulados}
