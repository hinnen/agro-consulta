"""Contagem cíclica — sessão multi-celular (Postgres). Estoque só no fechamento."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from estoque.models import (
    AjusteRapidoEstoque,
    ContagemCiclicaEscopo,
    ContagemCiclicaLinha,
    ContagemCiclicaParticipante,
    ContagemCiclicaSessao,
    ContagemCiclicaStatus,
    OrigemAjusteEstoque,
)


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v if v is not None else 0).replace(",", "."))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _operador_da_request(request) -> tuple[str, Any]:
    rotulo = str(request.session.get("ajuste_mobile_operador") or "").strip()
    uid = request.session.get("ajuste_mobile_user_id")
    user = None
    if uid:
        from django.contrib.auth import get_user_model

        user = get_user_model().objects.filter(pk=uid).first()
    if not rotulo and user is not None:
        rotulo = (
            (user.get_full_name() or "").strip()
            or (getattr(user, "username", None) or "").strip()
        )
    return rotulo[:120], user


def sessao_gate_ok(request) -> bool:
    return bool(
        str(request.session.get("ajuste_mobile_operador") or "").strip()
        or request.session.get("ajuste_mobile_user_id")
    )


def _qs_produtos_escopo(escopo_tipo: str, escopo_valor: str):
    from produtos.models import Produto

    qs = (
        Produto.objects.filter(ativo=True, cadastro_inativo=False)
        .exclude(Q(produto_externo_id__isnull=True) | Q(produto_externo_id=""))
        .only(
            "produto_externo_id",
            "codigo_interno",
            "codigo_nfe",
            "nome",
            "categoria",
            "custo",
        )
    )
    if escopo_tipo == ContagemCiclicaEscopo.CATEGORIA:
        cat = str(escopo_valor or "").strip()
        if not cat:
            return qs.none()
        qs = qs.filter(categoria__iexact=cat)
    elif escopo_tipo == ContagemCiclicaEscopo.CORREDOR:
        # Corredor: linhas nascem ao bipar (sem checklist prévio).
        return qs.none()
    return qs.order_by("nome")


def _mapa_saldos(pids: list[str], deposito: str) -> dict[str, float]:
    from produtos.estoque_saldo_agro_util import mapa_saldos_operacionais_agro

    if not pids:
        return {}
    out: dict[str, float] = {}
    dep = str(deposito or "centro").strip().lower() or "centro"
    db = client = None
    try:
        from produtos.views import obter_conexao_mongo

        db, client = obter_conexao_mongo()
    except Exception:
        db = client = None
    chunk = 400
    for i in range(0, len(pids), chunk):
        slice_ids = pids[i : i + chunk]
        try:
            m = mapa_saldos_operacionais_agro(slice_ids, db=db, client=client) or {}
        except Exception:
            m = {}
        key = "saldo_vila" if dep == "vila" else "saldo_centro"
        for pid in slice_ids:
            row = m.get(str(pid)) or {}
            out[str(pid)] = float(row.get(key) or 0)
    return out


def _sessao_aberta_conflito(deposito: str, escopo_tipo: str, escopo_valor: str):
    dep = str(deposito or "centro").strip().lower() or "centro"
    return (
        ContagemCiclicaSessao.objects.filter(
            deposito=dep,
            escopo_tipo=escopo_tipo,
            escopo_valor=str(escopo_valor or "").strip(),
            status__in=(ContagemCiclicaStatus.PASS1, ContagemCiclicaStatus.PASS2),
        )
        .order_by("-aberta_em")
        .first()
    )


def _pids_com_movimento(deposito: str, dias: int) -> set[str] | None:
    """None = sem filtro. set vazio = ninguém mexeu no período."""
    try:
        n = int(dias)
    except (TypeError, ValueError):
        n = 60
    if n <= 0:
        return None
    n = min(n, 3650)
    dep = str(deposito or "centro").strip().lower() or "centro"
    desde = timezone.now() - timedelta(days=n)
    rows = (
        AjusteRapidoEstoque.objects.filter(deposito=dep, criado_em__gte=desde)
        .exclude(produto_externo_id="")
        .values_list("produto_externo_id", flat=True)
        .distinct()
    )
    return {str(pid).strip() for pid in rows if str(pid).strip()}


@transaction.atomic
def abrir_sessao(
    *,
    deposito: str,
    escopo_tipo: str,
    escopo_valor: str,
    operador_rotulo: str,
    user=None,
    dias_movimentacao: int = 60,
) -> ContagemCiclicaSessao:
    dep = str(deposito or "centro").strip().lower() or "centro"
    if dep not in ("centro", "vila"):
        dep = "centro"
    tipo = str(escopo_tipo or "").strip().lower()
    if tipo not in (
        ContagemCiclicaEscopo.LOJA,
        ContagemCiclicaEscopo.CATEGORIA,
        ContagemCiclicaEscopo.CORREDOR,
    ):
        raise ValueError("Escopo inválido. Use loja, categoria ou corredor.")
    valor = str(escopo_valor or "").strip()
    if tipo == ContagemCiclicaEscopo.CATEGORIA and not valor:
        raise ValueError("Informe a categoria.")
    if tipo == ContagemCiclicaEscopo.CORREDOR and not valor:
        raise ValueError("Informe o nome do corredor.")
    if tipo == ContagemCiclicaEscopo.LOJA:
        valor = ""

    try:
        dias = int(dias_movimentacao)
    except (TypeError, ValueError):
        dias = 60
    if dias < 0:
        dias = 60
    dias = min(dias, 3650)

    conflito = _sessao_aberta_conflito(dep, tipo, valor)
    if conflito is not None:
        raise ValueError(
            f"Já existe contagem cíclica aberta #{conflito.pk} neste escopo. "
            "Entre nela ou cancele a outra."
        )

    mov_pids = _pids_com_movimento(dep, dias) if tipo != ContagemCiclicaEscopo.CORREDOR else None

    sessao = ContagemCiclicaSessao.objects.create(
        deposito=dep,
        escopo_tipo=tipo,
        escopo_valor=valor[:200],
        dias_movimentacao=dias,
        status=ContagemCiclicaStatus.PASS1,
        passagem_atual=1,
        aberta_por_rotulo=(operador_rotulo or "")[:120],
        aberta_por=user,
    )
    ContagemCiclicaParticipante.objects.create(
        sessao=sessao,
        operador_rotulo=(operador_rotulo or "Operador")[:120],
        usuario=user,
    )

    if tipo != ContagemCiclicaEscopo.CORREDOR:
        produtos = list(_qs_produtos_escopo(tipo, valor))
        if mov_pids is not None:
            produtos = [
                p
                for p in produtos
                if str(p.produto_externo_id or "").strip() in mov_pids
            ]
            if not produtos:
                sessao.delete()
                raise ValueError(
                    f"Nenhum produto com movimento nos últimos {dias} dias neste estoque. "
                    "Aumente os dias ou use «Todos»."
                )
        pids = [str(p.produto_externo_id).strip() for p in produtos if p.produto_externo_id]
        saldos = _mapa_saldos(pids, dep)
        batch: list[ContagemCiclicaLinha] = []
        for p in produtos:
            pid = str(p.produto_externo_id or "").strip()
            if not pid:
                continue
            codigo = (
                str(getattr(p, "codigo_nfe", None) or "").strip()
                or str(p.codigo_interno or "").strip()
            )[:100]
            batch.append(
                ContagemCiclicaLinha(
                    sessao=sessao,
                    produto_externo_id=pid[:100],
                    codigo_interno=codigo,
                    nome_produto=str(p.nome or "")[:255],
                    categoria=str(p.categoria or "")[:200],
                    saldo_referencia=_dec(saldos.get(pid, 0)),
                    custo_ref=_dec(getattr(p, "custo", 0) or 0),
                )
            )
            if len(batch) >= 400:
                ContagemCiclicaLinha.objects.bulk_create(batch, ignore_conflicts=True)
                batch = []
        if batch:
            ContagemCiclicaLinha.objects.bulk_create(batch, ignore_conflicts=True)
        sessao.total_itens = ContagemCiclicaLinha.objects.filter(sessao=sessao).count()
        sessao.save(update_fields=["total_itens"])

    return sessao


def entrar_sessao(sessao: ContagemCiclicaSessao, operador_rotulo: str, user=None) -> None:
    if sessao.status not in (ContagemCiclicaStatus.PASS1, ContagemCiclicaStatus.PASS2):
        raise ValueError("Esta contagem já foi fechada ou cancelada.")
    rot = (operador_rotulo or "Operador")[:120]
    ContagemCiclicaParticipante.objects.update_or_create(
        sessao=sessao,
        operador_rotulo=rot,
        defaults={"usuario": user},
    )


def listar_sessoes_abertas(deposito: str | None = None) -> list[ContagemCiclicaSessao]:
    qs = ContagemCiclicaSessao.objects.filter(
        status__in=(ContagemCiclicaStatus.PASS1, ContagemCiclicaStatus.PASS2)
    )
    if deposito:
        dep = str(deposito).strip().lower()
        if dep in ("centro", "vila"):
            qs = qs.filter(deposito=dep)
    return list(qs.order_by("-aberta_em")[:40])


def _diff_abs(a: Decimal | None, b: Decimal) -> Decimal:
    if a is None:
        return abs(b)
    return abs(_dec(a) - _dec(b))


def marcar_recontagem(linha: ContagemCiclicaLinha) -> bool:
    """True se divergiu do saldo congelado (qualquer diferença ≥ 0,001)."""
    q = linha.qtd_pass1 if linha.qtd_pass1 is not None else Decimal("0")
    return _diff_abs(q, linha.saldo_referencia) >= Decimal("0.001")


@transaction.atomic
def registrar_contagem(
    sessao: ContagemCiclicaSessao,
    *,
    produto_externo_id: str,
    qtd,
    operador_rotulo: str,
    nome_produto: str = "",
    codigo_interno: str = "",
    categoria: str = "",
) -> ContagemCiclicaLinha:
    if sessao.status not in (ContagemCiclicaStatus.PASS1, ContagemCiclicaStatus.PASS2):
        raise ValueError("Contagem fechada — não dá para lançar.")
    pid = str(produto_externo_id or "").strip()
    if not pid:
        raise ValueError("Produto inválido.")
    q = _dec(qtd)
    if q < 0:
        raise ValueError("Quantidade não pode ser negativa.")
    rot = (operador_rotulo or "")[:120]
    now = timezone.now()

    linha = (
        ContagemCiclicaLinha.objects.select_for_update()
        .filter(sessao=sessao, produto_externo_id=pid[:100])
        .first()
    )

    if linha is None:
        if sessao.escopo_tipo != ContagemCiclicaEscopo.CORREDOR and sessao.status == ContagemCiclicaStatus.PASS1:
            raise ValueError("Produto fora do escopo desta contagem.")
        if sessao.status == ContagemCiclicaStatus.PASS2:
            raise ValueError("Na recontagem só entram itens da fila de diferenças.")
        saldos = _mapa_saldos([pid], sessao.deposito)
        linha = ContagemCiclicaLinha.objects.create(
            sessao=sessao,
            produto_externo_id=pid[:100],
            codigo_interno=(codigo_interno or "")[:100],
            nome_produto=(nome_produto or "")[:255],
            categoria=(categoria or "")[:200],
            saldo_referencia=_dec(saldos.get(pid, 0)),
        )
        ContagemCiclicaSessao.objects.filter(pk=sessao.pk).update(
            total_itens=ContagemCiclicaLinha.objects.filter(sessao=sessao).count()
        )

    if sessao.status == ContagemCiclicaStatus.PASS1:
        ja = linha.contado_pass1
        # Sempre soma: produto pode estar em 2+ lugares (prateleira + fundo).
        base = _dec(linha.qtd_pass1) if ja and linha.qtd_pass1 is not None else Decimal("0")
        linha.qtd_pass1 = base + q
        linha.contado_pass1 = True
        linha.auto_zero_pass1 = False
        linha.operador_pass1 = rot
        linha.contado_pass1_em = now
        if nome_produto and not linha.nome_produto:
            linha.nome_produto = nome_produto[:255]
        if codigo_interno and not linha.codigo_interno:
            linha.codigo_interno = codigo_interno[:100]
        linha.save(
            update_fields=[
                "qtd_pass1",
                "contado_pass1",
                "auto_zero_pass1",
                "operador_pass1",
                "contado_pass1_em",
                "nome_produto",
                "codigo_interno",
            ]
        )
        if not ja:
            ContagemCiclicaSessao.objects.filter(pk=sessao.pk).update(
                contados_pass1=ContagemCiclicaLinha.objects.filter(
                    sessao=sessao, contado_pass1=True
                ).count()
            )
    else:
        if not linha.precisa_recontagem:
            raise ValueError("Este item não está na fila de recontagem.")
        ja = linha.contado_pass2
        base = _dec(linha.qtd_pass2) if ja and linha.qtd_pass2 is not None else Decimal("0")
        linha.qtd_pass2 = base + q
        linha.contado_pass2 = True
        linha.operador_pass2 = rot
        linha.contado_pass2_em = now
        linha.save(
            update_fields=[
                "qtd_pass2",
                "contado_pass2",
                "operador_pass2",
                "contado_pass2_em",
            ]
        )
        if not ja:
            ContagemCiclicaSessao.objects.filter(pk=sessao.pk).update(
                contados_pass2=ContagemCiclicaLinha.objects.filter(
                    sessao=sessao, contado_pass2=True, precisa_recontagem=True
                ).count()
            )
    return linha


@transaction.atomic
def fechar_passagem_1(sessao: ContagemCiclicaSessao) -> dict[str, Any]:
    if sessao.status != ContagemCiclicaStatus.PASS1:
        raise ValueError("Só dá para fechar a passagem 1 nesta fase.")

    # Loja/categoria: não bipados = 0
    if sessao.escopo_tipo != ContagemCiclicaEscopo.CORREDOR:
        ContagemCiclicaLinha.objects.filter(sessao=sessao, contado_pass1=False).update(
            qtd_pass1=Decimal("0"),
            contado_pass1=True,
            auto_zero_pass1=True,
            operador_pass1="(auto zero)",
            contado_pass1_em=timezone.now(),
        )

    linhas = list(ContagemCiclicaLinha.objects.filter(sessao=sessao))
    n_rec = 0
    for ln in linhas:
        precisa = marcar_recontagem(ln)
        if precisa != ln.precisa_recontagem:
            ln.precisa_recontagem = precisa
            ln.save(update_fields=["precisa_recontagem"])
        if precisa:
            n_rec += 1

    sessao.status = ContagemCiclicaStatus.PASS2
    sessao.passagem_atual = 2
    sessao.pass1_fechada_em = timezone.now()
    sessao.contados_pass1 = ContagemCiclicaLinha.objects.filter(
        sessao=sessao, contado_pass1=True
    ).count()
    sessao.total_itens = ContagemCiclicaLinha.objects.filter(sessao=sessao).count()
    sessao.save(
        update_fields=[
            "status",
            "passagem_atual",
            "pass1_fechada_em",
            "contados_pass1",
            "total_itens",
        ]
    )
    return {
        "total": sessao.total_itens,
        "recontagem": n_rec,
        "auto_zero": ContagemCiclicaLinha.objects.filter(
            sessao=sessao, auto_zero_pass1=True
        ).count(),
    }


def _qtd_final_linha(ln: ContagemCiclicaLinha) -> Decimal:
    if ln.precisa_recontagem:
        if ln.qtd_pass2 is not None:
            return _dec(ln.qtd_pass2)
        return _dec(ln.qtd_pass1 if ln.qtd_pass1 is not None else 0)
    return _dec(ln.qtd_pass1 if ln.qtd_pass1 is not None else 0)


@transaction.atomic
def gravar_fechamento(sessao: ContagemCiclicaSessao, *, user=None, operador_rotulo: str = "") -> dict[str, Any]:
    if sessao.status != ContagemCiclicaStatus.PASS2:
        raise ValueError("Feche a passagem 1 e faça a recontagem antes de gravar.")

    pend = ContagemCiclicaLinha.objects.filter(
        sessao=sessao, precisa_recontagem=True, contado_pass2=False
    ).count()
    if pend:
        raise ValueError(f"Ainda faltam {pend} itens na recontagem.")

    from base.models import Empresa

    empresa = Empresa.objects.filter(nome_fantasia="Agro Mais").first()
    rot = (operador_rotulo or sessao.aberta_por_rotulo or "")[:80]
    criados = 0
    for ln in ContagemCiclicaLinha.objects.select_for_update().filter(sessao=sessao):
        q_final = _qtd_final_linha(ln)
        ln.qtd_final = q_final
        if _diff_abs(q_final, ln.saldo_referencia) < Decimal("0.001"):
            ln.save(update_fields=["qtd_final"])
            continue
        nome = (ln.nome_produto or "").strip()
        if rot and rot not in nome:
            nome = (f"{nome} · cíclica · {rot}" if nome else f"cíclica · {rot}")[:255]
        else:
            nome = (f"{nome} · cíclica" if nome else "cíclica")[:255]
        aj = AjusteRapidoEstoque.objects.create(
            empresa=empresa,
            produto_externo_id=ln.produto_externo_id,
            codigo_interno=ln.codigo_interno or "",
            nome_produto=nome,
            deposito=sessao.deposito,
            saldo_erp_referencia=ln.saldo_referencia,
            saldo_informado=q_final,
            origem=OrigemAjusteEstoque.CONTAGEM_CICLICA,
            usuario=user,
            observacao=f"Contagem cíclica sessão #{sessao.pk}",
        )
        ln.ajuste_id = aj.pk
        ln.save(update_fields=["qtd_final", "ajuste_id"])
        criados += 1

    sessao.status = ContagemCiclicaStatus.FECHADA
    sessao.fechada_em = timezone.now()
    sessao.contados_pass2 = ContagemCiclicaLinha.objects.filter(
        sessao=sessao, contado_pass2=True, precisa_recontagem=True
    ).count()
    sessao.save(update_fields=["status", "fechada_em", "contados_pass2"])

    try:
        from produtos.views import (
            _invalidar_caches_apos_ajuste_pin,
            _patch_catalogo_pdv_saldo_apos_ajuste,
        )

        _invalidar_caches_apos_ajuste_pin()
        for ln in ContagemCiclicaLinha.objects.filter(sessao=sessao, ajuste_id__isnull=False):
            try:
                _patch_catalogo_pdv_saldo_apos_ajuste(
                    ln.produto_externo_id, sessao.deposito, ln.qtd_final
                )
            except Exception:
                pass
    except Exception:
        pass

    return {"ajustes": criados, "sessao_id": sessao.pk}


@transaction.atomic
def cancelar_sessao(sessao: ContagemCiclicaSessao) -> None:
    if sessao.status == ContagemCiclicaStatus.FECHADA:
        raise ValueError("Contagem já gravada — não cancela.")
    sessao.status = ContagemCiclicaStatus.CANCELADA
    sessao.fechada_em = timezone.now()
    sessao.save(update_fields=["status", "fechada_em"])


def sessao_payload(sessao: ContagemCiclicaSessao, *, detalhe: bool = False) -> dict[str, Any]:
    total = sessao.total_itens or ContagemCiclicaLinha.objects.filter(sessao=sessao).count()
    c1 = sessao.contados_pass1
    if sessao.status == ContagemCiclicaStatus.PASS1:
        c1 = ContagemCiclicaLinha.objects.filter(sessao=sessao, contado_pass1=True).count()
    fila2 = ContagemCiclicaLinha.objects.filter(sessao=sessao, precisa_recontagem=True).count()
    c2 = ContagemCiclicaLinha.objects.filter(
        sessao=sessao, precisa_recontagem=True, contado_pass2=True
    ).count()
    parts = list(
        ContagemCiclicaParticipante.objects.filter(sessao=sessao).values_list(
            "operador_rotulo", flat=True
        )
    )
    out: dict[str, Any] = {
        "id": sessao.pk,
        "deposito": sessao.deposito,
        "escopo_tipo": sessao.escopo_tipo,
        "escopo_valor": sessao.escopo_valor,
        "dias_movimentacao": int(getattr(sessao, "dias_movimentacao", 60) or 0),
        "status": sessao.status,
        "passagem_atual": sessao.passagem_atual,
        "total_itens": total,
        "contados_pass1": c1,
        "faltam_pass1": max(0, total - c1) if sessao.escopo_tipo != ContagemCiclicaEscopo.CORREDOR else 0,
        "fila_recontagem": fila2,
        "contados_pass2": c2,
        "faltam_pass2": max(0, fila2 - c2),
        "aberta_por": sessao.aberta_por_rotulo,
        "aberta_em": sessao.aberta_em.isoformat() if sessao.aberta_em else "",
        "participantes": parts,
        "cego": True,
    }
    if detalhe:
        qs = ContagemCiclicaLinha.objects.filter(sessao=sessao)
        if sessao.status == ContagemCiclicaStatus.PASS2:
            qs = qs.filter(precisa_recontagem=True).order_by(
                "contado_pass2", "-saldo_referencia", "nome_produto"
            )
        else:
            qs = qs.order_by("contado_pass1", "nome_produto")
        total_qs = qs.count()
        linhas_raw = list(qs[:800])
        out["linhas_enviadas"] = len(linhas_raw)
        out["linhas_truncadas"] = total_qs > len(linhas_raw)
        # Completa nome/código pelo cadastro PG quando a linha veio vazia (Mongo id).
        pids_vazios = [
            str(ln.produto_externo_id).strip()
            for ln in linhas_raw
            if not str(ln.nome_produto or "").strip()
        ]
        nome_map: dict[str, tuple[str, str]] = {}
        if pids_vazios:
            from produtos.models import Produto

            for p in Produto.objects.filter(produto_externo_id__in=pids_vazios).only(
                "produto_externo_id", "nome", "codigo_nfe", "codigo_interno"
            ):
                pid = str(p.produto_externo_id or "").strip()
                if not pid:
                    continue
                cod = (
                    str(p.codigo_nfe or "").strip()
                    or str(p.codigo_interno or "").strip()
                )
                nome_map[pid] = (str(p.nome or "").strip(), cod)
        linhas = []
        for ln in linhas_raw:
            pid = str(ln.produto_externo_id or "").strip()
            nome = str(ln.nome_produto or "").strip()
            codigo = str(ln.codigo_interno or "").strip()
            if not nome and pid in nome_map:
                nome, cod_alt = nome_map[pid]
                if not codigo:
                    codigo = cod_alt
            if not nome:
                nome = codigo or f"Produto {pid[:8]}"
            item = {
                "id": ln.pk,
                "produto_id": pid,
                "codigo": codigo,
                "nome": nome,
                "categoria": ln.categoria,
                "contado": ln.contado_pass2
                if sessao.status == ContagemCiclicaStatus.PASS2
                else ln.contado_pass1,
                "operador": ln.operador_pass2
                if sessao.status == ContagemCiclicaStatus.PASS2
                else ln.operador_pass1,
                "auto_zero": ln.auto_zero_pass1,
                "faltando": not (
                    ln.contado_pass2
                    if sessao.status == ContagemCiclicaStatus.PASS2
                    else ln.contado_pass1
                ),
            }
            linhas.append(item)
        out["linhas"] = linhas
    return out


def categorias_disponiveis() -> list[str]:
    from produtos.models import Produto

    rows = (
        Produto.objects.filter(ativo=True, cadastro_inativo=False)
        .exclude(categoria__isnull=True)
        .exclude(categoria="")
        .values_list("categoria", flat=True)
        .distinct()
        .order_by("categoria")
    )
    return [str(c).strip() for c in rows if str(c).strip()][:400]
