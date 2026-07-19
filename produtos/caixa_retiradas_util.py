"""Histórico de retiradas / saídas do caixa (financeiro + movimento turno + vales RH)."""
from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from typing import Any

from django.db.models import Q
from django.utils import timezone

try:
    from produtos.caixa_util import (
        normalizar_rotulo_operador_exibicao as _normalizar_rotulo_operador_exibicao,
        rotulo_usuario_django as _rotulo_usuario_django,
    )
except ImportError:  # loja v5.65 — cherry-pick perdeu helpers em caixa_util

    def _rotulo_usuario_django(user) -> str:
        if user is None or not getattr(user, "is_authenticated", False):
            return ""
        nome = (user.get_full_name() or user.first_name or "").strip()
        if nome:
            return nome[:150]
        un = (user.get_username() if hasattr(user, "get_username") else "").strip()
        if un:
            return un[:150]
        email = (getattr(user, "email", None) or "").strip()
        if email and "@" in email:
            return email.split("@", 1)[0].strip()[:150]
        pk = getattr(user, "pk", None)
        return str(pk)[:150] if pk is not None else ""

    def _normalizar_rotulo_operador_exibicao(raw: str) -> str:
        s = (raw or "").strip()
        if not s:
            return ""
        if "@" in s and not s.startswith("@"):
            local = s.split("@", 1)[0].strip()
            return local or s
        return s

from produtos.models import MovimentoCaixa, TituloFinanceiroAgro
from produtos.saida_caixa_planos import SAIDA_CAIXA_PLANOS
from rh.constants import PLANO_ADIANTAMENTO_CANONICO
from rh.services.importador_vales_caixa import plano_e_adiantamento_salario_vale

# ASCII — evita mojibake (ÔÇö) em colunas vazias na loja
_SEM_VALOR = "-"

_VALE_PLANO_LABEL = next(
    (p["label"] for p in SAIDA_CAIXA_PLANOS if p.get("id") == "adiant_vale"),
    "Adiantamento de Salário (Vale)",
)


def _op_exib(raw: str) -> str:
    n = _normalizar_rotulo_operador_exibicao(raw)
    return n or _SEM_VALOR


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


def _extrair_quem_descricao(desc: str) -> str:
    m = re.search(r"Quem:\s*(.+?)(?:\s*·|$)", str(desc or ""), re.I)
    return (m.group(1).strip() if m else "")[:200]


def _row_sort_key(row: dict[str, Any]) -> tuple:
    d = row.get("data") or date.min
    ts = row.get("criado_em")
    return (d, ts or timezone.now())


def _variantes_plano_filtro(plano_f: str) -> list[str]:
    plano_f = (plano_f or "").strip()
    if not plano_f:
        return []
    out = {plano_f}
    for p in SAIDA_CAIXA_PLANOS:
        if (p.get("plano") or "").strip() == plano_f or (p.get("label") or "").strip() == plano_f:
            if p.get("plano"):
                out.add(str(p["plano"]).strip())
            if p.get("label"):
                out.add(str(p["label"]).strip())
    return [x for x in out if x]


def _texto_match_plano_filtro(plano_f: str, *textos: str) -> bool:
    if not (plano_f or "").strip():
        return True
    if plano_e_adiantamento_salario_vale(plano_f):
        for t in textos:
            if t and plano_e_adiantamento_salario_vale(str(t)):
                return True
    pf = plano_f.strip().lower()
    for v in _variantes_plano_filtro(plano_f):
        vl = v.lower()
        for t in textos:
            tl = (t or "").strip().lower()
            if not tl:
                continue
            if pf in tl or vl in tl or tl in pf:
                return True
    return False


def _plano_filtro_inclui_vales_rh(plano_f: str) -> bool:
    if not (plano_f or "").strip():
        return True
    return _texto_match_plano_filtro(
        plano_f,
        PLANO_ADIANTAMENTO_CANONICO,
        _VALE_PLANO_LABEL,
    )


def _movimento_e_vale_adiantamento(obs: str) -> bool:
    o = (obs or "").strip().lower()
    if not o:
        return False
    if plano_e_adiantamento_salario_vale(obs):
        return True
    return "adiantamento" in o and ("vale" in o or "salário" in o or "salario" in o)


def _chave_dedup(data: date, valor: Decimal, quem: str) -> tuple:
    return (data, _dec(valor), (quem or "").strip().lower())


def listar_retiradas_historico(
    *,
    data_de: date,
    data_ate: date,
    plano: str = "",
    quem: str = "",
    limite: int = 300,
    exportar: bool = False,
    deposito: str | None = None,
) -> dict[str, Any]:
    """
    ``deposito``: ``centro`` | ``vila`` | ``None`` (todas).
    Filtra pelo ponto do turno (Gaveta=Centro · Vila Elias).
    """
    from produtos.caixa_util import (
        PONTO_CAIXA_GAVETA,
        normalizar_ponto_caixa,
        ponto_pai_de_deposito,
    )
    from produtos.models import SessaoCaixa
    from produtos.pdv_deposito_util import normalizar_deposito

    plano_f = (plano or "").strip()
    quem_f = (quem or "").strip().lower()
    cap = 10000 if exportar else 500
    default_lim = 5000 if exportar else 300
    limite = max(1, min(int(limite or default_lim), cap))

    dep_filtro: str | None = None
    raw_dep = str(deposito or "").strip().lower()
    if raw_dep in ("todas", "todos", "all"):
        dep_filtro = None
    elif raw_dep:
        dep_filtro = normalizar_deposito(raw_dep)

    ponto_filtro = ponto_pai_de_deposito(dep_filtro) if dep_filtro else None
    sessoes_ok: set[int] | None = None
    mov_ids_loja: set[int] = set()
    if ponto_filtro:
        sessoes_ok = set(
            SessaoCaixa.objects.filter(ponto_caixa=ponto_filtro).values_list("pk", flat=True)
        )
        mov_ids_loja = set(
            MovimentoCaixa.objects.filter(
                tipo=MovimentoCaixa.Tipo.RETIRADA,
                criado_em__date__gte=data_de,
                criado_em__date__lte=data_ate,
                sessao_caixa_id__in=sessoes_ok,
            ).values_list("pk", flat=True)
        )

    def _sessao_ok(sid) -> bool:
        if sessoes_ok is None:
            return True
        try:
            return int(sid) in sessoes_ok
        except (TypeError, ValueError):
            return False

    def _titulo_bate_loja(snap: dict, mov_id) -> bool:
        if sessoes_ok is None:
            return True
        sid = snap.get("sessao_caixa_id")
        if sid is not None and str(sid).strip() != "":
            return _sessao_ok(sid)
        if mov_id is not None and str(mov_id).strip() != "":
            try:
                mid = int(mov_id)
            except (TypeError, ValueError):
                mid = 0
            if mid and mid in mov_ids_loja:
                return True
            m = (
                MovimentoCaixa.objects.filter(pk=mid)
                .select_related("sessao_caixa")
                .first()
            )
            if m and m.sessao_caixa_id:
                return _sessao_ok(m.sessao_caixa_id)
            return False
        # Legado sem vínculo de turno → só conta no Centro (padrão histórico).
        return dep_filtro == "centro"

    def _vale_bate_loja(v) -> bool:
        if sessoes_ok is None:
            return True
        ref = (getattr(v, "referencia_externa_id", None) or "").strip()
        if ref.isdigit():
            mid = int(ref)
            if mid in mov_ids_loja:
                return True
            m = (
                MovimentoCaixa.objects.filter(pk=mid)
                .select_related("sessao_caixa")
                .first()
            )
            if m and m.sessao_caixa_id:
                return _sessao_ok(m.sessao_caixa_id)
        # Vale sem vínculo: legado Centro; Vila não lista (evita misturar).
        return dep_filtro == "centro"

    linhas: list[dict[str, Any]] = []
    ids_mov_vistos: set[int] = set()
    chaves_vistos: set[tuple] = set()

    qs = TituloFinanceiroAgro.objects.filter(
        despesa=True,
        descricao__icontains="Saída caixa",
        data_competencia__gte=data_de,
        data_competencia__lte=data_ate,
    )
    if plano_f:
        plano_q = Q()
        for v in _variantes_plano_filtro(plano_f):
            plano_q |= Q(plano_conta__icontains=v)
        if plano_q:
            qs = qs.filter(plano_q)
        else:
            qs = qs.filter(plano_conta__icontains=plano_f)
    if quem_f:
        qs = qs.filter(
            Q(cliente__icontains=quem_f) | Q(descricao__icontains=quem_f)
        )

    for t in qs.order_by("-data_competencia", "-importado_em")[:limite]:
        nome_quem = (t.cliente or "").strip() or _extrair_quem_descricao(t.descricao)
        if plano_f and not _texto_match_plano_filtro(plano_f, t.plano_conta or ""):
            continue
        snap = t.dados_snapshot_json if isinstance(t.dados_snapshot_json, dict) else {}
        mov_id = snap.get("movimento_caixa_id")
        if not _titulo_bate_loja(snap, mov_id):
            continue
        if mov_id:
            try:
                ids_mov_vistos.add(int(mov_id))
            except (TypeError, ValueError):
                pass
        chaves_vistos.add(_chave_dedup(t.data_competencia, _dec(t.valor_bruto), nome_quem))
        linhas.append(
            {
                "id": f"t-{t.pk}",
                "fonte": "financeiro",
                "data": t.data_competencia,
                "criado_em": t.importado_em or t.atualizado_em,
                "valor": _dec(t.valor_bruto),
                "plano": (t.plano_conta or "").strip() or _SEM_VALOR,
                "quem": nome_quem or _SEM_VALOR,
                "forma": (t.forma_pagamento or "").strip() or _SEM_VALOR,
                "banco": (t.banco or "").strip() or _SEM_VALOR,
                "descricao": (t.descricao or "").strip(),
                "observacoes": (t.observacoes or "").strip(),
                "operador": _op_exib(t.usuario_lancou or t.criado_por or ""),
                "operador_pin": _op_exib(
                    t.usuario_lancou or t.criado_por or t.modificado_por or ""
                ),
                "sessao_id": snap.get("sessao_caixa_id"),
                "mongo_id": (t.mongo_id or "").strip(),
            }
        )

    if _plano_filtro_inclui_vales_rh(plano_f):
        from rh.models import ValeFuncionario

        vq = ValeFuncionario.objects.filter(
            cancelado=False,
            data__gte=data_de,
            data__lte=data_ate,
        ).select_related("funcionario", "criado_por")
        if quem_f:
            vq = vq.filter(
                Q(funcionario__nome_cache__icontains=quem_f)
                | Q(funcionario__cliente_agro__nome__icontains=quem_f)
            )

        for v in vq.order_by("-data", "-criado_em")[:limite]:
            if not _vale_bate_loja(v):
                continue
            nome_quem = (v.funcionario.nome_exibicao if v.funcionario else "").strip()
            if quem_f and quem_f not in nome_quem.lower():
                continue
            ck = _chave_dedup(v.data, _dec(v.valor), nome_quem)
            if ck in chaves_vistos:
                continue
            chaves_vistos.add(ck)
            op = _rotulo_usuario_django(v.criado_por) if v.criado_por else ""
            linhas.append(
                {
                    "id": f"v-{v.pk}",
                    "fonte": "rh_vale",
                    "data": v.data,
                    "criado_em": v.criado_em,
                    "valor": _dec(v.valor),
                    "plano": _VALE_PLANO_LABEL,
                    "quem": nome_quem or _SEM_VALOR,
                    "forma": _SEM_VALOR,
                    "banco": _SEM_VALOR,
                    "descricao": (v.observacao or "").strip() or "Vale / adiantamento (RH)",
                    "observacoes": "",
                    "operador": _op_exib(op),
                    "operador_pin": _op_exib(op),
                    "sessao_id": None,
                    "mongo_id": (v.referencia_externa_id or "").strip(),
                }
            )

    mov_qs = MovimentoCaixa.objects.filter(
        tipo=MovimentoCaixa.Tipo.RETIRADA,
        criado_em__date__gte=data_de,
        criado_em__date__lte=data_ate,
    ).select_related("sessao_caixa", "usuario")
    if sessoes_ok is not None:
        mov_qs = mov_qs.filter(sessao_caixa_id__in=sessoes_ok)
    if quem_f:
        mov_qs = mov_qs.filter(observacao__icontains=quem_f)
    if plano_f:
        plano_mov_q = Q()
        for v in _variantes_plano_filtro(plano_f):
            plano_mov_q |= Q(observacao__icontains=v)
        if plano_mov_q:
            mov_qs = mov_qs.filter(plano_mov_q)
        else:
            mov_qs = mov_qs.filter(observacao__icontains=plano_f)

    for m in mov_qs.order_by("-criado_em")[:limite]:
        if m.pk in ids_mov_vistos:
            continue
        if sessoes_ok is not None and not _sessao_ok(m.sessao_caixa_id):
            continue
        obs = (m.observacao or "").strip()
        if plano_f and not _texto_match_plano_filtro(plano_f, obs):
            continue
        if _movimento_e_vale_adiantamento(obs):
            continue
        data_mov = timezone.localdate(m.criado_em)
        val_mov = _dec(m.valor)
        if any(
            r["fonte"] == "financeiro"
            and r["data"] == data_mov
            and _dec(r["valor"]) == val_mov
            for r in linhas
        ):
            continue
        op_mov = _rotulo_usuario_django(m.usuario) if m.usuario else ""
        ponto = normalizar_ponto_caixa(
            getattr(m.sessao_caixa, "ponto_caixa", None) if m.sessao_caixa_id else None
        )
        linhas.append(
            {
                "id": f"m-{m.pk}",
                "fonte": "caixa",
                "data": timezone.localdate(m.criado_em),
                "criado_em": m.criado_em,
                "valor": _dec(m.valor),
                "plano": obs.split(" · ")[0][:120] if obs else "Depósito / caixa",
                "quem": _SEM_VALOR,
                "forma": (m.forma_pagamento or "").strip() or _SEM_VALOR,
                "banco": _SEM_VALOR,
                "descricao": obs or "Retirada no turno",
                "observacoes": "",
                "operador": _op_exib(op_mov),
                "operador_pin": _op_exib(op_mov),
                "sessao_id": m.sessao_caixa_id,
                "mongo_id": "",
                "ponto_caixa": ponto or PONTO_CAIXA_GAVETA,
            }
        )

    linhas.sort(key=_row_sort_key, reverse=True)
    linhas = linhas[:limite]
    total = sum((_dec(r["valor"]) for r in linhas), Decimal("0.00"))

    filtro_loja = dep_filtro or "todas"
    if filtro_loja == "vila":
        filtro_loja_label = "Vila Elias"
    elif filtro_loja == "centro":
        filtro_loja_label = "Centro"
    else:
        filtro_loja_label = "Todas"

    return {
        "linhas": linhas,
        "qtd": len(linhas),
        "total": total,
        "filtro_loja": filtro_loja,
        "filtro_loja_label": filtro_loja_label,
    }


def listar_quem_retiradas_distintas(*, limite: int = 80) -> list[str]:
    """Nomes usados em retiradas recentes (para filtro)."""
    limite = max(1, min(int(limite or 80), 200))
    nomes: list[str] = []
    vistos: set[str] = set()
    qs = (
        TituloFinanceiroAgro.objects.filter(despesa=True, descricao__icontains="Saída caixa")
        .exclude(cliente="")
        .order_by("-data_competencia")[:500]
    )
    for t in qs:
        n = (t.cliente or "").strip()
        if not n:
            n = _extrair_quem_descricao(t.descricao)
        if not n:
            continue
        key = n.lower()
        if key in vistos:
            continue
        vistos.add(key)
        nomes.append(n)
        if len(nomes) >= limite:
            break

    if len(nomes) < limite:
        from rh.models import ValeFuncionario

        for v in (
            ValeFuncionario.objects.filter(cancelado=False)
            .select_related("funcionario")
            .order_by("-data")[:300]
        ):
            n = (v.funcionario.nome_exibicao if v.funcionario else "").strip()
            if not n:
                continue
            key = n.lower()
            if key in vistos:
                continue
            vistos.add(key)
            nomes.append(n)
            if len(nomes) >= limite:
                break

    nomes.sort(key=lambda x: x.lower())
    return nomes[:limite]
