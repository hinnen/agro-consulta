"""Lista CP/CR no Postgres ``TituloFinanceiroAgro`` — espelho Mongo com dedup."""
from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from django.db.models import Q, QuerySet
from django.utils import timezone

from produtos.models import TituloFinanceiroAgro

logger = logging.getLogger(__name__)

_TOL = Decimal("0.02")
_CAP_LINHAS = 25_000
_SEM_PLANO_MARKER = "__SEM_PLANO__"
_RX_SO_DIGITOS = re.compile(r"\D+")
_RX_PARCELA_TXT = re.compile(r"(?i)\bparcela\s*[:=]?\s*(\d{1,3})\b")
_RX_PARCELA_FRAC = re.compile(r"^(\d{1,3})\s*/\s*(\d{1,3})$")
_RX_DATA = re.compile(
    r"^(?:"
    r"(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{2,4})"  # dd/mm/aaaa
    r"|"
    r"(\d{4})[/.\-](\d{1,2})[/.\-](\d{1,2})"  # aaaa-mm-dd
    r")$"
)


def _cap_linhas_consulta_pg(
    *,
    mongo_id: str | None,
    vencimento_de: date | None,
    vencimento_ate: date | None,
    competencia_de: date | None,
    competencia_ate: date | None,
    pagamento_de: date | None,
    pagamento_ate: date | None,
    texto: str | None,
) -> int:
    """Limita scan em memória — janela de 1 dia (ex. «hoje») não precisa carregar milhares de títulos."""
    if (mongo_id or "").strip():
        return 4
    if vencimento_de and vencimento_ate and vencimento_de == vencimento_ate:
        return 800
    if vencimento_de or vencimento_ate:
        return 4000
    if competencia_de or competencia_ate or pagamento_de or pagamento_ate:
        return 6000
    if (texto or "").strip():
        return 8000
    return _CAP_LINHAS


def _ordenacao_pg_campo(ordenacao: str) -> str | None:
    ord_ = (ordenacao or "vencimento_asc").strip().lower()
    mapping = {
        "vencimento_asc": "data_vencimento",
        "vencimento_desc": "-data_vencimento",
        "fluxo_desc": "-data_fluxo",
        "cliente_asc": "cliente",
        "cliente_desc": "-cliente",
        "forma_asc": "forma_pagamento",
        "forma_desc": "-forma_pagamento",
        "plano_asc": "plano_conta",
        "plano_desc": "-plano_conta",
        "bruto_asc": "valor_bruto",
        "bruto_desc": "-valor_bruto",
        "saldo_asc": "valor_restante",
        "saldo_desc": "-valor_restante",
    }
    return mapping.get(ord_)


def _dec2(v: object) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


def _titulo_manual_agro_lote(t: TituloFinanceiroAgro) -> bool:
    obs = (t.observacoes or "").lower()
    mod = (t.modificado_por or "").lower()
    return "lote manual agro" in obs or "manual em lote agro" in mod


def _tem_id_erp_valido(t: TituloFinanceiroAgro) -> bool:
    snap = t.dados_snapshot_json or {}
    x = str(snap.get("id_erp") or "").strip()
    if not x:
        return False
    if len(x) == 24 and re.match(r"^[a-fA-F0-9]{24}$", x):
        return False
    return True


def dedup_key_titulo(t: TituloFinanceiroAgro) -> str:
    if _titulo_manual_agro_lote(t):
        return f"O|{t.mongo_id}"
    venc = t.data_vencimento.isoformat() if t.data_vencimento else "nod"
    pag = t.data_pagamento.isoformat() if t.data_pagamento else "np"
    bruto = round(float(t.valor_bruto), 2)
    return "|".join(
        [
            "SIG",
            (t.empresa or "").strip(),
            str(bool(t.despesa)),
            (t.cliente or "").strip(),
            str(bruto),
            venc,
            (t.plano_conta or "").strip(),
            (t.forma_pagamento or "").strip(),
            str(int(t.parcela or 0)),
            pag,
        ]
    )


def _dedup_ord(t: TituloFinanceiroAgro) -> tuple[Any, ...]:
    lu = t.mongo_ultima_atualizacao
    if lu is None:
        lu = datetime.min.replace(tzinfo=timezone.get_current_timezone())
    elif timezone.is_naive(lu):
        lu = timezone.make_aware(lu, timezone.get_current_timezone())
    return (_tem_id_erp_valido(t), lu, t.mongo_id or "")


def dedup_titulos(titulos: list[TituloFinanceiroAgro]) -> list[TituloFinanceiroAgro]:
    buckets: dict[str, TituloFinanceiroAgro] = {}
    for t in titulos:
        k = dedup_key_titulo(t)
        prev = buckets.get(k)
        if prev is None or _dedup_ord(t) > _dedup_ord(prev):
            buckets[k] = t
    return list(buckets.values())


def _titulo_aberto(t: TituloFinanceiroAgro) -> bool:
    if t.quitado:
        return False
    return _dec2(t.valor_restante) > _TOL


def _titulo_quitado_negocio(t: TituloFinanceiroAgro) -> bool:
    if t.quitado:
        return True
    return _dec2(t.valor_restante) <= _TOL


def _aplicar_status_qs(qs: QuerySet, status: str) -> QuerySet:
    st = (status or "abertos").strip().lower()
    if st == "abertos":
        return qs.filter(quitado=False, valor_restante__gt=_TOL)
    if st == "quitados":
        return qs.filter(Q(quitado=True) | Q(valor_restante__lte=_TOL))
    return qs


def _aplicar_exclusao_planos(qs: QuerySet, excluir_planos: list[str] | None) -> QuerySet:
    raw = [str(x).strip() for x in (excluir_planos or []) if x and str(x).strip()]
    if not raw:
        return qs
    exclui_sem = _SEM_PLANO_MARKER in raw or any(x.lower() == "(sem plano)" for x in raw)
    nomes = [x for x in raw if x != _SEM_PLANO_MARKER and x.lower() != "(sem plano)"]
    if nomes:
        qs = qs.exclude(plano_conta__in=nomes[:200])
    if exclui_sem:
        qs = qs.exclude(plano_conta="")
    return qs


def _so_digitos(s: str) -> str:
    return _RX_SO_DIGITOS.sub("", s or "")


def _normalizar_tokens_busca_pg(texto: str) -> list[str]:
    """Separa termos; junta «parcela N»; remove pontuação nas bordas; máx. 12."""
    t = (texto or "").strip()
    if not t:
        return []
    t = _RX_PARCELA_TXT.sub(r"parcela:\1", t)
    parts = re.split(r"\s+", t)
    out: list[str] = []
    for p in parts:
        p2 = p.strip('.,;:|()[]{}"\'').strip()
        if not p2:
            continue
        out.append(p2[:120])
        if len(out) >= 12:
            break
    return out or [t[:120]]


def _parse_data_busca_pg(tok: str) -> date | None:
    s = (tok or "").strip()
    if not s:
        return None
    m = _RX_DATA.match(s)
    if not m:
        return None
    try:
        if m.group(1) is not None:
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if y < 100:
                y += 2000 if y < 70 else 1900
        else:
            y, mo, d = int(m.group(4)), int(m.group(5)), int(m.group(6))
        return date(y, mo, d)
    except ValueError:
        return None


def _parse_valor_busca_pg(tok: str) -> Decimal | None:
    """Valor monetário (com ou sem vírgula / R$). Inteiro curto também conta."""
    s = (tok or "").strip()
    if not s:
        return None
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\xa0", " ").replace("\u202f", " ").strip()
    if s.upper().startswith("R$"):
        s = s[2:].lstrip().strip()
    if not s or not re.search(r"\d", s):
        return None
    if _parse_data_busca_pg(s) is not None:
        return None
    if _RX_PARCELA_FRAC.match(s) or s.lower().startswith("parcela:"):
        return None
    dig = _so_digitos(s)
    # CPF / CNPJ / boleto — não tratar como valor
    if dig and "," not in s and "." not in s and not s.upper().startswith("R"):
        if len(dig) in (11, 14) or len(dig) >= 40:
            return None
        # Nº documento longo (NF) — evita valor falso
        if len(dig) >= 8:
            return None
    s_num = s.replace(" ", "")
    try:
        if "," in s_num:
            q = Decimal(s_num.replace(".", "").replace(",", "."))
        elif re.fullmatch(r"\d{1,3}(\.\d{3})+", s_num):
            q = Decimal(s_num.replace(".", ""))
        else:
            q = Decimal(s_num)
    except (InvalidOperation, ValueError):
        return None
    if abs(q) >= Decimal("1e12"):
        return None
    return q.quantize(Decimal("0.01"))


def _parse_parcela_busca_pg(tok: str) -> int | None:
    s = (tok or "").strip()
    if not s:
        return None
    m = re.fullmatch(r"(?i)parcela:(\d{1,3})", s)
    if m:
        n = int(m.group(1))
        return n if 1 <= n <= 999 else None
    m2 = _RX_PARCELA_FRAC.match(s)
    if m2:
        n = int(m2.group(1))
        return n if 1 <= n <= 999 else None
    return None


def _variantes_doc_cpf_cnpj(dig: str) -> list[str]:
    """Gera grafias comuns de CPF/CNPJ a partir só dos dígitos."""
    out = [dig]
    if len(dig) == 11:
        out.append(f"{dig[:3]}.{dig[3:6]}.{dig[6:9]}-{dig[9:]}")
        out.append(f"{dig[:3]}{dig[3:6]}{dig[6:9]}{dig[9:]}")
    elif len(dig) == 14:
        out.append(f"{dig[:2]}.{dig[2:5]}.{dig[5:8]}/{dig[8:12]}-{dig[12:]}")
        out.append(f"{dig[:2]}{dig[2:5]}{dig[5:8]}{dig[8:12]}{dig[12:]}")
    # dedup preservando ordem
    seen: set[str] = set()
    uniq: list[str] = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _q_texto_basico(tok: str) -> Q:
    return (
        Q(cliente__icontains=tok)
        | Q(descricao__icontains=tok)
        | Q(numero_documento__icontains=tok)
        | Q(plano_conta__icontains=tok)
        | Q(grupo__icontains=tok)
        | Q(forma_pagamento__icontains=tok)
        | Q(banco__icontains=tok)
        | Q(empresa__icontains=tok)
        | Q(observacoes__icontains=tok)
        | Q(mongo_id__icontains=tok)
        | Q(centro_custo__icontains=tok)
        | Q(boleto_codigo_barras__icontains=tok)
        | Q(criado_por__icontains=tok)
        | Q(usuario_lancou__icontains=tok)
    )


def _q_token_busca_pg(tok: str) -> Q:
    """Um termo: texto + valor + data + boleto + doc + parcela + CPF/CNPJ."""
    q = _q_texto_basico(tok)

    dig = _so_digitos(tok)
    if dig and dig != tok:
        q |= (
            Q(numero_documento__icontains=dig)
            | Q(observacoes__icontains=dig)
            | Q(descricao__icontains=dig)
            | Q(boleto_codigo_barras__icontains=dig)
            | Q(cliente__icontains=dig)
        )

    # P1 — boleto / linha digitável
    if dig and len(dig) >= 40:
        q |= Q(boleto_codigo_barras__icontains=dig[:54])
        if len(dig) >= 44:
            q |= Q(boleto_codigo_barras__icontains=dig[:44])

    # P2 — CPF / CNPJ (com ou sem máscara)
    if dig and len(dig) in (11, 14):
        for v in _variantes_doc_cpf_cnpj(dig):
            q |= (
                Q(cliente__icontains=v)
                | Q(descricao__icontains=v)
                | Q(observacoes__icontains=v)
                | Q(numero_documento__icontains=v)
            )

    # P2 — parcela (2/6 ou parcela:2)
    parc = _parse_parcela_busca_pg(tok)
    if parc is not None:
        q |= Q(parcela=parc)

    # P0 — data digitada (venc. / competência / pagamento)
    dt = _parse_data_busca_pg(tok)
    if dt is not None:
        q |= Q(data_vencimento=dt) | Q(data_competencia=dt) | Q(data_pagamento=dt)

    # P0 — valor (bruto / pago / restante)
    val = _parse_valor_busca_pg(tok)
    if val is not None:
        lo, hi = val - _TOL, val + _TOL
        q |= (
            Q(valor_bruto__gte=lo, valor_bruto__lte=hi)
            | Q(valor_pago__gte=lo, valor_pago__lte=hi)
            | Q(valor_restante__gte=lo, valor_restante__lte=hi)
        )

    return q


def _aplicar_texto_qs(qs: QuerySet, texto: str | None) -> QuerySet:
    t = (texto or "").strip()
    if not t:
        return qs
    tokens = _normalizar_tokens_busca_pg(t)
    for tok in tokens:
        if not tok:
            continue
        qs = qs.filter(_q_token_busca_pg(tok))
    return qs


def titulos_financeiro_montar_qs(
    *,
    despesa: bool | None = None,
    status: str = "abertos",
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    competencia_de: date | None = None,
    competencia_ate: date | None = None,
    pagamento_de: date | None = None,
    pagamento_ate: date | None = None,
    texto: str | None = None,
    excluir_planos_nomes: list[str] | None = None,
    mongo_id: str | None = None,
) -> QuerySet:
    qs = TituloFinanceiroAgro.objects.all()
    if despesa is not None:
        qs = qs.filter(despesa=bool(despesa))
    mid = (mongo_id or "").strip()
    if mid:
        return qs.filter(mongo_id=mid)
    qs = _aplicar_status_qs(qs, status)
    if vencimento_de is not None:
        qs = qs.filter(data_vencimento__gte=vencimento_de)
    if vencimento_ate is not None:
        qs = qs.filter(data_vencimento__lte=vencimento_ate)
    if competencia_de is not None:
        qs = qs.filter(data_competencia__gte=competencia_de)
    if competencia_ate is not None:
        qs = qs.filter(data_competencia__lte=competencia_ate)
    if pagamento_de is not None:
        qs = qs.filter(data_pagamento__gte=pagamento_de)
    if pagamento_ate is not None:
        qs = qs.filter(data_pagamento__lte=pagamento_ate)
    qs = _aplicar_exclusao_planos(qs, excluir_planos_nomes)
    qs = _aplicar_texto_qs(qs, texto)
    return qs


def contas_pagar_montar_qs(
    *,
    status: str = "abertos",
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    competencia_de: date | None = None,
    competencia_ate: date | None = None,
    pagamento_de: date | None = None,
    pagamento_ate: date | None = None,
    texto: str | None = None,
    excluir_planos_nomes: list[str] | None = None,
    mongo_id: str | None = None,
) -> QuerySet:
    return titulos_financeiro_montar_qs(
        despesa=True,
        status=status,
        vencimento_de=vencimento_de,
        vencimento_ate=vencimento_ate,
        competencia_de=competencia_de,
        competencia_ate=competencia_ate,
        pagamento_de=pagamento_de,
        pagamento_ate=pagamento_ate,
        texto=texto,
        excluir_planos_nomes=excluir_planos_nomes,
        mongo_id=mongo_id,
    )


def contas_receber_montar_qs(**kwargs) -> QuerySet:
    return titulos_financeiro_montar_qs(despesa=False, **kwargs)


def _sort_key_titulo(t: TituloFinanceiroAgro, ordenacao: str) -> tuple:
    ord_ = (ordenacao or "vencimento_asc").strip().lower()
    venc = t.data_vencimento or date.min
    fluxo = t.data_fluxo or date.min
    if ord_ == "vencimento_desc":
        return (-venc.toordinal(), t.pk or 0)
    if ord_ == "fluxo_desc":
        return (-fluxo.toordinal(), t.pk or 0)
    if ord_ == "cliente_asc":
        return ((t.cliente or "").lower(), venc.toordinal())
    if ord_ == "cliente_desc":
        return (-1, (t.cliente or "").lower(), -venc.toordinal())
    if ord_ == "forma_asc":
        return ((t.forma_pagamento or "").lower(), venc.toordinal())
    if ord_ == "forma_desc":
        return (-1, (t.forma_pagamento or "").lower(), -venc.toordinal())
    if ord_ == "plano_asc":
        return ((t.plano_conta or "").lower(), venc.toordinal())
    if ord_ == "plano_desc":
        return (-1, (t.plano_conta or "").lower(), -venc.toordinal())
    if ord_ == "bruto_asc":
        return (float(t.valor_bruto), venc.toordinal())
    if ord_ == "bruto_desc":
        return (-float(t.valor_bruto), -venc.toordinal())
    if ord_ == "saldo_asc":
        return (float(t.valor_restante), venc.toordinal())
    if ord_ == "saldo_desc":
        return (-float(t.valor_restante), -venc.toordinal())
    return (venc.toordinal(), t.pk or 0)


def _totais_de_titulos(titulos: list[TituloFinanceiroAgro]) -> dict[str, float]:
    n = len(titulos)
    bruto = Decimal("0")
    mov = Decimal("0")
    saldo = Decimal("0")
    for t in titulos:
        bruto += _dec2(t.valor_bruto)
        mov += _dec2(t.valor_pago)
        saldo += _dec2(t.valor_restante)
    return {
        "quantidade": n,
        "bruto": float(bruto),
        "movimentado": float(mov),
        "saldo_aberto": float(saldo),
    }


def titulo_financeiro_agro_para_api(t: TituloFinanceiroAgro) -> dict[str, Any]:
    """Formato compatível com ``lancamento_para_api`` (lista CP/CR)."""
    quitado = _titulo_quitado_negocio(t)
    mov_r = round(float(t.valor_pago), 2)
    rest = round(float(t.valor_restante), 2)
    bruto = round(float(t.valor_bruto), 2)
    despesa = bool(t.despesa)

    def _iso_d(d: date | None) -> str | None:
        if d is None:
            return None
        return datetime.combine(d, datetime.min.time()).replace(
            tzinfo=timezone.get_current_timezone()
        ).isoformat()

    pode_excluir = False
    if not _tem_id_erp_valido(t):
        if _titulo_manual_agro_lote(t):
            pode_excluir = True
        elif not quitado and mov_r <= 0.02:
            pode_excluir = True

    snap = t.dados_snapshot_json or {}
    return {
        "id": t.mongo_id,
        "despesa": despesa,
        "descricao": t.descricao or "",
        "cliente": t.cliente or "",
        "cliente_id": t.cliente_id or "",
        "numero_documento": t.numero_documento or "",
        "parcela": int(t.parcela or 0),
        "plano_conta": t.plano_conta or "",
        "plano_conta_id": t.plano_conta_id or "",
        "grupo": t.grupo or "",
        "forma_pagamento": t.forma_pagamento or "",
        "forma_pagamento_id": t.forma_pagamento_id or "",
        "banco": t.banco or "",
        "banco_id": t.banco_id or "",
        "centro_custo": t.centro_custo or "",
        "empresa": t.empresa or "",
        "observacoes": (t.observacoes or "")[:500],
        "valor_bruto": bruto,
        "valor_movimentado": mov_r,
        "restante": rest,
        "pago": quitado,
        "data_vencimento": _iso_d(t.data_vencimento),
        "data_competencia": _iso_d(t.data_competencia),
        "data_fluxo": _iso_d(t.data_fluxo),
        "data_pagamento": _iso_d(t.data_pagamento),
        "valor_previsto": bruto,
        "valor_pago": mov_r,
        "pode_editar": not quitado,
        "pode_editar_valor": (not quitado) and mov_r <= 0.02,
        "pode_excluir": pode_excluir,
        "agro_recorrente": bool(t.agro_recorrente),
        "recorrencia_intervalo_meses": max(1, min(int(t.recorrencia_intervalo_meses or 1), 36)),
        "agro_recorrente_sempre": bool(t.agro_recorrente_sempre),
        "boleto_codigo_barras": (t.boleto_codigo_barras or "")[:54],
        "usuario_lancou": t.usuario_lancou or "",
        "usuario_quitou": t.usuario_quitou or "",
        "modificado_por": t.modificado_por or "",
        "criado_por": t.criado_por or "",
        "last_update": snap.get("last_update"),
        "data_modificacao": snap.get("data_modificacao"),
        "fonte_postgres": True,
    }


def titulos_financeiro_buscar_pagina_pg(
    *,
    despesa: bool,
    status: str = "abertos",
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    competencia_de: date | None = None,
    competencia_ate: date | None = None,
    pagamento_de: date | None = None,
    pagamento_ate: date | None = None,
    texto: str | None = None,
    excluir_planos_nomes: list[str] | None = None,
    mongo_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
    ordenacao: str = "vencimento_asc",
    skip_totais: bool = False,
    limite_max: int = 200,
) -> tuple[list[dict], int, dict[str, float] | None]:
    page = max(1, page)
    cap = max(1, int(limite_max) if limite_max else 200)
    page_size = min(cap, max(1, page_size))
    skip = (page - 1) * page_size

    qs = titulos_financeiro_montar_qs(
        despesa=despesa,
        status=status,
        vencimento_de=vencimento_de,
        vencimento_ate=vencimento_ate,
        competencia_de=competencia_de,
        competencia_ate=competencia_ate,
        pagamento_de=pagamento_de,
        pagamento_ate=pagamento_ate,
        texto=texto,
        excluir_planos_nomes=excluir_planos_nomes,
        mongo_id=mongo_id,
    )
    cap_linhas = _cap_linhas_consulta_pg(
        mongo_id=mongo_id,
        vencimento_de=vencimento_de,
        vencimento_ate=vencimento_ate,
        competencia_de=competencia_de,
        competencia_ate=competencia_ate,
        pagamento_de=pagamento_de,
        pagamento_ate=pagamento_ate,
        texto=texto,
    )
    ord_pg = _ordenacao_pg_campo(ordenacao)
    if ord_pg:
        qs = qs.order_by(ord_pg, "-pk" if ord_pg.startswith("-") else "pk")
    rows = list(qs[: cap_linhas + 1])
    if len(rows) > cap_linhas:
        logger.warning(
            "titulos_financeiro_buscar_pagina_pg: truncado em %s linhas (despesa=%s cap=%s)",
            cap_linhas,
            despesa,
            cap_linhas,
        )
        rows = rows[:cap_linhas]

    if mongo_id and rows:
        deduped = rows
    else:
        deduped = dedup_titulos(rows)

    deduped.sort(key=lambda t: _sort_key_titulo(t, ordenacao))

    total = len(deduped)
    totais = None if skip_totais else _totais_de_titulos(deduped)
    page_rows = deduped[skip : skip + page_size]
    linhas = [titulo_financeiro_agro_para_api(t) for t in page_rows]
    return linhas, total, totais


def contas_pagar_buscar_pagina_pg(
    *,
    status: str = "abertos",
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    competencia_de: date | None = None,
    competencia_ate: date | None = None,
    pagamento_de: date | None = None,
    pagamento_ate: date | None = None,
    texto: str | None = None,
    excluir_planos_nomes: list[str] | None = None,
    mongo_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
    ordenacao: str = "vencimento_asc",
    skip_totais: bool = False,
    limite_max: int = 200,
) -> tuple[list[dict], int, dict[str, float] | None]:
    return titulos_financeiro_buscar_pagina_pg(
        despesa=True,
        status=status,
        vencimento_de=vencimento_de,
        vencimento_ate=vencimento_ate,
        competencia_de=competencia_de,
        competencia_ate=competencia_ate,
        pagamento_de=pagamento_de,
        pagamento_ate=pagamento_ate,
        texto=texto,
        excluir_planos_nomes=excluir_planos_nomes,
        mongo_id=mongo_id,
        page=page,
        page_size=page_size,
        ordenacao=ordenacao,
        skip_totais=skip_totais,
        limite_max=limite_max,
    )


def contas_receber_buscar_pagina_pg(**kwargs) -> tuple[list[dict], int, dict[str, float] | None]:
    return titulos_financeiro_buscar_pagina_pg(despesa=False, **kwargs)


def planos_distintos_pg(
    *,
    despesa: bool,
    status: str = "abertos",
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    competencia_de: date | None = None,
    competencia_ate: date | None = None,
    pagamento_de: date | None = None,
    pagamento_ate: date | None = None,
    texto: str | None = None,
    limit: int = 400,
) -> list[dict[str, str]]:
    qs = titulos_financeiro_montar_qs(
        despesa=despesa,
        status=status,
        vencimento_de=vencimento_de,
        vencimento_ate=vencimento_ate,
        competencia_de=competencia_de,
        competencia_ate=competencia_ate,
        pagamento_de=pagamento_de,
        pagamento_ate=pagamento_ate,
        texto=texto,
    )
    rows = dedup_titulos(list(qs[:_CAP_LINHAS]))
    nomes: set[str] = set()
    for t in rows:
        n = (t.plano_conta or "").strip()
        nomes.add(n if n else "(sem plano)")
    lim = min(max(int(limit or 400), 1), 500)
    return [{"nome": x} for x in sorted(nomes, key=lambda s: s.lower())][:lim]


def planos_distintos_cp_pg(
    *,
    status: str = "abertos",
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    competencia_de: date | None = None,
    competencia_ate: date | None = None,
    pagamento_de: date | None = None,
    pagamento_ate: date | None = None,
    texto: str | None = None,
    limit: int = 400,
) -> list[dict[str, str]]:
    return planos_distintos_pg(
        despesa=True,
        status=status,
        vencimento_de=vencimento_de,
        vencimento_ate=vencimento_ate,
        competencia_de=competencia_de,
        competencia_ate=competencia_ate,
        pagamento_de=pagamento_de,
        pagamento_ate=pagamento_ate,
        texto=texto,
        limit=limit,
    )


def planos_distintos_cr_pg(**kwargs) -> list[dict[str, str]]:
    return planos_distintos_pg(despesa=False, **kwargs)


_SUGESTOES_CAMPOS_PG: dict[str, tuple[str, str | None]] = {
    "empresa": ("empresa", None),
    "cliente": ("cliente", "cliente_id"),
    "plano": ("plano_conta", "plano_conta_id"),
    "forma": ("forma_pagamento", "forma_pagamento_id"),
    "banco": ("banco", "banco_id"),
    "grupo": ("grupo", None),
    "centro": ("centro_custo", None),
}


def _financeiro_id_str(v: object) -> str:
    return str(v or "").strip()


def listar_formas_e_bancos_distintos_pg(
    limit: int = 400,
    *,
    modo: str = "erp",
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Formas/contas a partir de ``TituloFinanceiroAgro`` (modo ERP ou histórico)."""
    from produtos.mongo_financeiro_util import (
        _bancos_lista_com_placeholder_inicio,
        normalizar_rotulo_banco_erp,
    )

    cap = min(max(int(limit or 400), 1), 800)
    modo_n = (modo or "erp").strip().lower()
    formas: list[dict[str, str]] = []
    bancos: list[dict[str, str]] = []
    if modo_n == "historico":
        seen_f: set[tuple[str, str]] = set()
        seen_b: set[tuple[str, str]] = set()
        for fn, fid, bn, bid in TituloFinanceiroAgro.objects.values_list(
            "forma_pagamento", "forma_pagamento_id", "banco", "banco_id"
        ).iterator(chunk_size=4000):
            fn_s = (fn or "").strip()
            fid_s = _financeiro_id_str(fid)
            if fn_s:
                key = (fn_s.lower(), fid_s)
                if key not in seen_f:
                    seen_f.add(key)
                    formas.append({"id": fid_s, "nome": fn_s})
            bn_s = (bn or "").strip()
            bid_s = _financeiro_id_str(bid)
            if bn_s:
                key_b = (bn_s.lower(), bid_s)
                if key_b not in seen_b:
                    seen_b.add(key_b)
                    bancos.append({"id": bid_s, "nome": normalizar_rotulo_banco_erp(bid_s, bn_s)})
        formas.sort(key=lambda x: (x.get("nome") or "").lower())
        bancos.sort(key=lambda x: (x.get("nome") or "").lower())
        return formas[:cap], _bancos_lista_com_placeholder_inicio(bancos[:cap])

    by_fid: dict[str, str] = {}
    by_bid: dict[str, str] = {}
    for fn, fid, bn, bid in TituloFinanceiroAgro.objects.values_list(
        "forma_pagamento", "forma_pagamento_id", "banco", "banco_id"
    ).iterator(chunk_size=4000):
        fn_s = (fn or "").strip()
        fid_s = _financeiro_id_str(fid)
        if fn_s and fid_s and not re.match(r"^criar\s+novo", fn_s, re.IGNORECASE):
            by_fid.setdefault(fid_s, fn_s)
        bn_s = (bn or "").strip()
        bid_s = _financeiro_id_str(bid)
        if (
            bn_s
            and bid_s
            and bn_s.upper() not in ("ADICIONAR BANCO", "ADICIONAR CONTA")
        ):
            by_bid.setdefault(bid_s, normalizar_rotulo_banco_erp(bid_s, bn_s))
    formas = [{"id": k, "nome": v} for k, v in by_fid.items()]
    formas.sort(key=lambda x: (x.get("nome") or "").lower())
    bancos = [{"id": k, "nome": v} for k, v in by_bid.items()]
    bancos.sort(key=lambda x: (x.get("nome") or "").lower())
    return formas[:cap], _bancos_lista_com_placeholder_inicio(bancos[:cap])


def lancamentos_sugestoes_campo_pg(
    campo: str,
    q: str | None = None,
    limit: int = 30,
    *,
    escopo: str = "todos",
    ordenar: str = "nome",
    empresa_id: str | None = None,
) -> list[dict[str, str]]:
    """Autocomplete financeiro a partir dos títulos Postgres."""
    from django.db.models import Count, Max

    from produtos.mongo_financeiro_util import _banco_placeholder_para_select

    campo = (campo or "").strip().lower()
    if campo not in _SUGESTOES_CAMPOS_PG:
        return []
    nome_f, id_f = _SUGESTOES_CAMPOS_PG[campo]
    cap = 500 if campo == "plano" else 80
    lim = min(max(int(limit or 30), 1), cap)
    qq = (q or "").strip()

    qs = TituloFinanceiroAgro.objects.all()
    esc = (escopo or "todos").strip().lower()
    eid = (empresa_id or "").strip()
    if eid and campo == "cliente":
        qs = qs.filter(
            Q(dados_snapshot_json__empresa_id=eid)
            | Q(dados_snapshot_json__EmpresaID=eid)
        )
    if campo == "cliente":
        if esc == "pagar":
            qs = qs.filter(despesa=True)
        elif esc == "receber":
            qs = qs.filter(despesa=False)
        elif esc == "emprestimo":
            qs = qs.filter(
                Q(plano_conta__icontains="empréstimo") | Q(plano_conta__icontains="emprestimo")
            )
    if qq:
        qs = qs.filter(**{f"{nome_f}__icontains": qq[:100]})
    else:
        qs = qs.exclude(**{nome_f: ""})

    if campo == "banco":
        qs = qs.exclude(banco__in=["", "ADICIONAR BANCO", "Adicionar banco"])

    if campo == "cliente" and ordenar in ("recente", "frequencia"):
        qs = (
            qs.values(nome_f, id_f)
            .annotate(cnt=Count("id"), ult=Max("data_vencimento"))
            .order_by("-cnt" if ordenar == "frequencia" else "-ult")
        )
        out: list[dict[str, str]] = []
        seen: set[str] = set()
        for row in qs[: lim * 3]:
            nome = str(row.get(nome_f) or "").strip()
            if not nome or nome.lower() in seen:
                continue
            seen.add(nome.lower())
            rid = row.get(id_f) if id_f else ""
            out.append({"nome": nome, "id": _financeiro_id_str(rid)})
            if len(out) >= lim:
                break
        return out

    seen_n: set[str] = set()
    out = []
    ord_n = (ordenar or "nome").strip().lower()
    values = qs.values_list(nome_f, id_f if id_f else nome_f).distinct()
    rows = list(values[:200])
    if ord_n == "nome_desc":
        rows.sort(key=lambda r: str(r[0] or "").lower(), reverse=True)
    else:
        rows.sort(key=lambda r: str(r[0] or "").lower())
    for nome, rid in rows:
        nome_s = str(nome or "").strip()
        if not nome_s or nome_s.lower() in seen_n:
            continue
        seen_n.add(nome_s.lower())
        out.append({"nome": nome_s, "id": _financeiro_id_str(rid) if id_f else ""})
        if len(out) >= lim:
            break
    if campo == "banco":
        ph = _banco_placeholder_para_select()
        pid = str(ph.get("id") or "")
        if pid and not any(str(x.get("id") or "") == pid for x in out):
            out.insert(0, ph)
    return out[:lim]


def dashboard_gerencial_linhas_financeiras_pg(
    *,
    hoje: date,
    limite: int = 12,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Linhas CP/CR para cards da home BI — espelha ``dashboard_gerencial_linhas_financeiras``."""
    from produtos.mongo_financeiro_util import _dashboard_mapear_linha_financeiro

    lim = min(max(int(limite or 12), 1), 50)
    fim_janela = hoje + timedelta(days=7)
    try:
        rec_rows, _, _ = contas_receber_buscar_pagina_pg(
            status="abertos",
            vencimento_de=hoje,
            vencimento_ate=fim_janela,
            page=1,
            page_size=lim,
            ordenacao="vencimento_asc",
            skip_totais=True,
        )
        pag_rows, _, _ = contas_pagar_buscar_pagina_pg(
            status="abertos",
            page=1,
            page_size=lim,
            ordenacao="vencimento_asc",
            skip_totais=True,
        )
    except Exception:
        logger.exception("dashboard_gerencial_linhas_financeiras_pg")
        return [], []
    out_rec = [_dashboard_mapear_linha_financeiro(r, hoje, despesa=False) for r in rec_rows]
    out_pag = [_dashboard_mapear_linha_financeiro(r, hoje, despesa=True) for r in pag_rows]
    return out_rec, out_pag


def dashboard_gerencial_totais_financeiros_pg(hoje: date, ontem: date) -> dict[str, float]:
    """Totais saldo aberto (hoje + atraso) para KPIs financeiros da home BI."""
    out = {
        "total_receber_hoje": 0.0,
        "total_pagar_hoje": 0.0,
        "total_receber_atraso": 0.0,
        "total_pagar_atraso": 0.0,
    }
    try:
        _, _, tot_rec_hoje = contas_receber_buscar_pagina_pg(
            status="abertos",
            vencimento_de=hoje,
            vencimento_ate=hoje,
            page=1,
            page_size=1,
            skip_totais=False,
            limite_max=200,
        )
        _, _, tot_pag_hoje = contas_pagar_buscar_pagina_pg(
            status="abertos",
            vencimento_de=hoje,
            vencimento_ate=hoje,
            page=1,
            page_size=1,
            skip_totais=False,
            limite_max=200,
        )
        out["total_receber_hoje"] = round(float((tot_rec_hoje or {}).get("saldo_aberto") or 0), 2)
        out["total_pagar_hoje"] = round(float((tot_pag_hoje or {}).get("saldo_aberto") or 0), 2)
        _, _, tot_rec_atraso = contas_receber_buscar_pagina_pg(
            status="abertos",
            vencimento_ate=ontem,
            page=1,
            page_size=1,
            skip_totais=False,
            limite_max=200,
        )
        _, _, tot_pag_atraso = contas_pagar_buscar_pagina_pg(
            status="abertos",
            vencimento_ate=ontem,
            page=1,
            page_size=1,
            skip_totais=False,
            limite_max=200,
        )
        out["total_receber_atraso"] = round(float((tot_rec_atraso or {}).get("saldo_aberto") or 0), 2)
        out["total_pagar_atraso"] = round(float((tot_pag_atraso or {}).get("saldo_aberto") or 0), 2)
    except Exception:
        logger.exception("dashboard_gerencial_totais_financeiros_pg")
    return out


def financeiro_pg_conferencia_abertos() -> dict[str, Any]:
    """Totais CP em aberto Mongo (dedup) vs Postgres (dedup) — diagnóstico pré-flag."""
    from produtos.mongo_financeiro_util import (
        contas_pagar_totais_filtrados,
        lancamentos_montar_query_mongo,
    )
    from produtos.views import obter_conexao_mongo

    out: dict[str, Any] = {
        "ok": True,
        "pg_registros_brutos": TituloFinanceiroAgro.objects.filter(despesa=True).count(),
    }
    _, total_pg, tot_pg = contas_pagar_buscar_pagina_pg(
        status="abertos",
        page=1,
        page_size=1,
        skip_totais=False,
        limite_max=200,
    )
    out["pg_abertos_dedup"] = {"quantidade": total_pg, **(tot_pg or {})}

    _, db = obter_conexao_mongo()
    if db is None:
        out["mongo_ok"] = False
        out["mongo_erro"] = "Mongo indisponível"
        return out
    q = lancamentos_montar_query_mongo(despesa=True, status="abertos")
    tot_m = contas_pagar_totais_filtrados(db, q)
    out["mongo_ok"] = True
    out["mongo_abertos_dedup"] = tot_m
    return out
