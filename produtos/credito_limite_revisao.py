"""
Revisão manual de limite a partir do laboratório de crédito.

O cálculo do score continua só leitura. Aqui o limite real só muda
quando alguém aprova, e sempre via definir_limite_fiado_cliente.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from django.db import transaction

from produtos.credito_score_shadow import (
    _LIMITE_BLOQUEIO,
    _Q2,
    _limite_efetivo_local,
    rotulo_candidato_revisao,
)
from produtos.fiado_gestao_util import definir_limite_fiado_cliente
from produtos.models import (
    ClienteAgro,
    ClienteAnaliseCreditoAgro,
    CreditoLimiteRevisaoDecisaoAgro,
)

_PCT_AUMENTO_MIN = 20
_PCT_AUMENTO_MAX = 100
_PCT_AUMENTO_PADRAO = 20
_CONFIANCA_OK = {
    ClienteAnaliseCreditoAgro.Confianca.MEDIA,
    ClienteAnaliseCreditoAgro.Confianca.ALTA,
}


def normalizar_pct_aumento_max(val) -> int:
    """Teto de aumento por aprovação (20–100 %). Padrão 20."""
    try:
        if val is None or val == "":
            n = _PCT_AUMENTO_PADRAO
        else:
            n = int(Decimal(str(val).replace(",", ".")).quantize(Decimal("1")))
    except Exception:
        n = _PCT_AUMENTO_PADRAO
    return max(_PCT_AUMENTO_MIN, min(_PCT_AUMENTO_MAX, n))


def _cap_multiplier(pct_max: int) -> Decimal:
    pct = normalizar_pct_aumento_max(pct_max)
    return (Decimal("1") + Decimal(pct) / Decimal("100")).quantize(Decimal("0.0001"))


def _q2(val) -> Decimal:
    try:
        if val is None or val == "":
            return Decimal("0.00")
        return Decimal(str(val).replace(",", ".")).quantize(_Q2, rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0.00")


def novo_limite_aplicavel(limite_atual, limite_sugerido, *, pct_max: int = _PCT_AUMENTO_PADRAO) -> Decimal:
    """Teto de +pct_max % sobre o atual. Redução segue o sugerido."""
    atual = _q2(limite_atual)
    sugerido = _q2(limite_sugerido)
    if sugerido <= atual:
        return sugerido
    mult = _cap_multiplier(pct_max)
    teto = (atual * mult).quantize(_Q2, rounding=ROUND_HALF_UP)
    return min(sugerido, teto)


def _ultimo_snapshot_por_cliente() -> dict[int, ClienteAnaliseCreditoAgro]:
    vistos: dict[int, ClienteAnaliseCreditoAgro] = {}
    qs = ClienteAnaliseCreditoAgro.objects.select_related("cliente").order_by(
        "-calculado_em", "-pk"
    )
    for snap in qs.iterator(chunk_size=300):
        if snap.cliente_id in vistos:
            continue
        vistos[int(snap.cliente_id)] = snap
    return vistos


def _apto(
    snap: ClienteAnaliseCreditoAgro,
    limite_atual: Decimal,
    *,
    pct_max: int = _PCT_AUMENTO_PADRAO,
) -> bool:
    if snap.score is None:
        return False
    if snap.tem_vencido_snapshot:
        return False
    if snap.confianca not in _CONFIANCA_OK:
        return False
    cad = _q2(snap.limite_cadastrado_snapshot)
    efet = _q2(snap.limite_efetivo_snapshot)
    if limite_atual == _LIMITE_BLOQUEIO or cad == _LIMITE_BLOQUEIO or efet == _LIMITE_BLOQUEIO:
        return False
    alertas = snap.alertas_json if isinstance(snap.alertas_json, list) else []
    ind = snap.indicadores_json if isinstance(snap.indicadores_json, dict) else {}
    rotulo = rotulo_candidato_revisao(
        candidato=bool(ind.get("candidato_revisao_limite")),
        alertas=alertas,
        limite_cadastrado=cad,
        limite_efetivo=efet,
    )
    if rotulo in ("Revisar dados", "Revisar bloqueio"):
        return False
    sugerido = _q2(snap.limite_sugerido)
    if sugerido == limite_atual:
        return False
    novo = novo_limite_aplicavel(limite_atual, sugerido, pct_max=pct_max)
    return novo != limite_atual


def _situacao_fiado_cliente(cliente_pk: int) -> tuple[Decimal, str, bool]:
    from produtos.fiado_credito_util import resumo_credito_fiado_cliente

    try:
        cred = resumo_credito_fiado_cliente(cliente_agro_pk=int(cliente_pk))
    except Exception:
        return Decimal("0.00"), "—", False
    usado = _q2(cred.get("usado"))
    tem_vencido = bool(cred.get("tem_vencido"))
    if tem_vencido:
        label = "Com vencidas"
    elif usado > Decimal("0.009"):
        label = "Em dia"
    else:
        label = "Sem saldo aberto"
    return usado, label, tem_vencido


def listar_revisoes_limite(pct_max: int = _PCT_AUMENTO_PADRAO) -> list[dict]:
    pct_max = normalizar_pct_aumento_max(pct_max)
    snaps = _ultimo_snapshot_por_cliente()
    if not snaps:
        return []
    decididos = set(
        CreditoLimiteRevisaoDecisaoAgro.objects.filter(
            analise_id__in=[s.pk for s in snaps.values()]
        ).values_list("analise_id", flat=True)
    )
    linhas: list[dict] = []
    for cliente_id, snap in snaps.items():
        if snap.pk in decididos:
            continue
        atual = _limite_efetivo_local(_q2(getattr(snap.cliente, "limite_fiado_local", 0)))
        if not _apto(snap, atual, pct_max=pct_max):
            continue
        sugerido = _q2(snap.limite_sugerido)
        novo = novo_limite_aplicavel(atual, sugerido, pct_max=pct_max)
        saldo_fiado, situacao_label, tem_vencido_atual = _situacao_fiado_cliente(cliente_id)
        linhas.append(
            {
                "cliente_pk": cliente_id,
                "analise_id": snap.pk,
                "cliente_nome": (snap.cliente.nome if snap.cliente_id else "") or "—",
                "saldo_fiado": saldo_fiado,
                "situacao_label": situacao_label,
                "tem_vencido_atual": tem_vencido_atual,
                "limite_atual": atual,
                "limite_sugerido": sugerido,
                "novo_limite": novo,
                "score": snap.score,
                "confianca": snap.confianca,
                "confianca_label": snap.get_confianca_display(),
                "diferenca": (novo - atual).quantize(_Q2),
            }
        )
    linhas.sort(key=lambda r: (-abs(r["diferenca"]), r["cliente_nome"]))
    return linhas


def _snap_apto_vivo(
    cliente_pk: int,
    *,
    pct_max: int = _PCT_AUMENTO_PADRAO,
) -> tuple[ClienteAnaliseCreditoAgro, Decimal, Decimal] | None:
    pct_max = normalizar_pct_aumento_max(pct_max)
    snap = (
        ClienteAnaliseCreditoAgro.objects.select_related("cliente")
        .filter(cliente_id=cliente_pk)
        .order_by("-calculado_em", "-pk")
        .first()
    )
    if snap is None or snap.cliente_id is None:
        return None
    if CreditoLimiteRevisaoDecisaoAgro.objects.filter(analise_id=snap.pk).exists():
        return None
    atual = _limite_efetivo_local(_q2(snap.cliente.limite_fiado_local))
    if not _apto(snap, atual, pct_max=pct_max):
        return None
    novo = novo_limite_aplicavel(atual, snap.limite_sugerido, pct_max=pct_max)
    return snap, atual, novo


@transaction.atomic
def aprovar_limite_cliente(
    cliente_pk: int,
    *,
    usuario: str = "",
    pct_max: int = _PCT_AUMENTO_PADRAO,
) -> bool:
    achado = _snap_apto_vivo(int(cliente_pk), pct_max=pct_max)
    if achado is None:
        return False
    snap, atual, novo = achado
    definir_limite_fiado_cliente(int(cliente_pk), novo, usuario=(usuario or "")[:150])
    CreditoLimiteRevisaoDecisaoAgro.objects.create(
        cliente_id=int(cliente_pk),
        analise=snap,
        acao=CreditoLimiteRevisaoDecisaoAgro.Acao.APROVADO,
        limite_anterior=atual,
        limite_aplicado=novo,
        usuario=(usuario or "")[:150],
    )
    return True


@transaction.atomic
def ignorar_limite_cliente(
    cliente_pk: int,
    *,
    usuario: str = "",
    pct_max: int = _PCT_AUMENTO_PADRAO,
) -> bool:
    achado = _snap_apto_vivo(int(cliente_pk), pct_max=pct_max)
    if achado is None:
        return False
    snap, atual, _novo = achado
    antes = ClienteAgro.objects.get(pk=int(cliente_pk)).limite_fiado_local
    CreditoLimiteRevisaoDecisaoAgro.objects.create(
        cliente_id=int(cliente_pk),
        analise=snap,
        acao=CreditoLimiteRevisaoDecisaoAgro.Acao.IGNORADO,
        limite_anterior=atual,
        limite_aplicado=None,
        usuario=(usuario or "")[:150],
    )
    depois = ClienteAgro.objects.get(pk=int(cliente_pk)).limite_fiado_local
    if _q2(depois) != _q2(antes):
        raise RuntimeError("Ignorar não pode alterar o limite.")
    return True


def aprovar_limites_selecionados(
    cliente_pks: list[int],
    *,
    usuario: str = "",
    pct_max: int = _PCT_AUMENTO_PADRAO,
) -> tuple[int, int]:
    ok = 0
    pulados = 0
    for pk in cliente_pks:
        if aprovar_limite_cliente(int(pk), usuario=usuario, pct_max=pct_max):
            ok += 1
        else:
            pulados += 1
    return ok, pulados
