"""
Laboratório de análise de crédito (shadow_v1).

SOMENTE LEITURA financeira. A única gravação permitida é
ClienteAnaliseCreditoAgro.objects.create(...) quando persist=True.

Não chama definir_limite_fiado_cliente, baixar_titulo, criar_titulos_de_venda,
resumo_credito_fiado_cliente nem qualquer rotina operacional de fiado/PDV.
"""

from __future__ import annotations

from calendar import monthrange
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from django.db.models import Prefetch, Q
from django.utils import timezone

from produtos.fiado_credito_util import (
    cliente_agro_pk_de_ref,
    fiado_limite_padrao,
    resolver_cliente_fiado,
    valor_fiado_venda_local,
)
from produtos.models import (
    ClienteAgro,
    ClienteAnaliseCreditoAgro,
    FiadoBaixaAgro,
    FiadoTituloAgro,
    VendaAgro,
)

REGRA_VERSAO = "shadow_v1"
_Q2 = Decimal("0.01")
_ZERO = Decimal("0.00")

# Alerta de título quitado sem baixas suficientes (inconsistência de pagamento).
_MARCA_ALERTA_BAIXAS_INSUF = "sem baixas suficientes"


def alerta_inconsistencia_pagamento(alertas: list | None) -> bool:
    """True se há alerta de quitado sem baixas suficientes (ou inconsistência explícita)."""
    for a in alertas or []:
        s = str(a).lower()
        if _MARCA_ALERTA_BAIXAS_INSUF in s or "inconsist" in s:
            return True
    return False


def rotulo_candidato_revisao(*, candidato: bool, alertas: list | None) -> str:
    """
    Regra provisória de apresentação:
    inconsistência de baixas → «Revisar dados» (nunca «Sim»).
    """
    if alerta_inconsistencia_pagamento(alertas):
        return "Revisar dados"
    return "Sim" if candidato else "Não"



def _dec(val) -> Decimal:
    try:
        if val is None:
            return _ZERO
        return Decimal(str(val).replace(",", ".")).quantize(_Q2, rounding=ROUND_HALF_UP)
    except Exception:
        return _ZERO


def _clamp_int(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(n)))


def _data_local(dt) -> date:
    if isinstance(dt, date) and not isinstance(dt, datetime):
        return dt
    if dt is None:
        return timezone.localdate()
    if timezone.is_aware(dt):
        return timezone.localtime(dt).date()
    return dt.date() if hasattr(dt, "date") else timezone.localdate()


def _meses_calendario(hoje: date, n: int) -> list[tuple[int, int]]:
    y, m = hoje.year, hoje.month
    out: list[tuple[int, int]] = []
    for _ in range(n):
        out.append((y, m))
        m -= 1
        if m < 1:
            m = 12
            y -= 1
    return out


def _inicio_mes_mais_antigo(hoje: date, n_meses: int) -> date:
    ano, mes = _meses_calendario(hoje, n_meses)[-1]
    return date(ano, mes, 1)


def _subtrair_meses(d: date, meses: int) -> date:
    y, m = d.year, d.month - meses
    while m < 1:
        m += 12
        y -= 1
    dia = min(d.day, monthrange(y, m)[1])
    return date(y, m, dia)


def _fator_pontualidade(dias_atraso: int) -> Decimal:
    if dias_atraso <= 0:
        return Decimal("1.00")
    if dias_atraso <= 3:
        return Decimal("0.90")
    if dias_atraso <= 7:
        return Decimal("0.75")
    if dias_atraso <= 15:
        return Decimal("0.50")
    if dias_atraso <= 30:
        return Decimal("0.20")
    return Decimal("0.00")


def _pontos_situacao_atraso(maior_atraso: int) -> int:
    if maior_atraso <= 0:
        return 25
    if maior_atraso <= 3:
        return 20
    if maior_atraso <= 7:
        return 15
    if maior_atraso <= 15:
        return 8
    if maior_atraso <= 30:
        return 3
    return 0


def _pontos_frequencia(meses_com_compra: int) -> int:
    m = max(0, int(meses_com_compra))
    if m <= 0:
        return 0
    if m == 1:
        return 2
    if m == 2:
        return 4
    if m == 3:
        return 6
    if m == 4:
        return 8
    return 10


def _pontos_relacionamento(dias: int) -> int:
    if dias < 30:
        return 2
    if dias < 90:
        return 4
    if dias < 180:
        return 6
    if dias < 365:
        return 8
    return 10


def _classificacao_de_score(score: int | None) -> str:
    if score is None:
        return ClienteAnaliseCreditoAgro.Classificacao.SEM_HISTORICO
    if score <= 49:
        return ClienteAnaliseCreditoAgro.Classificacao.ALTO_RISCO
    if score <= 69:
        return ClienteAnaliseCreditoAgro.Classificacao.REGULAR
    if score <= 84:
        return ClienteAnaliseCreditoAgro.Classificacao.BOM
    if score <= 94:
        return ClienteAnaliseCreditoAgro.Classificacao.MUITO_BOM
    return ClienteAnaliseCreditoAgro.Classificacao.EXCELENTE


def _multiplicador_limite(score: int | None) -> Decimal:
    if score is None:
        return Decimal("0.50")
    if score <= 49:
        return Decimal("0.50")
    if score <= 69:
        return Decimal("0.75")
    if score <= 84:
        return Decimal("1.00")
    if score <= 94:
        return Decimal("1.20")
    return Decimal("1.40")


def _limite_efetivo_local(limite_cadastrado: Decimal) -> Decimal:
    """Espelha a regra operacional sem gravar: >0 usa o valor; 0 → padrão loja."""
    if limite_cadastrado > _ZERO:
        return limite_cadastrado
    return fiado_limite_padrao()


def _norm_nome(nome: str) -> str:
    return " ".join(str(nome or "").strip().lower().split())


@dataclass
class ResultadoAnaliseCredito:
    cliente_pk: int
    cliente_nome: str
    score: int | None
    classificacao: str
    confianca: str
    limite_cadastrado: Decimal
    limite_efetivo: Decimal
    saldo_aberto: Decimal
    saldo_vencido: Decimal
    media_fiado_3m: Decimal
    limite_sugerido: Decimal
    tem_vencido: bool
    maior_atraso_dias: int
    indicadores: dict[str, Any] = field(default_factory=dict)
    alertas: list[str] = field(default_factory=list)
    candidato_revisao: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "cliente_pk": self.cliente_pk,
            "cliente_nome": self.cliente_nome,
            "score": self.score,
            "classificacao": self.classificacao,
            "classificacao_label": dict(ClienteAnaliseCreditoAgro.Classificacao.choices).get(
                self.classificacao, self.classificacao
            ),
            "confianca": self.confianca,
            "confianca_label": dict(ClienteAnaliseCreditoAgro.Confianca.choices).get(
                self.confianca, self.confianca
            ),
            "limite_cadastrado": float(self.limite_cadastrado),
            "limite_efetivo": float(self.limite_efetivo),
            "saldo_aberto": float(self.saldo_aberto),
            "saldo_vencido": float(self.saldo_vencido),
            "media_fiado_3m": float(self.media_fiado_3m),
            "limite_sugerido": float(self.limite_sugerido),
            "piso_operacional_sem_travar": float(
                max(self.limite_sugerido, self.saldo_aberto).quantize(_Q2)
            ),
            "tem_vencido": self.tem_vencido,
            "maior_atraso_dias": self.maior_atraso_dias,
            "indicadores": self.indicadores,
            "alertas": list(self.alertas),
            "candidato_revisao": self.candidato_revisao,
            "regra_versao": REGRA_VERSAO,
        }


def _media_fiado_3m_mapa(
    cliente_pks: set[int],
    *,
    hoje: date,
) -> dict[int, Decimal]:
    """Média fiado/mês (3 meses calendário) — leitura espelhada da planilha de clientes."""
    if not cliente_pks:
        return {}
    desde = _inicio_mes_mais_antigo(hoje, 3)
    meses_ref = _meses_calendario(hoje, 3)
    por_cliente: dict[int, list[tuple[date, Decimal]]] = defaultdict(list)

    refs = set()
    for pk in cliente_pks:
        refs.add(f"agro:{pk}")
        refs.add(f"local:{pk}")

    vendas = (
        VendaAgro.objects.filter(devolvida_em__isnull=True, criado_em__date__gte=desde)
        .filter(
            Q(cliente_id_erp__in=list(refs))
            | Q(cliente_id_erp__in=[str(pk) for pk in cliente_pks])
        )
        .only("pk", "cliente_id_erp", "pagamentos_json", "forma_pagamento", "total", "criado_em")
    )
    for venda in vendas.iterator(chunk_size=400):
        valor = valor_fiado_venda_local(venda)
        if valor <= Decimal("0.009"):
            continue
        _erp, agro_pk, _cli = resolver_cliente_fiado(venda.cliente_id_erp)
        if not agro_pk:
            agro_pk = cliente_agro_pk_de_ref(venda.cliente_id_erp)
        if not agro_pk or agro_pk not in cliente_pks or not venda.criado_em:
            continue
        por_cliente[int(agro_pk)].append((_data_local(venda.criado_em), valor.quantize(_Q2)))

    titulos_sem_venda = (
        FiadoTituloAgro.objects.filter(
            cliente_agro_id__in=list(cliente_pks),
            venda_agro_id__isnull=True,
            criado_em__date__gte=desde,
        )
        .exclude(situacao=FiadoTituloAgro.Situacao.CANCELADO)
        .only("pk", "cliente_agro_id", "numero_documento", "valor_bruto", "chave_unica", "criado_em")
    )
    agrupado: dict[tuple[int, str], tuple[date, Decimal]] = {}
    for t in titulos_sem_venda.iterator(chunk_size=400):
        if not t.cliente_agro_id:
            continue
        doc = (t.numero_documento or t.chave_unica or f"t{t.pk}").strip()
        key = (int(t.cliente_agro_id), doc)
        dt = _data_local(t.criado_em) if t.criado_em else hoje
        val = _dec(t.valor_bruto)
        if key in agrupado:
            prev_dt, prev_val = agrupado[key]
            agrupado[key] = (min(prev_dt, dt), (prev_val + val).quantize(_Q2))
        else:
            agrupado[key] = (dt, val)
    for (pk, _doc), (dt, val) in agrupado.items():
        if val > Decimal("0.009") and pk in cliente_pks:
            por_cliente[pk].append((dt, val))

    out: dict[int, Decimal] = {pk: _ZERO for pk in cliente_pks}
    for pk, compras in por_cliente.items():
        soma_mes: dict[tuple[int, int], Decimal] = defaultdict(lambda: _ZERO)
        for d, v in compras:
            soma_mes[(d.year, d.month)] += v
        total_3m = sum((soma_mes.get(chave, _ZERO) for chave in meses_ref), _ZERO).quantize(_Q2)
        out[pk] = (total_3m / Decimal("3")).quantize(_Q2)
    return out


def _meses_com_fiado_6m(
    cliente_pks: set[int],
    *,
    hoje: date,
) -> dict[int, set[tuple[int, int]]]:
    if not cliente_pks:
        return {}
    desde = _inicio_mes_mais_antigo(hoje, 6)
    meses_ok = set(_meses_calendario(hoje, 6))
    out: dict[int, set[tuple[int, int]]] = {pk: set() for pk in cliente_pks}
    refs = set()
    for pk in cliente_pks:
        refs.add(f"agro:{pk}")
        refs.add(f"local:{pk}")

    vendas = (
        VendaAgro.objects.filter(devolvida_em__isnull=True, criado_em__date__gte=desde)
        .filter(Q(cliente_id_erp__in=list(refs)))
        .only("pk", "cliente_id_erp", "pagamentos_json", "forma_pagamento", "total", "criado_em")
    )
    for venda in vendas.iterator(chunk_size=400):
        if valor_fiado_venda_local(venda) <= Decimal("0.009"):
            continue
        _erp, agro_pk, _cli = resolver_cliente_fiado(venda.cliente_id_erp)
        if not agro_pk or agro_pk not in cliente_pks or not venda.criado_em:
            continue
        d = _data_local(venda.criado_em)
        chave = (d.year, d.month)
        if chave in meses_ok:
            out[int(agro_pk)].add(chave)

    titulos = (
        FiadoTituloAgro.objects.filter(
            cliente_agro_id__in=list(cliente_pks),
            criado_em__date__gte=desde,
        )
        .exclude(situacao=FiadoTituloAgro.Situacao.CANCELADO)
        .only("cliente_agro_id", "criado_em", "venda_agro_id")
    )
    for t in titulos.iterator(chunk_size=400):
        if not t.cliente_agro_id or not t.criado_em:
            continue
        d = _data_local(t.criado_em)
        chave = (d.year, d.month)
        if chave in meses_ok:
            out[int(t.cliente_agro_id)].add(chave)
    return out


def analisar_cliente(
    cliente: ClienteAgro,
    *,
    hoje: date | None = None,
    media_3m: Decimal | None = None,
    meses_fiado_6m: set[tuple[int, int]] | None = None,
    nomes_duplicados: set[str] | None = None,
) -> ResultadoAnaliseCredito:
    """Calcula score/indicadores de um cliente. Não grava nada."""
    ref = hoje or timezone.localdate()
    alertas: list[str] = []
    limite_cad = _dec(cliente.limite_fiado_local)
    limite_efet = _limite_efetivo_local(limite_cad)

    if limite_cad == Decimal("0.01"):
        alertas.append("Limite cadastrado 0,01 (bloqueio prático de fiado no PDV).")
    elif limite_cad == _ZERO:
        alertas.append("Limite cadastrado 0 — legado: PDV usa padrão da loja (não converter).")

    nome_n = _norm_nome(cliente.nome)
    if nomes_duplicados and nome_n and nome_n in nomes_duplicados:
        alertas.append(
            "Possível cadastro duplicado (outro ClienteAgro com o mesmo nome). "
            "Análise usa só FK deste pk — históricos não foram misturados."
        )

    titulos = list(
        FiadoTituloAgro.objects.filter(cliente_agro_id=cliente.pk)
        .prefetch_related(
            Prefetch(
                "baixas",
                queryset=FiadoBaixaAgro.objects.order_by("criado_em", "pk"),
            )
        )
        .select_related("venda_agro")
        .order_by("vencimento", "pk")
    )

    janela_pag_ini = _subtrair_meses(ref, 12)
    titulos_janela = [
        t
        for t in titulos
        if t.situacao != FiadoTituloAgro.Situacao.CANCELADO
        and (
            (t.criado_em and _data_local(t.criado_em) >= janela_pag_ini)
            or (t.vencimento and t.vencimento >= janela_pag_ini)
            or t.situacao
            in (
                FiadoTituloAgro.Situacao.ABERTO,
                FiadoTituloAgro.Situacao.PARCIAL,
            )
        )
    ]

    saldo_aberto = _ZERO
    saldo_vencido = _ZERO
    maior_atraso_atual = 0
    qtd_vencidos = 0
    titulo_vencido_mais_antigo = ""
    venc_mais_antigo_dt: date | None = None

    for t in titulos:
        if t.situacao == FiadoTituloAgro.Situacao.CANCELADO:
            continue
        if t.situacao == FiadoTituloAgro.Situacao.QUITADO:
            continue
        saldo = max(_ZERO, (_dec(t.valor_bruto) - _dec(t.valor_pago)).quantize(_Q2))
        if saldo <= Decimal("0.009"):
            continue
        saldo_aberto += saldo
        if t.vencimento and t.vencimento < ref:
            atraso = (ref - t.vencimento).days
            qtd_vencidos += 1
            saldo_vencido += saldo
            if atraso > maior_atraso_atual:
                maior_atraso_atual = atraso
            if venc_mais_antigo_dt is None or t.vencimento < venc_mais_antigo_dt:
                venc_mais_antigo_dt = t.vencimento
                titulo_vencido_mais_antigo = (
                    f"{t.numero_documento or t.pk} · {t.vencimento.strftime('%d/%m/%Y')}"
                )

    saldo_aberto = saldo_aberto.quantize(_Q2)
    saldo_vencido = saldo_vencido.quantize(_Q2)
    tem_vencido = qtd_vencidos > 0

    # --- Pontualidade (títulos quitados reconstruíveis na janela) ---
    pesos: list[tuple[Decimal, Decimal]] = []  # (valor, fator)
    em_dia = 0
    atr_ate_3 = 0
    maior_atraso_pago = 0
    titulos_quitados_avaliaveis = 0
    titulos_import_sem_venda = 0

    for t in titulos_janela:
        if t.origem == FiadoTituloAgro.Origem.IMPORTACAO and not t.venda_agro_id:
            titulos_import_sem_venda += 1
        if t.situacao != FiadoTituloAgro.Situacao.QUITADO:
            continue
        bruto = _dec(t.valor_bruto)
        if bruto <= Decimal("0.009"):
            continue
        baixas = list(t.baixas.all())
        soma_baixas = sum((_dec(b.valor) for b in baixas), _ZERO)
        if not baixas or soma_baixas + Decimal("0.02") < bruto:
            alertas.append(
                f"Título quitado #{t.pk} sem baixas suficientes "
                f"(pago no título R$ {_dec(t.valor_pago)} · baixas R$ {soma_baixas})."
            )
            continue
        data_pag = _data_local(baixas[-1].criado_em)
        if not t.vencimento:
            alertas.append(f"Título quitado #{t.pk} sem data de vencimento.")
            continue
        atraso = max(0, (data_pag - t.vencimento).days)
        fator = _fator_pontualidade(atraso)
        pesos.append((bruto, fator))
        titulos_quitados_avaliaveis += 1
        maior_atraso_pago = max(maior_atraso_pago, atraso)
        if atraso <= 0:
            em_dia += 1
        elif atraso <= 3:
            atr_ate_3 += 1

    if titulos_import_sem_venda:
        alertas.append(
            f"{titulos_import_sem_venda} título(s) importado(s) sem VendaAgro "
            "(incluídos na análise por FK do cliente)."
        )

    if pesos:
        soma_v = sum((v for v, _f in pesos), _ZERO)
        media_pond = (
            sum((v * f for v, f in pesos), _ZERO) / soma_v if soma_v > 0 else _ZERO
        )
        pontualidade_pts = float((media_pond * Decimal("45")).quantize(_Q2))
    else:
        media_pond = _ZERO
        pontualidade_pts = 0.0

    # --- Situação atual ---
    if not tem_vencido:
        situacao_pts = 25
    else:
        situacao_pts = _pontos_situacao_atraso(maior_atraso_atual)

    # --- Quitação ---
    devido_vencido = _ZERO
    pago_sobre_vencido = _ZERO
    for t in titulos_janela:
        if t.situacao == FiadoTituloAgro.Situacao.CANCELADO:
            continue
        if not t.vencimento or t.vencimento >= ref:
            continue
        if t.criado_em and _data_local(t.criado_em) < janela_pag_ini and t.vencimento < janela_pag_ini:
            # título antigo já fora da janela de observação
            if t.situacao == FiadoTituloAgro.Situacao.QUITADO:
                continue
        bruto = _dec(t.valor_bruto)
        pago = min(_dec(t.valor_pago), bruto)
        devido_vencido += bruto
        pago_sobre_vencido += pago
    if devido_vencido > Decimal("0.009"):
        ratio_q = min(Decimal("1"), max(_ZERO, pago_sobre_vencido / devido_vencido))
    else:
        ratio_q = Decimal("1") if titulos_quitados_avaliaveis else Decimal("0")
    quitacao_pts = float((ratio_q * Decimal("10")).quantize(_Q2))

    # --- Frequência ---
    meses_set = meses_fiado_6m if meses_fiado_6m is not None else set()
    freq_pts = _pontos_frequencia(len(meses_set))

    # --- Relacionamento ---
    datas_hist: list[date] = []
    for t in titulos:
        if t.situacao == FiadoTituloAgro.Situacao.CANCELADO:
            continue
        if t.criado_em:
            datas_hist.append(_data_local(t.criado_em))
        if t.venda_agro_id and t.venda_agro and t.venda_agro.criado_em:
            datas_hist.append(_data_local(t.venda_agro.criado_em))
    primeiro = min(datas_hist) if datas_hist else None
    dias_rel = (ref - primeiro).days if primeiro else 0
    rel_pts = _pontos_relacionamento(dias_rel) if primeiro else 0

    # Histórico suficiente = ao menos 1 quitado com baixa reconstruível,
    # OU dívida em aberto/vencida (ainda assim a pontualidade pode ser 0).
    historico_suficiente = bool(
        titulos_quitados_avaliaveis >= 1
        or tem_vencido
        or saldo_aberto > Decimal("0.009")
    )

    if not historico_suficiente:
        score = None
        confianca = ClienteAnaliseCreditoAgro.Confianca.SEM_DADOS
        classificacao = ClienteAnaliseCreditoAgro.Classificacao.SEM_HISTORICO
        pontualidade_pts = 0.0
        situacao_pts = 0
        quitacao_pts = 0.0
        freq_pts = 0
        rel_pts = 0
        alertas.append("Sem histórico suficiente para score numérico.")
    else:
        score_raw = pontualidade_pts + situacao_pts + quitacao_pts + freq_pts + rel_pts
        score = _clamp_int(int(round(score_raw)), 0, 100)
        classificacao = _classificacao_de_score(score)
        if titulos_quitados_avaliaveis <= 2 or dias_rel < 90:
            confianca = ClienteAnaliseCreditoAgro.Confianca.BAIXA
        elif titulos_quitados_avaliaveis >= 6 and dias_rel >= 180:
            confianca = ClienteAnaliseCreditoAgro.Confianca.ALTA
        else:
            confianca = ClienteAnaliseCreditoAgro.Confianca.MEDIA

    media = media_3m if media_3m is not None else _ZERO
    mult = _multiplicador_limite(score)
    sugerido = (media * mult).quantize(_Q2) if score is not None else _ZERO

    candidato = bool(
        score is not None
        and score >= 85
        and confianca in (
            ClienteAnaliseCreditoAgro.Confianca.MEDIA,
            ClienteAnaliseCreditoAgro.Confianca.ALTA,
        )
        and not tem_vencido
        and sugerido > limite_efet
    )
    # Regra provisória: inconsistência quitado/baixas → não é candidato (revisar dados).
    revisar_dados = alerta_inconsistencia_pagamento(alertas)
    if revisar_dados:
        candidato = False

    indicadores = {
        "regra": REGRA_VERSAO,
        "janela_pagamento_meses": 12,
        "janela_media_meses": 3,
        "titulos_total_cliente": len(titulos),
        "titulos_analisados_janela": len(
            [t for t in titulos_janela if t.situacao != FiadoTituloAgro.Situacao.CANCELADO]
        ),
        "titulos_quitados_avaliaveis": titulos_quitados_avaliaveis,
        "titulos_vencidos_atualmente": qtd_vencidos,
        "titulo_vencido_mais_antigo": titulo_vencido_mais_antigo,
        "pagamentos_em_dia": em_dia,
        "atrasos_ate_3_dias": atr_ate_3,
        "maior_atraso_pago_dias": maior_atraso_pago,
        "pontualidade_media_ponderada": float(media_pond.quantize(_Q2)),
        "pontualidade_pontos": pontualidade_pts,
        "situacao_atual_pontos": situacao_pts,
        "quitacao_ratio": float(ratio_q.quantize(_Q2)),
        "quitacao_pontos": quitacao_pts,
        "frequencia_meses_com_compra": len(meses_set),
        "frequencia_pontos": freq_pts,
        "relacionamento_dias": dias_rel,
        "relacionamento_primeiro": primeiro.isoformat() if primeiro else "",
        "relacionamento_pontos": rel_pts,
        "multiplicador_limite": float(mult),
        "piso_operacional_sem_travar": float(max(sugerido, saldo_aberto).quantize(_Q2)),
        "candidato_revisao_limite": candidato,
        "revisar_dados_inconsistencia": revisar_dados,
    }

    return ResultadoAnaliseCredito(
        cliente_pk=cliente.pk,
        cliente_nome=(cliente.nome or "")[:200],
        score=score,
        classificacao=classificacao,
        confianca=confianca,
        limite_cadastrado=limite_cad,
        limite_efetivo=limite_efet,
        saldo_aberto=saldo_aberto,
        saldo_vencido=saldo_vencido,
        media_fiado_3m=media,
        limite_sugerido=sugerido,
        tem_vencido=tem_vencido,
        maior_atraso_dias=maior_atraso_atual if tem_vencido else maior_atraso_pago,
        indicadores=indicadores,
        alertas=alertas,
        candidato_revisao=candidato,
    )


def persistir_analise(resultado: ResultadoAnaliseCredito) -> ClienteAnaliseCreditoAgro:
    """Única escrita permitida deste módulo: INSERT na tabela shadow."""
    return ClienteAnaliseCreditoAgro.objects.create(
        cliente_id=resultado.cliente_pk,
        regra_versao=REGRA_VERSAO,
        score=resultado.score,
        classificacao=resultado.classificacao,
        confianca=resultado.confianca,
        limite_cadastrado_snapshot=resultado.limite_cadastrado,
        limite_efetivo_snapshot=resultado.limite_efetivo,
        saldo_aberto_snapshot=resultado.saldo_aberto,
        saldo_vencido_snapshot=resultado.saldo_vencido,
        media_fiado_3m=resultado.media_fiado_3m,
        limite_sugerido=resultado.limite_sugerido,
        tem_vencido_snapshot=resultado.tem_vencido,
        maior_atraso_dias=resultado.maior_atraso_dias,
        indicadores_json=resultado.indicadores,
        alertas_json=list(resultado.alertas),
    )


def analisar_clientes(
    *,
    cliente_ids: list[int] | None = None,
    persist: bool = False,
    hoje: date | None = None,
) -> list[ResultadoAnaliseCredito]:
    """Analisa um ou vários clientes. persist=True só grava ClienteAnaliseCreditoAgro."""
    ref = hoje or timezone.localdate()
    qs = ClienteAgro.objects.all().order_by("nome", "pk")
    if cliente_ids:
        qs = qs.filter(pk__in=list(cliente_ids))
    clientes = list(qs)
    if not clientes:
        return []

    pks = {c.pk for c in clientes}
    media_map = _media_fiado_3m_mapa(pks, hoje=ref)
    meses_map = _meses_com_fiado_6m(pks, hoje=ref)

    # Duplicados por nome (alerta) — não mistura histórico
    cont_nomes: dict[str, int] = defaultdict(int)
    for c in ClienteAgro.objects.all().only("nome").iterator(chunk_size=500):
        n = _norm_nome(c.nome)
        if n:
            cont_nomes[n] += 1
    nomes_dup = {n for n, q in cont_nomes.items() if q > 1}

    resultados: list[ResultadoAnaliseCredito] = []
    for cli in clientes:
        r = analisar_cliente(
            cli,
            hoje=ref,
            media_3m=media_map.get(cli.pk, _ZERO),
            meses_fiado_6m=meses_map.get(cli.pk, set()),
            nomes_duplicados=nomes_dup,
        )
        if persist:
            persistir_analise(r)
        resultados.append(r)
    return resultados


def ultimos_snapshots_por_cliente(
    *,
    limit_por_cliente: int = 1,
    cliente_ids: list[int] | None = None,
) -> dict[int, list[ClienteAnaliseCreditoAgro]]:
    """Últimos N snapshots por cliente (leitura)."""
    qs = ClienteAnaliseCreditoAgro.objects.select_related("cliente").order_by(
        "cliente_id", "-calculado_em", "-pk"
    )
    if cliente_ids:
        qs = qs.filter(cliente_id__in=list(cliente_ids))
    out: dict[int, list[ClienteAnaliseCreditoAgro]] = defaultdict(list)
    for row in qs.iterator(chunk_size=300):
        bucket = out[row.cliente_id]
        if len(bucket) >= limit_por_cliente:
            continue
        bucket.append(row)
    return dict(out)
