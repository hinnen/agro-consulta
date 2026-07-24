"""Gravação CP/CR em ``TituloFinanceiroAgro`` — desvinculação Mongo."""
from __future__ import annotations

import logging
import secrets
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from bson import ObjectId
from django.db import transaction
from django.utils import timezone

from produtos.lancamentos_financeiro_pg_util import (
    _dec2,
    _tem_id_erp_valido,
    _titulo_manual_agro_lote,
)
from produtos.models import TituloFinanceiroAgro
from produtos.mongo_financeiro_util import (
    _adicionar_meses_preservando_dia_referencia,
    _fin_banco_id_valido_quitado,
    _fin_ln_bool,
    _fin_ln_campo,
    _fin_ln_despesa,
    _fin_ln_id_campo,
    _fin_ln_parse_date,
    _fin_ln_txt,
    _fin_parse_valor_entrada_manual,
    _financeiro_id_para_string,
    emprestimo_plano_juros_resolvido,
    normalizar_boleto_codigo_barras_mongo,
    normalizar_rotulo_banco_erp,
    resolver_plano_conta_para_pedido_erp,
)

logger = logging.getLogger(__name__)

_TOL = Decimal("0.02")


def _notificar_baixa_rh_titulo_salario_pg(
    *,
    mongo_id: str,
    valor_baixa: Decimal,
    data: date | None,
    tipo_origem: str,
    referencia_externa_id: str,
) -> None:
    try:
        from rh.services.pagamento_salario import processar_baixa_cp_titulo_salario

        d = data or timezone.localdate()
        processar_baixa_cp_titulo_salario(
            mongo_id=mongo_id,
            valor_baixa=valor_baixa,
            data=d,
            tipo_origem=tipo_origem,
            referencia_externa_id=referencia_externa_id,
        )
    except Exception:
        logger.exception("RH: hook baixa CP título salário (PG) mongo_id=%s", mongo_id)


def financeiro_grava_postgres(despesa: bool) -> bool:
    from produtos.agro_fonte_config import agro_financeiro_usa_postgres, agro_mongo_erp_desligado

    # Loja com Mongo ERP cortado: sempre grava/alinha título no Postgres.
    return agro_financeiro_usa_postgres() or agro_mongo_erp_desligado()


def financeiro_cp_grava_postgres(despesa: bool) -> bool:
    return financeiro_grava_postgres(despesa)


def _get_titulo(mongo_id: str, *, despesa: bool | None = None) -> TituloFinanceiroAgro | None:
    mid = str(mongo_id or "").strip()
    if not mid:
        return None
    qs = TituloFinanceiroAgro.objects.filter(mongo_id=mid)
    if despesa is not None:
        qs = qs.filter(despesa=bool(despesa))
    return qs.first()


def _get_titulo_cp(mongo_id: str) -> TituloFinanceiroAgro | None:
    return _get_titulo(mongo_id, despesa=True)


def _titulo_quitado(t: TituloFinanceiroAgro) -> bool:
    if t.quitado:
        return True
    return _dec2(t.valor_restante) <= _TOL


def _titulo_pode_excluir(t: TituloFinanceiroAgro) -> bool:
    quitado = _titulo_quitado(t)
    mov = _dec2(t.valor_pago)
    if _titulo_manual_agro_lote(t):
        return True
    if _tem_id_erp_valido(t):
        return False
    return (not quitado) and mov <= _TOL


def _touch_titulo(t: TituloFinanceiroAgro, *, mod: str, now: datetime | None = None) -> None:
    agora = now or timezone.now()
    t.mongo_ultima_atualizacao = agora
    t.modificado_por = mod[:200]
    snap = dict(t.dados_snapshot_json or {})
    snap["data_modificacao"] = agora.isoformat()
    snap["last_update"] = agora.isoformat()
    t.dados_snapshot_json = snap


def _sync_valores_pos_baixa(t: TituloFinanceiroAgro, *, quitado: bool) -> None:
    bruto = _dec2(t.valor_bruto)
    pago = _dec2(t.valor_pago)
    if quitado:
        pago = bruto
    rest = max(Decimal("0"), bruto - pago)
    if rest <= _TOL:
        rest = Decimal("0")
        quitado = True
        pago = bruto
    t.valor_pago = pago
    t.valor_restante = rest
    t.quitado = quitado


def alinhar_titulo_pg_apos_sync_folha_rh(
    mongo_id: str,
    *,
    valor_bruto: Decimal | float,
    valor_pago: Decimal | float,
    data_vencimento: date | None = None,
) -> dict[str, Any]:
    """
    Força bruto/pago/restante no Postgres após sync folha RH.
    Complementa espelhar Mongo→PG (baixas parciais no CP podem deixar PG inflado).
    """
    if not financeiro_grava_postgres(True):
        return {"ok": True, "skipped": True, "motivo": "financeiro_legacy"}

    mid = str(mongo_id or "").strip()
    if not mid:
        return {"ok": False, "erro": "mongo_id vazio."}

    t = TituloFinanceiroAgro.objects.filter(mongo_id=mid, despesa=True).first()
    if t is None:
        return {"ok": False, "erro": "Título não encontrado no Postgres."}

    bruto = _dec2(valor_bruto)
    pago = _dec2(valor_pago)
    if pago > bruto:
        pago = bruto
    rest = bruto - pago
    quitado = rest <= _TOL
    if quitado:
        rest = Decimal("0")
        pago = bruto

    now = timezone.now()
    mod = "Agro — sync folha RH"
    t.valor_bruto = bruto
    t.valor_pago = pago
    t.valor_restante = rest
    t.quitado = quitado
    if data_vencimento:
        t.data_vencimento = data_vencimento
    _touch_titulo(t, mod=mod, now=now)
    t.save()
    return {"ok": True, "id": mid, "valor_pago": float(pago)}


def _criar_proximo_recorrente_pg(t: TituloFinanceiroAgro, *, usuario_label: str) -> str | None:
    if not t.agro_recorrente or not _titulo_quitado(t):
        return None
    intervalo = 1 if t.agro_recorrente_sempre else max(1, min(int(t.recorrencia_intervalo_meses or 1), 36))
    dc = t.data_competencia
    dv = t.data_vencimento or dc
    if dc is None or dv is None:
        return None
    ndc = _adicionar_meses_preservando_dia_referencia(dc, intervalo)
    ndv = _adicionar_meses_preservando_dia_referencia(dv, intervalo)
    new_id = str(ObjectId())
    user = (usuario_label or "Agro")[:200]
    base_nd = (t.numero_documento or "MAN")[:60]
    obs_ant = (t.observacoes or "").strip()
    linha_rec = f"Gerado automaticamente (recorrência) a partir do título quitado {t.mongo_id}."
    obs = " | ".join(p for p in (linha_rec, obs_ant) if p)[:2000]
    novo = TituloFinanceiroAgro(
        mongo_id=new_id,
        despesa=bool(t.despesa),
        descricao=t.descricao,
        cliente=t.cliente,
        cliente_id=t.cliente_id,
        numero_documento=f"{base_nd}-R{secrets.token_hex(3).upper()}"[:80],
        parcela=t.parcela,
        plano_conta=t.plano_conta,
        plano_conta_id=t.plano_conta_id,
        grupo=t.grupo,
        forma_pagamento="",
        forma_pagamento_id="",
        banco=t.banco,
        banco_id=t.banco_id,
        centro_custo=t.centro_custo,
        empresa=t.empresa,
        observacoes=obs,
        valor_bruto=_dec2(t.valor_bruto),
        valor_pago=Decimal("0"),
        valor_restante=_dec2(t.valor_bruto),
        quitado=False,
        data_vencimento=ndv,
        data_competencia=ndc,
        data_fluxo=timezone.localdate(),
        data_pagamento=None,
        agro_recorrente=bool(t.agro_recorrente_sempre),
        recorrencia_intervalo_meses=1 if t.agro_recorrente_sempre else t.recorrencia_intervalo_meses,
        agro_recorrente_sempre=bool(t.agro_recorrente_sempre),
        boleto_codigo_barras=t.boleto_codigo_barras,
        usuario_lancou=user,
        criado_por=user,
        modificado_por=f"{user} — recorrência Agro (após quitação)"[:200],
        mongo_congelado=True,
        mongo_ultima_atualizacao=timezone.now(),
        dados_snapshot_json={"mongo_id": new_id, "id_erp": "", "lancamento_id": ""},
    )
    novo.save()
    return new_id


def baixar_lancamentos_pg(
    ids: list[str],
    *,
    despesa: bool,
    data_movimento: datetime,
    forma_nome: str,
    forma_id: str | None,
    banco_nome: str,
    banco_id: str | None,
    usuario_label: str,
) -> dict[str, Any]:
    forma_nome = (forma_nome or "").strip()
    banco_nome = (banco_nome or "").strip()
    if not forma_nome or not banco_nome:
        return {"ok": False, "atualizados": [], "erros": [{"id": "", "erro": "Informe forma de pagamento e conta/banco."}]}

    fid = _financeiro_id_para_string(forma_id)
    bid = _financeiro_id_para_string(banco_id)
    banco_nome = normalizar_rotulo_banco_erp(bid, banco_nome)[:120]
    mod = ((usuario_label or "Agro")[:80] + " — baixa SisVale")[:200]
    now = timezone.now()
    dp = data_movimento.date() if hasattr(data_movimento, "date") else None
    lbl_saldo = "Sem saldo a pagar" if despesa else "Sem saldo a receber"

    res_ok: list[str] = []
    res_err: list[dict] = []

    for sid in (ids or [])[:80]:
        t = _get_titulo(sid, despesa=despesa)
        if t is None:
            res_err.append({"id": sid, "erro": "Lançamento não encontrado"})
            continue
        if _titulo_quitado(t):
            res_err.append({"id": sid, "erro": "Já quitado"})
            continue
        rest = _dec2(t.valor_restante)
        bruto = _dec2(t.valor_bruto)
        if rest <= 0 or bruto <= 0:
            res_err.append({"id": sid, "erro": lbl_saldo})
            continue
        t.forma_pagamento = forma_nome[:120]
        t.forma_pagamento_id = fid or ""
        t.banco = banco_nome[:120]
        t.banco_id = bid or ""
        t.valor_pago = bruto
        t.valor_restante = Decimal("0")
        t.quitado = True
        t.data_pagamento = dp
        t.usuario_quitou = (usuario_label or "Agro")[:150]
        _touch_titulo(t, mod=mod, now=now)
        t.save()
        res_ok.append(t.mongo_id)
        from rh.models import PagamentoSalarioFuncionario

        _notificar_baixa_rh_titulo_salario_pg(
            mongo_id=t.mongo_id,
            valor_baixa=rest,
            data=dp,
            tipo_origem=PagamentoSalarioFuncionario.TipoOrigem.CP_TOTAL,
            referencia_externa_id=f"{t.mongo_id}:total:{secrets.token_hex(8)}",
        )
        _criar_proximo_recorrente_pg(t, usuario_label=usuario_label)

    return {"ok": len(res_err) == 0, "atualizados": res_ok, "erros": res_err}


def baixar_lancamento_parcial_pg(
    lancamento_id: str,
    *,
    despesa: bool,
    data_movimento: datetime,
    parcelas: list[dict[str, Any]],
    usuario_label: str,
    notificar_rh_baixa_cp: bool = True,
) -> dict[str, Any]:
    raw = [p for p in (parcelas or []) if isinstance(p, dict)]
    if not raw or len(raw) > 24:
        return {"ok": False, "id": None, "erro": "Informe de 1 a 24 parcelas (valor + forma + banco).", "quitado": False}

    lid = str(lancamento_id or "").strip()
    t = _get_titulo(lid, despesa=despesa)
    if t is None:
        return {"ok": False, "id": None, "erro": "Lançamento não encontrado", "quitado": False}
    if _titulo_quitado(t):
        return {"ok": False, "id": lid, "erro": "Título já quitado", "quitado": False}

    soma_par = Decimal("0")
    parsed: list[tuple[Decimal, str, str, str, str]] = []
    for par in raw:
        forma_nome = str(par.get("forma_pagamento") or par.get("forma_nome") or "").strip()
        banco_nome = str(par.get("banco") or par.get("banco_nome") or "").strip()
        try:
            valor_par = _dec2(par.get("valor"))
        except Exception:
            return {"ok": False, "id": lid, "erro": "Valor inválido em uma das parcelas.", "quitado": False}
        if valor_par <= 0:
            return {"ok": False, "id": lid, "erro": "Cada parcela deve ter valor maior que zero.", "quitado": False}
        if not forma_nome or not banco_nome:
            return {"ok": False, "id": lid, "erro": "Cada parcela precisa de forma de pagamento e banco/conta.", "quitado": False}
        fid = _financeiro_id_para_string(par.get("forma_pagamento_id") or par.get("forma_id"))
        bid = _financeiro_id_para_string(par.get("banco_id"))
        banco_nome = normalizar_rotulo_banco_erp(bid, banco_nome)[:120]
        parsed.append((valor_par, forma_nome[:120], banco_nome, fid or "", bid or ""))
        soma_par += valor_par

    rest_ini = _dec2(t.valor_restante)
    if rest_ini <= 0 or soma_par > rest_ini + _TOL:
        return {
            "ok": False,
            "id": lid,
            "erro": f"Soma das parcelas (R$ {float(soma_par):.2f}) não pode exceder o saldo (R$ {rest_ini:.2f}).",
            "quitado": False,
        }

    mod = ((usuario_label or "Agro")[:80] + " — baixa parcial Agro")[:200]
    now = timezone.now()
    dp = data_movimento.date() if hasattr(data_movimento, "date") else None
    quitado_final = False

    for valor_par, forma_nome, banco_nome, fid, bid in parsed:
        t.refresh_from_db()
        if _titulo_quitado(t):
            return {"ok": False, "id": lid, "erro": "Título quitado durante a baixa", "quitado": False}
        obs_ant = (t.observacoes or "")[:1800]
        linha_obs = (
            f"Agro parc. {timezone.localtime(data_movimento).strftime('%d/%m/%Y')} "
            f"{forma_nome[:50]}/{banco_nome[:50]} R$ {float(valor_par):.2f}"
        )
        t.observacoes = (obs_ant + (" | " if obs_ant else "") + linha_obs)[:2000]
        t.valor_pago = _dec2(t.valor_pago) + valor_par
        t.forma_pagamento = forma_nome
        t.forma_pagamento_id = fid
        t.banco = banco_nome
        t.banco_id = bid
        t.data_pagamento = dp
        bruto = _dec2(t.valor_bruto)
        rest_apos = bruto - _dec2(t.valor_pago)
        if rest_apos <= _TOL:
            t.valor_pago = bruto
            t.valor_restante = Decimal("0")
            t.quitado = True
            quitado_final = True
        else:
            t.valor_restante = rest_apos
            t.quitado = False
        t.usuario_quitou = (usuario_label or "Agro")[:150]
        _touch_titulo(t, mod=mod, now=now)
        t.save()

    if quitado_final:
        _criar_proximo_recorrente_pg(t, usuario_label=usuario_label)

    if notificar_rh_baixa_cp:
        from rh.models import PagamentoSalarioFuncionario

        _notificar_baixa_rh_titulo_salario_pg(
            mongo_id=lid,
            valor_baixa=soma_par,
            data=dp,
            tipo_origem=PagamentoSalarioFuncionario.TipoOrigem.CP_PARCIAL,
            referencia_externa_id=f"{lid}:parc:{secrets.token_hex(8)}",
        )

    return {"ok": True, "id": lid, "quitado": quitado_final}


def excluir_lancamento_pg(lancamento_id: str, usuario_label: str) -> dict[str, Any]:
    t = _get_titulo(lancamento_id)
    if t is None:
        return {"ok": False, "erro": "Lançamento não encontrado"}
    if not _titulo_pode_excluir(t):
        if _tem_id_erp_valido(t):
            erro = "Exclusão não permitida: título vinculado ao ERP."
        elif not _titulo_manual_agro_lote(t) and (_titulo_quitado(t) or _dec2(t.valor_pago) > _TOL):
            erro = "Exclusão não permitida: quitado ou com movimento (exceto lançamento manual Agro)."
        else:
            erro = "Exclusão não permitida."
        return {"ok": False, "erro": erro}
    mid = t.mongo_id
    t.delete()
    logger.info("excluir_lancamento_pg: mongo_id=%s por=%s", mid, (usuario_label or "")[:80])
    return {"ok": True}


def atualizar_lancamento_pg(lancamento_id: str, patch: dict[str, Any], usuario_label: str) -> dict[str, Any]:
    t = _get_titulo(lancamento_id)
    if t is None:
        return {"ok": False, "erro": "Lançamento não encontrado"}
    if _titulo_quitado(t):
        return {"ok": False, "erro": "Não é possível alterar título quitado."}
    mov_r = float(_dec2(t.valor_pago))
    mod = ((usuario_label or "Agro")[:80] + " — edição lançamento Agro")[:200]
    now = timezone.now()
    alterou = False

    if "descricao" in patch:
        t.descricao = str(patch.get("descricao") or "").strip()[:500]
        alterou = True
    if "cliente" in patch:
        t.cliente = str(patch.get("cliente") or "").strip()[:300]
        alterou = True
    if "cliente_id" in patch and patch.get("cliente_id") is not None:
        t.cliente_id = (_financeiro_id_para_string(patch.get("cliente_id")) or "")[:32]
        alterou = True
    if "plano_conta" in patch:
        t.plano_conta = str(patch.get("plano_conta") or "").strip()[:200]
        alterou = True
    if "plano_conta_id" in patch and patch.get("plano_conta_id") is not None:
        t.plano_conta_id = (_financeiro_id_para_string(patch.get("plano_conta_id")) or "")[:32]
        alterou = True
    dv = patch.get("data_vencimento")
    if dv is not None:
        ds = str(dv).strip()[:10]
        try:
            d = date.fromisoformat(ds)
        except ValueError:
            return {"ok": False, "erro": "data_vencimento inválida (AAAA-MM-DD)."}
        t.data_vencimento = d
        alterou = True
    if "valor_bruto" in patch and patch.get("valor_bruto") is not None:
        if mov_r > 0.02:
            return {"ok": False, "erro": "Não é possível alterar o valor com pagamento já registrado."}
        try:
            vb = _dec2(patch.get("valor_bruto"))
        except Exception:
            return {"ok": False, "erro": "valor_bruto inválido."}
        if vb <= 0:
            return {"ok": False, "erro": "valor_bruto deve ser maior que zero."}
        t.valor_bruto = vb
        t.valor_restante = vb
        alterou = True
    if "banco" in patch:
        bn = str(patch.get("banco") or "").strip()
        if bn:
            bid_e = _financeiro_id_para_string(patch.get("banco_id"))
            t.banco = normalizar_rotulo_banco_erp(bid_e, bn)[:120]
            t.banco_id = (bid_e or "")[:32]
            alterou = True
    if "forma_pagamento" in patch:
        fn = str(patch.get("forma_pagamento") or "").strip()
        if fn:
            t.forma_pagamento = fn[:120]
            t.forma_pagamento_id = (_financeiro_id_para_string(patch.get("forma_pagamento_id")) or "")[:32]
            alterou = True
    if "boleto_codigo_barras" in patch:
        bv = normalizar_boleto_codigo_barras_mongo(patch.get("boleto_codigo_barras"))
        t.boleto_codigo_barras = (bv or "")[:54]
        alterou = True

    if not alterou:
        return {"ok": False, "erro": "Nenhum campo para atualizar."}
    _touch_titulo(t, mod=mod, now=now)
    t.save()
    return {"ok": True, "id": t.mongo_id}


def registrar_titulo_juros_apos_baixa_contas_pagar_pg(
    *,
    mongo_id_titulo_referencia: str,
    valor_juros: Decimal,
    data_movimento: date,
    forma_nome: str,
    forma_id: str | None,
    banco_nome: str,
    banco_id: str | None,
    usuario_label: str,
    db=None,
) -> dict[str, Any]:
    valor_juros = _dec2(valor_juros)
    if valor_juros <= 0:
        return {"ok": False, "erro": "Valor de juros inválido."}
    ref = _get_titulo(mongo_id_titulo_referencia, despesa=True)
    if ref is None:
        return {"ok": False, "erro": "Título de referência não encontrado."}
    fn = (forma_nome or "").strip()
    bid_s = _financeiro_id_para_string(banco_id)
    bn = normalizar_rotulo_banco_erp(bid_s, (banco_nome or "").strip())[:120]
    if not fn or not bn:
        return {"ok": False, "erro": "Forma e conta são obrigatórios para lançar juros."}

    pj_texto = emprestimo_plano_juros_resolvido()
    pj_id = ""
    if db is not None:
        snap = ref.dados_snapshot_json or {}
        empresa_id = str(snap.get("empresa_id") or "").strip() or None
        pj_texto_res, pj_id_res = resolver_plano_conta_para_pedido_erp(
            db,
            texto_config=pj_texto,
            id_config=None,
            empresa_id=empresa_id,
        )
        pj_texto = ((pj_texto_res or "").strip() or pj_texto)[:200]
        pj_id = (pj_id_res or "").strip()

    r = inserir_lancamentos_manual_lote_pg(
        despesa=True,
        empresa_nome=ref.empresa,
        empresa_id=None,
        pessoa_nome=ref.cliente or "—",
        pessoa_id=ref.cliente_id or None,
        data_competencia=data_movimento,
        data_vencimento=data_movimento,
        banco_nome=bn,
        banco_id=banco_id,
        forma_nome=fn,
        forma_id=forma_id,
        grupo_nome=None,
        grupo_id=None,
        usuario_label=usuario_label,
        linhas=[
            {
                "plano_conta": pj_texto,
                "plano_conta_id": pj_id or None,
                "valor": float(valor_juros),
                "descricao": "Juros na quitação",
                "observacao": f"Ref. título {mongo_id_titulo_referencia}"[:500],
            }
        ],
        marcar_quitado_pagar=True,
    )
    if not r.get("ok") or not r.get("ids"):
        erros = r.get("erros") or []
        msg = erros[0].get("erro") if erros else "Falha ao inserir título de juros."
        return {"ok": False, "erro": str(msg)[:500]}
    return {"ok": True, "id": r["ids"][0]}


def inserir_lancamentos_manual_lote_pg(
    *,
    despesa: bool,
    empresa_nome: str,
    empresa_id: str | None,
    pessoa_nome: str,
    pessoa_id: str | None,
    data_competencia: date,
    data_vencimento: date,
    banco_nome: str,
    banco_id: str | None,
    forma_nome: str,
    forma_id: str | None,
    grupo_nome: str | None,
    grupo_id: str | None,
    usuario_label: str,
    linhas: list[dict[str, Any]],
    marcar_quitado_pagar: bool = False,
    marcar_quitado_receber: bool = False,
    recorrente: bool = False,
    recorrente_modo: str = "sempre",
    recorrente_parcelas: int = 1,
    exigir_plano_cadastrado: bool = False,
) -> dict[str, Any]:
    linhas = [x for x in (linhas or []) if isinstance(x, dict)]
    if not linhas:
        return {"ok": False, "ids": [], "erros": [{"erro": "Informe ao menos uma linha."}]}

    cab_ok = bool((empresa_nome or "").strip() and (pessoa_nome or "").strip())
    if not cab_ok:
        cab_ok = all(
            _fin_ln_txt(ln, "empresa_nome", empresa_nome) and _fin_ln_txt(ln, "pessoa_nome", pessoa_nome)
            for ln in linhas
        )
    if not cab_ok:
        return {
            "ok": False,
            "ids": [],
            "erros": [{"erro": "Preencha empresa, cliente/fornecedor e conta bancária (conta só nas despesas / saída)."}],
        }

    planned_pre = 0
    for idx, ln in enumerate(linhas):
        ln_desp_pre = _fin_ln_despesa(ln, despesa)
        ln_rec = _fin_ln_bool(ln, "recorrente", recorrente)
        ln_mod = (str(ln.get("recorrente_modo") or recorrente_modo or "sempre")).strip().lower()
        if ln_mod not in ("sempre", "normal"):
            ln_mod = "sempre"
        try:
            ln_n = int(ln.get("recorrente_parcelas") or recorrente_parcelas or 1)
        except (TypeError, ValueError):
            ln_n = 1
        ln_n = max(1, min(ln_n, 12))
        n_cp = ln_n if (ln_rec and ln_mod == "normal") else 1
        planned_pre += n_cp
        ln_quit = _fin_ln_bool(ln, "quitado", marcar_quitado_pagar or marcar_quitado_receber)
        ln_quit_pagar_pre = ln_quit and ln_desp_pre
        if ln_rec and ln_mod == "normal" and ln_quit_pagar_pre and ln_n > 1:
            return {
                "ok": False,
                "ids": [],
                "erros": [
                    {
                        "linha": idx + 1,
                        "erro": "Modo Normal com mais de um título não combina com «Lançar quitado». Desmarque quitado ou use quantidade 1.",
                    }
                ],
            }
    if planned_pre > 60:
        return {"ok": False, "ids": [], "erros": [{"erro": "Modo Normal: no máximo 60 títulos no lote (linhas × quantidade)."}]}

    now = timezone.now()
    lote = f"AG{secrets.token_hex(4).upper()}"
    user = (usuario_label or "Agro")[:200]
    eid = _financeiro_id_para_string(empresa_id)
    pid = _financeiro_id_para_string(pessoa_id)
    bid_hdr = _financeiro_id_para_string(banco_id)
    fid_hdr = _financeiro_id_para_string(forma_id)
    gid = _financeiro_id_para_string(grupo_id)

    inserted: list[str] = []
    erros: list[dict] = []
    novos: list[TituloFinanceiroAgro] = []
    parcela_seq = 0

    for idx, ln in enumerate(linhas):
        n = idx + 1
        try:
            valor = _fin_parse_valor_entrada_manual(ln.get("valor", ""))
        except (ValueError, TypeError):
            erros.append({"linha": n, "erro": "Valor inválido"})
            continue
        if valor <= 0:
            erros.append({"linha": n, "erro": "Valor deve ser maior que zero"})
            continue
        plano_nome = (ln.get("plano_conta") or ln.get("plano_nome") or "").strip()
        plano_id_raw = ln.get("plano_conta_id") or ln.get("plano_id")
        if not plano_nome:
            erros.append({"linha": n, "erro": "Plano de conta obrigatório"})
            continue
        # Só Nova saída / lote manual (UI). RH, NF, juros, recorrência NÃO bloqueiam.
        if exigir_plano_cadastrado:
            try:
                from produtos.plano_conta_agro_util import (
                    cadastro_planos_disponivel,
                    validar_plano_para_lancamento_manual,
                )

                if cadastro_planos_disponivel():
                    vplano = validar_plano_para_lancamento_manual(plano_nome, plano_id_raw)
                    if not vplano.get("ok"):
                        erros.append({"linha": n, "erro": vplano.get("erro") or "Plano inválido"})
                        continue
                    plano_nome = str(vplano.get("nome") or plano_nome).strip()
                    if vplano.get("plano_conta_id"):
                        plano_id_raw = vplano.get("plano_conta_id")
            except Exception:
                logger.exception("validar_plano_para_lancamento_manual")

        ln_empresa = _fin_ln_txt(ln, "empresa_nome", empresa_nome)
        ln_pessoa = _fin_ln_txt(ln, "pessoa_nome", pessoa_nome)
        ln_banco = _fin_ln_campo(ln, "banco_nome", banco_nome)
        ln_forma = _fin_ln_campo(ln, "forma_nome", forma_nome)
        ln_despesa = _fin_ln_despesa(ln, despesa)
        if not ln_empresa or not ln_pessoa:
            erros.append({"linha": n, "erro": "Preencha loja e pessoa."})
            continue
        if ln_despesa and not ln_banco:
            erros.append({"linha": n, "erro": "Preencha conta bancária (obrigatória na saída / pagamento)."})
            continue
        le_id = (_financeiro_id_para_string(ln.get("empresa_id") or empresa_id) or "")[:32]
        lp_id = (_financeiro_id_para_string(ln.get("pessoa_id") or pessoa_id) or "")[:32]
        lb_id = (_fin_ln_id_campo(ln, "banco_id", banco_id) or "")[:32]
        lf_id = (_fin_ln_id_campo(ln, "forma_id", forma_id) or "")[:32]

        ln_rec = _fin_ln_bool(ln, "recorrente", recorrente)
        ln_mod = (str(ln.get("recorrente_modo") or recorrente_modo or "sempre")).strip().lower()
        if ln_mod not in ("sempre", "normal"):
            ln_mod = "sempre"
        try:
            ln_n = int(ln.get("recorrente_parcelas") or recorrente_parcelas or 1)
        except (TypeError, ValueError):
            ln_n = 1
        ln_n = max(1, min(ln_n, 12))
        n_copies = ln_n if (ln_rec and ln_mod == "normal") else 1
        ln_quit = _fin_ln_bool(ln, "quitado", marcar_quitado_pagar or marcar_quitado_receber)
        ln_quit_pagar = ln_quit and ln_despesa
        ln_quit_receber = ln_quit and not ln_despesa
        if ln_quit_pagar and not _fin_banco_id_valido_quitado(lb_id):
            erros.append(
                {
                    "linha": n,
                    "erro": "Saída quitada: escolha conta real na lista (não use «ADICIONAR CONTA»).",
                }
            )
            continue

        base_dc = _fin_ln_parse_date(ln.get("data_competencia"), data_competencia)
        base_dv = _fin_ln_parse_date(ln.get("data_vencimento"), data_vencimento)
        desc_base = (ln.get("descricao") or f"Lançamento manual {n}").strip()[:500]
        valor_dec = _dec2(valor)
        bc = normalizar_boleto_codigo_barras_mongo(
            ln.get("boleto_codigo_barras") or ln.get("codigo_barras_boleto")
        )

        for sub in range(n_copies):
            parcela_seq += 1
            use_dc_d = _adicionar_meses_preservando_dia_referencia(base_dc, sub)
            use_dv_d = _adicionar_meses_preservando_dia_referencia(base_dv, sub)
            desc_suf = ""
            if ln_rec and ln_mod == "normal" and n_copies > 1:
                desc_suf = f" ({sub + 1}/{n_copies})"
            obs_linha = (ln.get("observacao") or ln.get("observacoes") or "").strip()
            obs_antecipado = ""
            if ln_rec and ln_mod == "normal" and n_copies > 1:
                obs_antecipado = f"Antecipado {sub + 1}/{n_copies} (modo Normal)"
            obs_quitado = ""
            if ln_quit_pagar:
                obs_quitado = "Título lançado como quitado via lote manual"
            elif ln_quit_receber:
                obs_quitado = "Título lançado como recebido via lote manual"
            observacoes = " | ".join(
                p for p in (obs_linha, obs_antecipado, obs_quitado, f"Lote manual Agro {lote}") if p
            )[:2000]
            mongo_id = str(ObjectId())
            quitado = bool(ln_quit_pagar or ln_quit_receber)
            pago = valor_dec if quitado else Decimal("0")
            rest = Decimal("0") if quitado else valor_dec
            bn_norm = normalizar_rotulo_banco_erp(lb_id, ln_banco)[:120] if ln_banco else ""

            titulo = TituloFinanceiroAgro(
                mongo_id=mongo_id,
                despesa=bool(ln_despesa),
                descricao=(desc_base + desc_suf)[:500],
                cliente=ln_pessoa[:300],
                cliente_id=lp_id,
                numero_documento=(
                    f"{lote}-{n:02d}" if n_copies == 1 else f"{lote}-{n:02d}-p{sub + 1}"
                )[:80],
                parcela=max(0, parcela_seq - 1),
                plano_conta=plano_nome[:200],
                plano_conta_id=(_financeiro_id_para_string(plano_id_raw) or "")[:32],
                grupo=(grupo_nome or "").strip()[:200],
                forma_pagamento=ln_forma[:120],
                forma_pagamento_id=lf_id,
                banco=bn_norm,
                banco_id=lb_id,
                empresa=ln_empresa[:200],
                observacoes=observacoes,
                valor_bruto=valor_dec,
                valor_pago=pago,
                valor_restante=rest,
                quitado=quitado,
                data_vencimento=use_dv_d,
                data_competencia=use_dc_d,
                data_fluxo=now.date(),
                data_pagamento=use_dv_d if quitado else None,
                agro_recorrente=bool(ln_rec and ln_mod == "sempre"),
                recorrencia_intervalo_meses=1 if (ln_rec and ln_mod == "sempre") else max(1, ln_n),
                agro_recorrente_sempre=bool(ln_rec and ln_mod == "sempre"),
                boleto_codigo_barras=(bc or "")[:54] if ln_despesa else "",
                usuario_lancou=user,
                usuario_quitou=user if quitado else "",
                criado_por=user,
                modificado_por=f"{user} — inclusão manual em lote Agro"[:200],
                mongo_congelado=True,
                mongo_ultima_atualizacao=now,
                dados_snapshot_json={
                    "mongo_id": mongo_id,
                    "id_erp": "",
                    "lancamento_id": "",
                    "empresa_id": le_id or eid or "",
                    "last_update": now.isoformat(),
                    "data_modificacao": now.isoformat(),
                },
            )
            novos.append(titulo)
            inserted.append(mongo_id)

    if novos:
        with transaction.atomic():
            TituloFinanceiroAgro.objects.bulk_create(novos, batch_size=100)

    ok = len(erros) == 0 and bool(inserted)
    return {
        "ok": ok,
        "lote": lote,
        "ids": inserted,
        "erros": erros,
    }


def inserir_lancamentos_manual_lote_dispatch(
    db,
    *,
    despesa: bool,
    **kwargs: Any,
) -> dict[str, Any]:
    """Mongo vs Postgres CP vs simulação staging."""
    from produtos.agro_fonte_config import agro_mongo_erp_desligado
    from produtos.agro_mongo_guard import agro_mongo_escrita_bloqueada
    from produtos.mongo_financeiro_util import (
        inserir_lancamentos_manual_lote,
        simular_lancamentos_manual_lote_staging,
    )

    if financeiro_grava_postgres(despesa):
        return inserir_lancamentos_manual_lote_pg(despesa=despesa, **kwargs)
    # Mongo ERP morto / desligado: grava no Postgres (mesmo sem AGRO_FONTE_FINANCEIRO=agro_pg).
    if agro_mongo_erp_desligado() or db is None:
        try:
            return inserir_lancamentos_manual_lote_pg(despesa=despesa, **kwargs)
        except Exception:
            logger.exception("inserir_lancamentos_manual_lote_dispatch fallback PG")
            return simular_lancamentos_manual_lote_staging(linhas=kwargs.get("linhas", []))
    if agro_mongo_escrita_bloqueada():
        return simular_lancamentos_manual_lote_staging(linhas=kwargs.get("linhas", []))
    return inserir_lancamentos_manual_lote(db, despesa=despesa, **kwargs)


def excluir_lancamento_dispatch(
    db,
    lancamento_id: str,
    usuario_label: str,
    *,
    despesa: bool = True,
) -> dict[str, Any]:
    """Postgres CP vs Mongo — espelha ``inserir_lancamentos_manual_lote_dispatch``."""
    from produtos.agro_fonte_config import agro_mongo_erp_desligado
    from produtos.agro_mongo_guard import agro_mongo_escrita_bloqueada
    from produtos.mongo_financeiro_util import excluir_lancamento_mongo_agro

    # Mesma regra do insert: ERP morto / db None → Postgres (reabrir NF não pode falhar).
    if financeiro_grava_postgres(despesa) or agro_mongo_erp_desligado() or db is None:
        r = excluir_lancamento_pg(lancamento_id, usuario_label)
        if r.get("ok") or agro_mongo_erp_desligado() or db is None:
            return r
        if agro_mongo_escrita_bloqueada():
            return r
    return excluir_lancamento_mongo_agro(db, lancamento_id, usuario_label)
