# -*- coding: utf-8 -*-
"""Prova detalhada — CAIXA-DEVOL-DINHEIRO-MP.

Venda no Point/cartão/Pix + devolução em dinheiro no mesmo turno:
a maquininha continua com o valor; a gaveta cai; auto não mostra «Sobra».
FL-017 (dinheiro + dinheiro) permanece: esperado = abertura.
"""
from __future__ import annotations

import inspect
import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.test import RequestFactory

from produtos.caixa_util import (  # noqa: E402
    FORMAS_MP_POINT_AUTO_CONFERENCIA,
    _agregar_resumo_turno_sessao,
    eh_movimento_retirada_devolucao,
    forma_fechamento_auto_ocultavel,
    resumo_devolucao_dinheiro_maquina,
    serializar_estado_conferencia_fechar,
)
from produtos.views import api_caixa_conferencia_estado

FAILS: list[str] = []
OKS = 0

MP_DEB = "Cartão de débito — Mercado Pago"
MP_PIX = "Pix — Mercado Pago"
MP_CRED = "Cartão de crédito — Mercado Pago"


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


class _Rel:
    def __init__(self, items=None):
        self._items = list(items or [])

    def all(self):
        return self._items


def _venda(*, pk: int, forma: str, valor: Decimal, maquina: str | None, devolvida: bool):
    row: dict = {"forma": forma, "valor": float(valor)}
    if maquina:
        row["maquinaId"] = maquina
        row["cobrarNoPointMp"] = True
        row["mpBalcaoModo"] = "point"
    return SimpleNamespace(
        pk=pk,
        pagamentos_json=[row],
        forma_pagamento=forma,
        total=valor,
        devolvida_em=object() if devolvida else None,
    )


def _mov(*, pk: int, tipo: str, forma: str, valor: Decimal, obs: str):
    return SimpleNamespace(
        pk=pk,
        tipo=tipo,
        forma_pagamento=forma,
        valor=valor,
        observacao=obs,
    )


def _sessao(vendas, movimentos, *, abertura: str = "0", pk: int = 50, ponto: str = "gaveta"):
    return SimpleNamespace(
        pk=pk,
        ponto_caixa=ponto,
        valor_abertura=Decimal(abertura),
        usuario=None,
        usuario_id=None,
        vendas=_Rel(vendas),
        movimentos=_Rel(movimentos),
        fechado_em=None,
    )


def _agregar(sessao):
    with patch("produtos.models.PdvMercadoPagoPointOrder.objects") as objs:
        objs.filter.return_value.values_list.return_value = []
        return _agregar_resumo_turno_sessao(sessao)


def _serialize(sessoes, *, deposito: str = "centro"):
    with patch("produtos.models.PdvMercadoPagoPointOrder.objects") as objs:
        objs.filter.return_value.values_list.return_value = []
        return serializar_estado_conferencia_fechar(sessoes, deposito=deposito)


def _linha(estado: dict, forma: str) -> dict | None:
    for L in estado.get("linhas") or []:
        if L.get("forma") == forma:
            return L
    return None


def _git_changed_vs_live() -> list[str]:
    r = subprocess.run(
        ["git", "diff", "--name-only", "1c870a5a...HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if r.returncode != 0:
        return []
    return [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]


def _catalogo_fonte() -> None:
    util_p = ROOT / "produtos/caixa_util.py"
    html_p = ROOT / "produtos/templates/produtos/caixa_fechar.html"
    inc_p = ROOT / "produtos/templates/produtos/includes/caixa_fechar_linha_conf.html"
    views_p = ROOT / "produtos/views.py"
    rel_p = ROOT / "produtos/caixa_relatorio_util.py"
    js_rep = ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js"

    util = util_p.read_text(encoding="utf-8")
    html = html_p.read_text(encoding="utf-8")
    inc = inc_p.read_text(encoding="utf-8")
    views = views_p.read_text(encoding="utf-8")
    rel = rel_p.read_text(encoding="utf-8")
    js_repasse = js_rep.read_text(encoding="utf-8")

    check(
        "_movimentos_retirada_devolucao_duplicados_turno" not in util,
        "fonte: helper FL-017 duplicado removido do util",
    )
    check(
        "_movimentos_retirada_devolucao_duplicados_turno" not in rel,
        "fonte: helper FL-017 duplicado ausente no relatorio",
    )
    check("vendas_list = list(vendas_rel.all())" in util, "fonte: agrega todas as vendas do turno")
    check("resumo_devolucao_dinheiro_maquina" in util, "fonte: helper aviso gaveta")
    check('"aviso_devolucao_dinheiro": resumo_devolucao_dinheiro_maquina' in util, "fonte: serialize inclui aviso")

    src_ag = inspect.getsource(_agregar_resumo_turno_sessao)
    check("eh_movimento_retirada_devolucao" not in src_ag, "fonte: agregar nao ignora retirada de devolucao")
    check("devolvida_em" not in src_ag, "fonte: agregar nao filtra venda devolvida")
    check("esperado[fn] -= val" in src_ag, "fonte: retirada sempre desconta esperado")

    check("cf-aviso-devolucao-dinheiro" in html, "fonte: banner aviso no Fechar caixa")
    check("aplicarAvisoDevolucaoDinheiro" in html, "fonte: JS atualiza aviso no refresh")
    check("aplicarAutoContadoEsperado" in html, "fonte: JS auto copia esperado")
    check(
        "data-auto-contado') === '1') return" in html
        or 'data-auto-contado") === "1") return' in html,
        "fonte: rascunho nao sobrescreve linha auto",
    )
    check("data.aviso_devolucao_dinheiro" in html, "fonte: refresh aplica aviso da API")
    check("escopo=loja" in html, "fonte: refresh filtra loja (nao mistura Centro+Vila)")
    check(
        "Devolução em <strong>dinheiro</strong> de venda no cartão/Pix" in html,
        "fonte: ajuda ? menciona devolucao em dinheiro",
    )
    check("conte a gaveta já sem esse valor" in util, "fonte: texto aviso pede contar gaveta sem o valor")
    check("Cartão/Pix da maquininha não muda" in util, "fonte: texto aviso maquininha nao muda")

    check("readonly" in inc, "fonte: input auto readonly")
    check('data-auto-contado="1" readonly' in inc, "fonte: auto + readonly no mesmo input")
    check(
        "Devolução em dinheiro não tira este valor da maquininha" in inc,
        "fonte: ? da linha auto explica maquininha",
    )
    check("Preenchido sozinho com o esperado" in inc, "fonte: title auto sem contar")
    check("L.forma == 'Dinheiro'" in inc, "fonte: dinheiro tem campo proprio")
    check("cf-conf-row--dinheiro" in inc, "fonte: linha dinheiro destacada")

    check("aviso_devolucao_dinheiro" in views, "fonte: view Fechar caixa passa aviso")
    check("serializar_estado_conferencia_fechar" in views, "fonte: view usa serialize")
    check('{"ok": True, **estado}' in views, "fonte: API conferencia devolve estado (inclui aviso)")
    check("filtrar_sessoes_por_deposito" in views, "fonte: API filtra deposito")

    check("if eh_movimento_retirada_devolucao(obs):" in rel, "fonte: relatorio ainda pula movimento de devolucao")
    check("Devoluções (eventos" in rel or "Devoluções (eventos parciais" in rel, "fonte: relatorio lista devolucao pelos eventos")

    check("agro-caixa-fechar-atualizar" in js_repasse, "fonte: repasse avisa tela fechar")
    check("notifyParentFecharAtualizar" in js_repasse, "fonte: repasse notify parent intacto")

    mudou = _git_changed_vs_live()
    migr = [p for p in mudou if "/migrations/" in p or p.endswith("migrations")]
    check(not migr, f"fonte: sem migrate neste pacote got {migr}")
    check(any(p.endswith("produtos/caixa_util.py") for p in mudou), "fonte: pacote toca caixa_util")
    check(
        any("caixa_fechar.html" in p for p in mudou),
        "fonte: pacote toca Fechar caixa",
    )


def _caso_mp_debito_devolucao_dinheiro() -> None:
    """Caso loja: Point débito R$ 49 devolvido em dinheiro + R$ 5,90 que ficou."""
    v_dev = _venda(pk=1, forma="Cartão de débito", valor=Decimal("49.00"), maquina="mp_balcao", devolvida=True)
    v_ok = _venda(pk=2, forma="Cartão de débito", valor=Decimal("5.90"), maquina="mp_balcao", devolvida=False)
    ret = _mov(pk=10, tipo="retirada", forma="Dinheiro", valor=Decimal("49.00"), obs="Devolução venda #1")
    sess = _sessao([v_dev, v_ok], [ret], abertura="100.00")
    esperado, vendas, _ref, retirada = _agregar(sess)
    check(vendas.get(MP_DEB) == Decimal("54.90"), f"MP debito vendas 54.90 got {vendas.get(MP_DEB)}")
    check(esperado.get(MP_DEB) == Decimal("54.90"), f"MP debito esperado 54.90 (pinpad) got {esperado.get(MP_DEB)}")
    check(retirada.get("Dinheiro") == Decimal("49.00"), f"retirada dinheiro 49 got {retirada.get('Dinheiro')}")
    check(esperado.get("Dinheiro") == Decimal("51.00"), f"dinheiro 100-49=51 got {esperado.get('Dinheiro')}")
    check(esperado.get("Cartão de débito", Decimal("0")) == Decimal("0"), "Cielo debito nao recebe o Point")

    av = resumo_devolucao_dinheiro_maquina([sess])
    check(av.get("tem") is True, "aviso ON no caso loja")
    check(av.get("qtd") == 1, f"aviso qtd 1 got {av.get('qtd')}")
    check(av.get("valor") == "49.00", f"aviso valor 49.00 got {av.get('valor')}")
    check("49,00" in str(av.get("texto") or ""), f"aviso texto 49,00 got {av.get('texto')}")
    check("gaveta" in str(av.get("texto") or "").lower(), "aviso pede gaveta")
    check("não muda" in str(av.get("texto") or ""), "aviso diz maquininha nao muda")

    st = _serialize([sess])
    avs = st.get("aviso_devolucao_dinheiro") or {}
    check(avs.get("tem") is True, "serialize aviso ON")
    check(st.get("tot_esperado_dinheiro") == "51.00", f"serialize tot dinheiro 51 got {st.get('tot_esperado_dinheiro')}")
    lm = _linha(st, MP_DEB)
    check(lm is not None, "serialize tem linha MP debito")
    check(lm and lm.get("esperado") == "54.90", f"serialize MP esperado 54.90 got {lm}")
    check(lm and lm.get("auto_contado") is True, "MP debito e linha auto")
    check(lm and lm.get("grupo_oculto") is True, "MP debito no bloco oculto")
    ld = _linha(st, "Dinheiro")
    check(ld and ld.get("esperado") == "51.00", f"serialize dinheiro 51 got {ld}")
    check(ld and not ld.get("auto_contado"), "dinheiro NAO e auto (caixa conta)")


def _caso_pix_mp() -> None:
    v = _venda(pk=11, forma="PIX", valor=Decimal("43.00"), maquina="pix_mp_qr", devolvida=True)
    ret = _mov(pk=20, tipo="retirada", forma="Dinheiro", valor=Decimal("43.00"), obs="Devolução venda #11")
    sess = _sessao([v], [ret], abertura="10.00")
    esperado, vendas, _, _ = _agregar(sess)
    check(vendas.get(MP_PIX) == Decimal("43.00"), f"Pix MP vendas 43 got {vendas.get(MP_PIX)}")
    check(esperado.get(MP_PIX) == Decimal("43.00"), f"Pix MP esperado 43 (pinpad) got {esperado.get(MP_PIX)}")
    check(esperado.get("Dinheiro") == Decimal("-33.00"), f"pix dinheiro 10-43=-33 got {esperado.get('Dinheiro')}")
    av = resumo_devolucao_dinheiro_maquina([sess])
    check(av.get("tem") is True, "aviso ON pix MP → dinheiro")


def _caso_credito_mp() -> None:
    v = _venda(pk=12, forma="Cartão de crédito", valor=Decimal("80.00"), maquina="mp_balcao", devolvida=True)
    ret = _mov(pk=21, tipo="retirada", forma="Dinheiro", valor=Decimal("80.00"), obs="Devolução venda #12")
    sess = _sessao([v], [ret], abertura="200.00")
    esperado, _, _, _ = _agregar(sess)
    check(esperado.get(MP_CRED) == Decimal("80.00"), f"credito MP esperado 80 got {esperado.get(MP_CRED)}")
    check(esperado.get("Dinheiro") == Decimal("120.00"), f"credito MP dinheiro 200-80=120 got {esperado.get('Dinheiro')}")
    check(resumo_devolucao_dinheiro_maquina([sess]).get("tem") is True, "aviso ON credito MP → dinheiro")


def _caso_fl017_dinheiro() -> None:
    v = _venda(pk=3, forma="Dinheiro", valor=Decimal("49.00"), maquina=None, devolvida=True)
    ret = _mov(pk=11, tipo="retirada", forma="Dinheiro", valor=Decimal("49.00"), obs="Devolução venda #3")
    sess = _sessao([v], [ret], abertura="20.00")
    esperado, vendas, _, retirada = _agregar(sess)
    check(vendas.get("Dinheiro") == Decimal("49.00"), "FL-017 vendas incluem devolvida")
    check(retirada.get("Dinheiro") == Decimal("49.00"), "FL-017 retirada aplicada")
    check(esperado.get("Dinheiro") == Decimal("20.00"), f"FL-017 esperado = abertura got {esperado.get('Dinheiro')}")
    av = resumo_devolucao_dinheiro_maquina([sess])
    check(av.get("tem") is False, "aviso OFF se devolucao foi so dinheiro")
    st = _serialize([sess])
    check((st.get("aviso_devolucao_dinheiro") or {}).get("tem") is False, "serialize aviso OFF cash+cash")


def _caso_cielo_debito_dinheiro() -> None:
    v = _venda(pk=4, forma="Cartão de débito", valor=Decimal("70.00"), maquina=None, devolvida=True)
    ret = _mov(pk=12, tipo="retirada", forma="Dinheiro", valor=Decimal("70.00"), obs="Devolução venda #4")
    sess = _sessao([v], [ret], abertura="100.00")
    esperado, vendas, _, _ = _agregar(sess)
    check(vendas.get("Cartão de débito") == Decimal("70.00"), "Cielo debito permanece nas vendas")
    check(esperado.get("Cartão de débito") == Decimal("70.00"), "Cielo debito esperado nao zera")
    check(esperado.get("Dinheiro") == Decimal("30.00"), f"Cielo dinheiro 100-70=30 got {esperado.get('Dinheiro')}")
    check(esperado.get(MP_DEB, Decimal("0")) == Decimal("0"), "Cielo nao vaza para linha MP")
    check(resumo_devolucao_dinheiro_maquina([sess]).get("tem") is True, "aviso ON Cielo → dinheiro")


def _caso_parcial() -> None:
    """Devolução parcial: venda ainda aberta (sem devolvida_em) + retirada."""
    v = _venda(pk=5, forma="Cartão de débito", valor=Decimal("100.00"), maquina="mp_balcao", devolvida=False)
    ret = _mov(pk=13, tipo="retirada", forma="Dinheiro", valor=Decimal("40.00"), obs="Devolução venda #5")
    sess = _sessao([v], [ret], abertura="50.00")
    esperado, vendas, _, retirada = _agregar(sess)
    check(v.devolvida_em is None, "parcial: venda nao marcada devolvida_em")
    check(vendas.get(MP_DEB) == Decimal("100.00"), "parcial: pinpad fica 100")
    check(esperado.get(MP_DEB) == Decimal("100.00"), "parcial: esperado MP 100")
    check(retirada.get("Dinheiro") == Decimal("40.00"), "parcial: retirada 40")
    check(esperado.get("Dinheiro") == Decimal("10.00"), f"parcial dinheiro 50-40=10 got {esperado.get('Dinheiro')}")
    av = resumo_devolucao_dinheiro_maquina([sess])
    check(av.get("tem") is True, "parcial: aviso ON")
    check(av.get("valor") == "40.00", f"parcial aviso 40 got {av.get('valor')}")


def _caso_outro_turno() -> None:
    """Venda de outro turno: so a retirada deste caixa. Esperado MP deste turno nao infla."""
    venda_antiga = _venda(pk=99, forma="Cartão de débito", valor=Decimal("49.00"), maquina="mp_balcao", devolvida=True)
    ret = _mov(pk=30, tipo="retirada", forma="Dinheiro", valor=Decimal("49.00"), obs="Devolução venda #99")
    sess = _sessao([], [ret], abertura="100.00")
    esperado, vendas, _, retirada = _agregar(sess)
    check(not vendas, "outro turno: sem vendas deste caixa")
    check(esperado.get(MP_DEB, Decimal("0")) == Decimal("0"), "outro turno: MP deste caixa nao muda")
    check(retirada.get("Dinheiro") == Decimal("49.00"), "outro turno: retirada dinheiro aplicada")
    check(esperado.get("Dinheiro") == Decimal("51.00"), f"outro turno dinheiro 100-49=51 got {esperado.get('Dinheiro')}")

    with patch("produtos.models.VendaAgro.objects") as vos:
        vos.filter.return_value = []
        av_sem = resumo_devolucao_dinheiro_maquina([sess])
    check(av_sem.get("tem") is False, "outro turno sem lookup: aviso OFF (venda nao na memoria)")

    with patch("produtos.models.VendaAgro.objects") as vos:
        vos.filter.return_value = [venda_antiga]
        av_com = resumo_devolucao_dinheiro_maquina([sess])
    check(av_com.get("tem") is True, "outro turno com lookup: aviso ON")
    check(av_com.get("valor") == "49.00", f"outro turno aviso 49 got {av_com.get('valor')}")


def _caso_sangria_nao_e_devolucao() -> None:
    ret = _mov(pk=40, tipo="retirada", forma="Dinheiro", valor=Decimal("15.00"), obs="Sangria banco")
    sess = _sessao([], [ret], abertura="100.00")
    esperado, _, _, retirada = _agregar(sess)
    check(retirada.get("Dinheiro") == Decimal("15.00"), "sangria desconta")
    check(esperado.get("Dinheiro") == Decimal("85.00"), f"sangria 100-15=85 got {esperado.get('Dinheiro')}")
    check(resumo_devolucao_dinheiro_maquina([sess]).get("tem") is False, "sangria nao dispara aviso")
    check(eh_movimento_retirada_devolucao("Sangria banco") is False, "helper nao classifica sangria")
    check(eh_movimento_retirada_devolucao("Devolução venda #1") is True, "helper classifica devolucao")
    check(eh_movimento_retirada_devolucao("devolucao venda #1") is True, "helper aceita sem acento")


def _caso_duas_devolucoes() -> None:
    v1 = _venda(pk=21, forma="PIX", valor=Decimal("10.00"), maquina="pix_mp_qr", devolvida=True)
    v2 = _venda(pk=22, forma="Cartão de débito", valor=Decimal("20.00"), maquina="mp_balcao", devolvida=True)
    r1 = _mov(pk=51, tipo="retirada", forma="Dinheiro", valor=Decimal("10.00"), obs="Devolução venda #21")
    r2 = _mov(pk=52, tipo="retirada", forma="Dinheiro", valor=Decimal("20.00"), obs="Devolução venda #22")
    sess = _sessao([v1, v2], [r1, r2], abertura="30.00")
    esperado, _, _, _ = _agregar(sess)
    check(esperado.get(MP_PIX) == Decimal("10.00"), "duas: Pix MP 10")
    check(esperado.get(MP_DEB) == Decimal("20.00"), "duas: debito MP 20")
    check(esperado.get("Dinheiro") == Decimal("0.00"), f"duas: dinheiro 30-30=0 got {esperado.get('Dinheiro')}")
    av = resumo_devolucao_dinheiro_maquina([sess])
    check(av.get("tem") is True, "duas: aviso ON")
    check(av.get("qtd") == 2, f"duas: qtd 2 got {av.get('qtd')}")
    check(av.get("valor") == "30.00", f"duas: valor 30 got {av.get('valor')}")


def _caso_auto_formas() -> None:
    for fn in FORMAS_MP_POINT_AUTO_CONFERENCIA:
        check(forma_fechamento_auto_ocultavel(fn, deposito="centro"), f"auto ocultavel {fn} centro")
        check(forma_fechamento_auto_ocultavel(fn, deposito="vila"), f"auto ocultavel {fn} vila")
    check(forma_fechamento_auto_ocultavel("Fiado"), "auto Fiado")
    check(forma_fechamento_auto_ocultavel("Vale crédito"), "auto Vale")
    check(forma_fechamento_auto_ocultavel("Cashback"), "auto Cashback")
    check(not forma_fechamento_auto_ocultavel("Dinheiro"), "Dinheiro nao e auto")
    check(not forma_fechamento_auto_ocultavel("PIX"), "PIX Cielo nao e auto")
    check(not forma_fechamento_auto_ocultavel("Cartão de débito"), "debito Cielo nao e auto")


def _caso_api_aviso_e_loja() -> None:
    v_dev = _venda(pk=1, forma="Cartão de débito", valor=Decimal("49.00"), maquina="mp_balcao", devolvida=True)
    v_ok = _venda(pk=2, forma="Cartão de débito", valor=Decimal("5.90"), maquina="mp_balcao", devolvida=False)
    ret = _mov(pk=10, tipo="retirada", forma="Dinheiro", valor=Decimal("49.00"), obs="Devolução venda #1")
    s_c = _sessao([v_dev, v_ok], [ret], abertura="100.00", pk=202, ponto="gaveta")
    s_v = _sessao([], [], abertura="333.33", pk=101, ponto="vila")

    qs = MagicMock()
    qs.select_related.return_value = qs
    qs.prefetch_related.return_value = qs
    qs.order_by.return_value = [s_v, s_c]
    rf = RequestFactory()

    def _chama(dep: str):
        r = rf.get("/api/caixa/conferencia-estado/", {"escopo": "loja"})
        r.user = SimpleNamespace(is_authenticated=True, pk=1)
        r.session = {}
        with (
            patch("produtos.views.SessaoCaixa.objects.filter", return_value=qs),
            patch("produtos.views.deposito_caixa_browser", return_value=dep),
            patch("produtos.models.PdvMercadoPagoPointOrder.objects") as objs,
        ):
            objs.filter.return_value.values_list.return_value = []
            return api_caixa_conferencia_estado(r)

    try:
        resp = _chama("centro")
        data = json.loads(resp.content.decode())
        ids = {int(c.get("sessao_id") or 0) for c in (data.get("cards") or [])}
        check(data.get("ok") is True, "api centro ok")
        check(202 in ids, "api centro inclui gaveta")
        check(101 not in ids, "api centro nao inclui Vila")
        check(data.get("tot_esperado_dinheiro") == "51.00", f"api centro tot 51 got {data.get('tot_esperado_dinheiro')}")
        av = data.get("aviso_devolucao_dinheiro") or {}
        check(av.get("tem") is True, "api centro devolve aviso ON")
        check("49,00" in str(av.get("texto") or ""), "api aviso mostra 49,00")

        resp_v = _chama("vila")
        data_v = json.loads(resp_v.content.decode())
        ids_v = {int(c.get("sessao_id") or 0) for c in (data_v.get("cards") or [])}
        check(101 in ids_v, "api vila inclui vila")
        check(202 not in ids_v, "api vila nao inclui gaveta centro")
        check(data_v.get("tot_esperado_dinheiro") == "333.33", f"api vila tot 333.33 got {data_v.get('tot_esperado_dinheiro')}")
        av_v = data_v.get("aviso_devolucao_dinheiro") or {}
        check(av_v.get("tem") is False, "api vila aviso OFF (sem devolucao)")
    except Exception as exc:
        fail(f"api estado aviso/loja: {exc}")


def main() -> int:
    _catalogo_fonte()
    _caso_mp_debito_devolucao_dinheiro()
    _caso_pix_mp()
    _caso_credito_mp()
    _caso_fl017_dinheiro()
    _caso_cielo_debito_dinheiro()
    _caso_parcial()
    _caso_outro_turno()
    _caso_sangria_nao_e_devolucao()
    _caso_duas_devolucoes()
    _caso_auto_formas()
    _caso_api_aviso_e_loja()

    print(f"---\noks={OKS} fails={len(FAILS)}")
    if FAILS:
        for f in FAILS:
            print(" ", f)
        return 1
    print("VERIFY_CAIXA_DEVOL_DINHEIRO_MP_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
