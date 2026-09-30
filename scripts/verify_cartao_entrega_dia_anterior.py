# -*- coding: utf-8 -*-
"""Prova — cartão de entrega do dia anterior não entra no esperado de hoje.

  python scripts/verify_cartao_entrega_dia_anterior.py
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ.setdefault("SECRET_KEY", "verify-cartao-entrega-dia-anterior")

import django

django.setup()

from django.utils import timezone

from produtos.caixa_util import (
    _agregar_resumo_turno_sessao,
    linha_eh_cartao_maquina,
    linha_passa_na_maquininha,
    linhas_conferencia_fechar,
    resumo_cartao_entrega_dia_anterior,
    serializar_estado_conferencia_fechar,
)
from produtos.entrega_pdv_pendente_util import (
    cartao_maquina_dia_anterior_aceito,
    data_cartao_maquina_escolhida,
)
import produtos.entrega_pdv_pendente_util as eu
import produtos.models as pm

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print("  OK  " + name + ((" — " + detail) if detail else ""))
    else:
        fails.append(name)
        print("  FAIL " + name + ((" — " + detail) if detail else ""))


class _QS:
    def values_list(self, *a, **k):
        return []


class _Mgr:
    class Status:
        FINALIZED = "finalized"

    class objects:
        @staticmethod
        def filter(**k):
            return _QS()


class _Rel:
    def __init__(self, items):
        self._items = items

    def all(self):
        return list(self._items)


class _Venda:
    def __init__(self, pk, pagamentos, flag=False):
        self.pk = pk
        self.pagamentos_json = pagamentos
        self.cartao_maquina_dia_anterior = flag
        self.devolvida_em = None
        self.forma_pagamento = ""
        self.total = 0


class _Mov:
    def __init__(self, tipo, forma, valor, obs=""):
        self.tipo = tipo
        self.forma_pagamento = forma
        self.valor = Decimal(str(valor))
        self.observacao = obs


class _Sessao:
    def __init__(self, vendas, movimentos=None):
        self.pk = 1
        self.usuario_id = None
        self.valor_abertura = Decimal("100.00")
        self.vendas = _Rel(vendas)
        self.movimentos = _Rel(movimentos or [])


def main() -> int:
    check("cartão débito", linha_eh_cartao_maquina("Cartão de débito"))
    check("cartão MP", linha_eh_cartao_maquina("Cartão de crédito — Mercado Pago"))
    check("pix fora do cartão", not linha_eh_cartao_maquina("PIX"))
    check("pix entra na máquina", linha_passa_na_maquininha("PIX"))
    check("pix MP entra na máquina", linha_passa_na_maquininha("Pix — Mercado Pago"))
    check("dinheiro fora", not linha_eh_cartao_maquina("Dinheiro") and not linha_passa_na_maquininha("Dinheiro"))

    real_mp = pm.PdvMercadoPagoPointOrder
    pm.PdvMercadoPagoPointOrder = _Mgr
    try:
        venda = _Venda(
            1,
            [
                {"forma": "Cartão de débito", "valor": 40},
                {"forma": "Dinheiro", "valor": 10},
            ],
            flag=True,
        )
        esp, vendas, _, _ = _agregar_resumo_turno_sessao(_Sessao([venda]))
        check("cartão ontem fora do esperado", esp.get("Cartão de débito", Decimal("0")) == 0)
        check(
            "dinheiro de hoje continua",
            esp.get("Dinheiro") == Decimal("110.00"),
            str(esp.get("Dinheiro")),
        )
        check("vendas cartão zeradas", vendas.get("Cartão de débito", Decimal("0")) == 0)

        normal = _Venda(2, [{"forma": "Cartão de crédito", "valor": 25}], flag=False)
        esp2, _, _, _ = _agregar_resumo_turno_sessao(_Sessao([normal]))
        check("cartão de hoje entra", esp2.get("Cartão de crédito") == Decimal("25.00"))

        av = resumo_cartao_entrega_dia_anterior([_Sessao([venda])])
        check("aviso tem", av.get("tem") is True, str(av.get("valor")))
        check("aviso 40", av.get("valor") == "40.00")
        check("aviso some sem marca", resumo_cartao_entrega_dia_anterior([_Sessao([normal])]).get("tem") is False)

        mp = _Venda(
            3,
            [
                {
                    "forma": "Cartão de débito",
                    "valor": 55,
                    "cobrarNoPointMp": True,
                    "maquinaId": "mp_balcao",
                }
            ],
            flag=True,
        )
        esp_mp, _, _, _ = _agregar_resumo_turno_sessao(_Sessao([mp]))
        check(
            "point ontem fora da linha MP",
            esp_mp.get("Cartão de débito — Mercado Pago", Decimal("0")) == 0,
        )
        check(
            "point ontem não cai no débito comum",
            esp_mp.get("Cartão de débito", Decimal("0")) == 0,
        )
        av_mp = resumo_cartao_entrega_dia_anterior([_Sessao([mp])])
        check("aviso point 55", av_mp.get("valor") == "55.00" and av_mp.get("tem") is True)

        pix = _Venda(4, [{"forma": "PIX", "valor": 30}], flag=True)
        esp_pix, _, _, _ = _agregar_resumo_turno_sessao(_Sessao([pix]))
        check("pix de outro dia fora do esperado", esp_pix.get("PIX", Decimal("0")) == 0)
        av_pix = resumo_cartao_entrega_dia_anterior([_Sessao([pix])])
        check("pix de outro dia gera aviso", av_pix.get("tem") is True and av_pix.get("valor") == "30.00")
        pix_hoje = _Venda(41, [{"forma": "PIX", "valor": 12}], flag=False)
        esp_pix_hoje, _, _, _ = _agregar_resumo_turno_sessao(_Sessao([pix_hoje]))
        check("pix de hoje continua no esperado", esp_pix_hoje.get("PIX") == Decimal("12.00"))

        misto = _Venda(
            42,
            [{"forma": "PIX", "valor": 20}, {"forma": "Dinheiro", "valor": 5}],
            flag=True,
        )
        esp_m, _, _, _ = _agregar_resumo_turno_sessao(_Sessao([misto]))
        check(
            "pix misturado fora e dinheiro fica",
            esp_m.get("PIX", Decimal("0")) == 0 and esp_m.get("Dinheiro") == Decimal("105.00"),
            str(esp_m.get("Dinheiro")),
        )
        check(
            "aviso só o pix misturado",
            resumo_cartao_entrega_dia_anterior([_Sessao([misto])]).get("valor") == "20.00",
        )

        pix_mp = _Venda(
            43,
            [{"forma": "PIX", "valor": 18, "maquinaId": "pix_mp_qr"}],
            flag=True,
        )
        esp_pm, _, _, _ = _agregar_resumo_turno_sessao(_Sessao([pix_mp]))
        check(
            "pix MP automático fora da linha",
            esp_pm.get("Pix — Mercado Pago", Decimal("0")) == 0,
        )
        check("pix MP não cai no pix comum", esp_pm.get("PIX", Decimal("0")) == 0)
        check(
            "aviso pix MP",
            resumo_cartao_entrega_dia_anterior([_Sessao([pix_mp])]).get("valor") == "18.00",
        )

        pix_conta = _Venda(
            44,
            [{"forma": "PIX", "valor": 7, "maquinaId": "pix_sicoob_chave"}],
            flag=True,
        )
        esp_pc, _, _, _ = _agregar_resumo_turno_sessao(_Sessao([pix_conta]))
        check("pix da conta fora do esperado", esp_pc.get("PIX", Decimal("0")) == 0)
        check(
            "aviso pix da conta",
            resumo_cartao_entrega_dia_anterior([_Sessao([pix_conta])]).get("valor") == "7.00",
        )

        dev_pix = _Mov("retirada", "PIX", 30, "Devolução venda #4")
        esp_dp, _, _, ret_dp = _agregar_resumo_turno_sessao(_Sessao([pix], [dev_pix]))
        check("devolução do pix de outro dia não fura", esp_dp.get("PIX", Decimal("0")) == 0)
        check(
            "devolução do pix de outro dia não entra em retirada",
            ret_dp.get("PIX", Decimal("0")) == 0,
        )

        parc = _Venda(5, [{"forma": "Cartão de crédito parcelado", "valor": 80}], flag=True)
        esp_p, _, _, _ = _agregar_resumo_turno_sessao(_Sessao([parc]))
        check("parcelado ontem fora do crédito", esp_p.get("Cartão de crédito", Decimal("0")) == 0)
        check("aviso parcelado 80", resumo_cartao_entrega_dia_anterior([_Sessao([parc])]).get("valor") == "80.00")

        dev_card = _Mov("retirada", "Cartão de débito", 40, "Devolução venda #1")
        esp_d, _, _, ret_d = _agregar_resumo_turno_sessao(_Sessao([venda], [dev_card]))
        check("devolução do cartão de ontem não fura o esperado", esp_d.get("Cartão de débito", Decimal("0")) == 0)
        check("devolução do cartão de ontem não entra em retirada", ret_d.get("Cartão de débito", Decimal("0")) == 0)
        check("dinheiro da mesma venda segue", esp_d.get("Dinheiro") == Decimal("110.00"))

        so_card = _Venda(7, [{"forma": "Cartão de crédito", "valor": 40}], flag=True)
        dev_din = _Mov("retirada", "Dinheiro", 40, "Devolução venda #7")
        esp_dd, _, _, _ = _agregar_resumo_turno_sessao(_Sessao([so_card], [dev_din]))
        check(
            "devolver em dinheiro hoje baixa a gaveta",
            esp_dd.get("Dinheiro") == Decimal("60.00"),
            str(esp_dd.get("Dinheiro")),
        )
        check("crédito de ontem continua fora", esp_dd.get("Cartão de crédito", Decimal("0")) == 0)

        outra = _Venda(8, [{"forma": "Cartão de débito", "valor": 15}], flag=True)
        av2 = resumo_cartao_entrega_dia_anterior([_Sessao([venda, outra])])
        check("duas entregas somam 55", av2.get("valor") == "55.00" and av2.get("qtd") == 2)

        venda.devolvida_em = timezone.now()
        check(
            "venda devolvida some do aviso",
            resumo_cartao_entrega_dia_anterior([_Sessao([venda])]).get("tem") is False,
        )
        venda.devolvida_em = None

        linhas = linhas_conferencia_fechar(_Sessao([venda]))
        card_linhas = [L for L in linhas if str(L["forma"]).lower().startswith("cart")]
        check("fechar não lista cartão zerado", card_linhas == [])
        estado = serializar_estado_conferencia_fechar([_Sessao([venda])], deposito="centro")
        check("json do fechar traz o aviso", (estado.get("aviso_cartao_entrega_ontem") or {}).get("tem") is True)
        dia_aviso = timezone.localdate() - timedelta(days=3)
        venda.cartao_maquina_dia = dia_aviso
        texto_av = (resumo_cartao_entrega_dia_anterior([_Sessao([venda])]).get("texto") or "")
        check("aviso cita o dia escolhido", dia_aviso.strftime("%d/%m/%Y") in texto_av)
    finally:
        pm.PdvMercadoPagoPointOrder = real_mp

    class _Ent:
        criado_em = timezone.now() - timedelta(days=1, hours=3)

    class _Filtro:
        def exclude(self, **k):
            return self

        def first(self):
            return _Ent()

    real_filter = eu.PedidoEntrega.objects.filter
    eu.PedidoEntrega.objects.filter = lambda *a, **k: _Filtro()
    try:
        pag = [{"forma": "Cartão de débito", "valor": 15}]
        check(
            "aceita outro dia no calendário",
            data_cartao_maquina_escolhida(
                {
                    "cartao_maquina_dia_anterior": "outro",
                    "cartao_maquina_data": (timezone.localdate() - timedelta(days=3)).isoformat(),
                    "pedido_entrega_pendente_id": 9,
                },
                pag,
            )
            == timezone.localdate() - timedelta(days=3),
        )
        check(
            "recusa dia de hoje no calendário",
            not cartao_maquina_dia_anterior_aceito(
                {
                    "cartao_maquina_dia_anterior": True,
                    "cartao_maquina_data": timezone.localdate().isoformat(),
                    "pedido_entrega_pendente_id": 9,
                },
                pag,
            ),
        )
        check(
            "recusa dia futuro",
            not cartao_maquina_dia_anterior_aceito(
                {
                    "cartao_maquina_dia_anterior": True,
                    "cartao_maquina_data": (timezone.localdate() + timedelta(days=1)).isoformat(),
                    "pedido_entrega_pendente_id": 9,
                },
                pag,
            ),
        )
        check(
            "recusa sem marca",
            not cartao_maquina_dia_anterior_aceito(
                {"pedido_entrega_pendente_id": 9},
                pag,
            ),
        )
        check(
            "aceita pix",
            cartao_maquina_dia_anterior_aceito(
                {"cartao_maquina_dia_anterior": True, "pedido_entrega_pendente_id": 9},
                [{"forma": "PIX", "valor": 15}],
            ),
        )
        check(
            "aceita pix da conta",
            cartao_maquina_dia_anterior_aceito(
                {"cartao_maquina_dia_anterior": True, "pedido_entrega_pendente_id": 9},
                [{"forma": "Sicoob chave Pix", "valor": 7}],
            ),
        )
        check(
            "recusa só dinheiro",
            not cartao_maquina_dia_anterior_aceito(
                {"cartao_maquina_dia_anterior": True, "pedido_entrega_pendente_id": 9},
                [{"forma": "Dinheiro", "valor": 15}],
            ),
        )
        check(
            "aceita texto ontem",
            cartao_maquina_dia_anterior_aceito(
                {"cartao_maquina_dia_anterior": "ontem", "pedido_entrega_pendente_id": 9},
                pag,
            ),
        )
        check(
            "aceita crédito",
            cartao_maquina_dia_anterior_aceito(
                {"cartao_maquina_dia_anterior": True, "pedido_entrega_pendente_id": 9},
                [{"forma": "Cartão de crédito", "valor": 15}],
            ),
        )
        check(
            "recusa sem entrega",
            not cartao_maquina_dia_anterior_aceito(
                {"cartao_maquina_dia_anterior": True},
                pag,
            ),
        )
        _Filtro.first = lambda self: None
        check(
            "recusa entrega inexistente",
            not cartao_maquina_dia_anterior_aceito(
                {"cartao_maquina_dia_anterior": True, "pedido_entrega_pendente_id": 9},
                pag,
            ),
        )
        _Filtro.first = lambda self: _Ent()
        _Ent.criado_em = timezone.now()
        check(
            "recusa mesmo dia",
            not cartao_maquina_dia_anterior_aceito(
                {"cartao_maquina_dia_anterior": True, "pedido_entrega_pendente_id": 9},
                pag,
            ),
        )
    finally:
        eu.PedidoEntrega.objects.filter = real_filter

    wizard = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    state_js = (ROOT / "produtos/static/produtos/js/pdv_state.js").read_text(encoding="utf-8")
    caixa = (ROOT / "produtos/templates/produtos/caixa_fechar.html").read_text(encoding="utf-8")
    pagamento = (ROOT / "produtos/templates/produtos/partials/pdv/step_pagamento.html").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    mig = (ROOT / "produtos/migrations/0134_vendaagro_cartao_maquina_dia_anterior.py").read_text(encoding="utf-8")
    mig135 = (ROOT / "produtos/migrations/0135_vendaagro_cartao_maquina_dia.py").read_text(encoding="utf-8")
    check("pdv pergunta o dia", "O cartão ou o Pix passou em qual dia?" in pagamento)
    check("botão passou hoje", 'id="pdv-cartao-dia-hoje"' in pagamento)
    check("botão passou ontem", 'id="pdv-cartao-dia-ontem"' in pagamento)
    check("botão outro dia", 'id="pdv-cartao-dia-outro"' in pagamento)
    check("calendário novo", "AgroDatePicker.calOpen" in wizard and "agro-date-picker" in pagamento)
    check("payload ontem ou outro dia", "escCartao === 'ontem' || escCartao === 'outro'" in wizard)
    check("trava sem escolha", wizard.count("entregaPrecisaEscolhaCartaoDia() && !entregaCartaoDiaEscolha()") >= 2)
    check("retomar manda o dia", "criadoEm: ent.criado_em" in wizard)
    check("state guarda lancadaEm", "lancadaEm:" in state_js)
    check("state guarda a data", "cartaoMaquinaData:" in state_js)
    check("fechar caixa faixa", "cf-aviso-cartao-entrega-ontem" in caixa)
    check("fechar atualiza sozinho", "aplicarAvisoCartaoEntregaOntem" in caixa)
    check("grava na venda", "cartao_maquina_dia_anterior=cartao_ontem" in views)
    check("grava o dia escolhido", "cartao_maquina_dia=cartao_dia" in views)
    check("migrate 0134", "cartao_maquina_dia_anterior" in mig and "0133_pedido_entrega_data_prevista" in mig)
    check("migrate 0135", "cartao_maquina_dia" in mig135 and "0134_vendaagro_cartao_maquina_dia_anterior" in mig135)

    print(f"\n{len(oks)} ok, {len(fails)} falha(s)")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
