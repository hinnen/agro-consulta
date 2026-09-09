# -*- coding: utf-8 -*-
"""Prova path CAIXA-ENTREGA-ADIAR + retomar no Fechar caixa.

  python scripts/verify_caixa_entrega_adiar_path.py
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.utils import timezone

from produtos.entrega_pdv_pendente_util import (
    adiar_entrega_caixa_um_dia,
    data_hoje_loja,
    filtrar_qs_por_loja,
    listar_entregas_bloqueando_fechamento_caixa,
)
from produtos.models import PedidoEntrega, SessaoCaixa

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print("  OK  " + name + ((" — " + detail) if detail else ""))
    else:
        fails.append(name)
        print("  FAIL " + name + ((" — " + detail) if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Arquivos / contratos ==")
    models = _read("produtos/models.py")
    util = _read("produtos/entrega_pdv_pendente_util.py")
    views = _read("produtos/views.py")
    urls = _read("produtos/urls.py")
    pdv_views = _read("pdv/views.py")
    js = _read("produtos/static/produtos/js/pdv_wizard.js")
    html_cf = _read("produtos/templates/produtos/caixa_fechar.html")
    html_step = _read("produtos/templates/produtos/partials/pdv/step_produtos.html")
    mig = ROOT / "produtos/migrations/0128_pedido_entrega_caixa_adiada.py"

    check("mig_0128", mig.is_file())
    check("model_adiada_para", "caixa_adiada_para" in models)
    check("util_adiar_fn", "def adiar_entrega_caixa_um_dia" in util)
    check("util_exclui_futuro", "def qs_excluindo_adiadas_futuras" in util)
    check("util_filtro_sessao", "sessao_caixa__ponto_caixa" in util)
    check("view_api", "def api_pdv_entrega_pendente_adiar_caixa" in views)
    check("url_adiar", "adiar-caixa/" in urls)
    check("pdv_url_boot", "apiPdvEntregaPendenteAdiarCaixa" in pdv_views)
    check("js_fn", "function adiarEntregaPendenteCaixa" in js)
    check("js_retomar_query", "p.get('retomar')" in js)
    check("js_btn_adiar", "pdv-entrega-adiar" in js)
    check("cf_retomar", "Retomar pagamento" in html_cf)
    check("cf_adiar_btn", "btn-cf-adiar-entrega" in html_cf)
    check("help_adiar", "Adiar 1 dia" in html_step)
    check("bloqueio_usa_qs", "qs_excluindo_adiadas_futuras" in util.split("def listar_entregas_bloqueando")[1][:1800])


def test_logica_db() -> None:
    print("== Logica Django ==")
    hoje = data_hoje_loja()
    check("hoje_date", hasattr(hoje, "year"))

    user = SessaoCaixa._meta.get_field("usuario").remote_field.model.objects.filter(is_superuser=True).first()
    if user is None:
        user = SessaoCaixa._meta.get_field("usuario").remote_field.model.objects.first()
    check("tem_user", user is not None)
    if not user:
        return

    sess = SessaoCaixa.objects.create(
        usuario=user,
        ponto_caixa="gaveta",
        valor_abertura=0,
    )
    ent = PedidoEntrega.objects.create(
        cliente_nome="Prova adiar caixa",
        aguarda_pagamento_pdv=True,
        status=PedidoEntrega.Status.PENDENTE,
        sessao_caixa=sess,
        loja_entrega="",
        pdv_wizard_state={"x": 1},
        origem="pdv",
    )
    try:
        bloq = listar_entregas_bloqueando_fechamento_caixa(sessao_ids=[sess.pk], loja="centro")
        check("bloqueia_hoje", any(r["id"] == ent.pk for r in bloq))

        qs_cen = filtrar_qs_por_loja(PedidoEntrega.objects.filter(pk=ent.pk), "centro")
        check("centro_ve_pela_sessao", qs_cen.filter(pk=ent.pk).exists())

        out, err = adiar_entrega_caixa_um_dia(ent.pk, loja="centro", pin="", quem="Renan prova")
        check("adiar_ok", out is not None and err is None, err or "")
        ent.refresh_from_db()
        check("soltou_sessao", ent.sessao_caixa_id is None)
        check("dona_centro", ent.loja_entrega == "centro")
        check("adiada_amanha", ent.caixa_adiada_para == hoje + timedelta(days=1))

        bloq2 = listar_entregas_bloqueando_fechamento_caixa(sessao_ids=[sess.pk], loja="centro")
        check("nao_bloqueia_hoje", not any(r["id"] == ent.pk for r in bloq2))

        ent.caixa_adiada_para = hoje
        ent.save(update_fields=["caixa_adiada_para", "atualizado_em"])
        bloq3 = listar_entregas_bloqueando_fechamento_caixa(sessao_ids=[sess.pk], loja="centro")
        check("bloqueia_no_dia", any(r["id"] == ent.pk for r in bloq3))

        qs_v = filtrar_qs_por_loja(PedidoEntrega.objects.filter(pk=ent.pk), "vila")
        check("vila_nao_ve_sem_dono_vila", not qs_v.filter(pk=ent.pk).exists())
    finally:
        PedidoEntrega.objects.filter(pk=ent.pk).delete()
        SessaoCaixa.objects.filter(pk=sess.pk).delete()


def main() -> None:
    print("=== CAIXA-ENTREGA-ADIAR ===")
    test_arquivos()
    try:
        test_logica_db()
    except Exception as ex:
        check("logica_db", False, str(ex)[:200])
    print("")
    print(f"VERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhas: " + ", ".join(fails))
        sys.exit(1)


if __name__ == "__main__":
    main()
