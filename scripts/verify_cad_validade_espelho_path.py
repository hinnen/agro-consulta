"""
Path CAD-VAL-ESPELHO — validade tela/NF aparece na aba 8 do cadastro.
Roda: python scripts/verify_cad_validade_espelho_path.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import RequestFactory

from produtos.models import (
    EstoqueLote,
    ProdutoGestaoOverlayAgro,
    garantir_estoque_lote_desde_extras,
    registrar_lote_validade_apos_entrada_nf,
    sync_overlay_extra_validade_para_lote,
)
from produtos.views import (
    _montar_produto_cadastro_detalhe,
    api_overlay_lote_adicionar,
)

PASS = 0
FAIL = 0
PREFIX = "VERIFYCADVAL"


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print(f"  OK  {msg}")


def bad(msg: str) -> None:
    global FAIL
    FAIL += 1
    print(f"  FAIL {msg}")


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        bad(msg)


def wipe() -> None:
    ProdutoGestaoOverlayAgro.objects.filter(
        produto_externo_id__startswith=PREFIX
    ).delete()


def test_heal_extras_para_lote() -> None:
    print("\n== 1 heal extras -> EstoqueLote ==")
    wipe()
    pid = f"{PREFIX}HEAL01"
    dv = (date.today() + timedelta(days=40)).isoformat()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=pid,
        nome="Heal",
        cadastro_extras={"validade": dv, "lote": "L-HEAL"},
    )
    check(EstoqueLote.objects.filter(overlay=ov).count() == 0, "sem lote antes")
    lotes = garantir_estoque_lote_desde_extras(ov, quantidade_atual=Decimal("3.00"))
    check(len(lotes) == 1, "criou 1 lote")
    el = lotes[0]
    check(el.lote_codigo == "L-HEAL", "codigo lote")
    check(el.data_validade.isoformat()[:10] == dv, "data validade")
    check(Decimal(el.quantidade_atual) == Decimal("3.00"), "qtd")
    lotes2 = garantir_estoque_lote_desde_extras(ov)
    check(len(lotes2) == 1 and lotes2[0].pk == el.pk, "idempotente")


def test_heal_sem_validade() -> None:
    print("\n== 2 heal sem validade ==")
    wipe()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}HEAL02",
        nome="SemVal",
        cadastro_extras={"lote": "X"},
    )
    lotes = garantir_estoque_lote_desde_extras(ov)
    check(lotes == [], "nao cria sem data")
    check(EstoqueLote.objects.filter(overlay=ov).count() == 0, "zero lotes no banco")


def test_heal_data_invalida() -> None:
    print("\n== 3 heal data invalida ==")
    wipe()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}HEAL03",
        nome="BadDate",
        cadastro_extras={"validade": "nao-e-data", "lote": "Z"},
    )
    el = sync_overlay_extra_validade_para_lote(ov)
    check(el is None, "sync extras rejeita data ruim")
    lotes = garantir_estoque_lote_desde_extras(ov)
    check(lotes == [], "garantir nao cria com data ruim")


def test_heal_nao_duplica_lote_existente() -> None:
    print("\n== 4 heal nao duplica lote existente ==")
    wipe()
    pid = f"{PREFIX}HEAL04"
    dv = (date.today() + timedelta(days=10)).isoformat()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=pid,
        nome="JaTem",
        cadastro_extras={"validade": dv, "lote": "NOVO"},
    )
    el0 = EstoqueLote.objects.create(
        overlay=ov,
        lote_codigo="JA-TEM",
        data_validade=date.today() + timedelta(days=5),
        quantidade_atual=Decimal("1.00"),
    )
    lotes = garantir_estoque_lote_desde_extras(ov, quantidade_atual=Decimal("99"))
    check(len(lotes) == 1, "mantem 1 lote")
    check(lotes[0].pk == el0.pk, "mesmo pk")
    check(lotes[0].lote_codigo == "JA-TEM", "nao troca codigo")
    check(Decimal(lotes[0].quantidade_atual) == Decimal("1.00"), "nao muda qtd")


def test_nf_atualiza_resumo() -> None:
    print("\n== 5 NF lote atualiza extras ==")
    wipe()
    pid = f"{PREFIX}NF01"
    dv = (date.today() + timedelta(days=20)).isoformat()
    info = registrar_lote_validade_apos_entrada_nf(
        pid,
        {"lote_validade": dv, "lote_numero": "NF-1"},
        Decimal("2"),
        nome_produto="NF",
        deposito="centro",
    )
    check(bool(info and info.get("lote_id")), "criou lote NF")
    ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id=pid)
    ex = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
    check(ex.get("validade") == dv, "extras.validade espelhada")
    check(str(ex.get("lote") or "") == "NF-1", "extras.lote espelhado")
    check(EstoqueLote.objects.filter(overlay=ov).count() == 1, "1 lote no cadastro")


def test_nf_sem_data() -> None:
    print("\n== 6 NF sem data etapa 4 ==")
    wipe()
    pid = f"{PREFIX}NF02"
    info = registrar_lote_validade_apos_entrada_nf(
        pid,
        {"lote_numero": "SEM-DATA"},
        Decimal("5"),
        deposito="centro",
    )
    check(info is None, "sem data nao cria")
    check(
        EstoqueLote.objects.filter(
            overlay__produto_externo_id=pid
        ).count()
        == 0,
        "sem lote no banco",
    )


def test_nf_soma_mesmo_lote() -> None:
    print("\n== 7 NF soma mesmo codigo de lote ==")
    wipe()
    pid = f"{PREFIX}NF03"
    dv = (date.today() + timedelta(days=15)).isoformat()
    registrar_lote_validade_apos_entrada_nf(
        pid,
        {"lote_validade": dv, "lote_numero": "SOMA"},
        Decimal("2"),
        deposito="centro",
    )
    registrar_lote_validade_apos_entrada_nf(
        pid,
        {"lote_validade": dv, "lote_numero": "SOMA"},
        Decimal("3"),
        deposito="centro",
    )
    ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id=pid)
    els = list(EstoqueLote.objects.filter(overlay=ov))
    check(len(els) == 1, "um so lote")
    check(Decimal(els[0].quantidade_atual) == Decimal("5.00"), "qtd 2+3=5")


def test_detalhe_cadastro_heala() -> None:
    print("\n== 8 detalhe cadastro heala extras ==")
    wipe()
    pid = f"{PREFIX}DET01"
    dv = (date.today() + timedelta(days=30)).isoformat()
    ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=pid,
        nome="Detalhe",
        cadastro_extras={"validade": dv, "lote": "DET-L"},
    )
    stub = {"Id": pid, "Nome": "Detalhe", "_id": pid}
    with patch(
        "produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro",
        return_value={pid: {"saldo_centro": 4.0, "saldo_vila": 1.0}},
    ):
        row = _montar_produto_cadastro_detalhe(None, None, stub)
    lotes = row.get("lotes") or []
    check(len(lotes) == 1, "detalhe devolve 1 lote")
    check(lotes[0].get("lote_codigo") == "DET-L", "codigo no detalhe")
    check(str(lotes[0].get("data_validade"))[:10] == dv, "data no detalhe")
    check(Decimal(str(lotes[0].get("quantidade_atual"))) == Decimal("5.00"), "qtd C+V=5")
    ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id=pid)
    check(EstoqueLote.objects.filter(overlay=ov).count() == 1, "persistiu no PG")


def test_api_lote_atualiza_resumo() -> None:
    print("\n== 9 API Salvar lote (tela Validade) ==")
    wipe()
    User = get_user_model()
    user, _ = User.objects.get_or_create(
        username=f"{PREFIX.lower()}_user",
        defaults={"is_staff": True},
    )
    pid = f"{PREFIX}API01"
    dv = (date.today() + timedelta(days=25)).isoformat()
    factory = RequestFactory()
    req = factory.post(
        "/api/overlay/lote/",
        data=json.dumps(
            {
                "produto_id": pid,
                "lote_codigo": "API-L",
                "data_validade": dv,
                "quantidade": "7.5",
                "deposito": "centro",
            }
        ),
        content_type="application/json",
    )
    req.user = user
    resp = api_overlay_lote_adicionar(req)
    body = json.loads(resp.content.decode("utf-8"))
    check(resp.status_code == 200 and body.get("ok"), "API ok")
    ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id=pid)
    ex = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
    check(ex.get("validade") == dv, "API espelhou extras.validade")
    check(str(ex.get("lote") or "") == "API-L", "API espelhou extras.lote")
    check(EstoqueLote.objects.filter(overlay=ov).count() == 1, "1 lote apos API")


def test_template_sempre_lote() -> None:
    print("\n== 10 template Validade ==")
    html = (
        ROOT / "produtos/templates/produtos/relatorios_validade.html"
    ).read_text(encoding="utf-8")
    check("Sempre grava EstoqueLote" in html, "JS sempre grava lote")
    check("data-saldo-cv" in html, "data-saldo-cv na linha")
    check("apiLote" in html, "usa apiLote")
    # ramo antigo extras-only removido do handler Salvar
    check(
        "extra_validade: v" not in html and "extra_lote: l" not in html,
        "Salvar nao manda so extras",
    )
    check("fetch(apiLote" in html, "fetch apiLote no Salvar")


def test_codigo_fonte() -> None:
    print("\n== 11 codigo fonte ==")
    src = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    models = (ROOT / "produtos/models.py").read_text(encoding="utf-8")
    check("garantir_estoque_lote_desde_extras" in src, "views importa garantir")
    check("teve_lote_ativo" in src, "relatorio lote qtd0 nao esconde extras")
    check("garantir_estoque_lote_desde_extras" in models, "helper no models")
    check("sync_overlay_validade_resumo_de_lotes(ov)" in models, "NF atualiza resumo")
    check(
        "Resumo da tela Validade" in src or "espelha na aba do cadastro" in src,
        "heal no detalhe cadastro",
    )


def test_lote_zerado_nao_bloqueia_extras_src() -> None:
    print("\n== 12 lote qtd0 + extras (cenario) ==")
    wipe()
    pid = f"{PREFIX}ZERO01"
    dv = (date.today() + timedelta(days=12)).isoformat()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=pid,
        nome="Zero",
        cadastro_extras={"validade": dv, "lote": "ZERO"},
    )
    EstoqueLote.objects.create(
        overlay=ov,
        lote_codigo="VELHO-ZERO",
        data_validade=date.today() - timedelta(days=1),
        quantidade_atual=Decimal("0"),
    )
    # garantir nao cria segundo porque ja existe lote (mesmo qtd 0)
    lotes = garantir_estoque_lote_desde_extras(ov)
    check(len(lotes) == 1, "ja tem lote zerado — nao duplica")
    # detalhe lista o lote zerado (aba cadastro ve a linha)
    stub = {"Id": pid, "Nome": "Zero", "_id": pid}
    row = _montar_produto_cadastro_detalhe(None, None, stub)
    check(len(row.get("lotes") or []) == 1, "detalhe lista lote qtd0")
    check(
        Decimal(str((row["lotes"][0]).get("quantidade_atual"))) == Decimal("0"),
        "qtd 0 visivel no cadastro",
    )


def main() -> int:
    print("VERIFY CAD-VAL-ESPELHO")
    test_heal_extras_para_lote()
    test_heal_sem_validade()
    test_heal_data_invalida()
    test_heal_nao_duplica_lote_existente()
    test_nf_atualiza_resumo()
    test_nf_sem_data()
    test_nf_soma_mesmo_lote()
    test_detalhe_cadastro_heala()
    test_api_lote_atualiza_resumo()
    test_template_sempre_lote()
    test_codigo_fonte()
    test_lote_zerado_nao_bloqueia_extras_src()
    wipe()
    total = PASS + FAIL
    print(f"\n== RESULTADO {PASS}/{total} ==")
    if FAIL:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
