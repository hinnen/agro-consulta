"""
Prova — loja da compra no fiado (CENTRO / VILA).

Path:
  FiadoTituloAgro.deposito gravado na criação (venda PDV)
  → backfill migrate 0139 a partir de VendaAgro.deposito
  → /fiado/ filtro Todas|Centro|Vila|Sem loja
  → coluna Loja entre Situação e Baixa (sem botão Ver)
  → Editar marca/corrige loja (incl. legado)
  → cupom fiado (via cliente + via loja) mostra LOJA: CENTRO|VILA

  python scripts/verify_fiado_loja_compra_path.py
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client
from django.utils import timezone

from produtos.fiado_gestao_util import (
    criar_titulos_de_venda,
    editar_titulo_fiado,
    listar_clientes_fiado,
    listar_titulos,
    normalizar_deposito_fiado,
    rotulo_deposito_fiado,
    titulo_para_dict,
)
from produtos.models import ClienteAgro, FiadoTituloAgro, VendaAgro
from produtos.venda_cupom_util import serializar_venda_cupom_80mm

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Arquivos / contratos ==")
    html = _read("produtos/templates/produtos/fiado_gestao.html")
    js = _read("produtos/static/produtos/js/fiado_gestao.js")
    util = _read("produtos/fiado_gestao_util.py")
    models = _read("produtos/models.py")
    mig = _read("produtos/migrations/0139_fiadotituloagro_deposito.py")
    cupom_js = _read("produtos/static/produtos/js/venda_cupom_80mm.js")
    cupom_py = _read("produtos/venda_cupom_util.py")
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")

    check("campo_deposito_modelo", 'name="deposito"' in models or "deposito = models.CharField" in models)
    check("migrate_0139", "fiadotituloagro" in mig.lower() and "backfill_deposito_fiado" in mig)
    check("normalizar_util", "def normalizar_deposito_fiado" in util and "def rotulo_deposito_fiado" in util)
    check("criar_grava_deposito", 'deposito=dep' in util and "normalizar_deposito_fiado" in util)
    check("listar_filtro_deposito", "deposito: str = \"\"" in util and "_filtrar_qs_deposito" in util)
    check("editar_aceita_deposito", "deposito: str | None = None" in util)
    check("sem_botao_ver", "fiado-btn-ver-tit" not in js)
    check("coluna_loja", ">Loja<" in html)
    check("filtro_loja_ui", 'id="fiado-filtro-loja"' in html and "data-loja=\"centro\"" in html)
    check("editar_select_loja", 'id="fiado-editar-loja"' in html)
    check("chip_js", "lojaChipHtml" in js and "filtroLojaAtual" in js)
    check("cupom_loja_py", '"loja_label"' in cupom_py and '"deposito"' in cupom_py)
    check("cupom_loja_js", "LOJA:" in cupom_js and "loja_label" in cupom_js)
    check("vias_fiado", "VIA DO CLIENTE" in cupom_js and "VIA DA LOJA" in cupom_js)
    check("cupom_saldo_fiado_js", "SALDO FIADO" in cupom_js and "resolverFiadoMistoCupom" in cupom_js)
    check("cupom_fiado_misto_py", "def _fiado_misto_cupom_campos" in cupom_py and '"fiado_misto"' in cupom_py)
    check("cupom_ja_pago_js", "Já pago:" in cupom_js and "total-linha-sec" in cupom_js)
    check("wizard_payload_misto", "fiado_misto:" in wizard and "ja_pago_texto:" in wizard)


def test_logica() -> None:
    print("== Lógica / API ==")
    check("norm_centro", normalizar_deposito_fiado("Centro") == "centro")
    check("norm_vila", normalizar_deposito_fiado("vila elias") == "vila")
    check("norm_vazio", normalizar_deposito_fiado("") == "")
    check("rotulo_centro", rotulo_deposito_fiado("centro") == "CENTRO")
    check("rotulo_vazio", rotulo_deposito_fiado("") == "—")

    suf = timezone.now().strftime("%H%M%S%f")
    cli = ClienteAgro.objects.create(
        nome=f"Fiado Loja Teste {suf}",
        ativo=True,
        limite_fiado_local=Decimal("5000"),
    )
    venda_c = VendaAgro.objects.create(
        cliente_nome=cli.nome,
        cliente_id_erp=f"agro:{cli.pk}",
        total=Decimal("100.00"),
        forma_pagamento="Fiado",
        pagamentos_json=[
            {
                "forma": "Fiado",
                "valor": 100,
                "fiado_parcelas": 1,
                "fiado_dias_primeiro": 30,
            }
        ],
        deposito="centro",
        usuario_registro="prova-fiado-loja",
    )
    venda_v = VendaAgro.objects.create(
        cliente_nome=cli.nome,
        cliente_id_erp=f"agro:{cli.pk}",
        total=Decimal("50.00"),
        forma_pagamento="Fiado",
        pagamentos_json=[
            {
                "forma": "Fiado",
                "valor": 50,
                "fiado_parcelas": 1,
                "fiado_dias_primeiro": 30,
            }
        ],
        deposito="vila",
        usuario_registro="prova-fiado-loja",
    )
    tit_c = criar_titulos_de_venda(venda_c, usuario="prova")
    tit_v = criar_titulos_de_venda(venda_v, usuario="prova")
    check("criou_centro", bool(tit_c) and tit_c[0].deposito == "centro", str(getattr(tit_c[0], "deposito", None) if tit_c else None))
    check("criou_vila", bool(tit_v) and tit_v[0].deposito == "vila")

    d_c = titulo_para_dict(tit_c[0])
    check("dict_loja_label", d_c.get("loja_label") == "CENTRO" and d_c.get("deposito") == "centro")

    legado = FiadoTituloAgro.objects.create(
        chave_unica=f"prova-legado-loja-{suf}",
        cliente_agro=cli,
        cliente_nome=cli.nome,
        numero_documento="LEGADO",
        parcela_num=1,
        parcela_total=1,
        vencimento=date.today() + timedelta(days=10),
        valor_bruto=Decimal("20.00"),
        valor_pago=Decimal("0"),
        situacao=FiadoTituloAgro.Situacao.ABERTO,
        origem=FiadoTituloAgro.Origem.IMPORTACAO,
        deposito="",
    )
    check("legado_vazio", legado.deposito == "")

    clientes_todas = listar_clientes_fiado(busca=cli.nome, apenas_com_saldo=True, deposito="")
    clientes_centro = listar_clientes_fiado(busca=cli.nome, apenas_com_saldo=True, deposito="centro")
    clientes_vila = listar_clientes_fiado(busca=cli.nome, apenas_com_saldo=True, deposito="vila")
    clientes_sem = listar_clientes_fiado(busca=cli.nome, apenas_com_saldo=True, deposito="sem")
    check("filtro_clientes_centro", any(c.get("cliente_agro_pk") == cli.pk for c in clientes_centro))
    check("filtro_clientes_vila", any(c.get("cliente_agro_pk") == cli.pk for c in clientes_vila))
    check("filtro_clientes_sem", any(c.get("cliente_agro_pk") == cli.pk for c in clientes_sem))

    g_todas = next((c for c in clientes_todas if c.get("cliente_agro_pk") == cli.pk), None)
    g_centro = next((c for c in clientes_centro if c.get("cliente_agro_pk") == cli.pk), None)
    g_vila = next((c for c in clientes_vila if c.get("cliente_agro_pk") == cli.pk), None)
    check(
        "dual_todas_soma",
        bool(g_todas)
        and abs(float(g_todas.get("saldo_aberto") or 0) - 170.0) < 0.01
        and int(g_todas.get("titulos_abertos") or 0) >= 3,
        str(g_todas),
    )
    check(
        "dual_centro_so_100",
        bool(g_centro)
        and abs(float(g_centro.get("saldo_aberto") or 0) - 100.0) < 0.01
        and int(g_centro.get("titulos_abertos") or 0) == 1,
        str(g_centro),
    )
    check(
        "dual_vila_so_50",
        bool(g_vila)
        and abs(float(g_vila.get("saldo_aberto") or 0) - 50.0) < 0.01
        and int(g_vila.get("titulos_abertos") or 0) == 1,
        str(g_vila),
    )

    t_centro = listar_titulos(cliente_agro_pk=cli.pk, deposito="centro")
    t_vila = listar_titulos(cliente_agro_pk=cli.pk, deposito="vila")
    t_sem = listar_titulos(cliente_agro_pk=cli.pk, deposito="sem")
    check("filtro_titulos_centro", all(x.get("deposito") == "centro" for x in t_centro) and len(t_centro) >= 1)
    check("filtro_titulos_vila", all(x.get("deposito") == "vila" for x in t_vila) and len(t_vila) >= 1)
    check("filtro_titulos_sem", all(not x.get("deposito") for x in t_sem) and any(x.get("id") == legado.pk for x in t_sem))

    editado = editar_titulo_fiado(legado.pk, deposito="vila", usuario="prova")
    check("editar_legado_vila", editado.deposito == "vila")
    clientes_sem_depois = listar_clientes_fiado(busca=cli.nome, apenas_com_saldo=True, deposito="sem")
    clientes_vila_depois = listar_clientes_fiado(busca=cli.nome, apenas_com_saldo=True, deposito="vila")
    g_vila_d = next((c for c in clientes_vila_depois if c.get("cliente_agro_pk") == cli.pk), None)
    check(
        "legado_sai_sem_entra_vila",
        not any(c.get("cliente_agro_pk") == cli.pk for c in clientes_sem_depois)
        and bool(g_vila_d)
        and abs(float(g_vila_d.get("saldo_aberto") or 0) - 70.0) < 0.01,
        str(g_vila_d),
    )

    cupom = serializar_venda_cupom_80mm(venda_v)
    check("cupom_eh_fiado", bool(cupom.get("eh_fiado")))
    check("cupom_loja_vila", cupom.get("loja_label") == "VILA" and cupom.get("deposito") == "vila")
    check("cupom_so_fiado_nao_misto", cupom.get("fiado_misto") is False)

    venda_mista = VendaAgro.objects.create(
        cliente_nome=cli.nome,
        cliente_id_erp=f"agro:{cli.pk}",
        total=Decimal("101.00"),
        forma_pagamento="Dinheiro + Fiado",
        pagamentos_json=[
            {"forma": "Dinheiro", "valor": 44},
            {
                "forma": "Fiado",
                "valor": 57,
                "fiado_parcelas": 1,
                "fiado_dias_primeiro": 30,
            },
        ],
        deposito="centro",
        usuario_registro="prova-fiado-misto",
    )
    cupom_m = serializar_venda_cupom_80mm(venda_mista)
    check("cupom_misto_flag", cupom_m.get("fiado_misto") is True)
    check(
        "cupom_misto_valor_fiado",
        abs(float(cupom_m.get("valor_fiado") or 0) - 57.0) < 0.01,
        str(cupom_m.get("valor_fiado")),
    )
    check(
        "cupom_misto_ja_pago",
        "Dinheiro" in str(cupom_m.get("ja_pago_texto") or "") and "44" in str(cupom_m.get("ja_pago_texto") or ""),
        str(cupom_m.get("ja_pago_texto")),
    )
    check("cupom_misto_texto_fiado", "57" in str(cupom_m.get("valor_fiado_texto") or ""))

    User = get_user_model()
    user, _ = User.objects.get_or_create(username=f"prova_fiado_loja_{suf}", defaults={"is_staff": True})
    user.set_password("x")
    user.save()
    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(user)
    r = c.get("/api/fiado/clientes/", {"q": cli.nome, "deposito": "centro", "apenas_saldo": "1"})
    check("api_clientes_filtro", r.status_code == 200 and r.json().get("ok") is True)
    r2 = c.get("/api/fiado/titulos/", {"cliente_agro_pk": cli.pk, "deposito": "vila"})
    check("api_titulos_filtro", r2.status_code == 200 and all(t.get("deposito") == "vila" for t in (r2.json().get("titulos") or [])))
    r3 = c.post(
        "/api/fiado/titulo-editar/",
        data='{"titulo_id": %d, "deposito": "centro"}' % legado.pk,
        content_type="application/json",
    )
    check("api_editar_loja", r3.status_code == 200 and (r3.json().get("titulo") or {}).get("deposito") == "centro")

    # limpeza
    FiadoTituloAgro.objects.filter(cliente_agro=cli).delete()
    venda_c.delete()
    venda_v.delete()
    cli.delete()


def main() -> int:
    print("FIADO-LOJA-COMPRA — prova path")
    test_arquivos()
    test_logica()
    print()
    print(f"OK {len(oks)} · FAIL {len(fails)}")
    if fails:
        print("Falhas:")
        for f in fails:
            print(" -", f)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
