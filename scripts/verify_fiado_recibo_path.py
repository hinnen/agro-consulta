"""
Prova detalhada FL-019 — recibo pagamento fiado 80mm (FIADO-RECIBO).

Path:
  /fiado/ Baixa → PDV cobrança → api/fiado/baixa-pdv/ grava recibo_id
    → finalizeFiadoCobrancaOk → agroEscolherImprimirReciboFiado
    → GET /api/fiado/recibo/<id>/ → venda_cupom_80mm (80mm)
  /fiado/ ficha → GET /api/fiado/recibos/ → Reimprimir (2ª via)
  MP Point também cai em finalizeFiadoCobrancaOk.
  Sem migrate. Fora: NFC-e baixa (FL-052) · vale crédito (FL-058).

  python scripts/verify_fiado_recibo_path.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" - {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" - {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Path arquivos ==")
    urls = _read("produtos/urls.py")
    views = _read("produtos/fiado_gestao_views.py")
    util_g = _read("produtos/fiado_gestao_util.py")
    util_r = _read("produtos/fiado_recibo_util.py")
    cupom = _read("produtos/static/produtos/js/venda_cupom_80mm.js")
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")
    fiado_js = _read("produtos/static/produtos/js/fiado_gestao.js")
    fiado_html = _read("produtos/templates/produtos/fiado_gestao.html")
    pdv_views = _read("pdv/views.py")

    check("url_recibos", "api/fiado/recibos/" in urls and "name='api_fiado_recibos'" in urls)
    check("url_recibo_id", "api/fiado/recibo/<int:recibo_id>/" in urls)
    check("url_recibo_baixas", "name='api_fiado_recibo_baixas'" in urls)
    check("view_get", "@require_GET" in views and "def api_fiado_recibo" in views and "def api_fiado_recibos" in views)
    check("view_login", views.count("@login_required") >= 3)
    check("util_montar", "def montar_recibo_pagamento_fiado" in util_r)
    check("util_listar", "def listar_recibos_pagamento_fiado" in util_r)
    check("util_listar_pk_primeiro", "cliente_agro_id=int(cliente_agro_pk)" in util_r)
    check("baixa_recibo_id", '"recibo_id"' in util_g and "saldo_restante" in util_g)
    check("baixa_formas_snap", '"formas"' in util_g and '"recibo": True' in util_g)
    check("pdv_boot_urls", '"apiFiadoRecibo"' in pdv_views and '"apiFiadoRecibos"' in pdv_views)
    check("pdv_boot_baixa", '"apiFiadoBaixaPdv"' in pdv_views)

    check("cupom_tipo_primeiro", "tipo === 'recibo_fiado'" in cupom.split("function buildCupomInnerHtml")[1][:400])
    check("cupom_titulo", "RECIBO DE PAGAMENTO FIADO" in cupom)
    check("cupom_pago", "PAGO" in cupom and "Ainda deve" in cupom and "QUITADO" in cupom)
    check("cupom_assinatura", "Assinatura do cliente" in cupom)
    check("cupom_2via", "2ª VIA" in cupom or "2a VIA" in cupom)
    check("cupom_fn_print", "function agroImprimirReciboFiado80mm" in cupom)
    check("cupom_fn_escolher", "function agroEscolherImprimirReciboFiado" in cupom)
    check("cupom_fn_carregar", "function agroCarregarEImprimirReciboFiado" in cupom)
    check("cupom_global", "global.agroEscolherImprimirReciboFiado" in cupom)
    check("cupom_url_api", "/api/fiado/recibo/" in cupom)
    check("cupom_venda_intacta", "function agroImprimirCupomVenda80mm" in cupom and "Não há itens para imprimir nesta venda." in cupom)
    check("cupom_fiado_venda_2vias", "VIA DO CLIENTE" in cupom and "VIA DA LOJA" in cupom)

    check("wizard_perguntar", "agroEscolherImprimirReciboFiado" in wizard)
    check("wizard_recibo_id", "recibo_id" in wizard and "finalizeFiadoCobrancaOk" in wizard)
    check("wizard_mp_ok", wizard.count("finalizeFiadoCobrancaOk(") >= 3)
    check("wizard_nao_auto_print", "Agora não" not in wizard or "agroEscolherImprimirReciboFiado" in wizard)

    check("fiado_html_box", 'id="fiado-recibos-box"' in fiado_html)
    check("fiado_html_cupom_js", "venda_cupom_80mm.js" in fiado_html)
    check("fiado_html_urls", "api_fiado_recibos" in fiado_html and "api_fiado_recibo_baixas" in fiado_html)
    check("fiado_js_load", "function carregarRecibosCliente" in fiado_js)
    check("fiado_js_print", "agroCarregarEImprimirReciboFiado" in fiado_js)
    check("fiado_js_segunda_via", "segunda_via: true" in fiado_js)
    check("fiado_js_reimprimir", "fiado-btn-reimprimir" in fiado_js)


def test_js_syntax() -> None:
    print("== JS syntax ==")
    files = [
        "produtos/static/produtos/js/venda_cupom_80mm.js",
        "produtos/static/produtos/js/pdv_wizard.js",
        "produtos/static/produtos/js/fiado_gestao.js",
    ]
    for rel in files:
        r = subprocess.run(["node", "--check", str(ROOT / rel)], capture_output=True, text=True)
        check("node_" + Path(rel).name, r.returncode == 0, (r.stderr or "").strip()[:120])


def test_django_urls() -> None:
    print("== Django urls / check ==")
    from django.urls import reverse

    check("reverse_recibos", reverse("api_fiado_recibos") == "/api/fiado/recibos/")
    check("reverse_recibo", reverse("api_fiado_recibo", args=[7]) == "/api/fiado/recibo/7/")
    check("reverse_recibo_q", reverse("api_fiado_recibo_baixas") == "/api/fiado/recibo/")
    check("reverse_baixa_pdv", reverse("api_fiado_baixa_pdv") == "/api/fiado/baixa-pdv/")
    check("reverse_fiado_tela", reverse("fiado_gestao") == "/fiado/")

    r = subprocess.run([sys.executable, "manage.py", "check"], cwd=str(ROOT), capture_output=True, text=True)
    check("manage_check", r.returncode == 0, (r.stdout or r.stderr or "")[-160:])


def test_runtime_recibo() -> None:
    print("== Runtime PG/SQLite (cria e apaga) ==")
    from django.contrib.auth import get_user_model
    from django.test import Client

    from produtos.fiado_recibo_util import (
        listar_recibos_pagamento_fiado,
        montar_recibo_pagamento_fiado,
        saldo_aberto_cliente_fiado,
    )
    from produtos.models import ClienteAgro, FiadoBaixaAgro, FiadoEventoAgro, FiadoTituloAgro

    raised = False
    try:
        montar_recibo_pagamento_fiado(recibo_id=None, baixas_ids=[])
    except ValueError:
        raised = True
    check("montar_vazio_erro", raised)

    missing = False
    try:
        montar_recibo_pagamento_fiado(recibo_id=999999991)
    except ValueError:
        missing = True
    check("montar_id_inexistente", missing)

    tag = "FL019-VERIFY"
    cli = None
    tit = None
    baixa = None
    ev = None
    ev_idem = None
    user = None
    try:
        cli = ClienteAgro.objects.create(nome=f"{tag} Cliente Recibo")
        tit = FiadoTituloAgro.objects.create(
            chave_unica=f"verify:fl019:{cli.pk}",
            cliente_agro=cli,
            cliente_nome=cli.nome,
            numero_documento="FL019-T1",
            parcela_num=1,
            parcela_total=2,
            vencimento=date.today() + timedelta(days=10),
            valor_bruto=Decimal("100.00"),
            valor_pago=Decimal("30.00"),
            situacao=FiadoTituloAgro.Situacao.PARCIAL,
            origem=FiadoTituloAgro.Origem.PDV,
        )
        baixa = FiadoBaixaAgro.objects.create(
            titulo=tit,
            valor=Decimal("30.00"),
            forma_pagamento="Dinheiro",
            usuario="verify-fl019",
            observacao=tag,
        )
        ev = FiadoEventoAgro.objects.create(
            tipo=FiadoEventoAgro.Tipo.BAIXA,
            cliente_agro=cli,
            titulo=tit,
            baixa=baixa,
            usuario="verify-fl019",
            payload_json={
                "origem": "pdv",
                "recibo": True,
                "valor_aplicado": 30.0,
                "saldo_restante": 70.0,
                "parcial": True,
                "baixas_ids": [baixa.pk],
                "formas": [{"forma": "Dinheiro", "valor": 30.0}],
                "cliente_nome": cli.nome,
                "cliente_agro_pk": cli.pk,
            },
        )
        ev_idem = FiadoEventoAgro.objects.create(
            tipo=FiadoEventoAgro.Tipo.BAIXA,
            cliente_agro=cli,
            titulo=tit,
            usuario="verify-fl019",
            payload_json={
                "origem": "pdv_idempotencia",
                "client_request_id": f"verify-{cli.pk}",
                "resultado": {"recibo_id": ev.pk, "baixas_ids": [baixa.pk], "valor_aplicado": 30.0},
            },
        )

        saldo = saldo_aberto_cliente_fiado(cliente_agro_pk=cli.pk)
        check("saldo_cliente_70", saldo == Decimal("70.00"), str(saldo))

        cupom = montar_recibo_pagamento_fiado(recibo_id=ev.pk)
        check("cupom_tipo", cupom.get("tipo") == "recibo_fiado")
        check("cupom_subtitulo", "PAGAMENTO FIADO" in str(cupom.get("subtitulo") or ""))
        check("cupom_cliente", cupom.get("cliente_nome") == cli.nome)
        check("cupom_valor_30", abs(float(cupom.get("valor_pago") or 0) - 30.0) < 0.001)
        check("cupom_texto_rs", str(cupom.get("valor_pago_texto") or "").startswith("R$"))
        check("cupom_saldo_snap_70", abs(float(cupom.get("saldo_restante") or 0) - 70.0) < 0.001, str(cupom.get("saldo_restante")))
        check("cupom_nao_quitou", cupom.get("quitou") is False)
        check("cupom_parcial_forma", "Dinheiro" in str(cupom.get("forma_pagamento") or ""))
        check("cupom_recibo_id", cupom.get("recibo_id") == ev.pk)
        check("cupom_itens", isinstance(cupom.get("itens"), list) and len(cupom["itens"]) == 1)
        check("cupom_parcela", "1/2" in str(cupom["itens"][0].get("nome") or ""))
        check("cupom_assinatura_flag", cupom.get("com_assinatura") is True)
        check("cupom_2via_off", cupom.get("segunda_via") is False)

        cupom2 = montar_recibo_pagamento_fiado(recibo_id=ev.pk, segunda_via=True)
        check("cupom_2via_on", cupom2.get("segunda_via") is True)

        cupom_b = montar_recibo_pagamento_fiado(baixas_ids=[baixa.pk])
        check("cupom_por_baixas", abs(float(cupom_b.get("valor_pago") or 0) - 30.0) < 0.001)

        cupom_idem = montar_recibo_pagamento_fiado(recibo_id=ev_idem.pk)
        check("cupom_idem_redirect", cupom_idem.get("recibo_id") == ev.pk)

        tit.valor_pago = Decimal("100.00")
        tit.situacao = FiadoTituloAgro.Situacao.QUITADO
        tit.save(update_fields=["valor_pago", "situacao"])
        cupom_q = montar_recibo_pagamento_fiado(recibo_id=ev.pk)
        check(
            "cupom_saldo_nao_muda_depois",
            abs(float(cupom_q.get("saldo_restante") or 0) - 70.0) < 0.001,
            "snapshot 70 mesmo após quitar título",
        )

        lista = listar_recibos_pagamento_fiado(cliente_agro_pk=cli.pk)
        ids_lista = {r.get("recibo_id") for r in lista}
        check("lista_tem_evento", ev.pk in ids_lista, str(ids_lista))
        check("lista_sem_idem", ev_idem.pk not in ids_lista)
        check("lista_vazia_sem_cliente", listar_recibos_pagamento_fiado() == [])

        User = get_user_model()
        uname = f"fl019_verify_{cli.pk}"
        user = User.objects.create_user(username=uname, password="x12345")
        c = Client(HTTP_HOST="127.0.0.1")
        anon = c.get(f"/api/fiado/recibo/{ev.pk}/")
        check("api_anon_bloqueia", anon.status_code in (302, 401, 403), str(anon.status_code))
        ok_login = c.login(username=uname, password="x12345")
        check("api_login", ok_login)
        r1 = c.get(f"/api/fiado/recibo/{ev.pk}/")
        check("api_recibo_200", r1.status_code == 200, str(r1.status_code))
        body = r1.json() if r1.status_code == 200 else {}
        check("api_recibo_ok", body.get("ok") is True and (body.get("cupom") or body).get("tipo") == "recibo_fiado")
        r2a = c.get(f"/api/fiado/recibo/?baixas={baixa.pk}&segunda_via=1")
        check("api_recibo_baixas_200", r2a.status_code == 200, str(r2a.status_code))
        body2 = r2a.json() if r2a.status_code == 200 else {}
        check("api_recibo_2via", bool((body2.get("cupom") or body2).get("segunda_via")))
        r3 = c.get(f"/api/fiado/recibos/?cliente_agro_pk={cli.pk}")
        check("api_recibos_200", r3.status_code == 200, str(r3.status_code))
        recs = (r3.json() or {}).get("recibos") if r3.status_code == 200 else []
        check("api_recibos_lista", isinstance(recs, list) and any(x.get("recibo_id") == ev.pk for x in recs))
        r4 = c.get("/api/fiado/recibo/")
        check("api_recibo_sem_id_400", r4.status_code == 400, str(r4.status_code))
        tela = c.get("/fiado/")
        check("tela_fiado_200", tela.status_code == 200, str(tela.status_code))
        html = tela.content.decode("utf-8", errors="ignore") if tela.status_code == 200 else ""
        check("tela_tem_box_recibos", 'id="fiado-recibos-box"' in html)
        check("tela_tem_cupom_js", "venda_cupom_80mm.js" in html)
    finally:
        if ev_idem is not None:
            FiadoEventoAgro.objects.filter(pk=ev_idem.pk).delete()
        if ev is not None:
            FiadoEventoAgro.objects.filter(pk=ev.pk).delete()
        if baixa is not None:
            FiadoBaixaAgro.objects.filter(pk=baixa.pk).delete()
        if tit is not None:
            FiadoTituloAgro.objects.filter(pk=tit.pk).delete()
        if cli is not None:
            ClienteAgro.objects.filter(pk=cli.pk).delete()
        if user is not None:
            get_user_model().objects.filter(pk=user.pk).delete()


def main() -> int:
    test_arquivos()
    test_js_syntax()
    test_django_urls()
    test_runtime_recibo()
    print()
    print(f"OK {len(oks)}  FAIL {len(fails)}")
    if fails:
        print("Falhou:", ", ".join(fails))
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
