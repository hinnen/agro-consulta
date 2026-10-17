# -*- coding: utf-8 -*-
"""PDV orçamento: grava no Postgres por cliente; lista filtrada; sync multi-PC."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def check_static() -> None:
    views = read("produtos/views.py")
    wiz = read("produtos/static/produtos/js/pdv_wizard.js")
    cons = read("produtos/static/produtos/js/consulta_produtos.js")
    chunk = views[views.find("def api_pdv_orcamentos") : views.find("def api_pdv_orcamento_detalhe")]
    check("@login_required" not in chunk, "API orcamentos sem login_required")
    before = views[views.find("@require_GET\ndef api_pdv_orcamento_detalhe") - 80 : views.find("def api_pdv_orcamento_detalhe")]
    check("@login_required" not in before, "detalhe orcamento sem login_required")
    check("@ensure_csrf_cookie\ndef _render_pdv_operacional" in views.replace("\r\n", "\n"), "PDV seta cookie CSRF")
    check("cloneOrcamentoItens" in wiz, "clone seguro itens")
    check("parseJsonRespostaOrcamento" in wiz, "POST nao quebra em HTML login")
    check("fromWhatsapp: true, silent: true" in wiz and "pSave.then" in wiz, "WhatsApp espera gravar")
    check("origem: 'whatsapp'" in cons or 'origem: "whatsapp"' in cons, "consulta Zap marca origem")
    check("return postOrcamentoPdvServidor(novo)" in cons, "consulta POST retorna promise")
    check("pSave.then(abrirZap)" in cons, "consulta Zap espera gravar")
    check("await salvarHistoricoLocal" in cons, "consulta botao espera gravar")
    check("syncHistoricoOrcamentosCliente" in wiz, "PDV baixa orcamentos por cliente")
    check("syncHistoricoOrcamentosRecentes" not in wiz, "nao mistura recentes da loja no UI")
    check("__pdvOrcamentosRecentesBoot" not in wiz, "boot nao usa recentes misturados")
    check("?recentes=1" not in wiz, "wizard nao chama recentes=1")
    check("cliente_key=' + encodeURIComponent(key)" in wiz, "GET sync manda cliente_key")
    check("clienteMode !== 'unset' && budgetKeyNow" not in wiz, "sync nao espera sair do modal")
    check(
        "clienteMode === 'unset' || state.clienteMode === 'consumidor_final'" in wiz,
        "unset e consumidor = mesma pasta",
    )
    idx_save = wiz.find("function salvarOrcamentoWizard")
    save = wiz[idx_save : idx_save + 4500]
    check(idx_save > 0 and "cliente_key: key" in save, "salvar grava cliente_key do estado")
    check("budgetClienteKeyFromState(state)" in save, "salvar usa pasta do cliente da tela")
    check("setConsumidorFinal" in save, "salvar com modal aberto vira consumidor")
    check(
        "syncHistoricoOrcamentosCliente(key, { silent: true })" in save
        and "doneFeedback()" in save,
        "apos OK repuxa servidor antes do verde",
    )
    check(
        "PDV sem URL de orçamento" in save or "PDV sem URL de orcamento" in save,
        "sem URL nao mente verde",
    )
    check("_orcamentosMem" in wiz, "lista tambem na memoria do PDV")
    check("sortHistoricoOrcamentosPorId" in wiz, "card ordena por id novo primeiro")
    idx_fmt = wiz.find("function formatBudgetCardDate")
    fmt = wiz[idx_fmt : idx_fmt + 700]
    check(
        idx_fmt > 0
        and fmt.find("raw.match") < fmt.find("new Date(raw)")
        and fmt.find("raw.match") > 0,
        "data BR antes do Date US",
    )
    idx_snip = wiz.find("function renderRecentBudgetsSnippet")
    snip = wiz[idx_snip : idx_snip + 700]
    check(idx_snip > 0 and "filterHistoricoPorCliente" in snip, "card lateral filtra pelo cliente")
    check("sortHistoricoOrcamentosPorId" in snip, "card lateral ordena id")
    idx_hist = wiz.find("function openBudgetHistory")
    hist = wiz[idx_hist : idx_hist + 4500]
    check(idx_hist > 0 and "filterHistoricoPorCliente" in hist, "F6 lista filtra pelo cliente")
    check("syncHistoricoOrcamentosCliente(key, { silent: true })" in hist, "F6 sincroniza o cliente da tela")
    check("event.code === 'F6'" in wiz and "openBudgetHistory();" in wiz, "F6 abre lista")
    # Boot sync: muda a chave e chama sync por cliente (nao so render).
    boot_mark = "// Sync online do cliente da tela"
    idx_boot = wiz.find(boot_mark)
    boot = wiz[idx_boot : idx_boot + 350] if idx_boot >= 0 else ""
    check(idx_boot > 0 and "syncHistoricoOrcamentosCliente(budgetKeyNow)" in boot, "boot sync por cliente da tela")
    check(idx_boot > 0 and "clienteMode !== 'unset'" not in boot, "boot sync mesmo com modal unset")
    html_wiz = read("produtos/templates/produtos/pdv_wizard.html")
    check('name="csrfmiddlewaretoken"' in html_wiz, "PDV HTML tem CSRF")
    check("deste cliente" in html_wiz.lower(), "ajuda F6 fala deste cliente")
    check("X-CSRFToken" in wiz and "pdvCsrfTokenOrcamentos" in wiz, "wizard manda CSRF")
    check("X-CSRFToken" in cons and "gmCsrfTokenParaFetch" in cons, "consulta manda CSRF")
    check("cliente_key:" in cons, "consulta tambem grava cliente_key")
    pdv = read("pdv/views.py")
    check("@ensure_csrf_cookie\ndef pdv_home" in pdv.replace("\r\n", "\n"), "wizard PDV seta cookie CSRF")
    check('OrcamentoPdvAgro.objects.filter(cliente_key=key)' in chunk, "API lista filtra por cliente_key")
    check('escopo": "cliente"' in chunk, "API responde escopo cliente")


def _ids(resp) -> list[int]:
    try:
        return [int(x.get("id") or x.get("orc_local_id") or 0) for x in (resp.json().get("items") or [])]
    except Exception:
        return []


def check_runtime() -> None:
    import django

    django.setup()
    from django.conf import settings
    from django.test import Client, override_settings
    from django.urls import reverse

    from produtos.models import OrcamentoPdvAgro

    hosts = list(getattr(settings, "ALLOWED_HOSTS", []) or [])
    if "testserver" not in hosts:
        hosts = hosts + ["testserver", "localhost", "127.0.0.1"]

    oid_renan = 1999990000001
    oid_cf = 1999990000002
    oid_csrf = 1999990000008
    keys = [oid_renan, oid_cf, oid_csrf]
    OrcamentoPdvAgro.objects.filter(orc_local_id__in=keys).delete()

    with override_settings(ALLOWED_HOSTS=hosts):
        c = Client(enforce_csrf_checks=False)
        url = reverse("api_pdv_orcamentos")

        body_renan = {
            "entry": {
                "id": oid_renan,
                "cliente": "Renan Hinnen 1403",
                "cliente_key": "pk:1403",
                "cliente_mode": "cliente",
                "total": "R$ 10,00",
                "itens": [{"id": "x", "nome": "Item", "qtd": 1, "preco": 10}],
                "origem": "manual",
            }
        }
        r1 = c.post(url, data=json.dumps(body_renan), content_type="application/json")
        check(r1.status_code == 200, f"POST Renan sem login ({r1.status_code})")
        try:
            j1 = r1.json()
        except Exception:
            j1 = {}
        check(j1.get("ok") is True and j1.get("item"), f"POST Renan grava item ({j1})")
        obj = OrcamentoPdvAgro.objects.filter(orc_local_id=oid_renan).first()
        check(obj is not None, "Postgres tem orcamento Renan")
        if obj:
            check(obj.cliente_key == "pk:1403", "cliente_key Renan gravado")
            check(obj.cliente_nome == "Renan Hinnen 1403", "nome Renan gravado")

        body_cf = {
            "entry": {
                "id": oid_cf,
                "cliente": "Consumidor nao identificado",
                "cliente_key": "consumidor_final",
                "cliente_mode": "consumidor_final",
                "total": "R$ 5,00",
                "itens": [{"id": "y", "nome": "Item CF", "qtd": 1, "preco": 5}],
                "origem": "manual",
            }
        }
        r_cf = c.post(url, data=json.dumps(body_cf), content_type="application/json")
        check(r_cf.status_code == 200, f"POST consumidor ({r_cf.status_code})")

        r_renan = c.get(url + "?cliente_key=" + "pk%3A1403")
        check(r_renan.status_code == 200, f"GET por cliente Renan ({r_renan.status_code})")
        ids_renan = _ids(r_renan)
        check(oid_renan in ids_renan, "lista Renan inclui o gravado")
        check(oid_cf not in ids_renan, "lista Renan nao mistura consumidor")

        r_consu = c.get(url + "?cliente_key=consumidor_final")
        ids_cf = _ids(r_consu)
        check(oid_cf in ids_cf, "lista consumidor inclui o gravado")
        check(oid_renan not in ids_cf, "lista consumidor nao mistura Renan")

        r2 = c.get(reverse("api_pdv_orcamento_detalhe", args=[oid_renan]))
        check(r2.status_code == 200, f"GET detalhe sem login ({r2.status_code})")
        r400 = c.post(
            url,
            data=json.dumps({"entry": {"id": oid_renan + 99, "itens": []}}),
            content_type="application/json",
        )
        check(r400.status_code == 400, f"POST sem itens = 400 ({r400.status_code})")
        r_key = c.get(url)
        check(r_key.status_code == 400, f"GET sem cliente_key = 400 ({r_key.status_code})")

        c_page = Client(enforce_csrf_checks=True)
        r_cons = c_page.get(reverse("consulta_produtos"))
        check(r_cons.status_code == 200, f"GET /consulta/ sem login ({r_cons.status_code})")
        html = r_cons.content.decode("utf-8", errors="replace")
        check("apiPdvOrcamentos" in html, "consulta HTML tem URL orcamentos")
        ck = c_page.cookies.get("csrftoken")
        tok = ck.value if ck else ""
        check(bool(tok), "cookie CSRF apos GET consulta")
        body_csrf = {
            "entry": {
                "id": oid_csrf,
                "cliente": "Cliente CSRF Orc",
                "cliente_key": "tmp:csrf orc:13988887777",
                "cliente_mode": "cliente",
                "total": "R$ 10,00",
                "itens": [{"id": "x", "nome": "Item", "qtd": 1, "preco": 10}],
                "origem": "manual",
            }
        }
        r_no = c_page.post(url, data=json.dumps(body_csrf), content_type="application/json")
        check(r_no.status_code == 403, f"POST sem token CSRF = 403 ({r_no.status_code})")
        r_okc = c_page.post(
            url,
            data=json.dumps(body_csrf),
            content_type="application/json",
            HTTP_X_CSRFTOKEN=tok,
            HTTP_REFERER="http://testserver/consulta/",
        )
        check(r_okc.status_code == 200, f"POST com CSRF cookie ({r_okc.status_code})")
        try:
            joc = r_okc.json()
        except Exception:
            joc = {}
        check(joc.get("ok") is True, "POST CSRF grava JSON ok")
        check(
            OrcamentoPdvAgro.objects.filter(orc_local_id=oid_csrf).exists(),
            "Postgres CSRF gravou",
        )
        r_wiz = c_page.get(reverse("pdv_home"))
        check(r_wiz.status_code == 200, f"GET /pdv/ sem login ({r_wiz.status_code})")
        html_pdv = r_wiz.content.decode("utf-8", errors="replace")
        check("apiPdvOrcamentos" in html_pdv, "PDV HTML tem URL orcamentos")
        check("csrfmiddlewaretoken" in html_pdv or "csrfToken" in html_pdv, "PDV HTML tem token CSRF")
        check("deste cliente" in html_pdv.lower(), "PDV HTML ajuda deste cliente")
        r_dual = c_page.get(reverse("pdv_home") + "?agro_dual=1&agro_app_role=pdv")
        check(r_dual.status_code == 200, f"GET /pdv/ agro_dual ({r_dual.status_code})")

        # Outro aparelho: mesma pasta do cliente vê o orçamento.
        c_outro = Client(enforce_csrf_checks=False)
        ids_pc2 = _ids(c_outro.get(url + "?cliente_key=" + "pk%3A1403"))
        check(oid_renan in ids_pc2, "outro PC com Renan ve o orcamento")
        ids_pc2_cf = _ids(c_outro.get(url + "?cliente_key=consumidor_final"))
        check(oid_cf in ids_pc2_cf, "outro PC com consumidor ve o dele")
        check(oid_renan not in ids_pc2_cf, "outro PC consumidor nao ve Renan")

        # Troca de PC + troca de cliente: Renan some quando abre outro.
        r_outro_cli = c_outro.get(url + "?cliente_key=id:999001")
        check(r_outro_cli.status_code == 200, f"GET outro cliente ({r_outro_cli.status_code})")
        check(oid_renan not in _ids(r_outro_cli), "outro cliente nao ve orcamento Renan")

        # id: estilo (sem pk) tambem isola.
        oid_id = 1999990000009
        OrcamentoPdvAgro.objects.filter(orc_local_id=oid_id).delete()
        body_id = {
            "entry": {
                "id": oid_id,
                "cliente": "Cliente ID Style",
                "cliente_key": "id:555",
                "cliente_mode": "cliente",
                "total": "R$ 3,00",
                "itens": [{"id": "z", "nome": "Z", "qtd": 1, "preco": 3}],
                "origem": "manual",
            }
        }
        r_id = c.post(url, data=json.dumps(body_id), content_type="application/json")
        check(r_id.status_code == 200, f"POST chave id: ({r_id.status_code})")
        check(oid_id in _ids(c.get(url + "?cliente_key=id%3A555")), "GET id:555 acha o seu")
        check(oid_id not in _ids(c.get(url + "?cliente_key=" + "pk%3A1403")), "pk:1403 nao puxa id:555")
        OrcamentoPdvAgro.objects.filter(orc_local_id=oid_id).delete()

        OrcamentoPdvAgro.objects.filter(orc_local_id__in=keys).delete()


def main() -> int:
    print("=== PDV-ORC-POR-CLIENTE ===")
    check_static()
    try:
        check_runtime()
    except Exception as e:
        fail(f"runtime: {e}")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    print("VERIFY_OK" if not FAILS else "VERIFY_FAIL")
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
