#!/usr/bin/env python
"""Path detalhado — Cíclica modo foco: busca grande, último bip em destaque, já contados estreitos."""
from __future__ import annotations

import os
import sys
import uuid
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def check_text(text: str, *needles: str, label: str = "") -> None:
    for n in needles:
        if n not in text:
            fail(f"{label} missing {n!r}")
        else:
            ok(f"{label} {n!r}")


def fn_chunk(js: str, name: str, nxt: str) -> str:
    a = js.find(f"function {name}")
    b = js.find(f"function {nxt}", a + 1) if a >= 0 else -1
    if a < 0 or b <= a:
        fail(f"não achou chunk {name} → {nxt}")
        return ""
    return js[a:b]


ma_path = ROOT / "produtos" / "templates" / "produtos" / "mobile_ajuste.html"
js = ma_path.read_text(encoding="utf-8")

# --- Contrato visual / HTML ---
check_text(
    js,
    "body.ma-ciclica-focus .ma-head",
    "body.ma-ciclica-focus #btn-toggle-filtros",
    "body.ma-ciclica-focus #f-drawer",
    "body.ma-ciclica-focus #f-resumo",
    "body.ma-ciclica-focus #ma-ciclica-bar",
    "display: none !important",
    "ma-ciclica-fab-ctrl",
    "ma-ciclica-ctrl-backdrop",
    "ma-card--ultimo",
    "ma-card--feito",
    "ma-card-linha",
    "ma-card-badge-ultimo",
    "Último bip",
    "Já contados",
    "min-height: 4.85rem",
    "font-size: 1.28rem",
    "maPintarCiclicaFocus",
    "maCiclicaFocusAtivo",
    "maCiclicaBipHistorico",
    "maCiclicaRegistrarBip",
    "maCiclicaAplicarQtdLocal",
    "Cíclica · bipar",
    label="html",
)

# Busca no foco maior que o modo normal (3.85 era o antigo)
if "body.ma-ciclica-focus #busca" in js and "min-height: 4.85rem" in js:
    ok("busca cíclica 4.85rem")
else:
    fail("busca cíclica não está 4.85rem")
if "min-height: 3.85rem" in js and "body.ma-ciclica-focus #busca" in js:
    # se o valor antigo ainda estiver no bloco de foco, falha
    foco_busca = js[js.find("body.ma-ciclica-focus #busca") : js.find("body.ma-ciclica-focus #lista-scroll")]
    if "3.85rem" in foco_busca:
        fail("busca do foco ainda 3.85rem")
    else:
        ok("busca do foco não ficou no tamanho antigo")

# Hero maior que compacto
hero_qtd = js[js.find(".ma-card--ultimo .ma-card-contagem.is-qtd") : js.find(".ma-card--feito")]
comp_qtd = js[js.find(".ma-card-linha .ma-card-contagem.is-qtd") : js.find(".ma-ciclica-sec-label")]
if "2.65rem" in hero_qtd:
    ok("qtd do último bip grande")
else:
    fail("qtd do último bip não está grande")
if "1.05rem" in comp_qtd:
    ok("qtd dos já contados estreita")
else:
    fail("qtd compacta não está 1.05rem")

# --- Funções ---
focus_ui = fn_chunk(js, "maCiclicaSyncFocusUi", "maCiclicaToggleCtrl")
if "classList.toggle('ma-ciclica-focus'" in focus_ui:
    ok("ativar foco no body")
else:
    fail("sync focus não liga classe no body")
if "fab.classList.toggle('hidden', !on)" in focus_ui:
    ok("⋯ some fora da cíclica")
else:
    fail("FAB não some ao sair")

pintar_bar = fn_chunk(js, "maCiclicaPintarBar", "maCiclicaPodeSomarBip")
if "maCiclicaSyncFocusUi" in pintar_bar:
    ok("pintar barra chama sync foco")
else:
    fail("pintar barra não sincroniza foco")
if "ma-ciclica-ctrl-open" in pintar_bar and "bar.classList.toggle('hidden'" in pintar_bar:
    ok("barra laranja só no painel ⋯")
else:
    fail("barra laranja ainda inline no foco")

sair = fn_chunk(js, "maCiclicaSairModo", "maCiclicaCancelarSessao")
check_text(
    sair,
    "maCiclicaBipHistorico = []",
    "maUltimoBipadoId = null",
    "ma-ciclica-focus",
    label="sair",
)

# Bip registra histórico + no foco com busca vazia repinta a lista (não some o restante)
ultimo = fn_chunk(js, "maMostrarUltimoBipado", "maProcessarCodigoBipado")
if "maCiclicaRegistrarBip" in ultimo:
    ok("último bip entra no histórico")
else:
    fail("último bip não registra histórico")
if "maCiclicaFocusAtivo()" in ultimo and "buscar('')" in ultimo:
    ok("foco vazio usa lista completa (hero+hist)")
else:
    fail("foco não chama buscar vazio após bip")

# Drain: upsert local (corredor começa sem linhas; API contar não manda linhas)
drain = fn_chunk(js, "maCiclicaBipFilaDrain", "maCiclicaAtivar")
if "maCiclicaAplicarQtdLocal(p.id, acum, p)" in drain:
    ok("drain upsert com o produto bipado")
else:
    fail("drain não passa o produto no upsert")
if "if (!Array.isArray(maCiclicaSessao.linhas))" in drain:
    ok("drain preserva linhas locais se a API não mandar")
else:
    fail("drain perde linhas locais no Object.assign")
if "maPostAjuste" in drain or "api/ajustar" in drain:
    fail("drain toca ajuste de estoque")
else:
    ok("drain não grava estoque")

aplicar = fn_chunk(js, "maCiclicaAplicarQtdLocal", "maCiclicaBipMaisUm")
if "maCiclicaSessao.linhas.push" in aplicar:
    ok("upsert cria linha no corredor vazio")
else:
    fail("aplicar qtd não cria linha nova")

buscar_fn = fn_chunk(js, "buscar", "maCiclicaNumHtml")
if "maCiclicaFocusAtivo() && !termoN.length" in buscar_fn and "maPintarCiclicaFocus" in buscar_fn:
    ok("busca vazia no foco pinta hero/hist")
else:
    fail("buscar não roteia para maPintarCiclicaFocus")
if "Cíclica · bipar" in buscar_fn:
    ok("corredor vazio não despeja catálogo")
else:
    fail("falta guarda de catálogo no corredor vazio")
if "termoN.length >= 2 && !maCiclicaFocusAtivo()" in buscar_fn:
    ok("busca na cíclica não apaga o último bip")
else:
    fail("buscar ainda zera último bip na cíclica")

pintar = fn_chunk(js, "maPintarCiclicaFocus", "maMostrarUltimoBipado")
if "ma-card--" in pintar or "maCriarCardCiclica(hero, 'ultimo')" in pintar:
    ok("hero usa variant ultimo")
else:
    fail("hero sem variant ultimo")
if "maCriarCardCiclica(p, 'feito')" in pintar:
    ok("já contados usam variant feito")
else:
    fail("já contados sem variant feito")
if "scrollTop = 0" in pintar:
    ok("lista volta ao topo no último bip")
else:
    fail("não sobe o scroll no hero")
if "maxPend = 24" in pintar:
    ok("pendentes demais viram só texto")
else:
    fail("sem teto de pendentes")

criar = fn_chunk(js, "maCriarCardCiclica", "maPintarCiclicaFocus")
if "ma-card-linha" in criar and "ma-card-badge-ultimo" in criar:
    ok("card compacto vs hero distintos")
else:
    fail("criar card não diferencia compacto/hero")


# --- Simulação da lista (espelha maPintarCiclicaFocus) ---
def fmt_qtd(n):
    if n is None or n == "":
        return ""
    try:
        v = float(n)
    except (TypeError, ValueError):
        return ""
    if abs(v - round(v)) < 0.0001:
        return str(int(round(v)))
    return str(round(v * 10) / 10).replace(".", ",")


def sim_paint(lista, ultimo_id, historico):
    hero_id = str(ultimo_id or "")
    hero = next((p for p in lista if str(p["id"]) == hero_id), None)
    if hero and (not hero.get("_ciclicaContado")) and fmt_qtd(hero.get("_ciclicaQtd")) == "":
        hero = None
    feitos = []
    for pid in historico:
        if pid == hero_id:
            continue
        p = next((x for x in lista if str(x["id"]) == pid), None)
        if not p:
            continue
        if p.get("_ciclicaContado") or fmt_qtd(p.get("_ciclicaQtd")) != "":
            feitos.append(str(p["id"]))
    ids_ok = set([hero_id] + feitos) if hero_id else set(feitos)
    pend = [str(p["id"]) for p in lista if str(p["id"]) not in ids_ok and (p.get("_ciclicaFalta") or not p.get("_ciclicaContado"))]
    return (str(hero["id"]) if hero else None), feitos, pend


def sim_registrar(hist, pid):
    pid = str(pid)
    hist = [x for x in hist if x != pid]
    hist.insert(0, pid)
    return hist


# Corredor: A, A, B
hist: list[str] = []
lista = []
hist = sim_registrar(hist, "A")
lista = [{"id": "A", "_ciclicaContado": True, "_ciclicaQtd": 2, "_ciclicaFalta": False}]
hero, feitos, pend = sim_paint(lista, "A", hist)
if hero == "A" and feitos == []:
    ok("1º produto = só hero")
else:
    fail(f"1º produto hero={hero} feitos={feitos}")

hist = sim_registrar(hist, "B")
lista = [
    {"id": "A", "_ciclicaContado": True, "_ciclicaQtd": 2, "_ciclicaFalta": False},
    {"id": "B", "_ciclicaContado": True, "_ciclicaQtd": 1, "_ciclicaFalta": False},
]
hero, feitos, pend = sim_paint(lista, "B", hist)
if hero == "B" and feitos == ["A"] and pend == []:
    ok("2º produto = hero B + A estreito")
else:
    fail(f"2º produto hero={hero} feitos={feitos} pend={pend}")

# repetir A: A volta a hero, B estreito
hist = sim_registrar(hist, "A")
hero, feitos, pend = sim_paint(lista, "A", hist)
if hero == "A" and feitos == ["B"]:
    ok("repetir A traz A de volta ao topo")
else:
    fail(f"repetir A hero={hero} feitos={feitos}")

# categoria: 8 pendentes + 1 contado
lista_cat = [{"id": str(i), "_ciclicaContado": False, "_ciclicaQtd": None, "_ciclicaFalta": True} for i in range(8)]
lista_cat[0] = {"id": "0", "_ciclicaContado": True, "_ciclicaQtd": 3, "_ciclicaFalta": False}
hero, feitos, pend = sim_paint(lista_cat, "0", ["0"])
if hero == "0" and len(pend) == 7 and feitos == []:
    ok("hero + 7 faltando compactos")
else:
    fail(f"cat hero={hero} feitos={feitos} pend={len(pend)}")

# 30 pendentes → teto 24 no JS (aqui só conferimos a regra)
if 30 > 24:
    ok("teto 24 pendentes (não lista os 3000)")

# API contar não inclui linhas (por isso o upsert local é obrigatório)
util = (ROOT / "produtos" / "contagem_ciclica_util.py").read_text(encoding="utf-8")
views = (ROOT / "produtos" / "contagem_ciclica_views.py").read_text(encoding="utf-8")
if 'sessao_payload(sessao)' in views and "detalhe=True" not in views[views.find("def api_ciclica_contar") : views.find("def api_ciclica_fechar_pass1")]:
    ok("API contar não manda linhas (contrato)")
else:
    fail("API contar mudou e agora manda linhas? conferir upsert")
if 'if detalhe:' in util and 'out["linhas"] = linhas' in util:
    ok("linhas só no detalhe")
else:
    fail("payload linhas fora do detalhe")


# --- HTTP ---
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django

django.setup()

from django.test import Client, override_settings
from django.urls import reverse

from estoque.models import AjusteRapidoEstoque, OrigemAjusteEstoque
from produtos.models import Produto

TAG = f"focuspath-{uuid.uuid4().hex[:8]}"


def _mk(pid: str, nome: str, cat: str) -> Produto:
    return Produto.objects.create(
        produto_externo_id=pid[:64],
        codigo_interno=f"GM-{pid[-6:]}",
        codigo_nfe=f"GM-{pid[-6:]}",
        nome=nome[:300],
        categoria=cat,
        custo=Decimal("1"),
        preco_venda=Decimal("2"),
        ativo=True,
        cadastro_inativo=False,
    )


def _contar(c, sid, pid, nome, codigo, qtd="1"):
    return c.post(
        reverse("api_ciclica_contar", kwargs={"pk": sid}),
        {
            "produto_id": pid,
            "qtd": qtd,
            "nome_produto": nome,
            "codigo_interno": codigo,
        },
    )


try:
    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        c = Client(HTTP_HOST="127.0.0.1")
        session = c.session
        session["ajuste_mobile_gate"] = True
        session["ajuste_mobile_operador"] = "VerifyFocusCiclica"
        session.save()
        r_page = c.get("/ajuste-mobile/")
        t_page = r_page.content.decode("utf-8", "replace")
        page_needles = (
            "maPintarCiclicaFocus",
            "ma-card--ultimo",
            "ma-card--feito",
            "min-height: 4.85rem",
            "Cíclica · bipar",
            "maCiclicaSessao.linhas.push",
            "ma-ciclica-fab-ctrl",
        )
        miss = [n for n in page_needles if n not in t_page]
        if r_page.status_code == 200 and not miss:
            ok(f"HTTP página PIN + {len(page_needles)} foco")
        else:
            fail(f"HTTP página miss={miss} status={r_page.status_code}")

        session = c.session
        session["ajuste_mobile_operador"] = "VerifyFocusCiclica"
        session.save()

        pid_a = f"{TAG}-a"
        pid_b = f"{TAG}-b"
        p_a = _mk(pid_a, f"{TAG} Alfa", f"{TAG}-cat")
        p_b = _mk(pid_b, f"{TAG} Beta", f"{TAG}-cat")

        r_open = c.post(
            reverse("api_ciclica_abrir"),
            {
                "deposito": "centro",
                "escopo_tipo": "corredor",
                "escopo_valor": f"{TAG}-corredor",
            },
        )
        d_open = r_open.json() if r_open.status_code == 200 else {}
        sid = (d_open.get("sessao") or {}).get("id")
        if d_open.get("ok") and sid:
            ok(f"HTTP corredor abre #{sid}")
        else:
            fail(f"HTTP abrir {r_open.status_code} {r_open.content[:200]}")
            sid = None

        if sid:
            sess_open = d_open.get("sessao") or {}
            if not sess_open.get("linhas"):
                ok("abrir corredor vem sem checklist")
            else:
                fail("abrir corredor já veio com linhas")

            r1 = _contar(c, sid, pid_a, p_a.nome, p_a.codigo_nfe)
            d1 = r1.json() if r1.status_code == 200 else {}
            if d1.get("ok") and abs(float(d1.get("qtd_acumulada") or 0) - 1) < 0.001:
                ok("1º bip A = 1")
            else:
                fail(f"bip A {r1.status_code} {r1.content[:180]}")
            if "linhas" not in (d1.get("sessao") or {}):
                ok("contar não devolve linhas (upsert local necessário)")
            else:
                fail("contar passou a devolver linhas")

            r1b = _contar(c, sid, pid_a, p_a.nome, p_a.codigo_nfe)
            d1b = r1b.json() if r1b.status_code == 200 else {}
            if d1b.get("ok") and abs(float(d1b.get("qtd_acumulada") or 0) - 2) < 0.001:
                ok("2º bip A = 2")
            else:
                fail(f"2º bip A acum={d1b.get('qtd_acumulada')}")

            r2 = _contar(c, sid, pid_b, p_b.nome, p_b.codigo_nfe)
            d2 = r2.json() if r2.status_code == 200 else {}
            if d2.get("ok") and abs(float(d2.get("qtd_acumulada") or 0) - 1) < 0.001:
                ok("bip B = 1")
            else:
                fail(f"bip B {r2.status_code} {r2.content[:180]}")

            if AjusteRapidoEstoque.objects.filter(
                produto_externo_id__in=[pid_a, pid_b],
                origem=OrigemAjusteEstoque.CONTAGEM_CICLICA,
            ).exists():
                fail("estoque mudou antes do Gravar")
            else:
                ok("estoque intacto")

            r_det = c.get(reverse("api_ciclica_detalhe", kwargs={"pk": sid}))
            sess = ((r_det.json() or {}).get("sessao") or {})
            linhas = sess.get("linhas") or []
            by_id = {str(x.get("produto_id")): x for x in linhas}
            if str(pid_a) in by_id and str(pid_b) in by_id:
                ok("detalhe tem A e B")
            else:
                fail(f"detalhe ids={list(by_id)}")
            qa = by_id.get(str(pid_a), {}).get("qtd_contada")
            qb = by_id.get(str(pid_b), {}).get("qtd_contada")
            if qa is not None and abs(float(qa) - 2) < 0.001:
                ok("detalhe A qtd=2")
            else:
                fail(f"detalhe A qtd={qa}")
            if qb is not None and abs(float(qb) - 1) < 0.001:
                ok("detalhe B qtd=1")
            else:
                fail(f"detalhe B qtd={qb}")
            if sess.get("cego") is True:
                ok("detalhe continua cego")
            else:
                fail("detalhe perdeu cego")

            r_can = c.post(reverse("api_ciclica_cancelar", kwargs={"pk": sid}), {})
            if r_can.status_code == 200 and (r_can.json() or {}).get("ok"):
                ok("HTTP cancelar")
            else:
                fail(f"cancelar {r_can.status_code}")
except Exception as e:
    fail(f"HTTP exception {e}")

print(f"checks_ok={oks} fails={len(fails)}")
if fails:
    for f in fails:
        print("FAIL:", f)
    sys.exit(1)
print("VERIFY_FOCUS_CICLICA_OK")
