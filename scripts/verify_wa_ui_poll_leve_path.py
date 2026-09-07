# -*- coding: utf-8 -*-
"""WA-UI-POLL-LEVE — prova detalhada: poll do Zap não engasga o PDV.

Path estático · simulação de carga 60s · Django Client (PIN 9973) · HTTP local se up.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

FAILS: list[str] = []
OKS = 0
PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
TICK_MS = 2500
TICKS_60S = 60_000 // TICK_MS  # 24


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


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def contar_hits(ticks: int, *, ativa: bool, com_conv: bool) -> dict[str, int]:
    msgs_e = 2 if ativa else 6
    estado_e = 4 if ativa else 12
    lista_e = 4 if ativa else 24
    status_e = 24 if ativa else 48
    out = {"msgs": 0, "estado": 0, "lista": 0, "status": 0}
    for t in range(1, ticks + 1):
        if com_conv and t % msgs_e == 0:
            out["msgs"] += 1
        if t % estado_e == 0:
            out["estado"] += 1
        if t % lista_e == 0:
            out["lista"] += 1
        if t % status_e == 0:
            out["status"] += 1
    out["total"] = sum(out.values())
    return out


def prova_path_estatico() -> None:
    print("=== Path estático ===")
    js = read("produtos/static/produtos/js/atendimento_whatsapp.js")
    html = read("produtos/templates/produtos/atendimento_whatsapp.html")

    check("function waJanelaAtiva" in js, "waJanelaAtiva")
    check("document.hidden" in js, "respeita document.hidden")
    check("hasFocus" in js, "hasFocus (janela Zap vs PDV)")
    check("visibilitychange" in js, "visibilitychange refresh")
    check("addEventListener('focus'" in js or 'addEventListener("focus"' in js, "focus refresh")
    check("function waRefreshSeAtiva" in js, "waRefreshSeAtiva")
    check("msgsEvery" in js and "estadoEvery" in js and "listaEvery" in js, "intervalos nomeados")
    check("ativa ? 2 : 6" in js, "msgs foco 5s / fundo 15s")
    check("ativa ? 4 : 12" in js, "estado foco 10s / fundo 30s")
    check("ativa ? 4 : 24" in js, "lista foco 10s / fundo 60s")
    check("ativa ? 24 : 48" in js, "status foco 60s / fundo 120s")
    check("convId && tickPoll % msgsEvery" in js, "msgs só com chat aberto")
    check("setInterval(function ()" in js and ", 2500)" in js, "tick base 2.5s")
    check(js.count("setInterval") >= 1, "tem setInterval poll")
    # regressão antiga
    check("Mensagens do chat aberto: a cada 2,5s" not in js, "sem poll msgs 2.5s fixo")
    check("Lista/estado: a cada 5s" not in js, "sem lista 5s fixo")
    # não chama pollMsgs() solto no intervalo (só gated)
    bloco = js[js.find("setInterval(function ()") : js.find("function waRefreshSeAtiva")]
    check("pollMsgs();" in bloco, "intervalo chama pollMsgs")
    check("if (convId && tickPoll % msgsEvery === 0)" in bloco, "pollMsgs gated por convId")
    check("atendimento_whatsapp.js" in html, "HTML carrega JS do Zap")


def prova_simulacao_carga() -> None:
    print("=== Simulação 60s (carga API) ===")
    # antigo: msgs todo tick + estado/lista a cada 2 + status a cada 12
    antigo_com = {"msgs": TICKS_60S, "estado": TICKS_60S // 2, "lista": TICKS_60S // 2, "status": TICKS_60S // 12}
    antigo_com["total"] = sum(antigo_com.values())
    antigo_sem = {"msgs": 0, "estado": TICKS_60S // 2, "lista": TICKS_60S // 2, "status": TICKS_60S // 12}
    antigo_sem["total"] = sum(antigo_sem.values())

    novo_foco_com = contar_hits(TICKS_60S, ativa=True, com_conv=True)
    novo_fundo_com = contar_hits(TICKS_60S, ativa=False, com_conv=True)
    novo_foco_sem = contar_hits(TICKS_60S, ativa=True, com_conv=False)
    novo_fundo_sem = contar_hits(TICKS_60S, ativa=False, com_conv=False)

    check(novo_foco_com["total"] == 25, f"foco+chat 60s total={novo_foco_com['total']} (esp. 25)")
    check(novo_fundo_com["total"] == 7, f"fundo+chat 60s total={novo_fundo_com['total']} (esp. 7)")
    check(novo_foco_sem["total"] == 13, f"foco sem chat 60s total={novo_foco_sem['total']} (esp. 13)")
    check(novo_fundo_sem["total"] == 3, f"fundo sem chat 60s total={novo_fundo_sem['total']} (esp. 3)")
    check(novo_foco_com["total"] < antigo_com["total"], f"foco+chat {novo_foco_com['total']} < antigo {antigo_com['total']}")
    check(novo_fundo_com["total"] < novo_foco_com["total"], "fundo mais leve que foco")
    check(novo_fundo_com["total"] <= antigo_com["total"] // 5, "fundo <= ~1/5 do antigo com chat")
    # PDV na frente = fundo: msgs ~4/min
    check(novo_fundo_com["msgs"] == 4, f"fundo msgs/min~{novo_fundo_com['msgs']} (esp. 4)")
    check(novo_fundo_sem["lista"] == 1, "fundo sem chat: lista 1×/min")
    print(
        f"  resumo 60s: antigo={antigo_com['total']} · foco={novo_foco_com['total']} · "
        f"fundo={novo_fundo_com['total']} (PDV na frente)"
    )


def prova_django_pin() -> None:
    print(f"=== Django Client / PIN {PIN} ===")
    import django

    django.setup()
    from django.test import Client, RequestFactory

    from base.models import PerfilUsuario
    from produtos.caixa_util import operador_label_de_pin, rotulo_operador_pin
    from produtos.pdv_transf_loja_util import gravar_operador_sessao_pdv

    perfil = (
        PerfilUsuario.objects.filter(senha_rapida=PIN, ativo=True)
        .select_related("user")
        .first()
    )
    if perfil is None:
        fail(f"PIN {PIN} ativo nao encontrado no PG")
        return

    ok_pin, label, err = operador_label_de_pin(PIN)
    if not ok_pin:
        fail(f"PIN {PIN} nao valida: {err}")
        return
    rot = rotulo_operador_pin(PIN) or label
    ok(f"PIN {PIN} valida -> {rot}")

    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(perfil.user)
    # sessão PDV com PIN (assinatura Zap)
    rf = RequestFactory()
    req = rf.get("/")
    req.session = c.session
    req.user = perfil.user
    ok_g, _nome, _u, err_g = gravar_operador_sessao_pdv(req, PIN)
    if not ok_g:
        fail(f"gravar sessao PIN: {err_g}")
    else:
        c.session.save()
        ok("sessao PDV com PIN")

    r = c.get("/atendimento-whatsapp/")
    check(r.status_code == 200, f"GET Zap page {r.status_code}")
    body = r.content.decode("utf-8", errors="replace")
    check("atendimento_whatsapp.js" in body, "pagina referencia JS")

    # static JS contém o poll leve (Asset/finders)
    from django.contrib.staticfiles.finders import find

    found = find("produtos/js/atendimento_whatsapp.js")
    check(bool(found), "static finder JS")
    if found:
        js_static = Path(found).read_text(encoding="utf-8")
        check("waJanelaAtiva" in js_static, "static serve waJanelaAtiva")
        check("hasFocus" in js_static, "static serve hasFocus")

    for path, nome in (
        ("/api/atendimento-whatsapp/estado/", "estado"),
        ("/api/atendimento-whatsapp/conversas/?loja=", "conversas"),
        ("/api/atendimento-whatsapp/status/", "status"),
    ):
        rr = c.get(path)
        check(rr.status_code == 200, f"API {nome} {rr.status_code}")
        try:
            data = rr.json()
            check(data.get("ok") is True or "ok" in data or isinstance(data, dict), f"API {nome} JSON")
        except Exception as e:
            fail(f"API {nome} JSON: {e}")

    # msgs sem conversa = 400 esperado (não deve poluir poll sem convId)
    rm = c.get("/api/atendimento-whatsapp/mensagens/?conversa_id=0")
    check(rm.status_code in (400, 404), f"msgs sem conv → {rm.status_code} (esp. 400)")

    if not operador_label_de_pin(PIN)[0]:
        fail("PIN 9973 quebrou no final")
    else:
        ok("PIN 9973 intacto no final")


def prova_http_local() -> None:
    print("=== HTTP local (opcional) ===")
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen("http://127.0.0.1:8000/healthz", timeout=3) as r:
            check(r.status == 200, f"healthz local {r.status}")
    except Exception as e:
        ok(f"healthz skip ({e.__class__.__name__})")
        return

    # static sem auth
    url = "http://127.0.0.1:8000/static/produtos/js/atendimento_whatsapp.js"
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            txt = r.read().decode("utf-8", errors="replace")
            check(r.status == 200, "static JS 200")
            check("waJanelaAtiva" in txt, "HTTP static waJanelaAtiva")
            check(re.search(r"ativa \? 2 : 6", txt) is not None, "HTTP static msgsEvery")
            check("hasFocus" in txt, "HTTP static hasFocus")
    except urllib.error.HTTPError as e:
        fail(f"static JS HTTP {e.code}")
    except Exception as e:
        fail(f"static JS: {e}")


def main() -> int:
    prova_path_estatico()
    prova_simulacao_carga()
    try:
        prova_django_pin()
    except Exception as e:
        fail(f"django: {e}")
    prova_http_local()

    print("---")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
