"""Prova detalhada — hub Vendas/Tarefas + fluxo PIN/API (path VL-HUB-TAREFAS)."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.test import Client, override_settings
from django.urls import reverse

from tarefas.models import TarefaAgro, TarefaComentarioAgro, TarefaEventoAgro
from tarefas.pin_util import SESSION_OPERADOR

OK = 0
FAIL = 0
PIN = (os.environ.get("AGRO_TEST_PIN") or "9973").strip()


def check(label: str, cond: bool, detail: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {label}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"  FAIL {label}" + (f" — {detail}" if detail else ""))


def _csrf(c: Client) -> str:
    return c.cookies.get("csrftoken").value if c.cookies.get("csrftoken") else ""


def main() -> int:
    print("VERIFY VL-HUB-TAREFAS (detalhado)")
    print("== Arquivos ==")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    settings = (ROOT / "config/settings.py").read_text(encoding="utf-8")
    hub_tpl = (ROOT / "produtos/templates/produtos/vendas_lojas_hub.html").read_text(encoding="utf-8")
    pin_tpl = (ROOT / "tarefas/templates/tarefas/pin.html").read_text(encoding="utf-8")
    lista_tpl = (ROOT / "tarefas/templates/tarefas/lista.html").read_text(encoding="utf-8")
    det_tpl = (ROOT / "tarefas/templates/tarefas/detalhe.html").read_text(encoding="utf-8")
    man = (ROOT / "tarefas/migrations/0001_initial.py").read_text(encoding="utf-8")
    seed_m = (ROOT / "tarefas/migrations/0002_seed_agro_mais.py").read_text(encoding="utf-8")

    check("app_installed", "tarefas.apps.TarefasConfig" in settings)
    check("url_hub", "vendas_lojas_hub" in urls and "path('vendas/lojas/', views.vendas_lojas_hub" in urls)
    check("url_painel", "vendas/lojas/painel/" in urls)
    check("url_tarefas_include", "vendas/lojas/tarefas/" in urls and "include('tarefas.urls')" in urls)
    check("hub_dois_botoes", "vl-hub-vendas" in hub_tpl and "vl-hub-tarefas" in hub_tpl)
    check("hub_links", "vendas_lojas_resumo" in hub_tpl and "tarefas_lista" in hub_tpl)
    check("pwa_scope_hub", 'scope: "/vendas/lojas/"' in hub_tpl)
    check("pin_ui", "PIN" in pin_tpl and "tarefas_pin" in pin_tpl)
    check("lista_trocar_pin", "Trocar PIN" in lista_tpl and "tarefas_logout" in lista_tpl)
    check("detalhe_status_comentario", "btnStatus" in det_tpl and "btnComentar" in det_tpl)
    check("migrate_models", "TarefaAgro" in man and "TarefaComentarioAgro" in man and "TarefaEventoAgro" in man)
    check("migrate_seed", "equipe-centro-vila" in seed_m and "guabi-precos" in seed_m)
    models_py = (ROOT / "tarefas/models.py").read_text(encoding="utf-8")
    check("status_adiado_perm", "adiado_permanente" in models_py)
    check("status_cancelado", 'CANCELADO = "cancelado"' in models_py)
    views_py = (ROOT / "tarefas/views.py").read_text(encoding="utf-8")
    check("ordem_penultimo_perm", "ADIADO_PERM" in views_py and views_py.find("ADIADO_PERM") < views_py.find("CANCELADO"))
    check("lista_grupos_ui", "tf-grupo" in lista_tpl)
    check("seed_count_db", TarefaAgro.objects.filter(seed_key__gt="").count() >= 8)

    print("== HTTP + PIN ==")
    with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
        c = Client(enforce_csrf_checks=True)

        r0 = c.get("/vendas/lojas/")
        b0 = r0.content.decode("utf-8", "replace")
        check("hub_200", r0.status_code == 200, str(r0.status_code))
        check("hub_texto", "Vendas" in b0 and "Tarefas" in b0 and "O que você quer" in b0)

        r_gate = c.get("/vendas/lojas/tarefas/")
        check("lista_sem_pin_redirect", r_gate.status_code in (301, 302), str(r_gate.status_code))
        check(
            "lista_redirect_pin",
            "/vendas/lojas/tarefas/pin/" in (r_gate.url or r_gate.get("Location") or ""),
            str(r_gate.url or r_gate.get("Location")),
        )

        r_pin = c.get("/vendas/lojas/tarefas/pin/")
        check("pin_page_200", r_pin.status_code == 200)
        csrf = _csrf(c)
        check("csrf_cookie", bool(csrf))

        # PIN errado
        bad = c.post(
            "/vendas/lojas/tarefas/pin/",
            {"pin": "0000"},
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
            HTTP_X_CSRFTOKEN=csrf,
        )
        check("pin_errado_403", bad.status_code == 403, str(bad.status_code))

        # PIN 9973
        csrf = _csrf(c) or csrf
        ok_pin = c.post(
            "/vendas/lojas/tarefas/pin/",
            {"pin": PIN},
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
            HTTP_X_CSRFTOKEN=csrf,
        )
        j_pin = {}
        try:
            j_pin = json.loads(ok_pin.content.decode("utf-8", "replace"))
        except Exception as exc:
            check("pin_json", False, str(exc))
        check("pin_ok_200", ok_pin.status_code == 200, str(ok_pin.status_code))
        check("pin_ok_flag", j_pin.get("ok") is True, str(j_pin))
        operador = str(j_pin.get("operador") or "").strip()
        check("pin_operador_nome", bool(operador), operador or "(vazio)")
        check("sessao_operador", bool(c.session.get(SESSION_OPERADOR)), str(c.session.get(SESSION_OPERADOR)))

        r_lista = c.get("/vendas/lojas/tarefas/")
        bl = r_lista.content.decode("utf-8", "replace")
        check("lista_200", r_lista.status_code == 200, str(r_lista.status_code))
        check("lista_mostra_operador", operador[:3].lower() in bl.lower() if operador else False, operador)
        check("lista_seed_equipe", "Equipe" in bl)
        check("lista_seed_delivery", "Delivery" in bl or "catálogo" in bl.lower() or "catalogo" in bl.lower())
        check("lista_seed_billy", "Billy" in bl)
        check("lista_nova_btn", "Nova tarefa" in bl)

        r_nova = c.get("/vendas/lojas/tarefas/nova/")
        check("nova_200", r_nova.status_code == 200)

        csrf = _csrf(c) or csrf
        titulo = "Prova path VL-HUB-TAREFAS"
        criar = c.post(
            "/vendas/lojas/tarefas/api/criar/",
            data=json.dumps(
                {
                    "titulo": titulo,
                    "descricao": "Criada pelo verify detalhado.",
                    "status": "decidir",
                    "loja": "geral",
                }
            ),
            content_type="application/json",
            HTTP_X_CSRFTOKEN=csrf,
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        j_criar = {}
        try:
            j_criar = json.loads(criar.content.decode("utf-8", "replace"))
        except Exception as exc:
            check("criar_json", False, str(exc))
        check("criar_200", criar.status_code == 200, str(criar.status_code))
        check("criar_ok", j_criar.get("ok") is True, str(j_criar))
        tid = (j_criar.get("tarefa") or {}).get("id")
        check("criar_id", bool(tid), str(tid))
        t = TarefaAgro.objects.filter(pk=tid).first() if tid else None
        check("criar_db", t is not None and t.titulo == titulo)
        check("criar_por_nome", bool(t and t.criado_por_nome == operador), getattr(t, "criado_por_nome", ""))
        ev_criada = TarefaEventoAgro.objects.filter(tarefa_id=tid, tipo="criada").first() if tid else None
        check("evento_criada", ev_criada is not None and ev_criada.autor_nome == operador)

        csrf = _csrf(c) or csrf
        st = c.post(
            f"/vendas/lojas/tarefas/api/{tid}/status/",
            data=json.dumps({"status": "em_andamento"}),
            content_type="application/json",
            HTTP_X_CSRFTOKEN=csrf,
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        j_st = json.loads(st.content.decode("utf-8", "replace"))
        check("status_ok", st.status_code == 200 and j_st.get("ok") is True, str(j_st))
        t.refresh_from_db()
        check("status_db", t.status == "em_andamento")
        check(
            "evento_status",
            TarefaEventoAgro.objects.filter(tarefa_id=tid, tipo="status", autor_nome=operador).exists(),
        )

        csrf = _csrf(c) or csrf
        cm = c.post(
            f"/vendas/lojas/tarefas/api/{tid}/comentar/",
            data=json.dumps({"texto": "Comentário de prova do path."}),
            content_type="application/json",
            HTTP_X_CSRFTOKEN=csrf,
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        j_cm = json.loads(cm.content.decode("utf-8", "replace"))
        check("comentar_ok", cm.status_code == 200 and j_cm.get("ok") is True, str(j_cm))
        check(
            "comentario_db",
            TarefaComentarioAgro.objects.filter(tarefa_id=tid, autor_nome=operador).exists(),
        )
        check(
            "evento_comentario",
            TarefaEventoAgro.objects.filter(tarefa_id=tid, tipo="comentario", autor_nome=operador).exists(),
        )

        r_det = c.get(f"/vendas/lojas/tarefas/{tid}/")
        bd = r_det.content.decode("utf-8", "replace")
        check("detalhe_200", r_det.status_code == 200)
        check("detalhe_titulo", titulo in bd)
        check("detalhe_comentario", "Comentário de prova" in bd)
        check("detalhe_timeline", "Linha do tempo" in bd)

        csrf = _csrf(c) or csrf
        conc = c.post(
            f"/vendas/lojas/tarefas/api/{tid}/status/",
            data=json.dumps({"status": "concluido"}),
            content_type="application/json",
            HTTP_X_CSRFTOKEN=csrf,
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        j_conc = json.loads(conc.content.decode("utf-8", "replace"))
        check("concluir_ok", conc.status_code == 200 and j_conc.get("ok") is True)
        t.refresh_from_db()
        check("concluir_db", t.status == "concluido" and t.concluido_em is not None)

        # Sem sessão: API bloqueia
        c2 = Client(enforce_csrf_checks=True)
        c2.get("/vendas/lojas/tarefas/pin/")
        csrf2 = _csrf(c2)
        bloqueado = c2.post(
            "/vendas/lojas/tarefas/api/criar/",
            data=json.dumps({"titulo": "Sem PIN"}),
            content_type="application/json",
            HTTP_X_CSRFTOKEN=csrf2,
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )
        jb = json.loads(bloqueado.content.decode("utf-8", "replace"))
        check("api_sem_pin_401", bloqueado.status_code == 401 and jb.get("precisa_pin") is True, str(jb))

        # Trocar PIN
        csrf = _csrf(c) or csrf
        sair = c.post("/vendas/lojas/tarefas/sair/", HTTP_X_CSRFTOKEN=csrf)
        check("logout_redirect", sair.status_code in (301, 302), str(sair.status_code))
        check("logout_limpa_sessao", not bool(c.session.get(SESSION_OPERADOR)))

        # Limpa tarefa de prova
        if tid:
            TarefaAgro.objects.filter(pk=tid).delete()
            check("cleanup_prova", not TarefaAgro.objects.filter(pk=tid).exists())

        # Painel vendas ainda no lugar
        from django.contrib.auth import get_user_model

        User = get_user_model()
        user = User.objects.filter(is_active=True).order_by("id").first()
        if user:
            c3 = Client()
            c3.force_login(user)
            rp = c3.get("/vendas/lojas/painel/?periodo=hoje")
            check("painel_vendas_200", rp.status_code == 200, str(rp.status_code))
            check("painel_inicio_link", "vendas_lojas_hub" in rp.content.decode("utf-8", "replace") or "Início" in rp.content.decode("utf-8", "replace"))
        else:
            check("painel_user", False, "sem usuario")

    print(f"\nVERIFY {'OK' if FAIL == 0 else 'FAIL'} {OK}/{OK + FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
