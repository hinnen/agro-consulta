# -*- coding: utf-8 -*-
"""
Prova detalhada — acesso ao laboratório de crédito
(`CREDITO-SCORE-LAB-ACESSO`: botão Gestão + não puxar pro PDV).

  set AGRO_PIN_TESTE=9973
  set PYTHONIOENCODING=utf-8
  python scripts/verify_credito_score_lab_acesso_path.py
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client, override_settings
from django.urls import reverse

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
oks: list[str] = []
fails: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_contratos_js_shell() -> None:
    print("== 1) Contratos dual-window + shell ==")
    dual = (ROOT / "produtos/static/produtos/js/agro_dual_window.js").read_text(encoding="utf-8")
    shell = (ROOT / "produtos/templates/produtos/_agro_open_external.html").read_text(encoding="utf-8")

    check("dual_fn_lab_path", "function isCreditoScoreLabPath" in dual)
    check(
        "dual_lab_match",
        "/fiado/analise-credito" in dual and "isCreditoScoreLabPath" in dual,
    )
    check(
        "dual_overlay_exclui_lab",
        "if (isCreditoScoreLabPath(pathname)) return false;" in dual,
    )
    check("dual_heal_open_gestao", "openGestao(here)" in dual and "isCreditoScoreLabPath()" in dual)
    check(
        "dual_heal_ainda_volta_pdv",
        "location.replace(pdvUrl('/pdv/?agro_dual=1&agro_app_role=pdv'))" in dual,
    )
    # PDV path detection intacta (não quebrar balcão)
    check("dual_is_pdv_path", "function isPdvPath" in dual and "/pdv/" in dual)
    check("dual_is_gestao_shell", "function isGestaoShellPath" in dual)

    check("shell_fn_lab", "function pathLookLikeCreditoScoreLab" in shell)
    check(
        "shell_early_return_lab",
        "pathLookLikeCreditoScoreLab(pathNow)" in shell
        and "Laboratório admin" in shell,
    )
    check(
        "shell_lab_nao_replace_home",
        re.search(
            r"pathLookLikeCreditoScoreLab\(pathNow\)\)\s*\{[^}]*return;",
            shell,
            re.S,
        )
        is not None,
    )


def test_botao_menu() -> None:
    print("== 2) Botão no menu Gestão ==")
    dash = (ROOT / "produtos/templates/produtos/dashboard_gerencial.html").read_text(
        encoding="utf-8"
    )
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")

    check("tpl_if_mostrar", "{% if mostrar_credito_score_shadow %}" in dash)
    check("tpl_url_lab", "credito_score_laboratorio" in dash)
    check("tpl_label", "Análise de crédito" in dash)
    check("tpl_apos_config", dash.find("Configuração") < dash.find("Análise de crédito"))
    check("tpl_key_q", 'launch-key">Q</span>' in dash or "launch-key\">Q<" in dash)
    check(
        "views_helper",
        "def _usuario_pode_ver_credito_score_shadow" in views,
    )
    check(
        "views_ctx",
        '"mostrar_credito_score_shadow"' in views
        or "'mostrar_credito_score_shadow'" in views,
    )
    check(
        "views_usa_acesso_util",
        "usuario_pode_acessar_credito_score_shadow" in views,
    )


def test_rotas_e_isolamento() -> None:
    print("== 3) Rotas + isolamento fiado operacional ==")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    check("rota_lab", "fiado/analise-credito/" in urls)
    check("rota_detalhe", "fiado/analise-credito/cliente/<int:pk>/" in urls)

    # Menu operacional de fiado não linka
    fiado_hits = 0
    for p in (ROOT / "produtos/templates/produtos").glob("*fiado*"):
        if "credito_score" in p.name:
            continue
        t = p.read_text(encoding="utf-8", errors="ignore")
        if "analise-credito" in t or "credito_score_laboratorio" in t:
            fiado_hits += 1
    check("sem_link_fiado_operacional", fiado_hits == 0, str(fiado_hits))

    # PDV wizard não consulta score
    wiz = ROOT / "produtos/static/produtos/js/pdv_wizard.js"
    if wiz.is_file():
        w = wiz.read_text(encoding="utf-8")
        check(
            "pdv_sem_score",
            "analise-credito" not in w and "credito_score" not in w,
        )


def test_http() -> None:
    print("== 4) HTTP acesso (flag / allowlist / botão) ==")
    User = get_user_model()
    superu = User.objects.filter(is_superuser=True).order_by("pk").first()
    check("tem_superuser", superu is not None, getattr(superu, "username", ""))
    if not superu:
        return

    hosts = list(
        getattr(
            __import__("django.conf", fromlist=["settings"]).settings,
            "ALLOWED_HOSTS",
            [],
        )
        or []
    )
    for h in ("testserver", "localhost", "127.0.0.1"):
        if h not in hosts:
            hosts.append(h)

    url_lab = reverse("credito_score_laboratorio")
    url_home = reverse("home")  # dashboard /
    c = Client()

    btn_marker = "dashLaunchpadAbrir('Análise de crédito'".encode("utf-8")

    with override_settings(
        AGRO_CREDITO_SCORE_SHADOW_ENABLED=False,
        AGRO_CREDITO_SCORE_SHADOW_USERNAMES="",
        ALLOWED_HOSTS=hosts,
    ):
        c.force_login(superu)
        check("flag_off_lab_404", c.get(url_lab).status_code == 404)
        r_home = c.get(url_home)
        check("flag_off_home_200", r_home.status_code == 200, str(r_home.status_code))
        check("flag_off_sem_botao_onclick", btn_marker not in r_home.content)

    with override_settings(
        AGRO_CREDITO_SCORE_SHADOW_ENABLED=True,
        AGRO_CREDITO_SCORE_SHADOW_USERNAMES="renan",
        ALLOWED_HOSTS=hosts,
    ):
        c.force_login(superu)
        r_lab = c.get(url_lab)
        check("flag_on_lab_200", r_lab.status_code == 200, str(r_lab.status_code))
        check("lab_html_titulo", b"Laborat" in r_lab.content)
        r_home2 = c.get(url_home)
        check("flag_on_home_botao", btn_marker in r_home2.content)

        comum = (
            User.objects.filter(is_superuser=False, is_staff=False)
            .exclude(username__iexact="renan")
            .first()
        )
        if comum:
            c.force_login(comum)
            check("operador_lab_404", c.get(url_lab).status_code == 404, comum.username)
            r_op = c.get(url_home)
            check("operador_sem_botao", btn_marker not in r_op.content, comum.username)


def test_regressao_shadow_e_pdv() -> None:
    print("== 5) Regressão shadow + PDV fiado ==")
    import subprocess

    env = os.environ.copy()
    env["AGRO_PIN_TESTE"] = PIN
    env["PYTHONIOENCODING"] = "utf-8"

    r1 = subprocess.run(
        [sys.executable, str(ROOT / "scripts/verify_credito_score_shadow_path.py")],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out1 = (r1.stdout or "") + (r1.stderr or "")
    m1 = re.search(r"(\d+)\s+OK\s*[·.]\s*(\d+)\s+FAIL", out1)
    if m1:
        check(
            "shadow_path_ok",
            r1.returncode == 0 and int(m1.group(2)) == 0,
            f"{m1.group(1)} OK · {m1.group(2)} FAIL",
        )
    else:
        check("shadow_path_ok", r1.returncode == 0, out1[-200:])

    r2 = subprocess.run(
        [sys.executable, str(ROOT / "scripts/verify_pdv_fiado_limite_refresh_path.py")],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out2 = (r2.stdout or "") + (r2.stderr or "")
    check(
        "pdv_fiado_refresh_ok",
        r2.returncode == 0 and "PREP_FAILS=0" in out2,
        "PREP_FAILS=0" if "PREP_FAILS=0" in out2 else out2[-180:],
    )

    r3 = subprocess.run(
        [sys.executable, str(ROOT / "scripts/verify_pdv_fiado_limite_card_path.py")],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out3 = (r3.stdout or "") + (r3.stderr or "")
    check(
        "pdv_fiado_card_ok",
        r3.returncode == 0 and ("39/39" in out3 or "OK verify" in out3),
        out3[-120:].replace("\n", " "),
    )


def main() -> int:
    print(f"=== verify CREDITO-SCORE-LAB-ACESSO · PIN={PIN} ===")
    test_contratos_js_shell()
    test_botao_menu()
    test_rotas_e_isolamento()
    test_http()
    test_regressao_shadow_e_pdv()
    print(f"=== {len(oks)} OK · {len(fails)} FAIL ===")
    if fails:
        print("FAILS:", ", ".join(fails))
        print("PREP_FAILS=" + str(len(fails)))
        return 1
    print("PREP_FAILS=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
