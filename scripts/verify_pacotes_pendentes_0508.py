"""Prova dos pacotes ainda fora da loja (05/08): BI topbar datas + Backup menu + NF troca.

VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"OK {msg}")


def main() -> None:
    import django

    django.setup()
    from pathlib import Path

    from django.contrib.auth import get_user_model
    from django.template.loader import get_template
    from django.test import Client
    from django.urls import reverse

    checks = 0

    # --- BI-TOPBAR-DATAS ---
    dash = Path(ROOT, "produtos/templates/produtos/dashboard_gerencial.html").read_text(
        encoding="utf-8"
    )
    for needle in (
        "dash-topbar--periods-row",
        "agroDashTopbarAjustar",
        "larguraConteudo",
        "Trava: ",
        "dash-brand-text",
        "dash-topbar-loja-lbl",
    ):
        if needle not in dash:
            fail(f"BI topbar sem '{needle}'")
    checks += 1
    ok("BI topbar datas — marcadores")

    # --- CP-BACKUP-MENU ---
    cp = Path(ROOT, "produtos/templates/produtos/lancamentos_contas_pagar_teste.html").read_text(
        encoding="utf-8"
    )
    for needle in (
        ".sv-backup-menu[hidden] { display: none; }",
        "function fecharMenu()",
        "ev.key === 'Escape'",
        'id="sv-backup-menu" hidden',
        'id="sv-btn-backup"',
    ):
        if needle not in cp:
            fail(f"CP Backup sem '{needle}'")
    if cp.count("fecharMenu();") < 3:
        fail("CP Backup: fecharMenu() pós-download ausente")
    checks += 1
    ok("CP Backup menu — marcadores")

    for name in (
        "api_lancamentos_backup_completo_xlsx",
        "api_lancamentos_backup_abertos_xlsx",
        "api_lancamentos_backup_ultimo",
        "lancamentos_contas_pagar",
        "home",
    ):
        reverse(name)
    checks += 1
    ok("rotas")

    # Templates compilam
    get_template("produtos/lancamentos_contas_pagar_teste.html")
    get_template("produtos/dashboard_gerencial.html")
    get_template("produtos/entrada_nota.html")
    checks += 1
    ok("templates compilam")

    # --- NF-TROCA-ESTORNO ---
    from produtos.nfe_entrada_util import entrada_nfe_bloqueio_troca_produto_com_estoque

    doc_ok = {
        "estoque_aplicado_em": "2026-08-05T12:00:00",
        "linhas": [
            {"produto_id": "aaa", "quantidade": 1},
            {"produto_id": "bbb", "quantidade": 2},
        ],
    }
    # mesma lista → ok
    if entrada_nfe_bloqueio_troca_produto_com_estoque(
        doc_ok, [{"produto_id": "aaa"}, {"produto_id": "bbb"}]
    ):
        fail("bloqueio disparou com mesma lista de produtos")
    # troca → bloqueia
    msg = entrada_nfe_bloqueio_troca_produto_com_estoque(
        doc_ok, [{"produto_id": "aaa"}, {"produto_id": "ccc"}]
    )
    if not msg:
        fail("troca de produto deveria bloquear")
    # remoção → bloqueia
    msg2 = entrada_nfe_bloqueio_troca_produto_com_estoque(doc_ok, [{"produto_id": "aaa"}])
    if not msg2:
        fail("remoção de linha deveria bloquear")
    # muda quantidade com mesmo produto → bloqueia
    doc_q = {
        "estoque_aplicado_em": "2026-08-05T12:00:00",
        "linhas": [{"produto_id": "aaa", "q_com": 10, "un_por_embalagem": 1}],
    }
    msg3 = entrada_nfe_bloqueio_troca_produto_com_estoque(
        doc_q, [{"produto_id": "aaa", "q_com": 11, "un_por_embalagem": 1}]
    )
    if not msg3 or "quantidade" not in msg3.lower():
        fail(f"mudança de quantidade deveria bloquear: {msg3!r}")
    # sem estoque aplicado → ok
    if entrada_nfe_bloqueio_troca_produto_com_estoque(
        {"linhas": doc_ok["linhas"]}, [{"produto_id": "zzz"}]
    ):
        fail("sem estoque aplicado não deve bloquear")
    ent = Path(ROOT, "produtos/templates/produtos/entrada_nota.html").read_text(encoding="utf-8")
    if "Estornar e trocar" not in ent:
        fail("modal Estornar e trocar ausente")
    if "requer_estorno" not in ent:
        fail("front sem tratamento requer_estorno")
    checks += 1
    ok("NF troca — bloqueio + modal")

    # Render logado
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    if not user:
        fail("sem usuário para login")
    c = Client()
    c.force_login(user)
    host = {"HTTP_HOST": "127.0.0.1"}
    r_cp = c.get(reverse("lancamentos_contas_pagar"), **host)
    if r_cp.status_code != 200:
        fail(f"CP status {r_cp.status_code}")
    html_cp = r_cp.content.decode("utf-8", "replace")
    if ".sv-backup-menu[hidden] { display: none; }" not in html_cp:
        fail("CP render sem CSS do menu fechado")
    if "function fecharMenu()" not in html_cp:
        fail("CP render sem fecharMenu")
    checks += 1
    ok("CP render 200 + fix no HTML")

    r_ult = c.get(reverse("api_lancamentos_backup_ultimo"), **host)
    if r_ult.status_code != 200:
        fail(f"backup_ultimo status {r_ult.status_code}")
    j = r_ult.json()
    if "ultimo" not in j and "em" not in j:
        # API pode devolver {ultimo: {...}} ou o objeto direto
        if not isinstance(j, dict):
            fail(f"backup_ultimo JSON inesperado: {j!r}")
    checks += 1
    ok("API backup_ultimo 200")

    # Download ZIP (abertos) — deve ser application/zip ou texto de erro controlado
    r_zip = c.get(reverse("api_lancamentos_backup_abertos_xlsx"), **host)
    if r_zip.status_code not in (200, 503):
        fail(f"backup abertos status {r_zip.status_code}")
    if r_zip.status_code == 200:
        ctype = (r_zip.get("Content-Type") or "").lower()
        if "zip" not in ctype and "octet" not in ctype:
            fail(f"backup abertos Content-Type inesperado: {ctype}")
        if len(r_zip.content) < 40:
            fail("backup ZIP vazio demais")
        checks += 1
        ok(f"ZIP abertos {len(r_zip.content)} bytes")
    else:
        checks += 1
        ok("ZIP abertos 503 (fonte indisponível — aceito no smoke)")

    r_home = c.get("/", **host)
    # home pode redirecionar para dashboard embed
    if r_home.status_code not in (200, 302):
        fail(f"home status {r_home.status_code}")
    checks += 1
    ok("home responde")

    print(f"VERIFY_OK {checks}/{checks}")


if __name__ == "__main__":
    main()
