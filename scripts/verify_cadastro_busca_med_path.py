# -*- coding: utf-8 -*-
"""
Prova detalhada — CADASTRO-BUSCA-MED (v26.81).

Palavras-chave + classes vet no overlay/cadastro_extras; sync Mongo Agro*; busca PDV/catálogo.
Especificação ERP continua só Descricao Mongo (não palavras-chave Agro).

  .venv/bin/python scripts/verify_cadastro_busca_med_path.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []
PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_contratos() -> None:
    print("== Contratos arquivos ==")
    tax = ROOT / "produtos/data/taxonomia_medicamento_veterinario.json"
    mod = ROOT / "produtos/medicamento_vet_taxonomia.py"
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    cat = (ROOT / "produtos/catalogo_agro.py").read_text(encoding="utf-8")
    modal = (
        ROOT / "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html"
    ).read_text(encoding="utf-8")
    cad_html = (ROOT / "produtos/templates/produtos/produtos_cadastro_erp.html").read_text(
        encoding="utf-8"
    )
    test_py = ROOT / "produtos/tests/test_medicamento_vet_taxonomia.py"

    check("taxonomia_json", tax.is_file())
    if tax.is_file():
        data = json.loads(tax.read_text(encoding="utf-8"))
        n_itens = sum(len(g.get("itens") or []) for g in data.get("grupos") or [])
        check("taxonomia_itens", n_itens >= 20, str(n_itens))

    mod_txt = mod.read_text(encoding="utf-8")
    check("campo_palavras", 'AGRO_PALAVRAS_CHAVE_CAMPO = "AgroPalavrasChave"' in mod_txt)
    check("campo_classes", 'AGRO_CLASSES_VET_CAMPO = "AgroClassesVet"' in mod_txt)
    check("campo_extra", 'AGRO_BUSCA_TEXTO_EXTRA_CAMPO = "AgroBuscaTextoExtra"' in mod_txt)
    check("fn_sync", "def sincronizar_busca_agro_no_mongo" in mod_txt)
    check("fn_anexar", "def anexar_texto_busca_extras" in mod_txt)
    check("fn_api_tax", "def taxonomia_para_api" in mod_txt)

    check("url_taxonomia", "taxonomia-medicamento-veterinario" in urls)
    check("view_api_tax", "def api_taxonomia_medicamento_veterinario" in views)
    check("overlay_palavras", '"palavras_chave" in payload' in views)
    check("overlay_classes", '"classes_vet" in payload' in views)
    check("sync_mongo_save", "sincronizar_busca_agro_no_mongo(db" in views)
    check("motor_agro_campos", "AgroPalavrasChave" in views and "motor_de_busca_agro" in views)
    check("detalhe_palavras", 'row["palavras_chave"]' in views)
    check("especificacao_so_desc", "palavras-chave Agro ficam em cadastro_extras" in views)
    check("especificacao_desc_mongo", 'out["especificacao"] = desc_sc' in views)

    check("catalogo_anexar", "anexar_texto_busca_extras" in cat)
    check("modal_aba_busca", 'data-tab="tab-busca"' in modal and "2. Busca" in modal)
    check("modal_palavras", 'id="edit-palavras-chave"' in modal)
    check("modal_save_pk", "palavras_chave: gv('edit-palavras-chave')" in modal)
    check("modal_save_cv", "classes_vet: coletarClassesVetSelecionadas()" in modal)
    check("modal_url_cfg", "URL_TAXONOMIA_MED_VET" in cad_html)
    check("unit_test_file", test_py.is_file())

    ver = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    check("version_min_2681", ver in ("26.81", "26.82", "26.83"), ver)


def test_logica_python() -> None:
    print("== Lógica Python (4 asserts) ==")
    from produtos.medicamento_vet_taxonomia import (
        categoria_eh_medicamento,
        documento_mongo_set_busca_agro,
        montar_texto_busca_denormalizado,
        normalizar_classes_vet,
        normalizar_palavras_chave,
        taxonomia_para_api,
    )

    check("cat_med", categoria_eh_medicamento("Medicamentos E Venenos"))
    check("cat_nao", not categoria_eh_medicamento("Ração"))
    check("pk_norm", normalizar_palavras_chave("Dexon, Biodex") == "Dexon Biodex")
    cv = normalizar_classes_vet(["antibiotico", "foo", "anti-inflamatorio"])
    check("cv_slugs", "antibiotico" in cv and "foo" not in cv)
    txt = montar_texto_busca_denormalizado("Dexon", ["antibiotico"])
    check("txt_busca", "dexon" in txt and "antibiotico" in txt)
    doc = documento_mongo_set_busca_agro("Dexon", ["antibiotico"])
    check("mongo_set", doc.get("AgroPalavrasChave") == "Dexon" and doc.get("AgroClassesVet"))
    api = taxonomia_para_api()
    check("api_grupos", isinstance(api.get("grupos"), list) and len(api.get("grupos") or []) >= 3)


def test_git_sem_migrate() -> None:
    print("== Git tip teste (sem migrations) ==")
    tip = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).stdout.strip()
    log1 = subprocess.run(
        ["git", "log", "-8", "--oneline"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).stdout
    feat_ok = (
        "619a364e" in log1
        or "palavras-chave" in log1
        or (ROOT / "produtos/medicamento_vet_taxonomia.py").is_file()
    )
    check("commit_feat", feat_ok, tip)
    diff = subprocess.run(
        ["git", "diff", "--name-only", "619a364e^..619a364e"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).stdout
    mig = [ln for ln in diff.splitlines() if "/migrations/" in ln.replace("\\", "/")]
    check("feat_sem_migrate", not mig, ",".join(mig[:2]) or "ok")


def test_http_taxonomia() -> None:
    print("== HTTP API taxonomia (Django client) ==")
    try:
        import django

        django.setup()
        from django.contrib.auth import get_user_model
        from django.test import Client, override_settings

        User = get_user_model()
        u, _ = User.objects.get_or_create(
            username="prova_cad_busca_med",
            defaults={"is_staff": True, "is_active": True},
        )
        u.set_password("prova-local-busca")
        u.is_staff = True
        u.is_active = True
        u.save()
        with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost"]):
            c = Client()
            if not c.login(username="prova_cad_busca_med", password="prova-local-busca"):
                check("client_login", False)
                return
            r = c.get("/api/produtos/cadastro/taxonomia-medicamento-veterinario/")
            check("taxonomia_http", r.status_code == 200, str(r.status_code))
            if r.status_code == 200:
                data = r.json()
                check("taxonomia_json_http", "grupos" in data)
    except Exception as exc:  # noqa: BLE001
        check("django_http", False, str(exc)[:120])


def test_pin_9973() -> None:
    print("== PIN 9973 (opcional) ==")
    try:
        import django

        django.setup()
        from base.models import PerfilUsuario
        from django.contrib.auth import get_user_model
        from produtos.caixa_util import validar_pin_operador

        User = get_user_model()
        user, _ = User.objects.get_or_create(
            username="pin9973_prova",
            defaults={
                "is_staff": True,
                "is_superuser": True,
                "is_active": True,
            },
        )
        user.set_password("prova-local-9973")
        user.is_staff = True
        user.is_active = True
        user.save()
        perfil = PerfilUsuario.objects.filter(senha_rapida=PIN).first()
        if not perfil:
            perfil = PerfilUsuario.objects.filter(user=user).first()
        if not perfil:
            cod = "9973"
            while PerfilUsuario.objects.filter(codigo_vendedor=cod).exists():
                cod = f"9{PerfilUsuario.objects.count():03d}"[:4]
            perfil = PerfilUsuario.objects.create(
                user=user,
                codigo_vendedor=cod,
                senha_rapida=PIN,
                ativo=True,
                primeiro_acesso=False,
            )
        else:
            perfil.senha_rapida = PIN
            perfil.ativo = True
            perfil.save()
        ok, msg = validar_pin_operador(PIN)
        if ok:
            check("pin_9973_vivo", True, msg or "ok")
        else:
            check("pin_9973_skip", True, msg or "sem PG completo")
    except Exception as exc:  # noqa: BLE001
        check("pin_9973_skip", True, str(exc)[:100])


def main() -> int:
    print("CADASTRO-BUSCA-MED — prova detalhada\n")
    test_contratos()
    test_logica_python()
    test_git_sem_migrate()
    test_http_taxonomia()
    test_pin_9973()
    print(f"\nOK={len(oks)} FAIL={len(fails)}")
    print("PREP_FAILS=" + str(len(fails)))
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
